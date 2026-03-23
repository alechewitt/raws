// REST-JSON protocol serializer/parser
//
// Used by services like Lambda, API Gateway, Kinesis.
// - HTTP method from operation.http.method (GET, POST, PUT, DELETE, etc.)
// - URI template from operation.http.requestUri (e.g., /2015-03-31/functions/{FunctionName})
// - Members with location=uri go into the URL path
// - Members with location=querystring go into query params
// - Members with location=header go into HTTP headers
// - Remaining members go into JSON body
// - If operation has no body members, no body is sent
//
// Response parsing:
// - Members with location=header come from response headers
// - Members with location=statusCode come from HTTP status
// - Remaining members parsed from JSON body

use anyhow::{Context, Result};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde_json::Value;
use std::collections::HashMap;

/// The percent-encoding set for URI path segments.
/// Encodes everything except unreserved characters: A-Z a-z 0-9 - _ . ~
const URI_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Serialized REST-JSON request parts:
/// (resolved_uri, extra_headers, query_params, body_json_string)
pub type RestJsonRequestParts = (
    String,
    Vec<(String, String)>,
    Vec<(String, String)>,
    Option<String>,
);

/// Build the REST-JSON request: resolve URI template, extract headers/querystring,
/// serialize remaining members as JSON body.
///
/// Returns (resolved_uri, extra_headers, query_params, body_json_string)
pub fn serialize_rest_json_request(
    uri_template: &str,
    input: &Value,
    input_shape_name: &str,
    shapes: &HashMap<String, Value>,
) -> Result<RestJsonRequestParts> {
    let shape_def = shapes
        .get(input_shape_name)
        .ok_or_else(|| anyhow::anyhow!("Input shape '{}' not found", input_shape_name))?;

    let members = shape_def
        .get("members")
        .and_then(|m| m.as_object())
        .cloned()
        .unwrap_or_default();

    let input_obj = input.as_object().cloned().unwrap_or_default();

    // Partition members by location
    let mut uri_params: HashMap<String, String> = HashMap::new();
    let mut query_params: Vec<(String, String)> = Vec::new();
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut body_map = serde_json::Map::new();

    for (member_name, member_def) in &members {
        let param_value = match input_obj.get(member_name) {
            Some(v) if !v.is_null() => v,
            _ => continue,
        };

        let location = member_def
            .get("location")
            .and_then(|l| l.as_str())
            .unwrap_or("");

        let location_name = member_def
            .get("locationName")
            .and_then(|n| n.as_str())
            .unwrap_or(member_name.as_str());

        match location {
            "uri" => {
                let str_val = value_to_string(param_value);
                uri_params.insert(location_name.to_string(), str_val);
            }
            "querystring" => {
                let str_val = value_to_string(param_value);
                query_params.push((location_name.to_string(), str_val));
            }
            "header" => {
                let str_val = value_to_string(param_value);
                headers.push((location_name.to_string(), str_val));
            }
            "headers" => {
                // "headers" location: the value is a map, and each key/value pair
                // becomes a header with the prefix + key as the header name
                if let Some(obj) = param_value.as_object() {
                    for (k, v) in obj {
                        let header_name = format!("{}{}", location_name, k);
                        let header_val = value_to_string(v);
                        headers.push((header_name, header_val));
                    }
                }
            }
            _ => {
                // No location: goes into JSON body
                body_map.insert(member_name.clone(), param_value.clone());
            }
        }
    }

    // Resolve URI template
    let resolved_uri = render_uri_template(uri_template, &uri_params)?;

    // Build body: only if there are body members
    let body = if body_map.is_empty() {
        None
    } else {
        let body_str = serde_json::to_string(&Value::Object(body_map))
            .context("Failed to serialize REST-JSON request body")?;
        Some(body_str)
    };

    Ok((resolved_uri, headers, query_params, body))
}

/// Render a URI template by replacing `{ParamName}` placeholders with
/// percent-encoded values. Greedy labels (`{ParamName+}`) preserve forward slashes.
fn render_uri_template(
    uri_template: &str,
    params: &HashMap<String, String>,
) -> Result<String> {
    let mut result = String::with_capacity(uri_template.len());
    let mut chars = uri_template.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '{' {
            // Extract the template variable name
            let mut var_name = String::new();
            for c in chars.by_ref() {
                if c == '}' {
                    break;
                }
                var_name.push(c);
            }

            // Check for greedy label (ends with '+')
            let (lookup_name, greedy) = if var_name.ends_with('+') {
                (&var_name[..var_name.len() - 1], true)
            } else {
                (var_name.as_str(), false)
            };

            let value = params
                .get(lookup_name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "URI template variable '{}' not found in input parameters",
                        lookup_name
                    )
                })?;

            if greedy {
                // Greedy: encode each segment but preserve '/'
                let encoded: String = value
                    .split('/')
                    .map(|segment| {
                        utf8_percent_encode(segment, URI_ENCODE_SET).to_string()
                    })
                    .collect::<Vec<_>>()
                    .join("/");
                result.push_str(&encoded);
            } else {
                let encoded = utf8_percent_encode(value, URI_ENCODE_SET).to_string();
                result.push_str(&encoded);
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

/// Convert a serde_json::Value to its string representation for use
/// in URI, querystring, or header values.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Null => String::new(),
        // For arrays and objects, use JSON serialization
        _ => value.to_string(),
    }
}

/// Parse a REST-JSON response body (plus headers and status code) into serde_json::Value.
pub fn parse_rest_json_response(
    body: &str,
    status_code: u16,
    response_headers: &HashMap<String, String>,
    output_shape_name: &str,
    shapes: &HashMap<String, Value>,
) -> Result<Value> {
    let shape_def = shapes
        .get(output_shape_name)
        .ok_or_else(|| anyhow::anyhow!("Output shape '{}' not found", output_shape_name))?;

    let members = shape_def
        .get("members")
        .and_then(|m| m.as_object())
        .cloned()
        .unwrap_or_default();

    // Check if there's a payload member
    let payload_member = shape_def
        .get("payload")
        .and_then(|p| p.as_str());

    // Start with body-parsed fields
    let mut result = if let Some(payload_name) = payload_member {
        // If a payload is specified, only that member comes from the body
        let mut map = serde_json::Map::new();
        if let Some(member_def) = members.get(payload_name) {
            let member_shape_name = member_def
                .get("shape")
                .and_then(|s| s.as_str())
                .unwrap_or("");
            let member_shape = shapes.get(member_shape_name);
            let member_type = member_shape
                .and_then(|s| s.get("type"))
                .and_then(|t| t.as_str())
                .unwrap_or("string");

            if member_type == "blob" || member_type == "string" {
                // Streaming: raw body is the value
                map.insert(payload_name.to_string(), Value::String(body.to_string()));
            } else if !body.trim().is_empty() {
                // Parse body as JSON for the payload member
                let parsed: Value = serde_json::from_str(body)
                    .context("Failed to parse REST-JSON payload body")?;
                map.insert(payload_name.to_string(), parsed);
            }
        }
        map
    } else {
        // No payload: parse entire body as JSON and use as the base
        if body.trim().is_empty() {
            serde_json::Map::new()
        } else {
            let parsed: Value = serde_json::from_str(body)
                .context("Failed to parse REST-JSON response body")?;
            match parsed {
                Value::Object(map) => map,
                _ => serde_json::Map::new(),
            }
        }
    };

    // Extract non-payload attributes (headers, statusCode)
    for (member_name, member_def) in &members {
        let location = member_def
            .get("location")
            .and_then(|l| l.as_str())
            .unwrap_or("");

        let location_name = member_def
            .get("locationName")
            .and_then(|n| n.as_str())
            .unwrap_or(member_name.as_str());

        match location {
            "statusCode" => {
                result.insert(
                    member_name.clone(),
                    Value::Number(serde_json::Number::from(status_code)),
                );
            }
            "header" => {
                // Case-insensitive header lookup
                let lower_location = location_name.to_lowercase();
                for (hdr_name, hdr_value) in response_headers {
                    if hdr_name.to_lowercase() == lower_location {
                        // Determine the member's type to parse the value correctly
                        let member_shape_name = member_def
                            .get("shape")
                            .and_then(|s| s.as_str())
                            .unwrap_or("");
                        let member_shape = shapes.get(member_shape_name);
                        let parsed_value =
                            parse_header_value(hdr_value, member_shape);
                        result.insert(member_name.clone(), parsed_value);
                        break;
                    }
                }
            }
            "headers" => {
                // Prefix-based header map
                let prefix = location_name.to_lowercase();
                let mut header_map = serde_json::Map::new();
                for (hdr_name, hdr_value) in response_headers {
                    let lower_name = hdr_name.to_lowercase();
                    if lower_name.starts_with(&prefix) {
                        let key = hdr_name[prefix.len()..].to_string();
                        header_map.insert(key, Value::String(hdr_value.clone()));
                    }
                }
                if !header_map.is_empty() {
                    result.insert(member_name.clone(), Value::Object(header_map));
                }
            }
            _ => {
                // Body members are already in `result` from the JSON parse
            }
        }
    }

    Ok(Value::Object(result))
}

/// Parse a header value into the appropriate JSON type based on the shape definition.
fn parse_header_value(value: &str, shape: Option<&Value>) -> Value {
    let shape_type = shape
        .and_then(|s| s.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("string");

    match shape_type {
        "integer" | "long" => value
            .parse::<i64>()
            .map(|n| Value::Number(serde_json::Number::from(n)))
            .unwrap_or_else(|_| Value::String(value.to_string())),
        "float" | "double" => value
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string())),
        "boolean" => Value::Bool(value.eq_ignore_ascii_case("true")),
        _ => Value::String(value.to_string()),
    }
}

/// Parse a REST-JSON error response. Returns (error_code, message).
///
/// Error format varies:
/// - `{"__type": "com.amazonaws.lambda#ResourceNotFoundException", "message": "..."}`
/// - `{"code": "...", "message": "..."}`
/// - `{"Code": "...", "Message": "..."}`
/// - Error code may also come from `x-amzn-errortype` header (handled by caller).
///
/// The `__type` field may contain a namespace prefix separated by `#`.
pub fn parse_rest_json_error(body: &str) -> Result<(String, String)> {
    let parsed: Value =
        serde_json::from_str(body).context("Failed to parse REST-JSON error response")?;

    // Extract error code: try __type first, then code/Code
    let raw_code = parsed
        .get("__type")
        .and_then(|v| v.as_str())
        .or_else(|| parsed.get("code").and_then(|v| v.as_str()))
        .or_else(|| parsed.get("Code").and_then(|v| v.as_str()))
        .unwrap_or("");

    // Strip namespace prefix (everything before and including '#')
    // Also strip colon suffix (e.g., "ErrorType:http://..." -> "ErrorType")
    let error_code = raw_code
        .rsplit_once('#')
        .map(|(_, code)| code)
        .unwrap_or(raw_code);

    let error_code = error_code
        .split_once(':')
        .map(|(code, _)| code)
        .unwrap_or(error_code);

    // Extract message: try "message" (lowercase) first, then "Message" (capitalized)
    let message = parsed
        .get("message")
        .and_then(|v| v.as_str())
        .or_else(|| parsed.get("Message").and_then(|v| v.as_str()))
        .unwrap_or("");

    Ok((error_code.to_string(), message.to_string()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Helper: build a shapes map mimicking Lambda's GetFunction
    fn lambda_get_function_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();
        shapes.insert(
            "GetFunctionRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "FunctionName": {
                        "shape": "NamespacedFunctionName",
                        "location": "uri",
                        "locationName": "FunctionName"
                    },
                    "Qualifier": {
                        "shape": "Qualifier",
                        "location": "querystring",
                        "locationName": "Qualifier"
                    }
                }
            }),
        );
        shapes.insert(
            "NamespacedFunctionName".to_string(),
            json!({"type": "string"}),
        );
        shapes.insert("Qualifier".to_string(), json!({"type": "string"}));
        shapes
    }

    // Helper: build a shapes map mimicking Lambda's Invoke
    fn lambda_invoke_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();
        shapes.insert(
            "InvocationRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "FunctionName": {
                        "shape": "NamespacedFunctionName",
                        "location": "uri",
                        "locationName": "FunctionName"
                    },
                    "InvocationType": {
                        "shape": "InvocationType",
                        "location": "header",
                        "locationName": "X-Amz-Invocation-Type"
                    },
                    "LogType": {
                        "shape": "LogType",
                        "location": "header",
                        "locationName": "X-Amz-Log-Type"
                    },
                    "Qualifier": {
                        "shape": "Qualifier",
                        "location": "querystring",
                        "locationName": "Qualifier"
                    },
                    "Payload": {
                        "shape": "Blob"
                    }
                },
                "payload": "Payload"
            }),
        );
        shapes.insert(
            "NamespacedFunctionName".to_string(),
            json!({"type": "string"}),
        );
        shapes.insert("InvocationType".to_string(), json!({"type": "string"}));
        shapes.insert("LogType".to_string(), json!({"type": "string"}));
        shapes.insert("Qualifier".to_string(), json!({"type": "string"}));
        shapes.insert("Blob".to_string(), json!({"type": "blob"}));
        shapes
    }

    // Helper: shapes for a service with body members (e.g., API Gateway CreateRestApi)
    fn body_members_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();
        shapes.insert(
            "CreateRestApiRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "name": { "shape": "String" },
                    "description": { "shape": "String" },
                    "version": { "shape": "String" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes
    }

    // Helper: shapes for response parsing (Lambda InvocationResponse)
    fn lambda_invocation_response_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();
        shapes.insert(
            "InvocationResponse".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "StatusCode": {
                        "shape": "Integer",
                        "location": "statusCode"
                    },
                    "FunctionError": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "X-Amz-Function-Error"
                    },
                    "LogResult": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "X-Amz-Log-Result"
                    },
                    "Payload": {
                        "shape": "Blob"
                    },
                    "ExecutedVersion": {
                        "shape": "Version",
                        "location": "header",
                        "locationName": "X-Amz-Executed-Version"
                    }
                },
                "payload": "Payload"
            }),
        );
        shapes.insert("Integer".to_string(), json!({"type": "integer"}));
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes.insert("Blob".to_string(), json!({"type": "blob"}));
        shapes.insert("Version".to_string(), json!({"type": "string"}));
        shapes
    }

    // Helper: shapes for response with JSON body (e.g., Lambda GetFunctionResponse)
    fn lambda_get_function_response_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();
        shapes.insert(
            "GetFunctionResponse".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Configuration": { "shape": "FunctionConfiguration" },
                    "Code": { "shape": "FunctionCodeLocation" }
                }
            }),
        );
        shapes.insert(
            "FunctionConfiguration".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "FunctionName": { "shape": "String" },
                    "Runtime": { "shape": "String" }
                }
            }),
        );
        shapes.insert(
            "FunctionCodeLocation".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "RepositoryType": { "shape": "String" },
                    "Location": { "shape": "String" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes
    }

    // ---------------------------------------------------------------
    // Feature: rest-json-protocol-serializer
    // ---------------------------------------------------------------

    #[test]
    fn rest_json_serialize_uri_template_simple() {
        let shapes = lambda_get_function_shapes();
        let input = json!({
            "FunctionName": "my-function"
        });

        let (uri, headers, query, body) = serialize_rest_json_request(
            "/2015-03-31/functions/{FunctionName}",
            &input,
            "GetFunctionRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/2015-03-31/functions/my-function");
        assert!(headers.is_empty());
        assert!(query.is_empty());
        assert!(body.is_none());
    }

    #[test]
    fn rest_json_serialize_uri_with_querystring() {
        let shapes = lambda_get_function_shapes();
        let input = json!({
            "FunctionName": "my-function",
            "Qualifier": "$LATEST"
        });

        let (uri, headers, query, body) = serialize_rest_json_request(
            "/2015-03-31/functions/{FunctionName}",
            &input,
            "GetFunctionRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/2015-03-31/functions/my-function");
        assert!(headers.is_empty());
        assert_eq!(query.len(), 1);
        assert_eq!(query[0].0, "Qualifier");
        assert_eq!(query[0].1, "$LATEST");
        assert!(body.is_none());
    }

    #[test]
    fn rest_json_serialize_uri_percent_encoding() {
        let shapes = lambda_get_function_shapes();
        let input = json!({
            "FunctionName": "my function/special"
        });

        let (uri, _headers, _query, _body) = serialize_rest_json_request(
            "/2015-03-31/functions/{FunctionName}",
            &input,
            "GetFunctionRequest",
            &shapes,
        )
        .unwrap();

        // Spaces encoded as %20, slashes encoded as %2F for non-greedy
        assert_eq!(uri, "/2015-03-31/functions/my%20function%2Fspecial");
    }

    #[test]
    fn rest_json_serialize_greedy_label() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "GetObjectRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Bucket": {
                        "shape": "String",
                        "location": "uri",
                        "locationName": "Bucket"
                    },
                    "Key": {
                        "shape": "String",
                        "location": "uri",
                        "locationName": "Key"
                    }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "Bucket": "my-bucket",
            "Key": "path/to/my file.txt"
        });

        let (uri, _headers, _query, _body) = serialize_rest_json_request(
            "/{Bucket}/{Key+}",
            &input,
            "GetObjectRequest",
            &shapes,
        )
        .unwrap();

        // Greedy label preserves slashes but encodes spaces
        assert_eq!(uri, "/my-bucket/path/to/my%20file.txt");
    }

    #[test]
    fn rest_json_serialize_with_headers() {
        let shapes = lambda_invoke_shapes();
        let input = json!({
            "FunctionName": "my-function",
            "InvocationType": "RequestResponse",
            "LogType": "Tail",
            "Qualifier": "v1"
        });

        let (uri, headers, query, body) = serialize_rest_json_request(
            "/2015-03-31/functions/{FunctionName}/invocations",
            &input,
            "InvocationRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/2015-03-31/functions/my-function/invocations");

        // Check headers
        let header_map: HashMap<&str, &str> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            header_map.get("X-Amz-Invocation-Type"),
            Some(&"RequestResponse")
        );
        assert_eq!(header_map.get("X-Amz-Log-Type"), Some(&"Tail"));

        // Check querystring
        assert_eq!(query.len(), 1);
        assert_eq!(query[0].0, "Qualifier");
        assert_eq!(query[0].1, "v1");

        // No body (Payload not provided)
        assert!(body.is_none());
    }

    #[test]
    fn rest_json_serialize_body_members() {
        let shapes = body_members_shapes();
        let input = json!({
            "name": "MyAPI",
            "description": "Test API",
            "version": "1.0"
        });

        let (uri, headers, query, body) = serialize_rest_json_request(
            "/restapis",
            &input,
            "CreateRestApiRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/restapis");
        assert!(headers.is_empty());
        assert!(query.is_empty());

        // Body should contain all members as JSON
        let body_str = body.unwrap();
        let body_json: Value = serde_json::from_str(&body_str).unwrap();
        assert_eq!(body_json["name"], "MyAPI");
        assert_eq!(body_json["description"], "Test API");
        assert_eq!(body_json["version"], "1.0");
    }

    #[test]
    fn rest_json_serialize_empty_input() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "EmptyRequest".to_string(),
            json!({
                "type": "structure",
                "members": {}
            }),
        );

        let input = json!({});
        let (uri, headers, query, body) = serialize_rest_json_request(
            "/resource",
            &input,
            "EmptyRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/resource");
        assert!(headers.is_empty());
        assert!(query.is_empty());
        assert!(body.is_none());
    }

    #[test]
    fn rest_json_serialize_null_input() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "EmptyRequest".to_string(),
            json!({
                "type": "structure",
                "members": {}
            }),
        );

        let input = Value::Null;
        let (uri, headers, query, body) = serialize_rest_json_request(
            "/resource",
            &input,
            "EmptyRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/resource");
        assert!(headers.is_empty());
        assert!(query.is_empty());
        assert!(body.is_none());
    }

    #[test]
    fn rest_json_serialize_mixed_locations() {
        // Test a request with uri, querystring, header, and body members all at once
        let mut shapes = HashMap::new();
        shapes.insert(
            "MixedRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Id": {
                        "shape": "String",
                        "location": "uri",
                        "locationName": "Id"
                    },
                    "Filter": {
                        "shape": "String",
                        "location": "querystring",
                        "locationName": "filter"
                    },
                    "Token": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "X-Auth-Token"
                    },
                    "Name": { "shape": "String" },
                    "Description": { "shape": "String" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "Id": "abc123",
            "Filter": "active",
            "Token": "my-token",
            "Name": "TestResource",
            "Description": "A test resource"
        });

        let (uri, headers, query, body) = serialize_rest_json_request(
            "/resources/{Id}",
            &input,
            "MixedRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/resources/abc123");

        let header_map: HashMap<&str, &str> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(header_map.get("X-Auth-Token"), Some(&"my-token"));

        assert_eq!(query.len(), 1);
        assert_eq!(query[0], ("filter".to_string(), "active".to_string()));

        let body_str = body.unwrap();
        let body_json: Value = serde_json::from_str(&body_str).unwrap();
        assert_eq!(body_json["Name"], "TestResource");
        assert_eq!(body_json["Description"], "A test resource");
        // URI, querystring, and header members should NOT be in the body
        assert!(body_json.get("Id").is_none());
        assert!(body_json.get("Filter").is_none());
        assert!(body_json.get("Token").is_none());
    }

    #[test]
    fn rest_json_serialize_boolean_in_querystring() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "DryRun": {
                        "shape": "Boolean",
                        "location": "querystring",
                        "locationName": "dryRun"
                    }
                }
            }),
        );
        shapes.insert("Boolean".to_string(), json!({"type": "boolean"}));

        let input = json!({ "DryRun": true });
        let (_uri, _headers, query, _body) = serialize_rest_json_request(
            "/test",
            &input,
            "TestRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(query.len(), 1);
        assert_eq!(query[0], ("dryRun".to_string(), "true".to_string()));
    }

    #[test]
    fn rest_json_serialize_number_in_querystring() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "MaxItems": {
                        "shape": "Integer",
                        "location": "querystring",
                        "locationName": "maxItems"
                    }
                }
            }),
        );
        shapes.insert("Integer".to_string(), json!({"type": "integer"}));

        let input = json!({ "MaxItems": 50 });
        let (_uri, _headers, query, _body) = serialize_rest_json_request(
            "/test",
            &input,
            "TestRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(query.len(), 1);
        assert_eq!(query[0], ("maxItems".to_string(), "50".to_string()));
    }

    // ---------------------------------------------------------------
    // Feature: rest-json-protocol-parser
    // ---------------------------------------------------------------

    #[test]
    fn rest_json_parse_response_json_body() {
        let shapes = lambda_get_function_response_shapes();
        let body = r#"{
            "Configuration": {
                "FunctionName": "my-function",
                "Runtime": "python3.9"
            },
            "Code": {
                "RepositoryType": "S3",
                "Location": "https://awslambda-us-east-1-tasks.s3.us-east-1.amazonaws.com/..."
            }
        }"#;

        let headers = HashMap::new();

        let result = parse_rest_json_response(
            body,
            200,
            &headers,
            "GetFunctionResponse",
            &shapes,
        )
        .unwrap();

        assert_eq!(
            result["Configuration"]["FunctionName"]
                .as_str()
                .unwrap(),
            "my-function"
        );
        assert_eq!(
            result["Configuration"]["Runtime"].as_str().unwrap(),
            "python3.9"
        );
        assert_eq!(result["Code"]["RepositoryType"].as_str().unwrap(), "S3");
    }

    #[test]
    fn rest_json_parse_response_with_headers_and_status() {
        let shapes = lambda_invocation_response_shapes();
        let body = "hello world";

        let mut headers = HashMap::new();
        headers.insert(
            "X-Amz-Function-Error".to_string(),
            "Unhandled".to_string(),
        );
        headers.insert(
            "X-Amz-Log-Result".to_string(),
            "base64logdata".to_string(),
        );
        headers.insert(
            "X-Amz-Executed-Version".to_string(),
            "$LATEST".to_string(),
        );

        let result = parse_rest_json_response(
            body,
            200,
            &headers,
            "InvocationResponse",
            &shapes,
        )
        .unwrap();

        // Status code
        assert_eq!(result["StatusCode"].as_u64().unwrap(), 200);

        // Headers
        assert_eq!(
            result["FunctionError"].as_str().unwrap(),
            "Unhandled"
        );
        assert_eq!(
            result["LogResult"].as_str().unwrap(),
            "base64logdata"
        );
        assert_eq!(
            result["ExecutedVersion"].as_str().unwrap(),
            "$LATEST"
        );

        // Payload (streaming, raw body)
        assert_eq!(result["Payload"].as_str().unwrap(), "hello world");
    }

    #[test]
    fn rest_json_parse_response_empty_body() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "DeleteResponse".to_string(),
            json!({
                "type": "structure",
                "members": {}
            }),
        );

        let result = parse_rest_json_response(
            "",
            204,
            &HashMap::new(),
            "DeleteResponse",
            &shapes,
        )
        .unwrap();

        assert!(result.is_object());
    }

    #[test]
    fn rest_json_parse_response_case_insensitive_headers() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestResponse".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "RequestId": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "x-amzn-requestid"
                    }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let mut headers = HashMap::new();
        headers.insert(
            "X-Amzn-RequestId".to_string(),
            "abc-123-def".to_string(),
        );

        let result = parse_rest_json_response(
            "{}",
            200,
            &headers,
            "TestResponse",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["RequestId"].as_str().unwrap(), "abc-123-def");
    }

    #[test]
    fn rest_json_parse_response_integer_header() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestResponse".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "ContentLength": {
                        "shape": "Long",
                        "location": "header",
                        "locationName": "Content-Length"
                    }
                }
            }),
        );
        shapes.insert("Long".to_string(), json!({"type": "long"}));

        let mut headers = HashMap::new();
        headers.insert("Content-Length".to_string(), "12345".to_string());

        let result = parse_rest_json_response(
            "{}",
            200,
            &headers,
            "TestResponse",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["ContentLength"].as_i64().unwrap(), 12345);
    }

    #[test]
    fn rest_json_parse_response_body_and_headers_combined() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "CombinedResponse".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "StatusCode": {
                        "shape": "Integer",
                        "location": "statusCode"
                    },
                    "RequestId": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "x-amzn-requestid"
                    },
                    "Name": { "shape": "String" },
                    "Count": { "shape": "Integer" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes.insert("Integer".to_string(), json!({"type": "integer"}));

        let body = r#"{"Name": "test", "Count": 42}"#;
        let mut headers = HashMap::new();
        headers.insert("x-amzn-requestid".to_string(), "req-456".to_string());

        let result = parse_rest_json_response(
            body,
            200,
            &headers,
            "CombinedResponse",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["StatusCode"].as_u64().unwrap(), 200);
        assert_eq!(result["RequestId"].as_str().unwrap(), "req-456");
        assert_eq!(result["Name"].as_str().unwrap(), "test");
        assert_eq!(result["Count"].as_i64().unwrap(), 42);
    }

    // ---------------------------------------------------------------
    // Error parsing
    // ---------------------------------------------------------------

    #[test]
    fn rest_json_parse_error_with_type_and_message() {
        let body = r#"{"__type": "ResourceNotFoundException", "message": "Function not found"}"#;
        let (code, message) = parse_rest_json_error(body).unwrap();
        assert_eq!(code, "ResourceNotFoundException");
        assert_eq!(message, "Function not found");
    }

    #[test]
    fn rest_json_parse_error_with_namespace() {
        let body = r#"{"__type": "com.amazonaws.lambda#ResourceNotFoundException", "message": "Function not found: arn:aws:lambda:us-east-1:123456789012:function:missing"}"#;
        let (code, message) = parse_rest_json_error(body).unwrap();
        assert_eq!(code, "ResourceNotFoundException");
        assert!(message.contains("Function not found"));
    }

    #[test]
    fn rest_json_parse_error_with_code_field() {
        let body = r#"{"code": "ThrottlingException", "message": "Rate exceeded"}"#;
        let (code, message) = parse_rest_json_error(body).unwrap();
        assert_eq!(code, "ThrottlingException");
        assert_eq!(message, "Rate exceeded");
    }

    #[test]
    fn rest_json_parse_error_capitalized_message() {
        let body =
            r#"{"__type": "ValidationException", "Message": "Invalid input"}"#;
        let (code, message) = parse_rest_json_error(body).unwrap();
        assert_eq!(code, "ValidationException");
        assert_eq!(message, "Invalid input");
    }

    #[test]
    fn rest_json_parse_error_with_colon_suffix() {
        // Some services return type with HTTP URL suffix
        let body =
            r#"{"__type": "AccessDeniedException:http://internal.amazon.com/", "Message": "Access denied"}"#;
        let (code, message) = parse_rest_json_error(body).unwrap();
        assert_eq!(code, "AccessDeniedException");
        assert_eq!(message, "Access denied");
    }

    #[test]
    fn rest_json_parse_error_missing_type() {
        let body = r#"{"message": "Something went wrong"}"#;
        let (code, message) = parse_rest_json_error(body).unwrap();
        assert_eq!(code, "");
        assert_eq!(message, "Something went wrong");
    }

    #[test]
    fn rest_json_parse_error_invalid_json() {
        let result = parse_rest_json_error("not json");
        assert!(result.is_err());
    }
}
