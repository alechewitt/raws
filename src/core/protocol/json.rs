// AWS JSON protocol serializer/parser
//
// Used by services like DynamoDB, KMS, CloudTrail, CodeCommit.
// - HTTP method: POST
// - Content-Type: application/x-amz-json-{version}
// - Header: X-Amz-Target: {targetPrefix}.{operationName}
// - Body: JSON serialization of input
// - Response: JSON body deserialized to serde_json::Value
// - Errors: {"__type": "...", "message"/"Message": "..."}

use anyhow::{Context, Result};
use serde_json::Value;

/// Build the X-Amz-Target header value for JSON protocol requests.
///
/// Format: `{target_prefix}.{operation_name}`
/// Example: `DynamoDB_20120810.ListTables`
pub fn build_target_header(target_prefix: &str, operation_name: &str) -> String {
    format!("{}.{}", target_prefix, operation_name)
}

/// Build the Content-Type header for JSON protocol.
///
/// Format: `application/x-amz-json-{json_version}`
/// Example: `application/x-amz-json-1.0`
pub fn build_content_type(json_version: &str) -> String {
    format!("application/x-amz-json-{}", json_version)
}

/// Serialize the request body as JSON. For JSON protocol, the input value
/// is serialized directly. If there are no input params, send "{}".
pub fn serialize_json_request(input: &Value) -> Result<String> {
    match input {
        Value::Null => Ok("{}".to_string()),
        Value::Object(map) if map.is_empty() => Ok("{}".to_string()),
        _ => serde_json::to_string(input).context("Failed to serialize JSON request body"),
    }
}

/// Parse a JSON response body into serde_json::Value.
pub fn parse_json_response(body: &str) -> Result<Value> {
    if body.trim().is_empty() {
        return Ok(Value::Object(serde_json::Map::new()));
    }
    serde_json::from_str(body).context("Failed to parse JSON response body")
}

/// Parse a JSON error response. Returns (error_code, message).
///
/// Error format: `{"__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException", "message": "..."}`
/// or: `{"__type": "ResourceNotFoundException", "Message": "..."}`
///
/// The error code extraction handles the `#` prefix:
/// `com.amazonaws.dynamodb.v20120810#ResourceNotFoundException` -> `ResourceNotFoundException`
///
/// Both "message" (lowercase) and "Message" (capitalized) fields are supported.
pub fn parse_json_error(body: &str) -> Result<(String, String)> {
    let parsed: Value =
        serde_json::from_str(body).context("Failed to parse JSON error response")?;

    // Extract __type and strip namespace prefix (everything before and including '#')
    let raw_type = parsed
        .get("__type")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let error_code = match raw_type.rfind('#') {
        Some(pos) => &raw_type[pos + 1..],
        None => raw_type,
    };

    // Try "message" (lowercase) first, then "Message" (capitalized)
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

    // ---------------------------------------------------------------
    // Feature: json-protocol-serializer
    // ---------------------------------------------------------------

    #[test]
    fn json_protocol_serialize_target_header() {
        let target = build_target_header("DynamoDB_20120810", "ListTables");
        assert_eq!(target, "DynamoDB_20120810.ListTables");
    }

    #[test]
    fn json_protocol_serialize_target_header_kms() {
        let target = build_target_header("TrentService", "Encrypt");
        assert_eq!(target, "TrentService.Encrypt");
    }

    #[test]
    fn json_protocol_serialize_content_type_1_0() {
        let ct = build_content_type("1.0");
        assert_eq!(ct, "application/x-amz-json-1.0");
    }

    #[test]
    fn json_protocol_serialize_content_type_1_1() {
        let ct = build_content_type("1.1");
        assert_eq!(ct, "application/x-amz-json-1.1");
    }

    #[test]
    fn json_protocol_serialize_empty_input() {
        let input = json!({});
        let body = serialize_json_request(&input).unwrap();
        assert_eq!(body, "{}");
    }

    #[test]
    fn json_protocol_serialize_null_input() {
        let input = Value::Null;
        let body = serialize_json_request(&input).unwrap();
        assert_eq!(body, "{}");
    }

    #[test]
    fn json_protocol_serialize_with_params() {
        let input = json!({
            "TableName": "my-table",
            "Limit": 10
        });
        let body = serialize_json_request(&input).unwrap();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["TableName"], "my-table");
        assert_eq!(parsed["Limit"], 10);
    }

    #[test]
    fn json_protocol_serialize_nested_params() {
        let input = json!({
            "TableName": "my-table",
            "Key": {
                "pk": {"S": "user#123"},
                "sk": {"S": "profile"}
            }
        });
        let body = serialize_json_request(&input).unwrap();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["Key"]["pk"]["S"], "user#123");
        assert_eq!(parsed["Key"]["sk"]["S"], "profile");
    }

    // ---------------------------------------------------------------
    // Feature: json-protocol-parser
    // ---------------------------------------------------------------

    #[test]
    fn json_protocol_parse_simple_response() {
        let body = r#"{"TableNames": ["Table1", "Table2"], "LastEvaluatedTableName": "Table2"}"#;
        let result = parse_json_response(body).unwrap();
        let table_names = result["TableNames"].as_array().unwrap();
        assert_eq!(table_names.len(), 2);
        assert_eq!(table_names[0], "Table1");
        assert_eq!(table_names[1], "Table2");
        assert_eq!(result["LastEvaluatedTableName"], "Table2");
    }

    #[test]
    fn json_protocol_parse_empty_response() {
        let result = parse_json_response("").unwrap();
        assert!(result.is_object());
        assert_eq!(result.as_object().unwrap().len(), 0);
    }

    #[test]
    fn json_protocol_parse_nested_response() {
        let body = r#"{
            "Table": {
                "TableName": "my-table",
                "TableStatus": "ACTIVE",
                "ItemCount": 42
            }
        }"#;
        let result = parse_json_response(body).unwrap();
        assert_eq!(result["Table"]["TableName"], "my-table");
        assert_eq!(result["Table"]["TableStatus"], "ACTIVE");
        assert_eq!(result["Table"]["ItemCount"], 42);
    }

    #[test]
    fn json_protocol_parse_invalid_json() {
        let result = parse_json_response("not valid json {{{");
        assert!(result.is_err());
    }

    // ---------------------------------------------------------------
    // Feature: json-protocol-error-parser
    // ---------------------------------------------------------------

    #[test]
    fn json_protocol_error_with_namespace() {
        let body = r#"{"__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException", "message": "Requested resource not found"}"#;
        let (code, message) = parse_json_error(body).unwrap();
        assert_eq!(code, "ResourceNotFoundException");
        assert_eq!(message, "Requested resource not found");
    }

    #[test]
    fn json_protocol_error_without_namespace() {
        let body =
            r#"{"__type": "ResourceNotFoundException", "message": "Requested resource not found"}"#;
        let (code, message) = parse_json_error(body).unwrap();
        assert_eq!(code, "ResourceNotFoundException");
        assert_eq!(message, "Requested resource not found");
    }

    #[test]
    fn json_protocol_error_capitalized_message() {
        let body = r#"{"__type": "ValidationException", "Message": "The input fails to satisfy the constraints"}"#;
        let (code, message) = parse_json_error(body).unwrap();
        assert_eq!(code, "ValidationException");
        assert_eq!(message, "The input fails to satisfy the constraints");
    }

    #[test]
    fn json_protocol_error_both_message_fields() {
        // When both "message" and "Message" exist, prefer lowercase "message"
        let body = r#"{"__type": "SomeError", "message": "lowercase wins", "Message": "uppercase loses"}"#;
        let (code, message) = parse_json_error(body).unwrap();
        assert_eq!(code, "SomeError");
        assert_eq!(message, "lowercase wins");
    }

    #[test]
    fn json_protocol_error_missing_message() {
        let body = r#"{"__type": "InternalServerError"}"#;
        let (code, message) = parse_json_error(body).unwrap();
        assert_eq!(code, "InternalServerError");
        assert_eq!(message, "");
    }

    #[test]
    fn json_protocol_error_missing_type() {
        let body = r#"{"message": "Something went wrong"}"#;
        let (code, message) = parse_json_error(body).unwrap();
        assert_eq!(code, "");
        assert_eq!(message, "Something went wrong");
    }

    #[test]
    fn json_protocol_error_invalid_json() {
        let result = parse_json_error("not json");
        assert!(result.is_err());
    }

    #[test]
    fn json_protocol_error_complex_namespace() {
        let body = r#"{"__type": "com.amazon.coral.service#SerializationException", "Message": "Start of structure or map found where not expected"}"#;
        let (code, message) = parse_json_error(body).unwrap();
        assert_eq!(code, "SerializationException");
        assert_eq!(
            message,
            "Start of structure or map found where not expected"
        );
    }
}
