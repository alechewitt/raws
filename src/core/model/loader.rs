use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

use super::{Operation, ServiceMetadata, ServiceModel};

#[allow(dead_code)]
pub fn load_service_model(path: &Path) -> Result<ServiceModel> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read service model: {}", path.display()))?;
    parse_service_model(&content)
}

pub fn parse_service_model(json_str: &str) -> Result<ServiceModel> {
    let raw: Value = serde_json::from_str(json_str).context("Failed to parse service model JSON")?;

    let metadata = parse_metadata(&raw)?;
    let operations = parse_operations(&raw)?;
    let shapes = parse_shapes(&raw)?;

    Ok(ServiceModel {
        metadata,
        operations,
        shapes,
        raw,
    })
}

fn parse_metadata(raw: &Value) -> Result<ServiceMetadata> {
    let meta = raw
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing 'metadata' in service model"))?;

    let protocols = meta
        .get("protocols")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    Ok(ServiceMetadata {
        api_version: get_str(meta, "apiVersion")?.to_string(),
        endpoint_prefix: get_str(meta, "endpointPrefix")?.to_string(),
        protocol: get_str(meta, "protocol")?.to_string(),
        protocols,
        service_id: meta
            .get("serviceId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        signature_version: get_str(meta, "signatureVersion")?.to_string(),
        target_prefix: meta
            .get("targetPrefix")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        json_version: meta
            .get("jsonVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        global_endpoint: meta
            .get("globalEndpoint")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        signing_name: meta
            .get("signingName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    })
}

fn parse_operations(raw: &Value) -> Result<HashMap<String, Operation>> {
    let ops = raw
        .get("operations")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("Missing 'operations' in service model"))?;

    let mut operations = HashMap::new();
    for (name, op) in ops {
        let http = op.get("http").unwrap_or(&Value::Null);

        let input_shape = op
            .get("input")
            .and_then(|i| i.get("shape"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());

        let output_shape = op
            .get("output")
            .and_then(|o| o.get("shape"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());

        let result_wrapper = op
            .get("output")
            .and_then(|o| o.get("resultWrapper"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());

        let errors = op
            .get("errors")
            .and_then(|e| e.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| e.get("shape").and_then(|s| s.as_str()).map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let documentation = op
            .get("documentation")
            .and_then(|d| d.as_str())
            .map(|s| s.to_string());

        let static_context_params = op.get("staticContextParams").cloned();

        operations.insert(
            name.clone(),
            Operation {
                name: name.clone(),
                http_method: http
                    .get("method")
                    .and_then(|m| m.as_str())
                    .unwrap_or("POST")
                    .to_string(),
                http_request_uri: http
                    .get("requestUri")
                    .and_then(|u| u.as_str())
                    .unwrap_or("/")
                    .to_string(),
                input_shape,
                output_shape,
                result_wrapper,
                errors,
                documentation,
                static_context_params,
            },
        );
    }

    Ok(operations)
}

fn parse_shapes(raw: &Value) -> Result<HashMap<String, Value>> {
    let shapes = raw
        .get("shapes")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("Missing 'shapes' in service model"))?;

    Ok(shapes
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect())
}

fn get_str<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing or invalid '{}' in metadata", key))
}

pub fn discover_services(models_dir: &Path) -> Result<Vec<String>> {
    let mut services = Vec::new();
    if !models_dir.exists() {
        bail!("Models directory not found: {}", models_dir.display());
    }

    for entry in std::fs::read_dir(models_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name().to_string_lossy().to_string();
            // Skip non-service entries (files like endpoints.json)
            if !name.starts_with('.') && name != "endpoints.json" && name != "partitions.json" {
                // Check if it has a version subdirectory with service-2.json
                if find_service_model(&path).is_some() {
                    services.push(name);
                }
            }
        }
    }

    services.sort();
    Ok(services)
}

pub fn find_service_model(service_dir: &Path) -> Option<std::path::PathBuf> {
    // Service directories contain version subdirectories (e.g., 2011-06-15/)
    // Find the latest version directory with a service-2.json
    if let Ok(entries) = std::fs::read_dir(service_dir) {
        let mut versions: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        versions.sort_by_key(|b| std::cmp::Reverse(b.file_name()));

        for version in versions {
            let model_path = version.path().join("service-2.json");
            if model_path.exists() {
                return Some(model_path);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_loader_sts() {
        let model_path = Path::new("models/sts/2011-06-15/service-2.json");
        if !model_path.exists() {
            eprintln!("Skipping test: STS model not yet copied");
            return;
        }
        let model = load_service_model(model_path).unwrap();
        assert_eq!(model.metadata.protocol, "query");
        assert_eq!(model.metadata.endpoint_prefix, "sts");
        assert!(model.operations.contains_key("GetCallerIdentity"));
    }

    #[test]
    fn test_model_loader_parse_minimal() {
        let json = r#"{
            "version": "2.0",
            "metadata": {
                "apiVersion": "2011-06-15",
                "endpointPrefix": "sts",
                "protocol": "query",
                "serviceId": "STS",
                "signatureVersion": "v4"
            },
            "operations": {
                "GetCallerIdentity": {
                    "name": "GetCallerIdentity",
                    "http": { "method": "POST", "requestUri": "/" },
                    "input": { "shape": "GetCallerIdentityRequest" },
                    "output": { "shape": "GetCallerIdentityResponse", "resultWrapper": "GetCallerIdentityResult" }
                }
            },
            "shapes": {
                "GetCallerIdentityRequest": { "type": "structure", "members": {} },
                "GetCallerIdentityResponse": {
                    "type": "structure",
                    "members": {
                        "UserId": { "shape": "userIdType" },
                        "Account": { "shape": "accountType" },
                        "Arn": { "shape": "arnType" }
                    }
                },
                "userIdType": { "type": "string" },
                "accountType": { "type": "string" },
                "arnType": { "type": "string" }
            }
        }"#;

        let model = parse_service_model(json).unwrap();
        assert_eq!(model.metadata.api_version, "2011-06-15");
        assert_eq!(model.metadata.protocol, "query");
        assert_eq!(model.operations.len(), 1);

        let op = &model.operations["GetCallerIdentity"];
        assert_eq!(op.http_method, "POST");
        assert_eq!(op.input_shape, Some("GetCallerIdentityRequest".to_string()));
        assert_eq!(
            op.result_wrapper,
            Some("GetCallerIdentityResult".to_string())
        );

        assert!(model.shapes.contains_key("GetCallerIdentityRequest"));
        assert!(model.shapes.contains_key("userIdType"));
    }

    #[test]
    fn test_load_sts_real_model() {
        let path = Path::new("models/sts/2011-06-15/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: models not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();
        assert_eq!(model.metadata.protocol, "query");
        assert!(model.operations.contains_key("GetCallerIdentity"));
        assert!(model.operations.contains_key("AssumeRole"));
    }

    #[test]
    fn test_load_s3_real_model() {
        let path = Path::new("models/s3/2006-03-01/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: models not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();
        assert_eq!(model.metadata.protocol, "rest-xml");
        assert!(model.operations.contains_key("ListBuckets"));
        assert!(model.operations.contains_key("PutObject"));
        assert!(model.shapes.len() > 100);

        // Verify staticContextParams are parsed from the model
        let list_dir = &model.operations["ListDirectoryBuckets"];
        assert!(list_dir.uses_s3_express_control_endpoint());
        let list_buckets = &model.operations["ListBuckets"];
        assert!(!list_buckets.uses_s3_express_control_endpoint());
    }

    #[test]
    fn test_load_dynamodb_real_model() {
        let path = Path::new("models/dynamodb/2012-08-10/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: models not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();
        assert_eq!(model.metadata.protocol, "json");
        assert!(model.operations.contains_key("ListTables"));
    }

    #[test]
    fn test_load_ec2_real_model() {
        let path = Path::new("models/ec2/2016-11-15/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: models not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();
        assert_eq!(model.metadata.protocol, "ec2");
        assert!(model.operations.contains_key("DescribeInstances"));
        // EC2 is the largest model
        assert!(model.shapes.len() > 1000);
    }

    #[test]
    fn test_service_discovery() {
        let models_dir = Path::new("models");
        if !models_dir.exists() {
            eprintln!("Skipping: models not copied yet");
            return;
        }
        let services = discover_services(models_dir).unwrap();
        assert!(services.len() > 400, "Expected 400+ services, got {}", services.len());
        assert!(services.contains(&"sts".to_string()));
        assert!(services.contains(&"s3".to_string()));
    }

    // ---------------------------------------------------------------
    // Synthetic model JSON covering all shape types and member modifiers
    // ---------------------------------------------------------------

    fn synthetic_model_json() -> &'static str {
        r#"{
            "version": "2.0",
            "metadata": {
                "apiVersion": "2023-01-01",
                "endpointPrefix": "synth",
                "protocol": "json",
                "serviceId": "Synth",
                "signatureVersion": "v4",
                "targetPrefix": "SynthService_20230101",
                "jsonVersion": "1.1"
            },
            "operations": {
                "DoSomething": {
                    "name": "DoSomething",
                    "http": { "method": "POST", "requestUri": "/do-something" },
                    "input": { "shape": "DoSomethingInput" },
                    "output": { "shape": "DoSomethingOutput", "resultWrapper": "DoSomethingResult" },
                    "errors": [
                        { "shape": "ValidationException" },
                        { "shape": "ResourceNotFoundException" }
                    ],
                    "documentation": "Performs the DoSomething action."
                },
                "GetItem": {
                    "name": "GetItem",
                    "http": { "method": "GET", "requestUri": "/items/{ItemId}" },
                    "input": { "shape": "GetItemInput" },
                    "output": { "shape": "GetItemOutput" }
                },
                "DeleteItem": {
                    "name": "DeleteItem",
                    "http": { "method": "DELETE", "requestUri": "/items/{ItemId}" }
                }
            },
            "shapes": {
                "DoSomethingInput": {
                    "type": "structure",
                    "required": ["Name", "Count"],
                    "members": {
                        "Name": {
                            "shape": "StringType",
                            "location": "header",
                            "locationName": "x-synth-name"
                        },
                        "Count": {
                            "shape": "IntegerType"
                        },
                        "Token": {
                            "shape": "StringType",
                            "location": "querystring",
                            "locationName": "token"
                        },
                        "Body": {
                            "shape": "BlobType",
                            "streaming": true
                        }
                    }
                },
                "DoSomethingOutput": {
                    "type": "structure",
                    "members": {
                        "Id": { "shape": "StringType" },
                        "Active": { "shape": "BooleanType" },
                        "Score": { "shape": "DoubleType" },
                        "Rating": { "shape": "FloatType" },
                        "CreatedAt": { "shape": "TimestampType" },
                        "Tags": { "shape": "TagMap" },
                        "Items": { "shape": "StringList" },
                        "Size": { "shape": "LongType" }
                    }
                },
                "GetItemInput": {
                    "type": "structure",
                    "required": ["ItemId"],
                    "members": {
                        "ItemId": {
                            "shape": "StringType",
                            "location": "uri",
                            "locationName": "ItemId"
                        }
                    }
                },
                "GetItemOutput": {
                    "type": "structure",
                    "members": {
                        "Data": { "shape": "BlobType" }
                    }
                },
                "ValidationException": {
                    "type": "structure",
                    "members": {
                        "Message": { "shape": "StringType" }
                    }
                },
                "ResourceNotFoundException": {
                    "type": "structure",
                    "members": {
                        "Message": { "shape": "StringType" }
                    }
                },
                "StringType": { "type": "string" },
                "IntegerType": { "type": "integer" },
                "LongType": { "type": "long" },
                "FloatType": { "type": "float" },
                "DoubleType": { "type": "double" },
                "BooleanType": { "type": "boolean" },
                "TimestampType": { "type": "timestamp" },
                "BlobType": { "type": "blob" },
                "StringList": {
                    "type": "list",
                    "member": { "shape": "StringType" }
                },
                "TagMap": {
                    "type": "map",
                    "key": { "shape": "StringType" },
                    "value": { "shape": "StringType" }
                }
            }
        }"#
    }

    // ---------------------------------------------------------------
    // Feature 1: shape_type — verify all shape types are stored and
    //            their "type" field can be read back.
    // ---------------------------------------------------------------

    #[test]
    fn test_shape_type_all_types_in_synthetic_model() {
        let model = parse_service_model(synthetic_model_json()).unwrap();

        // Every expected shape type mapped to at least one shape name
        let expected: Vec<(&str, &str)> = vec![
            ("StringType", "string"),
            ("IntegerType", "integer"),
            ("LongType", "long"),
            ("FloatType", "float"),
            ("DoubleType", "double"),
            ("BooleanType", "boolean"),
            ("TimestampType", "timestamp"),
            ("BlobType", "blob"),
            ("StringList", "list"),
            ("TagMap", "map"),
            ("DoSomethingInput", "structure"),
        ];

        for (shape_name, expected_type) in &expected {
            let shape = model
                .shapes
                .get(*shape_name)
                .unwrap_or_else(|| panic!("Shape '{}' not found", shape_name));
            let actual_type = shape
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("Shape '{}' missing 'type' field", shape_name));
            assert_eq!(
                actual_type, *expected_type,
                "Shape '{}' has type '{}', expected '{}'",
                shape_name, actual_type, expected_type
            );
        }
    }

    #[test]
    fn test_shape_type_list_has_member() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        let list_shape = &model.shapes["StringList"];
        assert_eq!(list_shape["type"].as_str().unwrap(), "list");
        let member_ref = list_shape
            .get("member")
            .and_then(|m| m.get("shape"))
            .and_then(|s| s.as_str())
            .unwrap();
        assert_eq!(member_ref, "StringType");
    }

    #[test]
    fn test_shape_type_map_has_key_and_value() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        let map_shape = &model.shapes["TagMap"];
        assert_eq!(map_shape["type"].as_str().unwrap(), "map");
        let key_ref = map_shape["key"]["shape"].as_str().unwrap();
        let val_ref = map_shape["value"]["shape"].as_str().unwrap();
        assert_eq!(key_ref, "StringType");
        assert_eq!(val_ref, "StringType");
    }

    #[test]
    fn test_shape_type_structure_has_members() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        let structure = &model.shapes["DoSomethingInput"];
        assert_eq!(structure["type"].as_str().unwrap(), "structure");
        let members = structure["members"].as_object().unwrap();
        assert!(members.contains_key("Name"));
        assert!(members.contains_key("Count"));
        assert!(members.contains_key("Token"));
        assert!(members.contains_key("Body"));
    }

    #[test]
    fn test_shape_type_real_s3_model() {
        let path = Path::new("models/s3/2006-03-01/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: S3 model not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();

        // S3 has blob, boolean, integer, list, long, map, string, structure, timestamp
        let expected_types: Vec<&str> = vec![
            "blob", "boolean", "integer", "list", "long", "map", "string", "structure", "timestamp",
        ];

        let actual_types: std::collections::BTreeSet<String> = model
            .shapes
            .values()
            .filter_map(|v| v.get("type").and_then(|t| t.as_str()).map(|s| s.to_string()))
            .collect();

        for t in &expected_types {
            assert!(
                actual_types.contains(*t),
                "S3 model missing expected shape type '{}'",
                t
            );
        }
    }

    #[test]
    fn test_shape_type_real_ec2_model_includes_float_double() {
        let path = Path::new("models/ec2/2016-11-15/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: EC2 model not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();

        let actual_types: std::collections::BTreeSet<String> = model
            .shapes
            .values()
            .filter_map(|v| v.get("type").and_then(|t| t.as_str()).map(|s| s.to_string()))
            .collect();

        assert!(actual_types.contains("float"), "EC2 model missing 'float' type");
        assert!(actual_types.contains("double"), "EC2 model missing 'double' type");
    }

    // ---------------------------------------------------------------
    // Feature 2: operation_parsing — verify operation definitions
    // ---------------------------------------------------------------

    #[test]
    fn test_operation_parsing_synthetic_do_something() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        assert_eq!(model.operations.len(), 3);

        let op = &model.operations["DoSomething"];
        assert_eq!(op.name, "DoSomething");
        assert_eq!(op.http_method, "POST");
        assert_eq!(op.http_request_uri, "/do-something");
        assert_eq!(op.input_shape.as_deref(), Some("DoSomethingInput"));
        assert_eq!(op.output_shape.as_deref(), Some("DoSomethingOutput"));
        assert_eq!(op.result_wrapper.as_deref(), Some("DoSomethingResult"));
        assert_eq!(op.errors.len(), 2);
        assert!(op.errors.contains(&"ValidationException".to_string()));
        assert!(op.errors.contains(&"ResourceNotFoundException".to_string()));
        assert_eq!(
            op.documentation.as_deref(),
            Some("Performs the DoSomething action.")
        );
    }

    #[test]
    fn test_operation_parsing_synthetic_get_item() {
        let model = parse_service_model(synthetic_model_json()).unwrap();

        let op = &model.operations["GetItem"];
        assert_eq!(op.name, "GetItem");
        assert_eq!(op.http_method, "GET");
        assert_eq!(op.http_request_uri, "/items/{ItemId}");
        assert_eq!(op.input_shape.as_deref(), Some("GetItemInput"));
        assert_eq!(op.output_shape.as_deref(), Some("GetItemOutput"));
        assert!(op.result_wrapper.is_none());
        assert!(op.errors.is_empty());
    }

    #[test]
    fn test_operation_parsing_synthetic_delete_no_io() {
        let model = parse_service_model(synthetic_model_json()).unwrap();

        let op = &model.operations["DeleteItem"];
        assert_eq!(op.name, "DeleteItem");
        assert_eq!(op.http_method, "DELETE");
        assert_eq!(op.http_request_uri, "/items/{ItemId}");
        assert!(op.input_shape.is_none());
        assert!(op.output_shape.is_none());
        assert!(op.result_wrapper.is_none());
        assert!(op.errors.is_empty());
        assert!(op.documentation.is_none());
    }

    #[test]
    fn test_operation_parsing_real_sts() {
        let path = Path::new("models/sts/2011-06-15/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: STS model not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();

        // GetCallerIdentity
        let gci = &model.operations["GetCallerIdentity"];
        assert_eq!(gci.http_method, "POST");
        assert_eq!(gci.http_request_uri, "/");
        assert!(gci.input_shape.is_some());
        assert!(gci.output_shape.is_some());

        // AssumeRole has errors
        let ar = &model.operations["AssumeRole"];
        assert_eq!(ar.http_method, "POST");
        assert!(!ar.errors.is_empty(), "AssumeRole should have errors");
        assert!(
            ar.errors.contains(&"MalformedPolicyDocumentException".to_string()),
            "AssumeRole errors should include MalformedPolicyDocumentException"
        );
    }

    #[test]
    fn test_operation_parsing_real_s3() {
        let path = Path::new("models/s3/2006-03-01/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: S3 model not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();

        // PutObject uses PUT
        let put = &model.operations["PutObject"];
        assert_eq!(put.http_method, "PUT");
        assert!(put.http_request_uri.contains("{Key+}"));

        // ListBuckets uses GET
        let list = &model.operations["ListBuckets"];
        assert_eq!(list.http_method, "GET");
    }

    // ---------------------------------------------------------------
    // Feature 3: metadata_parsing — verify service metadata fields
    // ---------------------------------------------------------------

    #[test]
    fn test_metadata_parsing_synthetic() {
        let model = parse_service_model(synthetic_model_json()).unwrap();

        assert_eq!(model.metadata.api_version, "2023-01-01");
        assert_eq!(model.metadata.protocol, "json");
        assert_eq!(model.metadata.endpoint_prefix, "synth");
        assert_eq!(model.metadata.service_id, "Synth");
        assert_eq!(model.metadata.signature_version, "v4");
        assert_eq!(
            model.metadata.target_prefix.as_deref(),
            Some("SynthService_20230101")
        );
        assert_eq!(model.metadata.json_version.as_deref(), Some("1.1"));
    }

    #[test]
    fn test_metadata_parsing_no_optional_fields() {
        let json = r#"{
            "version": "2.0",
            "metadata": {
                "apiVersion": "2024-01-01",
                "endpointPrefix": "bare",
                "protocol": "query",
                "serviceId": "Bare",
                "signatureVersion": "v4"
            },
            "operations": {},
            "shapes": {}
        }"#;
        let model = parse_service_model(json).unwrap();

        assert_eq!(model.metadata.api_version, "2024-01-01");
        assert_eq!(model.metadata.protocol, "query");
        assert_eq!(model.metadata.endpoint_prefix, "bare");
        assert_eq!(model.metadata.service_id, "Bare");
        assert_eq!(model.metadata.signature_version, "v4");
        assert!(model.metadata.target_prefix.is_none());
        assert!(model.metadata.json_version.is_none());
        assert!(model.metadata.global_endpoint.is_none());
        assert!(model.metadata.signing_name.is_none());
        assert!(model.metadata.protocols.is_empty());
    }

    #[test]
    fn test_metadata_parsing_protocols_array() {
        let json = r#"{
            "version": "2.0",
            "metadata": {
                "apiVersion": "2024-01-01",
                "endpointPrefix": "monitoring",
                "protocol": "smithy-rpc-v2-cbor",
                "protocols": ["smithy-rpc-v2-cbor", "json", "query"],
                "serviceId": "CloudWatch",
                "signatureVersion": "v4"
            },
            "operations": {},
            "shapes": {}
        }"#;
        let model = parse_service_model(json).unwrap();

        assert_eq!(model.metadata.protocol, "smithy-rpc-v2-cbor");
        assert_eq!(
            model.metadata.protocols,
            vec!["smithy-rpc-v2-cbor", "json", "query"]
        );
        // effective_protocol should fall back to "json"
        assert_eq!(model.metadata.effective_protocol(), "json");
    }

    #[test]
    fn test_metadata_parsing_real_sts() {
        let path = Path::new("models/sts/2011-06-15/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: STS model not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();
        assert_eq!(model.metadata.api_version, "2011-06-15");
        assert_eq!(model.metadata.protocol, "query");
        assert_eq!(model.metadata.endpoint_prefix, "sts");
        assert_eq!(model.metadata.service_id, "STS");
        assert_eq!(model.metadata.signature_version, "v4");
        assert!(model.metadata.target_prefix.is_none());
        assert!(model.metadata.json_version.is_none());
        assert_eq!(
            model.metadata.global_endpoint.as_deref(),
            Some("sts.amazonaws.com")
        );
    }

    #[test]
    fn test_metadata_parsing_real_dynamodb() {
        let path = Path::new("models/dynamodb/2012-08-10/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: DynamoDB model not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();
        assert_eq!(model.metadata.api_version, "2012-08-10");
        assert_eq!(model.metadata.protocol, "json");
        assert_eq!(model.metadata.endpoint_prefix, "dynamodb");
        assert_eq!(model.metadata.service_id, "DynamoDB");
        assert_eq!(model.metadata.signature_version, "v4");
        assert_eq!(
            model.metadata.target_prefix.as_deref(),
            Some("DynamoDB_20120810")
        );
        assert_eq!(model.metadata.json_version.as_deref(), Some("1.0"));
    }

    #[test]
    fn test_metadata_parsing_real_s3() {
        let path = Path::new("models/s3/2006-03-01/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: S3 model not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();
        assert_eq!(model.metadata.api_version, "2006-03-01");
        assert_eq!(model.metadata.protocol, "rest-xml");
        assert_eq!(model.metadata.endpoint_prefix, "s3");
        assert_eq!(model.metadata.service_id, "S3");
        assert_eq!(model.metadata.signature_version, "s3");
    }

    // ---------------------------------------------------------------
    // Feature 4: member_modifier — required, location, locationName,
    //            streaming modifiers on structure members
    // ---------------------------------------------------------------

    #[test]
    fn test_member_modifier_required_list() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        let shape = &model.shapes["DoSomethingInput"];

        let required = shape["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(required, vec!["Name", "Count"]);
    }

    #[test]
    fn test_member_modifier_location_header() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        let shape = &model.shapes["DoSomethingInput"];
        let name_member = &shape["members"]["Name"];

        assert_eq!(name_member["location"].as_str().unwrap(), "header");
        assert_eq!(
            name_member["locationName"].as_str().unwrap(),
            "x-synth-name"
        );
    }

    #[test]
    fn test_member_modifier_location_querystring() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        let shape = &model.shapes["DoSomethingInput"];
        let token_member = &shape["members"]["Token"];

        assert_eq!(token_member["location"].as_str().unwrap(), "querystring");
        assert_eq!(token_member["locationName"].as_str().unwrap(), "token");
    }

    #[test]
    fn test_member_modifier_location_uri() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        let shape = &model.shapes["GetItemInput"];
        let id_member = &shape["members"]["ItemId"];

        assert_eq!(id_member["location"].as_str().unwrap(), "uri");
        assert_eq!(id_member["locationName"].as_str().unwrap(), "ItemId");
    }

    #[test]
    fn test_member_modifier_streaming() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        let shape = &model.shapes["DoSomethingInput"];
        let body_member = &shape["members"]["Body"];

        assert_eq!(body_member["streaming"].as_bool().unwrap(), true);
    }

    #[test]
    fn test_member_modifier_no_required_field() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        let shape = &model.shapes["DoSomethingOutput"];
        // DoSomethingOutput has no required list
        assert!(shape.get("required").is_none());
    }

    #[test]
    fn test_member_modifier_member_without_location() {
        let model = parse_service_model(synthetic_model_json()).unwrap();
        let shape = &model.shapes["DoSomethingInput"];
        let count_member = &shape["members"]["Count"];

        // Count has no location or locationName
        assert!(count_member.get("location").is_none());
        assert!(count_member.get("locationName").is_none());
        // But it does have a shape reference
        assert_eq!(count_member["shape"].as_str().unwrap(), "IntegerType");
    }

    #[test]
    fn test_member_modifier_real_s3_header_and_uri() {
        let path = Path::new("models/s3/2006-03-01/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: S3 model not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();

        // AbortMultipartUploadRequest has Bucket (uri), Key (uri), UploadId (querystring)
        let shape = &model.shapes["AbortMultipartUploadRequest"];
        let members = shape["members"].as_object().unwrap();

        let bucket = &members["Bucket"];
        assert_eq!(bucket["location"].as_str().unwrap(), "uri");
        assert_eq!(bucket["locationName"].as_str().unwrap(), "Bucket");

        let key = &members["Key"];
        assert_eq!(key["location"].as_str().unwrap(), "uri");
        assert_eq!(key["locationName"].as_str().unwrap(), "Key");

        let upload_id = &members["UploadId"];
        assert_eq!(upload_id["location"].as_str().unwrap(), "querystring");
        assert_eq!(upload_id["locationName"].as_str().unwrap(), "uploadId");

        // Required list
        let required = shape["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(required.contains(&"Bucket"));
        assert!(required.contains(&"Key"));
        assert!(required.contains(&"UploadId"));
    }

    #[test]
    fn test_member_modifier_real_s3_streaming() {
        let path = Path::new("models/s3/2006-03-01/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: S3 model not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();

        // PutObjectRequest has a Body member with streaming: true
        let shape = &model.shapes["PutObjectRequest"];
        let body = &shape["members"]["Body"];
        assert_eq!(body.get("streaming").and_then(|v| v.as_bool()), Some(true));
    }

    // ---------------------------------------------------------------
    // Feature 5: signing_name — verify signingName is parsed from metadata
    // ---------------------------------------------------------------

    #[test]
    fn test_signing_name_parsed_when_present() {
        let json = r#"{
            "version": "2.0",
            "metadata": {
                "apiVersion": "2015-09-21",
                "endpointPrefix": "api.ecr",
                "protocol": "json",
                "serviceId": "ECR",
                "signatureVersion": "v4",
                "signingName": "ecr",
                "targetPrefix": "AmazonEC2ContainerRegistry_V20150921",
                "jsonVersion": "1.1"
            },
            "operations": {},
            "shapes": {}
        }"#;
        let model = parse_service_model(json).unwrap();
        assert_eq!(model.metadata.endpoint_prefix, "api.ecr");
        assert_eq!(model.metadata.signing_name.as_deref(), Some("ecr"));
        assert_eq!(model.metadata.signing_service(), "ecr");
    }

    #[test]
    fn test_signing_name_none_when_absent() {
        let json = r#"{
            "version": "2.0",
            "metadata": {
                "apiVersion": "2011-06-15",
                "endpointPrefix": "sts",
                "protocol": "query",
                "serviceId": "STS",
                "signatureVersion": "v4"
            },
            "operations": {},
            "shapes": {}
        }"#;
        let model = parse_service_model(json).unwrap();
        assert_eq!(model.metadata.endpoint_prefix, "sts");
        assert!(model.metadata.signing_name.is_none());
        assert_eq!(model.metadata.signing_service(), "sts");
    }

    #[test]
    fn test_signing_name_real_ecr_model() {
        let path = Path::new("models/ecr/2015-09-21/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: ECR model not copied yet");
            return;
        }
        let model = load_service_model(path).unwrap();
        assert_eq!(model.metadata.endpoint_prefix, "api.ecr");
        assert_eq!(model.metadata.signing_name.as_deref(), Some("ecr"));
        assert_eq!(model.metadata.signing_service(), "ecr");
    }
}
