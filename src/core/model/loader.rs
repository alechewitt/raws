use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

use super::{Operation, ServiceMetadata, ServiceModel};

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

    Ok(ServiceMetadata {
        api_version: get_str(meta, "apiVersion")?.to_string(),
        endpoint_prefix: get_str(meta, "endpointPrefix")?.to_string(),
        protocol: get_str(meta, "protocol")?.to_string(),
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
}
