#[cfg(feature = "embed-models")]
pub mod embedded;
pub mod loader;
pub mod store;

use heck::ToKebabCase;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ServiceModel {
    pub metadata: ServiceMetadata,
    pub operations: HashMap<String, Operation>,
    pub shapes: HashMap<String, Value>,
    #[allow(dead_code)]
    pub raw: Value,
}

#[derive(Debug, Clone)]
pub struct ServiceMetadata {
    pub api_version: String,
    pub endpoint_prefix: String,
    pub protocol: String,
    #[allow(dead_code)]
    pub service_id: String,
    #[allow(dead_code)]
    pub signature_version: String,
    pub target_prefix: Option<String>,
    pub json_version: Option<String>,
    pub global_endpoint: Option<String>,
    pub signing_name: Option<String>,
}

impl ServiceMetadata {
    /// Returns the service name to use for SigV4 signing.
    ///
    /// Uses `signingName` from the model metadata when present, falling back
    /// to `endpointPrefix`.  This matters for services like ECR where the
    /// endpoint prefix is "api.ecr" but the signing service must be "ecr".
    pub fn signing_service(&self) -> &str {
        self.signing_name.as_deref().unwrap_or(&self.endpoint_prefix)
    }
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub name: String,
    pub http_method: String,
    pub http_request_uri: String,
    pub input_shape: Option<String>,
    pub output_shape: Option<String>,
    pub result_wrapper: Option<String>,
    #[allow(dead_code)]
    pub errors: Vec<String>,
    pub documentation: Option<String>,
}

// ---------------------------------------------------------------------------
// Operation name mapping: PascalCase <-> kebab-case
// ---------------------------------------------------------------------------

/// Convert a PascalCase operation name to kebab-case CLI name.
///
/// Example: "GetCallerIdentity" -> "get-caller-identity"
pub fn pascal_to_kebab(name: &str) -> String {
    name.to_kebab_case()
}

/// Convert a kebab-case CLI operation name to PascalCase model name.
///
/// Example: "get-caller-identity" -> "GetCallerIdentity"
pub fn kebab_to_pascal(name: &str) -> String {
    name.split('-')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => {
                    let upper: String = c.to_uppercase().collect();
                    upper + &chars.as_str().to_lowercase()
                }
            }
        })
        .collect()
}

/// Find an operation in the model by its kebab-case CLI name.
///
/// Returns the PascalCase operation name if found.
pub fn find_operation_by_cli_name<'a>(
    operations: &'a HashMap<String, Operation>,
    cli_name: &str,
) -> Option<&'a str> {
    // First try direct conversion
    let pascal = kebab_to_pascal(cli_name);
    if operations.contains_key(&pascal) {
        return operations.get(&pascal).map(|op| op.name.as_str());
    }

    // Fallback: iterate and compare kebab-cased versions
    for key in operations.keys() {
        if pascal_to_kebab(key) == cli_name {
            return Some(key.as_str());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_name_map_pascal_to_kebab() {
        assert_eq!(pascal_to_kebab("GetCallerIdentity"), "get-caller-identity");
        assert_eq!(pascal_to_kebab("ListBuckets"), "list-buckets");
        assert_eq!(pascal_to_kebab("DescribeInstances"), "describe-instances");
        assert_eq!(pascal_to_kebab("PutObject"), "put-object");
        assert_eq!(pascal_to_kebab("AssumeRole"), "assume-role");
    }

    #[test]
    fn test_operation_name_map_kebab_to_pascal() {
        assert_eq!(kebab_to_pascal("get-caller-identity"), "GetCallerIdentity");
        assert_eq!(kebab_to_pascal("list-buckets"), "ListBuckets");
        assert_eq!(kebab_to_pascal("describe-instances"), "DescribeInstances");
        assert_eq!(kebab_to_pascal("put-object"), "PutObject");
        assert_eq!(kebab_to_pascal("assume-role"), "AssumeRole");
    }

    #[test]
    fn test_operation_name_map_roundtrip() {
        let names = vec![
            "GetCallerIdentity",
            "ListBuckets",
            "DescribeInstances",
            "PutObject",
            "AssumeRole",
            "CreateMultipartUpload",
            "TagResource",
        ];
        for name in names {
            let kebab = pascal_to_kebab(name);
            let back = kebab_to_pascal(&kebab);
            assert_eq!(back, name, "Roundtrip failed for '{name}': kebab='{kebab}' -> '{back}'");
        }
    }

    #[test]
    fn test_operation_name_map_single_word() {
        assert_eq!(pascal_to_kebab("Invoke"), "invoke");
        assert_eq!(kebab_to_pascal("invoke"), "Invoke");
    }

    #[test]
    fn test_operation_name_map_find_in_model() {
        let mut ops = HashMap::new();
        ops.insert("GetCallerIdentity".to_string(), Operation {
            name: "GetCallerIdentity".to_string(),
            http_method: "POST".to_string(),
            http_request_uri: "/".to_string(),
            input_shape: None,
            output_shape: None,
            result_wrapper: None,
            errors: vec![],
            documentation: None,
        });
        ops.insert("AssumeRole".to_string(), Operation {
            name: "AssumeRole".to_string(),
            http_method: "POST".to_string(),
            http_request_uri: "/".to_string(),
            input_shape: None,
            output_shape: None,
            result_wrapper: None,
            errors: vec![],
            documentation: None,
        });

        assert_eq!(
            find_operation_by_cli_name(&ops, "get-caller-identity"),
            Some("GetCallerIdentity")
        );
        assert_eq!(
            find_operation_by_cli_name(&ops, "assume-role"),
            Some("AssumeRole")
        );
        assert_eq!(
            find_operation_by_cli_name(&ops, "nonexistent-op"),
            None
        );
    }

    fn make_metadata(endpoint_prefix: &str, signing_name: Option<&str>) -> ServiceMetadata {
        ServiceMetadata {
            api_version: "2023-01-01".to_string(),
            endpoint_prefix: endpoint_prefix.to_string(),
            protocol: "json".to_string(),
            service_id: "Test".to_string(),
            signature_version: "v4".to_string(),
            target_prefix: None,
            json_version: None,
            global_endpoint: None,
            signing_name: signing_name.map(|s| s.to_string()),
        }
    }

    #[test]
    fn test_signing_service_uses_signing_name_when_present() {
        // ECR: endpointPrefix is "api.ecr" but signingName is "ecr"
        let meta = make_metadata("api.ecr", Some("ecr"));
        assert_eq!(meta.signing_service(), "ecr");
    }

    #[test]
    fn test_signing_service_falls_back_to_endpoint_prefix() {
        // STS: no signingName, so signing service should be the endpointPrefix
        let meta = make_metadata("sts", None);
        assert_eq!(meta.signing_service(), "sts");
    }

    #[test]
    fn test_signing_service_same_values() {
        // When signingName equals endpointPrefix, either is fine
        let meta = make_metadata("dynamodb", Some("dynamodb"));
        assert_eq!(meta.signing_service(), "dynamodb");
    }
}
