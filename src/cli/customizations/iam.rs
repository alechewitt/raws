//! IAM response customizations.
//!
//! IAM returns policy documents as URL-encoded JSON strings (e.g.
//! `%7B%22Version%22%3A%222012-10-17%22%2C...%7D`).  The AWS CLI decodes
//! these into proper JSON objects so that downstream formatters (json, table,
//! text, yaml) can display them natively.
//!
//! The set of fields that need decoding is determined by the botocore model:
//! any member whose shape name is `policyDocumentType` is a URL-encoded JSON
//! policy string.  This module walks the response using the model shapes to
//! find and decode those fields.

use percent_encoding::percent_decode_str;
use serde_json::Value;
use std::collections::HashMap;

/// The botocore shape name that marks a URL-encoded policy document string.
const POLICY_DOC_SHAPE: &str = "policyDocumentType";

/// Decode URL-encoded policy document fields in an IAM response.
///
/// `output_shape_name` is the top-level output shape (e.g. `"ListRolesResponse"`).
/// `shapes` is the full shapes map from the service model.
pub fn decode_iam_policy_documents(
    parsed: &mut Value,
    output_shape_name: &str,
    shapes: &HashMap<String, Value>,
) {
    if output_shape_name.is_empty() {
        return;
    }
    if let Some(shape) = shapes.get(output_shape_name) {
        decode_recursive(parsed, shape, shapes);
    }
}

/// Recursively walk `parsed` guided by `shape` and decode any
/// `policyDocumentType` string members.
fn decode_recursive(
    parsed: &mut Value,
    shape: &Value,
    shapes: &HashMap<String, Value>,
) {
    let type_name = shape.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match type_name {
        "structure" => {
            let members = match shape.get("members").and_then(|m| m.as_object()) {
                Some(m) => m,
                None => return,
            };
            let obj = match parsed.as_object_mut() {
                Some(o) => o,
                None => return,
            };
            for (member_name, member_def) in members {
                let member_shape_name = match member_def.get("shape").and_then(|s| s.as_str()) {
                    Some(s) => s,
                    None => continue,
                };

                if !obj.contains_key(member_name) {
                    continue;
                }

                if member_shape_name == POLICY_DOC_SHAPE {
                    // This member is a URL-encoded policy document string — decode it.
                    if let Some(val) = obj.get(member_name).and_then(|v| v.as_str()) {
                        if let Some(decoded) = decode_url_encoded_json(val) {
                            obj.insert(member_name.clone(), decoded);
                        }
                    }
                } else if let Some(child_shape) = shapes.get(member_shape_name) {
                    // Recurse into nested structures / lists.
                    if let Some(child_val) = obj.get_mut(member_name) {
                        decode_recursive(child_val, child_shape, shapes);
                    }
                }
            }
        }
        "list" => {
            let member_shape_name = shape
                .get("member")
                .and_then(|m| m.get("shape"))
                .and_then(|s| s.as_str());
            let member_shape_name = match member_shape_name {
                Some(s) => s,
                None => return,
            };
            let member_shape = match shapes.get(member_shape_name) {
                Some(s) => s,
                None => return,
            };
            if let Some(arr) = parsed.as_array_mut() {
                for item in arr.iter_mut() {
                    decode_recursive(item, member_shape, shapes);
                }
            }
        }
        "map" => {
            let value_shape_name = shape
                .get("value")
                .and_then(|v| v.get("shape"))
                .and_then(|s| s.as_str());
            let value_shape_name = match value_shape_name {
                Some(s) => s,
                None => return,
            };
            let value_shape = match shapes.get(value_shape_name) {
                Some(s) => s,
                None => return,
            };
            if let Some(obj) = parsed.as_object_mut() {
                for val in obj.values_mut() {
                    decode_recursive(val, value_shape, shapes);
                }
            }
        }
        _ => {
            // Scalar types — nothing to recurse into.
        }
    }
}

/// URL-decode a string and attempt to parse it as JSON.
///
/// Returns `Some(Value)` on success, `None` if decoding or parsing fails (in
/// which case the caller should leave the original value untouched).
fn decode_url_encoded_json(encoded: &str) -> Option<Value> {
    let decoded = percent_decode_str(encoded).decode_utf8().ok()?;
    serde_json::from_str::<Value>(&decoded).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Helper: build a minimal shapes map for testing.
    fn test_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();

        shapes.insert("ListRolesResponse".into(), json!({
            "type": "structure",
            "members": {
                "Roles": { "shape": "roleListType" },
                "IsTruncated": { "shape": "booleanType" }
            }
        }));

        shapes.insert("roleListType".into(), json!({
            "type": "list",
            "member": { "shape": "Role" }
        }));

        shapes.insert("Role".into(), json!({
            "type": "structure",
            "members": {
                "RoleName": { "shape": "roleNameType" },
                "AssumeRolePolicyDocument": { "shape": "policyDocumentType" },
                "Arn": { "shape": "arnType" }
            }
        }));

        shapes.insert("roleNameType".into(), json!({ "type": "string" }));
        shapes.insert("policyDocumentType".into(), json!({ "type": "string" }));
        shapes.insert("arnType".into(), json!({ "type": "string" }));
        shapes.insert("booleanType".into(), json!({ "type": "boolean" }));

        shapes
    }

    #[test]
    fn decodes_assume_role_policy_document_in_list_roles() {
        let shapes = test_shapes();
        let encoded = "%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%7B%22Effect%22%3A%22Allow%22%2C%22Principal%22%3A%7B%22Service%22%3A%22ec2.amazonaws.com%22%7D%2C%22Action%22%3A%22sts%3AAssumeRole%22%7D%5D%7D";
        let mut response = json!({
            "Roles": [
                {
                    "RoleName": "TestRole",
                    "Arn": "arn:aws:iam::123456789012:role/TestRole",
                    "AssumeRolePolicyDocument": encoded
                }
            ],
            "IsTruncated": false
        });

        decode_iam_policy_documents(&mut response, "ListRolesResponse", &shapes);

        let doc = &response["Roles"][0]["AssumeRolePolicyDocument"];
        assert!(doc.is_object(), "Expected JSON object, got: {doc}");
        assert_eq!(doc["Version"], "2012-10-17");
        assert_eq!(doc["Statement"][0]["Effect"], "Allow");
        assert_eq!(doc["Statement"][0]["Principal"]["Service"], "ec2.amazonaws.com");
        assert_eq!(doc["Statement"][0]["Action"], "sts:AssumeRole");
    }

    #[test]
    fn decodes_multiple_roles() {
        let shapes = test_shapes();
        let encoded1 = "%7B%22Version%22%3A%222012-10-17%22%7D";
        let encoded2 = "%7B%22Version%22%3A%222008-10-17%22%7D";
        let mut response = json!({
            "Roles": [
                { "RoleName": "Role1", "AssumeRolePolicyDocument": encoded1 },
                { "RoleName": "Role2", "AssumeRolePolicyDocument": encoded2 }
            ]
        });

        decode_iam_policy_documents(&mut response, "ListRolesResponse", &shapes);

        assert_eq!(response["Roles"][0]["AssumeRolePolicyDocument"]["Version"], "2012-10-17");
        assert_eq!(response["Roles"][1]["AssumeRolePolicyDocument"]["Version"], "2008-10-17");
    }

    #[test]
    fn leaves_non_policy_fields_untouched() {
        let shapes = test_shapes();
        let encoded = "%7B%22Version%22%3A%222012-10-17%22%7D";
        let mut response = json!({
            "Roles": [
                {
                    "RoleName": "TestRole",
                    "Arn": "arn:aws:iam::123456789012:role/TestRole",
                    "AssumeRolePolicyDocument": encoded
                }
            ],
            "IsTruncated": false
        });

        decode_iam_policy_documents(&mut response, "ListRolesResponse", &shapes);

        assert_eq!(response["Roles"][0]["RoleName"], "TestRole");
        assert_eq!(response["Roles"][0]["Arn"], "arn:aws:iam::123456789012:role/TestRole");
        assert_eq!(response["IsTruncated"], false);
    }

    #[test]
    fn malformed_url_encoding_left_as_is() {
        let shapes = test_shapes();
        let bad = "not%ZZvalid%encoding";
        let mut response = json!({
            "Roles": [
                { "RoleName": "TestRole", "AssumeRolePolicyDocument": bad }
            ]
        });
        let original = response.clone();

        decode_iam_policy_documents(&mut response, "ListRolesResponse", &shapes);

        assert_eq!(response, original);
    }

    #[test]
    fn decoded_but_not_valid_json_left_as_is() {
        let shapes = test_shapes();
        // This URL-decodes to "not json {{{" which is not valid JSON
        let bad = "not%20json%20%7B%7B%7B";
        let mut response = json!({
            "Roles": [
                { "RoleName": "TestRole", "AssumeRolePolicyDocument": bad }
            ]
        });
        let original = response.clone();

        decode_iam_policy_documents(&mut response, "ListRolesResponse", &shapes);

        assert_eq!(response, original);
    }

    #[test]
    fn empty_output_shape_name_is_noop() {
        let shapes = test_shapes();
        let mut response = json!({ "Roles": [] });
        let original = response.clone();

        decode_iam_policy_documents(&mut response, "", &shapes);

        assert_eq!(response, original);
    }

    #[test]
    fn unknown_output_shape_is_noop() {
        let shapes = test_shapes();
        let mut response = json!({ "Roles": [] });
        let original = response.clone();

        decode_iam_policy_documents(&mut response, "NonExistentShape", &shapes);

        assert_eq!(response, original);
    }

    #[test]
    fn decodes_nested_policy_document_in_get_account_authorization_details() {
        let mut shapes = HashMap::new();

        shapes.insert("GetAccountAuthorizationDetailsResponse".into(), json!({
            "type": "structure",
            "members": {
                "RoleDetailList": { "shape": "roleDetailListType" }
            }
        }));
        shapes.insert("roleDetailListType".into(), json!({
            "type": "list",
            "member": { "shape": "RoleDetail" }
        }));
        shapes.insert("RoleDetail".into(), json!({
            "type": "structure",
            "members": {
                "RoleName": { "shape": "roleNameType" },
                "AssumeRolePolicyDocument": { "shape": "policyDocumentType" },
                "RolePolicyList": { "shape": "policyDetailListType" }
            }
        }));
        shapes.insert("policyDetailListType".into(), json!({
            "type": "list",
            "member": { "shape": "PolicyDetail" }
        }));
        shapes.insert("PolicyDetail".into(), json!({
            "type": "structure",
            "members": {
                "PolicyName": { "shape": "policyNameType" },
                "PolicyDocument": { "shape": "policyDocumentType" }
            }
        }));
        shapes.insert("roleNameType".into(), json!({ "type": "string" }));
        shapes.insert("policyNameType".into(), json!({ "type": "string" }));
        shapes.insert("policyDocumentType".into(), json!({ "type": "string" }));

        let assume_role_encoded = "%7B%22Version%22%3A%222012-10-17%22%7D";
        let inline_policy_encoded = "%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%5D%7D";

        let mut response = json!({
            "RoleDetailList": [
                {
                    "RoleName": "MyRole",
                    "AssumeRolePolicyDocument": assume_role_encoded,
                    "RolePolicyList": [
                        {
                            "PolicyName": "InlinePolicy",
                            "PolicyDocument": inline_policy_encoded
                        }
                    ]
                }
            ]
        });

        decode_iam_policy_documents(
            &mut response,
            "GetAccountAuthorizationDetailsResponse",
            &shapes,
        );

        let role = &response["RoleDetailList"][0];
        assert!(role["AssumeRolePolicyDocument"].is_object());
        assert_eq!(role["AssumeRolePolicyDocument"]["Version"], "2012-10-17");

        let policy = &role["RolePolicyList"][0];
        assert!(policy["PolicyDocument"].is_object());
        assert_eq!(policy["PolicyDocument"]["Version"], "2012-10-17");
        assert!(policy["PolicyDocument"]["Statement"].is_array());
    }

    #[test]
    fn decodes_policy_version_document() {
        let mut shapes = HashMap::new();

        shapes.insert("GetPolicyVersionResponse".into(), json!({
            "type": "structure",
            "members": {
                "PolicyVersion": { "shape": "PolicyVersion" }
            }
        }));
        shapes.insert("PolicyVersion".into(), json!({
            "type": "structure",
            "members": {
                "Document": { "shape": "policyDocumentType" },
                "VersionId": { "shape": "policyVersionIdType" }
            }
        }));
        shapes.insert("policyDocumentType".into(), json!({ "type": "string" }));
        shapes.insert("policyVersionIdType".into(), json!({ "type": "string" }));

        let encoded = "%7B%22Version%22%3A%222012-10-17%22%7D";
        let mut response = json!({
            "PolicyVersion": {
                "Document": encoded,
                "VersionId": "v1"
            }
        });

        decode_iam_policy_documents(&mut response, "GetPolicyVersionResponse", &shapes);

        assert!(response["PolicyVersion"]["Document"].is_object());
        assert_eq!(response["PolicyVersion"]["Document"]["Version"], "2012-10-17");
        assert_eq!(response["PolicyVersion"]["VersionId"], "v1");
    }

    #[test]
    fn non_string_policy_field_left_as_is() {
        let shapes = test_shapes();
        // If the policy doc is already an object (shouldn't happen in practice but be safe)
        let mut response = json!({
            "Roles": [
                {
                    "RoleName": "TestRole",
                    "AssumeRolePolicyDocument": { "Version": "2012-10-17" }
                }
            ]
        });
        let original = response.clone();

        decode_iam_policy_documents(&mut response, "ListRolesResponse", &shapes);

        assert_eq!(response, original);
    }

    #[test]
    fn decode_url_encoded_json_helper_works() {
        let encoded = "%7B%22key%22%3A%22value%22%7D";
        let result = decode_url_encoded_json(encoded);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), json!({"key": "value"}));
    }

    #[test]
    fn decode_url_encoded_json_helper_returns_none_for_invalid() {
        assert!(decode_url_encoded_json("not%20json").is_none());
        assert!(decode_url_encoded_json("").is_none());
    }
}
