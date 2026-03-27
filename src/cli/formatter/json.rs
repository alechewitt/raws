use anyhow::{Context, Result};
use serde_json::Value;

/// Recursively strip null values from a JSON value.
///
/// AWS CLI omits null-valued fields from JSON output. For example,
/// `get-bucket-versioning` on a bucket that has never had versioning enabled
/// returns `{}` rather than `{"Status": null, "MFADelete": null}`.
pub fn strip_nulls(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let filtered: serde_json::Map<String, Value> = map
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), strip_nulls(v)))
                .collect();
            Value::Object(filtered)
        }
        Value::Array(arr) => {
            Value::Array(arr.iter().map(strip_nulls).collect())
        }
        other => other.clone(),
    }
}

/// Format JSON output with 4-space indentation, matching AWS CLI output.
pub fn format_json(value: &Value) -> Result<String> {
    let mut buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
    serde::Serialize::serialize(value, &mut ser)
        .context("Failed to serialize JSON output")?;
    String::from_utf8(buf).context("JSON output contained invalid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_format_simple() {
        let value = serde_json::json!({
            "UserId": "AIDAEXAMPLE",
            "Account": "123456789012",
            "Arn": "arn:aws:iam::123456789012:user/alice"
        });
        let output = format_json(&value).unwrap();
        assert!(output.contains("\"UserId\""));
        assert!(output.contains("AIDAEXAMPLE"));
    }

    #[test]
    fn test_json_output_format_match_4_space_indent() {
        let value = serde_json::json!({
            "Account": "123456789012",
            "Arn": "arn:aws:iam::123456789012:user/alice",
            "UserId": "AIDAEXAMPLE"
        });
        let output = format_json(&value).unwrap();
        // Must use 4-space indentation (not 2-space like serde default)
        assert!(output.contains("    \"Account\""), "Expected 4-space indent, got: {output}");
        assert!(!output.contains("  \"Account\"") || output.contains("    \"Account\""),
            "Should not have 2-space-only indent");
    }

    #[test]
    fn test_json_output_format_match_nested_indent() {
        let value = serde_json::json!({
            "Buckets": [
                {
                    "Name": "my-bucket",
                    "CreationDate": "2023-01-01"
                }
            ]
        });
        let output = format_json(&value).unwrap();
        // Nested items should have 8-space indent (2 levels)
        assert!(output.contains("        \"Name\""), "Expected 8-space indent for nested, got: {output}");
    }

    #[test]
    fn test_json_output_format_match_sts_structure() {
        // Simulate exact STS get-caller-identity output structure
        let value = serde_json::json!({
            "Account": "668864905351",
            "Arn": "arn:aws:sts::668864905351:assumed-role/Admin/user",
            "UserId": "AROAEXAMPLE:user"
        });
        let output = format_json(&value).unwrap();
        let expected = "{\n    \"Account\": \"668864905351\",\n    \"Arn\": \"arn:aws:sts::668864905351:assumed-role/Admin/user\",\n    \"UserId\": \"AROAEXAMPLE:user\"\n}";
        assert_eq!(output, expected);
    }

    #[test]
    fn test_json_output_format_match_empty_object() {
        let value = serde_json::json!({});
        let output = format_json(&value).unwrap();
        assert_eq!(output, "{}");
    }

    #[test]
    fn test_json_output_format_match_empty_array() {
        let value = serde_json::json!([]);
        let output = format_json(&value).unwrap();
        assert_eq!(output, "[]");
    }

    #[test]
    fn test_strip_nulls_removes_null_fields() {
        let value = serde_json::json!({"Status": null, "MFADelete": null});
        let result = strip_nulls(&value);
        assert_eq!(result, serde_json::json!({}));
    }

    #[test]
    fn test_strip_nulls_preserves_non_null() {
        let value = serde_json::json!({"Status": "Enabled", "MFADelete": null});
        let result = strip_nulls(&value);
        assert_eq!(result, serde_json::json!({"Status": "Enabled"}));
    }

    #[test]
    fn test_strip_nulls_recursive() {
        let value = serde_json::json!({
            "Outer": {
                "Keep": "yes",
                "Drop": null,
                "Inner": {
                    "A": 1,
                    "B": null
                }
            },
            "TopNull": null
        });
        let result = strip_nulls(&value);
        let expected = serde_json::json!({
            "Outer": {
                "Keep": "yes",
                "Inner": {
                    "A": 1
                }
            }
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_strip_nulls_in_arrays() {
        // Nulls inside arrays are kept (they are array elements, not object fields)
        let value = serde_json::json!([{"A": null, "B": 1}, {"C": null}]);
        let result = strip_nulls(&value);
        assert_eq!(result, serde_json::json!([{"B": 1}, {}]));
    }

    #[test]
    fn test_strip_nulls_scalar_passthrough() {
        assert_eq!(strip_nulls(&serde_json::json!(42)), serde_json::json!(42));
        assert_eq!(strip_nulls(&serde_json::json!("hello")), serde_json::json!("hello"));
        assert_eq!(strip_nulls(&serde_json::json!(true)), serde_json::json!(true));
        assert_eq!(strip_nulls(&Value::Null), Value::Null);
    }
}
