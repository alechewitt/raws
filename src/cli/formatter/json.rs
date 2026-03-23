use anyhow::{Context, Result};
use serde_json::Value;

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
}
