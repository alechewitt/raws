pub mod json;
pub mod table;
pub mod text;
pub mod yaml;

use anyhow::Result;
use serde_json::Value;

/// Format output in the requested format, stripping null values first.
///
/// AWS CLI omits null-valued fields from output. We apply `strip_nulls`
/// before formatting so that all output formats (json, text, table, yaml)
/// consistently omit null fields.
pub fn format_output_with_title(value: &Value, format: &str, title: Option<&str>) -> Result<String> {
    let cleaned = json::strip_nulls(value);
    match format {
        "json" => json::format_json(&cleaned),
        "text" => text::format_text(&cleaned),
        "table" => table::format_table_with_title(&cleaned, title),
        "yaml" => yaml::format_yaml(&cleaned),
        "yaml-stream" => yaml::format_yaml_stream(&cleaned),
        _ => json::format_json(&cleaned),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_strip_nulls_in_json_output() {
        let value = json!({"Status": null, "MFADelete": null});
        let result = format_output_with_title(&value, "json", None).unwrap();
        assert_eq!(result, "{}");
    }

    #[test]
    fn test_strip_nulls_preserves_non_null_values() {
        let value = json!({"Status": "Enabled", "MFADelete": null});
        let result = format_output_with_title(&value, "json", None).unwrap();
        assert!(result.contains("Status"));
        assert!(result.contains("Enabled"));
        assert!(!result.contains("MFADelete"));
        assert!(!result.contains("null"));
    }

    #[test]
    fn test_strip_nulls_nested() {
        let value = json!({
            "Outer": {
                "Keep": "yes",
                "Drop": null
            }
        });
        let result = format_output_with_title(&value, "json", None).unwrap();
        assert!(result.contains("Keep"));
        assert!(!result.contains("Drop"));
    }

    #[test]
    fn test_strip_nulls_in_text_output() {
        let value = json!({"Status": null, "Name": "test"});
        let result = format_output_with_title(&value, "text", None).unwrap();
        assert!(result.contains("test"));
        // Null field should be stripped, so "None" should not appear
        assert!(!result.contains("None"), "text output should not show None for stripped nulls");
    }

    #[test]
    fn test_strip_nulls_in_table_output() {
        let value = json!({"Status": null, "Name": "test"});
        let result = format_output_with_title(&value, "table", None).unwrap();
        assert!(result.contains("test"));
        // Should not have a Status row since it was null
        assert!(!result.contains("Status"), "table output should not show null fields");
    }
}
