use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{BufRead, Write};

/// Check if auto-prompt should be triggered for the given operation.
/// Returns true if `--cli-auto-prompt` is set and there are missing required parameters.
pub fn should_auto_prompt(
    auto_prompt_enabled: bool,
    provided_args: &[String],
    input_shape: &Value,
    _shapes: &HashMap<String, Value>,
) -> bool {
    if !auto_prompt_enabled {
        return false;
    }
    let required = get_required_members(input_shape);
    let provided = collect_provided_params(provided_args);
    required.iter().any(|r| !provided.contains(r))
}

/// Get the list of required member names from an input shape.
pub fn get_required_members(input_shape: &Value) -> Vec<String> {
    input_shape
        .get("required")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Collect parameter names that were provided on the command line.
/// Looks for `--param-name` patterns and converts to PascalCase member names.
fn collect_provided_params(args: &[String]) -> Vec<String> {
    args.iter()
        .filter(|a| a.starts_with("--"))
        .map(|a| {
            let param = a.trim_start_matches("--");
            // Convert kebab-case to PascalCase (e.g., "bucket-name" -> "BucketName")
            kebab_to_pascal(param)
        })
        .collect()
}

/// Convert kebab-case to PascalCase.
fn kebab_to_pascal(s: &str) -> String {
    s.split('-')
        .map(|part| {
            let mut c = part.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().chain(c).collect(),
            }
        })
        .collect()
}

/// Prompt the user for missing required parameters and return the additional args.
/// Uses the provided writer/reader for testability.
pub fn prompt_for_missing_params<R: BufRead, W: Write>(
    input_shape: &Value,
    shapes: &HashMap<String, Value>,
    provided_args: &[String],
    reader: &mut R,
    writer: &mut W,
) -> Result<Vec<String>> {
    let required = get_required_members(input_shape);
    let provided = collect_provided_params(provided_args);
    let mut additional_args = Vec::new();

    let empty_map = serde_json::Map::new();
    let members = input_shape
        .get("members")
        .and_then(|v| v.as_object())
        .unwrap_or(&empty_map);

    for member_name in &required {
        if provided.contains(member_name) {
            continue;
        }

        // Convert PascalCase to kebab-case for the CLI arg name
        let cli_param = pascal_to_kebab(member_name);

        // Get the member's shape type for display
        let type_hint = members
            .get(member_name)
            .and_then(|m| m.get("shape"))
            .and_then(|s| s.as_str())
            .and_then(|name| shapes.get(name))
            .and_then(|s| s.get("type"))
            .and_then(|t| t.as_str())
            .unwrap_or("string");

        write!(writer, "{} [{}]: ", member_name, type_hint)?;
        writer.flush()?;

        let mut input = String::new();
        reader.read_line(&mut input)?;
        let value = input.trim().to_string();

        if !value.is_empty() {
            additional_args.push(format!("--{}", cli_param));
            additional_args.push(value);
        }
    }

    Ok(additional_args)
}

/// Convert PascalCase to kebab-case.
fn pascal_to_kebab(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('-');
        }
        if let Some(lower) = ch.to_lowercase().next() {
            result.push(lower);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_get_required_members_with_required_field() {
        let shape = json!({
            "type": "structure",
            "required": ["BucketName", "Key"],
            "members": {
                "BucketName": {"shape": "BucketName"},
                "Key": {"shape": "ObjectKey"}
            }
        });
        let result = get_required_members(&shape);
        assert_eq!(result, vec!["BucketName", "Key"]);
    }

    #[test]
    fn test_get_required_members_with_no_required_field() {
        let shape = json!({
            "type": "structure",
            "members": {
                "MaxItems": {"shape": "MaxItems"}
            }
        });
        let result = get_required_members(&shape);
        assert!(result.is_empty());
    }

    #[test]
    fn test_get_required_members_with_empty_required_array() {
        let shape = json!({
            "type": "structure",
            "required": [],
            "members": {}
        });
        let result = get_required_members(&shape);
        assert!(result.is_empty());
    }

    #[test]
    fn test_collect_provided_params_extracts_param_names() {
        let args = vec![
            "--bucket-name".to_string(),
            "my-bucket".to_string(),
            "--key".to_string(),
            "my-key".to_string(),
        ];
        let result = collect_provided_params(&args);
        assert_eq!(result, vec!["BucketName", "Key"]);
    }

    #[test]
    fn test_collect_provided_params_with_no_params() {
        let args: Vec<String> = vec!["some-value".to_string()];
        let result = collect_provided_params(&args);
        assert!(result.is_empty());
    }

    #[test]
    fn test_kebab_to_pascal_basic_conversions() {
        assert_eq!(kebab_to_pascal("bucket-name"), "BucketName");
        assert_eq!(kebab_to_pascal("key"), "Key");
        assert_eq!(kebab_to_pascal("user-pool-id"), "UserPoolId");
        assert_eq!(kebab_to_pascal(""), "");
        assert_eq!(kebab_to_pascal("a"), "A");
    }

    #[test]
    fn test_pascal_to_kebab_basic_conversions() {
        assert_eq!(pascal_to_kebab("BucketName"), "bucket-name");
        assert_eq!(pascal_to_kebab("Key"), "key");
        assert_eq!(pascal_to_kebab("UserPoolId"), "user-pool-id");
        assert_eq!(pascal_to_kebab(""), "");
        assert_eq!(pascal_to_kebab("A"), "a");
    }

    #[test]
    fn test_should_auto_prompt_returns_false_when_disabled() {
        let shape = json!({
            "required": ["BucketName"],
            "members": {
                "BucketName": {"shape": "BucketName"}
            }
        });
        let shapes = HashMap::new();
        let args: Vec<String> = vec![];
        assert!(!should_auto_prompt(false, &args, &shape, &shapes));
    }

    #[test]
    fn test_should_auto_prompt_returns_false_when_all_params_provided() {
        let shape = json!({
            "required": ["BucketName"],
            "members": {
                "BucketName": {"shape": "BucketName"}
            }
        });
        let shapes = HashMap::new();
        let args = vec!["--bucket-name".to_string(), "my-bucket".to_string()];
        assert!(!should_auto_prompt(true, &args, &shape, &shapes));
    }

    #[test]
    fn test_should_auto_prompt_returns_true_when_params_missing() {
        let shape = json!({
            "required": ["BucketName", "Key"],
            "members": {
                "BucketName": {"shape": "BucketName"},
                "Key": {"shape": "ObjectKey"}
            }
        });
        let shapes = HashMap::new();
        let args = vec!["--bucket-name".to_string(), "my-bucket".to_string()];
        assert!(should_auto_prompt(true, &args, &shape, &shapes));
    }

    #[test]
    fn test_prompt_for_missing_params_with_mock_reader_writer() {
        let input_shape = json!({
            "required": ["TableName", "Key"],
            "members": {
                "TableName": {"shape": "TableName"},
                "Key": {"shape": "KeySchema"}
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert(
            "TableName".to_string(),
            json!({"type": "string"}),
        );
        shapes.insert(
            "KeySchema".to_string(),
            json!({"type": "map"}),
        );

        let provided_args: Vec<String> = vec![];
        let input = b"my-table\n{\"id\": {\"S\": \"123\"}}\n";
        let mut reader = &input[..];
        let mut writer = Vec::new();

        let result = prompt_for_missing_params(
            &input_shape,
            &shapes,
            &provided_args,
            &mut reader,
            &mut writer,
        )
        .unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], "--table-name");
        assert_eq!(result[1], "my-table");
        assert_eq!(result[2], "--key");
        assert_eq!(result[3], "{\"id\": {\"S\": \"123\"}}");

        let output = String::from_utf8(writer).unwrap();
        assert!(output.contains("TableName [string]: "));
        assert!(output.contains("Key [map]: "));
    }

    #[test]
    fn test_prompt_for_missing_params_skips_already_provided() {
        let input_shape = json!({
            "required": ["TableName", "Key"],
            "members": {
                "TableName": {"shape": "TableName"},
                "Key": {"shape": "KeySchema"}
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert(
            "TableName".to_string(),
            json!({"type": "string"}),
        );
        shapes.insert(
            "KeySchema".to_string(),
            json!({"type": "map"}),
        );

        let provided_args = vec!["--table-name".to_string(), "existing-table".to_string()];
        let input = b"{\"id\": {\"S\": \"456\"}}\n";
        let mut reader = &input[..];
        let mut writer = Vec::new();

        let result = prompt_for_missing_params(
            &input_shape,
            &shapes,
            &provided_args,
            &mut reader,
            &mut writer,
        )
        .unwrap();

        // Only Key should be prompted (TableName was already provided)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "--key");
        assert_eq!(result[1], "{\"id\": {\"S\": \"456\"}}");

        let output = String::from_utf8(writer).unwrap();
        assert!(!output.contains("TableName"));
        assert!(output.contains("Key [map]: "));
    }

    #[test]
    fn test_prompt_for_missing_params_empty_input_skips_arg() {
        let input_shape = json!({
            "required": ["TableName"],
            "members": {
                "TableName": {"shape": "TableName"}
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert(
            "TableName".to_string(),
            json!({"type": "string"}),
        );

        let provided_args: Vec<String> = vec![];
        let input = b"\n";
        let mut reader = &input[..];
        let mut writer = Vec::new();

        let result = prompt_for_missing_params(
            &input_shape,
            &shapes,
            &provided_args,
            &mut reader,
            &mut writer,
        )
        .unwrap();

        // Empty input should not produce any additional args
        assert!(result.is_empty());
    }

    #[test]
    fn test_prompt_for_missing_params_unknown_shape_defaults_to_string() {
        let input_shape = json!({
            "required": ["FooParam"],
            "members": {
                "FooParam": {"shape": "UnknownShape"}
            }
        });
        let shapes = HashMap::new(); // No shape definitions at all

        let provided_args: Vec<String> = vec![];
        let input = b"some-value\n";
        let mut reader = &input[..];
        let mut writer = Vec::new();

        let result = prompt_for_missing_params(
            &input_shape,
            &shapes,
            &provided_args,
            &mut reader,
            &mut writer,
        )
        .unwrap();

        assert_eq!(result, vec!["--foo-param", "some-value"]);

        let output = String::from_utf8(writer).unwrap();
        assert!(output.contains("FooParam [string]: "));
    }

    #[test]
    fn test_should_auto_prompt_no_required_members() {
        let shape = json!({
            "type": "structure",
            "members": {
                "MaxItems": {"shape": "MaxItems"}
            }
        });
        let shapes = HashMap::new();
        let args: Vec<String> = vec![];
        // No required members => nothing missing => false
        assert!(!should_auto_prompt(true, &args, &shape, &shapes));
    }

    #[test]
    fn test_kebab_to_pascal_single_segment() {
        assert_eq!(kebab_to_pascal("name"), "Name");
    }

    #[test]
    fn test_pascal_to_kebab_single_word() {
        assert_eq!(pascal_to_kebab("Name"), "name");
    }

    #[test]
    fn test_collect_provided_params_ignores_values() {
        let args = vec![
            "--table-name".to_string(),
            "my-table".to_string(),
            "--region".to_string(),
            "us-east-1".to_string(),
            "positional".to_string(),
        ];
        let result = collect_provided_params(&args);
        // Should only pick up --table-name and --region, not the values
        assert_eq!(result, vec!["TableName", "Region"]);
    }
}
