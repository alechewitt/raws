use anyhow::Result;
use serde_json::Value;

/// Format a JSON value as YAML output, matching AWS CLI `--output yaml` behavior.
pub fn format_yaml(value: &Value) -> Result<String> {
    let mut output = String::new();
    write_value(value, 0, false, &mut output);
    // Remove trailing newline for clean output
    if output.ends_with('\n') {
        output.pop();
    }
    Ok(output)
}

/// Format a JSON value as YAML stream output (with `---` document start marker).
pub fn format_yaml_stream(value: &Value) -> Result<String> {
    let mut output = String::from("---\n");
    write_value(value, 0, false, &mut output);
    // Remove trailing newline for clean output
    if output.ends_with('\n') {
        output.pop();
    }
    Ok(output)
}

/// Characters that force a string to be quoted in YAML.
const SPECIAL_CHARS: &[char] = &[
    ':', '{', '}', '[', ']', ',', '&', '*', '#', '?', '|', '-', '<', '>', '=', '!', '%', '@',
    '`', '\'', '"', '\n',
];

/// Write a JSON value as YAML into the output buffer.
///
/// - `indent`: current indentation level (number of spaces)
/// - `inline`: if true, the value is being written inline after a key (no leading indent)
fn write_value(value: &Value, indent: usize, inline: bool, output: &mut String) {
    match value {
        Value::Null => {
            if !inline {
                push_indent(output, indent);
            }
            output.push_str("null\n");
        }
        Value::Bool(b) => {
            if !inline {
                push_indent(output, indent);
            }
            if *b {
                output.push_str("true\n");
            } else {
                output.push_str("false\n");
            }
        }
        Value::Number(n) => {
            if !inline {
                push_indent(output, indent);
            }
            output.push_str(&n.to_string());
            output.push('\n');
        }
        Value::String(s) => {
            if !inline {
                push_indent(output, indent);
            }
            write_yaml_string(s, output);
            output.push('\n');
        }
        Value::Object(map) => {
            if map.is_empty() {
                if !inline {
                    push_indent(output, indent);
                }
                output.push_str("{}\n");
                return;
            }
            // If inline (after a key on same line), we need a newline first
            // then write the object keys at indent + 2
            if inline {
                output.push('\n');
                write_object_entries(map, indent + 2, output);
            } else {
                write_object_entries(map, indent, output);
            }
        }
        Value::Array(arr) => {
            if arr.is_empty() {
                if !inline {
                    push_indent(output, indent);
                }
                output.push_str("[]\n");
                return;
            }
            if inline {
                output.push('\n');
                write_array_entries(arr, indent, output);
            } else {
                write_array_entries(arr, indent, output);
            }
        }
    }
}

/// Write the key-value entries of a JSON object as YAML.
fn write_object_entries(map: &serde_json::Map<String, Value>, indent: usize, output: &mut String) {
    // Iterate in map order (alphabetical by default in serde_json)
    for (key, val) in map.iter() {
        push_indent(output, indent);
        write_yaml_string(key, output);
        output.push(':');

        match val {
            Value::Object(inner) if !inner.is_empty() => {
                output.push('\n');
                write_object_entries(inner, indent + 2, output);
            }
            Value::Array(inner) if !inner.is_empty() => {
                output.push('\n');
                write_array_entries(inner, indent, output);
            }
            _ => {
                // Scalar, empty object, or empty array: write inline after ": "
                output.push(' ');
                write_value(val, indent + 2, true, output);
            }
        }
    }
}

/// Write array entries as YAML list items with `- ` prefix.
fn write_array_entries(arr: &[Value], indent: usize, output: &mut String) {
    for item in arr {
        push_indent(output, indent);
        output.push_str("- ");

        match item {
            Value::Object(map) if !map.is_empty() => {
                // First key-value pair goes on the same line as `- `
                let mut first = true;
                for (key, val) in map.iter() {
                    if first {
                        first = false;
                        write_yaml_string(key, output);
                        output.push(':');
                        match val {
                            Value::Object(inner) if !inner.is_empty() => {
                                output.push('\n');
                                write_object_entries(inner, indent + 4, output);
                            }
                            Value::Array(inner) if !inner.is_empty() => {
                                output.push('\n');
                                write_array_entries(inner, indent + 2, output);
                            }
                            _ => {
                                output.push(' ');
                                write_value(val, indent + 4, true, output);
                            }
                        }
                    } else {
                        push_indent(output, indent + 2);
                        write_yaml_string(key, output);
                        output.push(':');
                        match val {
                            Value::Object(inner) if !inner.is_empty() => {
                                output.push('\n');
                                write_object_entries(inner, indent + 4, output);
                            }
                            Value::Array(inner) if !inner.is_empty() => {
                                output.push('\n');
                                write_array_entries(inner, indent + 2, output);
                            }
                            _ => {
                                output.push(' ');
                                write_value(val, indent + 4, true, output);
                            }
                        }
                    }
                }
            }
            Value::Array(inner) if !inner.is_empty() => {
                // Nested non-empty array: first item on same line as `- `,
                // remaining items indented to align with the first
                write_array_first_inline(inner, indent + 2, output);
            }
            _ => {
                // Scalar, empty object, or empty array
                write_value(item, indent + 2, true, output);
            }
        }
    }
}

/// Write a nested array where the first element appears inline (after `- `)
/// and remaining elements are indented to align.
fn write_array_first_inline(arr: &[Value], indent: usize, output: &mut String) {
    for (i, item) in arr.iter().enumerate() {
        if i == 0 {
            // First item: written inline (no indent, already after `- `)
            match item {
                Value::Array(inner) if !inner.is_empty() => {
                    output.push_str("- ");
                    write_array_first_inline(inner, indent + 2, output);
                }
                _ => {
                    output.push_str("- ");
                    write_value(item, indent + 2, true, output);
                }
            }
        } else {
            push_indent(output, indent);
            output.push_str("- ");
            match item {
                Value::Array(inner) if !inner.is_empty() => {
                    write_array_first_inline(inner, indent + 2, output);
                }
                _ => {
                    write_value(item, indent + 2, true, output);
                }
            }
        }
    }
}

/// Push `count` spaces into the output buffer.
fn push_indent(output: &mut String, count: usize) {
    for _ in 0..count {
        output.push(' ');
    }
}

/// Write a string value in YAML format. Strings that contain special characters
/// or look like YAML reserved words/numbers are single-quoted; otherwise unquoted.
fn write_yaml_string(s: &str, output: &mut String) {
    if s.is_empty() {
        output.push_str("''");
        return;
    }

    if needs_quoting(s) {
        // Use single quotes, escaping embedded single quotes by doubling them
        output.push('\'');
        for ch in s.chars() {
            if ch == '\'' {
                output.push_str("''");
            } else {
                output.push(ch);
            }
        }
        output.push('\'');
    } else {
        output.push_str(s);
    }
}

/// Determine whether a string needs quoting in YAML.
fn needs_quoting(s: &str) -> bool {
    // Empty strings always need quoting (handled before this is called, but be safe)
    if s.is_empty() {
        return true;
    }

    // Contains special YAML characters
    if s.chars().any(|c| SPECIAL_CHARS.contains(&c)) {
        return true;
    }

    // Looks like a boolean
    let lower = s.to_lowercase();
    if matches!(
        lower.as_str(),
        "true" | "false" | "yes" | "no" | "on" | "off" | "y" | "n"
    ) {
        return true;
    }

    // Looks like null
    if matches!(lower.as_str(), "null" | "~") {
        return true;
    }

    // Looks like a number (integer or float)
    if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
        return true;
    }

    // Starts or ends with whitespace
    if s.starts_with(' ') || s.ends_with(' ') {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_yaml_simple_flat_object() {
        // STS get-caller-identity style
        let value = json!({
            "UserId": "AIDAEXAMPLE",
            "Account": "123456789012",
            "Arn": "arn:aws:iam::123456789012:user/alice"
        });
        let output = format_yaml(&value).unwrap();
        assert!(output.contains("UserId: AIDAEXAMPLE"));
        assert!(output.contains("Account:"));
        assert!(output.contains("Arn:"));
        // Account value looks like a number so should be quoted
        assert!(output.contains("'123456789012'"));
    }

    #[test]
    fn test_yaml_nested_objects() {
        let value = json!({
            "Instance": {
                "InstanceId": "i-abc123",
                "State": {
                    "Code": 16,
                    "Name": "running"
                }
            }
        });
        let output = format_yaml(&value).unwrap();
        assert!(output.contains("Instance:\n"));
        assert!(output.contains("  InstanceId: 'i-abc123'\n"));
        assert!(output.contains("  State:\n"));
        assert!(output.contains("    Code: 16\n"));
        assert!(output.contains("    Name: running"));
    }

    #[test]
    fn test_yaml_array_of_strings() {
        let value = json!({
            "TableNames": ["my-table1", "my-table2", "my-table3"]
        });
        let output = format_yaml(&value).unwrap();
        // Strings with hyphens need quoting
        assert!(output.contains("TableNames:\n"));
        assert!(output.contains("- 'my-table1'\n"));
        assert!(output.contains("- 'my-table2'\n"));
        assert!(output.contains("- 'my-table3'"));
    }

    #[test]
    fn test_yaml_array_of_objects() {
        // S3 list-buckets style
        let value = json!({
            "Buckets": [
                {
                    "Name": "my-bucket-1",
                    "CreationDate": "2023-01-01T00:00:00+00:00"
                },
                {
                    "Name": "my-bucket-2",
                    "CreationDate": "2023-06-15T00:00:00+00:00"
                }
            ]
        });
        let output = format_yaml(&value).unwrap();
        assert!(output.contains("Buckets:\n"));
        // serde_json::Map iterates in sorted key order: CreationDate before Name
        assert!(output.contains("- CreationDate: '2023-01-01T00:00:00+00:00'\n"));
        assert!(output.contains("  Name: 'my-bucket-1'\n"));
        assert!(output.contains("- CreationDate: '2023-06-15T00:00:00+00:00'\n"));
        assert!(output.contains("  Name: 'my-bucket-2'"));
    }

    #[test]
    fn test_yaml_empty_object() {
        let value = json!({});
        let output = format_yaml(&value).unwrap();
        assert_eq!(output, "{}");
    }

    #[test]
    fn test_yaml_empty_array() {
        let value = json!([]);
        let output = format_yaml(&value).unwrap();
        assert_eq!(output, "[]");
    }

    #[test]
    fn test_yaml_empty_nested() {
        let value = json!({
            "Items": [],
            "Metadata": {}
        });
        let output = format_yaml(&value).unwrap();
        assert!(output.contains("Items: []\n"));
        assert!(output.contains("Metadata: {}"));
    }

    #[test]
    fn test_yaml_boolean_null_number() {
        let value = json!({
            "Enabled": true,
            "Active": false,
            "Description": null,
            "Count": 42,
            "Ratio": 3.14
        });
        let output = format_yaml(&value).unwrap();
        assert!(output.contains("Enabled: true\n"));
        assert!(output.contains("Active: false\n"));
        assert!(output.contains("Description: null\n"));
        assert!(output.contains("Count: 42\n"));
        assert!(output.contains("Ratio: 3.14"));
    }

    #[test]
    fn test_yaml_strings_needing_quoting_colon() {
        let value = json!({
            "Arn": "arn:aws:iam::123456789012:root"
        });
        let output = format_yaml(&value).unwrap();
        // Contains colons, so must be quoted
        assert!(
            output.contains("Arn: 'arn:aws:iam::123456789012:root'"),
            "Expected quoted ARN, got: {output}"
        );
    }

    #[test]
    fn test_yaml_strings_needing_quoting_special() {
        let value = json!({
            "Filter": "key=value&other=123",
            "Comment": "# this is a comment",
            "Flag": "true",
            "Empty": "",
            "Numeric": "42"
        });
        let output = format_yaml(&value).unwrap();
        // & requires quoting
        assert!(output.contains("'key=value&other=123'"));
        // # requires quoting
        assert!(output.contains("'# this is a comment'"));
        // "true" as string must be quoted to distinguish from boolean
        assert!(output.contains("Flag: 'true'"));
        // Empty string must be quoted
        assert!(output.contains("Empty: ''"));
        // "42" as string must be quoted to distinguish from number
        assert!(output.contains("Numeric: '42'"));
    }

    #[test]
    fn test_yaml_string_with_single_quote() {
        let value = json!({
            "Message": "it's working"
        });
        let output = format_yaml(&value).unwrap();
        // Single quotes inside are doubled in YAML single-quoted style
        assert!(
            output.contains("'it''s working'"),
            "Expected escaped single quote, got: {output}"
        );
    }

    #[test]
    fn test_yaml_stream_format() {
        let value = json!({
            "UserId": "AIDAEXAMPLE",
            "Account": "123456789012"
        });
        let output = format_yaml_stream(&value).unwrap();
        assert!(
            output.starts_with("---\n"),
            "yaml-stream must start with document marker, got: {output}"
        );
        assert!(output.contains("UserId: AIDAEXAMPLE"));
    }

    #[test]
    fn test_yaml_stream_simple_scalar() {
        let value = json!("hello");
        let output = format_yaml_stream(&value).unwrap();
        assert_eq!(output, "---\nhello");
    }

    #[test]
    fn test_yaml_deeply_nested() {
        // EC2 describe-instances style
        let value = json!({
            "Reservations": [
                {
                    "ReservationId": "r-0123456789abcdef0",
                    "Instances": [
                        {
                            "InstanceId": "i-abc123",
                            "State": {
                                "Code": 16,
                                "Name": "running"
                            },
                            "Tags": [
                                {
                                    "Key": "Name",
                                    "Value": "web-server"
                                }
                            ]
                        }
                    ]
                }
            ]
        });
        let output = format_yaml(&value).unwrap();
        // serde_json sorts keys alphabetically: Instances before ReservationId
        assert!(output.contains("Reservations:\n"));
        assert!(output.contains("- Instances:\n"));
        assert!(output.contains("  - InstanceId: 'i-abc123'\n"));
        assert!(output.contains("    State:\n"));
        assert!(output.contains("      Code: 16\n"));
        assert!(output.contains("      Name: running\n"));
        assert!(output.contains("    Tags:\n"));
        assert!(output.contains("    - Key: Name\n"));
        assert!(output.contains("      Value: 'web-server'\n"));
        assert!(output.contains("  ReservationId: 'r-0123456789abcdef0'"));
    }

    #[test]
    fn test_yaml_mixed_scalar_nonscalar() {
        let value = json!({
            "Name": "my-resource",
            "Id": "abc-123",
            "Tags": [
                {"Key": "env", "Value": "prod"}
            ],
            "Config": {
                "Timeout": 30,
                "Retries": 3
            }
        });
        let output = format_yaml(&value).unwrap();
        // Scalars are inline
        assert!(output.contains("Name: 'my-resource'\n"));
        assert!(output.contains("Id: 'abc-123'\n"));
        // Non-scalars are nested
        assert!(output.contains("Tags:\n"));
        assert!(output.contains("- Key: env\n"));
        assert!(output.contains("Config:\n"));
        assert!(output.contains("  Timeout: 30\n"));
    }

    #[test]
    fn test_yaml_top_level_array() {
        let value = json!(["alpha", "beta", "gamma"]);
        let output = format_yaml(&value).unwrap();
        assert_eq!(output, "- alpha\n- beta\n- gamma");
    }

    #[test]
    fn test_yaml_top_level_scalar() {
        let value = json!(42);
        let output = format_yaml(&value).unwrap();
        assert_eq!(output, "42");
    }

    #[test]
    fn test_yaml_top_level_null() {
        let value = Value::Null;
        let output = format_yaml(&value).unwrap();
        assert_eq!(output, "null");
    }

    #[test]
    fn test_yaml_top_level_bool() {
        let value = json!(true);
        let output = format_yaml(&value).unwrap();
        assert_eq!(output, "true");
    }

    #[test]
    fn test_yaml_nested_empty_collections() {
        let value = json!({
            "Data": {
                "Items": [],
                "Meta": {}
            }
        });
        let output = format_yaml(&value).unwrap();
        assert!(output.contains("Data:\n"));
        assert!(output.contains("  Items: []\n"));
        assert!(output.contains("  Meta: {}"));
    }

    #[test]
    fn test_yaml_needs_quoting_detection() {
        // Strings that need quoting
        assert!(needs_quoting("arn:aws:iam::123:root")); // colon
        assert!(needs_quoting("{key}")); // braces
        assert!(needs_quoting("[item]")); // brackets
        assert!(needs_quoting("a,b")); // comma
        assert!(needs_quoting("true")); // boolean
        assert!(needs_quoting("false")); // boolean
        assert!(needs_quoting("yes")); // boolean
        assert!(needs_quoting("null")); // null
        assert!(needs_quoting("42")); // number
        assert!(needs_quoting("3.14")); // float
        assert!(needs_quoting("")); // empty
        assert!(needs_quoting(" leading")); // leading space
        assert!(needs_quoting("trailing ")); // trailing space
        assert!(needs_quoting("line\nbreak")); // newline

        // Strings that don't need quoting
        assert!(!needs_quoting("simple"));
        assert!(!needs_quoting("InstanceId"));
        assert!(!needs_quoting("running"));
        assert!(!needs_quoting("my_resource"));
    }

    #[test]
    fn test_yaml_stream_empty_object() {
        let value = json!({});
        let output = format_yaml_stream(&value).unwrap();
        assert_eq!(output, "---\n{}");
    }

    #[test]
    fn test_yaml_array_of_arrays() {
        let value = json!({
            "Matrix": [[1, 2], [3, 4]]
        });
        let output = format_yaml(&value).unwrap();
        assert!(output.contains("Matrix:\n"));
        // Nested arrays: first sub-item on same line as `- `
        assert!(output.contains("- - 1\n"));
        assert!(output.contains("  - 2\n"));
        assert!(output.contains("- - 3\n"));
        assert!(output.contains("  - 4"));
    }
}
