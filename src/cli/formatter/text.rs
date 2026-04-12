use anyhow::Result;
use serde_json::Value;

/// Format a JSON value as AWS CLI text output.
///
/// Text output uses tab-separated values with lists expanded one item per line.
/// Object keys are sorted alphabetically. Non-scalar members are recursed into
/// with their key (uppercased) as an identifier prefix.
pub fn format_text(value: &Value) -> Result<String> {
    let mut output = String::new();
    format_value(value, None, &mut output);
    // Remove trailing newline if present for clean output
    if output.ends_with('\n') {
        output.pop();
    }
    Ok(output)
}

fn format_value(value: &Value, identifier: Option<&str>, output: &mut String) {
    match value {
        Value::Null => {
            if let Some(id) = identifier {
                output.push_str(id);
                output.push('\t');
            }
            output.push_str("None");
            output.push('\n');
        }
        Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            if let Some(id) = identifier {
                output.push_str(id);
                output.push('\t');
            }
            output.push_str(&scalar_to_string(value));
            output.push('\n');
        }
        Value::Object(map) => {
            format_object(map, identifier, output);
        }
        Value::Array(arr) => {
            format_array(arr, identifier, output);
        }
    }
}

fn format_object(
    map: &serde_json::Map<String, Value>,
    identifier: Option<&str>,
    output: &mut String,
) {
    if map.is_empty() {
        return;
    }

    // Collect keys sorted alphabetically
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();

    // Partition into scalar and non-scalar values
    let mut scalar_values: Vec<String> = Vec::new();
    let mut non_scalar_keys: Vec<&String> = Vec::new();

    for key in &keys {
        let val = &map[*key];
        if is_scalar(val) {
            scalar_values.push(scalar_to_string(val));
        } else {
            non_scalar_keys.push(key);
        }
    }

    // Output scalar values on one line (with identifier prefix if present)
    if !scalar_values.is_empty() {
        if let Some(id) = identifier {
            output.push_str(id);
            output.push('\t');
        }
        output.push_str(&scalar_values.join("\t"));
        output.push('\n');
    }

    // Recurse into non-scalar values with key as identifier
    for key in non_scalar_keys {
        let id = key.to_uppercase();
        format_value(&map[key], Some(&id), output);
    }
}

fn format_array(arr: &[Value], identifier: Option<&str>, output: &mut String) {
    if arr.is_empty() {
        return;
    }

    // Check if all elements are scalars
    let all_scalars = arr.iter().all(is_scalar);

    if all_scalars {
        if identifier.is_none() {
            // Top-level scalar array (e.g. from --query): tab-separated on one line
            let vals: Vec<String> = arr.iter().map(scalar_to_string).collect();
            output.push_str(&vals.join("\t"));
            output.push('\n');
        } else {
            // Nested scalar array: each on its own line with identifier prefix
            for item in arr {
                if let Some(id) = identifier {
                    output.push_str(id);
                    output.push('\t');
                }
                output.push_str(&scalar_to_string(item));
                output.push('\n');
            }
        }
    } else {
        // Non-scalar items: recurse into each element
        for item in arr {
            format_value(item, identifier, output);
        }
    }
}

fn is_scalar(value: &Value) -> bool {
    matches!(value, Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_))
}

fn scalar_to_string(value: &Value) -> String {
    match value {
        Value::Null => "None".to_string(),
        Value::Bool(b) => {
            if *b {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_text_format_sts_get_caller_identity() {
        // STS get-caller-identity returns a flat object
        let value = json!({
            "UserId": "AIDAI123",
            "Account": "668864905351",
            "Arn": "arn:aws:iam::668864905351:user/alice"
        });
        let result = format_text(&value).unwrap();
        // Keys sorted alphabetically: Account, Arn, UserId
        assert_eq!(
            result,
            "668864905351\tarn:aws:iam::668864905351:user/alice\tAIDAI123"
        );
    }

    #[test]
    fn test_text_format_dynamodb_list_tables() {
        // DynamoDB list-tables returns an object with a list of strings
        let value = json!({
            "TableNames": ["my-table1", "my-table2"]
        });
        let result = format_text(&value).unwrap();
        assert_eq!(result, "TABLENAMES\tmy-table1\nTABLENAMES\tmy-table2");
    }

    #[test]
    fn test_text_format_empty_list() {
        // Empty list produces no output
        let value = json!({
            "TableNames": []
        });
        let result = format_text(&value).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_text_format_s3_list_buckets() {
        // S3 list-buckets: nested objects with mixed scalar/non-scalar
        let value = json!({
            "Buckets": [
                {"Name": "my-bucket-1", "CreationDate": "2023-01-01T00:00:00+00:00"},
                {"Name": "my-bucket-2", "CreationDate": "2023-06-15T00:00:00+00:00"}
            ],
            "Owner": {
                "DisplayName": "my-display-name",
                "ID": "my-owner-id"
            }
        });
        let result = format_text(&value).unwrap();
        // Buckets is non-scalar (array of objects), Owner is non-scalar (object)
        // Both get recursed with uppercase identifiers
        // Keys sorted: Buckets, Owner
        // Each bucket: BUCKETS\tCreationDate\tName (sorted keys)
        // Owner: OWNER\tDisplayName\tID (sorted keys)
        let lines: Vec<&str> = result.split('\n').collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(
            lines[0],
            "BUCKETS\t2023-01-01T00:00:00+00:00\tmy-bucket-1"
        );
        assert_eq!(
            lines[1],
            "BUCKETS\t2023-06-15T00:00:00+00:00\tmy-bucket-2"
        );
        assert_eq!(lines[2], "OWNER\tmy-display-name\tmy-owner-id");
    }

    #[test]
    fn test_text_format_null_value() {
        let value = json!({
            "Key": null
        });
        let result = format_text(&value).unwrap();
        assert_eq!(result, "None");
    }

    #[test]
    fn test_text_format_boolean_values() {
        let value = json!({
            "Enabled": true,
            "Active": false
        });
        let result = format_text(&value).unwrap();
        // Keys sorted: Active, Enabled
        assert_eq!(result, "False\tTrue");
    }

    #[test]
    fn test_text_format_number_values() {
        let value = json!({
            "Count": 42,
            "Amount": 3.14
        });
        let result = format_text(&value).unwrap();
        // Keys sorted: Amount, Count
        assert_eq!(result, "3.14\t42");
    }

    #[test]
    fn test_text_format_mixed_scalar_nonscalar() {
        // Object with both scalar and non-scalar members
        let value = json!({
            "Name": "my-resource",
            "Id": "12345",
            "Tags": [
                {"Key": "env", "Value": "prod"},
                {"Key": "team", "Value": "infra"}
            ]
        });
        let result = format_text(&value).unwrap();
        let lines: Vec<&str> = result.split('\n').collect();
        // Scalars first (sorted: Id, Name): "12345\tmy-resource"
        // Then non-scalar Tags with identifier
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "12345\tmy-resource");
        assert_eq!(lines[1], "TAGS\tenv\tprod");
        assert_eq!(lines[2], "TAGS\tteam\tinfra");
    }

    #[test]
    fn test_text_format_scalar_string() {
        // A bare scalar value (unlikely from AWS but test coverage)
        let value = json!("hello");
        let result = format_text(&value).unwrap();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_text_format_scalar_null() {
        let value = Value::Null;
        let result = format_text(&value).unwrap();
        assert_eq!(result, "None");
    }

    #[test]
    fn test_text_format_empty_object() {
        let value = json!({});
        let result = format_text(&value).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_text_format_list_of_scalars_with_no_identifier() {
        // A bare array at top level (no identifier)
        let value = json!(["alpha", "beta", "gamma"]);
        let result = format_text(&value).unwrap();
        assert_eq!(result, "alpha\tbeta\tgamma");
    }

    #[test]
    fn test_text_format_nested_objects() {
        // EC2-style deeply nested response
        let value = json!({
            "Reservations": [
                {
                    "ReservationId": "r-123",
                    "Instances": [
                        {
                            "InstanceId": "i-abc",
                            "State": {
                                "Code": 16,
                                "Name": "running"
                            }
                        }
                    ]
                }
            ]
        });
        let result = format_text(&value).unwrap();
        let lines: Vec<&str> = result.split('\n').collect();
        // RESERVATIONS -> each reservation object:
        //   scalar: ReservationId -> "RESERVATIONS\tr-123"
        //   non-scalar: Instances -> recurse with INSTANCES
        //     INSTANCES -> each instance object:
        //       scalar: InstanceId -> "INSTANCES\ti-abc"
        //       non-scalar: State -> recurse with STATE
        //         STATE -> scalars Code, Name sorted: "STATE\t16\trunning"
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "RESERVATIONS\tr-123");
        assert_eq!(lines[1], "INSTANCES\ti-abc");
        assert_eq!(lines[2], "STATE\t16\trunning");
    }

    #[test]
    fn test_text_format_list_of_lists() {
        // Nested arrays
        let value = json!({
            "Data": [["a", "b"], ["c", "d"]]
        });
        let result = format_text(&value).unwrap();
        // DATA -> array of arrays -> recurse each sub-array with DATA identifier
        // Each sub-array is array of scalars -> each scalar on its own line with DATA prefix
        let lines: Vec<&str> = result.split('\n').collect();
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[0], "DATA\ta");
        assert_eq!(lines[1], "DATA\tb");
        assert_eq!(lines[2], "DATA\tc");
        assert_eq!(lines[3], "DATA\td");
    }

    #[test]
    fn test_text_format_single_scalar_object() {
        let value = json!({"Status": "active"});
        let result = format_text(&value).unwrap();
        assert_eq!(result, "active");
    }
}
