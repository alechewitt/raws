pub mod json;
pub mod query;
pub mod rest_json;
pub mod rest_xml;

use chrono::{DateTime, Utc};

/// Format a DateTime<Utc> in AWS CLI ISO 8601 format with UTC offset (+00:00).
///
/// AWS CLI v2 outputs timestamps in UTC with +00:00 offset,
/// with 6-digit microsecond precision when sub-second component is non-zero.
fn format_utc_datetime(dt: DateTime<Utc>) -> String {
    let nanos = dt.timestamp_subsec_nanos();
    if nanos == 0 {
        dt.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()
    } else {
        // Python's isoformat uses 6-digit microseconds
        let micros = nanos / 1000;
        dt.format("%Y-%m-%dT%H:%M:%S").to_string()
            + &format!(".{micros:06}+00:00")
    }
}

/// Normalize a timestamp string to the format used by the AWS CLI.
/// AWS CLI v2 outputs timestamps as ISO 8601 in UTC (+00:00),
/// and omits sub-second precision when it's zero.
///
/// Examples:
///   "2023-01-01T00:00:00.000Z" -> "2023-01-01T00:00:00+00:00"
///   "2023-01-01T00:00:00Z" -> "2023-01-01T00:00:00+00:00"
///   "2023-01-01T00:00:00.123Z" -> "2023-01-01T00:00:00.123000+00:00"
pub fn normalize_timestamp(s: &str) -> String {
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        format_utc_datetime(dt)
    } else {
        // Can't parse; return as-is
        s.to_string()
    }
}

/// Convert an epoch timestamp (seconds, possibly with fractional part) to
/// the AWS CLI ISO 8601 format in UTC.
pub fn epoch_to_iso(epoch: f64) -> String {
    let secs = epoch as i64;
    // Round to nearest microsecond to avoid floating-point precision issues
    let frac = epoch - secs as f64;
    let micros = (frac * 1_000_000.0).round() as u32;
    let nanos = micros * 1000;
    if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
        format_utc_datetime(dt)
    } else {
        format!("{epoch}")
    }
}

use std::collections::HashMap;
use serde_json::Value;

/// Walk a response Value using the model's shape definitions to:
/// 1. Convert timestamp fields from epoch numbers or Z-suffix strings to AWS CLI format
/// 2. Reorder structure keys to match model member order (for AWS CLI output parity)
///
/// This is needed for JSON/REST-JSON protocols where timestamps may arrive
/// as epoch seconds and keys may be in API response order rather than model order.
pub fn normalize_response_value(
    value: &mut Value,
    shape_name: &str,
    shapes: &HashMap<String, Value>,
) {
    let Some(shape_def) = shapes.get(shape_name) else {
        return;
    };
    let shape_type = shape_def.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match shape_type {
        "structure" => {
            if let Value::Object(map) = value {
                let members = shape_def
                    .get("members")
                    .and_then(|m| m.as_object());

                if let Some(members) = members {
                    // Build a new map with keys in model member order
                    let mut ordered = serde_json::Map::with_capacity(map.len());

                    // First: insert model members in model order
                    for (member_name, member_def) in members {
                        if let Some(mut member_value) = map.remove(member_name) {
                            let member_shape = member_def
                                .get("shape")
                                .and_then(|s| s.as_str())
                                .unwrap_or("");

                            // Convert null to empty array when model says the field is a list
                            if member_value.is_null() {
                                let member_shape_type = shapes
                                    .get(member_shape)
                                    .and_then(|s| s.get("type"))
                                    .and_then(|t| t.as_str())
                                    .unwrap_or("");
                                if member_shape_type == "list" {
                                    member_value = Value::Array(Vec::new());
                                }
                            }

                            normalize_response_value(&mut member_value, member_shape, shapes);
                            ordered.insert(member_name.clone(), member_value);
                        }
                    }

                    // Drop non-model keys (matching botocore which only deserializes model members)
                    *map = ordered;
                }
            }
        }
        "list" => {
            let member_shape = shape_def
                .get("member")
                .and_then(|m| m.get("shape"))
                .and_then(|s| s.as_str())
                .unwrap_or("");

            // If a single scalar value is provided where a list is expected,
            // wrap it in an array (some services return a bare string for single-element lists)
            if !value.is_array() && !value.is_null() {
                let single = std::mem::replace(value, Value::Null);
                *value = Value::Array(vec![single]);
            }

            if let Value::Array(arr) = value {
                for item in arr.iter_mut() {
                    normalize_response_value(item, member_shape, shapes);
                }
            }
        }
        "map" => {
            let value_shape = shape_def
                .get("value")
                .and_then(|v| v.get("shape"))
                .and_then(|s| s.as_str())
                .unwrap_or("");
            if let Value::Object(map) = value {
                for v in map.values_mut() {
                    normalize_response_value(v, value_shape, shapes);
                }
            }
        }
        "timestamp" => {
            match value {
                Value::Number(n) => {
                    // Epoch seconds -> ISO 8601
                    if let Some(f) = n.as_f64() {
                        *value = Value::String(epoch_to_iso(f));
                    }
                }
                Value::String(s) => {
                    // Already a string but might need normalization (Z -> +00:00)
                    let normalized = normalize_timestamp(s);
                    *s = normalized;
                }
                _ => {}
            }
        }
        _ => {
            // string, integer, float, boolean, blob - no transformation needed
        }
    }
}

/// Add null for missing top-level output shape members (matching botocore behavior).
/// This only applies to the top-level output structure, not nested structures.
pub fn fill_missing_top_level_members(
    value: &mut Value,
    shape_name: &str,
    shapes: &HashMap<String, Value>,
) {
    let Some(shape_def) = shapes.get(shape_name) else {
        return;
    };
    if shape_def.get("type").and_then(|t| t.as_str()) != Some("structure") {
        return;
    }
    let Some(members) = shape_def.get("members").and_then(|m| m.as_object()) else {
        return;
    };
    if let Value::Object(map) = value {
        for member_name in members.keys() {
            if !map.contains_key(member_name) {
                map.insert(member_name.clone(), Value::Null);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_timestamp_z_suffix() {
        let result = normalize_timestamp("2023-01-01T00:00:00Z");
        assert_eq!(result, "2023-01-01T00:00:00+00:00");
    }

    #[test]
    fn test_normalize_timestamp_z_with_millis() {
        let result = normalize_timestamp("2026-03-17T21:03:46.000Z");
        // .000Z means no sub-second, so no microseconds in output
        assert_eq!(result, "2026-03-17T21:03:46+00:00");
    }

    #[test]
    fn test_normalize_timestamp_nonzero_millis() {
        let result = normalize_timestamp("2023-01-01T00:00:00.123Z");
        assert_eq!(result, "2023-01-01T00:00:00.123000+00:00");
    }

    #[test]
    fn test_normalize_timestamp_already_offset() {
        let result = normalize_timestamp("2023-01-01T00:00:00+00:00");
        assert_eq!(result, "2023-01-01T00:00:00+00:00");
    }

    #[test]
    fn test_normalize_timestamp_unparseable() {
        assert_eq!(normalize_timestamp("not-a-date"), "not-a-date");
    }

    #[test]
    fn test_normalize_timestamp_uses_utc() {
        // Verify the output always uses UTC +00:00
        let result = normalize_timestamp("2023-06-15T12:00:00Z");
        assert!(
            result.ends_with("+00:00"),
            "Expected +00:00 offset, got: {result}"
        );
        assert_eq!(result, "2023-06-15T12:00:00+00:00");
    }

    #[test]
    fn test_epoch_to_iso() {
        let result = epoch_to_iso(1672531200.0);
        assert_eq!(result, "2023-01-01T00:00:00+00:00");
    }

    #[test]
    fn test_epoch_to_iso_with_fraction() {
        let result = epoch_to_iso(1672531200.123);
        assert_eq!(result, "2023-01-01T00:00:00.123000+00:00");
    }

    #[test]
    fn test_normalize_timestamps_epoch_to_iso() {
        use serde_json::json;
        let mut shapes = HashMap::new();
        shapes.insert("Output".to_string(), json!({
            "type": "structure",
            "members": {
                "Name": {"shape": "StringType"},
                "CreatedAt": {"shape": "Timestamp"}
            }
        }));
        shapes.insert("StringType".to_string(), json!({"type": "string"}));
        shapes.insert("Timestamp".to_string(), json!({"type": "timestamp"}));

        let mut value = json!({
            "Name": "test",
            "CreatedAt": 1672531200.0
        });
        normalize_response_value(&mut value, "Output", &shapes);
        assert_eq!(value["CreatedAt"], "2023-01-01T00:00:00+00:00");
        assert_eq!(value["Name"], "test");
    }

    #[test]
    fn test_normalize_timestamps_nested_list() {
        use serde_json::json;
        let mut shapes = HashMap::new();
        shapes.insert("Output".to_string(), json!({
            "type": "structure",
            "members": {
                "Items": {"shape": "ItemList"}
            }
        }));
        shapes.insert("ItemList".to_string(), json!({
            "type": "list",
            "member": {"shape": "Item"}
        }));
        shapes.insert("Item".to_string(), json!({
            "type": "structure",
            "members": {
                "CreatedAt": {"shape": "Timestamp"}
            }
        }));
        shapes.insert("Timestamp".to_string(), json!({"type": "timestamp"}));

        let mut value = json!({
            "Items": [
                {"CreatedAt": 1672531200.0},
                {"CreatedAt": 1672617600.0}
            ]
        });
        normalize_response_value(&mut value, "Output", &shapes);
        assert_eq!(value["Items"][0]["CreatedAt"], "2023-01-01T00:00:00+00:00");
        assert_eq!(value["Items"][1]["CreatedAt"], "2023-01-02T00:00:00+00:00");
    }

    #[test]
    fn test_normalize_timestamps_string_z_suffix() {
        use serde_json::json;
        let mut shapes = HashMap::new();
        shapes.insert("Output".to_string(), json!({
            "type": "structure",
            "members": {
                "ModifiedAt": {"shape": "Timestamp"}
            }
        }));
        shapes.insert("Timestamp".to_string(), json!({"type": "timestamp"}));

        let mut value = json!({"ModifiedAt": "2023-01-01T00:00:00.000Z"});
        normalize_response_value(&mut value, "Output", &shapes);
        assert_eq!(value["ModifiedAt"], "2023-01-01T00:00:00+00:00");
    }

    #[test]
    fn test_normalize_reorders_keys_to_model_order() {
        use serde_json::json;
        let mut shapes = HashMap::new();
        // Model defines members in order: UserId, Account, Arn
        shapes.insert("Output".to_string(), json!({
            "type": "structure",
            "members": {
                "UserId": {"shape": "StringType"},
                "Account": {"shape": "StringType"},
                "Arn": {"shape": "StringType"}
            }
        }));
        shapes.insert("StringType".to_string(), json!({"type": "string"}));

        // API response has keys in different order
        let mut value = json!({
            "Account": "123456789012",
            "Arn": "arn:aws:iam::123456789012:root",
            "UserId": "AIDAEXAMPLE"
        });
        normalize_response_value(&mut value, "Output", &shapes);

        // After normalization, keys should be in model order
        let keys: Vec<&String> = value.as_object().unwrap().keys().collect();
        assert_eq!(keys, vec!["UserId", "Account", "Arn"]);
    }

    #[test]
    fn test_normalize_reorders_nested_structures() {
        use serde_json::json;
        let mut shapes = HashMap::new();
        shapes.insert("Output".to_string(), json!({
            "type": "structure",
            "members": {
                "Items": {"shape": "ItemList"}
            }
        }));
        shapes.insert("ItemList".to_string(), json!({
            "type": "list",
            "member": {"shape": "Item"}
        }));
        // Model order: Name, CreatedAt, Id
        shapes.insert("Item".to_string(), json!({
            "type": "structure",
            "members": {
                "Name": {"shape": "StringType"},
                "CreatedAt": {"shape": "Timestamp"},
                "Id": {"shape": "StringType"}
            }
        }));
        shapes.insert("StringType".to_string(), json!({"type": "string"}));
        shapes.insert("Timestamp".to_string(), json!({"type": "timestamp"}));

        // Response has keys in different order
        let mut value = json!({
            "Items": [
                {"Id": "1", "CreatedAt": 1672531200.0, "Name": "test"}
            ]
        });
        normalize_response_value(&mut value, "Output", &shapes);

        let item = &value["Items"][0];
        let keys: Vec<&String> = item.as_object().unwrap().keys().collect();
        assert_eq!(keys, vec!["Name", "CreatedAt", "Id"]);
        assert_eq!(item["CreatedAt"], "2023-01-01T00:00:00+00:00");
    }

    #[test]
    fn test_normalize_null_list_to_empty_array() {
        use serde_json::json;
        let mut shapes = HashMap::new();
        shapes.insert("Output".to_string(), json!({
            "type": "structure",
            "members": {
                "Items": {"shape": "ItemList"},
                "Name": {"shape": "StringType"}
            }
        }));
        shapes.insert("ItemList".to_string(), json!({
            "type": "list",
            "member": {"shape": "StringType"}
        }));
        shapes.insert("StringType".to_string(), json!({"type": "string"}));

        let mut value = json!({
            "Items": null,
            "Name": "test"
        });
        normalize_response_value(&mut value, "Output", &shapes);
        assert_eq!(value["Items"], json!([]));
        assert_eq!(value["Name"], "test");
    }

    #[test]
    fn test_normalize_null_string_stays_null() {
        use serde_json::json;
        let mut shapes = HashMap::new();
        shapes.insert("Output".to_string(), json!({
            "type": "structure",
            "members": {
                "Name": {"shape": "StringType"}
            }
        }));
        shapes.insert("StringType".to_string(), json!({"type": "string"}));

        let mut value = json!({"Name": null});
        normalize_response_value(&mut value, "Output", &shapes);
        // Non-list null values should remain null (stripped later by strip_nulls)
        assert!(value["Name"].is_null());
    }

    #[test]
    fn test_normalize_scalar_to_list_wrapping() {
        // Some services return a bare string where a list is expected (e.g., API Gateway features)
        use serde_json::json;
        let mut shapes = HashMap::new();
        shapes.insert("Output".to_string(), json!({
            "type": "structure",
            "members": {
                "features": {"shape": "ListOfString"}
            }
        }));
        shapes.insert("ListOfString".to_string(), json!({
            "type": "list",
            "member": {"shape": "StringType"}
        }));
        shapes.insert("StringType".to_string(), json!({"type": "string"}));

        let mut value = json!({"features": "UsagePlans"});
        normalize_response_value(&mut value, "Output", &shapes);
        assert_eq!(value["features"], json!(["UsagePlans"]));
    }

    #[test]
    fn test_normalize_null_list_nested_in_structure() {
        // Ensure null-to-empty-array conversion works in nested structures
        use serde_json::json;
        let mut shapes = HashMap::new();
        shapes.insert("Output".to_string(), json!({
            "type": "structure",
            "members": {
                "Config": {"shape": "ConfigType"}
            }
        }));
        shapes.insert("ConfigType".to_string(), json!({
            "type": "structure",
            "members": {
                "Tags": {"shape": "TagList"}
            }
        }));
        shapes.insert("TagList".to_string(), json!({
            "type": "list",
            "member": {"shape": "StringType"}
        }));
        shapes.insert("StringType".to_string(), json!({"type": "string"}));

        let mut value = json!({"Config": {"Tags": null}});
        normalize_response_value(&mut value, "Output", &shapes);
        assert_eq!(value["Config"]["Tags"], json!([]));
    }

    #[test]
    fn test_normalize_existing_array_unchanged() {
        // Verify that a normal array is not altered
        use serde_json::json;
        let mut shapes = HashMap::new();
        shapes.insert("Output".to_string(), json!({
            "type": "structure",
            "members": {
                "Items": {"shape": "ItemList"}
            }
        }));
        shapes.insert("ItemList".to_string(), json!({
            "type": "list",
            "member": {"shape": "StringType"}
        }));
        shapes.insert("StringType".to_string(), json!({"type": "string"}));

        let mut value = json!({"Items": ["a", "b", "c"]});
        normalize_response_value(&mut value, "Output", &shapes);
        assert_eq!(value["Items"], json!(["a", "b", "c"]));
    }
}
