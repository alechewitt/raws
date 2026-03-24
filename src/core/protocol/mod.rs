pub mod json;
pub mod query;
pub mod rest_json;
pub mod rest_xml;

use chrono::{DateTime, Utc};

/// Normalize a timestamp string to the format used by the AWS CLI.
/// AWS CLI outputs timestamps as ISO 8601 with +00:00 offset (no Z suffix),
/// and omits sub-second precision when it's zero.
///
/// Examples:
///   "2023-01-01T00:00:00.000Z" -> "2023-01-01T00:00:00+00:00"
///   "2023-01-01T00:00:00Z" -> "2023-01-01T00:00:00+00:00"
///   "2023-01-01T00:00:00.123Z" -> "2023-01-01T00:00:00.123000+00:00"
pub fn normalize_timestamp(s: &str) -> String {
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        let nanos = dt.timestamp_subsec_nanos();
        if nanos == 0 {
            dt.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()
        } else {
            // Python's isoformat uses 6-digit microseconds
            let micros = nanos / 1000;
            dt.format("%Y-%m-%dT%H:%M:%S").to_string()
                + &format!(".{micros:06}+00:00")
        }
    } else {
        // Can't parse; return as-is
        s.to_string()
    }
}

/// Convert an epoch timestamp (seconds, possibly with fractional part) to
/// the AWS CLI ISO 8601 format.
pub fn epoch_to_iso(epoch: f64) -> String {
    let secs = epoch as i64;
    // Round to nearest microsecond to avoid floating-point precision issues
    let frac = epoch - secs as f64;
    let micros = (frac * 1_000_000.0).round() as u32;
    let nanos = micros * 1000;
    if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
        if micros == 0 {
            dt.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()
        } else {
            dt.format("%Y-%m-%dT%H:%M:%S").to_string()
                + &format!(".{micros:06}+00:00")
        }
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
        assert_eq!(
            normalize_timestamp("2023-01-01T00:00:00Z"),
            "2023-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn test_normalize_timestamp_z_with_millis() {
        assert_eq!(
            normalize_timestamp("2026-03-17T21:03:46.000Z"),
            "2026-03-17T21:03:46+00:00"
        );
    }

    #[test]
    fn test_normalize_timestamp_nonzero_millis() {
        assert_eq!(
            normalize_timestamp("2023-01-01T00:00:00.123Z"),
            "2023-01-01T00:00:00.123000+00:00"
        );
    }

    #[test]
    fn test_normalize_timestamp_already_offset() {
        assert_eq!(
            normalize_timestamp("2023-01-01T00:00:00+00:00"),
            "2023-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn test_normalize_timestamp_unparseable() {
        assert_eq!(normalize_timestamp("not-a-date"), "not-a-date");
    }

    #[test]
    fn test_epoch_to_iso() {
        assert_eq!(
            epoch_to_iso(1672531200.0),
            "2023-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn test_epoch_to_iso_with_fraction() {
        assert_eq!(
            epoch_to_iso(1672531200.123),
            "2023-01-01T00:00:00.123000+00:00"
        );
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
}
