use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;

/// Check if an operation's output shape has a streaming payload member.
/// Returns (member_name, is_event_stream) if found.
pub fn find_streaming_member(
    output_shape: &Value,
    shapes: &HashMap<String, Value>,
) -> Option<(String, bool)> {
    // Check for "payload" trait on the output shape
    let payload_name = output_shape.get("payload")?.as_str()?;
    let members = output_shape.get("members")?.as_object()?;
    let member = members.get(payload_name)?;

    // Check if the member has streaming trait or its shape has streaming trait
    let has_streaming = member
        .get("streaming")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let shape_name = member.get("shape")?.as_str()?;
    let shape_def = shapes.get(shape_name)?;
    let shape_streaming = shape_def
        .get("streaming")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_event_stream = shape_def
        .get("eventstream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if has_streaming || shape_streaming {
        Some((payload_name.to_string(), is_event_stream))
    } else {
        None
    }
}

/// Determine if a response should be streamed based on the output shape.
pub fn is_streaming_operation(
    output_shape: &Value,
    shapes: &HashMap<String, Value>,
) -> bool {
    find_streaming_member(output_shape, shapes).is_some()
}

/// Write streaming response data to the given writer.
/// For non-event-stream responses, this writes the raw bytes.
/// For event-stream responses, this writes events as they arrive (simplified).
pub fn write_streaming_response<W: Write>(
    data: &[u8],
    writer: &mut W,
    _is_event_stream: bool,
) -> Result<()> {
    writer
        .write_all(data)
        .context("Failed to write streaming response")?;
    writer
        .flush()
        .context("Failed to flush streaming output")?;
    Ok(())
}

/// Build a response Value for operations with streaming payloads.
/// The streaming payload is excluded from the JSON response, and metadata
/// (like ContentType, ContentLength) is preserved.
pub fn build_metadata_response(full_response: &Value, streaming_member: &str) -> Value {
    match full_response {
        Value::Object(map) => {
            let mut result = serde_json::Map::new();
            for (key, val) in map {
                if key != streaming_member {
                    result.insert(key.clone(), val.clone());
                }
            }
            Value::Object(result)
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -------------------------------------------------------
    // Tests for find_streaming_member
    // -------------------------------------------------------

    #[test]
    fn find_streaming_member_with_streaming_blob() {
        let output_shape = json!({
            "type": "structure",
            "payload": "Body",
            "members": {
                "Body": {
                    "shape": "StreamingBlob",
                    "streaming": true
                },
                "ContentType": {
                    "shape": "StringType"
                }
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert("StreamingBlob".to_string(), json!({"type": "blob"}));
        shapes.insert("StringType".to_string(), json!({"type": "string"}));

        let result = find_streaming_member(&output_shape, &shapes);
        assert_eq!(result, Some(("Body".to_string(), false)));
    }

    #[test]
    fn find_streaming_member_with_non_streaming_member() {
        let output_shape = json!({
            "type": "structure",
            "payload": "Data",
            "members": {
                "Data": {
                    "shape": "RegularBlob"
                }
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert("RegularBlob".to_string(), json!({"type": "blob"}));

        let result = find_streaming_member(&output_shape, &shapes);
        assert_eq!(result, None);
    }

    #[test]
    fn find_streaming_member_with_no_payload() {
        let output_shape = json!({
            "type": "structure",
            "members": {
                "Body": {
                    "shape": "StreamingBlob",
                    "streaming": true
                }
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert(
            "StreamingBlob".to_string(),
            json!({"type": "blob", "streaming": true}),
        );

        let result = find_streaming_member(&output_shape, &shapes);
        assert_eq!(result, None);
    }

    #[test]
    fn find_streaming_member_with_eventstream() {
        let output_shape = json!({
            "type": "structure",
            "payload": "EventStream",
            "members": {
                "EventStream": {
                    "shape": "MyEventStream",
                    "streaming": true
                }
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert(
            "MyEventStream".to_string(),
            json!({"type": "structure", "eventstream": true}),
        );

        let result = find_streaming_member(&output_shape, &shapes);
        assert_eq!(result, Some(("EventStream".to_string(), true)));
    }

    #[test]
    fn find_streaming_member_with_shape_level_streaming_trait() {
        let output_shape = json!({
            "type": "structure",
            "payload": "Body",
            "members": {
                "Body": {
                    "shape": "StreamingBlob"
                },
                "ContentLength": {
                    "shape": "Long"
                }
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert(
            "StreamingBlob".to_string(),
            json!({"type": "blob", "streaming": true}),
        );
        shapes.insert("Long".to_string(), json!({"type": "long"}));

        let result = find_streaming_member(&output_shape, &shapes);
        assert_eq!(result, Some(("Body".to_string(), false)));
    }

    // -------------------------------------------------------
    // Tests for is_streaming_operation
    // -------------------------------------------------------

    #[test]
    fn is_streaming_operation_true() {
        let output_shape = json!({
            "type": "structure",
            "payload": "Body",
            "members": {
                "Body": {
                    "shape": "StreamingBlob",
                    "streaming": true
                }
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert("StreamingBlob".to_string(), json!({"type": "blob"}));

        assert!(is_streaming_operation(&output_shape, &shapes));
    }

    #[test]
    fn is_streaming_operation_false() {
        let output_shape = json!({
            "type": "structure",
            "members": {
                "Status": {
                    "shape": "StringType"
                }
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert("StringType".to_string(), json!({"type": "string"}));

        assert!(!is_streaming_operation(&output_shape, &shapes));
    }

    // -------------------------------------------------------
    // Tests for write_streaming_response
    // -------------------------------------------------------

    #[test]
    fn write_streaming_response_writes_bytes() {
        let data = b"Hello, streaming world!";
        let mut buffer: Vec<u8> = Vec::new();

        write_streaming_response(data, &mut buffer, false).unwrap();

        assert_eq!(buffer, data);
    }

    #[test]
    fn write_streaming_response_flushes() {
        // Use a BufWriter with a large buffer to verify flush behavior.
        // After write_streaming_response, the data should be flushed to the inner writer.
        let inner: Vec<u8> = Vec::new();
        let mut writer = std::io::BufWriter::with_capacity(8192, inner);

        let data = b"small payload";
        write_streaming_response(data, &mut writer, false).unwrap();

        // After flush, get_ref gives us the inner Vec which should have the data
        assert_eq!(writer.get_ref().as_slice(), data);
    }

    // -------------------------------------------------------
    // Tests for build_metadata_response
    // -------------------------------------------------------

    #[test]
    fn build_metadata_response_removes_streaming_member() {
        let response = json!({
            "Body": "base64encodeddata",
            "ContentType": "application/octet-stream",
            "ContentLength": 1024
        });

        let result = build_metadata_response(&response, "Body");
        assert!(result.get("Body").is_none());
    }

    #[test]
    fn build_metadata_response_preserves_other_fields() {
        let response = json!({
            "Body": "base64encodeddata",
            "ContentType": "application/octet-stream",
            "ContentLength": 1024,
            "ETag": "\"abc123\""
        });

        let result = build_metadata_response(&response, "Body");
        assert_eq!(
            result.get("ContentType").and_then(|v| v.as_str()),
            Some("application/octet-stream")
        );
        assert_eq!(
            result.get("ContentLength").and_then(|v| v.as_i64()),
            Some(1024)
        );
        assert_eq!(
            result.get("ETag").and_then(|v| v.as_str()),
            Some("\"abc123\"")
        );
    }

    #[test]
    fn build_metadata_response_with_empty_object() {
        let response = json!({});

        let result = build_metadata_response(&response, "Body");
        assert_eq!(result, json!({}));
    }

    #[test]
    fn build_metadata_response_with_non_object_value() {
        let response = json!("just a string");

        let result = build_metadata_response(&response, "Body");
        assert_eq!(result, json!("just a string"));
    }
}
