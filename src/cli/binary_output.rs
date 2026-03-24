use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;

/// Check if an operation's output shape has a streaming/binary payload.
/// Returns the member name of the payload field if found.
pub fn find_binary_payload_member(
    output_shape: &Value,
    shapes: &HashMap<String, Value>,
) -> Option<String> {
    // Check for "payload" trait in the output shape
    let payload = output_shape.get("payload")?.as_str()?;
    // Check if the payload member's shape is a blob type
    let members = output_shape.get("members")?.as_object()?;
    let member_def = members.get(payload)?;
    let shape_name = member_def.get("shape")?.as_str()?;
    let shape_def = shapes.get(shape_name)?;
    let shape_type = shape_def.get("type")?.as_str()?;
    if shape_type == "blob" {
        Some(payload.to_string())
    } else {
        None
    }
}

/// Write binary output to the appropriate destination.
/// If outfile is provided, write to that file. Otherwise write to stdout.
pub fn write_binary_output(data: &[u8], outfile: Option<&str>) -> Result<()> {
    if let Some(path) = outfile {
        let mut file = std::fs::File::create(path)
            .with_context(|| format!("Failed to create output file: {path}"))?;
        file.write_all(data)
            .with_context(|| format!("Failed to write to output file: {path}"))?;
    } else {
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        handle
            .write_all(data)
            .context("Failed to write binary output to stdout")?;
    }
    Ok(())
}

/// Extract base64-encoded binary data from JSON response and decode it.
pub fn extract_binary_data(response: &Value, payload_member: &str) -> Result<Vec<u8>> {
    let encoded = response
        .get(payload_member)
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Binary payload member '{}' not found in response",
                payload_member
            )
        })?;

    base64_decode(encoded)
        .with_context(|| format!("Failed to decode base64 payload for '{payload_member}'"))
}

/// Decode a base64 string using the standard alphabet (A-Za-z0-9+/) with = padding.
/// Returns the decoded bytes, or an error if the input contains invalid characters
/// or has incorrect padding.
fn base64_decode(input: &str) -> Result<Vec<u8>> {
    // Strip whitespace (base64 may contain linebreaks)
    let cleaned: String = input.chars().filter(|c| !c.is_ascii_whitespace()).collect();

    if cleaned.is_empty() {
        return Ok(Vec::new());
    }

    // Validate length: base64 encoded data length must be a multiple of 4
    if cleaned.len() % 4 != 0 {
        bail!(
            "Invalid base64 input: length {} is not a multiple of 4",
            cleaned.len()
        );
    }

    // Count padding
    let padding = cleaned.chars().rev().take_while(|&c| c == '=').count();
    if padding > 2 {
        bail!("Invalid base64 input: more than 2 padding characters");
    }

    let mut output = Vec::with_capacity(cleaned.len() * 3 / 4);

    // Process 4-character blocks
    let chars: Vec<u8> = cleaned.bytes().collect();
    for chunk in chars.chunks(4) {
        let mut sextet = [0u8; 4];
        let mut pad_count = 0;

        for (i, &byte) in chunk.iter().enumerate() {
            if byte == b'=' {
                sextet[i] = 0;
                pad_count += 1;
            } else {
                sextet[i] = decode_base64_char(byte)?;
            }
        }

        // Combine 4 sextets (6 bits each) into 3 bytes (8 bits each)
        let combined: u32 = (u32::from(sextet[0]) << 18)
            | (u32::from(sextet[1]) << 12)
            | (u32::from(sextet[2]) << 6)
            | u32::from(sextet[3]);

        output.push((combined >> 16) as u8);
        if pad_count < 2 {
            output.push((combined >> 8) as u8);
        }
        if pad_count < 1 {
            output.push(combined as u8);
        }
    }

    Ok(output)
}

/// Decode a single base64 character to its 6-bit value.
fn decode_base64_char(c: u8) -> Result<u8> {
    match c {
        b'A'..=b'Z' => Ok(c - b'A'),
        b'a'..=b'z' => Ok(c - b'a' + 26),
        b'0'..=b'9' => Ok(c - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => bail!("Invalid base64 character: {:?}", char::from(c)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -------------------------------------------------------
    // Tests for find_binary_payload_member
    // -------------------------------------------------------

    #[test]
    fn find_binary_payload_member_with_blob_payload() {
        let output_shape = json!({
            "type": "structure",
            "payload": "Body",
            "members": {
                "Body": {
                    "shape": "BlobBody"
                },
                "ContentType": {
                    "shape": "StringType"
                }
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert(
            "BlobBody".to_string(),
            json!({"type": "blob"}),
        );
        shapes.insert(
            "StringType".to_string(),
            json!({"type": "string"}),
        );

        let result = find_binary_payload_member(&output_shape, &shapes);
        assert_eq!(result, Some("Body".to_string()));
    }

    #[test]
    fn find_binary_payload_member_with_non_blob_payload() {
        let output_shape = json!({
            "type": "structure",
            "payload": "Data",
            "members": {
                "Data": {
                    "shape": "StringPayload"
                }
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert(
            "StringPayload".to_string(),
            json!({"type": "string"}),
        );

        let result = find_binary_payload_member(&output_shape, &shapes);
        assert_eq!(result, None);
    }

    #[test]
    fn find_binary_payload_member_with_no_payload_trait() {
        let output_shape = json!({
            "type": "structure",
            "members": {
                "Body": {
                    "shape": "BlobBody"
                }
            }
        });
        let mut shapes = HashMap::new();
        shapes.insert(
            "BlobBody".to_string(),
            json!({"type": "blob"}),
        );

        let result = find_binary_payload_member(&output_shape, &shapes);
        assert_eq!(result, None);
    }

    #[test]
    fn find_binary_payload_member_with_missing_shape() {
        let output_shape = json!({
            "type": "structure",
            "payload": "Body",
            "members": {
                "Body": {
                    "shape": "NonExistentShape"
                }
            }
        });
        let shapes = HashMap::new();

        let result = find_binary_payload_member(&output_shape, &shapes);
        assert_eq!(result, None);
    }

    // -------------------------------------------------------
    // Tests for write_binary_output
    // -------------------------------------------------------

    #[test]
    fn write_binary_output_to_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.bin");
        let data = b"hello binary world";

        write_binary_output(data, Some(path.to_str().unwrap())).unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, data);
    }

    #[test]
    fn write_binary_output_overwrites_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.bin");

        // Write initial content
        std::fs::write(&path, b"old data").unwrap();

        // Overwrite with new content
        let new_data = b"new binary data";
        write_binary_output(new_data, Some(path.to_str().unwrap())).unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, new_data.as_slice());
    }

    // -------------------------------------------------------
    // Tests for extract_binary_data
    // -------------------------------------------------------

    #[test]
    fn extract_binary_data_with_valid_base64() {
        // "SGVsbG8=" is base64 for "Hello"
        let response = json!({
            "Plaintext": "SGVsbG8="
        });

        let result = extract_binary_data(&response, "Plaintext").unwrap();
        assert_eq!(result, b"Hello");
    }

    #[test]
    fn extract_binary_data_with_missing_member() {
        let response = json!({
            "Other": "value"
        });

        let result = extract_binary_data(&response, "Plaintext");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Binary payload member 'Plaintext' not found"),
            "Unexpected error: {err_msg}"
        );
    }

    // -------------------------------------------------------
    // Tests for base64_decode
    // -------------------------------------------------------

    #[test]
    fn base64_decode_basic_string() {
        // "QUJD" is base64 for "ABC"
        let result = base64_decode("QUJD").unwrap();
        assert_eq!(result, b"ABC");
    }

    #[test]
    fn base64_decode_with_padding() {
        // "QQ==" is base64 for "A" (two padding chars)
        let result = base64_decode("QQ==").unwrap();
        assert_eq!(result, b"A");

        // "QUI=" is base64 for "AB" (one padding char)
        let result = base64_decode("QUI=").unwrap();
        assert_eq!(result, b"AB");
    }

    #[test]
    fn base64_decode_empty_string() {
        let result = base64_decode("").unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn base64_decode_hello_world() {
        // "SGVsbG8gV29ybGQ=" is base64 for "Hello World"
        let result = base64_decode("SGVsbG8gV29ybGQ=").unwrap();
        assert_eq!(result, b"Hello World");
    }
}
