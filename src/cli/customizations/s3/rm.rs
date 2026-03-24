//! S3 `rm` high-level command implementation.
//!
//! Handles:
//! - `raws s3 rm s3://bucket/key` - delete a single object
//! - `raws s3 rm s3://bucket/prefix --recursive` - delete all objects with prefix

use anyhow::{bail, Result};

use super::cp;
use super::mv;
use super::parse_s3_url;
use super::s3_bucket_api_call;
use super::S3CommandContext;

/// Execute the `s3 rm` command.
///
/// Deletes one or more S3 objects. With `--recursive`, deletes all objects
/// matching the given prefix.
pub async fn execute(args: &[String], ctx: &S3CommandContext) -> Result<()> {
    let recursive = args.iter().any(|a| a == "--recursive");

    // Find the positional S3 URL argument
    let positional: Vec<&String> = args.iter().filter(|a| !a.starts_with('-')).collect();

    if positional.is_empty() {
        bail!(
            "s3 rm requires an S3 URL argument.\n\
             Usage: raws s3 rm s3://bucket/key [--recursive]"
        );
    }

    let s3_url = positional[0].as_str();
    let (bucket, key) = parse_s3_url(s3_url)?;

    if recursive {
        // Delete all objects with the given prefix
        delete_recursive(ctx, &bucket, &key).await
    } else {
        if key.is_empty() {
            bail!(
                "S3 URL must include a key for non-recursive delete.\n\
                 Use --recursive to delete all objects in a bucket."
            );
        }
        delete_single(ctx, &bucket, &key).await
    }
}

/// Delete a single S3 object.
async fn delete_single(
    ctx: &S3CommandContext,
    bucket: &str,
    key: &str,
) -> Result<()> {
    mv::delete_s3_object(ctx, bucket, key).await?;
    println!("delete: s3://{}/{}", bucket, key);
    Ok(())
}

/// Delete all S3 objects matching a prefix (recursive delete).
///
/// Uses ListObjectsV2 with pagination to discover all matching keys,
/// then deletes each one individually.
pub(super) async fn delete_recursive(
    ctx: &S3CommandContext,
    bucket: &str,
    prefix: &str,
) -> Result<()> {
    let mut continuation_token: Option<String> = None;

    loop {
        // Build query parameters for ListObjectsV2
        let mut params: Vec<(&str, &str)> = vec![("list-type", "2")];

        if !prefix.is_empty() {
            params.push(("prefix", prefix));
        }

        let token_string;
        if let Some(ref token) = continuation_token {
            token_string = token.clone();
            params.push(("continuation-token", &token_string));
        }

        let response =
            s3_bucket_api_call(ctx, bucket, "GET", "/", &params, None, &[]).await?;

        if response.status >= 300 {
            let body = response.body_string();
            bail!(
                "ListObjectsV2 failed (HTTP {}): {}",
                response.status,
                cp::extract_s3_error(&body)
            );
        }

        let body = response.body_string();
        let page = parse_list_keys(&body)?;

        // Delete each object in this page
        for key in &page.keys {
            mv::delete_s3_object(ctx, bucket, key).await?;
            println!("delete: s3://{}/{}", bucket, key);
        }

        // Handle pagination
        if page.is_truncated {
            if let Some(token) = page.next_continuation_token {
                continuation_token = Some(token);
            } else {
                break;
            }
        } else {
            break;
        }
    }

    Ok(())
}

/// Minimal parsed result of a ListObjectsV2 response for rm purposes.
struct ListKeysPage {
    keys: Vec<String>,
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

/// Parse a ListObjectsV2 XML response, extracting only the keys and pagination info.
fn parse_list_keys(xml_body: &str) -> Result<ListKeysPage> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml_body);
    let mut keys = Vec::new();
    let mut is_truncated = false;
    let mut next_continuation_token: Option<String> = None;

    let mut in_contents = false;
    let mut current_tag = String::new();
    let mut top_level_tag = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let tag = strip_ns(std::str::from_utf8(e.name().as_ref()).unwrap_or(""));
                match tag.as_str() {
                    "Contents" => {
                        in_contents = true;
                    }
                    _ => {
                        if in_contents {
                            current_tag = tag;
                        } else {
                            top_level_tag = tag;
                        }
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let tag = strip_ns(std::str::from_utf8(e.name().as_ref()).unwrap_or(""));
                if tag == "Contents" {
                    in_contents = false;
                }
                current_tag.clear();
                top_level_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let text = e.unescape().unwrap_or_default().to_string();
                if in_contents && current_tag == "Key" {
                    keys.push(text);
                } else if !in_contents {
                    match top_level_tag.as_str() {
                        "IsTruncated" => {
                            is_truncated = text == "true";
                        }
                        "NextContinuationToken" => {
                            next_continuation_token = Some(text);
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => bail!("Failed to parse ListObjectsV2 XML response: {}", e),
        }
    }

    Ok(ListKeysPage {
        keys,
        is_truncated,
        next_continuation_token,
    })
}

/// Strip XML namespace prefix from a tag name.
fn strip_ns(tag: &str) -> String {
    if let Some(pos) = tag.rfind('}') {
        return tag[pos + 1..].to_string();
    }
    if let Some(pos) = tag.rfind(':') {
        return tag[pos + 1..].to_string();
    }
    tag.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::credentials::Credentials;

    /// Helper to create a dummy S3CommandContext for testing.
    fn dummy_context() -> S3CommandContext {
        S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,

            no_sign_request: false,
        }
    }

    // ---------------------------------------------------------------
    // Argument validation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_rm_no_args_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(execute(&[], &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("s3 rm requires an S3 URL argument"),
        );
    }

    #[test]
    fn test_rm_no_key_without_recursive_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec!["s3://bucket".to_string()];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("S3 URL must include a key for non-recursive delete"),
        );
    }

    // ---------------------------------------------------------------
    // parse_list_keys tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_list_keys_typical() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>photos/cat.jpg</Key>
    <LastModified>2023-01-15T10:30:45.000Z</LastModified>
    <Size>12345</Size>
  </Contents>
  <Contents>
    <Key>photos/dog.jpg</Key>
    <LastModified>2023-06-20T14:22:10.000Z</LastModified>
    <Size>678</Size>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_keys(xml).unwrap();
        assert_eq!(page.keys.len(), 2);
        assert_eq!(page.keys[0], "photos/cat.jpg");
        assert_eq!(page.keys[1], "photos/dog.jpg");
        assert!(!page.is_truncated);
        assert!(page.next_continuation_token.is_none());
    }

    #[test]
    fn test_parse_list_keys_truncated() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>abc123token</NextContinuationToken>
  <Contents>
    <Key>file1.txt</Key>
    <LastModified>2023-01-15T10:30:45.000Z</LastModified>
    <Size>100</Size>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_keys(xml).unwrap();
        assert!(page.is_truncated);
        assert_eq!(page.next_continuation_token, Some("abc123token".to_string()));
        assert_eq!(page.keys.len(), 1);
        assert_eq!(page.keys[0], "file1.txt");
    }

    #[test]
    fn test_parse_list_keys_empty() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>"#;

        let page = parse_list_keys(xml).unwrap();
        assert_eq!(page.keys.len(), 0);
        assert!(!page.is_truncated);
    }

    #[test]
    fn test_parse_list_keys_with_namespace() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>test.txt</Key>
    <LastModified>2023-01-01T00:00:00.000Z</LastModified>
    <Size>42</Size>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_keys(xml).unwrap();
        assert_eq!(page.keys.len(), 1);
        assert_eq!(page.keys[0], "test.txt");
    }

    // ---------------------------------------------------------------
    // strip_ns tests
    // ---------------------------------------------------------------

    #[test]
    fn test_strip_ns_with_braces() {
        assert_eq!(
            strip_ns("{http://s3.amazonaws.com/doc/2006-03-01/}Key"),
            "Key"
        );
    }

    #[test]
    fn test_strip_ns_no_namespace() {
        assert_eq!(strip_ns("Key"), "Key");
    }
}
