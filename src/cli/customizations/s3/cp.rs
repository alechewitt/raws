//! S3 `cp` high-level command implementation.
//!
//! Handles `raws s3 cp <source> <destination>`:
//! - Local file -> S3 (upload): `raws s3 cp file.txt s3://bucket/key`
//! - S3 -> local file (download): `raws s3 cp s3://bucket/key local-file`
//! - S3 -> S3 (copy): `raws s3 cp s3://src-bucket/key s3://dst-bucket/key`

use std::path::Path;

use anyhow::{bail, Context, Result};

use super::is_s3_url;
use super::parse_s3_url;
use super::s3_bucket_api_call;
use super::S3CommandContext;

/// Execute the `s3 cp` command.
///
/// Parses the source and destination arguments, determines the transfer direction,
/// and dispatches to the appropriate handler.
pub async fn execute(args: &[String], ctx: &S3CommandContext) -> Result<()> {
    // Filter out flags to find the two positional arguments (source and destination)
    let positional: Vec<&String> = args.iter().filter(|a| !a.starts_with('-')).collect();

    if positional.len() < 2 {
        bail!(
            "s3 cp requires a source and destination argument.\n\
             Usage: raws s3 cp <source> <destination>\n\
             Example: raws s3 cp file.txt s3://bucket/key"
        );
    }

    let source = positional[0].as_str();
    let destination = positional[1].as_str();

    let src_is_s3 = is_s3_url(source);
    let dst_is_s3 = is_s3_url(destination);

    match (src_is_s3, dst_is_s3) {
        (false, true) => {
            // Local -> S3 upload
            upload_file(source, destination, ctx).await
        }
        (true, false) => {
            // S3 -> local download
            download_file(source, destination, ctx).await
        }
        (true, true) => {
            // S3 -> S3 copy
            copy_s3_to_s3(source, destination, ctx).await
        }
        (false, false) => {
            bail!(
                "At least one of the source or destination must be an S3 URL (s3://...)"
            )
        }
    }
}

/// Upload a local file to S3 using PutObject.
///
/// Reads the entire file into memory and sends it as a PUT request to the
/// appropriate S3 bucket endpoint with virtual-hosted style addressing.
pub(super) async fn upload_file(local_path: &str, s3_url: &str, ctx: &S3CommandContext) -> Result<()> {
    let (bucket, key) = parse_s3_url(s3_url)?;

    if key.is_empty() {
        bail!(
            "S3 destination must include a key (e.g., s3://bucket/key), got: {}",
            s3_url
        );
    }

    // Read the local file
    let path = Path::new(local_path);
    let file_content = std::fs::read(path)
        .with_context(|| format!("Failed to read file: {}", local_path))?;

    let content_length = file_content.len().to_string();

    if ctx.debug {
        eprintln!(
            "[debug] cp upload: {} ({} bytes) -> s3://{}/{}",
            local_path,
            file_content.len(),
            bucket,
            key
        );
    }

    // Build the URI path: /{key}
    // The key needs to have each segment URI-encoded for the path, but we
    // pass it as a raw path and let the signing handle encoding.
    let uri_path = format!("/{key}");

    // Extra headers for PutObject
    let extra_headers: Vec<(&str, &str)> = vec![
        ("content-length", &content_length),
    ];

    // Optionally detect content type from file extension
    let content_type = guess_content_type(local_path);
    let mut headers_with_type: Vec<(&str, &str)> = extra_headers;
    if let Some(ref ct) = content_type {
        headers_with_type.push(("content-type", ct));
    }

    let response = s3_bucket_api_call(
        ctx,
        &bucket,
        "PUT",
        &uri_path,
        &[],
        Some(&file_content),
        &headers_with_type,
    )
    .await?;

    if response.status >= 300 {
        let body = response.body_string();
        bail!(
            "PutObject failed (HTTP {}): {}",
            response.status,
            extract_s3_error(&body)
        );
    }

    // Print success message matching AWS CLI format
    println!("upload: {} to s3://{}/{}", local_path, bucket, key);

    Ok(())
}

/// Download a file from S3 using GetObject.
///
/// Sends a GET request to the S3 bucket endpoint and writes the response body
/// to the local file path. If `local_path` is an existing directory, the
/// filename portion of the S3 key is used as the output file name.
pub(super) async fn download_file(s3_url: &str, local_path: &str, ctx: &S3CommandContext) -> Result<()> {
    let (bucket, key) = parse_s3_url(s3_url)?;

    if key.is_empty() {
        bail!(
            "S3 source must include a key (e.g., s3://bucket/key), got: {}",
            s3_url
        );
    }

    // If the local path is an existing directory, append the key's filename
    let output_path = {
        let p = Path::new(local_path);
        if p.is_dir() {
            let filename = Path::new(&key)
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("S3 key '{}' has no filename component", key))?;
            p.join(filename)
        } else {
            p.to_path_buf()
        }
    };

    if ctx.debug {
        eprintln!(
            "[debug] cp download: s3://{}/{} -> {}",
            bucket,
            key,
            output_path.display()
        );
    }

    // Build the URI path: /{key}
    let uri_path = format!("/{key}");

    let response = s3_bucket_api_call(
        ctx,
        &bucket,
        "GET",
        &uri_path,
        &[],
        None,
        &[],
    )
    .await?;

    if response.status >= 300 {
        let body = response.body_string();
        bail!(
            "GetObject failed (HTTP {}): {}",
            response.status,
            extract_s3_error(&body)
        );
    }

    // Write the response body to the local file
    std::fs::write(&output_path, &response.body)
        .with_context(|| format!("Failed to write file: {}", output_path.display()))?;

    // Print success message matching AWS CLI format
    println!("download: s3://{}/{} to {}", bucket, key, output_path.display());

    Ok(())
}

/// Copy an object from one S3 location to another using server-side CopyObject.
///
/// Sends a PUT request to the destination bucket with the `x-amz-copy-source`
/// header pointing to the source bucket/key. The copy happens entirely
/// server-side - no data is transferred through the client.
pub(super) async fn copy_s3_to_s3(
    src_url: &str,
    dst_url: &str,
    ctx: &S3CommandContext,
) -> Result<()> {
    let (src_bucket, src_key) = parse_s3_url(src_url)?;
    let (dst_bucket, dst_key) = parse_s3_url(dst_url)?;

    if src_key.is_empty() {
        bail!(
            "S3 source must include a key (e.g., s3://bucket/key), got: {}",
            src_url
        );
    }

    if dst_key.is_empty() {
        bail!(
            "S3 destination must include a key (e.g., s3://bucket/key), got: {}",
            dst_url
        );
    }

    if ctx.debug {
        eprintln!(
            "[debug] cp s3-to-s3: s3://{}/{} -> s3://{}/{}",
            src_bucket, src_key, dst_bucket, dst_key
        );
    }

    // Build the URI path for the destination: /{dst_key}
    let uri_path = format!("/{dst_key}");

    // Build the copy source header: /{src_bucket}/{src_key}
    // The key must be URL-encoded for the header value
    let copy_source = format!(
        "/{}/{}",
        src_bucket,
        url_encode_copy_source(&src_key)
    );

    let extra_headers: Vec<(&str, &str)> = vec![
        ("x-amz-copy-source", &copy_source),
    ];

    let response = s3_bucket_api_call(
        ctx,
        &dst_bucket,
        "PUT",
        &uri_path,
        &[],
        None,
        &extra_headers,
    )
    .await?;

    if response.status >= 300 {
        let body = response.body_string();
        bail!(
            "CopyObject failed (HTTP {}): {}",
            response.status,
            extract_s3_error(&body)
        );
    }

    // Print success message matching AWS CLI format
    println!(
        "copy: s3://{}/{} to s3://{}/{}",
        src_bucket, src_key, dst_bucket, dst_key
    );

    Ok(())
}

/// URL-encode the key portion of an S3 copy source header value.
///
/// Encodes characters that are not unreserved per RFC 3986, but preserves
/// forward slashes since they are part of the key path structure.
fn url_encode_copy_source(key: &str) -> String {
    use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
    const ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~')
        .remove(b'/');
    utf8_percent_encode(key, ENCODE_SET).to_string()
}

/// Guess the Content-Type of a file based on its extension.
///
/// Returns a MIME type string for common file types, or None if unknown.
fn guess_content_type(path: &str) -> Option<String> {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase())?;

    let mime = match ext.as_str() {
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" => "application/javascript",
        "json" => "application/json",
        "xml" => "application/xml",
        "txt" => "text/plain",
        "csv" => "text/csv",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" | "gzip" => "application/gzip",
        "tar" => "application/x-tar",
        "mp3" => "audio/mpeg",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "wasm" => "application/wasm",
        _ => return None,
    };

    Some(mime.to_string())
}

/// Extract an error message from an S3 XML error response.
pub(super) fn extract_s3_error(body: &str) -> String {
    // Try to find <Message>...</Message> in the error XML
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(body);
    let mut in_message = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let tag_name = std::str::from_utf8(e.name().as_ref())
                    .unwrap_or("")
                    .to_string();
                let tag = match tag_name.rfind('}') {
                    Some(p) => &tag_name[p + 1..],
                    None => &tag_name,
                };
                if tag == "Message" {
                    in_message = true;
                }
            }
            Ok(Event::Text(ref e)) => {
                if in_message {
                    return e.unescape().unwrap_or_default().to_string();
                }
            }
            Ok(Event::End(_)) => {
                if in_message {
                    return String::new();
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
    }

    // Fallback: return the raw body (truncated)
    if body.len() > 200 {
        format!("{}...", &body[..200])
    } else {
        body.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // guess_content_type tests
    // ---------------------------------------------------------------

    #[test]
    fn test_guess_content_type_html() {
        assert_eq!(guess_content_type("index.html"), Some("text/html".to_string()));
    }

    #[test]
    fn test_guess_content_type_htm() {
        assert_eq!(guess_content_type("page.htm"), Some("text/html".to_string()));
    }

    #[test]
    fn test_guess_content_type_json() {
        assert_eq!(
            guess_content_type("data.json"),
            Some("application/json".to_string())
        );
    }

    #[test]
    fn test_guess_content_type_png() {
        assert_eq!(guess_content_type("photo.png"), Some("image/png".to_string()));
    }

    #[test]
    fn test_guess_content_type_jpeg() {
        assert_eq!(
            guess_content_type("photo.jpeg"),
            Some("image/jpeg".to_string())
        );
    }

    #[test]
    fn test_guess_content_type_jpg() {
        assert_eq!(
            guess_content_type("photo.jpg"),
            Some("image/jpeg".to_string())
        );
    }

    #[test]
    fn test_guess_content_type_txt() {
        assert_eq!(
            guess_content_type("readme.txt"),
            Some("text/plain".to_string())
        );
    }

    #[test]
    fn test_guess_content_type_unknown() {
        assert_eq!(guess_content_type("file.xyz"), None);
    }

    #[test]
    fn test_guess_content_type_no_extension() {
        assert_eq!(guess_content_type("Makefile"), None);
    }

    #[test]
    fn test_guess_content_type_case_insensitive() {
        assert_eq!(guess_content_type("file.JSON"), Some("application/json".to_string()));
        assert_eq!(guess_content_type("file.PNG"), Some("image/png".to_string()));
    }

    #[test]
    fn test_guess_content_type_pdf() {
        assert_eq!(
            guess_content_type("doc.pdf"),
            Some("application/pdf".to_string())
        );
    }

    #[test]
    fn test_guess_content_type_zip() {
        assert_eq!(
            guess_content_type("archive.zip"),
            Some("application/zip".to_string())
        );
    }

    #[test]
    fn test_guess_content_type_gz() {
        assert_eq!(
            guess_content_type("file.gz"),
            Some("application/gzip".to_string())
        );
    }

    #[test]
    fn test_guess_content_type_css() {
        assert_eq!(guess_content_type("style.css"), Some("text/css".to_string()));
    }

    #[test]
    fn test_guess_content_type_js() {
        assert_eq!(
            guess_content_type("app.js"),
            Some("application/javascript".to_string())
        );
    }

    // ---------------------------------------------------------------
    // extract_s3_error tests
    // ---------------------------------------------------------------

    #[test]
    fn test_extract_s3_error_typical() {
        let xml = r#"<Error>
  <Code>AccessDenied</Code>
  <Message>Access Denied</Message>
</Error>"#;
        assert_eq!(extract_s3_error(xml), "Access Denied");
    }

    #[test]
    fn test_extract_s3_error_no_xml() {
        assert_eq!(extract_s3_error("plain text error"), "plain text error");
    }

    #[test]
    fn test_extract_s3_error_empty() {
        assert_eq!(extract_s3_error(""), "");
    }

    #[test]
    fn test_extract_s3_error_with_namespace() {
        let xml = r#"<Error xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
</Error>"#;
        assert_eq!(
            extract_s3_error(xml),
            "The specified key does not exist."
        );
    }

    // ---------------------------------------------------------------
    // execute argument validation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_cp_no_args_errors() {
        let ctx = super::super::S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: crate::core::credentials::Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(execute(&[], &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires a source and destination"),
        );
    }

    #[test]
    fn test_cp_one_arg_errors() {
        let ctx = super::super::S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: crate::core::credentials::Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec!["file.txt".to_string()];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires a source and destination"),
        );
    }

    #[test]
    fn test_cp_both_local_errors() {
        let ctx = super::super::S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: crate::core::credentials::Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec!["file.txt".to_string(), "other.txt".to_string()];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("At least one of the source or destination must be an S3 URL"),
        );
    }

    // ---------------------------------------------------------------
    // download_file validation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_cp_download_missing_key_errors() {
        let ctx = super::super::S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: crate::core::credentials::Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec![
            "s3://bucket".to_string(),
            "local.txt".to_string(),
        ];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("S3 source must include a key"),
        );
    }

    // ---------------------------------------------------------------
    // copy_s3_to_s3 validation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_cp_s3_to_s3_missing_src_key_errors() {
        let ctx = super::super::S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: crate::core::credentials::Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec![
            "s3://src-bucket".to_string(),
            "s3://dst-bucket/key.txt".to_string(),
        ];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("S3 source must include a key"),
        );
    }

    #[test]
    fn test_cp_s3_to_s3_missing_dst_key_errors() {
        let ctx = super::super::S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: crate::core::credentials::Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec![
            "s3://src-bucket/key.txt".to_string(),
            "s3://dst-bucket".to_string(),
        ];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("S3 destination must include a key"),
        );
    }

    // ---------------------------------------------------------------
    // url_encode_copy_source tests
    // ---------------------------------------------------------------

    #[test]
    fn test_url_encode_copy_source_simple() {
        assert_eq!(url_encode_copy_source("key.txt"), "key.txt");
    }

    #[test]
    fn test_url_encode_copy_source_with_slashes() {
        assert_eq!(url_encode_copy_source("path/to/key.txt"), "path/to/key.txt");
    }

    #[test]
    fn test_url_encode_copy_source_with_spaces() {
        assert_eq!(url_encode_copy_source("my file.txt"), "my%20file.txt");
    }

    #[test]
    fn test_url_encode_copy_source_with_special_chars() {
        assert_eq!(url_encode_copy_source("key+name"), "key%2Bname");
    }

    #[test]
    fn test_url_encode_copy_source_preserves_unreserved() {
        // Unreserved chars: alphanumeric, - _ . ~
        assert_eq!(url_encode_copy_source("a-b_c.d~e"), "a-b_c.d~e");
    }

    #[test]
    fn test_cp_upload_missing_key_errors() {
        let ctx = super::super::S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: crate::core::credentials::Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec![
            "file.txt".to_string(),
            "s3://bucket".to_string(),
        ];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("S3 destination must include a key"),
        );
    }

    #[test]
    fn test_cp_upload_nonexistent_file_errors() {
        let ctx = super::super::S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: crate::core::credentials::Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec![
            "/tmp/definitely-does-not-exist-raws-test-file.txt".to_string(),
            "s3://bucket/key.txt".to_string(),
        ];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to read file"),
        );
    }

    #[test]
    fn test_cp_flags_are_filtered() {
        // Flags like --recursive should not be treated as positional args
        let ctx = super::super::S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: crate::core::credentials::Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        // Only one positional arg after filtering --recursive
        let args = vec!["--recursive".to_string(), "file.txt".to_string()];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires a source and destination"),
        );
    }

    // ---------------------------------------------------------------
    // is_s3_url tests (imported from mod.rs)
    // ---------------------------------------------------------------

    #[test]
    fn test_is_s3_url_true() {
        assert!(is_s3_url("s3://bucket/key"));
        assert!(is_s3_url("s3://bucket"));
        assert!(is_s3_url("s3://"));
    }

    #[test]
    fn test_is_s3_url_false() {
        assert!(!is_s3_url("file.txt"));
        assert!(!is_s3_url("/tmp/file.txt"));
        assert!(!is_s3_url("https://example.com"));
        assert!(!is_s3_url("S3://bucket")); // case-sensitive
    }
}
