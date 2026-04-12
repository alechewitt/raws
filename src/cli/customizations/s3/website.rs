//! S3 `website` high-level command implementation.
//!
//! Usage: raws s3 website s3://bucket [--index-document <suffix>] [--error-document <key>]
//!
//! Sets the website configuration for an S3 bucket by calling the PutBucketWebsite API.

use anyhow::{bail, Result};

use super::cp;
use super::parse_s3_url;
use super::s3_bucket_api_call;
use super::S3CommandContext;

/// Execute the `s3 website` command.
///
/// Parses the positional S3 URL and optional `--index-document` / `--error-document`
/// flags, builds a `WebsiteConfiguration` XML body, and calls `PutBucketWebsite`.
pub async fn execute(args: &[String], ctx: &S3CommandContext) -> Result<()> {
    let parsed = parse_website_args(args)?;

    // If --help was used, help was already printed; nothing more to do.
    if parsed.s3_url.is_empty() {
        return Ok(());
    }

    let (bucket, _key) = parse_s3_url(&parsed.s3_url)?;

    let body = build_website_xml(parsed.index_document.as_deref(), parsed.error_document.as_deref());
    let body_bytes = body.as_bytes();

    let content_length_str = body_bytes.len().to_string();
    let extra_headers: Vec<(&str, &str)> = vec![
        ("content-type", "application/xml"),
        ("content-length", &content_length_str),
    ];

    let response = s3_bucket_api_call(
        ctx,
        &bucket,
        "PUT",
        "/",
        &[("website", "")],
        Some(body_bytes),
        &extra_headers,
    )
    .await?;

    if response.status >= 300 {
        let resp_body = response.body_string();
        bail!(
            "PutBucketWebsite failed (HTTP {}): {}",
            response.status,
            cp::extract_s3_error(&resp_body)
        );
    }

    // Success produces no output, matching AWS CLI behavior
    Ok(())
}

/// Parsed arguments for the website subcommand.
#[derive(Debug)]
struct WebsiteArgs {
    s3_url: String,
    index_document: Option<String>,
    error_document: Option<String>,
}

/// Parse website-specific arguments.
///
/// Expects a positional S3 URL and optional `--index-document` and `--error-document` flags.
fn parse_website_args(args: &[String]) -> Result<WebsiteArgs> {
    let mut s3_url: Option<String> = None;
    let mut index_document: Option<String> = None;
    let mut error_document: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--index-document" {
            i += 1;
            if i >= args.len() {
                bail!("--index-document requires a value");
            }
            index_document = Some(args[i].clone());
        } else if arg == "--error-document" {
            i += 1;
            if i >= args.len() {
                bail!("--error-document requires a value");
            }
            error_document = Some(args[i].clone());
        } else if arg.starts_with("s3://") {
            s3_url = Some(arg.clone());
        } else if arg == "--help" || arg == "-h" {
            print_website_help();
            return Ok(WebsiteArgs {
                s3_url: String::new(),
                index_document: None,
                error_document: None,
            });
        } else {
            bail!(
                "Unexpected argument: '{}'. Usage: raws s3 website s3://bucket [--index-document <suffix>] [--error-document <key>]",
                arg
            );
        }
        i += 1;
    }

    let url = s3_url.ok_or_else(|| {
        anyhow::anyhow!(
            "s3 website requires an S3 URL argument.\n\
             Usage: raws s3 website s3://bucket [--index-document <suffix>] [--error-document <key>]"
        )
    })?;

    Ok(WebsiteArgs {
        s3_url: url,
        index_document,
        error_document,
    })
}

/// Build the WebsiteConfiguration XML body.
///
/// Either or both of `index_document` and `error_document` may be provided.
fn build_website_xml(index_document: Option<&str>, error_document: Option<&str>) -> String {
    let mut xml = String::from(
        "<WebsiteConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    );

    if let Some(suffix) = index_document {
        xml.push_str("<IndexDocument><Suffix>");
        xml_escape_into(suffix, &mut xml);
        xml.push_str("</Suffix></IndexDocument>");
    }

    if let Some(key) = error_document {
        xml.push_str("<ErrorDocument><Key>");
        xml_escape_into(key, &mut xml);
        xml.push_str("</Key></ErrorDocument>");
    }

    xml.push_str("</WebsiteConfiguration>");
    xml
}

/// Escape special XML characters in a string.
fn xml_escape_into(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
}

/// Print help text for the website subcommand.
fn print_website_help() {
    println!("Usage: raws s3 website s3://bucket [options]\n");
    println!("Set the website configuration for a bucket.\n");
    println!("Options:");
    println!("  --index-document <suffix>  The suffix for index documents (e.g., index.html)");
    println!("  --error-document <key>     The key for the error document (e.g., error.html)");
    println!();
    println!("Example:");
    println!("  raws s3 website s3://mybucket --index-document index.html --error-document error.html");
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // Argument parsing tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_website_args_basic() {
        let args = vec!["s3://my-bucket".to_string()];
        let parsed = parse_website_args(&args).unwrap();
        assert_eq!(parsed.s3_url, "s3://my-bucket");
        assert!(parsed.index_document.is_none());
        assert!(parsed.error_document.is_none());
    }

    #[test]
    fn test_parse_website_args_with_index_document() {
        let args = vec![
            "s3://my-bucket".to_string(),
            "--index-document".to_string(),
            "index.html".to_string(),
        ];
        let parsed = parse_website_args(&args).unwrap();
        assert_eq!(parsed.s3_url, "s3://my-bucket");
        assert_eq!(parsed.index_document.as_deref(), Some("index.html"));
        assert!(parsed.error_document.is_none());
    }

    #[test]
    fn test_parse_website_args_with_error_document() {
        let args = vec![
            "s3://my-bucket".to_string(),
            "--error-document".to_string(),
            "error.html".to_string(),
        ];
        let parsed = parse_website_args(&args).unwrap();
        assert_eq!(parsed.s3_url, "s3://my-bucket");
        assert!(parsed.index_document.is_none());
        assert_eq!(parsed.error_document.as_deref(), Some("error.html"));
    }

    #[test]
    fn test_parse_website_args_with_both_documents() {
        let args = vec![
            "s3://my-bucket".to_string(),
            "--index-document".to_string(),
            "index.html".to_string(),
            "--error-document".to_string(),
            "error.html".to_string(),
        ];
        let parsed = parse_website_args(&args).unwrap();
        assert_eq!(parsed.s3_url, "s3://my-bucket");
        assert_eq!(parsed.index_document.as_deref(), Some("index.html"));
        assert_eq!(parsed.error_document.as_deref(), Some("error.html"));
    }

    #[test]
    fn test_parse_website_args_flags_before_url() {
        let args = vec![
            "--index-document".to_string(),
            "index.html".to_string(),
            "--error-document".to_string(),
            "error.html".to_string(),
            "s3://my-bucket".to_string(),
        ];
        let parsed = parse_website_args(&args).unwrap();
        assert_eq!(parsed.s3_url, "s3://my-bucket");
        assert_eq!(parsed.index_document.as_deref(), Some("index.html"));
        assert_eq!(parsed.error_document.as_deref(), Some("error.html"));
    }

    #[test]
    fn test_parse_website_args_trailing_slash_on_bucket() {
        let args = vec!["s3://my-bucket/".to_string()];
        let parsed = parse_website_args(&args).unwrap();
        assert_eq!(parsed.s3_url, "s3://my-bucket/");
        // parse_s3_url will handle the trailing slash
    }

    #[test]
    fn test_parse_website_args_no_url_errors() {
        let args: Vec<String> = vec![];
        let result = parse_website_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("s3 website requires an S3 URL argument"));
    }

    #[test]
    fn test_parse_website_args_index_missing_value() {
        let args = vec![
            "s3://my-bucket".to_string(),
            "--index-document".to_string(),
        ];
        let result = parse_website_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--index-document requires a value"));
    }

    #[test]
    fn test_parse_website_args_error_missing_value() {
        let args = vec![
            "s3://my-bucket".to_string(),
            "--error-document".to_string(),
        ];
        let result = parse_website_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--error-document requires a value"));
    }

    #[test]
    fn test_parse_website_args_unexpected_argument() {
        let args = vec![
            "s3://my-bucket".to_string(),
            "--bogus-flag".to_string(),
        ];
        let result = parse_website_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unexpected argument"));
    }

    // ---------------------------------------------------------------
    // XML body building tests
    // ---------------------------------------------------------------

    #[test]
    fn test_build_website_xml_both_documents() {
        let xml = build_website_xml(Some("index.html"), Some("error.html"));
        assert_eq!(
            xml,
            "<WebsiteConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <IndexDocument><Suffix>index.html</Suffix></IndexDocument>\
             <ErrorDocument><Key>error.html</Key></ErrorDocument>\
             </WebsiteConfiguration>"
        );
    }

    #[test]
    fn test_build_website_xml_index_only() {
        let xml = build_website_xml(Some("index.html"), None);
        assert_eq!(
            xml,
            "<WebsiteConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <IndexDocument><Suffix>index.html</Suffix></IndexDocument>\
             </WebsiteConfiguration>"
        );
    }

    #[test]
    fn test_build_website_xml_error_only() {
        let xml = build_website_xml(None, Some("error.html"));
        assert_eq!(
            xml,
            "<WebsiteConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <ErrorDocument><Key>error.html</Key></ErrorDocument>\
             </WebsiteConfiguration>"
        );
    }

    #[test]
    fn test_build_website_xml_no_documents() {
        let xml = build_website_xml(None, None);
        assert_eq!(
            xml,
            "<WebsiteConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             </WebsiteConfiguration>"
        );
    }

    #[test]
    fn test_build_website_xml_contains_namespace() {
        let xml = build_website_xml(Some("index.html"), None);
        assert!(xml.contains("xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\""));
    }

    // ---------------------------------------------------------------
    // Integration-style tests (argument parsing + URL parsing)
    // ---------------------------------------------------------------

    #[test]
    fn test_execute_no_args_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(execute(&[], &ctx));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("s3 website requires an S3 URL argument"));
    }

    #[test]
    fn test_execute_invalid_url_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec!["not-an-s3-url".to_string()];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unexpected argument"));
    }

    /// Helper to create a dummy S3CommandContext for testing.
    fn dummy_context() -> S3CommandContext {
        use crate::core::credentials::Credentials;
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
}
