//! S3 presign command: generate a presigned URL for an S3 object.
//!
//! Usage: raws s3 presign s3://bucket/key [--expires-in <seconds>]
//!
//! Generates a presigned URL using SigV4 query string signing. The URL can be
//! used without credentials to access the object for the duration specified by
//! --expires-in (default: 3600 seconds / 1 hour).

use anyhow::{bail, Result};

use crate::core::auth::sigv4;
use crate::core::endpoint::resolver;

/// Default expiry time in seconds (1 hour).
const DEFAULT_EXPIRES_IN: u64 = 3600;

/// Handle the `raws s3 presign` subcommand.
///
/// Parses the S3 URL and optional --expires-in flag from `args`, builds a
/// presigned URL using SigV4 query string signing, and prints it to stdout.
pub fn handle_presign(args: &[String], ctx: &super::S3CommandContext) -> Result<()> {
    // Parse arguments
    let (s3_url, expires_in) = parse_presign_args(args)?;

    // Parse s3://bucket/key
    let (bucket, key) = super::parse_s3_url(&s3_url)?;
    if key.is_empty() {
        bail!("s3 presign requires an object key: s3://bucket/key");
    }

    // Build the presigned URL
    let url = build_presigned_url(&bucket, &key, expires_in, ctx)?;

    println!("{url}");
    Ok(())
}

/// Parse presign-specific arguments.
///
/// Returns (s3_url, expires_in_seconds).
fn parse_presign_args(args: &[String]) -> Result<(String, u64)> {
    let mut s3_url: Option<String> = None;
    let mut expires_in: u64 = DEFAULT_EXPIRES_IN;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--expires-in" {
            i += 1;
            if i >= args.len() {
                bail!("--expires-in requires a value (seconds)");
            }
            expires_in = args[i]
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("--expires-in must be a positive integer, got '{}'", args[i]))?;
            if expires_in == 0 {
                bail!("--expires-in must be greater than 0");
            }
        } else if arg.starts_with("s3://") {
            s3_url = Some(arg.clone());
        } else {
            bail!("Unexpected argument: '{arg}'. Usage: raws s3 presign s3://bucket/key [--expires-in <seconds>]");
        }
        i += 1;
    }

    let url = s3_url.ok_or_else(|| {
        anyhow::anyhow!("s3 presign requires an S3 URL argument: s3://bucket/key")
    })?;

    Ok((url, expires_in))
}

/// Build a presigned URL for the given bucket/key using SigV4 query string signing.
fn build_presigned_url(
    bucket: &str,
    key: &str,
    expires_in: u64,
    ctx: &super::S3CommandContext,
) -> Result<String> {
    let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();

    build_presigned_url_with_datetime(bucket, key, expires_in, ctx, &datetime)
}

/// Build a presigned URL with a specific datetime (for testability).
fn build_presigned_url_with_datetime(
    bucket: &str,
    key: &str,
    expires_in: u64,
    ctx: &super::S3CommandContext,
    datetime: &str,
) -> Result<String> {
    // Resolve virtual-hosted style endpoint for this bucket
    let bucket_endpoint = resolver::apply_s3_virtual_hosted_style(&ctx.endpoint_url, bucket);

    let (base_url, uri_path) = if bucket_endpoint != ctx.endpoint_url {
        // Virtual-hosted style: bucket is in the hostname
        let key_path = if key.is_empty() {
            "/".to_string()
        } else {
            format!("/{key}")
        };
        (bucket_endpoint, key_path)
    } else {
        // Path-style fallback (e.g., dots in bucket name)
        let path = if key.is_empty() {
            format!("/{bucket}")
        } else {
            format!("/{bucket}/{key}")
        };
        (ctx.endpoint_url.clone(), path)
    };

    // Extract host from the base URL
    let parsed_url = url::Url::parse(&base_url)
        .map_err(|e| anyhow::anyhow!("Invalid endpoint URL '{}': {}", base_url, e))?;
    let host = parsed_url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("No host in endpoint URL '{}'", base_url))?;

    // Build signing params
    let signing_params = sigv4::SigningParams::from_credentials(
        &ctx.credentials,
        &ctx.region,
        "s3",
        datetime,
    );

    let credential = format!("{}/{}", signing_params.access_key, signing_params.scope());

    // Build the query string parameters for presigning.
    // These must be sorted alphabetically by parameter name for the canonical request.
    let mut query_pairs: Vec<(String, String)> = vec![
        ("X-Amz-Algorithm".to_string(), "AWS4-HMAC-SHA256".to_string()),
        ("X-Amz-Credential".to_string(), credential),
        ("X-Amz-Date".to_string(), datetime.to_string()),
        ("X-Amz-Expires".to_string(), expires_in.to_string()),
        ("X-Amz-SignedHeaders".to_string(), "host".to_string()),
    ];

    // Add security token if present
    if let Some(token) = &ctx.credentials.session_token {
        query_pairs.push(("X-Amz-Security-Token".to_string(), token.clone()));
    }

    // Sort by key for canonical query string
    query_pairs.sort_by(|a, b| a.0.cmp(&b.0));

    // Build the canonical query string (URL-encoded keys and values)
    let canonical_qs = query_pairs
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                sigv4::uri_encode(k, true),
                sigv4::uri_encode(v, true)
            )
        })
        .collect::<Vec<_>>()
        .join("&");

    // Headers for signing: only host
    let headers = vec![("host".to_string(), host.to_string())];

    // Payload hash for presigned URLs is always UNSIGNED-PAYLOAD
    let payload_hash = "UNSIGNED-PAYLOAD";

    // Build canonical request
    let cr = sigv4::canonical_request("GET", &uri_path, &canonical_qs, &headers, payload_hash);

    // String to sign
    let scope = signing_params.scope();
    let sts = sigv4::string_to_sign(datetime, &scope, &cr);

    // Calculate signature
    let key = sigv4::signing_key_cached(
        signing_params.secret_key,
        signing_params.date(),
        signing_params.region,
        signing_params.service,
    );
    let signature = sigv4::calculate_signature(&key, &sts);

    // Build the final URL with encoded path segments and all query parameters
    let encoded_uri = uri_path
        .split('/')
        .map(|seg| sigv4::uri_encode(seg, true))
        .collect::<Vec<_>>()
        .join("/");

    let base = base_url.trim_end_matches('/');

    // Build final query string with URL-encoded values for the URL
    let url_qs = query_pairs
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                sigv4::uri_encode(k, true),
                sigv4::uri_encode(v, true)
            )
        })
        .collect::<Vec<_>>()
        .join("&");

    let presigned_url = format!(
        "{base}{encoded_uri}?{url_qs}&X-Amz-Signature={signature}"
    );

    Ok(presigned_url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::customizations::s3::S3CommandContext;
    use crate::core::credentials::Credentials;

    fn test_context() -> S3CommandContext {
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
        }
    }

    fn test_context_with_token() -> S3CommandContext {
        S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: Some("FwoGZXIvYXdzEBY".to_string()),
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,
        }
    }

    // ---------------------------------------------------------------
    // Argument parsing tests
    // ---------------------------------------------------------------

    #[test]
    fn test_presign_parse_args_basic() {
        let args = vec!["s3://test-bucket/test-key".to_string()];
        let (url, expires) = parse_presign_args(&args).unwrap();
        assert_eq!(url, "s3://test-bucket/test-key");
        assert_eq!(expires, DEFAULT_EXPIRES_IN);
    }

    #[test]
    fn test_presign_parse_args_default_expiry_is_3600() {
        let args = vec!["s3://bucket/key".to_string()];
        let (_, expires) = parse_presign_args(&args).unwrap();
        assert_eq!(expires, 3600);
    }

    #[test]
    fn test_presign_parse_args_custom_expiry() {
        let args = vec![
            "s3://bucket/key".to_string(),
            "--expires-in".to_string(),
            "300".to_string(),
        ];
        let (url, expires) = parse_presign_args(&args).unwrap();
        assert_eq!(url, "s3://bucket/key");
        assert_eq!(expires, 300);
    }

    #[test]
    fn test_presign_parse_args_expires_before_url() {
        let args = vec![
            "--expires-in".to_string(),
            "600".to_string(),
            "s3://bucket/key".to_string(),
        ];
        let (url, expires) = parse_presign_args(&args).unwrap();
        assert_eq!(url, "s3://bucket/key");
        assert_eq!(expires, 600);
    }

    #[test]
    fn test_presign_parse_args_no_url_errors() {
        let args: Vec<String> = vec![];
        let result = parse_presign_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires an S3 URL"));
    }

    #[test]
    fn test_presign_parse_args_expires_missing_value() {
        let args = vec![
            "s3://bucket/key".to_string(),
            "--expires-in".to_string(),
        ];
        let result = parse_presign_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires a value"));
    }

    #[test]
    fn test_presign_parse_args_expires_zero_errors() {
        let args = vec![
            "s3://bucket/key".to_string(),
            "--expires-in".to_string(),
            "0".to_string(),
        ];
        let result = parse_presign_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("greater than 0"));
    }

    #[test]
    fn test_presign_parse_args_expires_non_numeric_errors() {
        let args = vec![
            "s3://bucket/key".to_string(),
            "--expires-in".to_string(),
            "abc".to_string(),
        ];
        let result = parse_presign_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("positive integer"));
    }

    // ---------------------------------------------------------------
    // URL generation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_presign_url_contains_required_query_params() {
        let ctx = test_context();
        let url = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"), "Missing Algorithm param: {url}");
        assert!(url.contains("X-Amz-Credential="), "Missing Credential param: {url}");
        assert!(url.contains("X-Amz-Date=20250101T000000Z"), "Missing Date param: {url}");
        assert!(url.contains("X-Amz-Expires=3600"), "Missing Expires param: {url}");
        assert!(url.contains("X-Amz-SignedHeaders=host"), "Missing SignedHeaders param: {url}");
        assert!(url.contains("X-Amz-Signature="), "Missing Signature param: {url}");
    }

    #[test]
    fn test_presign_url_default_expiry() {
        let ctx = test_context();
        let url = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            DEFAULT_EXPIRES_IN,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(url.contains("X-Amz-Expires=3600"), "Default expiry should be 3600: {url}");
    }

    #[test]
    fn test_presign_url_custom_expiry() {
        let ctx = test_context();
        let url = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            300,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(url.contains("X-Amz-Expires=300"), "Custom expiry should be 300: {url}");
    }

    #[test]
    fn test_presign_url_structure_virtual_hosted() {
        let ctx = test_context();
        let url = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        // Virtual-hosted style: bucket in hostname
        assert!(
            url.starts_with("https://test-bucket.s3.us-east-1.amazonaws.com/test-key?"),
            "URL should use virtual-hosted style: {url}"
        );
    }

    #[test]
    fn test_presign_url_structure_path_style_for_dotted_bucket() {
        let ctx = test_context();
        let url = build_presigned_url_with_datetime(
            "my.dotted.bucket",
            "some-key",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        // Path-style for dotted buckets
        assert!(
            url.starts_with("https://s3.us-east-1.amazonaws.com/my.dotted.bucket/some-key?"),
            "Dotted bucket should use path-style: {url}"
        );
    }

    #[test]
    fn test_presign_url_signature_is_present_and_hex() {
        let ctx = test_context();
        let url = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        // Extract signature value
        let sig_prefix = "X-Amz-Signature=";
        let sig_start = url.find(sig_prefix).unwrap() + sig_prefix.len();
        let sig_end = url[sig_start..].find('&').map(|i| sig_start + i).unwrap_or(url.len());
        let signature = &url[sig_start..sig_end];

        assert_eq!(signature.len(), 64, "Signature should be 64 hex chars: {signature}");
        assert!(
            signature.chars().all(|c| c.is_ascii_hexdigit()),
            "Signature should be hex: {signature}"
        );
    }

    #[test]
    fn test_presign_url_credential_contains_scope() {
        let ctx = test_context();
        let url = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        // Credential should contain: access_key/date/region/s3/aws4_request (URL-encoded)
        // The '/' in the credential is URL-encoded as %2F
        assert!(
            url.contains("AKIAIOSFODNN7EXAMPLE%2F20250101%2Fus-east-1%2Fs3%2Faws4_request"),
            "Credential should contain encoded scope: {url}"
        );
    }

    #[test]
    fn test_presign_url_with_session_token() {
        let ctx = test_context_with_token();
        let url = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(
            url.contains("X-Amz-Security-Token="),
            "URL should contain security token: {url}"
        );
        assert!(
            url.contains("FwoGZXIvYXdzEBY"),
            "URL should contain the token value: {url}"
        );
    }

    #[test]
    fn test_presign_url_without_session_token() {
        let ctx = test_context();
        let url = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(
            !url.contains("X-Amz-Security-Token"),
            "URL should not contain security token when none is set: {url}"
        );
    }

    #[test]
    fn test_presign_url_key_with_slashes() {
        let ctx = test_context();
        let url = build_presigned_url_with_datetime(
            "test-bucket",
            "path/to/my/object.txt",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(
            url.starts_with("https://test-bucket.s3.us-east-1.amazonaws.com/path/to/my/object.txt?"),
            "URL should preserve key path: {url}"
        );
    }

    #[test]
    fn test_presign_url_key_with_special_chars() {
        let ctx = test_context();
        let url = build_presigned_url_with_datetime(
            "test-bucket",
            "my file (1).txt",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        // Special chars in key should be URL-encoded
        assert!(url.contains("my%20file%20%281%29.txt"), "Special chars should be encoded: {url}");
    }

    #[test]
    fn test_presign_deterministic_signature() {
        // Same inputs should always produce the same URL
        let ctx = test_context();
        let url1 = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();
        let url2 = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        assert_eq!(url1, url2, "Same inputs should produce identical URLs");
    }

    #[test]
    fn test_presign_different_expiry_different_signature() {
        let ctx = test_context();
        let url1 = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            3600,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();
        let url2 = build_presigned_url_with_datetime(
            "test-bucket",
            "test-key",
            300,
            &ctx,
            "20250101T000000Z",
        )
        .unwrap();

        // Different expiry should produce different signatures
        let sig1 = extract_signature(&url1);
        let sig2 = extract_signature(&url2);
        assert_ne!(sig1, sig2, "Different expiry should produce different signatures");
    }

    #[test]
    fn test_presign_requires_key() {
        let ctx = test_context();
        let args = vec!["s3://bucket-only".to_string()];
        let result = handle_presign(&args, &ctx);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires an object key"));
    }

    /// Helper to extract the signature from a presigned URL.
    fn extract_signature(url: &str) -> String {
        let prefix = "X-Amz-Signature=";
        let start = url.find(prefix).unwrap() + prefix.len();
        let end = url[start..].find('&').map(|i| start + i).unwrap_or(url.len());
        url[start..end].to_string()
    }
}
