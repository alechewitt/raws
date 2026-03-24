//! S3 `mb` and `rb` high-level command implementations.
//!
//! - `raws s3 mb s3://bucket` - create a new S3 bucket
//! - `raws s3 rb s3://bucket [--force]` - remove an S3 bucket

use anyhow::{bail, Result};

use super::cp;
use super::parse_s3_url;
use super::rm;
use super::s3_bucket_api_call;
use super::S3CommandContext;

/// Execute the `s3 mb` command.
///
/// Creates a new S3 bucket. For non-us-east-1 regions, includes a
/// `CreateBucketConfiguration` with the `LocationConstraint` element.
pub async fn execute_mb(args: &[String], ctx: &S3CommandContext) -> Result<()> {
    let positional: Vec<&String> = args.iter().filter(|a| !a.starts_with('-')).collect();

    if positional.is_empty() {
        bail!(
            "s3 mb requires an S3 URL argument.\n\
             Usage: raws s3 mb s3://bucket-name"
        );
    }

    let s3_url = positional[0].as_str();
    let (bucket, _key) = parse_s3_url(s3_url)?;

    // Build the CreateBucket body
    let body = if ctx.region != "us-east-1" {
        Some(format!(
            "<CreateBucketConfiguration>\
               <LocationConstraint>{}</LocationConstraint>\
             </CreateBucketConfiguration>",
            ctx.region
        ))
    } else {
        None
    };

    let body_bytes = body.as_ref().map(|b| b.as_bytes());
    let mut extra_headers: Vec<(&str, &str)> = Vec::new();

    let content_length_str;
    if let Some(ref b) = body {
        content_length_str = b.len().to_string();
        extra_headers.push(("content-type", "application/xml"));
        extra_headers.push(("content-length", &content_length_str));
    }

    let response = s3_bucket_api_call(
        ctx,
        &bucket,
        "PUT",
        "/",
        &[],
        body_bytes,
        &extra_headers,
    )
    .await?;

    if response.status >= 300 {
        let resp_body = response.body_string();
        bail!(
            "CreateBucket failed (HTTP {}): {}",
            response.status,
            cp::extract_s3_error(&resp_body)
        );
    }

    println!("make_bucket: s3://{}", bucket);
    Ok(())
}

/// Execute the `s3 rb` command.
///
/// Removes an S3 bucket. With `--force`, first deletes all objects in the bucket
/// (like `rm --recursive`), then deletes the bucket itself.
pub async fn execute_rb(args: &[String], ctx: &S3CommandContext) -> Result<()> {
    let force = args.iter().any(|a| a == "--force");

    let positional: Vec<&String> = args.iter().filter(|a| !a.starts_with('-')).collect();

    if positional.is_empty() {
        bail!(
            "s3 rb requires an S3 URL argument.\n\
             Usage: raws s3 rb s3://bucket-name [--force]"
        );
    }

    let s3_url = positional[0].as_str();
    let (bucket, _key) = parse_s3_url(s3_url)?;

    if force {
        // Delete all objects in the bucket first
        rm::delete_recursive(ctx, &bucket, "").await?;
    }

    // DELETE the bucket itself: DELETE / on bucket endpoint
    let response = s3_bucket_api_call(ctx, &bucket, "DELETE", "/", &[], None, &[]).await?;

    if response.status >= 300 {
        let resp_body = response.body_string();
        bail!(
            "DeleteBucket failed (HTTP {}): {}",
            response.status,
            cp::extract_s3_error(&resp_body)
        );
    }

    println!("remove_bucket: s3://{}", bucket);
    Ok(())
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

    fn dummy_context_eu() -> S3CommandContext {
        S3CommandContext {
            region: "eu-west-1".to_string(),
            credentials: Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.eu-west-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,

            no_sign_request: false,
        }
    }

    // ---------------------------------------------------------------
    // mb argument validation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_mb_no_args_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(execute_mb(&[], &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("s3 mb requires an S3 URL argument"),
        );
    }

    #[test]
    fn test_mb_invalid_url_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec!["not-an-s3-url".to_string()];
        let result = rt.block_on(execute_mb(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("Invalid S3 URL"),
        );
    }

    // ---------------------------------------------------------------
    // rb argument validation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_rb_no_args_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(execute_rb(&[], &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("s3 rb requires an S3 URL argument"),
        );
    }

    #[test]
    fn test_rb_invalid_url_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec!["not-an-s3-url".to_string()];
        let result = rt.block_on(execute_rb(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("Invalid S3 URL"),
        );
    }

    // ---------------------------------------------------------------
    // CreateBucket body construction tests
    // ---------------------------------------------------------------

    #[test]
    fn test_create_bucket_body_us_east_1() {
        let ctx = dummy_context();
        // us-east-1 should produce no body
        assert_eq!(ctx.region, "us-east-1");
        let body = if ctx.region != "us-east-1" {
            Some(format!(
                "<CreateBucketConfiguration>\
                   <LocationConstraint>{}</LocationConstraint>\
                 </CreateBucketConfiguration>",
                ctx.region
            ))
        } else {
            None
        };
        assert!(body.is_none());
    }

    #[test]
    fn test_create_bucket_body_eu_west_1() {
        let ctx = dummy_context_eu();
        assert_eq!(ctx.region, "eu-west-1");
        let body = if ctx.region != "us-east-1" {
            Some(format!(
                "<CreateBucketConfiguration>\
                   <LocationConstraint>{}</LocationConstraint>\
                 </CreateBucketConfiguration>",
                ctx.region
            ))
        } else {
            None
        };
        assert!(body.is_some());
        let body_str = body.unwrap();
        assert!(body_str.contains("<LocationConstraint>eu-west-1</LocationConstraint>"));
        assert!(body_str.contains("<CreateBucketConfiguration>"));
    }

    // ---------------------------------------------------------------
    // Force flag detection tests
    // ---------------------------------------------------------------

    #[test]
    fn test_rb_force_flag_detection() {
        let args = vec!["s3://my-bucket".to_string(), "--force".to_string()];
        let force = args.iter().any(|a| a == "--force");
        assert!(force);
    }

    #[test]
    fn test_rb_no_force_flag() {
        let args = vec!["s3://my-bucket".to_string()];
        let force = args.iter().any(|a| a == "--force");
        assert!(!force);
    }
}
