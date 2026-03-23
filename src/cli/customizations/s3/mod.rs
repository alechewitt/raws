//! S3 high-level commands (cp, mv, sync, ls, rm, mb, rb).
//!
//! These are custom commands that do not map directly to a single S3 API operation.
//! Instead, they orchestrate multiple API calls, handle local file I/O, and provide
//! a user-friendly interface similar to familiar shell commands.
//!
//! The real AWS CLI implements these in `awscli/customizations/s3/`.

use anyhow::{bail, Context, Result};

use crate::cli::args::GlobalArgs;
use crate::core::config::provider::ConfigProvider;
use crate::core::credentials::chain::ChainCredentialProvider;
use crate::core::credentials::env::EnvCredentialProvider;
use crate::core::credentials::profile::ProfileCredentialProvider;
use crate::core::credentials::{CredentialProvider, Credentials};
use crate::core::endpoint::resolver;

/// The set of recognized S3 high-level subcommands.
const S3_SUBCOMMANDS: &[&str] = &["ls", "cp", "mv", "rm", "sync", "mb", "rb"];

/// Common context for S3 high-level command execution.
///
/// This bundles the resolved configuration, credentials, endpoint URL, and
/// other infrastructure that all S3 subcommands need.
pub struct S3CommandContext {
    /// The resolved AWS region.
    pub region: String,
    /// The resolved AWS credentials.
    pub credentials: Credentials,
    /// The resolved S3 endpoint URL (e.g., `https://s3.us-east-1.amazonaws.com`).
    pub endpoint_url: String,
    /// Output format (json, text, table).
    pub output_format: String,
    /// Whether debug output is enabled.
    pub debug: bool,
}

/// Entry point for S3 high-level commands.
///
/// Called from `driver.rs::run()` when `service == "s3"`. The `operation` field
/// in `GlobalArgs` contains the subcommand name (ls, cp, etc.), and `args` contains
/// the remaining arguments after the subcommand.
pub async fn handle_s3_command(args: &GlobalArgs) -> Result<()> {
    let subcommand = match &args.operation {
        Some(sub) => sub.as_str(),
        None => {
            print_s3_help();
            return Ok(());
        }
    };

    // Handle "raws s3 help"
    if subcommand == "help" {
        print_s3_help();
        return Ok(());
    }

    // Validate subcommand
    if !S3_SUBCOMMANDS.contains(&subcommand) {
        bail!(
            "Unknown s3 subcommand '{}'. Available subcommands: {}",
            subcommand,
            S3_SUBCOMMANDS.join(", ")
        );
    }

    // Set up the common infrastructure
    let ctx = build_s3_context(args)
        .context("Failed to set up S3 command context")?;

    if ctx.debug {
        eprintln!("[debug] s3 subcommand={subcommand}");
        eprintln!("[debug] region={} endpoint={}", ctx.region, ctx.endpoint_url);
        eprintln!(
            "[debug] credentials resolved: access_key={}...",
            &ctx.credentials.access_key_id[..8.min(ctx.credentials.access_key_id.len())]
        );
    }

    // Dispatch to the appropriate subcommand handler
    dispatch_s3_subcommand(subcommand, &args.args, &ctx).await
}

/// Build the common S3CommandContext from the global arguments.
///
/// Resolves config/region, credentials, and endpoint URL using the same
/// patterns as the main driver.
fn build_s3_context(args: &GlobalArgs) -> Result<S3CommandContext> {
    // 1. Load config (resolves region, profile, output format)
    let config = ConfigProvider::new(
        args.region.as_deref(),
        Some(args.output.as_str()),
        args.profile.as_deref(),
    )?;

    let region = config
        .region
        .as_deref()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No region specified. Use --region, AWS_REGION, or configure a default region."
            )
        })?
        .to_string();

    let output_format = config
        .output
        .as_deref()
        .unwrap_or("json")
        .to_string();

    // 2. Resolve credentials
    let mut providers: Vec<Box<dyn CredentialProvider>> = vec![Box::new(EnvCredentialProvider)];
    providers.push(Box::new(ProfileCredentialProvider::new(&config.profile)));
    let chain = ChainCredentialProvider::new(providers);
    let credentials = chain
        .resolve()
        .context("Failed to resolve AWS credentials")?;

    // 3. Resolve endpoint URL (with dualstack/FIPS variant support)
    let variant_tags = resolver::EndpointVariantTags {
        use_dualstack: args.use_dualstack_endpoint,
        use_fips: args.use_fips_endpoint,
    };
    let endpoint_url = match &args.endpoint_url {
        Some(url) => url.clone(),
        None => resolver::resolve_endpoint_with_variants(
            "s3",
            &region,
            None, // S3 is regionalized, no global endpoint
            &variant_tags,
        )?,
    };

    Ok(S3CommandContext {
        region,
        credentials,
        endpoint_url,
        output_format,
        debug: args.debug,
    })
}

/// Dispatch to the appropriate S3 subcommand handler.
async fn dispatch_s3_subcommand(
    subcommand: &str,
    args: &[String],
    ctx: &S3CommandContext,
) -> Result<()> {
    match subcommand {
        "ls" => handle_ls(args, ctx).await,
        "cp" => handle_cp(args, ctx).await,
        "mv" => handle_mv(args, ctx).await,
        "rm" => handle_rm(args, ctx).await,
        "sync" => handle_sync(args, ctx).await,
        "mb" => handle_mb(args, ctx).await,
        "rb" => handle_rb(args, ctx).await,
        _ => bail!("Unknown s3 subcommand '{}'", subcommand),
    }
}

/// Print help for the S3 high-level commands.
fn print_s3_help() {
    println!("Usage: raws s3 <subcommand> [options] [paths]\n");
    println!("Available subcommands:\n");
    println!("  ls       List S3 objects and common prefixes under a prefix or all buckets");
    println!("  cp       Copy files and S3 objects");
    println!("  mv       Move files and S3 objects");
    println!("  rm       Delete S3 objects");
    println!("  sync     Sync directories and S3 prefixes");
    println!("  mb       Make an S3 bucket");
    println!("  rb       Remove an S3 bucket");
    println!();
    println!("For API-level S3 operations, use: raws s3api <operation>");
}

// ---------------------------------------------------------------------------
// Stub subcommand handlers (to be implemented in subsequent milestones)
// ---------------------------------------------------------------------------

async fn handle_ls(_args: &[String], _ctx: &S3CommandContext) -> Result<()> {
    bail!("s3 ls is not yet implemented")
}

async fn handle_cp(_args: &[String], _ctx: &S3CommandContext) -> Result<()> {
    bail!("s3 cp is not yet implemented")
}

async fn handle_mv(_args: &[String], _ctx: &S3CommandContext) -> Result<()> {
    bail!("s3 mv is not yet implemented")
}

async fn handle_rm(_args: &[String], _ctx: &S3CommandContext) -> Result<()> {
    bail!("s3 rm is not yet implemented")
}

async fn handle_sync(_args: &[String], _ctx: &S3CommandContext) -> Result<()> {
    bail!("s3 sync is not yet implemented")
}

async fn handle_mb(_args: &[String], _ctx: &S3CommandContext) -> Result<()> {
    bail!("s3 mb is not yet implemented")
}

async fn handle_rb(_args: &[String], _ctx: &S3CommandContext) -> Result<()> {
    bail!("s3 rb is not yet implemented")
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    // ---------------------------------------------------------------
    // S3 subcommand recognition tests
    // ---------------------------------------------------------------

    #[test]
    fn test_s3_subcommands_list() {
        assert!(S3_SUBCOMMANDS.contains(&"ls"));
        assert!(S3_SUBCOMMANDS.contains(&"cp"));
        assert!(S3_SUBCOMMANDS.contains(&"mv"));
        assert!(S3_SUBCOMMANDS.contains(&"rm"));
        assert!(S3_SUBCOMMANDS.contains(&"sync"));
        assert!(S3_SUBCOMMANDS.contains(&"mb"));
        assert!(S3_SUBCOMMANDS.contains(&"rb"));
    }

    #[test]
    fn test_s3_subcommands_count() {
        assert_eq!(S3_SUBCOMMANDS.len(), 7);
    }

    #[test]
    fn test_s3_unknown_subcommand_not_in_list() {
        assert!(!S3_SUBCOMMANDS.contains(&"unknown"));
        assert!(!S3_SUBCOMMANDS.contains(&"list-buckets"));
        assert!(!S3_SUBCOMMANDS.contains(&"put-object"));
    }

    // ---------------------------------------------------------------
    // Help output tests
    // ---------------------------------------------------------------

    #[test]
    fn test_s3_help_does_not_panic() {
        // Just verify that print_s3_help doesn't panic
        print_s3_help();
    }

    // ---------------------------------------------------------------
    // handle_s3_command argument parsing tests (integration-style)
    // ---------------------------------------------------------------

    #[test]
    fn test_handle_s3_command_no_operation_shows_help() {
        // When no subcommand is given, handle_s3_command should succeed (prints help)
        let args = GlobalArgs::try_parse_from(["raws", "s3"]).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(handle_s3_command(&args));
        assert!(result.is_ok(), "No subcommand should show help and succeed");
    }

    #[test]
    fn test_handle_s3_command_help_subcommand() {
        let args = GlobalArgs::try_parse_from(["raws", "s3", "help"]).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(handle_s3_command(&args));
        assert!(result.is_ok(), "'help' subcommand should succeed");
    }

    #[test]
    fn test_handle_s3_command_unknown_subcommand() {
        // An unknown subcommand should fail before trying to set up context
        let args = GlobalArgs::try_parse_from(["raws", "s3", "foobar"]).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(handle_s3_command(&args));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Unknown s3 subcommand 'foobar'"),
            "Error should mention unknown subcommand, got: {err_msg}"
        );
    }

    // ---------------------------------------------------------------
    // dispatch_s3_subcommand stub tests
    // ---------------------------------------------------------------

    /// Helper to create a dummy S3CommandContext for testing dispatch.
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
        }
    }

    #[test]
    fn test_dispatch_ls_returns_not_yet_implemented() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(dispatch_s3_subcommand("ls", &[], &ctx));
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("s3 ls is not yet implemented"),
            "Should indicate ls is not yet implemented"
        );
    }

    #[test]
    fn test_dispatch_cp_returns_not_yet_implemented() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(dispatch_s3_subcommand("cp", &[], &ctx));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("s3 cp is not yet implemented"));
    }

    #[test]
    fn test_dispatch_mv_returns_not_yet_implemented() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(dispatch_s3_subcommand("mv", &[], &ctx));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("s3 mv is not yet implemented"));
    }

    #[test]
    fn test_dispatch_rm_returns_not_yet_implemented() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(dispatch_s3_subcommand("rm", &[], &ctx));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("s3 rm is not yet implemented"));
    }

    #[test]
    fn test_dispatch_sync_returns_not_yet_implemented() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(dispatch_s3_subcommand("sync", &[], &ctx));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("s3 sync is not yet implemented"));
    }

    #[test]
    fn test_dispatch_mb_returns_not_yet_implemented() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(dispatch_s3_subcommand("mb", &[], &ctx));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("s3 mb is not yet implemented"));
    }

    #[test]
    fn test_dispatch_rb_returns_not_yet_implemented() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(dispatch_s3_subcommand("rb", &[], &ctx));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("s3 rb is not yet implemented"));
    }

    #[test]
    fn test_dispatch_unknown_subcommand_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(dispatch_s3_subcommand("nonexistent", &[], &ctx));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown s3 subcommand"));
    }

    // ---------------------------------------------------------------
    // S3CommandContext construction tests
    // ---------------------------------------------------------------

    #[test]
    fn test_s3_context_fields() {
        let ctx = dummy_context();
        assert_eq!(ctx.region, "us-east-1");
        assert_eq!(ctx.endpoint_url, "https://s3.us-east-1.amazonaws.com");
        assert_eq!(ctx.output_format, "json");
        assert!(!ctx.debug);
        assert_eq!(ctx.credentials.access_key_id, "AKIAIOSFODNN7EXAMPLE");
    }

    // ---------------------------------------------------------------
    // Clap arg parsing tests for S3 subcommands
    // ---------------------------------------------------------------

    #[test]
    fn test_clap_s3_ls_parsing() {
        let args = GlobalArgs::try_parse_from(["raws", "s3", "ls"]).unwrap();
        assert_eq!(args.service, Some("s3".to_string()));
        assert_eq!(args.operation, Some("ls".to_string()));
        assert!(args.args.is_empty());
    }

    #[test]
    fn test_clap_s3_ls_with_path() {
        let args = GlobalArgs::try_parse_from([
            "raws", "s3", "ls", "s3://my-bucket/prefix/",
        ])
        .unwrap();
        assert_eq!(args.service, Some("s3".to_string()));
        assert_eq!(args.operation, Some("ls".to_string()));
        assert_eq!(args.args, vec!["s3://my-bucket/prefix/"]);
    }

    #[test]
    fn test_clap_s3_cp_with_paths() {
        let args = GlobalArgs::try_parse_from([
            "raws", "s3", "cp", "file.txt", "s3://my-bucket/file.txt",
        ])
        .unwrap();
        assert_eq!(args.service, Some("s3".to_string()));
        assert_eq!(args.operation, Some("cp".to_string()));
        assert_eq!(args.args, vec!["file.txt", "s3://my-bucket/file.txt"]);
    }

    #[test]
    fn test_clap_s3_cp_with_recursive() {
        let args = GlobalArgs::try_parse_from([
            "raws", "s3", "cp", ".", "s3://my-bucket/", "--recursive",
        ])
        .unwrap();
        assert_eq!(args.service, Some("s3".to_string()));
        assert_eq!(args.operation, Some("cp".to_string()));
        assert_eq!(
            args.args,
            vec![".", "s3://my-bucket/", "--recursive"]
        );
    }

    #[test]
    fn test_clap_s3_with_region() {
        let args = GlobalArgs::try_parse_from([
            "raws", "--region", "eu-west-1", "s3", "ls",
        ])
        .unwrap();
        assert_eq!(args.region, Some("eu-west-1".to_string()));
        assert_eq!(args.service, Some("s3".to_string()));
        assert_eq!(args.operation, Some("ls".to_string()));
    }

    #[test]
    fn test_clap_s3_mb_with_bucket() {
        let args = GlobalArgs::try_parse_from([
            "raws", "s3", "mb", "s3://new-bucket-name",
        ])
        .unwrap();
        assert_eq!(args.service, Some("s3".to_string()));
        assert_eq!(args.operation, Some("mb".to_string()));
        assert_eq!(args.args, vec!["s3://new-bucket-name"]);
    }
}
