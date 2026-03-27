//! CloudWatch Logs high-level commands.
//!
//! Currently supports `tail`, which streams log events from a CloudWatch Logs
//! log group using the FilterLogEvents API.

pub mod tail;

use anyhow::{bail, Context, Result};

use crate::cli::args::GlobalArgs;
use crate::core::config::provider::ConfigProvider;
use crate::core::credentials::chain::build_credential_chain;
use crate::core::credentials::CredentialProvider;
use crate::core::endpoint::resolver;

/// Recognized CloudWatch Logs high-level subcommands.
const LOGS_SUBCOMMANDS: &[&str] = &["tail"];

/// Check if this logs operation is one we handle as a custom command.
pub fn is_custom_command(operation: &str) -> bool {
    LOGS_SUBCOMMANDS.contains(&operation)
}

/// Entry point for CloudWatch Logs high-level commands.
///
/// Called from `driver.rs::run()` when the service is "logs" and
/// the operation matches one of our custom subcommands.
pub async fn handle_logs_command(
    args: &GlobalArgs,
    operation: &str,
) -> Result<()> {
    if operation == "help" || !LOGS_SUBCOMMANDS.contains(&operation) {
        bail!("__logs_passthrough__");
    }

    match operation {
        "tail" => handle_tail(args).await,
        _ => bail!("Unknown logs subcommand: {}", operation),
    }
}

/// Handle `raws logs tail <log-group-name>`.
async fn handle_tail(args: &GlobalArgs) -> Result<()> {
    // Parse tail-specific arguments
    let tail_args = tail::parse_tail_args(&args.args)
        .context("Failed to parse logs tail arguments")?;

    // Load config
    let config = ConfigProvider::new(
        args.region.as_deref(),
        args.output.as_deref(),
        args.profile.as_deref(),
    )?;

    if args.profile.is_some() {
        ConfigProvider::validate_profile_exists(&config.profile)?;
    }

    let region = config
        .region
        .as_deref()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No region specified. Use --region, AWS_REGION, or configure a default region."
            )
        })?
        .to_string();

    // Resolve credentials
    let explicit_profile = args.profile.is_some();
    let chain = build_credential_chain(&config.profile, explicit_profile, config.region.as_deref());
    let credentials = chain.resolve()?;

    if args.debug {
        eprintln!(
            "[debug] logs tail: region={} log_group={}",
            region, tail_args.log_group_name
        );
        eprintln!(
            "[debug] credentials resolved: access_key={}...",
            &credentials.access_key_id[..8.min(credentials.access_key_id.len())]
        );
    }

    // Resolve endpoint URL
    let variant_tags = resolver::EndpointVariantTags {
        use_dualstack: args.use_dualstack_endpoint,
        use_fips: args.use_fips_endpoint,
    };
    let endpoint_url = match &args.endpoint_url {
        Some(url) => url.clone(),
        None => resolver::resolve_endpoint_with_variants(
            "logs",
            &region,
            None,
            &variant_tags,
        )?,
    };

    if args.debug {
        eprintln!("[debug] logs endpoint: {}", endpoint_url);
    }

    // Run the tail command
    tail::run_tail(&tail_args, credentials, region, endpoint_url, args.debug).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_custom_command_tail() {
        assert!(is_custom_command("tail"));
    }

    #[test]
    fn test_is_custom_command_unknown() {
        assert!(!is_custom_command("describe-log-groups"));
        assert!(!is_custom_command("filter-log-events"));
        assert!(!is_custom_command("help"));
    }
}
