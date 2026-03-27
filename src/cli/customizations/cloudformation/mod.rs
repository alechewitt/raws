//! CloudFormation high-level commands.
//!
//! Currently supports `deploy`, which orchestrates changeset creation,
//! polling, and execution for CloudFormation stack deployments.

pub mod deploy;

use anyhow::{bail, Context, Result};

use crate::cli::args::GlobalArgs;
use crate::core::config::provider::ConfigProvider;
use crate::core::credentials::chain::build_credential_chain;
use crate::core::credentials::CredentialProvider;
use crate::core::endpoint::resolver;

/// Recognized CloudFormation high-level subcommands.
const CFN_SUBCOMMANDS: &[&str] = &["deploy"];

/// Entry point for CloudFormation high-level commands.
///
/// Called from `driver.rs::run()` when the service is "cloudformation" and
/// the operation matches one of our custom subcommands.
pub async fn handle_cloudformation_command(
    args: &GlobalArgs,
    operation: &str,
) -> Result<()> {
    if operation == "help" || !CFN_SUBCOMMANDS.contains(&operation) {
        // We only handle "deploy" right now; return None-like to signal
        // the driver should fall through to normal dispatch.
        bail!("__cfn_passthrough__");
    }

    match operation {
        "deploy" => handle_deploy(args).await,
        _ => bail!("Unknown cloudformation subcommand: {}", operation),
    }
}

/// Check if this cloudformation operation is one we handle as a custom command.
pub fn is_custom_command(operation: &str) -> bool {
    CFN_SUBCOMMANDS.contains(&operation)
}

/// Handle `raws cloudformation deploy`.
async fn handle_deploy(args: &GlobalArgs) -> Result<()> {
    // Parse deploy-specific arguments
    let deploy_args = deploy::parse_deploy_args(&args.args)
        .context("Failed to parse cloudformation deploy arguments")?;

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
            "[debug] cloudformation deploy: region={} stack={}",
            region, deploy_args.stack_name
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
            "cloudformation",
            &region,
            None,
            &variant_tags,
        )?,
    };

    if args.debug {
        eprintln!("[debug] cloudformation endpoint: {}", endpoint_url);
    }

    // Run the deploy
    deploy::run_deploy(&deploy_args, credentials, region, endpoint_url, args.debug).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_custom_command_deploy() {
        assert!(is_custom_command("deploy"));
    }

    #[test]
    fn test_is_custom_command_unknown() {
        assert!(!is_custom_command("describe-stacks"));
        assert!(!is_custom_command("create-stack"));
        assert!(!is_custom_command("help"));
    }
}
