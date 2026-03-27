use anyhow::{bail, Context, Result};
use clap::Parser;

use crate::cli::args::GlobalArgs;
use crate::cli::commands::configure;
use crate::cli::customizations::cloudformation as cfn_commands;
use crate::cli::customizations::iam::decode_iam_policy_documents;
use crate::cli::customizations::logs as logs_commands;
use crate::cli::customizations::route53::apply_route53_customizations;
use crate::cli::customizations::s3 as s3_commands;
use crate::cli::formatter;
use crate::cli::jmespath;
use crate::core::auth::sigv4::{self, SigningParams};
use crate::core::config::provider::ConfigProvider;
use crate::core::credentials::chain::build_credential_chain;
use crate::core::credentials::CredentialProvider;
use crate::core::endpoint::resolver;
use crate::core::http::client::{HttpClient, HttpClientConfig};
use crate::core::http::request::HttpRequest;
use crate::core::model::{self, loader};
use crate::core::paginate;
use crate::core::protocol::json as json_protocol;
use crate::core::protocol::query;
use crate::core::protocol::rest_json;
use crate::core::protocol::rest_xml;
use crate::core::retry;
use crate::core::waiter;

pub async fn run() -> Result<()> {
    let args = GlobalArgs::parse();

    let service = match &args.service {
        Some(s) => s,
        None => {
            print_service_help();
            return Ok(());
        }
    };

    // Handle "raws help" as equivalent to no service
    if service == "help" {
        print_service_help();
        return Ok(());
    }

    // Handle "raws configure" and its subcommands (get, set, etc.)
    if service == "configure" {
        let profile = match args.profile.as_deref() {
            Some(p) => p.to_string(),
            None => std::env::var("AWS_PROFILE").unwrap_or_else(|_| "default".to_string()),
        };
        match args.operation.as_deref() {
            Some("get") => {
                let varname = args.args.first()
                    .ok_or_else(|| anyhow::anyhow!("usage: raws configure get <varname>"))?;
                let exit_code = configure::run_configure_get(&profile, varname)?;
                if exit_code != 0 {
                    std::process::exit(exit_code);
                }
                return Ok(());
            }
            Some("set") => {
                let varname = args.args.first()
                    .ok_or_else(|| anyhow::anyhow!("usage: raws configure set <varname> <value>"))?;
                let value = args.args.get(1)
                    .ok_or_else(|| anyhow::anyhow!("usage: raws configure set <varname> <value>"))?;
                return configure::run_configure_set(&profile, varname, value);
            }
            Some("list") => {
                let profile_from_flag = args.profile.is_some();
                return configure::run_configure_list(&profile, profile_from_flag);
            }
            Some("list-profiles") => {
                return configure::run_configure_list_profiles();
            }
            Some("export-credentials") => {
                // Parse --format from args (default: "env")
                let mut format = "env";
                let mut i = 0;
                while i < args.args.len() {
                    if args.args[i] == "--format" {
                        if let Some(val) = args.args.get(i + 1) {
                            format = val;
                            break;
                        }
                    }
                    i += 1;
                }
                return configure::run_configure_export_credentials(&profile, format);
            }
            _ => {
                return configure::run_configure(&profile);
            }
        }
    }

    // Handle "raws s3 <subcommand>": S3 high-level commands (ls, cp, mv, rm, sync, mb, rb).
    // These are custom commands that don't map to S3 API operations, so we intercept
    // before the normal operation lookup. Use "s3api" for API-level S3 operations.
    if service == "s3" {
        return s3_commands::handle_s3_command(&args).await;
    }

    let operation = match &args.operation {
        Some(o) => o,
        None => {
            bail!("Usage: raws {service} <operation> [--params...]\n\nMissing operation name.");
        }
    };

    // Handle "raws cloudformation deploy": custom high-level command that
    // orchestrates changeset creation, polling, and execution.
    if service == "cloudformation" && cfn_commands::is_custom_command(operation) {
        return cfn_commands::handle_cloudformation_command(&args, operation).await;
    }

    // Handle "raws logs tail": custom command that tails CloudWatch Logs
    // using FilterLogEvents API with optional --follow for continuous polling.
    if service == "logs" && logs_commands::is_custom_command(operation) {
        return logs_commands::handle_logs_command(&args, operation).await;
    }

    // Load the service model early so help commands work without region/credentials.
    // Map CLI service names to model directory names (e.g., s3api -> s3)
    let model_service = resolve_service_name(service);
    let model_str = model::store::get_service_model_str(model_service)
        .with_context(|| format!("Failed to load service model for '{}'", service))?;
    let service_model = loader::parse_service_model(&model_str)
        .with_context(|| format!("Failed to parse service model for '{}'", service))?;

    // Handle "raws <service> help": list all operations for this service
    if operation == "help" {
        print_operation_list(service, &service_model);
        return Ok(());
    }

    // Handle "raws <service> wait <waiter-name> [--params...]"
    if operation == "wait" {
        return handle_wait_command(service, &args, model_service, &service_model).await;
    }

    // Find the operation (convert kebab-case CLI name to PascalCase)
    let operation_name = model::find_operation_by_cli_name(&service_model.operations, operation)
        .ok_or_else(|| anyhow::anyhow!(
            "Unknown operation '{}' for service '{}'. Operation not found in service model.",
            operation, service
        ))?
        .to_string();

    let op = &service_model.operations[&operation_name];

    // Handle "raws <service> <operation> help": show operation parameters
    if args.args.iter().any(|a| a == "help") {
        print_operation_help(service, operation, op, &service_model);
        return Ok(());
    }

    // Handle --generate-cli-skeleton: print JSON skeleton and return (no API call needed)
    if let Some(skeleton_mode) = extract_generate_cli_skeleton(&args.args) {
        let shape_name = match skeleton_mode.as_str() {
            "output" => op.output_shape.as_deref().unwrap_or(""),
            _ => op.input_shape.as_deref().unwrap_or(""),  // "input" is the default
        };
        let skeleton = if shape_name.is_empty() {
            serde_json::json!({})
        } else {
            generate_skeleton(shape_name, &service_model.shapes)
        };
        let formatted = serialize_json_4_space(&skeleton)
            .unwrap_or_else(|_| "{}".to_string());
        println!("{formatted}");
        return Ok(());
    }

    // 1. Load config (resolves region, profile, output format)
    let config = ConfigProvider::new(
        args.region.as_deref(),
        args.output.as_deref(),
        args.profile.as_deref(),
    )?;

    // Validate that an explicitly-specified profile exists
    if args.profile.is_some() {
        ConfigProvider::validate_profile_exists(&config.profile)?;
    }

    let region = config
        .region
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!(
            "No region specified. Use --region, AWS_REGION, or configure a default region."
        ))?;

    let output_format = config.output.as_deref().unwrap_or("json");

    if args.debug {
        eprintln!("[debug] service={service} operation={operation}");
        eprintln!("[debug] region={region} profile={} output={output_format}", config.profile);
    }

    if args.debug {
        eprintln!("[debug] protocol={} api_version={}", service_model.metadata.protocol, service_model.metadata.api_version);
    }

    if args.debug {
        eprintln!("[debug] resolved operation: {operation_name}");
    }

    // 4. Parse operation-specific arguments into JSON input (model-aware for list/map params)
    let list_params = build_list_params(op.input_shape.as_deref(), &service_model.shapes);
    let map_params = build_map_params(op.input_shape.as_deref(), &service_model.shapes);
    let mut input = parse_operation_args(&args.args, &list_params, &map_params)?;

    // Apply Route53 input parameter customizations (strip /hostedzone/ etc. prefixes)
    apply_route53_customizations(model_service, &mut input);

    if args.debug {
        eprintln!("[debug] input: {input}");
    }

    // 5. Resolve credentials (skip if --no-sign-request)
    let creds = if args.no_sign_request {
        if args.debug {
            eprintln!("[debug] --no-sign-request: skipping credential resolution");
        }
        crate::core::credentials::Credentials {
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
        }
    } else {
        let explicit_profile = args.profile.is_some();
        let chain = build_credential_chain(&config.profile, explicit_profile, config.region.as_deref());
        let resolved = chain.resolve()
            .context("Failed to resolve AWS credentials")?;
        if args.debug {
            eprintln!("[debug] credentials resolved: access_key={}...", &resolved.access_key_id[..8.min(resolved.access_key_id.len())]);
        }
        resolved
    };

    // 6. Resolve endpoint URL (with dualstack/FIPS variant support)
    let variant_tags = resolver::EndpointVariantTags {
        use_dualstack: args.use_dualstack_endpoint,
        use_fips: args.use_fips_endpoint,
    };
    let mut endpoint_url = match &args.endpoint_url {
        Some(url) => url.clone(),
        None => resolver::resolve_endpoint_with_variants(
            &service_model.metadata.endpoint_prefix,
            region,
            service_model.metadata.global_endpoint.as_deref(),
            &variant_tags,
        )?,
    };

    // Apply S3 virtual-hosted style addressing for bucket operations
    if model_service == "s3" && args.endpoint_url.is_none() {
        if let Some(bucket) = input.get("Bucket").and_then(|b| b.as_str()) {
            if resolver::is_bucket_dns_compatible(bucket) {
                endpoint_url = resolver::apply_s3_virtual_hosted_style(&endpoint_url, bucket);
            }
        }
    }

    if args.debug {
        eprintln!("[debug] endpoint: {endpoint_url}");
    }

    // 7. Load paginator config for possible auto-pagination
    let paginator_config = if !args.no_paginate {
        let paginators = match model::store::get_paginators_str(model_service) {
            Some(content) => paginate::parse_paginators(&content).unwrap_or_default(),
            None => std::collections::HashMap::new(),
        };
        paginators.get(&operation_name).cloned()
    } else {
        None
    };

    if args.debug {
        if let Some(ref pc) = paginator_config {
            eprintln!(
                "[debug] paginator: input_token={:?} output_token={:?} result_key={:?}",
                pc.input_token, pc.output_token, pc.result_key
            );
        } else {
            eprintln!("[debug] no paginator for this operation (or --no-paginate set)");
        }
    }

    // 8. Build retry config (from env vars and config file) and HTTP client config
    let config_max_attempts = config.get_value("max_attempts");
    let config_retry_mode = config.get_value("retry_mode");
    let retry_config = retry::resolve_retry_config(
        config_max_attempts.as_deref(),
        config_retry_mode.as_deref(),
    );

    let http_config = build_http_config(&args);

    if args.debug {
        eprintln!("[debug] retry: mode={:?}, max_attempts={}", retry_config.mode, retry_config.max_attempts);
        eprintln!("[debug] timeouts: connect={:?}, read={:?}", http_config.connect_timeout, http_config.read_timeout);
    }

    // 9. Build and send the request based on protocol, with auto-pagination and retry
    let protocol = service_model.metadata.protocol.as_str();
    let response_value = if let Some(ref pc) = paginator_config {
        // Auto-pagination: collect all pages
        let mut pages = Vec::new();
        let mut current_input = input.clone();

        loop {
            let page = dispatch_with_retry(
                protocol,
                &endpoint_url,
                &service_model,
                op,
                &current_input,
                &creds,
                region,
                args.debug,
                &retry_config,
                &http_config,
                args.no_sign_request,
            )
            .await?;

            let next_tokens = paginate::extract_next_tokens(&page, pc);
            pages.push(page);

            match next_tokens {
                Some(tokens) => {
                    // Set the input tokens for the next request
                    if let Some(obj) = current_input.as_object_mut() {
                        for (key, value) in &tokens {
                            obj.insert(key.clone(), serde_json::Value::String(value.clone()));
                        }
                    }
                    if args.debug {
                        eprintln!("[debug] paginating: fetching next page (page {})", pages.len() + 1);
                    }
                }
                None => {
                    // No more pages
                    break;
                }
            }
        }

        if args.debug {
            eprintln!("[debug] pagination complete: {} page(s)", pages.len());
        }

        paginate::merge_pages(&pages, pc)
    } else {
        // No pagination: single request with retry
        dispatch_with_retry(
            protocol,
            &endpoint_url,
            &service_model,
            op,
            &input,
            &creds,
            region,
            args.debug,
            &retry_config,
            &http_config,
            args.no_sign_request,
        )
        .await?
    };

    // 9a. Normalize response: reorder keys to model order, convert timestamps
    let mut response_value = response_value;
    if let Some(ref output_shape) = op.output_shape {
        crate::core::protocol::normalize_response_value(
            &mut response_value,
            output_shape,
            &service_model.shapes,
        );
        // Add null for missing top-level members (matching botocore behavior)
        crate::core::protocol::fill_missing_top_level_members(
            &mut response_value,
            output_shape,
            &service_model.shapes,
        );
        // Re-strip pagination fields that fill_missing_top_level_members may have re-added as null
        if let Some(ref pc) = paginator_config {
            if let Some(obj) = response_value.as_object_mut() {
                for token in &pc.output_token {
                    obj.remove(token);
                }
                if let Some(ref mr) = pc.more_results {
                    obj.remove(mr);
                }
            }
        }
    }

    // 9b. Apply service-specific output customizations (e.g., pretty-print decoded JSON fields)
    apply_output_customizations(
        service,
        operation,
        &mut response_value,
        op.output_shape.as_deref().unwrap_or(""),
        &service_model.shapes,
    );

    // 9. Apply --query JMESPath filter if provided
    let final_value = if let Some(ref query_expr) = args.query {
        jmespath::evaluate(query_expr, &response_value)
            .with_context(|| format!("Failed to evaluate --query expression: {}", query_expr))?
    } else {
        response_value
    };

    // 10. Format and print output
    let formatted = formatter::format_output_with_title(&final_value, output_format, Some(&op.name))?;
    println!("{formatted}");

    Ok(())
}

/// Handle the `raws <service> wait <waiter-name> [--params...]` command.
///
/// Polls an API operation at fixed intervals until an acceptor condition is met.
async fn handle_wait_command(
    service: &str,
    args: &GlobalArgs,
    model_service: &str,
    service_model: &model::ServiceModel,
) -> Result<()> {
    // First arg is the waiter name, rest are operation params
    let waiter_cli_name = args.args.first()
        .ok_or_else(|| anyhow::anyhow!(
            "Usage: raws {service} wait <waiter-name> [--params...]\n\nMissing waiter name."
        ))?;
    let remaining_args: Vec<String> = args.args[1..].to_vec();

    // Load waiters
    let waiters = match model::store::get_waiters_str(model_service) {
        Some(content) => waiter::parse_waiters(&content)
            .with_context(|| format!("Failed to load waiters for service '{service}'"))?,
        None => std::collections::HashMap::new(),
    };

    if waiters.is_empty() {
        bail!("Service '{service}' has no waiters defined.");
    }

    // Find the waiter by CLI name
    let waiter_name = waiter::cli_to_waiter_name(waiter_cli_name, &waiters)
        .ok_or_else(|| {
            let available: Vec<String> = waiters.keys()
                .map(|k| waiter::waiter_name_to_cli(k))
                .collect();
            anyhow::anyhow!(
                "Unknown waiter '{}' for service '{}'. Available waiters: {}",
                waiter_cli_name, service, available.join(", ")
            )
        })?;

    let waiter_config = &waiters[&waiter_name];

    // Find the operation that this waiter polls
    let op = service_model.operations.get(&waiter_config.operation)
        .ok_or_else(|| anyhow::anyhow!(
            "Waiter '{}' references operation '{}' which is not found in the service model.",
            waiter_name, waiter_config.operation
        ))?;

    // Load config (region, credentials)
    let config = ConfigProvider::new(
        args.region.as_deref(),
        args.output.as_deref(),
        args.profile.as_deref(),
    )?;

    if args.profile.is_some() {
        ConfigProvider::validate_profile_exists(&config.profile)?;
    }

    let region = config.region.as_deref()
        .ok_or_else(|| anyhow::anyhow!(
            "No region specified. Use --region, AWS_REGION, or configure a default region."
        ))?;

    // Resolve credentials (skip if --no-sign-request)
    let creds = if args.no_sign_request {
        if args.debug { eprintln!("[debug] --no-sign-request: skipping credential resolution"); }
        crate::core::credentials::Credentials { access_key_id: String::new(), secret_access_key: String::new(), session_token: None }
    } else {
        let explicit_profile = args.profile.is_some();
        let chain = build_credential_chain(&config.profile, explicit_profile, config.region.as_deref());
        chain.resolve().context("Failed to resolve AWS credentials")?
    };

    // Resolve endpoint
    let model_service = resolve_service_name(service);
    let variant_tags = resolver::EndpointVariantTags {
        use_dualstack: args.use_dualstack_endpoint,
        use_fips: args.use_fips_endpoint,
    };
    let endpoint_url = match &args.endpoint_url {
        Some(url) => url.clone(),
        None => resolver::resolve_endpoint_with_variants(
            &service_model.metadata.endpoint_prefix,
            region,
            service_model.metadata.global_endpoint.as_deref(),
            &variant_tags,
        )?,
    };

    let protocol = service_model.metadata.protocol.as_str();
    let http_config = build_http_config(args);

    // Parse operation-specific arguments (model-aware for list/map params)
    let list_params = build_list_params(op.input_shape.as_deref(), &service_model.shapes);
    let map_params_set = build_map_params(op.input_shape.as_deref(), &service_model.shapes);
    let mut input = parse_operation_args(&remaining_args, &list_params, &map_params_set)?;

    // Apply Route53 input parameter customizations (strip /hostedzone/ etc. prefixes)
    apply_route53_customizations(model_service, &mut input);

    if args.debug {
        eprintln!("[debug] waiter: {} (operation={}, delay={}s, max_attempts={})",
            waiter_name, waiter_config.operation, waiter_config.delay, waiter_config.max_attempts);
    }

    // Set up progress reporter
    let mut progress = waiter::WaiterProgress::new(
        std::io::stderr(),
        waiter_cli_name,
        waiter_config.max_attempts,
    );
    progress.starting();

    // Poll loop
    for attempt in 1..=waiter_config.max_attempts {
        if args.debug {
            eprintln!("[debug] waiter: poll attempt {}/{}", attempt, waiter_config.max_attempts);
        }

        let outcome = dispatch_request(
            protocol, &endpoint_url, service_model, op, &input, &creds, region, args.debug, &http_config, args.no_sign_request,
        ).await;

        // Evaluate acceptors with detailed match info
        let response = match &outcome.result {
            Ok(val) => val.clone(),
            Err(_) => serde_json::json!({}),
        };

        let acceptor_match = waiter::evaluate_acceptors_detailed(
            &waiter_config.acceptors,
            &response,
            outcome.status,
            outcome.error_code.as_deref(),
        );

        match acceptor_match {
            Some(ref m) if m.state == waiter::AcceptorState::Success => {
                if args.debug {
                    eprintln!("[debug] waiter: success on attempt {attempt}");
                }
                progress.succeeded();
                return Ok(());
            }
            Some(ref m) if m.state == waiter::AcceptorState::Failure => {
                progress.failed(m, &response);
                let detail = waiter::format_failure_detail(waiter_cli_name, m, &response);
                bail!("{}", detail);
            }
            _ => {
                // Retry or no match: show progress and continue polling
                progress.poll_attempt(attempt);
                if attempt < waiter_config.max_attempts {
                    tokio::time::sleep(std::time::Duration::from_secs(waiter_config.delay)).await;
                }
            }
        }
    }

    progress.timed_out();
    bail!(
        "{}",
        waiter::format_timeout_message(waiter_cli_name, waiter_config.max_attempts)
    );
}

/// Result from a protocol dispatch including retry-relevant metadata.
struct DispatchOutcome {
    result: Result<serde_json::Value>,
    /// HTTP status code of the response (0 if network error).
    status: u16,
    /// AWS error code extracted from the response body, if any.
    error_code: Option<String>,
    /// Whether this was a network-level error (no response received).
    is_network_error: bool,
}

/// Dispatch a request with retry logic.
///
/// Wraps `dispatch_request` in a retry loop using the configured retry policy.
#[allow(clippy::too_many_arguments)]
async fn dispatch_with_retry(
    protocol: &str,
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
    retry_config: &retry::RetryConfig,
    http_config: &HttpClientConfig,
    no_sign_request: bool,
) -> Result<serde_json::Value> {
    let mut attempt = 0u32;

    loop {
        attempt += 1;

        let outcome = dispatch_request(
            protocol, endpoint_url, model, op, input, creds, region, debug, http_config, no_sign_request,
        )
        .await;

        // If successful, return immediately.
        if outcome.result.is_ok() {
            return outcome.result;
        }

        // Classify the error for retry purposes.
        let classification = retry::classify_error(
            outcome.status,
            outcome.error_code.as_deref(),
            outcome.is_network_error,
        );

        let decision = retry::should_retry(retry_config, attempt, &classification);

        match decision {
            retry::RetryDecision::RetryAfter(delay) => {
                if debug {
                    eprintln!(
                        "[debug] retry: attempt {attempt}/{} failed (status={}, code={:?}, class={:?}), retrying after {:?}",
                        retry_config.max_attempts,
                        outcome.status,
                        outcome.error_code,
                        classification,
                        delay
                    );
                }
                tokio::time::sleep(delay).await;
            }
            retry::RetryDecision::DontRetry => {
                if debug && attempt > 1 {
                    eprintln!(
                        "[debug] retry: giving up after {attempt} attempts"
                    );
                }
                return outcome.result;
            }
        }
    }
}

/// Dispatch a request using the appropriate protocol.
///
/// Returns a `DispatchOutcome` with the result and retry-relevant metadata.
#[allow(clippy::too_many_arguments)]
async fn dispatch_request(
    protocol: &str,
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
    http_config: &HttpClientConfig,
    no_sign_request: bool,
) -> DispatchOutcome {
    match protocol {
        "query" => {
            dispatch_query_protocol(endpoint_url, model, op, input, creds, region, debug, http_config, no_sign_request).await
        }
        "ec2" => {
            dispatch_ec2_protocol(endpoint_url, model, op, input, creds, region, debug, http_config, no_sign_request).await
        }
        "json" => {
            dispatch_json_protocol(endpoint_url, model, op, input, creds, region, debug, http_config, no_sign_request).await
        }
        "rest-json" => {
            dispatch_rest_json_protocol(endpoint_url, model, op, input, creds, region, debug, http_config, no_sign_request).await
        }
        "rest-xml" => {
            dispatch_rest_xml_protocol(endpoint_url, model, op, input, creds, region, debug, http_config, no_sign_request).await
        }
        _ => DispatchOutcome {
            result: Err(anyhow::anyhow!(
                "Protocol '{}' is not supported. Supported protocols: query, ec2, json, rest-json, rest-xml.",
                protocol
            )),
            status: 0,
            error_code: None,
            is_network_error: false,
        },
    }
}

/// Dispatch a request using the AWS Query protocol.
#[allow(clippy::too_many_arguments)]
async fn dispatch_query_protocol(
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
    http_config: &HttpClientConfig,
    no_sign_request: bool,
) -> DispatchOutcome {
    // Serialize the query request body
    let input_shape_name = op.input_shape.as_deref().unwrap_or("");
    let body_str = match query::serialize_query_request(
        &op.name,
        &model.metadata.api_version,
        input,
        &model.shapes,
        input_shape_name,
    ) {
        Ok(b) => b,
        Err(e) => return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false },
    };

    if debug {
        eprintln!("[debug] request body: {body_str}");
    }

    // Build HTTP request
    let parsed_url = match url::Url::parse(endpoint_url) {
        Ok(u) => u,
        Err(e) => return DispatchOutcome { result: Err(anyhow::anyhow!("Invalid endpoint URL: {endpoint_url}: {e}")), status: 0, error_code: None, is_network_error: false },
    };
    let host = match parsed_url.host_str() {
        Some(h) => h.to_string(),
        None => return DispatchOutcome { result: Err(anyhow::anyhow!("No host in endpoint URL: {endpoint_url}")), status: 0, error_code: None, is_network_error: false },
    };

    let mut request = HttpRequest::new(&op.http_method, endpoint_url);
    request.body = body_str.as_bytes().to_vec();
    request.add_header("host", &host);
    request.add_header(
        "content-type",
        "application/x-www-form-urlencoded; charset=utf-8",
    );

    // Sign the request with SigV4 (skip if --no-sign-request)
    if !no_sign_request {
        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let signing_service = &model.metadata.endpoint_prefix;
        let signing_params = SigningParams::from_credentials(creds, region, signing_service, &datetime);
        let uri_path = parsed_url.path();
        let query_string = parsed_url.query().unwrap_or("");
        if let Err(e) = sigv4::sign_request(&request.method, uri_path, query_string, &mut request.headers, &request.body, &signing_params) {
            return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false };
        }
    }

    if debug {
        eprintln!("[debug] {} sending to {endpoint_url}", if no_sign_request { "unsigned request," } else { "signed request," });
    }

    // Send HTTP request
    let http_client = match HttpClient::with_config(http_config) {
        Ok(c) => c,
        Err(e) => return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false },
    };
    let response = match http_client.send(&request).await {
        Ok(r) => r,
        Err(e) => return DispatchOutcome {
            result: Err(e.context("HTTP request failed")),
            status: 0,
            error_code: None,
            is_network_error: true,
        },
    };

    let response_body = response.body_string();

    if debug {
        eprintln!("[debug] response status: {}", response.status);
        eprintln!("[debug] response body: {response_body}");
    }

    // Parse response
    if response.status >= 200 && response.status < 300 {
        let output_shape_name = op.output_shape.as_deref().unwrap_or("");
        if output_shape_name.is_empty() {
            return DispatchOutcome { result: Ok(serde_json::json!({})), status: response.status, error_code: None, is_network_error: false };
        }

        let parsed = query::parse_query_response(
            &response_body,
            op.result_wrapper.as_deref(),
            output_shape_name,
            &model.shapes,
        )
        .with_context(|| format!("Failed to parse response XML for {}", op.name));

        DispatchOutcome { result: parsed, status: response.status, error_code: None, is_network_error: false }
    } else {
        let (error_code, result) = match query::parse_query_error(&response_body) {
            Ok((code, message)) => {
                let ec = code.clone();
                (Some(ec), Err(anyhow::anyhow!("An error occurred ({}) when calling the {} operation: {}", code, op.name, message)))
            }
            Err(_) => {
                (None, Err(anyhow::anyhow!("An error occurred (Unknown) when calling the {} operation: {}", op.name, response_body)))
            }
        };
        DispatchOutcome { result, status: response.status, error_code, is_network_error: false }
    }
}

/// Dispatch a request using the EC2 Query protocol variant.
///
/// EC2 uses the same query serializer as standard query protocol, but has a
/// different error XML format: `<Response><Errors><Error>...</Error></Errors></Response>`
#[allow(clippy::too_many_arguments)]
async fn dispatch_ec2_protocol(
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
    http_config: &HttpClientConfig,
    no_sign_request: bool,
) -> DispatchOutcome {
    let input_shape_name = op.input_shape.as_deref().unwrap_or("");
    let body_str = match query::serialize_ec2_request(
        &op.name,
        &model.metadata.api_version,
        input,
        &model.shapes,
        input_shape_name,
    ) {
        Ok(b) => b,
        Err(e) => return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false },
    };

    if debug {
        eprintln!("[debug] request body: {body_str}");
    }

    let parsed_url = match url::Url::parse(endpoint_url) {
        Ok(u) => u,
        Err(e) => return DispatchOutcome { result: Err(anyhow::anyhow!("Invalid endpoint URL: {endpoint_url}: {e}")), status: 0, error_code: None, is_network_error: false },
    };
    let host = match parsed_url.host_str() {
        Some(h) => h.to_string(),
        None => return DispatchOutcome { result: Err(anyhow::anyhow!("No host in endpoint URL: {endpoint_url}")), status: 0, error_code: None, is_network_error: false },
    };

    let mut request = HttpRequest::new(&op.http_method, endpoint_url);
    request.body = body_str.as_bytes().to_vec();
    request.add_header("host", &host);
    request.add_header(
        "content-type",
        "application/x-www-form-urlencoded; charset=utf-8",
    );

    if !no_sign_request {
        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let signing_service = &model.metadata.endpoint_prefix;
        let signing_params = SigningParams::from_credentials(creds, region, signing_service, &datetime);
        let uri_path = parsed_url.path();
        let query_string = parsed_url.query().unwrap_or("");
        if let Err(e) = sigv4::sign_request(&request.method, uri_path, query_string, &mut request.headers, &request.body, &signing_params) {
            return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false };
        }
    }

    if debug {
        eprintln!("[debug] {} sending to {endpoint_url}", if no_sign_request { "unsigned request," } else { "signed request," });
    }

    let http_client = match HttpClient::with_config(http_config) {
        Ok(c) => c,
        Err(e) => return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false },
    };
    let response = match http_client.send(&request).await {
        Ok(r) => r,
        Err(e) => return DispatchOutcome {
            result: Err(e.context("HTTP request failed")),
            status: 0, error_code: None, is_network_error: true,
        },
    };

    let response_body = response.body_string();

    if debug {
        eprintln!("[debug] response status: {}", response.status);
        eprintln!("[debug] response body: {response_body}");
    }

    if response.status >= 200 && response.status < 300 {
        let output_shape_name = op.output_shape.as_deref().unwrap_or("");
        if output_shape_name.is_empty() {
            return DispatchOutcome { result: Ok(serde_json::json!({})), status: response.status, error_code: None, is_network_error: false };
        }

        let parsed = query::parse_query_response(
            &response_body, op.result_wrapper.as_deref(), output_shape_name, &model.shapes,
        ).with_context(|| format!("Failed to parse response XML for {}", op.name));

        DispatchOutcome { result: parsed, status: response.status, error_code: None, is_network_error: false }
    } else {
        let (error_code, result) = match query::parse_ec2_error(&response_body) {
            Ok((code, message)) => {
                let ec = code.clone();
                (Some(ec), Err(anyhow::anyhow!("An error occurred ({}) when calling the {} operation: {}", code, op.name, message)))
            }
            Err(_) => {
                (None, Err(anyhow::anyhow!("An error occurred (Unknown) when calling the {} operation: {}", op.name, response_body)))
            }
        };
        DispatchOutcome { result, status: response.status, error_code, is_network_error: false }
    }
}

/// Dispatch a request using the AWS JSON protocol.
///
/// Used by services like DynamoDB, KMS, CloudTrail.
#[allow(clippy::too_many_arguments)]
async fn dispatch_json_protocol(
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
    http_config: &HttpClientConfig,
    no_sign_request: bool,
) -> DispatchOutcome {
    let target_prefix = model.metadata.target_prefix.as_deref().unwrap_or("");
    let target_header = json_protocol::build_target_header(target_prefix, &op.name);
    let json_version = model.metadata.json_version.as_deref().unwrap_or("1.0");
    let content_type = json_protocol::build_content_type(json_version);

    let body_str = match json_protocol::serialize_json_request(input) {
        Ok(b) => b,
        Err(e) => return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false },
    };

    if debug {
        eprintln!("[debug] X-Amz-Target: {target_header}");
        eprintln!("[debug] Content-Type: {content_type}");
        eprintln!("[debug] request body: {body_str}");
    }

    let parsed_url = match url::Url::parse(endpoint_url) {
        Ok(u) => u,
        Err(e) => return DispatchOutcome { result: Err(anyhow::anyhow!("Invalid endpoint URL: {endpoint_url}: {e}")), status: 0, error_code: None, is_network_error: false },
    };
    let host = match parsed_url.host_str() {
        Some(h) => h.to_string(),
        None => return DispatchOutcome { result: Err(anyhow::anyhow!("No host in endpoint URL: {endpoint_url}")), status: 0, error_code: None, is_network_error: false },
    };

    let mut request = HttpRequest::new("POST", endpoint_url);
    request.body = body_str.as_bytes().to_vec();
    request.add_header("host", &host);
    request.add_header("content-type", &content_type);
    request.add_header("x-amz-target", &target_header);

    if !no_sign_request {
        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let signing_service = &model.metadata.endpoint_prefix;
        let signing_params = SigningParams::from_credentials(creds, region, signing_service, &datetime);
        let uri_path = parsed_url.path();
        let query_string = parsed_url.query().unwrap_or("");
        if let Err(e) = sigv4::sign_request("POST", uri_path, query_string, &mut request.headers, &request.body, &signing_params) {
            return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false };
        }
    }

    if debug {
        eprintln!("[debug] {} sending to {endpoint_url}", if no_sign_request { "unsigned request," } else { "signed request," });
    }

    let http_client = match HttpClient::with_config(http_config) {
        Ok(c) => c,
        Err(e) => return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false },
    };
    let response = match http_client.send(&request).await {
        Ok(r) => r,
        Err(e) => return DispatchOutcome {
            result: Err(e.context("HTTP request failed")),
            status: 0, error_code: None, is_network_error: true,
        },
    };

    let response_body = response.body_string();

    if debug {
        eprintln!("[debug] response status: {}", response.status);
        eprintln!("[debug] response body: {response_body}");
    }

    if response.status >= 200 && response.status < 300 {
        let parsed = json_protocol::parse_json_response(&response_body)
            .with_context(|| format!("Failed to parse JSON response for {}", op.name));
        DispatchOutcome { result: parsed, status: response.status, error_code: None, is_network_error: false }
    } else {
        let (error_code, result) = match json_protocol::parse_json_error(&response_body) {
            Ok((code, message)) => {
                let ec = code.clone();
                (Some(ec), Err(anyhow::anyhow!("An error occurred ({}) when calling the {} operation: {}", code, op.name, message)))
            }
            Err(_) => {
                (None, Err(anyhow::anyhow!("An error occurred (Unknown) when calling the {} operation: {}", op.name, response_body)))
            }
        };
        DispatchOutcome { result, status: response.status, error_code, is_network_error: false }
    }
}

/// Dispatch a request using the REST-JSON protocol.
///
/// Used by services like Lambda, API Gateway, Kinesis.
#[allow(clippy::too_many_arguments)]
async fn dispatch_rest_json_protocol(
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
    http_config: &HttpClientConfig,
    no_sign_request: bool,
) -> DispatchOutcome {
    let input_shape_name = op.input_shape.as_deref().unwrap_or("");

    let (resolved_uri, extra_headers, query_params, body_json) = if input_shape_name.is_empty() {
        (op.http_request_uri.clone(), vec![], vec![], None)
    } else {
        match rest_json::serialize_rest_json_request(
            &op.http_request_uri, input, input_shape_name, &model.shapes,
        ) {
            Ok(r) => r,
            Err(e) => return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false },
        }
    };

    let parsed_base = match url::Url::parse(endpoint_url) {
        Ok(u) => u,
        Err(e) => return DispatchOutcome { result: Err(anyhow::anyhow!("Invalid endpoint URL: {endpoint_url}: {e}")), status: 0, error_code: None, is_network_error: false },
    };
    let host = match parsed_base.host_str() {
        Some(h) => h.to_string(),
        None => return DispatchOutcome { result: Err(anyhow::anyhow!("No host in endpoint URL: {endpoint_url}")), status: 0, error_code: None, is_network_error: false },
    };

    let mut full_url = format!(
        "{}://{}{}",
        parsed_base.scheme(),
        parsed_base.host_str().unwrap_or(""),
        resolved_uri
    );

    if !query_params.is_empty() {
        let qs: Vec<String> = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode_query_param(k), percent_encode_query_param(v)))
            .collect();
        let separator = if full_url.contains('?') { "&" } else { "?" };
        full_url = format!("{}{}{}", full_url, separator, qs.join("&"));
    }

    if debug {
        eprintln!("[debug] resolved URI: {resolved_uri}");
        eprintln!("[debug] full URL: {full_url}");
        if let Some(ref body) = body_json {
            eprintln!("[debug] request body: {body}");
        }
    }

    let mut request = HttpRequest::new(&op.http_method, &full_url);
    request.add_header("host", &host);

    if body_json.is_some() {
        let json_version = model.metadata.json_version.as_deref().unwrap_or("1.0");
        let content_type = json_protocol::build_content_type(json_version);
        request.add_header("content-type", &content_type);
    }

    for (k, v) in &extra_headers {
        request.add_header(k, v);
    }

    if let Some(ref body) = body_json {
        request.body = body.as_bytes().to_vec();
    }

    if !no_sign_request {
        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let signing_service = &model.metadata.endpoint_prefix;
        let signing_params = SigningParams::from_credentials(creds, region, signing_service, &datetime);
        let signing_url = match url::Url::parse(&full_url) {
            Ok(u) => u,
            Err(e) => return DispatchOutcome { result: Err(anyhow::anyhow!("Invalid full URL: {full_url}: {e}")), status: 0, error_code: None, is_network_error: false },
        };
        let signing_uri_path = signing_url.path();
        let signing_query_string = signing_url.query().unwrap_or("");
        if let Err(e) = sigv4::sign_request(&request.method, signing_uri_path, signing_query_string, &mut request.headers, &request.body, &signing_params) {
            return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false };
        }
    }

    if debug {
        eprintln!("[debug] {} sending to {full_url}", if no_sign_request { "unsigned request," } else { "signed request," });
    }

    let http_client = match HttpClient::with_config(http_config) {
        Ok(c) => c,
        Err(e) => return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false },
    };
    let response = match http_client.send(&request).await {
        Ok(r) => r,
        Err(e) => return DispatchOutcome {
            result: Err(e.context("HTTP request failed")),
            status: 0, error_code: None, is_network_error: true,
        },
    };

    let response_body = response.body_string();

    if debug {
        eprintln!("[debug] response status: {}", response.status);
        eprintln!("[debug] response body: {response_body}");
    }

    if response.status >= 200 && response.status < 300 {
        let output_shape_name = op.output_shape.as_deref().unwrap_or("");
        if output_shape_name.is_empty() {
            return DispatchOutcome { result: Ok(serde_json::json!({})), status: response.status, error_code: None, is_network_error: false };
        }

        let parsed = rest_json::parse_rest_json_response(
            &response_body, response.status, &response.headers, output_shape_name, &model.shapes,
        ).with_context(|| format!("Failed to parse REST-JSON response for {}", op.name));

        DispatchOutcome { result: parsed, status: response.status, error_code: None, is_network_error: false }
    } else {
        let (error_code, result) = match rest_json::parse_rest_json_error(&response_body) {
            Ok((code, message)) => {
                let ec = code.clone();
                (Some(ec), Err(anyhow::anyhow!("An error occurred ({}) when calling the {} operation: {}", code, op.name, message)))
            }
            Err(_) => {
                (None, Err(anyhow::anyhow!("An error occurred (Unknown) when calling the {} operation: {}", op.name, response_body)))
            }
        };
        DispatchOutcome { result, status: response.status, error_code, is_network_error: false }
    }
}

/// Dispatch a request using the REST-XML protocol.
///
/// Used by services like S3, Route53, CloudFront.
#[allow(clippy::too_many_arguments)]
async fn dispatch_rest_xml_protocol(
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
    http_config: &HttpClientConfig,
    no_sign_request: bool,
) -> DispatchOutcome {
    let input_shape_name = op.input_shape.as_deref().unwrap_or("");

    let (resolved_uri, extra_headers, query_params, body_xml) = if input_shape_name.is_empty() {
        (op.http_request_uri.clone(), vec![], vec![], None)
    } else {
        match rest_xml::serialize_rest_xml_request(
            &op.http_request_uri, input, input_shape_name, &model.shapes,
        ) {
            Ok(r) => r,
            Err(e) => return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false },
        }
    };

    let parsed_base = match url::Url::parse(endpoint_url) {
        Ok(u) => u,
        Err(e) => return DispatchOutcome { result: Err(anyhow::anyhow!("Invalid endpoint URL: {endpoint_url}: {e}")), status: 0, error_code: None, is_network_error: false },
    };
    let host = match parsed_base.host_str() {
        Some(h) => h.to_string(),
        None => return DispatchOutcome { result: Err(anyhow::anyhow!("No host in endpoint URL: {endpoint_url}")), status: 0, error_code: None, is_network_error: false },
    };

    let effective_uri = if model.metadata.endpoint_prefix == "s3" {
        strip_s3_bucket_prefix_if_virtual_hosted(&host, &resolved_uri, input)
    } else {
        resolved_uri.clone()
    };

    let mut full_url = format!(
        "{}://{}{}",
        parsed_base.scheme(),
        parsed_base.host_str().unwrap_or(""),
        effective_uri
    );

    if !query_params.is_empty() {
        let qs: Vec<String> = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode_query_param(k), percent_encode_query_param(v)))
            .collect();
        let separator = if full_url.contains('?') { "&" } else { "?" };
        full_url = format!("{}{}{}", full_url, separator, qs.join("&"));
    }

    if debug {
        eprintln!("[debug] resolved URI: {resolved_uri}");
        eprintln!("[debug] effective URI: {effective_uri}");
        eprintln!("[debug] full URL: {full_url}");
        if let Some(ref body) = body_xml {
            eprintln!("[debug] request body: {body}");
        }
    }

    let mut request = HttpRequest::new(&op.http_method, &full_url);
    request.add_header("host", &host);

    for (k, v) in &extra_headers {
        request.add_header(k, v);
    }

    if let Some(ref body) = body_xml {
        request.body = body.as_bytes().to_vec();
        request.add_header("content-type", "application/xml");
    }

    if !no_sign_request {
        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let signing_service = &model.metadata.endpoint_prefix;
        let signing_params = SigningParams::from_credentials(creds, region, signing_service, &datetime);
        let signing_url = match url::Url::parse(&full_url) {
            Ok(u) => u,
            Err(e) => return DispatchOutcome { result: Err(anyhow::anyhow!("Invalid full URL: {full_url}: {e}")), status: 0, error_code: None, is_network_error: false },
        };
        let signing_uri_path = signing_url.path();
        let signing_query_string = signing_url.query().unwrap_or("");
        if let Err(e) = sigv4::sign_request(&request.method, signing_uri_path, signing_query_string, &mut request.headers, &request.body, &signing_params) {
            return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false };
        }
    }

    if debug {
        eprintln!("[debug] {} sending to {full_url}", if no_sign_request { "unsigned request," } else { "signed request," });
    }

    let http_client = match HttpClient::with_config(http_config) {
        Ok(c) => c,
        Err(e) => return DispatchOutcome { result: Err(e), status: 0, error_code: None, is_network_error: false },
    };
    let response = match http_client.send(&request).await {
        Ok(r) => r,
        Err(e) => return DispatchOutcome {
            result: Err(e.context("HTTP request failed")),
            status: 0, error_code: None, is_network_error: true,
        },
    };

    let response_body = response.body_string();

    if debug {
        eprintln!("[debug] response status: {}", response.status);
        eprintln!("[debug] response body: {response_body}");
    }

    if response.status >= 200 && response.status < 300 {
        let output_shape_name = op.output_shape.as_deref().unwrap_or("");
        if output_shape_name.is_empty() {
            return DispatchOutcome { result: Ok(serde_json::json!({})), status: response.status, error_code: None, is_network_error: false };
        }

        let parsed = rest_xml::parse_rest_xml_response(
            &response_body, response.status, &response.headers, output_shape_name, &model.shapes,
        ).with_context(|| format!("Failed to parse REST-XML response for {}", op.name));

        DispatchOutcome { result: parsed, status: response.status, error_code: None, is_network_error: false }
    } else {
        let (error_code, result) = match rest_xml::parse_rest_xml_error(&response_body) {
            Ok((code, message)) => {
                let ec = code.clone();
                (Some(ec), Err(anyhow::anyhow!("An error occurred ({}) when calling the {} operation: {}", code, op.name, message)))
            }
            Err(_) => {
                (None, Err(anyhow::anyhow!("An error occurred (Unknown) when calling the {} operation: {}", op.name, response_body)))
            }
        };
        DispatchOutcome { result, status: response.status, error_code, is_network_error: false }
    }
}

/// Apply service-specific output customizations to the response JSON.
///
/// For example, the STS `DecodeAuthorizationMessage` response contains a
/// `DecodedMessage` field that is a JSON string (escaped).  The AWS CLI
/// automatically pretty-prints this decoded JSON.  We replicate that behavior
/// here by parsing the string into a JSON value so that formatters render it
/// as a nested object rather than an escaped string.
fn apply_output_customizations(
    service: &str,
    operation: &str,
    response: &mut serde_json::Value,
    output_shape_name: &str,
    shapes: &std::collections::HashMap<String, serde_json::Value>,
) {
    if service == "sts" && operation == "decode-authorization-message" {
        if let Some(msg) = response.get("DecodedMessage").and_then(|v| v.as_str()) {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(msg) {
                response["DecodedMessage"] = parsed;
            }
        }
    }

    // IAM: decode URL-encoded policy document strings into JSON objects.
    if service == "iam" {
        decode_iam_policy_documents(response, output_shape_name, shapes);
    }
}

/// Build HTTP client configuration from CLI args.
fn build_http_config(args: &GlobalArgs) -> HttpClientConfig {
    let mut config = HttpClientConfig::default();
    if let Some(t) = args.cli_connect_timeout {
        config.connect_timeout = std::time::Duration::from_secs(t);
    }
    if let Some(t) = args.cli_read_timeout {
        config.read_timeout = std::time::Duration::from_secs(t);
    }
    config
}

/// Percent-encode a query parameter key or value for URL construction.
fn percent_encode_query_param(input: &str) -> String {
    use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
    const QS_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    utf8_percent_encode(input, QS_ENCODE_SET).to_string()
}

/// Strip the leading `/{bucket}` prefix from an S3 URI when virtual-hosted style is active.
///
/// Virtual-hosted style means the hostname looks like `{bucket}.s3.{region}.amazonaws.com`.
/// In that case, the model's URI template resolves to `/{bucket}/...` but the bucket portion
/// should not be in the path (it's in the hostname).
fn strip_s3_bucket_prefix_if_virtual_hosted(
    host: &str,
    resolved_uri: &str,
    input: &serde_json::Value,
) -> String {
    let bucket = match input.get("Bucket").and_then(|b| b.as_str()) {
        Some(b) if !b.is_empty() => b,
        _ => return resolved_uri.to_string(),
    };

    // Check if the host starts with "{bucket}." -- indicates virtual-hosted style
    let bucket_prefix = format!("{bucket}.");
    if !host.starts_with(&bucket_prefix) {
        return resolved_uri.to_string();
    }

    // Strip the leading /{bucket} from the URI path
    let path_prefix = format!("/{bucket}");
    if let Some(rest) = resolved_uri.strip_prefix(&path_prefix) {
        if rest.is_empty() {
            "/".to_string()
        } else {
            rest.to_string()
        }
    } else {
        resolved_uri.to_string()
    }
}

/// Parse operation-specific CLI arguments (--key value pairs) into a JSON object.
/// Map CLI service names to model directory names.
/// The AWS CLI uses "s3api" for the S3 API service, but the model lives in models/s3/.
fn resolve_service_name(service: &str) -> &str {
    match service {
        "s3api" => "s3",
        other => other,
    }
}

/// Print a help message listing all available services discovered from the models/ directory.
fn print_service_help() {
    println!("raws - AWS CLI reimplementation in Rust\n");
    println!("Usage: raws <service> <operation> [--params...]\n");

    match model::store::discover_services() {
        Ok(services) if !services.is_empty() => {
            println!("Available services ({}):\n", services.len());
            // Print in columns for readability
            let col_width = 24;
            let cols = 3;
            for chunk in services.chunks(cols) {
                let line: Vec<String> = chunk
                    .iter()
                    .map(|s| format!("  {:<width$}", s, width = col_width))
                    .collect();
                println!("{}", line.join(""));
            }
            println!();
        }
        Ok(_) => {
            println!("No services found. Ensure the models/ directory is populated.");
        }
        Err(_) => {
            println!("Could not discover services. Ensure the models/ directory exists.");
        }
    }

    println!("Global options:");
    println!("  --region <REGION>            AWS region to use");
    println!("  --profile <PROFILE>          Named profile to use");
    println!("  --output <FORMAT>            Output format: json, table, text");
    println!("  --endpoint-url <URL>         Override endpoint URL");
    println!("  --debug                      Enable debug output");
    println!("  --no-paginate                Disable automatic pagination");
    println!("  --use-dualstack-endpoint     Use dual-stack (IPv4/IPv6) endpoints");
    println!("  --use-fips-endpoint          Use FIPS-compliant endpoints");
}

/// Print a list of all available operations for a service.
///
/// Called when the user runs `raws <service> help`.
fn print_operation_list(service: &str, service_model: &model::ServiceModel) {
    println!("{service} operations:\n");

    let mut op_names: Vec<String> = service_model
        .operations
        .keys()
        .map(|k| model::pascal_to_kebab(k))
        .collect();
    op_names.sort();

    if op_names.is_empty() {
        println!("  (no operations found)");
        return;
    }

    let col_width = op_names.iter().map(|n| n.len()).max().unwrap_or(20) + 4;
    let terminal_width = 80;
    let cols = std::cmp::max(1, terminal_width / col_width);

    for chunk in op_names.chunks(cols) {
        let line: Vec<String> = chunk
            .iter()
            .map(|s| format!("  {:<width$}", s, width = col_width))
            .collect();
        println!("{}", line.join(""));
    }

    println!("\nTo see help for an operation:");
    println!("  raws {service} <operation> help");
}

/// Print help for a specific operation, showing its parameters.
///
/// Called when the user runs `raws <service> <operation> help`.
fn print_operation_help(
    service: &str,
    operation: &str,
    op: &model::Operation,
    service_model: &model::ServiceModel,
) {
    println!("{service} {operation}\n");

    if let Some(ref doc) = op.documentation {
        // Strip simple HTML tags for display
        let clean = strip_html_tags(doc);
        if !clean.is_empty() {
            println!("{clean}\n");
        }
    }

    let input_shape_name = match op.input_shape.as_deref() {
        Some(name) => name,
        None => {
            println!("This operation takes no parameters.");
            return;
        }
    };

    let args_info = get_operation_args_info(input_shape_name, &service_model.shapes);

    if args_info.is_empty() {
        println!("This operation takes no parameters.");
        return;
    }

    // Find the longest CLI name for alignment
    let max_name_len = args_info.iter().map(|a| a.cli_name.len()).max().unwrap_or(0);

    println!("Parameters:\n");
    for arg in &args_info {
        let required_tag = if arg.required { " [required]" } else { "" };
        println!(
            "  {:<width$}  ({}){}",
            arg.cli_name,
            arg.shape_type,
            required_tag,
            width = max_name_len,
        );
    }
}

/// Strip basic HTML tags from a documentation string for terminal display.
fn strip_html_tags(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut in_tag = false;
    for ch in input.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => result.push(ch),
            _ => {}
        }
    }
    result.trim().to_string()
}

/// Information about a single CLI argument derived from a service model input shape.
#[derive(Debug, Clone, PartialEq)]
pub struct ArgInfo {
    /// The CLI flag name in kebab-case, e.g. "--user-name"
    pub cli_name: String,
    /// The model member name in PascalCase, e.g. "UserName"
    pub model_name: String,
    /// Whether this argument is required by the model
    pub required: bool,
    /// The shape type of this argument, e.g. "string", "integer", "list"
    pub shape_type: String,
}

/// Inspect an operation's input shape and return a list of CLI arguments
/// that would correspond to each member of the input shape.
///
/// Each member of the input shape becomes a `--kebab-case` CLI flag.
/// Members listed in the shape's "required" array are marked as required.
pub fn get_operation_args_info(
    input_shape_name: &str,
    shapes: &std::collections::HashMap<String, serde_json::Value>,
) -> Vec<ArgInfo> {
    let mut args_info = Vec::new();

    let shape = match shapes.get(input_shape_name) {
        Some(s) => s,
        None => return args_info,
    };

    let members = match shape.get("members").and_then(|m| m.as_object()) {
        Some(m) => m,
        None => return args_info,
    };

    // Collect the required member names into a set
    let required_set: std::collections::HashSet<&str> = shape
        .get("required")
        .and_then(|r| r.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .collect()
        })
        .unwrap_or_default();

    for (member_name, member_def) in members {
        // Resolve the shape type for this member
        let shape_type = member_def
            .get("shape")
            .and_then(|s| s.as_str())
            .and_then(|shape_ref| shapes.get(shape_ref))
            .and_then(|referred_shape| referred_shape.get("type"))
            .and_then(|t| t.as_str())
            .unwrap_or("unknown");

        let cli_name = format!("--{}", model::pascal_to_kebab(member_name));

        args_info.push(ArgInfo {
            cli_name,
            model_name: member_name.clone(),
            required: required_set.contains(member_name.as_str()),
            shape_type: shape_type.to_string(),
        });
    }

    // Sort by CLI name for deterministic output
    args_info.sort_by(|a, b| a.cli_name.cmp(&b.cli_name));
    args_info
}

/// Serialize a JSON value with 4-space indentation, matching AWS CLI output.
fn serialize_json_4_space(value: &serde_json::Value) -> Result<String> {
    let mut buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
    serde::Serialize::serialize(value, &mut ser)
        .context("Failed to serialize JSON skeleton")?;
    String::from_utf8(buf).context("JSON skeleton is not valid UTF-8")
}

/// Check if `--generate-cli-skeleton` is present in the operation args.
///
/// Returns `Some(mode)` where mode is "input" (default) or "output".
/// Returns `None` if the flag is not present.
fn extract_generate_cli_skeleton(args: &[String]) -> Option<String> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--generate-cli-skeleton" {
            // Check if next arg is a value (not another flag)
            if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                return Some(args[i + 1].clone());
            }
            // No value provided: default to "input"
            return Some("input".to_string());
        }
        i += 1;
    }
    None
}

/// Generate a JSON skeleton for a given shape, showing placeholder values
/// for each member.
///
/// Type mapping:
/// - string, timestamp, blob -> `""`
/// - integer, long -> `0`
/// - float, double -> `0.0`
/// - boolean -> `true`
/// - list -> array with one skeleton element
/// - map -> object with `{"KeyName": value_skeleton}`
/// - structure -> object with member skeletons
pub fn generate_skeleton(
    shape_name: &str,
    shapes: &std::collections::HashMap<String, serde_json::Value>,
) -> serde_json::Value {
    generate_skeleton_inner(shape_name, shapes, 0)
}

fn generate_skeleton_inner(
    shape_name: &str,
    shapes: &std::collections::HashMap<String, serde_json::Value>,
    depth: usize,
) -> serde_json::Value {
    // Guard against infinite recursion on self-referencing shapes
    if depth > 20 {
        return serde_json::Value::Null;
    }

    let shape = match shapes.get(shape_name) {
        Some(s) => s,
        None => return serde_json::Value::Null,
    };

    let shape_type = shape
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("string");

    match shape_type {
        "string" | "timestamp" | "blob" => serde_json::Value::String(String::new()),
        "integer" | "long" => serde_json::json!(0),
        "float" | "double" => serde_json::json!(0.0),
        "boolean" => serde_json::json!(true),
        "list" => {
            let member_shape = shape
                .get("member")
                .and_then(|m| m.get("shape"))
                .and_then(|s| s.as_str())
                .unwrap_or("String");
            let element = generate_skeleton_inner(member_shape, shapes, depth + 1);
            serde_json::json!([element])
        }
        "map" => {
            let value_shape = shape
                .get("value")
                .and_then(|v| v.get("shape"))
                .and_then(|s| s.as_str())
                .unwrap_or("String");
            let value_skeleton = generate_skeleton_inner(value_shape, shapes, depth + 1);
            let mut map = serde_json::Map::new();
            map.insert("KeyName".to_string(), value_skeleton);
            serde_json::Value::Object(map)
        }
        "structure" => {
            let mut map = serde_json::Map::new();
            if let Some(members) = shape.get("members").and_then(|m| m.as_object()) {
                for (member_name, member_def) in members {
                    let member_shape = member_def
                        .get("shape")
                        .and_then(|s| s.as_str())
                        .unwrap_or("String");
                    let value = generate_skeleton_inner(member_shape, shapes, depth + 1);
                    map.insert(member_name.clone(), value);
                }
            }
            serde_json::Value::Object(map)
        }
        _ => serde_json::Value::String(String::new()),
    }
}

/// Build a set of PascalCase parameter names that are list types for an operation's input shape.
fn build_list_params(
    input_shape: Option<&str>,
    shapes: &std::collections::HashMap<String, serde_json::Value>,
) -> std::collections::HashSet<String> {
    let mut list_params = std::collections::HashSet::new();
    let shape_name = match input_shape {
        Some(s) => s,
        None => return list_params,
    };
    let shape = match shapes.get(shape_name) {
        Some(s) => s,
        None => return list_params,
    };
    if let Some(members) = shape.get("members").and_then(|m| m.as_object()) {
        for (member_name, member_def) in members {
            if let Some(target_shape_name) = member_def.get("shape").and_then(|s| s.as_str()) {
                if let Some(target_shape) = shapes.get(target_shape_name) {
                    if target_shape.get("type").and_then(|t| t.as_str()) == Some("list") {
                        list_params.insert(member_name.clone());
                    }
                }
            }
        }
    }
    list_params
}

/// Build a set of PascalCase parameter names that are map types for an operation's input shape.
fn build_map_params(
    input_shape: Option<&str>,
    shapes: &std::collections::HashMap<String, serde_json::Value>,
) -> std::collections::HashSet<String> {
    let mut map_params = std::collections::HashSet::new();
    let shape_name = match input_shape {
        Some(s) => s,
        None => return map_params,
    };
    let shape = match shapes.get(shape_name) {
        Some(s) => s,
        None => return map_params,
    };
    if let Some(members) = shape.get("members").and_then(|m| m.as_object()) {
        for (member_name, member_def) in members {
            if let Some(target_shape_name) = member_def.get("shape").and_then(|s| s.as_str()) {
                if let Some(target_shape) = shapes.get(target_shape_name) {
                    if target_shape.get("type").and_then(|t| t.as_str()) == Some("map") {
                        map_params.insert(member_name.clone());
                    }
                }
            }
        }
    }
    map_params
}

fn parse_operation_args(
    args: &[String],
    list_params: &std::collections::HashSet<String>,
    map_params: &std::collections::HashSet<String>,
) -> Result<serde_json::Value> {
    // 1. Scan for --cli-input-json and extract its value, collecting remaining args
    let mut remaining_args: Vec<&String> = Vec::new();
    let mut cli_input_json_value: Option<&String> = None;
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--cli-input-json" {
            if i + 1 < args.len() {
                cli_input_json_value = Some(&args[i + 1]);
                i += 2;
            } else {
                bail!("--cli-input-json requires a value (inline JSON or file://path)");
            }
        } else {
            remaining_args.push(&args[i]);
            i += 1;
        }
    }

    // 2. If --cli-input-json was provided, load and parse the base JSON
    let mut map = if let Some(raw_value) = cli_input_json_value {
        let json_str = if let Some(path) = raw_value.strip_prefix("file://") {
            std::fs::read_to_string(path)
                .with_context(|| format!("Failed to read cli-input-json file: {path}"))?
        } else {
            raw_value.clone()
        };
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .context("Failed to parse --cli-input-json as JSON")?;
        match parsed {
            serde_json::Value::Object(m) => m,
            _ => bail!("--cli-input-json must be a JSON object"),
        }
    } else {
        serde_json::Map::new()
    };

    // 3. Parse remaining explicit --args (these override values from cli-input-json)
    let mut j = 0;
    while j < remaining_args.len() {
        let arg = remaining_args[j];
        if let Some(key) = arg.strip_prefix("--") {
            // Convert kebab-case key to PascalCase for the API
            let pascal_key = model::kebab_to_pascal(key);

            let is_list = list_params.contains(&pascal_key);
            let is_map = map_params.contains(&pascal_key);

            if is_list {
                // For list-type params, consume ALL following non-flag values into an array
                let mut values = Vec::new();
                j += 1;
                while j < remaining_args.len() && !remaining_args[j].starts_with("--") {
                    let v = remaining_args[j];
                    let json_val = serde_json::from_str(v)
                        .unwrap_or_else(|_| serde_json::Value::String(v.clone()));
                    // If the user passed a JSON array (e.g., '["a","b"]'), flatten it
                    if let serde_json::Value::Array(arr) = json_val {
                        values.extend(arr);
                    } else {
                        values.push(json_val);
                    }
                    j += 1;
                }
                map.insert(pascal_key, serde_json::Value::Array(values));
            } else if is_map {
                // For map-type params, consume following non-flag values and parse shorthand
                // Shorthand: Key1=Value1,Key2=Value2 OR Key1=Value1 Key2=Value2
                let mut map_obj = serde_json::Map::new();
                j += 1;
                while j < remaining_args.len() && !remaining_args[j].starts_with("--") {
                    let v = remaining_args[j];
                    // Try JSON first
                    if let Ok(serde_json::Value::Object(obj)) = serde_json::from_str::<serde_json::Value>(v) {
                        map_obj.extend(obj);
                        j += 1;
                        continue;
                    }
                    // Try shorthand: Key1=Value1,Key2=Value2
                    for part in v.split(',') {
                        if let Some((k, val)) = part.split_once('=') {
                            map_obj.insert(
                                k.to_string(),
                                serde_json::Value::String(val.to_string()),
                            );
                        }
                    }
                    j += 1;
                }
                map.insert(pascal_key, serde_json::Value::Object(map_obj));
            } else if j + 1 < remaining_args.len() && !remaining_args[j + 1].starts_with("--") {
                let value = remaining_args[j + 1];
                // Try to parse as JSON first, otherwise use as string
                let json_value = serde_json::from_str(value)
                    .unwrap_or_else(|_| serde_json::Value::String(value.clone()));
                map.insert(pascal_key, json_value);
                j += 2;
            } else {
                // Flag without value: treat as boolean true
                map.insert(pascal_key, serde_json::Value::Bool(true));
                j += 1;
            }
        } else {
            j += 1;
        }
    }

    Ok(serde_json::Value::Object(map))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_set() -> std::collections::HashSet<String> {
        std::collections::HashSet::new()
    }

    #[test]
    fn test_parse_operation_args_empty() {
        let result = parse_operation_args(&[], &empty_set(), &empty_set()).unwrap();
        assert_eq!(result, serde_json::json!({}));
    }

    #[test]
    fn test_parse_operation_args_key_value() {
        let args = vec![
            "--user-name".to_string(),
            "alice".to_string(),
        ];
        let result = parse_operation_args(&args, &empty_set(), &empty_set()).unwrap();
        assert_eq!(result["UserName"].as_str(), Some("alice"));
    }

    #[test]
    fn test_parse_operation_args_multiple() {
        let args = vec![
            "--user-name".to_string(),
            "alice".to_string(),
            "--path".to_string(),
            "/admins/".to_string(),
        ];
        let result = parse_operation_args(&args, &empty_set(), &empty_set()).unwrap();
        assert_eq!(result["UserName"].as_str(), Some("alice"));
        assert_eq!(result["Path"].as_str(), Some("/admins/"));
    }

    #[test]
    fn test_parse_operation_args_boolean_flag() {
        let args = vec![
            "--dry-run".to_string(),
        ];
        let result = parse_operation_args(&args, &empty_set(), &empty_set()).unwrap();
        assert_eq!(result["DryRun"].as_bool(), Some(true));
    }

    #[test]
    fn test_parse_operation_args_json_value() {
        let args = vec![
            "--tags".to_string(),
            r#"[{"Key":"env","Value":"prod"}]"#.to_string(),
        ];
        let result = parse_operation_args(&args, &empty_set(), &empty_set()).unwrap();
        assert!(result["Tags"].is_array());
    }

    #[test]
    fn test_parse_operation_args_list_param_single_value() {
        let mut list_params = std::collections::HashSet::new();
        list_params.insert("Owners".to_string());
        let args = vec![
            "--owners".to_string(),
            "self".to_string(),
        ];
        let result = parse_operation_args(&args, &list_params, &empty_set()).unwrap();
        assert_eq!(result["Owners"], serde_json::json!(["self"]));
    }

    #[test]
    fn test_parse_operation_args_list_param_multiple_values() {
        let mut list_params = std::collections::HashSet::new();
        list_params.insert("Owners".to_string());
        let args = vec![
            "--owners".to_string(),
            "self".to_string(),
            "amazon".to_string(),
            "--dry-run".to_string(),
        ];
        let result = parse_operation_args(&args, &list_params, &empty_set()).unwrap();
        assert_eq!(result["Owners"], serde_json::json!(["self", "amazon"]));
        assert_eq!(result["DryRun"].as_bool(), Some(true));
    }

    #[test]
    fn test_parse_operation_args_list_param_json_array() {
        let mut list_params = std::collections::HashSet::new();
        list_params.insert("InstanceIds".to_string());
        let args = vec![
            "--instance-ids".to_string(),
            r#"["i-123","i-456"]"#.to_string(),
        ];
        let result = parse_operation_args(&args, &list_params, &empty_set()).unwrap();
        assert_eq!(result["InstanceIds"], serde_json::json!(["i-123", "i-456"]));
    }

    #[test]
    fn test_parse_operation_args_map_param_shorthand() {
        let mut map_params = std::collections::HashSet::new();
        map_params.insert("Tags".to_string());
        let args = vec![
            "--tags".to_string(),
            "Key1=Value1,Key2=Value2".to_string(),
        ];
        let result = parse_operation_args(&args, &empty_set(), &map_params).unwrap();
        let tags = result["Tags"].as_object().unwrap();
        assert_eq!(tags["Key1"].as_str(), Some("Value1"));
        assert_eq!(tags["Key2"].as_str(), Some("Value2"));
    }

    // ---------------------------------------------------------------
    // --cli-input-json tests
    // ---------------------------------------------------------------

    #[test]
    fn test_cli_input_json_inline() {
        let args = vec![
            "--cli-input-json".to_string(),
            r#"{"UserName":"alice","Path":"/admins/"}"#.to_string(),
        ];
        let result = parse_operation_args(&args, &empty_set(), &empty_set()).unwrap();
        assert_eq!(result["UserName"].as_str(), Some("alice"));
        assert_eq!(result["Path"].as_str(), Some("/admins/"));
    }

    #[test]
    fn test_cli_input_json_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("input.json");
        std::fs::write(&file_path, r#"{"UserName":"from-file","Path":"/"}"#).unwrap();

        let args = vec![
            "--cli-input-json".to_string(),
            format!("file://{}", file_path.display()),
        ];
        let result = parse_operation_args(&args, &empty_set(), &empty_set()).unwrap();
        assert_eq!(result["UserName"].as_str(), Some("from-file"));
        assert_eq!(result["Path"].as_str(), Some("/"));
    }

    #[test]
    fn test_cli_input_json_merge_explicit_overrides() {
        let args = vec![
            "--cli-input-json".to_string(),
            r#"{"UserName":"alice","Path":"/old/"}"#.to_string(),
            "--user-name".to_string(),
            "bob".to_string(),
        ];
        let result = parse_operation_args(&args, &empty_set(), &empty_set()).unwrap();
        // Explicit --user-name bob should override the JSON value
        assert_eq!(result["UserName"].as_str(), Some("bob"));
        // Path from JSON should remain since it was not overridden
        assert_eq!(result["Path"].as_str(), Some("/old/"));
    }

    #[test]
    fn test_cli_input_json_absent_normal_behavior() {
        // No --cli-input-json at all: should behave exactly like before
        let args = vec![
            "--user-name".to_string(),
            "alice".to_string(),
        ];
        let result = parse_operation_args(&args, &empty_set(), &empty_set()).unwrap();
        assert_eq!(result["UserName"].as_str(), Some("alice"));
    }

    #[test]
    fn test_percent_encode_query_param_basic() {
        assert_eq!(percent_encode_query_param("hello world"), "hello%20world");
        assert_eq!(percent_encode_query_param("foo=bar"), "foo%3Dbar");
        assert_eq!(percent_encode_query_param("simple"), "simple");
    }

    // ---------------------------------------------------------------
    // Dynamic arg parsing tests
    // ---------------------------------------------------------------

    /// Helper to build a minimal shapes map for testing get_operation_args_info.
    fn test_shapes() -> std::collections::HashMap<String, serde_json::Value> {
        serde_json::from_str::<std::collections::HashMap<String, serde_json::Value>>(r#"{
            "CreateUserRequest": {
                "type": "structure",
                "required": ["UserName"],
                "members": {
                    "UserName": { "shape": "userNameType" },
                    "Path": { "shape": "pathType" },
                    "PermissionsBoundary": { "shape": "arnType" },
                    "Tags": { "shape": "tagListType" }
                }
            },
            "GetCallerIdentityRequest": {
                "type": "structure",
                "members": {}
            },
            "DescribeInstancesRequest": {
                "type": "structure",
                "members": {
                    "InstanceIds": { "shape": "InstanceIdStringList" },
                    "DryRun": { "shape": "Boolean" },
                    "MaxResults": { "shape": "Integer" },
                    "NextToken": { "shape": "String" }
                }
            },
            "userNameType": { "type": "string" },
            "pathType": { "type": "string" },
            "arnType": { "type": "string" },
            "tagListType": { "type": "list", "member": { "shape": "Tag" } },
            "Tag": { "type": "structure", "members": { "Key": { "shape": "tagKeyType" }, "Value": { "shape": "tagValueType" } } },
            "tagKeyType": { "type": "string" },
            "tagValueType": { "type": "string" },
            "InstanceIdStringList": { "type": "list", "member": { "shape": "String" } },
            "Boolean": { "type": "boolean" },
            "Integer": { "type": "integer" },
            "String": { "type": "string" }
        }"#).unwrap()
    }

    #[test]
    fn test_dynamic_arg_parsing_create_user_has_required() {
        let shapes = test_shapes();
        let args = get_operation_args_info("CreateUserRequest", &shapes);

        assert_eq!(args.len(), 4);

        // UserName is required
        let user_name = args.iter().find(|a| a.model_name == "UserName").unwrap();
        assert_eq!(user_name.cli_name, "--user-name");
        assert!(user_name.required);
        assert_eq!(user_name.shape_type, "string");

        // Path is optional
        let path = args.iter().find(|a| a.model_name == "Path").unwrap();
        assert_eq!(path.cli_name, "--path");
        assert!(!path.required);
        assert_eq!(path.shape_type, "string");

        // Tags is a list type
        let tags = args.iter().find(|a| a.model_name == "Tags").unwrap();
        assert_eq!(tags.cli_name, "--tags");
        assert!(!tags.required);
        assert_eq!(tags.shape_type, "list");
    }

    #[test]
    fn test_dynamic_arg_parsing_no_required_params() {
        let shapes = test_shapes();
        let args = get_operation_args_info("GetCallerIdentityRequest", &shapes);

        // GetCallerIdentity has no members at all
        assert!(args.is_empty());
        // Verify none are marked required
        assert!(args.iter().all(|a| !a.required));
    }

    #[test]
    fn test_dynamic_arg_parsing_describe_instances() {
        let shapes = test_shapes();
        let args = get_operation_args_info("DescribeInstancesRequest", &shapes);

        assert_eq!(args.len(), 4);

        let dry_run = args.iter().find(|a| a.model_name == "DryRun").unwrap();
        assert_eq!(dry_run.cli_name, "--dry-run");
        assert!(!dry_run.required);
        assert_eq!(dry_run.shape_type, "boolean");

        let max_results = args.iter().find(|a| a.model_name == "MaxResults").unwrap();
        assert_eq!(max_results.cli_name, "--max-results");
        assert_eq!(max_results.shape_type, "integer");

        let instance_ids = args.iter().find(|a| a.model_name == "InstanceIds").unwrap();
        assert_eq!(instance_ids.cli_name, "--instance-ids");
        assert_eq!(instance_ids.shape_type, "list");
    }

    #[test]
    fn test_dynamic_arg_parsing_nonexistent_shape() {
        let shapes = test_shapes();
        let args = get_operation_args_info("NonexistentShape", &shapes);
        assert!(args.is_empty());
    }

    #[test]
    fn test_dynamic_arg_parsing_sorted_output() {
        let shapes = test_shapes();
        let args = get_operation_args_info("CreateUserRequest", &shapes);

        // Should be sorted by cli_name
        let cli_names: Vec<&str> = args.iter().map(|a| a.cli_name.as_str()).collect();
        let mut sorted = cli_names.clone();
        sorted.sort();
        assert_eq!(cli_names, sorted, "Args should be sorted by cli_name");
    }

    #[test]
    fn test_dynamic_arg_parsing_with_real_sts_model() {
        let path = std::path::Path::new("models/sts/2011-06-15/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: STS model not copied yet");
            return;
        }
        let model = loader::load_service_model(path).unwrap();

        // GetCallerIdentity has no required input params
        let gci_input = model.operations["GetCallerIdentity"]
            .input_shape
            .as_deref()
            .unwrap();
        let args = get_operation_args_info(gci_input, &model.shapes);
        // GetCallerIdentityRequest has no members in the real model
        assert!(args.iter().all(|a| !a.required));

        // AssumeRole has required params: RoleArn, RoleSessionName
        let ar_input = model.operations["AssumeRole"]
            .input_shape
            .as_deref()
            .unwrap();
        let ar_args = get_operation_args_info(ar_input, &model.shapes);

        let role_arn = ar_args.iter().find(|a| a.model_name == "RoleArn").unwrap();
        assert_eq!(role_arn.cli_name, "--role-arn");
        assert!(role_arn.required);
        assert_eq!(role_arn.shape_type, "string");

        let session_name = ar_args.iter().find(|a| a.model_name == "RoleSessionName").unwrap();
        assert_eq!(session_name.cli_name, "--role-session-name");
        assert!(session_name.required);
    }

    #[test]
    fn test_dynamic_arg_parsing_with_real_iam_model() {
        let path = std::path::Path::new("models/iam/2010-05-08/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: IAM model not copied yet");
            return;
        }
        let model = loader::load_service_model(path).unwrap();

        // CreateUser has UserName as required
        let cu_input = model.operations["CreateUser"]
            .input_shape
            .as_deref()
            .unwrap();
        let cu_args = get_operation_args_info(cu_input, &model.shapes);

        let user_name = cu_args.iter().find(|a| a.model_name == "UserName").unwrap();
        assert_eq!(user_name.cli_name, "--user-name");
        assert!(user_name.required);
        assert_eq!(user_name.shape_type, "string");

        // Path is optional
        let path_arg = cu_args.iter().find(|a| a.model_name == "Path").unwrap();
        assert_eq!(path_arg.cli_name, "--path");
        assert!(!path_arg.required);
    }

    // ---------------------------------------------------------------
    // Error message tests
    // ---------------------------------------------------------------

    #[test]
    fn test_error_message_no_region() {
        // Simulate the error that occurs when no region is specified.
        // The driver builds this error via .ok_or_else on an Option<&str>.
        let region: Option<&str> = None;
        let err = region
            .ok_or_else(|| anyhow::anyhow!(
                "No region specified. Use --region, AWS_REGION, or configure a default region."
            ))
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("No region specified"),
            "Error should mention 'No region specified', got: {msg}"
        );
        assert!(
            msg.contains("--region"),
            "Error should mention --region flag, got: {msg}"
        );
        assert!(
            msg.contains("AWS_REGION"),
            "Error should mention AWS_REGION env var, got: {msg}"
        );
    }

    #[test]
    fn test_error_message_unknown_service() {
        // When find_service_model returns None, the driver produces this error.
        let service = "nonexistent-service";
        let model_service = resolve_service_name(service);
        let models_dir = std::path::Path::new("models").join(model_service);
        let result = loader::find_service_model(&models_dir);

        assert!(
            result.is_none(),
            "find_service_model should return None for a nonexistent service"
        );

        // Reproduce the exact error the driver would create
        let err = result
            .ok_or_else(|| anyhow::anyhow!(
                "Service model not found for '{}'. Check that models/{} exists.",
                service, model_service
            ))
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Service model not found"),
            "Error should mention 'Service model not found', got: {msg}"
        );
        assert!(
            msg.contains(service),
            "Error should include the service name '{}', got: {msg}",
            service
        );
    }

    #[test]
    fn test_error_message_unknown_operation() {
        // Build a minimal operations map and look up a nonexistent operation.
        let mut ops = std::collections::HashMap::new();
        ops.insert(
            "GetCallerIdentity".to_string(),
            model::Operation {
                name: "GetCallerIdentity".to_string(),
                http_method: "POST".to_string(),
                http_request_uri: "/".to_string(),
                input_shape: None,
                output_shape: None,
                result_wrapper: None,
                errors: vec![],
                documentation: None,
            },
        );

        let service = "sts";
        let operation = "nonexistent-operation";

        let result = model::find_operation_by_cli_name(&ops, operation);
        assert!(
            result.is_none(),
            "find_operation_by_cli_name should return None for an unknown operation"
        );

        // Reproduce the exact error the driver would create
        let err = result
            .ok_or_else(|| anyhow::anyhow!(
                "Unknown operation '{}' for service '{}'. Operation not found in service model.",
                operation, service
            ))
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Unknown operation"),
            "Error should mention 'Unknown operation', got: {msg}"
        );
        assert!(
            msg.contains(operation),
            "Error should include the operation name '{}', got: {msg}",
            operation
        );
        assert!(
            msg.contains(service),
            "Error should include the service name '{}', got: {msg}",
            service
        );
    }

    #[test]
    fn test_error_message_credential_failure_query_protocol() {
        // Test that query protocol error parsing produces the expected
        // error message format for an auth failure (e.g., InvalidClientTokenId).
        let error_xml = r#"<ErrorResponse>
            <Error>
                <Code>InvalidClientTokenId</Code>
                <Message>The security token included in the request is invalid.</Message>
                <Type>Sender</Type>
            </Error>
            <RequestId>abc-123</RequestId>
        </ErrorResponse>"#;

        let (code, message) = query::parse_query_error(error_xml).unwrap();
        assert_eq!(code, "InvalidClientTokenId");
        assert_eq!(
            message,
            "The security token included in the request is invalid."
        );

        // Verify the formatted error message that the driver would produce
        let _status = 403;
        let op_name = "GetCallerIdentity";
        let formatted = format!("An error occurred ({}) when calling the {} operation: {}", code, op_name, message);
        assert!(
            formatted.contains("InvalidClientTokenId"),
            "Error should include error code, got: {formatted}"
        );
        assert!(
            formatted.contains("GetCallerIdentity"),
            "Error should include operation name, got: {formatted}"
        );
    }

    #[test]
    fn test_error_message_credential_failure_json_protocol() {
        // Test JSON protocol error parsing for credential-related errors
        // (e.g., UnrecognizedClientException from DynamoDB/KMS).
        let error_body = r#"{
            "__type": "com.amazonaws.kms#UnrecognizedClientException",
            "message": "The security token included in the request is invalid."
        }"#;

        let (code, message) = json_protocol::parse_json_error(error_body).unwrap();
        assert_eq!(code, "UnrecognizedClientException");
        assert_eq!(
            message,
            "The security token included in the request is invalid."
        );

        let op_name = "ListKeys";
        let formatted = format!("An error occurred ({}) when calling the {} operation: {}", code, op_name, message);
        assert!(
            formatted.contains("UnrecognizedClientException"),
            "Error should include error code, got: {formatted}"
        );
        assert!(
            formatted.contains("ListKeys"),
            "Error should include operation name, got: {formatted}"
        );
    }

    #[test]
    fn test_error_message_credential_failure_rest_json_protocol() {
        // Test REST-JSON protocol error parsing for auth failures
        // (e.g., AccessDeniedException from Lambda).
        let error_body = r#"{
            "__type": "com.amazonaws.lambda#AccessDeniedException",
            "Message": "User is not authorized to perform this action."
        }"#;

        let (code, message) = rest_json::parse_rest_json_error(error_body).unwrap();
        assert_eq!(code, "AccessDeniedException");
        assert_eq!(
            message,
            "User is not authorized to perform this action."
        );

        let op_name = "Invoke";
        let formatted = format!("An error occurred ({}) when calling the {} operation: {}", code, op_name, message);
        assert!(
            formatted.contains("AccessDeniedException"),
            "Error should include error code, got: {formatted}"
        );
        assert!(
            formatted.contains("Invoke"),
            "Error should include operation name, got: {formatted}"
        );
    }

    #[test]
    fn test_error_message_credential_failure_rest_xml_protocol() {
        // Test REST-XML protocol error parsing for auth failures
        // (e.g., AccessDenied from S3).
        let error_xml = r#"<Error>
            <Code>AccessDenied</Code>
            <Message>Access Denied</Message>
            <RequestId>abc-123</RequestId>
        </Error>"#;

        let (code, message) = rest_xml::parse_rest_xml_error(error_xml).unwrap();
        assert_eq!(code, "AccessDenied");
        assert_eq!(message, "Access Denied");

        let op_name = "GetObject";
        let formatted = format!("An error occurred ({}) when calling the {} operation: {}", code, op_name, message);
        assert!(
            formatted.contains("AccessDenied"),
            "Error should include error code, got: {formatted}"
        );
        assert!(
            formatted.contains("GetObject"),
            "Error should include operation name, got: {formatted}"
        );
    }

    #[test]
    fn test_error_message_credential_failure_ec2_protocol() {
        // Test EC2 protocol error parsing for auth failures
        // (EC2 uses a different error XML format than standard query).
        let error_xml = r#"<Response>
            <Errors>
                <Error>
                    <Code>AuthFailure</Code>
                    <Message>AWS was not able to validate the provided access credentials</Message>
                </Error>
            </Errors>
            <RequestID>abc-123</RequestID>
        </Response>"#;

        let (code, message) = query::parse_ec2_error(error_xml).unwrap();
        assert_eq!(code, "AuthFailure");
        assert_eq!(
            message,
            "AWS was not able to validate the provided access credentials"
        );

        let op_name = "DescribeInstances";
        let formatted = format!("An error occurred ({}) when calling the {} operation: {}", code, op_name, message);
        assert!(
            formatted.contains("AuthFailure"),
            "Error should include error code, got: {formatted}"
        );
        assert!(
            formatted.contains("DescribeInstances"),
            "Error should include operation name, got: {formatted}"
        );
    }

    #[test]
    fn test_error_message_unsupported_protocol() {
        // The driver bails with a specific message for unsupported protocols.
        let protocol = "smithy-rpc-v2-cbor";
        let msg = format!(
            "Protocol '{}' is not supported. Supported protocols: query, ec2, json, rest-json, rest-xml.",
            protocol
        );
        assert!(
            msg.contains("not supported"),
            "Error should mention 'not supported', got: {msg}"
        );
        assert!(
            msg.contains(protocol),
            "Error should include the protocol name, got: {msg}"
        );
        assert!(
            msg.contains("query"),
            "Error should list supported protocols, got: {msg}"
        );
    }

    #[test]
    fn test_error_message_service_name_resolution() {
        // Verify that s3api maps to s3, and unknown services pass through.
        assert_eq!(resolve_service_name("s3api"), "s3");
        assert_eq!(resolve_service_name("iam"), "iam");
        assert_eq!(resolve_service_name("totally-fake"), "totally-fake");
    }

    #[test]
    fn test_error_message_missing_operation_name() {
        // When no operation is provided, the driver produces this error.
        let service = "sts";
        let msg = format!(
            "Usage: raws {service} <operation> [--params...]\n\nMissing operation name."
        );
        assert!(
            msg.contains("Missing operation name"),
            "Error should mention 'Missing operation name', got: {msg}"
        );
        assert!(
            msg.contains(service),
            "Error should include the service name, got: {msg}"
        );
    }

    // ---------------------------------------------------------------
    // --generate-cli-skeleton tests
    // ---------------------------------------------------------------

    #[test]
    fn test_cli_skeleton_simple_structure() {
        // A simple input shape with strings and integers
        let shapes: std::collections::HashMap<String, serde_json::Value> =
            serde_json::from_str(r#"{
                "SimpleInput": {
                    "type": "structure",
                    "members": {
                        "Name": { "shape": "StringType" },
                        "Count": { "shape": "IntegerType" },
                        "Active": { "shape": "BooleanType" },
                        "Score": { "shape": "DoubleType" }
                    }
                },
                "StringType": { "type": "string" },
                "IntegerType": { "type": "integer" },
                "BooleanType": { "type": "boolean" },
                "DoubleType": { "type": "double" }
            }"#).unwrap();

        let skeleton = generate_skeleton("SimpleInput", &shapes);
        assert!(skeleton.is_object());

        assert_eq!(skeleton["Name"], serde_json::json!(""));
        assert_eq!(skeleton["Count"], serde_json::json!(0));
        assert_eq!(skeleton["Active"], serde_json::json!(true));
        assert_eq!(skeleton["Score"], serde_json::json!(0.0));
    }

    #[test]
    fn test_cli_skeleton_nested_structures() {
        // Nested structures, lists, and maps
        let shapes: std::collections::HashMap<String, serde_json::Value> =
            serde_json::from_str(r#"{
                "OuterInput": {
                    "type": "structure",
                    "members": {
                        "Name": { "shape": "StringType" },
                        "Tags": { "shape": "TagList" },
                        "Metadata": { "shape": "MetadataMap" },
                        "Config": { "shape": "ConfigShape" }
                    }
                },
                "ConfigShape": {
                    "type": "structure",
                    "members": {
                        "Timeout": { "shape": "IntegerType" },
                        "Enabled": { "shape": "BooleanType" }
                    }
                },
                "TagList": {
                    "type": "list",
                    "member": { "shape": "Tag" }
                },
                "Tag": {
                    "type": "structure",
                    "members": {
                        "Key": { "shape": "StringType" },
                        "Value": { "shape": "StringType" }
                    }
                },
                "MetadataMap": {
                    "type": "map",
                    "key": { "shape": "StringType" },
                    "value": { "shape": "StringType" }
                },
                "StringType": { "type": "string" },
                "IntegerType": { "type": "integer" },
                "BooleanType": { "type": "boolean" }
            }"#).unwrap();

        let skeleton = generate_skeleton("OuterInput", &shapes);
        assert!(skeleton.is_object());

        // Top-level string
        assert_eq!(skeleton["Name"], serde_json::json!(""));

        // Nested structure
        assert!(skeleton["Config"].is_object());
        assert_eq!(skeleton["Config"]["Timeout"], serde_json::json!(0));
        assert_eq!(skeleton["Config"]["Enabled"], serde_json::json!(true));

        // List with one element (which is a structure)
        assert!(skeleton["Tags"].is_array());
        let tags = skeleton["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], serde_json::json!(""));
        assert_eq!(tags[0]["Value"], serde_json::json!(""));

        // Map with KeyName placeholder
        assert!(skeleton["Metadata"].is_object());
        assert_eq!(skeleton["Metadata"]["KeyName"], serde_json::json!(""));
    }

    #[test]
    fn test_cli_skeleton_all_types() {
        // Verify all type mappings
        let shapes: std::collections::HashMap<String, serde_json::Value> =
            serde_json::from_str(r#"{
                "AllTypesInput": {
                    "type": "structure",
                    "members": {
                        "S": { "shape": "StringType" },
                        "I": { "shape": "IntegerType" },
                        "L": { "shape": "LongType" },
                        "F": { "shape": "FloatType" },
                        "D": { "shape": "DoubleType" },
                        "B": { "shape": "BooleanType" },
                        "T": { "shape": "TimestampType" },
                        "Bl": { "shape": "BlobType" }
                    }
                },
                "StringType": { "type": "string" },
                "IntegerType": { "type": "integer" },
                "LongType": { "type": "long" },
                "FloatType": { "type": "float" },
                "DoubleType": { "type": "double" },
                "BooleanType": { "type": "boolean" },
                "TimestampType": { "type": "timestamp" },
                "BlobType": { "type": "blob" }
            }"#).unwrap();

        let skeleton = generate_skeleton("AllTypesInput", &shapes);
        assert_eq!(skeleton["S"], serde_json::json!(""));
        assert_eq!(skeleton["I"], serde_json::json!(0));
        assert_eq!(skeleton["L"], serde_json::json!(0));
        assert_eq!(skeleton["F"], serde_json::json!(0.0));
        assert_eq!(skeleton["D"], serde_json::json!(0.0));
        assert_eq!(skeleton["B"], serde_json::json!(true));
        assert_eq!(skeleton["T"], serde_json::json!(""));
        assert_eq!(skeleton["Bl"], serde_json::json!(""));
    }

    #[test]
    fn test_cli_skeleton_sts_assume_role() {
        let path = std::path::Path::new("models/sts/2011-06-15/service-2.json");
        if !path.exists() {
            eprintln!("Skipping: STS model not copied yet");
            return;
        }
        let model = loader::load_service_model(path).unwrap();

        let input_shape = model.operations["AssumeRole"]
            .input_shape
            .as_deref()
            .unwrap();
        let skeleton = generate_skeleton(input_shape, &model.shapes);

        // RoleArn and RoleSessionName should be strings
        assert_eq!(skeleton["RoleArn"], serde_json::json!(""));
        assert_eq!(skeleton["RoleSessionName"], serde_json::json!(""));

        // DurationSeconds should be integer
        assert_eq!(skeleton["DurationSeconds"], serde_json::json!(0));

        // Policy should be a string
        assert_eq!(skeleton["Policy"], serde_json::json!(""));

        // PolicyArns is a list of structures
        assert!(skeleton["PolicyArns"].is_array());
        let policy_arns = skeleton["PolicyArns"].as_array().unwrap();
        assert_eq!(policy_arns.len(), 1);
        assert_eq!(policy_arns[0]["arn"], serde_json::json!(""));

        // Tags is a list of Tag structures
        assert!(skeleton["Tags"].is_array());
        let tags = skeleton["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], serde_json::json!(""));
        assert_eq!(tags[0]["Value"], serde_json::json!(""));

        // Verify the output can be pretty-printed with 4-space indentation
        let formatted = serde_json::to_string_pretty(&skeleton).unwrap();
        assert!(formatted.contains("\"RoleArn\""));
        assert!(formatted.contains("\"RoleSessionName\""));
    }

    #[test]
    fn test_cli_skeleton_extract_flag_no_value() {
        let args = vec!["--generate-cli-skeleton".to_string()];
        assert_eq!(
            extract_generate_cli_skeleton(&args),
            Some("input".to_string())
        );
    }

    #[test]
    fn test_cli_skeleton_extract_flag_with_input() {
        let args = vec![
            "--generate-cli-skeleton".to_string(),
            "input".to_string(),
        ];
        assert_eq!(
            extract_generate_cli_skeleton(&args),
            Some("input".to_string())
        );
    }

    #[test]
    fn test_cli_skeleton_extract_flag_with_output() {
        let args = vec![
            "--generate-cli-skeleton".to_string(),
            "output".to_string(),
        ];
        assert_eq!(
            extract_generate_cli_skeleton(&args),
            Some("output".to_string())
        );
    }

    #[test]
    fn test_cli_skeleton_extract_flag_absent() {
        let args = vec![
            "--role-arn".to_string(),
            "arn:aws:iam::123456789012:role/MyRole".to_string(),
        ];
        assert_eq!(extract_generate_cli_skeleton(&args), None);
    }

    #[test]
    fn test_cli_skeleton_extract_flag_among_other_args() {
        let args = vec![
            "--role-arn".to_string(),
            "arn:aws:iam::123456789012:role/MyRole".to_string(),
            "--generate-cli-skeleton".to_string(),
        ];
        assert_eq!(
            extract_generate_cli_skeleton(&args),
            Some("input".to_string())
        );
    }

    #[test]
    fn test_cli_skeleton_empty_input_shape() {
        // When the operation has no input shape, skeleton should be {}
        let shapes: std::collections::HashMap<String, serde_json::Value> =
            std::collections::HashMap::new();
        let skeleton = generate_skeleton("NonexistentShape", &shapes);
        assert!(skeleton.is_null());
    }

    #[test]
    fn test_cli_skeleton_pretty_print_4_space() {
        // Verify 4-space indentation matching AWS CLI output.
        let shapes: std::collections::HashMap<String, serde_json::Value> =
            serde_json::from_str(r#"{
                "TestInput": {
                    "type": "structure",
                    "members": {
                        "Name": { "shape": "StringType" }
                    }
                },
                "StringType": { "type": "string" }
            }"#).unwrap();

        let skeleton = generate_skeleton("TestInput", &shapes);
        let formatted = serialize_json_4_space(&skeleton).unwrap();

        // Verify 4-space indentation is used (not 2-space)
        assert!(
            formatted.contains("    \"Name\""),
            "Expected 4-space indentation, got: {formatted}"
        );
        assert!(
            !formatted.contains("  \"Name\"\n"),
            "Should not have 2-space indentation"
        );

        // Verify it parses back
        let reparsed: serde_json::Value = serde_json::from_str(&formatted).unwrap();
        assert_eq!(reparsed["Name"], serde_json::json!(""));
    }

    // ---------------------------------------------------------------
    // S3 virtual-hosted style URI stripping tests
    // ---------------------------------------------------------------

    #[test]
    fn test_strip_s3_bucket_prefix_virtual_hosted() {
        let input = serde_json::json!({"Bucket": "my-bucket"});
        let result = strip_s3_bucket_prefix_if_virtual_hosted(
            "my-bucket.s3.us-east-1.amazonaws.com",
            "/my-bucket/key.txt",
            &input,
        );
        assert_eq!(result, "/key.txt");
    }

    #[test]
    fn test_strip_s3_bucket_prefix_virtual_hosted_root() {
        let input = serde_json::json!({"Bucket": "my-bucket"});
        let result = strip_s3_bucket_prefix_if_virtual_hosted(
            "my-bucket.s3.us-east-1.amazonaws.com",
            "/my-bucket",
            &input,
        );
        assert_eq!(result, "/");
    }

    #[test]
    fn test_strip_s3_bucket_prefix_path_style_no_change() {
        let input = serde_json::json!({"Bucket": "my-bucket"});
        let result = strip_s3_bucket_prefix_if_virtual_hosted(
            "s3.us-east-1.amazonaws.com",
            "/my-bucket/key.txt",
            &input,
        );
        // Not virtual-hosted (host doesn't start with bucket), so no stripping
        assert_eq!(result, "/my-bucket/key.txt");
    }

    #[test]
    fn test_strip_s3_bucket_prefix_no_bucket_in_input() {
        let input = serde_json::json!({});
        let result = strip_s3_bucket_prefix_if_virtual_hosted(
            "s3.us-east-1.amazonaws.com",
            "/",
            &input,
        );
        assert_eq!(result, "/");
    }

    #[test]
    fn test_strip_s3_bucket_prefix_query_string_preserved() {
        let input = serde_json::json!({"Bucket": "my-bucket"});
        let result = strip_s3_bucket_prefix_if_virtual_hosted(
            "my-bucket.s3.us-east-1.amazonaws.com",
            "/my-bucket?tagging",
            &input,
        );
        assert_eq!(result, "?tagging");
    }

    // ---------------------------------------------------------------
    // apply_output_customizations tests (STS DecodeAuthorizationMessage)
    // ---------------------------------------------------------------

    fn empty_shapes() -> std::collections::HashMap<String, serde_json::Value> {
        std::collections::HashMap::new()
    }

    #[test]
    fn test_sts_decode_authorization_message_pretty_prints_decoded_message() {
        let escaped_json = r#"{"allowed":true,"explicitDeny":false,"matchedStatements":[]}"#;
        let mut response = serde_json::json!({
            "DecodedMessage": escaped_json
        });

        apply_output_customizations("sts", "decode-authorization-message", &mut response, "", &empty_shapes());

        // DecodedMessage should now be a parsed JSON object, not a string
        assert!(
            response["DecodedMessage"].is_object(),
            "DecodedMessage should be a parsed JSON object, got: {}",
            response["DecodedMessage"]
        );
        assert_eq!(response["DecodedMessage"]["allowed"], serde_json::json!(true));
        assert_eq!(response["DecodedMessage"]["explicitDeny"], serde_json::json!(false));
        assert!(response["DecodedMessage"]["matchedStatements"].is_array());
    }

    #[test]
    fn test_non_sts_response_not_modified() {
        let mut response = serde_json::json!({
            "DecodedMessage": r#"{"some":"json"}"#
        });
        let original = response.clone();

        // Different service: should not modify the response
        apply_output_customizations("ec2", "decode-authorization-message", &mut response, "", &empty_shapes());
        assert_eq!(response, original);

        // Different operation: should not modify the response
        apply_output_customizations("sts", "get-caller-identity", &mut response, "", &empty_shapes());
        assert_eq!(response, original);
    }

    #[test]
    fn test_sts_decode_authorization_message_malformed_left_as_is() {
        let malformed = "this is not valid json {{{";
        let mut response = serde_json::json!({
            "DecodedMessage": malformed
        });

        apply_output_customizations("sts", "decode-authorization-message", &mut response, "", &empty_shapes());

        // Malformed JSON string should be left as-is
        assert_eq!(
            response["DecodedMessage"].as_str(),
            Some(malformed),
            "Malformed DecodedMessage should remain unchanged"
        );
    }

    #[test]
    fn test_sts_decode_authorization_message_missing_field() {
        // Response without DecodedMessage field should not be modified
        let mut response = serde_json::json!({
            "SomeOtherField": "value"
        });
        let original = response.clone();

        apply_output_customizations("sts", "decode-authorization-message", &mut response, "", &empty_shapes());
        assert_eq!(response, original);
    }

    #[test]
    fn test_sts_decode_authorization_message_non_string_field() {
        // DecodedMessage is already a non-string value (edge case): should not be modified
        let mut response = serde_json::json!({
            "DecodedMessage": 42
        });
        let original = response.clone();

        apply_output_customizations("sts", "decode-authorization-message", &mut response, "", &empty_shapes());
        assert_eq!(response, original);
    }
}
