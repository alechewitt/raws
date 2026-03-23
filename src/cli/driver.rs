use anyhow::{bail, Context, Result};
use clap::Parser;
use std::path::Path;

use crate::cli::args::GlobalArgs;
use crate::cli::commands::configure;
use crate::cli::formatter;
use crate::cli::jmespath;
use crate::core::auth::sigv4::{self, SigningParams};
use crate::core::config::provider::ConfigProvider;
use crate::core::credentials::chain::ChainCredentialProvider;
use crate::core::credentials::env::EnvCredentialProvider;
use crate::core::credentials::profile::ProfileCredentialProvider;
use crate::core::credentials::CredentialProvider;
use crate::core::endpoint::resolver;
use crate::core::http::client::HttpClient;
use crate::core::http::request::HttpRequest;
use crate::core::model::{self, loader};
use crate::core::paginate;
use crate::core::protocol::json as json_protocol;
use crate::core::protocol::query;
use crate::core::protocol::rest_json;
use crate::core::protocol::rest_xml;

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

    // Handle "raws configure": interactive credential setup
    if service == "configure" {
        let profile = match args.profile.as_deref() {
            Some(p) => p.to_string(),
            None => std::env::var("AWS_PROFILE").unwrap_or_else(|_| "default".to_string()),
        };
        return configure::run_configure(&profile);
    }

    let operation = match &args.operation {
        Some(o) => o,
        None => {
            bail!("Usage: raws {service} <operation> [--params...]\n\nMissing operation name.");
        }
    };

    // Load the service model early so help commands work without region/credentials.
    // Map CLI service names to model directory names (e.g., s3api -> s3)
    let model_service = resolve_service_name(service);
    let models_dir = Path::new("models").join(model_service);
    let model_path = loader::find_service_model(&models_dir)
        .ok_or_else(|| anyhow::anyhow!(
            "Service model not found for '{}'. Check that models/{} exists.",
            service, model_service
        ))?;

    let service_model = loader::load_service_model(&model_path)
        .with_context(|| format!("Failed to load service model for '{}'", service))?;

    // Handle "raws <service> help": list all operations for this service
    if operation == "help" {
        print_operation_list(service, &service_model);
        return Ok(());
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
        Some(args.output.as_str()),
        args.profile.as_deref(),
    )?;

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

    // 4. Parse operation-specific arguments into JSON input
    let input = parse_operation_args(&args.args)?;

    if args.debug {
        eprintln!("[debug] input: {input}");
    }

    // 5. Resolve credentials
    let mut providers: Vec<Box<dyn CredentialProvider>> =
        vec![Box::new(EnvCredentialProvider)];

    providers.push(Box::new(ProfileCredentialProvider::new(&config.profile)));

    let chain = ChainCredentialProvider::new(providers);
    let creds = chain.resolve()
        .context("Failed to resolve AWS credentials")?;

    if args.debug {
        eprintln!("[debug] credentials resolved: access_key={}...", &creds.access_key_id[..8.min(creds.access_key_id.len())]);
    }

    // 6. Resolve endpoint URL
    let endpoint_url = match &args.endpoint_url {
        Some(url) => url.clone(),
        None => resolver::resolve_endpoint(
            &service_model.metadata.endpoint_prefix,
            region,
            service_model.metadata.global_endpoint.as_deref(),
        )?,
    };

    if args.debug {
        eprintln!("[debug] endpoint: {endpoint_url}");
    }

    // 7. Load paginator config for possible auto-pagination
    let paginator_config = if !args.no_paginate {
        // model_path points to service-2.json; paginators-1.json is in the same directory
        let service_version_dir = model_path.parent().unwrap_or_else(|| Path::new("."));
        let paginators = paginate::load_paginators(service_version_dir)
            .unwrap_or_default();
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

    // 8. Build and send the request based on protocol, with auto-pagination
    let protocol = service_model.metadata.protocol.as_str();
    let response_value = if let Some(ref pc) = paginator_config {
        // Auto-pagination: collect all pages
        let mut pages = Vec::new();
        let mut current_input = input.clone();

        loop {
            let page = dispatch_request(
                protocol,
                &endpoint_url,
                &service_model,
                op,
                &current_input,
                &creds,
                region,
                args.debug,
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
        // No pagination: single request
        dispatch_request(
            protocol,
            &endpoint_url,
            &service_model,
            op,
            &input,
            &creds,
            region,
            args.debug,
        )
        .await?
    };

    // 9. Apply --query JMESPath filter if provided
    let final_value = if let Some(ref query_expr) = args.query {
        jmespath::evaluate(query_expr, &response_value)
            .with_context(|| format!("Failed to evaluate --query expression: {}", query_expr))?
    } else {
        response_value
    };

    // 10. Format and print output
    let formatted = formatter::format_output(&final_value, output_format)?;
    println!("{formatted}");

    Ok(())
}

/// Dispatch a request using the appropriate protocol.
///
/// This is a thin wrapper that selects the correct protocol dispatcher.
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
) -> Result<serde_json::Value> {
    match protocol {
        "query" => {
            dispatch_query_protocol(endpoint_url, model, op, input, creds, region, debug).await
        }
        "ec2" => {
            dispatch_ec2_protocol(endpoint_url, model, op, input, creds, region, debug).await
        }
        "json" => {
            dispatch_json_protocol(endpoint_url, model, op, input, creds, region, debug).await
        }
        "rest-json" => {
            dispatch_rest_json_protocol(endpoint_url, model, op, input, creds, region, debug).await
        }
        "rest-xml" => {
            dispatch_rest_xml_protocol(endpoint_url, model, op, input, creds, region, debug).await
        }
        _ => {
            bail!(
                "Protocol '{}' is not supported. Supported protocols: query, ec2, json, rest-json, rest-xml.",
                protocol
            );
        }
    }
}

/// Dispatch a request using the AWS Query protocol.
async fn dispatch_query_protocol(
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
) -> Result<serde_json::Value> {
    // Serialize the query request body
    let input_shape_name = op.input_shape.as_deref().unwrap_or("");
    let body_str = query::serialize_query_request(
        &op.name,
        &model.metadata.api_version,
        input,
        &model.shapes,
        input_shape_name,
    )?;

    if debug {
        eprintln!("[debug] request body: {body_str}");
    }

    // Build HTTP request
    let parsed_url = url::Url::parse(endpoint_url)
        .with_context(|| format!("Invalid endpoint URL: {endpoint_url}"))?;
    let host = parsed_url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("No host in endpoint URL: {endpoint_url}"))?;

    let mut request = HttpRequest::new(&op.http_method, endpoint_url);
    request.body = body_str.as_bytes().to_vec();
    request.add_header("host", host);
    request.add_header(
        "content-type",
        "application/x-www-form-urlencoded; charset=utf-8",
    );

    // Sign the request with SigV4
    let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let signing_service = &model.metadata.endpoint_prefix;

    let signing_params = SigningParams::from_credentials(creds, region, signing_service, &datetime);

    // Extract URI path and query from the URL
    let uri_path = parsed_url.path();
    let query_string = parsed_url.query().unwrap_or("");

    sigv4::sign_request(
        &request.method,
        uri_path,
        query_string,
        &mut request.headers,
        &request.body,
        &signing_params,
    )?;

    if debug {
        eprintln!("[debug] signed request, sending to {endpoint_url}");
    }

    // Send HTTP request
    let http_client = HttpClient::new()?;
    let response = http_client.send(&request).await
        .context("HTTP request failed")?;

    let response_body = response.body_string();

    if debug {
        eprintln!("[debug] response status: {}", response.status);
        eprintln!("[debug] response body: {response_body}");
    }

    // Parse response
    if response.status >= 200 && response.status < 300 {
        // Success: parse the XML response
        let output_shape_name = op.output_shape.as_deref().unwrap_or("");
        if output_shape_name.is_empty() {
            // No output shape: return empty object
            return Ok(serde_json::json!({}));
        }

        let parsed = query::parse_query_response(
            &response_body,
            op.result_wrapper.as_deref(),
            output_shape_name,
            &model.shapes,
        )
        .with_context(|| format!("Failed to parse response XML for {}", op.name))?;

        Ok(parsed)
    } else {
        // Error response: parse error XML
        match query::parse_query_error(&response_body) {
            Ok((code, message)) => {
                bail!(
                    "AWS Error (HTTP {}): {} - {}",
                    response.status,
                    code,
                    message
                );
            }
            Err(_) => {
                bail!(
                    "AWS Error (HTTP {}): {}",
                    response.status,
                    response_body
                );
            }
        }
    }
}

/// Dispatch a request using the EC2 Query protocol variant.
///
/// EC2 uses the same query serializer as standard query protocol, but has a
/// different error XML format: `<Response><Errors><Error>...</Error></Errors></Response>`
async fn dispatch_ec2_protocol(
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
) -> Result<serde_json::Value> {
    // EC2 uses the same query serializer
    let input_shape_name = op.input_shape.as_deref().unwrap_or("");
    let body_str = query::serialize_query_request(
        &op.name,
        &model.metadata.api_version,
        input,
        &model.shapes,
        input_shape_name,
    )?;

    if debug {
        eprintln!("[debug] request body: {body_str}");
    }

    // Build HTTP request
    let parsed_url = url::Url::parse(endpoint_url)
        .with_context(|| format!("Invalid endpoint URL: {endpoint_url}"))?;
    let host = parsed_url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("No host in endpoint URL: {endpoint_url}"))?;

    let mut request = HttpRequest::new(&op.http_method, endpoint_url);
    request.body = body_str.as_bytes().to_vec();
    request.add_header("host", host);
    request.add_header(
        "content-type",
        "application/x-www-form-urlencoded; charset=utf-8",
    );

    // Sign the request with SigV4
    let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let signing_service = &model.metadata.endpoint_prefix;
    let signing_params = SigningParams::from_credentials(creds, region, signing_service, &datetime);

    let uri_path = parsed_url.path();
    let query_string = parsed_url.query().unwrap_or("");

    sigv4::sign_request(
        &request.method,
        uri_path,
        query_string,
        &mut request.headers,
        &request.body,
        &signing_params,
    )?;

    if debug {
        eprintln!("[debug] signed request, sending to {endpoint_url}");
    }

    // Send HTTP request
    let http_client = HttpClient::new()?;
    let response = http_client.send(&request).await
        .context("HTTP request failed")?;

    let response_body = response.body_string();

    if debug {
        eprintln!("[debug] response status: {}", response.status);
        eprintln!("[debug] response body: {response_body}");
    }

    // Parse response
    if response.status >= 200 && response.status < 300 {
        let output_shape_name = op.output_shape.as_deref().unwrap_or("");
        if output_shape_name.is_empty() {
            return Ok(serde_json::json!({}));
        }

        let parsed = query::parse_query_response(
            &response_body,
            op.result_wrapper.as_deref(),
            output_shape_name,
            &model.shapes,
        )
        .with_context(|| format!("Failed to parse response XML for {}", op.name))?;

        Ok(parsed)
    } else {
        // EC2 uses a different error format
        match query::parse_ec2_error(&response_body) {
            Ok((code, message)) => {
                bail!(
                    "AWS Error (HTTP {}): {} - {}",
                    response.status,
                    code,
                    message
                );
            }
            Err(_) => {
                bail!(
                    "AWS Error (HTTP {}): {}",
                    response.status,
                    response_body
                );
            }
        }
    }
}

/// Dispatch a request using the AWS JSON protocol.
///
/// Used by services like DynamoDB, KMS, CloudTrail.
async fn dispatch_json_protocol(
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
) -> Result<serde_json::Value> {
    // Build X-Amz-Target header
    let target_prefix = model.metadata.target_prefix.as_deref().unwrap_or("");
    let target_header = json_protocol::build_target_header(target_prefix, &op.name);

    // Build Content-Type header
    let json_version = model.metadata.json_version.as_deref().unwrap_or("1.0");
    let content_type = json_protocol::build_content_type(json_version);

    // Serialize the request body as JSON
    let body_str = json_protocol::serialize_json_request(input)?;

    if debug {
        eprintln!("[debug] X-Amz-Target: {target_header}");
        eprintln!("[debug] Content-Type: {content_type}");
        eprintln!("[debug] request body: {body_str}");
    }

    // Build HTTP request
    let parsed_url = url::Url::parse(endpoint_url)
        .with_context(|| format!("Invalid endpoint URL: {endpoint_url}"))?;
    let host = parsed_url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("No host in endpoint URL: {endpoint_url}"))?;

    let mut request = HttpRequest::new("POST", endpoint_url);
    request.body = body_str.as_bytes().to_vec();
    request.add_header("host", host);
    request.add_header("content-type", &content_type);
    request.add_header("x-amz-target", &target_header);

    // Sign the request with SigV4
    let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let signing_service = &model.metadata.endpoint_prefix;
    let signing_params = SigningParams::from_credentials(creds, region, signing_service, &datetime);

    let uri_path = parsed_url.path();
    let query_string = parsed_url.query().unwrap_or("");

    sigv4::sign_request(
        "POST",
        uri_path,
        query_string,
        &mut request.headers,
        &request.body,
        &signing_params,
    )?;

    if debug {
        eprintln!("[debug] signed request, sending to {endpoint_url}");
    }

    // Send HTTP request
    let http_client = HttpClient::new()?;
    let response = http_client.send(&request).await
        .context("HTTP request failed")?;

    let response_body = response.body_string();

    if debug {
        eprintln!("[debug] response status: {}", response.status);
        eprintln!("[debug] response body: {response_body}");
    }

    // Parse response
    if response.status >= 200 && response.status < 300 {
        json_protocol::parse_json_response(&response_body)
            .with_context(|| format!("Failed to parse JSON response for {}", op.name))
    } else {
        match json_protocol::parse_json_error(&response_body) {
            Ok((code, message)) => {
                bail!(
                    "AWS Error (HTTP {}): {} - {}",
                    response.status,
                    code,
                    message
                );
            }
            Err(_) => {
                bail!(
                    "AWS Error (HTTP {}): {}",
                    response.status,
                    response_body
                );
            }
        }
    }
}

/// Dispatch a request using the REST-JSON protocol.
///
/// Used by services like Lambda, API Gateway, Kinesis.
async fn dispatch_rest_json_protocol(
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
) -> Result<serde_json::Value> {
    let input_shape_name = op.input_shape.as_deref().unwrap_or("");

    // Serialize the REST request: resolve URI, extract headers/query/body
    let (resolved_uri, extra_headers, query_params, body_json) = if input_shape_name.is_empty() {
        (op.http_request_uri.clone(), vec![], vec![], None)
    } else {
        rest_json::serialize_rest_json_request(
            &op.http_request_uri,
            input,
            input_shape_name,
            &model.shapes,
        )?
    };

    // Build the full URL: base endpoint + resolved URI + query params
    let parsed_base = url::Url::parse(endpoint_url)
        .with_context(|| format!("Invalid endpoint URL: {endpoint_url}"))?;
    let host = parsed_base
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("No host in endpoint URL: {endpoint_url}"))?;

    // Build full URL with resolved URI path and query parameters
    let mut full_url = format!(
        "{}://{}{}",
        parsed_base.scheme(),
        parsed_base.host_str().unwrap_or(""),
        resolved_uri
    );

    // Append query parameters
    if !query_params.is_empty() {
        let qs: Vec<String> = query_params
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    percent_encode_query_param(k),
                    percent_encode_query_param(v)
                )
            })
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

    // Build HTTP request
    let mut request = HttpRequest::new(&op.http_method, &full_url);
    request.add_header("host", host);

    // Add Content-Type for requests that have a body
    if body_json.is_some() {
        let json_version = model.metadata.json_version.as_deref().unwrap_or("1.0");
        let content_type = json_protocol::build_content_type(json_version);
        request.add_header("content-type", &content_type);
    }

    // Add extra headers from serialization (e.g., X-Amz-Invocation-Type)
    for (k, v) in &extra_headers {
        request.add_header(k, v);
    }

    // Set request body
    if let Some(ref body) = body_json {
        request.body = body.as_bytes().to_vec();
    }

    // Sign the request with SigV4
    let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let signing_service = &model.metadata.endpoint_prefix;
    let signing_params = SigningParams::from_credentials(creds, region, signing_service, &datetime);

    // For signing, we use the resolved URI path (not the template)
    let signing_url = url::Url::parse(&full_url)
        .with_context(|| format!("Invalid full URL: {full_url}"))?;
    let signing_uri_path = signing_url.path();
    let signing_query_string = signing_url.query().unwrap_or("");

    sigv4::sign_request(
        &request.method,
        signing_uri_path,
        signing_query_string,
        &mut request.headers,
        &request.body,
        &signing_params,
    )?;

    if debug {
        eprintln!("[debug] signed request, sending to {full_url}");
    }

    // Send HTTP request
    let http_client = HttpClient::new()?;
    let response = http_client.send(&request).await
        .context("HTTP request failed")?;

    let response_body = response.body_string();

    if debug {
        eprintln!("[debug] response status: {}", response.status);
        eprintln!("[debug] response body: {response_body}");
    }

    // Parse response
    if response.status >= 200 && response.status < 300 {
        let output_shape_name = op.output_shape.as_deref().unwrap_or("");
        if output_shape_name.is_empty() {
            return Ok(serde_json::json!({}));
        }

        rest_json::parse_rest_json_response(
            &response_body,
            response.status,
            &response.headers,
            output_shape_name,
            &model.shapes,
        )
        .with_context(|| format!("Failed to parse REST-JSON response for {}", op.name))
    } else {
        match rest_json::parse_rest_json_error(&response_body) {
            Ok((code, message)) => {
                bail!(
                    "AWS Error (HTTP {}): {} - {}",
                    response.status,
                    code,
                    message
                );
            }
            Err(_) => {
                bail!(
                    "AWS Error (HTTP {}): {}",
                    response.status,
                    response_body
                );
            }
        }
    }
}

/// Dispatch a request using the REST-XML protocol.
///
/// Used by services like S3, Route53, CloudFront.
async fn dispatch_rest_xml_protocol(
    endpoint_url: &str,
    model: &model::ServiceModel,
    op: &model::Operation,
    input: &serde_json::Value,
    creds: &crate::core::credentials::Credentials,
    region: &str,
    debug: bool,
) -> Result<serde_json::Value> {
    let input_shape_name = op.input_shape.as_deref().unwrap_or("");

    // Serialize the REST request: resolve URI, extract headers/query/body
    let (resolved_uri, extra_headers, query_params, body_xml) = if input_shape_name.is_empty() {
        (op.http_request_uri.clone(), vec![], vec![], None)
    } else {
        rest_xml::serialize_rest_xml_request(
            &op.http_request_uri,
            input,
            input_shape_name,
            &model.shapes,
        )?
    };

    // Build the full URL: base endpoint + resolved URI + query params
    let parsed_base = url::Url::parse(endpoint_url)
        .with_context(|| format!("Invalid endpoint URL: {endpoint_url}"))?;
    let host = parsed_base
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("No host in endpoint URL: {endpoint_url}"))?;

    let mut full_url = format!(
        "{}://{}{}",
        parsed_base.scheme(),
        parsed_base.host_str().unwrap_or(""),
        resolved_uri
    );

    // Append query parameters
    if !query_params.is_empty() {
        let qs: Vec<String> = query_params
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    percent_encode_query_param(k),
                    percent_encode_query_param(v)
                )
            })
            .collect();
        let separator = if full_url.contains('?') { "&" } else { "?" };
        full_url = format!("{}{}{}", full_url, separator, qs.join("&"));
    }

    if debug {
        eprintln!("[debug] resolved URI: {resolved_uri}");
        eprintln!("[debug] full URL: {full_url}");
        if let Some(ref body) = body_xml {
            eprintln!("[debug] request body: {body}");
        }
    }

    // Build HTTP request
    let mut request = HttpRequest::new(&op.http_method, &full_url);
    request.add_header("host", host);

    // Add extra headers from serialization
    for (k, v) in &extra_headers {
        request.add_header(k, v);
    }

    // Set request body
    if let Some(ref body) = body_xml {
        request.body = body.as_bytes().to_vec();
        // Set Content-Type for XML body
        request.add_header("content-type", "application/xml");
    }

    // Sign the request with SigV4
    let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let signing_service = &model.metadata.endpoint_prefix;
    let signing_params = SigningParams::from_credentials(creds, region, signing_service, &datetime);

    // For signing, use the resolved URI path
    let signing_url = url::Url::parse(&full_url)
        .with_context(|| format!("Invalid full URL: {full_url}"))?;
    let signing_uri_path = signing_url.path();
    let signing_query_string = signing_url.query().unwrap_or("");

    sigv4::sign_request(
        &request.method,
        signing_uri_path,
        signing_query_string,
        &mut request.headers,
        &request.body,
        &signing_params,
    )?;

    if debug {
        eprintln!("[debug] signed request, sending to {full_url}");
    }

    // Send HTTP request
    let http_client = HttpClient::new()?;
    let response = http_client.send(&request).await
        .context("HTTP request failed")?;

    let response_body = response.body_string();

    if debug {
        eprintln!("[debug] response status: {}", response.status);
        eprintln!("[debug] response body: {response_body}");
    }

    // Parse response
    if response.status >= 200 && response.status < 300 {
        let output_shape_name = op.output_shape.as_deref().unwrap_or("");
        if output_shape_name.is_empty() {
            return Ok(serde_json::json!({}));
        }

        rest_xml::parse_rest_xml_response(
            &response_body,
            response.status,
            &response.headers,
            output_shape_name,
            &model.shapes,
        )
        .with_context(|| format!("Failed to parse REST-XML response for {}", op.name))
    } else {
        match rest_xml::parse_rest_xml_error(&response_body) {
            Ok((code, message)) => {
                bail!(
                    "AWS Error (HTTP {}): {} - {}",
                    response.status,
                    code,
                    message
                );
            }
            Err(_) => {
                bail!(
                    "AWS Error (HTTP {}): {}",
                    response.status,
                    response_body
                );
            }
        }
    }
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

    let models_dir = std::path::Path::new("models");
    match loader::discover_services(models_dir) {
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
    println!("  --region <REGION>       AWS region to use");
    println!("  --profile <PROFILE>     Named profile to use");
    println!("  --output <FORMAT>       Output format: json, table, text");
    println!("  --endpoint-url <URL>    Override endpoint URL");
    println!("  --debug                 Enable debug output");
    println!("  --no-paginate           Disable automatic pagination");
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

fn parse_operation_args(args: &[String]) -> Result<serde_json::Value> {
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

            if j + 1 < remaining_args.len() && !remaining_args[j + 1].starts_with("--") {
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

    #[test]
    fn test_parse_operation_args_empty() {
        let result = parse_operation_args(&[]).unwrap();
        assert_eq!(result, serde_json::json!({}));
    }

    #[test]
    fn test_parse_operation_args_key_value() {
        let args = vec![
            "--user-name".to_string(),
            "alice".to_string(),
        ];
        let result = parse_operation_args(&args).unwrap();
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
        let result = parse_operation_args(&args).unwrap();
        assert_eq!(result["UserName"].as_str(), Some("alice"));
        assert_eq!(result["Path"].as_str(), Some("/admins/"));
    }

    #[test]
    fn test_parse_operation_args_boolean_flag() {
        let args = vec![
            "--dry-run".to_string(),
        ];
        let result = parse_operation_args(&args).unwrap();
        assert_eq!(result["DryRun"].as_bool(), Some(true));
    }

    #[test]
    fn test_parse_operation_args_json_value() {
        let args = vec![
            "--tags".to_string(),
            r#"[{"Key":"env","Value":"prod"}]"#.to_string(),
        ];
        let result = parse_operation_args(&args).unwrap();
        assert!(result["Tags"].is_array());
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
        let result = parse_operation_args(&args).unwrap();
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
        let result = parse_operation_args(&args).unwrap();
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
        let result = parse_operation_args(&args).unwrap();
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
        let result = parse_operation_args(&args).unwrap();
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
        let status = 403;
        let formatted = format!("AWS Error (HTTP {}): {} - {}", status, code, message);
        assert!(
            formatted.contains("403"),
            "Error should include HTTP status, got: {formatted}"
        );
        assert!(
            formatted.contains("InvalidClientTokenId"),
            "Error should include error code, got: {formatted}"
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

        let status = 403;
        let formatted = format!("AWS Error (HTTP {}): {} - {}", status, code, message);
        assert!(
            formatted.contains("403"),
            "Error should include HTTP status, got: {formatted}"
        );
        assert!(
            formatted.contains("UnrecognizedClientException"),
            "Error should include error code, got: {formatted}"
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

        let status = 403;
        let formatted = format!("AWS Error (HTTP {}): {} - {}", status, code, message);
        assert!(
            formatted.contains("403"),
            "Error should include HTTP status, got: {formatted}"
        );
        assert!(
            formatted.contains("AccessDeniedException"),
            "Error should include error code, got: {formatted}"
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

        let status = 403;
        let formatted = format!("AWS Error (HTTP {}): {} - {}", status, code, message);
        assert!(
            formatted.contains("403"),
            "Error should include HTTP status, got: {formatted}"
        );
        assert!(
            formatted.contains("AccessDenied"),
            "Error should include error code, got: {formatted}"
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

        let status = 401;
        let formatted = format!("AWS Error (HTTP {}): {} - {}", status, code, message);
        assert!(
            formatted.contains("401"),
            "Error should include HTTP status, got: {formatted}"
        );
        assert!(
            formatted.contains("AuthFailure"),
            "Error should include error code, got: {formatted}"
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
}
