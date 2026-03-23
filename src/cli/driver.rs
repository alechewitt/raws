use anyhow::{bail, Context, Result};
use clap::Parser;
use std::path::Path;

use crate::cli::args::GlobalArgs;
use crate::cli::formatter;
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

    let operation = match &args.operation {
        Some(o) => o,
        None => {
            bail!("Usage: raws {service} <operation> [--params...]\n\nMissing operation name.");
        }
    };

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

    // 2. Load the service model
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

    if args.debug {
        eprintln!("[debug] protocol={} api_version={}", service_model.metadata.protocol, service_model.metadata.api_version);
    }

    // 3. Find the operation (convert kebab-case CLI name to PascalCase)
    let operation_name = model::find_operation_by_cli_name(&service_model.operations, operation)
        .ok_or_else(|| anyhow::anyhow!(
            "Unknown operation '{}' for service '{}'. Operation not found in service model.",
            operation, service
        ))?
        .to_string();

    let op = &service_model.operations[&operation_name];

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

    // 7. Build and send the request based on protocol
    let protocol = service_model.metadata.protocol.as_str();
    let response_value = match protocol {
        "query" => {
            dispatch_query_protocol(
                &endpoint_url,
                &service_model,
                op,
                &input,
                &creds,
                region,
                args.debug,
            )
            .await?
        }
        "ec2" => {
            dispatch_ec2_protocol(
                &endpoint_url,
                &service_model,
                op,
                &input,
                &creds,
                region,
                args.debug,
            )
            .await?
        }
        "json" => {
            dispatch_json_protocol(
                &endpoint_url,
                &service_model,
                op,
                &input,
                &creds,
                region,
                args.debug,
            )
            .await?
        }
        "rest-json" => {
            dispatch_rest_json_protocol(
                &endpoint_url,
                &service_model,
                op,
                &input,
                &creds,
                region,
                args.debug,
            )
            .await?
        }
        "rest-xml" => {
            dispatch_rest_xml_protocol(
                &endpoint_url,
                &service_model,
                op,
                &input,
                &creds,
                region,
                args.debug,
            )
            .await?
        }
        _ => {
            bail!(
                "Protocol '{}' is not supported. Supported protocols: query, ec2, json, rest-json, rest-xml.",
                protocol
            );
        }
    };

    // 8. Format and print output
    let formatted = formatter::format_output(&response_value, output_format)?;
    println!("{formatted}");

    Ok(())
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

fn parse_operation_args(args: &[String]) -> Result<serde_json::Value> {
    let mut map = serde_json::Map::new();

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if let Some(key) = arg.strip_prefix("--") {
            // Convert kebab-case key to PascalCase for the API
            let pascal_key = model::kebab_to_pascal(key);

            if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                let value = &args[i + 1];
                // Try to parse as JSON first, otherwise use as string
                let json_value = serde_json::from_str(value)
                    .unwrap_or_else(|_| serde_json::Value::String(value.clone()));
                map.insert(pascal_key, json_value);
                i += 2;
            } else {
                // Flag without value: treat as boolean true
                map.insert(pascal_key, serde_json::Value::Bool(true));
                i += 1;
            }
        } else {
            i += 1;
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
}
