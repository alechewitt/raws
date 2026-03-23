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
use crate::core::protocol::query;

pub async fn run() -> Result<()> {
    let args = GlobalArgs::parse();

    let service = match &args.service {
        Some(s) => s,
        None => {
            bail!("Usage: raws <service> <operation> [--params...]\n\nRun 'raws --help' for more information.");
        }
    };

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
    let models_dir = Path::new("models").join(service);
    let model_path = loader::find_service_model(&models_dir)
        .ok_or_else(|| anyhow::anyhow!(
            "Service model not found for '{}'. Check that models/{} exists.",
            service, service
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
        None => resolver::resolve_endpoint(&service_model.metadata.endpoint_prefix, region)?,
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
        _ => {
            bail!(
                "Protocol '{}' is not yet supported. Currently only 'query' protocol is implemented.",
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

/// Parse operation-specific CLI arguments (--key value pairs) into a JSON object.
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
}
