//! CloudFormation `deploy` command implementation.
//!
//! Implements `raws cloudformation deploy` which:
//! 1. Reads a template file from disk
//! 2. Checks if the stack exists (DescribeStacks)
//! 3. Creates a changeset (CreateChangeSet)
//! 4. Polls until the changeset is ready (DescribeChangeSet)
//! 5. Optionally executes the changeset (ExecuteChangeSet)
//! 6. Polls until the stack reaches a terminal state (DescribeStacks)

use anyhow::{bail, Context, Result};
use std::time::Duration;

use crate::core::auth::sigv4::{self, SigningParams};
use crate::core::credentials::Credentials;
use crate::core::http::client::HttpClient;
use crate::core::http::request::HttpRequest;
use crate::core::protocol::query;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CFN_API_VERSION: &str = "2010-05-15";
const CHANGESET_POLL_INTERVAL: Duration = Duration::from_secs(5);
const STACK_POLL_INTERVAL: Duration = Duration::from_secs(10);
const MAX_CHANGESET_POLL_ATTEMPTS: u32 = 120; // 10 minutes at 5s intervals
const MAX_STACK_POLL_ATTEMPTS: u32 = 360; // 60 minutes at 10s intervals

// ---------------------------------------------------------------------------
// Deploy arguments
// ---------------------------------------------------------------------------

/// Parsed arguments for the `cloudformation deploy` command.
#[derive(Debug)]
pub struct DeployArgs {
    pub template_file: String,
    pub stack_name: String,
    pub parameter_overrides: Vec<(String, String)>,
    pub capabilities: Vec<String>,
    pub no_execute_changeset: bool,
    pub role_arn: Option<String>,
}

/// Parse deploy arguments from the raw CLI argument list.
pub fn parse_deploy_args(args: &[String]) -> Result<DeployArgs> {
    let mut template_file: Option<String> = None;
    let mut stack_name: Option<String> = None;
    let mut parameter_overrides: Vec<(String, String)> = Vec::new();
    let mut capabilities: Vec<String> = Vec::new();
    let mut no_execute_changeset = false;
    let mut role_arn: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--template-file" => {
                i += 1;
                template_file = Some(
                    args.get(i)
                        .ok_or_else(|| anyhow::anyhow!("--template-file requires a value"))?
                        .clone(),
                );
            }
            "--stack-name" => {
                i += 1;
                stack_name = Some(
                    args.get(i)
                        .ok_or_else(|| anyhow::anyhow!("--stack-name requires a value"))?
                        .clone(),
                );
            }
            "--parameter-overrides" => {
                i += 1;
                // Consume all following Key=Value pairs until we hit another --flag
                while i < args.len() && !args[i].starts_with("--") {
                    let kv = &args[i];
                    let (key, value) = parse_key_value(kv).with_context(|| {
                        format!(
                            "Invalid parameter override '{}'. Expected format: Key=Value",
                            kv
                        )
                    })?;
                    parameter_overrides.push((key, value));
                    i += 1;
                }
                continue; // Don't increment i again
            }
            "--capabilities" => {
                i += 1;
                // Consume all following capability values until we hit another --flag
                while i < args.len() && !args[i].starts_with("--") {
                    capabilities.push(args[i].clone());
                    i += 1;
                }
                continue; // Don't increment i again
            }
            "--no-execute-changeset" => {
                no_execute_changeset = true;
            }
            "--role-arn" => {
                i += 1;
                role_arn = Some(
                    args.get(i)
                        .ok_or_else(|| anyhow::anyhow!("--role-arn requires a value"))?
                        .clone(),
                );
            }
            other => {
                bail!("Unknown argument for cloudformation deploy: {}", other);
            }
        }
        i += 1;
    }

    let template_file = template_file
        .ok_or_else(|| anyhow::anyhow!("--template-file is required for cloudformation deploy"))?;
    let stack_name = stack_name
        .ok_or_else(|| anyhow::anyhow!("--stack-name is required for cloudformation deploy"))?;

    Ok(DeployArgs {
        template_file,
        stack_name,
        parameter_overrides,
        capabilities,
        no_execute_changeset,
        role_arn,
    })
}

/// Parse a "Key=Value" string into a (key, value) tuple.
fn parse_key_value(s: &str) -> Result<(String, String)> {
    let eq_pos = s
        .find('=')
        .ok_or_else(|| anyhow::anyhow!("No '=' found"))?;
    let key = s[..eq_pos].to_string();
    let value = s[eq_pos + 1..].to_string();
    if key.is_empty() {
        bail!("Empty key in Key=Value pair");
    }
    Ok((key, value))
}

// ---------------------------------------------------------------------------
// CfnClient - CloudFormation API client
// ---------------------------------------------------------------------------

/// A lightweight CloudFormation API client that makes raw query-protocol calls.
pub struct CfnClient {
    credentials: Credentials,
    region: String,
    endpoint_url: String,
    debug: bool,
}

impl CfnClient {
    pub fn new(credentials: Credentials, region: String, endpoint_url: String, debug: bool) -> Self {
        Self {
            credentials,
            region,
            endpoint_url,
            debug,
        }
    }

    /// Make a raw CloudFormation API call with form-encoded parameters.
    ///
    /// Returns the response body as a string.
    async fn call(&self, params: &[(&str, &str)]) -> Result<String> {
        // Build the query body
        let mut parts: Vec<String> = Vec::new();
        parts.push(format!("Version={}", percent_encode(CFN_API_VERSION)));
        for (k, v) in params {
            parts.push(format!("{}={}", percent_encode(k), percent_encode(v)));
        }
        let body_str = parts.join("&");

        if self.debug {
            let action = params
                .iter()
                .find(|(k, _)| *k == "Action")
                .map(|(_, v)| *v)
                .unwrap_or("?");
            eprintln!("[debug] CloudFormation API call: {}", action);
        }

        // Parse endpoint URL
        let parsed_url = url::Url::parse(&self.endpoint_url)
            .context("Invalid CloudFormation endpoint URL")?;
        let host = parsed_url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("No host in CloudFormation endpoint URL"))?
            .to_string();

        // Build and sign the request
        let mut headers: Vec<(String, String)> = vec![
            ("host".to_string(), host),
            (
                "content-type".to_string(),
                "application/x-www-form-urlencoded; charset=utf-8".to_string(),
            ),
        ];

        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let signing_params = SigningParams::from_credentials(
            &self.credentials,
            &self.region,
            "cloudformation",
            &datetime,
        );

        let uri_path = parsed_url.path();
        let query_string = parsed_url.query().unwrap_or("");

        sigv4::sign_request(
            "POST",
            uri_path,
            query_string,
            &mut headers,
            body_str.as_bytes(),
            &signing_params,
        )?;

        // Build HTTP request
        let mut request = HttpRequest::new("POST", &self.endpoint_url);
        request.body = body_str.as_bytes().to_vec();
        for (k, v) in &headers {
            request.add_header(k, v);
        }

        // Send
        let client = HttpClient::new()?;
        let response = client.send(&request).await.context("CloudFormation API request failed")?;
        let response_body = response.body_string();

        if self.debug {
            eprintln!("[debug] CloudFormation response status: {}", response.status);
            if response.status >= 400 {
                eprintln!("[debug] CloudFormation response body: {}", response_body);
            }
        }

        // Check for errors
        if response.status >= 400 {
            let (code, message) = query::parse_query_error(&response_body)
                .unwrap_or_else(|_| ("Unknown".to_string(), response_body.clone()));
            bail!(
                "CloudFormation error (HTTP {}): {} - {}",
                response.status,
                code,
                message
            );
        }

        Ok(response_body)
    }

    /// DescribeStacks - check if a stack exists.
    ///
    /// Returns Ok(true) if the stack exists (not in DELETE_COMPLETE state),
    /// Ok(false) if the stack does not exist.
    pub async fn stack_exists(&self, stack_name: &str) -> Result<bool> {
        let params = [("Action", "DescribeStacks"), ("StackName", stack_name)];
        match self.call(&params).await {
            Ok(body) => {
                // If stack is in DELETE_COMPLETE, treat as not existing
                if body.contains("<StackStatus>DELETE_COMPLETE</StackStatus>") {
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
            Err(e) => {
                let msg = format!("{}", e);
                // "Stack with id X does not exist" means it doesn't exist
                if msg.contains("does not exist") {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// CreateChangeSet - create a new changeset.
    ///
    /// Returns the changeset ID.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_change_set(
        &self,
        stack_name: &str,
        changeset_name: &str,
        changeset_type: &str,
        template_body: &str,
        parameters: &[(String, String)],
        capabilities: &[String],
        role_arn: Option<&str>,
    ) -> Result<String> {
        let mut final_params: Vec<(String, String)> = vec![
            ("Action".to_string(), "CreateChangeSet".to_string()),
            ("StackName".to_string(), stack_name.to_string()),
            ("ChangeSetName".to_string(), changeset_name.to_string()),
            ("ChangeSetType".to_string(), changeset_type.to_string()),
            ("TemplateBody".to_string(), template_body.to_string()),
        ];

        for (i, (key, value)) in parameters.iter().enumerate() {
            let idx = i + 1;
            final_params.push((
                format!("Parameters.member.{}.ParameterKey", idx),
                key.clone(),
            ));
            final_params.push((
                format!("Parameters.member.{}.ParameterValue", idx),
                value.clone(),
            ));
        }

        for (i, cap) in capabilities.iter().enumerate() {
            let idx = i + 1;
            final_params.push((format!("Capabilities.member.{}", idx), cap.clone()));
        }

        if let Some(arn) = role_arn {
            final_params.push(("RoleARN".to_string(), arn.to_string()));
        }

        // Convert to &str pairs for call
        let ref_params: Vec<(&str, &str)> = final_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let body = self.call(&ref_params).await?;

        // Extract changeset ID from response XML
        let id = extract_xml_text(&body, "Id")
            .ok_or_else(|| anyhow::anyhow!("Could not extract changeset ID from CreateChangeSet response"))?;

        Ok(id)
    }

    /// DescribeChangeSet - get changeset details.
    ///
    /// Returns (status, status_reason, changes_xml).
    pub async fn describe_change_set(
        &self,
        changeset_name: &str,
        stack_name: &str,
    ) -> Result<ChangeSetDescription> {
        let params = [
            ("Action", "DescribeChangeSet"),
            ("ChangeSetName", changeset_name),
            ("StackName", stack_name),
        ];
        let body = self.call(&params).await?;

        let status = extract_xml_text(&body, "Status").unwrap_or_default();
        let status_reason = extract_xml_text(&body, "StatusReason");
        let execution_status = extract_xml_text(&body, "ExecutionStatus");

        Ok(ChangeSetDescription {
            status,
            status_reason,
            execution_status,
        })
    }

    /// ExecuteChangeSet.
    pub async fn execute_change_set(
        &self,
        changeset_name: &str,
        stack_name: &str,
    ) -> Result<()> {
        let params = [
            ("Action", "ExecuteChangeSet"),
            ("ChangeSetName", changeset_name),
            ("StackName", stack_name),
        ];
        self.call(&params).await?;
        Ok(())
    }

    /// DescribeStacks - get the current stack status.
    pub async fn describe_stack_status(&self, stack_name: &str) -> Result<StackDescription> {
        let params = [("Action", "DescribeStacks"), ("StackName", stack_name)];
        let body = self.call(&params).await?;

        let status = extract_xml_text(&body, "StackStatus").unwrap_or_default();
        let status_reason = extract_xml_text(&body, "StackStatusReason");

        Ok(StackDescription {
            status,
            status_reason,
        })
    }
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

pub struct ChangeSetDescription {
    pub status: String,
    pub status_reason: Option<String>,
    pub execution_status: Option<String>,
}

pub struct StackDescription {
    pub status: String,
    pub status_reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/// URL-encode a parameter value for query protocol.
fn percent_encode(input: &str) -> String {
    use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
    const ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    utf8_percent_encode(input, ENCODE_SET).to_string()
}

/// Extract text content of a specific XML element from raw XML.
///
/// This is a lightweight extraction that doesn't need full shape definitions.
/// It finds the first occurrence of `<tag>text</tag>` and returns the text.
fn extract_xml_text(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

/// Generate a changeset name based on timestamp.
fn generate_changeset_name() -> String {
    let ts = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
    format!("raws-deploy-{}", ts)
}

// ---------------------------------------------------------------------------
// Main deploy logic
// ---------------------------------------------------------------------------

/// Execute the `cloudformation deploy` command.
pub async fn run_deploy(
    args: &DeployArgs,
    credentials: Credentials,
    region: String,
    endpoint_url: String,
    debug: bool,
) -> Result<()> {
    // 1. Read the template file
    let template_body = std::fs::read_to_string(&args.template_file)
        .with_context(|| format!("Failed to read template file: {}", args.template_file))?;

    eprintln!(
        "Deploying stack '{}' with template '{}'",
        args.stack_name, args.template_file
    );

    let client = CfnClient::new(credentials, region, endpoint_url, debug);

    // 2. Check if stack exists to determine changeset type
    let stack_exists = client
        .stack_exists(&args.stack_name)
        .await
        .context("Failed to check if stack exists")?;

    let changeset_type = if stack_exists { "UPDATE" } else { "CREATE" };
    eprintln!("Changeset type: {} (stack exists: {})", changeset_type, stack_exists);

    // 3. Create a changeset
    let changeset_name = generate_changeset_name();
    eprintln!("Creating changeset '{}'...", changeset_name);

    let changeset_id = client
        .create_change_set(
            &args.stack_name,
            &changeset_name,
            changeset_type,
            &template_body,
            &args.parameter_overrides,
            &args.capabilities,
            args.role_arn.as_deref(),
        )
        .await
        .context("Failed to create changeset")?;

    eprintln!("Changeset created: {}", changeset_id);

    // 4. Poll DescribeChangeSet until status is CREATE_COMPLETE or FAILED
    eprintln!("Waiting for changeset to be ready...");
    let mut poll_count = 0u32;
    let final_changeset: ChangeSetDescription;
    loop {
        poll_count += 1;
        if poll_count > MAX_CHANGESET_POLL_ATTEMPTS {
            bail!(
                "Timed out waiting for changeset '{}' to be ready after {} attempts",
                changeset_name,
                MAX_CHANGESET_POLL_ATTEMPTS
            );
        }

        tokio::time::sleep(CHANGESET_POLL_INTERVAL).await;

        let desc = client
            .describe_change_set(&changeset_name, &args.stack_name)
            .await
            .context("Failed to describe changeset")?;

        if debug {
            eprintln!(
                "[debug] changeset status: {} (reason: {:?})",
                desc.status,
                desc.status_reason
            );
        }

        match desc.status.as_str() {
            "CREATE_COMPLETE" => {
                eprintln!("Changeset is ready.");
                final_changeset = desc;
                break;
            }
            "FAILED" => {
                let reason = desc.status_reason.as_deref().unwrap_or("unknown reason");

                // Special case: "no changes" is not an error
                if reason.contains("didn't contain changes")
                    || reason.contains("No updates are to be performed")
                    || reason.contains("The submitted information didn't contain changes")
                {
                    eprintln!(
                        "No changes to deploy. The stack '{}' is already up to date.",
                        args.stack_name
                    );
                    return Ok(());
                }

                bail!(
                    "Changeset '{}' failed: {}",
                    changeset_name,
                    reason
                );
            }
            "CREATE_PENDING" | "CREATE_IN_PROGRESS" => {
                // Still in progress, continue polling
            }
            other => {
                bail!("Unexpected changeset status: {}", other);
            }
        }
    }

    // 5. If --no-execute-changeset, print details and return
    if args.no_execute_changeset {
        eprintln!("Changeset created successfully (--no-execute-changeset specified).");
        println!("ChangeSetId: {}", changeset_id);
        println!("ChangeSetName: {}", changeset_name);
        println!("StackName: {}", args.stack_name);
        println!("Status: {}", final_changeset.status);
        if let Some(ref exec_status) = final_changeset.execution_status {
            println!("ExecutionStatus: {}", exec_status);
        }
        return Ok(());
    }

    // 6. Execute the changeset
    eprintln!("Executing changeset '{}'...", changeset_name);
    client
        .execute_change_set(&changeset_name, &args.stack_name)
        .await
        .context("Failed to execute changeset")?;

    // 7. Poll DescribeStacks until terminal state
    eprintln!("Waiting for stack '{}' to complete...", args.stack_name);
    poll_count = 0;
    loop {
        poll_count += 1;
        if poll_count > MAX_STACK_POLL_ATTEMPTS {
            bail!(
                "Timed out waiting for stack '{}' to reach a terminal state after {} attempts",
                args.stack_name,
                MAX_STACK_POLL_ATTEMPTS
            );
        }

        tokio::time::sleep(STACK_POLL_INTERVAL).await;

        let stack = client
            .describe_stack_status(&args.stack_name)
            .await
            .context("Failed to describe stack")?;

        if debug {
            eprintln!(
                "[debug] stack status: {} (reason: {:?})",
                stack.status,
                stack.status_reason
            );
        }

        match stack.status.as_str() {
            "CREATE_COMPLETE" | "UPDATE_COMPLETE" | "IMPORT_COMPLETE" => {
                eprintln!(
                    "Stack '{}' deployment complete. Status: {}",
                    args.stack_name, stack.status
                );
                return Ok(());
            }
            "CREATE_FAILED" | "ROLLBACK_COMPLETE" | "ROLLBACK_FAILED"
            | "UPDATE_ROLLBACK_COMPLETE" | "UPDATE_ROLLBACK_FAILED"
            | "DELETE_COMPLETE" | "DELETE_FAILED"
            | "IMPORT_ROLLBACK_COMPLETE" | "IMPORT_ROLLBACK_FAILED"
            | "UPDATE_FAILED" => {
                let reason = stack.status_reason.as_deref().unwrap_or("no reason provided");
                bail!(
                    "Stack '{}' deployment failed. Status: {} - {}",
                    args.stack_name,
                    stack.status,
                    reason
                );
            }
            _ => {
                // Still in progress (CREATE_IN_PROGRESS, UPDATE_IN_PROGRESS, etc.)
                if poll_count.is_multiple_of(6) {
                    // Print progress every ~60 seconds
                    eprintln!("  Stack status: {}", stack.status);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_deploy_args_minimal() {
        let args = vec![
            "--template-file".to_string(),
            "template.yaml".to_string(),
            "--stack-name".to_string(),
            "my-stack".to_string(),
        ];
        let parsed = parse_deploy_args(&args).unwrap();
        assert_eq!(parsed.template_file, "template.yaml");
        assert_eq!(parsed.stack_name, "my-stack");
        assert!(parsed.parameter_overrides.is_empty());
        assert!(parsed.capabilities.is_empty());
        assert!(!parsed.no_execute_changeset);
        assert!(parsed.role_arn.is_none());
    }

    #[test]
    fn test_parse_deploy_args_full() {
        let args = vec![
            "--template-file".to_string(),
            "cfn.yaml".to_string(),
            "--stack-name".to_string(),
            "prod-stack".to_string(),
            "--parameter-overrides".to_string(),
            "Env=prod".to_string(),
            "Version=1.0".to_string(),
            "--capabilities".to_string(),
            "CAPABILITY_IAM".to_string(),
            "CAPABILITY_NAMED_IAM".to_string(),
            "--no-execute-changeset".to_string(),
            "--role-arn".to_string(),
            "arn:aws:iam::123456789012:role/cfn-role".to_string(),
        ];
        let parsed = parse_deploy_args(&args).unwrap();
        assert_eq!(parsed.template_file, "cfn.yaml");
        assert_eq!(parsed.stack_name, "prod-stack");
        assert_eq!(
            parsed.parameter_overrides,
            vec![
                ("Env".to_string(), "prod".to_string()),
                ("Version".to_string(), "1.0".to_string()),
            ]
        );
        assert_eq!(
            parsed.capabilities,
            vec!["CAPABILITY_IAM".to_string(), "CAPABILITY_NAMED_IAM".to_string()]
        );
        assert!(parsed.no_execute_changeset);
        assert_eq!(
            parsed.role_arn,
            Some("arn:aws:iam::123456789012:role/cfn-role".to_string())
        );
    }

    #[test]
    fn test_parse_deploy_args_missing_template() {
        let args = vec!["--stack-name".to_string(), "my-stack".to_string()];
        let result = parse_deploy_args(&args);
        assert!(result.is_err());
        assert!(
            format!("{}", result.unwrap_err()).contains("--template-file is required")
        );
    }

    #[test]
    fn test_parse_deploy_args_missing_stack_name() {
        let args = vec![
            "--template-file".to_string(),
            "template.yaml".to_string(),
        ];
        let result = parse_deploy_args(&args);
        assert!(result.is_err());
        assert!(
            format!("{}", result.unwrap_err()).contains("--stack-name is required")
        );
    }

    #[test]
    fn test_parse_deploy_args_parameter_overrides_empty_value() {
        let args = vec![
            "--template-file".to_string(),
            "t.yaml".to_string(),
            "--stack-name".to_string(),
            "s".to_string(),
            "--parameter-overrides".to_string(),
            "Key=".to_string(),
        ];
        let parsed = parse_deploy_args(&args).unwrap();
        assert_eq!(
            parsed.parameter_overrides,
            vec![("Key".to_string(), "".to_string())]
        );
    }

    #[test]
    fn test_parse_deploy_args_parameter_overrides_value_with_equals() {
        let args = vec![
            "--template-file".to_string(),
            "t.yaml".to_string(),
            "--stack-name".to_string(),
            "s".to_string(),
            "--parameter-overrides".to_string(),
            "Conn=host=localhost;port=5432".to_string(),
        ];
        let parsed = parse_deploy_args(&args).unwrap();
        assert_eq!(
            parsed.parameter_overrides,
            vec![(
                "Conn".to_string(),
                "host=localhost;port=5432".to_string()
            )]
        );
    }

    #[test]
    fn test_parse_deploy_args_unknown_arg() {
        let args = vec![
            "--template-file".to_string(),
            "t.yaml".to_string(),
            "--stack-name".to_string(),
            "s".to_string(),
            "--unknown".to_string(),
        ];
        let result = parse_deploy_args(&args);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("Unknown argument"));
    }

    #[test]
    fn test_parse_key_value_basic() {
        let (k, v) = parse_key_value("Foo=Bar").unwrap();
        assert_eq!(k, "Foo");
        assert_eq!(v, "Bar");
    }

    #[test]
    fn test_parse_key_value_empty_value() {
        let (k, v) = parse_key_value("Foo=").unwrap();
        assert_eq!(k, "Foo");
        assert_eq!(v, "");
    }

    #[test]
    fn test_parse_key_value_no_equals() {
        let result = parse_key_value("FooBar");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_key_value_empty_key() {
        let result = parse_key_value("=value");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_xml_text_basic() {
        let xml = "<Root><Status>CREATE_COMPLETE</Status><Id>abc-123</Id></Root>";
        assert_eq!(
            extract_xml_text(xml, "Status"),
            Some("CREATE_COMPLETE".to_string())
        );
        assert_eq!(extract_xml_text(xml, "Id"), Some("abc-123".to_string()));
    }

    #[test]
    fn test_extract_xml_text_nested() {
        let xml = "<Response><Result><Status>READY</Status></Result></Response>";
        assert_eq!(
            extract_xml_text(xml, "Status"),
            Some("READY".to_string())
        );
    }

    #[test]
    fn test_extract_xml_text_not_found() {
        let xml = "<Root><Other>value</Other></Root>";
        assert_eq!(extract_xml_text(xml, "Missing"), None);
    }

    #[test]
    fn test_extract_xml_text_empty() {
        let xml = "<Root><Status></Status></Root>";
        assert_eq!(
            extract_xml_text(xml, "Status"),
            Some("".to_string())
        );
    }

    #[test]
    fn test_generate_changeset_name_format() {
        let name = generate_changeset_name();
        assert!(name.starts_with("raws-deploy-"));
        // Should have a timestamp suffix (14 digits: YYYYMMDDHHmmss)
        let suffix = &name["raws-deploy-".len()..];
        assert_eq!(suffix.len(), 14);
        assert!(suffix.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn test_percent_encode_basic() {
        assert_eq!(percent_encode("hello"), "hello");
        assert_eq!(percent_encode("hello world"), "hello%20world");
        assert_eq!(percent_encode("a=b&c=d"), "a%3Db%26c%3Dd");
    }

    #[test]
    fn test_percent_encode_preserves_unreserved() {
        assert_eq!(percent_encode("abc-def_ghi.jkl~mno"), "abc-def_ghi.jkl~mno");
    }

    #[test]
    fn test_parse_deploy_args_capabilities_before_params() {
        // Test that --capabilities followed by --parameter-overrides works correctly
        let args = vec![
            "--template-file".to_string(),
            "t.yaml".to_string(),
            "--stack-name".to_string(),
            "s".to_string(),
            "--capabilities".to_string(),
            "CAPABILITY_IAM".to_string(),
            "--parameter-overrides".to_string(),
            "Key=Val".to_string(),
        ];
        let parsed = parse_deploy_args(&args).unwrap();
        assert_eq!(parsed.capabilities, vec!["CAPABILITY_IAM".to_string()]);
        assert_eq!(
            parsed.parameter_overrides,
            vec![("Key".to_string(), "Val".to_string())]
        );
    }
}
