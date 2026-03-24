// EC2 Instance Metadata Service (IMDSv2) and ECS Container credential provider.
//
// Resolution order:
//   1. ECS container credentials via AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
//   2. EC2 IMDSv2 instance role credentials

use anyhow::{bail, Context, Result};
use std::time::Duration;

use super::{CredentialProvider, Credentials};

const IMDS_BASE: &str = "http://169.254.169.254";
const IMDS_TOKEN_PATH: &str = "/latest/api/token";
const IMDS_ROLE_PATH: &str = "/latest/meta-data/iam/security-credentials/";
const IMDS_TOKEN_TTL: &str = "21600";

const ECS_BASE: &str = "http://169.254.170.2";

const IMDS_TIMEOUT: Duration = Duration::from_secs(1);

pub struct ImdsCredentialProvider;

impl ImdsCredentialProvider {
    fn build_client() -> Result<reqwest::blocking::Client> {
        reqwest::blocking::Client::builder()
            .timeout(IMDS_TIMEOUT)
            .build()
            .context("Failed to build HTTP client for IMDS")
    }

    /// Try ECS container credentials first (via AWS_CONTAINER_CREDENTIALS_RELATIVE_URI).
    fn try_ecs_container(client: &reqwest::blocking::Client) -> Result<Credentials> {
        let relative_uri = std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
            .map_err(|_| anyhow::anyhow!("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI not set"))?;

        let url = build_ecs_url(&relative_uri)?;

        let response = client
            .get(&url)
            .send()
            .context("Failed to reach ECS container credentials endpoint")?;

        let status = response.status();
        let body = response
            .text()
            .context("Failed to read ECS container credentials response")?;

        if !status.is_success() {
            bail!(
                "ECS container credentials endpoint returned status {}: {}",
                status.as_u16(),
                body
            );
        }

        parse_credentials_json(&body)
            .context("Failed to parse ECS container credentials response")
    }

    /// Try EC2 IMDSv2 instance role credentials.
    fn try_imdsv2(client: &reqwest::blocking::Client) -> Result<Credentials> {
        // Step 1: Get session token
        let token_url = format!("{}{}", IMDS_BASE, IMDS_TOKEN_PATH);
        let token_response = client
            .put(&token_url)
            .header("X-aws-ec2-metadata-token-ttl-seconds", IMDS_TOKEN_TTL)
            .send()
            .context("Failed to reach IMDSv2 token endpoint")?;

        let token_status = token_response.status();
        if !token_status.is_success() {
            bail!(
                "IMDSv2 token request returned status {}",
                token_status.as_u16()
            );
        }

        let token = token_response
            .text()
            .context("Failed to read IMDSv2 token response")?;

        if token.is_empty() {
            bail!("IMDSv2 returned empty session token");
        }

        // Step 2: Get role name
        let role_url = format!("{}{}", IMDS_BASE, IMDS_ROLE_PATH);
        let role_response = client
            .get(&role_url)
            .header("X-aws-ec2-metadata-token", &token)
            .send()
            .context("Failed to reach IMDSv2 security-credentials endpoint")?;

        let role_status = role_response.status();
        let role_name = role_response
            .text()
            .context("Failed to read IMDSv2 role name response")?;

        if !role_status.is_success() {
            bail!(
                "IMDSv2 role name request returned status {}: {}",
                role_status.as_u16(),
                role_name
            );
        }

        let role_name = role_name.trim().to_string();
        if role_name.is_empty() {
            bail!("IMDSv2 returned empty role name");
        }

        // Step 3: Get credentials for the role
        let creds_url = format!("{}{}{}", IMDS_BASE, IMDS_ROLE_PATH, role_name);
        let creds_response = client
            .get(&creds_url)
            .header("X-aws-ec2-metadata-token", &token)
            .send()
            .context("Failed to reach IMDSv2 credentials endpoint")?;

        let creds_status = creds_response.status();
        let creds_body = creds_response
            .text()
            .context("Failed to read IMDSv2 credentials response")?;

        if !creds_status.is_success() {
            bail!(
                "IMDSv2 credentials request returned status {}: {}",
                creds_status.as_u16(),
                creds_body
            );
        }

        parse_credentials_json(&creds_body)
            .context("Failed to parse IMDSv2 credentials response")
    }
}

impl CredentialProvider for ImdsCredentialProvider {
    fn resolve(&self) -> Result<Credentials> {
        let client = Self::build_client()?;

        // Try ECS container credentials first
        if std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").is_ok() {
            return Self::try_ecs_container(&client);
        }

        // Fall back to EC2 IMDSv2
        Self::try_imdsv2(&client)
    }
}

/// Build the full ECS credentials URL from the relative URI.
fn build_ecs_url(relative_uri: &str) -> Result<String> {
    if relative_uri.is_empty() {
        bail!("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI is empty");
    }
    Ok(format!("{}{}", ECS_BASE, relative_uri))
}

/// Parse a JSON credential response (used by both ECS and IMDSv2).
///
/// Expected JSON shape:
/// ```json
/// {
///   "AccessKeyId": "...",
///   "SecretAccessKey": "...",
///   "Token": "...",
///   "Expiration": "..."
/// }
/// ```
fn parse_credentials_json(body: &str) -> Result<Credentials> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("Invalid JSON in credential response")?;

    let access_key_id = value
        .get("AccessKeyId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .context("Missing or empty AccessKeyId in credential response")?;

    let secret_access_key = value
        .get("SecretAccessKey")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .context("Missing or empty SecretAccessKey in credential response")?;

    let session_token = value
        .get("Token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    Ok(Credentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_credentials_json_full() {
        let json = r#"{
            "AccessKeyId": "ASIAEXAMPLE123",
            "SecretAccessKey": "secretkey456",
            "Token": "sessiontoken789",
            "Expiration": "2024-01-01T00:00:00Z"
        }"#;

        let creds = parse_credentials_json(json).unwrap();
        assert_eq!(creds.access_key_id, "ASIAEXAMPLE123");
        assert_eq!(creds.secret_access_key, "secretkey456");
        assert_eq!(creds.session_token, Some("sessiontoken789".to_string()));
    }

    #[test]
    fn test_parse_credentials_json_no_token() {
        let json = r#"{
            "AccessKeyId": "AKIAEXAMPLE",
            "SecretAccessKey": "mysecret"
        }"#;

        let creds = parse_credentials_json(json).unwrap();
        assert_eq!(creds.access_key_id, "AKIAEXAMPLE");
        assert_eq!(creds.secret_access_key, "mysecret");
        assert_eq!(creds.session_token, None);
    }

    #[test]
    fn test_parse_credentials_json_missing_access_key() {
        let json = r#"{
            "SecretAccessKey": "mysecret",
            "Token": "tok"
        }"#;

        let err = parse_credentials_json(json).unwrap_err();
        assert!(
            err.to_string().contains("AccessKeyId"),
            "Error should mention AccessKeyId: {}",
            err
        );
    }

    #[test]
    fn test_parse_credentials_json_missing_secret_key() {
        let json = r#"{
            "AccessKeyId": "AKIAEXAMPLE",
            "Token": "tok"
        }"#;

        let err = parse_credentials_json(json).unwrap_err();
        assert!(
            err.to_string().contains("SecretAccessKey"),
            "Error should mention SecretAccessKey: {}",
            err
        );
    }

    #[test]
    fn test_parse_credentials_json_empty_access_key() {
        let json = r#"{
            "AccessKeyId": "",
            "SecretAccessKey": "mysecret"
        }"#;

        let err = parse_credentials_json(json).unwrap_err();
        assert!(
            err.to_string().contains("AccessKeyId"),
            "Error should mention AccessKeyId: {}",
            err
        );
    }

    #[test]
    fn test_parse_credentials_json_invalid_json() {
        let err = parse_credentials_json("not json at all").unwrap_err();
        assert!(
            err.to_string().contains("Invalid JSON"),
            "Error should mention Invalid JSON: {}",
            err
        );
    }

    #[test]
    fn test_parse_credentials_json_empty_token_treated_as_none() {
        let json = r#"{
            "AccessKeyId": "AKIAEXAMPLE",
            "SecretAccessKey": "mysecret",
            "Token": ""
        }"#;

        let creds = parse_credentials_json(json).unwrap();
        assert_eq!(creds.session_token, None);
    }

    #[test]
    fn test_build_ecs_url_valid() {
        let url = build_ecs_url("/v2/credentials/some-uuid").unwrap();
        assert_eq!(url, "http://169.254.170.2/v2/credentials/some-uuid");
    }

    #[test]
    fn test_build_ecs_url_empty() {
        let err = build_ecs_url("").unwrap_err();
        assert!(
            err.to_string().contains("empty"),
            "Error should mention empty: {}",
            err
        );
    }

    #[test]
    fn test_build_ecs_url_root() {
        let url = build_ecs_url("/").unwrap();
        assert_eq!(url, "http://169.254.170.2/");
    }

    #[test]
    fn test_provider_instantiation() {
        // Verify that ImdsCredentialProvider can be created and implements CredentialProvider
        let provider = ImdsCredentialProvider;
        // On a non-EC2 machine, resolve should fail gracefully
        let result = provider.resolve();
        assert!(result.is_err());
    }

    #[test]
    fn test_imds_constants() {
        assert_eq!(IMDS_BASE, "http://169.254.169.254");
        assert_eq!(ECS_BASE, "http://169.254.170.2");
        assert_eq!(IMDS_TOKEN_TTL, "21600");
        assert_eq!(IMDS_TIMEOUT, Duration::from_secs(1));
    }

    #[test]
    fn test_parse_credentials_json_with_extra_fields() {
        // Real responses include extra fields like Code, Type, LastUpdated, Expiration
        let json = r#"{
            "Code": "Success",
            "LastUpdated": "2024-01-01T00:00:00Z",
            "Type": "AWS-HMAC",
            "AccessKeyId": "ASIATEST123",
            "SecretAccessKey": "testsecret",
            "Token": "testtoken",
            "Expiration": "2024-01-01T06:00:00Z"
        }"#;

        let creds = parse_credentials_json(json).unwrap();
        assert_eq!(creds.access_key_id, "ASIATEST123");
        assert_eq!(creds.secret_access_key, "testsecret");
        assert_eq!(creds.session_token, Some("testtoken".to_string()));
    }
}
