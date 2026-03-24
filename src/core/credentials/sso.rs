use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;

use super::{CredentialProvider, Credentials};
use crate::core::config::loader;

/// Configuration values read from the AWS config file for SSO-based authentication.
#[derive(Debug, Clone)]
struct SsoConfig {
    sso_start_url: String,
    sso_account_id: String,
    sso_role_name: String,
    sso_region: String,
}

/// A cached SSO access token read from `~/.aws/sso/cache/`.
#[derive(Debug, Clone)]
struct SsoToken {
    access_token: String,
    expires_at: String,
}

/// Credential provider that obtains temporary AWS credentials via AWS SSO.
///
/// The flow is:
/// 1. Read SSO configuration from the profile in `~/.aws/config`.
/// 2. Locate a cached SSO access token in `~/.aws/sso/cache/` (keyed by
///    SHA1 hash of the `sso_start_url`).
/// 3. Call the SSO `GetRoleCredentials` API (an unauthenticated GET request)
///    to exchange the access token for temporary IAM credentials.
pub struct SsoCredentialProvider {
    pub profile: String,
}

impl SsoCredentialProvider {
    pub fn new(profile: &str) -> Self {
        Self {
            profile: profile.to_string(),
        }
    }

    /// Return the path to the AWS config file, respecting `AWS_CONFIG_FILE`.
    fn config_file_path() -> PathBuf {
        if let Ok(p) = std::env::var("AWS_CONFIG_FILE") {
            return PathBuf::from(p);
        }
        let home = std::env::var("HOME").unwrap_or_else(|_| "/".to_string());
        PathBuf::from(home).join(".aws").join("config")
    }

    /// Load SSO-related settings from the profile in the config file.
    fn load_sso_config(&self) -> Result<SsoConfig> {
        let path = Self::config_file_path();
        if !path.exists() {
            bail!(
                "Config file not found: {}. Cannot resolve SSO config for profile '{}'",
                path.display(),
                self.profile
            );
        }

        let data = loader::load_config_file(&path)
            .with_context(|| format!("Failed to load config file: {}", path.display()))?;

        let section = data.get(&self.profile).ok_or_else(|| {
            anyhow::anyhow!(
                "Profile '{}' not found in config file {}",
                self.profile,
                path.display()
            )
        })?;

        parse_sso_config(section, &self.profile)
    }
}

/// Parse SSO configuration values from a profile section.
fn parse_sso_config(section: &HashMap<String, String>, profile_name: &str) -> Result<SsoConfig> {
    let sso_start_url = section
        .get("sso_start_url")
        .ok_or_else(|| {
            anyhow::anyhow!(
                "sso_start_url not found in config for profile '{}'. \
                 Add 'sso_start_url = https://...' to the [profile {}] section.",
                profile_name,
                profile_name,
            )
        })?
        .clone();

    let sso_account_id = section
        .get("sso_account_id")
        .ok_or_else(|| {
            anyhow::anyhow!(
                "sso_account_id not found in config for profile '{}'. \
                 Add 'sso_account_id = <account_id>' to the [profile {}] section.",
                profile_name,
                profile_name,
            )
        })?
        .clone();

    let sso_role_name = section
        .get("sso_role_name")
        .ok_or_else(|| {
            anyhow::anyhow!(
                "sso_role_name not found in config for profile '{}'. \
                 Add 'sso_role_name = <role_name>' to the [profile {}] section.",
                profile_name,
                profile_name,
            )
        })?
        .clone();

    let sso_region = section
        .get("sso_region")
        .cloned()
        .unwrap_or_else(|| "us-east-1".to_string());

    Ok(SsoConfig {
        sso_start_url,
        sso_account_id,
        sso_role_name,
        sso_region,
    })
}

/// Compute the SHA1 hash of the input string as a lowercase hex-encoded string.
///
/// This is used to derive the SSO token cache filename from the `sso_start_url`.
fn sha1_hash(input: &str) -> String {
    use ring::digest;
    let hash = digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, input.as_bytes());
    hash.as_ref().iter().map(|b| format!("{:02x}", b)).collect()
}

/// Return the directory where SSO token cache files are stored.
fn sso_cache_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/".to_string());
    PathBuf::from(home).join(".aws").join("sso").join("cache")
}

/// Build the full path for the cached token file for a given `sso_start_url`.
fn token_cache_path(start_url: &str) -> PathBuf {
    let hash = sha1_hash(start_url);
    sso_cache_dir().join(format!("{}.json", hash))
}

/// Parse the cached SSO token JSON.
fn parse_cached_token(content: &str) -> Result<SsoToken> {
    let json: serde_json::Value =
        serde_json::from_str(content).context("Failed to parse SSO token cache JSON")?;

    let access_token = json
        .get("accessToken")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("accessToken not found in SSO token cache"))?
        .to_string();

    let expires_at = json
        .get("expiresAt")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("expiresAt not found in SSO token cache"))?
        .to_string();

    Ok(SsoToken {
        access_token,
        expires_at,
    })
}

/// Check whether an SSO token has expired.
fn is_token_expired(token: &SsoToken) -> bool {
    // The expiresAt field uses ISO-8601 / RFC-3339 format (e.g. "2023-01-01T00:00:00Z"
    // or "2023-01-01T00:00:00UTC"). Try RFC-3339 first, then a few common variations.
    let expiry = chrono::DateTime::parse_from_rfc3339(&token.expires_at)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(&token.expires_at, "%Y-%m-%dT%H:%M:%SUTC")
                .map(|ndt| ndt.and_utc())
        })
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(&token.expires_at, "%Y-%m-%dT%H:%M:%S")
                .map(|ndt| ndt.and_utc())
        });

    match expiry {
        Ok(dt) => dt <= chrono::Utc::now(),
        // If we cannot parse the timestamp, treat the token as expired to be safe.
        Err(_) => true,
    }
}

/// Look up the cached SSO access token for the given `sso_start_url`.
fn find_cached_token(start_url: &str) -> Result<SsoToken> {
    let cache_path = token_cache_path(start_url);
    if !cache_path.exists() {
        bail!(
            "SSO token cache file not found: {}. Run `aws sso login` to authenticate.",
            cache_path.display()
        );
    }

    let content = std::fs::read_to_string(&cache_path)
        .with_context(|| format!("Failed to read SSO token cache: {}", cache_path.display()))?;

    let token = parse_cached_token(&content)?;

    if is_token_expired(&token) {
        bail!(
            "SSO access token has expired (expiresAt: {}). Run `aws sso login` to re-authenticate.",
            token.expires_at
        );
    }

    Ok(token)
}

/// Parse the SSO GetRoleCredentials JSON response body into `Credentials`.
fn parse_sso_credentials_response(body: &str) -> Result<Credentials> {
    let json: serde_json::Value =
        serde_json::from_str(body).context("Failed to parse SSO GetRoleCredentials response")?;

    let role_creds = json
        .get("roleCredentials")
        .ok_or_else(|| anyhow::anyhow!("roleCredentials not found in SSO response"))?;

    let access_key_id = role_creds
        .get("accessKeyId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("accessKeyId not found in SSO roleCredentials"))?
        .to_string();

    let secret_access_key = role_creds
        .get("secretAccessKey")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("secretAccessKey not found in SSO roleCredentials"))?
        .to_string();

    let session_token = role_creds
        .get("sessionToken")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    if access_key_id.is_empty() {
        bail!("accessKeyId is empty in SSO GetRoleCredentials response");
    }
    if secret_access_key.is_empty() {
        bail!("secretAccessKey is empty in SSO GetRoleCredentials response");
    }

    Ok(Credentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}

/// Call the SSO GetRoleCredentials API and return the raw response body.
///
/// This is an unauthenticated GET request (no SigV4 signing).
fn get_role_credentials(config: &SsoConfig, token: &str) -> Result<Credentials> {
    let endpoint = format!(
        "https://portal.sso.{}.amazonaws.com/federation/credentials",
        config.sso_region
    );

    let rt = tokio::runtime::Runtime::new()
        .context("Failed to create tokio runtime for SSO GetRoleCredentials call")?;

    let response_body = rt.block_on(async {
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .build()
            .context("Failed to build HTTP client for SSO")?;

        let resp = client
            .get(&endpoint)
            .query(&[
                ("role_name", config.sso_role_name.as_str()),
                ("account_id", config.sso_account_id.as_str()),
            ])
            .header("x-amz-sso_bearer_token", token)
            .send()
            .await
            .context("SSO GetRoleCredentials HTTP request failed")?;

        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .context("Failed to read SSO GetRoleCredentials response body")?;

        if status < 200 || status >= 300 {
            bail!(
                "SSO GetRoleCredentials failed with status {}: {}",
                status,
                body
            );
        }

        Ok::<String, anyhow::Error>(body)
    })?;

    parse_sso_credentials_response(&response_body)
}

impl CredentialProvider for SsoCredentialProvider {
    fn resolve(&self) -> Result<Credentials> {
        let config = self
            .load_sso_config()
            .with_context(|| format!("Failed to load SSO config for profile '{}'", self.profile))?;

        let token = find_cached_token(&config.sso_start_url)?;

        get_role_credentials(&config, &token.access_token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as IoWrite;

    // ---------------------------------------------------------------
    // SHA1 hashing
    // ---------------------------------------------------------------

    #[test]
    fn test_sha1_hash_known_value() {
        // SHA1("https://my-sso-portal.awsapps.com/start") is a known value.
        // Verify against an independently computed hash.
        let input = "https://my-sso-portal.awsapps.com/start";
        let hash = sha1_hash(input);
        assert_eq!(hash.len(), 40, "SHA1 hex digest should be 40 characters");
        // Pre-computed via ring::digest::SHA1_FOR_LEGACY_USE_ONLY
        assert_eq!(hash, "c7aaaf71fcc8777ae2475525ed049d39fe16c484");
    }

    #[test]
    fn test_sha1_hash_empty_string() {
        // SHA1("") == da39a3ee5e6b4b0d3255bfef95601890afd80709
        let hash = sha1_hash("");
        assert_eq!(hash, "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    // ---------------------------------------------------------------
    // SSO config parsing
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_sso_config_all_fields() {
        let mut section = HashMap::new();
        section.insert("sso_start_url".to_string(), "https://example.awsapps.com/start".to_string());
        section.insert("sso_account_id".to_string(), "123456789012".to_string());
        section.insert("sso_role_name".to_string(), "MyRole".to_string());
        section.insert("sso_region".to_string(), "eu-west-1".to_string());

        let config = parse_sso_config(&section, "test").unwrap();
        assert_eq!(config.sso_start_url, "https://example.awsapps.com/start");
        assert_eq!(config.sso_account_id, "123456789012");
        assert_eq!(config.sso_role_name, "MyRole");
        assert_eq!(config.sso_region, "eu-west-1");
    }

    #[test]
    fn test_parse_sso_config_default_region() {
        let mut section = HashMap::new();
        section.insert("sso_start_url".to_string(), "https://example.awsapps.com/start".to_string());
        section.insert("sso_account_id".to_string(), "123456789012".to_string());
        section.insert("sso_role_name".to_string(), "MyRole".to_string());
        // sso_region is NOT set

        let config = parse_sso_config(&section, "test").unwrap();
        assert_eq!(config.sso_region, "us-east-1");
    }

    #[test]
    fn test_parse_sso_config_missing_start_url() {
        let mut section = HashMap::new();
        section.insert("sso_account_id".to_string(), "123456789012".to_string());
        section.insert("sso_role_name".to_string(), "MyRole".to_string());

        let err = parse_sso_config(&section, "my-profile").unwrap_err();
        assert!(
            err.to_string().contains("sso_start_url"),
            "Error should mention sso_start_url: {}",
            err
        );
    }

    #[test]
    fn test_parse_sso_config_missing_account_id() {
        let mut section = HashMap::new();
        section.insert("sso_start_url".to_string(), "https://example.awsapps.com/start".to_string());
        section.insert("sso_role_name".to_string(), "MyRole".to_string());

        let err = parse_sso_config(&section, "my-profile").unwrap_err();
        assert!(
            err.to_string().contains("sso_account_id"),
            "Error should mention sso_account_id: {}",
            err
        );
    }

    #[test]
    fn test_parse_sso_config_missing_role_name() {
        let mut section = HashMap::new();
        section.insert("sso_start_url".to_string(), "https://example.awsapps.com/start".to_string());
        section.insert("sso_account_id".to_string(), "123456789012".to_string());

        let err = parse_sso_config(&section, "my-profile").unwrap_err();
        assert!(
            err.to_string().contains("sso_role_name"),
            "Error should mention sso_role_name: {}",
            err
        );
    }

    // ---------------------------------------------------------------
    // Token cache file path
    // ---------------------------------------------------------------

    #[test]
    fn test_token_cache_path_calculation() {
        let url = "https://my-sso-portal.awsapps.com/start";
        let path = token_cache_path(url);
        let expected_hash = sha1_hash(url);
        let expected_filename = format!("{}.json", expected_hash);

        assert!(
            path.to_string_lossy().ends_with(&expected_filename),
            "Cache path should end with {}: got {}",
            expected_filename,
            path.display()
        );
        assert!(
            path.to_string_lossy().contains(".aws/sso/cache/"),
            "Cache path should be under .aws/sso/cache/: got {}",
            path.display()
        );
    }

    // ---------------------------------------------------------------
    // Token JSON parsing
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_cached_token_valid() {
        let json = r#"{"accessToken": "my-access-token-abc123", "expiresAt": "2099-01-01T00:00:00Z"}"#;
        let token = parse_cached_token(json).unwrap();
        assert_eq!(token.access_token, "my-access-token-abc123");
        assert_eq!(token.expires_at, "2099-01-01T00:00:00Z");
    }

    #[test]
    fn test_parse_cached_token_missing_access_token() {
        let json = r#"{"expiresAt": "2099-01-01T00:00:00Z"}"#;
        let err = parse_cached_token(json).unwrap_err();
        assert!(
            err.to_string().contains("accessToken"),
            "Error should mention accessToken: {}",
            err
        );
    }

    #[test]
    fn test_parse_cached_token_missing_expires_at() {
        let json = r#"{"accessToken": "my-token"}"#;
        let err = parse_cached_token(json).unwrap_err();
        assert!(
            err.to_string().contains("expiresAt"),
            "Error should mention expiresAt: {}",
            err
        );
    }

    #[test]
    fn test_parse_cached_token_invalid_json() {
        let err = parse_cached_token("not valid json").unwrap_err();
        assert!(
            err.to_string().contains("parse"),
            "Error should mention parsing: {}",
            err
        );
    }

    // ---------------------------------------------------------------
    // Token expiry detection
    // ---------------------------------------------------------------

    #[test]
    fn test_token_not_expired() {
        let token = SsoToken {
            access_token: "tok".to_string(),
            expires_at: "2099-12-31T23:59:59Z".to_string(),
        };
        assert!(!is_token_expired(&token), "Token in the far future should not be expired");
    }

    #[test]
    fn test_token_expired() {
        let token = SsoToken {
            access_token: "tok".to_string(),
            expires_at: "2020-01-01T00:00:00Z".to_string(),
        };
        assert!(is_token_expired(&token), "Token from 2020 should be expired");
    }

    #[test]
    fn test_token_expired_unparseable_date() {
        let token = SsoToken {
            access_token: "tok".to_string(),
            expires_at: "garbage-date".to_string(),
        };
        assert!(
            is_token_expired(&token),
            "Unparseable expiry should be treated as expired"
        );
    }

    // ---------------------------------------------------------------
    // GetRoleCredentials response parsing
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_sso_credentials_response_success() {
        let body = r#"{
            "roleCredentials": {
                "accessKeyId": "ASIAIOSFODNN7EXAMPLE",
                "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "sessionToken": "FwoGZXIvYXdzEBYaDHqa0AP/token",
                "expiration": 1234567890
            }
        }"#;
        let creds = parse_sso_credentials_response(body).unwrap();
        assert_eq!(creds.access_key_id, "ASIAIOSFODNN7EXAMPLE");
        assert_eq!(
            creds.secret_access_key,
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        );
        assert_eq!(
            creds.session_token,
            Some("FwoGZXIvYXdzEBYaDHqa0AP/token".to_string())
        );
    }

    #[test]
    fn test_parse_sso_credentials_response_missing_role_credentials() {
        let body = r#"{"otherField": "value"}"#;
        let err = parse_sso_credentials_response(body).unwrap_err();
        assert!(
            err.to_string().contains("roleCredentials"),
            "Error should mention roleCredentials: {}",
            err
        );
    }

    #[test]
    fn test_parse_sso_credentials_response_missing_access_key() {
        let body = r#"{"roleCredentials": {"secretAccessKey": "secret", "sessionToken": "tok"}}"#;
        let err = parse_sso_credentials_response(body).unwrap_err();
        assert!(
            err.to_string().contains("accessKeyId"),
            "Error should mention accessKeyId: {}",
            err
        );
    }

    #[test]
    fn test_parse_sso_credentials_response_missing_secret_key() {
        let body = r#"{"roleCredentials": {"accessKeyId": "ASIA123", "sessionToken": "tok"}}"#;
        let err = parse_sso_credentials_response(body).unwrap_err();
        assert!(
            err.to_string().contains("secretAccessKey"),
            "Error should mention secretAccessKey: {}",
            err
        );
    }

    #[test]
    fn test_parse_sso_credentials_response_empty_access_key() {
        let body = r#"{"roleCredentials": {"accessKeyId": "", "secretAccessKey": "secret", "sessionToken": "tok"}}"#;
        let err = parse_sso_credentials_response(body).unwrap_err();
        assert!(
            err.to_string().contains("empty"),
            "Error should mention empty: {}",
            err
        );
    }

    #[test]
    fn test_parse_sso_credentials_response_no_session_token() {
        let body = r#"{"roleCredentials": {"accessKeyId": "ASIA123", "secretAccessKey": "secret"}}"#;
        let creds = parse_sso_credentials_response(body).unwrap();
        assert_eq!(creds.access_key_id, "ASIA123");
        assert_eq!(creds.secret_access_key, "secret");
        assert_eq!(creds.session_token, None);
    }

    #[test]
    fn test_parse_sso_credentials_response_invalid_json() {
        let err = parse_sso_credentials_response("not json").unwrap_err();
        assert!(
            err.to_string().contains("parse"),
            "Error should mention parsing: {}",
            err
        );
    }

    // ---------------------------------------------------------------
    // Config file loading via SsoCredentialProvider
    // ---------------------------------------------------------------

    #[test]
    fn test_load_sso_config_from_file() {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            config,
            "[profile sso-test]\nsso_start_url = https://example.awsapps.com/start\nsso_account_id = 111122223333\nsso_role_name = ReadOnly\nsso_region = us-west-2"
        )
        .unwrap();

        let data = loader::load_config_file(config.path()).unwrap();
        let section = data.get("sso-test").expect("section after normalization");
        let sso_config = parse_sso_config(section, "sso-test").unwrap();

        assert_eq!(sso_config.sso_start_url, "https://example.awsapps.com/start");
        assert_eq!(sso_config.sso_account_id, "111122223333");
        assert_eq!(sso_config.sso_role_name, "ReadOnly");
        assert_eq!(sso_config.sso_region, "us-west-2");
    }

    #[test]
    fn test_load_sso_config_profile_not_found() {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        writeln!(config, "[default]\nregion = us-east-1").unwrap();

        let data = loader::load_config_file(config.path()).unwrap();
        assert!(data.get("nonexistent-sso").is_none(), "profile should not exist");
    }

    // ---------------------------------------------------------------
    // Provider construction
    // ---------------------------------------------------------------

    #[test]
    fn test_provider_new() {
        let provider = SsoCredentialProvider::new("my-sso-profile");
        assert_eq!(provider.profile, "my-sso-profile");
    }
}
