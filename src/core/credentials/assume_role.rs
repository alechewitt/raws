use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;

use super::{CredentialProvider, Credentials};
use crate::core::auth::sigv4;
use crate::core::auth::sigv4::SigningParams;
use crate::core::credentials::env::EnvCredentialProvider;
use crate::core::credentials::profile::ProfileCredentialProvider;

/// Credential provider that assumes an IAM role via STS AssumeRole.
///
/// Reads `role_arn` and `source_profile` from the AWS config file, resolves
/// source credentials from the source profile, then calls STS AssumeRole to
/// obtain temporary credentials.
pub struct AssumeRoleProvider {
    pub profile: String,
    pub region: Option<String>,
}

impl AssumeRoleProvider {
    pub fn new(profile: &str, region: Option<&str>) -> Self {
        Self {
            profile: profile.to_string(),
            region: region.map(|s| s.to_string()),
        }
    }

    /// Return the path to the AWS config file.
    fn config_file_path() -> PathBuf {
        if let Ok(p) = std::env::var("AWS_CONFIG_FILE") {
            return PathBuf::from(p);
        }
        let home = std::env::var("HOME").unwrap_or_else(|_| "/".to_string());
        PathBuf::from(home).join(".aws").join("config")
    }

    /// Load the profile section from the config file.
    fn load_profile_config(&self) -> Result<HashMap<String, String>> {
        let path = Self::config_file_path();
        if !path.exists() {
            bail!(
                "Config file not found: {}. Cannot resolve role_arn for profile '{}'",
                path.display(),
                self.profile
            );
        }

        let data = crate::core::config::loader::load_config_file(&path)
            .with_context(|| format!("Failed to load config file: {}", path.display()))?;

        let section = data
            .get(&self.profile)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Profile '{}' not found in config file {}",
                    self.profile,
                    path.display()
                )
            })?
            .clone();

        Ok(section)
    }

    /// Resolve the source credentials by trying env vars first, then the
    /// source profile from the credentials file.
    fn resolve_source_credentials(&self, source_profile: &str) -> Result<Credentials> {
        // Try environment variables first
        let env_provider = EnvCredentialProvider;
        if let Ok(creds) = env_provider.resolve() {
            return Ok(creds);
        }

        // Fall back to the source profile in the credentials file
        let profile_provider = ProfileCredentialProvider::new(source_profile);
        profile_provider
            .resolve()
            .with_context(|| format!("Failed to resolve credentials for source_profile '{source_profile}'"))
    }

    /// Build the STS AssumeRole POST body.
    fn build_request_body(
        role_arn: &str,
        session_name: &str,
        external_id: Option<&str>,
    ) -> String {
        let mut body = format!(
            "Action=AssumeRole&Version=2011-06-15&RoleArn={}&RoleSessionName={}&DurationSeconds=3600",
            url_encode(role_arn),
            url_encode(session_name),
        );

        if let Some(eid) = external_id {
            body.push_str(&format!("&ExternalId={}", url_encode(eid)));
        }

        body
    }

    /// Make the STS AssumeRole HTTP call and return the raw response body.
    fn call_sts_assume_role(
        &self,
        source_creds: &Credentials,
        role_arn: &str,
        session_name: &str,
        external_id: Option<&str>,
    ) -> Result<String> {
        let body = Self::build_request_body(role_arn, session_name, external_id);
        let endpoint = "https://sts.amazonaws.com";

        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let region = self.region.as_deref().unwrap_or("us-east-1");

        let mut headers = vec![
            ("host".to_string(), "sts.amazonaws.com".to_string()),
            (
                "content-type".to_string(),
                "application/x-www-form-urlencoded; charset=utf-8".to_string(),
            ),
        ];

        let signing_params = SigningParams::from_credentials(source_creds, region, "sts", &datetime);

        sigv4::sign_request(
            "POST",
            "/",
            "",
            &mut headers,
            body.as_bytes(),
            &signing_params,
        )
        .context("Failed to sign STS AssumeRole request")?;

        // Build the reqwest request using the async client inside a new runtime.
        // We cannot use reqwest::blocking because the feature is not enabled,
        // and the CredentialProvider trait is sync, so we spin up a small runtime.
        let rt = tokio::runtime::Runtime::new()
            .context("Failed to create tokio runtime for STS call")?;

        let response_body = rt.block_on(async {
            let client = reqwest::Client::builder()
                .use_rustls_tls()
                .build()
                .context("Failed to build HTTP client for STS")?;

            let mut req_builder = client.post(endpoint);
            for (k, v) in &headers {
                req_builder = req_builder.header(k, v);
            }
            req_builder = req_builder.body(body);

            let resp = req_builder
                .send()
                .await
                .context("STS AssumeRole HTTP request failed")?;

            let status = resp.status().as_u16();
            let resp_body = resp
                .text()
                .await
                .context("Failed to read STS AssumeRole response body")?;

            if !(200..300).contains(&status) {
                bail!(
                    "STS AssumeRole failed with status {}: {}",
                    status,
                    resp_body
                );
            }

            Ok::<String, anyhow::Error>(resp_body)
        })?;

        Ok(response_body)
    }
}

/// URL-encode a value for use in a form-urlencoded body.
fn url_encode(value: &str) -> String {
    percent_encoding::utf8_percent_encode(
        value,
        percent_encoding::NON_ALPHANUMERIC,
    )
    .to_string()
}

/// Extract the text content between `<Tag>` and `</Tag>` from an XML string.
///
/// Returns `None` if the tag is not found.
fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");

    let start = xml.find(&open)?;
    let after_open = start + open.len();
    let end = xml[after_open..].find(&close)?;
    Some(xml[after_open..after_open + end].to_string())
}

/// Parse temporary credentials from an STS AssumeRole XML response.
fn parse_assume_role_response(xml: &str) -> Result<Credentials> {
    let access_key = extract_xml_tag(xml, "AccessKeyId")
        .context("AccessKeyId not found in STS AssumeRole response")?;
    let secret_key = extract_xml_tag(xml, "SecretAccessKey")
        .context("SecretAccessKey not found in STS AssumeRole response")?;
    let session_token = extract_xml_tag(xml, "SessionToken")
        .context("SessionToken not found in STS AssumeRole response")?;

    if access_key.is_empty() {
        bail!("AccessKeyId is empty in STS AssumeRole response");
    }
    if secret_key.is_empty() {
        bail!("SecretAccessKey is empty in STS AssumeRole response");
    }

    Ok(Credentials {
        access_key_id: access_key,
        secret_access_key: secret_key,
        session_token: Some(session_token),
    })
}

impl CredentialProvider for AssumeRoleProvider {
    fn resolve(&self) -> Result<Credentials> {
        let config = self
            .load_profile_config()
            .with_context(|| format!("Failed to load config for profile '{}'", self.profile))?;

        let role_arn = config.get("role_arn").ok_or_else(|| {
            anyhow::anyhow!(
                "role_arn not found in config for profile '{}'. \
                 Add 'role_arn = arn:aws:iam::...' to the [profile {}] section in your AWS config file.",
                self.profile,
                self.profile,
            )
        })?;

        let source_profile = config.get("source_profile").ok_or_else(|| {
            anyhow::anyhow!(
                "source_profile not found in config for profile '{}'. \
                 Add 'source_profile = <profile_name>' to the [profile {}] section in your AWS config file.",
                self.profile,
                self.profile,
            )
        })?;

        let session_name = config
            .get("role_session_name")
            .cloned()
            .unwrap_or_else(|| "raws-session".to_string());

        let external_id = config.get("external_id").cloned();

        let source_creds = self.resolve_source_credentials(source_profile)?;

        let response_xml = self.call_sts_assume_role(
            &source_creds,
            role_arn,
            &session_name,
            external_id.as_deref(),
        )?;

        parse_assume_role_response(&response_xml)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as IoWrite;

    // ---------------------------------------------------------------
    // XML parsing tests
    // ---------------------------------------------------------------

    #[test]
    fn test_extract_xml_tag_found() {
        let xml = "<Root><Name>hello</Name></Root>";
        assert_eq!(extract_xml_tag(xml, "Name"), Some("hello".to_string()));
    }

    #[test]
    fn test_extract_xml_tag_not_found() {
        let xml = "<Root><Other>value</Other></Root>";
        assert_eq!(extract_xml_tag(xml, "Name"), None);
    }

    #[test]
    fn test_extract_xml_tag_empty_content() {
        let xml = "<Root><Name></Name></Root>";
        assert_eq!(extract_xml_tag(xml, "Name"), Some(String::new()));
    }

    #[test]
    fn test_extract_xml_tag_nested() {
        let xml = r#"<AssumeRoleResponse>
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIAFOO</AccessKeyId>
      <SecretAccessKey>secretbar</SecretAccessKey>
      <SessionToken>tokenbaz</SessionToken>
      <Expiration>2023-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;
        assert_eq!(
            extract_xml_tag(xml, "AccessKeyId"),
            Some("ASIAFOO".to_string())
        );
        assert_eq!(
            extract_xml_tag(xml, "SecretAccessKey"),
            Some("secretbar".to_string())
        );
        assert_eq!(
            extract_xml_tag(xml, "SessionToken"),
            Some("tokenbaz".to_string())
        );
        assert_eq!(
            extract_xml_tag(xml, "Expiration"),
            Some("2023-01-01T00:00:00Z".to_string())
        );
    }

    #[test]
    fn test_parse_assume_role_response_success() {
        let xml = r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIAIOSFODNN7EXAMPLE</AccessKeyId>
      <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</SecretAccessKey>
      <SessionToken>FwoGZXIvYXdzEBYaDHqa0AP/session/token</SessionToken>
      <Expiration>2023-06-15T12:00:00Z</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <AssumedRoleId>AROA3XFRBF23:raws-session</AssumedRoleId>
      <Arn>arn:aws:sts::123456789012:assumed-role/TestRole/raws-session</Arn>
    </AssumedRoleUser>
  </AssumeRoleResult>
  <ResponseMetadata>
    <RequestId>c6104cbe-af31-11e0-8154-cef7c0780000</RequestId>
  </ResponseMetadata>
</AssumeRoleResponse>"#;

        let creds = parse_assume_role_response(xml).expect("should parse");
        assert_eq!(creds.access_key_id, "ASIAIOSFODNN7EXAMPLE");
        assert_eq!(
            creds.secret_access_key,
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        );
        assert_eq!(
            creds.session_token,
            Some("FwoGZXIvYXdzEBYaDHqa0AP/session/token".to_string())
        );
    }

    #[test]
    fn test_parse_assume_role_response_missing_access_key() {
        let xml = r#"<AssumeRoleResponse>
  <AssumeRoleResult>
    <Credentials>
      <SecretAccessKey>secret</SecretAccessKey>
      <SessionToken>token</SessionToken>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;

        let result = parse_assume_role_response(xml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("AccessKeyId"),
            "Error should mention AccessKeyId: {err_msg}"
        );
    }

    #[test]
    fn test_parse_assume_role_response_missing_secret_key() {
        let xml = r#"<AssumeRoleResponse>
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIA123</AccessKeyId>
      <SessionToken>token</SessionToken>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;

        let result = parse_assume_role_response(xml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("SecretAccessKey"),
            "Error should mention SecretAccessKey: {err_msg}"
        );
    }

    #[test]
    fn test_parse_assume_role_response_missing_session_token() {
        let xml = r#"<AssumeRoleResponse>
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIA123</AccessKeyId>
      <SecretAccessKey>secret</SecretAccessKey>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;

        let result = parse_assume_role_response(xml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("SessionToken"),
            "Error should mention SessionToken: {err_msg}"
        );
    }

    #[test]
    fn test_parse_assume_role_response_empty_access_key() {
        let xml = r#"<AssumeRoleResponse>
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId></AccessKeyId>
      <SecretAccessKey>secret</SecretAccessKey>
      <SessionToken>token</SessionToken>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;

        let result = parse_assume_role_response(xml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("empty"),
            "Error should mention empty: {err_msg}"
        );
    }

    // ---------------------------------------------------------------
    // Request body construction tests
    // ---------------------------------------------------------------

    #[test]
    fn test_build_request_body_basic() {
        let body = AssumeRoleProvider::build_request_body(
            "arn:aws:iam::123456789012:role/TestRole",
            "raws-session",
            None,
        );
        assert!(body.contains("Action=AssumeRole"));
        assert!(body.contains("Version=2011-06-15"));
        assert!(body.contains("DurationSeconds=3600"));
        assert!(body.contains("RoleSessionName=raws%2Dsession"));
        assert!(body.contains("RoleArn="));
        assert!(!body.contains("ExternalId="));
    }

    #[test]
    fn test_build_request_body_with_external_id() {
        let body = AssumeRoleProvider::build_request_body(
            "arn:aws:iam::123456789012:role/TestRole",
            "raws-session",
            Some("ext-id-123"),
        );
        assert!(body.contains("ExternalId=ext%2Did%2D123"));
    }

    #[test]
    fn test_build_request_body_custom_session_name() {
        let body = AssumeRoleProvider::build_request_body(
            "arn:aws:iam::123456789012:role/TestRole",
            "my-custom-session",
            None,
        );
        assert!(body.contains("RoleSessionName=my%2Dcustom%2Dsession"));
    }

    // ---------------------------------------------------------------
    // Config loading / profile resolution tests
    // ---------------------------------------------------------------

    #[test]
    fn test_missing_role_arn_detected() {
        // Test the logic without env vars - parse a config and check role_arn absence
        let content = "[profile test-assume]\nsource_profile = base\nregion = us-east-1";
        let data = crate::core::config::loader::parse_ini(content);
        let section = data.get("profile test-assume").expect("section");
        assert!(section.get("role_arn").is_none(), "role_arn should be absent");
    }

    #[test]
    fn test_missing_source_profile_detected() {
        let content = "[profile test-assume]\nrole_arn = arn:aws:iam::123456789012:role/TestRole\nregion = us-east-1";
        let data = crate::core::config::loader::parse_ini(content);
        let section = data.get("profile test-assume").expect("section");
        assert!(section.get("source_profile").is_none(), "source_profile should be absent");
        assert!(section.get("role_arn").is_some());
    }

    #[test]
    fn test_profile_not_found_in_config() {
        let content = "[default]\nregion = us-east-1";
        let data = crate::core::config::loader::parse_ini(content);
        assert!(data.get("profile nonexistent").is_none());
    }

    #[test]
    fn test_config_reads_role_arn_and_source_profile() {
        let content = "[profile assume-test]\nrole_arn = arn:aws:iam::111111111111:role/MyRole\nsource_profile = dev\nregion = eu-west-1";
        let mut temp = tempfile::NamedTempFile::new().expect("temp file");
        IoWrite::write_all(&mut temp, content.as_bytes()).expect("write");
        let data = crate::core::config::loader::load_config_file(temp.path()).expect("load");
        let section = data.get("assume-test").expect("section after normalization");

        assert_eq!(section.get("role_arn").map(|s| s.as_str()), Some("arn:aws:iam::111111111111:role/MyRole"));
        assert_eq!(section.get("source_profile").map(|s| s.as_str()), Some("dev"));
        assert_eq!(section.get("region").map(|s| s.as_str()), Some("eu-west-1"));
    }

    #[test]
    fn test_config_reads_external_id() {
        let content = "[profile ext-test]\nrole_arn = arn:aws:iam::222222222222:role/ExtRole\nsource_profile = base\nexternal_id = my-ext-id-456";
        let mut temp = tempfile::NamedTempFile::new().expect("temp file");
        IoWrite::write_all(&mut temp, content.as_bytes()).expect("write");
        let data = crate::core::config::loader::load_config_file(temp.path()).expect("load");
        let section = data.get("ext-test").expect("section");

        assert_eq!(section.get("external_id").map(|s| s.as_str()), Some("my-ext-id-456"));
    }

    #[test]
    fn test_config_reads_custom_session_name() {
        let content = "[profile session-test]\nrole_arn = arn:aws:iam::333333333333:role/SessRole\nsource_profile = base\nrole_session_name = custom-session-42";
        let mut temp = tempfile::NamedTempFile::new().expect("temp file");
        IoWrite::write_all(&mut temp, content.as_bytes()).expect("write");
        let data = crate::core::config::loader::load_config_file(temp.path()).expect("load");
        let section = data.get("session-test").expect("section");

        assert_eq!(section.get("role_session_name").map(|s| s.as_str()), Some("custom-session-42"));
    }

    // ---------------------------------------------------------------
    // URL encoding test
    // ---------------------------------------------------------------

    #[test]
    fn test_url_encode() {
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(
            url_encode("arn:aws:iam::123:role/Test"),
            "arn%3Aaws%3Aiam%3A%3A123%3Arole%2FTest"
        );
        assert_eq!(url_encode("simple"), "simple");
    }

    // ---------------------------------------------------------------
    // Provider construction test
    // ---------------------------------------------------------------

    #[test]
    fn test_provider_new() {
        let provider = AssumeRoleProvider::new("my-profile", Some("us-west-2"));
        assert_eq!(provider.profile, "my-profile");
        assert_eq!(provider.region, Some("us-west-2".to_string()));
    }

    #[test]
    fn test_provider_new_no_region() {
        let provider = AssumeRoleProvider::new("default", None);
        assert_eq!(provider.profile, "default");
        assert_eq!(provider.region, None);
    }
}
