use anyhow::{bail, Context, Result};
use std::path::PathBuf;

use super::{CredentialProvider, Credentials};
use crate::core::config::loader;

pub struct WebIdentityTokenProvider {
    pub profile: String,
}

impl WebIdentityTokenProvider {
    pub fn new(profile: &str) -> Self {
        Self {
            profile: profile.to_string(),
        }
    }

    fn config_file_path() -> PathBuf {
        if let Ok(p) = std::env::var("AWS_CONFIG_FILE") {
            return PathBuf::from(p);
        }
        let home = std::env::var("HOME").unwrap_or_else(|_| "/".to_string());
        PathBuf::from(home).join(".aws").join("config")
    }

    /// Resolve the token file path and role ARN. Environment variables take
    /// precedence over config-file settings.
    fn resolve_config(&self) -> Result<(PathBuf, String, String)> {
        // 1. Try environment variables first
        if let (Ok(token_file), Ok(role_arn)) = (
            std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE"),
            std::env::var("AWS_ROLE_ARN"),
        ) {
            let session_name = std::env::var("AWS_ROLE_SESSION_NAME")
                .unwrap_or_else(|_| "raws-web-identity-session".to_string());
            return Ok((PathBuf::from(token_file), role_arn, session_name));
        }

        // 2. Fall back to config file
        let config_path = Self::config_file_path();
        if config_path.exists() {
            let data = loader::load_config_file(&config_path)?;
            if let Some(section) = data.get(&self.profile) {
                if let (Some(token_file), Some(role_arn)) = (
                    section.get("web_identity_token_file"),
                    section.get("role_arn"),
                ) {
                    let session_name = section
                        .get("role_session_name")
                        .cloned()
                        .unwrap_or_else(|| "raws-web-identity-session".to_string());
                    return Ok((PathBuf::from(token_file), role_arn.clone(), session_name));
                }
            }
        }

        bail!(
            "No web identity token configuration found. Set AWS_WEB_IDENTITY_TOKEN_FILE and \
             AWS_ROLE_ARN environment variables or configure web_identity_token_file and role_arn \
             in your profile '{}'",
            self.profile
        )
    }

    fn read_token_file(path: &PathBuf) -> Result<String> {
        let token = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read web identity token file: {}", path.display()))?;
        let token = token.trim().to_string();
        if token.is_empty() {
            bail!(
                "Web identity token file is empty: {}",
                path.display()
            );
        }
        Ok(token)
    }

    fn build_request_body(role_arn: &str, session_name: &str, token: &str) -> String {
        format!(
            "Action=AssumeRoleWithWebIdentity\
             &Version=2011-06-15\
             &RoleArn={}\
             &RoleSessionName={}\
             &WebIdentityToken={}\
             &DurationSeconds=3600",
            percent_encode(role_arn),
            percent_encode(session_name),
            percent_encode(token),
        )
    }

    fn call_sts(body: &str) -> Result<String> {
        let client = reqwest::blocking::Client::new();
        let response = client
            .post("https://sts.amazonaws.com")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body.to_string())
            .send()
            .context("Failed to send AssumeRoleWithWebIdentity request to STS")?;

        let status = response.status();
        let text = response
            .text()
            .context("Failed to read STS response body")?;

        if !status.is_success() {
            bail!(
                "STS AssumeRoleWithWebIdentity request failed with status {}: {}",
                status.as_u16(),
                text
            );
        }

        Ok(text)
    }

    fn parse_assume_role_response(xml: &str) -> Result<Credentials> {
        let access_key_id = extract_xml_value(xml, "AccessKeyId")
            .context("STS response missing AccessKeyId")?;
        let secret_access_key = extract_xml_value(xml, "SecretAccessKey")
            .context("STS response missing SecretAccessKey")?;
        let session_token = extract_xml_value(xml, "SessionToken")
            .context("STS response missing SessionToken")?;

        Ok(Credentials {
            access_key_id,
            secret_access_key,
            session_token: Some(session_token),
        })
    }
}

impl CredentialProvider for WebIdentityTokenProvider {
    fn resolve(&self) -> Result<Credentials> {
        let (token_file, role_arn, session_name) = self.resolve_config()?;
        let token = Self::read_token_file(&token_file)?;
        let body = Self::build_request_body(&role_arn, &session_name, &token);
        let xml = Self::call_sts(&body)?;
        Self::parse_assume_role_response(&xml)
    }
}

fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

fn percent_encode(input: &str) -> String {
    percent_encoding::utf8_percent_encode(
        input,
        percent_encoding::NON_ALPHANUMERIC,
    )
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_extract_xml_value_basic() {
        let xml = "<Root><AccessKeyId>ASIA1234</AccessKeyId></Root>";
        assert_eq!(
            extract_xml_value(xml, "AccessKeyId"),
            Some("ASIA1234".to_string())
        );
    }

    #[test]
    fn test_extract_xml_value_missing_tag() {
        let xml = "<Root><Other>value</Other></Root>";
        assert_eq!(extract_xml_value(xml, "AccessKeyId"), None);
    }

    #[test]
    fn test_extract_xml_value_empty_value() {
        let xml = "<Root><AccessKeyId></AccessKeyId></Root>";
        assert_eq!(
            extract_xml_value(xml, "AccessKeyId"),
            Some("".to_string())
        );
    }

    #[test]
    fn test_parse_assume_role_with_web_identity_response() {
        let xml = r#"<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>ASIAEXAMPLE123</AccessKeyId>
      <SecretAccessKey>secretkey456</SecretAccessKey>
      <SessionToken>sessiontoken789</SessionToken>
      <Expiration>2023-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#;

        let creds = WebIdentityTokenProvider::parse_assume_role_response(xml).unwrap();
        assert_eq!(creds.access_key_id, "ASIAEXAMPLE123");
        assert_eq!(creds.secret_access_key, "secretkey456");
        assert_eq!(creds.session_token, Some("sessiontoken789".to_string()));
    }

    #[test]
    fn test_parse_response_missing_access_key_id() {
        let xml = r#"<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <SecretAccessKey>secretkey456</SecretAccessKey>
      <SessionToken>sessiontoken789</SessionToken>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#;

        let err = WebIdentityTokenProvider::parse_assume_role_response(xml).unwrap_err();
        assert!(
            err.to_string().contains("AccessKeyId"),
            "Error should mention AccessKeyId: {}",
            err
        );
    }

    #[test]
    fn test_parse_response_missing_secret_access_key() {
        let xml = r#"<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>ASIAEXAMPLE123</AccessKeyId>
      <SessionToken>sessiontoken789</SessionToken>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#;

        let err = WebIdentityTokenProvider::parse_assume_role_response(xml).unwrap_err();
        assert!(
            err.to_string().contains("SecretAccessKey"),
            "Error should mention SecretAccessKey: {}",
            err
        );
    }

    #[test]
    fn test_parse_response_missing_session_token() {
        let xml = r#"<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>ASIAEXAMPLE123</AccessKeyId>
      <SecretAccessKey>secretkey456</SecretAccessKey>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#;

        let err = WebIdentityTokenProvider::parse_assume_role_response(xml).unwrap_err();
        assert!(
            err.to_string().contains("SessionToken"),
            "Error should mention SessionToken: {}",
            err
        );
    }

    #[test]
    fn test_build_request_body_all_fields() {
        let body = WebIdentityTokenProvider::build_request_body(
            "arn:aws:iam::123456789012:role/MyRole",
            "my-session",
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9",
        );

        assert!(body.contains("Action=AssumeRoleWithWebIdentity"));
        assert!(body.contains("Version=2011-06-15"));
        assert!(body.contains("DurationSeconds=3600"));
        // Role ARN should be percent-encoded (colons become %3A, slashes become %2F)
        assert!(body.contains("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2FMyRole"));
        assert!(body.contains("RoleSessionName=my%2Dsession"));
        assert!(body.contains("WebIdentityToken=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"));
    }

    #[test]
    fn test_build_request_body_default_session_name() {
        let body = WebIdentityTokenProvider::build_request_body(
            "arn:aws:iam::123456789012:role/MyRole",
            "raws-web-identity-session",
            "token123",
        );

        assert!(body.contains("RoleSessionName=raws%2Dweb%2Didentity%2Dsession"));
    }

    #[test]
    fn test_read_token_file() {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp, "my-oidc-token-value").unwrap();

        let token = WebIdentityTokenProvider::read_token_file(&temp.path().to_path_buf()).unwrap();
        assert_eq!(token, "my-oidc-token-value");
    }

    #[test]
    fn test_read_token_file_trims_whitespace() {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp, "  token-with-spaces  \n").unwrap();

        let token = WebIdentityTokenProvider::read_token_file(&temp.path().to_path_buf()).unwrap();
        assert_eq!(token, "token-with-spaces");
    }

    #[test]
    fn test_missing_token_file() {
        let path = PathBuf::from("/tmp/nonexistent_raws_web_identity_token_file_xyz");
        let err = WebIdentityTokenProvider::read_token_file(&path).unwrap_err();
        assert!(
            err.to_string().contains("Failed to read web identity token file"),
            "Error should mention token file: {}",
            err
        );
    }

    #[test]
    fn test_empty_token_file() {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        write!(temp, "   \n  ").unwrap();

        let err = WebIdentityTokenProvider::read_token_file(&temp.path().to_path_buf()).unwrap_err();
        assert!(
            err.to_string().contains("empty"),
            "Error should mention empty: {}",
            err
        );
    }

    #[test]
    fn test_config_file_lookup() {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            config,
            "[profile webid]\nweb_identity_token_file = /tmp/token\nrole_arn = arn:aws:iam::123456789012:role/MyRole\nrole_session_name = my-session"
        )
        .unwrap();

        let data = loader::load_config_file(config.path()).unwrap();
        let section = data.get("webid").expect("section");
        assert_eq!(section.get("web_identity_token_file").map(|s| s.as_str()), Some("/tmp/token"));
        assert_eq!(section.get("role_arn").map(|s| s.as_str()), Some("arn:aws:iam::123456789012:role/MyRole"));
        assert_eq!(section.get("role_session_name").map(|s| s.as_str()), Some("my-session"));
    }

    #[test]
    fn test_config_file_default_session_name() {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            config,
            "[profile webid2]\nweb_identity_token_file = /tmp/token2\nrole_arn = arn:aws:iam::111111111111:role/OtherRole"
        )
        .unwrap();

        let data = loader::load_config_file(config.path()).unwrap();
        let section = data.get("webid2").expect("section");
        assert!(section.get("web_identity_token_file").is_some());
        assert!(section.get("role_arn").is_some());
        // role_session_name not set means default should be used
        assert!(section.get("role_session_name").is_none());
    }

    #[test]
    fn test_config_file_parses_all_web_identity_fields() {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            config,
            "[profile envtest]\nweb_identity_token_file = /tmp/config-token\nrole_arn = arn:aws:iam::111111111111:role/ConfigRole\nrole_session_name = config-session"
        )
        .unwrap();

        let data = loader::load_config_file(config.path()).unwrap();
        let section = data.get("envtest").expect("section");
        assert_eq!(section.get("web_identity_token_file").map(|s| s.as_str()), Some("/tmp/config-token"));
        assert_eq!(section.get("role_arn").map(|s| s.as_str()), Some("arn:aws:iam::111111111111:role/ConfigRole"));
        assert_eq!(section.get("role_session_name").map(|s| s.as_str()), Some("config-session"));
    }

    #[test]
    fn test_missing_web_identity_config_fields() {
        let content = "[profile nowebid]\nregion = us-east-1";
        let data = crate::core::config::loader::parse_ini(content);
        let section = data.get("profile nowebid").expect("section");
        assert!(section.get("web_identity_token_file").is_none());
        assert!(section.get("role_arn").is_none());
    }

    #[test]
    fn test_profile_not_found_in_config() {
        let content = "[default]\nregion = us-east-1";
        let data = crate::core::config::loader::parse_ini(content);
        assert!(data.get("profile noprofile").is_none());
    }

    #[test]
    fn test_percent_encode_special_chars() {
        let encoded = percent_encode("arn:aws:iam::123:role/test");
        assert_eq!(encoded, "arn%3Aaws%3Aiam%3A%3A123%3Arole%2Ftest");
    }

    #[test]
    fn test_percent_encode_plain_string() {
        let encoded = percent_encode("simpletoken123");
        assert_eq!(encoded, "simpletoken123");
    }
}
