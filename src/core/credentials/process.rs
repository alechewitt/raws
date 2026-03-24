use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

use super::{CredentialProvider, Credentials};
use crate::core::config::loader;

pub struct CredentialProcessProvider {
    pub profile: String,
}

impl CredentialProcessProvider {
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

    fn credentials_file_path() -> PathBuf {
        if let Ok(p) = std::env::var("AWS_SHARED_CREDENTIALS_FILE") {
            return PathBuf::from(p);
        }
        let home = std::env::var("HOME").unwrap_or_else(|_| "/".to_string());
        PathBuf::from(home).join(".aws").join("credentials")
    }

    fn find_credential_process(&self) -> Result<String> {
        let config_path = Self::config_file_path();
        let creds_path = Self::credentials_file_path();
        Self::find_credential_process_in_paths(&self.profile, &config_path, &creds_path)
    }

    fn find_credential_process_in_paths(
        profile: &str,
        config_path: &Path,
        credentials_path: &Path,
    ) -> Result<String> {
        // Check config file first (~/.aws/config)
        if config_path.exists() {
            let data = loader::load_config_file(config_path)?;
            if let Some(section) = data.get(profile) {
                if let Some(cmd) = section.get("credential_process") {
                    return Ok(cmd.clone());
                }
            }
        }

        // Fall back to credentials file (~/.aws/credentials)
        if credentials_path.exists() {
            let data = loader::load_credentials_file(credentials_path)?;
            if let Some(section) = data.get(profile) {
                if let Some(cmd) = section.get("credential_process") {
                    return Ok(cmd.clone());
                }
            }
        }

        bail!(
            "No credential_process found for profile '{}'",
            profile
        )
    }

    fn execute_and_parse(cmd: &str) -> Result<Credentials> {
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .output()
            .with_context(|| format!("Failed to execute credential_process command: {}", cmd))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!(
                "credential_process exited with status {}: {}",
                output.status,
                stderr.trim()
            );
        }

        let stdout = String::from_utf8(output.stdout)
            .context("credential_process output is not valid UTF-8")?;

        let json: serde_json::Value =
            serde_json::from_str(&stdout).context("Failed to parse credential_process JSON output")?;

        let version = json
            .get("Version")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow::anyhow!("credential_process output missing or invalid 'Version' field"))?;

        if version != 1 {
            bail!(
                "Unsupported credential_process Version: {} (expected 1)",
                version
            );
        }

        let access_key_id = json
            .get("AccessKeyId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("credential_process output missing 'AccessKeyId'"))?
            .to_string();

        let secret_access_key = json
            .get("SecretAccessKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("credential_process output missing 'SecretAccessKey'"))?
            .to_string();

        let session_token = json
            .get("SessionToken")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(Credentials {
            access_key_id,
            secret_access_key,
            session_token,
        })
    }
}

impl CredentialProvider for CredentialProcessProvider {
    fn resolve(&self) -> Result<Credentials> {
        let cmd = self.find_credential_process()?;
        Self::execute_and_parse(&cmd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_parse_basic_credentials() {
        let json_output = r#"{"Version": 1, "AccessKeyId": "AKIAEXAMPLE", "SecretAccessKey": "secretvalue"}"#;
        let cmd = format!("echo '{}'", json_output);
        let creds = CredentialProcessProvider::execute_and_parse(&cmd).unwrap();
        assert_eq!(creds.access_key_id, "AKIAEXAMPLE");
        assert_eq!(creds.secret_access_key, "secretvalue");
        assert_eq!(creds.session_token, None);
    }

    #[test]
    fn test_parse_with_session_token() {
        let json_output = r#"{"Version": 1, "AccessKeyId": "AKIAEXAMPLE", "SecretAccessKey": "secretvalue", "SessionToken": "tokenvalue"}"#;
        let cmd = format!("echo '{}'", json_output);
        let creds = CredentialProcessProvider::execute_and_parse(&cmd).unwrap();
        assert_eq!(creds.access_key_id, "AKIAEXAMPLE");
        assert_eq!(creds.secret_access_key, "secretvalue");
        assert_eq!(creds.session_token, Some("tokenvalue".to_string()));
    }

    #[test]
    fn test_missing_access_key_id() {
        let json_output = r#"{"Version": 1, "SecretAccessKey": "secretvalue"}"#;
        let cmd = format!("echo '{}'", json_output);
        let err = CredentialProcessProvider::execute_and_parse(&cmd).unwrap_err();
        assert!(
            err.to_string().contains("AccessKeyId"),
            "Error should mention AccessKeyId: {}",
            err
        );
    }

    #[test]
    fn test_missing_secret_access_key() {
        let json_output = r#"{"Version": 1, "AccessKeyId": "AKIAEXAMPLE"}"#;
        let cmd = format!("echo '{}'", json_output);
        let err = CredentialProcessProvider::execute_and_parse(&cmd).unwrap_err();
        assert!(
            err.to_string().contains("SecretAccessKey"),
            "Error should mention SecretAccessKey: {}",
            err
        );
    }

    #[test]
    fn test_invalid_json() {
        let cmd = "echo 'not json at all'";
        let err = CredentialProcessProvider::execute_and_parse(cmd).unwrap_err();
        assert!(
            err.to_string().contains("parse"),
            "Error should mention parsing: {}",
            err
        );
    }

    #[test]
    fn test_command_not_found() {
        let cmd = "nonexistent_command_that_does_not_exist_xyz_2024";
        let result = CredentialProcessProvider::execute_and_parse(cmd);
        assert!(result.is_err());
    }

    #[test]
    fn test_version_must_be_1() {
        let json_output = r#"{"Version": 2, "AccessKeyId": "AKIAEXAMPLE", "SecretAccessKey": "secretvalue"}"#;
        let cmd = format!("echo '{}'", json_output);
        let err = CredentialProcessProvider::execute_and_parse(&cmd).unwrap_err();
        assert!(
            err.to_string().contains("Unsupported credential_process Version"),
            "Error should mention unsupported version: {}",
            err
        );
    }

    #[test]
    fn test_missing_version() {
        let json_output = r#"{"AccessKeyId": "AKIAEXAMPLE", "SecretAccessKey": "secretvalue"}"#;
        let cmd = format!("echo '{}'", json_output);
        let err = CredentialProcessProvider::execute_and_parse(&cmd).unwrap_err();
        assert!(
            err.to_string().contains("Version"),
            "Error should mention Version: {}",
            err
        );
    }

    #[test]
    fn test_find_credential_process_in_config_file() {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            config,
            "[profile myprof]\ncredential_process = /usr/bin/my-cred-tool"
        )
        .unwrap();

        // Credentials file has no credential_process for this profile
        let mut creds = tempfile::NamedTempFile::new().unwrap();
        writeln!(creds, "[myprof]\naws_access_key_id = AKIAIGNORE").unwrap();

        let cmd = CredentialProcessProvider::find_credential_process_in_paths(
            "myprof",
            config.path(),
            creds.path(),
        )
        .unwrap();
        assert_eq!(cmd, "/usr/bin/my-cred-tool");
    }

    #[test]
    fn test_find_credential_process_in_credentials_file() {
        // Config file exists but does NOT have credential_process for the profile
        let mut config = tempfile::NamedTempFile::new().unwrap();
        writeln!(config, "[profile myprof]\nregion = us-east-1").unwrap();

        let mut creds = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            creds,
            "[myprof]\ncredential_process = /usr/bin/cred-from-creds-file"
        )
        .unwrap();

        let cmd = CredentialProcessProvider::find_credential_process_in_paths(
            "myprof",
            config.path(),
            creds.path(),
        )
        .unwrap();
        assert_eq!(cmd, "/usr/bin/cred-from-creds-file");
    }

    #[test]
    fn test_profile_not_found() {
        let nonexistent = Path::new("/tmp/nonexistent_raws_test_config_process");
        let err = CredentialProcessProvider::find_credential_process_in_paths(
            "no-such-profile",
            nonexistent,
            nonexistent,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("no-such-profile"),
            "Error should mention the profile name: {}",
            err
        );
    }

    #[test]
    fn test_command_exit_nonzero() {
        let cmd = "exit 1";
        let err = CredentialProcessProvider::execute_and_parse(cmd).unwrap_err();
        assert!(
            err.to_string().contains("exited with status"),
            "Error should mention exit status: {}",
            err
        );
    }
}
