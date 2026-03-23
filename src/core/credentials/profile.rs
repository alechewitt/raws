use anyhow::{bail, Result};
use std::path::PathBuf;

use super::{CredentialProvider, Credentials};
use crate::core::config::loader;

pub struct ProfileCredentialProvider {
    pub profile: String,
}

impl ProfileCredentialProvider {
    pub fn new(profile: &str) -> Self {
        Self {
            profile: profile.to_string(),
        }
    }

    fn credentials_file_path() -> PathBuf {
        if let Ok(p) = std::env::var("AWS_SHARED_CREDENTIALS_FILE") {
            return PathBuf::from(p);
        }
        let home = std::env::var("HOME").unwrap_or_else(|_| "/".to_string());
        PathBuf::from(home).join(".aws").join("credentials")
    }
}

impl CredentialProvider for ProfileCredentialProvider {
    fn resolve(&self) -> Result<Credentials> {
        let path = Self::credentials_file_path();
        if !path.exists() {
            bail!(
                "Credentials file not found: {}",
                path.display()
            );
        }

        let data = loader::load_credentials_file(&path)?;
        let section = data
            .get(&self.profile)
            .ok_or_else(|| anyhow::anyhow!("Profile '{}' not found in credentials file", self.profile))?;

        let access_key = section
            .get("aws_access_key_id")
            .ok_or_else(|| anyhow::anyhow!("aws_access_key_id not found for profile '{}'", self.profile))?
            .clone();

        let secret_key = section
            .get("aws_secret_access_key")
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "aws_secret_access_key not found for profile '{}'",
                    self.profile
                )
            })?
            .clone();

        let session_token = section.get("aws_session_token").cloned();

        Ok(Credentials {
            access_key_id: access_key,
            secret_access_key: secret_key,
            session_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_profile_credential_provider() {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            temp,
            "[test-profile]\naws_access_key_id = AKIATEST\naws_secret_access_key = secrettest\naws_session_token = tokentest"
        )
        .unwrap();

        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", temp.path().to_str().unwrap());
        let provider = ProfileCredentialProvider::new("test-profile");
        let creds = provider.resolve().unwrap();
        assert_eq!(creds.access_key_id, "AKIATEST");
        assert_eq!(creds.secret_access_key, "secrettest");
        assert_eq!(creds.session_token, Some("tokentest".to_string()));
        std::env::remove_var("AWS_SHARED_CREDENTIALS_FILE");
    }
}
