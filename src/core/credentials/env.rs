use anyhow::{bail, Result};

use super::{CredentialProvider, Credentials};

pub struct EnvCredentialProvider;

impl CredentialProvider for EnvCredentialProvider {
    fn resolve(&self) -> Result<Credentials> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| anyhow::anyhow!("AWS_ACCESS_KEY_ID not set"))?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| anyhow::anyhow!("AWS_SECRET_ACCESS_KEY not set"))?;

        if access_key.is_empty() {
            bail!("AWS_ACCESS_KEY_ID is empty");
        }
        if secret_key.is_empty() {
            bail!("AWS_SECRET_ACCESS_KEY is empty");
        }

        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

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

    #[test]
    fn test_env_credential_provider_missing() {
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        let provider = EnvCredentialProvider;
        assert!(provider.resolve().is_err());
    }
}
