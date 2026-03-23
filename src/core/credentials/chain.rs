use anyhow::{bail, Result};

use super::{CredentialProvider, Credentials};

pub struct ChainCredentialProvider {
    providers: Vec<Box<dyn CredentialProvider>>,
}

impl ChainCredentialProvider {
    pub fn new(providers: Vec<Box<dyn CredentialProvider>>) -> Self {
        Self { providers }
    }
}

impl CredentialProvider for ChainCredentialProvider {
    fn resolve(&self) -> Result<Credentials> {
        for provider in &self.providers {
            match provider.resolve() {
                Ok(creds) => return Ok(creds),
                Err(_) => continue,
            }
        }
        bail!("No credentials found. Configure credentials via environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY), ~/.aws/credentials, or --profile")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::credentials::env::EnvCredentialProvider;

    #[test]
    fn test_credential_chain_empty() {
        let chain = ChainCredentialProvider::new(vec![]);
        assert!(chain.resolve().is_err());
    }

    #[test]
    fn test_credential_chain_fallthrough() {
        // Env provider should fail, so chain should fail with only env
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        let chain = ChainCredentialProvider::new(vec![Box::new(EnvCredentialProvider)]);
        assert!(chain.resolve().is_err());
    }
}
