use anyhow::{bail, Result};

use super::assume_role::AssumeRoleProvider;
use super::env::EnvCredentialProvider;
use super::imds::ImdsCredentialProvider;
use super::process::CredentialProcessProvider;
use super::profile::ProfileCredentialProvider;
use super::sso::SsoCredentialProvider;
use super::web_identity::WebIdentityTokenProvider;
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

/// Build the standard credential provider chain.
///
/// When `explicit_profile` is true (i.e. the user passed `--profile`), environment
/// variable credentials are skipped so that the named profile's configuration is
/// always honoured.
///
/// The full chain (when `explicit_profile` is false) is:
///   EnvCredentialProvider -> CredentialProcessProvider -> ProfileCredentialProvider
///   -> AssumeRoleProvider -> SsoCredentialProvider -> WebIdentityTokenProvider
///   -> ImdsCredentialProvider (ECS/EC2 instance metadata)
///
/// When `explicit_profile` is true the chain starts at CredentialProcessProvider.
pub fn build_credential_chain(
    profile: &str,
    explicit_profile: bool,
    region: Option<&str>,
) -> ChainCredentialProvider {
    let mut providers: Vec<Box<dyn CredentialProvider>> = Vec::new();

    if !explicit_profile {
        providers.push(Box::new(EnvCredentialProvider));
    }

    providers.push(Box::new(CredentialProcessProvider::new(profile)));
    providers.push(Box::new(ProfileCredentialProvider::new(profile)));
    providers.push(Box::new(AssumeRoleProvider::new(profile, region)));
    providers.push(Box::new(SsoCredentialProvider::new(profile)));
    providers.push(Box::new(WebIdentityTokenProvider::new(profile)));
    providers.push(Box::new(ImdsCredentialProvider));

    ChainCredentialProvider::new(providers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credential_chain_empty() {
        let chain = ChainCredentialProvider::new(vec![]);
        assert!(chain.resolve().is_err());
    }

    #[test]
    fn test_credential_chain_all_fail() {
        // A chain where every provider fails should return an error
        struct FailingProvider;
        impl CredentialProvider for FailingProvider {
            fn resolve(&self) -> Result<Credentials> {
                bail!("always fails")
            }
        }
        let chain = ChainCredentialProvider::new(vec![
            Box::new(FailingProvider),
            Box::new(FailingProvider),
        ]);
        assert!(chain.resolve().is_err());
    }

    #[test]
    fn test_credential_chain_first_success_wins() {
        struct SuccessProvider(String);
        impl CredentialProvider for SuccessProvider {
            fn resolve(&self) -> Result<Credentials> {
                Ok(Credentials {
                    access_key_id: self.0.clone(),
                    secret_access_key: "secret".to_string(),
                    session_token: None,
                })
            }
        }
        struct FailingProvider;
        impl CredentialProvider for FailingProvider {
            fn resolve(&self) -> Result<Credentials> {
                bail!("always fails")
            }
        }
        let chain = ChainCredentialProvider::new(vec![
            Box::new(FailingProvider),
            Box::new(SuccessProvider("AKID_SECOND".to_string())),
            Box::new(SuccessProvider("AKID_THIRD".to_string())),
        ]);
        let creds = chain.resolve().unwrap();
        assert_eq!(creds.access_key_id, "AKID_SECOND");
    }

    #[test]
    fn test_build_credential_chain_no_explicit_profile() {
        // When explicit_profile is false, the chain should include the env provider
        // (7 providers total: env, process, profile, assume_role, sso, web_identity, imds)
        let chain = build_credential_chain("default", false, Some("us-east-1"));
        assert_eq!(chain.providers.len(), 7);
    }

    #[test]
    fn test_build_credential_chain_explicit_profile() {
        // When explicit_profile is true, the chain should skip the env provider
        // (6 providers total: process, profile, assume_role, sso, web_identity, imds)
        let chain = build_credential_chain("my-profile", true, Some("us-east-1"));
        assert_eq!(chain.providers.len(), 6);
    }

    #[test]
    fn test_build_credential_chain_no_region() {
        // Region can be None
        let chain = build_credential_chain("default", false, None);
        assert_eq!(chain.providers.len(), 7);
    }
}
