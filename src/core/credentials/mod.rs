#[allow(dead_code)]
pub mod assume_role;
pub mod chain;
pub mod env;
pub mod imds;
#[allow(dead_code)]
pub mod process;
pub mod profile;
#[allow(dead_code)]
pub mod sso;
#[allow(dead_code)]
pub mod web_identity;

use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

pub trait CredentialProvider: Send + Sync {
    fn resolve(&self) -> Result<Credentials>;
}
