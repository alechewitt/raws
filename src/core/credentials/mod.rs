pub mod assume_role;
pub mod chain;
pub mod env;
pub mod imds;
pub mod process;
pub mod profile;
pub mod sso;
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
