use std::fmt;

#[derive(Debug)]
pub enum RawsError {
    Credential(String),
    Config(String),
    Signing(String),
    Http(String),
    Protocol(String),
    Model(String),
    Endpoint(String),
    Service {
        code: String,
        message: String,
        status: u16,
    },
}

impl fmt::Display for RawsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RawsError::Credential(msg) => write!(f, "Credential error: {msg}"),
            RawsError::Config(msg) => write!(f, "Config error: {msg}"),
            RawsError::Signing(msg) => write!(f, "Signing error: {msg}"),
            RawsError::Http(msg) => write!(f, "HTTP error: {msg}"),
            RawsError::Protocol(msg) => write!(f, "Protocol error: {msg}"),
            RawsError::Model(msg) => write!(f, "Model error: {msg}"),
            RawsError::Endpoint(msg) => write!(f, "Endpoint error: {msg}"),
            RawsError::Service {
                code,
                message,
                status,
            } => write!(f, "Service error (HTTP {status}): {code} - {message}"),
        }
    }
}

impl std::error::Error for RawsError {}
