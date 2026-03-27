use std::fmt;

#[derive(Debug)]
#[allow(dead_code)]
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

// ---------------------------------------------------------------------------
// CLI exit code classification
// ---------------------------------------------------------------------------

/// CLI exit error types for mapping to AWS CLI-compatible exit codes.
#[derive(Debug)]
#[allow(dead_code)]
pub enum CliExitError {
    /// AWS service error (API returned error) -> exit 254
    ServiceError(String),
    /// No credentials available -> exit 253
    NoCredentials(String),
    /// Parameter validation error -> exit 252
    ParamValidation(String),
    /// Usage/argument error (missing args, unknown service) -> exit 2
    Usage(String),
    /// Waiter failure or timeout -> exit 255
    Waiter(String),
    /// Client-side error (config, credentials, internal) -> exit 255
    ClientError(String),
}

impl fmt::Display for CliExitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliExitError::ServiceError(msg) => write!(f, "{msg}"),
            CliExitError::NoCredentials(msg) => write!(f, "{msg}"),
            CliExitError::ParamValidation(msg) => write!(f, "{msg}"),
            CliExitError::Usage(msg) => write!(f, "{msg}"),
            CliExitError::Waiter(msg) => write!(f, "{msg}"),
            CliExitError::ClientError(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for CliExitError {}

/// Classify an anyhow error into an AWS CLI-compatible exit code.
///
/// Exit codes:
/// - 0: success (not handled here)
/// - 2: usage error (missing args, unknown service/operation)
/// - 130: SIGINT (handled in main.rs)
/// - 252: parameter validation error
/// - 253: no credentials available
/// - 254: AWS service error (API returned error)
/// - 255: internal/server error, waiter failures, other client errors
pub fn classify_exit_code(err: &anyhow::Error) -> i32 {
    // 1. Check for explicit CliExitError
    if let Some(cli_err) = err.downcast_ref::<CliExitError>() {
        return match cli_err {
            CliExitError::ServiceError(_) => 254,
            CliExitError::NoCredentials(_) => 253,
            CliExitError::ParamValidation(_) => 252,
            CliExitError::Usage(_) => 2,
            CliExitError::Waiter(_) => 255,
            CliExitError::ClientError(_) => 255,
        };
    }

    // 2. Check for RawsError variants
    if let Some(raws_err) = err.downcast_ref::<RawsError>() {
        return match raws_err {
            RawsError::Service { .. } => 254,
            RawsError::Credential(_) => 253,
            _ => 255,
        };
    }

    // 3. Message-based heuristic fallback
    let msg = format!("{err:#}");
    if msg.contains("Missing operation")
        || msg.contains("Unknown operation")
        || msg.contains("Usage:")
        || msg.contains("usage:")
        || msg.contains("Service model not found")
    {
        return 2;
    }
    if msg.contains("Unable to locate credentials") {
        return 253;
    }
    if msg.contains("Waiter") && (msg.contains("failed") || msg.contains("timed out")) {
        return 255;
    }
    // "An error occurred" is the standard AWS error format
    if msg.contains("An error occurred") && msg.contains("when calling the") {
        return 254;
    }

    // Default: exit 255 for any unclassified error (client error)
    255
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_service_error() {
        let err = anyhow::Error::new(CliExitError::ServiceError("Not found".into()));
        assert_eq!(classify_exit_code(&err), 254);
    }

    #[test]
    fn test_classify_usage_error() {
        let err = anyhow::Error::new(CliExitError::Usage("Missing arg".into()));
        assert_eq!(classify_exit_code(&err), 2);
    }

    #[test]
    fn test_classify_waiter_error() {
        let err = anyhow::Error::new(CliExitError::Waiter("Waiter failed".into()));
        assert_eq!(classify_exit_code(&err), 255);
    }

    #[test]
    fn test_classify_no_credentials_error() {
        let err = anyhow::Error::new(CliExitError::NoCredentials("Unable to locate credentials".into()));
        assert_eq!(classify_exit_code(&err), 253);
    }

    #[test]
    fn test_classify_param_validation_error() {
        let err = anyhow::Error::new(CliExitError::ParamValidation("Missing required param".into()));
        assert_eq!(classify_exit_code(&err), 252);
    }

    #[test]
    fn test_classify_client_error() {
        let err = anyhow::Error::new(CliExitError::ClientError("Internal".into()));
        assert_eq!(classify_exit_code(&err), 255);
    }

    #[test]
    fn test_classify_raws_credential_error() {
        let err = anyhow::Error::new(RawsError::Credential("No creds".into()));
        assert_eq!(classify_exit_code(&err), 253);
    }

    #[test]
    fn test_classify_raws_service_4xx() {
        let err = anyhow::Error::new(RawsError::Service {
            code: "AccessDenied".into(),
            message: "Access denied".into(),
            status: 403,
        });
        assert_eq!(classify_exit_code(&err), 254);
    }

    #[test]
    fn test_classify_raws_service_5xx() {
        let err = anyhow::Error::new(RawsError::Service {
            code: "InternalError".into(),
            message: "Internal".into(),
            status: 500,
        });
        assert_eq!(classify_exit_code(&err), 254);
    }

    #[test]
    fn test_classify_message_missing_operation() {
        let err = anyhow::anyhow!("Missing operation name.");
        assert_eq!(classify_exit_code(&err), 2);
    }

    #[test]
    fn test_classify_message_unknown_operation() {
        let err = anyhow::anyhow!("Unknown operation 'foo' for service 'bar'.");
        assert_eq!(classify_exit_code(&err), 2);
    }

    #[test]
    fn test_classify_message_waiter_failed() {
        let err = anyhow::anyhow!("Waiter instance-running failed: entered terminated state");
        assert_eq!(classify_exit_code(&err), 255);
    }

    #[test]
    fn test_classify_message_waiter_timed_out() {
        let err = anyhow::anyhow!("Waiter instance-running timed out after 40 attempts");
        assert_eq!(classify_exit_code(&err), 255);
    }

    #[test]
    fn test_classify_message_aws_error() {
        let err = anyhow::anyhow!("An error occurred (NoSuchBucket) when calling the GetObject operation: not found");
        assert_eq!(classify_exit_code(&err), 254);
    }

    #[test]
    fn test_classify_message_unable_to_locate_credentials() {
        let err = anyhow::anyhow!("Unable to locate credentials. You can configure credentials by running \"raws configure\".");
        assert_eq!(classify_exit_code(&err), 253);
    }

    #[test]
    fn test_classify_unknown_defaults_to_255() {
        let err = anyhow::anyhow!("Something completely unexpected happened");
        assert_eq!(classify_exit_code(&err), 255);
    }
}
