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
    /// HTTP 4xx client error -> exit 1
    Client(String),
    /// Usage/argument error (missing args, unknown service) -> exit 2
    Usage(String),
    /// Waiter failure or timeout -> exit 255
    Waiter(String),
    /// HTTP 5xx server error or internal error -> exit 255
    Server(String),
}

impl fmt::Display for CliExitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliExitError::Client(msg) => write!(f, "{msg}"),
            CliExitError::Usage(msg) => write!(f, "{msg}"),
            CliExitError::Waiter(msg) => write!(f, "{msg}"),
            CliExitError::Server(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for CliExitError {}

/// Classify an anyhow error into an AWS CLI-compatible exit code.
///
/// Exit codes:
/// - 0: success (not handled here)
/// - 1: client error (HTTP 4xx)
/// - 2: usage error (missing args, unknown service/operation)
/// - 130: SIGINT (handled in main.rs)
/// - 255: internal/server error, waiter failures
pub fn classify_exit_code(err: &anyhow::Error) -> i32 {
    // 1. Check for explicit CliExitError
    if let Some(cli_err) = err.downcast_ref::<CliExitError>() {
        return match cli_err {
            CliExitError::Client(_) => 1,
            CliExitError::Usage(_) => 2,
            CliExitError::Waiter(_) => 255,
            CliExitError::Server(_) => 255,
        };
    }

    // 2. Check for RawsError::Service with HTTP status
    if let Some(RawsError::Service { status, .. }) = err.downcast_ref::<RawsError>() {
        return if *status >= 400 && *status < 500 { 1 } else { 255 };
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
    if msg.contains("Waiter") && (msg.contains("failed") || msg.contains("timed out")) {
        return 255;
    }
    if msg.contains("HTTP 4") || msg.contains("(HTTP 4") {
        return 1;
    }
    if msg.contains("HTTP 5") || msg.contains("(HTTP 5") {
        return 255;
    }

    // Default: exit 1 for any unclassified error
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_client_error() {
        let err = anyhow::Error::new(CliExitError::Client("Not found".into()));
        assert_eq!(classify_exit_code(&err), 1);
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
    fn test_classify_server_error() {
        let err = anyhow::Error::new(CliExitError::Server("Internal".into()));
        assert_eq!(classify_exit_code(&err), 255);
    }

    #[test]
    fn test_classify_raws_service_4xx() {
        let err = anyhow::Error::new(RawsError::Service {
            code: "AccessDenied".into(),
            message: "Access denied".into(),
            status: 403,
        });
        assert_eq!(classify_exit_code(&err), 1);
    }

    #[test]
    fn test_classify_raws_service_5xx() {
        let err = anyhow::Error::new(RawsError::Service {
            code: "InternalError".into(),
            message: "Internal".into(),
            status: 500,
        });
        assert_eq!(classify_exit_code(&err), 255);
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
    fn test_classify_message_http_4xx() {
        let err = anyhow::anyhow!("AWS Error (HTTP 404): NoSuchBucket");
        assert_eq!(classify_exit_code(&err), 1);
    }

    #[test]
    fn test_classify_message_http_5xx() {
        let err = anyhow::anyhow!("AWS Error (HTTP 500): InternalServerError");
        assert_eq!(classify_exit_code(&err), 255);
    }

    #[test]
    fn test_classify_unknown_defaults_to_1() {
        let err = anyhow::anyhow!("Something completely unexpected happened");
        assert_eq!(classify_exit_code(&err), 1);
    }
}
