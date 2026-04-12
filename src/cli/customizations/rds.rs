//! RDS custom commands.
//!
//! Implements `generate-db-auth-token`, a client-side command that produces
//! a presigned URL used as an IAM authentication token for RDS databases.

use anyhow::{bail, Result};

use crate::core::auth::sigv4;
use crate::core::credentials::Credentials;

/// Default token lifetime in seconds (15 minutes, matches AWS CLI).
const DEFAULT_EXPIRES_IN: u64 = 900;

/// Signing service for RDS IAM database auth tokens.
const SIGNING_SERVICE: &str = "rds-db";

/// Check whether `operation` is a custom RDS command handled here.
pub fn is_custom_command(operation: &str) -> bool {
    operation == "generate-db-auth-token"
}

/// Generate an IAM auth token for an RDS database connection.
///
/// The token is a SigV4-presigned URL (with scheme stripped) that encodes
/// `Action=connect&DBUser=<username>` against the database endpoint.
pub fn generate_db_auth_token(
    hostname: &str,
    port: u16,
    username: &str,
    region: &str,
    credentials: &Credentials,
) -> Result<String> {
    let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    generate_db_auth_token_with_datetime(hostname, port, username, region, credentials, &datetime)
}

fn generate_db_auth_token_with_datetime(
    hostname: &str,
    port: u16,
    username: &str,
    region: &str,
    credentials: &Credentials,
    datetime: &str,
) -> Result<String> {
    let host_with_port = format!("{hostname}:{port}");

    let signing_params = sigv4::SigningParams::from_credentials(
        credentials,
        region,
        SIGNING_SERVICE,
        datetime,
    );

    let credential = format!("{}/{}", signing_params.access_key, signing_params.scope());

    // Build the query parameters that will be presigned.
    // Action=connect and DBUser=<username> are the "payload" parameters.
    let mut query_pairs: Vec<(String, String)> = vec![
        ("Action".to_string(), "connect".to_string()),
        ("DBUser".to_string(), username.to_string()),
        ("X-Amz-Algorithm".to_string(), "AWS4-HMAC-SHA256".to_string()),
        ("X-Amz-Credential".to_string(), credential),
        ("X-Amz-Date".to_string(), datetime.to_string()),
        ("X-Amz-Expires".to_string(), DEFAULT_EXPIRES_IN.to_string()),
        ("X-Amz-SignedHeaders".to_string(), "host".to_string()),
    ];

    if let Some(token) = &credentials.session_token {
        query_pairs.push(("X-Amz-Security-Token".to_string(), token.clone()));
    }

    query_pairs.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_qs = query_pairs
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                sigv4::uri_encode(k, true),
                sigv4::uri_encode(v, true)
            )
        })
        .collect::<Vec<_>>()
        .join("&");

    // Only the host header is signed.
    let headers = vec![("host".to_string(), host_with_port.clone())];
    let payload_hash = "UNSIGNED-PAYLOAD";

    let cr = sigv4::canonical_request("GET", "/", &canonical_qs, &headers, payload_hash);
    let scope = signing_params.scope();
    let sts = sigv4::string_to_sign(datetime, &scope, &cr);

    let key = sigv4::signing_key_cached(
        signing_params.secret_key,
        signing_params.date(),
        signing_params.region,
        signing_params.service,
    );
    let signature = sigv4::calculate_signature(&key, &sts);

    // Build final query string for URL
    let url_qs = query_pairs
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                sigv4::uri_encode(k, true),
                sigv4::uri_encode(v, true)
            )
        })
        .collect::<Vec<_>>()
        .join("&");

    // Token is the presigned URL without the https:// scheme.
    let token = format!(
        "{host_with_port}/?{url_qs}&X-Amz-Signature={signature}"
    );

    Ok(token)
}

/// Parse `generate-db-auth-token` arguments from the trailing args slice.
pub fn parse_generate_db_auth_token_args(args: &[String]) -> Result<(String, u16, String)> {
    let mut hostname: Option<String> = None;
    let mut port: Option<u16> = None;
    let mut username: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--hostname" => {
                i += 1;
                if i >= args.len() {
                    bail!("--hostname requires a value");
                }
                hostname = Some(args[i].clone());
            }
            "--port" => {
                i += 1;
                if i >= args.len() {
                    bail!("--port requires a value");
                }
                port = Some(args[i].parse::<u16>().map_err(|_| {
                    anyhow::anyhow!("--port must be a valid port number, got '{}'", args[i])
                })?);
            }
            "--username" => {
                i += 1;
                if i >= args.len() {
                    bail!("--username requires a value");
                }
                username = Some(args[i].clone());
            }
            other => {
                bail!(
                    "Unknown argument: '{other}'. Usage: raws rds generate-db-auth-token --hostname <host> --port <port> --username <user>"
                );
            }
        }
        i += 1;
    }

    let hostname = hostname.ok_or_else(|| {
        anyhow::anyhow!("the following arguments are required: --hostname")
    })?;
    let port = port.ok_or_else(|| {
        anyhow::anyhow!("the following arguments are required: --port")
    })?;
    let username = username.ok_or_else(|| {
        anyhow::anyhow!("the following arguments are required: --username")
    })?;

    Ok((hostname, port, username))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::credentials::Credentials;

    fn test_creds() -> Credentials {
        Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        }
    }

    fn test_creds_with_token() -> Credentials {
        Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: Some("FwoGZXIvYXdzEBY".to_string()),
        }
    }

    #[test]
    fn test_token_starts_with_hostname() {
        let creds = test_creds();
        let token = generate_db_auth_token_with_datetime(
            "host.us-east-1.rds.amazonaws.com",
            3306,
            "mySQLUser",
            "us-east-1",
            &creds,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(
            token.starts_with("host.us-east-1.rds.amazonaws.com:3306"),
            "Token should start with hostname:port, got: {token}"
        );
    }

    #[test]
    fn test_token_contains_no_scheme() {
        let creds = test_creds();
        let token = generate_db_auth_token_with_datetime(
            "mydb.abc123.us-east-1.rds.amazonaws.com",
            3306,
            "admin",
            "us-east-1",
            &creds,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(
            !token.contains("https://"),
            "Token should not contain scheme: {token}"
        );
    }

    #[test]
    fn test_token_contains_action_connect() {
        let creds = test_creds();
        let token = generate_db_auth_token_with_datetime(
            "mydb.us-east-1.rds.amazonaws.com",
            3306,
            "admin",
            "us-east-1",
            &creds,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(token.contains("Action=connect"), "Token should contain Action=connect: {token}");
    }

    #[test]
    fn test_token_contains_db_user() {
        let creds = test_creds();
        let token = generate_db_auth_token_with_datetime(
            "mydb.us-east-1.rds.amazonaws.com",
            3306,
            "mySQLUser",
            "us-east-1",
            &creds,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(token.contains("DBUser=mySQLUser"), "Token should contain DBUser: {token}");
    }

    #[test]
    fn test_token_uses_rds_db_signing_service() {
        let creds = test_creds();
        let token = generate_db_auth_token_with_datetime(
            "mydb.us-east-1.rds.amazonaws.com",
            3306,
            "admin",
            "us-east-1",
            &creds,
            "20250101T000000Z",
        )
        .unwrap();

        // Credential scope should include "rds-db" as the signing service
        assert!(
            token.contains("rds-db%2Faws4_request"),
            "Token credential should use rds-db signing service: {token}"
        );
    }

    #[test]
    fn test_token_expires_in_900() {
        let creds = test_creds();
        let token = generate_db_auth_token_with_datetime(
            "mydb.us-east-1.rds.amazonaws.com",
            3306,
            "admin",
            "us-east-1",
            &creds,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(token.contains("X-Amz-Expires=900"), "Token should expire in 900s: {token}");
    }

    #[test]
    fn test_token_contains_signature() {
        let creds = test_creds();
        let token = generate_db_auth_token_with_datetime(
            "mydb.us-east-1.rds.amazonaws.com",
            3306,
            "admin",
            "us-east-1",
            &creds,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(token.contains("X-Amz-Signature="), "Token should contain signature: {token}");
        // Extract signature and verify it's 64 hex chars
        let sig_start = token.find("X-Amz-Signature=").unwrap() + 16;
        let sig = &token[sig_start..];
        let sig = sig.split('&').next().unwrap();
        assert_eq!(sig.len(), 64, "Signature should be 64 hex chars: {sig}");
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()), "Signature should be hex: {sig}");
    }

    #[test]
    fn test_token_with_session_token() {
        let creds = test_creds_with_token();
        let token = generate_db_auth_token_with_datetime(
            "mydb.us-east-1.rds.amazonaws.com",
            3306,
            "admin",
            "us-east-1",
            &creds,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(
            token.contains("X-Amz-Security-Token=FwoGZXIvYXdzEBY"),
            "Token should contain security token: {token}"
        );
    }

    #[test]
    fn test_token_without_session_token() {
        let creds = test_creds();
        let token = generate_db_auth_token_with_datetime(
            "mydb.us-east-1.rds.amazonaws.com",
            3306,
            "admin",
            "us-east-1",
            &creds,
            "20250101T000000Z",
        )
        .unwrap();

        assert!(
            !token.contains("X-Amz-Security-Token"),
            "Token should not contain security token: {token}"
        );
    }

    #[test]
    fn test_token_deterministic() {
        let creds = test_creds();
        let t1 = generate_db_auth_token_with_datetime(
            "mydb.us-east-1.rds.amazonaws.com", 3306, "admin", "us-east-1",
            &creds, "20250101T000000Z",
        ).unwrap();
        let t2 = generate_db_auth_token_with_datetime(
            "mydb.us-east-1.rds.amazonaws.com", 3306, "admin", "us-east-1",
            &creds, "20250101T000000Z",
        ).unwrap();
        assert_eq!(t1, t2, "Same inputs should produce identical tokens");
    }

    // Argument parsing tests

    #[test]
    fn test_parse_args_all_present() {
        let args = vec![
            "--hostname".into(), "mydb.rds.amazonaws.com".into(),
            "--port".into(), "3306".into(),
            "--username".into(), "admin".into(),
        ];
        let (h, p, u) = parse_generate_db_auth_token_args(&args).unwrap();
        assert_eq!(h, "mydb.rds.amazonaws.com");
        assert_eq!(p, 3306);
        assert_eq!(u, "admin");
    }

    #[test]
    fn test_parse_args_different_order() {
        let args = vec![
            "--username".into(), "root".into(),
            "--port".into(), "5432".into(),
            "--hostname".into(), "pg.example.com".into(),
        ];
        let (h, p, u) = parse_generate_db_auth_token_args(&args).unwrap();
        assert_eq!(h, "pg.example.com");
        assert_eq!(p, 5432);
        assert_eq!(u, "root");
    }

    #[test]
    fn test_parse_args_missing_hostname() {
        let args = vec![
            "--port".into(), "3306".into(),
            "--username".into(), "admin".into(),
        ];
        let result = parse_generate_db_auth_token_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--hostname"));
    }

    #[test]
    fn test_parse_args_missing_port() {
        let args = vec![
            "--hostname".into(), "mydb.rds.amazonaws.com".into(),
            "--username".into(), "admin".into(),
        ];
        let result = parse_generate_db_auth_token_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--port"));
    }

    #[test]
    fn test_parse_args_missing_username() {
        let args = vec![
            "--hostname".into(), "mydb.rds.amazonaws.com".into(),
            "--port".into(), "3306".into(),
        ];
        let result = parse_generate_db_auth_token_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--username"));
    }

    #[test]
    fn test_parse_args_invalid_port() {
        let args = vec![
            "--hostname".into(), "mydb.rds.amazonaws.com".into(),
            "--port".into(), "abc".into(),
            "--username".into(), "admin".into(),
        ];
        let result = parse_generate_db_auth_token_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("port number"));
    }

    #[test]
    fn test_is_custom_command() {
        assert!(is_custom_command("generate-db-auth-token"));
        assert!(!is_custom_command("describe-db-instances"));
        assert!(!is_custom_command("create-db-instance"));
    }
}
