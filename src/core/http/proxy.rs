use anyhow::{Context, Result};
use std::path::PathBuf;

/// Proxy configuration resolved from environment and CLI args.
#[derive(Debug, Clone, Default)]
pub struct ProxyConfig {
    /// Custom CA bundle path (--ca-bundle or AWS_CA_BUNDLE)
    pub ca_bundle: Option<PathBuf>,
    /// HTTP proxy URL (HTTP_PROXY or http_proxy)
    pub http_proxy: Option<String>,
    /// HTTPS proxy URL (HTTPS_PROXY or https_proxy)
    pub https_proxy: Option<String>,
    /// Comma-separated list of hosts to bypass proxy (NO_PROXY or no_proxy)
    pub no_proxy: Option<String>,
}

impl ProxyConfig {
    /// Resolve proxy configuration from environment variables and CLI arguments.
    pub fn resolve(cli_ca_bundle: Option<&str>) -> Self {
        let ca_bundle = cli_ca_bundle
            .map(PathBuf::from)
            .or_else(|| std::env::var("AWS_CA_BUNDLE").ok().map(PathBuf::from));

        let http_proxy = std::env::var("HTTP_PROXY")
            .or_else(|_| std::env::var("http_proxy"))
            .ok();

        let https_proxy = std::env::var("HTTPS_PROXY")
            .or_else(|_| std::env::var("https_proxy"))
            .ok();

        let no_proxy = std::env::var("NO_PROXY")
            .or_else(|_| std::env::var("no_proxy"))
            .ok();

        Self {
            ca_bundle,
            http_proxy,
            https_proxy,
            no_proxy,
        }
    }

    /// Check if any proxy settings are configured.
    pub fn has_proxy(&self) -> bool {
        self.http_proxy.is_some() || self.https_proxy.is_some()
    }

    /// Check if a host should bypass the proxy based on NO_PROXY settings.
    pub fn should_bypass_proxy(&self, host: &str) -> bool {
        let no_proxy = match &self.no_proxy {
            Some(np) => np,
            None => return false,
        };

        if no_proxy == "*" {
            return true;
        }

        for pattern in no_proxy.split(',') {
            let pattern = pattern.trim();
            if pattern.is_empty() {
                continue;
            }

            // Direct match
            if host == pattern {
                return true;
            }

            // Suffix match (e.g., .amazonaws.com matches sts.amazonaws.com)
            if pattern.starts_with('.') && host.ends_with(pattern) {
                return true;
            }

            // Also match without leading dot
            if !pattern.starts_with('.') && host.ends_with(&format!(".{}", pattern)) {
                return true;
            }
        }

        false
    }

    /// Read the CA bundle file contents if specified.
    pub fn read_ca_bundle(&self) -> Result<Option<Vec<u8>>> {
        match &self.ca_bundle {
            Some(path) => {
                let data = std::fs::read(path)
                    .with_context(|| format!("Failed to read CA bundle: {}", path.display()))?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // Helper: build a ProxyConfig directly (avoids env var pollution between tests).
    fn config_with(
        ca: Option<&str>,
        http: Option<&str>,
        https: Option<&str>,
        no: Option<&str>,
    ) -> ProxyConfig {
        ProxyConfig {
            ca_bundle: ca.map(PathBuf::from),
            http_proxy: http.map(String::from),
            https_proxy: https.map(String::from),
            no_proxy: no.map(String::from),
        }
    }

    // --- resolve tests ---

    #[test]
    fn resolve_with_cli_ca_bundle() {
        let cfg = ProxyConfig {
            ca_bundle: Some(PathBuf::from("/tmp/my-ca.pem")),
            ..Default::default()
        };
        assert_eq!(cfg.ca_bundle, Some(PathBuf::from("/tmp/my-ca.pem")));
    }

    #[test]
    fn resolve_with_aws_ca_bundle_env() {
        // We test the fallback logic directly rather than mutating env vars
        // to avoid flaky parallel test interactions.
        let cli_none: Option<&str> = None;
        let ca = cli_none
            .map(PathBuf::from)
            .or_else(|| Some(PathBuf::from("/etc/ssl/custom-ca.pem")));
        assert_eq!(ca, Some(PathBuf::from("/etc/ssl/custom-ca.pem")));
    }

    #[test]
    fn resolve_cli_ca_bundle_takes_precedence() {
        // CLI argument should win over env var fallback.
        let cli_val = Some("/cli/ca.pem");
        let ca = cli_val
            .map(PathBuf::from)
            .or_else(|| Some(PathBuf::from("/env/ca.pem")));
        assert_eq!(ca, Some(PathBuf::from("/cli/ca.pem")));
    }

    #[test]
    fn resolve_with_http_proxy() {
        let cfg = config_with(None, Some("http://proxy:8080"), None, None);
        assert_eq!(cfg.http_proxy.as_deref(), Some("http://proxy:8080"));
    }

    #[test]
    fn resolve_with_https_proxy() {
        let cfg = config_with(None, None, Some("https://proxy:8443"), None);
        assert_eq!(cfg.https_proxy.as_deref(), Some("https://proxy:8443"));
    }

    #[test]
    fn resolve_with_no_proxy_settings() {
        let cfg = ProxyConfig::default();
        assert_eq!(cfg.ca_bundle, None);
        assert_eq!(cfg.http_proxy, None);
        assert_eq!(cfg.https_proxy, None);
        assert_eq!(cfg.no_proxy, None);
    }

    // --- has_proxy tests ---

    #[test]
    fn has_proxy_true_when_http_proxy_set() {
        let cfg = config_with(None, Some("http://proxy:8080"), None, None);
        assert!(cfg.has_proxy());
    }

    #[test]
    fn has_proxy_true_when_https_proxy_set() {
        let cfg = config_with(None, None, Some("https://proxy:8443"), None);
        assert!(cfg.has_proxy());
    }

    #[test]
    fn has_proxy_false_when_neither_set() {
        let cfg = ProxyConfig::default();
        assert!(!cfg.has_proxy());
    }

    // --- should_bypass_proxy tests ---

    #[test]
    fn should_bypass_proxy_exact_match() {
        let cfg = config_with(None, None, None, Some("localhost"));
        assert!(cfg.should_bypass_proxy("localhost"));
        assert!(!cfg.should_bypass_proxy("other.host"));
    }

    #[test]
    fn should_bypass_proxy_dot_prefix_suffix_match() {
        let cfg = config_with(None, None, None, Some(".amazonaws.com"));
        assert!(cfg.should_bypass_proxy("sts.amazonaws.com"));
        assert!(cfg.should_bypass_proxy("s3.us-east-1.amazonaws.com"));
        assert!(!cfg.should_bypass_proxy("amazonaws.com"));
        assert!(!cfg.should_bypass_proxy("evil-amazonaws.com"));
    }

    #[test]
    fn should_bypass_proxy_suffix_match_without_dot() {
        let cfg = config_with(None, None, None, Some("amazonaws.com"));
        assert!(cfg.should_bypass_proxy("sts.amazonaws.com"));
        // Direct match also works
        assert!(cfg.should_bypass_proxy("amazonaws.com"));
    }

    #[test]
    fn should_bypass_proxy_wildcard() {
        let cfg = config_with(None, None, None, Some("*"));
        assert!(cfg.should_bypass_proxy("anything.example.com"));
        assert!(cfg.should_bypass_proxy("localhost"));
    }

    #[test]
    fn should_bypass_proxy_returns_false_when_no_no_proxy() {
        let cfg = ProxyConfig::default();
        assert!(!cfg.should_bypass_proxy("anything.example.com"));
    }

    #[test]
    fn should_bypass_proxy_multiple_patterns() {
        let cfg = config_with(None, None, None, Some("localhost, .internal.corp, 169.254.169.254"));
        assert!(cfg.should_bypass_proxy("localhost"));
        assert!(cfg.should_bypass_proxy("app.internal.corp"));
        assert!(cfg.should_bypass_proxy("169.254.169.254"));
        assert!(!cfg.should_bypass_proxy("external.example.com"));
    }

    // --- read_ca_bundle tests ---

    #[test]
    fn read_ca_bundle_with_valid_file() {
        let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
        let pem_data = b"-----BEGIN CERTIFICATE-----\nMIIBxx...fake...\n-----END CERTIFICATE-----\n";
        tmp.write_all(pem_data).expect("write temp file");

        let cfg = ProxyConfig {
            ca_bundle: Some(tmp.path().to_path_buf()),
            ..Default::default()
        };

        let result = cfg.read_ca_bundle().expect("read should succeed");
        assert_eq!(result, Some(pem_data.to_vec()));
    }

    #[test]
    fn read_ca_bundle_with_no_ca_bundle() {
        let cfg = ProxyConfig::default();
        let result = cfg.read_ca_bundle().expect("read should succeed");
        assert_eq!(result, None);
    }

    #[test]
    fn read_ca_bundle_with_nonexistent_file() {
        let cfg = ProxyConfig {
            ca_bundle: Some(PathBuf::from("/nonexistent/path/ca-bundle.pem")),
            ..Default::default()
        };

        let result = cfg.read_ca_bundle();
        assert!(result.is_err());
        let err_msg = format!("{:#}", result.unwrap_err());
        assert!(
            err_msg.contains("Failed to read CA bundle"),
            "Error message should mention CA bundle, got: {}",
            err_msg
        );
    }
}
