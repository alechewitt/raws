use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;

use super::request::{HttpRequest, HttpResponse};

/// Configuration for HTTP client timeouts.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Connection timeout (time to establish TCP connection). Default: 60s.
    pub connect_timeout: Duration,
    /// Read/request timeout (total time for the request). Default: 60s.
    pub read_timeout: Duration,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(60),
            read_timeout: Duration::from_secs(60),
        }
    }
}

pub struct HttpClient {
    client: reqwest::Client,
}

impl HttpClient {
    pub fn new() -> Result<Self> {
        Self::with_config(&HttpClientConfig::default())
    }

    /// Create an HTTP client with custom timeout configuration.
    pub fn with_config(config: &HttpClientConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .connect_timeout(config.connect_timeout)
            .timeout(config.read_timeout)
            .build()?;
        Ok(Self { client })
    }

    pub async fn send(&self, request: &HttpRequest) -> Result<HttpResponse> {
        let mut builder = match request.method.as_str() {
            "GET" => self.client.get(&request.url),
            "POST" => self.client.post(&request.url),
            "PUT" => self.client.put(&request.url),
            "DELETE" => self.client.delete(&request.url),
            "HEAD" => self.client.head(&request.url),
            "PATCH" => self.client.patch(&request.url),
            _ => anyhow::bail!("Unsupported HTTP method: {}", request.method),
        };

        for (key, value) in &request.headers {
            builder = builder.header(key, value);
        }

        if !request.body.is_empty() {
            builder = builder.body(request.body.clone());
        }

        let response = builder.send().await?;
        let status = response.status().as_u16();

        let mut headers = HashMap::new();
        for (key, value) in response.headers() {
            if let Ok(v) = value.to_str() {
                headers.insert(key.as_str().to_string(), v.to_string());
            }
        }

        let body = response.bytes().await?.to_vec();

        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeout_default_config() {
        let config = HttpClientConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(60));
        assert_eq!(config.read_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_timeout_custom_config() {
        let config = HttpClientConfig {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
        };
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.read_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_timeout_client_creation_with_config() {
        let config = HttpClientConfig {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(15),
        };
        let client = HttpClient::with_config(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_timeout_client_creation_default() {
        let client = HttpClient::new();
        assert!(client.is_ok());
    }

    #[test]
    fn test_timeout_zero_values() {
        // Zero timeout should still create a valid client (reqwest handles this)
        let config = HttpClientConfig {
            connect_timeout: Duration::ZERO,
            read_timeout: Duration::ZERO,
        };
        let client = HttpClient::with_config(&config);
        assert!(client.is_ok());
    }
}
