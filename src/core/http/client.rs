use anyhow::Result;
use std::collections::HashMap;

use super::request::{HttpRequest, HttpResponse};

pub struct HttpClient {
    client: reqwest::Client,
}

impl HttpClient {
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .use_rustls_tls()
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
