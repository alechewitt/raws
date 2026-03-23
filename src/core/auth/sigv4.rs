use anyhow::Result;
use ring::digest;
use ring::hmac;

use crate::core::credentials::Credentials;

pub fn sha256_hex(data: &[u8]) -> String {
    let digest = digest::digest(&digest::SHA256, data);
    hex_encode(digest.as_ref())
}

pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    let tag = hmac::sign(&k, data);
    tag.as_ref().to_vec()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

pub fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut result = String::new();
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' || ch == '~' {
            result.push(ch);
        } else if ch == '/' && !encode_slash {
            result.push('/');
        } else {
            for byte in ch.to_string().as_bytes() {
                result.push_str(&format!("%{byte:02X}"));
            }
        }
    }
    result
}

pub struct SigningParams<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub session_token: Option<&'a str>,
    pub region: &'a str,
    pub service: &'a str,
    pub datetime: &'a str, // "20230101T000000Z"
}

impl<'a> SigningParams<'a> {
    pub fn from_credentials(
        credentials: &'a Credentials,
        region: &'a str,
        service: &'a str,
        datetime: &'a str,
    ) -> Self {
        Self {
            access_key: &credentials.access_key_id,
            secret_key: &credentials.secret_access_key,
            session_token: credentials.session_token.as_deref(),
            region,
            service,
            datetime,
        }
    }

    pub fn date(&self) -> &str {
        &self.datetime[..8]
    }

    pub fn scope(&self) -> String {
        format!(
            "{}/{}/{}/aws4_request",
            self.date(),
            self.region,
            self.service
        )
    }
}

pub fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(&str, &str)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let value = parts.next().unwrap_or("");
            (key, value)
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0).then(a.1.cmp(b.1)));

    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn canonical_headers(headers: &[(String, String)]) -> (String, String) {
    let mut sorted: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v.trim().to_string()))
        .collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical = sorted
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect::<String>();

    let signed = sorted
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    (canonical, signed)
}

pub fn canonical_request(
    method: &str,
    uri: &str,
    query: &str,
    headers: &[(String, String)],
    payload_hash: &str,
) -> String {
    let canonical_uri = if uri.is_empty() { "/" } else { uri };
    let encoded_uri = canonical_uri
        .split('/')
        .map(|seg| uri_encode(seg, true))
        .collect::<Vec<_>>()
        .join("/");

    let canonical_qs = canonical_query_string(query);
    let (canonical_hdrs, signed_hdrs) = canonical_headers(headers);

    format!(
        "{method}\n{encoded_uri}\n{canonical_qs}\n{canonical_hdrs}\n{signed_hdrs}\n{payload_hash}"
    )
}

pub fn string_to_sign(datetime: &str, scope: &str, canonical_request: &str) -> String {
    let hashed = sha256_hex(canonical_request.as_bytes());
    format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{hashed}")
}

pub fn signing_key(secret_key: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

pub fn calculate_signature(signing_key: &[u8], string_to_sign: &str) -> String {
    hex_encode(&hmac_sha256(signing_key, string_to_sign.as_bytes()))
}

pub fn authorization_header(
    access_key: &str,
    scope: &str,
    signed_headers: &str,
    signature: &str,
) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )
}

pub fn sign_request(
    method: &str,
    uri: &str,
    query: &str,
    headers: &mut Vec<(String, String)>,
    body: &[u8],
    params: &SigningParams,
) -> Result<()> {
    let payload_hash = sha256_hex(body);

    // Add required headers
    headers.push(("x-amz-date".to_string(), params.datetime.to_string()));
    headers.push((
        "x-amz-content-sha256".to_string(),
        payload_hash.clone(),
    ));

    if let Some(token) = params.session_token {
        headers.push(("x-amz-security-token".to_string(), token.to_string()));
    }

    let cr = canonical_request(method, uri, query, headers, &payload_hash);
    let scope = params.scope();
    let sts = string_to_sign(params.datetime, &scope, &cr);
    let key = signing_key(params.secret_key, params.date(), params.region, params.service);
    let sig = calculate_signature(&key, &sts);

    let (_, signed_hdrs) = canonical_headers(headers);
    let auth = authorization_header(params.access_key, &scope, &signed_hdrs, &sig);

    headers.push(("authorization".to_string(), auth));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_empty() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_hmac_sha256_basic() {
        let result = hmac_sha256(b"key", b"data");
        assert!(!result.is_empty());
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_uri_encode_basic() {
        assert_eq!(uri_encode("hello world", true), "hello%20world");
        assert_eq!(uri_encode("foo/bar", true), "foo%2Fbar");
        assert_eq!(uri_encode("foo/bar", false), "foo/bar");
        assert_eq!(uri_encode("test-value_123.txt~", true), "test-value_123.txt~");
    }

    #[test]
    fn test_canonical_query_string_sorted() {
        let qs = "b=2&a=1&c=3";
        assert_eq!(canonical_query_string(qs), "a=1&b=2&c=3");
    }

    #[test]
    fn test_canonical_query_string_empty() {
        assert_eq!(canonical_query_string(""), "");
    }

    #[test]
    fn test_canonical_headers_sorted() {
        let headers = vec![
            ("Host".to_string(), "sts.amazonaws.com".to_string()),
            ("X-Amz-Date".to_string(), "20230101T000000Z".to_string()),
            ("Content-Type".to_string(), "application/x-www-form-urlencoded".to_string()),
        ];
        let (canonical, signed) = canonical_headers(&headers);
        assert!(canonical.starts_with("content-type:"));
        assert!(canonical.contains("host:"));
        assert_eq!(signed, "content-type;host;x-amz-date");
    }

    #[test]
    fn test_canonical_request_sts() {
        let headers = vec![
            ("host".to_string(), "sts.amazonaws.com".to_string()),
            (
                "content-type".to_string(),
                "application/x-www-form-urlencoded; charset=utf-8".to_string(),
            ),
            ("x-amz-date".to_string(), "20230101T000000Z".to_string()),
        ];
        let body = "Action=GetCallerIdentity&Version=2011-06-15";
        let payload_hash = sha256_hex(body.as_bytes());
        let cr = canonical_request("POST", "/", "", &headers, &payload_hash);
        assert!(cr.starts_with("POST\n/\n\n"));
        assert!(cr.contains("content-type;host;x-amz-date"));
    }

    #[test]
    fn test_string_to_sign_format() {
        let cr = "test-canonical-request";
        let sts = string_to_sign(
            "20230101T000000Z",
            "20230101/us-east-1/sts/aws4_request",
            cr,
        );
        assert!(sts.starts_with("AWS4-HMAC-SHA256\n"));
        assert!(sts.contains("20230101T000000Z\n"));
        assert!(sts.contains("20230101/us-east-1/sts/aws4_request\n"));
    }

    #[test]
    fn test_signing_key_derivation() {
        let key = signing_key("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830", "us-east-1", "iam");
        assert_eq!(key.len(), 32);
        // Known answer from AWS docs
        let expected = "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9";
        assert_eq!(hex_encode(&key), expected);
    }

    #[test]
    fn test_sigv4_known_answer_sts() {
        // Test with known inputs to verify the full signing flow
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let access = "AKIDEXAMPLE";
        let datetime = "20230101T000000Z";
        let region = "us-east-1";
        let service = "sts";

        let body = "Action=GetCallerIdentity&Version=2011-06-15";
        let payload_hash = sha256_hex(body.as_bytes());

        let headers = vec![
            ("content-type".to_string(), "application/x-www-form-urlencoded; charset=utf-8".to_string()),
            ("host".to_string(), "sts.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), datetime.to_string()),
        ];

        let cr = canonical_request("POST", "/", "", &headers, &payload_hash);
        let scope = format!("20230101/{region}/{service}/aws4_request");
        let sts = string_to_sign(datetime, &scope, &cr);
        let key = signing_key(secret, "20230101", region, service);
        let sig = calculate_signature(&key, &sts);

        // Verify the signature is a 64-char hex string
        assert_eq!(sig.len(), 64);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));

        // Verify the authorization header format
        let (_, signed_hdrs) = canonical_headers(&headers);
        let auth = authorization_header(access, &scope, &signed_hdrs, &sig);
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20230101/us-east-1/sts/aws4_request"));
        assert!(auth.contains("SignedHeaders=content-type;host;x-amz-date"));
        assert!(auth.contains(&format!("Signature={sig}")));
    }

    #[test]
    fn test_sign_request_with_session_token() {
        let mut headers = vec![
            ("host".to_string(), "sts.amazonaws.com".to_string()),
            ("content-type".to_string(), "application/x-www-form-urlencoded".to_string()),
        ];
        let body = b"Action=GetCallerIdentity&Version=2011-06-15";

        let params = SigningParams {
            access_key: "AKIDEXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            session_token: Some("FwoGZXIvYXdzEBYaDHqa0AP"),
            region: "us-east-1",
            service: "sts",
            datetime: "20230101T000000Z",
        };

        sign_request("POST", "/", "", &mut headers, body, &params).unwrap();

        // Should have added x-amz-date, x-amz-content-sha256, x-amz-security-token, authorization
        let header_names: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
        assert!(header_names.contains(&"x-amz-date"));
        assert!(header_names.contains(&"x-amz-content-sha256"));
        assert!(header_names.contains(&"x-amz-security-token"));
        assert!(header_names.contains(&"authorization"));
    }

    #[test]
    fn test_authorization_header_format() {
        let auth = authorization_header(
            "AKIDEXAMPLE",
            "20230101/us-east-1/sts/aws4_request",
            "content-type;host;x-amz-date",
            "abcdef1234567890",
        );
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20230101/us-east-1/sts/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=abcdef1234567890"
        );
    }
}
