use anyhow::Result;

pub fn resolve_endpoint(endpoint_prefix: &str, region: &str) -> Result<String> {
    Ok(format!("https://{endpoint_prefix}.{region}.amazonaws.com"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_resolve_basic() {
        let url = resolve_endpoint("sts", "us-east-1").unwrap();
        assert_eq!(url, "https://sts.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_endpoint_resolve_different_region() {
        let url = resolve_endpoint("s3", "eu-west-1").unwrap();
        assert_eq!(url, "https://s3.eu-west-1.amazonaws.com");
    }
}
