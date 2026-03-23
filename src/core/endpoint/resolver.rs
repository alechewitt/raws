use anyhow::Result;

pub fn resolve_endpoint(
    endpoint_prefix: &str,
    region: &str,
    global_endpoint: Option<&str>,
) -> Result<String> {
    if let Some(global) = global_endpoint {
        return Ok(format!("https://{global}"));
    }
    Ok(format!("https://{endpoint_prefix}.{region}.amazonaws.com"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_resolve_basic() {
        let url = resolve_endpoint("sts", "us-east-1", None).unwrap();
        assert_eq!(url, "https://sts.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_endpoint_resolve_different_region() {
        let url = resolve_endpoint("s3", "eu-west-1", None).unwrap();
        assert_eq!(url, "https://s3.eu-west-1.amazonaws.com");
    }

    #[test]
    fn test_endpoint_resolve_global() {
        let url = resolve_endpoint("iam", "us-east-1", Some("iam.amazonaws.com")).unwrap();
        assert_eq!(url, "https://iam.amazonaws.com");
    }

    #[test]
    fn test_endpoint_resolve_route53_global() {
        let url = resolve_endpoint("route53", "us-east-1", Some("route53.amazonaws.com")).unwrap();
        assert_eq!(url, "https://route53.amazonaws.com");
    }
}
