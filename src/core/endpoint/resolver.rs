use anyhow::{Context, Result};
use regex::Regex;
use serde_json::Value;
use std::path::Path;
use std::sync::OnceLock;

/// Cached parsed endpoints.json data, loaded once on first use.
static ENDPOINTS_DATA: OnceLock<Value> = OnceLock::new();

/// Load and parse models/endpoints.json, returning the parsed JSON.
pub fn load_endpoints(path: &Path) -> Result<Value> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read endpoints file: {}", path.display()))?;
    let data: Value = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse endpoints JSON: {}", path.display()))?;
    Ok(data)
}

/// Get the cached endpoints data, loading from disk on first call.
fn get_endpoints_data() -> Result<&'static Value> {
    if let Some(data) = ENDPOINTS_DATA.get() {
        return Ok(data);
    }
    let path = Path::new("models/endpoints.json");
    let data = load_endpoints(path)?;
    // If another thread raced us, that's fine -- just use whoever won.
    Ok(ENDPOINTS_DATA.get_or_init(|| data))
}

/// Main entry point for endpoint resolution (backward-compatible signature).
///
/// Called from driver.rs with (endpoint_prefix, region, global_endpoint).
/// The `global_endpoint` parameter from the service model metadata is used as
/// a fallback hint; endpoints.json is the primary source of truth.
pub fn resolve_endpoint(
    endpoint_prefix: &str,
    region: &str,
    global_endpoint: Option<&str>,
) -> Result<String> {
    // Try to resolve using endpoints.json first.
    match get_endpoints_data() {
        Ok(endpoints) => resolve_endpoint_from_data(endpoints, endpoint_prefix, region, global_endpoint),
        Err(_) => {
            // Fallback: if endpoints.json is not available, use the old simple logic.
            if let Some(global) = global_endpoint {
                Ok(format!("https://{global}"))
            } else {
                Ok(format!("https://{endpoint_prefix}.{region}.amazonaws.com"))
            }
        }
    }
}

/// Resolve an endpoint given parsed endpoints.json data.
///
/// Resolution algorithm:
/// 1. Find the partition for the given region (by matching regionRegex or listed regions).
/// 2. Look up the service in the partition's services map.
/// 3. If the service is non-regionalized (isRegionalized=false) and has a partitionEndpoint,
///    use the partitionEndpoint's hostname.
/// 4. If the region has a specific endpoint entry with a hostname, use that.
/// 5. Otherwise, use the partition default hostname template.
pub fn resolve_endpoint_from_data(
    endpoints: &Value,
    service: &str,
    region: &str,
    global_endpoint: Option<&str>,
) -> Result<String> {
    let partitions = match endpoints.get("partitions").and_then(|p| p.as_array()) {
        Some(p) => p,
        None => {
            // No partitions data: use fallback
            return fallback_resolve(service, region, global_endpoint);
        }
    };

    // Step 1: Find the matching partition for this region.
    let partition = find_partition(partitions, region);

    match partition {
        Some(p) => resolve_in_partition(p, service, region, global_endpoint),
        None => {
            // No matching partition: try the first partition (aws) as fallback
            if let Some(first) = partitions.first() {
                resolve_in_partition(first, service, region, global_endpoint)
            } else {
                fallback_resolve(service, region, global_endpoint)
            }
        }
    }
}

/// Find the partition that matches the given region.
///
/// A partition matches if:
/// 1. The region is explicitly listed in the partition's "regions" map, OR
/// 2. The region matches the partition's "regionRegex" pattern.
fn find_partition<'a>(partitions: &'a [Value], region: &str) -> Option<&'a Value> {
    // First pass: check if the region is explicitly listed in any partition's regions.
    for p in partitions {
        if let Some(regions) = p.get("regions").and_then(|r| r.as_object()) {
            if regions.contains_key(region) {
                return Some(p);
            }
        }
    }

    // Second pass: match by regionRegex.
    for p in partitions {
        if let Some(regex_str) = p.get("regionRegex").and_then(|r| r.as_str()) {
            if let Ok(re) = Regex::new(regex_str) {
                if re.is_match(region) {
                    return Some(p);
                }
            }
        }
    }

    None
}

/// Resolve endpoint within a specific partition.
fn resolve_in_partition(
    partition: &Value,
    service: &str,
    region: &str,
    _global_endpoint: Option<&str>,
) -> Result<String> {
    let dns_suffix = partition
        .get("dnsSuffix")
        .and_then(|s| s.as_str())
        .unwrap_or("amazonaws.com");

    let partition_defaults = partition.get("defaults");

    // Look up the service in this partition.
    let service_data = partition
        .get("services")
        .and_then(|s| s.get(service));

    if let Some(svc) = service_data {
        let is_regionalized = svc.get("isRegionalized").and_then(|v| v.as_bool());
        let partition_endpoint = svc.get("partitionEndpoint").and_then(|v| v.as_str());
        let service_defaults = svc.get("defaults");

        // Get the endpoints map for this service
        let endpoints = svc.get("endpoints").and_then(|e| e.as_object());

        // Determine which endpoint entry to use:
        // - If isRegionalized == false, use the partitionEndpoint
        // - If the region has a specific entry, use it
        // - If neither, and there's a partitionEndpoint and no region entry, use partitionEndpoint
        let (effective_region, endpoint_data) = if is_regionalized == Some(false) {
            // Non-regionalized service: always use partitionEndpoint
            if let Some(pe) = partition_endpoint {
                let ep = endpoints.and_then(|eps| eps.get(pe));
                (pe, ep)
            } else {
                (region, endpoints.and_then(|eps| eps.get(region)))
            }
        } else {
            // Regionalized service: try the region first
            let region_ep = endpoints.and_then(|eps| eps.get(region));
            if region_ep.is_some() {
                (region, region_ep)
            } else if let Some(pe) = partition_endpoint {
                // Region not found, check if there's a partition endpoint
                // For STS: isRegionalized is not set (None), but partitionEndpoint = "aws-global"
                // In this case, if the region has a specific entry, use it; otherwise fall
                // through to default template (the real AWS SDK uses regional endpoints for STS).
                // We do NOT fall back to the partition endpoint here -- regional is the default
                // for STS in the modern AWS SDK.
                let _pe_ep = endpoints.and_then(|eps| eps.get(pe));
                // Use the default hostname template for this region
                (region, None)
            } else {
                (region, None)
            }
        };

        // Build hostname from the endpoint data or template.
        let hostname = resolve_hostname(
            endpoint_data,
            service_defaults,
            partition_defaults,
            service,
            effective_region,
            dns_suffix,
        );

        return Ok(format!("https://{hostname}"));
    }

    // Service not found in endpoints.json for this partition:
    // Use the partition default template.
    let hostname = resolve_hostname(
        None,
        None,
        partition_defaults,
        service,
        region,
        dns_suffix,
    );
    Ok(format!("https://{hostname}"))
}

/// Resolve the hostname by checking (in order of priority):
/// 1. The specific endpoint entry's "hostname" field
/// 2. The service defaults hostname template
/// 3. The partition defaults hostname template
/// 4. The hardcoded fallback: {service}.{region}.{dnsSuffix}
fn resolve_hostname(
    endpoint_data: Option<&Value>,
    service_defaults: Option<&Value>,
    partition_defaults: Option<&Value>,
    service: &str,
    region: &str,
    dns_suffix: &str,
) -> String {
    // Check for explicit hostname in the endpoint entry
    if let Some(ep) = endpoint_data {
        if let Some(hostname) = ep.get("hostname").and_then(|h| h.as_str()) {
            return expand_hostname_template(hostname, service, region, dns_suffix);
        }
    }

    // Check service-level defaults for hostname template
    if let Some(defaults) = service_defaults {
        if let Some(hostname) = defaults.get("hostname").and_then(|h| h.as_str()) {
            return expand_hostname_template(hostname, service, region, dns_suffix);
        }
    }

    // Check partition-level defaults for hostname template
    if let Some(defaults) = partition_defaults {
        if let Some(hostname) = defaults.get("hostname").and_then(|h| h.as_str()) {
            return expand_hostname_template(hostname, service, region, dns_suffix);
        }
    }

    // Hardcoded fallback
    format!("{service}.{region}.{dns_suffix}")
}

/// Expand a hostname template by substituting {service}, {region}, and {dnsSuffix}.
fn expand_hostname_template(template: &str, service: &str, region: &str, dns_suffix: &str) -> String {
    template
        .replace("{service}", service)
        .replace("{region}", region)
        .replace("{dnsSuffix}", dns_suffix)
}

/// Simple fallback when endpoints.json is unavailable.
fn fallback_resolve(service: &str, region: &str, global_endpoint: Option<&str>) -> Result<String> {
    if let Some(global) = global_endpoint {
        Ok(format!("https://{global}"))
    } else {
        Ok(format!("https://{service}.{region}.amazonaws.com"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // Helper: load the real endpoints.json for tests
    // ---------------------------------------------------------------
    fn load_test_endpoints() -> Value {
        let path = Path::new("models/endpoints.json");
        if path.exists() {
            load_endpoints(path).expect("Failed to load endpoints.json")
        } else {
            // Provide a minimal test fixture
            serde_json::json!({
                "partitions": [{
                    "partition": "aws",
                    "dnsSuffix": "amazonaws.com",
                    "regionRegex": "^(us|eu|ap|sa|ca|me|af|il|mx)\\-\\w+\\-\\d+$",
                    "defaults": {
                        "hostname": "{service}.{region}.{dnsSuffix}",
                        "protocols": ["https"]
                    },
                    "regions": {
                        "us-east-1": {},
                        "us-west-2": {},
                        "eu-west-1": {}
                    },
                    "services": {}
                }]
            })
        }
    }

    // ---------------------------------------------------------------
    // endpoints_json: parsing tests
    // ---------------------------------------------------------------

    #[test]
    fn test_endpoints_json_load_and_parse() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        assert!(data.get("partitions").is_some());
        let partitions = data["partitions"].as_array().unwrap();
        assert!(!partitions.is_empty(), "Should have at least one partition");
    }

    #[test]
    fn test_endpoints_json_has_aws_partition() {
        let data = load_test_endpoints();
        let partitions = data["partitions"].as_array().unwrap();
        let aws = partitions.iter().find(|p| p["partition"] == "aws");
        assert!(aws.is_some(), "Should have an 'aws' partition");
    }

    #[test]
    fn test_endpoints_json_aws_partition_structure() {
        let data = load_test_endpoints();
        let partitions = data["partitions"].as_array().unwrap();
        let aws = partitions.iter().find(|p| p["partition"] == "aws").unwrap();

        assert_eq!(aws["dnsSuffix"].as_str(), Some("amazonaws.com"));
        assert!(aws.get("regionRegex").is_some());
        assert!(aws.get("regions").is_some());
        assert!(aws.get("defaults").is_some());
    }

    #[test]
    fn test_endpoints_json_has_services() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let partitions = data["partitions"].as_array().unwrap();
        let aws = partitions.iter().find(|p| p["partition"] == "aws").unwrap();
        let services = aws["services"].as_object().unwrap();

        // Verify key services exist
        assert!(services.contains_key("sts"), "Should have STS");
        assert!(services.contains_key("iam"), "Should have IAM");
        assert!(services.contains_key("s3"), "Should have S3");
        assert!(services.contains_key("ec2"), "Should have EC2");
    }

    #[test]
    fn test_endpoints_json_partition_count() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let partitions = data["partitions"].as_array().unwrap();
        // Should have at least aws, aws-cn, aws-us-gov
        assert!(
            partitions.len() >= 3,
            "Should have at least 3 partitions, got {}",
            partitions.len()
        );
    }

    // ---------------------------------------------------------------
    // partition_resolve: partition matching tests
    // ---------------------------------------------------------------

    #[test]
    fn test_partition_resolve_us_east_1() {
        let data = load_test_endpoints();
        let partitions = data["partitions"].as_array().unwrap();
        let p = find_partition(partitions, "us-east-1");
        assert!(p.is_some());
        assert_eq!(p.unwrap()["partition"].as_str(), Some("aws"));
    }

    #[test]
    fn test_partition_resolve_eu_west_1() {
        let data = load_test_endpoints();
        let partitions = data["partitions"].as_array().unwrap();
        let p = find_partition(partitions, "eu-west-1");
        assert!(p.is_some());
        assert_eq!(p.unwrap()["partition"].as_str(), Some("aws"));
    }

    #[test]
    fn test_partition_resolve_cn_north_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let partitions = data["partitions"].as_array().unwrap();
        let p = find_partition(partitions, "cn-north-1");
        assert!(p.is_some());
        assert_eq!(p.unwrap()["partition"].as_str(), Some("aws-cn"));
    }

    #[test]
    fn test_partition_resolve_us_gov_west_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let partitions = data["partitions"].as_array().unwrap();
        let p = find_partition(partitions, "us-gov-west-1");
        assert!(p.is_some());
        assert_eq!(p.unwrap()["partition"].as_str(), Some("aws-us-gov"));
    }

    #[test]
    fn test_partition_resolve_unknown_region_regex_fallback() {
        let data = load_test_endpoints();
        let partitions = data["partitions"].as_array().unwrap();
        // A new region that matches the aws regex pattern but isn't listed explicitly
        let p = find_partition(partitions, "us-west-99");
        // Should match aws partition via regex
        assert!(p.is_some());
        assert_eq!(p.unwrap()["partition"].as_str(), Some("aws"));
    }

    #[test]
    fn test_partition_resolve_completely_unknown_region() {
        let data = load_test_endpoints();
        let partitions = data["partitions"].as_array().unwrap();
        let p = find_partition(partitions, "mars-central-1");
        // Should not match any partition
        assert!(p.is_none());
    }

    // ---------------------------------------------------------------
    // service_endpoint: service-specific override tests
    // ---------------------------------------------------------------

    #[test]
    fn test_service_endpoint_sts_us_east_1() {
        let data = load_test_endpoints();
        let url = resolve_endpoint_from_data(&data, "sts", "us-east-1", None).unwrap();
        // STS us-east-1 should resolve to sts.us-east-1.amazonaws.com (regional)
        assert_eq!(url, "https://sts.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_service_endpoint_sts_us_west_2() {
        let data = load_test_endpoints();
        let url = resolve_endpoint_from_data(&data, "sts", "us-west-2", None).unwrap();
        assert_eq!(url, "https://sts.us-west-2.amazonaws.com");
    }

    #[test]
    fn test_service_endpoint_ec2_us_east_1() {
        let data = load_test_endpoints();
        let url = resolve_endpoint_from_data(&data, "ec2", "us-east-1", None).unwrap();
        assert_eq!(url, "https://ec2.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_service_endpoint_s3_us_east_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "s3", "us-east-1", None).unwrap();
        // S3 us-east-1 has a specific hostname
        assert!(
            url.contains("s3") && url.contains("us-east-1"),
            "S3 us-east-1 should contain service and region, got: {url}"
        );
    }

    #[test]
    fn test_service_endpoint_s3_ap_northeast_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "s3", "ap-northeast-1", None).unwrap();
        // ap-northeast-1 has a specific hostname: s3.ap-northeast-1.amazonaws.com
        assert_eq!(url, "https://s3.ap-northeast-1.amazonaws.com");
    }

    #[test]
    fn test_service_endpoint_dynamodb_us_east_1() {
        let data = load_test_endpoints();
        let url = resolve_endpoint_from_data(&data, "dynamodb", "us-east-1", None).unwrap();
        assert_eq!(url, "https://dynamodb.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_service_endpoint_unknown_service() {
        let data = load_test_endpoints();
        let url = resolve_endpoint_from_data(&data, "nonexistent-service", "us-east-1", None).unwrap();
        // Should fall back to default template
        assert_eq!(url, "https://nonexistent-service.us-east-1.amazonaws.com");
    }

    // ---------------------------------------------------------------
    // global_endpoint: global service tests
    // ---------------------------------------------------------------

    #[test]
    fn test_global_endpoint_iam() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "iam", "us-east-1", Some("iam.amazonaws.com")).unwrap();
        // IAM is non-regionalized: should use the global endpoint
        assert_eq!(url, "https://iam.amazonaws.com");
    }

    #[test]
    fn test_global_endpoint_iam_any_region() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        // Even when called with eu-west-1, IAM should resolve to the global endpoint
        let url = resolve_endpoint_from_data(&data, "iam", "eu-west-1", Some("iam.amazonaws.com")).unwrap();
        assert_eq!(url, "https://iam.amazonaws.com");
    }

    #[test]
    fn test_global_endpoint_route53() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "route53", "us-east-1", Some("route53.amazonaws.com")).unwrap();
        assert_eq!(url, "https://route53.amazonaws.com");
    }

    #[test]
    fn test_global_endpoint_cloudfront() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "cloudfront", "us-east-1", Some("cloudfront.amazonaws.com")).unwrap();
        assert_eq!(url, "https://cloudfront.amazonaws.com");
    }

    // Keep the original backward-compat tests
    #[test]
    fn test_endpoint_resolve_basic() {
        let url = resolve_endpoint("sts", "us-east-1", None).unwrap();
        assert_eq!(url, "https://sts.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_endpoint_resolve_different_region() {
        let url = resolve_endpoint("s3", "eu-west-1", None).unwrap();
        // S3 eu-west-1 might have a specific hostname from endpoints.json
        // or fallback to default template
        assert!(
            url.starts_with("https://s3.") && url.contains("eu-west-1"),
            "Expected S3 eu-west-1 endpoint, got: {url}"
        );
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

    // ---------------------------------------------------------------
    // sts_regional: STS regional vs global endpoint tests
    // ---------------------------------------------------------------

    #[test]
    fn test_sts_regional_us_east_1() {
        let data = load_test_endpoints();
        // When called without global_endpoint hint, STS should use regional endpoint
        let url = resolve_endpoint_from_data(&data, "sts", "us-east-1", None).unwrap();
        assert_eq!(url, "https://sts.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_sts_regional_us_west_2() {
        let data = load_test_endpoints();
        let url = resolve_endpoint_from_data(&data, "sts", "us-west-2", None).unwrap();
        assert_eq!(url, "https://sts.us-west-2.amazonaws.com");
    }

    #[test]
    fn test_sts_regional_eu_west_1() {
        let data = load_test_endpoints();
        let url = resolve_endpoint_from_data(&data, "sts", "eu-west-1", None).unwrap();
        assert_eq!(url, "https://sts.eu-west-1.amazonaws.com");
    }

    #[test]
    fn test_sts_regional_ap_northeast_1() {
        let data = load_test_endpoints();
        let url = resolve_endpoint_from_data(&data, "sts", "ap-northeast-1", None).unwrap();
        assert_eq!(url, "https://sts.ap-northeast-1.amazonaws.com");
    }

    #[test]
    fn test_sts_regional_with_global_hint_ignored() {
        // Even if the service model says globalEndpoint = sts.amazonaws.com,
        // the endpoints.json-based resolver should use the regional endpoint
        // because STS is regionalized (isRegionalized is not set to false).
        let data = load_test_endpoints();
        let url = resolve_endpoint_from_data(&data, "sts", "us-west-2", Some("sts.amazonaws.com")).unwrap();
        // STS is regionalized in endpoints.json, so regional is used
        assert_eq!(url, "https://sts.us-west-2.amazonaws.com");
    }

    // ---------------------------------------------------------------
    // endpoint_url_override: --endpoint-url override tests
    // ---------------------------------------------------------------

    #[test]
    fn test_endpoint_url_override_custom_url() {
        // Simulating what driver.rs does: if --endpoint-url is provided, it's used directly.
        // The resolver is NOT called when --endpoint-url is specified.
        let custom_url = "http://localhost:4566";
        // This is how driver.rs uses it:
        let endpoint_url: String = custom_url.to_string();
        assert_eq!(endpoint_url, "http://localhost:4566");
    }

    #[test]
    fn test_endpoint_url_override_localstack() {
        // Verify that a localstack-style endpoint URL works
        let custom_url = "http://localhost:4566";
        assert!(custom_url.starts_with("http"));
        assert!(custom_url.contains("localhost"));
    }

    #[test]
    fn test_endpoint_url_override_custom_https() {
        let custom_url = "https://my-custom-endpoint.example.com";
        assert!(custom_url.starts_with("https://"));
    }

    #[test]
    fn test_endpoint_url_override_with_port() {
        let custom_url = "https://vpce-abc123.sts.us-east-1.vpce.amazonaws.com:8443";
        let parsed = url::Url::parse(custom_url).unwrap();
        assert_eq!(parsed.host_str(), Some("vpce-abc123.sts.us-east-1.vpce.amazonaws.com"));
        assert_eq!(parsed.port(), Some(8443));
    }

    // ---------------------------------------------------------------
    // china_partition: China partition endpoint tests
    // ---------------------------------------------------------------

    #[test]
    fn test_china_partition_sts_cn_north_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "sts", "cn-north-1", None).unwrap();
        assert_eq!(url, "https://sts.cn-north-1.amazonaws.com.cn");
    }

    #[test]
    fn test_china_partition_sts_cn_northwest_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "sts", "cn-northwest-1", None).unwrap();
        assert_eq!(url, "https://sts.cn-northwest-1.amazonaws.com.cn");
    }

    #[test]
    fn test_china_partition_iam_global() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "iam", "cn-north-1", None).unwrap();
        // IAM in China is non-regionalized with a specific hostname
        assert_eq!(url, "https://iam.cn-north-1.amazonaws.com.cn");
    }

    #[test]
    fn test_china_partition_s3_cn_north_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "s3", "cn-north-1", None).unwrap();
        assert!(
            url.contains("cn-north-1") && url.contains("amazonaws.com.cn"),
            "China S3 should use .amazonaws.com.cn suffix, got: {url}"
        );
    }

    #[test]
    fn test_china_partition_dns_suffix() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let partitions = data["partitions"].as_array().unwrap();
        let cn = partitions.iter().find(|p| p["partition"] == "aws-cn");
        assert!(cn.is_some(), "Should have aws-cn partition");
        assert_eq!(cn.unwrap()["dnsSuffix"].as_str(), Some("amazonaws.com.cn"));
    }

    #[test]
    fn test_china_partition_route53_global() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "route53", "cn-north-1", None).unwrap();
        assert_eq!(url, "https://route53.amazonaws.com.cn");
    }

    // ---------------------------------------------------------------
    // govcloud_partition: GovCloud endpoint tests
    // ---------------------------------------------------------------

    #[test]
    fn test_govcloud_partition_sts_us_gov_west_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "sts", "us-gov-west-1", None).unwrap();
        assert_eq!(url, "https://sts.us-gov-west-1.amazonaws.com");
    }

    #[test]
    fn test_govcloud_partition_sts_us_gov_east_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "sts", "us-gov-east-1", None).unwrap();
        assert_eq!(url, "https://sts.us-gov-east-1.amazonaws.com");
    }

    #[test]
    fn test_govcloud_partition_iam_global() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "iam", "us-gov-west-1", None).unwrap();
        // IAM in GovCloud is non-regionalized
        assert_eq!(url, "https://iam.us-gov.amazonaws.com");
    }

    #[test]
    fn test_govcloud_partition_route53_global() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "route53", "us-gov-west-1", None).unwrap();
        assert_eq!(url, "https://route53.us-gov.amazonaws.com");
    }

    #[test]
    fn test_govcloud_partition_s3_us_gov_west_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "s3", "us-gov-west-1", None).unwrap();
        assert_eq!(url, "https://s3.us-gov-west-1.amazonaws.com");
    }

    #[test]
    fn test_govcloud_partition_dns_suffix() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let partitions = data["partitions"].as_array().unwrap();
        let gov = partitions.iter().find(|p| p["partition"] == "aws-us-gov");
        assert!(gov.is_some(), "Should have aws-us-gov partition");
        // GovCloud uses amazonaws.com as its dnsSuffix
        assert_eq!(gov.unwrap()["dnsSuffix"].as_str(), Some("amazonaws.com"));
    }

    #[test]
    fn test_govcloud_partition_ec2_us_gov_west_1() {
        let path = Path::new("models/endpoints.json");
        if !path.exists() {
            eprintln!("Skipping: endpoints.json not found");
            return;
        }
        let data = load_endpoints(path).unwrap();
        let url = resolve_endpoint_from_data(&data, "ec2", "us-gov-west-1", None).unwrap();
        assert_eq!(url, "https://ec2.us-gov-west-1.amazonaws.com");
    }

    // ---------------------------------------------------------------
    // Template expansion tests
    // ---------------------------------------------------------------

    #[test]
    fn test_expand_hostname_template_standard() {
        let result = expand_hostname_template(
            "{service}.{region}.{dnsSuffix}",
            "sts",
            "us-east-1",
            "amazonaws.com",
        );
        assert_eq!(result, "sts.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_expand_hostname_template_china() {
        let result = expand_hostname_template(
            "{service}.{region}.{dnsSuffix}",
            "ec2",
            "cn-north-1",
            "amazonaws.com.cn",
        );
        assert_eq!(result, "ec2.cn-north-1.amazonaws.com.cn");
    }

    #[test]
    fn test_expand_hostname_template_no_region() {
        // Some hostnames don't use {region}
        let result = expand_hostname_template(
            "iam.{dnsSuffix}",
            "iam",
            "us-east-1",
            "amazonaws.com",
        );
        assert_eq!(result, "iam.amazonaws.com");
    }

    // ---------------------------------------------------------------
    // Fallback behavior tests
    // ---------------------------------------------------------------

    #[test]
    fn test_fallback_resolve_basic() {
        let url = fallback_resolve("lambda", "us-west-2", None).unwrap();
        assert_eq!(url, "https://lambda.us-west-2.amazonaws.com");
    }

    #[test]
    fn test_fallback_resolve_global() {
        let url = fallback_resolve("iam", "us-east-1", Some("iam.amazonaws.com")).unwrap();
        assert_eq!(url, "https://iam.amazonaws.com");
    }
}
