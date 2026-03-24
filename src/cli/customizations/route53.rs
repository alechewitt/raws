//! Route53 input parameter customizations.
//!
//! The AWS CLI strips certain path prefixes from Route53 hosted-zone, delegation-set,
//! and change IDs so that users can pass either the bare ID or the full ARN-style
//! path (e.g. `/hostedzone/Z123ABC`). We replicate that behaviour here.

/// Strip Route53-specific path prefixes from input parameters before serialization.
///
/// When the service is "route53", this function:
///   - Strips `/hostedzone/` from `HostedZoneId` and `Id`
///   - Strips `/delegationset/` from `DelegationSetId`
///   - Strips `/change/` from `Id`
///
/// For any other service the function is a no-op.
pub fn apply_route53_customizations(service: &str, params: &mut serde_json::Value) {
    if service != "route53" {
        return;
    }

    // Strip /hostedzone/ prefix from HostedZoneId and Id
    for key in &["HostedZoneId", "Id"] {
        if let Some(val) = params.get_mut(*key) {
            if let Some(s) = val.as_str() {
                if let Some(stripped) = s.strip_prefix("/hostedzone/") {
                    *val = serde_json::Value::String(stripped.to_string());
                }
            }
        }
    }

    // Strip /delegationset/ prefix from DelegationSetId
    if let Some(val) = params.get_mut("DelegationSetId") {
        if let Some(s) = val.as_str() {
            if let Some(stripped) = s.strip_prefix("/delegationset/") {
                *val = serde_json::Value::String(stripped.to_string());
            }
        }
    }

    // Strip /change/ prefix from Id (for change operations like GetChange)
    if let Some(val) = params.get_mut("Id") {
        if let Some(s) = val.as_str() {
            if let Some(stripped) = s.strip_prefix("/change/") {
                *val = serde_json::Value::String(stripped.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strip_hostedzone_prefix_from_hosted_zone_id() {
        let mut params = json!({"HostedZoneId": "/hostedzone/Z123ABC"});
        apply_route53_customizations("route53", &mut params);
        assert_eq!(params["HostedZoneId"].as_str(), Some("Z123ABC"));
    }

    #[test]
    fn strip_hostedzone_prefix_from_id() {
        let mut params = json!({"Id": "/hostedzone/Z123ABC"});
        apply_route53_customizations("route53", &mut params);
        assert_eq!(params["Id"].as_str(), Some("Z123ABC"));
    }

    #[test]
    fn strip_delegationset_prefix_from_delegation_set_id() {
        let mut params = json!({"DelegationSetId": "/delegationset/N12345"});
        apply_route53_customizations("route53", &mut params);
        assert_eq!(params["DelegationSetId"].as_str(), Some("N12345"));
    }

    #[test]
    fn strip_change_prefix_from_id() {
        let mut params = json!({"Id": "/change/C1234"});
        apply_route53_customizations("route53", &mut params);
        assert_eq!(params["Id"].as_str(), Some("C1234"));
    }

    #[test]
    fn no_strip_when_prefix_absent() {
        let mut params = json!({"HostedZoneId": "Z123ABC", "Id": "C1234", "DelegationSetId": "N12345"});
        apply_route53_customizations("route53", &mut params);
        assert_eq!(params["HostedZoneId"].as_str(), Some("Z123ABC"));
        assert_eq!(params["Id"].as_str(), Some("C1234"));
        assert_eq!(params["DelegationSetId"].as_str(), Some("N12345"));
    }

    #[test]
    fn no_modification_for_non_route53_service() {
        let mut params = json!({"HostedZoneId": "/hostedzone/Z123ABC", "Id": "/change/C1234"});
        apply_route53_customizations("iam", &mut params);
        assert_eq!(params["HostedZoneId"].as_str(), Some("/hostedzone/Z123ABC"));
        assert_eq!(params["Id"].as_str(), Some("/change/C1234"));
    }

    #[test]
    fn hostedzone_prefix_stripped_before_change_prefix_for_id() {
        // If Id has /hostedzone/ prefix, it gets stripped first.
        // The /change/ strip then sees "Z123ABC" which doesn't have /change/ prefix.
        let mut params = json!({"Id": "/hostedzone/Z999"});
        apply_route53_customizations("route53", &mut params);
        assert_eq!(params["Id"].as_str(), Some("Z999"));
    }

    #[test]
    fn empty_params_is_noop() {
        let mut params = json!({});
        apply_route53_customizations("route53", &mut params);
        assert_eq!(params, json!({}));
    }

    #[test]
    fn multiple_fields_stripped_together() {
        let mut params = json!({
            "HostedZoneId": "/hostedzone/ZABC",
            "DelegationSetId": "/delegationset/N999",
            "Id": "/change/C555"
        });
        apply_route53_customizations("route53", &mut params);
        assert_eq!(params["HostedZoneId"].as_str(), Some("ZABC"));
        assert_eq!(params["DelegationSetId"].as_str(), Some("N999"));
        assert_eq!(params["Id"].as_str(), Some("C555"));
    }
}
