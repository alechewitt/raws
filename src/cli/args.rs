use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "raws", about = "AWS CLI reimplementation in Rust")]
pub struct GlobalArgs {
    /// AWS region to use
    #[arg(long, global = true)]
    pub region: Option<String>,

    /// Named profile to use
    #[arg(long, global = true)]
    pub profile: Option<String>,

    /// Output format: json, table, text, yaml, yaml-stream
    #[arg(long, global = true, default_value = "json")]
    pub output: String,

    /// Enable debug output
    #[arg(long, global = true)]
    pub debug: bool,

    /// Disable automatic pagination
    #[arg(long, global = true)]
    pub no_paginate: bool,

    /// Use dual-stack (IPv4/IPv6) endpoints
    #[arg(long, global = true)]
    pub use_dualstack_endpoint: bool,

    /// Use FIPS-compliant endpoints
    #[arg(long, global = true)]
    pub use_fips_endpoint: bool,

    /// JMESPath query to filter/transform output
    #[arg(long, global = true)]
    pub query: Option<String>,

    /// Override endpoint URL
    #[arg(long, global = true)]
    pub endpoint_url: Option<String>,

    /// Connection timeout in seconds (default: 60)
    #[arg(long, global = true)]
    pub cli_connect_timeout: Option<u64>,

    /// Read/request timeout in seconds (default: 60)
    #[arg(long, global = true)]
    pub cli_read_timeout: Option<u64>,

    /// Do not sign requests (for anonymous/public access)
    #[arg(long, global = true)]
    pub no_sign_request: bool,

    /// Do not verify SSL certificates
    #[arg(long, global = true)]
    pub no_verify_ssl: bool,

    /// Service name (e.g., sts, s3, ec2)
    pub service: Option<String>,

    /// Operation name (e.g., get-caller-identity, list-buckets)
    pub operation: Option<String>,

    /// Remaining arguments for the operation
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_args_region() {
        let args = GlobalArgs::try_parse_from([
            "raws", "--region", "us-east-1", "sts", "get-caller-identity",
        ])
        .unwrap();
        assert_eq!(args.region, Some("us-east-1".to_string()));
        assert_eq!(args.service, Some("sts".to_string()));
        assert_eq!(args.operation, Some("get-caller-identity".to_string()));
    }

    #[test]
    fn test_global_args_profile() {
        let args = GlobalArgs::try_parse_from([
            "raws", "--profile", "my-profile", "s3", "list-buckets",
        ])
        .unwrap();
        assert_eq!(args.profile, Some("my-profile".to_string()));
        assert_eq!(args.service, Some("s3".to_string()));
        assert_eq!(args.operation, Some("list-buckets".to_string()));
    }

    #[test]
    fn test_global_args_output() {
        let args = GlobalArgs::try_parse_from([
            "raws", "--output", "table", "ec2", "describe-instances",
        ])
        .unwrap();
        assert_eq!(args.output, "table");
    }

    #[test]
    fn test_global_args_output_default() {
        let args = GlobalArgs::try_parse_from([
            "raws", "sts", "get-caller-identity",
        ])
        .unwrap();
        assert_eq!(args.output, "json");
    }

    #[test]
    fn test_global_args_debug() {
        let args = GlobalArgs::try_parse_from([
            "raws", "--debug", "sts", "get-caller-identity",
        ])
        .unwrap();
        assert!(args.debug);
    }

    #[test]
    fn test_global_args_debug_default_false() {
        let args = GlobalArgs::try_parse_from([
            "raws", "sts", "get-caller-identity",
        ])
        .unwrap();
        assert!(!args.debug);
    }

    #[test]
    fn test_global_args_all_combined() {
        let args = GlobalArgs::try_parse_from([
            "raws",
            "--region", "eu-west-1",
            "--profile", "prod",
            "--output", "text",
            "--debug",
            "sts",
            "get-caller-identity",
        ])
        .unwrap();
        assert_eq!(args.region, Some("eu-west-1".to_string()));
        assert_eq!(args.profile, Some("prod".to_string()));
        assert_eq!(args.output, "text");
        assert!(args.debug);
        assert_eq!(args.service, Some("sts".to_string()));
        assert_eq!(args.operation, Some("get-caller-identity".to_string()));
    }

    #[test]
    fn test_global_args_no_service_or_operation() {
        let args = GlobalArgs::try_parse_from(["raws"]).unwrap();
        assert!(args.service.is_none());
        assert!(args.operation.is_none());
    }

    #[test]
    fn test_global_args_endpoint_url() {
        let args = GlobalArgs::try_parse_from([
            "raws",
            "--endpoint-url", "http://localhost:4566",
            "sts",
            "get-caller-identity",
        ])
        .unwrap();
        assert_eq!(
            args.endpoint_url,
            Some("http://localhost:4566".to_string())
        );
    }

    #[test]
    fn test_global_args_trailing_operation_args() {
        let args = GlobalArgs::try_parse_from([
            "raws", "iam", "create-user", "--user-name", "alice",
        ])
        .unwrap();
        assert_eq!(args.service, Some("iam".to_string()));
        assert_eq!(args.operation, Some("create-user".to_string()));
        assert_eq!(args.args, vec!["--user-name", "alice"]);
    }

    #[test]
    fn test_global_args_region_after_service() {
        // Global args can appear after service/operation
        let args = GlobalArgs::try_parse_from([
            "raws", "sts", "get-caller-identity", "--region", "us-west-2",
        ])
        .unwrap();
        assert_eq!(args.region, Some("us-west-2".to_string()));
        assert_eq!(args.service, Some("sts".to_string()));
        assert_eq!(args.operation, Some("get-caller-identity".to_string()));
    }

    #[test]
    fn test_global_args_use_dualstack_endpoint() {
        let args = GlobalArgs::try_parse_from([
            "raws", "--use-dualstack-endpoint", "s3api", "list-buckets",
        ])
        .unwrap();
        assert!(args.use_dualstack_endpoint);
        assert!(!args.use_fips_endpoint);
    }

    #[test]
    fn test_global_args_use_fips_endpoint() {
        let args = GlobalArgs::try_parse_from([
            "raws", "--use-fips-endpoint", "sts", "get-caller-identity",
        ])
        .unwrap();
        assert!(args.use_fips_endpoint);
        assert!(!args.use_dualstack_endpoint);
    }

    #[test]
    fn test_global_args_dualstack_and_fips_combined() {
        let args = GlobalArgs::try_parse_from([
            "raws",
            "--use-dualstack-endpoint",
            "--use-fips-endpoint",
            "s3api",
            "list-buckets",
        ])
        .unwrap();
        assert!(args.use_dualstack_endpoint);
        assert!(args.use_fips_endpoint);
    }

    #[test]
    fn test_global_args_dualstack_default_false() {
        let args = GlobalArgs::try_parse_from([
            "raws", "sts", "get-caller-identity",
        ])
        .unwrap();
        assert!(!args.use_dualstack_endpoint);
        assert!(!args.use_fips_endpoint);
    }

    #[test]
    fn test_global_args_no_sign_request() {
        let args = GlobalArgs::try_parse_from(["raws", "--no-sign-request", "s3api", "list-objects-v2", "--bucket", "pub"]).unwrap();
        assert!(args.no_sign_request);
    }

    #[test]
    fn test_global_args_no_sign_request_default_false() {
        let args = GlobalArgs::try_parse_from(["raws", "sts", "get-caller-identity"]).unwrap();
        assert!(!args.no_sign_request);
    }

    #[test]
    fn test_global_args_no_verify_ssl() {
        let args = GlobalArgs::try_parse_from(["raws", "--no-verify-ssl", "s3api", "list-buckets"]).unwrap();
        assert!(args.no_verify_ssl);
    }

    #[test]
    fn test_global_args_no_verify_ssl_default_false() {
        let args = GlobalArgs::try_parse_from(["raws", "sts", "get-caller-identity"]).unwrap();
        assert!(!args.no_verify_ssl);
    }

    #[test]
    fn test_global_args_no_sign_and_no_verify_combined() {
        let args = GlobalArgs::try_parse_from(["raws", "--no-sign-request", "--no-verify-ssl", "s3api", "get-object", "--bucket", "pub", "--key", "k"]).unwrap();
        assert!(args.no_sign_request);
        assert!(args.no_verify_ssl);
    }

    #[test]
    fn test_global_args_no_sign_request_after_service() {
        let args = GlobalArgs::try_parse_from(["raws", "s3api", "list-objects-v2", "--no-sign-request", "--bucket", "pub"]).unwrap();
        assert!(args.no_sign_request);
    }
}
