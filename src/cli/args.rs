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

    /// Output format: json, table, text
    #[arg(long, global = true, default_value = "json")]
    pub output: String,

    /// Enable debug output
    #[arg(long, global = true)]
    pub debug: bool,

    /// Override endpoint URL
    #[arg(long, global = true)]
    pub endpoint_url: Option<String>,

    /// Service name (e.g., sts, s3, ec2)
    pub service: Option<String>,

    /// Operation name (e.g., get-caller-identity, list-buckets)
    pub operation: Option<String>,

    /// Remaining arguments for the operation
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}
