pub mod loader;

use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ServiceModel {
    pub metadata: ServiceMetadata,
    pub operations: HashMap<String, Operation>,
    pub shapes: HashMap<String, Value>,
    pub raw: Value,
}

#[derive(Debug, Clone)]
pub struct ServiceMetadata {
    pub api_version: String,
    pub endpoint_prefix: String,
    pub protocol: String,
    pub service_id: String,
    pub signature_version: String,
    pub target_prefix: Option<String>,
    pub json_version: Option<String>,
    pub global_endpoint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub name: String,
    pub http_method: String,
    pub http_request_uri: String,
    pub input_shape: Option<String>,
    pub output_shape: Option<String>,
    pub result_wrapper: Option<String>,
    pub errors: Vec<String>,
    pub documentation: Option<String>,
}
