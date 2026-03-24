pub mod json;
pub mod table;
pub mod text;
pub mod yaml;

use anyhow::Result;
use serde_json::Value;

pub fn format_output_with_title(value: &Value, format: &str, title: Option<&str>) -> Result<String> {
    match format {
        "json" => json::format_json(value),
        "text" => text::format_text(value),
        "table" => table::format_table_with_title(value, title),
        "yaml" => yaml::format_yaml(value),
        "yaml-stream" => yaml::format_yaml_stream(value),
        _ => json::format_json(value),
    }
}
