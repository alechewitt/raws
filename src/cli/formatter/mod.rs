pub mod json;
pub mod table;
pub mod text;

use anyhow::Result;
use serde_json::Value;

pub fn format_output(value: &Value, format: &str) -> Result<String> {
    match format {
        "json" => json::format_json(value),
        "text" => text::format_text(value),
        "table" => table::format_table(value),
        _ => json::format_json(value),
    }
}
