use anyhow::Result;
use serde_json::Value;

pub fn format_json(value: &Value) -> Result<String> {
    Ok(serde_json::to_string_pretty(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_format_simple() {
        let value = serde_json::json!({
            "UserId": "AIDAEXAMPLE",
            "Account": "123456789012",
            "Arn": "arn:aws:iam::123456789012:user/alice"
        });
        let output = format_json(&value).unwrap();
        assert!(output.contains("\"UserId\""));
        assert!(output.contains("AIDAEXAMPLE"));
    }
}
