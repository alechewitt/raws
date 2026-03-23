use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;

type IniData = HashMap<String, HashMap<String, String>>;

pub fn parse_ini(content: &str) -> IniData {
    let mut data = IniData::new();
    let mut current_section = String::new();

    for line in content.lines() {
        let line = line.trim();

        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }

        if line.starts_with('[') && line.ends_with(']') {
            current_section = line[1..line.len() - 1].trim().to_string();
            data.entry(current_section.clone())
                .or_default();
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            if !current_section.is_empty() {
                data.entry(current_section.clone())
                    .or_default()
                    .insert(key.trim().to_string(), value.trim().to_string());
            }
        }
    }

    data
}

pub fn load_credentials_file(path: &Path) -> Result<IniData> {
    let content = std::fs::read_to_string(path)?;
    Ok(parse_ini(&content))
}

pub fn load_config_file(path: &Path) -> Result<IniData> {
    let content = std::fs::read_to_string(path)?;
    let raw = parse_ini(&content);

    // Config file uses "profile <name>" prefix for non-default sections
    let mut normalized = IniData::new();
    for (section, values) in raw {
        let normalized_name = if section == "default" {
            section
        } else if let Some(name) = section.strip_prefix("profile ") {
            name.trim().to_string()
        } else {
            section
        };
        normalized.insert(normalized_name, values);
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ini_parser_basic() {
        let content = r#"
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

[my-profile]
aws_access_key_id = AKIAI44QH8DHBEXAMPLE
aws_secret_access_key = je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY
aws_session_token = FwoGZXIvYXdzEBYaDHqa0AP...
"#;
        let data = parse_ini(content);
        assert_eq!(data.len(), 2);
        assert_eq!(
            data["default"]["aws_access_key_id"],
            "AKIAIOSFODNN7EXAMPLE"
        );
        assert_eq!(
            data["my-profile"]["aws_access_key_id"],
            "AKIAI44QH8DHBEXAMPLE"
        );
        assert!(data["my-profile"].contains_key("aws_session_token"));
    }

    #[test]
    fn test_ini_parser_comments() {
        let content = r#"
# This is a comment
[default]
; This is also a comment
key = value
"#;
        let data = parse_ini(content);
        assert_eq!(data.len(), 1);
        assert_eq!(data["default"]["key"], "value");
    }

    #[test]
    fn test_config_file_profile_prefix() {
        let content = r#"
[default]
region = us-east-1
output = json

[profile my-profile]
region = eu-west-1
output = table
"#;
        let raw = parse_ini(content);
        // Config file has "profile my-profile"
        assert!(raw.contains_key("profile my-profile"));

        // After normalization
        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp.path(), content).unwrap();
        let normalized = load_config_file(temp.path()).unwrap();
        assert!(normalized.contains_key("my-profile"));
        assert_eq!(normalized["my-profile"]["region"], "eu-west-1");
    }
}
