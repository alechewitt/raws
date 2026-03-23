use anyhow::{Context, Result};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use crate::core::config::loader::parse_ini;

type IniData = HashMap<String, HashMap<String, String>>;

/// Run the interactive `raws configure` command.
///
/// Prompts for 4 values (access key, secret key, region, output format),
/// showing current values in brackets. Writes results to ~/.aws/credentials
/// and ~/.aws/config.
pub fn run_configure(profile: &str) -> Result<()> {
    let aws_dir = aws_directory()?;
    let credentials_path = aws_dir.join("credentials");
    let config_path = aws_dir.join("config");

    // Load existing values
    let existing_creds = load_ini_file(&credentials_path);
    let existing_config = load_ini_file(&config_path);

    let cred_section = credentials_section_name(profile);
    let config_section = config_section_name(profile);

    let current_access_key = get_value(&existing_creds, &cred_section, "aws_access_key_id");
    let current_secret_key = get_value(&existing_creds, &cred_section, "aws_secret_access_key");
    let current_region = get_value(&existing_config, &config_section, "region");
    let current_output = get_value(&existing_config, &config_section, "output");

    let stdin = io::stdin();
    let mut reader = stdin.lock();

    let access_key = prompt_value(&mut reader, "AWS Access Key ID", current_access_key.as_deref())?;
    let secret_key = prompt_value(&mut reader, "AWS Secret Access Key", current_secret_key.as_deref())?;
    let region = prompt_value(&mut reader, "Default region name", current_region.as_deref())?;
    let output = prompt_value(&mut reader, "Default output format", current_output.as_deref())?;

    // Ensure ~/.aws/ directory exists
    if !aws_dir.exists() {
        std::fs::create_dir_all(&aws_dir)
            .with_context(|| format!("Failed to create directory: {}", aws_dir.display()))?;
    }

    // Update credentials file
    let mut creds_data = existing_creds;
    let cred_entry = creds_data.entry(cred_section.clone()).or_default();
    if let Some(ref v) = access_key {
        cred_entry.insert("aws_access_key_id".to_string(), v.clone());
    }
    if let Some(ref v) = secret_key {
        cred_entry.insert("aws_secret_access_key".to_string(), v.clone());
    }
    write_ini_file(&credentials_path, &creds_data)
        .with_context(|| format!("Failed to write credentials file: {}", credentials_path.display()))?;

    // Update config file
    let mut conf_data = existing_config;
    let conf_entry = conf_data.entry(config_section.clone()).or_default();
    if let Some(ref v) = region {
        conf_entry.insert("region".to_string(), v.clone());
    }
    if let Some(ref v) = output {
        conf_entry.insert("output".to_string(), v.clone());
    }
    write_ini_file(&config_path, &conf_data)
        .with_context(|| format!("Failed to write config file: {}", config_path.display()))?;

    Ok(())
}

/// Prompt the user for a value. Shows the current value in brackets.
/// If the user enters an empty string, returns the current value (or None if there is none).
fn prompt_value(
    reader: &mut impl BufRead,
    prompt: &str,
    current: Option<&str>,
) -> Result<Option<String>> {
    let display = current.unwrap_or("None");
    print!("{} [{}]: ", prompt, display);
    io::stdout().flush().context("Failed to flush stdout")?;

    let mut input = String::new();
    reader.read_line(&mut input).context("Failed to read input")?;
    let trimmed = input.trim();

    if trimmed.is_empty() {
        Ok(current.map(|s| s.to_string()))
    } else {
        Ok(Some(trimmed.to_string()))
    }
}

/// Get the section name for the credentials file.
/// In credentials, "default" section is just `[default]`, named profiles are `[profile_name]`.
fn credentials_section_name(profile: &str) -> String {
    profile.to_string()
}

/// Get the section name for the config file.
/// In config, "default" section is `[default]`, named profiles are `[profile name]`.
fn config_section_name(profile: &str) -> String {
    if profile == "default" {
        "default".to_string()
    } else {
        format!("profile {}", profile)
    }
}

/// Get the AWS home directory path (~/.aws/).
fn aws_directory() -> Result<PathBuf> {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| "/".to_string());
    Ok(PathBuf::from(home).join(".aws"))
}

/// Load an INI file, returning empty data if it doesn't exist or can't be read.
fn load_ini_file(path: &Path) -> IniData {
    match std::fs::read_to_string(path) {
        Ok(content) => parse_ini(&content),
        Err(_) => IniData::new(),
    }
}

/// Get a value from parsed INI data.
fn get_value(data: &IniData, section: &str, key: &str) -> Option<String> {
    data.get(section)
        .and_then(|s| s.get(key))
        .cloned()
}

/// Write INI data to a file, preserving section order (default first, then alphabetical).
fn write_ini_file(path: &Path, data: &IniData) -> Result<()> {
    let content = serialize_ini(data);
    std::fs::write(path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
}

/// Serialize INI data to a string.
/// Puts "default" section first, then other sections in alphabetical order.
fn serialize_ini(data: &IniData) -> String {
    let mut output = String::new();
    let mut sections: Vec<&String> = data.keys().collect();
    sections.sort_by(|a, b| {
        // "default" comes first, then alphabetical
        match (a.as_str(), b.as_str()) {
            ("default", _) => std::cmp::Ordering::Less,
            (_, "default") => std::cmp::Ordering::Greater,
            _ => a.cmp(b),
        }
    });

    let mut first = true;
    for section in sections {
        let values = match data.get(section) {
            Some(v) => v,
            None => continue,
        };
        // Skip empty sections
        if values.is_empty() {
            continue;
        }

        if !first {
            output.push('\n');
        }
        first = false;

        output.push('[');
        output.push_str(section);
        output.push_str("]\n");

        let mut keys: Vec<&String> = values.keys().collect();
        keys.sort();
        for key in keys {
            if let Some(value) = values.get(key) {
                output.push_str(key);
                output.push_str(" = ");
                output.push_str(value);
                output.push('\n');
            }
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials_section_name_default() {
        assert_eq!(credentials_section_name("default"), "default");
    }

    #[test]
    fn test_credentials_section_name_named() {
        assert_eq!(credentials_section_name("my-profile"), "my-profile");
    }

    #[test]
    fn test_config_section_name_default() {
        assert_eq!(config_section_name("default"), "default");
    }

    #[test]
    fn test_config_section_name_named() {
        assert_eq!(config_section_name("my-profile"), "profile my-profile");
    }

    #[test]
    fn test_serialize_ini_default_section() {
        let mut data = IniData::new();
        let mut section = HashMap::new();
        section.insert("aws_access_key_id".to_string(), "AKIAEXAMPLE".to_string());
        section.insert(
            "aws_secret_access_key".to_string(),
            "wJalrXUtnFEMI".to_string(),
        );
        data.insert("default".to_string(), section);

        let result = serialize_ini(&data);
        assert!(result.contains("[default]"));
        assert!(result.contains("aws_access_key_id = AKIAEXAMPLE"));
        assert!(result.contains("aws_secret_access_key = wJalrXUtnFEMI"));
    }

    #[test]
    fn test_serialize_ini_multiple_sections() {
        let mut data = IniData::new();

        let mut default_section = HashMap::new();
        default_section.insert("region".to_string(), "us-east-1".to_string());
        data.insert("default".to_string(), default_section);

        let mut profile_section = HashMap::new();
        profile_section.insert("region".to_string(), "eu-west-1".to_string());
        data.insert("profile my-profile".to_string(), profile_section);

        let result = serialize_ini(&data);
        // default should come first
        let default_pos = result.find("[default]");
        let profile_pos = result.find("[profile my-profile]");
        assert!(default_pos.is_some());
        assert!(profile_pos.is_some());
        assert!(
            default_pos < profile_pos,
            "default section should come before profile section"
        );
    }

    #[test]
    fn test_serialize_ini_empty_section_skipped() {
        let mut data = IniData::new();
        data.insert("default".to_string(), HashMap::new());
        let result = serialize_ini(&data);
        assert!(result.is_empty());
    }

    #[test]
    fn test_serialize_ini_roundtrip() {
        let mut data = IniData::new();
        let mut section = HashMap::new();
        section.insert("aws_access_key_id".to_string(), "AKIATEST".to_string());
        section.insert("aws_secret_access_key".to_string(), "secretkey".to_string());
        data.insert("default".to_string(), section);

        let serialized = serialize_ini(&data);
        let parsed = parse_ini(&serialized);

        assert_eq!(parsed["default"]["aws_access_key_id"], "AKIATEST");
        assert_eq!(parsed["default"]["aws_secret_access_key"], "secretkey");
    }

    #[test]
    fn test_get_value_exists() {
        let mut data = IniData::new();
        let mut section = HashMap::new();
        section.insert("region".to_string(), "us-east-1".to_string());
        data.insert("default".to_string(), section);

        assert_eq!(
            get_value(&data, "default", "region"),
            Some("us-east-1".to_string())
        );
    }

    #[test]
    fn test_get_value_missing_key() {
        let mut data = IniData::new();
        let section = HashMap::new();
        data.insert("default".to_string(), section);

        assert_eq!(get_value(&data, "default", "region"), None);
    }

    #[test]
    fn test_get_value_missing_section() {
        let data = IniData::new();
        assert_eq!(get_value(&data, "default", "region"), None);
    }

    #[test]
    fn test_prompt_value_with_input() {
        let input = b"AKIANEWKEY\n";
        let mut reader = io::BufReader::new(&input[..]);
        let result = prompt_value(&mut reader, "AWS Access Key ID", Some("AKIAOLDKEY")).unwrap();
        assert_eq!(result, Some("AKIANEWKEY".to_string()));
    }

    #[test]
    fn test_prompt_value_empty_keeps_current() {
        let input = b"\n";
        let mut reader = io::BufReader::new(&input[..]);
        let result = prompt_value(&mut reader, "AWS Access Key ID", Some("AKIAOLDKEY")).unwrap();
        assert_eq!(result, Some("AKIAOLDKEY".to_string()));
    }

    #[test]
    fn test_prompt_value_empty_no_current() {
        let input = b"\n";
        let mut reader = io::BufReader::new(&input[..]);
        let result = prompt_value(&mut reader, "AWS Access Key ID", None).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_write_and_read_ini_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.ini");

        let mut data = IniData::new();
        let mut section = HashMap::new();
        section.insert("key1".to_string(), "value1".to_string());
        section.insert("key2".to_string(), "value2".to_string());
        data.insert("default".to_string(), section);

        write_ini_file(&path, &data).unwrap();

        let loaded = load_ini_file(&path);
        assert_eq!(loaded["default"]["key1"], "value1");
        assert_eq!(loaded["default"]["key2"], "value2");
    }

    #[test]
    fn test_load_ini_file_nonexistent() {
        let data = load_ini_file(Path::new("/nonexistent/path/config"));
        assert!(data.is_empty());
    }

    #[test]
    fn test_write_ini_preserves_other_sections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");

        // Write initial data with two sections
        let mut data = IniData::new();
        let mut default_section = HashMap::new();
        default_section.insert("aws_access_key_id".to_string(), "AKIA1".to_string());
        data.insert("default".to_string(), default_section);

        let mut other_section = HashMap::new();
        other_section.insert("aws_access_key_id".to_string(), "AKIA2".to_string());
        data.insert("other".to_string(), other_section);

        write_ini_file(&path, &data).unwrap();

        // Read back and verify both sections are present
        let loaded = load_ini_file(&path);
        assert_eq!(loaded["default"]["aws_access_key_id"], "AKIA1");
        assert_eq!(loaded["other"]["aws_access_key_id"], "AKIA2");
    }

    #[test]
    fn test_update_existing_section_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config");

        // Write initial data
        let mut data = IniData::new();
        let mut section = HashMap::new();
        section.insert("region".to_string(), "us-east-1".to_string());
        section.insert("output".to_string(), "json".to_string());
        data.insert("default".to_string(), section);
        write_ini_file(&path, &data).unwrap();

        // Load, update, and write back
        let mut loaded = load_ini_file(&path);
        loaded
            .entry("default".to_string())
            .or_default()
            .insert("region".to_string(), "eu-west-1".to_string());
        write_ini_file(&path, &loaded).unwrap();

        // Verify the update persisted and other values are preserved
        let reloaded = load_ini_file(&path);
        assert_eq!(reloaded["default"]["region"], "eu-west-1");
        assert_eq!(reloaded["default"]["output"], "json");
    }
}
