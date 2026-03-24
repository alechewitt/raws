use anyhow::{Context, Result};
use std::collections::{BTreeSet, HashMap};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use crate::core::config::loader::parse_ini;

type IniData = HashMap<String, HashMap<String, String>>;

/// Credential keys that live in ~/.aws/credentials rather than ~/.aws/config.
const CREDENTIAL_KEYS: &[&str] = &[
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
];

/// Run `raws configure get <varname>` to read a single config/credentials value.
///
/// Supports three forms of `varname`:
/// - Simple key (e.g. `region`): looks up in the current profile's config section,
///   or credentials section for credential keys.
/// - `profile.<name>.<key>`: looks up `<key>` in the named profile.
/// - `<section>.<key>` (e.g. `s3.max_concurrent_requests`): looks up a subsection
///   key in the current profile's config section.
///
/// Returns 0 if the value was found (printed to stdout), 1 if not found.
pub fn run_configure_get(profile: &str, varname: &str) -> Result<i32> {
    let aws_dir = aws_directory()?;
    let config_path = aws_dir.join("config");
    let credentials_path = aws_dir.join("credentials");

    let config_data = load_ini_file(&config_path);
    let creds_data = load_ini_file(&credentials_path);

    let value = resolve_configure_get_value(profile, varname, &config_data, &creds_data);

    match value {
        Some(v) => {
            println!("{}", v);
            Ok(0)
        }
        None => Ok(1),
    }
}

/// Core lookup logic for `configure get`, separated for testability.
fn resolve_configure_get_value(
    profile: &str,
    varname: &str,
    config_data: &IniData,
    creds_data: &IniData,
) -> Option<String> {
    let parts: Vec<&str> = varname.splitn(3, '.').collect();

    match parts.len() {
        1 => {
            // Simple key: look in current profile
            let key = parts[0];
            if CREDENTIAL_KEYS.contains(&key) {
                let cred_section = credentials_section_name(profile);
                get_value(creds_data, &cred_section, key)
                    .or_else(|| {
                        let config_section = config_section_name(profile);
                        get_value(config_data, &config_section, key)
                    })
            } else {
                let config_section = config_section_name(profile);
                get_value(config_data, &config_section, key)
            }
        }
        2 => {
            // <section>.<key> — treat as subsection key in the current profile's config section
            // e.g., "s3.max_concurrent_requests" looks up key "s3.max_concurrent_requests"
            // in the profile's config section. AWS CLI stores these as dotted keys.
            let config_section = config_section_name(profile);
            get_value(config_data, &config_section, varname)
        }
        3 if parts[0] == "profile" => {
            // profile.<name>.<key> — look in specified profile
            let target_profile = parts[1];
            let key = parts[2];
            if CREDENTIAL_KEYS.contains(&key) {
                let cred_section = credentials_section_name(target_profile);
                get_value(creds_data, &cred_section, key)
                    .or_else(|| {
                        let config_section = config_section_name(target_profile);
                        get_value(config_data, &config_section, key)
                    })
            } else {
                let config_section = config_section_name(target_profile);
                get_value(config_data, &config_section, key)
            }
        }
        _ => None,
    }
}

/// Run `raws configure set <varname> <value>` to write a single config/credentials value.
///
/// Supports three forms of `varname`:
/// - Simple key (e.g. `region`): writes to config or credentials for the current profile,
///   depending on whether the key is a credential key.
/// - `profile.<name>.<key>`: writes to the named profile (config or credentials depending on key).
/// - `<section>.<key>` (e.g. `s3.max_concurrent_requests`): writes as a flat dotted key
///   in the current profile's config section.
pub fn run_configure_set(profile: &str, varname: &str, value: &str) -> Result<()> {
    let aws_dir = aws_directory()?;

    // Ensure ~/.aws/ directory exists
    if !aws_dir.exists() {
        std::fs::create_dir_all(&aws_dir)
            .with_context(|| format!("Failed to create directory: {}", aws_dir.display()))?;
    }

    let parts: Vec<&str> = varname.splitn(3, '.').collect();

    match parts.len() {
        1 => {
            // Simple key: write to current profile
            let key = parts[0];
            if CREDENTIAL_KEYS.contains(&key) {
                let cred_section = credentials_section_name(profile);
                set_ini_value(&aws_dir.join("credentials"), &cred_section, key, value)?;
            } else {
                let config_section = config_section_name(profile);
                set_ini_value(&aws_dir.join("config"), &config_section, key, value)?;
            }
        }
        2 => {
            // <section>.<key> — store as flat dotted key in current profile's config section
            let config_section = config_section_name(profile);
            set_ini_value(&aws_dir.join("config"), &config_section, varname, value)?;
        }
        3 if parts[0] == "profile" => {
            // profile.<name>.<key> — write to named profile
            let target_profile = parts[1];
            let key = parts[2];
            if CREDENTIAL_KEYS.contains(&key) {
                let cred_section = credentials_section_name(target_profile);
                set_ini_value(&aws_dir.join("credentials"), &cred_section, key, value)?;
            } else {
                let config_section = config_section_name(target_profile);
                set_ini_value(&aws_dir.join("config"), &config_section, key, value)?;
            }
        }
        _ => {
            anyhow::bail!("Invalid variable name: {}", varname);
        }
    }

    Ok(())
}

/// Helper: load an INI file, set a single key in a section, and write it back.
fn set_ini_value(path: &Path, section: &str, key: &str, value: &str) -> Result<()> {
    let mut data = load_ini_file(path);
    data.entry(section.to_string())
        .or_default()
        .insert(key.to_string(), value.to_string());
    write_ini_file(path, &data)
}

/// Run `raws configure list` to show where each config value comes from.
///
/// Shows 4 configuration items: profile, access_key, secret_key, region.
/// For each item, displays the value, its type (env, config-file, manual, None),
/// and the location it was loaded from. Credential values are masked to show
/// only the last 4 characters.
pub fn run_configure_list(profile: &str, profile_from_flag: bool) -> Result<()> {
    let output = build_configure_list_output(profile, profile_from_flag);
    print!("{}", output);
    Ok(())
}

/// Describes where a configuration value came from.
struct ConfigSource {
    value: String,
    source_type: String,
    location: String,
}

/// Build the full output string for `configure list`, separated for testability.
fn build_configure_list_output(profile: &str, profile_from_flag: bool) -> String {
    let profile_source = resolve_profile_source(profile, profile_from_flag);
    let access_key_source = resolve_credential_source(
        "aws_access_key_id",
        "AWS_ACCESS_KEY_ID",
        profile,
    );
    let secret_key_source = resolve_credential_source(
        "aws_secret_access_key",
        "AWS_SECRET_ACCESS_KEY",
        profile,
    );
    let region_source = resolve_region_source(profile);

    let rows: Vec<(&str, Option<ConfigSource>)> = vec![
        ("profile", profile_source),
        ("access_key", access_key_source),
        ("secret_key", secret_key_source),
        ("region", region_source),
    ];

    format_configure_list_output(&rows)
}

/// Format the configure list output with aligned columns matching AWS CLI format.
fn format_configure_list_output(rows: &[(&str, Option<ConfigSource>)]) -> String {
    let mut output = String::new();
    // Header line
    output.push_str(&format!(
        "{:>10}{:>24}{:>16}    {}\n",
        "Name", "Value", "Type", "Location"
    ));
    // Separator line
    output.push_str(&format!(
        "{:>10}{:>24}{:>16}    {}\n",
        "----", "-----", "----", "--------"
    ));

    for (name, source) in rows {
        match source {
            Some(src) => {
                let display_value = if *name == "access_key" || *name == "secret_key" {
                    mask_credential(&src.value)
                } else {
                    src.value.clone()
                };
                output.push_str(&format!(
                    "{:>10}{:>24}{:>16}    {}\n",
                    name, display_value, src.source_type, src.location
                ));
            }
            None => {
                output.push_str(&format!(
                    "{:>10}{:>24}{:>16}    {}\n",
                    name, "<not set>", "None", "None"
                ));
            }
        }
    }

    output
}

/// Mask a credential value, showing only the last 4 characters.
fn mask_credential(value: &str) -> String {
    if value.len() <= 4 {
        value.to_string()
    } else {
        format!("****************{}", &value[value.len() - 4..])
    }
}

/// Determine the source of the profile configuration value.
fn resolve_profile_source(profile: &str, profile_from_flag: bool) -> Option<ConfigSource> {
    if profile_from_flag {
        Some(ConfigSource {
            value: profile.to_string(),
            source_type: "manual".to_string(),
            location: "--profile".to_string(),
        })
    } else if std::env::var("AWS_PROFILE").is_ok() {
        Some(ConfigSource {
            value: profile.to_string(),
            source_type: "env".to_string(),
            location: "AWS_PROFILE".to_string(),
        })
    } else {
        None
    }
}

/// Determine the source of a credential value (access_key or secret_key).
/// Checks env var first, then credentials file, then config file.
fn resolve_credential_source(
    ini_key: &str,
    env_var: &str,
    profile: &str,
) -> Option<ConfigSource> {
    // Check environment variable first
    if let Ok(val) = std::env::var(env_var) {
        if !val.is_empty() {
            return Some(ConfigSource {
                value: val,
                source_type: "env".to_string(),
                location: env_var.to_string(),
            });
        }
    }

    // Check credentials file
    let aws_dir = match aws_directory() {
        Ok(d) => d,
        Err(_) => return None,
    };

    let creds_path = aws_dir.join("credentials");
    let creds_data = load_ini_file(&creds_path);
    let cred_section = credentials_section_name(profile);
    if let Some(val) = get_value(&creds_data, &cred_section, ini_key) {
        return Some(ConfigSource {
            value: val,
            source_type: "config-file".to_string(),
            location: "~/.aws/credentials".to_string(),
        });
    }

    // Check config file
    let config_path = aws_dir.join("config");
    let config_data = load_ini_file(&config_path);
    let config_section = config_section_name(profile);
    if let Some(val) = get_value(&config_data, &config_section, ini_key) {
        return Some(ConfigSource {
            value: val,
            source_type: "config-file".to_string(),
            location: "~/.aws/config".to_string(),
        });
    }

    None
}

/// Determine the source of the region configuration value.
/// Checks AWS_REGION, then AWS_DEFAULT_REGION env vars, then config file.
fn resolve_region_source(profile: &str) -> Option<ConfigSource> {
    // Check AWS_REGION env var
    if let Ok(val) = std::env::var("AWS_REGION") {
        if !val.is_empty() {
            return Some(ConfigSource {
                value: val,
                source_type: "env".to_string(),
                location: "AWS_REGION".to_string(),
            });
        }
    }

    // Check AWS_DEFAULT_REGION env var
    if let Ok(val) = std::env::var("AWS_DEFAULT_REGION") {
        if !val.is_empty() {
            return Some(ConfigSource {
                value: val,
                source_type: "env".to_string(),
                location: "AWS_DEFAULT_REGION".to_string(),
            });
        }
    }

    // Check config file
    let aws_dir = match aws_directory() {
        Ok(d) => d,
        Err(_) => return None,
    };

    let config_path = aws_dir.join("config");
    let config_data = load_ini_file(&config_path);
    let config_section = config_section_name(profile);
    if let Some(val) = get_value(&config_data, &config_section, "region") {
        return Some(ConfigSource {
            value: val,
            source_type: "config-file".to_string(),
            location: "~/.aws/config".to_string(),
        });
    }

    None
}

/// Run `raws configure list-profiles` to list all profile names from
/// both ~/.aws/config and ~/.aws/credentials, deduplicated and sorted.
pub fn run_configure_list_profiles() -> Result<()> {
    let output = build_list_profiles_output()?;
    print!("{}", output);
    Ok(())
}

/// Build the output for `configure list-profiles`, separated for testability.
fn build_list_profiles_output() -> Result<String> {
    let aws_dir = aws_directory()?;
    let config_path = aws_dir.join("config");
    let credentials_path = aws_dir.join("credentials");

    let config_data = load_ini_file(&config_path);
    let creds_data = load_ini_file(&credentials_path);

    let profiles = collect_profile_names(&config_data, &creds_data);
    let mut output = String::new();
    for name in &profiles {
        output.push_str(name);
        output.push('\n');
    }
    Ok(output)
}

/// Extract all profile names from config and credentials INI data,
/// deduplicated and sorted alphabetically.
fn collect_profile_names(config_data: &IniData, creds_data: &IniData) -> Vec<String> {
    let mut names = BTreeSet::new();

    // Config file: sections are [default] or [profile <name>]
    for section in config_data.keys() {
        if section == "default" {
            names.insert("default".to_string());
        } else if let Some(name) = section.strip_prefix("profile ") {
            names.insert(name.to_string());
        }
    }

    // Credentials file: sections are [default] or [<name>]
    for section in creds_data.keys() {
        names.insert(section.to_string());
    }

    names.into_iter().collect()
}

/// Run `raws configure export-credentials` to output resolved credentials
/// in an exportable format (env, env-no-export, or json).
pub fn run_configure_export_credentials(profile: &str, format: &str) -> Result<()> {
    let output = build_export_credentials_output(profile, format)?;
    print!("{}", output);
    Ok(())
}

/// Resolve credentials for a profile: env vars first, then credentials file,
/// then config file. Returns (access_key, secret_key, optional session_token).
fn resolve_export_credentials(profile: &str) -> Result<(String, String, Option<String>)> {
    // Try env vars first
    let env_access = std::env::var("AWS_ACCESS_KEY_ID").ok().filter(|v| !v.is_empty());
    let env_secret = std::env::var("AWS_SECRET_ACCESS_KEY").ok().filter(|v| !v.is_empty());

    if let (Some(ak), Some(sk)) = (env_access, env_secret) {
        let token = std::env::var("AWS_SESSION_TOKEN").ok().filter(|v| !v.is_empty());
        return Ok((ak, sk, token));
    }

    // Try credentials file, then config file
    let aws_dir = aws_directory()?;
    let credentials_path = aws_dir.join("credentials");
    let config_path = aws_dir.join("config");

    let creds_data = load_ini_file(&credentials_path);
    let config_data = load_ini_file(&config_path);

    let cred_section = credentials_section_name(profile);
    let config_section = config_section_name(profile);

    let access_key = get_value(&creds_data, &cred_section, "aws_access_key_id")
        .or_else(|| get_value(&config_data, &config_section, "aws_access_key_id"));
    let secret_key = get_value(&creds_data, &cred_section, "aws_secret_access_key")
        .or_else(|| get_value(&config_data, &config_section, "aws_secret_access_key"));
    let session_token = get_value(&creds_data, &cred_section, "aws_session_token")
        .or_else(|| get_value(&config_data, &config_section, "aws_session_token"));

    match (access_key, secret_key) {
        (Some(ak), Some(sk)) => Ok((ak, sk, session_token)),
        _ => anyhow::bail!(
            "Unable to locate credentials for profile '{}'. \
             Configure credentials with `raws configure`.",
            profile
        ),
    }
}

/// Build the output string for `configure export-credentials`, separated for testability.
fn build_export_credentials_output(profile: &str, format: &str) -> Result<String> {
    let (access_key, secret_key, session_token) = resolve_export_credentials(profile)?;
    format_export_credentials(&access_key, &secret_key, session_token.as_deref(), format)
}

fn format_export_credentials(
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
    format: &str,
) -> Result<String> {
    match format {
        "env" => {
            let mut out = String::new();
            out.push_str(&std::format!("export AWS_ACCESS_KEY_ID={}\n", access_key));
            out.push_str(&std::format!("export AWS_SECRET_ACCESS_KEY={}\n", secret_key));
            if let Some(token) = session_token {
                out.push_str(&std::format!("export AWS_SESSION_TOKEN={}\n", token));
            }
            Ok(out)
        }
        "env-no-export" => {
            let mut out = String::new();
            out.push_str(&std::format!("AWS_ACCESS_KEY_ID={}\n", access_key));
            out.push_str(&std::format!("AWS_SECRET_ACCESS_KEY={}\n", secret_key));
            if let Some(token) = session_token {
                out.push_str(&std::format!("AWS_SESSION_TOKEN={}\n", token));
            }
            Ok(out)
        }
        "json" => {
            let mut out = String::from("{\n");
            out.push_str(&std::format!("    \"AccessKeyId\": \"{}\",\n", access_key));
            out.push_str(&std::format!("    \"SecretAccessKey\": \"{}\",\n", secret_key));
            match session_token {
                Some(token) => {
                    out.push_str(&std::format!("    \"SessionToken\": \"{}\"\n", token));
                }
                None => {
                    out.clear();
                    out.push_str("{\n");
                    out.push_str(&std::format!("    \"AccessKeyId\": \"{}\",\n", access_key));
                    out.push_str(&std::format!("    \"SecretAccessKey\": \"{}\"\n", secret_key));
                }
            }
            out.push_str("}\n");
            Ok(out)
        }
        _ => anyhow::bail!(
            "Unknown format '{}'. Supported formats: env, env-no-export, json",
            format
        ),
    }
}

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

    // --- configure get tests ---

    fn make_config_data(section: &str, key: &str, value: &str) -> IniData {
        let mut data = IniData::new();
        let mut s = HashMap::new();
        s.insert(key.to_string(), value.to_string());
        data.insert(section.to_string(), s);
        data
    }

    #[test]
    fn test_configure_get_simple_key_from_config() {
        let config = make_config_data("default", "region", "us-west-2");
        let creds = IniData::new();
        let result = resolve_configure_get_value("default", "region", &config, &creds);
        assert_eq!(result, Some("us-west-2".to_string()));
    }

    #[test]
    fn test_configure_get_simple_key_named_profile() {
        let config = make_config_data("profile myprofile", "region", "eu-west-1");
        let creds = IniData::new();
        let result = resolve_configure_get_value("myprofile", "region", &config, &creds);
        assert_eq!(result, Some("eu-west-1".to_string()));
    }

    #[test]
    fn test_configure_get_key_not_found() {
        let config = IniData::new();
        let creds = IniData::new();
        let result = resolve_configure_get_value("default", "region", &config, &creds);
        assert_eq!(result, None);
    }

    #[test]
    fn test_configure_get_credential_key_from_credentials() {
        let config = IniData::new();
        let creds = make_config_data("default", "aws_access_key_id", "AKIAEXAMPLE");
        let result = resolve_configure_get_value("default", "aws_access_key_id", &config, &creds);
        assert_eq!(result, Some("AKIAEXAMPLE".to_string()));
    }

    #[test]
    fn test_configure_get_secret_key_from_credentials() {
        let config = IniData::new();
        let creds = make_config_data("default", "aws_secret_access_key", "secretvalue");
        let result = resolve_configure_get_value("default", "aws_secret_access_key", &config, &creds);
        assert_eq!(result, Some("secretvalue".to_string()));
    }

    #[test]
    fn test_configure_get_session_token_from_credentials() {
        let config = IniData::new();
        let creds = make_config_data("default", "aws_session_token", "tokenvalue");
        let result = resolve_configure_get_value("default", "aws_session_token", &config, &creds);
        assert_eq!(result, Some("tokenvalue".to_string()));
    }

    #[test]
    fn test_configure_get_dotted_profile_key() {
        let config = make_config_data("profile myprofile", "region", "ap-southeast-1");
        let creds = IniData::new();
        let result = resolve_configure_get_value("default", "profile.myprofile.region", &config, &creds);
        assert_eq!(result, Some("ap-southeast-1".to_string()));
    }

    #[test]
    fn test_configure_get_dotted_profile_credential_key() {
        let config = IniData::new();
        let creds = make_config_data("myprofile", "aws_access_key_id", "AKIAOTHER");
        let result = resolve_configure_get_value("default", "profile.myprofile.aws_access_key_id", &config, &creds);
        assert_eq!(result, Some("AKIAOTHER".to_string()));
    }

    #[test]
    fn test_configure_get_dotted_subsection_key() {
        let config = make_config_data("default", "s3.max_concurrent_requests", "10");
        let creds = IniData::new();
        let result = resolve_configure_get_value("default", "s3.max_concurrent_requests", &config, &creds);
        assert_eq!(result, Some("10".to_string()));
    }

    #[test]
    fn test_configure_get_dotted_subsection_not_found() {
        let config = IniData::new();
        let creds = IniData::new();
        let result = resolve_configure_get_value("default", "s3.max_concurrent_requests", &config, &creds);
        assert_eq!(result, None);
    }

    #[test]
    fn test_configure_get_dotted_profile_not_found() {
        let config = IniData::new();
        let creds = IniData::new();
        let result = resolve_configure_get_value("default", "profile.nonexistent.region", &config, &creds);
        assert_eq!(result, None);
    }

    // --- configure set tests ---

    #[test]
    fn test_configure_set_simple_config_key() {
        let dir = tempfile::tempdir().unwrap();
        let aws_dir = dir.path().join(".aws");
        std::fs::create_dir_all(&aws_dir).unwrap();
        let config_path = aws_dir.join("config");

        // Pre-populate config with a value to ensure it's preserved
        let mut data = IniData::new();
        let mut section = HashMap::new();
        section.insert("output".to_string(), "json".to_string());
        data.insert("default".to_string(), section);
        write_ini_file(&config_path, &data).unwrap();

        // Use set_ini_value to write region
        set_ini_value(&config_path, "default", "region", "eu-west-1").unwrap();

        // Verify
        let loaded = load_ini_file(&config_path);
        assert_eq!(loaded["default"]["region"], "eu-west-1");
        assert_eq!(loaded["default"]["output"], "json"); // preserved
    }

    #[test]
    fn test_configure_set_credential_key() {
        let dir = tempfile::tempdir().unwrap();
        let aws_dir = dir.path().join(".aws");
        std::fs::create_dir_all(&aws_dir).unwrap();
        let creds_path = aws_dir.join("credentials");

        set_ini_value(&creds_path, "default", "aws_access_key_id", "AKIANEWKEY").unwrap();

        let loaded = load_ini_file(&creds_path);
        assert_eq!(loaded["default"]["aws_access_key_id"], "AKIANEWKEY");
    }

    #[test]
    fn test_configure_set_dotted_profile_key() {
        let dir = tempfile::tempdir().unwrap();
        let aws_dir = dir.path().join(".aws");
        std::fs::create_dir_all(&aws_dir).unwrap();
        let config_path = aws_dir.join("config");

        // Write via set_ini_value using config_section_name for a named profile
        set_ini_value(&config_path, &config_section_name("myprofile"), "region", "ap-southeast-1").unwrap();

        let loaded = load_ini_file(&config_path);
        assert_eq!(loaded["profile myprofile"]["region"], "ap-southeast-1");
    }

    #[test]
    fn test_configure_set_preserves_other_sections() {
        let dir = tempfile::tempdir().unwrap();
        let aws_dir = dir.path().join(".aws");
        std::fs::create_dir_all(&aws_dir).unwrap();
        let config_path = aws_dir.join("config");

        // Write initial data with two sections
        let mut data = IniData::new();
        let mut default_section = HashMap::new();
        default_section.insert("region".to_string(), "us-east-1".to_string());
        data.insert("default".to_string(), default_section);

        let mut profile_section = HashMap::new();
        profile_section.insert("region".to_string(), "eu-west-1".to_string());
        data.insert("profile other".to_string(), profile_section);
        write_ini_file(&config_path, &data).unwrap();

        // Set a value in default section
        set_ini_value(&config_path, "default", "output", "table").unwrap();

        // Verify both sections and all values
        let loaded = load_ini_file(&config_path);
        assert_eq!(loaded["default"]["region"], "us-east-1");
        assert_eq!(loaded["default"]["output"], "table");
        assert_eq!(loaded["profile other"]["region"], "eu-west-1");
    }

    #[test]
    fn test_configure_set_dotted_subsection_key() {
        let dir = tempfile::tempdir().unwrap();
        let aws_dir = dir.path().join(".aws");
        std::fs::create_dir_all(&aws_dir).unwrap();
        let config_path = aws_dir.join("config");

        // Write a subsection-style dotted key
        set_ini_value(&config_path, "default", "s3.max_concurrent_requests", "20").unwrap();

        let loaded = load_ini_file(&config_path);
        assert_eq!(loaded["default"]["s3.max_concurrent_requests"], "20");
    }

    #[test]
    fn test_configure_set_overwrites_existing_value() {
        let dir = tempfile::tempdir().unwrap();
        let aws_dir = dir.path().join(".aws");
        std::fs::create_dir_all(&aws_dir).unwrap();
        let config_path = aws_dir.join("config");

        set_ini_value(&config_path, "default", "region", "us-east-1").unwrap();
        set_ini_value(&config_path, "default", "region", "eu-west-1").unwrap();

        let loaded = load_ini_file(&config_path);
        assert_eq!(loaded["default"]["region"], "eu-west-1");
    }

    #[test]
    fn test_configure_set_creates_file_if_missing() {
        let dir = tempfile::tempdir().unwrap();
        let aws_dir = dir.path().join(".aws");
        std::fs::create_dir_all(&aws_dir).unwrap();
        let config_path = aws_dir.join("config");

        // File does not exist yet
        assert!(!config_path.exists());

        set_ini_value(&config_path, "default", "region", "us-west-2").unwrap();

        assert!(config_path.exists());
        let loaded = load_ini_file(&config_path);
        assert_eq!(loaded["default"]["region"], "us-west-2");
    }

    // --- configure list tests ---

    #[test]
    fn test_mask_credential_long_value() {
        assert_eq!(mask_credential("AKIAIOSFODNN7EXAMPLE"), "****************MPLE");
    }

    #[test]
    fn test_mask_credential_exactly_4_chars() {
        assert_eq!(mask_credential("ABCD"), "ABCD");
    }

    #[test]
    fn test_mask_credential_short_value() {
        assert_eq!(mask_credential("AB"), "AB");
    }

    #[test]
    fn test_mask_credential_empty() {
        assert_eq!(mask_credential(""), "");
    }

    #[test]
    fn test_format_configure_list_output_all_none() {
        let rows: Vec<(&str, Option<ConfigSource>)> = vec![
            ("profile", None),
            ("access_key", None),
            ("secret_key", None),
            ("region", None),
        ];
        let output = format_configure_list_output(&rows);
        // Check header
        assert!(output.contains("Name"));
        assert!(output.contains("Value"));
        assert!(output.contains("Type"));
        assert!(output.contains("Location"));
        // Check all rows show <not set>
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 6); // header + separator + 4 rows
        for line in &lines[2..] {
            assert!(line.contains("<not set>"));
            assert!(line.contains("None"));
        }
    }

    #[test]
    fn test_format_configure_list_output_with_values() {
        let rows: Vec<(&str, Option<ConfigSource>)> = vec![
            ("profile", Some(ConfigSource {
                value: "myprofile".to_string(),
                source_type: "manual".to_string(),
                location: "--profile".to_string(),
            })),
            ("access_key", Some(ConfigSource {
                value: "AKIAIOSFODNN7EXAMPLE".to_string(),
                source_type: "config-file".to_string(),
                location: "~/.aws/credentials".to_string(),
            })),
            ("secret_key", Some(ConfigSource {
                value: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                source_type: "config-file".to_string(),
                location: "~/.aws/credentials".to_string(),
            })),
            ("region", Some(ConfigSource {
                value: "us-west-2".to_string(),
                source_type: "config-file".to_string(),
                location: "~/.aws/config".to_string(),
            })),
        ];
        let output = format_configure_list_output(&rows);

        // access_key and secret_key should be masked
        assert!(output.contains("****************MPLE"));
        assert!(output.contains("****************EKEY"));
        // profile and region should NOT be masked
        assert!(output.contains("myprofile"));
        assert!(output.contains("us-west-2"));
        // Check source types
        assert!(output.contains("manual"));
        assert!(output.contains("config-file"));
        // Check locations
        assert!(output.contains("--profile"));
        assert!(output.contains("~/.aws/credentials"));
        assert!(output.contains("~/.aws/config"));
    }

    #[test]
    fn test_format_configure_list_column_alignment() {
        let rows: Vec<(&str, Option<ConfigSource>)> = vec![
            ("profile", None),
            ("access_key", Some(ConfigSource {
                value: "AKIAIOSFODNN7EXAMPLE".to_string(),
                source_type: "env".to_string(),
                location: "AWS_ACCESS_KEY_ID".to_string(),
            })),
        ];
        let output = format_configure_list_output(&rows);
        let lines: Vec<&str> = output.lines().collect();

        // Header line should have right-aligned Name at width 10
        assert!(lines[0].starts_with("      Name"));
        // profile row: "profile" is 7 chars, right-aligned to 10 -> "   profile"
        assert!(lines[2].contains("   profile"));
        // access_key row: "access_key" is 10 chars, right-aligned to 10 -> "access_key"
        assert!(lines[3].starts_with("access_key"));
    }

    #[test]
    fn test_resolve_profile_source_from_flag() {
        let source = resolve_profile_source("myprofile", true);
        assert!(source.is_some());
        let src = source.as_ref().map(|s| &s.source_type);
        assert_eq!(src, Some(&"manual".to_string()));
        let loc = source.as_ref().map(|s| &s.location);
        assert_eq!(loc, Some(&"--profile".to_string()));
    }

    #[test]
    fn test_resolve_profile_source_not_set() {
        // Temporarily unset AWS_PROFILE to test the "not set" path
        let saved = std::env::var("AWS_PROFILE").ok();
        std::env::remove_var("AWS_PROFILE");

        let source = resolve_profile_source("default", false);
        assert!(source.is_none());

        // Restore
        if let Some(val) = saved {
            std::env::set_var("AWS_PROFILE", val);
        }
    }

    #[test]
    fn test_configure_set_credential_vs_config_routing() {
        // Verify that run_configure_set routes credential keys to credentials
        // and config keys to config, using a temporary HOME directory
        let dir = tempfile::tempdir().unwrap();
        let aws_dir = dir.path().join(".aws");
        std::fs::create_dir_all(&aws_dir).unwrap();

        // Directly test the routing logic by calling set_ini_value as run_configure_set would
        let creds_path = aws_dir.join("credentials");
        let config_path = aws_dir.join("config");

        // Credential key -> credentials file
        let key = "aws_secret_access_key";
        assert!(CREDENTIAL_KEYS.contains(&key));
        set_ini_value(&creds_path, "default", key, "mysecret").unwrap();

        // Config key -> config file
        let key = "region";
        assert!(!CREDENTIAL_KEYS.contains(&key));
        set_ini_value(&config_path, "default", key, "us-west-2").unwrap();

        // Verify credentials file has only cred key
        let creds_loaded = load_ini_file(&creds_path);
        assert_eq!(creds_loaded["default"]["aws_secret_access_key"], "mysecret");
        assert!(creds_loaded["default"].get("region").is_none());

        // Verify config file has only config key
        let config_loaded = load_ini_file(&config_path);
        assert_eq!(config_loaded["default"]["region"], "us-west-2");
        assert!(config_loaded["default"].get("aws_secret_access_key").is_none());
    }

    // --- configure list-profiles tests ---

    #[test]
    fn test_collect_profile_names_empty() {
        let config = IniData::new();
        let creds = IniData::new();
        let profiles = collect_profile_names(&config, &creds);
        assert!(profiles.is_empty());
    }

    #[test]
    fn test_collect_profile_names_config_only() {
        let mut config = IniData::new();
        config.insert("default".to_string(), HashMap::new());
        config.insert("profile dev".to_string(), HashMap::new());
        config.insert("profile prod".to_string(), HashMap::new());
        let creds = IniData::new();

        let profiles = collect_profile_names(&config, &creds);
        assert_eq!(profiles, vec!["default", "dev", "prod"]);
    }

    #[test]
    fn test_collect_profile_names_creds_only() {
        let config = IniData::new();
        let mut creds = IniData::new();
        creds.insert("default".to_string(), HashMap::new());
        creds.insert("staging".to_string(), HashMap::new());

        let profiles = collect_profile_names(&config, &creds);
        assert_eq!(profiles, vec!["default", "staging"]);
    }

    #[test]
    fn test_collect_profile_names_deduplication() {
        let mut config = IniData::new();
        config.insert("default".to_string(), HashMap::new());
        config.insert("profile dev".to_string(), HashMap::new());

        let mut creds = IniData::new();
        creds.insert("default".to_string(), HashMap::new());
        creds.insert("dev".to_string(), HashMap::new());
        creds.insert("prod".to_string(), HashMap::new());

        let profiles = collect_profile_names(&config, &creds);
        assert_eq!(profiles, vec!["default", "dev", "prod"]);
    }

    #[test]
    fn test_collect_profile_names_sorted() {
        let mut config = IniData::new();
        config.insert("profile zebra".to_string(), HashMap::new());
        config.insert("profile alpha".to_string(), HashMap::new());

        let mut creds = IniData::new();
        creds.insert("middle".to_string(), HashMap::new());

        let profiles = collect_profile_names(&config, &creds);
        assert_eq!(profiles, vec!["alpha", "middle", "zebra"]);
    }

    #[test]
    fn test_collect_profile_names_ignores_non_profile_config_sections() {
        // Sections in config that don't start with "profile " and aren't "default"
        // should be ignored (e.g., plugin sections or other special sections).
        let mut config = IniData::new();
        config.insert("default".to_string(), HashMap::new());
        config.insert("profile myprofile".to_string(), HashMap::new());
        config.insert("sso-session mysession".to_string(), HashMap::new());
        let creds = IniData::new();

        let profiles = collect_profile_names(&config, &creds);
        assert_eq!(profiles, vec!["default", "myprofile"]);
    }

    // --- configure export-credentials tests ---
    // These tests use format_export_credentials directly to avoid env var races.

    #[test]
    fn test_format_export_credentials_env_format() {
        let result = format_export_credentials(
            "AKIATESTKEY123", "secretTestKey456", Some("testToken789"), "env"
        ).unwrap();
        assert!(result.contains("export AWS_ACCESS_KEY_ID=AKIATESTKEY123"));
        assert!(result.contains("export AWS_SECRET_ACCESS_KEY=secretTestKey456"));
        assert!(result.contains("export AWS_SESSION_TOKEN=testToken789"));
    }

    #[test]
    fn test_format_export_credentials_env_no_export_format() {
        let result = format_export_credentials(
            "AKIATEST", "secretTest", None, "env-no-export"
        ).unwrap();
        assert!(result.contains("AWS_ACCESS_KEY_ID=AKIATEST\n"));
        assert!(result.contains("AWS_SECRET_ACCESS_KEY=secretTest\n"));
        assert!(!result.contains("export"));
        assert!(!result.contains("SESSION_TOKEN"));
    }

    #[test]
    fn test_format_export_credentials_json_format() {
        let result = format_export_credentials(
            "AKIAJSON", "secretJson", Some("tokenJson"), "json"
        ).unwrap();
        assert!(result.contains("\"AccessKeyId\": \"AKIAJSON\""));
        assert!(result.contains("\"SecretAccessKey\": \"secretJson\""));
        assert!(result.contains("\"SessionToken\": \"tokenJson\""));
        assert!(result.starts_with('{'));
        assert!(result.trim_end().ends_with('}'));
    }

    #[test]
    fn test_format_export_credentials_json_no_token() {
        let result = format_export_credentials(
            "AKIANOTOKEN", "secretNoToken", None, "json"
        ).unwrap();
        assert!(result.contains("\"AccessKeyId\": \"AKIANOTOKEN\""));
        assert!(result.contains("\"SecretAccessKey\": \"secretNoToken\""));
        assert!(!result.contains("SessionToken"));
        // Verify valid JSON structure: no trailing comma before }
        assert!(!result.contains(",\n}"));
    }

    #[test]
    fn test_format_export_credentials_unknown_format() {
        let result = format_export_credentials("AKIATEST", "secret", None, "xml");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Unknown format 'xml'"));
    }

    #[test]
    fn test_export_credentials_no_credentials_found() {
        // Ensure env vars are not set
        let saved_ak = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let saved_sk = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        let saved_home = std::env::var("HOME").ok();

        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        // Point HOME to a temp dir with no .aws
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("HOME", dir.path());

        let result = build_export_credentials_output("nonexistent", "env");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Unable to locate credentials"));

        // Restore
        match saved_ak {
            Some(v) => std::env::set_var("AWS_ACCESS_KEY_ID", v),
            None => std::env::remove_var("AWS_ACCESS_KEY_ID"),
        }
        match saved_sk {
            Some(v) => std::env::set_var("AWS_SECRET_ACCESS_KEY", v),
            None => std::env::remove_var("AWS_SECRET_ACCESS_KEY"),
        }
        match saved_home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
    }
}
