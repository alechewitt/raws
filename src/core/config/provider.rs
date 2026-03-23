use anyhow::Result;
use std::path::PathBuf;

use super::loader;

pub struct ConfigProvider {
    pub region: Option<String>,
    pub output: Option<String>,
    pub profile: String,
}

impl ConfigProvider {
    pub fn new(
        cli_region: Option<&str>,
        cli_output: Option<&str>,
        cli_profile: Option<&str>,
    ) -> Result<Self> {
        let profile = Self::resolve_profile(cli_profile);
        let region = Self::resolve_region(cli_region, &profile)?;
        let output = Self::resolve_output(cli_output, &profile)?;

        Ok(Self {
            region,
            output,
            profile,
        })
    }

    fn resolve_profile(cli_profile: Option<&str>) -> String {
        if let Some(p) = cli_profile {
            return p.to_string();
        }
        if let Ok(p) = std::env::var("AWS_PROFILE") {
            return p;
        }
        "default".to_string()
    }

    fn resolve_region(cli_region: Option<&str>, profile: &str) -> Result<Option<String>> {
        if let Some(r) = cli_region {
            return Ok(Some(r.to_string()));
        }
        if let Ok(r) = std::env::var("AWS_REGION") {
            return Ok(Some(r));
        }
        if let Ok(r) = std::env::var("AWS_DEFAULT_REGION") {
            return Ok(Some(r));
        }
        // Try config file
        if let Some(r) = Self::get_config_value(profile, "region")? {
            return Ok(Some(r));
        }
        Ok(None)
    }

    fn resolve_output(cli_output: Option<&str>, profile: &str) -> Result<Option<String>> {
        if let Some(o) = cli_output {
            return Ok(Some(o.to_string()));
        }
        if let Ok(o) = std::env::var("AWS_DEFAULT_OUTPUT") {
            return Ok(Some(o));
        }
        if let Some(o) = Self::get_config_value(profile, "output")? {
            return Ok(Some(o));
        }
        Ok(None)
    }

    fn get_config_value(profile: &str, key: &str) -> Result<Option<String>> {
        let config_path = Self::config_file_path();
        if config_path.exists() {
            let config = loader::load_config_file(&config_path)?;
            if let Some(section) = config.get(profile) {
                if let Some(value) = section.get(key) {
                    return Ok(Some(value.clone()));
                }
            }
        }
        Ok(None)
    }

    fn config_file_path() -> PathBuf {
        if let Ok(p) = std::env::var("AWS_CONFIG_FILE") {
            return PathBuf::from(p);
        }
        let mut path = dirs_home();
        path.push(".aws");
        path.push("config");
        path
    }
}

fn dirs_home() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_selection_default() {
        // Clear env vars for isolated test
        std::env::remove_var("AWS_PROFILE");
        let provider = ConfigProvider::new(None, None, None).unwrap();
        assert_eq!(provider.profile, "default");
    }

    #[test]
    fn test_profile_selection_cli_override() {
        let provider = ConfigProvider::new(None, None, Some("my-profile")).unwrap();
        assert_eq!(provider.profile, "my-profile");
    }

    #[test]
    fn test_region_resolution_cli_override() {
        let provider = ConfigProvider::new(Some("eu-west-1"), None, None).unwrap();
        assert_eq!(provider.region, Some("eu-west-1".to_string()));
    }

    #[test]
    fn test_output_resolution_cli_override() {
        let provider = ConfigProvider::new(None, Some("table"), None).unwrap();
        assert_eq!(provider.output, Some("table".to_string()));
    }
}
