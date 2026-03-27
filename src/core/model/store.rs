use anyhow::{bail, Context, Result};
use std::borrow::Cow;
use std::path::{Path, PathBuf};

use super::loader;

/// Check if a filesystem override is configured via RAWS_MODELS_DIR env var.
fn fs_override_dir() -> Option<PathBuf> {
    std::env::var("RAWS_MODELS_DIR").ok().map(PathBuf::from)
}

/// Get the service model JSON string for a given service name.
pub fn get_service_model_str(service: &str) -> Result<Cow<'static, str>> {
    // Check filesystem override first
    if let Some(models_dir) = fs_override_dir() {
        let service_dir = models_dir.join(service);
        let model_path = loader::find_service_model(&service_dir)
            .ok_or_else(|| anyhow::anyhow!(
                "Service model not found for '{}' in {}",
                service, models_dir.display()
            ))?;
        let content = std::fs::read_to_string(&model_path)
            .with_context(|| format!("Failed to read service model: {}", model_path.display()))?;
        return Ok(Cow::Owned(content));
    }

    // Try embedded models
    #[cfg(feature = "embed-models")]
    {
        if let Some(content) = super::embedded::get_service_model(service) {
            return Ok(Cow::Borrowed(content));
        }
    }

    // Fall back to filesystem (for non-embedded builds or missing embedded data)
    let models_dir = Path::new("models").join(service);
    let model_path = loader::find_service_model(&models_dir)
        .ok_or_else(|| anyhow::anyhow!(
            "Service model not found for '{}'. Check that models/{} exists.",
            service, service
        ))?;
    let content = std::fs::read_to_string(&model_path)
        .with_context(|| format!("Failed to read service model: {}", model_path.display()))?;
    Ok(Cow::Owned(content))
}

/// Get the paginators JSON string for a given service name.
pub fn get_paginators_str(service: &str) -> Option<Cow<'static, str>> {
    // Check filesystem override first
    if let Some(models_dir) = fs_override_dir() {
        let service_dir = models_dir.join(service);
        if let Some(model_path) = loader::find_service_model(&service_dir) {
            let version_dir = model_path.parent()?;
            let paginators_path = version_dir.join("paginators-1.json");
            if paginators_path.exists() {
                return std::fs::read_to_string(&paginators_path).ok().map(Cow::Owned);
            }
        }
        return None;
    }

    // Try embedded models
    #[cfg(feature = "embed-models")]
    {
        if let Some(content) = super::embedded::get_paginators(service) {
            return Some(Cow::Borrowed(content));
        }
    }

    // Fall back to filesystem
    let service_dir = Path::new("models").join(service);
    let model_path = loader::find_service_model(&service_dir)?;
    let version_dir = model_path.parent()?;
    let paginators_path = version_dir.join("paginators-1.json");
    if paginators_path.exists() {
        return std::fs::read_to_string(&paginators_path).ok().map(Cow::Owned);
    }
    None
}

/// Get the waiters JSON string for a given service name.
pub fn get_waiters_str(service: &str) -> Option<Cow<'static, str>> {
    // Check filesystem override first
    if let Some(models_dir) = fs_override_dir() {
        let service_dir = models_dir.join(service);
        if let Some(model_path) = loader::find_service_model(&service_dir) {
            let version_dir = model_path.parent()?;
            let waiters_path = version_dir.join("waiters-2.json");
            if waiters_path.exists() {
                return std::fs::read_to_string(&waiters_path).ok().map(Cow::Owned);
            }
        }
        return None;
    }

    // Try embedded models
    #[cfg(feature = "embed-models")]
    {
        if let Some(content) = super::embedded::get_waiters(service) {
            return Some(Cow::Borrowed(content));
        }
    }

    // Fall back to filesystem
    let service_dir = Path::new("models").join(service);
    let model_path = loader::find_service_model(&service_dir)?;
    let version_dir = model_path.parent()?;
    let waiters_path = version_dir.join("waiters-2.json");
    if waiters_path.exists() {
        return std::fs::read_to_string(&waiters_path).ok().map(Cow::Owned);
    }
    None
}

/// Get the endpoints.json content.
pub fn get_endpoints_str() -> Result<Cow<'static, str>> {
    // Check filesystem override first
    if let Some(models_dir) = fs_override_dir() {
        let path = models_dir.join("endpoints.json");
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read endpoints file: {}", path.display()))?;
        return Ok(Cow::Owned(content));
    }

    // Try embedded models
    #[cfg(feature = "embed-models")]
    {
        if let Some(content) = super::embedded::get_endpoints() {
            return Ok(Cow::Borrowed(content));
        }
    }

    // Fall back to filesystem
    let path = Path::new("models/endpoints.json");
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read endpoints file: {}", path.display()))?;
    Ok(Cow::Owned(content))
}

/// Discover all available service names.
pub fn discover_services() -> Result<Vec<String>> {
    // Check filesystem override first
    if let Some(models_dir) = fs_override_dir() {
        return loader::discover_services(&models_dir);
    }

    // Try embedded models
    #[cfg(feature = "embed-models")]
    {
        let services = super::embedded::list_services();
        if !services.is_empty() {
            return Ok(services);
        }
    }

    // Fall back to filesystem
    let models_dir = Path::new("models");
    if !models_dir.exists() {
        bail!("Models directory not found and no embedded models available");
    }
    loader::discover_services(models_dir)
}
