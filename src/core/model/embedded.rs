use include_dir::{include_dir, Dir};

static EMBEDDED_MODELS: Dir<'static> = include_dir!("$OUT_DIR/embedded_models");

pub fn list_services() -> Vec<String> {
    let mut services: Vec<String> = EMBEDDED_MODELS
        .dirs()
        .map(|d| d.path().file_name().unwrap().to_string_lossy().to_string())
        .collect();
    services.sort();
    services
}

pub fn get_service_model(service: &str) -> Option<&'static str> {
    let service_dir = EMBEDDED_MODELS.get_dir(service)?;
    // Find the first (only) version subdirectory containing service-2.json
    for version_dir in service_dir.dirs() {
        if let Some(file) = version_dir.get_file(version_dir.path().join("service-2.json")) {
            return file.contents_utf8();
        }
    }
    None
}

pub fn get_paginators(service: &str) -> Option<&'static str> {
    let service_dir = EMBEDDED_MODELS.get_dir(service)?;
    for version_dir in service_dir.dirs() {
        if let Some(file) = version_dir.get_file(version_dir.path().join("paginators-1.json")) {
            return file.contents_utf8();
        }
    }
    None
}

pub fn get_waiters(service: &str) -> Option<&'static str> {
    let service_dir = EMBEDDED_MODELS.get_dir(service)?;
    for version_dir in service_dir.dirs() {
        if let Some(file) = version_dir.get_file(version_dir.path().join("waiters-2.json")) {
            return file.contents_utf8();
        }
    }
    None
}

pub fn get_endpoints() -> Option<&'static str> {
    EMBEDDED_MODELS
        .get_file("endpoints.json")
        .and_then(|f| f.contents_utf8())
}
