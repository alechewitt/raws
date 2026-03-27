use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    // Expose the Rust compiler version so the binary can print it at runtime.
    if let Ok(output) = std::process::Command::new("rustc").arg("--version").output() {
        if let Ok(version_str) = String::from_utf8(output.stdout) {
            // Format: "rustc 1.93.1 (01f6ddf75 2026-02-11)"  ->  extract "1.93.1"
            if let Some(ver) = version_str.split_whitespace().nth(1) {
                println!("cargo:rustc-env=RAWS_RUSTC_VERSION={ver}");
            }
        }
    }

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest = PathBuf::from(&out_dir).join("embedded_models");

    let models_dir = Path::new("models");
    if !models_dir.exists() {
        // Create an empty directory so include_dir! doesn't fail
        fs::create_dir_all(&dest).unwrap();
        println!("cargo:rerun-if-changed=models");
        return;
    }

    // Clean and recreate destination
    if dest.exists() {
        fs::remove_dir_all(&dest).unwrap();
    }
    fs::create_dir_all(&dest).unwrap();

    // Copy endpoints.json
    let endpoints_src = models_dir.join("endpoints.json");
    if endpoints_src.exists() {
        println!("cargo:rerun-if-changed={}", endpoints_src.display());
        fs::copy(&endpoints_src, dest.join("endpoints.json")).unwrap();
    } else {
        println!("cargo:warning=endpoints.json not found in models/ -- endpoint resolution will fall back to filesystem");
    }

    // Copy needed files for each service
    let mut entries: Vec<_> = fs::read_dir(models_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let service_name = entry.file_name();
        let service_dir = entry.path();

        // Find the latest version directory
        let version_dir = match find_latest_version(&service_dir) {
            Some(v) => v,
            None => continue,
        };

        let version_name = version_dir.file_name().unwrap();
        let dest_version_dir = dest.join(&service_name).join(version_name);
        fs::create_dir_all(&dest_version_dir).unwrap();

        // Copy only the files we need, tracking each for incremental rebuilds
        for filename in &["service-2.json", "paginators-1.json", "waiters-2.json"] {
            let src = version_dir.join(filename);
            if src.exists() {
                println!("cargo:rerun-if-changed={}", src.display());
                fs::copy(&src, dest_version_dir.join(filename)).unwrap();
            }
        }
    }

    // Also rebuild if the top-level models directory changes (new services added/removed)
    println!("cargo:rerun-if-changed=models");
}

fn find_latest_version(service_dir: &Path) -> Option<PathBuf> {
    let mut versions: Vec<_> = fs::read_dir(service_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    versions.sort_by_key(|e| std::cmp::Reverse(e.file_name()));

    for version in versions {
        if version.path().join("service-2.json").exists() {
            return Some(version.path());
        }
    }
    None
}
