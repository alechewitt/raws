//! S3 `sync` high-level command implementation.
//!
//! Handles `raws s3 sync <source> <destination>`:
//! - `raws s3 sync local-dir/ s3://bucket/prefix/` - upload new/changed files
//! - `raws s3 sync s3://bucket/prefix/ local-dir/` - download new/changed files
//! - `raws s3 sync s3://src/prefix/ s3://dst/prefix/` - sync between S3 locations
//!
//! Sync logic (matching AWS CLI behavior):
//! - Compare files by SIZE and LAST MODIFIED TIME
//! - A file needs sync if:
//!   - It exists in source but not in destination
//!   - It exists in both but size differs
//!   - It exists in both, same size, but source is newer
//! - With `--delete`: files in destination but not in source get deleted

use std::collections::HashMap;
use std::path::Path;

use anyhow::{bail, Context, Result};

use super::cp;
use super::is_s3_url;
use super::mv;
use super::parse_s3_url;
use super::s3_bucket_api_call;
use super::S3CommandContext;

/// Metadata about a file (either local or S3).
#[derive(Debug, Clone)]
struct FileInfo {
    /// File size in bytes.
    size: u64,
    /// Last modified time as seconds since Unix epoch.
    last_modified: i64,
}

/// An S3 object's metadata as parsed from ListObjectsV2 response.
#[derive(Debug, Clone)]
struct S3ObjectInfo {
    key: String,
    size: u64,
    last_modified: String,
}

/// Parsed page from a ListObjectsV2 response including size and last modified.
struct ListObjectsPage {
    objects: Vec<S3ObjectInfo>,
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

/// Execute the `s3 sync` command.
///
/// Parses the source and destination arguments, determines the sync direction,
/// and dispatches to the appropriate handler.
pub async fn execute(args: &[String], ctx: &S3CommandContext) -> Result<()> {
    let delete = args.iter().any(|a| a == "--delete");

    // Filter out flags to find positional arguments
    let positional: Vec<&String> = args.iter().filter(|a| !a.starts_with('-')).collect();

    if positional.len() < 2 {
        bail!(
            "s3 sync requires a source and destination argument.\n\
             Usage: raws s3 sync <source> <destination> [--delete]\n\
             Example: raws s3 sync . s3://bucket/prefix/"
        );
    }

    let source = positional[0].as_str();
    let destination = positional[1].as_str();

    let src_is_s3 = is_s3_url(source);
    let dst_is_s3 = is_s3_url(destination);

    match (src_is_s3, dst_is_s3) {
        (false, true) => sync_local_to_s3(source, destination, delete, ctx).await,
        (true, false) => sync_s3_to_local(source, destination, delete, ctx).await,
        (true, true) => sync_s3_to_s3(source, destination, delete, ctx).await,
        (false, false) => {
            bail!("At least one of the source or destination must be an S3 URL (s3://...)")
        }
    }
}

// ---------------------------------------------------------------------------
// Sync: local -> S3
// ---------------------------------------------------------------------------

/// Sync a local directory to an S3 prefix.
///
/// 1. Walk the local directory to build a map of {relative_path -> (size, mtime)}
/// 2. List S3 objects with prefix to build a map of {relative_key -> (size, last_modified)}
/// 3. Upload files that are new or changed
/// 4. With --delete: delete S3 objects not in local
async fn sync_local_to_s3(
    local_dir: &str,
    s3_url: &str,
    delete: bool,
    ctx: &S3CommandContext,
) -> Result<()> {
    let (bucket, prefix) = parse_s3_url(s3_url)?;
    let local_base = Path::new(local_dir);

    if !local_base.is_dir() {
        bail!(
            "Source path '{}' is not a directory. s3 sync requires a directory source.",
            local_dir
        );
    }

    // 1. Build local file map
    let local_map = build_local_file_map(local_base)?;

    // 2. Build S3 object map
    let s3_map = list_s3_objects_map(ctx, &bucket, &prefix).await?;

    // 3. Upload new/changed files
    for (relative_path, local_info) in &local_map {
        let needs_sync = match s3_map.get(relative_path) {
            None => true, // new file
            Some(s3_info) => file_needs_sync(local_info, s3_info),
        };

        if needs_sync {
            let local_file = local_base.join(relative_path);
            let local_str = local_file.to_string_lossy().to_string();
            let s3_key = format!("{}{}", prefix, relative_path);
            let s3_dest = format!("s3://{}/{}", bucket, s3_key);
            cp::upload_file(&local_str, &s3_dest, ctx).await?;
        }
    }

    // 4. Delete S3 objects not in local (if --delete)
    if delete {
        for relative_key in s3_map.keys() {
            if !local_map.contains_key(relative_key) {
                let s3_key = format!("{}{}", prefix, relative_key);
                mv::delete_s3_object(ctx, &bucket, &s3_key).await?;
                println!("delete: s3://{}/{}", bucket, s3_key);
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Sync: S3 -> local
// ---------------------------------------------------------------------------

/// Sync an S3 prefix to a local directory.
///
/// 1. List S3 objects to build source map
/// 2. Walk local directory to build destination map
/// 3. Download files that are new or changed
/// 4. With --delete: delete local files not in S3
async fn sync_s3_to_local(
    s3_url: &str,
    local_dir: &str,
    delete: bool,
    ctx: &S3CommandContext,
) -> Result<()> {
    let (bucket, prefix) = parse_s3_url(s3_url)?;
    let local_base = Path::new(local_dir);

    // Create the local directory if it doesn't exist
    if !local_base.exists() {
        std::fs::create_dir_all(local_base)
            .with_context(|| format!("Failed to create directory: {}", local_dir))?;
    }

    // 1. Build S3 object map (source)
    let s3_map = list_s3_objects_map(ctx, &bucket, &prefix).await?;

    // 2. Build local file map (destination)
    let local_map = build_local_file_map(local_base)?;

    // 3. Download new/changed files
    for (relative_key, s3_info) in &s3_map {
        let needs_sync = match local_map.get(relative_key) {
            None => true, // new file
            Some(local_info) => file_needs_sync(s3_info, local_info),
        };

        if needs_sync {
            let s3_obj_url = format!("s3://{}/{}{}", bucket, prefix, relative_key);
            let output_path = local_base.join(relative_key);

            // Create parent directories as needed
            if let Some(parent) = output_path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
            }

            let output_str = output_path.to_string_lossy().to_string();
            cp::download_file(&s3_obj_url, &output_str, ctx).await?;
        }
    }

    // 4. Delete local files not in S3 (if --delete)
    if delete {
        for relative_path in local_map.keys() {
            if !s3_map.contains_key(relative_path) {
                let file_path = local_base.join(relative_path);
                std::fs::remove_file(&file_path)
                    .with_context(|| format!("Failed to delete file: {}", file_path.display()))?;
                println!("delete: {}", file_path.display());
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Sync: S3 -> S3
// ---------------------------------------------------------------------------

/// Sync one S3 prefix to another.
///
/// 1. List source objects
/// 2. List destination objects
/// 3. Copy new/changed objects
/// 4. With --delete: delete destination objects not in source
async fn sync_s3_to_s3(
    src_url: &str,
    dst_url: &str,
    delete: bool,
    ctx: &S3CommandContext,
) -> Result<()> {
    let (src_bucket, src_prefix) = parse_s3_url(src_url)?;
    let (dst_bucket, dst_prefix) = parse_s3_url(dst_url)?;

    // 1. Build source S3 object map
    let src_map = list_s3_objects_map(ctx, &src_bucket, &src_prefix).await?;

    // 2. Build destination S3 object map
    let dst_map = list_s3_objects_map(ctx, &dst_bucket, &dst_prefix).await?;

    // 3. Copy new/changed objects
    for (relative_key, src_info) in &src_map {
        let needs_sync = match dst_map.get(relative_key) {
            None => true,
            Some(dst_info) => file_needs_sync(src_info, dst_info),
        };

        if needs_sync {
            let src_obj_url = format!("s3://{}/{}{}", src_bucket, src_prefix, relative_key);
            let dst_obj_url = format!("s3://{}/{}{}", dst_bucket, dst_prefix, relative_key);
            cp::copy_s3_to_s3(&src_obj_url, &dst_obj_url, ctx).await?;
        }
    }

    // 4. Delete destination objects not in source (if --delete)
    if delete {
        for relative_key in dst_map.keys() {
            if !src_map.contains_key(relative_key) {
                let dst_key = format!("{}{}", dst_prefix, relative_key);
                mv::delete_s3_object(ctx, &dst_bucket, &dst_key).await?;
                println!("delete: s3://{}/{}", dst_bucket, dst_key);
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Comparison logic
// ---------------------------------------------------------------------------

/// Determine if a file needs syncing from source to destination.
///
/// A file needs sync if:
/// - Size differs between source and destination
/// - Same size, but source is newer than destination
fn file_needs_sync(source: &FileInfo, destination: &FileInfo) -> bool {
    if source.size != destination.size {
        return true;
    }
    // Same size: sync only if source is strictly newer
    source.last_modified > destination.last_modified
}

// ---------------------------------------------------------------------------
// Local file map builder
// ---------------------------------------------------------------------------

/// Build a map of relative file paths to their metadata for a local directory.
///
/// Walks the directory tree recursively, collecting size and modification time
/// for each file. The map keys use forward slashes as separators (matching S3 convention).
fn build_local_file_map(base_dir: &Path) -> Result<HashMap<String, FileInfo>> {
    let mut map = HashMap::new();
    collect_local_files(base_dir, base_dir, &mut map)?;
    Ok(map)
}

/// Recursively collect local file metadata into the map.
fn collect_local_files(
    base_dir: &Path,
    current_dir: &Path,
    map: &mut HashMap<String, FileInfo>,
) -> Result<()> {
    let entries = std::fs::read_dir(current_dir)
        .with_context(|| format!("Failed to read directory: {}", current_dir.display()))?;

    for entry in entries {
        let entry = entry.with_context(|| {
            format!(
                "Failed to read directory entry in: {}",
                current_dir.display()
            )
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_local_files(base_dir, &path, map)?;
        } else {
            let metadata = std::fs::metadata(&path)
                .with_context(|| format!("Failed to read metadata: {}", path.display()))?;

            let relative = path
                .strip_prefix(base_dir)
                .with_context(|| {
                    format!(
                        "Failed to compute relative path for {}",
                        path.display()
                    )
                })?;

            let relative_str = relative
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Non-UTF-8 file path: {}", relative.display()))?;
            // Normalize to forward slashes for S3 key compatibility
            let normalized = relative_str.replace('\\', "/");

            let mtime = metadata
                .modified()
                .with_context(|| format!("Failed to get mtime for {}", path.display()))?;

            let mtime_secs = mtime
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            map.insert(
                normalized,
                FileInfo {
                    size: metadata.len(),
                    last_modified: mtime_secs,
                },
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// S3 object map builder
// ---------------------------------------------------------------------------

/// List all S3 objects under a prefix and build a map of relative keys to metadata.
///
/// Uses ListObjectsV2 with pagination to enumerate all objects. The relative key
/// is computed by stripping the prefix from each object's key.
async fn list_s3_objects_map(
    ctx: &S3CommandContext,
    bucket: &str,
    prefix: &str,
) -> Result<HashMap<String, FileInfo>> {
    let mut map = HashMap::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut params: Vec<(&str, &str)> = vec![("list-type", "2")];
        if !prefix.is_empty() {
            params.push(("prefix", prefix));
        }

        let token_string;
        if let Some(ref token) = continuation_token {
            token_string = token.clone();
            params.push(("continuation-token", &token_string));
        }

        let response =
            s3_bucket_api_call(ctx, bucket, "GET", "/", &params, None, &[]).await?;

        if response.status >= 300 {
            let body = response.body_string();
            bail!(
                "ListObjectsV2 failed (HTTP {}): {}",
                response.status,
                cp::extract_s3_error(&body)
            );
        }

        let body = response.body_string();
        let page = parse_list_objects_with_metadata(&body)?;

        for obj in &page.objects {
            // Skip directory markers
            if obj.key.ends_with('/') {
                continue;
            }

            // Compute relative key by stripping prefix
            let relative = obj.key.strip_prefix(prefix).unwrap_or(&obj.key);
            if relative.is_empty() {
                continue;
            }

            let mtime_secs = parse_s3_timestamp(&obj.last_modified);

            map.insert(
                relative.to_string(),
                FileInfo {
                    size: obj.size,
                    last_modified: mtime_secs,
                },
            );
        }

        if page.is_truncated {
            if let Some(token) = page.next_continuation_token {
                continuation_token = Some(token);
            } else {
                break;
            }
        } else {
            break;
        }
    }

    Ok(map)
}

// ---------------------------------------------------------------------------
// S3 timestamp parsing
// ---------------------------------------------------------------------------

/// Parse an S3 ISO 8601 timestamp string into seconds since Unix epoch.
///
/// Handles formats like:
/// - `2023-01-15T10:30:45.000Z`
/// - `2023-01-15T10:30:45Z`
fn parse_s3_timestamp(iso_date: &str) -> i64 {
    use chrono::{DateTime, Utc};

    // Try RFC 3339 first (handles "2023-01-15T10:30:45.000Z")
    if let Ok(dt) = DateTime::parse_from_rfc3339(iso_date) {
        return dt.timestamp();
    }

    // Try without timezone suffix
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(
        iso_date.trim_end_matches('Z'),
        "%Y-%m-%dT%H:%M:%S%.f",
    ) {
        let utc = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
        return utc.timestamp();
    }

    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(
        iso_date.trim_end_matches('Z'),
        "%Y-%m-%dT%H:%M:%S",
    ) {
        let utc = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
        return utc.timestamp();
    }

    0
}

// ---------------------------------------------------------------------------
// ListObjectsV2 XML parser (with Size and LastModified)
// ---------------------------------------------------------------------------

/// Parse a ListObjectsV2 XML response, extracting key, size, and last modified
/// for each object plus pagination info.
fn parse_list_objects_with_metadata(xml_body: &str) -> Result<ListObjectsPage> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml_body);
    let mut objects = Vec::new();
    let mut is_truncated = false;
    let mut next_continuation_token: Option<String> = None;

    let mut in_contents = false;
    let mut current_tag = String::new();
    let mut top_level_tag = String::new();

    // Current object being built
    let mut current_key: Option<String> = None;
    let mut current_size: Option<u64> = None;
    let mut current_last_modified: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let tag = strip_ns(std::str::from_utf8(e.name().as_ref()).unwrap_or(""));
                match tag.as_str() {
                    "Contents" => {
                        in_contents = true;
                        current_key = None;
                        current_size = None;
                        current_last_modified = None;
                    }
                    _ => {
                        if in_contents {
                            current_tag = tag;
                        } else {
                            top_level_tag = tag;
                        }
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let tag = strip_ns(std::str::from_utf8(e.name().as_ref()).unwrap_or(""));
                if tag == "Contents" && in_contents {
                    if let Some(key) = current_key.take() {
                        objects.push(S3ObjectInfo {
                            key,
                            size: current_size.unwrap_or(0),
                            last_modified: current_last_modified
                                .take()
                                .unwrap_or_default(),
                        });
                    }
                    in_contents = false;
                }
                current_tag.clear();
                top_level_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let text = e.unescape().unwrap_or_default().to_string();
                if in_contents {
                    match current_tag.as_str() {
                        "Key" => current_key = Some(text),
                        "Size" => current_size = text.parse::<u64>().ok(),
                        "LastModified" => current_last_modified = Some(text),
                        _ => {}
                    }
                } else {
                    match top_level_tag.as_str() {
                        "IsTruncated" => {
                            is_truncated = text == "true";
                        }
                        "NextContinuationToken" => {
                            next_continuation_token = Some(text);
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => bail!("Failed to parse ListObjectsV2 XML response: {}", e),
        }
    }

    Ok(ListObjectsPage {
        objects,
        is_truncated,
        next_continuation_token,
    })
}

/// Strip XML namespace prefix from a tag name.
fn strip_ns(tag: &str) -> String {
    if let Some(pos) = tag.rfind('}') {
        return tag[pos + 1..].to_string();
    }
    if let Some(pos) = tag.rfind(':') {
        return tag[pos + 1..].to_string();
    }
    tag.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::credentials::Credentials;

    /// Helper to create a dummy S3CommandContext for testing.
    fn dummy_context() -> S3CommandContext {
        S3CommandContext {
            region: "us-east-1".to_string(),
            credentials: Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            endpoint_url: "https://s3.us-east-1.amazonaws.com".to_string(),
            output_format: "json".to_string(),
            debug: false,

            no_sign_request: false,
        }
    }

    // ---------------------------------------------------------------
    // Argument validation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_sync_no_args_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(execute(&[], &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("s3 sync requires a source and destination"),
        );
    }

    #[test]
    fn test_sync_one_arg_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec!["s3://bucket/prefix/".to_string()];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("s3 sync requires a source and destination"),
        );
    }

    #[test]
    fn test_sync_both_local_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec!["dir1/".to_string(), "dir2/".to_string()];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("At least one of the source or destination must be an S3 URL"),
        );
    }

    #[test]
    fn test_sync_local_to_s3_not_a_dir_errors() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec![
            "/tmp/raws_nonexistent_sync_dir_xyzzy/".to_string(),
            "s3://bucket/prefix/".to_string(),
        ];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("is not a directory"),
        );
    }

    #[test]
    fn test_sync_flags_are_filtered() {
        let ctx = dummy_context();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = vec!["--delete".to_string(), "s3://bucket/".to_string()];
        let result = rt.block_on(execute(&args, &ctx));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("s3 sync requires a source and destination"),
        );
    }

    // ---------------------------------------------------------------
    // file_needs_sync tests
    // ---------------------------------------------------------------

    #[test]
    fn test_file_needs_sync_new_file() {
        // If the destination doesn't have the file, it always needs sync
        // (This is handled by the None case in the calling code, not this function)
        // But we can test when sizes differ
        let src = FileInfo {
            size: 100,
            last_modified: 1000,
        };
        let dst = FileInfo {
            size: 50,
            last_modified: 1000,
        };
        assert!(file_needs_sync(&src, &dst));
    }

    #[test]
    fn test_file_needs_sync_same_size_source_newer() {
        let src = FileInfo {
            size: 100,
            last_modified: 2000,
        };
        let dst = FileInfo {
            size: 100,
            last_modified: 1000,
        };
        assert!(file_needs_sync(&src, &dst));
    }

    #[test]
    fn test_file_needs_sync_same_size_destination_newer() {
        let src = FileInfo {
            size: 100,
            last_modified: 1000,
        };
        let dst = FileInfo {
            size: 100,
            last_modified: 2000,
        };
        assert!(!file_needs_sync(&src, &dst));
    }

    #[test]
    fn test_file_needs_sync_same_size_same_time() {
        let src = FileInfo {
            size: 100,
            last_modified: 1000,
        };
        let dst = FileInfo {
            size: 100,
            last_modified: 1000,
        };
        assert!(!file_needs_sync(&src, &dst));
    }

    #[test]
    fn test_file_needs_sync_different_size_source_older() {
        // Even if source is older, different size means sync needed
        let src = FileInfo {
            size: 200,
            last_modified: 500,
        };
        let dst = FileInfo {
            size: 100,
            last_modified: 1000,
        };
        assert!(file_needs_sync(&src, &dst));
    }

    #[test]
    fn test_file_needs_sync_zero_sizes() {
        let src = FileInfo {
            size: 0,
            last_modified: 1000,
        };
        let dst = FileInfo {
            size: 0,
            last_modified: 1000,
        };
        assert!(!file_needs_sync(&src, &dst));
    }

    // ---------------------------------------------------------------
    // parse_s3_timestamp tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_s3_timestamp_with_millis() {
        let ts = parse_s3_timestamp("2023-01-15T10:30:45.000Z");
        // 2023-01-15T10:30:45Z -> known epoch value
        assert!(ts > 0);
        // Verify it's in the right ballpark (2023)
        assert!(ts > 1_670_000_000); // Dec 2022
        assert!(ts < 1_710_000_000); // Mar 2024
    }

    #[test]
    fn test_parse_s3_timestamp_without_millis() {
        let ts = parse_s3_timestamp("2023-01-15T10:30:45Z");
        assert!(ts > 0);
        assert!(ts > 1_670_000_000);
        assert!(ts < 1_710_000_000);
    }

    #[test]
    fn test_parse_s3_timestamp_exact_value() {
        // 2020-01-01T00:00:00Z = 1577836800
        let ts = parse_s3_timestamp("2020-01-01T00:00:00.000Z");
        assert_eq!(ts, 1577836800);
    }

    #[test]
    fn test_parse_s3_timestamp_invalid() {
        let ts = parse_s3_timestamp("not-a-date");
        assert_eq!(ts, 0);
    }

    #[test]
    fn test_parse_s3_timestamp_empty() {
        let ts = parse_s3_timestamp("");
        assert_eq!(ts, 0);
    }

    // ---------------------------------------------------------------
    // parse_list_objects_with_metadata tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_list_objects_typical() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>photos/cat.jpg</Key>
    <LastModified>2023-01-15T10:30:45.000Z</LastModified>
    <Size>12345</Size>
  </Contents>
  <Contents>
    <Key>photos/dog.jpg</Key>
    <LastModified>2023-06-20T14:22:10.000Z</LastModified>
    <Size>678</Size>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_objects_with_metadata(xml).unwrap();
        assert_eq!(page.objects.len(), 2);
        assert_eq!(page.objects[0].key, "photos/cat.jpg");
        assert_eq!(page.objects[0].size, 12345);
        assert_eq!(
            page.objects[0].last_modified,
            "2023-01-15T10:30:45.000Z"
        );
        assert_eq!(page.objects[1].key, "photos/dog.jpg");
        assert_eq!(page.objects[1].size, 678);
        assert!(!page.is_truncated);
        assert!(page.next_continuation_token.is_none());
    }

    #[test]
    fn test_parse_list_objects_truncated() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>abc123token</NextContinuationToken>
  <Contents>
    <Key>file1.txt</Key>
    <LastModified>2023-01-15T10:30:45.000Z</LastModified>
    <Size>100</Size>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_objects_with_metadata(xml).unwrap();
        assert!(page.is_truncated);
        assert_eq!(
            page.next_continuation_token,
            Some("abc123token".to_string())
        );
        assert_eq!(page.objects.len(), 1);
    }

    #[test]
    fn test_parse_list_objects_empty() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>"#;

        let page = parse_list_objects_with_metadata(xml).unwrap();
        assert_eq!(page.objects.len(), 0);
        assert!(!page.is_truncated);
    }

    #[test]
    fn test_parse_list_objects_with_namespace() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>test.txt</Key>
    <LastModified>2023-01-01T00:00:00.000Z</LastModified>
    <Size>42</Size>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_objects_with_metadata(xml).unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, "test.txt");
        assert_eq!(page.objects[0].size, 42);
    }

    #[test]
    fn test_parse_list_objects_missing_size() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>file.txt</Key>
    <LastModified>2023-01-01T00:00:00.000Z</LastModified>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_objects_with_metadata(xml).unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].size, 0);
    }

    #[test]
    fn test_parse_list_objects_missing_last_modified() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>file.txt</Key>
    <Size>100</Size>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_objects_with_metadata(xml).unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].last_modified, "");
    }

    // ---------------------------------------------------------------
    // strip_ns tests
    // ---------------------------------------------------------------

    #[test]
    fn test_strip_ns_with_braces() {
        assert_eq!(
            strip_ns("{http://s3.amazonaws.com/doc/2006-03-01/}Key"),
            "Key"
        );
    }

    #[test]
    fn test_strip_ns_no_namespace() {
        assert_eq!(strip_ns("Key"), "Key");
    }

    #[test]
    fn test_strip_ns_with_colon() {
        assert_eq!(strip_ns("s3:Key"), "Key");
    }

    // ---------------------------------------------------------------
    // build_local_file_map tests
    // ---------------------------------------------------------------

    #[test]
    fn test_build_local_file_map_basic() {
        let temp_dir = std::env::temp_dir().join("raws_test_sync_local_map");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(temp_dir.join("sub")).unwrap();
        std::fs::write(temp_dir.join("a.txt"), "hello").unwrap();
        std::fs::write(temp_dir.join("sub/b.txt"), "world!!").unwrap();

        let map = build_local_file_map(&temp_dir).unwrap();

        assert_eq!(map.len(), 2);
        assert!(map.contains_key("a.txt"));
        assert!(map.contains_key("sub/b.txt"));
        assert_eq!(map["a.txt"].size, 5); // "hello" = 5 bytes
        assert_eq!(map["sub/b.txt"].size, 7); // "world!!" = 7 bytes
        assert!(map["a.txt"].last_modified > 0);

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_build_local_file_map_empty_dir() {
        let temp_dir = std::env::temp_dir().join("raws_test_sync_local_map_empty");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let map = build_local_file_map(&temp_dir).unwrap();
        assert!(map.is_empty());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_build_local_file_map_nested() {
        let temp_dir = std::env::temp_dir().join("raws_test_sync_local_map_nested");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(temp_dir.join("a/b/c")).unwrap();
        std::fs::write(temp_dir.join("a/b/c/deep.txt"), "deep").unwrap();
        std::fs::write(temp_dir.join("a/top.txt"), "top").unwrap();

        let map = build_local_file_map(&temp_dir).unwrap();
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("a/b/c/deep.txt"));
        assert!(map.contains_key("a/top.txt"));
        assert_eq!(map["a/b/c/deep.txt"].size, 4);
        assert_eq!(map["a/top.txt"].size, 3);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_build_local_file_map_nonexistent_dir() {
        let result = build_local_file_map(Path::new("/tmp/raws_nonexistent_sync_dir_xyzzy"));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to read directory")
        );
    }

    // ---------------------------------------------------------------
    // Integration-style sync comparison tests
    // ---------------------------------------------------------------

    #[test]
    fn test_sync_comparison_new_files_only() {
        // Simulate: source has files A, B; destination has nothing
        let mut source = HashMap::new();
        source.insert(
            "a.txt".to_string(),
            FileInfo {
                size: 100,
                last_modified: 1000,
            },
        );
        source.insert(
            "b.txt".to_string(),
            FileInfo {
                size: 200,
                last_modified: 2000,
            },
        );
        let destination: HashMap<String, FileInfo> = HashMap::new();

        let mut to_sync = Vec::new();
        for (key, src_info) in &source {
            let needs_sync = match destination.get(key) {
                None => true,
                Some(dst_info) => file_needs_sync(src_info, dst_info),
            };
            if needs_sync {
                to_sync.push(key.clone());
            }
        }
        to_sync.sort();

        assert_eq!(to_sync, vec!["a.txt", "b.txt"]);
    }

    #[test]
    fn test_sync_comparison_mixed() {
        // source has A (changed), B (unchanged), C (new)
        // destination has A (old version), B (same), D (orphan)
        let mut source = HashMap::new();
        source.insert(
            "a.txt".to_string(),
            FileInfo {
                size: 150,
                last_modified: 2000,
            },
        );
        source.insert(
            "b.txt".to_string(),
            FileInfo {
                size: 200,
                last_modified: 1000,
            },
        );
        source.insert(
            "c.txt".to_string(),
            FileInfo {
                size: 300,
                last_modified: 3000,
            },
        );

        let mut destination = HashMap::new();
        destination.insert(
            "a.txt".to_string(),
            FileInfo {
                size: 100,
                last_modified: 1000,
            },
        ); // different size
        destination.insert(
            "b.txt".to_string(),
            FileInfo {
                size: 200,
                last_modified: 1000,
            },
        ); // same
        destination.insert(
            "d.txt".to_string(),
            FileInfo {
                size: 400,
                last_modified: 500,
            },
        ); // orphan

        // Files to sync (upload/copy)
        let mut to_sync = Vec::new();
        for (key, src_info) in &source {
            let needs_sync = match destination.get(key) {
                None => true,
                Some(dst_info) => file_needs_sync(src_info, dst_info),
            };
            if needs_sync {
                to_sync.push(key.clone());
            }
        }
        to_sync.sort();
        assert_eq!(to_sync, vec!["a.txt", "c.txt"]);

        // Files to delete (with --delete)
        let mut to_delete = Vec::new();
        for key in destination.keys() {
            if !source.contains_key(key) {
                to_delete.push(key.clone());
            }
        }
        to_delete.sort();
        assert_eq!(to_delete, vec!["d.txt"]);
    }

    #[test]
    fn test_sync_comparison_nothing_to_do() {
        // Both sides have same files with same sizes and timestamps
        let mut source = HashMap::new();
        source.insert(
            "a.txt".to_string(),
            FileInfo {
                size: 100,
                last_modified: 1000,
            },
        );
        source.insert(
            "b.txt".to_string(),
            FileInfo {
                size: 200,
                last_modified: 2000,
            },
        );

        let destination = source.clone();

        let mut to_sync = Vec::new();
        for (key, src_info) in &source {
            let needs_sync = match destination.get(key) {
                None => true,
                Some(dst_info) => file_needs_sync(src_info, dst_info),
            };
            if needs_sync {
                to_sync.push(key.clone());
            }
        }
        assert!(to_sync.is_empty());
    }

    #[test]
    fn test_sync_delete_flag_identification() {
        // Verify that the --delete flag is correctly detected from args
        let args_with_delete = vec![
            ".".to_string(),
            "s3://bucket/".to_string(),
            "--delete".to_string(),
        ];
        assert!(args_with_delete.iter().any(|a| a == "--delete"));

        let args_without_delete = vec![".".to_string(), "s3://bucket/".to_string()];
        assert!(!args_without_delete.iter().any(|a| a == "--delete"));
    }
}
