//! Multipart upload support for S3.
//!
//! Files larger than `MULTIPART_THRESHOLD` (8 MB) are uploaded using the
//! S3 multipart upload API:
//!   1. CreateMultipartUpload  -> returns an UploadId
//!   2. UploadPart (concurrent) -> each returns an ETag
//!   3. CompleteMultipartUpload -> finalizes the object
//!
//! If any part upload fails, AbortMultipartUpload is called to clean up.

use anyhow::{bail, Context, Result};
use std::sync::Arc;
use tokio::sync::Semaphore;

use super::cp::extract_s3_error;
use super::{s3_bucket_api_call, S3CommandContext};

/// Maximum retries for a single part upload.
const PART_UPLOAD_MAX_RETRIES: u32 = 3;

/// Base delay for part upload retry (milliseconds).
const PART_RETRY_BASE_DELAY_MS: u64 = 500;

/// Files at or above this size use multipart upload (8 MB).
pub const MULTIPART_THRESHOLD: u64 = 8 * 1024 * 1024;

/// Size of each upload part (8 MB).
pub const PART_SIZE: usize = 8 * 1024 * 1024;

/// Maximum number of concurrent part uploads.
const MAX_CONCURRENCY: usize = 10;

/// A completed part with its part number and ETag, needed for CompleteMultipartUpload.
#[derive(Debug, Clone)]
struct CompletedPart {
    part_number: usize,
    etag: String,
}

/// Perform a multipart upload of `data` to `s3://{bucket}/{key}`.
///
/// Assumes `data.len() >= MULTIPART_THRESHOLD`. The caller should fall back to
/// a simple PutObject for smaller files.
pub async fn multipart_upload(
    ctx: &S3CommandContext,
    bucket: &str,
    key: &str,
    data: Vec<u8>,
) -> Result<()> {
    // Step 1: Initiate multipart upload
    let upload_id = create_multipart_upload(ctx, bucket, key).await?;

    // Step 2: Upload parts concurrently
    match upload_parts(ctx, bucket, key, &upload_id, &data).await {
        Ok(completed_parts) => {
            // Step 3: Complete the multipart upload
            complete_multipart_upload(ctx, bucket, key, &upload_id, &completed_parts).await
        }
        Err(e) => {
            // On failure, attempt to abort
            if ctx.debug {
                eprintln!("[debug] multipart upload failed, aborting: {}", e);
            }
            let _ = abort_multipart_upload(ctx, bucket, key, &upload_id).await;
            Err(e)
        }
    }
}

/// Initiate a multipart upload and return the UploadId.
async fn create_multipart_upload(
    ctx: &S3CommandContext,
    bucket: &str,
    key: &str,
) -> Result<String> {
    let uri_path = format!("/{key}");
    let query_params: Vec<(&str, &str)> = vec![("uploads", "")];

    let response = s3_bucket_api_call(ctx, bucket, "POST", &uri_path, &query_params, None, &[])
        .await
        .context("CreateMultipartUpload request failed")?;

    if response.status >= 300 {
        let body = response.body_string();
        bail!(
            "CreateMultipartUpload failed (HTTP {}): {}",
            response.status,
            extract_s3_error(&body)
        );
    }

    let body = response.body_string();
    parse_upload_id(&body)
}

/// Upload all parts concurrently with a concurrency limit.
///
/// Returns the list of completed parts (part number + ETag) on success.
async fn upload_parts(
    ctx: &S3CommandContext,
    bucket: &str,
    key: &str,
    upload_id: &str,
    data: &[u8],
) -> Result<Vec<CompletedPart>> {
    let parts = split_parts(data.len(), PART_SIZE);
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENCY));

    let mut handles = Vec::with_capacity(parts.len());

    for (part_number, (offset, length)) in parts.iter().enumerate() {
        let part_number = part_number + 1; // S3 part numbers are 1-based
        let part_data = data[*offset..*offset + *length].to_vec();
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .context("Failed to acquire semaphore permit")?;

        // Clone what we need for the spawned task
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let region = ctx.region.clone();
        let credentials = ctx.credentials.clone();
        let endpoint_url = ctx.endpoint_url.clone();
        let debug = ctx.debug;
        let no_sign_request_flag = ctx.no_sign_request;

        let handle = tokio::spawn(async move {
            let task_ctx = S3CommandContext {
                region,
                credentials,
                endpoint_url,
                output_format: "json".to_string(),
                debug,
                no_sign_request: no_sign_request_flag,
            };

            let result =
                upload_single_part(&task_ctx, &bucket, &key, &upload_id, part_number, &part_data)
                    .await;

            drop(permit); // Release the semaphore permit
            result
        });

        handles.push(handle);
    }

    // Collect all results
    let mut completed_parts = Vec::with_capacity(handles.len());
    for handle in handles {
        let part = handle
            .await
            .context("Part upload task panicked")?
            .context("Part upload failed")?;
        completed_parts.push(part);
    }

    // Sort by part number to ensure correct ordering
    completed_parts.sort_by_key(|p| p.part_number);

    Ok(completed_parts)
}

/// Upload a single part with per-part retry logic.
///
/// Retries on transient errors (5xx, network errors) up to `PART_UPLOAD_MAX_RETRIES` times.
async fn upload_single_part(
    ctx: &S3CommandContext,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: usize,
    part_data: &[u8],
) -> Result<CompletedPart> {
    let mut last_error = None;

    for attempt in 1..=PART_UPLOAD_MAX_RETRIES {
        match upload_single_part_attempt(ctx, bucket, key, upload_id, part_number, part_data).await {
            Ok(part) => return Ok(part),
            Err(e) => {
                if attempt < PART_UPLOAD_MAX_RETRIES {
                    if ctx.debug {
                        eprintln!(
                            "[debug] part {} upload attempt {}/{} failed: {}, retrying...",
                            part_number, attempt, PART_UPLOAD_MAX_RETRIES, e
                        );
                    }
                    let delay_ms = PART_RETRY_BASE_DELAY_MS * (1u64 << (attempt - 1));
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Part {} upload failed", part_number)))
}

/// Single attempt to upload a part (no retry).
async fn upload_single_part_attempt(
    ctx: &S3CommandContext,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: usize,
    part_data: &[u8],
) -> Result<CompletedPart> {
    let uri_path = format!("/{key}");
    let part_num_str = part_number.to_string();
    let content_length_str = part_data.len().to_string();

    let query_params: Vec<(&str, &str)> = vec![
        ("partNumber", &part_num_str),
        ("uploadId", upload_id),
    ];

    let extra_headers: Vec<(&str, &str)> = vec![("content-length", &content_length_str)];

    let response = s3_bucket_api_call(
        ctx,
        bucket,
        "PUT",
        &uri_path,
        &query_params,
        Some(part_data),
        &extra_headers,
    )
    .await
    .with_context(|| format!("UploadPart {} request failed", part_number))?;

    if response.status >= 300 {
        let body = response.body_string();
        bail!(
            "UploadPart {} failed (HTTP {}): {}",
            part_number,
            response.status,
            extract_s3_error(&body)
        );
    }

    // Extract the ETag from the response headers
    let etag = response
        .headers
        .get("etag")
        .or_else(|| response.headers.get("ETag"))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("UploadPart {} response missing ETag header", part_number))?;

    if ctx.debug {
        eprintln!(
            "[debug] uploaded part {}/{} ({} bytes), etag={}",
            part_number,
            split_parts(0, PART_SIZE).len().max(1),
            part_data.len(),
            etag
        );
    }

    Ok(CompletedPart {
        part_number,
        etag,
    })
}

/// Complete a multipart upload by sending the list of parts and their ETags.
async fn complete_multipart_upload(
    ctx: &S3CommandContext,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[CompletedPart],
) -> Result<()> {
    let uri_path = format!("/{key}");
    let query_params: Vec<(&str, &str)> = vec![("uploadId", upload_id)];

    let body = build_complete_multipart_xml(parts);
    let content_length = body.len().to_string();
    let extra_headers: Vec<(&str, &str)> = vec![("content-length", &content_length)];

    let response = s3_bucket_api_call(
        ctx,
        bucket,
        "POST",
        &uri_path,
        &query_params,
        Some(body.as_bytes()),
        &extra_headers,
    )
    .await
    .context("CompleteMultipartUpload request failed")?;

    if response.status >= 300 {
        let resp_body = response.body_string();
        bail!(
            "CompleteMultipartUpload failed (HTTP {}): {}",
            response.status,
            extract_s3_error(&resp_body)
        );
    }

    Ok(())
}

/// Abort a multipart upload (best-effort cleanup).
async fn abort_multipart_upload(
    ctx: &S3CommandContext,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<()> {
    let uri_path = format!("/{key}");
    let query_params: Vec<(&str, &str)> = vec![("uploadId", upload_id)];

    let response =
        s3_bucket_api_call(ctx, bucket, "DELETE", &uri_path, &query_params, None, &[]).await?;

    if response.status >= 300 && ctx.debug {
        let body = response.body_string();
        eprintln!(
            "[debug] AbortMultipartUpload returned HTTP {}: {}",
            response.status,
            extract_s3_error(&body)
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// XML helpers
// ---------------------------------------------------------------------------

/// Parse the UploadId from a CreateMultipartUpload XML response.
///
/// Expected format:
/// ```xml
/// <InitiateMultipartUploadResult>
///   <Bucket>my-bucket</Bucket>
///   <Key>my-key</Key>
///   <UploadId>abc123</UploadId>
/// </InitiateMultipartUploadResult>
/// ```
fn parse_upload_id(xml_body: &str) -> Result<String> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml_body);
    let mut in_upload_id = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name();
                let tag = std::str::from_utf8(name.as_ref()).unwrap_or("");
                let local = strip_ns(tag);
                if local == "UploadId" {
                    in_upload_id = true;
                }
            }
            Ok(Event::Text(ref e)) => {
                if in_upload_id {
                    let text = e.unescape().unwrap_or_default().to_string();
                    return Ok(text);
                }
            }
            Ok(Event::End(_)) => {
                if in_upload_id {
                    // Empty UploadId tag
                    bail!("CreateMultipartUpload response contained empty UploadId");
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => bail!("Failed to parse CreateMultipartUpload XML response: {}", e),
        }
    }

    bail!("CreateMultipartUpload response did not contain an UploadId element")
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

/// Build the XML body for a CompleteMultipartUpload request.
///
/// Produces:
/// ```xml
/// <CompleteMultipartUpload>
///   <Part>
///     <PartNumber>1</PartNumber>
///     <ETag>"abc123"</ETag>
///   </Part>
///   ...
/// </CompleteMultipartUpload>
/// ```
fn build_complete_multipart_xml(parts: &[CompletedPart]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for part in parts {
        xml.push_str("<Part>");
        xml.push_str(&format!("<PartNumber>{}</PartNumber>", part.part_number));
        xml.push_str(&format!("<ETag>{}</ETag>", part.etag));
        xml.push_str("</Part>");
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

// ---------------------------------------------------------------------------
// Part splitting
// ---------------------------------------------------------------------------

/// Calculate the (offset, length) pairs for splitting a file of `total_size`
/// bytes into parts of `part_size` bytes each.
///
/// Returns a Vec of (offset, length) tuples. The last part may be smaller
/// than `part_size`.
fn split_parts(total_size: usize, part_size: usize) -> Vec<(usize, usize)> {
    if total_size == 0 || part_size == 0 {
        return vec![];
    }

    let mut parts = Vec::new();
    let mut offset = 0;
    while offset < total_size {
        let length = std::cmp::min(part_size, total_size - offset);
        parts.push((offset, length));
        offset += length;
    }
    parts
}

/// Returns true if the file size requires multipart upload.
pub fn needs_multipart(file_size: u64) -> bool {
    file_size >= MULTIPART_THRESHOLD
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // parse_upload_id tests
    // ---------------------------------------------------------------

    #[test]
    fn test_multipart_parse_upload_id_basic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
  <Bucket>my-bucket</Bucket>
  <Key>my-key</Key>
  <UploadId>VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
</InitiateMultipartUploadResult>"#;

        let upload_id = parse_upload_id(xml).unwrap();
        assert_eq!(
            upload_id,
            "VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"
        );
    }

    #[test]
    fn test_multipart_parse_upload_id_with_namespace() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>example-bucket</Bucket>
  <Key>example-object</Key>
  <UploadId>EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-</UploadId>
</InitiateMultipartUploadResult>"#;

        let upload_id = parse_upload_id(xml).unwrap();
        assert_eq!(
            upload_id,
            "EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-"
        );
    }

    #[test]
    fn test_multipart_parse_upload_id_missing() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
  <Bucket>my-bucket</Bucket>
  <Key>my-key</Key>
</InitiateMultipartUploadResult>"#;

        let result = parse_upload_id(xml);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("did not contain an UploadId"));
    }

    #[test]
    fn test_multipart_parse_upload_id_empty_xml() {
        let result = parse_upload_id("");
        assert!(result.is_err());
    }

    #[test]
    fn test_multipart_parse_upload_id_simple() {
        let xml = "<InitiateMultipartUploadResult><UploadId>abc123</UploadId></InitiateMultipartUploadResult>";
        let upload_id = parse_upload_id(xml).unwrap();
        assert_eq!(upload_id, "abc123");
    }

    // ---------------------------------------------------------------
    // build_complete_multipart_xml tests
    // ---------------------------------------------------------------

    #[test]
    fn test_multipart_build_complete_xml_single_part() {
        let parts = vec![CompletedPart {
            part_number: 1,
            etag: "\"abc123def456\"".to_string(),
        }];

        let xml = build_complete_multipart_xml(&parts);
        assert_eq!(
            xml,
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"abc123def456\"</ETag></Part></CompleteMultipartUpload>"
        );
    }

    #[test]
    fn test_multipart_build_complete_xml_multiple_parts() {
        let parts = vec![
            CompletedPart {
                part_number: 1,
                etag: "\"etag1\"".to_string(),
            },
            CompletedPart {
                part_number: 2,
                etag: "\"etag2\"".to_string(),
            },
            CompletedPart {
                part_number: 3,
                etag: "\"etag3\"".to_string(),
            },
        ];

        let xml = build_complete_multipart_xml(&parts);
        assert!(xml.starts_with("<CompleteMultipartUpload>"));
        assert!(xml.ends_with("</CompleteMultipartUpload>"));
        assert!(xml.contains("<PartNumber>1</PartNumber>"));
        assert!(xml.contains("<PartNumber>2</PartNumber>"));
        assert!(xml.contains("<PartNumber>3</PartNumber>"));
        assert!(xml.contains("<ETag>\"etag1\"</ETag>"));
        assert!(xml.contains("<ETag>\"etag2\"</ETag>"));
        assert!(xml.contains("<ETag>\"etag3\"</ETag>"));
    }

    #[test]
    fn test_multipart_build_complete_xml_empty_parts() {
        let parts: Vec<CompletedPart> = vec![];
        let xml = build_complete_multipart_xml(&parts);
        assert_eq!(xml, "<CompleteMultipartUpload></CompleteMultipartUpload>");
    }

    #[test]
    fn test_multipart_build_complete_xml_preserves_order() {
        let parts = vec![
            CompletedPart {
                part_number: 1,
                etag: "\"first\"".to_string(),
            },
            CompletedPart {
                part_number: 2,
                etag: "\"second\"".to_string(),
            },
        ];

        let xml = build_complete_multipart_xml(&parts);
        let pos1 = xml.find("<PartNumber>1</PartNumber>").unwrap();
        let pos2 = xml.find("<PartNumber>2</PartNumber>").unwrap();
        assert!(
            pos1 < pos2,
            "Part 1 should appear before Part 2 in the XML"
        );
    }

    // ---------------------------------------------------------------
    // split_parts tests
    // ---------------------------------------------------------------

    #[test]
    fn test_multipart_split_parts_exact_multiple() {
        // 24 MB file with 8 MB parts = exactly 3 parts
        let total = 24 * 1024 * 1024;
        let part_size = 8 * 1024 * 1024;
        let parts = split_parts(total, part_size);
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], (0, part_size));
        assert_eq!(parts[1], (part_size, part_size));
        assert_eq!(parts[2], (2 * part_size, part_size));
    }

    #[test]
    fn test_multipart_split_parts_with_remainder() {
        // 10 MB file with 8 MB parts = 2 parts (8 MB + 2 MB)
        let total = 10 * 1024 * 1024;
        let part_size = 8 * 1024 * 1024;
        let parts = split_parts(total, part_size);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], (0, part_size));
        assert_eq!(parts[1], (part_size, 2 * 1024 * 1024));
    }

    #[test]
    fn test_multipart_split_parts_single_part() {
        // 5 MB file with 8 MB parts = 1 part
        let total = 5 * 1024 * 1024;
        let part_size = 8 * 1024 * 1024;
        let parts = split_parts(total, part_size);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], (0, total));
    }

    #[test]
    fn test_multipart_split_parts_zero_size() {
        let parts = split_parts(0, 8 * 1024 * 1024);
        assert!(parts.is_empty());
    }

    #[test]
    fn test_multipart_split_parts_zero_part_size() {
        let parts = split_parts(100, 0);
        assert!(parts.is_empty());
    }

    #[test]
    fn test_multipart_split_parts_small_values() {
        // 10 bytes, 3 bytes per part = 4 parts (3+3+3+1)
        let parts = split_parts(10, 3);
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], (0, 3));
        assert_eq!(parts[1], (3, 3));
        assert_eq!(parts[2], (6, 3));
        assert_eq!(parts[3], (9, 1));
    }

    #[test]
    fn test_multipart_split_parts_one_byte() {
        let parts = split_parts(1, 8 * 1024 * 1024);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], (0, 1));
    }

    #[test]
    fn test_multipart_split_parts_covers_all_data() {
        // Verify that the parts cover every byte of the input
        let total = 25 * 1024 * 1024 + 42; // odd size
        let part_size = 8 * 1024 * 1024;
        let parts = split_parts(total, part_size);

        let total_covered: usize = parts.iter().map(|(_, len)| len).sum();
        assert_eq!(total_covered, total);

        // Verify contiguity
        for i in 1..parts.len() {
            assert_eq!(parts[i].0, parts[i - 1].0 + parts[i - 1].1);
        }
    }

    // ---------------------------------------------------------------
    // needs_multipart (threshold check) tests
    // ---------------------------------------------------------------

    #[test]
    fn test_multipart_threshold_below() {
        // 7 MB is below the 8 MB threshold
        assert!(!needs_multipart(7 * 1024 * 1024));
    }

    #[test]
    fn test_multipart_threshold_exactly_at() {
        // Exactly 8 MB should use multipart
        assert!(needs_multipart(8 * 1024 * 1024));
    }

    #[test]
    fn test_multipart_threshold_above() {
        // 9 MB is above the threshold
        assert!(needs_multipart(9 * 1024 * 1024));
    }

    #[test]
    fn test_multipart_threshold_zero() {
        assert!(!needs_multipart(0));
    }

    #[test]
    fn test_multipart_threshold_one_byte_below() {
        assert!(!needs_multipart(8 * 1024 * 1024 - 1));
    }

    #[test]
    fn test_multipart_threshold_one_byte_above() {
        assert!(needs_multipart(8 * 1024 * 1024 + 1));
    }

    #[test]
    fn test_multipart_threshold_large_file() {
        // 5 GB
        assert!(needs_multipart(5 * 1024 * 1024 * 1024));
    }

    // ---------------------------------------------------------------
    // constants sanity checks
    // ---------------------------------------------------------------

    #[test]
    fn test_multipart_constants() {
        assert_eq!(MULTIPART_THRESHOLD, 8 * 1024 * 1024);
        assert_eq!(PART_SIZE, 8 * 1024 * 1024);
        assert_eq!(MAX_CONCURRENCY, 10);
    }

    // ---------------------------------------------------------------
    // Per-part retry configuration tests
    // ---------------------------------------------------------------

    #[test]
    fn test_transfer_retry_constants() {
        assert_eq!(PART_UPLOAD_MAX_RETRIES, 3);
        assert_eq!(PART_RETRY_BASE_DELAY_MS, 500);
    }

    #[test]
    fn test_transfer_retry_delay_calculation() {
        // Verify exponential backoff for part retries
        // Attempt 1: 500ms * 2^0 = 500ms
        // Attempt 2: 500ms * 2^1 = 1000ms
        // Attempt 3 (if it existed): 500ms * 2^2 = 2000ms
        let delay1 = PART_RETRY_BASE_DELAY_MS * (1u64 << 0);
        let delay2 = PART_RETRY_BASE_DELAY_MS * (1u64 << 1);
        let delay3 = PART_RETRY_BASE_DELAY_MS * (1u64 << 2);

        assert_eq!(delay1, 500);
        assert_eq!(delay2, 1000);
        assert_eq!(delay3, 2000);
    }
}
