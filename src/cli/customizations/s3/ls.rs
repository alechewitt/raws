//! S3 `ls` high-level command implementation.
//!
//! Handles `raws s3 ls` (list all buckets) and `raws s3 ls s3://bucket/prefix`
//! (list objects in a bucket with optional prefix).

use anyhow::{bail, Result};

use super::s3_api_call;
use super::s3_bucket_api_call;
use super::S3CommandContext;

/// Represents a single S3 bucket returned by ListBuckets.
#[derive(Debug, Clone)]
struct BucketInfo {
    name: String,
    creation_date: String,
}

/// Execute the `s3 ls` command.
///
/// When no path argument is given, lists all S3 buckets.
/// When a path argument like `s3://bucket/prefix` is given, lists objects in the bucket.
pub async fn execute(args: &[String], ctx: &S3CommandContext) -> Result<()> {
    // Check for --recursive flag
    let recursive = args.iter().any(|a| a == "--recursive");

    // Filter out flags and find the positional path argument
    let path_arg = args.iter().find(|a| !a.starts_with('-'));

    match path_arg {
        None => list_buckets(ctx).await,
        Some(path) => {
            let (bucket, prefix) = parse_s3_url(path)?;
            list_objects(ctx, &bucket, &prefix, recursive).await
        }
    }
}

/// Parse an S3 URL like `s3://bucket/prefix` into (bucket, prefix).
///
/// Handles edge cases:
/// - `s3://bucket` -> ("bucket", "")
/// - `s3://bucket/` -> ("bucket", "")
/// - `s3://bucket/prefix` -> ("bucket", "prefix")
/// - `s3://bucket/path/to/prefix` -> ("bucket", "path/to/prefix")
fn parse_s3_url(url: &str) -> Result<(String, String)> {
    let stripped = url
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow::anyhow!("Invalid S3 URL: '{}'. Expected format: s3://bucket[/prefix]", url))?;

    if stripped.is_empty() {
        bail!("Invalid S3 URL: '{}'. Bucket name is required.", url);
    }

    let (bucket, prefix) = match stripped.find('/') {
        Some(pos) => {
            let bucket = &stripped[..pos];
            let prefix = &stripped[pos + 1..];
            (bucket.to_string(), prefix.to_string())
        }
        None => (stripped.to_string(), String::new()),
    };

    if bucket.is_empty() {
        bail!("Invalid S3 URL: '{}'. Bucket name is required.", url);
    }

    Ok((bucket, prefix))
}

/// Represents a single object in a ListObjectsV2 response.
#[derive(Debug, Clone)]
struct ObjectInfo {
    key: String,
    last_modified: String,
    size: u64,
}

/// Represents a common prefix (virtual directory) in a ListObjectsV2 response.
#[derive(Debug, Clone)]
struct CommonPrefixInfo {
    prefix: String,
}

/// List objects in an S3 bucket with optional prefix.
///
/// Calls the ListObjectsV2 API, handles pagination, and prints results in AWS CLI format:
/// - Common prefixes: `                           PRE dirname/`
/// - Objects: `2023-01-15 10:30:45      12345 file.txt`
async fn list_objects(
    ctx: &S3CommandContext,
    bucket: &str,
    prefix: &str,
    recursive: bool,
) -> Result<()> {
    let mut continuation_token: Option<String> = None;

    loop {
        // Build query parameters
        let mut params: Vec<(&str, &str)> = vec![("list-type", "2")];

        if !prefix.is_empty() {
            params.push(("prefix", prefix));
        }

        if !recursive {
            params.push(("delimiter", "/"));
        }

        // We need to own the token string for borrowing
        let token_string;
        if let Some(ref token) = continuation_token {
            token_string = token.clone();
            params.push(("continuation-token", &token_string));
        }

        let response = s3_bucket_api_call(ctx, bucket, "GET", "/", &params, None, &[]).await?;

        if response.status >= 300 {
            let body = response.body_string();
            bail!(
                "ListObjectsV2 failed (HTTP {}): {}",
                response.status,
                extract_error_message(&body)
            );
        }

        let body = response.body_string();
        let page = parse_list_objects_v2_response(&body)?;

        // Print common prefixes first (shown as PRE)
        for cp in &page.common_prefixes {
            let display_name = strip_prefix_from_key(&cp.prefix, prefix);
            println!("{:>30} {}", "PRE", display_name);
        }

        // Print objects
        for obj in &page.objects {
            let display_name = strip_prefix_from_key(&obj.key, prefix);
            let formatted_date = format_creation_date(&obj.last_modified);
            println!("{} {:>10} {}", formatted_date, obj.size, display_name);
        }

        // Handle pagination
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

    Ok(())
}

/// Strip the search prefix from a key for display purposes.
///
/// When listing `s3://bucket/photos/`, the prefix "photos/" is stripped from
/// displayed keys, so `photos/cat.jpg` becomes `cat.jpg`.
fn strip_prefix_from_key<'a>(key: &'a str, prefix: &str) -> &'a str {
    key.strip_prefix(prefix).unwrap_or(key)
}

/// Parsed result of a single ListObjectsV2 response page.
#[derive(Debug)]
struct ListObjectsV2Page {
    objects: Vec<ObjectInfo>,
    common_prefixes: Vec<CommonPrefixInfo>,
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

/// Parse a ListObjectsV2 XML response.
fn parse_list_objects_v2_response(xml_body: &str) -> Result<ListObjectsV2Page> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml_body);
    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut is_truncated = false;
    let mut next_continuation_token: Option<String> = None;

    // State machine
    let mut in_contents = false;
    let mut in_common_prefixes = false;
    let mut current_tag = String::new();

    // Current object fields
    let mut current_key: Option<String> = None;
    let mut current_last_modified: Option<String> = None;
    let mut current_size: Option<u64> = None;

    // Current common prefix
    let mut current_prefix: Option<String> = None;

    // Top-level tag tracking (for IsTruncated, NextContinuationToken)
    let mut top_level_tag = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let tag = strip_ns(std::str::from_utf8(e.name().as_ref()).unwrap_or(""));
                match tag.as_str() {
                    "Contents" => {
                        in_contents = true;
                        current_key = None;
                        current_last_modified = None;
                        current_size = None;
                    }
                    "CommonPrefixes" => {
                        in_common_prefixes = true;
                        current_prefix = None;
                    }
                    _ => {
                        if in_contents || in_common_prefixes {
                            current_tag = tag;
                        } else {
                            top_level_tag = tag;
                        }
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let tag = strip_ns(std::str::from_utf8(e.name().as_ref()).unwrap_or(""));
                match tag.as_str() {
                    "Contents" => {
                        if in_contents {
                            if let (Some(key), Some(last_modified)) =
                                (current_key.take(), current_last_modified.take())
                            {
                                objects.push(ObjectInfo {
                                    key,
                                    last_modified,
                                    size: current_size.unwrap_or(0),
                                });
                            }
                            in_contents = false;
                        }
                    }
                    "CommonPrefixes" => {
                        if in_common_prefixes {
                            if let Some(prefix) = current_prefix.take() {
                                common_prefixes.push(CommonPrefixInfo { prefix });
                            }
                            in_common_prefixes = false;
                        }
                    }
                    _ => {}
                }
                current_tag.clear();
                top_level_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let text = e.unescape().unwrap_or_default().to_string();
                if in_contents {
                    match current_tag.as_str() {
                        "Key" => current_key = Some(text),
                        "LastModified" => current_last_modified = Some(text),
                        "Size" => current_size = text.parse::<u64>().ok(),
                        _ => {}
                    }
                } else if in_common_prefixes {
                    if current_tag == "Prefix" {
                        current_prefix = Some(text);
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

    Ok(ListObjectsV2Page {
        objects,
        common_prefixes,
        is_truncated,
        next_continuation_token,
    })
}

/// List all S3 buckets (no path argument).
///
/// Calls the S3 ListBuckets API, parses the XML response, and prints
/// buckets in the AWS CLI format: `YYYY-MM-DD HH:MM:SS bucket-name`
async fn list_buckets(ctx: &S3CommandContext) -> Result<()> {
    let response = s3_api_call(ctx, "GET", "/", &[], None, &[]).await?;

    if response.status >= 300 {
        let body = response.body_string();
        bail!(
            "ListBuckets failed (HTTP {}): {}",
            response.status,
            extract_error_message(&body)
        );
    }

    let body = response.body_string();
    let mut buckets = parse_list_buckets_response(&body)?;

    // Sort by name (AWS CLI sorts by name)
    buckets.sort_by(|a, b| a.name.cmp(&b.name));

    for bucket in &buckets {
        let formatted_date = format_creation_date(&bucket.creation_date);
        println!("{} {}", formatted_date, bucket.name);
    }

    Ok(())
}

/// Parse the ListBuckets XML response into a vector of BucketInfo.
///
/// Expected XML format:
/// ```xml
/// <ListAllMyBucketsResult>
///   <Buckets>
///     <Bucket>
///       <CreationDate>2023-01-15T10:30:45.000Z</CreationDate>
///       <Name>my-bucket</Name>
///     </Bucket>
///   </Buckets>
/// </ListAllMyBucketsResult>
/// ```
fn parse_list_buckets_response(xml_body: &str) -> Result<Vec<BucketInfo>> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml_body);
    let mut buckets = Vec::new();

    // State machine for parsing
    let mut in_bucket = false;
    let mut current_name: Option<String> = None;
    let mut current_date: Option<String> = None;
    let mut current_tag = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let tag = strip_ns(std::str::from_utf8(e.name().as_ref()).unwrap_or(""));
                match tag.as_str() {
                    "Bucket" => {
                        in_bucket = true;
                        current_name = None;
                        current_date = None;
                    }
                    _ => {
                        if in_bucket {
                            current_tag = tag;
                        }
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let tag = strip_ns(std::str::from_utf8(e.name().as_ref()).unwrap_or(""));
                if tag == "Bucket" && in_bucket {
                    if let (Some(name), Some(date)) = (current_name.take(), current_date.take()) {
                        buckets.push(BucketInfo {
                            name,
                            creation_date: date,
                        });
                    }
                    in_bucket = false;
                }
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                if in_bucket {
                    let text = e.unescape().unwrap_or_default().to_string();
                    match current_tag.as_str() {
                        "Name" => current_name = Some(text),
                        "CreationDate" => current_date = Some(text),
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => bail!("Failed to parse ListBuckets XML response: {}", e),
        }
    }

    Ok(buckets)
}

/// Format an ISO 8601 creation date to the AWS CLI display format in local time.
///
/// Input:  `2023-01-15T10:30:45.000Z` (UTC)
/// Output: `2023-01-15 03:30:45` (local time, e.g., UTC-7)
///
/// The AWS CLI displays timestamps in local time, not UTC.
fn format_creation_date(iso_date: &str) -> String {
    use chrono::{DateTime, Local, Utc};

    // Try parsing with milliseconds first, then without
    if let Ok(dt) = DateTime::parse_from_rfc3339(iso_date) {
        let local: DateTime<Local> = dt.with_timezone(&Local);
        return local.format("%Y-%m-%d %H:%M:%S").to_string();
    }

    // Try common S3 formats: "2023-01-15T10:30:45.000Z" or "2023-01-15T10:30:45Z"
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(
        iso_date.trim_end_matches('Z'),
        "%Y-%m-%dT%H:%M:%S%.f",
    ) {
        let utc = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
        let local: DateTime<Local> = utc.with_timezone(&Local);
        return local.format("%Y-%m-%d %H:%M:%S").to_string();
    }

    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(
        iso_date.trim_end_matches('Z'),
        "%Y-%m-%dT%H:%M:%S",
    ) {
        let utc = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
        let local: DateTime<Local> = utc.with_timezone(&Local);
        return local.format("%Y-%m-%d %H:%M:%S").to_string();
    }

    // Fallback: return as-is
    iso_date.to_string()
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

/// Extract a human-readable error message from an S3 XML error response.
fn extract_error_message(body: &str) -> String {
    // Try to find <Message>...</Message> in the error XML
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(body);
    let mut in_message = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let tag = strip_ns(std::str::from_utf8(e.name().as_ref()).unwrap_or(""));
                if tag == "Message" {
                    in_message = true;
                }
            }
            Ok(Event::Text(ref e)) => {
                if in_message {
                    return e.unescape().unwrap_or_default().to_string();
                }
            }
            Ok(Event::End(_)) => {
                if in_message {
                    return String::new();
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
    }

    // If we can't parse XML, return the raw body (truncated)
    if body.len() > 200 {
        format!("{}...", &body[..200])
    } else {
        body.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // format_creation_date tests
    // ---------------------------------------------------------------

    #[test]
    fn test_format_creation_date_with_millis() {
        // Verify it produces a valid "YYYY-MM-DD HH:MM:SS" format (converted to local time)
        let result = format_creation_date("2023-01-15T10:30:45.000Z");
        assert!(result.len() == 19, "Expected 19 chars, got: {result}");
        assert_eq!(&result[4..5], "-");
        assert_eq!(&result[7..8], "-");
        assert_eq!(&result[10..11], " ");
        assert_eq!(&result[13..14], ":");
        assert_eq!(&result[16..17], ":");
    }

    #[test]
    fn test_format_creation_date_without_millis() {
        let result = format_creation_date("2023-06-20T14:22:10Z");
        assert!(result.len() == 19, "Expected 19 chars, got: {result}");
        assert_eq!(&result[10..11], " ");
    }

    #[test]
    fn test_format_creation_date_roundtrip_utc() {
        // Verify the conversion uses chrono properly by checking it's a valid datetime
        use chrono::{DateTime, Local, Utc};
        let input = "2023-01-15T10:30:45.000Z";
        let result = format_creation_date(input);

        // Parse the UTC input and convert to local for comparison
        let utc_dt = DateTime::parse_from_rfc3339(input).unwrap();
        let local_dt: DateTime<Local> = utc_dt.with_timezone(&Local);
        let expected = local_dt.format("%Y-%m-%d %H:%M:%S").to_string();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_format_creation_date_no_t_separator() {
        // Edge case: if format is unexpected, return as-is
        assert_eq!(
            format_creation_date("2023-01-15 10:30:45"),
            "2023-01-15 10:30:45"
        );
    }

    #[test]
    fn test_format_creation_date_empty() {
        assert_eq!(format_creation_date(""), "");
    }

    // ---------------------------------------------------------------
    // parse_list_buckets_response tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_list_buckets_typical_response() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>abc123</ID>
    <DisplayName>webfile</DisplayName>
  </Owner>
  <Buckets>
    <Bucket>
      <Name>my-first-bucket</Name>
      <CreationDate>2023-01-15T10:30:45.000Z</CreationDate>
    </Bucket>
    <Bucket>
      <Name>my-second-bucket</Name>
      <CreationDate>2023-06-20T14:22:10.000Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>"#;

        let buckets = parse_list_buckets_response(xml).unwrap();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].name, "my-first-bucket");
        assert_eq!(buckets[0].creation_date, "2023-01-15T10:30:45.000Z");
        assert_eq!(buckets[1].name, "my-second-bucket");
        assert_eq!(buckets[1].creation_date, "2023-06-20T14:22:10.000Z");
    }

    #[test]
    fn test_parse_list_buckets_empty() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>abc123</ID>
    <DisplayName>webfile</DisplayName>
  </Owner>
  <Buckets/>
</ListAllMyBucketsResult>"#;

        let buckets = parse_list_buckets_response(xml).unwrap();
        assert_eq!(buckets.len(), 0);
    }

    #[test]
    fn test_parse_list_buckets_single_bucket() {
        let xml = r#"<ListAllMyBucketsResult>
  <Buckets>
    <Bucket>
      <Name>only-bucket</Name>
      <CreationDate>2024-03-01T00:00:00.000Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>"#;

        let buckets = parse_list_buckets_response(xml).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name, "only-bucket");
    }

    #[test]
    fn test_parse_list_buckets_no_buckets_element() {
        let xml = r#"<ListAllMyBucketsResult>
  <Owner>
    <ID>abc123</ID>
  </Owner>
</ListAllMyBucketsResult>"#;

        let buckets = parse_list_buckets_response(xml).unwrap();
        assert_eq!(buckets.len(), 0);
    }

    // ---------------------------------------------------------------
    // extract_error_message tests
    // ---------------------------------------------------------------

    #[test]
    fn test_extract_error_message_s3_style() {
        let xml = r#"<Error>
  <Code>AccessDenied</Code>
  <Message>Access Denied</Message>
</Error>"#;

        assert_eq!(extract_error_message(xml), "Access Denied");
    }

    #[test]
    fn test_extract_error_message_non_xml() {
        assert_eq!(extract_error_message("some plain text"), "some plain text");
    }

    #[test]
    fn test_extract_error_message_empty() {
        assert_eq!(extract_error_message(""), "");
    }

    // ---------------------------------------------------------------
    // strip_ns tests
    // ---------------------------------------------------------------

    #[test]
    fn test_strip_ns_with_braces() {
        assert_eq!(
            strip_ns("{http://s3.amazonaws.com/doc/2006-03-01/}Bucket"),
            "Bucket"
        );
    }

    #[test]
    fn test_strip_ns_with_colon() {
        assert_eq!(strip_ns("s3:Bucket"), "Bucket");
    }

    #[test]
    fn test_strip_ns_no_namespace() {
        assert_eq!(strip_ns("Bucket"), "Bucket");
    }

    // ---------------------------------------------------------------
    // Output formatting integration test
    // ---------------------------------------------------------------

    #[test]
    fn test_list_buckets_output_format() {
        // Verify the output format matches AWS CLI: "YYYY-MM-DD HH:MM:SS bucket-name"
        let date = format_creation_date("2023-01-15T10:30:45.000Z");
        let line = format!("{} {}", date, "my-first-bucket");
        // Should be 19 chars date + space + bucket name
        assert!(line.ends_with("my-first-bucket"));
        assert_eq!(date.len(), 19);
    }

    #[test]
    fn test_buckets_sorted_by_name() {
        let xml = r#"<ListAllMyBucketsResult>
  <Buckets>
    <Bucket>
      <Name>zebra-bucket</Name>
      <CreationDate>2023-01-01T00:00:00.000Z</CreationDate>
    </Bucket>
    <Bucket>
      <Name>alpha-bucket</Name>
      <CreationDate>2023-06-01T00:00:00.000Z</CreationDate>
    </Bucket>
    <Bucket>
      <Name>middle-bucket</Name>
      <CreationDate>2023-03-01T00:00:00.000Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>"#;

        let mut buckets = parse_list_buckets_response(xml).unwrap();
        buckets.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(buckets[0].name, "alpha-bucket");
        assert_eq!(buckets[1].name, "middle-bucket");
        assert_eq!(buckets[2].name, "zebra-bucket");
    }

    // ---------------------------------------------------------------
    // parse_s3_url tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_s3_url_bucket_only() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_s3_url_bucket_with_trailing_slash() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket/").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_s3_url_bucket_with_prefix() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket/photos/").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "photos/");
    }

    #[test]
    fn test_parse_s3_url_bucket_with_deep_prefix() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket/path/to/dir/").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path/to/dir/");
    }

    #[test]
    fn test_parse_s3_url_bucket_with_file_prefix() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket/file.txt").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "file.txt");
    }

    #[test]
    fn test_parse_s3_url_invalid_no_s3_scheme() {
        let result = parse_s3_url("https://my-bucket/");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid S3 URL"));
    }

    #[test]
    fn test_parse_s3_url_invalid_empty_after_scheme() {
        let result = parse_s3_url("s3://");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_s3_url_invalid_no_scheme() {
        let result = parse_s3_url("my-bucket/prefix");
        assert!(result.is_err());
    }

    // ---------------------------------------------------------------
    // parse_list_objects_v2_response tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_list_objects_v2_typical() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>my-bucket</Name>
  <Prefix>photos/</Prefix>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>photos/cat.jpg</Key>
    <LastModified>2023-01-15T10:30:45.000Z</LastModified>
    <ETag>"abc123"</ETag>
    <Size>12345</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>photos/dog.jpg</Key>
    <LastModified>2023-06-20T14:22:10.000Z</LastModified>
    <ETag>"def456"</ETag>
    <Size>678</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <CommonPrefixes>
    <Prefix>photos/vacation/</Prefix>
  </CommonPrefixes>
</ListBucketResult>"#;

        let page = parse_list_objects_v2_response(xml).unwrap();
        assert_eq!(page.objects.len(), 2);
        assert_eq!(page.objects[0].key, "photos/cat.jpg");
        assert_eq!(page.objects[0].last_modified, "2023-01-15T10:30:45.000Z");
        assert_eq!(page.objects[0].size, 12345);
        assert_eq!(page.objects[1].key, "photos/dog.jpg");
        assert_eq!(page.objects[1].size, 678);
        assert_eq!(page.common_prefixes.len(), 1);
        assert_eq!(page.common_prefixes[0].prefix, "photos/vacation/");
        assert!(!page.is_truncated);
        assert!(page.next_continuation_token.is_none());
    }

    #[test]
    fn test_parse_list_objects_v2_truncated() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>abc123token</NextContinuationToken>
  <Contents>
    <Key>file1.txt</Key>
    <LastModified>2023-01-15T10:30:45.000Z</LastModified>
    <Size>100</Size>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_objects_v2_response(xml).unwrap();
        assert!(page.is_truncated);
        assert_eq!(
            page.next_continuation_token,
            Some("abc123token".to_string())
        );
        assert_eq!(page.objects.len(), 1);
    }

    #[test]
    fn test_parse_list_objects_v2_empty() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>"#;

        let page = parse_list_objects_v2_response(xml).unwrap();
        assert_eq!(page.objects.len(), 0);
        assert_eq!(page.common_prefixes.len(), 0);
        assert!(!page.is_truncated);
    }

    #[test]
    fn test_parse_list_objects_v2_only_common_prefixes() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <CommonPrefixes>
    <Prefix>photos/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>videos/</Prefix>
  </CommonPrefixes>
</ListBucketResult>"#;

        let page = parse_list_objects_v2_response(xml).unwrap();
        assert_eq!(page.objects.len(), 0);
        assert_eq!(page.common_prefixes.len(), 2);
        assert_eq!(page.common_prefixes[0].prefix, "photos/");
        assert_eq!(page.common_prefixes[1].prefix, "videos/");
    }

    #[test]
    fn test_parse_list_objects_v2_with_namespace() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>test.txt</Key>
    <LastModified>2023-01-01T00:00:00.000Z</LastModified>
    <Size>42</Size>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_objects_v2_response(xml).unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, "test.txt");
        assert_eq!(page.objects[0].size, 42);
    }

    // ---------------------------------------------------------------
    // strip_prefix_from_key tests
    // ---------------------------------------------------------------

    #[test]
    fn test_strip_prefix_from_key_with_prefix() {
        assert_eq!(strip_prefix_from_key("photos/cat.jpg", "photos/"), "cat.jpg");
    }

    #[test]
    fn test_strip_prefix_from_key_no_prefix() {
        assert_eq!(strip_prefix_from_key("file.txt", ""), "file.txt");
    }

    #[test]
    fn test_strip_prefix_from_key_prefix_not_matching() {
        // If the key doesn't start with the prefix, return the full key
        assert_eq!(
            strip_prefix_from_key("other/file.txt", "photos/"),
            "other/file.txt"
        );
    }

    #[test]
    fn test_strip_prefix_from_key_dir_prefix() {
        assert_eq!(
            strip_prefix_from_key("photos/vacation/", "photos/"),
            "vacation/"
        );
    }

    // ---------------------------------------------------------------
    // Output format tests for list objects
    // ---------------------------------------------------------------

    #[test]
    fn test_list_objects_pre_format() {
        // Verify PRE format matches AWS CLI: right-aligned to 30 chars
        let line = format!("{:>30} {}", "PRE", "photos/");
        assert!(line.contains("PRE photos/"));
        // "PRE" should be right-aligned within 30 chars
        assert_eq!(line.len(), 30 + 1 + "photos/".len());
    }

    #[test]
    fn test_list_objects_object_format() {
        // Verify object format: date (19) + space + size (10 right-aligned) + space + name
        let date = "2023-01-15 10:30:45";
        let size: u64 = 12345;
        let name = "file.txt";
        let line = format!("{} {:>10} {}", date, size, name);
        assert_eq!(line, "2023-01-15 10:30:45      12345 file.txt");
    }

    #[test]
    fn test_list_objects_object_format_small_size() {
        let date = "2023-06-20 14:22:10";
        let size: u64 = 678;
        let name = "file2.txt";
        let line = format!("{} {:>10} {}", date, size, name);
        assert_eq!(line, "2023-06-20 14:22:10        678 file2.txt");
    }

    #[test]
    fn test_list_objects_object_format_zero_size() {
        let date = "2023-01-01 00:00:00";
        let size: u64 = 0;
        let name = "empty.txt";
        let line = format!("{} {:>10} {}", date, size, name);
        assert_eq!(line, "2023-01-01 00:00:00          0 empty.txt");
    }

    #[test]
    fn test_list_objects_object_format_large_size() {
        let date = "2023-01-01 00:00:00";
        let size: u64 = 1_073_741_824; // 1 GB
        let name = "large.bin";
        let line = format!("{} {:>10} {}", date, size, name);
        assert_eq!(line, "2023-01-01 00:00:00 1073741824 large.bin");
    }

    // ---------------------------------------------------------------
    // parse_list_objects_v2_response edge cases
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_list_objects_v2_missing_size() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>file.txt</Key>
    <LastModified>2023-01-01T00:00:00.000Z</LastModified>
  </Contents>
</ListBucketResult>"#;

        let page = parse_list_objects_v2_response(xml).unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].size, 0); // Defaults to 0 when missing
    }

    #[test]
    fn test_parse_list_objects_v2_multiple_pages_structure() {
        // Verify that we can parse two separate pages correctly
        let page1_xml = r#"<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>token1</NextContinuationToken>
  <Contents>
    <Key>a.txt</Key>
    <LastModified>2023-01-01T00:00:00.000Z</LastModified>
    <Size>10</Size>
  </Contents>
</ListBucketResult>"#;

        let page2_xml = r#"<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>b.txt</Key>
    <LastModified>2023-01-02T00:00:00.000Z</LastModified>
    <Size>20</Size>
  </Contents>
</ListBucketResult>"#;

        let page1 = parse_list_objects_v2_response(page1_xml).unwrap();
        assert!(page1.is_truncated);
        assert_eq!(page1.next_continuation_token, Some("token1".to_string()));
        assert_eq!(page1.objects.len(), 1);
        assert_eq!(page1.objects[0].key, "a.txt");

        let page2 = parse_list_objects_v2_response(page2_xml).unwrap();
        assert!(!page2.is_truncated);
        assert!(page2.next_continuation_token.is_none());
        assert_eq!(page2.objects.len(), 1);
        assert_eq!(page2.objects[0].key, "b.txt");
    }
}
