//! CloudWatch Logs `tail` command implementation.
//!
//! Implements `raws logs tail <log-group-name>` which tails CloudWatch Logs
//! using the FilterLogEvents API. Supports:
//! - `--since <time>`: relative (e.g. "5m", "1h", "1d") or ISO 8601 timestamp
//! - `--follow`: continuous polling with 1-second intervals
//! - `--format short|detailed|json`: output formatting

use anyhow::{bail, Context, Result};
use std::collections::HashSet;
use std::time::Duration;

use crate::core::auth::sigv4::{self, SigningParams};
use crate::core::credentials::Credentials;
use crate::core::http::client::HttpClient;
use crate::core::http::request::HttpRequest;
use crate::core::protocol::json as json_protocol;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LOGS_TARGET_PREFIX: &str = "Logs_20140328";
const LOGS_JSON_VERSION: &str = "1.1";
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Default --since value: 10 minutes ago.
const DEFAULT_SINCE_MINUTES: i64 = 10;

// ---------------------------------------------------------------------------
// Tail arguments
// ---------------------------------------------------------------------------

/// Output format for the tail command.
#[derive(Debug, Clone, PartialEq)]
pub enum TailFormat {
    Short,
    Detailed,
    Json,
}

/// Parsed arguments for the `logs tail` command.
#[derive(Debug)]
pub struct TailArgs {
    pub log_group_name: String,
    pub since: Option<String>,
    pub follow: bool,
    pub format: TailFormat,
}

/// Parse tail arguments from the raw CLI argument list.
///
/// Expected usage: `raws logs tail <log-group-name> [--since <time>] [--follow] [--format short|detailed|json]`
pub fn parse_tail_args(args: &[String]) -> Result<TailArgs> {
    let mut log_group_name: Option<String> = None;
    let mut since: Option<String> = None;
    let mut follow = false;
    let mut format = TailFormat::Short;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--since" => {
                i += 1;
                since = Some(
                    args.get(i)
                        .ok_or_else(|| anyhow::anyhow!("--since requires a value"))?
                        .clone(),
                );
            }
            "--follow" => {
                follow = true;
            }
            "--format" => {
                i += 1;
                let fmt_str = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--format requires a value (short|detailed|json)"))?;
                format = parse_format(fmt_str)?;
            }
            arg if arg.starts_with("--") => {
                bail!("Unknown argument for logs tail: {}", arg);
            }
            _ => {
                // Positional argument: log group name
                if log_group_name.is_some() {
                    bail!(
                        "Unexpected positional argument '{}'. Only one log group name is expected.",
                        args[i]
                    );
                }
                log_group_name = Some(args[i].clone());
            }
        }
        i += 1;
    }

    let log_group_name = log_group_name
        .ok_or_else(|| anyhow::anyhow!("Missing required argument: <log-group-name>"))?;

    Ok(TailArgs {
        log_group_name,
        since,
        follow,
        format,
    })
}

/// Parse a format string into a TailFormat variant.
fn parse_format(s: &str) -> Result<TailFormat> {
    match s {
        "short" => Ok(TailFormat::Short),
        "detailed" => Ok(TailFormat::Detailed),
        "json" => Ok(TailFormat::Json),
        other => bail!(
            "Invalid format '{}'. Must be one of: short, detailed, json",
            other
        ),
    }
}

// ---------------------------------------------------------------------------
// Time parsing
// ---------------------------------------------------------------------------

/// Parse a --since value into epoch milliseconds.
///
/// Accepts:
/// - Relative durations: "5m" (minutes), "1h" (hours), "1d" (days), "30s" (seconds)
/// - ISO 8601 timestamps: "2024-01-01T00:00:00Z" or "2024-01-01T00:00:00+00:00"
pub fn parse_since_time(since_str: &str) -> Result<i64> {
    // Try relative duration first (e.g. "5m", "1h", "1d", "30s")
    if let Some(ms) = try_parse_relative(since_str) {
        return Ok(ms);
    }

    // Try ISO 8601 timestamp
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(since_str) {
        return Ok(dt.timestamp_millis());
    }

    // Try without timezone (assume UTC)
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(since_str, "%Y-%m-%dT%H:%M:%S") {
        let utc = dt.and_utc();
        return Ok(utc.timestamp_millis());
    }

    bail!(
        "Invalid --since value '{}'. Use a relative duration (e.g. 5m, 1h, 1d) or an ISO 8601 timestamp.",
        since_str
    )
}

/// Try to parse a relative duration string (e.g. "5m", "1h", "1d", "30s").
///
/// Returns epoch milliseconds (now minus the duration), or None if not a valid relative format.
fn try_parse_relative(s: &str) -> Option<i64> {
    if s.len() < 2 {
        return None;
    }

    let (num_str, unit) = s.split_at(s.len() - 1);
    let num: i64 = num_str.parse().ok()?;

    if num < 0 {
        return None;
    }

    let seconds = match unit {
        "s" => num,
        "m" => num * 60,
        "h" => num * 3600,
        "d" => num * 86400,
        _ => return None,
    };

    let now = chrono::Utc::now();
    let target = now - chrono::Duration::seconds(seconds);
    Some(target.timestamp_millis())
}

// ---------------------------------------------------------------------------
// LogsClient - CloudWatch Logs API client
// ---------------------------------------------------------------------------

/// A lightweight CloudWatch Logs API client that makes raw JSON protocol calls.
pub struct LogsClient {
    credentials: Credentials,
    region: String,
    endpoint_url: String,
    debug: bool,
}

impl LogsClient {
    pub fn new(
        credentials: Credentials,
        region: String,
        endpoint_url: String,
        debug: bool,
    ) -> Self {
        Self {
            credentials,
            region,
            endpoint_url,
            debug,
        }
    }

    /// Make a raw CloudWatch Logs API call using JSON protocol.
    ///
    /// Returns the parsed JSON response.
    async fn call(
        &self,
        operation: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let body_str = json_protocol::serialize_json_request(body)
            .context("Failed to serialize Logs request")?;

        let target = json_protocol::build_target_header(LOGS_TARGET_PREFIX, operation);
        let content_type = json_protocol::build_content_type(LOGS_JSON_VERSION);

        if self.debug {
            eprintln!("[debug] CloudWatch Logs API call: {}", operation);
            eprintln!("[debug] request body: {}", body_str);
        }

        // Parse endpoint URL
        let parsed_url = url::Url::parse(&self.endpoint_url)
            .context("Invalid CloudWatch Logs endpoint URL")?;
        let host = parsed_url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("No host in CloudWatch Logs endpoint URL"))?
            .to_string();

        // Build and sign the request
        let mut headers: Vec<(String, String)> = vec![
            ("host".to_string(), host),
            ("content-type".to_string(), content_type),
            ("x-amz-target".to_string(), target),
        ];

        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let signing_params = SigningParams::from_credentials(
            &self.credentials,
            &self.region,
            "logs",
            &datetime,
        );

        let uri_path = parsed_url.path();
        let query_string = parsed_url.query().unwrap_or("");

        sigv4::sign_request(
            "POST",
            uri_path,
            query_string,
            &mut headers,
            body_str.as_bytes(),
            &signing_params,
        )?;

        // Build HTTP request
        let mut request = HttpRequest::new("POST", &self.endpoint_url);
        request.body = body_str.as_bytes().to_vec();
        for (k, v) in &headers {
            request.add_header(k, v);
        }

        // Send
        let client = HttpClient::new()?;
        let response = client
            .send(&request)
            .await
            .context("CloudWatch Logs API request failed")?;
        let response_body = response.body_string();

        if self.debug {
            eprintln!(
                "[debug] CloudWatch Logs response status: {}",
                response.status
            );
            if response.status >= 400 {
                eprintln!(
                    "[debug] CloudWatch Logs response body: {}",
                    response_body
                );
            }
        }

        // Check for errors
        if response.status >= 400 {
            let (code, message) = json_protocol::parse_json_error(&response_body)
                .unwrap_or_else(|_| ("Unknown".to_string(), response_body.clone()));
            bail!(
                "CloudWatch Logs error (HTTP {}): {} - {}",
                response.status,
                code,
                message
            );
        }

        json_protocol::parse_json_response(&response_body)
            .context("Failed to parse CloudWatch Logs response")
    }

    /// Call FilterLogEvents API.
    ///
    /// Returns (events, next_token).
    pub async fn filter_log_events(
        &self,
        log_group: &str,
        start_time: i64,
        next_token: Option<&str>,
    ) -> Result<(Vec<LogEvent>, Option<String>)> {
        let mut body = serde_json::json!({
            "logGroupName": log_group,
            "startTime": start_time,
            "interleaved": true
        });

        if let Some(token) = next_token {
            body["nextToken"] = serde_json::Value::String(token.to_string());
        }

        let response = self.call("FilterLogEvents", &body).await?;

        // Parse events
        let events = response
            .get("events")
            .and_then(|e| e.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|event| {
                        let timestamp = event.get("timestamp").and_then(|t| t.as_i64())?;
                        let message = event
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("")
                            .to_string();
                        let log_stream_name = event
                            .get("logStreamName")
                            .and_then(|s| s.as_str())
                            .unwrap_or("")
                            .to_string();
                        let event_id = event
                            .get("eventId")
                            .and_then(|e| e.as_str())
                            .unwrap_or("")
                            .to_string();
                        Some(LogEvent {
                            timestamp,
                            message,
                            log_stream_name,
                            event_id,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let next_token = response
            .get("nextToken")
            .and_then(|t| t.as_str())
            .map(|s| s.to_string());

        Ok((events, next_token))
    }
}

// ---------------------------------------------------------------------------
// Log event types
// ---------------------------------------------------------------------------

/// A single log event from FilterLogEvents.
#[derive(Debug, Clone)]
pub struct LogEvent {
    pub timestamp: i64,
    pub message: String,
    pub log_stream_name: String,
    pub event_id: String,
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

/// Format a log event according to the chosen format.
pub fn format_event(event: &LogEvent, fmt: &TailFormat) -> String {
    match fmt {
        TailFormat::Short => {
            // Just the message, trimmed of trailing newline
            event.message.trim_end().to_string()
        }
        TailFormat::Detailed => {
            // "timestamp stream_name message"
            let ts = format_timestamp(event.timestamp);
            let msg = event.message.trim_end();
            format!("{} {} {}", ts, event.log_stream_name, msg)
        }
        TailFormat::Json => {
            // Print each event as JSON
            let obj = serde_json::json!({
                "timestamp": format_timestamp(event.timestamp),
                "message": event.message.trim_end(),
                "logStreamName": event.log_stream_name,
                "eventId": event.event_id
            });
            // Use compact JSON for each event line
            serde_json::to_string(&obj).unwrap_or_default()
        }
    }
}

/// Format an epoch milliseconds timestamp as ISO 8601.
pub fn format_timestamp(epoch_ms: i64) -> String {
    let dt = chrono::DateTime::from_timestamp_millis(epoch_ms);
    match dt {
        Some(d) => d.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        None => format!("{}", epoch_ms),
    }
}

// ---------------------------------------------------------------------------
// Main tail logic
// ---------------------------------------------------------------------------

/// Execute the `logs tail` command.
pub async fn run_tail(
    args: &TailArgs,
    credentials: Credentials,
    region: String,
    endpoint_url: String,
    debug: bool,
) -> Result<()> {
    let client = LogsClient::new(credentials, region, endpoint_url, debug);

    // Determine start time
    let start_time = match &args.since {
        Some(since_str) => parse_since_time(since_str)?,
        None => {
            // Default: 10 minutes ago
            let now = chrono::Utc::now();
            let default_start = now - chrono::Duration::minutes(DEFAULT_SINCE_MINUTES);
            default_start.timestamp_millis()
        }
    };

    if debug {
        eprintln!(
            "[debug] logs tail: log_group={} start_time={} follow={} format={:?}",
            args.log_group_name, start_time, args.follow, args.format
        );
    }

    if args.follow {
        run_tail_follow(&client, &args.log_group_name, start_time, &args.format).await
    } else {
        run_tail_once(&client, &args.log_group_name, start_time, &args.format).await
    }
}

/// Run a single pass of FilterLogEvents, paginating through all results.
async fn run_tail_once(
    client: &LogsClient,
    log_group: &str,
    start_time: i64,
    fmt: &TailFormat,
) -> Result<()> {
    let mut next_token: Option<String> = None;

    loop {
        let (events, token) = client
            .filter_log_events(log_group, start_time, next_token.as_deref())
            .await?;

        for event in &events {
            println!("{}", format_event(event, fmt));
        }

        match token {
            Some(t) => next_token = Some(t),
            None => break,
        }
    }

    Ok(())
}

/// Run in --follow mode: continuously poll for new events.
async fn run_tail_follow(
    client: &LogsClient,
    log_group: &str,
    start_time: i64,
    fmt: &TailFormat,
) -> Result<()> {
    let mut next_token: Option<String> = None;
    let mut seen_event_ids: HashSet<String> = HashSet::new();
    let mut current_start_time = start_time;

    loop {
        let (events, token) = client
            .filter_log_events(log_group, current_start_time, next_token.as_deref())
            .await?;

        for event in &events {
            // Deduplicate events we've already printed
            if !event.event_id.is_empty() && !seen_event_ids.insert(event.event_id.clone()) {
                continue;
            }
            println!("{}", format_event(event, fmt));

            // Advance start_time to the latest event timestamp
            if event.timestamp > current_start_time {
                current_start_time = event.timestamp;
            }
        }

        match token {
            Some(t) => {
                // More pages to fetch in this batch, don't sleep
                next_token = Some(t);
            }
            None => {
                // No more pages; sleep then poll again
                next_token = None;
                tokio::time::sleep(POLL_INTERVAL).await;

                // Limit seen_event_ids growth: if it gets large, keep only recent entries
                if seen_event_ids.len() > 10000 {
                    seen_event_ids.clear();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // Argument parsing tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_tail_args_minimal() {
        let args = vec!["/aws/lambda/my-func".to_string()];
        let parsed = parse_tail_args(&args).unwrap();
        assert_eq!(parsed.log_group_name, "/aws/lambda/my-func");
        assert!(parsed.since.is_none());
        assert!(!parsed.follow);
        assert_eq!(parsed.format, TailFormat::Short);
    }

    #[test]
    fn test_parse_tail_args_all_options() {
        let args = vec![
            "/aws/lambda/my-func".to_string(),
            "--since".to_string(),
            "1h".to_string(),
            "--follow".to_string(),
            "--format".to_string(),
            "detailed".to_string(),
        ];
        let parsed = parse_tail_args(&args).unwrap();
        assert_eq!(parsed.log_group_name, "/aws/lambda/my-func");
        assert_eq!(parsed.since, Some("1h".to_string()));
        assert!(parsed.follow);
        assert_eq!(parsed.format, TailFormat::Detailed);
    }

    #[test]
    fn test_parse_tail_args_json_format() {
        let args = vec![
            "my-log-group".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ];
        let parsed = parse_tail_args(&args).unwrap();
        assert_eq!(parsed.format, TailFormat::Json);
    }

    #[test]
    fn test_parse_tail_args_missing_log_group() {
        let args: Vec<String> = vec!["--follow".to_string()];
        let result = parse_tail_args(&args);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("log-group-name"), "Error: {}", err);
    }

    #[test]
    fn test_parse_tail_args_since_missing_value() {
        let args = vec!["my-group".to_string(), "--since".to_string()];
        let result = parse_tail_args(&args);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("--since requires a value"), "Error: {}", err);
    }

    #[test]
    fn test_parse_tail_args_format_missing_value() {
        let args = vec!["my-group".to_string(), "--format".to_string()];
        let result = parse_tail_args(&args);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("--format requires a value"), "Error: {}", err);
    }

    #[test]
    fn test_parse_tail_args_invalid_format() {
        let args = vec![
            "my-group".to_string(),
            "--format".to_string(),
            "xml".to_string(),
        ];
        let result = parse_tail_args(&args);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("Invalid format"), "Error: {}", err);
    }

    #[test]
    fn test_parse_tail_args_unknown_flag() {
        let args = vec!["my-group".to_string(), "--unknown".to_string()];
        let result = parse_tail_args(&args);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("Unknown argument"), "Error: {}", err);
    }

    #[test]
    fn test_parse_tail_args_duplicate_positional() {
        let args = vec!["group1".to_string(), "group2".to_string()];
        let result = parse_tail_args(&args);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("Unexpected positional"), "Error: {}", err);
    }

    #[test]
    fn test_parse_tail_args_options_before_positional() {
        let args = vec![
            "--follow".to_string(),
            "--since".to_string(),
            "5m".to_string(),
            "my-group".to_string(),
        ];
        let parsed = parse_tail_args(&args).unwrap();
        assert_eq!(parsed.log_group_name, "my-group");
        assert!(parsed.follow);
        assert_eq!(parsed.since, Some("5m".to_string()));
    }

    // ---------------------------------------------------------------
    // parse_since_time tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_since_relative_minutes() {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let result = parse_since_time("5m").unwrap();
        // Should be roughly 5 minutes ago (within 2 seconds tolerance)
        let expected = now_ms - 5 * 60 * 1000;
        assert!(
            (result - expected).abs() < 2000,
            "result={} expected={}",
            result,
            expected
        );
    }

    #[test]
    fn test_parse_since_relative_hours() {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let result = parse_since_time("1h").unwrap();
        let expected = now_ms - 3600 * 1000;
        assert!(
            (result - expected).abs() < 2000,
            "result={} expected={}",
            result,
            expected
        );
    }

    #[test]
    fn test_parse_since_relative_days() {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let result = parse_since_time("1d").unwrap();
        let expected = now_ms - 86400 * 1000;
        assert!(
            (result - expected).abs() < 2000,
            "result={} expected={}",
            result,
            expected
        );
    }

    #[test]
    fn test_parse_since_relative_seconds() {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let result = parse_since_time("30s").unwrap();
        let expected = now_ms - 30 * 1000;
        assert!(
            (result - expected).abs() < 2000,
            "result={} expected={}",
            result,
            expected
        );
    }

    #[test]
    fn test_parse_since_iso_timestamp_utc() {
        let result = parse_since_time("2024-01-15T10:30:00Z").unwrap();
        // 2024-01-15T10:30:00Z in epoch ms
        let expected = chrono::DateTime::parse_from_rfc3339("2024-01-15T10:30:00Z")
            .unwrap()
            .timestamp_millis();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_since_iso_timestamp_with_offset() {
        let result = parse_since_time("2024-01-15T10:30:00+05:00").unwrap();
        let expected = chrono::DateTime::parse_from_rfc3339("2024-01-15T10:30:00+05:00")
            .unwrap()
            .timestamp_millis();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_since_iso_timestamp_no_tz() {
        let result = parse_since_time("2024-01-15T10:30:00").unwrap();
        // Should be parsed as UTC
        let expected = chrono::NaiveDateTime::parse_from_str("2024-01-15T10:30:00", "%Y-%m-%dT%H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_millis();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_since_invalid() {
        let result = parse_since_time("yesterday");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_since_invalid_unit() {
        let result = parse_since_time("5w");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_since_zero_minutes() {
        // "0m" should be approximately now
        let now_ms = chrono::Utc::now().timestamp_millis();
        let result = parse_since_time("0m").unwrap();
        assert!(
            (result - now_ms).abs() < 2000,
            "result={} now={}",
            result,
            now_ms
        );
    }

    // ---------------------------------------------------------------
    // Format tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_format_short() {
        assert_eq!(parse_format("short").unwrap(), TailFormat::Short);
    }

    #[test]
    fn test_parse_format_detailed() {
        assert_eq!(parse_format("detailed").unwrap(), TailFormat::Detailed);
    }

    #[test]
    fn test_parse_format_json() {
        assert_eq!(parse_format("json").unwrap(), TailFormat::Json);
    }

    #[test]
    fn test_parse_format_invalid() {
        assert!(parse_format("xml").is_err());
        assert!(parse_format("").is_err());
        assert!(parse_format("SHORT").is_err());
    }

    // ---------------------------------------------------------------
    // Output formatting tests
    // ---------------------------------------------------------------

    #[test]
    fn test_format_event_short() {
        let event = LogEvent {
            timestamp: 1705314600000, // 2024-01-15T10:30:00Z
            message: "Hello world\n".to_string(),
            log_stream_name: "stream-1".to_string(),
            event_id: "abc123".to_string(),
        };
        let output = format_event(&event, &TailFormat::Short);
        assert_eq!(output, "Hello world");
    }

    #[test]
    fn test_format_event_detailed() {
        let event = LogEvent {
            timestamp: 1705314600000,
            message: "Hello world\n".to_string(),
            log_stream_name: "stream-1".to_string(),
            event_id: "abc123".to_string(),
        };
        let output = format_event(&event, &TailFormat::Detailed);
        assert!(output.contains("stream-1"), "Output: {}", output);
        assert!(output.contains("Hello world"), "Output: {}", output);
        assert!(output.contains("2024-01-15T"), "Output: {}", output);
    }

    #[test]
    fn test_format_event_json() {
        let event = LogEvent {
            timestamp: 1705314600000,
            message: "Hello world\n".to_string(),
            log_stream_name: "stream-1".to_string(),
            event_id: "abc123".to_string(),
        };
        let output = format_event(&event, &TailFormat::Json);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["message"], "Hello world");
        assert_eq!(parsed["logStreamName"], "stream-1");
        assert_eq!(parsed["eventId"], "abc123");
        assert!(parsed["timestamp"].as_str().unwrap().contains("2024-01-15T"));
    }

    #[test]
    fn test_format_event_short_no_trailing_newline() {
        let event = LogEvent {
            timestamp: 1705314600000,
            message: "No newline".to_string(),
            log_stream_name: "stream-1".to_string(),
            event_id: "abc".to_string(),
        };
        let output = format_event(&event, &TailFormat::Short);
        assert_eq!(output, "No newline");
    }

    // ---------------------------------------------------------------
    // Timestamp formatting tests
    // ---------------------------------------------------------------

    #[test]
    fn test_format_timestamp() {
        let ts = format_timestamp(1705314600000);
        assert_eq!(ts, "2024-01-15T10:30:00.000Z");
    }

    #[test]
    fn test_format_timestamp_with_millis() {
        let ts = format_timestamp(1705314600123);
        assert_eq!(ts, "2024-01-15T10:30:00.123Z");
    }
}
