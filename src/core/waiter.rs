use anyhow::{Context, Result};
use heck::ToKebabCase;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

/// Configuration for a single waiter, loaded from waiters-2.json.
#[derive(Debug, Clone)]
pub struct WaiterConfig {
    /// Seconds between polls.
    pub delay: u64,
    /// Maximum number of poll attempts before timing out.
    pub max_attempts: u32,
    /// The PascalCase operation name to call (e.g., "DescribeInstances").
    pub operation: String,
    /// Ordered list of acceptors evaluated after each poll.
    pub acceptors: Vec<Acceptor>,
}

/// A single acceptor rule within a waiter.
#[derive(Debug, Clone)]
pub struct Acceptor {
    /// How to match the response.
    pub matcher: MatcherType,
    /// The value to compare against.
    pub expected: Value,
    /// What to do when this acceptor matches.
    pub state: AcceptorState,
    /// JMESPath expression for path/pathAll/pathAny matchers.
    pub argument: Option<String>,
}

/// The type of matching logic for an acceptor.
#[derive(Debug, Clone, PartialEq)]
pub enum MatcherType {
    /// Evaluate JMESPath on response, compare result to expected.
    Path,
    /// Evaluate JMESPath (must yield array), ALL elements must equal expected.
    PathAll,
    /// Evaluate JMESPath (must yield array), ANY element equals expected.
    PathAny,
    /// Compare HTTP status code to expected integer.
    Status,
    /// Compare AWS error code string to expected string.
    Error,
}

/// The resulting state when an acceptor matches.
#[derive(Debug, Clone, PartialEq)]
pub enum AcceptorState {
    /// Waiter succeeded; stop polling.
    Success,
    /// Waiter failed; return error.
    Failure,
    /// Expected transient condition; continue polling.
    Retry,
}

/// Detailed result of acceptor evaluation, including which acceptor matched.
#[derive(Debug, Clone)]
pub struct AcceptorMatch {
    /// The resulting state from the matching acceptor.
    pub state: AcceptorState,
    /// The expected value of the matching acceptor (used for descriptive messages).
    pub expected: Value,
    /// The JMESPath argument of the matching acceptor, if any.
    pub argument: Option<String>,
    /// The matcher type of the matching acceptor.
    pub matcher: MatcherType,
}

/// Progress reporter for waiter operations.
///
/// Writes status messages to stderr during the wait loop.
pub struct WaiterProgress<W: Write> {
    writer: W,
    waiter_cli_name: String,
    max_attempts: u32,
}

impl<W: Write> WaiterProgress<W> {
    /// Create a new progress reporter.
    pub fn new(writer: W, waiter_cli_name: &str, max_attempts: u32) -> Self {
        Self {
            writer,
            waiter_cli_name: waiter_cli_name.to_string(),
            max_attempts,
        }
    }

    /// Print the initial "Waiting for..." message.
    pub fn starting(&mut self) {
        let _ = writeln!(
            self.writer,
            "Waiting for {}...",
            self.waiter_cli_name
        );
    }

    /// Print attempt progress after each poll.
    pub fn poll_attempt(&mut self, attempt: u32) {
        let _ = writeln!(
            self.writer,
            "Waiting for {}... (attempt {}/{})",
            self.waiter_cli_name, attempt, self.max_attempts
        );
    }

    /// Print success message.
    pub fn succeeded(&mut self) {
        let _ = writeln!(
            self.writer,
            "Waiter {} succeeded",
            self.waiter_cli_name
        );
    }

    /// Print a timeout message when max attempts are exceeded.
    pub fn timed_out(&mut self) {
        let _ = writeln!(
            self.writer,
            "Waiter {} timed out after {} attempts",
            self.waiter_cli_name, self.max_attempts
        );
    }

    /// Print a failure message with details from the matching acceptor.
    pub fn failed(&mut self, acceptor_match: &AcceptorMatch, response: &Value) {
        let detail = format_failure_detail(
            &self.waiter_cli_name,
            acceptor_match,
            response,
        );
        let _ = writeln!(self.writer, "{}", detail);
    }
}

/// Format a human-friendly failure detail message from a waiter failure acceptor match.
///
/// This inspects the acceptor's matcher type, argument, and the response to build a
/// descriptive message such as:
///   "Waiter instance-running failed: Instance entered terminated state"
pub fn format_failure_detail(
    waiter_cli_name: &str,
    acceptor_match: &AcceptorMatch,
    response: &Value,
) -> String {
    let base = format!("Waiter {} failed", waiter_cli_name);

    match &acceptor_match.matcher {
        MatcherType::PathAll | MatcherType::PathAny | MatcherType::Path => {
            // Try to extract what actual value was observed
            if let Some(ref arg) = acceptor_match.argument {
                let observed = evaluate_jmespath(arg, response);
                let expected_str = format_value_brief(&acceptor_match.expected);
                let observed_str = format_value_brief(&observed);
                format!(
                    "{}: {} matched expected value {}. Observed: {}",
                    base, arg, expected_str, observed_str
                )
            } else {
                format!("{}: failure condition matched", base)
            }
        }
        MatcherType::Error => {
            let code = acceptor_match
                .expected
                .as_str()
                .unwrap_or("unknown error");
            format!("{}: received error {}", base, code)
        }
        MatcherType::Status => {
            let status = acceptor_match
                .expected
                .as_u64()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            format!("{}: received HTTP status {}", base, status)
        }
    }
}

/// Format a JSON value briefly for display in progress/error messages.
fn format_value_brief(v: &Value) -> String {
    match v {
        Value::String(s) => format!("\"{}\"", s),
        Value::Array(arr) if arr.len() <= 5 => {
            let items: Vec<String> = arr.iter().map(format_value_brief).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().take(3).map(format_value_brief).collect();
            format!("[{}, ... ({} total)]", items.join(", "), arr.len())
        }
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

/// Format a timeout error message for display.
pub fn format_timeout_message(waiter_cli_name: &str, max_attempts: u32) -> String {
    format!(
        "Waiter {} timed out after {} attempts",
        waiter_cli_name, max_attempts
    )
}

/// Load waiter definitions from waiters-2.json in the given service version directory.
///
/// Returns an empty map if the file does not exist.
#[allow(dead_code)]
pub fn load_waiters(service_version_dir: &Path) -> Result<HashMap<String, WaiterConfig>> {
    let waiters_path = service_version_dir.join("waiters-2.json");
    if !waiters_path.exists() {
        return Ok(HashMap::new());
    }

    let content = std::fs::read_to_string(&waiters_path)
        .with_context(|| format!("Failed to read waiters file: {}", waiters_path.display()))?;

    parse_waiters(&content)
}

/// Parse the JSON content of a waiters-2.json file.
pub fn parse_waiters(json_str: &str) -> Result<HashMap<String, WaiterConfig>> {
    let raw: Value =
        serde_json::from_str(json_str).context("Failed to parse waiters-2.json")?;

    let waiters_obj = match raw.get("waiters").and_then(|v| v.as_object()) {
        Some(w) => w,
        None => return Ok(HashMap::new()),
    };

    let mut result = HashMap::new();

    for (name, config) in waiters_obj {
        let delay = config
            .get("delay")
            .and_then(|v| v.as_u64())
            .unwrap_or(15);

        let max_attempts = config
            .get("maxAttempts")
            .and_then(|v| v.as_u64())
            .unwrap_or(25) as u32;

        let operation = config
            .get("operation")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let acceptors = config
            .get("acceptors")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(parse_acceptor)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        if operation.is_empty() {
            continue;
        }

        result.insert(
            name.clone(),
            WaiterConfig {
                delay,
                max_attempts,
                operation,
                acceptors,
            },
        );
    }

    Ok(result)
}

/// Parse a single acceptor JSON object.
fn parse_acceptor(value: &Value) -> Option<Acceptor> {
    let matcher_str = value.get("matcher")?.as_str()?;
    let matcher = match matcher_str {
        "path" => MatcherType::Path,
        "pathAll" => MatcherType::PathAll,
        "pathAny" => MatcherType::PathAny,
        "status" => MatcherType::Status,
        "error" => MatcherType::Error,
        _ => return None,
    };

    let state_str = value.get("state")?.as_str()?;
    let state = match state_str {
        "success" => AcceptorState::Success,
        "failure" => AcceptorState::Failure,
        "retry" => AcceptorState::Retry,
        _ => return None,
    };

    let expected = value.get("expected")?.clone();

    let argument = value
        .get("argument")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Some(Acceptor {
        matcher,
        expected,
        state,
        argument,
    })
}

/// Evaluate acceptors in order against a response. Returns the state of the first matching
/// acceptor, or `None` if no acceptor matches (implying an implicit retry).
#[allow(dead_code)]
pub fn evaluate_acceptors(
    acceptors: &[Acceptor],
    response: &Value,
    status: u16,
    error_code: Option<&str>,
) -> Option<AcceptorState> {
    for acceptor in acceptors {
        if match_acceptor(acceptor, response, status, error_code) {
            return Some(acceptor.state.clone());
        }
    }
    None
}

/// Evaluate acceptors in order and return detailed information about the matching acceptor.
///
/// Returns `None` if no acceptor matches (implying an implicit retry).
pub fn evaluate_acceptors_detailed(
    acceptors: &[Acceptor],
    response: &Value,
    status: u16,
    error_code: Option<&str>,
) -> Option<AcceptorMatch> {
    for acceptor in acceptors {
        if match_acceptor(acceptor, response, status, error_code) {
            return Some(AcceptorMatch {
                state: acceptor.state.clone(),
                expected: acceptor.expected.clone(),
                argument: acceptor.argument.clone(),
                matcher: acceptor.matcher.clone(),
            });
        }
    }
    None
}

/// Check whether a single acceptor matches the given response context.
pub fn match_acceptor(
    acceptor: &Acceptor,
    response: &Value,
    status: u16,
    error_code: Option<&str>,
) -> bool {
    match &acceptor.matcher {
        MatcherType::Path => {
            if let Some(ref arg) = acceptor.argument {
                let result = evaluate_jmespath(arg, response);
                values_equal(&result, &acceptor.expected)
            } else {
                false
            }
        }
        MatcherType::PathAll => {
            if let Some(ref arg) = acceptor.argument {
                let result = evaluate_jmespath(arg, response);
                match result {
                    Value::Array(ref arr) => {
                        !arr.is_empty() && arr.iter().all(|v| values_equal(v, &acceptor.expected))
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        MatcherType::PathAny => {
            if let Some(ref arg) = acceptor.argument {
                let result = evaluate_jmespath(arg, response);
                match result {
                    Value::Array(ref arr) => {
                        arr.iter().any(|v| values_equal(v, &acceptor.expected))
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        MatcherType::Status => {
            if let Some(expected_status) = acceptor.expected.as_u64() {
                status as u64 == expected_status
            } else {
                false
            }
        }
        MatcherType::Error => {
            if let Some(expected_code) = acceptor.expected.as_str() {
                error_code == Some(expected_code)
            } else {
                false
            }
        }
    }
}

/// Compare two JSON values for equality, handling cross-type string comparisons.
///
/// Waiter expected values are often strings while JMESPath results may also be strings,
/// booleans, or numbers. This does a straightforward `==` comparison.
fn values_equal(a: &Value, b: &Value) -> bool {
    a == b
}

/// Convert a PascalCase waiter name to kebab-case CLI form.
///
/// Examples: "InstanceRunning" -> "instance-running", "TableExists" -> "table-exists"
pub fn waiter_name_to_cli(name: &str) -> String {
    name.to_kebab_case()
}

/// Find the original PascalCase waiter name that matches the given CLI (kebab-case) name.
///
/// Returns `None` if no waiter matches.
pub fn cli_to_waiter_name(
    cli_name: &str,
    waiters: &HashMap<String, WaiterConfig>,
) -> Option<String> {
    for key in waiters.keys() {
        if waiter_name_to_cli(key) == cli_name {
            return Some(key.clone());
        }
    }
    None
}

/// Simple JMESPath evaluator for the subset of expressions used by waiter acceptors.
///
/// Supports:
/// - Simple field access: `Status`
/// - Dot paths: `Table.TableStatus`
/// - Array flatten with field projection: `Reservations[].Instances[].State.Name`
///
/// This is intentionally minimal. Complex expressions (e.g., those using `length()`,
/// comparisons, filters) are not supported and will return `Value::Null`.
pub fn evaluate_jmespath(expr: &str, data: &Value) -> Value {
    // Split the expression into segments on '.'
    // But we need to handle '[]' carefully: "Reservations[].Instances[].State.Name"
    // segments: ["Reservations[]", "Instances[]", "State", "Name"]

    let segments = split_jmespath_segments(expr);

    if segments.is_empty() {
        return Value::Null;
    }

    eval_segments(&segments, data)
}

/// Split a JMESPath expression into segments, respecting `[]` as part of the preceding field name.
///
/// "Reservations[].Instances[].State.Name" -> ["Reservations[]", "Instances[]", "State", "Name"]
/// "Table.TableStatus" -> ["Table", "TableStatus"]
/// "Status" -> ["Status"]
fn split_jmespath_segments(expr: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();

    let mut chars = expr.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                if !current.is_empty() {
                    segments.push(current.clone());
                    current.clear();
                }
            }
            '[' => {
                // Consume until ']'
                current.push('[');
                for inner in chars.by_ref() {
                    current.push(inner);
                    if inner == ']' {
                        break;
                    }
                }
                // If the next char is '.', consume it (segment boundary after [])
                if chars.peek() == Some(&'.') {
                    segments.push(current.clone());
                    current.clear();
                    chars.next(); // consume the '.'
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() {
        segments.push(current);
    }

    segments
}

/// Recursively evaluate segments against a JSON value.
fn eval_segments(segments: &[String], data: &Value) -> Value {
    if segments.is_empty() {
        return data.clone();
    }

    let segment = &segments[0];
    let rest = &segments[1..];

    if let Some(field) = segment.strip_suffix("[]") {
        // Array flatten: access field, then flatten
        let array_val = if field.is_empty() {
            // Bare "[]" means flatten the current value
            data.clone()
        } else {
            // Access the field first
            match data.get(field) {
                Some(v) => v.clone(),
                None => return Value::Null,
            }
        };

        match array_val {
            Value::Array(arr) => {
                if rest.is_empty() {
                    // No more segments; return the flattened array
                    Value::Array(arr)
                } else {
                    // Apply remaining segments to each element and flatten results
                    let mut results = Vec::new();
                    for item in &arr {
                        let sub = eval_segments(rest, item);
                        match sub {
                            Value::Array(inner) => results.extend(inner),
                            Value::Null => {}
                            other => results.push(other),
                        }
                    }
                    Value::Array(results)
                }
            }
            _ => Value::Null,
        }
    } else {
        // Simple field access
        match data.get(segment.as_str()) {
            Some(v) => eval_segments(rest, v),
            None => Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---------------------------------------------------------------
    // 1. Load real EC2 waiters
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_load_ec2_waiters() {
        let path = Path::new("models/ec2/2016-11-15");
        if !path.exists() {
            eprintln!("Skipping: EC2 model not copied yet");
            return;
        }
        let waiters = load_waiters(path).unwrap();
        assert!(!waiters.is_empty());
        assert!(waiters.contains_key("InstanceRunning"));
        assert!(waiters.contains_key("InstanceStopped"));
        assert!(waiters.contains_key("InstanceTerminated"));

        let running = &waiters["InstanceRunning"];
        assert_eq!(running.delay, 15);
        assert_eq!(running.max_attempts, 40);
        assert_eq!(running.operation, "DescribeInstances");
        assert!(!running.acceptors.is_empty());

        // First acceptor should be pathAll for "running"
        let first = &running.acceptors[0];
        assert_eq!(first.matcher, MatcherType::PathAll);
        assert_eq!(first.expected, json!("running"));
        assert_eq!(first.state, AcceptorState::Success);
        assert_eq!(
            first.argument.as_deref(),
            Some("Reservations[].Instances[].State.Name")
        );
    }

    // ---------------------------------------------------------------
    // 2. Path matcher evaluates JMESPath and compares
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_acceptor_path_match() {
        let acceptor = Acceptor {
            matcher: MatcherType::Path,
            expected: json!("ACTIVE"),
            state: AcceptorState::Success,
            argument: Some("Table.TableStatus".to_string()),
        };

        let response = json!({
            "Table": {
                "TableName": "my-table",
                "TableStatus": "ACTIVE"
            }
        });

        assert!(match_acceptor(&acceptor, &response, 200, None));

        // Non-matching value
        let response2 = json!({
            "Table": {
                "TableName": "my-table",
                "TableStatus": "CREATING"
            }
        });
        assert!(!match_acceptor(&acceptor, &response2, 200, None));
    }

    // ---------------------------------------------------------------
    // 3. PathAll: all elements must match
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_acceptor_path_all_match() {
        let acceptor = Acceptor {
            matcher: MatcherType::PathAll,
            expected: json!("running"),
            state: AcceptorState::Success,
            argument: Some("Reservations[].Instances[].State.Name".to_string()),
        };

        // All running -> match
        let response = json!({
            "Reservations": [
                {
                    "Instances": [
                        {"State": {"Name": "running"}},
                        {"State": {"Name": "running"}}
                    ]
                }
            ]
        });
        assert!(match_acceptor(&acceptor, &response, 200, None));

        // One not running -> no match
        let response2 = json!({
            "Reservations": [
                {
                    "Instances": [
                        {"State": {"Name": "running"}},
                        {"State": {"Name": "pending"}}
                    ]
                }
            ]
        });
        assert!(!match_acceptor(&acceptor, &response2, 200, None));

        // Empty array -> no match (pathAll requires non-empty)
        let response3 = json!({
            "Reservations": []
        });
        assert!(!match_acceptor(&acceptor, &response3, 200, None));
    }

    // ---------------------------------------------------------------
    // 4. PathAny: any element matches
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_acceptor_path_any_match() {
        let acceptor = Acceptor {
            matcher: MatcherType::PathAny,
            expected: json!("shutting-down"),
            state: AcceptorState::Failure,
            argument: Some("Reservations[].Instances[].State.Name".to_string()),
        };

        // One shutting-down -> match
        let response = json!({
            "Reservations": [
                {
                    "Instances": [
                        {"State": {"Name": "running"}},
                        {"State": {"Name": "shutting-down"}}
                    ]
                }
            ]
        });
        assert!(match_acceptor(&acceptor, &response, 200, None));

        // None shutting-down -> no match
        let response2 = json!({
            "Reservations": [
                {
                    "Instances": [
                        {"State": {"Name": "running"}},
                        {"State": {"Name": "running"}}
                    ]
                }
            ]
        });
        assert!(!match_acceptor(&acceptor, &response2, 200, None));
    }

    // ---------------------------------------------------------------
    // 5. Status matcher compares HTTP status
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_acceptor_status_match() {
        let acceptor = Acceptor {
            matcher: MatcherType::Status,
            expected: json!(200),
            state: AcceptorState::Success,
            argument: None,
        };

        assert!(match_acceptor(&acceptor, &json!({}), 200, None));
        assert!(!match_acceptor(&acceptor, &json!({}), 404, None));
        assert!(!match_acceptor(&acceptor, &json!({}), 500, None));
    }

    // ---------------------------------------------------------------
    // 6. Error matcher compares error code
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_acceptor_error_match() {
        let acceptor = Acceptor {
            matcher: MatcherType::Error,
            expected: json!("ResourceNotFoundException"),
            state: AcceptorState::Success,
            argument: None,
        };

        assert!(match_acceptor(
            &acceptor,
            &json!({}),
            400,
            Some("ResourceNotFoundException")
        ));
        assert!(!match_acceptor(
            &acceptor,
            &json!({}),
            400,
            Some("ValidationException")
        ));
        assert!(!match_acceptor(&acceptor, &json!({}), 200, None));
    }

    // ---------------------------------------------------------------
    // 7. Acceptors evaluated in order; first match wins
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_evaluate_acceptors_first_match_wins() {
        let acceptors = vec![
            Acceptor {
                matcher: MatcherType::Path,
                expected: json!("ACTIVE"),
                state: AcceptorState::Success,
                argument: Some("Status".to_string()),
            },
            Acceptor {
                matcher: MatcherType::Path,
                expected: json!("ACTIVE"),
                state: AcceptorState::Failure,
                argument: Some("Status".to_string()),
            },
        ];

        let response = json!({"Status": "ACTIVE"});
        // Both match, but first one (Success) should win
        let result = evaluate_acceptors(&acceptors, &response, 200, None);
        assert_eq!(result, Some(AcceptorState::Success));
    }

    // ---------------------------------------------------------------
    // 8. No acceptor matches -> returns None
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_evaluate_acceptors_no_match() {
        let acceptors = vec![
            Acceptor {
                matcher: MatcherType::Path,
                expected: json!("ACTIVE"),
                state: AcceptorState::Success,
                argument: Some("Status".to_string()),
            },
            Acceptor {
                matcher: MatcherType::Error,
                expected: json!("ResourceNotFoundException"),
                state: AcceptorState::Retry,
                argument: None,
            },
        ];

        let response = json!({"Status": "CREATING"});
        let result = evaluate_acceptors(&acceptors, &response, 200, None);
        assert_eq!(result, None);
    }

    // ---------------------------------------------------------------
    // 9. PascalCase to kebab-case conversion
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_name_conversion() {
        assert_eq!(waiter_name_to_cli("InstanceRunning"), "instance-running");
        assert_eq!(waiter_name_to_cli("TableExists"), "table-exists");
        assert_eq!(waiter_name_to_cli("TableNotExists"), "table-not-exists");
        assert_eq!(
            waiter_name_to_cli("BundleTaskComplete"),
            "bundle-task-complete"
        );
        assert_eq!(
            waiter_name_to_cli("CustomerGatewayAvailable"),
            "customer-gateway-available"
        );

        // Round-trip via cli_to_waiter_name
        let mut waiters = HashMap::new();
        waiters.insert(
            "InstanceRunning".to_string(),
            WaiterConfig {
                delay: 15,
                max_attempts: 40,
                operation: "DescribeInstances".to_string(),
                acceptors: vec![],
            },
        );
        waiters.insert(
            "TableExists".to_string(),
            WaiterConfig {
                delay: 20,
                max_attempts: 25,
                operation: "DescribeTable".to_string(),
                acceptors: vec![],
            },
        );

        assert_eq!(
            cli_to_waiter_name("instance-running", &waiters),
            Some("InstanceRunning".to_string())
        );
        assert_eq!(
            cli_to_waiter_name("table-exists", &waiters),
            Some("TableExists".to_string())
        );
        assert_eq!(cli_to_waiter_name("nonexistent-waiter", &waiters), None);
    }

    // ---------------------------------------------------------------
    // 10. JMESPath: simple field access
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_jmespath_simple_field() {
        let data = json!({"Status": "ACTIVE"});
        assert_eq!(evaluate_jmespath("Status", &data), json!("ACTIVE"));

        let data2 = json!({"Count": 42});
        assert_eq!(evaluate_jmespath("Count", &data2), json!(42));

        // Missing field
        assert_eq!(evaluate_jmespath("Missing", &data), Value::Null);
    }

    // ---------------------------------------------------------------
    // 11. JMESPath: nested field access (dot path)
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_jmespath_dot_path() {
        let data = json!({
            "Table": {
                "TableStatus": "ACTIVE",
                "TableName": "my-table"
            }
        });
        assert_eq!(
            evaluate_jmespath("Table.TableStatus", &data),
            json!("ACTIVE")
        );
        assert_eq!(
            evaluate_jmespath("Table.TableName", &data),
            json!("my-table")
        );

        // Deeper nesting
        let data2 = json!({
            "ExportDescription": {
                "ExportStatus": "COMPLETED"
            }
        });
        assert_eq!(
            evaluate_jmespath("ExportDescription.ExportStatus", &data2),
            json!("COMPLETED")
        );

        // Missing intermediate field
        assert_eq!(evaluate_jmespath("Missing.Field", &data), Value::Null);
    }

    // ---------------------------------------------------------------
    // 12. JMESPath: array flatten
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_jmespath_array_flatten() {
        let data = json!({
            "Reservations": [
                {
                    "Instances": [
                        {"State": {"Name": "running"}},
                        {"State": {"Name": "running"}}
                    ]
                },
                {
                    "Instances": [
                        {"State": {"Name": "stopped"}}
                    ]
                }
            ]
        });

        let result = evaluate_jmespath("Reservations[].Instances[].State.Name", &data);
        assert_eq!(result, json!(["running", "running", "stopped"]));

        // Single level flatten
        let data2 = json!({
            "BundleTasks": [
                {"State": "complete"},
                {"State": "complete"}
            ]
        });
        let result2 = evaluate_jmespath("BundleTasks[].State", &data2);
        assert_eq!(result2, json!(["complete", "complete"]));

        // Empty array
        let data3 = json!({
            "Reservations": []
        });
        let result3 = evaluate_jmespath("Reservations[].Instances[].State.Name", &data3);
        assert_eq!(result3, json!([]));
    }

    // ---------------------------------------------------------------
    // 13. WaiterProgress: starting message
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_progress_starting_message() {
        let mut buf = Vec::new();
        {
            let mut progress = WaiterProgress::new(&mut buf, "instance-running", 40);
            progress.starting();
        }
        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output, "Waiting for instance-running...\n");
    }

    // ---------------------------------------------------------------
    // 14. WaiterProgress: poll attempt message
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_progress_poll_attempt_message() {
        let mut buf = Vec::new();
        {
            let mut progress = WaiterProgress::new(&mut buf, "instance-running", 40);
            progress.poll_attempt(3);
        }
        let output = String::from_utf8(buf).unwrap();
        assert_eq!(
            output,
            "Waiting for instance-running... (attempt 3/40)\n"
        );
    }

    // ---------------------------------------------------------------
    // 15. WaiterProgress: success message
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_progress_success_message() {
        let mut buf = Vec::new();
        {
            let mut progress = WaiterProgress::new(&mut buf, "table-exists", 25);
            progress.succeeded();
        }
        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output, "Waiter table-exists succeeded\n");
    }

    // ---------------------------------------------------------------
    // 16. WaiterProgress: timeout message
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_progress_timeout_message() {
        let mut buf = Vec::new();
        {
            let mut progress = WaiterProgress::new(&mut buf, "instance-running", 40);
            progress.timed_out();
        }
        let output = String::from_utf8(buf).unwrap();
        assert_eq!(
            output,
            "Waiter instance-running timed out after 40 attempts\n"
        );
    }

    // ---------------------------------------------------------------
    // 17. format_timeout_message standalone function
    // ---------------------------------------------------------------

    #[test]
    fn test_format_timeout_message() {
        assert_eq!(
            format_timeout_message("instance-running", 40),
            "Waiter instance-running timed out after 40 attempts"
        );
        assert_eq!(
            format_timeout_message("table-exists", 25),
            "Waiter table-exists timed out after 25 attempts"
        );
    }

    // ---------------------------------------------------------------
    // 18. Failure detail: path-based acceptor with observed state
    // ---------------------------------------------------------------

    #[test]
    fn test_failure_detail_path_acceptor() {
        let acceptor_match = AcceptorMatch {
            state: AcceptorState::Failure,
            expected: json!("terminated"),
            argument: Some("Reservations[].Instances[].State.Name".to_string()),
            matcher: MatcherType::PathAny,
        };

        let response = json!({
            "Reservations": [
                {
                    "Instances": [
                        {"State": {"Name": "running"}},
                        {"State": {"Name": "terminated"}}
                    ]
                }
            ]
        });

        let detail = format_failure_detail("instance-running", &acceptor_match, &response);
        assert!(detail.starts_with("Waiter instance-running failed:"));
        assert!(detail.contains("Reservations[].Instances[].State.Name"));
        assert!(detail.contains("\"terminated\""));
        assert!(detail.contains("Observed:"));
    }

    // ---------------------------------------------------------------
    // 19. Failure detail: error code acceptor
    // ---------------------------------------------------------------

    #[test]
    fn test_failure_detail_error_acceptor() {
        let acceptor_match = AcceptorMatch {
            state: AcceptorState::Failure,
            expected: json!("InvalidInstanceID.NotFound"),
            argument: None,
            matcher: MatcherType::Error,
        };

        let detail = format_failure_detail("instance-running", &acceptor_match, &json!({}));
        assert_eq!(
            detail,
            "Waiter instance-running failed: received error InvalidInstanceID.NotFound"
        );
    }

    // ---------------------------------------------------------------
    // 20. Failure detail: status code acceptor
    // ---------------------------------------------------------------

    #[test]
    fn test_failure_detail_status_acceptor() {
        let acceptor_match = AcceptorMatch {
            state: AcceptorState::Failure,
            expected: json!(500),
            argument: None,
            matcher: MatcherType::Status,
        };

        let detail = format_failure_detail("instance-running", &acceptor_match, &json!({}));
        assert_eq!(
            detail,
            "Waiter instance-running failed: received HTTP status 500"
        );
    }

    // ---------------------------------------------------------------
    // 21. evaluate_acceptors_detailed returns match info
    // ---------------------------------------------------------------

    #[test]
    fn test_evaluate_acceptors_detailed_returns_match() {
        let acceptors = vec![
            Acceptor {
                matcher: MatcherType::PathAny,
                expected: json!("terminated"),
                state: AcceptorState::Failure,
                argument: Some("Reservations[].Instances[].State.Name".to_string()),
            },
            Acceptor {
                matcher: MatcherType::PathAll,
                expected: json!("running"),
                state: AcceptorState::Success,
                argument: Some("Reservations[].Instances[].State.Name".to_string()),
            },
        ];

        let response = json!({
            "Reservations": [
                {
                    "Instances": [
                        {"State": {"Name": "terminated"}}
                    ]
                }
            ]
        });

        let result = evaluate_acceptors_detailed(&acceptors, &response, 200, None);
        assert!(result.is_some());
        let m = result.unwrap();
        assert_eq!(m.state, AcceptorState::Failure);
        assert_eq!(m.expected, json!("terminated"));
        assert_eq!(m.matcher, MatcherType::PathAny);
    }

    // ---------------------------------------------------------------
    // 22. evaluate_acceptors_detailed returns None when no match
    // ---------------------------------------------------------------

    #[test]
    fn test_evaluate_acceptors_detailed_no_match() {
        let acceptors = vec![Acceptor {
            matcher: MatcherType::Path,
            expected: json!("ACTIVE"),
            state: AcceptorState::Success,
            argument: Some("Status".to_string()),
        }];

        let response = json!({"Status": "CREATING"});
        let result = evaluate_acceptors_detailed(&acceptors, &response, 200, None);
        assert!(result.is_none());
    }

    // ---------------------------------------------------------------
    // 23. WaiterProgress: failure message via .failed()
    // ---------------------------------------------------------------

    #[test]
    fn test_waiter_progress_failure_output() {
        let mut buf = Vec::new();
        {
            let mut progress = WaiterProgress::new(&mut buf, "instance-running", 40);
            let acceptor_match = AcceptorMatch {
                state: AcceptorState::Failure,
                expected: json!("terminated"),
                argument: Some("Reservations[].Instances[].State.Name".to_string()),
                matcher: MatcherType::PathAny,
            };
            let response = json!({
                "Reservations": [{
                    "Instances": [{"State": {"Name": "terminated"}}]
                }]
            });
            progress.failed(&acceptor_match, &response);
        }
        let output = String::from_utf8(buf).unwrap();
        assert!(output.starts_with("Waiter instance-running failed:"));
        assert!(output.contains("\"terminated\""));
    }

    // ---------------------------------------------------------------
    // 24. format_value_brief formatting
    // ---------------------------------------------------------------

    #[test]
    fn test_format_value_brief() {
        assert_eq!(format_value_brief(&json!("running")), "\"running\"");
        assert_eq!(format_value_brief(&json!(42)), "42");
        assert_eq!(format_value_brief(&Value::Null), "null");
        assert_eq!(format_value_brief(&json!(true)), "true");
        assert_eq!(
            format_value_brief(&json!(["a", "b"])),
            "[\"a\", \"b\"]"
        );
        // Large array is truncated
        let large = json!(["a", "b", "c", "d", "e", "f", "g"]);
        let brief = format_value_brief(&large);
        assert!(brief.contains("... (7 total)"));
    }
}
