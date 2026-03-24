use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Retry mode
// ---------------------------------------------------------------------------

/// Retry mode matching AWS SDK behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryMode {
    /// Simple retry with no rate limiting. Max 5 attempts.
    Legacy,
    /// Exponential backoff with jitter. Max 3 attempts. (default)
    Standard,
    /// Like standard but adds client-side rate limiting via token bucket.
    Adaptive,
}

// ---------------------------------------------------------------------------
// Retry configuration
// ---------------------------------------------------------------------------

/// Configuration controlling retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub mode: RetryMode,
    pub max_attempts: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::from_mode(RetryMode::Standard)
    }
}

impl RetryConfig {
    /// Build a config with mode-appropriate defaults.
    pub fn from_mode(mode: RetryMode) -> Self {
        let max_attempts = match mode {
            RetryMode::Legacy => 5,
            RetryMode::Standard | RetryMode::Adaptive => 3,
        };
        Self { mode, max_attempts }
    }
}

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

/// Classification of an error for retry purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClassification {
    /// Transient error (5xx, network) -- eligible for retry.
    TransientError,
    /// Throttling error (429, specific error codes) -- eligible for retry;
    /// adaptive mode additionally applies rate limiting.
    ThrottlingError,
    /// Non-retryable error (4xx except 429, auth errors, etc).
    NonRetryable,
}

/// Well-known throttling error codes returned by AWS services.
const THROTTLE_ERROR_CODES: &[&str] = &[
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottledException",
    "TooManyRequestsException",
    "RequestLimitExceeded",
    "BandwidthLimitExceeded",
    "SlowDown",
    "PriorRequestNotComplete",
    "EC2ThrottledException",
];

/// Classify an HTTP response (or network failure) for retry purposes.
///
/// * `status`           -- HTTP status code (ignored when `is_network_error` is true).
/// * `error_code`       -- Optional AWS error code from the response body.
/// * `is_network_error` -- `true` when the request never received a response
///                         (connection refused, timeout, DNS failure, etc).
pub fn classify_error(
    status: u16,
    error_code: Option<&str>,
    is_network_error: bool,
) -> RetryClassification {
    // Network errors are always transient.
    if is_network_error {
        return RetryClassification::TransientError;
    }

    // Check error code first -- an explicit throttle code wins over status.
    if let Some(code) = error_code {
        if is_throttle_code(code) {
            return RetryClassification::ThrottlingError;
        }
    }

    // HTTP 429 is always throttling.
    if status == 429 {
        return RetryClassification::ThrottlingError;
    }

    // 5xx server errors are transient.
    if (500..=599).contains(&status) {
        return RetryClassification::TransientError;
    }

    RetryClassification::NonRetryable
}

/// Returns `true` if `code` matches one of the well-known throttle codes.
/// The comparison checks whether `code` *contains* any of the known tokens
/// (case-sensitive on the known tokens, but we also do a contains-check for
/// the substring "Throttl" to catch future variants).
fn is_throttle_code(code: &str) -> bool {
    for &known in THROTTLE_ERROR_CODES {
        if code == known {
            return true;
        }
    }
    // Catch-all: any code that contains "Throttl" or "TooManyRequests".
    if code.contains("Throttl") || code.contains("TooManyRequests") {
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// Retry decision
// ---------------------------------------------------------------------------

/// The outcome of evaluating whether a request should be retried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    /// Retry after sleeping for the given duration.
    RetryAfter(Duration),
    /// Do not retry.
    DontRetry,
}

/// Decide whether the request should be retried.
///
/// * `config`          -- The active retry configuration.
/// * `attempt`         -- The attempt number that just completed (1-based).
/// * `classification`  -- How the error was classified.
///
/// Returns `RetryDecision::RetryAfter(delay)` when a retry is warranted,
/// or `RetryDecision::DontRetry` otherwise.
pub fn should_retry(
    config: &RetryConfig,
    attempt: u32,
    classification: &RetryClassification,
) -> RetryDecision {
    // Non-retryable errors are never retried.
    if *classification == RetryClassification::NonRetryable {
        return RetryDecision::DontRetry;
    }

    // Check if we have exhausted our attempts budget.
    // `attempt` is 1-based; `max_attempts` is total attempts including the first.
    if attempt >= config.max_attempts {
        return RetryDecision::DontRetry;
    }

    // Compute delay.
    let delay = match config.mode {
        RetryMode::Legacy => {
            // Legacy: simple exponential backoff with jitter.
            calculate_backoff(attempt, BASE_DELAY_MS, MAX_DELAY_MS)
        }
        RetryMode::Standard => {
            calculate_backoff(attempt, BASE_DELAY_MS, MAX_DELAY_MS)
        }
        RetryMode::Adaptive => {
            calculate_backoff(attempt, BASE_DELAY_MS, MAX_DELAY_MS)
        }
    };

    RetryDecision::RetryAfter(delay)
}

// ---------------------------------------------------------------------------
// Backoff calculation
// ---------------------------------------------------------------------------

/// Base delay scaling factor in milliseconds.
const BASE_DELAY_MS: u64 = 100;

/// Maximum backoff cap in milliseconds (20 seconds).
const MAX_DELAY_MS: u64 = 20_000;

/// Calculate the maximum backoff delay **before** jitter for the given attempt.
///
/// Formula: min(base_delay_ms * 2^(attempt-1), max_delay_ms)
///
/// This is useful for tests that want to verify bounds without randomness.
pub fn calculate_backoff_max(attempt: u32, base_delay_ms: u64, max_delay_ms: u64) -> Duration {
    // Guard against overflow: cap the shift to 63.
    let shift = (attempt.saturating_sub(1)).min(63) as u32;
    let power = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    let computed = base_delay_ms.saturating_mul(power);
    let capped = computed.min(max_delay_ms);
    Duration::from_millis(capped)
}

/// Calculate the backoff delay with full jitter for the given attempt.
///
/// Full jitter: actual_delay = random(0, calculated_max_delay).
///
/// Uses a simple deterministic-ish entropy source (system time nanos mixed
/// with the attempt number) so no external `rand` crate is needed.  For
/// deterministic testing, use [`calculate_backoff_max`] which omits jitter.
pub fn calculate_backoff(attempt: u32, base_delay_ms: u64, max_delay_ms: u64) -> Duration {
    let max = calculate_backoff_max(attempt, base_delay_ms, max_delay_ms);
    let max_ms = max.as_millis() as u64;
    if max_ms == 0 {
        return Duration::ZERO;
    }

    // Simple entropy: mix system time nanos with the attempt counter.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::from_nanos(42))
        .as_nanos() as u64;

    // Simple hash / mix (splitmix-like step).
    let mut x = nanos.wrapping_add((attempt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;

    let jittered_ms = x % (max_ms + 1); // range [0, max_ms]
    Duration::from_millis(jittered_ms)
}

// ---------------------------------------------------------------------------
// Token bucket (for adaptive retry mode)
// ---------------------------------------------------------------------------

/// A thread-safe token bucket used by the adaptive retry mode to rate-limit
/// requests after receiving throttling errors.
///
/// * Starts with 500 tokens.
/// * Costs 5 tokens per retry after a throttling error.
/// * Refills 1 token per successful request.
pub struct TokenBucket {
    tokens: AtomicI64,
    max_tokens: i64,
    retry_cost: i64,
    refill_amount: i64,
}

impl TokenBucket {
    /// Create a new token bucket with default parameters.
    pub fn new() -> Self {
        Self {
            tokens: AtomicI64::new(500),
            max_tokens: 500,
            retry_cost: 5,
            refill_amount: 1,
        }
    }

    /// Create a token bucket with custom parameters (useful for testing).
    pub fn with_params(max_tokens: i64, retry_cost: i64, refill_amount: i64) -> Self {
        Self {
            tokens: AtomicI64::new(max_tokens),
            max_tokens,
            retry_cost,
            refill_amount,
        }
    }

    /// Current number of available tokens.
    pub fn available(&self) -> i64 {
        self.tokens.load(Ordering::Relaxed)
    }

    /// Try to acquire tokens for a retry after a throttling error.
    ///
    /// Returns `true` if tokens were successfully acquired, `false` if
    /// insufficient tokens are available.
    pub fn acquire(&self) -> bool {
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < self.retry_cost {
                return false;
            }
            let new = current - self.retry_cost;
            match self.tokens.compare_exchange(
                current,
                new,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue, // CAS retry
            }
        }
    }

    /// Refill the bucket after a successful request.
    pub fn refill(&self) {
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            let new = (current + self.refill_amount).min(self.max_tokens);
            if new == current {
                return; // already at max
            }
            match self.tokens.compare_exchange(
                current,
                new,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Config resolution from environment and ~/.aws/config
// ---------------------------------------------------------------------------

/// Parse a retry mode string into a RetryMode enum.
///
/// Recognized values: "legacy", "standard", "adaptive" (case-insensitive).
pub fn parse_retry_mode(s: &str) -> Option<RetryMode> {
    match s.to_lowercase().as_str() {
        "legacy" => Some(RetryMode::Legacy),
        "standard" => Some(RetryMode::Standard),
        "adaptive" => Some(RetryMode::Adaptive),
        _ => None,
    }
}

/// Build a RetryConfig by checking (in priority order):
///
/// 1. `AWS_MAX_ATTEMPTS` env var
/// 2. `max_attempts` in `~/.aws/config` for the given profile
/// 3. `AWS_RETRY_MODE` env var
/// 4. `retry_mode` in `~/.aws/config` for the given profile
/// 5. Default: standard mode, 3 attempts
///
/// `config_max_attempts` and `config_retry_mode` should be pre-read from
/// the config file by the caller.
pub fn resolve_retry_config(
    config_max_attempts: Option<&str>,
    config_retry_mode: Option<&str>,
) -> RetryConfig {
    resolve_retry_config_inner(
        config_max_attempts,
        config_retry_mode,
        std::env::var("AWS_MAX_ATTEMPTS").ok().as_deref(),
        std::env::var("AWS_RETRY_MODE").ok().as_deref(),
    )
}

/// Inner implementation that accepts explicit env values for testability.
fn resolve_retry_config_inner(
    config_max_attempts: Option<&str>,
    config_retry_mode: Option<&str>,
    env_max_attempts: Option<&str>,
    env_retry_mode: Option<&str>,
) -> RetryConfig {
    // Resolve retry mode: env var takes priority over config
    let mode = env_retry_mode
        .and_then(parse_retry_mode)
        .or_else(|| config_retry_mode.and_then(parse_retry_mode))
        .unwrap_or(RetryMode::Standard);

    let mut config = RetryConfig::from_mode(mode);

    // Resolve max_attempts (overrides mode default): env var takes priority
    if let Some(max) = env_max_attempts
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|&v| v >= 1)
    {
        config.max_attempts = max;
    } else if let Some(max) = config_max_attempts
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|&v| v >= 1)
    {
        config.max_attempts = max;
    }

    config
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Classification tests -----------------------------------------------

    #[test]
    fn test_retry_classification_5xx() {
        for status in [500, 501, 502, 503, 504] {
            let c = classify_error(status, None, false);
            assert_eq!(
                c,
                RetryClassification::TransientError,
                "HTTP {status} should be TransientError"
            );
        }
    }

    #[test]
    fn test_retry_classification_429() {
        let c = classify_error(429, None, false);
        assert_eq!(c, RetryClassification::ThrottlingError);
    }

    #[test]
    fn test_retry_classification_4xx_not_retryable() {
        for status in [400, 401, 403, 404] {
            let c = classify_error(status, None, false);
            assert_eq!(
                c,
                RetryClassification::NonRetryable,
                "HTTP {status} should be NonRetryable"
            );
        }
    }

    #[test]
    fn test_retry_classification_throttle_error_codes() {
        let throttle_codes = [
            "Throttling",
            "ThrottlingException",
            "TooManyRequestsException",
            "RequestLimitExceeded",
            "BandwidthLimitExceeded",
            "SlowDown",
            "PriorRequestNotComplete",
            "EC2ThrottledException",
        ];
        for code in throttle_codes {
            let c = classify_error(200, Some(code), false);
            assert_eq!(
                c,
                RetryClassification::ThrottlingError,
                "Error code '{code}' should be ThrottlingError"
            );
        }
    }

    #[test]
    fn test_retry_classification_network_error() {
        let c = classify_error(0, None, true);
        assert_eq!(c, RetryClassification::TransientError);
    }

    // -- Backoff tests ------------------------------------------------------

    #[test]
    fn test_retry_backoff_increases() {
        // Use the deterministic max (no jitter) to verify exponential growth.
        let d1 = calculate_backoff_max(1, BASE_DELAY_MS, MAX_DELAY_MS);
        let d2 = calculate_backoff_max(2, BASE_DELAY_MS, MAX_DELAY_MS);
        let d3 = calculate_backoff_max(3, BASE_DELAY_MS, MAX_DELAY_MS);
        let d4 = calculate_backoff_max(4, BASE_DELAY_MS, MAX_DELAY_MS);

        // 100, 200, 400, 800
        assert_eq!(d1, Duration::from_millis(100));
        assert_eq!(d2, Duration::from_millis(200));
        assert_eq!(d3, Duration::from_millis(400));
        assert_eq!(d4, Duration::from_millis(800));

        assert!(d2 > d1);
        assert!(d3 > d2);
        assert!(d4 > d3);
    }

    #[test]
    fn test_retry_backoff_capped() {
        // At attempt 20 the raw value would be huge, but must be capped at 20s.
        let d = calculate_backoff_max(20, BASE_DELAY_MS, MAX_DELAY_MS);
        assert_eq!(d, Duration::from_millis(MAX_DELAY_MS));

        // Also verify large attempt numbers don't panic or overflow.
        let d_big = calculate_backoff_max(100, BASE_DELAY_MS, MAX_DELAY_MS);
        assert_eq!(d_big, Duration::from_millis(MAX_DELAY_MS));
    }

    #[test]
    fn test_retry_backoff_jitter_within_bounds() {
        // Run many iterations and verify the jittered delay is always
        // between 0 and the calculated max (inclusive).
        for attempt in 1..=5 {
            let max = calculate_backoff_max(attempt, BASE_DELAY_MS, MAX_DELAY_MS);
            for _ in 0..100 {
                let d = calculate_backoff(attempt, BASE_DELAY_MS, MAX_DELAY_MS);
                assert!(
                    d <= max,
                    "Jittered delay {:?} exceeded max {:?} for attempt {attempt}",
                    d,
                    max
                );
            }
        }
    }

    // -- should_retry / max attempts tests ----------------------------------

    #[test]
    fn test_retry_max_attempts_standard() {
        let config = RetryConfig::from_mode(RetryMode::Standard);
        assert_eq!(config.max_attempts, 3);

        // Attempt 1 and 2 should retry on transient errors.
        let d1 = should_retry(&config, 1, &RetryClassification::TransientError);
        assert!(matches!(d1, RetryDecision::RetryAfter(_)));

        let d2 = should_retry(&config, 2, &RetryClassification::TransientError);
        assert!(matches!(d2, RetryDecision::RetryAfter(_)));

        // Attempt 3 should not retry (we've used all 3 attempts).
        let d3 = should_retry(&config, 3, &RetryClassification::TransientError);
        assert_eq!(d3, RetryDecision::DontRetry);
    }

    #[test]
    fn test_retry_max_attempts_legacy() {
        let config = RetryConfig::from_mode(RetryMode::Legacy);
        assert_eq!(config.max_attempts, 5);

        // Attempts 1-4 should retry.
        for attempt in 1..=4 {
            let d = should_retry(&config, attempt, &RetryClassification::TransientError);
            assert!(
                matches!(d, RetryDecision::RetryAfter(_)),
                "Attempt {attempt} should allow retry"
            );
        }

        // Attempt 5 should stop.
        let d5 = should_retry(&config, 5, &RetryClassification::TransientError);
        assert_eq!(d5, RetryDecision::DontRetry);
    }

    #[test]
    fn test_retry_non_retryable_stops_immediately() {
        let config = RetryConfig::default();
        let d = should_retry(&config, 1, &RetryClassification::NonRetryable);
        assert_eq!(d, RetryDecision::DontRetry);
    }

    // -- Config defaults ----------------------------------------------------

    #[test]
    fn test_retry_default_config() {
        let config = RetryConfig::default();
        assert_eq!(config.mode, RetryMode::Standard);
        assert_eq!(config.max_attempts, 3);
    }

    // -- Config resolution tests --------------------------------------------

    #[test]
    fn test_retry_mode_parse() {
        assert_eq!(parse_retry_mode("standard"), Some(RetryMode::Standard));
        assert_eq!(parse_retry_mode("Standard"), Some(RetryMode::Standard));
        assert_eq!(parse_retry_mode("STANDARD"), Some(RetryMode::Standard));
        assert_eq!(parse_retry_mode("legacy"), Some(RetryMode::Legacy));
        assert_eq!(parse_retry_mode("adaptive"), Some(RetryMode::Adaptive));
        assert_eq!(parse_retry_mode("unknown"), None);
        assert_eq!(parse_retry_mode(""), None);
    }

    #[test]
    fn test_max_attempts_config_from_config_file() {
        // Clear env vars so they don't interfere
        std::env::remove_var("AWS_MAX_ATTEMPTS");
        std::env::remove_var("AWS_RETRY_MODE");

        let config = resolve_retry_config(Some("5"), None);
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.mode, RetryMode::Standard);
    }

    #[test]
    fn test_max_attempts_from_config_value() {
        // Test with config values only (no env vars dependency)
        let config = resolve_retry_config_inner(Some("5"), None, None, None);
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.mode, RetryMode::Standard);
    }

    #[test]
    fn test_max_attempts_env_overrides_config() {
        // Use the inner function with explicit env-like values
        let config = resolve_retry_config_inner(Some("5"), None, Some("10"), None);
        assert_eq!(config.max_attempts, 10);
    }

    #[test]
    fn test_retry_mode_from_config_value() {
        let config = resolve_retry_config_inner(None, Some("legacy"), None, None);
        assert_eq!(config.mode, RetryMode::Legacy);
        assert_eq!(config.max_attempts, 5); // legacy default
    }

    #[test]
    fn test_retry_mode_env_overrides_config() {
        let config = resolve_retry_config_inner(None, Some("legacy"), None, Some("adaptive"));
        assert_eq!(config.mode, RetryMode::Adaptive);
        assert_eq!(config.max_attempts, 3); // adaptive default
    }

    #[test]
    fn test_max_attempts_invalid_values_ignored() {
        // Zero is invalid
        let config = resolve_retry_config_inner(Some("0"), None, None, None);
        assert_eq!(config.max_attempts, 3); // falls back to default

        // Non-numeric is invalid
        let config = resolve_retry_config_inner(Some("abc"), None, None, None);
        assert_eq!(config.max_attempts, 3);
    }

    #[test]
    fn test_resolve_retry_config_defaults() {
        let config = resolve_retry_config_inner(None, None, None, None);
        assert_eq!(config.mode, RetryMode::Standard);
        assert_eq!(config.max_attempts, 3);
    }

    // -- Token bucket (adaptive mode) ---------------------------------------

    #[test]
    fn test_retry_adaptive_token_bucket() {
        let bucket = TokenBucket::new();
        assert_eq!(bucket.available(), 500);

        // Acquire tokens for throttled retries.
        assert!(bucket.acquire()); // 500 -> 495
        assert_eq!(bucket.available(), 495);

        assert!(bucket.acquire()); // 495 -> 490
        assert_eq!(bucket.available(), 490);

        // Refill on success.
        bucket.refill(); // 490 -> 491
        assert_eq!(bucket.available(), 491);

        // Drain the bucket to verify it rejects when insufficient.
        let small_bucket = TokenBucket::with_params(10, 5, 1);
        assert_eq!(small_bucket.available(), 10);
        assert!(small_bucket.acquire()); // 10 -> 5
        assert!(small_bucket.acquire()); // 5 -> 0
        assert!(!small_bucket.acquire()); // 0 -> can't acquire
        assert_eq!(small_bucket.available(), 0);

        // Refill and acquire again.
        for _ in 0..5 {
            small_bucket.refill();
        }
        assert_eq!(small_bucket.available(), 5);
        assert!(small_bucket.acquire()); // 5 -> 0

        // Refill should not exceed max.
        let full_bucket = TokenBucket::new();
        full_bucket.refill();
        assert_eq!(full_bucket.available(), 500); // stays at max
    }
}
