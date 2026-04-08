//! Retry + timeout wrapper for LLM calls.
//!
//! Real provider implementations want the same error-handling shape:
//!
//! - retry transient failures (network errors, HTTP 429, HTTP 5xx) with
//!   exponential backoff and jitter
//! - abort the whole call after a configurable timeout
//! - surface non-transient errors immediately (4xx other than 429, schema
//!   validation failures, malformed responses)
//!
//! Rather than duplicate this in every provider, both Claude and `OpenAI` call
//! [`with_retry`] with a closure that makes a single request attempt. The
//! closure returns `Result<T, Retry>` where [`Retry::Transient`] triggers
//! another attempt and [`Retry::Permanent`] bails out immediately.

use std::time::Duration;

use super::LlmError;

/// Classification of an attempt's failure.
pub enum Retry {
    /// Retry after backoff.
    Transient(LlmError),
    /// Fail immediately.
    Permanent(LlmError),
}

/// Retry `attempt` with exponential backoff, capped at `max_attempts`.
/// Aborts if the total elapsed time exceeds `overall_timeout`.
pub async fn with_retry<T, F, Fut>(
    max_attempts: u32,
    overall_timeout: Duration,
    mut attempt: F,
) -> Result<T, LlmError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Retry>>,
{
    let deadline = tokio::time::Instant::now() + overall_timeout;
    let mut delay = Duration::from_millis(250);
    let mut last_err: Option<LlmError> = None;

    for attempt_idx in 0..max_attempts {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        // Cap each individual attempt to the remaining budget.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let call = attempt();
        match tokio::time::timeout_at(deadline, call).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(Retry::Transient(e))) => {
                last_err = Some(e);
                // Backoff before the next attempt.
                if attempt_idx + 1 < max_attempts {
                    let until_deadline =
                        deadline.saturating_duration_since(tokio::time::Instant::now());
                    let sleep_for = delay.min(until_deadline);
                    if sleep_for.is_zero() {
                        break;
                    }
                    tokio::time::sleep(sleep_for).await;
                    delay = delay.saturating_mul(2).min(Duration::from_secs(8));
                }
            }
            Ok(Err(Retry::Permanent(e))) => return Err(e),
            Err(_elapsed) => {
                return Err(LlmError::Timeout {
                    provider: "unknown".into(),
                    seconds: overall_timeout.as_secs(),
                });
            }
        }
    }

    Err(last_err.unwrap_or(LlmError::Timeout {
        provider: "unknown".into(),
        seconds: overall_timeout.as_secs(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn succeeds_on_first_try() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();
        let result: Result<u32, LlmError> = with_retry(3, Duration::from_secs(5), || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Retry>(42)
            }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_transient_errors() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();
        let result: Result<u32, LlmError> = with_retry(5, Duration::from_secs(5), || {
            let c = c.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(Retry::Transient(LlmError::network("mock", "boom")))
                } else {
                    Ok(99)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 99);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn bails_on_permanent_errors() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();
        let result: Result<u32, LlmError> = with_retry(5, Duration::from_secs(5), || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(Retry::Permanent(LlmError::api("mock", 401, "nope")))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
