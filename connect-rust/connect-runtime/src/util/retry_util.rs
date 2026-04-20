// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Retry utility for operations that may fail temporarily.
//!
//! This corresponds to `org.apache.kafka.connect.util.RetryUtil` in Java.

use common_trait::util::time::Time;
use std::sync::Arc;
use std::time::Duration;

/// Retry utility for executing operations with retry logic.
///
/// This corresponds to `org.apache.kafka.connect.util.RetryUtil` in Java.
pub struct RetryUtil;

impl RetryUtil {
    /// Executes a callable with retry logic until timeout.
    ///
    /// The method executes the callable at least once, optionally retrying
    /// if a retriable exception is thrown. If timeout is exhausted,
    /// the last exception is wrapped in a ConnectException and thrown.
    ///
    /// # Arguments
    /// * `callable` - The function to execute
    /// * `description` - Optional description for logging purposes
    /// * `timeout_duration` - The maximum duration to retry
    /// * `retry_backoff_ms` - Milliseconds to wait between retries
    /// * `time` - The time interface for timing operations
    ///
    /// # Returns
    /// The result of the callable if successful
    ///
    /// # Errors
    /// Returns an error if all retries are exhausted
    pub fn retry_until_timeout<T, E, F>(
        callable: F,
        description: Option<&str>,
        timeout_duration: Duration,
        retry_backoff_ms: i64,
        time: Arc<dyn Time>,
    ) -> Result<T, RetryError>
    where
        F: Fn() -> Result<T, E>,
        E: std::error::Error + IsRetriable + 'static,
    {
        let description_str = description.unwrap_or("callable");
        let timeout_ms = timeout_duration.as_millis() as i64;

        // Validate parameters
        let retry_backoff_ms = if retry_backoff_ms < 0 {
            log::debug!(
                "Assuming no retry backoff since retryBackoffMs={} is negative",
                retry_backoff_ms
            );
            0
        } else {
            retry_backoff_ms
        };

        if timeout_ms <= 0 || retry_backoff_ms >= timeout_ms {
            log::debug!(
                "Executing {} only once, since timeoutMs={} is not larger than retryBackoffMs={}",
                description_str,
                timeout_ms,
                retry_backoff_ms
            );
            return callable().map_err(|e| RetryError::new(e, description_str, 1));
        }

        let end = time.milliseconds() + timeout_ms;
        let mut attempt = 0;
        let mut last_error: Option<Box<dyn std::error::Error>> = None;

        loop {
            attempt += 1;
            match callable() {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if e.is_retriable() {
                        log::warn!(
                            "Attempt {} to {} resulted in retriable error; retrying automatically. Reason: {}",
                            attempt, description_str, e
                        );
                        last_error = Some(Box::new(e));
                    } else {
                        // Non-retriable error, fail immediately
                        return Err(RetryError::new(e, description_str, attempt));
                    }
                }
            }

            // Check if we should sleep before retrying
            if retry_backoff_ms > 0 {
                let millis_remaining = end - time.milliseconds();
                if millis_remaining <= 0 {
                    break;
                }
                if millis_remaining < retry_backoff_ms {
                    // Exit when remaining time is less than retry_backoff_ms
                    break;
                }
                time.sleep(retry_backoff_ms);
            }

            // Check if timeout has expired
            if time.milliseconds() >= end {
                break;
            }
        }

        // All retries exhausted
        let error = last_error.unwrap_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Unknown error during retry",
            ))
        });
        Err(RetryError::from_boxed(error, description_str, attempt))
    }

    /// Simple retry until timeout without custom time interface.
    pub fn retry_until_timeout_simple<T, E, F>(
        callable: F,
        description: Option<&str>,
        timeout_duration: Duration,
        retry_backoff_ms: i64,
    ) -> Result<T, RetryError>
    where
        F: Fn() -> Result<T, E>,
        E: std::error::Error + IsRetriable + 'static,
    {
        use common_trait::util::time::SystemTimeImpl;
        let time = Arc::new(SystemTimeImpl::new());
        Self::retry_until_timeout(
            callable,
            description,
            timeout_duration,
            retry_backoff_ms,
            time,
        )
    }
}

/// Trait to determine if an error is retriable.
pub trait IsRetriable {
    /// Returns true if this error can be retried.
    fn is_retriable(&self) -> bool;
}

/// Error that occurred during retry operations.
#[derive(Debug)]
pub struct RetryError {
    message: String,
    attempts: usize,
    source: Option<Box<dyn std::error::Error>>,
}

impl RetryError {
    /// Creates a new RetryError.
    pub fn new<E: std::error::Error + 'static>(
        error: E,
        description: &str,
        attempts: usize,
    ) -> Self {
        RetryError {
            message: format!(
                "Failed to {} after {} attempts. Reason: {}",
                description, attempts, error
            ),
            attempts,
            source: Some(Box::new(error)),
        }
    }

    /// Creates a RetryError from a boxed error.
    pub fn from_boxed(
        error: Box<dyn std::error::Error>,
        description: &str,
        attempts: usize,
    ) -> Self {
        RetryError {
            message: format!(
                "Failed to {} after {} attempts. Reason: {}",
                description, attempts, error
            ),
            attempts,
            source: Some(error),
        }
    }

    /// Returns the number of attempts made.
    pub fn attempts(&self) -> usize {
        self.attempts
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for RetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RetryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// A simple retriable error for testing.
#[derive(Debug)]
pub struct SimpleRetriableError {
    message: String,
}

impl SimpleRetriableError {
    pub fn new(message: String) -> Self {
        SimpleRetriableError { message }
    }
}

impl std::fmt::Display for SimpleRetriableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SimpleRetriableError {}

impl IsRetriable for SimpleRetriableError {
    fn is_retriable(&self) -> bool {
        true
    }
}

/// A non-retriable error.
#[derive(Debug)]
pub struct NonRetriableError {
    message: String,
}

impl NonRetriableError {
    pub fn new(message: String) -> Self {
        NonRetriableError { message }
    }
}

impl std::fmt::Display for NonRetriableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for NonRetriableError {}

impl IsRetriable for NonRetriableError {
    fn is_retriable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_trait::util::time::SystemTimeImpl;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_retry_immediate_success() {
        let result: Result<i32, RetryError> = RetryUtil::retry_until_timeout_simple(
            || Ok::<i32, SimpleRetriableError>(42),
            Some("test"),
            Duration::from_secs(1),
            100,
        );
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_retry_success_after_failures() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result = RetryUtil::retry_until_timeout_simple(
            || {
                let count = attempts_clone.fetch_add(1, Ordering::SeqCst);
                if count < 2 {
                    Err(SimpleRetriableError::new("temporary failure".to_string()))
                } else {
                    Ok(42)
                }
            },
            Some("test"),
            Duration::from_secs(5),
            100,
        );

        assert_eq!(result.unwrap(), 42);
        assert!(attempts.load(Ordering::SeqCst) >= 3);
    }

    #[test]
    fn test_retry_non_retriable_fails_immediately() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result: Result<i32, RetryError> = RetryUtil::retry_until_timeout_simple(
            || {
                attempts_clone.fetch_add(1, Ordering::SeqCst);
                Err(NonRetriableError::new("permanent failure".to_string()))
            },
            Some("test"),
            Duration::from_secs(5),
            100,
        );

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_retry_exhausted() {
        let time = Arc::new(SystemTimeImpl::new());
        let result: Result<i32, RetryError> = RetryUtil::retry_until_timeout(
            || Err(SimpleRetriableError::new("always fails".to_string())),
            Some("test"),
            Duration::from_millis(300), // Short timeout
            100,
            time,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message().contains("Failed to test"));
    }

    #[test]
    fn test_retry_no_timeout() {
        let result: Result<i32, RetryError> = RetryUtil::retry_until_timeout_simple(
            || Ok::<i32, SimpleRetriableError>(42),
            Some("test"),
            Duration::from_secs(0), // Zero timeout
            100,
        );
        assert_eq!(result.unwrap(), 42);
    }
}
