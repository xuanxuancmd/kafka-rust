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

//! RetryWithToleranceOperator executes operations with retry and tolerance logic.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator` in Java.

use std::error::Error;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use connect_api::errors::{ConnectException, RetriableException};

use super::error_reporter::ErrorReporter;
use super::metrics::ErrorHandlingMetrics;
use super::processing_context::ProcessingContext;
use super::stage::Stage;
use super::tolerance_type::ToleranceType;

/// Minimum retry delay in milliseconds (corresponds to RETRIES_DELAY_MIN_MS in Java)
const RETRIES_DELAY_MIN_MS: u64 = 300;

/// Operation is a callable that represents an operation to be executed.
/// Returns a result or an error.
///
/// Uses FnMut to allow multiple calls for retry semantics (corresponds to Java's Operation<V> interface
/// where call() can be invoked repeatedly).
pub type Operation<T> = Box<dyn FnMut() -> Result<T, Box<dyn Error + Send + Sync>> + Send + Sync>;

/// RetryWithToleranceOperator executes operations with retry and tolerance logic.
///
/// This operator:
/// 1. Retries operations that throw RetriableException with exponential backoff
/// 2. Tolerates exceptions based on the configured ToleranceType
/// 3. Reports errors to registered ErrorReporter instances
/// 4. Tracks error metrics via ErrorHandlingMetrics
pub struct RetryWithToleranceOperator {
    /// The timeout for retry attempts in milliseconds
    error_retry_timeout: u64,
    /// The maximum delay between retries in milliseconds
    error_max_delay_in_millis: u64,
    /// The tolerance type (NONE or ALL)
    tolerance_type: ToleranceType,
    /// List of error reporters
    error_reporters: Vec<Arc<dyn ErrorReporter>>,
    /// Error handling metrics
    metrics: Arc<ErrorHandlingMetrics>,
    /// Flag to indicate if stop was requested
    stop_requested: Arc<std::sync::atomic::AtomicBool>,
}

impl RetryWithToleranceOperator {
    /// Creates a new RetryWithToleranceOperator.
    ///
    /// # Arguments
    /// * `error_retry_timeout` - Timeout for retries in milliseconds (default: 0 means no retries)
    /// * `error_max_delay_in_millis` - Maximum delay between retries in milliseconds
    /// * `tolerance_type` - How to handle errors (NONE or ALL)
    /// * `metrics` - Error handling metrics tracker
    pub fn new(
        error_retry_timeout: u64,
        error_max_delay_in_millis: u64,
        tolerance_type: ToleranceType,
        metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        RetryWithToleranceOperator {
            error_retry_timeout,
            error_max_delay_in_millis,
            tolerance_type,
            error_reporters: Vec::new(),
            metrics,
            stop_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Creates a new operator with default settings (no retries, no tolerance).
    pub fn default_operator() -> Self {
        Self::new(
            0, // no retries
            0, // no max delay
            ToleranceType::NONE,
            Arc::new(ErrorHandlingMetrics::new()),
        )
    }

    /// Adds an error reporter to the operator.
    pub fn add_reporter(&mut self, reporter: Arc<dyn ErrorReporter>) {
        self.error_reporters.push(reporter);
    }

    /// Sets the stop requested flag to terminate retries early.
    pub fn request_stop(&self) {
        self.stop_requested
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Clears the stop requested flag.
    pub fn clear_stop_request(&self) {
        self.stop_requested
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Returns true if stop was requested.
    pub fn is_stop_requested(&self) -> bool {
        self.stop_requested
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Executes an operation with retry and tolerance logic.
    ///
    /// # Arguments
    /// * `context` - The processing context to track state
    /// * `operation` - The operation to execute
    /// * `stage` - The stage where this operation is being executed
    /// * `executing_class` - The class name executing the operation
    ///
    /// # Returns
    /// The result of the operation, or None if the operation failed
    /// and was tolerated (in tolerance mode ALL).
    pub fn execute<T: Send + Sync>(
        &mut self,
        context: &mut ProcessingContext,
        operation: Operation<T>,
        stage: Stage,
        executing_class: &str,
    ) -> Option<T> {
        context.set_stage(stage);
        context.set_executing_class(executing_class.to_string());

        // If context is already failed, return None
        if context.is_failed() {
            return None;
        }

        // Execute with retry logic
        let deadline = Instant::now() + Duration::from_millis(self.error_retry_timeout);
        let result = self.exec_and_retry(context, operation, deadline);

        match result {
            Ok(value) => Some(value),
            Err(error) => {
                context.set_error(error);
                self.handle_error(context, stage, executing_class);
                if self.within_tolerance_limits() {
                    self.metrics.record_tolerated_error(stage);
                    None
                } else {
                    self.metrics.record_failed_error(stage);
                    // In NONE tolerance mode, we should throw, but in Rust we return None
                    // The actual exception would be thrown at a higher level
                    None
                }
            }
        }
    }

    /// Executes an operation with retry logic.
    ///
    /// Corresponds to Java's `execAndRetry` method which:
    /// 1. Loops attempting the operation
    /// 2. Retries on RetriableException if within deadline
    /// 3. Implements exponential backoff between retries
    /// 4. Returns null on stop request (setting error in context)
    /// 5. Throws the exception if deadline exceeded
    fn exec_and_retry<T: Send + Sync>(
        &mut self,
        context: &mut ProcessingContext,
        mut operation: Operation<T>,
        deadline: Instant,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        // attempt counter is tracked via context.increment_attempts()
        // Java: int attempt = 0; then attempt++ in loop; finally context.attempt(attempt)
        // Rust equivalent: increment_attempts() at each loop iteration

        loop {
            // Track attempt (equivalent to Java's finally block: context.attempt(attempt))
            context.increment_attempts();

            // Execute the operation (Java: operation.call())
            let result = operation();

            match result {
                Ok(value) => {
                    // Success - return result
                    // Java: return operation.call() succeeds
                    return Ok(value);
                }
                Err(error) => {
                    // Check if it's a RetriableException (Java: catch (RetriableException e))
                    let is_retriable = error.downcast_ref::<RetriableException>().is_some();

                    if is_retriable {
                        // Java: errorHandlingMetrics.recordFailure()
                        self.metrics.record_failure();

                        // Check deadline (Java: if (time.milliseconds() < deadline))
                        let current_time = Instant::now();
                        if current_time < deadline {
                            // Java: backoff(attempt, deadline); errorHandlingMetrics.recordRetry();
                            self.backoff(context.attempts(), deadline);
                            self.metrics.record_retry();

                            // Check for stop request (Java: if (stopping))
                            if self.is_stop_requested() {
                                // Java: context.error(e); return null;
                                context.set_error(error);
                                // Return a special error to indicate stopped (null in Java)
                                return Err(Box::new(ConnectException::new(
                                    "Operation stopped due to shutdown request",
                                )));
                            }

                            // Continue retry loop (Java: implicit continue in do-while)
                            continue;
                        } else {
                            // Deadline exceeded (Java: throw e)
                            return Err(error);
                        }
                    } else {
                        // Non-retriable exception - rethrow (Java: implicit throw)
                        return Err(error);
                    }
                }
            }
        }
    }

    /// Executes an operation and handles any errors.
    /// This is the main entry point for operations that need retry/tolerance handling.
    ///
    /// # Arguments  
    /// * `operation_fn` - A function that returns the operation result
    /// * `stage` - The stage where this operation is being executed
    /// * `executing_class` - The class name executing the operation
    ///
    /// # Returns
    /// The result of the operation, or None if failed and tolerated.
    pub fn execute_with_retry<T, F>(
        &mut self,
        mut operation_fn: F,
        stage: Stage,
        executing_class: &str,
    ) -> Option<T>
    where
        T: Send + Sync,
        F: FnMut() -> Result<T, Box<dyn Error + Send + Sync>>,
    {
        let mut context = ProcessingContext::new();
        context.set_stage(stage);
        context.set_executing_class(executing_class.to_string());

        // If tolerance is NONE and we already have failures, don't proceed
        if !self.within_tolerance_limits() {
            return None;
        }

        let deadline = if self.error_retry_timeout > 0 {
            Instant::now() + Duration::from_millis(self.error_retry_timeout)
        } else {
            Instant::now()
        };

        let mut attempt = 0;
        loop {
            attempt += 1;
            context.increment_attempts();

            let result = operation_fn();

            match result {
                Ok(value) => {
                    // Success - only record retried error if we made retries
                    if context.attempts() > 1 {
                        self.metrics.record_retried_error(stage);
                    }
                    return Some(value);
                }
                Err(error) => {
                    // Check if it's a retriable exception
                    let is_retriable = error.downcast_ref::<RetriableException>().is_some()
                        || error
                            .downcast_ref::<ConnectException>()
                            .map(|e| e.message().contains("retriable"))
                            .unwrap_or(false);

                    if is_retriable
                        && self.error_retry_timeout > 0
                        && Instant::now() < deadline
                        && !self.is_stop_requested()
                    {
                        // Retry with backoff
                        self.backoff(attempt, deadline);
                        continue;
                    }

                    // Non-retriable or timeout exceeded
                    context.set_error(error);
                    self.handle_error(&context, stage, executing_class);

                    // Record error based on tolerance type
                    match self.tolerance_type {
                        ToleranceType::NONE => {
                            self.metrics.record_failed_error(stage);
                        }
                        ToleranceType::ALL => {
                            self.metrics.record_tolerated_error(stage);
                        }
                    }
                    return None;
                }
            }
        }
    }

    /// Implements the exponential backoff logic.
    ///
    /// # Arguments
    /// * `attempt` - The current attempt number
    /// * `deadline` - The deadline for retries
    fn backoff(&self, attempt: u32, deadline: Instant) {
        if Instant::now() >= deadline {
            return;
        }

        // Calculate delay: RETRIES_DELAY_MIN_MS * 2^(attempt-1)
        let base_delay = RETRIES_DELAY_MIN_MS << (attempt.saturating_sub(1) as u32);
        let delay_ms = std::cmp::min(base_delay, self.error_max_delay_in_millis);

        // If calculated delay exceeds max, use random delay up to max
        let actual_delay = if base_delay > self.error_max_delay_in_millis {
            // Use random-ish delay (simplified: just use max/2)
            self.error_max_delay_in_millis / 2
        } else {
            delay_ms
        };

        // Sleep for the delay duration, checking stop_requested periodically
        let sleep_duration = Duration::from_millis(actual_delay);
        let check_interval = Duration::from_millis(100);
        let mut remaining = sleep_duration;

        while remaining > Duration::ZERO && !self.is_stop_requested() {
            let sleep_time = std::cmp::min(remaining, check_interval);
            thread::sleep(sleep_time);
            remaining -= sleep_time;
        }
    }

    /// Handles an error by reporting it to all registered reporters.
    fn handle_error(&self, context: &ProcessingContext, stage: Stage, executing_class: &str) {
        for reporter in &self.error_reporters {
            reporter.report(context);
        }
    }

    /// Checks if the current number of failures is within the configured tolerance limits.
    /// For ToleranceType::NONE, returns false after any failure (no tolerance).
    /// For ToleranceType::ALL, always returns true (within limits).
    pub fn within_tolerance_limits(&self) -> bool {
        match self.tolerance_type {
            ToleranceType::NONE => self.metrics.failed_errors() == 0,
            ToleranceType::ALL => true,
        }
    }

    /// Records an external failure when the operation cannot be wrapped by execute().
    ///
    /// # Arguments
    /// * `context` - The processing context
    /// * `stage` - The stage where the failure occurred
    /// * `executing_class` - The class name where the failure occurred
    /// * `error` - The error that occurred
    pub fn execute_failed(
        &self,
        context: &mut ProcessingContext,
        stage: Stage,
        executing_class: &str,
        error: Box<dyn Error + Send + Sync>,
    ) {
        context.set_stage(stage);
        context.set_executing_class(executing_class.to_string());
        context.set_error(error);
        context.mark_failed();

        self.handle_error(context, stage, executing_class);
        self.metrics.record_failed_error(stage);
    }

    /// Returns the tolerance type.
    pub fn tolerance_type(&self) -> ToleranceType {
        self.tolerance_type
    }

    /// Returns the error retry timeout in milliseconds.
    pub fn error_retry_timeout(&self) -> u64 {
        self.error_retry_timeout
    }

    /// Returns the maximum delay in milliseconds.
    pub fn error_max_delay(&self) -> u64 {
        self.error_max_delay_in_millis
    }

    /// Returns the error handling metrics.
    pub fn metrics(&self) -> &ErrorHandlingMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_retry_with_tolerance_operator_new() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let operator = RetryWithToleranceOperator::new(
            60000, // 60 second timeout
            5000,  // 5 second max delay
            ToleranceType::ALL,
            metrics,
        );
        assert_eq!(operator.error_retry_timeout(), 60000);
        assert_eq!(operator.error_max_delay(), 5000);
        assert_eq!(operator.tolerance_type(), ToleranceType::ALL);
    }

    #[test]
    fn test_retry_with_tolerance_operator_default() {
        let operator = RetryWithToleranceOperator::default_operator();
        assert_eq!(operator.error_retry_timeout(), 0);
        assert_eq!(operator.tolerance_type(), ToleranceType::NONE);
    }

    #[test]
    fn test_retry_with_tolerance_operator_add_reporter() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let mut operator = RetryWithToleranceOperator::new(0, 0, ToleranceType::NONE, metrics);
        operator.add_reporter(Arc::new(super::super::error_reporter::LogReporter::new(
            "test",
        )));
        // Reporter is added
    }

    #[test]
    fn test_retry_with_tolerance_operator_stop_request() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let operator = RetryWithToleranceOperator::new(0, 0, ToleranceType::NONE, metrics);
        assert!(!operator.is_stop_requested());
        operator.request_stop();
        assert!(operator.is_stop_requested());
        operator.clear_stop_request();
        assert!(!operator.is_stop_requested());
    }

    #[test]
    fn test_within_tolerance_limits_none() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let operator = RetryWithToleranceOperator::new(0, 0, ToleranceType::NONE, metrics.clone());
        assert!(operator.within_tolerance_limits());

        // After recording a failure
        metrics.record_failed_error(Stage::KEY_CONVERTER);
        assert!(!operator.within_tolerance_limits());
    }

    #[test]
    fn test_within_tolerance_limits_all() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let operator = RetryWithToleranceOperator::new(0, 0, ToleranceType::ALL, metrics.clone());
        assert!(operator.within_tolerance_limits());

        // Even after failures, still within limits for ALL tolerance
        metrics.record_failed_error(Stage::KEY_CONVERTER);
        assert!(operator.within_tolerance_limits());
    }

    #[test]
    fn test_execute_successful_operation() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let mut operator = RetryWithToleranceOperator::new(0, 0, ToleranceType::NONE, metrics);

        let result: Option<i32> =
            operator.execute_with_retry(|| Ok(42), Stage::KEY_CONVERTER, "TestClass");
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_execute_failed_operation_none_tolerance() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let mut operator =
            RetryWithToleranceOperator::new(0, 0, ToleranceType::NONE, metrics.clone());

        let result: Option<String> = operator.execute_with_retry(
            || Err(Box::new(io::Error::new(io::ErrorKind::Other, "test error"))),
            Stage::KEY_CONVERTER,
            "TestClass",
        );
        assert_eq!(result, None);
        assert_eq!(metrics.failed_errors(), 1);
    }

    #[test]
    fn test_execute_failed_operation_all_tolerance() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let mut operator =
            RetryWithToleranceOperator::new(0, 0, ToleranceType::ALL, metrics.clone());

        let result: Option<String> = operator.execute_with_retry(
            || Err(Box::new(io::Error::new(io::ErrorKind::Other, "test error"))),
            Stage::VALUE_CONVERTER,
            "TestClass",
        );
        assert_eq!(result, None);
        assert_eq!(metrics.tolerated_errors(), 1);
    }

    #[test]
    fn test_execute_with_retriable_exception() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let mut operator = RetryWithToleranceOperator::new(1000, 100, ToleranceType::ALL, metrics);

        let mut attempts = 0;
        let result: Option<&str> = operator.execute_with_retry(
            || {
                attempts += 1;
                if attempts < 3 {
                    Err(Box::new(RetriableException::new("retriable error")))
                } else {
                    Ok("success")
                }
            },
            Stage::TRANSFORMATION,
            "TestClass",
        );
        assert_eq!(result, Some("success"));
        assert!(attempts >= 3);
    }

    #[test]
    fn test_execute_failed_method() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let operator = RetryWithToleranceOperator::new(0, 0, ToleranceType::NONE, metrics.clone());

        let mut context = ProcessingContext::new();
        operator.execute_failed(
            &mut context,
            Stage::KAFKA_PRODUCE,
            "TestClass",
            Box::new(ConnectException::new("external failure")),
        );

        assert!(context.is_failed());
        assert_eq!(metrics.failed_errors(), 1);
        assert_eq!(metrics.stage_errors(Stage::KAFKA_PRODUCE), 1);
    }

    #[test]
    fn test_backoff_timing() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let operator = RetryWithToleranceOperator::new(10000, 5000, ToleranceType::ALL, metrics);

        let start = Instant::now();
        operator.backoff(1, Instant::now() + Duration::from_secs(10));
        let elapsed = start.elapsed().as_millis();

        // Should have slept at least RETRIES_DELAY_MIN_MS (300ms)
        assert!(elapsed >= 250); // Allow some tolerance
    }
}
