//! Retry with tolerance operator module
//!
//! Provides a RetryWithToleranceOperator for attempting to recover failed operations with retries and tolerance limits.

use crate::errors::error_handling_metrics::ErrorHandlingMetrics;
use crate::errors::error_reporter::ErrorReporter;
use crate::errors::operation::Operation;
use crate::errors::processing_context::ProcessingContext;
use crate::errors::stage::Stage;
use crate::errors::tolerance_type::ToleranceType;
use connect_runtime_core::errors::ConnectRuntimeError;
use async_trait::async_trait;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, trace, warn as log_error};

/// Minimum retry delay in milliseconds
const RETRIES_DELAY_MIN_MS: u64 = 300;

/// Tolerable exceptions mapping for each stage
fn get_tolerable_exceptions() -> HashMap<Stage, TypeId> {
    let mut map = HashMap::new();
    map.insert(Stage::Transformation, TypeId::of::<Box<dyn std::error::Error + Send + Sync>>());
    map.insert(Stage::HeaderConverter, TypeId::of::<Box<dyn std::error::Error + Send + Sync>>());
    map.insert(Stage::KeyConverter, TypeId::of::<Box<dyn std::error::Error + Send + Sync>>());
    map.insert(Stage::ValueConverter, TypeId::of::<Box<dyn std::error::Error + Send + Sync>>());
    map
}

/// Attempt to recover a failed operation with retries and tolerance limits.
///
/// A retry is attempted if the operation throws a RetriableException. Retries are accompanied by exponential backoffs,
/// starting with RETRIES_DELAY_MIN_MS, up to what is specified with error_max_delay_in_millis.
/// Including the first attempt and future retries, the total time taken to evaluate the operation should be within
/// error_max_delay_in_millis millis.
///
/// This executor will tolerate failures, as specified by error_tolerance_type.
/// For transformations and converters, all exceptions are tolerated. For others operations, only RetriableException are tolerated.
///
/// There are three outcomes to executing an operation. It might succeed, in which case the result is returned to the caller.
/// If it fails, this class does one of these two things: (1) if the failure occurred due to a tolerable exception, then
/// set appropriate error reason in the ProcessingContext and return None, or (2) if the exception is not tolerated,
/// then it is wrapped into a ConnectException and rethrown to the caller.
///
/// Instances of this class are thread safe.
pub struct RetryWithToleranceOperator<T> {
    /// Error retry timeout in milliseconds
    error_retry_timeout: i64,

    /// Error max delay in milliseconds
    error_max_delay_in_millis: i64,

    /// Error tolerance type
    error_tolerance_type: ToleranceType,

    /// Total failures count
    total_failures: Arc<AtomicI64>,

    /// Error handling metrics
    error_handling_metrics: Arc<ErrorHandlingMetrics>,

    /// Whether the operator has been asked to stop retrying
    stopping: Arc<AtomicBool>,

    /// Error reporters
    reporters: Arc<Mutex<Vec<Arc<dyn ErrorReporter<T>>>>>,
}

impl<T> RetryWithToleranceOperator<T> {
    /// Create a new retry with tolerance operator
    ///
    /// # Arguments
    ///
    /// * `error_retry_timeout` - error retry timeout in milliseconds
    /// * `error_max_delay_in_millis` - error max delay in milliseconds
    /// * `tolerance_type` - tolerance type
    /// * `error_handling_metrics` - error handling metrics
    ///
    /// # Returns
    ///
    /// New retry with tolerance operator
    pub fn new(
        error_retry_timeout: i64,
        error_max_delay_in_millis: i64,
        tolerance_type: ToleranceType,
        error_handling_metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        Self {
            error_retry_timeout,
            error_max_delay_in_millis,
            error_tolerance_type: tolerance_type,
            total_failures: Arc::new(AtomicI64::new(0)),
            error_handling_metrics,
            stopping: Arc::new(AtomicBool::new(false)),
            reporters: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Inform this class that some external operation has already failed. This is used when the control flow does not
    /// allow for the operation to be started and stopped within the scope of a single Operation, and the
    /// execute method cannot be used.
    ///
    /// # Arguments
    ///
    /// * `context` - The ProcessingContext used to hold state about this operation
    /// * `stage` - The logical stage within the overall pipeline of the operation that has failed
    /// * `executing_class` - The class containing the operation implementation that failed
    /// * `error` - The error which caused the operation to fail
    ///
    /// # Returns
    ///
    /// Result containing () or an error if the operation is not tolerated
    pub async fn execute_failed(
        &self,
        context: &mut ProcessingContext<T>,
        stage: Stage,
        executing_class: TypeId,
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> Result<(), ConnectRuntimeError> {
        self.mark_as_failed();
        context.current_context(stage, executing_class);
        context.set_error(error);
        self.error_handling_metrics.record_failure();

        self.report(context).await;

        if !self.within_tolerance_limits() {
            self.error_handling_metrics.record_error();
            return Err(ConnectRuntimeError::connector_error(
                "Tolerance exceeded in error handler".to_string(),
            ));
        }

        Ok(())
    }

    /// Report an error to all configured ErrorReporter instances
    ///
    /// # Arguments
    ///
    /// * `context` - The context containing details of the error to report
    async fn report(&self, context: &ProcessingContext<T>) {
        let reporters = self.reporters.lock().await;
        for reporter in reporters.iter() {
            if let Err(e) = reporter.report(context).await {
                error!("Error reporting failed: {}", e);
            }
        }
    }

    /// Attempt to execute an operation. Handles retriable and tolerated exceptions thrown by the operation. This is
    /// used for small blocking operations which can be represented as an Operation. For operations which do not
    /// fit this interface, see execute_failed.
    ///
    /// If any error is already present in the context, return None without modifying the context.
    /// Retries are allowed if the operator is still running, and this operation is within the error retry timeout.
    /// Tolerable exceptions are different for each stage, and encoded in get_tolerable_exceptions
    /// This method mutates the passed-in ProcessingContext with the number of attempts made to execute the
    /// operation, and the last error encountered if no attempt was successful.
    ///
    /// # Arguments
    ///
    /// * `context` - The ProcessingContext used to hold state about this operation
    /// * `operation` - The recoverable operation
    /// * `stage` - The logical stage within the overall pipeline of the operation that has failed
    /// * `executing_class` - The class containing the operation implementation that failed
    ///
    /// # Returns
    ///
    /// Result containing the operation result, or None if a prior exception occurred or the operation only threw retriable or tolerable exceptions
    pub async fn execute<V>(
        &self,
        context: &mut ProcessingContext<T>,
        operation: &dyn Operation<V>,
        stage: Stage,
        executing_class: TypeId,
    ) -> Result<Option<V>, ConnectRuntimeError> {
        context.current_context(stage, executing_class);

        if context.failed() {
            debug!("ProcessingContext is already in failed state. Ignoring requested operation.");
            return Ok(None);
        }

        context.current_context(stage, executing_class);

        let result = self.exec_and_handle_error(context, operation, stage).await;

        if context.failed() {
            self.error_handling_metrics.record_error();
            self.report(context).await;
        }

        result
    }

    /// Attempt to execute an operation. Handles retriable and tolerated exceptions thrown by the operation.
    ///
    /// # Arguments
    ///
    /// * `context` - The ProcessingContext used to hold state about this operation
    /// * `operation` - The operation to be executed
    /// * `stage` - The stage for determining tolerable exceptions
    ///
    /// # Returns
    ///
    /// Result containing the operation result, or None if the operation only threw retriable or tolerable exceptions
    async fn exec_and_handle_error<V>(
        &self,
        context: &mut ProcessingContext<T>,
        operation: &dyn Operation<V>,
        stage: Stage,
    ) -> Result<Option<V>, ConnectRuntimeError> {
        let result = self.exec_and_retry(context, operation).await;

        if context.failed() {
            self.mark_as_failed();
            self.error_handling_metrics.record_skipped();
        }

        result
    }

    /// Attempt to execute an operation. Handles retriable exceptions raised by the operation.
    ///
    /// # Arguments
    ///
    /// * `context` - The ProcessingContext used to hold state about this operation
    /// * `operation` - The operation to be executed
    ///
    /// # Returns
    ///
    /// Result containing the operation result if it succeeded, or None if the operation only threw retriable exceptions
    async fn exec_and_retry<V>(
        &self,
        context: &mut ProcessingContext<T>,
        operation: &dyn Operation<V>,
    ) -> Result<Option<V>, ConnectRuntimeError> {
        let mut attempt = 0;
        let start_time = Instant::now();
        let deadline = if self.error_retry_timeout >= 0 {
            start_time + Duration::from_millis(self.error_retry_timeout as u64)
        } else {
            // No deadline
            Instant::now() + Duration::from_secs(u64::MAX)
        };

        loop {
            attempt += 1;

            match operation.call() {
                Ok(result) => {
                    context.attempt(attempt);
                    return Ok(Some(result));
                }
                Err(e) => {
                    trace!(
                        "Caught a retriable exception while executing {} operation",
                        context.stage().map(|s| s.name()).unwrap_or("unknown")
                    );
                    self.error_handling_metrics.record_failure();

                    let now = Instant::now();
                    if now < deadline {
                        self.backoff(attempt, deadline).await;
                        self.error_handling_metrics.record_retry();
                    } else {
                        trace!(
                            "Can't retry. start={}, attempt={}, deadline={:?}",
                            start_time.elapsed().as_millis(),
                            attempt,
                            deadline
                        );
                        context.set_error(e);
                        context.attempt(attempt);
                        return Ok(None);
                    }

                    if self.stopping.load(Ordering::Relaxed) {
                        trace!("Shutdown has been scheduled. Marking operation as failed.");
                        context.set_error(e);
                        context.attempt(attempt);
                        return Ok(None);
                    }

                    context.attempt(attempt);
                }
            }
        }
    }

    /// Mark as failed
    fn mark_as_failed(&self) {
        self.error_handling_metrics.record_error_timestamp();
        self.total_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if within tolerance limits
    pub fn within_tolerance_limits(&self) -> bool {
        match self.error_tolerance_type {
            ToleranceType::None => self.total_failures.load(Ordering::Relaxed) == 0,
            ToleranceType::All => true,
        }
    }

    /// Get error tolerance type
    pub fn get_error_tolerance_type(&self) -> ToleranceType {
        self.error_tolerance_type
    }

    /// Do an exponential backoff bounded by RETRIES_DELAY_MIN_MS and error_max_delay_in_millis
    /// which can be exited prematurely if trigger_stop is called or if the thread is interrupted.
    ///
    /// # Arguments
    ///
    /// * `attempt` - The number indicating which backoff attempt it is (beginning with 1)
    /// * `deadline` - The time until when retries can be attempted
    async fn backoff(&self, attempt: i32, deadline: Instant) {
        let num_retry = (attempt - 1) as u32;
        let mut delay = RETRIES_DELAY_MIN_MS << num_retry;

        if delay > self.error_max_delay_in_millis as u64 {
            use std::time::SystemTime;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            delay = (now % self.error_max_delay_in_millis as u64);
        }

        let now = Instant::now();
        if delay > 0 && now + Duration::from_millis(delay) > deadline {
            let remaining = deadline.saturating_duration_since(now);
            delay = remaining.as_millis() as u64;
        }

        debug!("Sleeping for up to {} millis", delay);

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(delay)) => {
                // Sleep completed
            }
            _ = self.wait_for_stop() => {
                // Stop requested
            }
        }
    }

    /// Wait for stop signal
    async fn wait_for_stop(&self) {
        while !self.stopping.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Set the error reporters for this connector
    ///
    /// # Arguments
    ///
    /// * `reporters` - the error reporters (should not be null)
    pub async fn reporters(&self, reporters: Vec<Arc<dyn ErrorReporter<T>>>) {
        let mut r = self.reporters.lock().await;
        *r = reporters;
    }

    /// This will stop any further retries for operations.
    /// This will also mark any ongoing operations that are currently backing off for retry as failed.
    /// This can be called from a separate thread to break out of retry/backoff loops.
    pub fn trigger_stop(&self) {
        self.stopping.store(true, Ordering::Relaxed);
    }
}

impl<T> std::fmt::Display for RetryWithToleranceOperator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RetryWithToleranceOperator{{error_retry_timeout={}, error_max_delay_in_millis={}, error_tolerance_type={}, total_failures={}}}",
            self.error_retry_timeout,
            self.error_max_delay_in_millis,
            self.error_tolerance_type,
            self.total_failures.load(Ordering::Relaxed)
        )
    }
}
