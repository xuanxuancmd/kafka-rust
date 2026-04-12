//! Error handling metrics module
//!
//! Provides metrics for monitoring errors in Connect runtime.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tracing::debug;

/// Contains various sensors used for monitoring errors.
pub struct ErrorHandlingMetrics {
    /// The number of failed operations (retriable and non-retriable)
    record_processing_failures: Arc<AtomicI64>,

    /// The number of operations which could not be successfully executed
    record_processing_errors: Arc<AtomicI64>,

    /// The number of records skipped
    records_skipped: Arc<AtomicI64>,

    /// The number of retries made while executing operations
    retries: Arc<AtomicI64>,

    /// The number of errors logged by the LogReporter
    errors_logged: Arc<AtomicI64>,

    /// The number of produce requests to the DeadLetterQueueReporter
    dlq_produce_requests: Arc<AtomicI64>,

    /// The number of produce requests to the DeadLetterQueueReporter which failed to be successfully produced into Kafka
    dlq_produce_failures: Arc<AtomicI64>,

    /// The timestamp of the last error
    last_error_time: Arc<AtomicI64>,
}

impl ErrorHandlingMetrics {
    /// Create a new error handling metrics instance
    pub fn new() -> Self {
        Self {
            record_processing_failures: Arc::new(AtomicI64::new(0)),
            record_processing_errors: Arc::new(AtomicI64::new(0)),
            records_skipped: Arc::new(AtomicI64::new(0)),
            retries: Arc::new(AtomicI64::new(0)),
            errors_logged: Arc::new(AtomicI64::new(0)),
            dlq_produce_requests: Arc::new(AtomicI64::new(0)),
            dlq_produce_failures: Arc::new(AtomicI64::new(0)),
            last_error_time: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Increment the number of failed operations (retriable and non-retriable)
    pub fn record_failure(&self) {
        self.record_processing_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the number of operations which could not be successfully executed
    pub fn record_error(&self) {
        self.record_processing_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the number of records skipped
    pub fn record_skipped(&self) {
        self.records_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// The number of retries made while executing operations
    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    /// The number of errors logged by the LogReporter
    pub fn record_error_logged(&self) {
        self.errors_logged.fetch_add(1, Ordering::Relaxed);
    }

    /// The number of produce requests to the DeadLetterQueueReporter
    pub fn record_dead_letter_queue_produce_request(&self) {
        self.dlq_produce_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// The number of produce requests to the DeadLetterQueueReporter which failed to be successfully produced into Kafka
    pub fn record_dead_letter_queue_produce_failed(&self) {
        self.dlq_produce_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the time of error
    pub fn record_error_timestamp(&self) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.last_error_time.store(now, Ordering::Relaxed);
    }

    /// Get the number of failed operations
    pub fn get_record_processing_failures(&self) -> i64 {
        self.record_processing_failures.load(Ordering::Relaxed)
    }

    /// Get the number of operations which could not be successfully executed
    pub fn get_record_processing_errors(&self) -> i64 {
        self.record_processing_errors.load(Ordering::Relaxed)
    }

    /// Get the number of records skipped
    pub fn get_records_skipped(&self) -> i64 {
        self.records_skipped.load(Ordering::Relaxed)
    }

    /// Get the number of retries made while executing operations
    pub fn get_retries(&self) -> i64 {
        self.retries.load(Ordering::Relaxed)
    }

    /// Get the number of errors logged by the LogReporter
    pub fn get_errors_logged(&self) -> i64 {
        self.errors_logged.load(Ordering::Relaxed)
    }

    /// Get the number of produce requests to the DeadLetterQueueReporter
    pub fn get_dlq_produce_requests(&self) -> i64 {
        self.dlq_produce_requests.load(Ordering::Relaxed)
    }

    /// Get the number of produce requests to the DeadLetterQueueReporter which failed
    pub fn get_dlq_produce_failures(&self) -> i64 {
        self.dlq_produce_failures.load(Ordering::Relaxed)
    }

    /// Get the timestamp of the last error
    pub fn get_last_error_time(&self) -> i64 {
        self.last_error_time.load(Ordering::Relaxed)
    }
}

impl Default for ErrorHandlingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ErrorHandlingMetrics {
    fn drop(&mut self) {
        debug!("Removing error handling metrics");
    }
}
