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

//! ErrorHandlingMetrics for tracking error handling statistics.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics` in Java.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::Stage;

/// ErrorHandlingMetrics tracks metrics related to error handling during
/// connector operation.
///
/// This struct tracks counts of:
/// - Total errors encountered
/// - Errors retried successfully
/// - Errors tolerated (skipped)
/// - Errors that caused task failure
/// - Errors logged by LogReporter
/// - DLQ produce requests and failures
/// - Per-stage error counts
/// - Last error timestamp
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics`
pub struct ErrorHandlingMetrics {
    /// Total number of errors encountered (recordProcessingFailures in Java)
    total_errors: AtomicU64,
    /// Number of errors that were successfully retried (retries in Java)
    retried_errors: AtomicU64,
    /// Number of errors that were tolerated (skipped) (recordsSkipped in Java)
    tolerated_errors: AtomicU64,
    /// Number of errors that caused task failure (recordProcessingErrors in Java)
    failed_errors: AtomicU64,
    /// Number of errors logged by LogReporter (errorsLogged in Java)
    errors_logged: AtomicU64,
    /// Number of DLQ produce requests (dlqProduceRequests in Java)
    dlq_produce_requests: AtomicU64,
    /// Number of DLQ produce failures (dlqProduceFailures in Java)
    dlq_produce_failures: AtomicU64,
    /// Timestamp of the last error (lastErrorTimestamp in Java)
    last_error_timestamp: AtomicU64,
    /// Number of errors at the transformation stage
    transformation_errors: AtomicU64,
    /// Number of errors at the key converter stage
    key_converter_errors: AtomicU64,
    /// Number of errors at the value converter stage
    value_converter_errors: AtomicU64,
    /// Number of errors at the header converter stage
    header_converter_errors: AtomicU64,
    /// Number of errors at the kafka produce stage
    kafka_produce_errors: AtomicU64,
    /// Number of errors at the kafka consume stage
    kafka_consume_errors: AtomicU64,
    /// Number of errors at the task put stage (sink task put operation)
    task_put_errors: AtomicU64,
}

impl ErrorHandlingMetrics {
    /// Creates a new ErrorHandlingMetrics with all counters initialized to zero.
    pub fn new() -> Self {
        ErrorHandlingMetrics {
            total_errors: AtomicU64::new(0),
            retried_errors: AtomicU64::new(0),
            tolerated_errors: AtomicU64::new(0),
            failed_errors: AtomicU64::new(0),
            errors_logged: AtomicU64::new(0),
            dlq_produce_requests: AtomicU64::new(0),
            dlq_produce_failures: AtomicU64::new(0),
            last_error_timestamp: AtomicU64::new(0),
            transformation_errors: AtomicU64::new(0),
            key_converter_errors: AtomicU64::new(0),
            value_converter_errors: AtomicU64::new(0),
            header_converter_errors: AtomicU64::new(0),
            kafka_produce_errors: AtomicU64::new(0),
            kafka_consume_errors: AtomicU64::new(0),
            task_put_errors: AtomicU64::new(0),
        }
    }

    /// Records an error at a specific stage.
    pub fn record_error(&self, stage: Stage) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        self.increment_stage_error(stage);
    }

    /// Records an error that was successfully retried.
    pub fn record_retried_error(&self, stage: Stage) {
        self.retried_errors.fetch_add(1, Ordering::Relaxed);
        self.record_error(stage);
    }

    /// Records an error that was tolerated (skipped).
    pub fn record_tolerated_error(&self, stage: Stage) {
        self.tolerated_errors.fetch_add(1, Ordering::Relaxed);
        self.record_error(stage);
    }

    /// Records an error that caused task failure.
    pub fn record_failed_error(&self, stage: Stage) {
        self.failed_errors.fetch_add(1, Ordering::Relaxed);
        self.record_error(stage);
    }

    /// Records that an error was logged by LogReporter.
    /// Corresponds to Java: `recordErrorLogged()`
    pub fn record_error_logged(&self) {
        self.errors_logged.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a DLQ produce request.
    /// Corresponds to Java: `recordDeadLetterQueueProduceRequest()`
    pub fn record_dlq_produce_request(&self) {
        self.dlq_produce_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a DLQ produce failure.
    /// Corresponds to Java: `recordDeadLetterQueueProduceFailed()`
    pub fn record_dlq_produce_failed(&self) {
        self.dlq_produce_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Records the timestamp of the last error.
    /// Corresponds to Java: `recordErrorTimestamp()`
    pub fn record_error_timestamp(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_error_timestamp.store(now, Ordering::Relaxed);
    }

    /// Returns the total number of errors encountered.
    pub fn total_errors(&self) -> u64 {
        self.total_errors.load(Ordering::Relaxed)
    }

    /// Returns the number of errors that were successfully retried.
    pub fn retried_errors(&self) -> u64 {
        self.retried_errors.load(Ordering::Relaxed)
    }

    /// Returns the number of errors that were tolerated.
    pub fn tolerated_errors(&self) -> u64 {
        self.tolerated_errors.load(Ordering::Relaxed)
    }

    /// Returns the number of errors that caused task failure.
    pub fn failed_errors(&self) -> u64 {
        self.failed_errors.load(Ordering::Relaxed)
    }

    /// Returns the number of errors logged by LogReporter.
    /// Corresponds to Java: errorsLogged metric
    pub fn errors_logged(&self) -> u64 {
        self.errors_logged.load(Ordering::Relaxed)
    }

    /// Returns the number of DLQ produce requests.
    /// Corresponds to Java: dlqProduceRequests metric
    pub fn dlq_produce_requests(&self) -> u64 {
        self.dlq_produce_requests.load(Ordering::Relaxed)
    }

    /// Returns the number of DLQ produce failures.
    /// Corresponds to Java: dlqProduceFailures metric
    pub fn dlq_produce_failures(&self) -> u64 {
        self.dlq_produce_failures.load(Ordering::Relaxed)
    }

    /// Returns the timestamp of the last error.
    /// Corresponds to Java: lastErrorTimestamp metric
    pub fn last_error_timestamp(&self) -> u64 {
        self.last_error_timestamp.load(Ordering::Relaxed)
    }

    /// Returns the number of errors at a specific stage.
    pub fn stage_errors(&self, stage: Stage) -> u64 {
        match stage {
            Stage::TRANSFORMATION => self.transformation_errors.load(Ordering::Relaxed),
            Stage::KEY_CONVERTER => self.key_converter_errors.load(Ordering::Relaxed),
            Stage::VALUE_CONVERTER => self.value_converter_errors.load(Ordering::Relaxed),
            Stage::HEADER_CONVERTER => self.header_converter_errors.load(Ordering::Relaxed),
            Stage::KAFKA_PRODUCE => self.kafka_produce_errors.load(Ordering::Relaxed),
            Stage::KAFKA_CONSUME => self.kafka_consume_errors.load(Ordering::Relaxed),
            Stage::TASK_PUT => self.task_put_errors.load(Ordering::Relaxed),
        }
    }

    /// Increments the error counter for a specific stage.
    fn increment_stage_error(&self, stage: Stage) {
        match stage {
            Stage::TRANSFORMATION => self.transformation_errors.fetch_add(1, Ordering::Relaxed),
            Stage::KEY_CONVERTER => self.key_converter_errors.fetch_add(1, Ordering::Relaxed),
            Stage::VALUE_CONVERTER => self.value_converter_errors.fetch_add(1, Ordering::Relaxed),
            Stage::HEADER_CONVERTER => self.header_converter_errors.fetch_add(1, Ordering::Relaxed),
            Stage::KAFKA_PRODUCE => self.kafka_produce_errors.fetch_add(1, Ordering::Relaxed),
            Stage::KAFKA_CONSUME => self.kafka_consume_errors.fetch_add(1, Ordering::Relaxed),
            Stage::TASK_PUT => self.task_put_errors.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Resets all counters to zero.
    pub fn reset(&self) {
        self.total_errors.store(0, Ordering::Relaxed);
        self.retried_errors.store(0, Ordering::Relaxed);
        self.tolerated_errors.store(0, Ordering::Relaxed);
        self.failed_errors.store(0, Ordering::Relaxed);
        self.errors_logged.store(0, Ordering::Relaxed);
        self.dlq_produce_requests.store(0, Ordering::Relaxed);
        self.dlq_produce_failures.store(0, Ordering::Relaxed);
        self.last_error_timestamp.store(0, Ordering::Relaxed);
        self.transformation_errors.store(0, Ordering::Relaxed);
        self.key_converter_errors.store(0, Ordering::Relaxed);
        self.value_converter_errors.store(0, Ordering::Relaxed);
        self.header_converter_errors.store(0, Ordering::Relaxed);
        self.kafka_produce_errors.store(0, Ordering::Relaxed);
        self.kafka_consume_errors.store(0, Ordering::Relaxed);
        self.task_put_errors.store(0, Ordering::Relaxed);
    }

    /// Returns a summary of all metrics.
    pub fn summary(&self) -> String {
        format!(
            "ErrorHandlingMetrics: total={}, retried={}, tolerated={}, failed={}, \
            errors_logged={}, dlq_requests={}, dlq_failures={}, last_error_ts={}, \
            transformation={}, key_converter={}, value_converter={}, header_converter={}, \
            kafka_produce={}, kafka_consume={}, task_put={}",
            self.total_errors(),
            self.retried_errors(),
            self.tolerated_errors(),
            self.failed_errors(),
            self.errors_logged(),
            self.dlq_produce_requests(),
            self.dlq_produce_failures(),
            self.last_error_timestamp(),
            self.stage_errors(Stage::TRANSFORMATION),
            self.stage_errors(Stage::KEY_CONVERTER),
            self.stage_errors(Stage::VALUE_CONVERTER),
            self.stage_errors(Stage::HEADER_CONVERTER),
            self.stage_errors(Stage::KAFKA_PRODUCE),
            self.stage_errors(Stage::KAFKA_CONSUME),
            self.stage_errors(Stage::TASK_PUT)
        )
    }
}

impl Default for ErrorHandlingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ErrorHandlingMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErrorHandlingMetrics")
            .field("total_errors", &self.total_errors())
            .field("retried_errors", &self.retried_errors())
            .field("tolerated_errors", &self.tolerated_errors())
            .field("failed_errors", &self.failed_errors())
            .field("errors_logged", &self.errors_logged())
            .field("dlq_produce_requests", &self.dlq_produce_requests())
            .field("dlq_produce_failures", &self.dlq_produce_failures())
            .field("last_error_timestamp", &self.last_error_timestamp())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_handling_metrics_new() {
        let metrics = ErrorHandlingMetrics::new();
        assert_eq!(metrics.total_errors(), 0);
        assert_eq!(metrics.retried_errors(), 0);
        assert_eq!(metrics.tolerated_errors(), 0);
        assert_eq!(metrics.failed_errors(), 0);
        assert_eq!(metrics.errors_logged(), 0);
        assert_eq!(metrics.dlq_produce_requests(), 0);
        assert_eq!(metrics.dlq_produce_failures(), 0);
        assert_eq!(metrics.last_error_timestamp(), 0);
    }

    #[test]
    fn test_error_handling_metrics_record_error() {
        let metrics = ErrorHandlingMetrics::new();
        metrics.record_error(Stage::KEY_CONVERTER);
        assert_eq!(metrics.total_errors(), 1);
        assert_eq!(metrics.stage_errors(Stage::KEY_CONVERTER), 1);
    }

    #[test]
    fn test_error_handling_metrics_record_retried_error() {
        let metrics = ErrorHandlingMetrics::new();
        metrics.record_retried_error(Stage::TRANSFORMATION);
        assert_eq!(metrics.total_errors(), 1);
        assert_eq!(metrics.retried_errors(), 1);
        assert_eq!(metrics.stage_errors(Stage::TRANSFORMATION), 1);
    }

    #[test]
    fn test_error_handling_metrics_record_tolerated_error() {
        let metrics = ErrorHandlingMetrics::new();
        metrics.record_tolerated_error(Stage::VALUE_CONVERTER);
        assert_eq!(metrics.total_errors(), 1);
        assert_eq!(metrics.tolerated_errors(), 1);
        assert_eq!(metrics.stage_errors(Stage::VALUE_CONVERTER), 1);
    }

    #[test]
    fn test_error_handling_metrics_record_failed_error() {
        let metrics = ErrorHandlingMetrics::new();
        metrics.record_failed_error(Stage::KAFKA_PRODUCE);
        assert_eq!(metrics.total_errors(), 1);
        assert_eq!(metrics.failed_errors(), 1);
        assert_eq!(metrics.stage_errors(Stage::KAFKA_PRODUCE), 1);
    }

    #[test]
    fn test_error_handling_metrics_multiple_errors() {
        let metrics = ErrorHandlingMetrics::new();
        metrics.record_error(Stage::KEY_CONVERTER);
        metrics.record_error(Stage::KEY_CONVERTER);
        metrics.record_error(Stage::VALUE_CONVERTER);
        assert_eq!(metrics.total_errors(), 3);
        assert_eq!(metrics.stage_errors(Stage::KEY_CONVERTER), 2);
        assert_eq!(metrics.stage_errors(Stage::VALUE_CONVERTER), 1);
    }

    #[test]
    fn test_error_handling_metrics_reset() {
        let metrics = ErrorHandlingMetrics::new();
        metrics.record_error(Stage::TRANSFORMATION);
        metrics.record_retried_error(Stage::KEY_CONVERTER);
        metrics.reset();
        assert_eq!(metrics.total_errors(), 0);
        assert_eq!(metrics.retried_errors(), 0);
        assert_eq!(metrics.stage_errors(Stage::TRANSFORMATION), 0);
    }

    #[test]
    fn test_error_handling_metrics_summary() {
        let metrics = ErrorHandlingMetrics::new();
        metrics.record_error(Stage::KEY_CONVERTER);
        let summary = metrics.summary();
        assert!(summary.contains("total=1"));
        assert!(summary.contains("key_converter=1"));
    }

    #[test]
    fn test_error_handling_metrics_all_stages() {
        let metrics = ErrorHandlingMetrics::new();
        metrics.record_error(Stage::TRANSFORMATION);
        metrics.record_error(Stage::KEY_CONVERTER);
        metrics.record_error(Stage::VALUE_CONVERTER);
        metrics.record_error(Stage::HEADER_CONVERTER);
        metrics.record_error(Stage::KAFKA_PRODUCE);
        metrics.record_error(Stage::KAFKA_CONSUME);

        assert_eq!(metrics.total_errors(), 6);
        assert_eq!(metrics.stage_errors(Stage::TRANSFORMATION), 1);
        assert_eq!(metrics.stage_errors(Stage::KAFKA_CONSUME), 1);
    }

    #[test]
    fn test_error_handling_metrics_record_error_logged() {
        let metrics = ErrorHandlingMetrics::new();
        assert_eq!(metrics.errors_logged(), 0);
        metrics.record_error_logged();
        assert_eq!(metrics.errors_logged(), 1);
        metrics.record_error_logged();
        assert_eq!(metrics.errors_logged(), 2);
    }

    #[test]
    fn test_error_handling_metrics_record_dlq_produce_request() {
        let metrics = ErrorHandlingMetrics::new();
        assert_eq!(metrics.dlq_produce_requests(), 0);
        metrics.record_dlq_produce_request();
        assert_eq!(metrics.dlq_produce_requests(), 1);
        metrics.record_dlq_produce_request();
        assert_eq!(metrics.dlq_produce_requests(), 2);
    }

    #[test]
    fn test_error_handling_metrics_record_dlq_produce_failed() {
        let metrics = ErrorHandlingMetrics::new();
        assert_eq!(metrics.dlq_produce_failures(), 0);
        metrics.record_dlq_produce_failed();
        assert_eq!(metrics.dlq_produce_failures(), 1);
        metrics.record_dlq_produce_failed();
        assert_eq!(metrics.dlq_produce_failures(), 2);
    }

    #[test]
    fn test_error_handling_metrics_record_error_timestamp() {
        let metrics = ErrorHandlingMetrics::new();
        assert_eq!(metrics.last_error_timestamp(), 0);

        // Record error timestamp
        metrics.record_error_timestamp();
        let ts1 = metrics.last_error_timestamp();
        assert!(ts1 > 0);

        // Wait a bit and record again
        std::thread::sleep(std::time::Duration::from_millis(10));
        metrics.record_error_timestamp();
        let ts2 = metrics.last_error_timestamp();
        assert!(ts2 >= ts1);
    }

    #[test]
    fn test_error_handling_metrics_reset_includes_new_counters() {
        let metrics = ErrorHandlingMetrics::new();
        metrics.record_error_logged();
        metrics.record_dlq_produce_request();
        metrics.record_dlq_produce_failed();
        metrics.record_error_timestamp();

        metrics.reset();

        assert_eq!(metrics.errors_logged(), 0);
        assert_eq!(metrics.dlq_produce_requests(), 0);
        assert_eq!(metrics.dlq_produce_failures(), 0);
        assert_eq!(metrics.last_error_timestamp(), 0);
    }

    #[test]
    fn test_error_handling_metrics_summary_includes_new_counters() {
        let metrics = ErrorHandlingMetrics::new();
        metrics.record_error_logged();
        metrics.record_dlq_produce_request();
        metrics.record_dlq_produce_failed();

        let summary = metrics.summary();
        assert!(summary.contains("errors_logged=1"));
        assert!(summary.contains("dlq_requests=1"));
        assert!(summary.contains("dlq_failures=1"));
    }
}
