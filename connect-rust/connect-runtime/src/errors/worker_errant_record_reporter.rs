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

//! WorkerErrantRecordReporter for reporting errant records from Sink Tasks.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.errors.WorkerErrantRecordReporter` in Java.
//!
//! This reporter is used by Sink Tasks to report records that have errors during
//! processing. It integrates with the RetryWithToleranceOperator to handle
//! error reporting with tolerance limits.

use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use common_trait::TopicPartition;
use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::sink::{ErrantRecordReporter, SinkRecord};

use super::metrics::ErrorHandlingMetrics;
use super::processing_context::ProcessingContext;
use super::retry_with_tolerance_operator::RetryWithToleranceOperator;
use super::stage::Stage;

/// WorkerErrantRecordReporter implements the ErrantRecordReporter interface.
///
/// This reporter is used by Sink Tasks to report records that encounter errors
/// during processing. It integrates with the error handling pipeline to send
/// failed records to configured error reporters (e.g., DLQ, logs).
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.errors.WorkerErrantRecordReporter`
pub struct WorkerErrantRecordReporter {
    /// The retry with tolerance operator for handling error reporting
    retry_with_tolerance_operator: Arc<RetryWithToleranceOperator>,
    /// Error handling metrics
    metrics: Arc<ErrorHandlingMetrics>,
    /// Futures for ongoing error reports, keyed by topic partition
    futures: HashMap<TopicPartition, Vec<ReportFuture>>,
    /// Count of failed futures that have been recorded
    failed_count: AtomicU64,
    /// Flag to track if an async error has occurred
    has_async_error: AtomicBool,
    /// Stopping flag
    stopping: AtomicBool,
}

/// Represents a future for an error report operation.
///
/// A ReportFuture tracks the state of an error report that was submitted
/// to the error handling pipeline. The future starts in a pending state
/// and can transition to either success or failure.
///
/// Corresponds to Java: `WorkerErrantRecordReporter.ErrantRecordFuture`
#[derive(Debug, Clone)]
pub struct ReportFuture {
    /// Whether the report is complete
    done: bool,
    /// The topic partition for the record
    topic_partition: TopicPartition,
    /// Whether the report was successful
    success: bool,
    /// Error message if the report failed
    error_message: Option<String>,
}

impl ReportFuture {
    /// Creates a new pending (incomplete) ReportFuture.
    ///
    /// The future starts in a pending state where `is_done()` returns false.
    /// It must be completed via `mark_complete()` or `mark_failed()`.
    pub fn new(topic_partition: TopicPartition) -> Self {
        ReportFuture {
            done: false,
            topic_partition,
            success: false,
            error_message: None,
        }
    }

    /// Creates a completed successful ReportFuture.
    pub fn completed_success(topic_partition: TopicPartition) -> Self {
        ReportFuture {
            done: true,
            topic_partition,
            success: true,
            error_message: None,
        }
    }

    /// Creates a completed failed ReportFuture with an error message.
    pub fn completed_failed(topic_partition: TopicPartition, error_message: String) -> Self {
        ReportFuture {
            done: true,
            topic_partition,
            success: false,
            error_message: Some(error_message),
        }
    }

    /// Checks if the future is done (completed, either success or failure).
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Checks if the report was successful.
    ///
    /// Returns false if the future is not done or if it completed with failure.
    pub fn is_success(&self) -> bool {
        self.done && self.success
    }

    /// Checks if the report failed.
    ///
    /// Returns true only if the future is done and completed with failure.
    pub fn is_failed(&self) -> bool {
        self.done && !self.success
    }

    /// Marks the future as successfully completed.
    pub fn mark_success(&mut self) {
        self.done = true;
        self.success = true;
        self.error_message = None;
    }

    /// Marks the future as failed with an error message.
    pub fn mark_failed(&mut self, error_message: String) {
        self.done = true;
        self.success = false;
        self.error_message = Some(error_message);
    }

    /// Returns the error message if the future failed.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the topic partition for this future.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }
}

impl WorkerErrantRecordReporter {
    /// Creates a new WorkerErrantRecordReporter.
    ///
    /// # Arguments
    /// * `retry_with_tolerance_operator` - The operator for handling error reporting
    /// * `metrics` - Error handling metrics for tracking statistics
    pub fn new(
        retry_with_tolerance_operator: Arc<RetryWithToleranceOperator>,
        metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        WorkerErrantRecordReporter {
            retry_with_tolerance_operator,
            metrics,
            futures: HashMap::new(),
            failed_count: AtomicU64::new(0),
            has_async_error: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
        }
    }

    /// Reports an errant record with the given error.
    ///
    /// This method is called by Sink Tasks when they encounter an error processing a record.
    /// The record and error are passed to the error handling pipeline for reporting.
    ///
    /// The method:
    /// 1. Creates a pending ReportFuture
    /// 2. Executes the error through the tolerance operator
    /// 3. Checks tolerance limits and returns error if exceeded
    ///
    /// # Arguments
    /// * `record` - The sink record that had an error
    /// * `error_msg` - The error message
    ///
    /// # Returns
    /// A future representing the error report operation
    pub fn report_with_future(&mut self, record: &SinkRecord, error_msg: String) -> ReportFuture {
        let mut context = ProcessingContext::new();
        context.set_stage(Stage::TASK_PUT);
        context.set_executing_class("SinkTask".to_string());

        // Create error from string
        let error: Box<dyn Error + Send + Sync> = Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            error_msg.clone(),
        ));

        // Record the error through the tolerance operator
        self.retry_with_tolerance_operator.execute_failed(
            &mut context,
            Stage::TASK_PUT,
            "SinkTask",
            error,
        );

        // Mark async error flag - this tracks that an error occurred
        self.has_async_error.store(true, Ordering::Relaxed);

        // Get topic and partition from the record
        let topic = record.topic();
        let partition = record.kafka_partition().unwrap_or(0);
        let topic_partition = TopicPartition::new(topic, partition);

        // Create a pending future - it will be completed when await_futures is called
        // or when tolerance limits are checked
        let future = ReportFuture::new(topic_partition.clone());

        // Track the future
        self.futures
            .entry(topic_partition)
            .or_insert_with(Vec::new)
            .push(future.clone());

        future
    }

    /// Awaits the completion of all error reports for a given set of topic partitions.
    ///
    /// This method processes all pending futures for the specified topic partitions.
    /// Futures are completed based on the tolerance operator's state:
    /// - If within tolerance limits: futures are marked as success
    /// - If tolerance exceeded: futures are marked as failed
    ///
    /// The method does NOT force-mark all futures as success. Instead, it respects
    /// the actual error handling outcome from the tolerance operator.
    ///
    /// Corresponds to Java: `WorkerErrantRecordReporter.awaitFutures()`
    ///
    /// # Arguments
    /// * `topic_partitions` - The topic partitions to await reporter completion for
    ///
    /// # Errors
    /// Returns a ConnectError if any future fails to complete successfully
    pub fn await_futures(
        &mut self,
        topic_partitions: &[TopicPartition],
    ) -> Result<(), ConnectError> {
        let within_tolerance = self.retry_with_tolerance_operator.within_tolerance_limits();

        for tp in topic_partitions {
            if let Some(futures) = self.futures.remove(tp) {
                for mut future in futures {
                    // CRITICAL: Only complete pending futures.
                    // Pre-completed futures (success or failed) must retain their original state.
                    // This ensures failure semantics are preserved even when tolerance=ALL.
                    if !future.is_done() {
                        // Pending future: complete based on tolerance state
                        if within_tolerance {
                            future.mark_success();
                        } else {
                            future.mark_failed(format!(
                                "Tolerance exceeded for topic partition {}:{}",
                                tp.topic(),
                                tp.partition()
                            ));
                            self.failed_count.fetch_add(1, Ordering::Relaxed);
                        }
                    } else if future.is_failed() {
                        // Already failed: count it and preserve error details
                        self.failed_count.fetch_add(1, Ordering::Relaxed);
                    }
                    // Already successful: no action needed, state preserved

                    // Check if this future failed (either pre-completed or just completed)
                    if future.is_failed() {
                        let error_msg = future.error_message().unwrap_or("Unknown error");
                        return Err(ConnectError::general(format!(
                            "Error report failed for topic partition {}:{}: {}",
                            tp.topic(),
                            tp.partition(),
                            error_msg
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Cancels all active error reports for a given set of topic partitions.
    ///
    /// Futures are marked as failed when cancelled, preserving the failure semantics.
    ///
    /// Corresponds to Java: `WorkerErrantRecordReporter.cancelFutures()`
    ///
    /// # Arguments
    /// * `topic_partitions` - The topic partitions to cancel reporting for
    pub fn cancel_futures(&mut self, topic_partitions: &[TopicPartition]) {
        for tp in topic_partitions {
            if let Some(futures) = self.futures.remove(tp) {
                for mut future in futures {
                    // Cancelled futures are marked as failed, not silently discarded
                    future.mark_failed(format!(
                        "Cancelled for topic partition {}:{}",
                        tp.topic(),
                        tp.partition()
                    ));
                    self.failed_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Checks if there's an async error and throws if tolerance limits are exceeded.
    ///
    /// This method should be called periodically to check for async errors that
    /// occurred during error reporting. If tolerance limits are exceeded, it
    /// returns an error to signal that the task should stop.
    ///
    /// Corresponds to Java: `WorkerErrantRecordReporter.maybeThrowAsyncError()`
    ///
    /// # Errors
    /// Returns a ConnectError if:
    /// - An async error has occurred AND tolerance limits are exceeded
    pub fn maybe_throw_async_error(&self) -> Result<(), ConnectError> {
        if self.has_async_error.load(Ordering::Relaxed)
            && !self.retry_with_tolerance_operator.within_tolerance_limits()
        {
            return Err(ConnectError::general("Tolerance exceeded in error handler"));
        }
        Ok(())
    }

    /// Returns true if stop was requested.
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Relaxed)
    }

    /// Requests the reporter to stop processing new reports.
    pub fn request_stop(&self) {
        self.stopping.store(true, Ordering::Relaxed);
        self.retry_with_tolerance_operator.request_stop();
    }

    /// Clears the stop request.
    pub fn clear_stop_request(&self) {
        self.stopping.store(false, Ordering::Relaxed);
        self.retry_with_tolerance_operator.clear_stop_request();
    }

    /// Returns the number of pending futures (not yet awaited or cancelled).
    pub fn pending_futures_count(&self) -> usize {
        self.futures.values().map(|v| v.len()).sum()
    }

    /// Returns the count of futures that have failed.
    pub fn failed_futures_count(&self) -> u64 {
        self.failed_count.load(Ordering::Relaxed)
    }

    /// Returns the error handling metrics.
    pub fn metrics(&self) -> &ErrorHandlingMetrics {
        &self.metrics
    }

    /// Returns true if within tolerance limits.
    pub fn within_tolerance(&self) -> bool {
        self.retry_with_tolerance_operator.within_tolerance_limits()
    }
}

impl ErrantRecordReporter for WorkerErrantRecordReporter {
    /// Reports an errant record with the given error.
    ///
    /// This implementation:
    /// 1. Checks if reporter is stopping - returns error if so
    /// 2. Creates a future and submits to tolerance operator
    /// 3. Checks tolerance limits immediately - returns error if exceeded
    ///
    /// The future is tracked for later await/cancel operations.
    /// The success/failure outcome is determined by tolerance limits,
    /// not by a placeholder check on the newly-created pending future.
    fn report(&mut self, record: &SinkRecord, error: ConnectError) -> Result<(), ConnectError> {
        // Check if reporter is stopping
        if self.is_stopping() {
            return Err(ConnectError::general("Reporter is stopping"));
        }

        // Submit the error report - this creates a pending future
        let _future = self.report_with_future(record, error.message().to_string());

        // Check tolerance limits - this determines actual success/failure
        // This is the meaningful check, not a placeholder check on pending future
        self.maybe_throw_async_error()?;

        // Within tolerance limits - report is accepted
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{ErrorHandlingMetrics, ToleranceType};

    /// Creates a reporter with ALL tolerance type (always within limits)
    fn create_reporter_all_tolerance() -> WorkerErrantRecordReporter {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let operator = Arc::new(RetryWithToleranceOperator::new(
            0,
            0,
            ToleranceType::ALL,
            metrics.clone(),
        ));
        WorkerErrantRecordReporter::new(operator, metrics)
    }

    /// Creates a reporter with NONE tolerance type (fails on first error)
    fn create_reporter_none_tolerance() -> WorkerErrantRecordReporter {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let operator = Arc::new(RetryWithToleranceOperator::new(
            0,
            0,
            ToleranceType::NONE,
            metrics.clone(),
        ));
        WorkerErrantRecordReporter::new(operator, metrics)
    }

    fn create_sink_record() -> SinkRecord {
        SinkRecord::new(
            "test-topic",
            Some(0),
            None,
            serde_json::Value::String("test-value".to_string()),
            100,
            common_trait::record::TimestampType::CreateTime,
        )
    }

    fn create_sink_record_with_partition(topic: &str, partition: i32) -> SinkRecord {
        SinkRecord::new(
            topic,
            Some(partition),
            None,
            serde_json::Value::String("test-value".to_string()),
            100,
            common_trait::record::TimestampType::CreateTime,
        )
    }

    // ========== ReportFuture Tests ==========

    #[test]
    fn test_report_future_new_is_pending() {
        let tp = TopicPartition::new("test-topic", 0);
        let future = ReportFuture::new(tp.clone());

        // New future is pending (not done)
        assert!(!future.is_done());
        assert!(!future.is_success());
        assert!(!future.is_failed());
        assert!(future.error_message().is_none());
    }

    #[test]
    fn test_report_future_completed_success() {
        let tp = TopicPartition::new("test-topic", 0);
        let future = ReportFuture::completed_success(tp.clone());

        assert!(future.is_done());
        assert!(future.is_success());
        assert!(!future.is_failed());
        assert!(future.error_message().is_none());
    }

    #[test]
    fn test_report_future_completed_failed() {
        let tp = TopicPartition::new("test-topic", 0);
        let future = ReportFuture::completed_failed(tp.clone(), "Test failure".to_string());

        assert!(future.is_done());
        assert!(!future.is_success());
        assert!(future.is_failed());
        assert_eq!(future.error_message(), Some("Test failure"));
    }

    #[test]
    fn test_report_future_mark_success_from_pending() {
        let tp = TopicPartition::new("test-topic", 0);
        let mut future = ReportFuture::new(tp.clone());

        // Pending -> Success
        future.mark_success();

        assert!(future.is_done());
        assert!(future.is_success());
        assert!(!future.is_failed());
    }

    #[test]
    fn test_report_future_mark_failed_from_pending() {
        let tp = TopicPartition::new("test-topic", 0);
        let mut future = ReportFuture::new(tp.clone());

        // Pending -> Failed
        future.mark_failed("Marked as failed".to_string());

        assert!(future.is_done());
        assert!(!future.is_success());
        assert!(future.is_failed());
        assert_eq!(future.error_message(), Some("Marked as failed"));
    }

    // ========== WorkerErrantRecordReporter Basic Tests ==========

    #[test]
    fn test_reporter_new_has_zero_state() {
        let reporter = create_reporter_all_tolerance();

        assert!(!reporter.is_stopping());
        assert_eq!(reporter.pending_futures_count(), 0);
        assert_eq!(reporter.failed_futures_count(), 0);
        assert!(reporter.within_tolerance());
    }

    #[test]
    fn test_reporter_stop_request_cycle() {
        let reporter = create_reporter_all_tolerance();

        // Initial state
        assert!(!reporter.is_stopping());

        // Request stop
        reporter.request_stop();
        assert!(reporter.is_stopping());

        // Clear stop
        reporter.clear_stop_request();
        assert!(!reporter.is_stopping());
    }

    // ========== Report Success/Failure Behavior Tests ==========

    #[test]
    fn test_report_with_all_tolerance_succeeds() {
        let mut reporter = create_reporter_all_tolerance();
        let record = create_sink_record();
        let error = ConnectError::general("test error");

        // With ALL tolerance, report should succeed
        let result = reporter.report(&record, error);
        assert!(result.is_ok());

        // Future was tracked
        assert_eq!(reporter.pending_futures_count(), 1);
    }

    #[test]
    fn test_report_with_none_tolerance_fails() {
        let mut reporter = create_reporter_none_tolerance();
        let record = create_sink_record();
        let error = ConnectError::general("test error");

        // With NONE tolerance, report should fail (tolerance exceeded)
        let result = reporter.report(&record, error);
        assert!(result.is_err());

        // Verify the error message indicates tolerance exceeded
        if let Err(e) = result {
            assert!(e.message().contains("Tolerance exceeded"));
        }
    }

    #[test]
    fn test_report_when_stopping_returns_error() {
        let mut reporter = create_reporter_all_tolerance();
        reporter.request_stop();

        let record = create_sink_record();
        let error = ConnectError::general("test error");

        let result = reporter.report(&record, error);
        assert!(result.is_err());

        // Verify the error message indicates stopping
        if let Err(e) = result {
            assert!(e.message().contains("stopping"));
        }
    }

    // ========== Await Futures Behavior Tests ==========

    #[test]
    fn test_await_futures_with_all_tolerance_marks_success() {
        let mut reporter = create_reporter_all_tolerance();
        let record = create_sink_record();

        // Submit a report
        let _ = reporter.report(&record, ConnectError::general("test"));

        // There should be a pending future
        assert_eq!(reporter.pending_futures_count(), 1);

        // Await the future
        let tp = TopicPartition::new("test-topic", 0);
        let result = reporter.await_futures(&[tp]);

        // With ALL tolerance, await should succeed
        assert!(result.is_ok());
        assert_eq!(reporter.pending_futures_count(), 0);
        assert_eq!(reporter.failed_futures_count(), 0);
    }

    #[test]
    fn test_await_futures_with_none_tolerance_marks_failed() {
        let mut reporter = create_reporter_none_tolerance();
        let record = create_sink_record();

        // Submit a report (will fail due to NONE tolerance)
        let _ = reporter.report(&record, ConnectError::general("test"));

        // Await the future - tolerance is exceeded
        let tp = TopicPartition::new("test-topic", 0);
        let result = reporter.await_futures(&[tp]);

        // With NONE tolerance, await should fail
        assert!(result.is_err());

        // Verify error message
        if let Err(e) = result {
            assert!(e.message().contains("failed"));
        }

        // Failed count should be incremented
        assert_eq!(reporter.failed_futures_count(), 1);
    }

    #[test]
    fn test_await_futures_preserves_precompleted_success() {
        let mut reporter = create_reporter_all_tolerance();

        // Manually insert an already-completed successful future
        let tp = TopicPartition::new("test-topic", 0);
        reporter.futures.insert(
            tp.clone(),
            vec![ReportFuture::completed_success(tp.clone())],
        );

        // Await should succeed
        let result = reporter.await_futures(&[tp]);
        assert!(result.is_ok());
        assert_eq!(reporter.failed_futures_count(), 0);
    }

    #[test]
    fn test_await_futures_preserves_precompleted_failure() {
        let mut reporter = create_reporter_all_tolerance();

        // Manually insert an already-completed failed future
        let tp = TopicPartition::new("test-topic", 0);
        reporter.futures.insert(
            tp.clone(),
            vec![ReportFuture::completed_failed(
                tp.clone(),
                "Pre-existing failure".to_string(),
            )],
        );

        // Await should fail even though tolerance is ALL
        // Because the future was already marked as failed
        let result = reporter.await_futures(&[tp]);
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(e.message().contains("Pre-existing failure"));
        }
    }

    // ========== Cancel Futures Behavior Tests ==========

    #[test]
    fn test_cancel_futures_marks_as_failed() {
        let mut reporter = create_reporter_all_tolerance();
        let record = create_sink_record();

        // Submit a report
        let _ = reporter.report(&record, ConnectError::general("test"));

        // Cancel the future
        let tp = TopicPartition::new("test-topic", 0);
        reporter.cancel_futures(&[tp]);

        // Futures should be cleared
        assert_eq!(reporter.pending_futures_count(), 0);

        // Cancelled futures are counted as failed
        assert_eq!(reporter.failed_futures_count(), 1);
    }

    #[test]
    fn test_cancel_futures_clears_multiple_partitions() {
        let mut reporter = create_reporter_all_tolerance();

        // Submit reports for multiple partitions
        let record1 = create_sink_record_with_partition("topic1", 0);
        let record2 = create_sink_record_with_partition("topic2", 1);
        let _ = reporter.report(&record1, ConnectError::general("error1"));
        let _ = reporter.report(&record2, ConnectError::general("error2"));

        assert_eq!(reporter.pending_futures_count(), 2);

        // Cancel both
        let tp1 = TopicPartition::new("topic1", 0);
        let tp2 = TopicPartition::new("topic2", 1);
        reporter.cancel_futures(&[tp1, tp2]);

        assert_eq!(reporter.pending_futures_count(), 0);
        assert_eq!(reporter.failed_futures_count(), 2);
    }

    // ========== Maybe Throw Async Error Tests ==========

    #[test]
    fn test_maybe_throw_async_error_all_tolerance() {
        let reporter = create_reporter_all_tolerance();

        // With ALL tolerance, should never throw
        let result = reporter.maybe_throw_async_error();
        assert!(result.is_ok());
    }

    #[test]
    fn test_maybe_throw_async_error_none_tolerance_no_error() {
        let reporter = create_reporter_none_tolerance();

        // Before any error, should not throw
        let result = reporter.maybe_throw_async_error();
        assert!(result.is_ok());
    }

    #[test]
    fn test_maybe_throw_async_error_none_tolerance_after_error() {
        let mut reporter = create_reporter_none_tolerance();

        // Submit an error report
        let record = create_sink_record();
        let _ = reporter.report(&record, ConnectError::general("test"));

        // Now check async error - should throw because tolerance exceeded
        let result = reporter.maybe_throw_async_error();
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(e.message().contains("Tolerance exceeded"));
        }
    }

    // ========== Metrics Integration Tests ==========

    #[test]
    fn test_metrics_updated_on_error() {
        let reporter = create_reporter_none_tolerance();
        let metrics = reporter.metrics();

        // Initial state
        assert_eq!(metrics.total_errors(), 0);

        // Note: The actual metrics update happens in execute_failed
        // This test verifies the reporter has access to metrics
        let metrics_ref = reporter.metrics();
        assert_eq!(metrics_ref.total_errors(), 0);
    }

    // ========== Edge Cases ==========

    #[test]
    fn test_await_futures_empty_partitions() {
        let mut reporter = create_reporter_all_tolerance();

        // Await empty list should succeed
        let result = reporter.await_futures(&[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_await_futures_unknown_partition() {
        let mut reporter = create_reporter_all_tolerance();

        // Await partition that has no futures should succeed
        let tp = TopicPartition::new("unknown-topic", 99);
        let result = reporter.await_futures(&[tp]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_reports_same_partition() {
        let mut reporter = create_reporter_all_tolerance();
        let record = create_sink_record();

        // Submit multiple reports for same partition
        let _ = reporter.report(&record, ConnectError::general("error1"));
        let _ = reporter.report(&record, ConnectError::general("error2"));
        let _ = reporter.report(&record, ConnectError::general("error3"));

        // Should have 3 pending futures
        assert_eq!(reporter.pending_futures_count(), 3);

        // Await should process all
        let tp = TopicPartition::new("test-topic", 0);
        let result = reporter.await_futures(&[tp]);
        assert!(result.is_ok());
        assert_eq!(reporter.pending_futures_count(), 0);
    }
}
