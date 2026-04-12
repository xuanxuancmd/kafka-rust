//! Log reporter module
//!
//! Provides a LogReporter for writing errors and their context to application logs.

use crate::errors::error_reporter::ErrorReporter;
use crate::errors::error_handling_metrics::ErrorHandlingMetrics;
use crate::errors::processing_context::ProcessingContext;
use crate::errors::stage::Stage;
use async_trait::async_trait;
use std::any::TypeId;
use std::sync::Arc;
use tracing::error;

/// Writes errors and their context to application logs.
pub struct LogReporter<T> {
    /// Connector task ID
    connector_task_id: String,

    /// Whether error logging is enabled
    enable_error_log: bool,

    /// Whether to include record details in error log
    include_record_details_in_error_log: bool,

    /// Error handling metrics
    error_handling_metrics: Arc<ErrorHandlingMetrics>,
}

impl<T> LogReporter<T> {
    /// Create a new log reporter
    ///
    /// # Arguments
    ///
    /// * `connector_task_id` - connector task ID
    /// * `enable_error_log` - whether error logging is enabled
    /// * `include_record_details_in_error_log` - whether to include record details in error log
    /// * `error_handling_metrics` - error handling metrics
    pub fn new(
        connector_task_id: String,
        enable_error_log: bool,
        include_record_details_in_error_log: bool,
        error_handling_metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        Self {
            connector_task_id,
            enable_error_log,
            include_record_details_in_error_log,
            error_handling_metrics,
        }
    }

    /// Log error context
    ///
    /// # Arguments
    ///
    /// * `context` - the processing context
    fn log_error(&self, context: &ProcessingContext<T>) {
        if !self.enable_error_log {
            return;
        }

        if !context.failed() {
            return;
        }

        let message = self.message(context);
        if let Some(err) = context.error() {
            error!("{}: {}", message, err);
        } else {
            error!("{}", message);
        }
        self.error_handling_metrics.record_error_logged();
    }

    /// Create error message
    ///
    /// # Arguments
    ///
    /// * `context` - the processing context
    fn message(&self, context: &ProcessingContext<T>) -> String {
        format!(
            "Error encountered in task {}. {}",
            self.connector_task_id,
            self.to_string(context, self.include_record_details_in_error_log)
        )
    }

    /// Convert processing context to string
    ///
    /// # Arguments
    ///
    /// * `context` - the processing context
    /// * `include_message` - whether to include message
    fn to_string(&self, context: &ProcessingContext<T>, include_message: bool) -> String {
        let mut builder = String::new();
        builder.push_str("Executing stage '");
        builder.push_str(
            context
                .stage()
                .map(|s| s.name())
                .unwrap_or("unknown")
        );
        builder.push_str("' with class '");
        if let Some(type_id) = context.executing_class() {
            builder.push_str(&format!("{:?}", type_id));
        } else {
            builder.push_str("null");
        }
        builder.push('\'');

        if include_message {
            self.append_message(&mut builder, context);
        }

        builder.push('.');
        builder
    }

    /// Append message to builder (to be implemented by specific log reporters)
    ///
    /// # Arguments
    ///
    /// * `builder` - string builder
    /// * `original` - original record
    fn append_message(&self, builder: &mut String, original: &T);
}

/// Sink log reporter for ConsumerRecord
pub struct SinkLogReporter {
    inner: LogReporter<Vec<u8>>,
}

impl SinkLogReporter {
    /// Create a new sink log reporter
    ///
    /// # Arguments
    ///
    /// * `connector_task_id` - connector task ID
    /// * `enable_error_log` - whether error logging is enabled
    /// * `include_record_details_in_error_log` - whether to include record details in error log
    /// * `error_handling_metrics` - error handling metrics
    pub fn new(
        connector_task_id: String,
        enable_error_log: bool,
        include_record_details_in_error_log: bool,
        error_handling_metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        Self {
            inner: LogReporter::new(
                connector_task_id,
                enable_error_log,
                include_record_details_in_error_log,
                error_handling_metrics,
            ),
        }
    }
}

#[async_trait]
impl ErrorReporter<Vec<u8>> for SinkLogReporter {
    async fn report(&self, context: &ProcessingContext<Vec<u8>>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.log_error(context);
        Ok(())
    }
}

/// Source log reporter for SourceRecord
pub struct SourceLogReporter {
    inner: LogReporter<String>,
}

impl SourceLogReporter {
    /// Create a new source log reporter
    ///
    /// # Arguments
    ///
    /// * `connector_task_id` - connector task ID
    /// * `enable_error_log` - whether error logging is enabled
    /// * `include_record_details_in_error_log` - whether to include record details in error log
    /// * `error_handling_metrics` - error handling metrics
    pub fn new(
        connector_task_id: String,
        enable_error_log: bool,
        include_record_details_in_error_log: bool,
        error_handling_metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        Self {
            inner: LogReporter::new(
                connector_task_id,
                enable_error_log,
                include_record_details_in_error_log,
                error_handling_metrics,
            ),
        }
    }
}

#[async_trait]
impl ErrorReporter<String> for SourceLogReporter {
    async fn report(&self, context: &ProcessingContext<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.log_error(context);
        Ok(())
    }
}
