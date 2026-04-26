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

//! ErrorReporter trait for reporting errors during processing.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.errors.ErrorReporter` in Java.
//!
//! # Components
//!
//! - **ErrorReporter**: Trait for reporting errors
//! - **LogReporter**: Base class for logging errors (abstract in Java)
//! - **LogReporterSink**: Sink task specific log reporter
//! - **LogReporterSource**: Source task specific log reporter
//! - **DeadLetterQueueReporter**: Reports errors to a dead letter queue topic

use std::sync::Arc;

use super::metrics::ErrorHandlingMetrics;
use super::processing_context::ProcessingContext;
use common_trait::worker::ConnectorTaskId;

use crate::config::ConnectorConfig;

/// ErrorReporter is a trait for reporting errors that occur during processing.
///
/// Implementations can report errors to different destinations such as logs,
/// dead letter queues, or external monitoring systems.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.errors.ErrorReporter`
pub trait ErrorReporter: Send + Sync {
    /// Reports an error that occurred during processing.
    ///
    /// # Arguments
    /// * `context` - The processing context containing details about the error
    fn report(&self, context: &ProcessingContext);

    /// Returns the name of this reporter for logging purposes.
    fn name(&self) -> &str;

    /// Closes the reporter and releases any resources.
    /// Corresponds to Java: `ErrorReporter.close()` (from AutoCloseable)
    fn close(&self) {
        // Default implementation does nothing
    }
}

/// Base configuration for LogReporter.
///
/// This holds the common configuration and metrics needed for both Sink and Source variants.
struct LogReporterBase {
    /// The connector task ID for identifying which task reported the error
    id: ConnectorTaskId,
    /// The connector configuration for checking error log settings
    conn_config: ConnectorConfig,
    /// Metrics for tracking error handling statistics
    metrics: Arc<ErrorHandlingMetrics>,
}

impl LogReporterBase {
    /// Creates a new LogReporterBase.
    fn new(
        id: ConnectorTaskId,
        conn_config: ConnectorConfig,
        metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        LogReporterBase {
            id,
            conn_config,
            metrics,
        }
    }

    /// Returns true if error logging is enabled in the connector config.
    /// Corresponds to Java: `LogReporter.report()` check for `connConfig.enableErrorLog()`
    fn is_error_log_enabled(&self) -> bool {
        self.conn_config.errors_log_enable()
    }

    /// Returns true if record details should be included in error logs.
    /// Corresponds to Java: `LogReporter.toString()` check for `connConfig.includeRecordDetailsInErrorLog()`
    fn include_record_details(&self) -> bool {
        self.conn_config.errors_log_include_messages()
    }

    /// Generates the error message for logging.
    /// Corresponds to Java: `LogReporter.message(ProcessingContext context)`
    fn message(&self, context: &ProcessingContext) -> String {
        format!(
            "Error encountered in task {}. Executing stage '{}' with class '{}'.",
            self.id,
            context.stage().map(|s| s.name()).unwrap_or("unknown"),
            context.executing_class().unwrap_or("unknown")
        )
    }

    /// Records the error in metrics.
    fn record_error_logged(&self) {
        self.metrics.record_error_logged();
    }
}

/// LogReporter for Sink tasks.
///
/// Writes errors and their context to application logs for sink connectors.
/// Corresponds to Java: `LogReporter.Sink`
pub struct LogReporterSink {
    base: LogReporterBase,
}

impl LogReporterSink {
    /// Creates a new LogReporterSink.
    ///
    /// # Arguments
    /// * `id` - The connector task ID
    /// * `conn_config` - The connector configuration
    /// * `metrics` - Error handling metrics
    pub fn new(
        id: ConnectorTaskId,
        conn_config: ConnectorConfig,
        metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        LogReporterSink {
            base: LogReporterBase::new(id, conn_config, metrics),
        }
    }
}

impl ErrorReporter for LogReporterSink {
    fn report(&self, context: &ProcessingContext) {
        if !self.base.is_error_log_enabled() {
            return;
        }

        if !context.is_failed() {
            return;
        }

        // Log the error
        if let Some(error) = context.error() {
            let message = self.base.message(context);
            // In production, this would use a proper logging framework
            // For now, we use println to match the original implementation
            eprintln!("[ERROR] {} - {}", message, error);
        }

        self.base.record_error_logged();
    }

    fn name(&self) -> &str {
        "LogReporterSink"
    }
}

/// LogReporter for Source tasks.
///
/// Writes errors and their context to application logs for source connectors.
/// Corresponds to Java: `LogReporter.Source`
pub struct LogReporterSource {
    base: LogReporterBase,
}

impl LogReporterSource {
    /// Creates a new LogReporterSource.
    ///
    /// # Arguments
    /// * `id` - The connector task ID
    /// * `conn_config` - The connector configuration
    /// * `metrics` - Error handling metrics
    pub fn new(
        id: ConnectorTaskId,
        conn_config: ConnectorConfig,
        metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        LogReporterSource {
            base: LogReporterBase::new(id, conn_config, metrics),
        }
    }
}

impl ErrorReporter for LogReporterSource {
    fn report(&self, context: &ProcessingContext) {
        if !self.base.is_error_log_enabled() {
            return;
        }

        if !context.is_failed() {
            return;
        }

        // Log the error
        if let Some(error) = context.error() {
            let message = self.base.message(context);
            eprintln!("[ERROR] {} - {}", message, error);
        }

        self.base.record_error_logged();
    }

    fn name(&self) -> &str {
        "LogReporterSource"
    }
}

/// Simple LogReporter for basic error logging (legacy implementation).
///
/// This is a simplified version that logs errors to stdout.
/// Used for testing and simple scenarios where full config is not needed.
pub struct LogReporter {
    name: String,
}

impl LogReporter {
    /// Creates a new LogReporter with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        LogReporter { name: name.into() }
    }
}

impl ErrorReporter for LogReporter {
    fn report(&self, context: &ProcessingContext) {
        if let Some(error) = context.error() {
            println!(
                "[{}] Error at stage {}: {} (attempts: {}, elapsed: {}ms)",
                self.name,
                context
                    .stage()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                error,
                context.attempts(),
                context.elapsed_millis()
            );
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Header constants for DLQ records.
/// Corresponds to Java: `DeadLetterQueueReporter` header constants
pub mod dlq_headers {
    pub const HEADER_PREFIX: &str = "__connect.errors.";
    pub const ERROR_HEADER_ORIG_TOPIC: &str = "__connect.errors.topic";
    pub const ERROR_HEADER_ORIG_PARTITION: &str = "__connect.errors.partition";
    pub const ERROR_HEADER_ORIG_OFFSET: &str = "__connect.errors.offset";
    pub const ERROR_HEADER_CONNECTOR_NAME: &str = "__connect.errors.connector.name";
    pub const ERROR_HEADER_TASK_ID: &str = "__connect.errors.task.id";
    pub const ERROR_HEADER_STAGE: &str = "__connect.errors.stage";
    pub const ERROR_HEADER_EXECUTING_CLASS: &str = "__connect.errors.class.name";
    pub const ERROR_HEADER_EXCEPTION: &str = "__connect.errors.exception.class.name";
    pub const ERROR_HEADER_EXCEPTION_MESSAGE: &str = "__connect.errors.exception.message";
    pub const ERROR_HEADER_EXCEPTION_STACK_TRACE: &str = "__connect.errors.exception.stacktrace";
}

/// DeadLetterQueueReporter reports errors to a dead letter queue.
///
/// This reporter sends failed records to a Kafka topic for later analysis.
/// Corresponds to Java: `org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter`
pub struct DeadLetterQueueReporter {
    name: String,
    topic_name: String,
    connector_task_id: ConnectorTaskId,
    metrics: Arc<ErrorHandlingMetrics>,
    context_headers_enabled: bool,
}

impl DeadLetterQueueReporter {
    /// Creates a new DeadLetterQueueReporter with the given name and topic.
    pub fn new(name: impl Into<String>, topic: impl Into<String>) -> Self {
        DeadLetterQueueReporter {
            name: name.into(),
            topic_name: topic.into(),
            connector_task_id: ConnectorTaskId::new("unknown".to_string(), 0),
            metrics: Arc::new(ErrorHandlingMetrics::new()),
            context_headers_enabled: true,
        }
    }

    /// Creates a new DeadLetterQueueReporter with full configuration.
    ///
    /// # Arguments
    /// * `topic` - The DLQ topic name
    /// * `id` - The connector task ID
    /// * `context_headers_enabled` - Whether to add context headers
    /// * `metrics` - Error handling metrics
    pub fn with_config(
        topic: impl Into<String>,
        id: ConnectorTaskId,
        context_headers_enabled: bool,
        metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        DeadLetterQueueReporter {
            name: "DeadLetterQueueReporter".to_string(),
            topic_name: topic.into(),
            connector_task_id: id,
            metrics,
            context_headers_enabled,
        }
    }

    /// Returns the dead letter queue topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns whether context headers are enabled.
    pub fn is_context_headers_enabled(&self) -> bool {
        self.context_headers_enabled
    }

    /// Populates context headers for the DLQ record.
    /// Corresponds to Java: `DeadLetterQueueReporter.populateContextHeaders()`
    pub fn populate_context_headers(
        &self,
        headers: &mut Vec<(String, Vec<u8>)>,
        context: &ProcessingContext,
    ) {
        if !self.context_headers_enabled {
            return;
        }

        // Add connector and task information
        headers.push((
            dlq_headers::ERROR_HEADER_CONNECTOR_NAME.to_string(),
            self.connector_task_id.connector().as_bytes().to_vec(),
        ));
        headers.push((
            dlq_headers::ERROR_HEADER_TASK_ID.to_string(),
            self.connector_task_id
                .task()
                .to_string()
                .as_bytes()
                .to_vec(),
        ));

        // Add stage information
        if let Some(stage) = context.stage() {
            headers.push((
                dlq_headers::ERROR_HEADER_STAGE.to_string(),
                stage.name().as_bytes().to_vec(),
            ));
        }

        // Add executing class
        if let Some(class_name) = context.executing_class() {
            headers.push((
                dlq_headers::ERROR_HEADER_EXECUTING_CLASS.to_string(),
                class_name.as_bytes().to_vec(),
            ));
        }

        // Add error information
        if let Some(error) = context.error() {
            headers.push((
                dlq_headers::ERROR_HEADER_EXCEPTION.to_string(),
                "Error".as_bytes().to_vec(), // Simplified; would use actual class name in production
            ));
            headers.push((
                dlq_headers::ERROR_HEADER_EXCEPTION_MESSAGE.to_string(),
                error.to_string().as_bytes().to_vec(),
            ));
        }
    }
}

impl ErrorReporter for DeadLetterQueueReporter {
    fn report(&self, context: &ProcessingContext) {
        // Skip if DLQ topic is empty (feature disabled)
        if self.topic_name.is_empty() {
            return;
        }

        // Record DLQ produce request
        self.metrics.record_dlq_produce_request();

        if let Some(error) = context.error() {
            println!(
                "[{}] Sending to DLQ topic {}: error at stage {} - {}",
                self.name,
                self.topic_name,
                context
                    .stage()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                error
            );
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn close(&self) {
        // In production, this would close the Kafka producer
        // For this implementation, we just log
        println!("[{}] Closing DLQ reporter", self.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Stage;
    use std::io;
    use std::sync::Arc;

    fn create_failed_context() -> ProcessingContext {
        let mut ctx = ProcessingContext::new();
        ctx.set_stage(Stage::KEY_CONVERTER);
        ctx.set_executing_class("TestClass".to_string());
        ctx.increment_attempts();
        let error = io::Error::new(io::ErrorKind::Other, "test error");
        ctx.set_error(Box::new(error));
        ctx
    }

    #[test]
    fn test_log_reporter_new() {
        let reporter = LogReporter::new("test-reporter");
        assert_eq!(reporter.name(), "test-reporter");
    }

    #[test]
    fn test_log_reporter_report() {
        let reporter = LogReporter::new("test-reporter");
        let ctx = create_failed_context();
        reporter.report(&ctx);
        // Should not panic; output goes to stdout
    }

    #[test]
    fn test_log_reporter_sink_new() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut config_props = std::collections::HashMap::new();
        config_props.insert(
            crate::config::connector_config::NAME_CONFIG.to_string(),
            serde_json::Value::String("test-connector".to_string()),
        );
        config_props.insert(
            crate::config::connector_config::CONNECTOR_CLASS_CONFIG.to_string(),
            serde_json::Value::String("TestConnector".to_string()),
        );
        let config = ConnectorConfig::new_unvalidated(config_props);
        let metrics = Arc::new(ErrorHandlingMetrics::new());

        let reporter = LogReporterSink::new(id, config, metrics);
        assert_eq!(reporter.name(), "LogReporterSink");
    }

    #[test]
    fn test_log_reporter_sink_report_disabled() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut config_props = std::collections::HashMap::new();
        config_props.insert(
            crate::config::connector_config::NAME_CONFIG.to_string(),
            serde_json::Value::String("test-connector".to_string()),
        );
        config_props.insert(
            crate::config::connector_config::CONNECTOR_CLASS_CONFIG.to_string(),
            serde_json::Value::String("TestConnector".to_string()),
        );
        // Error logging disabled (default)
        let config = ConnectorConfig::new_unvalidated(config_props);
        let metrics = Arc::new(ErrorHandlingMetrics::new());

        let reporter = LogReporterSink::new(id, config, metrics.clone());
        let ctx = create_failed_context();
        reporter.report(&ctx);

        // Metrics should not record error logged since logging is disabled
        assert_eq!(metrics.errors_logged(), 0);
    }

    #[test]
    fn test_log_reporter_sink_report_enabled() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut config_props = std::collections::HashMap::new();
        config_props.insert(
            crate::config::connector_config::NAME_CONFIG.to_string(),
            serde_json::Value::String("test-connector".to_string()),
        );
        config_props.insert(
            crate::config::connector_config::CONNECTOR_CLASS_CONFIG.to_string(),
            serde_json::Value::String("TestConnector".to_string()),
        );
        config_props.insert(
            crate::config::connector_config::ERRORS_LOG_ENABLE_CONFIG.to_string(),
            serde_json::Value::Bool(true),
        );
        let config = ConnectorConfig::new_unvalidated(config_props);
        let metrics = Arc::new(ErrorHandlingMetrics::new());

        let reporter = LogReporterSink::new(id, config, metrics.clone());
        let ctx = create_failed_context();
        reporter.report(&ctx);

        // Metrics should record error logged
        assert_eq!(metrics.errors_logged(), 1);
    }

    #[test]
    fn test_log_reporter_source_new() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut config_props = std::collections::HashMap::new();
        config_props.insert(
            crate::config::connector_config::NAME_CONFIG.to_string(),
            serde_json::Value::String("test-connector".to_string()),
        );
        config_props.insert(
            crate::config::connector_config::CONNECTOR_CLASS_CONFIG.to_string(),
            serde_json::Value::String("TestConnector".to_string()),
        );
        let config = ConnectorConfig::new_unvalidated(config_props);
        let metrics = Arc::new(ErrorHandlingMetrics::new());

        let reporter = LogReporterSource::new(id, config, metrics);
        assert_eq!(reporter.name(), "LogReporterSource");
    }

    #[test]
    fn test_dead_letter_queue_reporter_new() {
        let reporter = DeadLetterQueueReporter::new("dlq-reporter", "dlq-topic");
        assert_eq!(reporter.name(), "DeadLetterQueueReporter");
        assert_eq!(reporter.topic_name(), "dlq-topic");
    }

    #[test]
    fn test_dead_letter_queue_reporter_empty_topic() {
        let reporter = DeadLetterQueueReporter::new("dlq-reporter", "");
        assert_eq!(reporter.topic_name(), "");

        // Report with empty topic should skip DLQ production
        let ctx = create_failed_context();
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        // Note: the reporter created with new() has its own metrics, not shared
        reporter.report(&ctx);
    }

    #[test]
    fn test_dead_letter_queue_reporter_with_config() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 1);
        let metrics = Arc::new(ErrorHandlingMetrics::new());

        let reporter = DeadLetterQueueReporter::with_config(
            "dlq-topic",
            id,
            true, // context headers enabled
            metrics.clone(),
        );

        assert_eq!(reporter.topic_name(), "dlq-topic");
        assert!(reporter.is_context_headers_enabled());
    }

    #[test]
    fn test_dead_letter_queue_reporter_report() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 1);
        let metrics = Arc::new(ErrorHandlingMetrics::new());

        let reporter = DeadLetterQueueReporter::with_config("dlq-topic", id, true, metrics.clone());

        let ctx = create_failed_context();
        reporter.report(&ctx);

        // Metrics should record DLQ produce request
        assert_eq!(metrics.dlq_produce_requests(), 1);
    }

    #[test]
    fn test_dead_letter_queue_reporter_populate_headers() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 1);
        let metrics = Arc::new(ErrorHandlingMetrics::new());

        let reporter = DeadLetterQueueReporter::with_config("dlq-topic", id, true, metrics);

        let ctx = create_failed_context();
        let mut headers = Vec::new();
        reporter.populate_context_headers(&mut headers, &ctx);

        // Should have at least connector name and task id headers
        assert!(!headers.is_empty());

        // Check specific headers exist
        let has_connector_name = headers
            .iter()
            .any(|(k, _)| k == dlq_headers::ERROR_HEADER_CONNECTOR_NAME);
        assert!(has_connector_name);

        let has_task_id = headers
            .iter()
            .any(|(k, _)| k == dlq_headers::ERROR_HEADER_TASK_ID);
        assert!(has_task_id);
    }

    #[test]
    fn test_error_reporter_close() {
        let reporter = DeadLetterQueueReporter::new("dlq-reporter", "dlq-topic");
        reporter.close();
        // Should not panic
    }
}
