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

use std::error::Error;

use super::ProcessingContext;

/// ErrorReporter is a trait for reporting errors that occur during processing.
///
/// Implementations can report errors to different destinations such as logs,
/// dead letter queues, or external monitoring systems.
pub trait ErrorReporter: Send + Sync {
    /// Reports an error that occurred during processing.
    ///
    /// # Arguments
    /// * `context` - The processing context containing details about the error
    fn report(&self, context: &ProcessingContext);

    /// Returns the name of this reporter for logging purposes.
    fn name(&self) -> &str;
}

/// LogReporter is a simple error reporter that logs errors.
///
/// This corresponds to `org.apache.kafka.connect.runtime.errors.LogReporter` in Java.
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

/// DeadLetterQueueReporter reports errors to a dead letter queue.
///
/// This is a placeholder for actual dead letter queue implementation.
/// In production, this would send failed records to a Kafka topic.
pub struct DeadLetterQueueReporter {
    name: String,
    topic_name: Option<String>,
}

impl DeadLetterQueueReporter {
    /// Creates a new DeadLetterQueueReporter with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        DeadLetterQueueReporter {
            name: name.into(),
            topic_name: None,
        }
    }

    /// Creates a new DeadLetterQueueReporter with a topic name.
    pub fn with_topic(name: impl Into<String>, topic: impl Into<String>) -> Self {
        DeadLetterQueueReporter {
            name: name.into(),
            topic_name: Some(topic.into()),
        }
    }

    /// Returns the dead letter queue topic name.
    pub fn topic_name(&self) -> Option<&str> {
        self.topic_name.as_deref()
    }
}

impl ErrorReporter for DeadLetterQueueReporter {
    fn report(&self, context: &ProcessingContext) {
        if let Some(error) = context.error() {
            println!(
                "[{}] Sending to DLQ topic {}: error at stage {} - {}",
                self.name,
                self.topic_name.as_deref().unwrap_or("undefined"),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    fn create_failed_context() -> ProcessingContext {
        let mut ctx = ProcessingContext::new();
        ctx.set_stage(super::super::Stage::KEY_CONVERTER);
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
    fn test_dead_letter_queue_reporter_new() {
        let reporter = DeadLetterQueueReporter::new("dlq-reporter");
        assert_eq!(reporter.name(), "dlq-reporter");
        assert!(reporter.topic_name().is_none());
    }

    #[test]
    fn test_dead_letter_queue_reporter_with_topic() {
        let reporter = DeadLetterQueueReporter::with_topic("dlq-reporter", "dlq-topic");
        assert_eq!(reporter.name(), "dlq-reporter");
        assert_eq!(reporter.topic_name(), Some("dlq-topic"));
    }

    #[test]
    fn test_dead_letter_queue_reporter_report() {
        let reporter = DeadLetterQueueReporter::with_topic("dlq-reporter", "dlq-topic");
        let ctx = create_failed_context();
        reporter.report(&ctx);
        // Should not panic; output goes to stdout
    }
}
