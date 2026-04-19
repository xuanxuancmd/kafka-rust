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

//! Errors module for Kafka Connect runtime error handling.
//!
//! This module provides error handling components that correspond to
//! `org.apache.kafka.connect.runtime.errors` in Java.
//!
//! # Components
//!
//! - **RetryWithToleranceOperator**: Executes operations with retry and tolerance logic
//! - **ToleranceType**: Defines behavior for tolerating errors (NONE or ALL)
//! - **Stage**: Represents different stages in the processing pipeline
//! - **ProcessingContext**: Holds state about an operation being processed
//! - **ErrorReporter**: Trait for reporting errors
//! - **ErrorHandlingMetrics**: Tracks error handling statistics

mod error_reporter;
mod metrics;
mod processing_context;
mod retry_with_tolerance_operator;
mod stage;
mod tolerance_type;

pub use error_reporter::*;
pub use metrics::*;
pub use processing_context::*;
pub use retry_with_tolerance_operator::*;
pub use stage::*;
pub use tolerance_type::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_full_integration() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let mut operator =
            RetryWithToleranceOperator::new(1000, 500, ToleranceType::ALL, metrics.clone());
        operator.add_reporter(Arc::new(LogReporter::new("test-reporter")));

        let result: Option<&str> =
            operator.execute_with_retry(|| Ok("success"), Stage::KEY_CONVERTER, "TestConnector");

        assert_eq!(result, Some("success"));
        assert_eq!(metrics.total_errors(), 0);
    }

    #[test]
    fn test_tolerance_workflow() {
        let metrics = Arc::new(ErrorHandlingMetrics::new());
        let mut operator =
            RetryWithToleranceOperator::new(0, 0, ToleranceType::ALL, metrics.clone());

        // Execute multiple operations, some failing
        operator.execute_with_retry::<String, _>(
            || Ok("success1".to_string()),
            Stage::TRANSFORMATION,
            "Test",
        );

        operator.execute_with_retry::<String, _>(
            || {
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "error",
                )))
            },
            Stage::KEY_CONVERTER,
            "Test",
        );

        operator.execute_with_retry::<String, _>(
            || Ok("success2".to_string()),
            Stage::VALUE_CONVERTER,
            "Test",
        );

        // Should have 1 tolerated error
        assert_eq!(metrics.tolerated_errors(), 1);
        assert_eq!(metrics.total_errors(), 1);
    }

    #[test]
    fn test_stage_enum_all_values() {
        let stages = [
            Stage::TRANSFORMATION,
            Stage::KEY_CONVERTER,
            Stage::VALUE_CONVERTER,
            Stage::HEADER_CONVERTER,
            Stage::KAFKA_PRODUCE,
            Stage::KAFKA_CONSUME,
        ];

        for stage in stages {
            assert!(!stage.name().is_empty());
        }
    }

    #[test]
    fn test_tolerance_type_parsing() {
        assert_eq!(ToleranceType::parse("none"), Some(ToleranceType::NONE));
        assert_eq!(ToleranceType::parse("all"), Some(ToleranceType::ALL));
        assert_eq!(ToleranceType::parse("NONE"), Some(ToleranceType::NONE));
        assert_eq!(ToleranceType::parse("ALL"), Some(ToleranceType::ALL));
        assert_eq!(ToleranceType::parse("invalid"), None);
    }

    #[test]
    fn test_processing_context_full_workflow() {
        let mut ctx = ProcessingContext::new();
        ctx.set_stage(Stage::VALUE_CONVERTER);
        ctx.set_executing_class("MyConnector".to_string());
        ctx.increment_attempts();
        ctx.increment_attempts();

        assert_eq!(ctx.stage(), Some(Stage::VALUE_CONVERTER));
        assert_eq!(ctx.executing_class(), Some("MyConnector"));
        assert_eq!(ctx.attempts(), 2);
        assert!(!ctx.is_failed());

        let error = std::io::Error::new(std::io::ErrorKind::Other, "test");
        ctx.set_error(Box::new(error));

        assert!(ctx.is_failed());
        assert!(ctx.error().is_some());

        ctx.reset();
        assert!(!ctx.is_failed());
        assert_eq!(ctx.attempts(), 0);
    }

    #[test]
    fn test_metrics_all_stages() {
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
}
