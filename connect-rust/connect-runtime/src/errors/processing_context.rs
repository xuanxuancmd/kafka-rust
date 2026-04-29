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

//! ProcessingContext holds state about an operation being processed.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.errors.ProcessingContext` in Java.

use std::error::Error;
use std::fmt;
use std::time::Instant;

use super::Stage;

/// ProcessingContext holds state about an operation being processed.
///
/// It tracks the current Stage, the executing class, the number of attempts,
/// and any error encountered. This context is mutated by RetryWithToleranceOperator
/// to reflect the outcome of operations.
pub struct ProcessingContext {
    /// The current stage in the processing pipeline
    stage: Option<Stage>,
    /// The class that is executing the operation (used for error reporting)
    executing_class: Option<String>,
    /// The number of retry attempts made
    attempts: u32,
    /// Any error that occurred during processing
    error: Option<Box<dyn Error + Send + Sync>>,
    /// The timestamp when processing started
    start_time: Instant,
    /// Whether this context represents a failed operation
    failed: bool,
}

impl ProcessingContext {
    /// Creates a new ProcessingContext with default values.
    pub fn new() -> Self {
        ProcessingContext {
            stage: None,
            executing_class: None,
            attempts: 0,
            error: None,
            start_time: Instant::now(),
            failed: false,
        }
    }

    /// Sets the current stage for this context.
    pub fn set_stage(&mut self, stage: Stage) {
        self.stage = Some(stage);
    }

    /// Returns the current stage, if set.
    pub fn stage(&self) -> Option<Stage> {
        self.stage
    }

    /// Sets the executing class name for this context.
    pub fn set_executing_class(&mut self, class_name: String) {
        self.executing_class = Some(class_name);
    }

    /// Returns the executing class name, if set.
    pub fn executing_class(&self) -> Option<&str> {
        self.executing_class.as_deref()
    }

    /// Increments the attempt counter.
    pub fn increment_attempts(&mut self) {
        self.attempts += 1;
    }

    /// Returns the number of attempts made.
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Sets the error for this context.
    pub fn set_error(&mut self, error: Box<dyn Error + Send + Sync>) {
        self.error = Some(error);
        self.failed = true;
    }

    /// Returns the error, if any.
    pub fn error(&self) -> Option<&(dyn Error + Send + Sync)> {
        self.error.as_deref()
    }

    /// Returns true if this context represents a failed operation.
    pub fn is_failed(&self) -> bool {
        self.failed
    }

    /// Marks this context as failed.
    pub fn mark_failed(&mut self) {
        self.failed = true;
    }

    /// Returns the elapsed time since processing started.
    pub fn elapsed_millis(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }

    /// Resets this context for a new operation.
    pub fn reset(&mut self) {
        self.stage = None;
        self.executing_class = None;
        self.attempts = 0;
        self.error = None;
        self.start_time = Instant::now();
        self.failed = false;
    }

    /// Returns a summary string for logging purposes.
    pub fn summary(&self) -> String {
        format!(
            "ProcessingContext: stage={}, class={}, attempts={}, failed={}, error={}",
            self.stage
                .map(|s| s.to_string())
                .unwrap_or_else(|| "none".to_string()),
            self.executing_class.as_deref().unwrap_or("none"),
            self.attempts,
            self.failed,
            self.error
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "none".to_string())
        )
    }
}

impl Default for ProcessingContext {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ProcessingContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessingContext")
            .field("stage", &self.stage)
            .field("executing_class", &self.executing_class)
            .field("attempts", &self.attempts)
            .field("failed", &self.failed)
            .field("elapsed_millis", &self.elapsed_millis())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_processing_context_new() {
        let ctx = ProcessingContext::new();
        assert!(ctx.stage().is_none());
        assert!(ctx.executing_class().is_none());
        assert_eq!(ctx.attempts(), 0);
        assert!(ctx.error().is_none());
        assert!(!ctx.is_failed());
    }

    #[test]
    fn test_processing_context_set_stage() {
        let mut ctx = ProcessingContext::new();
        ctx.set_stage(Stage::KEY_CONVERTER);
        assert_eq!(ctx.stage(), Some(Stage::KEY_CONVERTER));
    }

    #[test]
    fn test_processing_context_set_executing_class() {
        let mut ctx = ProcessingContext::new();
        ctx.set_executing_class("MyConnector".to_string());
        assert_eq!(ctx.executing_class(), Some("MyConnector"));
    }

    #[test]
    fn test_processing_context_attempts() {
        let mut ctx = ProcessingContext::new();
        assert_eq!(ctx.attempts(), 0);
        ctx.increment_attempts();
        assert_eq!(ctx.attempts(), 1);
        ctx.increment_attempts();
        assert_eq!(ctx.attempts(), 2);
    }

    #[test]
    fn test_processing_context_error() {
        let mut ctx = ProcessingContext::new();
        let error = io::Error::new(io::ErrorKind::Other, "test error");
        ctx.set_error(Box::new(error));
        assert!(ctx.error().is_some());
        assert!(ctx.is_failed());
    }

    #[test]
    fn test_processing_context_mark_failed() {
        let mut ctx = ProcessingContext::new();
        assert!(!ctx.is_failed());
        ctx.mark_failed();
        assert!(ctx.is_failed());
    }

    #[test]
    fn test_processing_context_reset() {
        let mut ctx = ProcessingContext::new();
        ctx.set_stage(Stage::TRANSFORMATION);
        ctx.set_executing_class("Test".to_string());
        ctx.increment_attempts();
        ctx.mark_failed();

        ctx.reset();
        assert!(ctx.stage().is_none());
        assert!(ctx.executing_class().is_none());
        assert_eq!(ctx.attempts(), 0);
        assert!(!ctx.is_failed());
    }

    #[test]
    fn test_processing_context_elapsed_millis() {
        let ctx = ProcessingContext::new();
        // Should be very small immediately after creation
        assert!(ctx.elapsed_millis() < 100);
    }

    #[test]
    fn test_processing_context_summary() {
        let mut ctx = ProcessingContext::new();
        ctx.set_stage(Stage::VALUE_CONVERTER);
        ctx.set_executing_class("MyClass".to_string());
        ctx.increment_attempts();

        let summary = ctx.summary();
        assert!(summary.contains("VALUE_CONVERTER"));
        assert!(summary.contains("MyClass"));
        assert!(summary.contains("attempts=1"));
    }

    #[test]
    fn test_processing_context_default() {
        let ctx = ProcessingContext::default();
        assert!(ctx.stage().is_none());
        assert_eq!(ctx.attempts(), 0);
    }
}
