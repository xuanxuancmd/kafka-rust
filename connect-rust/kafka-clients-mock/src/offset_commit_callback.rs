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

//! Offset commit callback for Kafka consumer.
//!
//! This corresponds to `org.apache.kafka.clients.consumer.OffsetCommitCallback` in Java.

use common_trait::TopicPartition;
use std::collections::HashMap;

use crate::OffsetAndMetadata;

/// Error for commit operations.
///
/// This corresponds to `org.apache.kafka.clients.consumer.CommitFailedException` in Java.
#[derive(Debug, Clone)]
pub struct CommitError {
    message: String,
}

impl CommitError {
    /// Creates a new commit error.
    pub fn new(message: impl Into<String>) -> Self {
        CommitError {
            message: message.into(),
        }
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Commit failed: {}", self.message)
    }
}

impl std::error::Error for CommitError {}

/// Callback trait for asynchronous offset commit operations.
///
/// This corresponds to `org.apache.kafka.clients.consumer.OffsetCommitCallback` in Java.
/// The `on_complete` method is invoked when an asynchronous offset commit completes.
pub trait OffsetCommitCallback: Send + Sync {
    /// Called when an offset commit completes.
    ///
    /// # Arguments
    /// * `offsets` - A map of topic partitions to their committed offsets and metadata
    /// * `error` - An error if the commit failed, or None if successful
    fn on_complete(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        error: Option<CommitError>,
    );
}

/// A simple callback implementation that does nothing.
/// Useful for testing or when no callback action is needed.
pub struct NoopOffsetCommitCallback;

impl OffsetCommitCallback for NoopOffsetCommitCallback {
    fn on_complete(
        &self,
        _offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        _error: Option<CommitError>,
    ) {
        // No operation
    }
}

/// A callback implementation that stores the result.
/// Useful for testing.
pub struct CapturingOffsetCommitCallback {
    result:
        std::sync::Mutex<Option<Result<HashMap<TopicPartition, OffsetAndMetadata>, CommitError>>>,
}

impl CapturingOffsetCommitCallback {
    /// Creates a new capturing callback.
    pub fn new() -> Self {
        CapturingOffsetCommitCallback {
            result: std::sync::Mutex::new(None),
        }
    }

    /// Returns the captured result.
    pub fn get_result(
        &self,
    ) -> Option<Result<HashMap<TopicPartition, OffsetAndMetadata>, CommitError>> {
        self.result.lock().unwrap().clone()
    }
}

impl Default for CapturingOffsetCommitCallback {
    fn default() -> Self {
        CapturingOffsetCommitCallback::new()
    }
}

impl OffsetCommitCallback for CapturingOffsetCommitCallback {
    fn on_complete(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        error: Option<CommitError>,
    ) {
        let mut result = self.result.lock().unwrap();
        match error {
            Some(e) => *result = Some(Err(e)),
            None => *result = Some(Ok(offsets)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_error_new() {
        let error = CommitError::new("test error");
        assert_eq!(error.message(), "test error");
    }

    #[test]
    fn test_commit_error_display() {
        let error = CommitError::new("test error");
        assert!(error.to_string().contains("test error"));
    }

    #[test]
    fn test_noop_callback() {
        let callback = NoopOffsetCommitCallback;
        let offsets = HashMap::new();
        callback.on_complete(offsets, None);
        // No exception means success
    }

    #[test]
    fn test_capturing_callback_success() {
        let callback = CapturingOffsetCommitCallback::new();

        let tp = TopicPartition::new("test-topic", 0);
        let offset_meta = OffsetAndMetadata::new(100);
        let offsets = HashMap::from([(tp.clone(), offset_meta.clone())]);

        callback.on_complete(offsets, None);

        let result = callback.get_result();
        assert!(result.is_some());
        let result = result.unwrap();
        assert!(result.is_ok());
        let committed = result.unwrap();
        assert!(committed.contains_key(&tp));
        assert_eq!(committed.get(&tp).unwrap().offset(), 100);
    }

    #[test]
    fn test_capturing_callback_error() {
        let callback = CapturingOffsetCommitCallback::new();
        let error = CommitError::new("commit failed");

        callback.on_complete(HashMap::new(), Some(error));

        let result = callback.get_result();
        assert!(result.is_some());
        let result = result.unwrap();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message().contains("commit failed"));
    }

    #[test]
    fn test_capturing_callback_default() {
        let callback = CapturingOffsetCommitCallback::default();
        assert!(callback.get_result().is_none());
    }
}
