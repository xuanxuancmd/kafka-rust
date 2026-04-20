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

//! Topic status for tracking topic usage by connectors.
//!
//! Represents the metadata that is stored as the value of the record in the
//! status backing store. This tracks which topics a connector/task is actively
//! using.
//!
//! Corresponds to Java: `org.apache.kafka.connect.runtime.TopicStatus`

use common_trait::worker::ConnectorTaskId;
use serde::{Deserialize, Serialize};

/// Represents the metadata that is stored as the value of the record
/// that is stored in the status backing store.
///
/// This tracks when a topic was discovered as being actively used by
/// a connector task.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.TopicStatus`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicStatus {
    /// The name of the topic.
    topic: String,

    /// The name of the connector.
    connector: String,

    /// The ID of the task that stored the topic status.
    task: i32,

    /// Timestamp that represents when this topic was discovered as being
    /// actively used by this connector.
    discover_timestamp: i64,
}

impl TopicStatus {
    /// Creates a new TopicStatus from a ConnectorTaskId.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic
    /// * `task` - The ConnectorTaskId of the task that discovered the topic
    /// * `discover_timestamp` - The discovery timestamp (ms since epoch)
    ///
    /// Corresponds to Java: `TopicStatus(String topic, ConnectorTaskId task, long discoverTimestamp)`
    pub fn new_with_task_id(
        topic: String,
        task: &ConnectorTaskId,
        discover_timestamp: i64,
    ) -> Self {
        TopicStatus {
            topic,
            connector: task.connector().to_string(),
            task: task.task(),
            discover_timestamp,
        }
    }

    /// Creates a new TopicStatus with explicit connector and task values.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic (must not be null)
    /// * `connector` - The name of the connector (must not be null)
    /// * `task` - The ID of the task
    /// * `discover_timestamp` - The discovery timestamp (ms since epoch)
    ///
    /// Corresponds to Java: `TopicStatus(String topic, String connector, int task, long discoverTimestamp)`
    pub fn new(topic: String, connector: String, task: i32, discover_timestamp: i64) -> Self {
        assert!(!topic.is_empty(), "Topic may not be null or empty");
        assert!(!connector.is_empty(), "Connector may not be null or empty");

        TopicStatus {
            topic,
            connector,
            task,
            discover_timestamp,
        }
    }

    /// Get the name of the topic.
    ///
    /// # Returns
    /// The topic name; never null.
    ///
    /// Corresponds to Java: `TopicStatus.topic()`
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the name of the connector.
    ///
    /// # Returns
    /// The connector name; never null.
    ///
    /// Corresponds to Java: `TopicStatus.connector()`
    pub fn connector(&self) -> &str {
        &self.connector
    }

    /// Get the ID of the task that stored the topic status.
    ///
    /// # Returns
    /// The task ID.
    ///
    /// Corresponds to Java: `TopicStatus.task()`
    pub fn task(&self) -> i32 {
        self.task
    }

    /// Get a timestamp that represents when this topic was discovered
    /// as being actively used by this connector.
    ///
    /// # Returns
    /// The discovery timestamp (ms since epoch).
    ///
    /// Corresponds to Java: `TopicStatus.discoverTimestamp()`
    pub fn discover_timestamp(&self) -> i64 {
        self.discover_timestamp
    }
}

impl std::fmt::Display for TopicStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TopicStatus{{topic='{}', connector='{}', task={}, discoverTimestamp={}}}",
            self.topic, self.connector, self.task, self.discover_timestamp
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_status_new() {
        let status = TopicStatus::new(
            "test-topic".to_string(),
            "test-connector".to_string(),
            0,
            1234567890,
        );

        assert_eq!(status.topic(), "test-topic");
        assert_eq!(status.connector(), "test-connector");
        assert_eq!(status.task(), 0);
        assert_eq!(status.discover_timestamp(), 1234567890);
    }

    #[test]
    fn test_topic_status_new_with_task_id() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 1);
        let status = TopicStatus::new_with_task_id("test-topic".to_string(), &task_id, 1234567890);

        assert_eq!(status.topic(), "test-topic");
        assert_eq!(status.connector(), "test-connector");
        assert_eq!(status.task(), 1);
        assert_eq!(status.discover_timestamp(), 1234567890);
    }

    #[test]
    #[should_panic(expected = "Topic may not be null or empty")]
    fn test_topic_status_empty_topic_panics() {
        TopicStatus::new("".to_string(), "connector".to_string(), 0, 1234567890);
    }

    #[test]
    #[should_panic(expected = "Connector may not be null or empty")]
    fn test_topic_status_empty_connector_panics() {
        TopicStatus::new("topic".to_string(), "".to_string(), 0, 1234567890);
    }

    #[test]
    fn test_topic_status_display() {
        let status = TopicStatus::new("my-topic".to_string(), "my-connector".to_string(), 2, 1000);
        let display = format!("{}", status);

        assert!(display.contains("my-topic"));
        assert!(display.contains("my-connector"));
        assert!(display.contains("task=2"));
        assert!(display.contains("discoverTimestamp=1000"));
    }

    #[test]
    fn test_topic_status_equality() {
        let status1 = TopicStatus::new("topic".to_string(), "connector".to_string(), 0, 1234567890);
        let status2 = TopicStatus::new("topic".to_string(), "connector".to_string(), 0, 1234567890);
        let status3 = TopicStatus::new("topic".to_string(), "connector".to_string(), 1, 1234567890);

        assert_eq!(status1, status2);
        assert_ne!(status1, status3);
    }

    #[test]
    fn test_topic_status_serialization() {
        let status = TopicStatus::new("topic".to_string(), "connector".to_string(), 0, 1234567890);

        let serialized = serde_json::to_string(&status).unwrap();
        let deserialized: TopicStatus = serde_json::from_str(&serialized).unwrap();

        assert_eq!(status, deserialized);
    }
}
