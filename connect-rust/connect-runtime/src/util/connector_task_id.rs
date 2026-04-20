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

//! Unique ID for a single task.
//!
//! This corresponds to `org.apache.kafka.connect.util.ConnectorTaskId` in Java.
//! It includes a unique connector ID and a task ID that is unique within the connector.

use serde::{Deserialize, Serialize};

/// Unique ID for a single task.
///
/// It includes a unique connector ID and a task ID that is unique within the connector.
/// This corresponds to `org.apache.kafka.connect.util.ConnectorTaskId` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectorTaskId {
    connector: String,
    task: i32,
}

impl ConnectorTaskId {
    /// Creates a new ConnectorTaskId with the given connector name and task number.
    ///
    /// # Arguments
    /// * `connector` - The name of the connector
    /// * `task` - The task number (0-based)
    pub fn new(connector: String, task: i32) -> Self {
        ConnectorTaskId { connector, task }
    }

    /// Returns the connector name.
    pub fn connector(&self) -> &str {
        &self.connector
    }

    /// Returns the task number.
    pub fn task(&self) -> i32 {
        self.task
    }
}

impl std::fmt::Display for ConnectorTaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.connector, self.task)
    }
}

impl Ord for ConnectorTaskId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.connector.cmp(&other.connector) {
            std::cmp::Ordering::Equal => self.task.cmp(&other.task),
            ordering => ordering,
        }
    }
}

impl PartialOrd for ConnectorTaskId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_task_id_creation() {
        let id = ConnectorTaskId::new("my-connector".to_string(), 0);
        assert_eq!(id.connector(), "my-connector");
        assert_eq!(id.task(), 0);
    }

    #[test]
    fn test_connector_task_id_display() {
        let id = ConnectorTaskId::new("my-connector".to_string(), 5);
        assert_eq!(format!("{}", id), "my-connector-5");
    }

    #[test]
    fn test_connector_task_id_ordering() {
        let id1 = ConnectorTaskId::new("connector-a".to_string(), 1);
        let id2 = ConnectorTaskId::new("connector-a".to_string(), 2);
        let id3 = ConnectorTaskId::new("connector-b".to_string(), 1);

        assert!(id1 < id2);
        assert!(id1 < id3);
        assert!(id2 < id3);
    }

    #[test]
    fn test_connector_task_id_serialization() {
        let id = ConnectorTaskId::new("test".to_string(), 3);
        let json = serde_json::to_string(&id).unwrap();
        assert!(json.contains("test"));
        assert!(json.contains("3"));

        let parsed: ConnectorTaskId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);
    }
}
