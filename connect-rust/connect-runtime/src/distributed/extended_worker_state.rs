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

//! A class that captures the deserialized form of a worker's metadata.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.distributed.ExtendedWorkerState` in Java.

use crate::distributed::extended_assignment::ExtendedAssignment;

/// A class that captures the deserialized form of a worker's metadata.
///
/// This captures the state of a worker at the moment its metadata was serialized,
/// including its URL, the config offset it has seen, and its current assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendedWorkerState {
    /// The URL of this worker
    url: String,
    /// The most up-to-date configuration offset known to this worker
    offset: i64,
    /// The assignment of connectors and tasks at the moment the state was captured
    assignment: ExtendedAssignment,
}

impl ExtendedWorkerState {
    /// Create a new extended worker state.
    ///
    /// # Arguments
    /// * `url` - The URL of this worker
    /// * `offset` - The most up-to-date configuration offset known to this worker
    /// * `assignment` - The assignment of connectors and tasks; if null, uses empty assignment
    pub fn new(url: String, offset: i64, assignment: Option<ExtendedAssignment>) -> Self {
        ExtendedWorkerState {
            url,
            offset,
            assignment: assignment.unwrap_or_else(|| ExtendedAssignment::empty()),
        }
    }

    /// Return the URL of this worker.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Return the most up-to-date (maximum) configuration offset known to this worker.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Return the assignment of connectors and tasks on a worker at the moment
    /// that its state was captured by this class.
    pub fn assignment(&self) -> &ExtendedAssignment {
        &self.assignment
    }
}

impl std::fmt::Display for ExtendedWorkerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerState{{url='{}', offset={}, {}}}",
            self.url, self.offset, self.assignment
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::connect_protocol::ConnectorTaskId;
    use crate::distributed::extended_assignment::{CONNECT_PROTOCOL_V1, NO_ERROR};

    #[test]
    fn test_worker_state_creation() {
        let assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            NO_ERROR,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["connector-a".to_string()],
            vec![ConnectorTaskId::new("connector-a".to_string(), 0)],
            vec![],
            vec![],
            0,
        );

        let state =
            ExtendedWorkerState::new("http://worker:8083".to_string(), 100, Some(assignment));

        assert_eq!(state.url(), "http://worker:8083");
        assert_eq!(state.offset(), 100);
        assert_eq!(state.assignment().connectors().len(), 1);
    }

    #[test]
    fn test_worker_state_with_null_assignment() {
        let state = ExtendedWorkerState::new("http://worker:8083".to_string(), 50, None);

        assert_eq!(state.url(), "http://worker:8083");
        assert_eq!(state.offset(), 50);
        // Should use empty assignment
        assert!(state.assignment().connectors().is_empty());
        assert!(state.assignment().tasks().is_empty());
    }

    #[test]
    fn test_display() {
        let assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            NO_ERROR,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["connector-a".to_string()],
            vec![ConnectorTaskId::new("connector-a".to_string(), 0)],
            vec![],
            vec![],
            0,
        );

        let state =
            ExtendedWorkerState::new("http://worker:8083".to_string(), 100, Some(assignment));

        let display_str = format!("{}", state);
        assert!(display_str.contains("url='http://worker:8083'"));
        assert!(display_str.contains("offset=100"));
    }

    #[test]
    fn test_equality() {
        let assignment1 = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            NO_ERROR,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["connector-a".to_string()],
            vec![ConnectorTaskId::new("connector-a".to_string(), 0)],
            vec![],
            vec![],
            0,
        );

        let assignment2 = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            NO_ERROR,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["connector-a".to_string()],
            vec![ConnectorTaskId::new("connector-a".to_string(), 0)],
            vec![],
            vec![],
            0,
        );

        let state1 =
            ExtendedWorkerState::new("http://worker:8083".to_string(), 100, Some(assignment1));

        let state2 =
            ExtendedWorkerState::new("http://worker:8083".to_string(), 100, Some(assignment2));

        assert_eq!(state1, state2);
    }

    #[test]
    fn test_clone() {
        let assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            NO_ERROR,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["connector-a".to_string()],
            vec![ConnectorTaskId::new("connector-a".to_string(), 0)],
            vec![],
            vec![],
            0,
        );

        let state =
            ExtendedWorkerState::new("http://worker:8083".to_string(), 100, Some(assignment));

        let cloned = state.clone();
        assert_eq!(state, cloned);
    }
}
