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

//! ConnectorState - describes the status, worker ID, and any errors associated with a connector.
//!
//! Corresponds to `org.apache.kafka.connect.health.ConnectorState` in Java.
//! This is a class that extends AbstractState, not an enum.

use crate::health::AbstractState;

/// Describes the status, worker ID, and any errors associated with a connector.
///
/// This corresponds to `org.apache.kafka.connect.health.ConnectorState` in Java.
/// In Java, this class extends AbstractState which contains state, workerId, and traceMessage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorState {
    /// The status of the connector (e.g., "RUNNING", "FAILED", etc.)
    state: String,
    /// The worker ID associated with the connector
    worker_id: String,
    /// Any error message associated with the connector
    trace_message: Option<String>,
}

impl ConnectorState {
    /// Creates a new ConnectorState.
    ///
    /// # Arguments
    ///
    /// * `state` - The status of the connector, may not be null or empty
    /// * `worker_id` - The workerId associated with the connector, may not be null or empty
    /// * `trace_message` - Any error message associated with the connector, may be null or empty
    ///
    /// # Panics
    ///
    /// Panics if state or worker_id is empty.
    pub fn new(
        state: impl Into<String>,
        worker_id: impl Into<String>,
        trace_message: Option<String>,
    ) -> Self {
        let state_str = state.into();
        let worker_id_str = worker_id.into();

        assert!(!state_str.is_empty(), "State must not be empty");
        assert!(!worker_id_str.is_empty(), "Worker ID must not be empty");

        ConnectorState {
            state: state_str,
            worker_id: worker_id_str,
            trace_message,
        }
    }

    /// Creates a ConnectorState from a state string.
    ///
    /// # Arguments
    ///
    /// * `state` - The status string (e.g., "RUNNING", "FAILED")
    /// * `worker_id` - The worker ID
    /// * `trace_message` - Optional error message
    pub fn from_state_str(
        state: impl Into<String>,
        worker_id: impl Into<String>,
        trace_message: Option<&str>,
    ) -> Self {
        ConnectorState::new(state, worker_id, trace_message.map(|s| s.to_string()))
    }

    /// Returns the state string.
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns the worker ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Returns the trace message (error message), if any.
    pub fn trace_message(&self) -> Option<&str> {
        self.trace_message.as_deref()
    }
}

impl AbstractState for ConnectorState {
    fn state(&self) -> &str {
        &self.state
    }

    fn is_healthy(&self) -> bool {
        self.state == "RUNNING"
    }
}

impl std::fmt::Display for ConnectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConnectorState{{state='{}', traceMessage='{}', workerId='{}'}}",
            self.state,
            self.trace_message.as_deref().unwrap_or(""),
            self.worker_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let state = ConnectorState::new("RUNNING", "worker1", Some(String::from("test error")));
        assert_eq!(state.state(), "RUNNING");
        assert_eq!(state.worker_id(), "worker1");
        assert_eq!(state.trace_message(), Some("test error"));
    }

    #[test]
    fn test_new_without_trace() {
        let state = ConnectorState::new("RUNNING", "worker1", None);
        assert_eq!(state.state(), "RUNNING");
        assert_eq!(state.trace_message(), None);
    }

    #[test]
    fn test_from_state_str() {
        let state = ConnectorState::from_state_str("FAILED", "worker2", Some("error"));
        assert_eq!(state.state(), "FAILED");
        assert_eq!(state.trace_message(), Some("error"));
    }

    #[test]
    fn test_is_healthy() {
        let running = ConnectorState::new("RUNNING", "worker1", None);
        assert!(running.is_healthy());

        let failed = ConnectorState::new("FAILED", "worker1", Some(String::from("error")));
        assert!(!failed.is_healthy());
    }

    #[test]
    fn test_display() {
        let state = ConnectorState::new("RUNNING", "worker1", Some(String::from("error")));
        let display = format!("{}", state);
        assert!(display.contains("RUNNING"));
        assert!(display.contains("worker1"));
        assert!(display.contains("error"));
    }
}
