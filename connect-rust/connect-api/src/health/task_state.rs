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

//! TaskState - describes the state, IDs, and any errors of a connector task.
//!
//! Corresponds to `org.apache.kafka.connect.health.TaskState` in Java.
//! This is a class that extends AbstractState, not an enum.

use std::hash::{Hash, Hasher};

use crate::health::AbstractState;

/// Describes the state, IDs, and any errors of a connector task.
///
/// This corresponds to `org.apache.kafka.connect.health.TaskState` in Java.
/// In Java, this class extends AbstractState and adds taskId field.
#[derive(Debug, Clone)]
pub struct TaskState {
    /// The ID of the task
    task_id: i32,
    /// The status of the task (e.g., "RUNNING", "FAILED", etc.)
    state: String,
    /// The worker ID associated with the task
    worker_id: String,
    /// Any error trace message associated with the task
    trace: Option<String>,
}

impl TaskState {
    /// Creates a new TaskState.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The ID associated with the connector task
    /// * `state` - The status of the task, may not be null or empty
    /// * `worker_id` - The worker ID the task is associated with, may not be null or empty
    /// * `trace` - Error message if the task had failed or errored out, may be null or empty
    ///
    /// # Panics
    ///
    /// Panics if state or worker_id is empty.
    pub fn new(
        task_id: i32,
        state: impl Into<String>,
        worker_id: impl Into<String>,
        trace: Option<String>,
    ) -> Self {
        let state_str = state.into();
        let worker_id_str = worker_id.into();

        assert!(!state_str.is_empty(), "State must not be empty");
        assert!(!worker_id_str.is_empty(), "Worker ID must not be empty");

        TaskState {
            task_id,
            state: state_str,
            worker_id: worker_id_str,
            trace,
        }
    }

    /// Creates a TaskState from individual components.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    /// * `state` - The status string (e.g., "RUNNING", "FAILED")
    /// * `worker_id` - The worker ID
    /// * `trace` - Optional error trace
    pub fn from_components(
        task_id: i32,
        state: impl Into<String>,
        worker_id: impl Into<String>,
        trace: Option<&str>,
    ) -> Self {
        TaskState::new(task_id, state, worker_id, trace.map(|s| s.to_string()))
    }

    /// Returns the task ID.
    pub fn task_id(&self) -> i32 {
        self.task_id
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
    pub fn trace(&self) -> Option<&str> {
        self.trace.as_deref()
    }
}

impl AbstractState for TaskState {
    fn state(&self) -> &str {
        &self.state
    }

    fn is_healthy(&self) -> bool {
        self.state == "RUNNING"
    }
}

impl PartialEq for TaskState {
    fn eq(&self, other: &Self) -> bool {
        self.task_id == other.task_id
    }
}

impl Eq for TaskState {}

impl Hash for TaskState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.task_id.hash(state);
    }
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TaskState{{taskId='{}', state='{}', traceMessage='{}', workerId='{}'}}",
            self.task_id,
            self.state,
            self.trace.as_deref().unwrap_or(""),
            self.worker_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let state = TaskState::new(0, "RUNNING", "worker1", Some(String::from("test error")));
        assert_eq!(state.task_id(), 0);
        assert_eq!(state.state(), "RUNNING");
        assert_eq!(state.worker_id(), "worker1");
        assert_eq!(state.trace(), Some("test error"));
    }

    #[test]
    fn test_new_without_trace() {
        let state = TaskState::new(1, "RUNNING", "worker1", None);
        assert_eq!(state.task_id(), 1);
        assert_eq!(state.trace(), None);
    }

    #[test]
    fn test_from_components() {
        let state = TaskState::from_components(2, "FAILED", "worker2", Some("error"));
        assert_eq!(state.task_id(), 2);
        assert_eq!(state.state(), "FAILED");
        assert_eq!(state.trace(), Some("error"));
    }

    #[test]
    fn test_is_healthy() {
        let running = TaskState::new(0, "RUNNING", "worker1", None);
        assert!(running.is_healthy());

        let failed = TaskState::new(0, "FAILED", "worker1", Some(String::from("error")));
        assert!(!failed.is_healthy());
    }

    #[test]
    fn test_equality() {
        let state1 = TaskState::new(0, "RUNNING", "worker1", None);
        let state2 = TaskState::new(0, "FAILED", "worker2", Some(String::from("error")));
        let state3 = TaskState::new(1, "RUNNING", "worker1", None);

        // Equality is based on task_id only
        assert_eq!(state1, state2);
        assert_ne!(state1, state3);
    }

    #[test]
    fn test_display() {
        let state = TaskState::new(0, "RUNNING", "worker1", Some(String::from("error")));
        let display = format!("{}", state);
        assert!(display.contains("taskId='0'"));
        assert!(display.contains("RUNNING"));
        assert!(display.contains("worker1"));
        assert!(display.contains("error"));
    }
}
