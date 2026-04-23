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

//! Status tracking for connectors and tasks.
//!
//! Provides status state tracking and management for Kafka Connect runtime.
//! Corresponds to `org.apache.kafka.connect.runtime.AbstractStatus` and related classes in Java.

use std::fmt;

/// AbstractStatus - Base class for connector and task status tracking.
///
/// Provides common state enumeration and tracking functionality.
/// Corresponds to `org.apache.kafka.connect.runtime.AbstractStatus` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    /// Connector/task is starting.
    Starting,
    /// Connector/task is running.
    Running,
    /// Connector/task is paused.
    Paused,
    /// Connector/task is stopped.
    Stopped,
    /// Connector/task has failed.
    Failed,
    /// Connector/task is unassigned.
    Unassigned,
    /// Connector/task is destroyed.
    Destroyed,
    /// Connector/task is restarting.
    Restarting,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Starting => write!(f, "STARTING"),
            State::Running => write!(f, "RUNNING"),
            State::Paused => write!(f, "PAUSED"),
            State::Stopped => write!(f, "STOPPED"),
            State::Failed => write!(f, "FAILED"),
            State::Unassigned => write!(f, "UNASSIGNED"),
            State::Destroyed => write!(f, "DESTROYED"),
            State::Restarting => write!(f, "RESTARTING"),
        }
    }
}

impl State {
    /// Check if this state is a transient state.
    /// Transient states are Starting, Restarting, and Unassigned.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            State::Starting | State::Restarting | State::Unassigned
        )
    }

    /// Check if this state indicates the connector/task is actively running.
    pub fn is_active(&self) -> bool {
        matches!(self, State::Running | State::Starting | State::Restarting)
    }

    /// Check if this state indicates a failure.
    pub fn is_failure(&self) -> bool {
        matches!(self, State::Failed)
    }

    /// Check if this state indicates the connector/task has stopped.
    pub fn is_stopped(&self) -> bool {
        matches!(self, State::Stopped | State::Destroyed | State::Unassigned)
    }
}

/// ConnectorStatus - Status tracking for a connector.
///
/// Tracks the current state, worker assignment, generation, and error trace.
/// Corresponds to `org.apache.kafka.connect.runtime.ConnectorStatus` in Java.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorStatus {
    /// Connector name.
    id: String,
    /// Current state.
    state: State,
    /// Worker ID that owns this connector.
    worker_id: String,
    /// Generation for coordination.
    generation: i32,
    /// Error trace if failed.
    trace: Option<String>,
    /// Connector version.
    version: Option<String>,
}

impl ConnectorStatus {
    /// Create a new ConnectorStatus.
    pub fn new(
        id: String,
        state: State,
        worker_id: String,
        generation: i32,
        trace: Option<String>,
        version: Option<String>,
    ) -> Self {
        ConnectorStatus {
            id,
            state,
            worker_id,
            generation,
            trace,
            version,
        }
    }

    /// Create a running status.
    pub fn running(
        id: String,
        worker_id: String,
        generation: i32,
        version: Option<String>,
    ) -> Self {
        ConnectorStatus::new(id, State::Running, worker_id, generation, None, version)
    }

    /// Create a paused status.
    pub fn paused(id: String, worker_id: String, generation: i32, version: Option<String>) -> Self {
        ConnectorStatus::new(id, State::Paused, worker_id, generation, None, version)
    }

    /// Create a failed status.
    pub fn failed(
        id: String,
        worker_id: String,
        generation: i32,
        trace: String,
        version: Option<String>,
    ) -> Self {
        ConnectorStatus::new(
            id,
            State::Failed,
            worker_id,
            generation,
            Some(trace),
            version,
        )
    }

    /// Create an unassigned status.
    pub fn unassigned(
        id: String,
        worker_id: String,
        generation: i32,
        version: Option<String>,
    ) -> Self {
        ConnectorStatus::new(id, State::Unassigned, worker_id, generation, None, version)
    }

    /// Create a destroyed status.
    pub fn destroyed(
        id: String,
        worker_id: String,
        generation: i32,
        version: Option<String>,
    ) -> Self {
        ConnectorStatus::new(id, State::Destroyed, worker_id, generation, None, version)
    }

    /// Create a restarting status.
    pub fn restarting(
        id: String,
        worker_id: String,
        generation: i32,
        version: Option<String>,
    ) -> Self {
        ConnectorStatus::new(id, State::Restarting, worker_id, generation, None, version)
    }

    /// Create a stopped status.
    pub fn stopped(
        id: String,
        worker_id: String,
        generation: i32,
        version: Option<String>,
    ) -> Self {
        ConnectorStatus::new(id, State::Stopped, worker_id, generation, None, version)
    }

    /// Get the connector ID (name).
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Get the current state.
    pub fn state(&self) -> State {
        self.state
    }

    /// Get the worker ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Get the generation.
    pub fn generation(&self) -> i32 {
        self.generation
    }

    /// Get the error trace.
    pub fn trace(&self) -> Option<&str> {
        self.trace.as_deref()
    }

    /// Get the connector version.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

/// TaskStatus - Status tracking for a task.
///
/// Tracks the current state, worker assignment, generation, and error trace.
/// Corresponds to `org.apache.kafka.connect.runtime.TaskStatus` in Java.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatus {
    /// Task ID.
    id: common_trait::storage::ConnectorTaskId,
    /// Current state.
    state: State,
    /// Worker ID that owns this task.
    worker_id: String,
    /// Generation for coordination.
    generation: i32,
    /// Error trace if failed.
    trace: Option<String>,
    /// Task version.
    version: Option<String>,
}

impl TaskStatus {
    /// Create a new TaskStatus.
    pub fn new(
        id: common_trait::storage::ConnectorTaskId,
        state: State,
        worker_id: String,
        generation: i32,
        trace: Option<String>,
        version: Option<String>,
    ) -> Self {
        TaskStatus {
            id,
            state,
            worker_id,
            generation,
            trace,
            version,
        }
    }

    /// Create a running status.
    pub fn running(
        id: common_trait::storage::ConnectorTaskId,
        worker_id: String,
        generation: i32,
        version: Option<String>,
    ) -> Self {
        TaskStatus::new(id, State::Running, worker_id, generation, None, version)
    }

    /// Create a paused status.
    pub fn paused(
        id: common_trait::storage::ConnectorTaskId,
        worker_id: String,
        generation: i32,
        version: Option<String>,
    ) -> Self {
        TaskStatus::new(id, State::Paused, worker_id, generation, None, version)
    }

    /// Create a failed status.
    pub fn failed(
        id: common_trait::storage::ConnectorTaskId,
        worker_id: String,
        generation: i32,
        trace: String,
        version: Option<String>,
    ) -> Self {
        TaskStatus::new(
            id,
            State::Failed,
            worker_id,
            generation,
            Some(trace),
            version,
        )
    }

    /// Create an unassigned status.
    pub fn unassigned(
        id: common_trait::storage::ConnectorTaskId,
        worker_id: String,
        generation: i32,
        version: Option<String>,
    ) -> Self {
        TaskStatus::new(id, State::Unassigned, worker_id, generation, None, version)
    }

    /// Create a destroyed status.
    pub fn destroyed(
        id: common_trait::storage::ConnectorTaskId,
        worker_id: String,
        generation: i32,
        version: Option<String>,
    ) -> Self {
        TaskStatus::new(id, State::Destroyed, worker_id, generation, None, version)
    }

    /// Create a restarting status.
    pub fn restarting(
        id: common_trait::storage::ConnectorTaskId,
        worker_id: String,
        generation: i32,
        version: Option<String>,
    ) -> Self {
        TaskStatus::new(id, State::Restarting, worker_id, generation, None, version)
    }

    /// Get the task ID.
    pub fn id(&self) -> &common_trait::storage::ConnectorTaskId {
        &self.id
    }

    /// Get the current state.
    pub fn state(&self) -> State {
        self.state
    }

    /// Get the worker ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Get the generation.
    pub fn generation(&self) -> i32 {
        self.generation
    }

    /// Get the error trace.
    pub fn trace(&self) -> Option<&str> {
        self.trace.as_deref()
    }

    /// Get the task version.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

/// AbstractStatus.Listener trait for status change notifications.
///
/// Implemented by AbstractHerder to receive status updates.
/// Corresponds to `org.apache.kafka.connect.runtime.AbstractStatus.Listener` in Java.
pub trait AbstractStatusListener: Send + Sync {
    /// Called when status changes.
    fn on_status_change(&self, id: &str, state: State);
}

/// Listener interface for connector status events.
///
/// This interface is used to receive notifications about connector
/// state changes.
///
/// Corresponds to Java: `ConnectorStatus.Listener`
pub trait ConnectorStatusListener {
    /// Invoked after connector has successfully been shutdown.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    ///
    /// Corresponds to Java: `Listener.onShutdown(String connector)`
    fn on_shutdown(&mut self, connector: &str);

    /// Invoked from the Connector using ConnectorContext.raiseError(Exception)
    /// or if either Connector.start(Map) or Connector.stop() throw an exception.
    /// Note that no shutdown event will follow after the connector has been failed.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    /// * `cause` - Error raised from the connector
    ///
    /// Corresponds to Java: `Listener.onFailure(String connector, Throwable cause)`
    fn on_failure(&mut self, connector: &str, cause: &dyn std::error::Error);

    /// Invoked when the connector is stopped through the REST API.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    ///
    /// Corresponds to Java: `Listener.onStop(String connector)`
    fn on_stop(&mut self, connector: &str);

    /// Invoked when the connector is paused through the REST API.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    ///
    /// Corresponds to Java: `Listener.onPause(String connector)`
    fn on_pause(&mut self, connector: &str);

    /// Invoked after the connector has been resumed.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    ///
    /// Corresponds to Java: `Listener.onResume(String connector)`
    fn on_resume(&mut self, connector: &str);

    /// Invoked after successful startup of the connector.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    ///
    /// Corresponds to Java: `Listener.onStartup(String connector)`
    fn on_startup(&mut self, connector: &str);

    /// Invoked when the connector is deleted through the REST API.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    ///
    /// Corresponds to Java: `Listener.onDeletion(String connector)`
    fn on_deletion(&mut self, connector: &str);

    /// Invoked when the connector is restarted asynchronously by the herder
    /// on processing a restart request.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    ///
    /// Corresponds to Java: `Listener.onRestart(String connector)`
    fn on_restart(&mut self, connector: &str);
}

/// Listener interface for task status events.
///
/// This interface is used to receive notifications about task
/// state changes.
///
/// Corresponds to Java: `TaskStatus.Listener`
pub trait TaskStatusListener {
    /// Invoked after successful startup of the task.
    ///
    /// # Arguments
    /// * `id` - The id of the task
    ///
    /// Corresponds to Java: `Listener.onStartup(ConnectorTaskId id)`
    fn on_startup(&mut self, id: &common_trait::storage::ConnectorTaskId);

    /// Invoked after the task has been paused.
    ///
    /// # Arguments
    /// * `id` - The id of the task
    ///
    /// Corresponds to Java: `Listener.onPause(ConnectorTaskId id)`
    fn on_pause(&mut self, id: &common_trait::storage::ConnectorTaskId);

    /// Invoked after the task has been resumed.
    ///
    /// # Arguments
    /// * `id` - The id of the task
    ///
    /// Corresponds to Java: `Listener.onResume(ConnectorTaskId id)`
    fn on_resume(&mut self, id: &common_trait::storage::ConnectorTaskId);

    /// Invoked if the task raises an error. No shutdown event will follow.
    ///
    /// # Arguments
    /// * `id` - The id of the task
    /// * `cause` - The error raised by the task
    ///
    /// Corresponds to Java: `Listener.onFailure(ConnectorTaskId id, Throwable cause)`
    fn on_failure(
        &mut self,
        id: &common_trait::storage::ConnectorTaskId,
        cause: &dyn std::error::Error,
    );

    /// Invoked after successful shutdown of the task.
    ///
    /// # Arguments
    /// * `id` - The id of the task
    ///
    /// Corresponds to Java: `Listener.onShutdown(ConnectorTaskId id)`
    fn on_shutdown(&mut self, id: &common_trait::storage::ConnectorTaskId);

    /// Invoked after the task has been deleted. Can be called if the
    /// connector tasks have been reduced, or if the connector itself has
    /// been deleted.
    ///
    /// # Arguments
    /// * `id` - The id of the task
    ///
    /// Corresponds to Java: `Listener.onDeletion(ConnectorTaskId id)`
    fn on_deletion(&mut self, id: &common_trait::storage::ConnectorTaskId);

    /// Invoked when the task is restarted asynchronously by the herder
    /// on processing a restart request.
    ///
    /// # Arguments
    /// * `id` - The id of the task
    ///
    /// Corresponds to Java: `Listener.onRestart(ConnectorTaskId id)`
    fn on_restart(&mut self, id: &common_trait::storage::ConnectorTaskId);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_display() {
        assert_eq!(State::Running.to_string(), "RUNNING");
        assert_eq!(State::Paused.to_string(), "PAUSED");
        assert_eq!(State::Failed.to_string(), "FAILED");
        assert_eq!(State::Unassigned.to_string(), "UNASSIGNED");
    }

    #[test]
    fn test_state_is_transient() {
        assert!(State::Starting.is_transient());
        assert!(State::Restarting.is_transient());
        assert!(State::Unassigned.is_transient());
        assert!(!State::Running.is_transient());
        assert!(!State::Failed.is_transient());
    }

    #[test]
    fn test_state_is_active() {
        assert!(State::Running.is_active());
        assert!(State::Starting.is_active());
        assert!(State::Restarting.is_active());
        assert!(!State::Paused.is_active());
        assert!(!State::Failed.is_active());
    }

    #[test]
    fn test_connector_status_running() {
        let status = ConnectorStatus::running(
            "test".to_string(),
            "worker1".to_string(),
            1,
            Some("1.0".to_string()),
        );
        assert_eq!(status.id(), "test");
        assert_eq!(status.state(), State::Running);
        assert_eq!(status.worker_id(), "worker1");
        assert_eq!(status.generation(), 1);
        assert!(status.trace().is_none());
        assert_eq!(status.version(), Some("1.0"));
    }

    #[test]
    fn test_connector_status_failed() {
        let status = ConnectorStatus::failed(
            "test".to_string(),
            "worker1".to_string(),
            1,
            "error trace".to_string(),
            Some("1.0".to_string()),
        );
        assert_eq!(status.state(), State::Failed);
        assert_eq!(status.trace(), Some("error trace"));
    }

    #[test]
    fn test_task_status_running() {
        let task_id = common_trait::storage::ConnectorTaskId::new("conn".to_string(), 0);
        let status = TaskStatus::running(
            task_id.clone(),
            "worker1".to_string(),
            1,
            Some("1.0".to_string()),
        );
        assert_eq!(status.id(), &task_id);
        assert_eq!(status.state(), State::Running);
    }
}
