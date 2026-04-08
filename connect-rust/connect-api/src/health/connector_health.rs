//! Connector Health Module
//!
//! This module provides health information for connectors and their tasks.

use std::collections::HashMap;

/// Task state enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskState {
    /// Task is running
    Running,
    /// Task is paused
    Paused,
    /// Task is failed
    Failed,
    /// Task is destroyed
    Destroyed,
    /// Task is unassigned
    Unassigned,
}

impl TaskState {
    /// Convert task state to string
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskState::Running => "RUNNING",
            TaskState::Paused => "PAUSED",
            TaskState::Failed => "FAILED",
            TaskState::Destroyed => "DESTROYED",
            TaskState::Unassigned => "UNASSIGNED",
        }
    }

    /// Convert string to task state
    pub fn from_str(s: &str) -> Option<TaskState> {
        match s.to_uppercase().as_str() {
            "RUNNING" => Some(TaskState::Running),
            "PAUSED" => Some(TaskState::Paused),
            "FAILED" => Some(TaskState::Failed),
            "DESTROYED" => Some(TaskState::Destroyed),
            "UNASSIGNED" => Some(TaskState::Unassigned),
            _ => None,
        }
    }
}

/// Connector health information
#[derive(Clone)]
pub struct ConnectorHealth {
    /// Connector name
    pub name: String,
    /// Connector state
    pub connector_state: TaskState,
    /// Tasks state map (task_id -> task_state)
    pub tasks: HashMap<i32, TaskState>,
    /// Connector type
    pub connector_type: String,
}

impl ConnectorHealth {
    /// Create a new connector health instance
    ///
    /// # Arguments
    ///
    /// * `name` - connector name
    /// * `connector_state` - connector state
    /// * `tasks` - tasks state map
    /// * `connector_type` - connector type
    ///
    /// # Returns
    ///
    /// New ConnectorHealth instance
    ///
    /// # Examples
    ///
    /// ```
    /// use connect_api::health::{ConnectorHealth, TaskState};
    /// use std::collections::HashMap;
    ///
    /// let mut tasks = HashMap::new();
    /// tasks.insert(0, TaskState::Running);
    ///
    /// let health = ConnectorHealth::new(
    ///     "test-connector".to_string(),
    ///     TaskState::Running,
    ///     tasks,
    ///     "source".to_string()
    /// );
    /// ```
    pub fn new(
        name: String,
        connector_state: TaskState,
        tasks: HashMap<i32, TaskState>,
        connector_type: String,
    ) -> Self {
        if name.is_empty() {
            panic!("Connector name is required");
        }

        Self {
            name,
            connector_state,
            tasks,
            connector_type,
        }
    }

    /// Get connector name
    ///
    /// # Returns
    ///
    /// Connector name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get connector state
    ///
    /// # Returns
    ///
    /// Connector state
    pub fn connector_state(&self) -> TaskState {
        self.connector_state
    }

    /// Get tasks state
    ///
    /// # Returns
    ///
    /// Tasks state map
    pub fn tasks(&self) -> &HashMap<i32, TaskState> {
        &self.tasks
    }

    /// Get connector type
    ///
    /// # Returns
    ///
    /// Connector type
    pub fn connector_type(&self) -> &str {
        &self.connector_type
    }

    /// Get task state for a specific task
    ///
    /// # Arguments
    ///
    /// * `task_id` - task ID
    ///
    /// # Returns
    ///
    /// Task state if task exists, None otherwise
    pub fn task_state(&self, task_id: i32) -> Option<TaskState> {
        self.tasks.get(&task_id).copied()
    }

    /// Update task state
    ///
    /// # Arguments
    ///
    /// * `task_id` - task ID
    /// * `state` - new task state
    pub fn update_task_state(&mut self, task_id: i32, state: TaskState) {
        self.tasks.insert(task_id, state);
    }

    /// Check if connector is healthy
    ///
    /// # Returns
    ///
    /// true if connector is healthy, false otherwise
    pub fn is_healthy(&self) -> bool {
        // Connector is healthy if it's running and ALL tasks are running
        if self.connector_state != TaskState::Running {
            return false;
        }

        self.tasks
            .values()
            .all(|state| *state == TaskState::Running)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_state_as_str() {
        assert_eq!(TaskState::Running.as_str(), "RUNNING");
        assert_eq!(TaskState::Paused.as_str(), "PAUSED");
        assert_eq!(TaskState::Failed.as_str(), "FAILED");
        assert_eq!(TaskState::Destroyed.as_str(), "DESTROYED");
        assert_eq!(TaskState::Unassigned.as_str(), "UNASSIGNED");
    }

    #[test]
    fn test_task_state_from_str() {
        assert_eq!(TaskState::from_str("RUNNING"), Some(TaskState::Running));
        assert_eq!(TaskState::from_str("running"), Some(TaskState::Running));
        assert_eq!(TaskState::from_str("UNKNOWN"), None);
    }

    #[test]
    fn test_connector_health_new() {
        let mut tasks = HashMap::new();
        tasks.insert(0, TaskState::Running);
        tasks.insert(1, TaskState::Paused);

        let health = ConnectorHealth::new(
            "test".to_string(),
            TaskState::Running,
            tasks,
            "source".to_string(),
        );

        assert_eq!(health.name(), "test");
        assert_eq!(health.connector_state(), TaskState::Running);
        assert_eq!(health.connector_type(), "source");
    }

    #[test]
    fn test_connector_health_task_state() {
        let mut tasks = HashMap::new();
        tasks.insert(0, TaskState::Running);
        tasks.insert(1, TaskState::Failed);

        let health = ConnectorHealth::new(
            "test".to_string(),
            TaskState::Running,
            tasks,
            "source".to_string(),
        );

        assert_eq!(health.task_state(0), Some(TaskState::Running));
        assert_eq!(health.task_state(1), Some(TaskState::Failed));
        assert_eq!(health.task_state(2), None);
    }

    #[test]
    fn test_connector_health_update_task_state() {
        let mut tasks = HashMap::new();
        tasks.insert(0, TaskState::Running);

        let mut health = ConnectorHealth::new(
            "test".to_string(),
            TaskState::Running,
            tasks,
            "source".to_string(),
        );

        health.update_task_state(0, TaskState::Failed);
        assert_eq!(health.task_state(0), Some(TaskState::Failed));
    }

    #[test]
    fn test_connector_health_is_healthy() {
        let mut tasks = HashMap::new();
        tasks.insert(0, TaskState::Running);
        tasks.insert(1, TaskState::Running);

        let mut health = ConnectorHealth::new(
            "test".to_string(),
            TaskState::Running,
            tasks,
            "source".to_string(),
        );

        assert!(health.is_healthy());

        health.update_task_state(0, TaskState::Failed);
        assert!(!health.is_healthy());
    }
}
