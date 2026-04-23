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

//! TasksResource - REST handlers for task management.
//!
//! This module provides REST endpoints for:
//! - Listing tasks for a connector
//! - Getting task status
//! - Restarting tasks
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.resources.TasksResource`
//! in Java (~150 lines).

use axum::{
    Json,
    http::StatusCode,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::rest::entities::{
    TaskInfo, ConnectorTaskId, TaskStatus, ConnectorState,
    ErrorMessage, ConnectorInfo,
};

/// TasksResource - REST handler for task endpoints.
///
/// Provides handlers for the `/connectors/{name}/tasks` REST API endpoints.
/// This skeleton implementation returns mock responses for testing.
///
/// Corresponds to `TasksResource` in Java.
pub struct TasksResource {
    /// In-memory connector storage for testing.
    connectors: Arc<RwLock<HashMap<String, ConnectorInfo>>>,
    /// Task statuses for testing.
    task_statuses: Arc<RwLock<HashMap<String, Vec<TaskStatus>>>>,
}

impl TasksResource {
    /// Creates a new TasksResource for testing (mock implementation).
    pub fn new_for_test() -> Self {
        TasksResource {
            connectors: Arc::new(RwLock::new(HashMap::new())),
            task_statuses: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// List tasks for a connector.
    ///
    /// GET /connectors/{name}/tasks
    /// Returns a list of tasks for the specified connector.
    pub async fn list_tasks(
        &self,
        connector_name: &str,
    ) -> Result<Json<Vec<TaskInfo>>, (StatusCode, Json<ErrorMessage>)> {
        let connectors = self.connectors.read().await;
        match connectors.get(connector_name) {
            Some(info) => {
                let tasks: Vec<TaskInfo> = info.tasks.iter().enumerate().map(|(idx, id)| {
                    TaskInfo::new(
                        ConnectorTaskId::new(connector_name.to_string(), idx as i32),
                        HashMap::new(),
                    )
                }).collect();
                Ok(Json(tasks))
            }
            None => Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Connector {} not found", connector_name))),
            )),
        }
    }

    /// Get task status.
    ///
    /// GET /connectors/{name}/tasks/{task_id}/status
    /// Returns the status of a specific task.
    pub async fn get_task_status(
        &self,
        connector_name: &str,
        task_id: i32,
    ) -> Result<Json<TaskStatus>, (StatusCode, Json<ErrorMessage>)> {
        let connectors = self.connectors.read().await;
        if !connectors.contains_key(connector_name) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Connector {} not found", connector_name))),
            ));
        }

        let statuses = self.task_statuses.read().await;
        match statuses.get(connector_name) {
            Some(tasks) => {
                if task_id >= 0 && (task_id as usize) < tasks.len() {
                    Ok(Json(tasks[task_id as usize].clone()))
                } else {
                    Err((
                        StatusCode::NOT_FOUND,
                        Json(ErrorMessage::new(404, format!("Task {}-{} not found", connector_name, task_id))),
                    ))
                }
            }
            None => {
                // Return a default running status
                Ok(Json(TaskStatus::new(
                    task_id,
                    ConnectorState::Running,
                    "worker-1".to_string(),
                    None,
                )))
            }
        }
    }

    /// Restart a task.
    ///
    /// POST /connectors/{name}/tasks/{task_id}/restart
    /// Restarts a specific task.
    pub async fn restart_task(
        &self,
        connector_name: &str,
        task_id: i32,
    ) -> Result<Json<ErrorMessage>, (StatusCode, Json<ErrorMessage>)> {
        let connectors = self.connectors.read().await;
        if !connectors.contains_key(connector_name) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Connector {} not found", connector_name))),
            ));
        }

        Ok(Json(ErrorMessage::new(
            202,
            format!("Task {}-{} restarted", connector_name, task_id),
        )))
    }

    /// Add a connector for testing purposes.
    pub async fn add_connector_for_test(&self, name: &str, info: ConnectorInfo) {
        let mut connectors = self.connectors.write().await;
        connectors.insert(name.to_string(), info);

        // Create task statuses
        let num_tasks = connectors.get(name).map(|c| c.tasks.len()).unwrap_or(0);
        let statuses: Vec<TaskStatus> = (0..num_tasks).map(|i| {
            TaskStatus::new(
                i as i32,
                ConnectorState::Running,
                "worker-1".to_string(),
                None,
            )
        }).collect();
        let mut task_statuses = self.task_statuses.write().await;
        task_statuses.insert(name.to_string(), statuses);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_list_tasks_empty() {
        let resource = TasksResource::new_for_test();
        let result = resource.list_tasks("non-existent").await;
        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_list_tasks() {
        let resource = TasksResource::new_for_test();
        
        // Add a connector
        let info = ConnectorInfo::new(
            "test-connector".to_string(),
            crate::rest::entities::ConnectorType::Source,
            HashMap::new(),
            vec![ConnectorTaskId::new("test-connector".to_string(), 0)],
        );
        resource.add_connector_for_test("test-connector", info).await;

        // List tasks
        let result = resource.list_tasks("test-connector").await;
        assert!(result.is_ok());
        let tasks = result.unwrap();
        assert_eq!(tasks.len(), 1);
    }

    #[tokio::test]
    async fn test_get_task_status() {
        let resource = TasksResource::new_for_test();
        
        // Add a connector
        let info = ConnectorInfo::new(
            "test-connector".to_string(),
            crate::rest::entities::ConnectorType::Source,
            HashMap::new(),
            vec![ConnectorTaskId::new("test-connector".to_string(), 0)],
        );
        resource.add_connector_for_test("test-connector", info).await;

        // Get task status
        let result = resource.get_task_status("test-connector", 0).await;
        assert!(result.is_ok());
        let status = result.unwrap();
        assert_eq!(status.id, 0);
        assert_eq!(status.state, ConnectorState::Running);
    }

    #[tokio::test]
    async fn test_get_task_status_not_found() {
        let resource = TasksResource::new_for_test();
        
        // Add a connector
        let info = ConnectorInfo::new(
            "test-connector".to_string(),
            crate::rest::entities::ConnectorType::Source,
            HashMap::new(),
            vec![ConnectorTaskId::new("test-connector".to_string(), 0)],
        );
        resource.add_connector_for_test("test-connector", info).await;

        // Get non-existent task status
        let result = resource.get_task_status("test-connector", 5).await;
        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_restart_task() {
        let resource = TasksResource::new_for_test();
        
        // Add a connector
        let info = ConnectorInfo::new(
            "test-connector".to_string(),
            crate::rest::entities::ConnectorType::Source,
            HashMap::new(),
            vec![ConnectorTaskId::new("test-connector".to_string(), 0)],
        );
        resource.add_connector_for_test("test-connector", info).await;

        // Restart task
        let result = resource.restart_task("test-connector", 0).await;
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert!(msg.message.contains("restarted"));
    }
}