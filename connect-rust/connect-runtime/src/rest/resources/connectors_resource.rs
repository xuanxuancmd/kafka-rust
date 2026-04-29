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

//! ConnectorsResource - REST handlers for connector management.
//!
//! This module provides REST endpoints for:
//! - Listing connectors
//! - Creating/updating/deleting connectors
//! - Getting connector info and status
//! - Pause/resume/restart connectors
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource`
//! in Java (~350 lines).

use axum::{
    Json,
    http::StatusCode,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::rest::entities::{
    ConnectorInfo, ConnectorList, ConnectorStatus, ConnectorState,
    ConnectorStateInfo, ConnectorTaskId, CreateConnectorRequest,
    ErrorMessage, ConnectorType,
};

/// ConnectorsResource - REST handler for connector endpoints.
///
/// Provides handlers for the `/connectors` REST API endpoints.
/// This skeleton implementation returns mock responses for testing.
///
/// Corresponds to `ConnectorsResource` in Java.
pub struct ConnectorsResource {
    /// In-memory connector storage for testing.
    connectors: Arc<RwLock<HashMap<String, ConnectorInfo>>>,
    /// Connector statuses for testing.
    statuses: Arc<RwLock<HashMap<String, ConnectorStatus>>>,
}

impl ConnectorsResource {
    /// Creates a new ConnectorsResource for testing (mock implementation).
    pub fn new_for_test() -> Self {
        ConnectorsResource {
            connectors: Arc::new(RwLock::new(HashMap::new())),
            statuses: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// List all connectors.
    ///
    /// GET /connectors
    /// Returns a list of connector names.
    pub async fn list_connectors(&self) -> Json<ConnectorList> {
        let connectors = self.connectors.read().await;
        let names: Vec<String> = connectors.keys().cloned().collect();
        Json(ConnectorList::new(names))
    }

    /// Create a new connector.
    ///
    /// POST /connectors
    /// Creates a connector with the provided configuration.
    pub async fn create_connector(
        &self,
        request: CreateConnectorRequest,
    ) -> Result<Json<ConnectorInfo>, (StatusCode, Json<ErrorMessage>)> {
        // Get connector name from request
        let name = request.name.clone().unwrap_or_else(|| {
            request.config.get("name").cloned().unwrap_or_default()
        });

        if name.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorMessage::new(400, "Connector name is required".to_string())),
            ));
        }

        // Check if connector already exists
        let connectors = self.connectors.read().await;
        if connectors.contains_key(&name) {
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorMessage::new(409, format!("Connector {} already exists", name))),
            ));
        }

        // Create connector info
        let info = ConnectorInfo::new(
            name.clone(),
            ConnectorType::Source, // Default to source
            request.config.clone(),
            vec![ConnectorTaskId::new(name.clone(), 0)],
        );

        // Store connector
        let mut connectors = self.connectors.write().await;
        connectors.insert(name.clone(), info.clone());

        // Create initial status
        let status = ConnectorStatus::new(
            name.clone(),
            ConnectorStateInfo::new(ConnectorState::Running, "worker-1".to_string()),
            vec![],
            ConnectorType::Source, // Default to Source for test
        );
        let mut statuses = self.statuses.write().await;
        statuses.insert(name.clone(), status);

        Ok(Json(info))
    }

    /// Get connector info.
    ///
    /// GET /connectors/{name}
    /// Returns information about a specific connector.
    pub async fn get_connector(
        &self,
        name: &str,
    ) -> Result<Json<ConnectorInfo>, (StatusCode, Json<ErrorMessage>)> {
        let connectors = self.connectors.read().await;
        match connectors.get(name) {
            Some(info) => Ok(Json(info.clone())),
            None => Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Connector {} not found", name))),
            )),
        }
    }

    /// Update connector configuration.
    ///
    /// PUT /connectors/{name}
    /// Updates the configuration for a connector.
    pub async fn update_connector(
        &self,
        name: &str,
        config: HashMap<String, String>,
    ) -> Result<Json<ConnectorInfo>, (StatusCode, Json<ErrorMessage>)> {
        let connectors = self.connectors.read().await;
        if !connectors.contains_key(name) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Connector {} not found", name))),
            ));
        }

        let mut connectors = self.connectors.write().await;
        let existing = connectors.get(name).cloned().unwrap();
        let updated = ConnectorInfo::new(
            name.to_string(),
            existing.connector_type,
            config,
            existing.tasks,
        );
        connectors.insert(name.to_string(), updated.clone());

        Ok(Json(updated))
    }

    /// Delete a connector.
    ///
    /// DELETE /connectors/{name}
    /// Removes a connector and its tasks.
    pub async fn delete_connector(
        &self,
        name: &str,
    ) -> Result<Json<ConnectorInfo>, (StatusCode, Json<ErrorMessage>)> {
        let mut connectors = self.connectors.write().await;
        match connectors.remove(name) {
            Some(info) => {
                let mut statuses = self.statuses.write().await;
                statuses.remove(name);
                Ok(Json(info))
            }
            None => Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Connector {} not found", name))),
            )),
        }
    }

    /// Get connector status.
    ///
    /// GET /connectors/{name}/status
    /// Returns the current status of a connector.
    pub async fn get_connector_status(
        &self,
        name: &str,
    ) -> Result<Json<ConnectorStatus>, (StatusCode, Json<ErrorMessage>)> {
        let statuses = self.statuses.read().await;
        match statuses.get(name) {
            Some(status) => Ok(Json(status.clone())),
            None => Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Connector {} not found", name))),
            )),
        }
    }

    /// Pause a connector.
    ///
    /// PUT /connectors/{name}/pause
    /// Pauses the connector and its tasks.
    pub async fn pause_connector(
        &self,
        name: &str,
    ) -> Result<Json<ErrorMessage>, (StatusCode, Json<ErrorMessage>)> {
        let connectors = self.connectors.read().await;
        if !connectors.contains_key(name) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Connector {} not found", name))),
            ));
        }

        let mut statuses = self.statuses.write().await;
        if let Some(status) = statuses.get_mut(name) {
            status.connector.state = ConnectorState::Paused;
        }

        Ok(Json(ErrorMessage::new(202, format!("Connector {} paused", name))))
    }

    /// Resume a connector.
    ///
    /// PUT /connectors/{name}/resume
    /// Resumes a paused connector.
    pub async fn resume_connector(
        &self,
        name: &str,
    ) -> Result<Json<ErrorMessage>, (StatusCode, Json<ErrorMessage>)> {
        let connectors = self.connectors.read().await;
        if !connectors.contains_key(name) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Connector {} not found", name))),
            ));
        }

        let mut statuses = self.statuses.write().await;
        if let Some(status) = statuses.get_mut(name) {
            status.connector.state = ConnectorState::Running;
        }

        Ok(Json(ErrorMessage::new(202, format!("Connector {} resumed", name))))
    }

    /// Restart a connector.
    ///
    /// POST /connectors/{name}/restart
    /// Restarts the connector and optionally its tasks.
    pub async fn restart_connector(
        &self,
        name: &str,
    ) -> Result<Json<ErrorMessage>, (StatusCode, Json<ErrorMessage>)> {
        let connectors = self.connectors.read().await;
        if !connectors.contains_key(name) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Connector {} not found", name))),
            ));
        }

        Ok(Json(ErrorMessage::new(202, format!("Connector {} restarted", name))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_list_connectors_empty() {
        let resource = ConnectorsResource::new_for_test();
        let result = resource.list_connectors().await;
        assert_eq!(result.connectors.len(), 0);
    }

    #[tokio::test]
    async fn test_create_connector() {
        let resource = ConnectorsResource::new_for_test();
        let request = CreateConnectorRequest::with_name(
            "test-connector".to_string(),
            HashMap::from([
                ("connector.class".to_string(), "FileStreamSource".to_string()),
            ]),
        );

        let result = resource.create_connector(request).await;
        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.name, "test-connector");
    }

    #[tokio::test]
    async fn test_create_connector_missing_name() {
        let resource = ConnectorsResource::new_for_test();
        let request = CreateConnectorRequest::new(HashMap::new());

        let result = resource.create_connector(request).await;
        assert!(result.is_err());
        let (status, error) = result.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_connector() {
        let resource = ConnectorsResource::new_for_test();
        
        // Create connector first
        let request = CreateConnectorRequest::with_name(
            "test-connector".to_string(),
            HashMap::new(),
        );
        resource.create_connector(request).await.ok();

        // Get connector
        let result = resource.get_connector("test-connector").await;
        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.name, "test-connector");
    }

    #[tokio::test]
    async fn test_get_connector_not_found() {
        let resource = ConnectorsResource::new_for_test();
        let result = resource.get_connector("non-existent").await;
        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_connector() {
        let resource = ConnectorsResource::new_for_test();
        
        // Create connector first
        let request = CreateConnectorRequest::with_name(
            "test-connector".to_string(),
            HashMap::new(),
        );
        resource.create_connector(request).await.ok();

        // Delete connector
        let result = resource.delete_connector("test-connector").await;
        assert!(result.is_ok());

        // Verify it's deleted
        let result = resource.get_connector("test-connector").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pause_resume_connector() {
        let resource = ConnectorsResource::new_for_test();
        
        // Create connector first
        let request = CreateConnectorRequest::with_name(
            "test-connector".to_string(),
            HashMap::new(),
        );
        resource.create_connector(request).await.ok();

        // Pause connector
        let result = resource.pause_connector("test-connector").await;
        assert!(result.is_ok());

        // Resume connector
        let result = resource.resume_connector("test-connector").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_connector_status() {
        let resource = ConnectorsResource::new_for_test();
        
        // Create connector first
        let request = CreateConnectorRequest::with_name(
            "test-connector".to_string(),
            HashMap::new(),
        );
        resource.create_connector(request).await.ok();

        // Get status
        let result = resource.get_connector_status("test-connector").await;
        assert!(result.is_ok());
        let status = result.unwrap();
        assert_eq!(status.name, "test-connector");
        assert_eq!(status.connector.state, ConnectorState::Running);
    }
}