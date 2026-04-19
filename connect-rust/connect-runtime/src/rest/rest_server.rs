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

//! REST server for Kafka Connect runtime.
//!
//! This module provides the HTTP server implementation for Kafka Connect's
//! REST API. It uses axum for routing and handling HTTP requests.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.RestServer` in Java.

use axum::{
    routing::get,
    Router,
    Json,
    Extension,
};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::entities::ServerInfo;
use super::resources::{
    ConnectorsResource, TasksResource, ConnectorPluginsResource,
};

/// RestServer - HTTP server for Kafka Connect REST API.
///
/// This server exposes Kafka Connect functionality through a REST API,
/// allowing users to create, manage, and monitor connectors and tasks.
///
/// Corresponds to `RestServer` in Java.
pub struct RestServer {
    /// Connectors resource handler.
    connectors_resource: Arc<ConnectorsResource>,
    /// Tasks resource handler.
    tasks_resource: Arc<TasksResource>,
    /// Connector plugins resource handler.
    plugins_resource: Arc<ConnectorPluginsResource>,
    /// Server configuration.
    config: RestServerConfig,
    /// Whether the server is started.
    started: Arc<RwLock<bool>>,
}

/// Configuration for the REST server.
#[derive(Debug, Clone)]
pub struct RestServerConfig {
    /// Host address to bind to.
    pub host: String,
    /// Port to listen on.
    pub port: u16,
    /// Server version string.
    pub version: String,
    /// Server commit ID.
    pub commit: String,
}

impl Default for RestServerConfig {
    fn default() -> Self {
        RestServerConfig {
            host: "localhost".to_string(),
            port: 8083,
            version: "3.0.0".to_string(),
            commit: "unknown".to_string(),
        }
    }
}

impl RestServerConfig {
    /// Creates a new RestServerConfig with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new RestServerConfig with custom host and port.
    pub fn with_host_port(host: String, port: u16) -> Self {
        RestServerConfig {
            host,
            port,
            version: "3.0.0".to_string(),
            commit: "unknown".to_string(),
        }
    }

    /// Returns the bind address string (host:port).
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl RestServer {
    /// Creates a new RestServer with default configuration.
    pub fn new(
        connectors_resource: Arc<ConnectorsResource>,
        tasks_resource: Arc<TasksResource>,
        plugins_resource: Arc<ConnectorPluginsResource>,
    ) -> Self {
        RestServer {
            connectors_resource,
            tasks_resource,
            plugins_resource,
            config: RestServerConfig::new(),
            started: Arc::new(RwLock::new(false)),
        }
    }

    /// Creates a new RestServer with custom configuration.
    pub fn with_config(
        connectors_resource: Arc<ConnectorsResource>,
        tasks_resource: Arc<TasksResource>,
        plugins_resource: Arc<ConnectorPluginsResource>,
        config: RestServerConfig,
    ) -> Self {
        RestServer {
            connectors_resource,
            tasks_resource,
            plugins_resource,
            config,
            started: Arc::new(RwLock::new(false)),
        }
    }

    /// Returns the server configuration.
    pub fn config(&self) -> &RestServerConfig {
        &self.config
    }

    /// Returns whether the server is started.
    pub async fn is_started(&self) -> bool {
        *self.started.read().await
    }

    /// Returns the connectors resource.
    pub fn connectors_resource(&self) -> &Arc<ConnectorsResource> {
        &self.connectors_resource
    }

    /// Returns the tasks resource.
    pub fn tasks_resource(&self) -> &Arc<TasksResource> {
        &self.tasks_resource
    }

    /// Returns the plugins resource.
    pub fn plugins_resource(&self) -> &Arc<ConnectorPluginsResource> {
        &self.plugins_resource
    }

    /// Creates the axum Router with basic route structure.
    ///
    /// This defines the routing structure for the REST API.
    /// Full handler implementations will use Extension to access resources.
    pub fn create_router(&self) -> Router {
        let server_info = ServerInfo::new(
            self.config.version.clone(),
            self.config.commit.clone(),
        );

        Router::new()
            // Root endpoint - server info
            .route("/", get(root_handler))
            // Add extensions for resources
            .layer(Extension(self.connectors_resource.clone()))
            .layer(Extension(self.tasks_resource.clone()))
            .layer(Extension(self.plugins_resource.clone()))
            .layer(Extension(server_info))
    }

    /// Start the REST server (skeleton - no actual TCP binding).
    pub async fn start(&self) {
        *self.started.write().await = true;
    }

    /// Stop the REST server (skeleton - no actual TCP shutdown).
    pub async fn stop(&self) {
        *self.started.write().await = false;
    }
}

/// Root handler - returns server info.
async fn root_handler(Extension(info): Extension<ServerInfo>) -> Json<ServerInfo> {
    Json(info)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rest_server_config_default() {
        let config = RestServerConfig::new();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 8083);
        assert_eq!(config.bind_address(), "localhost:8083");
    }

    #[test]
    fn test_rest_server_config_custom() {
        let config = RestServerConfig::with_host_port("127.0.0.1".to_string(), 9092);
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 9092);
        assert_eq!(config.bind_address(), "127.0.0.1:9092");
    }

    #[test]
    fn test_rest_server_config_version() {
        let config = RestServerConfig::default();
        assert_eq!(config.version, "3.0.0");
    }

    #[tokio::test]
    async fn test_rest_server_start_stop() {
        let connectors_resource = Arc::new(ConnectorsResource::new_for_test());
        let tasks_resource = Arc::new(TasksResource::new_for_test());
        let plugins_resource = Arc::new(ConnectorPluginsResource::new_for_test());
        
        let server = RestServer::new(
            connectors_resource,
            tasks_resource,
            plugins_resource,
        );
        
        assert!(!server.is_started().await);
        
        server.start().await;
        assert!(server.is_started().await);
        
        server.stop().await;
        assert!(!server.is_started().await);
    }

    #[tokio::test]
    async fn test_rest_server_create_router() {
        let connectors_resource = Arc::new(ConnectorsResource::new_for_test());
        let tasks_resource = Arc::new(TasksResource::new_for_test());
        let plugins_resource = Arc::new(ConnectorPluginsResource::new_for_test());
        
        let server = RestServer::new(
            connectors_resource,
            tasks_resource,
            plugins_resource,
        );
        
        let router = server.create_router();
        let _ = router;
    }

    #[tokio::test]
    async fn test_rest_server_with_config() {
        let config = RestServerConfig::with_host_port("0.0.0.0".to_string(), 9000);
        let connectors_resource = Arc::new(ConnectorsResource::new_for_test());
        let tasks_resource = Arc::new(TasksResource::new_for_test());
        let plugins_resource = Arc::new(ConnectorPluginsResource::new_for_test());
        
        let server = RestServer::with_config(
            connectors_resource,
            tasks_resource,
            plugins_resource,
            config,
        );
        
        assert_eq!(server.config().host, "0.0.0.0");
        assert_eq!(server.config().port, 9000);
    }

    #[tokio::test]
    async fn test_rest_server_resources() {
        let connectors_resource = Arc::new(ConnectorsResource::new_for_test());
        let tasks_resource = Arc::new(TasksResource::new_for_test());
        let plugins_resource = Arc::new(ConnectorPluginsResource::new_for_test());
        
        let server = RestServer::new(
            connectors_resource.clone(),
            tasks_resource.clone(),
            plugins_resource.clone(),
        );
        
        assert!(server.connectors_resource().list_connectors().await.connectors.is_empty());
        assert!(server.tasks_resource().list_tasks("test").await.is_err());
        assert_eq!(server.plugins_resource().list_plugins().await.len(), 2);
    }
}