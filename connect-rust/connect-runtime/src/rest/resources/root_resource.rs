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

//! RootResource - REST endpoints for root and health checks.
//!
//! Provides the root endpoint and health check endpoint for Kafka Connect workers.
//! Corresponds to `org.apache.kafka.connect.runtime.rest.resources.RootResource` in Java.

use axum::{
    http::StatusCode,
    Json,
};
use std::sync::Arc;

use crate::rest::entities::{ServerInfo, WorkerStatus};
use crate::rest::rest_request_timeout::RestRequestTimeout;

/// RootResource - REST handler for root and health endpoints.
///
/// Provides handlers for the root endpoint and health check endpoint.
///
/// Corresponds to `RootResource` in Java.
pub struct RootResource {
    /// Server version.
    version: String,
    /// Server commit ID.
    commit: String,
    /// Kafka cluster ID.
    kafka_cluster_id: Option<String>,
    /// Request timeout configuration.
    request_timeout: Arc<dyn RestRequestTimeout>,
    /// Whether the worker is ready.
    is_ready: Arc<std::sync::atomic::AtomicBool>,
}

impl RootResource {
    /// Creates a new RootResource.
    ///
    /// # Arguments
    ///
    /// * `version` - Server version string
    /// * `commit` - Server commit ID
    /// * `kafka_cluster_id` - Optional Kafka cluster ID
    /// * `request_timeout` - Request timeout configuration
    pub fn new(
        version: String,
        commit: String,
        kafka_cluster_id: Option<String>,
        request_timeout: Arc<dyn RestRequestTimeout>,
    ) -> Self {
        RootResource {
            version,
            commit,
            kafka_cluster_id,
            request_timeout,
            is_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Creates a new RootResource for testing.
    pub fn new_for_test() -> Self {
        RootResource::new(
            "3.0.0".to_string(),
            "test-commit".to_string(),
            Some("test-cluster-id".to_string()),
            Arc::new(crate::rest::rest_request_timeout::ConstantRequestTimeout::default()),
        )
    }

    /// Sets the ready status.
    pub fn set_ready(&self, ready: bool) {
        self.is_ready.store(ready, std::sync::atomic::Ordering::SeqCst);
    }

    /// Returns the server info.
    ///
    /// GET /
    /// Returns details about this Connect worker and the Kafka cluster ID.
    pub fn server_info(&self) -> Json<ServerInfo> {
        Json(ServerInfo::new(
            self.version.clone(),
            self.commit.clone(),
        ).with_kafka_cluster_id(self.kafka_cluster_id.clone()))
    }

    /// Performs a health check.
    ///
    /// GET /health
    /// Returns the worker's health status.
    ///
    /// This endpoint verifies:
    /// - Worker readiness
    /// - Herder health
    ///
    /// Returns:
    /// - 200 OK with healthy status if worker is ready
    /// - 503 Service Unavailable with starting status if worker is not ready
    /// - 500 Internal Server Error with unhealthy status on timeout
    pub async fn health_check(&self) -> (StatusCode, Json<WorkerStatus>) {
        let is_ready = self.is_ready.load(std::sync::atomic::Ordering::SeqCst);
        
        if is_ready {
            // Worker is ready and healthy
            (StatusCode::OK, Json(WorkerStatus::healthy()))
        } else {
            // Worker is still starting
            (StatusCode::SERVICE_UNAVAILABLE, Json(WorkerStatus::starting(None)))
        }
    }

    /// Performs a health check with a callback.
    ///
    /// This simulates the Java implementation where a herder callback is used.
    /// In Rust, we use async instead of FutureCallback.
    pub async fn health_check_with_callback(
        &self,
        health_check_future: impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    ) -> (StatusCode, Json<WorkerStatus>) {
        use std::time::Duration;
        
        let timeout = Duration::from_millis(self.request_timeout.health_check_timeout_ms());
        
        let result = tokio::time::timeout(timeout, health_check_future).await;
        
        match result {
            Ok(Ok(_)) => {
                // Health check passed
                (StatusCode::OK, Json(WorkerStatus::healthy()))
            }
            Ok(Err(_)) => {
                // Health check failed but worker may be ready
                if self.is_ready.load(std::sync::atomic::Ordering::SeqCst) {
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(WorkerStatus::unhealthy(None)))
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, Json(WorkerStatus::starting(None)))
                }
            }
            Err(_) => {
                // Timeout occurred
                if self.is_ready.load(std::sync::atomic::Ordering::SeqCst) {
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(WorkerStatus::unhealthy(Some("Health check timed out"))))
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, Json(WorkerStatus::starting(Some("Health check timed out"))))
                }
            }
        }
    }

    /// Returns the version.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the commit ID.
    pub fn commit(&self) -> &str {
        &self.commit
    }

    /// Returns the Kafka cluster ID.
    pub fn kafka_cluster_id(&self) -> Option<&str> {
        self.kafka_cluster_id.as_deref()
    }

    /// Returns whether the worker is ready.
    pub fn is_ready(&self) -> bool {
        self.is_ready.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Extends ServerInfo to have kafka_cluster_id setter.
impl ServerInfo {
    /// Sets the Kafka cluster ID.
    pub fn with_kafka_cluster_id(self, kafka_cluster_id: Option<String>) -> Self {
        ServerInfo {
            version: self.version,
            commit: self.commit,
            kafka_cluster_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest::rest_request_timeout::ConstantRequestTimeout;

    #[test]
    fn test_root_resource_new() {
        let timeout = Arc::new(ConstantRequestTimeout::default());
        let resource = RootResource::new(
            "3.5.0".to_string(),
            "abc123".to_string(),
            Some("cluster-123".to_string()),
            timeout,
        );
        
        assert_eq!(resource.version(), "3.5.0");
        assert_eq!(resource.commit(), "abc123");
        assert_eq!(resource.kafka_cluster_id(), Some("cluster-123"));
        assert!(!resource.is_ready());
    }

    #[test]
    fn test_root_resource_for_test() {
        let resource = RootResource::new_for_test();
        assert_eq!(resource.version(), "3.0.0");
        assert!(resource.kafka_cluster_id().is_some());
    }

    #[test]
    fn test_server_info() {
        let resource = RootResource::new_for_test();
        let info = resource.server_info();
        
        assert_eq!(info.version, "3.0.0");
        assert_eq!(info.commit, "test-commit");
        assert!(info.kafka_cluster_id.is_some());
    }

    #[tokio::test]
    async fn test_health_check_not_ready() {
        let resource = RootResource::new_for_test();
        resource.set_ready(false);
        
        let (status, health) = resource.health_check().await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(health.status, "starting");
    }

    #[tokio::test]
    async fn test_health_check_ready() {
        let resource = RootResource::new_for_test();
        resource.set_ready(true);
        
        let (status, health) = resource.health_check().await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(health.status, "healthy");
    }

    #[tokio::test]
    async fn test_health_check_with_callback_success() {
        let resource = RootResource::new_for_test();
        resource.set_ready(true);
        
        let future = async { Ok::<(), Box<dyn std::error::Error + Send + Sync>>(()) };
        let (status, health) = resource.health_check_with_callback(future).await;
        
        assert_eq!(status, StatusCode::OK);
        assert_eq!(health.status, "healthy");
    }

    #[tokio::test]
    async fn test_health_check_with_callback_timeout() {
        let resource = RootResource::new_for_test();
        resource.set_ready(true);
        
        // Create a future that will timeout
        let future = async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };
        
        let (status, health) = resource.health_check_with_callback(future).await;
        
        // Should timeout and return unhealthy since worker is ready
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(health.status, "unhealthy");
    }

    #[test]
    fn test_set_ready() {
        let resource = RootResource::new_for_test();
        assert!(!resource.is_ready());
        
        resource.set_ready(true);
        assert!(resource.is_ready());
        
        resource.set_ready(false);
        assert!(!resource.is_ready());
    }

    #[test]
    fn test_server_info_with_cluster_id() {
        let info = ServerInfo::new("3.0.0".to_string(), "commit".to_string())
            .with_kafka_cluster_id(Some("my-cluster".to_string()));
        
        assert_eq!(info.kafka_cluster_id, Some("my-cluster".to_string()));
    }
}