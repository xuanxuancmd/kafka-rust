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

//! InternalClusterResource - Endpoints for intra-cluster communication.
//!
//! Contains endpoints necessary for intra-cluster communication--requests that
//! workers issue to each other originating from within the cluster.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.resources.InternalClusterResource` in Java.

use axum::{
    extract::{Path, Query},
    body::Body,
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use crate::rest::herder_request_handler::HerderRequestHandler;
use crate::rest::internal_request_signature::{InternalRequestSignature, Crypto};
use crate::rest::rest_client::RestClient;
use crate::rest::rest_request_timeout::RestRequestTimeout;

/// Query parameters for internal cluster requests.
#[derive(Debug, Deserialize)]
pub struct InternalQueryParams {
    /// Whether to forward the request to another worker.
    #[serde(default)]
    pub forward: Option<bool>,
}

/// InternalClusterResource - Abstract base for internal cluster endpoints.
///
/// This resource provides endpoints for intra-cluster communication such as:
/// - Task configuration updates
/// - Zombie source task fencing
///
/// Corresponds to `InternalClusterResource` in Java.
pub struct InternalClusterResource {
    /// Request handler for completing/forwarding requests.
    request_handler: Arc<HerderRequestHandler>,
}

impl InternalClusterResource {
    /// Creates a new InternalClusterResource.
    ///
    /// # Arguments
    ///
    /// * `rest_client` - REST client for forwarding requests
    /// * `request_timeout` - Timeout configuration
    pub fn new(rest_client: Arc<RestClient>, request_timeout: Arc<dyn RestRequestTimeout>) -> Self {
        let request_handler = Arc::new(HerderRequestHandler::new(rest_client, request_timeout));
        InternalClusterResource { request_handler }
    }

    /// Creates a new InternalClusterResource for testing.
    pub fn new_for_test() -> Self {
        let rest_client = Arc::new(RestClient::new("http://localhost:8083".to_string()));
        let timeout = Arc::new(crate::rest::rest_request_timeout::ConstantRequestTimeout::default());
        InternalClusterResource::new(rest_client, timeout)
    }

    /// Returns the request handler.
    pub fn request_handler(&self) -> &HerderRequestHandler {
        self.request_handler.as_ref()
    }

    /// Puts task configurations for a connector.
    ///
    /// POST /{connector}/tasks
    /// Internal endpoint for task configuration updates.
    ///
    /// This endpoint is used for inter-worker communication when
    /// distributing task configurations across the cluster.
    ///
    /// # Arguments
    ///
    /// * `connector` - The connector name
    /// * `headers` - HTTP headers (including signature)
    /// * `params` - Query parameters (forward flag)
    /// * `body` - Raw request body containing task configs
    ///
    /// # Returns
    ///
    /// Empty response on success.
    pub async fn put_task_configs(
        &self,
        Path(connector): Path<String>,
        headers: HeaderMap,
        Query(params): Query<InternalQueryParams>,
        body: Body,
        put_task_configs_fn: impl Fn(String, Vec<HashMap<String, String>>, Option<InternalRequestSignature>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
    ) -> Result<StatusCode, (StatusCode, Json<String>)> {
        // Get request body bytes
        let body_bytes = axum::body::to_bytes(body, std::usize::MAX)
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, Json(format!("Failed to read body: {}", e))))?;
        
        // Parse task configs from body
        let task_configs: Vec<HashMap<String, String>> = serde_json::from_slice(&body_bytes)
            .map_err(|e| (StatusCode::BAD_REQUEST, Json(format!("Invalid task configs JSON: {}", e))))?;
        
        // Extract signature from headers
        let headers_map = self.extract_headers(&headers);
        let signature = InternalRequestSignature::from_headers(
            &Crypto::SYSTEM,
            body_bytes.to_vec(),
            &headers_map,
        ).map_err(|e| (StatusCode::BAD_REQUEST, Json(e.to_string())))?;
        
        // Call herder put_task_configs
        let result = put_task_configs_fn(connector, task_configs, signature);
        
        match result {
            Ok(_) => Ok(StatusCode::NO_CONTENT),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("forward") {
                    // Need to forward to another worker
                    Err((StatusCode::CONFLICT, Json(err_str)))
                } else if err_str.contains("Rebalance") {
                    Err((StatusCode::CONFLICT, Json(err_str)))
                } else {
                    Err((StatusCode::INTERNAL_SERVER_ERROR, Json(err_str)))
                }
            }
        }
    }

    /// Fence zombie source tasks for a connector.
    ///
    /// PUT /{connector}/fence
    /// Internal endpoint for zombie task fencing.
    ///
    /// This endpoint is used to fence zombie source tasks that may
    /// be running on failed workers.
    ///
    /// # Arguments
    ///
    /// * `connector` - The connector name
    /// * `headers` - HTTP headers (including signature)
    /// * `params` - Query parameters (forward flag)
    /// * `body` - Raw request body
    ///
    /// # Returns
    ///
    /// Empty response on success.
    pub async fn fence_zombies(
        &self,
        Path(connector): Path<String>,
        headers: HeaderMap,
        Query(params): Query<InternalQueryParams>,
        body: Body,
        fence_zombies_fn: impl Fn(String, Option<InternalRequestSignature>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
    ) -> Result<StatusCode, (StatusCode, Json<String>)> {
        // Get request body bytes
        let body_bytes = axum::body::to_bytes(body, std::usize::MAX)
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, Json(format!("Failed to read body: {}", e))))?;
        
        // Extract signature from headers
        let headers_map = self.extract_headers(&headers);
        let signature = InternalRequestSignature::from_headers(
            &Crypto::SYSTEM,
            body_bytes.to_vec(),
            &headers_map,
        ).map_err(|e| (StatusCode::BAD_REQUEST, Json(e.to_string())))?;
        
        // Call herder fence_zombie_source_tasks
        let result = fence_zombies_fn(connector, signature);
        
        match result {
            Ok(_) => Ok(StatusCode::NO_CONTENT),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("forward") {
                    Err((StatusCode::CONFLICT, Json(err_str)))
                } else if err_str.contains("Rebalance") {
                    Err((StatusCode::CONFLICT, Json(err_str)))
                } else {
                    Err((StatusCode::INTERNAL_SERVER_ERROR, Json(err_str)))
                }
            }
        }
    }

    /// Extracts headers from HeaderMap to HashMap.
    fn extract_headers(&self, headers: &HeaderMap) -> HashMap<String, String> {
        headers.iter()
            .filter_map(|(name, value)| {
                value.to_str().ok().map(|v| (name.to_string(), v.to_string()))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_internal_cluster_resource() {
        let resource = InternalClusterResource::new_for_test();
        assert!(resource.request_handler().request_timeout().timeout_ms() > 0);
    }

    #[test]
    fn test_extract_headers() {
        let resource = InternalClusterResource::new_for_test();
        let mut headers = HeaderMap::new();
        headers.insert("X-Custom-Header", "value".parse().unwrap());
        
        let extracted = resource.extract_headers(&headers);
        assert_eq!(extracted.get("x-custom-header"), Some(&"value".to_string()));
    }
}