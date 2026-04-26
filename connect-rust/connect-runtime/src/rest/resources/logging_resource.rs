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

//! LoggingResource - REST endpoints for adjusting log levels.
//!
//! Provides endpoints to list, get, and modify log levels of runtime loggers.
//! Corresponds to `org.apache.kafka.connect.runtime.rest.resources.LoggingResource` in Java.

use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::rest::entities::LoggerLevel;

/// Scope parameter for logging modifications.
const WORKER_SCOPE: &str = "worker";
const CLUSTER_SCOPE: &str = "cluster";

/// Query parameters for logging resource.
#[derive(Debug, Deserialize)]
pub struct LoggingQueryParams {
    /// Scope for the logging modification (worker or cluster).
    #[serde(default = "default_scope")]
    pub scope: String,
}

fn default_scope() -> String {
    WORKER_SCOPE.to_string()
}

/// Request body for setting log level.
#[derive(Debug, Deserialize)]
pub struct SetLevelRequest {
    /// The desired log level.
    pub level: String,
}

impl SetLevelRequest {
    /// Creates a new SetLevelRequest.
    pub fn new(level: String) -> Self {
        SetLevelRequest { level }
    }
}

/// LoggingResource - REST handler for logging endpoints.
///
/// Provides handlers for the `/admin/loggers` REST API endpoints.
/// Allows users to list, get, and modify log levels for runtime loggers.
///
/// Corresponds to `LoggingResource` in Java.
pub struct LoggingResource {
    /// Herder reference for logger operations (mock for now).
    logger_levels: Arc<RwLock<HashMap<String, LoggerLevel>>>,
}

impl LoggingResource {
    /// Creates a new LoggingResource for testing (mock implementation).
    pub fn new_for_test() -> Self {
        LoggingResource {
            logger_levels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a new LoggingResource with existing logger levels.
    pub fn new(logger_levels: HashMap<String, LoggerLevel>) -> Self {
        LoggingResource {
            logger_levels: Arc::new(RwLock::new(logger_levels)),
        }
    }

    /// List all loggers with their levels.
    ///
    /// GET /admin/loggers
    /// Returns a map of logger names to their levels.
    pub async fn list_loggers(&self) -> Json<HashMap<String, LoggerLevel>> {
        let levels = self.logger_levels.read().await;
        Json(levels.clone())
    }

    /// Get the log level of a named logger.
    ///
    /// GET /admin/loggers/{logger}
    /// Returns the level of the specified logger.
    pub async fn get_logger(
        &self,
        Path(logger_name): Path<String>,
    ) -> Result<Json<LoggerLevel>, (StatusCode, Json<String>)> {
        let levels = self.logger_levels.read().await;
        
        match levels.get(&logger_name) {
            Some(level) => Ok(Json(level.clone())),
            None => Err((
                StatusCode::NOT_FOUND,
                Json(format!("Logger {} not found.", logger_name)),
            )),
        }
    }

    /// Set the log level of a named logger.
    ///
    /// PUT /admin/loggers/{logger}
    /// Sets the log level for the specified logger.
    ///
    /// # Arguments
    ///
    /// * `logger_name` - The logger name/namespace
    /// * `request` - Request body containing the desired level
    /// * `params` - Query parameters (scope)
    ///
    /// # Returns
    ///
    /// For worker scope: returns list of affected loggers.
    /// For cluster scope: returns 204 No Content.
    pub async fn set_level(
        &self,
        Path(logger_name): Path<String>,
        Json(request): Json<SetLevelRequest>,
        Query(params): Query<LoggingQueryParams>,
    ) -> Result<Json<Vec<String>>, (StatusCode, Json<String>)> {
        // Validate the log level
        let valid_levels = ["DEBUG", "ERROR", "FATAL", "INFO", "TRACE", "WARN", "OFF"];
        let level_upper = request.level.to_uppercase();
        
        if !valid_levels.contains(&level_upper.as_str()) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(format!("Invalid log level '{}'.", request.level)),
            ));
        }

        // Handle scope
        let scope = if params.scope.is_empty() {
            log::warn!("Received empty scope, defaulting to worker");
            WORKER_SCOPE
        } else {
            &params.scope
        };

        match scope.to_lowercase().as_str() {
            WORKER_SCOPE => {
                // Update worker-level logger
                let mut levels = self.logger_levels.write().await;
                
                // Find affected loggers (all that match the namespace)
                let affected: Vec<String> = levels.keys()
                    .filter(|name| name.starts_with(logger_name.as_str()) || name.as_str() == logger_name.as_str())
                    .cloned()
                    .collect();
                
                // Update the logger level
                let new_level = LoggerLevel::new(level_upper.clone(), Some(current_time_ms()));
                levels.insert(logger_name.clone(), new_level);
                
                // If no affected loggers were found, at least the requested one is affected
                if affected.is_empty() {
                    Ok(Json(vec![logger_name]))
                } else {
                    Ok(Json(affected))
                }
            }
            CLUSTER_SCOPE => {
                // For cluster scope, we would propagate to all workers
                // In this mock implementation, we just update locally
                let mut levels = self.logger_levels.write().await;
                let new_level = LoggerLevel::new(level_upper, Some(current_time_ms()));
                levels.insert(logger_name, new_level);
                
                // Return empty response to indicate 204 No Content would be used
                // In real implementation, this would return StatusCode::NO_CONTENT
                Ok(Json(vec![]))
            }
            _ => {
                log::warn!("Received invalid scope '{}', defaulting to worker", scope);
                // Default to worker scope
                let mut levels = self.logger_levels.write().await;
                let new_level = LoggerLevel::new(level_upper, Some(current_time_ms()));
                levels.insert(logger_name.clone(), new_level);
                Ok(Json(vec![logger_name]))
            }
        }
    }
}

/// Gets current time in milliseconds (Unix epoch).
fn current_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_list_loggers_empty() {
        let resource = LoggingResource::new_for_test();
        let result = resource.list_loggers().await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_list_loggers_with_data() {
        let mut levels = HashMap::new();
        levels.insert("org.apache.kafka".to_string(), LoggerLevel::with_level("INFO".to_string()));
        levels.insert("org.apache.kafka.connect".to_string(), LoggerLevel::with_level("DEBUG".to_string()));
        
        let resource = LoggingResource::new(levels);
        let result = resource.list_loggers().await;
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_get_logger_found() {
        let mut levels = HashMap::new();
        levels.insert("test.logger".to_string(), LoggerLevel::with_level("INFO".to_string()));
        
        let resource = LoggingResource::new(levels);
        let result = resource.get_logger(Path("test.logger".to_string())).await;
        assert!(result.is_ok());
        let level = result.unwrap();
        assert_eq!(level.level, "INFO");
    }

    #[tokio::test]
    async fn test_get_logger_not_found() {
        let resource = LoggingResource::new_for_test();
        let result = resource.get_logger(Path("nonexistent".to_string())).await;
        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_set_level_worker_scope() {
        let resource = LoggingResource::new_for_test();
        
        let request = SetLevelRequest::new("DEBUG".to_string());
        let params = LoggingQueryParams { scope: WORKER_SCOPE.to_string() };
        
        let result = resource.set_level(
            Path("test.logger".to_string()),
            Json(request),
            Query(params),
        ).await;
        
        assert!(result.is_ok());
        let affected = result.unwrap();
        assert!(affected.contains(&"test.logger".to_string()));
        
        // Verify the level was set
        let level_result = resource.get_logger(Path("test.logger".to_string())).await;
        assert!(level_result.is_ok());
        assert_eq!(level_result.unwrap().level, "DEBUG");
    }

    #[tokio::test]
    async fn test_set_level_cluster_scope() {
        let resource = LoggingResource::new_for_test();
        
        let request = SetLevelRequest::new("WARN".to_string());
        let params = LoggingQueryParams { scope: CLUSTER_SCOPE.to_string() };
        
        let result = resource.set_level(
            Path("org.apache.kafka".to_string()),
            Json(request),
            Query(params),
        ).await;
        
        assert!(result.is_ok());
        // Cluster scope returns empty (would be 204 No Content)
        let affected = result.unwrap();
        assert!(affected.is_empty());
    }

    #[tokio::test]
    async fn test_set_level_invalid_level() {
        let resource = LoggingResource::new_for_test();
        
        let request = SetLevelRequest::new("INVALID_LEVEL".to_string());
        let params = LoggingQueryParams { scope: WORKER_SCOPE.to_string() };
        
        let result = resource.set_level(
            Path("test.logger".to_string()),
            Json(request),
            Query(params),
        ).await;
        
        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_set_level_case_insensitive() {
        let resource = LoggingResource::new_for_test();
        
        let request = SetLevelRequest::new("info".to_string()); // lowercase
        let params = LoggingQueryParams { scope: WORKER_SCOPE.to_string() };
        
        let result = resource.set_level(
            Path("test.logger".to_string()),
            Json(request),
            Query(params),
        ).await;
        
        assert!(result.is_ok());
        let level_result = resource.get_logger(Path("test.logger".to_string())).await;
        assert!(level_result.is_ok());
        assert_eq!(level_result.unwrap().level, "INFO"); // uppercase
    }

    #[test]
    fn test_default_scope() {
        assert_eq!(default_scope(), WORKER_SCOPE);
    }
}