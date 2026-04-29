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

//! ConnectorPluginsResource - REST handlers for plugin management.
//!
//! This module provides REST endpoints for:
//! - Listing available connector plugins
//! - Validating connector configurations
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource`
//! in Java (~150 lines).

use axum::{
    Json,
    http::StatusCode,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::rest::entities::{
    ConfigInfos, ConfigInfo, ConfigKeyDefinition, ConfigValueInfo,
    ConfigDefType, ConfigDefImportance, ConfigDefWidth,
    ErrorMessage,
};

/// Plugin descriptor for REST API responses.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PluginInfo {
    /// Plugin class name.
    pub class: String,
    /// Plugin type (source, sink, etc.).
    #[serde(rename = "type")]
    pub plugin_type: String,
    /// Plugin version.
    pub version: String,
}

impl PluginInfo {
    /// Creates a new PluginInfo.
    pub fn new(class: String, plugin_type: String, version: String) -> Self {
        Self { class, plugin_type, version }
    }
}

/// ConnectorPluginsResource - REST handler for plugin endpoints.
///
/// Provides handlers for the `/connector-plugins` REST API endpoints.
/// This skeleton implementation returns mock responses for testing.
///
/// Corresponds to `ConnectorPluginsResource` in Java.
pub struct ConnectorPluginsResource {
    /// Available plugins for testing.
    plugins: Arc<RwLock<Vec<PluginInfo>>>,
}

impl ConnectorPluginsResource {
    /// Creates a new ConnectorPluginsResource for testing (mock implementation).
    pub fn new_for_test() -> Self {
        ConnectorPluginsResource {
            plugins: Arc::new(RwLock::new(vec![
                PluginInfo::new(
                    "FileStreamSource".to_string(),
                    "source".to_string(),
                    "1.0.0".to_string(),
                ),
                PluginInfo::new(
                    "FileStreamSink".to_string(),
                    "sink".to_string(),
                    "1.0.0".to_string(),
                ),
            ])),
        }
    }

    /// List available connector plugins.
    ///
    /// GET /connector-plugins
    /// Returns a list of available connector plugins.
    pub async fn list_plugins(&self) -> Json<Vec<PluginInfo>> {
        let plugins = self.plugins.read().await;
        Json(plugins.clone())
    }

    /// Validate connector configuration.
    ///
    /// PUT /connector-plugins/{name}/config/validate
    /// Validates a connector configuration against the plugin schema.
    pub async fn validate_plugin_config(
        &self,
        plugin_name: &str,
        config: HashMap<String, String>,
    ) -> Result<Json<ConfigInfos>, (StatusCode, Json<ErrorMessage>)> {
        // Check if plugin exists
        let plugins = self.plugins.read().await;
        let plugin_exists = plugins.iter().any(|p| p.class == plugin_name);

        if !plugin_exists {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorMessage::new(404, format!("Plugin {} not found", plugin_name))),
            ));
        }

        // Build validation result
        let mut configs: Vec<ConfigInfo> = Vec::new();
        let mut error_count = 0;

        // Validate required fields
        let has_connector_class = config.contains_key("connector.class");
        let has_name = config.contains_key("name");

        // connector.class config
        let connector_class_value = config
            .get("connector.class")
            .cloned()
            .unwrap_or_default();
        let connector_class_errors: Vec<String> = if has_connector_class {
            vec![]
        } else {
            error_count += 1;
            vec!["Missing required configuration: connector.class".to_string()]
        };

        configs.push(ConfigInfo::new(
            ConfigKeyDefinition::new(
                "connector.class".to_string(),
                ConfigDefType::String,
                true,
                None,
                ConfigDefImportance::High,
                "Connector class name".to_string(),
                ConfigDefWidth::Long,
            ),
            ConfigValueInfo::new("connector.class".to_string(), Some(connector_class_value), true),
        ));

        // name config
        let name_value = config.get("name").cloned().unwrap_or_default();
        let name_errors: Vec<String> = if has_name {
            vec![]
        } else {
            error_count += 1;
            vec!["Missing required configuration: name".to_string()]
        };

        configs.push(ConfigInfo::new(
            ConfigKeyDefinition::new(
                "name".to_string(),
                ConfigDefType::String,
                true,
                None,
                ConfigDefImportance::High,
                "Connector name".to_string(),
                ConfigDefWidth::Long,
            ),
            ConfigValueInfo::new("name".to_string(), Some(name_value), true),
        ));

        // tasks.max config (optional)
        let tasks_max_value = config.get("tasks.max").cloned().unwrap_or_else(|| "1".to_string());
        configs.push(ConfigInfo::new(
            ConfigKeyDefinition::new(
                "tasks.max".to_string(),
                ConfigDefType::Int,
                false,
                Some("1".to_string()),
                ConfigDefImportance::Medium,
                "Maximum number of tasks".to_string(),
                ConfigDefWidth::Short,
            ),
            ConfigValueInfo::new("tasks.max".to_string(), Some(tasks_max_value), true),
        ));

        // Add other configs from the input
        for (key, value) in config.iter() {
            if !["connector.class", "name", "tasks.max"].contains(&key.as_str()) {
                configs.push(ConfigInfo::new(
                    ConfigKeyDefinition::new(
                        key.clone(),
                        ConfigDefType::String,
                        false,
                        None,
                        ConfigDefImportance::Low,
                        format!("Configuration property {}", key),
                        ConfigDefWidth::Medium,
                    ),
                    ConfigValueInfo::new(key.clone(), Some(value.clone()), true),
                ));
            }
        }

        // Build ConfigInfos result
        let result = ConfigInfos::new(
            config.get("name").cloned().unwrap_or_else(|| "connector".to_string()),
            error_count,
            Vec::new(), // groups - empty for basic validation
            configs,
        );

        Ok(Json(result))
    }

    /// Add a plugin for testing purposes.
    pub async fn add_plugin_for_test(&self, plugin: PluginInfo) {
        let mut plugins = self.plugins.write().await;
        plugins.push(plugin);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_list_plugins() {
        let resource = ConnectorPluginsResource::new_for_test();
        let result = resource.list_plugins().await;
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|p| p.class == "FileStreamSource"));
    }

    #[tokio::test]
    async fn test_validate_plugin_config_valid() {
        let resource = ConnectorPluginsResource::new_for_test();
        
        let config = HashMap::from([
            ("connector.class".to_string(), "FileStreamSource".to_string()),
            ("name".to_string(), "test-connector".to_string()),
            ("tasks.max".to_string(), "1".to_string()),
        ]);

        let result = resource.validate_plugin_config("FileStreamSource", config).await;
        assert!(result.is_ok());
        let infos = result.unwrap();
        assert_eq!(infos.error_count, 0);
    }

    #[tokio::test]
    async fn test_validate_plugin_config_missing_required() {
        let resource = ConnectorPluginsResource::new_for_test();
        
        let config = HashMap::new();

        let result = resource.validate_plugin_config("FileStreamSource", config).await;
        assert!(result.is_ok());
        let infos = result.unwrap();
        assert!(infos.error_count > 0);
    }

    #[tokio::test]
    async fn test_validate_plugin_config_plugin_not_found() {
        let resource = ConnectorPluginsResource::new_for_test();
        
        let config = HashMap::from([
            ("connector.class".to_string(), "UnknownPlugin".to_string()),
            ("name".to_string(), "test".to_string()),
        ]);

        let result = resource.validate_plugin_config("UnknownPlugin", config).await;
        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_add_plugin_for_test() {
        let resource = ConnectorPluginsResource::new_for_test();
        
        let new_plugin = PluginInfo::new(
            "CustomConnector".to_string(),
            "source".to_string(),
            "2.0.0".to_string(),
        );
        resource.add_plugin_for_test(new_plugin).await;

        let result = resource.list_plugins().await;
        assert_eq!(result.len(), 3);
        assert!(result.iter().any(|p| p.class == "CustomConnector"));
    }
}