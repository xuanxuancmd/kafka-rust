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

//! REST extension support for Kafka Connect.
//!
//! This module provides traits and types for implementing REST extensions in Kafka Connect.
//! REST extensions allow plugins to register custom JAX-RS resources like filters, REST endpoints,
//! and providers.

use crate::config::Configurable;
use crate::connector_impl::{Closeable, Versioned};
use crate::health::ConnectorHealth;
use std::any::Any;
use std::collections::HashMap;

/// A plugin interface to allow registration of new JAX-RS resources like Filters, REST endpoints,
/// providers, etc. The implementations will be discovered using the standard Java ServiceLoader
/// mechanism by Connect's plugin class loading mechanism.
///
/// Kafka Connect discovers implementations of this interface using the Java ServiceLoader mechanism.
/// To support this, implementations of this interface should also contain a service provider
/// configuration file in META-INF/services/org.apache.kafka.connect.rest.ConnectRestExtension.
///
/// The extension class(es) must be packaged as a plugin, including the JARs of all dependencies
/// except those already provided by the Connect framework.
///
/// To install into a Connect installation, add a directory named for the plugin and containing the
/// plugin's JARs into a directory that is on Connect's plugin.path, and (re)start the Connect worker.
///
/// When the Connect worker process starts up, it will read its configuration and instantiate all of
/// the REST extension implementation classes that are specified in the `rest.extension.classes`
/// configuration property. Connect will then pass its configuration to each extension via the
/// Configurable::configure method, and will then call register with a provided context.
///
/// When the Connect worker shuts down, it will call the extension's close method to allow the
/// implementation to release all of its resources.
///
/// Implement Monitorable to enable the extension to register metrics.
/// The following tags are automatically added to all metrics registered: `config` set to
/// `rest.extension.classes`, and `class` set to the ConnectRestExtension class name.
pub trait ConnectRestExtension: Configurable + Versioned + Closeable + Send + Sync {
    /// ConnectRestExtension implementations can register custom JAX-RS resources via this method.
    /// The Connect framework will invoke this method after registering the default Connect resources.
    /// If the implementations attempt to re-register any of the Connect resources, it will be ignored
    /// and will be logged.
    ///
    /// # Arguments
    ///
    /// * `rest_plugin_context` - The context provides access to JAX-RS Configurable and
    ///   ConnectClusterState. The custom JAX-RS resources can be registered via the
    ///   ConnectRestExtensionContext::configurable method.
    fn register(&self, rest_plugin_context: &dyn ConnectRestExtensionContext);
}

/// The interface provides the ability for ConnectRestExtension implementations to access the JAX-RS
/// Configurable and cluster state ConnectClusterState. The implementation for the interface is provided
/// by the Connect framework.
pub trait ConnectRestExtensionContext: Send + Sync {
    /// Provides an implementation of JAX-RS Configurable that can be used to register JAX-RS resources.
    ///
    /// # Returns
    ///
    /// The JAX-RS Configurable; never None
    fn configurable(&self) -> Option<&dyn Any>;

    /// Provides the cluster state and health information about the connectors and tasks.
    ///
    /// # Returns
    ///
    /// The cluster state information; never None
    fn cluster_state(&self) -> &dyn ConnectClusterState;
}

/// Provides ability to lookup connector metadata, including status and configurations, as well
/// as immutable cluster information such as Kafka cluster ID. This is made available to
/// ConnectRestExtension implementations. The Connect framework provides the implementation for
/// this interface.
pub trait ConnectClusterState: Send + Sync {
    /// Get names of the connectors currently deployed in this cluster. This is a full list of
    /// connectors in the cluster gathered from the current configuration, which may change over time.
    ///
    /// # Returns
    ///
    /// Collection of connector names, never empty
    fn connectors(&self) -> Vec<String>;

    /// Lookup the current health of a connector and its tasks. This provides the current snapshot of health
    /// by querying the underlying herder. A connector returned by a previous invocation of `connectors()` may
    /// no longer be available and could result in an error.
    ///
    /// # Arguments
    ///
    /// * `conn_name` - name of the connector
    ///
    /// # Returns
    ///
    /// The health of the connector for the connector name
    ///
    /// # Errors
    ///
    /// Returns None if the requested connector can't be found
    fn connector_health(&self, conn_name: &str) -> Option<ConnectorHealth>;

    /// Lookup the current configuration of a connector. This provides the current snapshot of configuration
    /// by querying the underlying herder. A connector returned by a previous invocation of `connectors()` may
    /// no longer be available and could result in an error.
    ///
    /// # Arguments
    ///
    /// * `conn_name` - name of the connector
    ///
    /// # Returns
    ///
    /// The configuration of the connector for the connector name
    ///
    /// # Errors
    ///
    /// Returns None if the requested connector can't be found or if the default implementation
    /// has not been overridden
    fn connector_config(&self, _conn_name: &str) -> Option<HashMap<String, String>> {
        None
    }

    /// Get details about the setup of the Connect cluster.
    ///
    /// # Returns
    ///
    /// A ConnectClusterDetails object containing information about the cluster
    ///
    /// # Errors
    ///
    /// Returns None if the default implementation has not been overridden
    fn cluster_details(&self) -> Option<&dyn ConnectClusterDetails> {
        None
    }
}

/// Provides immutable Connect cluster information, such as the ID of the backing Kafka cluster. The
/// Connect framework provides the implementation for this interface.
pub trait ConnectClusterDetails: Send + Sync {
    /// Get the cluster ID of the Kafka cluster backing this Connect cluster.
    ///
    /// # Returns
    ///
    /// The cluster ID of the Kafka cluster backing this Connect cluster
    fn kafka_cluster_id(&self) -> String;
}

// Mock implementations for testing

/// Mock implementation of ConnectRestExtensionContext for testing.
#[derive(Debug, Clone)]
pub struct MockConnectRestExtensionContext {
    configurable: Option<String>,
    cluster_state: MockConnectClusterState,
}

impl MockConnectRestExtensionContext {
    pub fn new() -> Self {
        Self {
            configurable: Some("mock-configurable".to_string()),
            cluster_state: MockConnectClusterState::new(),
        }
    }

    pub fn with_cluster_state(cluster_state: MockConnectClusterState) -> Self {
        Self {
            configurable: Some("mock-configurable".to_string()),
            cluster_state,
        }
    }
}

impl Default for MockConnectRestExtensionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectRestExtensionContext for MockConnectRestExtensionContext {
    fn configurable(&self) -> Option<&dyn Any> {
        static CONFIGURABLE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        CONFIGURABLE.get_or_init(|| "mock-configurable".to_string());
        // Return None as we cannot return a reference to a static String as &dyn Any
        None
    }

    fn cluster_state(&self) -> &dyn ConnectClusterState {
        &self.cluster_state
    }
}

/// Mock implementation of ConnectClusterState for testing.
#[derive(Debug, Clone)]
pub struct MockConnectClusterState {
    connectors: Vec<String>,
    connector_health_map: HashMap<String, ConnectorHealth>,
    connector_config_map: HashMap<String, HashMap<String, String>>,
    cluster_details: Option<MockConnectClusterDetails>,
}

impl MockConnectClusterState {
    pub fn new() -> Self {
        Self {
            connectors: Vec::new(),
            connector_health_map: HashMap::new(),
            connector_config_map: HashMap::new(),
            cluster_details: None,
        }
    }

    pub fn with_connectors(connectors: Vec<String>) -> Self {
        Self {
            connectors,
            connector_health_map: HashMap::new(),
            connector_config_map: HashMap::new(),
            cluster_details: None,
        }
    }

    pub fn add_connector_health(&mut self, name: String, health: ConnectorHealth) {
        self.connector_health_map.insert(name, health);
    }

    pub fn add_connector_config(&mut self, name: String, config: HashMap<String, String>) {
        self.connector_config_map.insert(name, config);
    }

    pub fn with_cluster_details(mut self, details: MockConnectClusterDetails) -> Self {
        self.cluster_details = Some(details);
        self
    }
}

impl Default for MockConnectClusterState {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectClusterState for MockConnectClusterState {
    fn connectors(&self) -> Vec<String> {
        self.connectors.clone()
    }

    fn connector_health(&self, conn_name: &str) -> Option<ConnectorHealth> {
        self.connector_health_map.get(conn_name).cloned()
    }

    fn connector_config(&self, conn_name: &str) -> Option<HashMap<String, String>> {
        self.connector_config_map.get(conn_name).cloned()
    }

    fn cluster_details(&self) -> Option<&dyn ConnectClusterDetails> {
        self.cluster_details
            .as_ref()
            .map(|d| d as &dyn ConnectClusterDetails)
    }
}

/// Mock implementation of ConnectClusterDetails for testing.
#[derive(Debug, Clone)]
pub struct MockConnectClusterDetails {
    kafka_cluster_id: String,
}

impl MockConnectClusterDetails {
    pub fn new(kafka_cluster_id: String) -> Self {
        Self { kafka_cluster_id }
    }
}

impl ConnectClusterDetails for MockConnectClusterDetails {
    fn kafka_cluster_id(&self) -> String {
        self.kafka_cluster_id.clone()
    }
}

/// Mock implementation of ConnectRestExtension for testing.
#[derive(Debug)]
pub struct MockConnectRestExtension {
    version: String,
    registered: bool,
}

impl MockConnectRestExtension {
    pub fn new(version: String) -> Self {
        Self {
            version,
            registered: false,
        }
    }
}

impl Default for MockConnectRestExtension {
    fn default() -> Self {
        Self::new("1.0.0".to_string())
    }
}

impl Configurable for MockConnectRestExtension {
    fn configure(&mut self, _configs: HashMap<String, Box<dyn Any>>) {
        // Mock implementation does nothing
    }
}

impl Versioned for MockConnectRestExtension {
    fn version(&self) -> String {
        self.version.clone()
    }
}

impl Closeable for MockConnectRestExtension {
    fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.registered = false;
        Ok(())
    }
}

impl ConnectRestExtension for MockConnectRestExtension {
    fn register(&self, _rest_plugin_context: &dyn ConnectRestExtensionContext) {
        // Mock implementation does nothing
        // In a real implementation, this would register JAX-RS resources
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::TaskState;

    #[test]
    fn test_mock_connect_rest_extension_context() {
        let context = MockConnectRestExtensionContext::new();
        let cluster_state = context.cluster_state();
        assert_eq!(cluster_state.connectors().len(), 0);
    }

    #[test]
    fn test_mock_connect_cluster_state() {
        let mut cluster_state = MockConnectClusterState::new();

        // Add some connectors
        let connectors = vec!["connector1".to_string(), "connector2".to_string()];
        cluster_state.connectors = connectors.clone();

        assert_eq!(cluster_state.connectors(), connectors);
    }

    #[test]
    fn test_mock_connect_cluster_state_with_connector_health() {
        let mut cluster_state = MockConnectClusterState::new();

        // Add connector health
        let mut tasks = HashMap::new();
        tasks.insert(0, TaskState::Running);

        let health = ConnectorHealth::new(
            "test-connector".to_string(),
            TaskState::Running,
            tasks,
            "source".to_string(),
        );

        cluster_state.add_connector_health("test-connector".to_string(), health);

        let retrieved_health = cluster_state.connector_health("test-connector");
        assert!(retrieved_health.is_some());
        assert_eq!(retrieved_health.unwrap().name(), "test-connector");
    }

    #[test]
    fn test_mock_connect_cluster_state_with_connector_config() {
        let mut cluster_state = MockConnectClusterState::new();

        // Add connector config
        let mut config = HashMap::new();
        config.insert("key1".to_string(), "value1".to_string());
        config.insert("key2".to_string(), "value2".to_string());

        cluster_state.add_connector_config("test-connector".to_string(), config.clone());

        let retrieved_config = cluster_state.connector_config("test-connector");
        assert!(retrieved_config.is_some());
        assert_eq!(retrieved_config.unwrap().len(), 2);
    }

    #[test]
    fn test_mock_connect_cluster_details() {
        let details = MockConnectClusterDetails::new("test-cluster-id".to_string());
        assert_eq!(details.kafka_cluster_id(), "test-cluster-id");
    }

    #[test]
    fn test_mock_connect_rest_extension() {
        let extension = MockConnectRestExtension::new("1.0.0".to_string());
        assert_eq!(extension.version(), "1.0.0");
    }

    #[test]
    fn test_mock_connect_rest_extension_configurable() {
        let mut extension = MockConnectRestExtension::new("1.0.0".to_string());
        let mut configs = HashMap::new();
        configs.insert(
            "key".to_string(),
            Box::new("value".to_string()) as Box<dyn Any>,
        );

        extension.configure(configs);
        // Should not panic
    }

    #[test]
    fn test_mock_connect_rest_extension_closeable() {
        let mut extension = MockConnectRestExtension::new("1.0.0".to_string());
        let result = extension.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_connect_rest_extension_register() {
        let extension = MockConnectRestExtension::new("1.0.0".to_string());
        let context = MockConnectRestExtensionContext::new();

        // Should not panic
        extension.register(&context);
    }

    #[test]
    fn test_connect_cluster_state_default_methods() {
        let cluster_state = MockConnectClusterState::new();

        // Test default implementations
        let config = cluster_state.connector_config("non-existent");
        assert!(config.is_none());

        let details = cluster_state.cluster_details();
        assert!(details.is_none());
    }

    #[test]
    fn test_connect_cluster_state_with_cluster_details() {
        let cluster_state = MockConnectClusterState::new()
            .with_cluster_details(MockConnectClusterDetails::new("cluster-123".to_string()));

        let details = cluster_state.cluster_details();
        assert!(details.is_some());
        assert_eq!(details.unwrap().kafka_cluster_id(), "cluster-123");
    }
}
