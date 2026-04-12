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

//! Policy module for Kafka Connect.
//!
//! This module provides policy enforcement for overriding Kafka client configs via connector configs.
//! Common use cases include:
//! - Ability to provide principal per connector (e.g., sasl.jaas.config)
//! - Enforcing that producer/consumer configurations for optimizations are within acceptable ranges

use crate::config::ConfigValue;
use crate::connector_impl::{Closeable, Configurable};
use crate::health::ConnectorType;
use std::any::Any;
use std::collections::HashMap;

// ============================================================================================
// ClientType enum
// ============================================================================================

/// Client type client (used for SOURCE connectors and DLQ in SINK connectors)
/// * `Consumer` - Consumer client (used for SINK connectors and offset topics in SOURCE connectors)
/// * `Admin` - Admin client (used for DLQ topic creation and topic management)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ClientType {
    /// Producer client
    #[default]
    Producer,
    /// Consumer client
    Consumer,
    /// Admin client
    Admin,
}

impl ClientType {
    /// Convert to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            ClientType::Producer => "PRODUCER",
            ClientType::Consumer => "CONSUMER",
            ClientType::Admin => "ADMIN",
        }
    }

    /// Convert string to ClientType
    pub fn from_str(s: &str) -> Option<ClientType> {
        match s.to_uppercase().as_str() {
            "PRODUCER" => Some(ClientType::Producer),
            "CONSUMER" => Some(ClientType::Consumer),
            "ADMIN" => Some(ClientType::Admin),
            _ => None,
        }
    }
}

impl std::fmt::Display for ClientType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ============================================================================================
// ConnectorClientConfigRequest struct
// ============================================================================================

/// Request object for connector client config override policy validation.
///
/// This object provides client configurations to be overridden and its context.
/// The client properties are provided with following prefixes removed:
/// - `producer.override.` for SOURCE connectors and DLQ in SINK connectors
/// - `consumer.override.` for SINK connectors and offset topics in SOURCE connectors
/// - `admin.override.` for DLQ topic creation and topic management
#[derive(Debug)]
pub struct ConnectorClientConfigRequest {
    /// The client override properties specified in the Connector Config
    /// (without the prefix)
    client_props: HashMap<String, Box<dyn Any + Send + Sync>>,
    /// The client type that is being overridden
    client_type: ClientType,
    /// Name of the connector specified in the connector config
    connector_name: String,
    /// Type of the connector (SOURCE or SINK)
    connector_type: ConnectorType,
    /// The class name of the Connector being created
    connector_class: String,
}

impl ConnectorClientConfigRequest {
    /// Create a new ConnectorClientConfigRequest.
    ///
    /// # Arguments
    ///
    /// * `connector_name` - Name of the connector
    /// * `connector_type` - Type of the connector (SOURCE or SINK)
    /// * `connector_class` - Class name of the connector
    /// * `client_props` - Client override properties (without prefix)
    /// * `client_type` - The client type being overridden
    ///
    /// # Returns
    ///
    /// A new ConnectorClientConfigRequest instance
    ///
    /// # Examples
    ///
    /// ```
    /// use connect_api::policy::{ConnectorClientConfigRequest, ClientType};
    /// use connect_api::health::ConnectorType;
    /// use std::collections::HashMap;
    ///
    /// let client_props = HashMap::new();
    ///
    /// let request = ConnectorClientConfigRequest::new(
    ///     "test-connector".to_string(),
    ///     ConnectorType::Source,
    ///     "com.example.TestConnector".to_string(),
    ///     client_props,
    ///     ClientType::Producer,
    /// );
    /// ```
    pub fn new(
        connector_name: String,
        connector_type: ConnectorType,
        connector_class: String,
        client_props: HashMap<String, Box<dyn Any + Send + Sync>>,
        client_type: ClientType,
    ) -> Self {
        if connector_name.is_empty() {
            panic!("Connector name is required");
        }

        Self {
            client_props,
            client_type,
            connector_name,
            connector_type,
            connector_class,
        }
    }

    /// Provides configs with prefix "producer.override." for SOURCE connectors
    /// and also SINK connectors that are configured with a DLQ topic.
    ///
    /// Provides configs with prefix "consumer.override." for SINK connectors
    /// and also SOURCE connectors that are configured with a connector specific offsets topic.
    ///
    /// Provides configs with prefix "admin.override." for SINK connectors configured
    /// with a DLQ topic and SOURCE connectors that are configured with exactly-once semantics,
    /// a connector specific offsets topic or topic creation enabled.
    ///
    /// The returned configs don't include the prefixes.
    ///
    /// # Returns
    ///
    /// The client override properties (without prefix)
    pub fn client_props(&self) -> &HashMap<String, Box<dyn Any + Send + Sync>> {
        &self.client_props
    }

    /// Get the client type being overridden.
    ///
    /// - `Producer` for SOURCE connectors
    /// - `Consumer` for SINK connectors
    /// - `Producer` for DLQ in SINK connectors
    /// - `Admin` for DLQ Topic Creation in SINK connectors
    ///
    /// # Returns
    ///
    /// The client type enumeration
    pub fn client_type(&self) -> ClientType {
        self.client_type
    }

    /// Get the name of the connector specified in the connector config.
    ///
    /// # Returns
    ///
    /// The connector name
    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }

    /// Get the type of the connector.
    ///
    /// # Returns
    ///
    /// The connector type (SOURCE or SINK)
    pub fn connector_type(&self) -> ConnectorType {
        self.connector_type
    }

    /// Get the class name of the Connector being created.
    ///
    /// # Returns
    ///
    /// The connector class name
    pub fn connector_class(&self) -> &str {
        &self.connector_class
    }
}

// ============================================================================================
// ConnectorClientConfigOverridePolicy trait
// ============================================================================================

/// An interface for enforcing a policy on overriding of Kafka client configs via connector configs.
///
/// Common use cases are ability to provide principal per connector (e.g., `sasl.jaas.config`)
/// and/or enforcing that producer/consumer configurations for optimizations are within acceptable ranges.
///
/// Workers will invoke the `validate` method before configuring per
/// connector Kafka admin, producer, and consumer client instances to validate if all the overridden
/// client configurations are allowed per the policy implementation.
///
/// This would also be invoked during the validation of connector configs via the REST API.
/// If there are any policy violations, the connector will not be started.
pub trait ConnectorClientConfigOverridePolicy: Configurable + Closeable {
    /// Validate the client configuration overrides.
    ///
    /// Workers will invoke this before configuring per-connector Kafka admin, producer, and consumer
    /// client instances to validate if all the overridden client configurations are allowed per the
    /// policy implementation.
    ///
    /// This would also be invoked during the validation of connector configs via the REST API.
    ///
    /// If there are any policy violations, the connector will not be started.
    ///
    /// # Arguments
    ///
    /// * `connector_client_config_request` - An instance of ConnectorClientConfigRequest that provides
    ///   the configs to be overridden and its context
    ///
    /// # Returns
    ///
    /// A list of ConfigValue instances that describe each client configuration in the request and
    /// includes an error if the configuration is not allowed by the policy
    ///
    /// # Examples
    ///
    /// ```
    /// use connect_api::policy::{ConnectorClientConfigOverridePolicy, ConnectorClientConfigRequest, ClientType};
    /// use connect_api::health::ConnectorType;
    /// use std::collections::HashMap;
    ///
    /// struct MyPolicy;
    ///
    /// impl ConnectorClientConfigOverridePolicy for MyPolicy {
    ///     fn validate(&self, _request: &ConnectorClientConfigRequest) -> Vec<ConfigValue> {
    ///         Vec::new()
    ///     }
    /// }
    ///
    /// // Implement Configurable and Closeable traits
    /// ```
    fn validate(
        &self,
        connector_client_config_request: &ConnectorClientConfigRequest,
    ) -> Vec<ConfigValue>;
}

// ============================================================================================
// Mock implementations for testing
// ============================================================================================

/// Mock implementation of ConnectorClientConfigOverridePolicy for testing.
#[derive(Debug, Default)]
pub struct MockConnectorClientConfigOverridePolicy {
    /// Whether to return empty validation results
    pub empty_validation: bool,
}

impl MockConnectorClientConfigOverridePolicy {
    /// Create a new mock policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a mock policy that returns empty validation results.
    pub fn with_empty_validation() -> Self {
        Self {
            empty_validation: true,
        }
    }
}

impl Configurable for MockConnectorClientConfigOverridePolicy {
    fn configure(&mut self, _configs: HashMap<String, Box<dyn Any>>) {
        // Mock implementation - does nothing
    }
}

impl Closeable for MockConnectorClientConfigOverridePolicy {
    fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Mock implementation - always succeeds
        Ok(())
    }
}

impl ConnectorClientConfigOverridePolicy for MockConnectorClientConfigOverridePolicy {
    fn validate(&self, _request: &ConnectorClientConfigRequest) -> Vec<ConfigValue> {
        if self.empty_validation {
            Vec::new()
        } else {
            // Return a sample config value for testing
            vec![ConfigValue::String("test".to_string())]
        }
    }
}

/// Mock implementation of ConnectorClientConfigRequest for testing.
#[derive(Debug, Clone)]
pub struct MockConnectorClientConfigRequest {
    /// Mock connector name
    pub connector_name: String,
    /// Mock connector type
    pub connector_type: ConnectorType,
    /// Mock connector class
    pub connector_class: String,
    /// Mock client type
    pub client_type: ClientType,
}

impl MockConnectorClientConfigRequest {
    /// Create a new mock request.
    pub fn new() -> Self {
        Self {
            connector_name: "mock-connector".to_string(),
            connector_type: ConnectorType::Source,
            connector_class: "com.example.MockConnector".to_string(),
            client_type: ClientType::Producer,
        }
    }

    /// Create a mock request with specified values.
    pub fn with_values(
        connector_name: String,
        connector_type: ConnectorType,
        connector_class: String,
        client_type: ClientType,
    ) -> Self {
        Self {
            connector_name,
            connector_type,
            connector_class,
            client_type,
        }
    }

    /// Convert to a real ConnectorClientConfigRequest.
    pub fn to_request(&self) -> ConnectorClientConfigRequest {
        ConnectorClientConfigRequest::new(
            self.connector_name.clone(),
            self.connector_type,
            self.connector_class.clone(),
            HashMap::new(),
            self.client_type,
        )
    }
}

impl Default for MockConnectorClientConfigRequest {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================================
// Tests
// ============================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_type_as_str() {
        assert_eq!(ClientType::Producer.as_str(), "PRODUCER");
        assert_eq!(ClientType::Consumer.as_str(), "CONSUMER");
        assert_eq!(ClientType::Admin.as_str(), "ADMIN");
    }

    #[test]
    fn test_client_type_from_str() {
        assert_eq!(ClientType::from_str("PRODUCER"), Some(ClientType::Producer));
        assert_eq!(ClientType::from_str("producer"), Some(ClientType::Producer));
        assert_eq!(ClientType::from_str("CONSUMER"), Some(ClientType::Consumer));
        assert_eq!(ClientType::from_str("consumer"), Some(ClientType::Consumer));
        assert_eq!(ClientType::from_str("ADMIN"), Some(ClientType::Admin));
        assert_eq!(ClientType::from_str("admin"), Some(ClientType::Admin));
        assert_eq!(ClientType::from_str("unknown"), None);
    }

    #[test]
    fn test_client_type_display() {
        assert_eq!(format!("{}", ClientType::Producer), "PRODUCER");
        assert_eq!(format!("{}", ClientType::Consumer), "CONSUMER");
        assert_eq!(format!("{}", ClientType::Admin), "ADMIN");
    }

    #[test]
    fn test_connector_client_config_request_new() {
        let client_props: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();

        let request = ConnectorClientConfigRequest::new(
            "test-connector".to_string(),
            ConnectorType::Source,
            "com.example.TestConnector".to_string(),
            client_props,
            ClientType::Producer,
        );

        assert_eq!(request.connector_name(), "test-connector");
        assert_eq!(request.connector_type(), ConnectorType::Source);
        assert_eq!(request.connector_class(), "com.example.TestConnector");
        assert_eq!(request.client_type(), ClientType::Producer);
        assert!(request.client_props().is_empty());
    }

    #[test]
    #[should_panic(expected = "Connector name is required")]
    fn test_connector_client_config_request_empty_name() {
        let client_props: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();

        ConnectorClientConfigRequest::new(
            String::new(),
            ConnectorType::Source,
            "com.example.TestConnector".to_string(),
            client_props,
            ClientType::Producer,
        );
    }

    #[test]
    fn test_connector_client_config_request_with_props() {
        let mut client_props: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        client_props.insert(
            "bootstrap.servers".to_string(),
            Box::new("localhost:9092".to_string()),
        );
        client_props.insert("key1".to_string(), Box::new("value1".to_string()));
        client_props.insert("key2".to_string(), Box::new("value2".to_string()));

        let request = ConnectorClientConfigRequest::new(
            "test-connector".to_string(),
            ConnectorType::Sink,
            "com.example.TestSinkConnector".to_string(),
            client_props,
            ClientType::Consumer,
        );

        assert_eq!(request.connector_name(), "test-connector");
        assert_eq!(request.connector_type(), ConnectorType::Sink);
        assert_eq!(request.connector_class(), "com.example.TestSinkConnector");
        assert_eq!(request.client_type(), ClientType::Consumer);
        assert_eq!(request.client_props().len(), 3);
    }

    #[test]
    fn test_connector_client_config_request_all_client_types() {
        // Test Producer client type
        let client_props_producer: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        let request_producer = ConnectorClientConfigRequest::new(
            "test-connector".to_string(),
            ConnectorType::Source,
            "com.example.TestConnector".to_string(),
            client_props_producer,
            ClientType::Producer,
        );
        assert_eq!(request_producer.client_type(), ClientType::Producer);

        // Test Consumer client type
        let client_props_consumer: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        let request_consumer = ConnectorClientConfigRequest::new(
            "test-connector".to_string(),
            ConnectorType::Sink,
            "com.example.TestConnector".to_string(),
            client_props_consumer,
            ClientType::Consumer,
        );
        assert_eq!(request_consumer.client_type(), ClientType::Consumer);

        // Test Admin client type
        let client_props_admin: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        let request_admin = ConnectorClientConfigRequest::new(
            "test-connector".to_string(),
            ConnectorType::Source,
            "com.example.TestConnector".to_string(),
            client_props_admin,
            ClientType::Admin,
        );
        assert_eq!(request_admin.client_type(), ClientType::Admin);
    }

    #[test]
    fn test_mock_connector_client_config_override_policy_validate() {
        let policy = MockConnectorClientConfigOverridePolicy::new();
        let mock_request = MockConnectorClientConfigRequest::new();
        let request = mock_request.to_request();

        let result = policy.validate(&request);

        // Default mock returns a non-empty result
        assert!(!result.is_empty());
    }

    #[test]
    fn test_mock_connector_client_config_override_policy_empty_validation() {
        let policy = MockConnectorClientConfigOverridePolicy::with_empty_validation();
        let mock_request = MockConnectorClientConfigRequest::new();
        let request = mock_request.to_request();

        let result = policy.validate(&request);

        assert!(result.is_empty());
    }

    #[test]
    fn test_mock_connector_client_config_override_policy_configure() {
        let mut policy = MockConnectorClientConfigOverridePolicy::new();
        let configs: HashMap<String, Box<dyn Any>> = HashMap::new();

        // Should not panic
        policy.configure(configs);
    }

    #[test]
    fn test_mock_connector_client_config_override_policy_close() {
        let mut policy = MockConnectorClientConfigOverridePolicy::new();

        let result = policy.close();

        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_connector_client_config_request_new() {
        let mock_request = MockConnectorClientConfigRequest::new();

        assert_eq!(mock_request.connector_name, "mock-connector");
        assert_eq!(mock_request.connector_type, ConnectorType::Source);
        assert_eq!(mock_request.connector_class, "com.example.MockConnector");
        assert_eq!(mock_request.client_type, ClientType::Producer);
    }

    #[test]
    fn test_mock_connector_client_config_request_with_values() {
        let mock_request = MockConnectorClientConfigRequest::with_values(
            "custom-connector".to_string(),
            ConnectorType::Sink,
            "com.example.CustomConnector".to_string(),
            ClientType::Consumer,
        );

        assert_eq!(mock_request.connector_name, "custom-connector");
        assert_eq!(mock_request.connector_type, ConnectorType::Sink);
        assert_eq!(mock_request.connector_class, "com.example.CustomConnector");
        assert_eq!(mock_request.client_type, ClientType::Consumer);
    }

    #[test]
    fn test_mock_connector_client_config_request_to_request() {
        let mock_request = MockConnectorClientConfigRequest::new();
        let request = mock_request.to_request();

        assert_eq!(request.connector_name(), "mock-connector");
        assert_eq!(request.connector_type(), ConnectorType::Source);
        assert_eq!(request.connector_class(), "com.example.MockConnector");
        assert_eq!(request.client_type(), ClientType::Producer);
    }

    #[test]
    fn test_mock_connector_client_config_request_default() {
        let mock_request = MockConnectorClientConfigRequest::default();

        assert_eq!(mock_request.connector_name, "mock-connector");
        assert_eq!(mock_request.connector_type, ConnectorType::Source);
        assert_eq!(mock_request.connector_class, "com.example.MockConnector");
        assert_eq!(mock_request.client_type, ClientType::Producer);
    }
}
