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

//! AbstractConnectorClientConfigOverridePolicy - base class for override policies.
//!
//! Corresponds to `org.apache.kafka.connect.connector.policy.AbstractConnectorClientConfigOverridePolicy` in Java.

use std::collections::HashMap;

/// ConfigValue - represents a configuration value with validation.
#[derive(Debug, Clone)]
pub struct ConfigValue {
    /// Configuration name.
    pub name: String,
    /// Configuration value.
    pub value: Option<String>,
    /// Recommended values.
    pub recommended_values: Vec<String>,
    /// Error messages.
    pub error_messages: Vec<String>,
}

impl ConfigValue {
    /// Creates a new ConfigValue.
    pub fn new(name: impl Into<String>, value: Option<String>) -> Self {
        ConfigValue {
            name: name.into(),
            value,
            recommended_values: Vec::new(),
            error_messages: Vec::new(),
        }
    }

    /// Adds an error message.
    pub fn add_error_message(&mut self, message: impl Into<String>) {
        self.error_messages.push(message.into());
    }

    /// Returns the name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the value.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns the error messages.
    pub fn error_messages(&self) -> &[String] {
        &self.error_messages
    }
}

/// ConnectorClientConfigRequest - represents a request for client config override.
#[derive(Debug, Clone)]
pub struct ConnectorClientConfigRequest {
    /// Connector name.
    pub connector_name: String,
    /// Client properties to validate.
    pub client_props: HashMap<String, String>,
}

impl ConnectorClientConfigRequest {
    /// Creates a new request.
    pub fn new(connector_name: impl Into<String>, client_props: HashMap<String, String>) -> Self {
        ConnectorClientConfigRequest {
            connector_name: connector_name.into(),
            client_props,
        }
    }

    /// Returns the connector name.
    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }

    /// Returns the client properties.
    pub fn client_props(&self) -> &HashMap<String, String> {
        &self.client_props
    }
}

/// ConnectorClientConfigOverridePolicy trait.
///
/// Defines the interface for policies that control which client configurations
/// can be overridden by connectors.
pub trait ConnectorClientConfigOverridePolicy {
    /// Validates the connector client config request.
    fn validate(&self, request: &ConnectorClientConfigRequest) -> Vec<ConfigValue>;

    /// Configures the policy.
    fn configure(&mut self, configs: HashMap<String, String>);

    /// Returns the policy version.
    fn version(&self) -> String;

    /// Closes the policy.
    fn close(&mut self);
}

/// AbstractConnectorClientConfigOverridePolicy - base implementation.
///
/// Provides common validation logic for all override policies.
/// Subclasses implement `policy_name()` and `is_allowed()` to define
/// specific policy behavior.
///
/// Corresponds to `org.apache.kafka.connect.connector.policy.AbstractConnectorClientConfigOverridePolicy` in Java.
pub struct AbstractConnectorClientConfigOverridePolicy {
    /// Policy name.
    policy_name: String,
}

impl AbstractConnectorClientConfigOverridePolicy {
    /// Creates a new abstract policy with the given name.
    pub fn new(policy_name: impl Into<String>) -> Self {
        AbstractConnectorClientConfigOverridePolicy {
            policy_name: policy_name.into(),
        }
    }

    /// Returns the policy name.
    pub fn policy_name(&self) -> &str {
        &self.policy_name
    }

    /// Creates a ConfigValue from a config entry.
    pub fn config_value(name: &str, value: Option<String>) -> ConfigValue {
        ConfigValue::new(name, value)
    }

    /// Validates a ConfigValue, adding error message if not allowed.
    pub fn validate_config_value(&self, config_value: &mut ConfigValue, is_allowed: bool) {
        if !is_allowed {
            config_value.add_error_message(format!(
                "The '{}' policy does not allow '{}' to be overridden in the connector configuration.",
                self.policy_name(),
                config_value.name()
            ));
        }
    }
}

impl ConnectorClientConfigOverridePolicy for AbstractConnectorClientConfigOverridePolicy {
    fn validate(&self, request: &ConnectorClientConfigRequest) -> Vec<ConfigValue> {
        request
            .client_props
            .iter()
            .map(|(name, value)| {
                let mut config_value = ConfigValue::new(name, Some(value.clone()));
                self.validate_config_value(&mut config_value, true);
                config_value
            })
            .collect()
    }

    fn configure(&mut self, _configs: HashMap<String, String>) {
        // Default: no configuration needed
    }

    fn version(&self) -> String {
        "1.0.0".to_string()
    }

    fn close(&mut self) {
        // Default: no cleanup needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_value() {
        let mut cv = ConfigValue::new("test.key".to_string(), Some("test.value".to_string()));
        cv.add_error_message("Test error".to_string());
        assert_eq!(cv.name(), "test.key");
        assert_eq!(cv.value(), Some("test.value"));
        assert_eq!(cv.error_messages().len(), 1);
    }

    #[test]
    fn test_connector_client_config_request() {
        let mut props = HashMap::new();
        props.insert("key".to_string(), "value".to_string());
        let request = ConnectorClientConfigRequest::new("test-connector".to_string(), props);
        assert_eq!(request.connector_name(), "test-connector");
        assert!(request.client_props().contains_key("key"));
    }
}
