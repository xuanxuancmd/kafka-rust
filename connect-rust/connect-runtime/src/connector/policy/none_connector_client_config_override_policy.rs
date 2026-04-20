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

//! NoneConnectorClientConfigOverridePolicy - disallows all configurations.
//!
//! Corresponds to `org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy` in Java.

use std::collections::HashMap;

use super::{
    AbstractConnectorClientConfigOverridePolicy, ConfigValue, ConnectorClientConfigOverridePolicy,
    ConnectorClientConfigRequest,
};

/// NoneConnectorClientConfigOverridePolicy - disallows all client configurations.
///
/// This policy disallows any client configuration to be overridden via connector configs.
/// Set `connector.client.config.override.policy` to `None` to use this policy.
/// This is the default behavior.
///
/// Corresponds to `org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy` in Java.
pub struct NoneConnectorClientConfigOverridePolicy {
    inner: AbstractConnectorClientConfigOverridePolicy,
}

impl NoneConnectorClientConfigOverridePolicy {
    /// Creates a new NoneConnectorClientConfigOverridePolicy.
    pub fn new() -> Self {
        NoneConnectorClientConfigOverridePolicy {
            inner: AbstractConnectorClientConfigOverridePolicy::new("None"),
        }
    }

    /// Always returns false (disallows all configurations).
    pub fn is_allowed(&self, _config_value: &ConfigValue) -> bool {
        false
    }
}

impl Default for NoneConnectorClientConfigOverridePolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorClientConfigOverridePolicy for NoneConnectorClientConfigOverridePolicy {
    fn validate(&self, request: &ConnectorClientConfigRequest) -> Vec<ConfigValue> {
        request
            .client_props
            .iter()
            .map(|(name, value)| {
                let mut config_value = ConfigValue::new(name, Some(value.clone()));
                config_value.add_error_message(format!(
                    "The '{}' policy does not allow '{}' to be overridden in the connector configuration.",
                    self.inner.policy_name(),
                    name
                ));
                config_value
            })
            .collect()
    }

    fn configure(&mut self, _configs: HashMap<String, String>) {
        // log::info!("Setting up None Policy for ConnectorClientConfigOverride. This will disallow any client configuration to be overridden");
    }

    fn version(&self) -> String {
        "1.0.0".to_string()
    }

    fn close(&mut self) {
        // No cleanup needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let policy = NoneConnectorClientConfigOverridePolicy::new();
        assert_eq!(policy.inner.policy_name(), "None");
    }

    #[test]
    fn test_is_allowed() {
        let policy = NoneConnectorClientConfigOverridePolicy::new();
        let cv = ConfigValue::new("any.config", Some("value".to_string()));
        assert!(!policy.is_allowed(&cv));
    }

    #[test]
    fn test_validate() {
        let policy = NoneConnectorClientConfigOverridePolicy::new();
        let mut props = HashMap::new();
        props.insert("config1".to_string(), "value1".to_string());

        let request = ConnectorClientConfigRequest::new("test".to_string(), props);
        let results = policy.validate(&request);

        assert_eq!(results.len(), 1);
        for cv in results {
            assert!(!cv.error_messages().is_empty());
        }
    }
}
