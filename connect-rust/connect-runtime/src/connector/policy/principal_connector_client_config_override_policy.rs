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

//! PrincipalConnectorClientConfigOverridePolicy - allows SASL configurations (deprecated).
//!
//! Corresponds to `org.apache.kafka.connect.connector.policy.PrincipalConnectorClientConfigOverridePolicy` in Java.
//!
//! **Deprecated**: Use AllowlistConnectorClientConfigOverridePolicy instead.

use std::collections::{HashMap, HashSet};

use super::{
    AbstractConnectorClientConfigOverridePolicy, ConfigValue, ConnectorClientConfigOverridePolicy,
    ConnectorClientConfigRequest,
};

/// Allowed SASL configurations for Principal policy.
/// Corresponds to Java's ALLOWED_CONFIG set.
pub const ALLOWED_SASL_CONFIGS: [&str; 3] =
    ["sasl.jaas.config", "sasl.mechanism", "security.protocol"];

/// PrincipalConnectorClientConfigOverridePolicy - allows SASL configurations.
///
/// This policy allows all `sasl` configurations to be overridden via connector configs.
/// Set `connector.client.config.override.policy` to `Principal` to use this policy.
/// This allows setting a principal per connector.
///
/// **Deprecated since 4.2, for removal**: Use AllowlistConnectorClientConfigOverridePolicy instead.
///
/// Corresponds to `org.apache.kafka.connect.connector.policy.PrincipalConnectorClientConfigOverridePolicy` in Java.
#[deprecated(
    since = "4.2.0",
    note = "Use AllowlistConnectorClientConfigOverridePolicy instead. To replicate Principal policy behavior, set connector.client.config.override.allowlist to 'sasl.jaas.config,sasl.mechanism,security.protocol'"
)]
pub struct PrincipalConnectorClientConfigOverridePolicy {
    inner: AbstractConnectorClientConfigOverridePolicy,
    allowed_configs: HashSet<String>,
}

#[allow(deprecated)]
impl PrincipalConnectorClientConfigOverridePolicy {
    /// Creates a new PrincipalConnectorClientConfigOverridePolicy.
    pub fn new() -> Self {
        PrincipalConnectorClientConfigOverridePolicy {
            inner: AbstractConnectorClientConfigOverridePolicy::new("Principal"),
            allowed_configs: ALLOWED_SASL_CONFIGS.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Checks if a config is in the allowed set.
    pub fn is_allowed(&self, config_value: &ConfigValue) -> bool {
        self.allowed_configs.contains(config_value.name())
    }

    /// Returns the allowed configurations.
    pub fn allowed_configs(&self) -> &HashSet<String> {
        &self.allowed_configs
    }
}

#[allow(deprecated)]
impl Default for PrincipalConnectorClientConfigOverridePolicy {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(deprecated)]
impl ConnectorClientConfigOverridePolicy for PrincipalConnectorClientConfigOverridePolicy {
    fn validate(&self, request: &ConnectorClientConfigRequest) -> Vec<ConfigValue> {
        request
            .client_props
            .iter()
            .map(|(name, value)| {
                let mut config_value = ConfigValue::new(name, Some(value.clone()));
                if !self.is_allowed(&config_value) {
                    config_value.add_error_message(format!(
                        "The '{}' policy does not allow '{}' to be overridden in the connector configuration.",
                        self.inner.policy_name(),
                        name
                    ));
                }
                config_value
            })
            .collect()
    }

    fn configure(&mut self, _configs: HashMap<String, String>) {
        // log::warn!("The Principal ConnectorClientConfigOverridePolicy is deprecated, use the Allowlist policy instead.");
        // log::info!("Setting up Principal policy for ConnectorClientConfigOverride. This will allow `sasl` client configuration to be overridden.");
    }

    fn version(&self) -> String {
        "1.0.0".to_string()
    }

    fn close(&mut self) {
        // No cleanup needed
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let policy = PrincipalConnectorClientConfigOverridePolicy::new();
        assert_eq!(policy.inner.policy_name(), "Principal");
        assert_eq!(policy.allowed_configs().len(), 3);
    }

    #[test]
    fn test_is_allowed() {
        let policy = PrincipalConnectorClientConfigOverridePolicy::new();

        let sasl_config = ConfigValue::new("sasl.jaas.config", Some("value".to_string()));
        assert!(policy.is_allowed(&sasl_config));

        let other_config = ConfigValue::new("other.config", Some("value".to_string()));
        assert!(!policy.is_allowed(&other_config));
    }

    #[test]
    fn test_validate() {
        let policy = PrincipalConnectorClientConfigOverridePolicy::new();
        let mut props = HashMap::new();
        props.insert("sasl.jaas.config".to_string(), "value".to_string());
        props.insert("not.allowed".to_string(), "value".to_string());

        let request = ConnectorClientConfigRequest::new("test".to_string(), props);
        let results = policy.validate(&request);

        assert_eq!(results.len(), 2);
        for cv in results {
            if cv.name() == "sasl.jaas.config" {
                assert!(cv.error_messages().is_empty());
            } else {
                assert!(!cv.error_messages().is_empty());
            }
        }
    }

    #[test]
    fn test_allowed_configs() {
        let policy = PrincipalConnectorClientConfigOverridePolicy::new();
        assert!(policy.allowed_configs().contains("sasl.jaas.config"));
        assert!(policy.allowed_configs().contains("sasl.mechanism"));
        assert!(policy.allowed_configs().contains("security.protocol"));
    }
}
