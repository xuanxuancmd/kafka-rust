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

//! AllowlistConnectorClientConfigOverridePolicy - allows only whitelisted configurations.
//!
//! Corresponds to `org.apache.kafka.connect.connector.policy.AllowlistConnectorClientConfigOverridePolicy` in Java.

use std::collections::{HashMap, HashSet};

use super::{
    AbstractConnectorClientConfigOverridePolicy, ConfigValue, ConnectorClientConfigOverridePolicy,
    ConnectorClientConfigRequest,
};

/// Allowlist configuration key.
pub const ALLOWLIST_CONFIG: &str = "connector.client.config.override.allowlist";

/// AllowlistConnectorClientConfigOverridePolicy - allows only whitelisted configs.
///
/// This policy allows only client configurations specified via
/// `connector.client.config.override.allowlist` to be overridden by connectors.
/// By default, the allowlist is empty, so connectors can't override any client configurations.
///
/// Corresponds to `org.apache.kafka.connect.connector.policy.AllowlistConnectorClientConfigOverridePolicy` in Java.
pub struct AllowlistConnectorClientConfigOverridePolicy {
    inner: AbstractConnectorClientConfigOverridePolicy,
    allowlist: HashSet<String>,
}

impl AllowlistConnectorClientConfigOverridePolicy {
    /// Creates a new AllowlistConnectorClientConfigOverridePolicy.
    pub fn new() -> Self {
        AllowlistConnectorClientConfigOverridePolicy {
            inner: AbstractConnectorClientConfigOverridePolicy::new("Allowlist"),
            allowlist: HashSet::new(),
        }
    }

    /// Creates with a predefined allowlist.
    pub fn with_allowlist(allowlist: Vec<String>) -> Self {
        AllowlistConnectorClientConfigOverridePolicy {
            inner: AbstractConnectorClientConfigOverridePolicy::new("Allowlist"),
            allowlist: allowlist.into_iter().collect(),
        }
    }

    /// Checks if a config is in the allowlist.
    pub fn is_allowed(&self, config_value: &ConfigValue) -> bool {
        self.allowlist.contains(config_value.name())
    }

    /// Returns the allowlist.
    pub fn allowlist(&self) -> &HashSet<String> {
        &self.allowlist
    }
}

impl Default for AllowlistConnectorClientConfigOverridePolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorClientConfigOverridePolicy for AllowlistConnectorClientConfigOverridePolicy {
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

    fn configure(&mut self, configs: HashMap<String, String>) {
        if let Some(allowlist_str) = configs.get(ALLOWLIST_CONFIG) {
            self.allowlist = allowlist_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        // log::info!("Setting up Allowlist policy for ConnectorClientConfigOverride. Allowed configs: {:?}", self.allowlist);
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
        let policy = AllowlistConnectorClientConfigOverridePolicy::new();
        assert!(policy.allowlist().is_empty());
    }

    #[test]
    fn test_with_allowlist() {
        let policy = AllowlistConnectorClientConfigOverridePolicy::with_allowlist(vec![
            "config1".to_string(),
            "config2".to_string(),
        ]);
        assert!(policy.allowlist().contains("config1"));
        assert!(policy.allowlist().contains("config2"));
        assert_eq!(policy.allowlist().len(), 2);
    }

    #[test]
    fn test_is_allowed() {
        let policy = AllowlistConnectorClientConfigOverridePolicy::with_allowlist(vec![
            "allowed.config".to_string(),
        ]);

        let allowed = ConfigValue::new("allowed.config", Some("value".to_string()));
        assert!(policy.is_allowed(&allowed));

        let not_allowed = ConfigValue::new("not.allowed", Some("value".to_string()));
        assert!(!policy.is_allowed(&not_allowed));
    }

    #[test]
    fn test_configure() {
        let mut policy = AllowlistConnectorClientConfigOverridePolicy::new();
        let mut configs = HashMap::new();
        configs.insert(ALLOWLIST_CONFIG.to_string(), "config1, config2".to_string());
        policy.configure(configs);

        assert!(policy.allowlist().contains("config1"));
        assert!(policy.allowlist().contains("config2"));
    }

    #[test]
    fn test_validate_with_allowlist() {
        let policy = AllowlistConnectorClientConfigOverridePolicy::with_allowlist(vec![
            "allowed".to_string()
        ]);

        let mut props = HashMap::new();
        props.insert("allowed".to_string(), "value1".to_string());
        props.insert("not.allowed".to_string(), "value2".to_string());

        let request = ConnectorClientConfigRequest::new("test".to_string(), props);
        let results = policy.validate(&request);

        assert_eq!(results.len(), 2);
        for cv in results {
            if cv.name() == "not.allowed" {
                assert!(!cv.error_messages().is_empty());
            } else {
                assert!(cv.error_messages().is_empty());
            }
        }
    }
}
