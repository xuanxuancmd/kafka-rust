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

//! AllConnectorClientConfigOverridePolicy - allows all configurations to be overridden.
//!
//! Corresponds to `org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy` in Java.

use std::collections::HashMap;

use super::{
    AbstractConnectorClientConfigOverridePolicy, ConfigValue, ConnectorClientConfigOverridePolicy,
    ConnectorClientConfigRequest,
};

/// AllConnectorClientConfigOverridePolicy - allows all client configurations.
///
/// This policy allows all client configurations to be overridden via connector configs.
/// Set `connector.client.config.override.policy` to `All` to use this policy.
///
/// Corresponds to `org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy` in Java.
pub struct AllConnectorClientConfigOverridePolicy {
    inner: AbstractConnectorClientConfigOverridePolicy,
}

impl AllConnectorClientConfigOverridePolicy {
    /// Creates a new AllConnectorClientConfigOverridePolicy.
    pub fn new() -> Self {
        AllConnectorClientConfigOverridePolicy {
            inner: AbstractConnectorClientConfigOverridePolicy::new("All"),
        }
    }

    /// Always returns true (allows all configurations).
    pub fn is_allowed(&self, _config_value: &ConfigValue) -> bool {
        true
    }
}

impl Default for AllConnectorClientConfigOverridePolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorClientConfigOverridePolicy for AllConnectorClientConfigOverridePolicy {
    fn validate(&self, request: &ConnectorClientConfigRequest) -> Vec<ConfigValue> {
        request
            .client_props
            .iter()
            .map(|(name, value)| ConfigValue::new(name, Some(value.clone())))
            .collect()
    }

    fn configure(&mut self, _configs: HashMap<String, String>) {
        // log::info!("Setting up All Policy for ConnectorClientConfigOverride. This will allow all client configurations to be overridden");
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
        let policy = AllConnectorClientConfigOverridePolicy::new();
        assert_eq!(policy.inner.policy_name(), "All");
    }

    #[test]
    fn test_is_allowed() {
        let policy = AllConnectorClientConfigOverridePolicy::new();
        let cv = ConfigValue::new("any.config", Some("value".to_string()));
        assert!(policy.is_allowed(&cv));
    }

    #[test]
    fn test_validate() {
        let policy = AllConnectorClientConfigOverridePolicy::new();
        let mut props = HashMap::new();
        props.insert("config1".to_string(), "value1".to_string());
        props.insert("config2".to_string(), "value2".to_string());

        let request = ConnectorClientConfigRequest::new("test".to_string(), props);
        let results = policy.validate(&request);

        assert_eq!(results.len(), 2);
        for cv in results {
            assert!(cv.error_messages().is_empty());
        }
    }
}
