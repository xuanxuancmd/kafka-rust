// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};

use crate::Configurable;

/// Configuration data from a ConfigProvider.
///
/// This corresponds to `org.apache.kafka.common.config.ConfigData` in Java.
#[derive(Debug, Clone)]
pub struct ConfigData {
    /// The configuration key-value pairs
    data: HashMap<String, String>,
    /// The time-to-live of the data in milliseconds, or None if there is no TTL
    ttl: Option<u64>,
}

impl ConfigData {
    /// Creates a new ConfigData with the given data and TTL (in milliseconds).
    ///
    /// # Arguments
    /// * `data` - A map of key-value pairs
    /// * `ttl` - The time-to-live of the data in milliseconds, or None if there is no TTL
    pub fn new(data: HashMap<String, String>, ttl: Option<u64>) -> Self {
        ConfigData { data, ttl }
    }

    /// Creates a new ConfigData with the given data and no TTL.
    ///
    /// # Arguments
    /// * `data` - A map of key-value pairs
    pub fn with_data(data: HashMap<String, String>) -> Self {
        ConfigData { data, ttl: None }
    }

    /// Returns the configuration data as a map of key-value pairs.
    pub fn data(&self) -> &HashMap<String, String> {
        &self.data
    }

    /// Returns the TTL (in milliseconds), or None if there is no TTL.
    pub fn ttl(&self) -> Option<u64> {
        self.ttl
    }
}

impl Default for ConfigData {
    fn default() -> Self {
        ConfigData {
            data: HashMap::new(),
            ttl: None,
        }
    }
}

/// A callback passed to ConfigProvider for subscribing to changes.
///
/// This corresponds to `org.apache.kafka.common.config.ConfigChangeCallback` in Java.
pub trait ConfigChangeCallback: Send + Sync {
    /// Called when configuration data changes.
    ///
    /// # Arguments
    /// * `path` - The path at which the data resides
    /// * `data` - The configuration data
    fn on_change(&self, path: &str, data: ConfigData);
}

/// A provider of configuration data, which may optionally support subscriptions to configuration changes.
///
/// Implementations are required to safely support concurrent calls to any of the methods in this trait.
///
/// This corresponds to `org.apache.kafka.common.config.provider.ConfigProvider` in Java.
pub trait ConfigProvider: Configurable + Send + Sync {
    /// Retrieves the data at the given path.
    ///
    /// # Arguments
    /// * `path` - The path where the data resides
    ///
    /// # Returns
    /// The configuration data
    fn get(&self, path: &str) -> ConfigData;

    /// Retrieves the data with the given keys at the given path.
    ///
    /// # Arguments
    /// * `path` - The path where the data resides
    /// * `keys` - The keys whose values will be retrieved
    ///
    /// # Returns
    /// The configuration data
    fn get_with_keys(&self, path: &str, keys: HashSet<String>) -> ConfigData;

    /// Subscribes to changes for the given keys at the given path (optional operation).
    ///
    /// # Arguments
    /// * `path` - The path where the data resides
    /// * `keys` - The keys whose values will be retrieved
    /// * `callback` - The callback to invoke upon change
    ///
    /// # Errors
    /// Returns an error if the subscribe operation is not supported.
    fn subscribe(
        &self,
        path: &str,
        keys: HashSet<String>,
        callback: Box<dyn ConfigChangeCallback>,
    ) -> Result<(), String> {
        let _ = (path, keys, callback);
        Err("subscribe operation is not supported".to_string())
    }

    /// Unsubscribes to changes for the given keys at the given path (optional operation).
    ///
    /// # Arguments
    /// * `path` - The path where the data resides
    /// * `keys` - The keys whose values will be retrieved
    /// * `callback` - The callback to be unsubscribed from changes
    ///
    /// # Errors
    /// Returns an error if the unsubscribe operation is not supported.
    fn unsubscribe(
        &self,
        path: &str,
        keys: HashSet<String>,
        callback: &dyn ConfigChangeCallback,
    ) -> Result<(), String> {
        let _ = (path, keys, callback);
        Err("unsubscribe operation is not supported".to_string())
    }

    /// Clears all subscribers (optional operation).
    ///
    /// # Errors
    /// Returns an error if the unsubscribe_all operation is not supported.
    fn unsubscribe_all(&self) -> Result<(), String> {
        Err("unsubscribe_all operation is not supported".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_data_new() {
        let mut data = HashMap::new();
        data.insert("key1".to_string(), "value1".to_string());
        let config_data = ConfigData::new(data.clone(), Some(1000));
        assert_eq!(config_data.data(), &data);
        assert_eq!(config_data.ttl(), Some(1000));
    }

    #[test]
    fn test_config_data_with_data() {
        let mut data = HashMap::new();
        data.insert("key1".to_string(), "value1".to_string());
        let config_data = ConfigData::with_data(data.clone());
        assert_eq!(config_data.data(), &data);
        assert_eq!(config_data.ttl(), None);
    }

    #[test]
    fn test_config_data_default() {
        let config_data = ConfigData::default();
        assert!(config_data.data().is_empty());
        assert_eq!(config_data.ttl(), None);
    }

    struct TestConfigChangeCallback;

    impl ConfigChangeCallback for TestConfigChangeCallback {
        fn on_change(&self, _path: &str, _data: ConfigData) {}
    }

    struct TestConfigProvider {
        configured: bool,
    }

    impl Configurable for TestConfigProvider {
        fn configure(&mut self, _configs: std::collections::HashMap<String, serde_json::Value>) {
            self.configured = true;
        }
    }

    impl ConfigProvider for TestConfigProvider {
        fn get(&self, path: &str) -> ConfigData {
            let mut data = HashMap::new();
            data.insert("path".to_string(), path.to_string());
            ConfigData::with_data(data)
        }

        fn get_with_keys(&self, path: &str, keys: HashSet<String>) -> ConfigData {
            let mut data = HashMap::new();
            data.insert("path".to_string(), path.to_string());
            for key in keys {
                data.insert(key.clone(), format!("value_for_{}", key));
            }
            ConfigData::with_data(data)
        }
    }

    #[test]
    fn test_config_provider_get() {
        let provider = TestConfigProvider { configured: false };
        let config_data = provider.get("/test/path");
        assert_eq!(
            config_data.data().get("path"),
            Some(&"/test/path".to_string())
        );
    }

    #[test]
    fn test_config_provider_get_with_keys() {
        let provider = TestConfigProvider { configured: false };
        let mut keys = HashSet::new();
        keys.insert("key1".to_string());
        let config_data = provider.get_with_keys("/test/path", keys);
        assert_eq!(
            config_data.data().get("path"),
            Some(&"/test/path".to_string())
        );
        assert_eq!(
            config_data.data().get("key1"),
            Some(&"value_for_key1".to_string())
        );
    }

    #[test]
    fn test_config_provider_subscribe_unsupported() {
        let provider = TestConfigProvider { configured: false };
        let keys = HashSet::new();
        let callback = Box::new(TestConfigChangeCallback);
        let result = provider.subscribe("/test/path", keys, callback);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "subscribe operation is not supported");
    }

    #[test]
    fn test_config_provider_unsubscribe_unsupported() {
        let provider = TestConfigProvider { configured: false };
        let keys = HashSet::new();
        let callback = TestConfigChangeCallback;
        let result = provider.unsubscribe("/test/path", keys, &callback);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "unsubscribe operation is not supported"
        );
    }

    #[test]
    fn test_config_provider_unsubscribe_all_unsupported() {
        let provider = TestConfigProvider { configured: false };
        let result = provider.unsubscribe_all();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "unsubscribe_all operation is not supported"
        );
    }
}
