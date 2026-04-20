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

//! Worker configuration transformer for Kafka Connect.
//!
//! This module provides configuration transformation with variable substitution
//! from ConfigProvider instances. The transformation uses the pattern
//! `${provider:[path:]key}` to replace variables with values from providers.
//!
//! Corresponds to Java: `org.apache.kafka.connect.runtime.WorkerConfigTransformer`

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use common_trait::config::ConfigProvider;
use common_trait::herder::{Callback, ConfigReloadAction, Herder, HerderRequest};

use crate::config::connector_config::{
    CONFIG_RELOAD_ACTION_CONFIG, CONFIG_RELOAD_ACTION_NONE, CONFIG_RELOAD_ACTION_RESTART,
};

/// The result of a configuration transformation.
///
/// Contains the transformed configuration data and TTL values for paths
/// that indicate when the configuration should be refreshed.
///
/// Corresponds to Java: `org.apache.kafka.common.config.ConfigTransformerResult`
#[derive(Debug, Clone)]
pub struct ConfigTransformerResult {
    /// The transformed configuration data with variables replaced.
    data: HashMap<String, String>,
    /// TTL values (in milliseconds) for paths from ConfigProvider.
    ttls: HashMap<String, u64>,
}

impl ConfigTransformerResult {
    /// Creates a new ConfigTransformerResult with the given data and TTLs.
    pub fn new(data: HashMap<String, String>, ttls: HashMap<String, u64>) -> Self {
        ConfigTransformerResult { data, ttls }
    }

    /// Returns the transformed data with variables replaced.
    pub fn data(&self) -> &HashMap<String, String> {
        &self.data
    }

    /// Returns the TTL values for paths.
    pub fn ttls(&self) -> &HashMap<String, u64> {
        &self.ttls
    }
}

impl Default for ConfigTransformerResult {
    fn default() -> Self {
        ConfigTransformerResult {
            data: HashMap::new(),
            ttls: HashMap::new(),
        }
    }
}

/// Configuration transformer that performs variable substitution.
///
/// This class wraps a set of ConfigProvider instances and uses them to
/// perform transformations using the pattern `${provider:[path:]key}`.
///
/// The pattern extracts provider name, optional path, and key, then
/// passes them to ConfigProvider to obtain values for replacement.
///
/// Corresponds to Java: `org.apache.kafka.common.config.ConfigTransformer`
pub struct ConfigTransformer {
    /// Map of provider names to ConfigProvider instances.
    config_providers: HashMap<String, Arc<dyn ConfigProvider>>,
}

impl ConfigTransformer {
    /// Empty path constant for when path is not specified.
    const EMPTY_PATH: &'static str = "";

    /// Creates a new ConfigTransformer with the given providers.
    pub fn new(config_providers: HashMap<String, Arc<dyn ConfigProvider>>) -> Self {
        ConfigTransformer { config_providers }
    }

    /// Transforms the given configuration by replacing variables.
    ///
    /// # Arguments
    /// * `configs` - The configuration values to transform
    ///
    /// # Returns
    /// A ConfigTransformerResult with transformed data and TTL values.
    pub fn transform(&self, configs: &HashMap<String, String>) -> ConfigTransformerResult {
        // Collect variables that need transformation: provider -> path -> keys
        let mut keys_by_provider: HashMap<String, HashMap<String, HashSet<String>>> =
            HashMap::new();

        // Parse all config values to extract variables
        for (_config_key, config_value) in configs.iter() {
            let config_vars = Self::get_vars(config_value);
            for config_var in config_vars {
                let keys_by_path = keys_by_provider
                    .entry(config_var.provider_name.clone())
                    .or_insert_with(HashMap::new);
                let keys = keys_by_path
                    .entry(config_var.path.clone())
                    .or_insert_with(HashSet::new);
                keys.insert(config_var.variable.clone());
            }
        }

        // Retrieve values from ConfigProviders
        let mut ttls: HashMap<String, u64> = HashMap::new();
        let mut lookups_by_provider: HashMap<String, HashMap<String, HashMap<String, String>>> =
            HashMap::new();

        for (provider_name, keys_by_path) in keys_by_provider.iter() {
            let provider = self.config_providers.get(provider_name);
            if let Some(provider) = provider {
                for (path, keys) in keys_by_path.iter() {
                    let keys_set: HashSet<String> = keys.clone();
                    let config_data = provider.get_with_keys(path, keys_set);

                    // Record TTL if present
                    if let Some(ttl) = config_data.ttl() {
                        if ttl >= 0 {
                            ttls.insert(path.clone(), ttl);
                        }
                    }

                    // Store lookup results
                    let lookups_by_path = lookups_by_provider
                        .entry(provider_name.clone())
                        .or_insert_with(HashMap::new);
                    lookups_by_path.insert(path.clone(), config_data.data().clone());
                }
            }
        }

        // Perform variable replacement
        let mut data = configs.clone();
        for (config_key, config_value) in configs.iter() {
            let replaced = Self::replace(&lookups_by_provider, config_value);
            data.insert(config_key.clone(), replaced);
        }

        ConfigTransformerResult::new(data, ttls)
    }

    /// Extracts all variables from a configuration value.
    fn get_vars(value: &str) -> Vec<ConfigVariable> {
        let mut config_vars = Vec::new();
        let bytes = value.as_bytes();
        let mut i = 0;

        while i < bytes.len() {
            // Look for "${"
            if i + 1 < bytes.len() && bytes[i] == b'$' && bytes[i + 1] == b'{' {
                i += 2;

                // Find the closing "}"
                let mut end = i;
                while end < bytes.len() && bytes[end] != b'}' {
                    end += 1;
                }

                if end < bytes.len() {
                    // Extract content between ${ and }
                    if let Ok(content) = std::str::from_utf8(&bytes[i..end]) {
                        // Parse provider:[path:]key
                        if let Some(config_var) = Self::parse_variable(content) {
                            config_vars.push(config_var);
                        }
                    }
                    i = end + 1;
                }
            } else {
                i += 1;
            }
        }

        config_vars
    }

    /// Parses a variable content string into provider, path, and key.
    fn parse_variable(content: &str) -> Option<ConfigVariable> {
        // Format: provider:[path:]key
        // Examples: "file:key" -> provider=file, path="", key=key
        //           "file:/path:key" -> provider=file, path=/path, key=key

        let parts: Vec<&str> = content.split(':').collect();

        if parts.len() < 2 {
            return None;
        }

        let provider_name = parts[0].to_string();

        if parts.len() == 2 {
            // Format: provider:key (no path)
            Some(ConfigVariable {
                provider_name,
                path: Self::EMPTY_PATH.to_string(),
                variable: parts[1].to_string(),
            })
        } else if parts.len() >= 3 {
            // Format: provider:path:key
            // Path is everything between first and last colon
            let path = parts[1..parts.len() - 1].join(":");
            let variable = parts[parts.len() - 1].to_string();
            Some(ConfigVariable {
                provider_name,
                path,
                variable,
            })
        } else {
            None
        }
    }

    /// Replaces all variables in a value with their resolved values.
    fn replace(
        lookups_by_provider: &HashMap<String, HashMap<String, HashMap<String, String>>>,
        value: &str,
    ) -> String {
        let mut result = String::new();
        let bytes = value.as_bytes();
        let mut i = 0;

        while i < bytes.len() {
            // Look for "${"
            if i + 1 < bytes.len() && bytes[i] == b'$' && bytes[i + 1] == b'{' {
                let start = i;
                i += 2;

                // Find the closing "}"
                let mut end = i;
                while end < bytes.len() && bytes[end] != b'}' {
                    end += 1;
                }

                if end < bytes.len() {
                    // Extract content and parse variable
                    if let Ok(content) = std::str::from_utf8(&bytes[i..end]) {
                        if let Some(config_var) = Self::parse_variable(content) {
                            // Look up the replacement value
                            let replacement = lookups_by_provider
                                .get(&config_var.provider_name)
                                .and_then(|lookups_by_path| lookups_by_path.get(&config_var.path))
                                .and_then(|key_values| key_values.get(&config_var.variable));

                            // Add everything before ${
                            if let Ok(prefix) = std::str::from_utf8(&bytes[start..i - 2]) {
                                result.push_str(prefix);
                            }

                            if let Some(repl) = replacement {
                                result.push_str(repl);
                            } else {
                                // No replacement found, keep original
                                if let Ok(original) = std::str::from_utf8(&bytes[start..end + 1]) {
                                    result.push_str(original);
                                }
                            }

                            i = end + 1;
                        } else {
                            // Invalid variable format, keep as-is
                            if let Ok(original) = std::str::from_utf8(&bytes[start..end + 1]) {
                                result.push_str(original);
                            }
                            i = end + 1;
                        }
                    } else {
                        // UTF-8 error, skip
                        i = end + 1;
                    }
                } else {
                    // Unclosed variable, keep rest as-is
                    if let Ok(remaining) = std::str::from_utf8(&bytes[start..]) {
                        result.push_str(remaining);
                    }
                    break;
                }
            } else {
                // Add current character
                result.push(value[i..i + 1].chars().next().unwrap());
                i += 1;
            }
        }

        result
    }
}

/// Represents a parsed configuration variable.
#[derive(Debug, Clone)]
struct ConfigVariable {
    /// The provider name.
    provider_name: String,
    /// The path (may be empty).
    path: String,
    /// The variable/key name.
    variable: String,
}

/// A simple callback for restart operations.
struct RestartCallback;

impl Callback<()> for RestartCallback {
    fn on_completion(&self, _result: ()) {}
    fn on_error(&self, _error: String) {}
}

impl ConfigVariable {
    fn new(provider_name: String, path: String, variable: String) -> Self {
        ConfigVariable {
            provider_name,
            path,
            variable,
        }
    }
}

/// A wrapper class to perform configuration transformations and schedule reloads.
///
/// This class wraps a ConfigTransformer and schedules connector restarts
/// when ConfigProviders return TTL values indicating configuration will expire.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.WorkerConfigTransformer`
pub struct WorkerConfigTransformer {
    /// Reference to the herder for scheduling restarts.
    herder: Option<Arc<dyn Herder>>,
    /// The underlying ConfigTransformer.
    config_transformer: ConfigTransformer,
    /// Map of connector names to pending reload requests.
    /// Inner map is path -> HerderRequest.
    requests: Mutex<HashMap<String, HashMap<String, Box<dyn HerderRequest>>>>,
    /// The ConfigProvider instances.
    config_providers: HashMap<String, Arc<dyn ConfigProvider>>,
}

impl WorkerConfigTransformer {
    /// Creates a new WorkerConfigTransformer.
    ///
    /// # Arguments
    /// * `herder` - The herder for scheduling connector restarts
    /// * `config_providers` - Map of provider names to ConfigProvider instances
    pub fn new(
        herder: Option<Arc<dyn Herder>>,
        config_providers: HashMap<String, Arc<dyn ConfigProvider>>,
    ) -> Self {
        let config_transformer = ConfigTransformer::new(config_providers.clone());
        WorkerConfigTransformer {
            herder,
            config_transformer,
            requests: Mutex::new(HashMap::new()),
            config_providers,
        }
    }

    /// Transforms configuration without connector name (no reload scheduling).
    ///
    /// # Arguments
    /// * `configs` - The configuration to transform
    ///
    /// # Returns
    /// The transformed configuration.
    pub fn transform(&self, configs: &HashMap<String, String>) -> HashMap<String, String> {
        self.transform_with_connector(None, configs)
    }

    /// Transforms configuration with connector name and schedules reloads if needed.
    ///
    /// # Arguments
    /// * `connector_name` - Optional connector name for reload scheduling
    /// * `configs` - The configuration to transform
    ///
    /// # Returns
    /// The transformed configuration.
    pub fn transform_with_connector(
        &self,
        connector_name: Option<&str>,
        configs: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        if configs.is_empty() {
            return HashMap::new();
        }

        let result = self.config_transformer.transform(configs);

        // Schedule reloads if connector name is provided and TTLs exist
        if let Some(conn_name) = connector_name {
            // Get the reload action from config
            let action = configs
                .get(CONFIG_RELOAD_ACTION_CONFIG)
                .cloned()
                .unwrap_or_else(|| CONFIG_RELOAD_ACTION_RESTART.to_string());

            // Parse the reload action
            let reload_action = if action.to_lowercase() == CONFIG_RELOAD_ACTION_NONE {
                ConfigReloadAction::None
            } else {
                ConfigReloadAction::Restart
            };

            if reload_action == ConfigReloadAction::Restart && !result.ttls().is_empty() {
                self.schedule_reload(conn_name, result.ttls());
            }
        }

        result.data().clone()
    }

    /// Schedules reloads for all paths with TTL values.
    fn schedule_reload(&self, connector_name: &str, ttls: &HashMap<String, u64>) {
        for (path, ttl) in ttls.iter() {
            self.schedule_reload_for_path(connector_name, path, *ttl);
        }
    }

    /// Schedules a reload for a specific path and TTL.
    fn schedule_reload_for_path(&self, connector_name: &str, path: &str, ttl: u64) {
        let mut requests = self.requests.lock().unwrap();

        let connector_requests = requests
            .entry(connector_name.to_string())
            .or_insert_with(HashMap::new);

        // Cancel any previous request for this path
        if let Some(previous_request) = connector_requests.get(path) {
            previous_request.cancel();
        }

        // Schedule new restart if herder is available
        if let Some(herder) = &self.herder {
            log::info!(
                "Scheduling a restart of connector {} in {} ms",
                connector_name,
                ttl
            );

            let request =
                herder.restart_connector_delayed(ttl, connector_name, Box::new(RestartCallback));

            connector_requests.insert(path.to_string(), request);
        }
    }

    /// Closes the transformer and all ConfigProviders.
    pub fn close(&self) {
        // Cancel all pending requests
        let mut requests = self.requests.lock().unwrap();
        for (_, connector_requests) in requests.iter_mut() {
            for (_, request) in connector_requests.iter() {
                request.cancel();
            }
        }
        requests.clear();

        // ConfigProviders are Arc, so they will be cleaned up when dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_trait::Configurable;

    const MY_KEY: &str = "myKey";
    const MY_CONNECTOR: &str = "myConnector";
    const TEST_KEY: &str = "testKey";
    const TEST_PATH: &str = "testPath";
    const TEST_KEY_WITH_TTL: &str = "testKeyWithTTL";
    const TEST_KEY_WITH_LONGER_TTL: &str = "testKeyWithLongerTTL";
    const TEST_RESULT: &str = "testResult";
    const TEST_RESULT_WITH_TTL: &str = "testResultWithTTL";
    const TEST_RESULT_WITH_LONGER_TTL: &str = "testResultWithLongerTTL";

    /// Test ConfigProvider for unit tests.
    struct TestConfigProvider {
        data: HashMap<String, HashMap<String, (String, Option<u64>)>>,
    }

    impl TestConfigProvider {
        fn new() -> Self {
            let mut path_data = HashMap::new();

            // Setup test data
            let mut keys1 = HashMap::new();
            keys1.insert(TEST_KEY.to_string(), (TEST_RESULT.to_string(), None));
            keys1.insert(
                TEST_KEY_WITH_TTL.to_string(),
                (TEST_RESULT_WITH_TTL.to_string(), Some(1)),
            );
            keys1.insert(
                TEST_KEY_WITH_LONGER_TTL.to_string(),
                (TEST_RESULT_WITH_LONGER_TTL.to_string(), Some(10)),
            );
            path_data.insert(TEST_PATH.to_string(), keys1);

            TestConfigProvider { data: path_data }
        }
    }

    impl Configurable for TestConfigProvider {
        fn configure(&mut self, _configs: HashMap<String, serde_json::Value>) {}
    }

    impl ConfigProvider for TestConfigProvider {
        fn get(&self, path: &str) -> common_trait::config::ConfigData {
            self.get_with_keys(path, HashSet::new())
        }

        fn get_with_keys(
            &self,
            path: &str,
            keys: HashSet<String>,
        ) -> common_trait::config::ConfigData {
            if let Some(path_data) = self.data.get(path) {
                let mut result_data = HashMap::new();
                let mut ttl: Option<u64> = None;

                for key in keys {
                    if let Some((value, key_ttl)) = path_data.get(&key) {
                        result_data.insert(key, value.clone());
                        if let Some(t) = key_ttl {
                            ttl = Some(*t);
                        }
                    }
                }

                common_trait::config::ConfigData::new(result_data, ttl)
            } else {
                common_trait::config::ConfigData::default()
            }
        }
    }

    #[test]
    fn test_config_transformer_result_new() {
        let mut data = HashMap::new();
        data.insert("key1".to_string(), "value1".to_string());
        let mut ttls = HashMap::new();
        ttls.insert("path1".to_string(), 1000);

        let result = ConfigTransformerResult::new(data.clone(), ttls.clone());
        assert_eq!(result.data(), &data);
        assert_eq!(result.ttls(), &ttls);
    }

    #[test]
    fn test_config_transformer_result_default() {
        let result = ConfigTransformerResult::default();
        assert!(result.data().is_empty());
        assert!(result.ttls().is_empty());
    }

    #[test]
    fn test_replace_variable() {
        let provider: Arc<dyn ConfigProvider> = Arc::new(TestConfigProvider::new());
        let providers: HashMap<String, Arc<dyn ConfigProvider>> =
            [("test".to_string(), provider)].into_iter().collect();

        let transformer = ConfigTransformer::new(providers);

        let mut configs = HashMap::new();
        configs.insert(MY_KEY.to_string(), "${test:testPath:testKey}".to_string());

        let result = transformer.transform(&configs);

        assert_eq!(result.data().get(MY_KEY), Some(&TEST_RESULT.to_string()));
    }

    #[test]
    fn test_replace_variable_with_ttl() {
        let provider: Arc<dyn ConfigProvider> = Arc::new(TestConfigProvider::new());
        let providers: HashMap<String, Arc<dyn ConfigProvider>> =
            [("test".to_string(), provider)].into_iter().collect();

        let transformer = ConfigTransformer::new(providers);

        let mut configs = HashMap::new();
        configs.insert(
            MY_KEY.to_string(),
            "${test:testPath:testKeyWithTTL}".to_string(),
        );

        let result = transformer.transform(&configs);

        assert_eq!(
            result.data().get(MY_KEY),
            Some(&TEST_RESULT_WITH_TTL.to_string())
        );
        assert_eq!(result.ttls().get(TEST_PATH), Some(&1u64));
    }

    #[test]
    fn test_transform_empty_configuration() {
        let providers: HashMap<String, Arc<dyn ConfigProvider>> = HashMap::new();
        let transformer = ConfigTransformer::new(providers);

        let configs = HashMap::new();
        let result = transformer.transform(&configs);

        assert!(result.data().is_empty());
    }

    #[test]
    fn test_worker_config_transformer_transform() {
        let provider: Arc<dyn ConfigProvider> = Arc::new(TestConfigProvider::new());
        let providers: HashMap<String, Arc<dyn ConfigProvider>> =
            [("test".to_string(), provider)].into_iter().collect();

        let worker_transformer = WorkerConfigTransformer::new(None, providers);

        let mut configs = HashMap::new();
        configs.insert(MY_KEY.to_string(), "${test:testPath:testKey}".to_string());

        let result = worker_transformer.transform_with_connector(Some(MY_CONNECTOR), &configs);

        assert_eq!(result.get(MY_KEY), Some(&TEST_RESULT.to_string()));
    }

    #[test]
    fn test_worker_config_transformer_with_reload_none() {
        let provider: Arc<dyn ConfigProvider> = Arc::new(TestConfigProvider::new());
        let providers: HashMap<String, Arc<dyn ConfigProvider>> =
            [("test".to_string(), provider)].into_iter().collect();

        let worker_transformer = WorkerConfigTransformer::new(None, providers);

        let mut configs = HashMap::new();
        configs.insert(
            MY_KEY.to_string(),
            "${test:testPath:testKeyWithTTL}".to_string(),
        );
        configs.insert(
            CONFIG_RELOAD_ACTION_CONFIG.to_string(),
            CONFIG_RELOAD_ACTION_NONE.to_string(),
        );

        let result = worker_transformer.transform_with_connector(Some(MY_CONNECTOR), &configs);

        // Should transform but not schedule restart (we can't verify scheduling without herder)
        assert_eq!(result.get(MY_KEY), Some(&TEST_RESULT_WITH_TTL.to_string()));
    }

    #[test]
    fn test_parse_variable_simple() {
        let var = ConfigTransformer::parse_variable("provider:key");
        assert!(var.is_some());
        let v = var.unwrap();
        assert_eq!(v.provider_name, "provider");
        assert_eq!(v.path, "");
        assert_eq!(v.variable, "key");
    }

    #[test]
    fn test_parse_variable_with_path() {
        let var = ConfigTransformer::parse_variable("provider:/path/to:key");
        assert!(var.is_some());
        let v = var.unwrap();
        assert_eq!(v.provider_name, "provider");
        assert_eq!(v.path, "/path/to");
        assert_eq!(v.variable, "key");
    }

    #[test]
    fn test_parse_variable_multiple_colons() {
        let var = ConfigTransformer::parse_variable("provider:path:subpath:key");
        assert!(var.is_some());
        let v = var.unwrap();
        assert_eq!(v.provider_name, "provider");
        assert_eq!(v.path, "path:subpath");
        assert_eq!(v.variable, "key");
    }

    #[test]
    fn test_parse_variable_invalid() {
        // Missing key
        let var = ConfigTransformer::parse_variable("provider");
        assert!(var.is_none());
    }

    #[test]
    fn test_get_vars_multiple() {
        let vars = ConfigTransformer::get_vars("${test:path:key1} and ${test:path:key2}");
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].provider_name, "test");
        assert_eq!(vars[0].path, "path");
        assert_eq!(vars[0].variable, "key1");
        assert_eq!(vars[1].provider_name, "test");
        assert_eq!(vars[1].path, "path");
        assert_eq!(vars[1].variable, "key2");
    }

    #[test]
    fn test_replace_multiple_variables() {
        let provider: Arc<dyn ConfigProvider> = Arc::new(TestConfigProvider::new());
        let providers: HashMap<String, Arc<dyn ConfigProvider>> =
            [("test".to_string(), provider)].into_iter().collect();

        let transformer = ConfigTransformer::new(providers);

        let mut configs = HashMap::new();
        configs.insert(
            "multi".to_string(),
            "${test:testPath:testKey} and ${test:testPath:testKeyWithTTL}".to_string(),
        );

        let result = transformer.transform(&configs);

        assert_eq!(
            result.data().get("multi"),
            Some(&"testResult and testResultWithTTL".to_string())
        );
    }
}
