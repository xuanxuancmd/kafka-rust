/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Worker configuration for Kafka Connect.
//!
//! This module provides the base WorkerConfig class that defines common
//! configuration options for both standalone and distributed Kafka Connect workers.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.WorkerConfig

use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType};
use common_trait::errors::ConfigException;
use serde_json::Value;
use std::collections::HashMap;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java WorkerConfig
// ============================================================================

/// Configuration key for bootstrap servers.
/// Corresponds to Java: WorkerConfig.BOOTSTRAP_SERVERS_CONFIG
pub const BOOTSTRAP_SERVERS_CONFIG: &str = "bootstrap.servers";

/// Configuration key for key converter class.
/// Corresponds to Java: WorkerConfig.KEY_CONVERTER_CLASS_CONFIG
pub const KEY_CONVERTER_CLASS_CONFIG: &str = "key.converter";

/// Configuration key for value converter class.
/// Corresponds to Java: WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG
pub const VALUE_CONVERTER_CLASS_CONFIG: &str = "value.converter";

/// Configuration key for header converter class.
/// Corresponds to Java: WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG
pub const HEADER_CONVERTER_CLASS_CONFIG: &str = "header.converter";

/// Default value for header converter class.
/// Corresponds to Java: WorkerConfig.HEADER_CONVERTER_CLASS_DEFAULT
pub const HEADER_CONVERTER_CLASS_DEFAULT: &str =
    "org.apache.kafka.connect.storage.SimpleHeaderConverter";

/// Configuration key for task shutdown graceful timeout in milliseconds.
/// Corresponds to Java: WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG
pub const TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG: &str = "task.shutdown.graceful.timeout.ms";

/// Default value for task shutdown graceful timeout in milliseconds.
/// Corresponds to Java: WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT
pub const TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT: i64 = 5000;

/// Configuration key for offset flush interval in milliseconds.
/// Corresponds to Java: WorkerConfig.OFFSET_FLUSH_INTERVAL_MS_CONFIG
pub const OFFSET_FLUSH_INTERVAL_MS_CONFIG: &str = "offset.flush.interval.ms";

/// Default value for offset flush interval in milliseconds.
/// Corresponds to Java: WorkerConfig.OFFSET_FLUSH_INTERVAL_MS_DEFAULT
pub const OFFSET_FLUSH_INTERVAL_MS_DEFAULT: i64 = 60000;

/// Configuration key for offset flush timeout in milliseconds.
/// Corresponds to Java: WorkerConfig.OFFSET_FLUSH_TIMEOUT_MS_CONFIG
pub const OFFSET_FLUSH_TIMEOUT_MS_CONFIG: &str = "offset.flush.timeout.ms";

/// Default value for offset flush timeout in milliseconds.
/// Corresponds to Java: WorkerConfig.OFFSET_FLUSH_TIMEOUT_MS_DEFAULT
pub const OFFSET_FLUSH_TIMEOUT_MS_DEFAULT: i64 = 5000;

/// Configuration key for plugin path.
/// Corresponds to Java: WorkerConfig.PLUGIN_PATH_CONFIG
pub const PLUGIN_PATH_CONFIG: &str = "plugin.path";

/// Configuration key for plugin discovery mode.
/// Corresponds to Java: WorkerConfig.PLUGIN_DISCOVERY_CONFIG
pub const PLUGIN_DISCOVERY_CONFIG: &str = "plugin.discovery";

/// Default value for plugin discovery mode.
/// Corresponds to Java: WorkerConfig.PLUGIN_DISCOVERY_DEFAULT
pub const PLUGIN_DISCOVERY_DEFAULT: &str = "HYBRID_WARN";

/// Configuration key for topic tracking enable.
/// Corresponds to Java: WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG
pub const TOPIC_TRACKING_ENABLE_CONFIG: &str = "topic.tracking.enable";

/// Default value for topic tracking enable.
/// Corresponds to Java: WorkerConfig.TOPIC_TRACKING_ENABLE_DEFAULT
pub const TOPIC_TRACKING_ENABLE_DEFAULT: bool = true;

/// Configuration key for topic tracking allow reset.
/// Corresponds to Java: WorkerConfig.TOPIC_TRACKING_ALLOW_RESET_CONFIG
pub const TOPIC_TRACKING_ALLOW_RESET_CONFIG: &str = "topic.tracking.allow.reset";

/// Default value for topic tracking allow reset.
/// Corresponds to Java: WorkerConfig.TOPIC_TRACKING_ALLOW_RESET_DEFAULT
pub const TOPIC_TRACKING_ALLOW_RESET_DEFAULT: bool = true;

/// Configuration key for topic creation enable.
/// Corresponds to Java: WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG
pub const TOPIC_CREATION_ENABLE_CONFIG: &str = "topic.creation.enable";

/// Default value for topic creation enable.
/// Corresponds to Java: WorkerConfig.TOPIC_CREATION_ENABLE_DEFAULT
pub const TOPIC_CREATION_ENABLE_DEFAULT: bool = true;

/// Configuration key for config providers.
/// Corresponds to Java: WorkerConfig.CONFIG_PROVIDERS_CONFIG
pub const CONFIG_PROVIDERS_CONFIG: &str = "config.providers";

/// Configuration key for connector client config override policy.
/// Corresponds to Java: WorkerConfig.CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY_CONFIG
pub const CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY_CONFIG: &str =
    "connector.client.config.override.policy";

/// Default value for connector client config override policy.
/// Corresponds to Java: WorkerConfig.CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY_DEFAULT
pub const CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY_DEFAULT: &str = "All";

/// Configuration key for client DNS lookup.
/// Corresponds to Java: WorkerConfig.CLIENT_DNS_LOOKUP_CONFIG
pub const CLIENT_DNS_LOOKUP_CONFIG: &str = "client.dns.lookup";

/// Default value for client DNS lookup.
/// Corresponds to Java: WorkerConfig.CLIENT_DNS_LOOKUP_DEFAULT
pub const CLIENT_DNS_LOOKUP_DEFAULT: &str = "use_all_dns_ips";

/// Configuration key for metrics sample window in milliseconds.
/// Corresponds to Java: WorkerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG
pub const METRICS_SAMPLE_WINDOW_MS_CONFIG: &str = "metrics.sample.window.ms";

/// Default value for metrics sample window in milliseconds.
/// Corresponds to Java: WorkerConfig.METRICS_SAMPLE_WINDOW_MS_DEFAULT
pub const METRICS_SAMPLE_WINDOW_MS_DEFAULT: i64 = 30000;

/// Configuration key for metrics num samples.
/// Corresponds to Java: WorkerConfig.METRICS_NUM_SAMPLES_CONFIG
pub const METRICS_NUM_SAMPLES_CONFIG: &str = "metrics.num.samples";

/// Default value for metrics num samples.
/// Corresponds to Java: WorkerConfig.METRICS_NUM_SAMPLES_DEFAULT
pub const METRICS_NUM_SAMPLES_DEFAULT: i64 = 2;

/// Configuration key for metrics recording level.
/// Corresponds to Java: WorkerConfig.METRICS_RECORDING_LEVEL_CONFIG
pub const METRICS_RECORDING_LEVEL_CONFIG: &str = "metrics.recording.level";

/// Default value for metrics recording level.
/// Corresponds to Java: WorkerConfig.METRICS_RECORDING_LEVEL_DEFAULT
pub const METRICS_RECORDING_LEVEL_DEFAULT: &str = "INFO";

/// Configuration key for metric reporters.
/// Corresponds to Java: WorkerConfig.METRIC_REPORTER_CLASSES_CONFIG
pub const METRIC_REPORTER_CLASSES_CONFIG: &str = "metric.reporters";

/// Configuration key for response HTTP headers.
/// Corresponds to Java: WorkerConfig.REST_RESPONSE_HTTP_HEADERS_CONFIG
pub const REST_RESPONSE_HTTP_HEADERS_CONFIG: &str = "rest.response.http.headers";

/// Configuration key for admin extensions.
/// Corresponds to Java: WorkerConfig.ADMIN_EXTENSIONS_CONFIG
pub const ADMIN_EXTENSIONS_CONFIG: &str = "admin.extensions";

// ============================================================================
// WorkerConfig
// ============================================================================

/// Base configuration class for Kafka Connect workers.
///
/// This class defines common configuration options for both standalone and
/// distributed Kafka Connect workers. It provides the foundation for all
/// worker-specific configuration classes.
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.WorkerConfig
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    originals: HashMap<String, Value>,
}

impl WorkerConfig {
    /// Creates a new WorkerConfig with the given configuration properties.
    ///
    /// Corresponds to Java: WorkerConfig(Map<?, ?> props)
    pub fn new(props: HashMap<String, Value>) -> Result<Self, ConfigException> {
        let config = WorkerConfig { originals: props };
        config.validate()?;
        Ok(config)
    }

    /// Creates a WorkerConfig without validation (for internal use).
    pub fn new_unvalidated(props: HashMap<String, Value>) -> Self {
        WorkerConfig { originals: props }
    }

    /// Returns the base configuration definition.
    ///
    /// This method defines the common configuration options for all workers.
    /// Corresponds to Java: WorkerConfig.baseConfigDef()
    pub fn base_config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        ConfigDefBuilder::new()
            // Bootstrap servers - required for connecting to Kafka cluster
            .define(
                BOOTSTRAP_SERVERS_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.",
            )
            // Key converter - required for converting message keys
            .define(
                KEY_CONVERTER_CLASS_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka. This controls the format of the keys in Kafka messages.",
            )
            // Value converter - required for converting message values
            .define(
                VALUE_CONVERTER_CLASS_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka. This controls the format of the values in Kafka messages.",
            )
            // Header converter - has default value
            .define(
                HEADER_CONVERTER_CLASS_CONFIG,
                ConfigDefType::String,
                Some(Value::String(HEADER_CONVERTER_CLASS_DEFAULT.to_string())),
                ConfigDefImportance::Low,
                "Header converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka. This controls the format of the headers in Kafka messages.",
            )
            // Task shutdown graceful timeout
            .define(
                TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                "The time to wait for tasks to shutdown gracefully. This is the maximum total time to wait for each individual task to shutdown.",
            )
            // Offset flush interval
            .define(
                OFFSET_FLUSH_INTERVAL_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(OFFSET_FLUSH_INTERVAL_MS_DEFAULT.into())),
                ConfigDefImportance::High,
                "The interval at which to try committing offsets for tasks.",
            )
            // Offset flush timeout
            .define(
                OFFSET_FLUSH_TIMEOUT_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(OFFSET_FLUSH_TIMEOUT_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                "Maximum time to wait for records to flush and commit offsets before cancelling the process.",
            )
            // Plugin path
            .define(
                PLUGIN_PATH_CONFIG,
                ConfigDefType::List,
                None,
                ConfigDefImportance::Medium,
                "List of paths for Connect plugins (connectors, converters, transformations).",
            )
            // Plugin discovery
            .define(
                PLUGIN_DISCOVERY_CONFIG,
                ConfigDefType::String,
                Some(Value::String(PLUGIN_DISCOVERY_DEFAULT.to_string())),
                ConfigDefImportance::Low,
                "Defines the method used for plugin discovery. Valid values: ONLY_SCAN, HYBRID_WARN, HYBRID_FAIL, SERVICE_LOAD.",
            )
            // Topic tracking enable
            .define(
                TOPIC_TRACKING_ENABLE_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(TOPIC_TRACKING_ENABLE_DEFAULT)),
                ConfigDefImportance::Low,
                "Enable tracking of active topics per connector during runtime.",
            )
            // Topic tracking allow reset
            .define(
                TOPIC_TRACKING_ALLOW_RESET_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(TOPIC_TRACKING_ALLOW_RESET_DEFAULT)),
                ConfigDefImportance::Low,
                "Allow user requests to reset the set of active topics per connector.",
            )
            // Topic creation enable
            .define(
                TOPIC_CREATION_ENABLE_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(TOPIC_CREATION_ENABLE_DEFAULT)),
                ConfigDefImportance::Medium,
                "Whether to allow automatic creation of topics used by source connectors.",
            )
            // Config providers
            .define(
                CONFIG_PROVIDERS_CONFIG,
                ConfigDefType::List,
                Some(Value::Array(vec![])),
                ConfigDefImportance::Low,
                "List of configuration provider classes.",
            )
            // Connector client config override policy
            .define(
                CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY_CONFIG,
                ConfigDefType::String,
                Some(Value::String(CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY_DEFAULT.to_string())),
                ConfigDefImportance::Low,
                "Class name of the connector client config override policy implementation.",
            )
            // Client DNS lookup
            .define(
                CLIENT_DNS_LOOKUP_CONFIG,
                ConfigDefType::String,
                Some(Value::String(CLIENT_DNS_LOOKUP_DEFAULT.to_string())),
                ConfigDefImportance::Low,
                "Controls how the client performs DNS lookups.",
            )
            // Metrics sample window
            .define(
                METRICS_SAMPLE_WINDOW_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(METRICS_SAMPLE_WINDOW_MS_DEFAULT.into())),
                ConfigDefImportance::Low,
                "The window of time a metrics sample is computed over.",
            )
            // Metrics num samples
            .define(
                METRICS_NUM_SAMPLES_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(METRICS_NUM_SAMPLES_DEFAULT.into())),
                ConfigDefImportance::Low,
                "The number of samples maintained to compute metrics.",
            )
            // Metrics recording level
            .define(
                METRICS_RECORDING_LEVEL_CONFIG,
                ConfigDefType::String,
                Some(Value::String(METRICS_RECORDING_LEVEL_DEFAULT.to_string())),
                ConfigDefImportance::Low,
                "The recording level for metrics.",
            )
            // Metric reporters
            .define(
                METRIC_REPORTER_CLASSES_CONFIG,
                ConfigDefType::List,
                Some(Value::Array(vec![])),
                ConfigDefImportance::Low,
                "A list of classes to use as metrics reporters.",
            )
            // REST response HTTP headers
            .define(
                REST_RESPONSE_HTTP_HEADERS_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::Low,
                "Additional HTTP headers to be included in REST responses.",
            )
            // Admin extensions
            .define(
                ADMIN_EXTENSIONS_CONFIG,
                ConfigDefType::List,
                Some(Value::Array(vec![])),
                ConfigDefImportance::Low,
                "List of admin extension classes.",
            )
            .build()
    }

    /// Validates the configuration values.
    pub fn validate(&self) -> Result<(), ConfigException> {
        let config_def = Self::base_config_def();

        for (name, key) in &config_def {
            let value = self.originals.get(name);

            // Check required configurations
            if value.is_none() && key.default_value().is_none() {
                // Skip validation for optional configs without defaults
                if key.importance() != ConfigDefImportance::High {
                    continue;
                }
                // Required configuration is missing
                if name == BOOTSTRAP_SERVERS_CONFIG
                    || name == KEY_CONVERTER_CLASS_CONFIG
                    || name == VALUE_CONVERTER_CLASS_CONFIG
                {
                    return Err(ConfigException::new(format!(
                        "Required configuration {} is missing",
                        name
                    )));
                }
            }

            // Validate the value if present
            if let Some(v) = value {
                key.validate(v)?;
            }
        }

        Ok(())
    }

    /// Returns the bootstrap servers configuration value.
    ///
    /// Corresponds to Java: WorkerConfig.bootstrapServers()
    pub fn bootstrap_servers(&self) -> Option<&str> {
        self.get_string(BOOTSTRAP_SERVERS_CONFIG)
    }

    /// Returns the key converter class configuration value.
    ///
    /// Corresponds to Java: WorkerConfig.keyConverter()
    pub fn key_converter_class(&self) -> Option<&str> {
        self.get_string(KEY_CONVERTER_CLASS_CONFIG)
    }

    /// Returns the value converter class configuration value.
    ///
    /// Corresponds to Java: WorkerConfig.valueConverter()
    pub fn value_converter_class(&self) -> Option<&str> {
        self.get_string(VALUE_CONVERTER_CLASS_CONFIG)
    }

    /// Returns the header converter class configuration value.
    ///
    /// Corresponds to Java: WorkerConfig.headerConverter()
    pub fn header_converter_class(&self) -> &str {
        self.get_string(HEADER_CONVERTER_CLASS_CONFIG)
            .unwrap_or(HEADER_CONVERTER_CLASS_DEFAULT)
    }

    /// Returns the task shutdown graceful timeout in milliseconds.
    ///
    /// Corresponds to Java: WorkerConfig.taskShutdownGracefulTimeoutMs()
    pub fn task_shutdown_graceful_timeout_ms(&self) -> i64 {
        self.get_long(TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG)
            .unwrap_or(TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT)
    }

    /// Returns the offset flush interval in milliseconds.
    ///
    /// Corresponds to Java: WorkerConfig.offsetCommitInterval()
    pub fn offset_commit_interval_ms(&self) -> i64 {
        self.get_long(OFFSET_FLUSH_INTERVAL_MS_CONFIG)
            .unwrap_or(OFFSET_FLUSH_INTERVAL_MS_DEFAULT)
    }

    /// Returns the offset flush timeout in milliseconds.
    ///
    /// Corresponds to Java: WorkerConfig.offsetFlushTimeoutMs()
    pub fn offset_flush_timeout_ms(&self) -> i64 {
        self.get_long(OFFSET_FLUSH_TIMEOUT_MS_CONFIG)
            .unwrap_or(OFFSET_FLUSH_TIMEOUT_MS_DEFAULT)
    }

    /// Returns the plugin path configuration value.
    ///
    /// Corresponds to Java: WorkerConfig.pluginPath()
    pub fn plugin_path(&self) -> Option<Vec<String>> {
        self.get_list(PLUGIN_PATH_CONFIG)
    }

    /// Returns the plugin discovery mode.
    ///
    /// Corresponds to Java: WorkerConfig.pluginDiscovery()
    pub fn plugin_discovery(&self) -> &str {
        self.get_string(PLUGIN_DISCOVERY_CONFIG)
            .unwrap_or(PLUGIN_DISCOVERY_DEFAULT)
    }

    /// Returns whether topic tracking is enabled.
    ///
    /// Corresponds to Java: WorkerConfig.topicTrackingEnable()
    pub fn topic_tracking_enable(&self) -> bool {
        self.get_bool(TOPIC_TRACKING_ENABLE_CONFIG)
            .unwrap_or(TOPIC_TRACKING_ENABLE_DEFAULT)
    }

    /// Returns whether topic tracking allows reset.
    ///
    /// Corresponds to Java: WorkerConfig.topicTrackingAllowReset()
    pub fn topic_tracking_allow_reset(&self) -> bool {
        self.get_bool(TOPIC_TRACKING_ALLOW_RESET_CONFIG)
            .unwrap_or(TOPIC_TRACKING_ALLOW_RESET_DEFAULT)
    }

    /// Returns whether topic creation is enabled.
    ///
    /// Corresponds to Java: WorkerConfig.topicCreationEnable()
    pub fn topic_creation_enable(&self) -> bool {
        self.get_bool(TOPIC_CREATION_ENABLE_CONFIG)
            .unwrap_or(TOPIC_CREATION_ENABLE_DEFAULT)
    }

    /// Returns the config providers list.
    ///
    /// Corresponds to Java: WorkerConfig.configProviders()
    pub fn config_providers(&self) -> Vec<String> {
        self.get_list(CONFIG_PROVIDERS_CONFIG).unwrap_or_default()
    }

    /// Returns the connector client config override policy.
    ///
    /// Corresponds to Java: WorkerConfig.connectorClientConfigOverridePolicy()
    pub fn connector_client_config_override_policy(&self) -> &str {
        self.get_string(CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY_CONFIG)
            .unwrap_or(CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY_DEFAULT)
    }

    /// Returns the raw configuration value for the given key.
    ///
    /// Corresponds to Java: AbstractConfig.get(String key)
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.originals.get(key)
    }

    /// Returns the configuration value as a string.
    ///
    /// Corresponds to Java: AbstractConfig.getString(String key)
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.originals.get(key).and_then(|v| v.as_str())
    }

    /// Returns the configuration value as an integer.
    ///
    /// Corresponds to Java: AbstractConfig.getInt(String key)
    pub fn get_int(&self, key: &str) -> Option<i32> {
        self.originals
            .get(key)
            .and_then(|v| v.as_i64().map(|i| i as i32))
    }

    /// Returns the configuration value as a long.
    ///
    /// Corresponds to Java: AbstractConfig.getLong(String key)
    pub fn get_long(&self, key: &str) -> Option<i64> {
        self.originals.get(key).and_then(|v| v.as_i64())
    }

    /// Returns the configuration value as a boolean.
    ///
    /// Corresponds to Java: AbstractConfig.getBoolean(String key)
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.originals.get(key).and_then(|v| v.as_bool())
    }

    /// Returns the configuration value as a list.
    ///
    /// Corresponds to Java: AbstractConfig.getList(String key)
    pub fn get_list(&self, key: &str) -> Option<Vec<String>> {
        self.originals.get(key).and_then(|v| match v {
            Value::Array(arr) => Some(
                arr.iter()
                    .filter_map(|item| item.as_str().map(|s| s.to_string()))
                    .collect(),
            ),
            Value::String(s) => Some(
                s.split(',')
                    .map(|part| part.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            ),
            _ => None,
        })
    }

    /// Returns all original configuration values.
    ///
    /// Corresponds to Java: AbstractConfig.originals()
    pub fn originals(&self) -> &HashMap<String, Value> {
        &self.originals
    }

    /// Returns all original configuration values as strings.
    ///
    /// Corresponds to Java: AbstractConfig.originalsStrings()
    pub fn originals_strings(&self) -> HashMap<String, String> {
        self.originals
            .iter()
            .filter_map(|(k, v)| {
                let s = match v {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Array(arr) => {
                        let items: Vec<String> = arr
                            .iter()
                            .filter_map(|item| item.as_str().map(|s| s.to_string()))
                            .collect();
                        items.join(",")
                    }
                    _ => v.to_string(),
                };
                Some((k.clone(), s))
            })
            .collect()
    }

    /// Returns the Kafka cluster ID from configuration.
    ///
    /// This is a simplified version that reads the cluster ID from the configuration
    /// property "cluster.id" instead of dynamically querying the Kafka cluster.
    ///
    /// In the Java implementation, this method dynamically fetches the cluster ID
    /// by creating an Admin client and calling describeCluster().clusterId().
    /// For simplicity in the Rust port, we read it from configuration.
    ///
    /// Corresponds to Java: WorkerConfig.kafkaClusterId()
    pub fn kafka_cluster_id(&self) -> Option<String> {
        self.get_string("cluster.id").map(|s| s.to_string())
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let mut props = HashMap::new();
        props.insert(
            BOOTSTRAP_SERVERS_CONFIG.to_string(),
            Value::String("localhost:9092".to_string()),
        );
        props.insert(
            KEY_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("org.apache.kafka.connect.json.JsonConverter".to_string()),
        );
        props.insert(
            VALUE_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("org.apache.kafka.connect.json.JsonConverter".to_string()),
        );
        WorkerConfig::new_unvalidated(props)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_config_basic() {
        let mut props = HashMap::new();
        props.insert(
            BOOTSTRAP_SERVERS_CONFIG.to_string(),
            Value::String("localhost:9092".to_string()),
        );
        props.insert(
            KEY_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("org.apache.kafka.connect.json.JsonConverter".to_string()),
        );
        props.insert(
            VALUE_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("org.apache.kafka.connect.json.JsonConverter".to_string()),
        );

        let config = WorkerConfig::new(props).unwrap();
        assert_eq!(config.bootstrap_servers(), Some("localhost:9092"));
        assert_eq!(
            config.key_converter_class(),
            Some("org.apache.kafka.connect.json.JsonConverter")
        );
        assert_eq!(
            config.value_converter_class(),
            Some("org.apache.kafka.connect.json.JsonConverter")
        );
    }

    #[test]
    fn test_worker_config_defaults() {
        let config = WorkerConfig::default();
        assert_eq!(
            config.header_converter_class(),
            HEADER_CONVERTER_CLASS_DEFAULT
        );
        assert_eq!(
            config.offset_commit_interval_ms(),
            OFFSET_FLUSH_INTERVAL_MS_DEFAULT
        );
        assert_eq!(
            config.offset_flush_timeout_ms(),
            OFFSET_FLUSH_TIMEOUT_MS_DEFAULT
        );
        assert_eq!(
            config.topic_tracking_enable(),
            TOPIC_TRACKING_ENABLE_DEFAULT
        );
        assert_eq!(
            config.topic_tracking_allow_reset(),
            TOPIC_TRACKING_ALLOW_RESET_DEFAULT
        );
        assert_eq!(
            config.topic_creation_enable(),
            TOPIC_CREATION_ENABLE_DEFAULT
        );
    }

    #[test]
    fn test_worker_config_validation_missing_required() {
        let props = HashMap::new();
        let result = WorkerConfig::new(props);
        assert!(result.is_err());
    }

    #[test]
    fn test_worker_config_custom_values() {
        let mut props = HashMap::new();
        props.insert(
            BOOTSTRAP_SERVERS_CONFIG.to_string(),
            Value::String("kafka:9092".to_string()),
        );
        props.insert(
            KEY_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("org.apache.kafka.connect.storage.StringConverter".to_string()),
        );
        props.insert(
            VALUE_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("org.apache.kafka.connect.storage.StringConverter".to_string()),
        );
        props.insert(
            OFFSET_FLUSH_INTERVAL_MS_CONFIG.to_string(),
            Value::Number(30000.into()),
        );
        props.insert(TOPIC_TRACKING_ENABLE_CONFIG.to_string(), Value::Bool(false));

        let config = WorkerConfig::new(props).unwrap();
        assert_eq!(config.bootstrap_servers(), Some("kafka:9092"));
        assert_eq!(config.offset_commit_interval_ms(), 30000);
        assert_eq!(config.topic_tracking_enable(), false);
    }

    #[test]
    fn test_worker_config_plugin_path() {
        let mut props = HashMap::new();
        props.insert(
            BOOTSTRAP_SERVERS_CONFIG.to_string(),
            Value::String("localhost:9092".to_string()),
        );
        props.insert(
            KEY_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        props.insert(
            VALUE_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        props.insert(
            PLUGIN_PATH_CONFIG.to_string(),
            Value::Array(vec![
                Value::String("/path/to/plugins".to_string()),
                Value::String("/another/path".to_string()),
            ]),
        );

        let config = WorkerConfig::new(props).unwrap();
        let plugin_path = config.plugin_path().unwrap();
        assert_eq!(plugin_path.len(), 2);
        assert_eq!(plugin_path[0], "/path/to/plugins");
        assert_eq!(plugin_path[1], "/another/path");
    }

    #[test]
    fn test_worker_config_string_as_list() {
        let mut props = HashMap::new();
        props.insert(
            BOOTSTRAP_SERVERS_CONFIG.to_string(),
            Value::String("localhost:9092".to_string()),
        );
        props.insert(
            KEY_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        props.insert(
            VALUE_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        props.insert(
            PLUGIN_PATH_CONFIG.to_string(),
            Value::String("/path1,/path2,/path3".to_string()),
        );

        let config = WorkerConfig::new(props).unwrap();
        let plugin_path = config.plugin_path().unwrap();
        assert_eq!(plugin_path.len(), 3);
    }

    #[test]
    fn test_base_config_def() {
        let config_def = WorkerConfig::base_config_def();
        assert!(config_def.contains_key(BOOTSTRAP_SERVERS_CONFIG));
        assert!(config_def.contains_key(KEY_CONVERTER_CLASS_CONFIG));
        assert!(config_def.contains_key(VALUE_CONVERTER_CLASS_CONFIG));
        assert!(config_def.contains_key(OFFSET_FLUSH_INTERVAL_MS_CONFIG));
        assert!(config_def.contains_key(PLUGIN_PATH_CONFIG));
    }

    #[test]
    fn test_originals_strings() {
        let config = WorkerConfig::default();
        let originals = config.originals_strings();
        assert!(originals.contains_key(BOOTSTRAP_SERVERS_CONFIG));
    }
}
