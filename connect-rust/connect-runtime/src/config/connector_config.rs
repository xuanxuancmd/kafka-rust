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

//! Connector configuration for Kafka Connect.
//!
//! This module provides the ConnectorConfig class that defines configuration
//! options common to all Kafka Connect connectors.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.ConnectorConfig

use common_trait::config::{
    ConfigDefBuilder, ConfigDefImportance, ConfigDefType, ConfigDefWidth, NonEmptyStringValidator,
    RangeValidator,
};
use common_trait::errors::ConfigException;
use serde_json::Value;
use std::collections::HashMap;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java ConnectorConfig
// ============================================================================

/// Configuration key for connector name.
/// Corresponds to Java: ConnectorConfig.NAME_CONFIG
pub const NAME_CONFIG: &str = "name";

/// Configuration key for connector class.
/// Corresponds to Java: ConnectorConfig.CONNECTOR_CLASS_CONFIG
pub const CONNECTOR_CLASS_CONFIG: &str = "connector.class";

/// Configuration key for tasks max.
/// Corresponds to Java: ConnectorConfig.TASKS_MAX_CONFIG
pub const TASKS_MAX_CONFIG: &str = "tasks.max";

/// Default value for tasks max.
/// Corresponds to Java: ConnectorConfig.TASKS_MAX_DEFAULT
pub const TASKS_MAX_DEFAULT: i64 = 1;

/// Configuration key for connector version.
/// Corresponds to Java: ConnectorConfig.CONNECTOR_VERSION_CONFIG
pub const CONNECTOR_VERSION_CONFIG: &str = "connector.version";

/// Configuration key for key converter class (connector override).
/// Corresponds to Java: ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG
pub const CONNECTOR_KEY_CONVERTER_CLASS_CONFIG: &str = "key.converter";

/// Configuration key for value converter class (connector override).
/// Corresponds to Java: ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG
pub const CONNECTOR_VALUE_CONVERTER_CLASS_CONFIG: &str = "value.converter";

/// Configuration key for header converter class (connector override).
/// Corresponds to Java: ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG
pub const CONNECTOR_HEADER_CONVERTER_CLASS_CONFIG: &str = "header.converter";

/// Configuration key for transforms.
/// Corresponds to Java: ConnectorConfig.TRANSFORMS_CONFIG
pub const TRANSFORMS_CONFIG: &str = "transforms";

/// Configuration key for predicates.
/// Corresponds to Java: ConnectorConfig.PREDICATES_CONFIG
pub const PREDICATES_CONFIG: &str = "predicates";

/// Configuration key for errors retry timeout.
/// Corresponds to Java: ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG
pub const ERRORS_RETRY_TIMEOUT_CONFIG: &str = "errors.retry.timeout";

/// Default value for errors retry timeout.
/// Corresponds to Java: ConnectorConfig.ERRORS_RETRY_TIMEOUT_DEFAULT
pub const ERRORS_RETRY_TIMEOUT_DEFAULT: i64 = 0;

/// Configuration key for errors retry delay max in milliseconds.
/// Corresponds to Java: ConnectorConfig.ERRORS_RETRY_DELAY_MAX_MS_CONFIG
pub const ERRORS_RETRY_DELAY_MAX_MS_CONFIG: &str = "errors.retry.delay.max.ms";

/// Default value for errors retry delay max in milliseconds.
/// Corresponds to Java: ConnectorConfig.ERRORS_RETRY_DELAY_MAX_MS_DEFAULT
pub const ERRORS_RETRY_DELAY_MAX_MS_DEFAULT: i64 = 60000;

/// Configuration key for errors tolerance.
/// Corresponds to Java: ConnectorConfig.ERRORS_TOLERANCE_CONFIG
pub const ERRORS_TOLERANCE_CONFIG: &str = "errors.tolerance";

/// Default value for errors tolerance.
/// Corresponds to Java: ConnectorConfig.ERRORS_TOLERANCE_DEFAULT
pub const ERRORS_TOLERANCE_DEFAULT: &str = "none";

/// Configuration key for errors log enable.
/// Corresponds to Java: ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG
pub const ERRORS_LOG_ENABLE_CONFIG: &str = "errors.log.enable";

/// Default value for errors log enable.
/// Corresponds to Java: ConnectorConfig.ERRORS_LOG_ENABLE_DEFAULT
pub const ERRORS_LOG_ENABLE_DEFAULT: bool = false;

/// Configuration key for errors log include messages.
/// Corresponds to Java: ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG
pub const ERRORS_LOG_INCLUDE_MESSAGES_CONFIG: &str = "errors.log.include.messages";

/// Default value for errors log include messages.
/// Corresponds to Java: ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT
pub const ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT: bool = false;

/// Configuration key for tasks max enforcement.
/// Corresponds to Java: ConnectorConfig.TASKS_MAX_ENFORCE_CONFIG
pub const TASKS_MAX_ENFORCE_CONFIG: &str = "tasks.max.enforce";

/// Default value for tasks max enforcement.
/// Corresponds to Java: ConnectorConfig.TASKS_MAX_ENFORCE_DEFAULT
pub const TASKS_MAX_ENFORCE_DEFAULT: bool = true;

/// Configuration key for config reload action.
/// Corresponds to Java: ConnectorConfig.CONFIG_RELOAD_ACTION_CONFIG
pub const CONFIG_RELOAD_ACTION_CONFIG: &str = "config.action.reload";

/// Default value for config reload action - restart.
/// Corresponds to Java: ConnectorConfig.CONFIG_RELOAD_ACTION_RESTART
pub const CONFIG_RELOAD_ACTION_RESTART: &str = "restart";

/// Config reload action value - none (no action).
/// Corresponds to Java: ConnectorConfig.CONFIG_RELOAD_ACTION_NONE
pub const CONFIG_RELOAD_ACTION_NONE: &str = "none";

// ============================================================================
// ConnectorConfig
// ============================================================================

/// Configuration for Kafka Connect connectors.
///
/// This class defines common configuration options for all connectors,
/// including the connector name, class, number of tasks, and error handling.
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.ConnectorConfig
#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    originals: HashMap<String, Value>,
}

impl ConnectorConfig {
    /// Creates a new ConnectorConfig with the given configuration properties.
    ///
    /// Corresponds to Java: ConnectorConfig(Map<?, ?> props)
    pub fn new(props: HashMap<String, Value>) -> Result<Self, ConfigException> {
        let config = ConnectorConfig { originals: props };
        config.validate()?;
        Ok(config)
    }

    /// Creates a ConnectorConfig without validation (for internal use).
    pub fn new_unvalidated(props: HashMap<String, Value>) -> Self {
        ConnectorConfig { originals: props }
    }

    /// Returns the base configuration definition for connectors.
    ///
    /// This method defines the common configuration options for all connectors.
    /// Corresponds to Java: ConnectorConfig.connectorConfigDef()
    pub fn connector_config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        ConfigDefBuilder::new()
            // Connector name - required, validated for non-empty without control chars
            .define_with_validator(
                NAME_CONFIG,
                ConfigDefType::String,
                None,
                Some(Box::new(NonEmptyStringValidator::new())),
                ConfigDefImportance::High,
                "The name of the connector.",
            )
            // Connector class - required
            .define_with_validator(
                CONNECTOR_CLASS_CONFIG,
                ConfigDefType::String,
                None,
                Some(Box::new(NonEmptyStringValidator::new())),
                ConfigDefImportance::High,
                "The class for the connector.",
            )
            // Tasks max - default 1, must be >= 1
            .define_with_validator(
                TASKS_MAX_CONFIG,
                ConfigDefType::Int,
                Some(Value::Number(TASKS_MAX_DEFAULT.into())),
                Some(Box::new(RangeValidator::at_least(1.0))),
                ConfigDefImportance::High,
                "The maximum number of tasks for this connector.",
            )
            // Connector version
            .define(
                CONNECTOR_VERSION_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::Low,
                "The version of the connector.",
            )
            // Key converter (connector override)
            .define(
                CONNECTOR_KEY_CONVERTER_CLASS_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::Medium,
                "The class for the key converter for this connector. Overrides worker setting.",
            )
            // Value converter (connector override)
            .define(
                CONNECTOR_VALUE_CONVERTER_CLASS_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::Medium,
                "The class for the value converter for this connector. Overrides worker setting.",
            )
            // Header converter (connector override)
            .define(
                CONNECTOR_HEADER_CONVERTER_CLASS_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::Medium,
                "The class for the header converter for this connector. Overrides worker setting.",
            )
            // Transforms
            .define(
                TRANSFORMS_CONFIG,
                ConfigDefType::List,
                Some(Value::Array(vec![])),
                ConfigDefImportance::Medium,
                "List of transformation aliases to apply to records.",
            )
            // Predicates
            .define(
                PREDICATES_CONFIG,
                ConfigDefType::List,
                Some(Value::Array(vec![])),
                ConfigDefImportance::Medium,
                "List of predicate aliases for transformations.",
            )
            // Errors retry timeout
            .define(
                ERRORS_RETRY_TIMEOUT_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(ERRORS_RETRY_TIMEOUT_DEFAULT.into())),
                ConfigDefImportance::Medium,
                "The maximum time in milliseconds to retry on errors.",
            )
            // Errors retry delay max
            .define(
                ERRORS_RETRY_DELAY_MAX_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(ERRORS_RETRY_DELAY_MAX_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                "The maximum delay in milliseconds between retries.",
            )
            // Errors tolerance
            .define(
                ERRORS_TOLERANCE_CONFIG,
                ConfigDefType::String,
                Some(Value::String(ERRORS_TOLERANCE_DEFAULT.to_string())),
                ConfigDefImportance::Medium,
                "The tolerance for errors during connector operation. Valid values: none, all.",
            )
            // Errors log enable
            .define(
                ERRORS_LOG_ENABLE_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(ERRORS_LOG_ENABLE_DEFAULT)),
                ConfigDefImportance::Medium,
                "Whether to log errors.",
            )
            // Errors log include messages
            .define(
                ERRORS_LOG_INCLUDE_MESSAGES_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT)),
                ConfigDefImportance::Medium,
                "Whether to include messages in error logs.",
            )
            .build()
    }

    /// Returns the full configuration definition including internal configs.
    ///
    /// Corresponds to Java: ConnectorConfig.configDef()
    pub fn config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        Self::connector_config_def()
    }

    /// Validates the configuration values.
    pub fn validate(&self) -> Result<(), ConfigException> {
        let config_def = Self::config_def();

        for (name, key) in &config_def {
            let value = self.originals.get(name);

            // Check required configurations
            if value.is_none() && key.default_value().is_none() {
                if name == NAME_CONFIG || name == CONNECTOR_CLASS_CONFIG {
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

    /// Returns the connector name.
    ///
    /// Corresponds to Java: ConnectorConfig.name()
    pub fn name(&self) -> Option<&str> {
        self.get_string(NAME_CONFIG)
    }

    /// Returns the connector class.
    ///
    /// Corresponds to Java: ConnectorConfig.connectorClass()
    pub fn connector_class(&self) -> Option<&str> {
        self.get_string(CONNECTOR_CLASS_CONFIG)
    }

    /// Returns the maximum number of tasks.
    ///
    /// Corresponds to Java: ConnectorConfig.tasksMax()
    pub fn tasks_max(&self) -> i64 {
        self.get_long(TASKS_MAX_CONFIG).unwrap_or(TASKS_MAX_DEFAULT)
    }

    /// Returns the connector version.
    ///
    /// Corresponds to Java: ConnectorConfig.version()
    pub fn connector_version(&self) -> Option<&str> {
        self.get_string(CONNECTOR_VERSION_CONFIG)
    }

    /// Returns the key converter class (connector override).
    ///
    /// Corresponds to Java: ConnectorConfig.keyConverter()
    pub fn key_converter_class(&self) -> Option<&str> {
        self.get_string(CONNECTOR_KEY_CONVERTER_CLASS_CONFIG)
    }

    /// Returns the value converter class (connector override).
    ///
    /// Corresponds to Java: ConnectorConfig.valueConverter()
    pub fn value_converter_class(&self) -> Option<&str> {
        self.get_string(CONNECTOR_VALUE_CONVERTER_CLASS_CONFIG)
    }

    /// Returns the header converter class (connector override).
    ///
    /// Corresponds to Java: ConnectorConfig.headerConverter()
    pub fn header_converter_class(&self) -> Option<&str> {
        self.get_string(CONNECTOR_HEADER_CONVERTER_CLASS_CONFIG)
    }

    /// Returns the transformation aliases.
    ///
    /// Corresponds to Java: ConnectorConfig.transforms()
    pub fn transforms(&self) -> Vec<String> {
        self.get_list(TRANSFORMS_CONFIG).unwrap_or_default()
    }

    /// Returns the predicate aliases.
    ///
    /// Corresponds to Java: ConnectorConfig.predicates()
    pub fn predicates(&self) -> Vec<String> {
        self.get_list(PREDICATES_CONFIG).unwrap_or_default()
    }

    /// Returns the errors retry timeout in milliseconds.
    ///
    /// Corresponds to Java: ConnectorConfig.errorsRetryTimeout()
    pub fn errors_retry_timeout(&self) -> i64 {
        self.get_long(ERRORS_RETRY_TIMEOUT_CONFIG)
            .unwrap_or(ERRORS_RETRY_TIMEOUT_DEFAULT)
    }

    /// Returns the errors retry delay max in milliseconds.
    ///
    /// Corresponds to Java: ConnectorConfig.errorsRetryDelayMaxMs()
    pub fn errors_retry_delay_max_ms(&self) -> i64 {
        self.get_long(ERRORS_RETRY_DELAY_MAX_MS_CONFIG)
            .unwrap_or(ERRORS_RETRY_DELAY_MAX_MS_DEFAULT)
    }

    /// Returns the errors tolerance level.
    ///
    /// Corresponds to Java: ConnectorConfig.errorsTolerance()
    pub fn errors_tolerance(&self) -> &str {
        self.get_string(ERRORS_TOLERANCE_CONFIG)
            .unwrap_or(ERRORS_TOLERANCE_DEFAULT)
    }

    /// Returns whether errors logging is enabled.
    ///
    /// Corresponds to Java: ConnectorConfig.errorsLogEnable()
    pub fn errors_log_enable(&self) -> bool {
        self.get_bool(ERRORS_LOG_ENABLE_CONFIG)
            .unwrap_or(ERRORS_LOG_ENABLE_DEFAULT)
    }

    /// Returns whether to include messages in error logs.
    ///
    /// Corresponds to Java: ConnectorConfig.errorsLogIncludeMessages()
    pub fn errors_log_include_messages(&self) -> bool {
        self.get_bool(ERRORS_LOG_INCLUDE_MESSAGES_CONFIG)
            .unwrap_or(ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_basic_connector_props() -> HashMap<String, Value> {
        let mut props = HashMap::new();
        props.insert(
            NAME_CONFIG.to_string(),
            Value::String("test-connector".to_string()),
        );
        props.insert(
            CONNECTOR_CLASS_CONFIG.to_string(),
            Value::String("org.apache.kafka.connect.file.FileSourceConnector".to_string()),
        );
        props.insert(TASKS_MAX_CONFIG.to_string(), Value::Number(3.into()));
        props
    }

    #[test]
    fn test_connector_config_basic() {
        let props = create_basic_connector_props();
        let config = ConnectorConfig::new(props).unwrap();

        assert_eq!(config.name(), Some("test-connector"));
        assert_eq!(
            config.connector_class(),
            Some("org.apache.kafka.connect.file.FileSourceConnector")
        );
        assert_eq!(config.tasks_max(), 3);
    }

    #[test]
    fn test_connector_config_defaults() {
        let props = create_basic_connector_props();
        let config = ConnectorConfig::new(props).unwrap();

        assert_eq!(config.tasks_max(), 3); // overridden
        assert_eq!(config.errors_retry_timeout(), ERRORS_RETRY_TIMEOUT_DEFAULT);
        assert_eq!(config.errors_tolerance(), ERRORS_TOLERANCE_DEFAULT);
        assert_eq!(config.errors_log_enable(), ERRORS_LOG_ENABLE_DEFAULT);
    }

    #[test]
    fn test_connector_config_validation_missing_name() {
        let mut props = HashMap::new();
        props.insert(
            CONNECTOR_CLASS_CONFIG.to_string(),
            Value::String("TestConnector".to_string()),
        );
        let result = ConnectorConfig::new(props);
        assert!(result.is_err());
    }

    #[test]
    fn test_connector_config_validation_missing_class() {
        let mut props = HashMap::new();
        props.insert(NAME_CONFIG.to_string(), Value::String("test".to_string()));
        let result = ConnectorConfig::new(props);
        assert!(result.is_err());
    }

    #[test]
    fn test_connector_config_validation_invalid_tasks_max() {
        let mut props = create_basic_connector_props();
        props.insert(TASKS_MAX_CONFIG.to_string(), Value::Number(0.into()));
        let result = ConnectorConfig::new(props);
        assert!(result.is_err());
    }

    #[test]
    fn test_connector_config_error_handling() {
        let mut props = create_basic_connector_props();
        props.insert(
            ERRORS_RETRY_TIMEOUT_CONFIG.to_string(),
            Value::Number(60000.into()),
        );
        props.insert(
            ERRORS_TOLERANCE_CONFIG.to_string(),
            Value::String("all".to_string()),
        );
        props.insert(ERRORS_LOG_ENABLE_CONFIG.to_string(), Value::Bool(true));

        let config = ConnectorConfig::new(props).unwrap();
        assert_eq!(config.errors_retry_timeout(), 60000);
        assert_eq!(config.errors_tolerance(), "all");
        assert_eq!(config.errors_log_enable(), true);
    }

    #[test]
    fn test_connector_config_transforms() {
        let mut props = create_basic_connector_props();
        props.insert(
            TRANSFORMS_CONFIG.to_string(),
            Value::Array(vec![
                Value::String("extract".to_string()),
                Value::String("filter".to_string()),
            ]),
        );

        let config = ConnectorConfig::new(props).unwrap();
        let transforms = config.transforms();
        assert_eq!(transforms.len(), 2);
        assert_eq!(transforms[0], "extract");
        assert_eq!(transforms[1], "filter");
    }

    #[test]
    fn test_connector_config_def() {
        let config_def = ConnectorConfig::config_def();
        assert!(config_def.contains_key(NAME_CONFIG));
        assert!(config_def.contains_key(CONNECTOR_CLASS_CONFIG));
        assert!(config_def.contains_key(TASKS_MAX_CONFIG));
        assert!(config_def.contains_key(ERRORS_RETRY_TIMEOUT_CONFIG));
    }
}
