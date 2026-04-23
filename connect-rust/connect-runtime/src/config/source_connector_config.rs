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

//! Source connector configuration for Kafka Connect.
//!
//! This module provides the SourceConnectorConfig class that defines
//! configuration options specific to source connectors.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.SourceConnectorConfig

use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType};
use common_trait::errors::ConfigException;
use serde_json::Value;
use std::collections::HashMap;

use super::connector_config::ConnectorConfig;
use super::worker_config::OFFSET_FLUSH_INTERVAL_MS_DEFAULT;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java SourceConnectorConfig
// ============================================================================

/// Configuration key for topic creation groups.
/// Corresponds to Java: SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG
pub const TOPIC_CREATION_GROUPS_CONFIG: &str = "topic.creation.groups";

/// Configuration key for exactly once support.
/// Corresponds to Java: SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG
pub const EXACTLY_ONCE_SUPPORT_CONFIG: &str = "exactly.once.support";

/// Default value for exactly once support.
/// Corresponds to Java: SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_DEFAULT
pub const EXACTLY_ONCE_SUPPORT_DEFAULT: &str = "requested";

/// Configuration key for transaction boundary.
/// Corresponds to Java: SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG
pub const TRANSACTION_BOUNDARY_CONFIG: &str = "transaction.boundary";

/// Default value for transaction boundary.
/// Corresponds to Java: SourceConnectorConfig.TRANSACTION_BOUNDARY_DEFAULT
pub const TRANSACTION_BOUNDARY_DEFAULT: &str = "poll";

/// Configuration key for transaction boundary interval in milliseconds.
/// Corresponds to Java: SourceConnectorConfig.TRANSACTION_BOUNDARY_INTERVAL_MS_CONFIG
pub const TRANSACTION_BOUNDARY_INTERVAL_MS_CONFIG: &str = "transaction.boundary.interval.ms";

/// Configuration key for offsets storage topic (for exactly-once).
/// Corresponds to Java: SourceConnectorConfig.OFFSETS_STORAGE_TOPIC_CONFIG
pub const OFFSETS_STORAGE_TOPIC_CONFIG: &str = "offsets.storage.topic";

/// Configuration key for topic creation prefix.
/// Corresponds to Java: SourceConnectorConfig.TOPIC_CREATION_PREFIX
pub const TOPIC_CREATION_PREFIX: &str = "topic.creation.";

// ============================================================================
// SourceConnectorConfig
// ============================================================================

/// Configuration for source connectors.
///
/// This class extends ConnectorConfig and adds configuration options specific
/// to source connectors, including topic creation settings and exactly-once
/// support options.
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.SourceConnectorConfig
#[derive(Debug, Clone)]
pub struct SourceConnectorConfig {
    connector_config: ConnectorConfig,
}

impl SourceConnectorConfig {
    /// Creates a new SourceConnectorConfig with the given configuration properties.
    ///
    /// Corresponds to Java: SourceConnectorConfig(Map<?, ?> props)
    pub fn new(props: HashMap<String, Value>) -> Result<Self, ConfigException> {
        let connector_config = ConnectorConfig::new_unvalidated(props);
        let config = SourceConnectorConfig { connector_config };
        config.validate()?;
        Ok(config)
    }

    /// Returns the configuration definition for source connectors.
    ///
    /// This includes both the base ConnectorConfig definitions and source-specific ones.
    /// Corresponds to Java: SourceConnectorConfig.sourceConfigDef()
    pub fn source_config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        let base_def = ConnectorConfig::connector_config_def();

        ConfigDefBuilder::from_map(base_def)
            // Topic creation groups
            .define(
                TOPIC_CREATION_GROUPS_CONFIG,
                ConfigDefType::List,
                Some(Value::Array(vec![])),
                ConfigDefImportance::Medium,
                "The groups of topics this source connector can create.",
            )
            // Exactly once support
            .define(
                EXACTLY_ONCE_SUPPORT_CONFIG,
                ConfigDefType::String,
                Some(Value::String(EXACTLY_ONCE_SUPPORT_DEFAULT.to_string())),
                ConfigDefImportance::Medium,
                "Whether this source connector supports exactly-once semantics. Valid values: requested, required.",
            )
            // Transaction boundary
            .define(
                TRANSACTION_BOUNDARY_CONFIG,
                ConfigDefType::String,
                Some(Value::String(TRANSACTION_BOUNDARY_DEFAULT.to_string())),
                ConfigDefImportance::Medium,
                "How transactions are bounded. Valid values: poll, connector, interval.",
            )
            // Transaction boundary interval
            .define(
                TRANSACTION_BOUNDARY_INTERVAL_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(OFFSET_FLUSH_INTERVAL_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                "The interval in milliseconds for transaction boundaries when using interval mode.",
            )
            // Offsets storage topic (for exactly-once source connectors)
            .define(
                OFFSETS_STORAGE_TOPIC_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::Medium,
                "The topic name for offsets storage when using exactly-once support.",
            )
            .build()
    }

    /// Returns the full configuration definition.
    ///
    /// Corresponds to Java: SourceConnectorConfig.configDef()
    pub fn config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        Self::source_config_def()
    }

    /// Validates the configuration values.
    pub fn validate(&self) -> Result<(), ConfigException> {
        // Validate base connector config
        self.connector_config.validate()?;

        // Validate exactly_once_support value
        if let Some(value) = self
            .connector_config
            .get_string(EXACTLY_ONCE_SUPPORT_CONFIG)
        {
            if value != "requested" && value != "required" {
                return Err(ConfigException::new(format!(
                    "Invalid value {} for {}, valid values are: requested, required",
                    value, EXACTLY_ONCE_SUPPORT_CONFIG
                )));
            }
        }

        // Validate transaction_boundary value
        if let Some(value) = self
            .connector_config
            .get_string(TRANSACTION_BOUNDARY_CONFIG)
        {
            if value != "poll" && value != "connector" && value != "interval" {
                return Err(ConfigException::new(format!(
                    "Invalid value {} for {}, valid values are: poll, connector, interval",
                    value, TRANSACTION_BOUNDARY_CONFIG
                )));
            }
        }

        Ok(())
    }

    /// Returns the topic creation groups.
    ///
    /// Corresponds to Java: SourceConnectorConfig.topicCreationGroups()
    pub fn topic_creation_groups(&self) -> Vec<String> {
        self.connector_config
            .get_list(TOPIC_CREATION_GROUPS_CONFIG)
            .unwrap_or_default()
    }

    /// Returns the exactly once support level.
    ///
    /// Corresponds to Java: SourceConnectorConfig.exactlyOnceSupport()
    pub fn exactly_once_support(&self) -> &str {
        self.connector_config
            .get_string(EXACTLY_ONCE_SUPPORT_CONFIG)
            .unwrap_or(EXACTLY_ONCE_SUPPORT_DEFAULT)
    }

    /// Returns the transaction boundary mode.
    ///
    /// Corresponds to Java: SourceConnectorConfig.transactionBoundary()
    pub fn transaction_boundary(&self) -> &str {
        self.connector_config
            .get_string(TRANSACTION_BOUNDARY_CONFIG)
            .unwrap_or(TRANSACTION_BOUNDARY_DEFAULT)
    }

    /// Returns the transaction boundary interval in milliseconds.
    ///
    /// Corresponds to Java: SourceConnectorConfig.transactionBoundaryIntervalMs()
    pub fn transaction_boundary_interval_ms(&self) -> i64 {
        self.connector_config
            .get_long(TRANSACTION_BOUNDARY_INTERVAL_MS_CONFIG)
            .unwrap_or(OFFSET_FLUSH_INTERVAL_MS_DEFAULT)
    }

    /// Returns the offsets storage topic name (for exactly-once).
    ///
    /// Corresponds to Java: SourceConnectorConfig.offsetsStorageTopic()
    pub fn offsets_storage_topic(&self) -> Option<&str> {
        self.connector_config
            .get_string(OFFSETS_STORAGE_TOPIC_CONFIG)
    }

    /// Returns the underlying ConnectorConfig.
    pub fn connector_config(&self) -> &ConnectorConfig {
        &self.connector_config
    }

    /// Returns the raw configuration value for the given key.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.connector_config.get(key)
    }

    /// Returns the configuration value as a string.
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.connector_config.get_string(key)
    }

    /// Returns the configuration value as a long.
    pub fn get_long(&self, key: &str) -> Option<i64> {
        self.connector_config.get_long(key)
    }

    /// Returns all original configuration values.
    pub fn originals(&self) -> &HashMap<String, Value> {
        self.connector_config.originals()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_basic_source_props() -> HashMap<String, Value> {
        let mut props = HashMap::new();
        props.insert(
            super::super::connector_config::NAME_CONFIG.to_string(),
            Value::String("source-connector".to_string()),
        );
        props.insert(
            super::super::connector_config::CONNECTOR_CLASS_CONFIG.to_string(),
            Value::String("TestSourceConnector".to_string()),
        );
        props.insert(
            super::super::connector_config::TASKS_MAX_CONFIG.to_string(),
            Value::Number(1.into()),
        );
        props
    }

    #[test]
    fn test_source_connector_config_basic() {
        let props = create_basic_source_props();
        let config = SourceConnectorConfig::new(props).unwrap();

        assert_eq!(config.connector_config().name(), Some("source-connector"));
        assert_eq!(
            config.connector_config().connector_class(),
            Some("TestSourceConnector")
        );
    }

    #[test]
    fn test_source_connector_config_defaults() {
        let props = create_basic_source_props();
        let config = SourceConnectorConfig::new(props).unwrap();

        assert_eq!(config.exactly_once_support(), EXACTLY_ONCE_SUPPORT_DEFAULT);
        assert_eq!(config.transaction_boundary(), TRANSACTION_BOUNDARY_DEFAULT);
        assert_eq!(
            config.transaction_boundary_interval_ms(),
            OFFSET_FLUSH_INTERVAL_MS_DEFAULT
        );
    }

    #[test]
    fn test_source_connector_config_custom_values() {
        let mut props = create_basic_source_props();
        props.insert(
            EXACTLY_ONCE_SUPPORT_CONFIG.to_string(),
            Value::String("required".to_string()),
        );
        props.insert(
            TRANSACTION_BOUNDARY_CONFIG.to_string(),
            Value::String("interval".to_string()),
        );
        props.insert(
            TRANSACTION_BOUNDARY_INTERVAL_MS_CONFIG.to_string(),
            Value::Number(10000.into()),
        );

        let config = SourceConnectorConfig::new(props).unwrap();
        assert_eq!(config.exactly_once_support(), "required");
        assert_eq!(config.transaction_boundary(), "interval");
        assert_eq!(config.transaction_boundary_interval_ms(), 10000);
    }

    #[test]
    fn test_source_connector_config_invalid_exactly_once() {
        let mut props = create_basic_source_props();
        props.insert(
            EXACTLY_ONCE_SUPPORT_CONFIG.to_string(),
            Value::String("invalid".to_string()),
        );
        let result = SourceConnectorConfig::new(props);
        assert!(result.is_err());
    }

    #[test]
    fn test_source_connector_config_invalid_transaction_boundary() {
        let mut props = create_basic_source_props();
        props.insert(
            TRANSACTION_BOUNDARY_CONFIG.to_string(),
            Value::String("invalid".to_string()),
        );
        let result = SourceConnectorConfig::new(props);
        assert!(result.is_err());
    }

    #[test]
    fn test_source_connector_config_topic_creation_groups() {
        let mut props = create_basic_source_props();
        props.insert(
            TOPIC_CREATION_GROUPS_CONFIG.to_string(),
            Value::Array(vec![
                Value::String("default".to_string()),
                Value::String("logs".to_string()),
            ]),
        );

        let config = SourceConnectorConfig::new(props).unwrap();
        let groups = config.topic_creation_groups();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0], "default");
        assert_eq!(groups[1], "logs");
    }

    #[test]
    fn test_source_connector_config_def() {
        let config_def = SourceConnectorConfig::config_def();
        assert!(config_def.contains_key(TOPIC_CREATION_GROUPS_CONFIG));
        assert!(config_def.contains_key(EXACTLY_ONCE_SUPPORT_CONFIG));
        assert!(config_def.contains_key(TRANSACTION_BOUNDARY_CONFIG));
        // Also includes base connector configs
        assert!(config_def.contains_key(super::super::connector_config::NAME_CONFIG));
    }
}
