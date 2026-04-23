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

//! Sink connector configuration for Kafka Connect.
//!
//! This module provides the SinkConnectorConfig class that defines
//! configuration options specific to sink connectors.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.SinkConnectorConfig

use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType};
use common_trait::errors::ConfigException;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;

use super::connector_config::ConnectorConfig;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java SinkConnectorConfig
// ============================================================================

/// Configuration key for topics list.
/// Corresponds to Java: SinkConnectorConfig.TOPICS_CONFIG
pub const TOPICS_CONFIG: &str = "topics";

/// Configuration key for topics regex pattern.
/// Corresponds to Java: SinkConnectorConfig.TOPICS_REGEX_CONFIG
pub const TOPICS_REGEX_CONFIG: &str = "topics.regex";

/// Configuration key for dead letter queue topic name.
/// Corresponds to Java: SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG
pub const DLQ_TOPIC_NAME_CONFIG: &str = "errors.deadletterqueue.topic.name";

/// Configuration key for dead letter queue topic replication factor.
/// Corresponds to Java: SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG
pub const DLQ_TOPIC_REPLICATION_FACTOR_CONFIG: &str =
    "errors.deadletterqueue.topic.replication.factor";

/// Default value for dead letter queue topic replication factor.
/// Corresponds to Java: SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_DEFAULT
pub const DLQ_TOPIC_REPLICATION_FACTOR_DEFAULT: i64 = 3;

/// Configuration key for dead letter queue context headers enable.
/// Corresponds to Java: SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG
pub const DLQ_CONTEXT_HEADERS_ENABLE_CONFIG: &str = "errors.deadletterqueue.context.headers.enable";

/// Default value for dead letter queue context headers enable.
/// Corresponds to Java: SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT
pub const DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT: bool = false;

// ============================================================================
// SinkConnectorConfig
// ============================================================================

/// Configuration for sink connectors.
///
/// This class extends ConnectorConfig and adds configuration options specific
/// to sink connectors, including topic selection and dead letter queue (DLQ)
/// settings.
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.SinkConnectorConfig
#[derive(Debug, Clone)]
pub struct SinkConnectorConfig {
    connector_config: ConnectorConfig,
}

impl SinkConnectorConfig {
    /// Creates a new SinkConnectorConfig with the given configuration properties.
    ///
    /// Corresponds to Java: SinkConnectorConfig(Map<?, ?> props)
    pub fn new(props: HashMap<String, Value>) -> Result<Self, ConfigException> {
        let connector_config = ConnectorConfig::new_unvalidated(props);
        let config = SinkConnectorConfig { connector_config };
        config.validate()?;
        Ok(config)
    }

    /// Returns the configuration definition for sink connectors.
    ///
    /// This includes both the base ConnectorConfig definitions and sink-specific ones.
    /// Corresponds to Java: SinkConnectorConfig.sinkConfigDef()
    pub fn sink_config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        let base_def = ConnectorConfig::connector_config_def();

        ConfigDefBuilder::from_map(base_def)
            // Topics list - mutually exclusive with topics.regex
            .define(
                TOPICS_CONFIG,
                ConfigDefType::List,
                Some(Value::Array(vec![])),
                ConfigDefImportance::High,
                "List of topics for this sink connector to consume. Mutually exclusive with topics.regex.",
            )
            // Topics regex - mutually exclusive with topics
            .define(
                TOPICS_REGEX_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "Regular expression for topics for this sink connector to consume. Mutually exclusive with topics.",
            )
            // DLQ topic name
            .define(
                DLQ_TOPIC_NAME_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::Medium,
                "The name of the dead letter queue topic for messages that cannot be processed.",
            )
            // DLQ topic replication factor
            .define(
                DLQ_TOPIC_REPLICATION_FACTOR_CONFIG,
                ConfigDefType::Int,
                Some(Value::Number(DLQ_TOPIC_REPLICATION_FACTOR_DEFAULT.into())),
                ConfigDefImportance::Medium,
                "The replication factor for the dead letter queue topic.",
            )
            // DLQ context headers enable
            .define(
                DLQ_CONTEXT_HEADERS_ENABLE_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT)),
                ConfigDefImportance::Medium,
                "Whether to include context headers in dead letter queue messages.",
            )
            .build()
    }

    /// Returns the full configuration definition.
    ///
    /// Corresponds to Java: SinkConnectorConfig.configDef()
    pub fn config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        Self::sink_config_def()
    }

    /// Validates the configuration values.
    pub fn validate(&self) -> Result<(), ConfigException> {
        // Validate base connector config
        self.connector_config.validate()?;

        // Validate topics and topics.regex mutual exclusivity
        let topics = self.topics();
        let topics_regex = self.topics_regex();

        if !topics.is_empty() && topics_regex.is_some() {
            return Err(ConfigException::new(format!(
                "{} and {} are mutually exclusive. Only one can be set.",
                TOPICS_CONFIG, TOPICS_REGEX_CONFIG
            )));
        }

        // Validate topics.regex is a valid regex if present
        if let Some(regex_str) = topics_regex {
            if Regex::new(regex_str).is_err() {
                return Err(ConfigException::new(format!(
                    "Invalid regex for {}: {}",
                    TOPICS_REGEX_CONFIG, regex_str
                )));
            }
        }

        // Validate DLQ topic is not in the topics list
        if let Some(dlq_topic) = self.dlq_topic_name() {
            for topic in &topics {
                if topic == dlq_topic {
                    return Err(ConfigException::new(format!(
                        "Dead letter queue topic {} cannot be included in the {} list",
                        dlq_topic, TOPICS_CONFIG
                    )));
                }
            }

            // Validate DLQ topic doesn't match topics.regex
            if let Some(regex_str) = topics_regex {
                if let Ok(regex) = Regex::new(regex_str) {
                    if regex.is_match(dlq_topic) {
                        return Err(ConfigException::new(format!(
                            "Dead letter queue topic {} cannot match the {} pattern {}",
                            dlq_topic, TOPICS_REGEX_CONFIG, regex_str
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns the list of topics to consume.
    ///
    /// Corresponds to Java: SinkConnectorConfig.topics()
    pub fn topics(&self) -> Vec<String> {
        self.connector_config
            .get_list(TOPICS_CONFIG)
            .unwrap_or_default()
    }

    /// Returns the topics regex pattern.
    ///
    /// Corresponds to Java: SinkConnectorConfig.topicsRegex()
    pub fn topics_regex(&self) -> Option<&str> {
        self.connector_config.get_string(TOPICS_REGEX_CONFIG)
    }

    /// Returns the dead letter queue topic name.
    ///
    /// Corresponds to Java: SinkConnectorConfig.dlqTopicName()
    pub fn dlq_topic_name(&self) -> Option<&str> {
        self.connector_config.get_string(DLQ_TOPIC_NAME_CONFIG)
    }

    /// Returns the dead letter queue topic replication factor.
    ///
    /// Corresponds to Java: SinkConnectorConfig.dlqTopicReplicationFactor()
    pub fn dlq_topic_replication_factor(&self) -> i64 {
        self.connector_config
            .get_long(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG)
            .unwrap_or(DLQ_TOPIC_REPLICATION_FACTOR_DEFAULT)
    }

    /// Returns whether to include context headers in DLQ messages.
    ///
    /// Corresponds to Java: SinkConnectorConfig.dlqContextHeadersEnable()
    pub fn dlq_context_headers_enable(&self) -> bool {
        self.connector_config
            .get_bool(DLQ_CONTEXT_HEADERS_ENABLE_CONFIG)
            .unwrap_or(DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT)
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

    fn create_basic_sink_props() -> HashMap<String, Value> {
        let mut props = HashMap::new();
        props.insert(
            super::super::connector_config::NAME_CONFIG.to_string(),
            Value::String("sink-connector".to_string()),
        );
        props.insert(
            super::super::connector_config::CONNECTOR_CLASS_CONFIG.to_string(),
            Value::String("TestSinkConnector".to_string()),
        );
        props.insert(
            super::super::connector_config::TASKS_MAX_CONFIG.to_string(),
            Value::Number(1.into()),
        );
        props.insert(
            TOPICS_CONFIG.to_string(),
            Value::Array(vec![
                Value::String("topic1".to_string()),
                Value::String("topic2".to_string()),
            ]),
        );
        props
    }

    #[test]
    fn test_sink_connector_config_basic() {
        let props = create_basic_sink_props();
        let config = SinkConnectorConfig::new(props).unwrap();

        assert_eq!(config.connector_config().name(), Some("sink-connector"));
        assert_eq!(
            config.connector_config().connector_class(),
            Some("TestSinkConnector")
        );
        let topics = config.topics();
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0], "topic1");
        assert_eq!(topics[1], "topic2");
    }

    #[test]
    fn test_sink_connector_config_defaults() {
        let props = create_basic_sink_props();
        let config = SinkConnectorConfig::new(props).unwrap();

        assert_eq!(
            config.dlq_topic_replication_factor(),
            DLQ_TOPIC_REPLICATION_FACTOR_DEFAULT
        );
        assert_eq!(
            config.dlq_context_headers_enable(),
            DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT
        );
    }

    #[test]
    fn test_sink_connector_config_topics_regex() {
        let mut props = create_basic_sink_props();
        props.remove(TOPICS_CONFIG);
        props.insert(
            TOPICS_REGEX_CONFIG.to_string(),
            Value::String("topic.*".to_string()),
        );

        let config = SinkConnectorConfig::new(props).unwrap();
        assert_eq!(config.topics_regex(), Some("topic.*"));
        assert_eq!(config.topics().len(), 0);
    }

    #[test]
    fn test_sink_connector_config_mutual_exclusivity() {
        let mut props = create_basic_sink_props();
        props.insert(
            TOPICS_REGEX_CONFIG.to_string(),
            Value::String("topic.*".to_string()),
        );

        let result = SinkConnectorConfig::new(props);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("mutually exclusive"));
    }

    #[test]
    fn test_sink_connector_config_dlq() {
        let mut props = create_basic_sink_props();
        props.insert(
            DLQ_TOPIC_NAME_CONFIG.to_string(),
            Value::String("dlq-topic".to_string()),
        );
        props.insert(
            DLQ_TOPIC_REPLICATION_FACTOR_CONFIG.to_string(),
            Value::Number(5.into()),
        );
        props.insert(
            DLQ_CONTEXT_HEADERS_ENABLE_CONFIG.to_string(),
            Value::Bool(true),
        );

        let config = SinkConnectorConfig::new(props).unwrap();
        assert_eq!(config.dlq_topic_name(), Some("dlq-topic"));
        assert_eq!(config.dlq_topic_replication_factor(), 5);
        assert_eq!(config.dlq_context_headers_enable(), true);
    }

    #[test]
    fn test_sink_connector_config_invalid_regex() {
        let mut props = create_basic_sink_props();
        props.remove(TOPICS_CONFIG);
        props.insert(
            TOPICS_REGEX_CONFIG.to_string(),
            Value::String("[invalid".to_string()),
        );

        let result = SinkConnectorConfig::new(props);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid regex"));
    }

    #[test]
    fn test_sink_connector_config_dlq_in_topics() {
        let mut props = create_basic_sink_props();
        props.insert(
            DLQ_TOPIC_NAME_CONFIG.to_string(),
            Value::String("topic1".to_string()),
        );

        let result = SinkConnectorConfig::new(props);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cannot be included"));
    }

    #[test]
    fn test_sink_connector_config_dlq_matches_regex() {
        let mut props = HashMap::new();
        props.insert(
            super::super::connector_config::NAME_CONFIG.to_string(),
            Value::String("sink".to_string()),
        );
        props.insert(
            super::super::connector_config::CONNECTOR_CLASS_CONFIG.to_string(),
            Value::String("Test".to_string()),
        );
        props.insert(
            super::super::connector_config::TASKS_MAX_CONFIG.to_string(),
            Value::Number(1.into()),
        );
        props.insert(
            TOPICS_REGEX_CONFIG.to_string(),
            Value::String(".*".to_string()),
        );
        props.insert(
            DLQ_TOPIC_NAME_CONFIG.to_string(),
            Value::String("dlq-topic".to_string()),
        );

        let result = SinkConnectorConfig::new(props);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot match"));
    }

    #[test]
    fn test_sink_connector_config_def() {
        let config_def = SinkConnectorConfig::config_def();
        assert!(config_def.contains_key(TOPICS_CONFIG));
        assert!(config_def.contains_key(TOPICS_REGEX_CONFIG));
        assert!(config_def.contains_key(DLQ_TOPIC_NAME_CONFIG));
        // Also includes base connector configs
        assert!(config_def.contains_key(super::super::connector_config::NAME_CONFIG));
    }
}
