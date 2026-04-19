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

//! Standalone configuration for Kafka Connect.
//!
//! This module provides the StandaloneConfig class that defines configuration
//! options specific to standalone mode Kafka Connect workers.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.standalone.StandaloneConfig

use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType};
use common_trait::errors::ConfigException;
use serde_json::Value;
use std::collections::HashMap;

use super::worker_config::WorkerConfig;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java StandaloneConfig
// ============================================================================

/// Configuration key for offset storage file filename.
/// Corresponds to Java: StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG
pub const OFFSET_STORAGE_FILE_FILENAME_CONFIG: &str = "offset.storage.file.filename";

/// Configuration key for key converter for offset storage.
/// Corresponds to Java: StandaloneConfig.KEY_CONVERTER_CLASS_CONFIG (override)
/// In standalone mode, this can be used to configure offset storage key converter
pub const OFFSET_STORAGE_KEY_CONVERTER_CLASS_CONFIG: &str = "key.converter";

/// Configuration key for value converter for offset storage.
/// Corresponds to Java: StandaloneConfig.VALUE_CONVERTER_CLASS_CONFIG (override)
/// In standalone mode, this can be used to configure offset storage value converter
pub const OFFSET_STORAGE_VALUE_CONVERTER_CLASS_CONFIG: &str = "value.converter";

// ============================================================================
// StandaloneConfig
// ============================================================================

/// Configuration for standalone mode Kafka Connect workers.
///
/// This class extends WorkerConfig and adds configuration options specific
/// to standalone mode, primarily for file-based offset storage.
///
/// In standalone mode, the worker operates independently without cluster coordination.
/// It uses a local file for storing offsets instead of Kafka topics.
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.standalone.StandaloneConfig
#[derive(Debug, Clone)]
pub struct StandaloneConfig {
    worker_config: WorkerConfig,
}

impl StandaloneConfig {
    /// Creates a new StandaloneConfig with the given configuration properties.
    ///
    /// Corresponds to Java: StandaloneConfig(Map<?, ?> props)
    pub fn new(props: HashMap<String, Value>) -> Result<Self, ConfigException> {
        let config = StandaloneConfig {
            worker_config: WorkerConfig::new_unvalidated(props),
        };
        config.validate()?;
        Ok(config)
    }

    /// Returns the configuration definition for standalone mode.
    ///
    /// This includes both the base WorkerConfig definitions and standalone-specific ones.
    /// Corresponds to Java: StandaloneConfig.configDef()
    pub fn config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        let base_def = WorkerConfig::base_config_def();

        ConfigDefBuilder::from_map(base_def)
            // Offset storage file filename - required for standalone mode
            .define(
                OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The file to store offset data in. This file is used for source connector offsets in standalone mode.",
            )
            .build()
    }

    /// Validates the configuration values.
    pub fn validate(&self) -> Result<(), ConfigException> {
        // Validate required standalone-specific configs
        if self
            .worker_config
            .get(OFFSET_STORAGE_FILE_FILENAME_CONFIG)
            .is_none()
        {
            return Err(ConfigException::new(format!(
                "Required configuration {} is missing",
                OFFSET_STORAGE_FILE_FILENAME_CONFIG
            )));
        }

        // Validate base worker configs
        self.worker_config.validate()?;
        Ok(())
    }

    /// Returns the offset storage file filename.
    ///
    /// Corresponds to Java: StandaloneConfig.offsetStorageFileFilename()
    pub fn offset_storage_file_filename(&self) -> Option<&str> {
        self.worker_config
            .get_string(OFFSET_STORAGE_FILE_FILENAME_CONFIG)
    }

    /// Returns the underlying WorkerConfig.
    pub fn worker_config(&self) -> &WorkerConfig {
        &self.worker_config
    }

    /// Returns the raw configuration value for the given key.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.worker_config.get(key)
    }

    /// Returns the configuration value as a string.
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.worker_config.get_string(key)
    }

    /// Returns the configuration value as a long.
    pub fn get_long(&self, key: &str) -> Option<i64> {
        self.worker_config.get_long(key)
    }

    /// Returns the configuration value as a boolean.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.worker_config.get_bool(key)
    }

    /// Returns all original configuration values.
    pub fn originals(&self) -> &HashMap<String, Value> {
        self.worker_config.originals()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_basic_standalone_props() -> HashMap<String, Value> {
        let mut props = HashMap::new();
        props.insert(
            super::super::worker_config::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            Value::String("localhost:9092".to_string()),
        );
        props.insert(
            super::super::worker_config::KEY_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        props.insert(
            super::super::worker_config::VALUE_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        props.insert(
            OFFSET_STORAGE_FILE_FILENAME_CONFIG.to_string(),
            Value::String("/tmp/connect.offsets".to_string()),
        );
        props
    }

    #[test]
    fn test_standalone_config_basic() {
        let props = create_basic_standalone_props();
        let config = StandaloneConfig::new(props).unwrap();

        assert_eq!(
            config.offset_storage_file_filename(),
            Some("/tmp/connect.offsets")
        );
    }

    #[test]
    fn test_standalone_config_validation_missing_offset_file() {
        let mut props = create_basic_standalone_props();
        props.remove(OFFSET_STORAGE_FILE_FILENAME_CONFIG);
        let result = StandaloneConfig::new(props);
        assert!(result.is_err());
    }

    #[test]
    fn test_standalone_config_worker_config_access() {
        let props = create_basic_standalone_props();
        let config = StandaloneConfig::new(props).unwrap();

        // Can access worker config values
        assert_eq!(
            config.worker_config().bootstrap_servers(),
            Some("localhost:9092")
        );
        assert_eq!(
            config.worker_config().key_converter_class(),
            Some("JsonConverter")
        );
    }

    #[test]
    fn test_standalone_config_def() {
        let config_def = StandaloneConfig::config_def();
        assert!(config_def.contains_key(OFFSET_STORAGE_FILE_FILENAME_CONFIG));
        // Also includes base worker configs
        assert!(config_def.contains_key(super::super::worker_config::BOOTSTRAP_SERVERS_CONFIG));
    }

    #[test]
    fn test_standalone_config_defaults() {
        let props = create_basic_standalone_props();
        let config = StandaloneConfig::new(props).unwrap();

        // Uses worker config defaults for common settings
        assert_eq!(
            config.worker_config().offset_commit_interval_ms(),
            super::super::worker_config::OFFSET_FLUSH_INTERVAL_MS_DEFAULT
        );
        assert_eq!(
            config.worker_config().topic_tracking_enable(),
            super::super::worker_config::TOPIC_TRACKING_ENABLE_DEFAULT
        );
    }
}
