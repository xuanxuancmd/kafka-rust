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

//! Distributed configuration for Kafka Connect.
//!
//! This module provides the DistributedConfig class that defines configuration
//! options specific to distributed mode Kafka Connect workers.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.distributed.DistributedConfig

use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType};
use common_trait::errors::ConfigException;
use serde_json::Value;
use std::collections::HashMap;

use super::worker_config::WorkerConfig;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java DistributedConfig
// ============================================================================

/// Configuration key for group ID.
/// Corresponds to Java: DistributedConfig.GROUP_ID_CONFIG
pub const GROUP_ID_CONFIG: &str = "group.id";

/// Configuration key for config storage topic.
/// Corresponds to Java: DistributedConfig.CONFIG_TOPIC_CONFIG
pub const CONFIG_TOPIC_CONFIG: &str = "config.storage.topic";

/// Configuration key for offset storage topic.
/// Corresponds to Java: DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG
pub const OFFSET_STORAGE_TOPIC_CONFIG: &str = "offset.storage.topic";

/// Configuration key for status storage topic.
/// Corresponds to Java: DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG
pub const STATUS_STORAGE_TOPIC_CONFIG: &str = "status.storage.topic";

/// Configuration key for heartbeat interval in milliseconds.
/// Corresponds to Java: DistributedConfig.HEARTBEAT_INTERVAL_MS_CONFIG
pub const HEARTBEAT_INTERVAL_MS_CONFIG: &str = "heartbeat.interval.ms";

/// Default value for heartbeat interval in milliseconds.
/// Corresponds to Java: DistributedConfig.HEARTBEAT_INTERVAL_MS_DEFAULT
pub const HEARTBEAT_INTERVAL_MS_DEFAULT: i64 = 3000;

/// Configuration key for session timeout in milliseconds.
/// Corresponds to Java: DistributedConfig.SESSION_TIMEOUT_MS_CONFIG
pub const SESSION_TIMEOUT_MS_CONFIG: &str = "session.timeout.ms";

/// Default value for session timeout in milliseconds.
/// Corresponds to Java: DistributedConfig.SESSION_TIMEOUT_MS_DEFAULT
pub const SESSION_TIMEOUT_MS_DEFAULT: i64 = 45000;

/// Configuration key for rebalance timeout in milliseconds.
/// Corresponds to Java: DistributedConfig.REBALANCE_TIMEOUT_MS_CONFIG
pub const REBALANCE_TIMEOUT_MS_CONFIG: &str = "rebalance.timeout.ms";

/// Default value for rebalance timeout in milliseconds.
/// Corresponds to Java: DistributedConfig.REBALANCE_TIMEOUT_MS_DEFAULT
pub const REBALANCE_TIMEOUT_MS_DEFAULT: i64 = 300000;

/// Configuration key for worker sync timeout in milliseconds.
/// Corresponds to Java: DistributedConfig.WORKER_SYNC_TIMEOUT_MS_CONFIG
pub const WORKER_SYNC_TIMEOUT_MS_CONFIG: &str = "worker.sync.timeout.ms";

/// Default value for worker sync timeout in milliseconds.
/// Corresponds to Java: DistributedConfig.WORKER_SYNC_TIMEOUT_MS_DEFAULT
pub const WORKER_SYNC_TIMEOUT_MS_DEFAULT: i64 = 3000;

/// Configuration key for worker unsync backoff in milliseconds.
/// Corresponds to Java: DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_CONFIG
pub const WORKER_UNSYNC_BACKOFF_MS_CONFIG: &str = "worker.unsync.backoff.ms";

/// Default value for worker unsync backoff in milliseconds.
/// Corresponds to Java: DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_DEFAULT
pub const WORKER_UNSYNC_BACKOFF_MS_DEFAULT: i64 = 300000;

/// Configuration key for scheduled rebalance max delay in milliseconds.
/// Corresponds to Java: DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG
pub const SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG: &str = "scheduled.rebalance.max.delay.ms";

/// Default value for scheduled rebalance max delay in milliseconds.
/// Corresponds to Java: DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT
pub const SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT: i64 = 300000;

/// Configuration key for metadata recovery strategy.
/// Corresponds to Java: DistributedConfig.METADATA_RECOVERY_STRATEGY_CONFIG
pub const METADATA_RECOVERY_STRATEGY_CONFIG: &str = "metadata.recovery.strategy";

/// Default value for metadata recovery strategy.
/// Corresponds to Java: DistributedConfig.METADATA_RECOVERY_STRATEGY_DEFAULT
pub const METADATA_RECOVERY_STRATEGY_DEFAULT: &str = "NONE";

/// Configuration key for metadata cluster check enable.
/// Corresponds to Java: DistributedConfig.METADATA_CLUSTER_CHECK_ENABLE_CONFIG
pub const METADATA_CLUSTER_CHECK_ENABLE_CONFIG: &str = "metadata.cluster.check.enable";

/// Default value for metadata cluster check enable.
/// Corresponds to Java: DistributedConfig.METADATA_CLUSTER_CHECK_ENABLE_DEFAULT
pub const METADATA_CLUSTER_CHECK_ENABLE_DEFAULT: bool = true;

/// Configuration key for config storage replication factor.
/// Corresponds to Java: DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG
pub const CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG: &str = "config.storage.replication.factor";

/// Default value for config storage replication factor.
/// Corresponds to Java: DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_DEFAULT
pub const CONFIG_STORAGE_REPLICATION_FACTOR_DEFAULT: i64 = 3;

/// Configuration key for offset storage replication factor.
/// Corresponds to Java: DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG
pub const OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG: &str = "offset.storage.replication.factor";

/// Default value for offset storage replication factor.
/// Corresponds to Java: DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_DEFAULT
pub const OFFSET_STORAGE_REPLICATION_FACTOR_DEFAULT: i64 = 3;

/// Configuration key for status storage replication factor.
/// Corresponds to Java: DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG
pub const STATUS_STORAGE_REPLICATION_FACTOR_CONFIG: &str = "status.storage.replication.factor";

/// Default value for status storage replication factor.
/// Corresponds to Java: DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_DEFAULT
pub const STATUS_STORAGE_REPLICATION_FACTOR_DEFAULT: i64 = 3;

// ============================================================================
// DistributedConfig
// ============================================================================

/// Configuration for distributed mode Kafka Connect workers.
///
/// This class extends WorkerConfig and adds configuration options specific
/// to distributed mode, including internal topic storage, group management,
/// and rebalance parameters.
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.distributed.DistributedConfig
#[derive(Debug, Clone)]
pub struct DistributedConfig {
    worker_config: WorkerConfig,
}

impl DistributedConfig {
    /// Creates a new DistributedConfig with the given configuration properties.
    ///
    /// Corresponds to Java: DistributedConfig(Map<?, ?> props)
    pub fn new(props: HashMap<String, Value>) -> Result<Self, ConfigException> {
        let config = DistributedConfig {
            worker_config: WorkerConfig::new_unvalidated(props),
        };
        config.validate()?;
        Ok(config)
    }

    /// Returns the configuration definition for distributed mode.
    ///
    /// This includes both the base WorkerConfig definitions and distributed-specific ones.
    /// Corresponds to Java: DistributedConfig.configDef()
    pub fn config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        let base_def = WorkerConfig::base_config_def();

        ConfigDefBuilder::from_map(base_def)
            // Group ID - required for distributed cluster identification
            .define(
                GROUP_ID_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "A unique string that identifies the Connect cluster group this worker belongs to.",
            )
            // Config storage topic - required for storing connector configurations
            .define(
                CONFIG_TOPIC_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The name of the topic in which to store connector and task configurations.",
            )
            // Offset storage topic - required for storing source connector offsets
            .define(
                OFFSET_STORAGE_TOPIC_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The name of the topic in which to store source connector offsets.",
            )
            // Status storage topic - required for storing connector/task status
            .define(
                STATUS_STORAGE_TOPIC_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The name of the topic in which to store connector and task status.",
            )
            // Heartbeat interval
            .define(
                HEARTBEAT_INTERVAL_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(HEARTBEAT_INTERVAL_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                "The expected time between heartbeats to the group coordinator.",
            )
            // Session timeout
            .define(
                SESSION_TIMEOUT_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(SESSION_TIMEOUT_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                "The timeout for detecting worker failures.",
            )
            // Rebalance timeout
            .define(
                REBALANCE_TIMEOUT_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(REBALANCE_TIMEOUT_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                "The maximum time allowed for each worker to join the group after a rebalance.",
            )
            // Worker sync timeout
            .define(
                WORKER_SYNC_TIMEOUT_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(WORKER_SYNC_TIMEOUT_MS_DEFAULT.into())),
                ConfigDefImportance::Low,
                "The timeout for a worker to resynchronize configurations with other workers.",
            )
            // Worker unsync backoff
            .define(
                WORKER_UNSYNC_BACKOFF_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(WORKER_UNSYNC_BACKOFF_MS_DEFAULT.into())),
                ConfigDefImportance::Low,
                "The time a worker waits before rejoining if it fails to catch up.",
            )
            // Scheduled rebalance max delay
            .define(
                SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(
                    SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT.into(),
                )),
                ConfigDefImportance::Low,
                "The maximum delay for a scheduled rebalance.",
            )
            // Metadata recovery strategy
            .define(
                METADATA_RECOVERY_STRATEGY_CONFIG,
                ConfigDefType::String,
                Some(Value::String(
                    METADATA_RECOVERY_STRATEGY_DEFAULT.to_string(),
                )),
                ConfigDefImportance::Low,
                "The strategy for metadata recovery.",
            )
            // Metadata cluster check enable
            .define(
                METADATA_CLUSTER_CHECK_ENABLE_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(METADATA_CLUSTER_CHECK_ENABLE_DEFAULT)),
                ConfigDefImportance::Low,
                "Enable metadata cluster checks.",
            )
            // Config storage replication factor
            .define(
                CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(
                    CONFIG_STORAGE_REPLICATION_FACTOR_DEFAULT.into(),
                )),
                ConfigDefImportance::Medium,
                "Replication factor for the config storage topic.",
            )
            // Offset storage replication factor
            .define(
                OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(
                    OFFSET_STORAGE_REPLICATION_FACTOR_DEFAULT.into(),
                )),
                ConfigDefImportance::Medium,
                "Replication factor for the offset storage topic.",
            )
            // Status storage replication factor
            .define(
                STATUS_STORAGE_REPLICATION_FACTOR_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(
                    STATUS_STORAGE_REPLICATION_FACTOR_DEFAULT.into(),
                )),
                ConfigDefImportance::Medium,
                "Replication factor for the status storage topic.",
            )
            .build()
    }

    /// Validates the configuration values.
    pub fn validate(&self) -> Result<(), ConfigException> {
        // Validate required distributed-specific configs
        if self.worker_config.get(GROUP_ID_CONFIG).is_none() {
            return Err(ConfigException::new(format!(
                "Required configuration {} is missing",
                GROUP_ID_CONFIG
            )));
        }
        if self.worker_config.get(CONFIG_TOPIC_CONFIG).is_none() {
            return Err(ConfigException::new(format!(
                "Required configuration {} is missing",
                CONFIG_TOPIC_CONFIG
            )));
        }
        if self
            .worker_config
            .get(OFFSET_STORAGE_TOPIC_CONFIG)
            .is_none()
        {
            return Err(ConfigException::new(format!(
                "Required configuration {} is missing",
                OFFSET_STORAGE_TOPIC_CONFIG
            )));
        }
        if self
            .worker_config
            .get(STATUS_STORAGE_TOPIC_CONFIG)
            .is_none()
        {
            return Err(ConfigException::new(format!(
                "Required configuration {} is missing",
                STATUS_STORAGE_CONFIG
            )));
        }

        // Validate base worker configs
        self.worker_config.validate()?;
        Ok(())
    }

    /// Returns the group ID for the distributed cluster.
    ///
    /// Corresponds to Java: DistributedConfig.groupId()
    pub fn group_id(&self) -> Option<&str> {
        self.worker_config.get_string(GROUP_ID_CONFIG)
    }

    /// Returns the config storage topic name.
    ///
    /// Corresponds to Java: DistributedConfig.configTopic()
    pub fn config_topic(&self) -> Option<&str> {
        self.worker_config.get_string(CONFIG_TOPIC_CONFIG)
    }

    /// Returns the offset storage topic name.
    ///
    /// Corresponds to Java: DistributedConfig.offsetTopic()
    pub fn offset_topic(&self) -> Option<&str> {
        self.worker_config.get_string(OFFSET_STORAGE_TOPIC_CONFIG)
    }

    /// Returns the status storage topic name.
    ///
    /// Corresponds to Java: DistributedConfig.statusTopic()
    pub fn status_topic(&self) -> Option<&str> {
        self.worker_config.get_string(STATUS_STORAGE_TOPIC_CONFIG)
    }

    /// Returns the heartbeat interval in milliseconds.
    ///
    /// Corresponds to Java: DistributedConfig.heartbeatInterval()
    pub fn heartbeat_interval_ms(&self) -> i64 {
        self.worker_config
            .get_long(HEARTBEAT_INTERVAL_MS_CONFIG)
            .unwrap_or(HEARTBEAT_INTERVAL_MS_DEFAULT)
    }

    /// Returns the session timeout in milliseconds.
    ///
    /// Corresponds to Java: DistributedConfig.sessionTimeout()
    pub fn session_timeout_ms(&self) -> i64 {
        self.worker_config
            .get_long(SESSION_TIMEOUT_MS_CONFIG)
            .unwrap_or(SESSION_TIMEOUT_MS_DEFAULT)
    }

    /// Returns the rebalance timeout in milliseconds.
    ///
    /// Corresponds to Java: DistributedConfig.rebalanceTimeout()
    pub fn rebalance_timeout_ms(&self) -> i64 {
        self.worker_config
            .get_long(REBALANCE_TIMEOUT_MS_CONFIG)
            .unwrap_or(REBALANCE_TIMEOUT_MS_DEFAULT)
    }

    /// Returns the worker sync timeout in milliseconds.
    ///
    /// Corresponds to Java: DistributedConfig.workerSyncTimeout()
    pub fn worker_sync_timeout_ms(&self) -> i64 {
        self.worker_config
            .get_long(WORKER_SYNC_TIMEOUT_MS_CONFIG)
            .unwrap_or(WORKER_SYNC_TIMEOUT_MS_DEFAULT)
    }

    /// Returns the worker unsync backoff in milliseconds.
    ///
    /// Corresponds to Java: DistributedConfig.workerUnsyncBackoff()
    pub fn worker_unsync_backoff_ms(&self) -> i64 {
        self.worker_config
            .get_long(WORKER_UNSYNC_BACKOFF_MS_CONFIG)
            .unwrap_or(WORKER_UNSYNC_BACKOFF_MS_DEFAULT)
    }

    /// Returns the scheduled rebalance max delay in milliseconds.
    pub fn scheduled_rebalance_max_delay_ms(&self) -> i64 {
        self.worker_config
            .get_long(SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG)
            .unwrap_or(SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT)
    }

    /// Returns the metadata recovery strategy.
    pub fn metadata_recovery_strategy(&self) -> &str {
        self.worker_config
            .get_string(METADATA_RECOVERY_STRATEGY_CONFIG)
            .unwrap_or(METADATA_RECOVERY_STRATEGY_DEFAULT)
    }

    /// Returns whether metadata cluster check is enabled.
    pub fn metadata_cluster_check_enable(&self) -> bool {
        self.worker_config
            .get_bool(METADATA_CLUSTER_CHECK_ENABLE_CONFIG)
            .unwrap_or(METADATA_CLUSTER_CHECK_ENABLE_DEFAULT)
    }

    /// Returns the config storage replication factor.
    pub fn config_storage_replication_factor(&self) -> i64 {
        self.worker_config
            .get_long(CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG)
            .unwrap_or(CONFIG_STORAGE_REPLICATION_FACTOR_DEFAULT)
    }

    /// Returns the offset storage replication factor.
    pub fn offset_storage_replication_factor(&self) -> i64 {
        self.worker_config
            .get_long(OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)
            .unwrap_or(OFFSET_STORAGE_REPLICATION_FACTOR_DEFAULT)
    }

    /// Returns the status storage replication factor.
    pub fn status_storage_replication_factor(&self) -> i64 {
        self.worker_config
            .get_long(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG)
            .unwrap_or(STATUS_STORAGE_REPLICATION_FACTOR_DEFAULT)
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

    /// Returns all original configuration values.
    pub fn originals(&self) -> &HashMap<String, Value> {
        self.worker_config.originals()
    }
}

// Fix: STATUS_STORAGE_CONFIG should reference STATUS_STORAGE_TOPIC_CONFIG
const STATUS_STORAGE_CONFIG: &str = STATUS_STORAGE_TOPIC_CONFIG;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_basic_distributed_props() -> HashMap<String, Value> {
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
            GROUP_ID_CONFIG.to_string(),
            Value::String("connect-cluster".to_string()),
        );
        props.insert(
            CONFIG_TOPIC_CONFIG.to_string(),
            Value::String("connect-configs".to_string()),
        );
        props.insert(
            OFFSET_STORAGE_TOPIC_CONFIG.to_string(),
            Value::String("connect-offsets".to_string()),
        );
        props.insert(
            STATUS_STORAGE_TOPIC_CONFIG.to_string(),
            Value::String("connect-status".to_string()),
        );
        props
    }

    #[test]
    fn test_distributed_config_basic() {
        let props = create_basic_distributed_props();
        let config = DistributedConfig::new(props).unwrap();

        assert_eq!(config.group_id(), Some("connect-cluster"));
        assert_eq!(config.config_topic(), Some("connect-configs"));
        assert_eq!(config.offset_topic(), Some("connect-offsets"));
        assert_eq!(config.status_topic(), Some("connect-status"));
    }

    #[test]
    fn test_distributed_config_defaults() {
        let props = create_basic_distributed_props();
        let config = DistributedConfig::new(props).unwrap();

        assert_eq!(
            config.heartbeat_interval_ms(),
            HEARTBEAT_INTERVAL_MS_DEFAULT
        );
        assert_eq!(config.session_timeout_ms(), SESSION_TIMEOUT_MS_DEFAULT);
        assert_eq!(config.rebalance_timeout_ms(), REBALANCE_TIMEOUT_MS_DEFAULT);
        assert_eq!(
            config.worker_sync_timeout_ms(),
            WORKER_SYNC_TIMEOUT_MS_DEFAULT
        );
        assert_eq!(
            config.config_storage_replication_factor(),
            CONFIG_STORAGE_REPLICATION_FACTOR_DEFAULT
        );
    }

    #[test]
    fn test_distributed_config_validation_missing_group_id() {
        let mut props = create_basic_distributed_props();
        props.remove(GROUP_ID_CONFIG);
        let result = DistributedConfig::new(props);
        assert!(result.is_err());
    }

    #[test]
    fn test_distributed_config_validation_missing_config_topic() {
        let mut props = create_basic_distributed_props();
        props.remove(CONFIG_TOPIC_CONFIG);
        let result = DistributedConfig::new(props);
        assert!(result.is_err());
    }

    #[test]
    fn test_distributed_config_custom_values() {
        let mut props = create_basic_distributed_props();
        props.insert(
            HEARTBEAT_INTERVAL_MS_CONFIG.to_string(),
            Value::Number(5000.into()),
        );
        props.insert(
            SESSION_TIMEOUT_MS_CONFIG.to_string(),
            Value::Number(60000.into()),
        );
        props.insert(
            REBALANCE_TIMEOUT_MS_CONFIG.to_string(),
            Value::Number(600000.into()),
        );

        let config = DistributedConfig::new(props).unwrap();
        assert_eq!(config.heartbeat_interval_ms(), 5000);
        assert_eq!(config.session_timeout_ms(), 60000);
        assert_eq!(config.rebalance_timeout_ms(), 600000);
    }

    #[test]
    fn test_distributed_config_def() {
        let config_def = DistributedConfig::config_def();
        assert!(config_def.contains_key(GROUP_ID_CONFIG));
        assert!(config_def.contains_key(CONFIG_TOPIC_CONFIG));
        assert!(config_def.contains_key(OFFSET_STORAGE_TOPIC_CONFIG));
        assert!(config_def.contains_key(STATUS_STORAGE_TOPIC_CONFIG));
        // Also includes base worker configs
        assert!(config_def.contains_key(super::super::worker_config::BOOTSTRAP_SERVERS_CONFIG));
    }

    #[test]
    fn test_worker_config_access() {
        let props = create_basic_distributed_props();
        let config = DistributedConfig::new(props).unwrap();

        // Can access worker config values through worker_config()
        assert_eq!(
            config.worker_config().bootstrap_servers(),
            Some("localhost:9092")
        );
    }
}
