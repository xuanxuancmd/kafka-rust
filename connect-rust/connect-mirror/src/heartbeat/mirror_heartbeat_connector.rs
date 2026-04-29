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

//! MirrorHeartbeatConnector - Source connector for MirrorMaker 2 heartbeat emission.
//!
//! This module provides the MirrorHeartbeatConnector class that emits heartbeat records
//! to the target cluster to monitor the health and connectivity of the replication process.
//!
//! The connector:
//! - Emits heartbeat records containing source and target cluster aliases
//! - Creates a single task for heartbeat emission
//! - Heartbeat records help determine reachability and latency between clusters
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorHeartbeatConnector

use std::collections::HashMap;
use std::sync::Arc;

use common_trait::config::{Config, ConfigDef, ConfigValueEntry};
use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext};
use connect_api::errors::ConnectError;
use connect_api::source::{ConnectorTransactionBoundaries, ExactlyOnceSupport, SourceConnector};
use connect_mirror_client::ReplicationPolicy;
use kafka_clients_mock::MockAdmin;
use serde_json::Value;

use crate::config::MirrorConnectorConfig;

use super::mirror_heartbeat_config::{
    MirrorHeartbeatConfig, EMIT_HEARTBEATS_ENABLED_CONFIG, EMIT_HEARTBEATS_ENABLED_DEFAULT,
    EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG, TASK_INDEX_CONFIG,
};

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorHeartbeatConnector
// ============================================================================

/// Number of partitions for heartbeats topic.
/// Corresponds to Java: MirrorHeartbeatConnector.NUM_HEARTBEATS_PARTITIONS
pub const NUM_HEARTBEATS_PARTITIONS: i32 = 1;

// ============================================================================
// MirrorHeartbeatConnectorConfigDef
// ============================================================================

/// Configuration definition for MirrorHeartbeatConnector.
struct MirrorHeartbeatConnectorConfigDef;

impl ConfigDef for MirrorHeartbeatConnectorConfigDef {
    fn config_def(&self) -> HashMap<String, ConfigValueEntry> {
        let mut defs = HashMap::new();

        defs.insert(
            EMIT_HEARTBEATS_ENABLED_CONFIG.to_string(),
            ConfigValueEntry::new(
                EMIT_HEARTBEATS_ENABLED_CONFIG,
                common_trait::config::ConfigDefType::Boolean,
                Some(serde_json::Value::Bool(EMIT_HEARTBEATS_ENABLED_DEFAULT)),
                common_trait::config::ConfigDefImportance::High,
                common_trait::config::ConfigDefWidth::Short,
                "Whether to emit heartbeat records",
            ),
        );

        defs.insert(
            EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG.to_string(),
            ConfigValueEntry::new(
                EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG,
                common_trait::config::ConfigDefType::Long,
                Some(serde_json::Value::Number(1.into())),
                common_trait::config::ConfigDefImportance::High,
                common_trait::config::ConfigDefWidth::Short,
                "Interval in seconds for emitting heartbeats",
            ),
        );

        defs
    }
}

static MIRROR_HEARTBEAT_CONNECTOR_CONFIG_DEF: MirrorHeartbeatConnectorConfigDef =
    MirrorHeartbeatConnectorConfigDef;

// ============================================================================
// MirrorHeartbeatConnector
// ============================================================================

/// Source connector for MirrorMaker 2 heartbeat emission.
///
/// The MirrorHeartbeatConnector is responsible for:
/// - Emitting heartbeat records to the target cluster
/// - Monitoring the health and connectivity of the replication process
/// - Creating a single task for heartbeat emission
///
/// Heartbeat records contain:
/// - Source cluster alias
/// - Target cluster alias
/// - Timestamp
///
/// These records help determine reachability and latency between clusters.
/// The connector typically creates only one task (single task strategy).
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorHeartbeatConnector
pub struct MirrorHeartbeatConnector {
    /// Connector context
    context: Option<Box<dyn ConnectorContext>>,
    /// MirrorHeartbeatConfig configuration
    config: Option<MirrorHeartbeatConfig>,
    /// Target cluster admin client
    target_admin: Arc<MockAdmin>,
    /// Flag indicating connector has started
    started: bool,
}

impl std::fmt::Debug for MirrorHeartbeatConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorHeartbeatConnector")
            .field("config", &self.config)
            .field("started", &self.started)
            .finish()
    }
}

impl MirrorHeartbeatConnector {
    /// Creates a new MirrorHeartbeatConnector.
    pub fn new() -> Self {
        MirrorHeartbeatConnector {
            context: None,
            config: None,
            target_admin: Arc::new(MockAdmin::new()),
            started: false,
        }
    }

    /// Creates a MirrorHeartbeatConnector with pre-configured admin client.
    pub fn with_admin_client(target_admin: Arc<MockAdmin>) -> Self {
        MirrorHeartbeatConnector {
            context: None,
            config: None,
            target_admin,
            started: false,
        }
    }

    /// Returns the target admin client.
    pub fn target_admin(&self) -> Arc<MockAdmin> {
        self.target_admin.clone()
    }

    /// Returns whether the connector is enabled.
    fn is_enabled(&self) -> bool {
        self.config
            .as_ref()
            .map(|c| c.emit_heartbeats_enabled())
            .unwrap_or(true)
    }

    /// Returns the replication policy.
    pub fn replication_policy(&self) -> Option<Arc<dyn ReplicationPolicy>> {
        self.config.as_ref().map(|c| c.replication_policy())
    }
}

impl Default for MirrorHeartbeatConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Versioned for MirrorHeartbeatConnector {
    fn version() -> &'static str {
        "1.0.0"
    }
}

impl Connector for MirrorHeartbeatConnector {
    fn context(&self) -> &dyn ConnectorContext {
        self.context
            .as_ref()
            .map(|c| c.as_ref())
            .unwrap_or_else(|| panic!("ConnectorContext not initialized"))
    }

    fn initialize(&mut self, context: Box<dyn ConnectorContext>) {
        self.context = Some(context);
    }

    fn initialize_with_task_configs(
        &mut self,
        context: Box<dyn ConnectorContext>,
        _task_configs: Vec<HashMap<String, String>>,
    ) {
        self.context = Some(context);
    }

    fn start(&mut self, props: HashMap<String, String>) {
        // Create configuration from properties
        self.config = Some(MirrorHeartbeatConfig::new(props.clone()));

        // Set started flag
        self.started = true;
    }

    fn stop(&mut self) {
        // Clear state
        self.started = false;
    }

    fn task_class(&self) -> &'static str {
        // Return the task class name
        // Corresponds to Java: MirrorHeartbeatConnector.taskClass()
        "MirrorHeartbeatTask"
    }

    fn task_configs(&self, max_tasks: i32) -> Result<Vec<HashMap<String, String>>, ConnectError> {
        // Corresponds to Java: MirrorHeartbeatConnector.taskConfigs(int maxTasks)
        // MirrorHeartbeatConnector typically creates a single task

        // Check if heartbeat emission is disabled
        if !self.is_enabled() {
            return Ok(Vec::new());
        }

        // Check if emit heartbeats interval is zero (disabled)
        if let Some(config) = &self.config {
            if config.emit_heartbeats_interval().is_zero() {
                return Ok(Vec::new());
            }
        }

        // Heartbeat connector creates exactly one task (single task strategy)
        // Even if max_tasks > 1, we still only create one heartbeat task
        if max_tasks <= 0 {
            return Ok(Vec::new());
        }

        // Create single task configuration
        let mut task_props = HashMap::new();

        // Add task index (always 0 for single task)
        task_props.insert(TASK_INDEX_CONFIG.to_string(), "0".to_string());

        // Add source and target cluster aliases
        if let Some(config) = &self.config {
            task_props.insert(
                crate::config::SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
                config.source_cluster_alias().to_string(),
            );
            task_props.insert(
                crate::config::TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
                config.target_cluster_alias().to_string(),
            );
        }

        Ok(vec![task_props])
    }

    fn validate(&self, configs: HashMap<String, String>) -> Config {
        // Validate configuration
        // Corresponds to Java: MirrorHeartbeatConnector.validate(Map<String, String> connectorConfig)

        let mut config_values = Vec::new();

        // Basic validation - check required fields
        if configs
            .get(crate::config::SOURCE_CLUSTER_ALIAS_CONFIG)
            .is_none()
        {
            let mut cv =
                common_trait::config::ConfigValue::new(crate::config::SOURCE_CLUSTER_ALIAS_CONFIG);
            cv.add_error_message("Missing required configuration");
            config_values.push(cv);
        }

        if configs
            .get(crate::config::TARGET_CLUSTER_ALIAS_CONFIG)
            .is_none()
        {
            let mut cv =
                common_trait::config::ConfigValue::new(crate::config::TARGET_CLUSTER_ALIAS_CONFIG);
            cv.add_error_message("Missing required configuration");
            config_values.push(cv);
        }

        Config::new(config_values)
    }

    fn config(&self) -> &'static dyn ConfigDef {
        &MIRROR_HEARTBEAT_CONNECTOR_CONFIG_DEF
    }
}

impl SourceConnector for MirrorHeartbeatConnector {
    fn exactly_once_support(
        &self,
        _connector_config: HashMap<String, String>,
    ) -> ExactlyOnceSupport {
        // MirrorHeartbeatConnector does not support exactly-once semantics
        // Corresponds to Java: MirrorHeartbeatConnector.exactlyOnceSupport()
        ExactlyOnceSupport::Unsupported
    }

    fn can_define_transaction_boundaries(
        &self,
        _connector_config: HashMap<String, String>,
    ) -> ConnectorTransactionBoundaries {
        // MirrorHeartbeatConnector uses coordinator-defined boundaries
        // Corresponds to Java: MirrorHeartbeatConnector.canDefineTransactionBoundaries()
        ConnectorTransactionBoundaries::CoordinatorDefined
    }

    fn alter_offsets(
        &self,
        _connector_config: HashMap<String, String>,
        _offsets: HashMap<HashMap<String, Value>, HashMap<String, Value>>,
    ) -> Result<bool, ConnectError> {
        // MirrorHeartbeatConnector does not support external offset management
        // Corresponds to Java: MirrorHeartbeatConnector.alterOffsets()
        Ok(false)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NAME_CONFIG, SOURCE_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_CONFIG};

    fn create_test_config() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert(
            SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
            "backup".to_string(),
        );
        props.insert(
            TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
            "primary".to_string(),
        );
        props.insert(
            NAME_CONFIG.to_string(),
            "mirror-heartbeat-connector".to_string(),
        );
        props
    }

    #[test]
    fn test_new_connector() {
        let connector = MirrorHeartbeatConnector::new();
        assert!(!connector.started);
        assert!(connector.config.is_none());
    }

    #[test]
    fn test_with_admin_client() {
        let target_admin = Arc::new(MockAdmin::new());
        let connector = MirrorHeartbeatConnector::with_admin_client(target_admin.clone());

        assert!(Arc::ptr_eq(&connector.target_admin(), &target_admin));
    }

    #[test]
    fn test_version() {
        assert_eq!(MirrorHeartbeatConnector::version(), "1.0.0");
    }

    #[test]
    fn test_task_class() {
        let connector = MirrorHeartbeatConnector::new();
        assert_eq!(connector.task_class(), "MirrorHeartbeatTask");
    }

    #[test]
    fn test_exactly_once_support() {
        let connector = MirrorHeartbeatConnector::new();
        let support = connector.exactly_once_support(HashMap::new());
        assert_eq!(support, ExactlyOnceSupport::Unsupported);
    }

    #[test]
    fn test_can_define_transaction_boundaries() {
        let connector = MirrorHeartbeatConnector::new();
        let boundaries = connector.can_define_transaction_boundaries(HashMap::new());
        assert_eq!(
            boundaries,
            ConnectorTransactionBoundaries::CoordinatorDefined
        );
    }

    #[test]
    fn test_task_configs_not_started() {
        let connector = MirrorHeartbeatConnector::new();
        // Connector not started, but defaults to enabled=true
        // Should still return a task config since defaults apply
        let result = connector.task_configs(1);
        assert!(result.is_ok());
        // With no config, defaults to enabled, so returns one task
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_task_configs_disabled() {
        let mut connector = MirrorHeartbeatConnector::new();
        let mut props = create_test_config();
        props.insert(
            EMIT_HEARTBEATS_ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );
        connector.start(props);

        let configs = connector.task_configs(1).unwrap();
        assert!(configs.is_empty());
    }

    #[test]
    fn test_task_configs_single_task() {
        let mut connector = MirrorHeartbeatConnector::new();
        connector.start(create_test_config());

        // Request 3 tasks but heartbeat connector always creates 1
        let configs = connector.task_configs(3).unwrap();
        assert_eq!(configs.len(), 1);

        // Check task config contains required fields
        let task_config = &configs[0];
        assert!(task_config.contains_key(TASK_INDEX_CONFIG));
        assert!(task_config.contains_key(SOURCE_CLUSTER_ALIAS_CONFIG));
        assert!(task_config.contains_key(TARGET_CLUSTER_ALIAS_CONFIG));
        assert_eq!(task_config.get(TASK_INDEX_CONFIG).unwrap(), "0");
    }

    #[test]
    fn test_task_configs_zero_max_tasks() {
        let connector = MirrorHeartbeatConnector::new();
        let configs = connector.task_configs(0).unwrap();
        assert!(configs.is_empty());
    }

    #[test]
    fn test_start_stop_cycle() {
        let mut connector = MirrorHeartbeatConnector::new();
        let props = create_test_config();

        connector.start(props);
        assert!(connector.started);
        assert!(connector.config.is_some());

        connector.stop();
        assert!(!connector.started);
    }

    #[test]
    fn test_validate_missing_source_alias() {
        let connector = MirrorHeartbeatConnector::new();
        let mut props = HashMap::new();
        props.insert(
            TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
            "primary".to_string(),
        );

        let config = connector.validate(props);
        let has_source_error = config
            .config_values()
            .iter()
            .any(|cv| cv.name() == SOURCE_CLUSTER_ALIAS_CONFIG && !cv.error_messages().is_empty());
        assert!(has_source_error);
    }

    #[test]
    fn test_validate_missing_target_alias() {
        let connector = MirrorHeartbeatConnector::new();
        let mut props = HashMap::new();
        props.insert(
            SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
            "backup".to_string(),
        );

        let config = connector.validate(props);
        let has_target_error = config
            .config_values()
            .iter()
            .any(|cv| cv.name() == TARGET_CLUSTER_ALIAS_CONFIG && !cv.error_messages().is_empty());
        assert!(has_target_error);
    }

    #[test]
    fn test_is_enabled_default() {
        let connector = MirrorHeartbeatConnector::new();
        // No config, should default to true
        assert!(connector.is_enabled());
    }

    #[test]
    fn test_is_enabled_with_config() {
        let mut connector = MirrorHeartbeatConnector::new();
        let mut props = create_test_config();
        props.insert(
            EMIT_HEARTBEATS_ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );
        connector.start(props);
        assert!(!connector.is_enabled());
    }

    #[test]
    fn test_replication_policy() {
        let mut connector = MirrorHeartbeatConnector::new();
        connector.start(create_test_config());

        let policy = connector.replication_policy();
        assert!(policy.is_some());
        // DefaultReplicationPolicy formats remote topic as "source.topic"
        let remote_topic = policy.unwrap().format_remote_topic("backup", "test-topic");
        assert_eq!(remote_topic, "backup.test-topic");
    }

    #[test]
    fn test_alter_offsets() {
        let connector = MirrorHeartbeatConnector::new();
        let result = connector.alter_offsets(HashMap::new(), HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_initialize_with_task_configs() {
        let mut connector = MirrorHeartbeatConnector::new();

        // Create a mock context
        struct MockContext;
        impl ConnectorContext for MockContext {
            fn request_task_reconfiguration(&mut self) {}
            fn raise_error(&mut self, _error: ConnectError) {}
        }

        let context = Box::new(MockContext);
        let task_configs = vec![HashMap::from([(
            TASK_INDEX_CONFIG.to_string(),
            "0".to_string(),
        )])];

        connector.initialize_with_task_configs(context, task_configs);

        assert!(connector.context.is_some());
    }
}
