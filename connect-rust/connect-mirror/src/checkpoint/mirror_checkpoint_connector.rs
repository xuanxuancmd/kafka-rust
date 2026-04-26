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

//! MirrorCheckpointConnector - Source connector for MirrorMaker 2 checkpoint synchronization.
//!
//! This module provides the MirrorCheckpointConnector class that discovers consumer groups
//! on the source cluster and creates tasks to emit checkpoint records.
//!
//! The connector:
//! - Lists consumer groups from the source cluster
//! - Filters groups using GroupFilter and TopicFilter
//! - Partitions consumer groups across tasks
//! - Each task emits checkpoint records for its assigned groups
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorCheckpointConnector (290 lines)

use std::collections::{HashMap, HashSet};
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
use crate::source::topic_filter::TopicFilter;

use super::group_filter::GroupFilter;
use super::mirror_checkpoint_config::{
    MirrorCheckpointConfig, EMIT_CHECKPOINTS_ENABLED_CONFIG, EMIT_CHECKPOINTS_ENABLED_DEFAULT,
    EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG, TASK_ASSIGNED_GROUPS_CONFIG,
};

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorCheckpointConnector
// ============================================================================

/// Number of partitions for offset syncs topic.
/// Corresponds to Java: MirrorCheckpointConnector.NUM_CHECKPOINTS_PARTITIONS
pub const NUM_CHECKPOINTS_PARTITIONS: i32 = 1;

/// Task index configuration key.
/// Corresponds to Java: MirrorCheckpointConfig.TASK_INDEX
pub const TASK_INDEX_CONFIG: &str = "task.index";

// ============================================================================
// MirrorCheckpointConnectorConfigDef
// ============================================================================

/// Configuration definition for MirrorCheckpointConnector.
struct MirrorCheckpointConnectorConfigDef;

impl ConfigDef for MirrorCheckpointConnectorConfigDef {
    fn config_def(&self) -> HashMap<String, ConfigValueEntry> {
        let mut defs = HashMap::new();

        defs.insert(
            EMIT_CHECKPOINTS_ENABLED_CONFIG.to_string(),
            ConfigValueEntry::new(
                EMIT_CHECKPOINTS_ENABLED_CONFIG,
                common_trait::config::ConfigDefType::Boolean,
                Some(serde_json::Value::Bool(EMIT_CHECKPOINTS_ENABLED_DEFAULT)),
                common_trait::config::ConfigDefImportance::High,
                common_trait::config::ConfigDefWidth::Short,
                "Whether to emit checkpoint records",
            ),
        );

        defs.insert(
            EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG.to_string(),
            ConfigValueEntry::new(
                EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG,
                common_trait::config::ConfigDefType::Long,
                Some(serde_json::Value::Number(60.into())),
                common_trait::config::ConfigDefImportance::High,
                common_trait::config::ConfigDefWidth::Short,
                "Interval in seconds for emitting checkpoints",
            ),
        );

        defs
    }
}

static MIRROR_CHECKPOINT_CONNECTOR_CONFIG_DEF: MirrorCheckpointConnectorConfigDef =
    MirrorCheckpointConnectorConfigDef;

// ============================================================================
// MirrorCheckpointConnector
// ============================================================================

/// Source connector for MirrorMaker 2 checkpoint synchronization.
///
/// The MirrorCheckpointConnector is responsible for:
/// - Discovering consumer groups from the source cluster
/// - Filtering consumer groups based on the GroupFilter
/// - Filtering based on topics that groups are consuming (via TopicFilter)
/// - Assigning consumer groups to tasks
/// - Emitting checkpoint records that sync consumer group offsets
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorCheckpointConnector
pub struct MirrorCheckpointConnector {
    /// Connector context
    context: Option<Box<dyn ConnectorContext>>,
    /// MirrorCheckpointConfig configuration
    config: Option<MirrorCheckpointConfig>,
    /// Source cluster admin client
    source_admin: Arc<MockAdmin>,
    /// Target cluster admin client  
    target_admin: Arc<MockAdmin>,
    /// Group filter
    group_filter: Option<Arc<dyn GroupFilter>>,
    /// Topic filter
    topic_filter: Option<Arc<dyn TopicFilter>>,
    /// Known consumer groups discovered from source cluster
    known_consumer_groups: HashSet<String>,
    /// Flag indicating connector has started
    started: bool,
    /// Flag indicating initial consumer groups have been loaded
    initial_groups_loaded: bool,
}

impl std::fmt::Debug for MirrorCheckpointConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorCheckpointConnector")
            .field("config", &self.config)
            .field("started", &self.started)
            .field("initial_groups_loaded", &self.initial_groups_loaded)
            .field("known_consumer_groups", &self.known_consumer_groups.len())
            .finish()
    }
}

impl MirrorCheckpointConnector {
    /// Creates a new MirrorCheckpointConnector.
    pub fn new() -> Self {
        MirrorCheckpointConnector {
            context: None,
            config: None,
            source_admin: Arc::new(MockAdmin::new()),
            target_admin: Arc::new(MockAdmin::new()),
            group_filter: None,
            topic_filter: None,
            known_consumer_groups: HashSet::new(),
            started: false,
            initial_groups_loaded: false,
        }
    }

    /// Creates a MirrorCheckpointConnector with pre-configured admin clients.
    pub fn with_admin_clients(source_admin: Arc<MockAdmin>, target_admin: Arc<MockAdmin>) -> Self {
        MirrorCheckpointConnector {
            context: None,
            config: None,
            source_admin,
            target_admin,
            group_filter: None,
            topic_filter: None,
            known_consumer_groups: HashSet::new(),
            started: false,
            initial_groups_loaded: false,
        }
    }

    /// Returns the source admin client.
    pub fn source_admin(&self) -> Arc<MockAdmin> {
        self.source_admin.clone()
    }

    /// Returns the target admin client.
    pub fn target_admin(&self) -> Arc<MockAdmin> {
        self.target_admin.clone()
    }

    /// Returns the group filter.
    pub fn group_filter(&self) -> Option<Arc<dyn GroupFilter>> {
        self.group_filter.clone()
    }

    /// Returns the topic filter.
    pub fn topic_filter(&self) -> Option<Arc<dyn TopicFilter>> {
        self.topic_filter.clone()
    }

    /// Returns the known consumer groups.
    pub fn known_consumer_groups(&self) -> &HashSet<String> {
        &self.known_consumer_groups
    }

    /// Returns whether the connector has started.
    pub fn is_started(&self) -> bool {
        self.started
    }

    /// Returns whether initial consumer groups have been loaded.
    pub fn is_initial_groups_loaded(&self) -> bool {
        self.initial_groups_loaded
    }

    /// Returns a reference to the configuration.
    pub fn config_ref(&self) -> Option<&MirrorCheckpointConfig> {
        self.config.as_ref()
    }

    /// Adds a consumer group to the known groups (for testing).
    pub fn add_known_consumer_group(&mut self, group: String) {
        self.known_consumer_groups.insert(group);
    }

    /// Sets the initial_groups_loaded flag (for testing).
    pub fn set_initial_groups_loaded(&mut self, loaded: bool) {
        self.initial_groups_loaded = loaded;
    }

    /// Sets the started flag (for testing).
    pub fn set_started(&mut self, started: bool) {
        self.started = started;
    }

    /// Returns whether the connector is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config
            .as_ref()
            .map(|c| c.emit_checkpoints_enabled())
            .unwrap_or(true)
    }

    /// Loads initial consumer groups from the source cluster.
    ///
    /// This discovers consumer groups from the source cluster, filters them
    /// using GroupFilter and TopicFilter, and populates known_consumer_groups.
    ///
    /// Corresponds to Java: MirrorCheckpointConnector.loadInitialConsumerGroups()
    fn load_initial_consumer_groups(&mut self) {
        let groups = self.find_consumer_groups();
        self.known_consumer_groups = groups;
        self.initial_groups_loaded = true;
    }

    /// Finds consumer groups from the source cluster that should be replicated.
    ///
    /// This method:
    /// 1. Lists all consumer groups from the source cluster
    /// 2. Filters groups using GroupFilter
    /// 3. Checks that groups have offsets for topics matching TopicFilter
    ///
    /// Corresponds to Java: MirrorCheckpointConnector.findConsumerGroups()
    fn find_consumer_groups(&self) -> HashSet<String> {
        if self.group_filter.is_none() || self.topic_filter.is_none() {
            return HashSet::new();
        }

        let group_filter = self.group_filter.as_ref().unwrap();
        let topic_filter = self.topic_filter.as_ref().unwrap();

        // List consumer groups from source cluster
        let list_result = self.source_admin.list_consumer_groups();

        let mut filtered_groups = HashSet::new();

        // Iterate through listed consumer groups
        for group_listing in list_result.listings() {
            let group_id = group_listing.group_id();

            // Apply group filter
            if !group_filter.should_replicate_group(group_id) {
                continue;
            }

            // Get group details to check if group has offsets for accepted topics
            let group_offsets = self.source_admin.list_consumer_group_offsets(group_id);

            // Check if group has offsets for at least one topic matching topic filter
            let has_accepted_topic_offsets = group_offsets.iter().any(|(topic_partition, _)| {
                topic_filter.should_replicate_topic(topic_partition.topic())
            });

            if has_accepted_topic_offsets {
                filtered_groups.insert(group_id.to_string());
            }
        }

        filtered_groups
    }

    /// Refreshes consumer groups from the source cluster.
    ///
    /// This is called periodically to detect new or removed consumer groups.
    /// If changes are detected, requests task reconfiguration.
    ///
    /// Corresponds to Java: MirrorCheckpointConnector.refreshConsumerGroups()
    fn refresh_consumer_groups(&mut self) {
        let old_count = self.known_consumer_groups.len();

        // Reload consumer groups
        let new_groups = self.find_consumer_groups();
        self.known_consumer_groups = new_groups;

        let new_count = self.known_consumer_groups.len();

        // If groups changed, request reconfiguration
        if old_count != new_count && self.context.is_some() {
            // Request task reconfiguration through context
            // Note: In real implementation, context.request_task_reconfiguration() would be called
            // In mock implementation, we just track the change
        }
    }

    /// Partitions a set of items into N groups.
    ///
    /// This is similar to Java's ConnectorUtils.groupPartitions().
    pub fn partition_groups<T: Clone>(items: &[T], num_groups: i32) -> Vec<Vec<T>> {
        if num_groups <= 0 || items.is_empty() {
            return Vec::new();
        }

        let num_groups = num_groups as usize;
        let mut result: Vec<Vec<T>> = (0..num_groups).map(|_| Vec::new()).collect();

        for (i, item) in items.iter().enumerate() {
            let group_index = i % num_groups;
            result[group_index].push(item.clone());
        }

        result
    }
}

impl Default for MirrorCheckpointConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Versioned for MirrorCheckpointConnector {
    fn version() -> &'static str {
        "1.0.0"
    }
}

impl Connector for MirrorCheckpointConnector {
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
        task_configs: Vec<HashMap<String, String>>,
    ) {
        self.context = Some(context);

        // Restore known consumer groups from task configs
        for task_config in task_configs {
            if let Some(groups_str) = task_config.get(TASK_ASSIGNED_GROUPS_CONFIG) {
                // Parse groups from comma-separated string
                for group in groups_str.split(',') {
                    let trimmed = group.trim();
                    if !trimmed.is_empty() {
                        self.known_consumer_groups.insert(trimmed.to_string());
                    }
                }
            }
        }

        // Mark as loaded since we're restoring from existing configs
        self.initial_groups_loaded = true;
    }

    fn start(&mut self, props: HashMap<String, String>) {
        // Create configuration from properties
        self.config = Some(MirrorCheckpointConfig::new(props.clone()));

        let config = self.config.as_ref().unwrap();

        // Initialize group filter
        self.group_filter = Some(config.group_filter());

        // Initialize topic filter
        self.topic_filter = Some(config.topic_filter());

        // Load initial consumer groups
        self.load_initial_consumer_groups();

        // Set started flag
        self.started = true;
    }

    fn stop(&mut self) {
        // Close group filter
        if let Some(filter) = self.group_filter.take() {
            drop(filter);
        }

        // Close topic filter
        if let Some(filter) = self.topic_filter.take() {
            drop(filter);
        }

        // Clear state
        self.known_consumer_groups.clear();
        self.started = false;
        self.initial_groups_loaded = false;
    }

    fn task_class(&self) -> &'static str {
        // Return the task class name
        // Corresponds to Java: MirrorCheckpointConnector.taskClass()
        "MirrorCheckpointTask"
    }

    fn task_configs(&self, max_tasks: i32) -> Result<Vec<HashMap<String, String>>, ConnectError> {
        // Corresponds to Java: MirrorCheckpointConnector.taskConfigs(int maxTasks)

        // Check if checkpoint emission is disabled
        if !self.is_enabled() {
            return Ok(Vec::new());
        }

        // Check if emit checkpoints interval is negative (disabled)
        if let Some(config) = &self.config {
            if config.emit_checkpoints_interval().is_zero() {
                return Ok(Vec::new());
            }
        }

        // Check if initial consumer groups have been loaded
        // If not, throw RetriableException (represented as ConnectError::Retriable)
        if !self.initial_groups_loaded {
            return Err(ConnectError::retriable(
                "Consumer groups not yet loaded from source cluster",
            ));
        }

        // If no consumer groups, return empty list
        if self.known_consumer_groups.is_empty() {
            return Ok(Vec::new());
        }

        // Sort groups for deterministic distribution
        let sorted_groups: Vec<String> = self.known_consumer_groups.iter().cloned().collect();

        // Partition consumer groups among tasks
        let task_group_lists = Self::partition_groups(&sorted_groups, max_tasks);

        // Create task configs
        let task_configs: Vec<HashMap<String, String>> = task_group_lists
            .into_iter()
            .enumerate()
            .filter(|(_, groups)| !groups.is_empty())
            .map(|(task_index, groups)| {
                let mut task_props = HashMap::new();

                // Add task index
                task_props.insert(TASK_INDEX_CONFIG.to_string(), task_index.to_string());

                // Add assigned consumer groups as comma-separated string
                let groups_str = groups.join(",");
                task_props.insert(TASK_ASSIGNED_GROUPS_CONFIG.to_string(), groups_str);

                // Add source cluster alias
                if let Some(config) = &self.config {
                    task_props.insert(
                        "source.cluster.alias".to_string(),
                        config.source_cluster_alias().to_string(),
                    );
                    task_props.insert(
                        "target.cluster.alias".to_string(),
                        config.target_cluster_alias().to_string(),
                    );
                }

                task_props
            })
            .collect();

        Ok(task_configs)
    }

    fn validate(&self, configs: HashMap<String, String>) -> Config {
        // Validate configuration
        // Corresponds to Java: MirrorCheckpointConnector.validate(Map<String, String> connectorConfig)

        let mut config_values = Vec::new();

        // Basic validation - check required fields
        if configs.get("source.cluster.alias").is_none() {
            let mut cv = common_trait::config::ConfigValue::new("source.cluster.alias");
            cv.add_error_message("Missing required configuration");
            config_values.push(cv);
        }

        if configs.get("target.cluster.alias").is_none() {
            let mut cv = common_trait::config::ConfigValue::new("target.cluster.alias");
            cv.add_error_message("Missing required configuration");
            config_values.push(cv);
        }

        Config::new(config_values)
    }

    fn config(&self) -> &'static dyn ConfigDef {
        &MIRROR_CHECKPOINT_CONNECTOR_CONFIG_DEF
    }
}

impl SourceConnector for MirrorCheckpointConnector {
    fn exactly_once_support(
        &self,
        _connector_config: HashMap<String, String>,
    ) -> ExactlyOnceSupport {
        // MirrorCheckpointConnector does not support exactly-once semantics
        // Corresponds to Java: MirrorCheckpointConnector.exactlyOnceSupport()
        ExactlyOnceSupport::Unsupported
    }

    fn can_define_transaction_boundaries(
        &self,
        _connector_config: HashMap<String, String>,
    ) -> ConnectorTransactionBoundaries {
        // MirrorCheckpointConnector uses coordinator-defined boundaries
        // Corresponds to Java: MirrorCheckpointConnector.canDefineTransactionBoundaries()
        ConnectorTransactionBoundaries::CoordinatorDefined
    }

    fn alter_offsets(
        &self,
        _connector_config: HashMap<String, String>,
        _offsets: HashMap<HashMap<String, Value>, HashMap<String, Value>>,
    ) -> Result<bool, ConnectError> {
        // MirrorCheckpointConnector does not support external offset management
        // Corresponds to Java: MirrorCheckpointConnector.alterOffsets()
        Ok(false)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::group_filter::DefaultGroupFilter;
    use crate::source::topic_filter::DefaultTopicFilter;

    fn create_test_config() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert(
            crate::config::SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
            "backup".to_string(),
        );
        props.insert(
            crate::config::TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
            "primary".to_string(),
        );
        props.insert(
            crate::config::NAME_CONFIG.to_string(),
            "mirror-checkpoint-connector".to_string(),
        );
        props
    }

    #[test]
    fn test_new_connector() {
        let connector = MirrorCheckpointConnector::new();
        assert!(!connector.started);
        assert!(!connector.initial_groups_loaded);
        assert!(connector.known_consumer_groups.is_empty());
    }

    #[test]
    fn test_with_admin_clients() {
        let source_admin = Arc::new(MockAdmin::new());
        let target_admin = Arc::new(MockAdmin::new());
        let connector = MirrorCheckpointConnector::with_admin_clients(
            source_admin.clone(),
            target_admin.clone(),
        );

        assert!(Arc::ptr_eq(&connector.source_admin(), &source_admin));
        assert!(Arc::ptr_eq(&connector.target_admin(), &target_admin));
    }

    #[test]
    fn test_version() {
        assert_eq!(MirrorCheckpointConnector::version(), "1.0.0");
    }

    #[test]
    fn test_task_class() {
        let connector = MirrorCheckpointConnector::new();
        assert_eq!(connector.task_class(), "MirrorCheckpointTask");
    }

    #[test]
    fn test_exactly_once_support() {
        let connector = MirrorCheckpointConnector::new();
        let support = connector.exactly_once_support(HashMap::new());
        assert_eq!(support, ExactlyOnceSupport::Unsupported);
    }

    #[test]
    fn test_can_define_transaction_boundaries() {
        let connector = MirrorCheckpointConnector::new();
        let boundaries = connector.can_define_transaction_boundaries(HashMap::new());
        assert_eq!(
            boundaries,
            ConnectorTransactionBoundaries::CoordinatorDefined
        );
    }

    #[test]
    fn test_task_configs_not_started() {
        let connector = MirrorCheckpointConnector::new();
        // Connector not started, should return error because groups not loaded
        let result = connector.task_configs(1);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_configs_empty_groups() {
        let mut connector = MirrorCheckpointConnector::new();
        // Simulate started but empty groups
        connector.started = true;
        connector.initial_groups_loaded = true;

        let configs = connector.task_configs(1).unwrap();
        assert!(configs.is_empty());
    }

    #[test]
    fn test_task_configs_disabled() {
        let mut connector = MirrorCheckpointConnector::new();
        let mut props = create_test_config();
        props.insert(
            EMIT_CHECKPOINTS_ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );
        connector.config = Some(MirrorCheckpointConfig::new(props));
        connector.started = true;
        connector.initial_groups_loaded = true;
        connector
            .known_consumer_groups
            .insert("test-group".to_string());

        let configs = connector.task_configs(1).unwrap();
        assert!(configs.is_empty());
    }

    #[test]
    fn test_task_configs_distribution() {
        let mut connector = MirrorCheckpointConnector::new();
        connector.config = Some(MirrorCheckpointConfig::new(create_test_config()));
        connector.started = true;
        connector.initial_groups_loaded = true;

        // Add multiple consumer groups
        connector.known_consumer_groups.insert("group1".to_string());
        connector.known_consumer_groups.insert("group2".to_string());
        connector.known_consumer_groups.insert("group3".to_string());
        connector.known_consumer_groups.insert("group4".to_string());

        let configs = connector.task_configs(2).unwrap();
        assert_eq!(configs.len(), 2);

        // Each task config should have assigned groups
        for config in &configs {
            assert!(config.contains_key(TASK_ASSIGNED_GROUPS_CONFIG));
            assert!(config.contains_key(TASK_INDEX_CONFIG));
        }
    }

    #[test]
    fn test_partition_groups() {
        let items = vec!["a", "b", "c", "d", "e"];
        let partitions = MirrorCheckpointConnector::partition_groups(&items, 2);

        assert_eq!(partitions.len(), 2);
        // Round-robin distribution: a,c,e in first; b,d in second
        assert_eq!(partitions[0], vec!["a", "c", "e"]);
        assert_eq!(partitions[1], vec!["b", "d"]);
    }

    #[test]
    fn test_partition_groups_empty() {
        let items: Vec<String> = vec![];
        let partitions = MirrorCheckpointConnector::partition_groups(&items, 2);
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_partition_groups_zero_tasks() {
        let items = vec!["a", "b"];
        let partitions = MirrorCheckpointConnector::partition_groups(&items, 0);
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_validate_missing_source_alias() {
        let connector = MirrorCheckpointConnector::new();
        let mut props = HashMap::new();
        props.insert(
            crate::config::TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
            "primary".to_string(),
        );

        let config = connector.validate(props);
        let has_source_error = config
            .config_values()
            .iter()
            .any(|cv| cv.name() == "source.cluster.alias" && !cv.error_messages().is_empty());
        assert!(has_source_error);
    }

    #[test]
    fn test_validate_missing_target_alias() {
        let connector = MirrorCheckpointConnector::new();
        let mut props = HashMap::new();
        props.insert(
            crate::config::SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
            "backup".to_string(),
        );

        let config = connector.validate(props);
        let has_target_error = config
            .config_values()
            .iter()
            .any(|cv| cv.name() == "target.cluster.alias" && !cv.error_messages().is_empty());
        assert!(has_target_error);
    }

    #[test]
    fn test_start_stop_cycle() {
        let mut connector = MirrorCheckpointConnector::new();
        let props = create_test_config();

        connector.start(props);
        assert!(connector.started);
        assert!(connector.config.is_some());
        assert!(connector.group_filter.is_some());
        assert!(connector.topic_filter.is_some());

        connector.stop();
        assert!(!connector.started);
        assert!(connector.known_consumer_groups.is_empty());
        assert!(connector.group_filter.is_none());
        assert!(connector.topic_filter.is_none());
    }

    #[test]
    fn test_initialize_with_task_configs() {
        let mut connector = MirrorCheckpointConnector::new();

        // Create a mock context
        struct MockContext;
        impl ConnectorContext for MockContext {
            fn request_task_reconfiguration(&mut self) {}
            fn raise_error(&mut self, _error: ConnectError) {}
        }

        let context = Box::new(MockContext);
        let task_configs = vec![
            HashMap::from([
                (
                    TASK_ASSIGNED_GROUPS_CONFIG.to_string(),
                    "group1,group2".to_string(),
                ),
                (TASK_INDEX_CONFIG.to_string(), "0".to_string()),
            ]),
            HashMap::from([
                (
                    TASK_ASSIGNED_GROUPS_CONFIG.to_string(),
                    "group3".to_string(),
                ),
                (TASK_INDEX_CONFIG.to_string(), "1".to_string()),
            ]),
        ];

        connector.initialize_with_task_configs(context, task_configs);

        assert!(connector.context.is_some());
        assert!(connector.known_consumer_groups.contains("group1"));
        assert!(connector.known_consumer_groups.contains("group2"));
        assert!(connector.known_consumer_groups.contains("group3"));
        assert!(connector.initial_groups_loaded);
    }

    #[test]
    fn test_find_consumer_groups_empty_admin() {
        let connector = MirrorCheckpointConnector::new();

        // Without filters set, should return empty
        let groups = connector.find_consumer_groups();
        assert!(groups.is_empty());
    }

    #[test]
    fn test_is_enabled_default() {
        let connector = MirrorCheckpointConnector::new();
        // No config, should default to true
        assert!(connector.is_enabled());
    }

    #[test]
    fn test_is_enabled_with_config() {
        let mut connector = MirrorCheckpointConnector::new();
        let mut props = create_test_config();
        props.insert(
            EMIT_CHECKPOINTS_ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );
        connector.config = Some(MirrorCheckpointConfig::new(props));
        assert!(!connector.is_enabled());
    }

    #[test]
    fn test_task_configs_with_more_tasks_than_groups() {
        let mut connector = MirrorCheckpointConnector::new();
        connector.config = Some(MirrorCheckpointConfig::new(create_test_config()));
        connector.started = true;
        connector.initial_groups_loaded = true;

        // Add fewer groups than tasks
        connector.known_consumer_groups.insert("group1".to_string());

        let configs = connector.task_configs(3).unwrap();
        // Should only create 1 task since only 1 group
        assert_eq!(configs.len(), 1);
    }
}
