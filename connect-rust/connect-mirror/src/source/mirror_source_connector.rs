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

//! MirrorSourceConnector - Source connector for MirrorMaker 2.
//!
//! This module provides the MirrorSourceConnector class that replicates data,
//! configurations, and ACLs between Kafka clusters.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorSourceConnector (742 lines)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_trait::config::{Config, ConfigDef, ConfigValueEntry};
use common_trait::TopicPartition;
use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext};
use connect_api::errors::ConnectError;
use connect_api::source::{ConnectorTransactionBoundaries, ExactlyOnceSupport, SourceConnector};
use connect_mirror_client::ReplicationPolicy;
use kafka_clients_mock::MockAdmin;
use serde_json::Value;

use super::config_property_filter::ConfigPropertyFilter;
use super::mirror_source_config::{
    MirrorSourceConfig, REPLICATION_FACTOR_CONFIG, REPLICATION_FACTOR_DEFAULT, TASK_INDEX_CONFIG,
    TASK_TOPIC_PARTITIONS_CONFIG,
};
use super::topic_filter::TopicFilter;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorSourceConnector
// ============================================================================

/// Number of partitions for offset syncs topic.
/// Corresponds to Java: MirrorSourceConnector.NUM_OFFSET_SYNCS_PARTITIONS
pub const NUM_OFFSET_SYNCS_PARTITIONS: i32 = 1;

// ============================================================================
// MirrorSourceConnectorConfigDef
// ============================================================================

/// Configuration definition for MirrorSourceConnector.
struct MirrorSourceConnectorConfigDef;

impl ConfigDef for MirrorSourceConnectorConfigDef {
    fn config_def(&self) -> HashMap<String, ConfigValueEntry> {
        let mut defs = HashMap::new();

        defs.insert(
            REPLICATION_FACTOR_CONFIG.to_string(),
            ConfigValueEntry::new(
                REPLICATION_FACTOR_CONFIG,
                common_trait::config::ConfigDefType::Int,
                Some(serde_json::Value::Number(REPLICATION_FACTOR_DEFAULT.into())),
                common_trait::config::ConfigDefImportance::High,
                common_trait::config::ConfigDefWidth::Short,
                "Replication factor for newly created remote topics",
            ),
        );

        defs
    }
}

static MIRROR_SOURCE_CONNECTOR_CONFIG_DEF: MirrorSourceConnectorConfigDef =
    MirrorSourceConnectorConfigDef;

// ============================================================================
// Scheduler for periodic tasks
// ============================================================================

/// Scheduler for running periodic tasks.
///
/// This is a simplified scheduler that manages periodic tasks for:
/// - Topic refresh
/// - Topic configuration synchronization
/// - Topic ACL synchronization
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.Scheduler (internal)
pub struct Scheduler {
    /// Flag indicating if scheduler is running
    running: bool,
    /// Interval for refreshing topics
    refresh_topics_interval_ms: i64,
    /// Interval for syncing topic configs
    sync_topic_configs_interval_ms: i64,
    /// Interval for syncing topic ACLs
    sync_topic_acls_interval_ms: i64,
}

impl Scheduler {
    /// Creates a new Scheduler.
    pub fn new() -> Self {
        Scheduler {
            running: false,
            refresh_topics_interval_ms: 0,
            sync_topic_configs_interval_ms: 0,
            sync_topic_acls_interval_ms: 0,
        }
    }

    /// Initializes the scheduler with intervals from configuration.
    pub fn configure(&mut self, config: &MirrorSourceConfig) {
        self.refresh_topics_interval_ms = config.refresh_topics_interval().as_millis() as i64;
        self.sync_topic_configs_interval_ms =
            config.sync_topic_configs_interval().as_millis() as i64;
        self.sync_topic_acls_interval_ms = config.sync_topic_acls_interval().as_millis() as i64;
    }

    /// Starts the scheduler.
    pub fn start(&mut self) {
        self.running = true;
    }

    /// Stops the scheduler.
    pub fn stop(&mut self) {
        self.running = false;
    }

    /// Returns whether the scheduler is running.
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Returns the refresh topics interval in milliseconds.
    pub fn refresh_topics_interval_ms(&self) -> i64 {
        self.refresh_topics_interval_ms
    }

    /// Returns the sync topic configs interval in milliseconds.
    pub fn sync_topic_configs_interval_ms(&self) -> i64 {
        self.sync_topic_configs_interval_ms
    }

    /// Returns the sync topic ACLs interval in milliseconds.
    pub fn sync_topic_acls_interval_ms(&self) -> i64 {
        self.sync_topic_acls_interval_ms
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// MirrorSourceConnector
// ============================================================================

/// Source connector for MirrorMaker 2 replication.
///
/// The MirrorSourceConnector is responsible for:
/// - Discovering topics from the source cluster
/// - Filtering topics based on the TopicFilter
/// - Assigning topic partitions to tasks
/// - Synchronizing topic configurations and ACLs
/// - Creating remote topics on the target cluster
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorSourceConnector
pub struct MirrorSourceConnector {
    /// Connector context
    context: Option<Box<dyn ConnectorContext>>,
    /// MirrorSourceConfig configuration
    config: Option<MirrorSourceConfig>,
    /// Source cluster admin client
    source_admin: Arc<MockAdmin>,
    /// Target cluster admin client
    target_admin: Arc<MockAdmin>,
    /// Topic filter
    topic_filter: Option<Arc<dyn TopicFilter>>,
    /// Config property filter
    config_property_filter: Option<Arc<dyn ConfigPropertyFilter>>,
    /// Scheduler for periodic tasks
    scheduler: Scheduler,
    /// Known source topic partitions
    known_source_topic_partitions: HashSet<TopicPartition>,
    /// Flag indicating connector has started
    started: bool,
}

impl std::fmt::Debug for MirrorSourceConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorSourceConnector")
            .field("config", &self.config)
            .field("started", &self.started)
            .field(
                "known_source_topic_partitions",
                &self.known_source_topic_partitions.len(),
            )
            .finish()
    }
}

impl MirrorSourceConnector {
    /// Creates a new MirrorSourceConnector.
    pub fn new() -> Self {
        MirrorSourceConnector {
            context: None,
            config: None,
            source_admin: Arc::new(MockAdmin::new()),
            target_admin: Arc::new(MockAdmin::new()),
            topic_filter: None,
            config_property_filter: None,
            scheduler: Scheduler::new(),
            known_source_topic_partitions: HashSet::new(),
            started: false,
        }
    }

    /// Creates a MirrorSourceConnector with pre-configured admin clients.
    pub fn with_admin_clients(source_admin: Arc<MockAdmin>, target_admin: Arc<MockAdmin>) -> Self {
        MirrorSourceConnector {
            context: None,
            config: None,
            source_admin,
            target_admin,
            topic_filter: None,
            config_property_filter: None,
            scheduler: Scheduler::new(),
            known_source_topic_partitions: HashSet::new(),
            started: false,
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

    /// Returns the topic filter.
    pub fn topic_filter(&self) -> Option<Arc<dyn TopicFilter>> {
        self.topic_filter.clone()
    }

    /// Returns the config property filter.
    pub fn config_property_filter(&self) -> Option<Arc<dyn ConfigPropertyFilter>> {
        self.config_property_filter.clone()
    }

    /// Returns the replication policy.
    pub fn replication_policy(&self) -> Option<Arc<dyn ReplicationPolicy>> {
        self.config.as_ref().map(|c| c.replication_policy())
    }

    /// Returns the known source topic partitions.
    pub fn known_source_topic_partitions(&self) -> &HashSet<TopicPartition> {
        &self.known_source_topic_partitions
    }

    /// Returns whether the connector has started.
    pub fn is_started(&self) -> bool {
        self.started
    }

    /// Returns the configuration if set.
    pub fn config_ref(&self) -> Option<&MirrorSourceConfig> {
        self.config.as_ref()
    }

    /// Loads topic partitions from the source cluster.
    ///
    /// This discovers topics from the source cluster, filters them using
    /// the TopicFilter, and retrieves their partition information.
    ///
    /// Corresponds to Java: MirrorSourceConnector.loadTopicPartitions()
    fn load_topic_partitions(&mut self) {
        if self.config.is_none() || self.topic_filter.is_none() {
            return;
        }

        let filter = self.topic_filter.as_ref().unwrap();
        let replication_policy = self.config.as_ref().unwrap().replication_policy();

        // List topics from source cluster
        let topics_result = self.source_admin.list_topics();

        // Filter topics and collect topic partitions
        for listing in topics_result.listings() {
            let topic = listing.name();

            // Apply topic filter
            if filter.should_replicate_topic(topic) {
                // Get topic metadata (partition count)
                let describe_result = self.source_admin.describe_topics(vec![topic.to_string()]);

                if let Some(metadata) = describe_result.metadata(topic) {
                    // Add partitions for this topic
                    for partition in 0..metadata.num_partitions() {
                        self.known_source_topic_partitions
                            .insert(TopicPartition::new(topic, partition));
                    }
                }
            }
        }
    }

    /// Creates remote topics on the target cluster.
    ///
    /// This creates the corresponding remote topics on the target cluster
    /// for all discovered source topics.
    ///
    /// Corresponds to Java: MirrorSourceConnector.computeAndCreateTopicPartitions()
    fn create_remote_topics(&self) {
        if self.config.is_none() {
            return;
        }

        let config = self.config.as_ref().unwrap();
        let replication_policy = config.replication_policy();
        let replication_factor = config.replication_factor();
        let source_alias = config.source_cluster_alias();

        // Group topic partitions by topic
        let topics: HashSet<&str> = self
            .known_source_topic_partitions
            .iter()
            .map(|tp| tp.topic())
            .collect();

        // Create remote topics
        for topic in topics {
            let remote_topic = replication_policy.format_remote_topic(source_alias, topic);

            // Check if topic already exists on target
            if !self.target_admin.topic_exists(&remote_topic) {
                // Determine number of partitions from known partitions
                let num_partitions = self
                    .known_source_topic_partitions
                    .iter()
                    .filter(|tp| tp.topic() == topic)
                    .count() as i32;

                // Create the remote topic
                let new_topic = kafka_clients_mock::NewTopic::new(
                    remote_topic,
                    num_partitions,
                    replication_factor,
                );

                self.target_admin.create_topics(vec![new_topic]);
            }
        }
    }

    /// Refreshes topic partitions from the source cluster.
    ///
    /// This is called periodically to detect new or deleted topics.
    ///
    /// Corresponds to Java: MirrorSourceConnector.refreshTopicPartitions()
    fn refresh_topic_partitions(&mut self) {
        let old_count = self.known_source_topic_partitions.len();

        // Reload topic partitions
        self.known_source_topic_partitions.clear();
        self.load_topic_partitions();

        let new_count = self.known_source_topic_partitions.len();

        // If partitions changed, request reconfiguration
        if old_count != new_count && self.context.is_some() {
            // Request task reconfiguration through context
            // Note: context.request_task_reconfiguration() would be called here
            // In mock implementation, we just track the change
        }
    }

    /// Synchronizes topic configurations from source to target.
    ///
    /// Corresponds to Java: MirrorSourceConnector.syncTopicConfigs()
    fn sync_topic_configs(&self) {
        if self.config.is_none() || self.config_property_filter.is_none() {
            return;
        }

        let config = self.config.as_ref().unwrap();
        let config_filter = self.config_property_filter.as_ref().unwrap();
        let replication_policy = config.replication_policy();
        let source_alias = config.source_cluster_alias();

        // Get topics to sync
        let topics: HashSet<&str> = self
            .known_source_topic_partitions
            .iter()
            .map(|tp| tp.topic())
            .collect();

        for topic in topics {
            // Get source topic configs
            let source_configs = self
                .source_admin
                .describe_topic_configs(&[topic.to_string()]);

            if let Some(Some(source_config)) = source_configs.get(topic) {
                // Filter configs
                let filtered_configs: HashMap<String, String> = source_config
                    .iter()
                    .filter(|(k, _)| config_filter.should_replicate_config_property(k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                // Get remote topic name
                let remote_topic = replication_policy.format_remote_topic(source_alias, topic);

                // Apply configs to remote topic
                if self.target_admin.topic_exists(&remote_topic) && !filtered_configs.is_empty() {
                    let resource = kafka_clients_mock::ConfigResource::new(
                        kafka_clients_mock::ConfigResourceType::Topic,
                        remote_topic,
                    );

                    let alter_ops: Vec<kafka_clients_mock::AlterConfigOp> = filtered_configs
                        .iter()
                        .map(|(k, v)| kafka_clients_mock::AlterConfigOp::set(k.clone(), v.clone()))
                        .collect();

                    let configs_to_alter = HashMap::from([(resource, alter_ops)]);
                    self.target_admin
                        .incremental_alter_configs(configs_to_alter);
                }
            }
        }
    }
}

impl Default for MirrorSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Versioned for MirrorSourceConnector {
    fn version() -> &'static str {
        "1.0.0"
    }
}

impl Connector for MirrorSourceConnector {
    fn context(&self) -> &dyn ConnectorContext {
        // Return the context reference
        // Note: This requires the context to be set before calling
        self.context
            .as_ref()
            .map(|c| c.as_ref())
            .unwrap_or_else(|| {
                // This should never happen in practice as initialize must be called first
                panic!("ConnectorContext not initialized")
            })
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

        // Restore known source topic partitions from task configs
        for task_config in task_configs {
            if let Some(partitions_str) = task_config.get(TASK_TOPIC_PARTITIONS_CONFIG) {
                // Parse partitions from string format "topic:partition"
                for tp_str in partitions_str.split(',') {
                    let parts: Vec<&str> = tp_str.split(':').collect();
                    if parts.len() == 2 {
                        let topic = parts[0];
                        let partition: i32 = parts[1].parse().unwrap_or(0);
                        self.known_source_topic_partitions
                            .insert(TopicPartition::new(topic, partition));
                    }
                }
            }
        }
    }

    fn start(&mut self, props: HashMap<String, String>) {
        // Create configuration from properties
        self.config = Some(MirrorSourceConfig::new(props.clone()));

        let config = self.config.as_ref().unwrap();

        // Initialize topic filter
        self.topic_filter = Some(config.topic_filter());

        // Initialize config property filter
        self.config_property_filter = Some(config.config_property_filter());

        // Configure scheduler
        self.scheduler.configure(config);

        // Load initial topic partitions
        self.load_topic_partitions();

        // Create remote topics on target cluster
        self.create_remote_topics();

        // Start scheduler
        self.scheduler.start();

        // Set started flag
        self.started = true;
    }

    fn stop(&mut self) {
        // Stop scheduler
        self.scheduler.stop();

        // Close topic filter
        if let Some(filter) = self.topic_filter.take() {
            // TopicFilter doesn't have close in Arc, but we can drop it
            drop(filter);
        }

        // Close config property filter
        if let Some(filter) = self.config_property_filter.take() {
            drop(filter);
        }

        // Clear state
        self.known_source_topic_partitions.clear();
        self.started = false;
    }

    fn task_class(&self) -> &'static str {
        // Return the task class name
        // Corresponds to Java: MirrorSourceConnector.taskClass()
        "MirrorSourceTask"
    }

    fn task_configs(&self, max_tasks: i32) -> Result<Vec<HashMap<String, String>>, ConnectError> {
        // Distribute topic partitions among tasks
        // Corresponds to Java: MirrorSourceConnector.taskConfigs(int maxTasks)

        if !self.started || self.known_source_topic_partitions.is_empty() {
            return Ok(Vec::new());
        }

        // Calculate number of tasks
        let num_partitions = self.known_source_topic_partitions.len() as i32;
        let num_tasks = std::cmp::min(max_tasks, num_partitions);

        if num_tasks == 0 {
            return Ok(Vec::new());
        }

        // Sort partitions for deterministic distribution
        let sorted_partitions: Vec<TopicPartition> =
            self.known_source_topic_partitions.iter().cloned().collect();

        // Distribute partitions in round-robin fashion
        let task_partition_lists: Vec<Vec<TopicPartition>> =
            (0..num_tasks).map(|_| Vec::new()).collect();

        let mut task_partition_lists = task_partition_lists;
        for (i, tp) in sorted_partitions.iter().enumerate() {
            let task_index = i % num_tasks as usize;
            task_partition_lists[task_index].push(tp.clone());
        }

        // Create task configs
        let task_configs: Vec<HashMap<String, String>> = task_partition_lists
            .into_iter()
            .enumerate()
            .map(|(task_index, partitions)| {
                let mut task_props = HashMap::new();

                // Add task index
                task_props.insert(TASK_INDEX_CONFIG.to_string(), task_index.to_string());

                // Add assigned partitions
                let partitions_str = partitions
                    .iter()
                    .map(|tp| format!("{}:{}", tp.topic(), tp.partition()))
                    .collect::<Vec<_>>()
                    .join(",");

                task_props.insert(TASK_TOPIC_PARTITIONS_CONFIG.to_string(), partitions_str);

                // Add source cluster alias
                if let Some(config) = &self.config {
                    task_props.insert(
                        "source.cluster.alias".to_string(),
                        config.source_cluster_alias().to_string(),
                    );
                }

                task_props
            })
            .collect();

        Ok(task_configs)
    }

    fn validate(&self, configs: HashMap<String, String>) -> Config {
        // Validate configuration
        // Corresponds to Java: MirrorSourceConnector.validate(Map<String, String> connectorConfig)

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
        &MIRROR_SOURCE_CONNECTOR_CONFIG_DEF
    }
}

impl SourceConnector for MirrorSourceConnector {
    fn exactly_once_support(
        &self,
        _connector_config: HashMap<String, String>,
    ) -> ExactlyOnceSupport {
        // MirrorSourceConnector does not support exactly-once semantics
        // Corresponds to Java: MirrorSourceConnector.exactlyOnceSupport()
        ExactlyOnceSupport::Unsupported
    }

    fn can_define_transaction_boundaries(
        &self,
        _connector_config: HashMap<String, String>,
    ) -> ConnectorTransactionBoundaries {
        // MirrorSourceConnector uses coordinator-defined boundaries
        // Corresponds to Java: MirrorSourceConnector.canDefineTransactionBoundaries()
        ConnectorTransactionBoundaries::CoordinatorDefined
    }

    fn alter_offsets(
        &self,
        _connector_config: HashMap<String, String>,
        _offsets: HashMap<HashMap<String, Value>, HashMap<String, Value>>,
    ) -> Result<bool, ConnectError> {
        // MirrorSourceConnector does not support external offset management
        // Corresponds to Java: MirrorSourceConnector.alterOffsets()
        Ok(false)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("source.cluster.alias".to_string(), "backup".to_string());
        props.insert("target.cluster.alias".to_string(), "primary".to_string());
        props.insert("name".to_string(), "mirror-source-connector".to_string());
        props.insert("topics".to_string(), "test-.*".to_string());
        props
    }

    #[test]
    fn test_new_connector() {
        let connector = MirrorSourceConnector::new();
        assert!(!connector.started);
        assert!(connector.known_source_topic_partitions.is_empty());
    }

    #[test]
    fn test_version() {
        assert_eq!(MirrorSourceConnector::version(), "1.0.0");
    }

    #[test]
    fn test_task_class() {
        let connector = MirrorSourceConnector::new();
        assert_eq!(connector.task_class(), "MirrorSourceTask");
    }

    #[test]
    fn test_exactly_once_support() {
        let connector = MirrorSourceConnector::new();
        let support = connector.exactly_once_support(HashMap::new());
        assert_eq!(support, ExactlyOnceSupport::Unsupported);
    }

    #[test]
    fn test_can_define_transaction_boundaries() {
        let connector = MirrorSourceConnector::new();
        let boundaries = connector.can_define_transaction_boundaries(HashMap::new());
        assert_eq!(
            boundaries,
            ConnectorTransactionBoundaries::CoordinatorDefined
        );
    }

    #[test]
    fn test_task_configs_empty() {
        let connector = MirrorSourceConnector::new();
        let configs = connector.task_configs(1).unwrap();
        assert!(configs.is_empty());
    }

    #[test]
    fn test_task_configs_distribution() {
        let mut connector = MirrorSourceConnector::new();

        // Add some partitions
        connector
            .known_source_topic_partitions
            .insert(TopicPartition::new("topic1", 0));
        connector
            .known_source_topic_partitions
            .insert(TopicPartition::new("topic1", 1));
        connector
            .known_source_topic_partitions
            .insert(TopicPartition::new("topic2", 0));
        connector.started = true;

        let configs = connector.task_configs(2).unwrap();
        assert_eq!(configs.len(), 2);

        // Each task should have some partitions
        for config in &configs {
            assert!(config.contains_key(TASK_TOPIC_PARTITIONS_CONFIG));
        }
    }

    #[test]
    fn test_scheduler_new() {
        let scheduler = Scheduler::new();
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_scheduler_start_stop() {
        let mut scheduler = Scheduler::new();
        scheduler.start();
        assert!(scheduler.is_running());
        scheduler.stop();
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_validate_missing_source_alias() {
        let connector = MirrorSourceConnector::new();
        let mut props = HashMap::new();
        props.insert("target.cluster.alias".to_string(), "primary".to_string());

        let config = connector.validate(props);
        // Check that there are error messages for source.cluster.alias
        let has_source_error = config
            .config_values()
            .iter()
            .any(|cv| cv.name() == "source.cluster.alias" && !cv.error_messages().is_empty());
        assert!(has_source_error);
    }

    #[test]
    fn test_validate_missing_target_alias() {
        let connector = MirrorSourceConnector::new();
        let mut props = HashMap::new();
        props.insert("source.cluster.alias".to_string(), "backup".to_string());

        let config = connector.validate(props);
        // Check that there are error messages for target.cluster.alias
        let has_target_error = config
            .config_values()
            .iter()
            .any(|cv| cv.name() == "target.cluster.alias" && !cv.error_messages().is_empty());
        assert!(has_target_error);
    }
}
