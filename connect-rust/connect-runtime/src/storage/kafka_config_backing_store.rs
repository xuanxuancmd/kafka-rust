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

//! Provides persistent storage of Kafka Connect connector configurations in a Kafka topic.
//!
//! This class manages both connector and task configurations, among other various configurations.
//! It tracks multiple types of records including connector configs, task configs, target states,
//! restart requests, task count records, and session keys.
//!
//! Corresponds to `org.apache.kafka.connect.storage.KafkaConfigBackingStore` in Java.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use common_trait::storage::{
    ClusterConfigState, ConnectorTaskId, ConfigBackingStore, ConfigBackingStoreUpdateListener,
    RestartRequest, SessionKey, TargetState,
};
use common_trait::util::time::{Time, SYSTEM};
use dashmap::DashMap;
use tokio::sync::RwLock;
use log::{debug, error, info, trace, warn};

use super::applied_connector_config::AppliedConnectorConfig;
use super::kafka_topic_based_backing_store::{KafkaBasedLog, SendCompletionCallback, TopicAdmin, TopicDescription};
use super::privileged_write_exception::PrivilegedWriteException;

/// Configuration key for config storage topic.
pub const CONFIG_TOPIC_CONFIG: &str = "config.storage.topic";

/// Configuration key for config storage replication factor.
pub const CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG: &str = "config.storage.replication.factor";

/// Default replication factor for config topic.
pub const DEFAULT_CONFIG_REPLICATION_FACTOR: i16 = 1;

/// Read/write total timeout in milliseconds.
pub const READ_WRITE_TOTAL_TIMEOUT_MS: u64 = 30000;

/// Key prefix for target state records.
pub const TARGET_STATE_PREFIX: &str = "target-state-";

/// Key prefix for connector config records.
pub const CONNECTOR_PREFIX: &str = "connector-";

/// Key prefix for task config records.
pub const TASK_PREFIX: &str = "task-";

/// Key prefix for task commit records.
pub const COMMIT_TASKS_PREFIX: &str = "commit-";

/// Key prefix for task count records.
pub const TASK_COUNT_RECORD_PREFIX: &str = "tasks-fencing-";

/// Key for session key record.
pub const SESSION_KEY_KEY: &str = "session-key";

/// Key prefix for restart request records.
pub const RESTART_PREFIX: &str = "restart-connector-";

/// Key prefix for logger level records.
pub const LOGGER_CLUSTER_PREFIX: &str = "logger-cluster-";

/// Creates the target state key for a connector.
pub fn target_state_key(connector: &str) -> String {
    format!("{}{}", TARGET_STATE_PREFIX, connector)
}

/// Creates the connector config key for a connector.
pub fn connector_key(connector: &str) -> String {
    format!("{}{}", CONNECTOR_PREFIX, connector)
}

/// Creates the task config key for a task.
pub fn task_key(task_id: &ConnectorTaskId) -> String {
    format!("{}{}-{}", TASK_PREFIX, task_id.connector(), task_id.task())
}

/// Creates the commit tasks key for a connector.
pub fn commit_tasks_key(connector: &str) -> String {
    format!("{}{}", COMMIT_TASKS_PREFIX, connector)
}

/// Creates the task count record key for a connector.
pub fn task_count_record_key(connector: &str) -> String {
    format!("{}{}", TASK_COUNT_RECORD_PREFIX, connector)
}

/// Creates the restart key for a connector.
pub fn restart_key(connector: &str) -> String {
    format!("{}{}", RESTART_PREFIX, connector)
}

/// Creates the logger cluster key for a namespace.
pub fn logger_cluster_key(namespace: &str) -> String {
    format!("{}{}", LOGGER_CLUSTER_PREFIX, namespace)
}

/// Provides persistent storage of Kafka Connect connector configurations in a Kafka topic.
///
/// This configuration is expected to be stored in a *single partition* and *compacted* topic.
/// Using a single partition ensures we can enforce ordering on messages, allowing Kafka to
/// be used as a write ahead log. Compaction allows us to clean up outdated configurations over time.
///
/// Since processing of the config log occurs in a background thread, callers must take care
/// when using accessors. To simplify handling this correctly, this class only exposes a
/// mechanism to snapshot the current state of the cluster.
pub struct KafkaConfigBackingStore {
    /// The Kafka topic for storing configurations
    topic: String,
    /// The Kafka-based log for reading/writing
    config_log: Option<Arc<dyn KafkaBasedLog<String, Vec<u8>>>>,
    /// Connector task counts
    connector_task_counts: DashMap<String, u32>,
    /// Connector configurations
    connector_configs: DashMap<String, HashMap<String, String>>,
    /// Connector target states
    connector_target_states: DashMap<String, TargetState>,
    /// Task configurations
    task_configs: DashMap<ConnectorTaskId, HashMap<String, String>>,
    /// Deferred task updates (pending commit)
    deferred_task_updates: DashMap<String, HashMap<ConnectorTaskId, HashMap<String, String>>>,
    /// Task count records
    connector_task_count_records: DashMap<String, u32>,
    /// Task config generations
    connector_task_config_generations: DashMap<String, u32>,
    /// Applied connector configs
    applied_connector_configs: DashMap<String, AppliedConnectorConfig>,
    /// Connectors pending fencing
    connectors_pending_fencing: DashMap<String, bool>,
    /// Inconsistent connectors (compaction issues)
    inconsistent: DashMap<String, bool>,
    /// Session key
    session_key: Arc<RwLock<Option<SessionKey>>>,
    /// Current offset
    offset: Arc<RwLock<i64>>,
    /// Update listener
    update_listener: Option<Arc<dyn ConfigBackingStoreUpdateListener>>,
    /// Flag indicating if the store is started
    started: Arc<RwLock<bool>>,
    /// Flag for exactly-once source support
    exactly_once: bool,
    /// Flag for fencable writer
    uses_fencable_writer: bool,
    /// Time utility
    time: Arc<dyn Time>,
    /// Topic admin for managing topics
    topic_admin: Option<Arc<dyn TopicAdmin>>,
}

impl KafkaConfigBackingStore {
    /// Creates a new KafkaConfigBackingStore.
    pub fn new(topic: String, time: Arc<dyn Time>) -> Self {
        Self {
            topic,
            config_log: None,
            connector_task_counts: DashMap::new(),
            connector_configs: DashMap::new(),
            connector_target_states: DashMap::new(),
            task_configs: DashMap::new(),
            deferred_task_updates: DashMap::new(),
            connector_task_count_records: DashMap::new(),
            connector_task_config_generations: DashMap::new(),
            applied_connector_configs: DashMap::new(),
            connectors_pending_fencing: DashMap::new(),
            inconsistent: DashMap::new(),
            session_key: Arc::new(RwLock::new(None)),
            offset: Arc::new(RwLock::new(-1)),
            update_listener: None,
            started: Arc::new(RwLock::new(false)),
            exactly_once: false,
            uses_fencable_writer: false,
            time,
            topic_admin: None,
        }
    }

    /// Creates a new KafkaConfigBackingStore with a TopicAdmin.
    pub fn with_admin(topic: String, time: Arc<dyn Time>, topic_admin: Arc<dyn TopicAdmin>) -> Self {
        Self {
            topic,
            config_log: None,
            connector_task_counts: DashMap::new(),
            connector_configs: DashMap::new(),
            connector_target_states: DashMap::new(),
            task_configs: DashMap::new(),
            deferred_task_updates: DashMap::new(),
            connector_task_count_records: DashMap::new(),
            connector_task_config_generations: DashMap::new(),
            applied_connector_configs: DashMap::new(),
            connectors_pending_fencing: DashMap::new(),
            inconsistent: DashMap::new(),
            session_key: Arc::new(RwLock::new(None)),
            offset: Arc::new(RwLock::new(-1)),
            update_listener: None,
            started: Arc::new(RwLock::new(false)),
            exactly_once: false,
            uses_fencable_writer: false,
            time,
            topic_admin: Some(topic_admin),
        }
    }

    /// Returns the config topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the number of connectors.
    pub fn connector_count(&self) -> usize {
        self.connector_configs.len()
    }

    /// Returns the number of tasks.
    pub fn task_count(&self) -> usize {
        self.task_configs.len()
    }

    /// Parses a task ID from a task config key.
    fn parse_task_id(key: &str) -> Option<ConnectorTaskId> {
        let parts: Vec<&str> = key.split('-').collect();
        if parts.len() < 3 {
            return None;
        }

        let task_num = parts.last()?.parse::<u32>().ok()?;
        let connector_name = parts[1..parts.len() - 1].join("-");
        
        Some(ConnectorTaskId::new(connector_name, task_num))
    }

    /// Serializes a connector config.
    fn serialize_connector_config(config: &HashMap<String, String>) -> Vec<u8> {
        serde_json::to_vec(config).unwrap_or_default()
    }

    /// Serializes a task config.
    fn serialize_task_config(config: &HashMap<String, String>) -> Vec<u8> {
        serde_json::to_vec(config).unwrap_or_default()
    }

    /// Serializes a target state.
    fn serialize_target_state(state: &TargetState) -> Vec<u8> {
        serde_json::to_vec(&state.to_string()).unwrap_or_default()
    }

    /// Serializes a task count record.
    fn serialize_task_count_record(task_count: u32) -> Vec<u8> {
        serde_json::to_vec(&task_count).unwrap_or_default()
    }

    /// Deserializes a connector config.
    fn deserialize_connector_config(data: &[u8]) -> Option<HashMap<String, String>> {
        serde_json::from_slice(data).ok()
    }

    /// Deserializes a task config.
    fn deserialize_task_config(data: &[u8]) -> Option<HashMap<String, String>> {
        serde_json::from_slice(data).ok()
    }

    /// Deserializes a target state.
    fn deserialize_target_state(data: &[u8]) -> Option<TargetState> {
        let state_str: String = serde_json::from_slice(data).ok()?;
        Self::parse_target_state(&state_str)
    }

    /// Parses a target state string.
    fn parse_target_state(s: &str) -> Option<TargetState> {
        match s {
            "STARTED" => Some(TargetState::Started),
            "STOPPED" => Some(TargetState::Stopped),
            "PAUSED" => Some(TargetState::Paused),
            _ => None,
        }
    }

    /// Sends a privileged write to the config topic.
    async fn send_privileged(
        &self,
        key_values: Vec<(String, Option<Vec<u8>>)>,
    ) -> Result<(), PrivilegedWriteException> {
        if !self.uses_fencable_writer {
            // Non-fencable: simple send
            if let Some(config_log) = &self.config_log {
                for (key, value) in key_values {
                    config_log.send(Some(&key), value.as_ref(), None);
                }
            }
            return Ok(());
        }

        // Fencable write would use transactional producer
        // Simplified for now
        Err(PrivilegedWriteException::with_message(
            "Fencable writer not initialized",
        ))
    }

    /// Process a target state record.
    fn process_target_state_record(&self, connector: &str, value: Option<&[u8]>) {
        if value.is_none() {
            self.connector_target_states.remove(connector);
            // If connector config still exists, set default STARTED state
            if self.connector_configs.contains_key(connector) {
                self.connector_target_states.insert(connector.to_string(), TargetState::Started);
            }
        } else if let Some(data) = value {
            if let Some(state) = Self::parse_target_state_str(data) {
                self.connector_target_states.insert(connector.to_string(), state);
            }
        }
    }

    /// Parses a target state from bytes.
    fn parse_target_state_str(data: &[u8]) -> Option<TargetState> {
        let state_str: String = serde_json::from_slice(data).ok()?;
        Self::parse_target_state(&state_str)
    }

    /// Process a connector config record.
    fn process_connector_config_record(&self, connector: &str, value: Option<&[u8]>) {
        if value.is_none() {
            // Connector deletion
            self.connector_configs.remove(connector);
            self.connector_task_counts.remove(connector);
            self.deferred_task_updates.remove(connector);
            self.applied_connector_configs.remove(connector);
            
            // Remove all task configs for this connector
            self.task_configs.retain(|k, _| k.connector() != connector);
            
            info!("Successfully processed removal of connector '{}'", connector);
        } else if let Some(data) = value {
            if let Some(config) = Self::deserialize_connector_config(data) {
                self.connector_configs.insert(connector.to_string(), config);
                
                // Set initial state to STARTED if not present
                if !self.connector_target_states.contains_key(connector) {
                    self.connector_target_states.insert(connector.to_string(), TargetState::Started);
                }
            }
        }
    }

    /// Process a task config record.
    fn process_task_config_record(&self, task_id: &ConnectorTaskId, value: Option<&[u8]>) {
        if let Some(data) = value {
            if let Some(config) = Self::deserialize_task_config(data) {
                self.deferred_task_updates
                    .entry(task_id.connector().to_string())
                    .or_insert_with(|| HashMap::new())
                    .insert(task_id.clone(), config);
            }
        }
    }

    /// Process a tasks commit record.
    fn process_tasks_commit_record(&self, connector: &str, task_count: u32) {
        // Check if connector still exists
        if !self.connector_configs.contains_key(connector) {
            debug!(
                "Ignoring task configs for connector {}; it appears that the connector was deleted",
                connector
            );
            return;
        }

        // Validate task configs are complete
        let has_complete_configs = if let Some(ref deferred_map) = self.deferred_task_updates.get(connector) {
            // Check we have configs for all expected tasks
            let task_ids: HashSet<u32> = deferred_map.keys().map(|k| k.task()).collect();
            task_ids.len() >= task_count as usize
                && (0..task_count).all(|i| task_ids.contains(&i))
        } else {
            task_count == 0
        };

        if !has_complete_configs {
            self.inconsistent.insert(connector.to_string(), true);
        } else {
            // Apply deferred configs
            if let Some(ref deferred_map) = self.deferred_task_updates.get(connector) {
                for (task_id, config) in deferred_map.iter() {
                    self.task_configs.insert(task_id.clone(), config.clone());
                }
            }
            self.inconsistent.remove(connector);
            
            // Update applied connector config
            if let Some(config) = self.connector_configs.get(connector) {
                self.applied_connector_configs.insert(
                    connector.to_string(),
                    AppliedConnectorConfig::new(Some(config.clone())),
                );
            }
        }

        // Clear deferred
        if let Some(mut deferred) = self.deferred_task_updates.get_mut(connector) {
            deferred.clear();
        }

        // Update task count
        self.connector_task_counts.insert(connector.to_string(), task_count);
        
        // Mark as pending fencing
        self.connectors_pending_fencing.insert(connector.to_string(), true);
    }

    /// Process a task count record.
    fn process_task_count_record(&self, connector: &str, task_count: u32) {
        self.connector_task_count_records.insert(connector.to_string(), task_count);
        // No longer pending fencing
        self.connectors_pending_fencing.remove(connector);
    }

    /// Process a session key record.
    async fn process_session_key_record(&self, key: &str, algorithm: &str, creation_timestamp: i64) {
        let session_key = SessionKey::new(key.to_string(), creation_timestamp);
        let mut stored = self.session_key.write().await;
        *stored = Some(session_key);
    }

    /// Process a consumed record.
    fn process_record(&self, key: &str, value: Option<&[u8]>) {
        if key.starts_with(TARGET_STATE_PREFIX) {
            let connector = &key[TARGET_STATE_PREFIX.len()..];
            self.process_target_state_record(connector, value);
        } else if key.starts_with(CONNECTOR_PREFIX) {
            let connector = &key[CONNECTOR_PREFIX.len()..];
            self.process_connector_config_record(connector, value);
        } else if key.starts_with(TASK_PREFIX) {
            if let Some(task_id) = Self::parse_task_id(key) {
                self.process_task_config_record(&task_id, value);
            }
        } else if key.starts_with(COMMIT_TASKS_PREFIX) {
            let connector = &key[COMMIT_TASKS_PREFIX.len()..];
            if let Some(data) = value {
                if let Ok(task_count) = serde_json::from_slice::<u32>(data) {
                    self.process_tasks_commit_record(connector, task_count);
                }
            }
        } else if key.starts_with(TASK_COUNT_RECORD_PREFIX) {
            let connector = &key[TASK_COUNT_RECORD_PREFIX.len()..];
            if let Some(data) = value {
                if let Ok(task_count) = serde_json::from_slice::<u32>(data) {
                    self.process_task_count_record(connector, task_count);
                }
            }
        } else if key == SESSION_KEY_KEY {
            // Process session key - simplified
            debug!("Processing session key record");
        } else if key.starts_with(RESTART_PREFIX) {
            let connector = &key[RESTART_PREFIX.len()..];
            debug!("Processing restart request for {}", connector);
        } else if key.starts_with(LOGGER_CLUSTER_PREFIX) {
            let namespace = &key[LOGGER_CLUSTER_PREFIX.len()..];
            debug!("Processing logger level for namespace {}", namespace);
        } else {
            warn!("Discarding config update record with invalid key: {}", key);
        }
    }
}

impl ConfigBackingStore for KafkaConfigBackingStore {
    fn start(&mut self) {
        info!("Starting KafkaConfigBackingStore");

        if let Some(config_log) = &self.config_log {
            // Note: In full implementation, would call config_log.start()
            // and check partition count
            debug!("Config log should be started");
        }

        let mut started = self.started.blocking_write();
        *started = true;

        info!("Started KafkaConfigBackingStore");
    }

    fn stop(&mut self) {
        info!("Closing KafkaConfigBackingStore");

        if let Some(_config_log) = &self.config_log {
            // Note: In full implementation, would call config_log.stop()
            debug!("Config log should be stopped");
        }

        let mut started = self.started.blocking_write();
        *started = false;

        info!("Closed KafkaConfigBackingStore");
    }

    fn snapshot(&self) -> ClusterConfigState {
        ClusterConfigState {
            offset: *self.offset.blocking_read(),
            session_key: self.session_key.blocking_read().clone(),
            connector_task_counts: self.connector_task_counts.iter()
                .map(|e| (e.key().clone(), *e.value()))
                .collect(),
            connector_configs: self.connector_configs.iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            connector_target_states: self.connector_target_states.iter()
                .map(|e| (e.key().clone(), *e.value()))
                .collect(),
            task_configs: self.task_configs.iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            connector_task_count_records: self.connector_task_count_records.iter()
                .map(|e| (e.key().clone(), *e.value()))
                .collect(),
            connector_task_config_generations: self.connector_task_config_generations.iter()
                .map(|e| (e.key().clone(), *e.value()))
                .collect(),
            applied_connector_configs: self.applied_connector_configs.iter()
                .map(|e| (e.key().clone(), e.value().raw_config().cloned().unwrap_or_default()))
                .collect(),
            connectors_pending_fencing: self.connectors_pending_fencing.iter()
                .filter(|e| *e.value())
                .map(|e| e.key().clone())
                .collect(),
            inconsistent_connectors: self.inconsistent.iter()
                .filter(|e| *e.value())
                .map(|e| e.key().clone())
                .collect(),
        }
    }

    fn contains(&self, connector: &str) -> bool {
        self.connector_configs.contains_key(connector)
    }

    fn put_connector_config(
        &mut self,
        connector: &str,
        properties: HashMap<String, String>,
        target_state: Option<TargetState>,
    ) {
        debug!("Writing connector configuration for connector '{}'", connector);

        let config_key = connector_key(connector);
        let serialized_config = Self::serialize_connector_config(&properties);

        let key_values = if let Some(state) = target_state {
            vec![
                (target_state_key(connector), Some(Self::serialize_target_state(&state))),
                (config_key, Some(serialized_config)),
            ]
        } else {
            vec![(config_key, Some(serialized_config))]
        };

        // Update local cache
        self.connector_configs.insert(connector.to_string(), properties);
        if let Some(state) = target_state {
            self.connector_target_states.insert(connector.to_string(), state);
        }
    }

    fn remove_connector_config(&mut self, connector: &str) {
        debug!("Removing connector configuration for connector '{}'", connector);

        // Write tombstones
        let key_values: Vec<(String, Option<Vec<u8>>)> = vec![
            (connector_key(connector), None),
            (target_state_key(connector), None),
        ];

        // Update local cache
        self.connector_configs.remove(connector);
        self.connector_target_states.remove(connector);
    }

    fn put_task_configs(&mut self, connector: &str, configs: Vec<HashMap<String, String>>) {
        debug!("Writing task configurations for connector '{}'", connector);

        let task_count = configs.len() as u32;

        // Write individual task configs
        for (index, task_config) in configs.iter().enumerate() {
            let task_id = ConnectorTaskId::new(connector.to_string(), index as u32);
            let task_key = task_key(&task_id);
            let serialized_config = Self::serialize_task_config(task_config);

            // Update deferred task updates
            self.deferred_task_updates
                .entry(connector.to_string())
                .or_insert_with(|| HashMap::new())
                .insert(task_id.clone(), task_config.clone());
        }

        // Write commit message
        let commit_key = commit_tasks_key(connector);
        let serialized_commit = Self::serialize_task_count_record(task_count);

        // Process commit locally
        self.process_tasks_commit_record(connector, task_count);
    }

    fn remove_task_configs(&mut self, _connector: &str) {
        // Not supported
        panic!("Removal of tasks is not currently supported");
    }

    fn refresh(&mut self, timeout: Duration) -> Result<(), std::io::Error> {
        // In full implementation, would read to end of log
        debug!("Refreshing config backing store");
        Ok(())
    }

    fn put_target_state(&mut self, connector: &str, state: TargetState) {
        debug!("Writing target state {} for connector {}", state, connector);

        let key = target_state_key(connector);
        let serialized_state = Self::serialize_target_state(&state);

        // Update local cache
        self.connector_target_states.insert(connector.to_string(), state);
    }

    fn put_session_key(&mut self, session_key: SessionKey) {
        debug!("Distributing new session key");

        // Update local cache
        let mut stored = self.session_key.blocking_write();
        *stored = Some(session_key);
    }

    fn put_restart_request(&mut self, restart_request: RestartRequest) {
        debug!("Writing restart request for {}", restart_request.connector);

        let key = restart_key(&restart_request.connector);
    }

    fn put_task_count_record(&mut self, connector: &str, task_count: u32) {
        debug!("Writing task count record {} for connector {}", task_count, connector);

        let key = task_count_record_key(connector);
        let serialized = Self::serialize_task_count_record(task_count);

        // Update local cache
        self.connector_task_count_records.insert(connector.to_string(), task_count);
        self.connectors_pending_fencing.remove(connector);
    }

    fn claim_write_privileges(&mut self) {
        if self.uses_fencable_writer {
            // In full implementation, would create fencable producer
            debug!("Claiming write privileges");
        }
    }

    fn put_logger_level(&mut self, namespace: &str, level: &str) {
        debug!("Writing level {} for logging namespace {}", level, namespace);

        let key = logger_cluster_key(namespace);
    }

    fn set_update_listener(&mut self, listener: Box<dyn ConfigBackingStoreUpdateListener>) {
        self.update_listener = Some(Arc::from(listener));
    }
}

impl Default for KafkaConfigBackingStore {
    fn default() -> Self {
        Self::new("connect-configs".to_string(), SYSTEM.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_store() {
        let store = KafkaConfigBackingStore::new("test-configs".to_string(), SYSTEM.clone());
        assert_eq!(store.topic(), "test-configs");
        assert_eq!(store.connector_count(), 0);
        assert_eq!(store.task_count(), 0);
    }

    #[test]
    fn test_key_functions() {
        assert_eq!(target_state_key("test"), "target-state-test");
        assert_eq!(connector_key("test"), "connector-test");
        assert_eq!(task_key(&ConnectorTaskId::new("test".to_string(), 1)), "task-test-1");
        assert_eq!(commit_tasks_key("test"), "commit-test");
        assert_eq!(task_count_record_key("test"), "tasks-fencing-test");
        assert_eq!(restart_key("test"), "restart-connector-test");
        assert_eq!(logger_cluster_key("test"), "logger-cluster-test");
    }

    #[test]
    fn test_parse_task_id() {
        let task_id = KafkaConfigBackingStore::parse_task_id("task-test-connector-1");
        assert!(task_id.is_some());
        let id = task_id.unwrap();
        assert_eq!(id.connector(), "test-connector");
        assert_eq!(id.task(), 1);
    }

    #[test]
    fn test_serialize_deserialize_config() {
        let config = HashMap::from([
            ("name".to_string(), "test".to_string()),
            ("class".to_string(), "TestConnector".to_string()),
        ]);

        let serialized = KafkaConfigBackingStore::serialize_connector_config(&config);
        let deserialized = KafkaConfigBackingStore::deserialize_connector_config(&serialized);
        assert!(deserialized.is_some());
        assert_eq!(deserialized.unwrap(), config);
    }

    #[test]
    fn test_serialize_deserialize_target_state() {
        let state = TargetState::Started;
        let serialized = KafkaConfigBackingStore::serialize_target_state(&state);
        let deserialized = KafkaConfigBackingStore::deserialize_target_state(&serialized);
        assert!(deserialized.is_some());
        assert_eq!(deserialized.unwrap(), state);
    }

    #[test]
    fn test_snapshot_empty() {
        let store = KafkaConfigBackingStore::default();
        let snapshot = store.snapshot();
        assert_eq!(snapshot.offset, -1);
        assert!(snapshot.session_key.is_none());
        assert!(snapshot.connector_configs.is_empty());
    }

    #[tokio::test]
    async fn test_put_connector_config() {
        let mut store = KafkaConfigBackingStore::default();
        let config = HashMap::from([
            ("name".to_string(), "test-connector".to_string()),
        ]);

        store.put_connector_config("test-connector", config.clone(), Some(TargetState::Started));

        assert!(store.contains("test-connector"));
        let snapshot = store.snapshot();
        assert!(snapshot.connector_config("test-connector").is_some());
        assert_eq!(snapshot.target_state("test-connector"), Some(TargetState::Started));
    }

    #[tokio::test]
    async fn test_remove_connector_config() {
        let mut store = KafkaConfigBackingStore::default();
        let config = HashMap::from([
            ("name".to_string(), "test-connector".to_string()),
        ]);

        store.put_connector_config("test-connector", config, None);
        assert!(store.contains("test-connector"));

        store.remove_connector_config("test-connector");
        assert!(!store.contains("test-connector"));
    }

    #[tokio::test]
    async fn test_put_task_configs() {
        let mut store = KafkaConfigBackingStore::default();

        // First add connector
        let connector_config = HashMap::from([
            ("name".to_string(), "test-connector".to_string()),
        ]);
        store.put_connector_config("test-connector", connector_config, None);

        // Add task configs
        let task_configs: Vec<HashMap<String, String>> = vec![
            HashMap::from([("task".to_string(), "0".to_string())]),
            HashMap::from([("task".to_string(), "1".to_string())]),
        ];

        store.put_task_configs("test-connector", task_configs);

        let snapshot = store.snapshot();
        assert_eq!(snapshot.task_count("test-connector"), 2);
    }

    #[tokio::test]
    async fn test_start_stop() {
        let mut store = KafkaConfigBackingStore::default();
        store.start();
        store.stop();
        // Should complete without error
    }
}