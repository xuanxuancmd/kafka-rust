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

//! An implementation of ConfigBackingStore that stores Kafka Connect connector
//! configurations in-memory (i.e. configs aren't persisted and will be wiped
//! if the worker is restarted).
//!
//! Corresponds to `org.apache.kafka.connect.storage.MemoryConfigBackingStore` in Java.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_trait::storage::{
    ClusterConfigState, ConfigBackingStore, ConfigBackingStoreUpdateListener, ConnectorTaskId,
    RestartRequest, SessionKey, TargetState,
};

/// Internal state for a connector.
struct ConnectorState {
    /// Target state for the connector
    target_state: TargetState,
    /// Connector configuration
    config: HashMap<String, String>,
    /// Task configurations indexed by ConnectorTaskId
    task_configs: HashMap<ConnectorTaskId, HashMap<String, String>>,
}

impl ConnectorState {
    fn new(config: HashMap<String, String>, target_state: Option<TargetState>) -> Self {
        Self {
            target_state: target_state.unwrap_or(TargetState::Started),
            config,
            task_configs: HashMap::new(),
        }
    }
}

/// An implementation of ConfigBackingStore that stores configurations in-memory.
///
/// Configurations aren't persisted and will be wiped if the worker is restarted.
pub struct MemoryConfigBackingStore {
    /// Map of connector name to connector state
    connectors: Arc<DashMap<String, ConnectorState>>,
    /// Update listener for configuration changes
    update_listener: Option<Arc<dyn ConfigBackingStoreUpdateListener>>,
}

impl MemoryConfigBackingStore {
    /// Creates a new MemoryConfigBackingStore.
    pub fn new() -> Self {
        Self {
            connectors: Arc::new(DashMap::new()),
            update_listener: None,
        }
    }

    /// Converts a list of task configs to a map indexed by ConnectorTaskId.
    fn task_config_list_as_map(
        connector: &str,
        configs: Vec<HashMap<String, String>>,
    ) -> HashMap<ConnectorTaskId, HashMap<String, String>> {
        let mut result = HashMap::new();
        for (index, task_config) in configs.into_iter().enumerate() {
            let task_id = ConnectorTaskId::new(connector.to_string(), index as u32);
            result.insert(task_id, task_config);
        }
        result
    }

    /// Returns the number of connectors stored.
    pub fn connector_count(&self) -> usize {
        self.connectors.len()
    }
}

impl Default for MemoryConfigBackingStore {
    fn default() -> Self {
        Self::new()
    }
}

// Import dashmap for concurrent HashMap
use dashmap::DashMap;

impl ConfigBackingStore for MemoryConfigBackingStore {
    fn start(&mut self) {
        // No-op for memory store
    }

    fn stop(&mut self) {
        // No-op for memory store
    }

    fn snapshot(&self) -> ClusterConfigState {
        let mut connector_task_counts = HashMap::new();
        let mut connector_configs = HashMap::new();
        let mut connector_target_states = HashMap::new();
        let mut task_configs = HashMap::new();

        for entry in self.connectors.iter() {
            let connector = entry.key();
            let state = entry.value();

            connector_task_counts.insert(connector.clone(), state.task_configs.len() as u32);
            connector_configs.insert(connector.clone(), state.config.clone());
            connector_target_states.insert(connector.clone(), state.target_state);
            task_configs.extend(state.task_configs.clone());
        }

        ClusterConfigState {
            offset: ClusterConfigState::NO_OFFSET,
            session_key: None,
            connector_task_counts,
            connector_configs,
            connector_target_states,
            task_configs,
            connector_task_count_records: HashMap::new(),
            connector_task_config_generations: HashMap::new(),
            applied_connector_configs: HashMap::new(),
            connectors_pending_fencing: Vec::new(),
            inconsistent_connectors: Vec::new(),
        }
    }

    fn contains(&self, connector: &str) -> bool {
        self.connectors.contains_key(connector)
    }

    fn put_connector_config(
        &mut self,
        connector: &str,
        properties: HashMap<String, String>,
        target_state: Option<TargetState>,
    ) {
        if let Some(mut state) = self.connectors.get_mut(connector) {
            state.config = properties;
            if let Some(ts) = target_state {
                state.target_state = ts;
            }
        } else {
            self.connectors.insert(
                connector.to_string(),
                ConnectorState::new(properties, target_state),
            );
        }

        if let Some(listener) = &self.update_listener {
            listener.on_connector_config_update(connector);
        }
    }

    fn remove_connector_config(&mut self, connector: &str) {
        if self.connectors.remove(connector).is_some() {
            if let Some(listener) = &self.update_listener {
                listener.on_connector_config_remove(connector);
            }
        }
    }

    fn put_task_configs(&mut self, connector: &str, configs: Vec<HashMap<String, String>>) {
        if !self.connectors.contains_key(connector) {
            panic!("Cannot put tasks for non-existing connector: {}", connector);
        }

        let task_configs_map = Self::task_config_list_as_map(connector, configs);
        let task_ids: Vec<ConnectorTaskId> = task_configs_map.keys().cloned().collect();

        if let Some(mut state) = self.connectors.get_mut(connector) {
            state.task_configs = task_configs_map;
        }

        if let Some(listener) = &self.update_listener {
            listener.on_task_config_update(&task_ids);
        }
    }

    fn remove_task_configs(&mut self, connector: &str) {
        if let Some(mut state) = self.connectors.get_mut(connector) {
            let task_ids: Vec<ConnectorTaskId> = state.task_configs.keys().cloned().collect();
            state.task_configs.clear();

            if let Some(listener) = &self.update_listener {
                listener.on_task_config_update(&task_ids);
            }
        } else {
            panic!(
                "Cannot remove tasks for non-existing connector: {}",
                connector
            );
        }
    }

    fn refresh(&mut self, _timeout: Duration) -> Result<(), std::io::Error> {
        // No-op for memory store - always fresh
        Ok(())
    }

    fn put_target_state(&mut self, connector: &str, state: TargetState) {
        if let Some(mut connector_state) = self.connectors.get_mut(connector) {
            let prev_state = connector_state.target_state;
            connector_state.target_state = state;

            if let Some(listener) = &self.update_listener {
                if state != prev_state {
                    listener.on_connector_target_state_change(connector);
                }
            }
        } else {
            panic!("No connector `{}` configured", connector);
        }
    }

    fn put_session_key(&mut self, _session_key: SessionKey) {
        // No-op for memory store
    }

    fn put_restart_request(&mut self, _restart_request: RestartRequest) {
        // No-op for memory store
    }

    fn put_task_count_record(&mut self, _connector: &str, _task_count: u32) {
        // No-op for memory store
    }

    fn claim_write_privileges(&mut self) {
        // No-op for memory store
    }

    fn put_logger_level(&mut self, _namespace: &str, _level: &str) {
        // No-op for memory store
    }

    fn set_update_listener(&mut self, listener: Box<dyn ConfigBackingStoreUpdateListener>) {
        self.update_listener = Some(Arc::from(listener));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_store_is_empty() {
        let store = MemoryConfigBackingStore::new();
        assert_eq!(store.connector_count(), 0);
    }

    #[test]
    fn test_put_and_contains() {
        let mut store = MemoryConfigBackingStore::new();
        let config = HashMap::from([
            ("name".to_string(), "test-connector".to_string()),
            ("connector.class".to_string(), "TestConnector".to_string()),
        ]);

        store.put_connector_config("test-connector", config, None);
        assert!(store.contains("test-connector"));
        assert_eq!(store.connector_count(), 1);
    }

    #[test]
    fn test_remove_connector_config() {
        let mut store = MemoryConfigBackingStore::new();
        let config = HashMap::from([("name".to_string(), "test-connector".to_string())]);

        store.put_connector_config("test-connector", config, None);
        assert!(store.contains("test-connector"));

        store.remove_connector_config("test-connector");
        assert!(!store.contains("test-connector"));
        assert_eq!(store.connector_count(), 0);
    }

    #[test]
    fn test_snapshot() {
        let mut store = MemoryConfigBackingStore::new();
        let config = HashMap::from([
            ("name".to_string(), "test-connector".to_string()),
            ("connector.class".to_string(), "TestConnector".to_string()),
        ]);

        store.put_connector_config("test-connector", config.clone(), None);
        let snapshot = store.snapshot();

        assert!(snapshot.contains("test-connector"));
        assert_eq!(snapshot.connector_config("test-connector"), Some(&config));
        assert_eq!(snapshot.task_count("test-connector"), 0);
    }

    #[test]
    fn test_put_task_configs() {
        let mut store = MemoryConfigBackingStore::new();
        let conn_config = HashMap::from([("name".to_string(), "test-connector".to_string())]);

        store.put_connector_config("test-connector", conn_config, None);

        let task_config1 = HashMap::from([("task.id".to_string(), "0".to_string())]);
        let task_config2 = HashMap::from([("task.id".to_string(), "1".to_string())]);

        store.put_task_configs(
            "test-connector",
            vec![task_config1.clone(), task_config2.clone()],
        );

        let snapshot = store.snapshot();
        assert_eq!(snapshot.task_count("test-connector"), 2);

        let task_id0 = ConnectorTaskId::new("test-connector".to_string(), 0);
        let task_id1 = ConnectorTaskId::new("test-connector".to_string(), 1);
        assert_eq!(snapshot.task_config(&task_id0), Some(&task_config1));
        assert_eq!(snapshot.task_config(&task_id1), Some(&task_config2));
    }

    #[test]
    fn test_remove_task_configs() {
        let mut store = MemoryConfigBackingStore::new();
        let conn_config = HashMap::from([("name".to_string(), "test-connector".to_string())]);

        store.put_connector_config("test-connector", conn_config, None);

        let task_config = HashMap::from([("task.id".to_string(), "0".to_string())]);
        store.put_task_configs("test-connector", vec![task_config]);

        let snapshot = store.snapshot();
        assert_eq!(snapshot.task_count("test-connector"), 1);

        store.remove_task_configs("test-connector");

        let snapshot = store.snapshot();
        assert_eq!(snapshot.task_count("test-connector"), 0);
    }

    #[test]
    fn test_put_target_state() {
        let mut store = MemoryConfigBackingStore::new();
        let config = HashMap::from([("name".to_string(), "test-connector".to_string())]);

        store.put_connector_config("test-connector", config, None);
        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.target_state("test-connector"),
            Some(TargetState::Started)
        );

        store.put_target_state("test-connector", TargetState::Paused);
        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.target_state("test-connector"),
            Some(TargetState::Paused)
        );
    }

    #[test]
    fn test_put_connector_config_with_target_state() {
        let mut store = MemoryConfigBackingStore::new();
        let config = HashMap::from([("name".to_string(), "test-connector".to_string())]);

        store.put_connector_config("test-connector", config, Some(TargetState::Stopped));
        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.target_state("test-connector"),
            Some(TargetState::Stopped)
        );
    }

    #[test]
    fn test_refresh() {
        let mut store = MemoryConfigBackingStore::new();
        let result = store.refresh(Duration::from_secs(1));
        assert!(result.is_ok());
    }
}
