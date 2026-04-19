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

//! An implementation of StatusBackingStore that stores statuses in-memory.
//!
//! Corresponds to `org.apache.kafka.connect.storage.MemoryStatusBackingStore` in Java.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_trait::storage::{
    ConnectorStatus, ConnectorTaskId, State, StatusBackingStore, TaskStatus, TopicStatus,
    WorkerConfig,
};
use dashmap::DashMap;

/// An implementation of StatusBackingStore that stores statuses in-memory.
pub struct MemoryStatusBackingStore {
    /// Task statuses indexed by (connector_name, task_id)
    tasks: Arc<DashMap<String, HashMap<u32, TaskStatus>>>,
    /// Connector statuses indexed by connector name
    connectors: Arc<DashMap<String, ConnectorStatus>>,
    /// Topic statuses indexed by (connector_name, topic_name)
    topics: Arc<DashMap<String, DashMap<String, TopicStatus>>>,
}

impl MemoryStatusBackingStore {
    /// Creates a new MemoryStatusBackingStore.
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(DashMap::new()),
            connectors: Arc::new(DashMap::new()),
            topics: Arc::new(DashMap::new()),
        }
    }

    /// Returns the number of connectors with stored status.
    pub fn connector_count(&self) -> usize {
        self.connectors.len()
    }

    /// Returns the total number of tasks with stored status.
    pub fn task_count(&self) -> usize {
        self.tasks.iter().map(|entry| entry.value().len()).sum()
    }
}

impl Default for MemoryStatusBackingStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StatusBackingStore for MemoryStatusBackingStore {
    fn configure(&mut self, _config: &WorkerConfig) {
        // No configuration needed for memory store
    }

    fn start(&mut self) {
        // No-op for memory store
    }

    fn stop(&mut self) {
        // No-op for memory store
    }

    fn put_connector_status(&mut self, status: ConnectorStatus) {
        if status.state == State::Destroyed {
            self.connectors.remove(&status.id);
        } else {
            self.connectors.insert(status.id.clone(), status);
        }
    }

    fn put_connector_status_safe(&mut self, status: ConnectorStatus) {
        // For memory store, safe put is the same as regular put
        self.put_connector_status(status);
    }

    fn put_task_status(&mut self, status: TaskStatus) {
        let connector = status.id.connector.clone();
        let task = status.id.task;

        if status.state == State::Destroyed {
            if let Some(mut tasks_map) = self.tasks.get_mut(&connector) {
                tasks_map.remove(&task);
                if tasks_map.is_empty() {
                    // Remove empty connector entry
                    self.tasks.remove(&connector);
                }
            }
        } else {
            self.tasks
                .entry(connector)
                .or_insert_with(HashMap::new)
                .insert(task, status);
        }
    }

    fn put_task_status_safe(&mut self, status: TaskStatus) {
        // For memory store, safe put is the same as regular put
        self.put_task_status(status);
    }

    fn put_topic_status(&mut self, status: TopicStatus) {
        self.topics
            .entry(status.connector.clone())
            .or_insert_with(DashMap::new)
            .insert(status.topic.clone(), status);
    }

    fn get_task_status(&self, id: &ConnectorTaskId) -> Option<TaskStatus> {
        self.tasks
            .get(&id.connector)
            .and_then(|tasks_map| tasks_map.get(&id.task).cloned())
    }

    fn get_connector_status(&self, connector: &str) -> Option<ConnectorStatus> {
        self.connectors.get(connector).map(|s| s.clone())
    }

    fn get_all_task_statuses(&self, connector: &str) -> Vec<TaskStatus> {
        self.tasks
            .get(connector)
            .map(|tasks_map| tasks_map.value().values().cloned().collect())
            .unwrap_or_default()
    }

    fn get_topic_status(&self, connector: &str, topic: &str) -> Option<TopicStatus> {
        self.topics
            .get(connector)
            .and_then(|topics_map| topics_map.get(topic).map(|s| s.clone()))
    }

    fn get_all_topic_statuses(&self, connector: &str) -> Vec<TopicStatus> {
        self.topics
            .get(connector)
            .map(|topics_map| {
                topics_map
                    .iter()
                    .map(|entry| entry.value().clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn delete_topic(&mut self, connector: &str, topic: &str) {
        if let Some(topics_map) = self.topics.get(connector) {
            topics_map.remove(topic);
        }
    }

    fn connectors(&self) -> HashSet<String> {
        self.connectors
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    fn flush(&mut self) {
        // No-op for memory store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_connector_status(id: &str, state: State) -> ConnectorStatus {
        ConnectorStatus::new(
            id.to_string(),
            state,
            "worker-1".to_string(),
            0,
            None,
            Some("1.0".to_string()),
        )
    }

    fn create_task_status(connector: &str, task: u32, state: State) -> TaskStatus {
        TaskStatus::new(
            ConnectorTaskId::new(connector.to_string(), task),
            state,
            "worker-1".to_string(),
            0,
            None,
            Some("1.0".to_string()),
        )
    }

    fn create_topic_status(connector: &str, topic: &str, state: State) -> TopicStatus {
        TopicStatus::new(connector.to_string(), topic.to_string(), state)
    }

    #[test]
    fn test_new_store_is_empty() {
        let store = MemoryStatusBackingStore::new();
        assert_eq!(store.connector_count(), 0);
        assert_eq!(store.task_count(), 0);
    }

    #[test]
    fn test_put_and_get_connector_status() {
        let mut store = MemoryStatusBackingStore::new();
        let status = create_connector_status("test-connector", State::Running);

        store.put_connector_status(status.clone());
        assert_eq!(store.connector_count(), 1);

        let retrieved = store.get_connector_status("test-connector");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().state, State::Running);
    }

    #[test]
    fn test_put_connector_status_destroyed() {
        let mut store = MemoryStatusBackingStore::new();
        let status = create_connector_status("test-connector", State::Running);
        store.put_connector_status(status);

        assert_eq!(store.connector_count(), 1);

        let destroyed_status = create_connector_status("test-connector", State::Destroyed);
        store.put_connector_status(destroyed_status);

        assert_eq!(store.connector_count(), 0);
        assert!(store.get_connector_status("test-connector").is_none());
    }

    #[test]
    fn test_put_and_get_task_status() {
        let mut store = MemoryStatusBackingStore::new();
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let status = create_task_status("test-connector", 0, State::Running);

        store.put_task_status(status.clone());
        assert_eq!(store.task_count(), 1);

        let retrieved = store.get_task_status(&task_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().state, State::Running);
    }

    #[test]
    fn test_put_task_status_destroyed() {
        let mut store = MemoryStatusBackingStore::new();
        let status = create_task_status("test-connector", 0, State::Running);
        store.put_task_status(status);

        assert_eq!(store.task_count(), 1);

        let destroyed_status = create_task_status("test-connector", 0, State::Destroyed);
        store.put_task_status(destroyed_status);

        assert_eq!(store.task_count(), 0);
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        assert!(store.get_task_status(&task_id).is_none());
    }

    #[test]
    fn test_get_all_task_statuses() {
        let mut store = MemoryStatusBackingStore::new();
        let status0 = create_task_status("test-connector", 0, State::Running);
        let status1 = create_task_status("test-connector", 1, State::Paused);

        store.put_task_status(status0);
        store.put_task_status(status1);

        let all_statuses = store.get_all_task_statuses("test-connector");
        assert_eq!(all_statuses.len(), 2);
    }

    #[test]
    fn test_put_and_get_topic_status() {
        let mut store = MemoryStatusBackingStore::new();
        let status = create_topic_status("test-connector", "test-topic", State::Running);

        store.put_topic_status(status.clone());

        let retrieved = store.get_topic_status("test-connector", "test-topic");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().state, State::Running);
    }

    #[test]
    fn test_get_all_topic_statuses() {
        let mut store = MemoryStatusBackingStore::new();
        let status1 = create_topic_status("test-connector", "topic-1", State::Running);
        let status2 = create_topic_status("test-connector", "topic-2", State::Running);

        store.put_topic_status(status1);
        store.put_topic_status(status2);

        let all_statuses = store.get_all_topic_statuses("test-connector");
        assert_eq!(all_statuses.len(), 2);
    }

    #[test]
    fn test_delete_topic() {
        let mut store = MemoryStatusBackingStore::new();
        let status = create_topic_status("test-connector", "test-topic", State::Running);
        store.put_topic_status(status);

        assert!(store
            .get_topic_status("test-connector", "test-topic")
            .is_some());

        store.delete_topic("test-connector", "test-topic");
        assert!(store
            .get_topic_status("test-connector", "test-topic")
            .is_none());
    }

    #[test]
    fn test_connectors() {
        let mut store = MemoryStatusBackingStore::new();
        let status1 = create_connector_status("connector-1", State::Running);
        let status2 = create_connector_status("connector-2", State::Running);

        store.put_connector_status(status1);
        store.put_connector_status(status2);

        let connectors = store.connectors();
        assert_eq!(connectors.len(), 2);
        assert!(connectors.contains("connector-1"));
        assert!(connectors.contains("connector-2"));
    }

    #[test]
    fn test_put_connector_status_safe() {
        let mut store = MemoryStatusBackingStore::new();
        let status = create_connector_status("test-connector", State::Running);

        store.put_connector_status_safe(status.clone());
        let retrieved = store.get_connector_status("test-connector");
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_put_task_status_safe() {
        let mut store = MemoryStatusBackingStore::new();
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let status = create_task_status("test-connector", 0, State::Running);

        store.put_task_status_safe(status);
        let retrieved = store.get_task_status(&task_id);
        assert!(retrieved.is_some());
    }
}
