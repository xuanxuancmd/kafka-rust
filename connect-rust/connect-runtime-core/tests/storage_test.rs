//! Storage module tests

use connect_runtime_core::storage::{
    Callback, ConfigBackingStore, ConnectorState, ConnectorStatus, ConnectorTaskId,
    MemoryConfigBackingStore, MemoryOffsetBackingStore, MemoryStatusBackingStore,
    OffsetBackingStore, StatusBackingStore, TargetState, TaskState, TaskStatus, TopicState,
    TopicStatus,
};
use std::any::Any;
use std::collections::HashMap;

// Simple callback implementation for testing
struct TestCallback<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> TestCallback<T> {
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Callback<T> for TestCallback<T> {
    fn call(&self, _result: T) {}
}

#[test]
fn test_memory_status_backing_store_new() {
    let _store = MemoryStatusBackingStore::new();

    // Store should be created successfully
}

#[test]
fn test_memory_status_backing_store_start() {
    let mut store = MemoryStatusBackingStore::new();

    // Start should succeed
    let result = store.start();
    assert!(result.is_ok());

    // Starting again should fail
    let result = store.start();
    assert!(result.is_err());
}

#[test]
fn test_memory_status_backing_store_stop() {
    let mut store = MemoryStatusBackingStore::new();

    // Stop before start should fail
    let result = store.stop();
    assert!(result.is_err());

    // Start then stop should succeed
    let _ = store.start();
    let result = store.stop();
    assert!(result.is_ok());
}

#[test]
fn test_memory_status_backing_store_put_and_get() {
    let mut store = MemoryStatusBackingStore::new();
    let _ = store.start();

    let status = ConnectorStatus {
        name: "test-connector".to_string(),
        state: ConnectorState::Running,
        trace: "test trace".to_string(),
        worker_id: "worker-1".to_string(),
    };

    // Put should succeed
    let result = store.put(status.clone());
    assert!(result.is_ok());

    // Get should return the status
    let retrieved = store.get("test-connector".to_string());
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name, "test-connector");
}

#[test]
fn test_memory_status_backing_store_put_safe() {
    let mut store = MemoryStatusBackingStore::new();
    let _ = store.start();

    let status = ConnectorStatus {
        name: "test-connector".to_string(),
        state: ConnectorState::Running,
        trace: "test trace".to_string(),
        worker_id: "worker-1".to_string(),
    };

    // put_safe should succeed
    let result = store.put_safe(status.clone());
    assert!(result.is_ok());

    // Get should return the status
    let retrieved = store.get("test-connector".to_string());
    assert!(retrieved.is_some());
}

#[test]
fn test_memory_status_backing_store_get_all() {
    let mut store = MemoryStatusBackingStore::new();
    let _ = store.start();

    let task_id1 = ConnectorTaskId {
        connector: "test-connector".to_string(),
        task: 0,
    };
    let task_status1 = TaskStatus {
        id: task_id1.clone(),
        state: TaskState::Running,
        trace: "trace1".to_string(),
        worker_id: "worker-1".to_string(),
    };

    let task_id2 = ConnectorTaskId {
        connector: "test-connector".to_string(),
        task: 1,
    };
    let task_status2 = TaskStatus {
        id: task_id2.clone(),
        state: TaskState::Running,
        trace: "trace2".to_string(),
        worker_id: "worker-1".to_string(),
    };

    let _ = store.put_task(task_status1);
    let _ = store.put_task(task_status2);

    // get_all should return both tasks
    let tasks = store.get_all("test-connector".to_string());
    assert_eq!(tasks.len(), 2);
}

#[test]
fn test_memory_status_backing_store_put_task_and_get_task() {
    let mut store = MemoryStatusBackingStore::new();
    let _ = store.start();

    let task_id = ConnectorTaskId {
        connector: "test-connector".to_string(),
        task: 0,
    };
    let task_status = TaskStatus {
        id: task_id.clone(),
        state: TaskState::Running,
        trace: "test trace".to_string(),
        worker_id: "worker-1".to_string(),
    };

    // put_task should succeed
    let result = store.put_task(task_status.clone());
    assert!(result.is_ok());

    // get_task should return the status
    let retrieved = store.get_task(task_id);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().id.connector, "test-connector");
}

#[test]
fn test_memory_status_backing_store_put_task_safe() {
    let mut store = MemoryStatusBackingStore::new();
    let _ = store.start();

    let task_id = ConnectorTaskId {
        connector: "test-connector".to_string(),
        task: 0,
    };
    let task_status = TaskStatus {
        id: task_id.clone(),
        state: TaskState::Running,
        trace: "test trace".to_string(),
        worker_id: "worker-1".to_string(),
    };

    // put_task_safe should succeed
    let result = store.put_task_safe(task_status.clone());
    assert!(result.is_ok());

    // get_task should return the status
    let retrieved = store.get_task(task_id);
    assert!(retrieved.is_some());
}

#[test]
fn test_memory_status_backing_store_put_topic_and_get_topic() {
    let mut store = MemoryStatusBackingStore::new();
    let _ = store.start();

    let topic_status = TopicStatus {
        connector: "test-connector".to_string(),
        topic: "test-topic".to_string(),
        state: TopicState::Active,
        partition_count: 3,
    };

    // put_topic should succeed
    let result = store.put_topic(topic_status.clone());
    assert!(result.is_ok());

    // get_topic should return the status
    let retrieved = store.get_topic("test-connector".to_string(), "test-topic".to_string());
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().topic, "test-topic");
}

#[test]
fn test_memory_status_backing_store_get_all_topics() {
    let mut store = MemoryStatusBackingStore::new();
    let _ = store.start();

    let topic_status1 = TopicStatus {
        connector: "test-connector".to_string(),
        topic: "topic1".to_string(),
        state: TopicState::Active,
        partition_count: 3,
    };

    let topic_status2 = TopicStatus {
        connector: "test-connector".to_string(),
        topic: "topic2".to_string(),
        state: TopicState::Active,
        partition_count: 5,
    };

    let _ = store.put_topic(topic_status1);
    let _ = store.put_topic(topic_status2);

    // get_all_topics should return both topics
    let topics = store.get_all_topics("test-connector".to_string());
    assert_eq!(topics.len(), 2);
}

#[test]
fn test_memory_status_backing_store_delete_topic() {
    let mut store = MemoryStatusBackingStore::new();
    let _ = store.start();

    let topic_status = TopicStatus {
        connector: "test-connector".to_string(),
        topic: "test-topic".to_string(),
        state: TopicState::Active,
        partition_count: 3,
    };

    let _ = store.put_topic(topic_status);

    // delete_topic should succeed
    let result = store.delete_topic("test-connector".to_string(), "test-topic".to_string());
    assert!(result.is_ok());

    // Topic should be deleted
    let retrieved = store.get_topic("test-connector".to_string(), "test-topic".to_string());
    assert!(retrieved.is_none());
}

#[test]
fn test_memory_config_backing_store_new() {
    let _store = MemoryConfigBackingStore::new();

    // Store should be created successfully
}

#[test]
fn test_memory_config_backing_store_start() {
    let mut store = MemoryConfigBackingStore::new();

    // Start should succeed
    let result = store.start();
    assert!(result.is_ok());

    // Starting again should fail
    let result = store.start();
    assert!(result.is_err());
}

#[test]
fn test_memory_config_backing_store_stop() {
    let mut store = MemoryConfigBackingStore::new();

    // Stop before start should fail
    let result = store.stop();
    assert!(result.is_err());

    // Start then stop should succeed
    let _ = store.start();
    let result = store.stop();
    assert!(result.is_ok());
}

#[test]
fn test_memory_config_backing_store_snapshot() {
    let mut store = MemoryConfigBackingStore::new();
    let _ = store.start();

    let mut config = HashMap::new();
    config.insert("key1".to_string(), "value1".to_string());

    let _ = store.put_connector_config(
        "test-connector".to_string(),
        config.clone(),
        TargetState::Started,
    );

    // snapshot should return the config
    let snapshot = store.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot.contains_key("test-connector"));
}

#[test]
fn test_memory_config_backing_store_contains() {
    let mut store = MemoryConfigBackingStore::new();
    let _ = store.start();

    let config = HashMap::new();
    let _ = store.put_connector_config("test-connector".to_string(), config, TargetState::Started);

    // contains should return true
    assert!(store.contains("test-connector".to_string()));

    // contains should return false for non-existent connector
    assert!(!store.contains("non-existent".to_string()));
}

#[test]
fn test_memory_config_backing_store_put_connector_config() {
    let mut store = MemoryConfigBackingStore::new();
    let _ = store.start();

    let mut config = HashMap::new();
    config.insert("key1".to_string(), "value1".to_string());

    // put_connector_config should succeed
    let result = store.put_connector_config(
        "test-connector".to_string(),
        config.clone(),
        TargetState::Started,
    );
    assert!(result.is_ok());

    // Config should be retrievable
    let snapshot = store.snapshot();
    assert_eq!(snapshot.get("test-connector").unwrap(), &config);
}

#[test]
fn test_memory_config_backing_store_remove_connector_config() {
    let mut store = MemoryConfigBackingStore::new();
    let _ = store.start();

    let config = HashMap::new();
    let _ = store.put_connector_config("test-connector".to_string(), config, TargetState::Started);

    // remove_connector_config should succeed
    let result = store.remove_connector_config("test-connector".to_string());
    assert!(result.is_ok());

    // Config should be removed
    assert!(!store.contains("test-connector".to_string()));
}

#[test]
fn test_memory_config_backing_store_put_task_configs() {
    let mut store = MemoryConfigBackingStore::new();
    let _ = store.start();

    let mut task_config1 = HashMap::new();
    task_config1.insert("task.key1".to_string(), "task.value1".to_string());

    let mut task_config2 = HashMap::new();
    task_config2.insert("task.key2".to_string(), "task.value2".to_string());

    let configs = vec![task_config1, task_config2];

    // put_task_configs should succeed
    let result = store.put_task_configs("test-connector".to_string(), configs);
    assert!(result.is_ok());
}

#[test]
fn test_memory_config_backing_store_remove_task_configs() {
    let mut store = MemoryConfigBackingStore::new();
    let _ = store.start();

    let configs = vec![HashMap::new()];
    let _ = store.put_task_configs("test-connector".to_string(), configs);

    // remove_task_configs should succeed
    let result = store.remove_task_configs("test-connector".to_string());
    assert!(result.is_ok());
}

#[test]
fn test_memory_config_backing_store_put_target_state() {
    let mut store = MemoryConfigBackingStore::new();
    let _ = store.start();

    // put_target_state should succeed
    let result = store.put_target_state("test-connector".to_string(), TargetState::Started);
    assert!(result.is_ok());
}

#[test]
fn test_memory_config_backing_store_claim_write_privileges() {
    let mut store = MemoryConfigBackingStore::new();
    let _ = store.start();

    // claim_write_privileges should succeed
    let result = store.claim_write_privileges();
    assert!(result.is_ok());
}

#[test]
fn test_memory_offset_backing_store_new() {
    let _store = MemoryOffsetBackingStore::new();

    // Store should be created successfully
}

#[test]
fn test_memory_offset_backing_store_start() {
    let mut store = MemoryOffsetBackingStore::new();

    // Start should succeed
    let result = store.start();
    assert!(result.is_ok());

    // Starting again should fail
    let result = store.start();
    assert!(result.is_err());
}

#[test]
fn test_memory_offset_backing_store_stop() {
    let mut store = MemoryOffsetBackingStore::new();

    // Stop before start should fail
    let result = store.stop();
    assert!(result.is_err());

    // Start then stop should succeed
    let _ = store.start();
    let result = store.stop();
    assert!(result.is_ok());
}

#[test]
fn test_memory_offset_backing_store_configure() {
    let mut store = MemoryOffsetBackingStore::new();
    let _ = store.start();

    let mut configs = HashMap::new();
    configs.insert(
        "key1".to_string(),
        Box::new("value1".to_string()) as Box<dyn std::any::Any>,
    );

    // configure should succeed
    let result = store.configure(configs);
    assert!(result.is_ok());
}

#[test]
fn test_memory_offset_backing_store_get() {
    let mut store = MemoryOffsetBackingStore::new();
    let _ = store.start();

    let partition: HashMap<String, String> = HashMap::new();

    // get should succeed (callback will be called)
    let callback: Box<dyn Callback<HashMap<String, Box<dyn Any>>>> = Box::new(TestCallback::new());
    let result = store.get(partition, callback);
    assert!(result.is_ok());
}

#[test]
fn test_memory_offset_backing_store_put() {
    let mut store = MemoryOffsetBackingStore::new();
    let _ = store.start();

    let mut offsets = HashMap::new();
    let mut offset_map = HashMap::new();
    offset_map.insert("offset".to_string(), Box::new(0i64) as Box<dyn Any>);
    offsets.insert("partition".to_string(), offset_map);

    // put should succeed (callback will be called)
    let callback: Box<dyn Callback<()>> = Box::new(TestCallback::new());
    let result = store.put(offsets, callback);
    assert!(result.is_ok());
}

#[test]
fn test_memory_offset_backing_store_connector_partitions() {
    let mut store = MemoryOffsetBackingStore::new();
    let _ = store.start();

    // connector_partitions should return empty vector
    let partitions = store.connector_partitions("test-connector".to_string());
    assert!(partitions.is_empty());
}

#[test]
fn test_connector_state_equality() {
    assert_eq!(ConnectorState::Running, ConnectorState::Running);
    assert_eq!(ConnectorState::Stopped, ConnectorState::Stopped);
    assert_eq!(ConnectorState::Paused, ConnectorState::Paused);
    assert_ne!(ConnectorState::Running, ConnectorState::Stopped);
}

#[test]
fn test_task_state_equality() {
    assert_eq!(TaskState::Running, TaskState::Running);
    assert_eq!(TaskState::Stopped, TaskState::Stopped);
    assert_eq!(TaskState::Paused, TaskState::Paused);
    assert_ne!(TaskState::Running, TaskState::Stopped);
}

#[test]
fn test_topic_state_equality() {
    assert_eq!(TopicState::Active, TopicState::Active);
    assert_eq!(TopicState::Inactive, TopicState::Inactive);
    assert_eq!(TopicState::Failed, TopicState::Failed);
    assert_ne!(TopicState::Active, TopicState::Inactive);
}

#[test]
fn test_target_state_equality() {
    assert_eq!(TargetState::Started, TargetState::Started);
    assert_eq!(TargetState::Stopped, TargetState::Stopped);
    assert_eq!(TargetState::Paused, TargetState::Paused);
    assert_ne!(TargetState::Started, TargetState::Stopped);
}

#[test]
fn test_connector_task_id_equality() {
    let task_id1 = ConnectorTaskId {
        connector: "test".to_string(),
        task: 0,
    };
    let task_id2 = ConnectorTaskId {
        connector: "test".to_string(),
        task: 0,
    };
    let task_id3 = ConnectorTaskId {
        connector: "test".to_string(),
        task: 1,
    };

    assert_eq!(task_id1, task_id2);
    assert_ne!(task_id1, task_id3);
}

#[test]
fn test_connector_status_clone() {
    let status = ConnectorStatus {
        name: "test-connector".to_string(),
        state: ConnectorState::Running,
        trace: "test trace".to_string(),
        worker_id: "worker-1".to_string(),
    };

    let cloned = status.clone();
    assert_eq!(cloned.name, status.name);
    assert_eq!(cloned.state, status.state);
}

#[test]
fn test_task_status_clone() {
    let task_id = ConnectorTaskId {
        connector: "test".to_string(),
        task: 0,
    };
    let status = TaskStatus {
        id: task_id.clone(),
        state: TaskState::Running,
        trace: "test trace".to_string(),
        worker_id: "worker-1".to_string(),
    };

    let cloned = status.clone();
    assert_eq!(cloned.id, status.id);
    assert_eq!(cloned.state, status.state);
}

#[test]
fn test_topic_status_clone() {
    let status = TopicStatus {
        connector: "test-connector".to_string(),
        topic: "test-topic".to_string(),
        state: TopicState::Active,
        partition_count: 3,
    };

    let cloned = status.clone();
    assert_eq!(cloned.connector, status.connector);
    assert_eq!(cloned.topic, status.topic);
}
