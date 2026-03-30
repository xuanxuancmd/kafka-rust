//! Herder module tests

use connect_runtime_core::herder::{
    ConnectorInfo, ConnectorStateInfo, Created, DistributedHerder, Herder,
    InternalRequestSignature, RestartRequest, StandaloneHerder, VersionRange,
};
use connect_runtime_core::storage::{
    ConnectorStatus, ConnectorTaskId as StorageConnectorTaskId, MemoryStatusBackingStore,
    StatusBackingStore, TaskState as StorageTaskState, TaskStatus,
};
use connect_runtime_core::{WorkerConnectorState, WorkerConnectorTaskId, WorkerTargetState};
use std::collections::HashMap;

#[test]
fn test_standalone_herder_new() {
    let herder = StandaloneHerder::new();

    // Herder should be created successfully
    assert!(!herder.is_ready());
}

#[test]
fn test_standalone_herder_start() {
    let mut herder = StandaloneHerder::new();

    // Start should succeed
    let result = herder.start();
    assert!(result.is_ok());

    // Should be ready after start
    assert!(herder.is_ready());

    // Starting again should fail
    let result = herder.start();
    assert!(result.is_err());
}

#[test]
fn test_standalone_herder_stop() {
    let mut herder = StandaloneHerder::new();

    // Stop before start should fail
    let result = herder.stop();
    assert!(result.is_err());

    // Start then stop should succeed
    let _ = herder.start();
    let result = herder.stop();
    assert!(result.is_ok());

    // Should not be ready after stop
    assert!(!herder.is_ready());
}

#[test]
fn test_standalone_herder_connectors_sync() {
    let herder = StandaloneHerder::new();

    // Initially should have no connectors
    let connectors = herder.connectors_sync();
    assert!(connectors.is_empty());
}

#[test]
fn test_standalone_herder_connector_info_sync() {
    let herder = StandaloneHerder::new();

    // Non-existent connector should return None
    let info = herder.connector_info_sync("non-existent".to_string());
    assert!(info.is_none());
}

#[test]
fn test_standalone_herder_connector_status() {
    let herder = StandaloneHerder::new();

    // Non-existent connector should return None
    let status = herder.connector_status("non-existent".to_string());
    assert!(status.is_none());
}

#[test]
fn test_standalone_herder_task_status() {
    let herder = StandaloneHerder::new();

    // Non-existent task should return Some (default implementation)
    let task_id = WorkerConnectorTaskId {
        connector: "test".to_string(),
        task: 0,
    };
    let status = herder.task_status(task_id);
    assert!(status.is_some());
}

#[test]
fn test_standalone_herder_connector_active_topics() {
    let herder = StandaloneHerder::new();

    // Should return empty vector
    let topics = herder.connector_active_topics("test".to_string());
    assert!(topics.is_empty());
}

#[test]
fn test_standalone_herder_reset_connector_active_topics() {
    let herder = StandaloneHerder::new();

    // Should not panic
    herder.reset_connector_active_topics("test".to_string());
}

#[test]
fn test_standalone_herder_kafka_cluster_id() {
    let herder = StandaloneHerder::new();

    // Should return None
    let cluster_id = herder.kafka_cluster_id();
    assert!(cluster_id.is_none());
}

#[test]
fn test_standalone_herder_connector_plugin_config() {
    let herder = StandaloneHerder::new();

    // Should return None
    let config = herder.connector_plugin_config("test-plugin".to_string());
    assert!(config.is_none());
}

#[test]
fn test_standalone_herder_connector_plugin_config_with_version() {
    let herder = StandaloneHerder::new();

    // Should return None
    let version_range = VersionRange {
        min_version: Some("1.0.0".to_string()),
        max_version: Some("2.0.0".to_string()),
    };
    let config =
        herder.connector_plugin_config_with_version("test-plugin".to_string(), version_range);
    assert!(config.is_none());
}

#[test]
fn test_standalone_herder_logger_level() {
    let herder = StandaloneHerder::new();

    // Should return None
    let level = herder.logger_level("test.logger".to_string());
    assert!(level.is_none());
}

#[test]
fn test_standalone_herder_all_logger_levels() {
    let herder = StandaloneHerder::new();

    // Should return empty map
    let levels = herder.all_logger_levels();
    assert!(levels.is_empty());
}

#[test]
fn test_standalone_herder_set_worker_logger_level() {
    let herder = StandaloneHerder::new();

    // Should succeed
    let result = herder.set_worker_logger_level("test.namespace".to_string(), "INFO".to_string());
    assert!(result.is_ok());
}

#[test]
fn test_standalone_herder_set_cluster_logger_level() {
    let herder = StandaloneHerder::new();

    // Should succeed
    let result = herder.set_cluster_logger_level("test.namespace".to_string(), "INFO".to_string());
    assert!(result.is_ok());
}

#[test]
fn test_distributed_herder_new() {
    let herder = DistributedHerder::new();

    // Herder should be created successfully
    assert!(!herder.is_ready());
}

#[test]
fn test_distributed_herder_start() {
    let mut herder = DistributedHerder::new();

    // Start should succeed
    let result = herder.start();
    assert!(result.is_ok());

    // Should be ready after start
    assert!(herder.is_ready());

    // Starting again should fail
    let result = herder.start();
    assert!(result.is_err());
}

#[test]
fn test_distributed_herder_stop() {
    let mut herder = DistributedHerder::new();

    // Stop before start should fail
    let result = herder.stop();
    assert!(result.is_err());

    // Start then stop should succeed
    let _ = herder.start();
    let result = herder.stop();
    assert!(result.is_ok());

    // Should not be ready after stop
    assert!(!herder.is_ready());
}

#[test]
fn test_distributed_herder_connectors_sync() {
    let herder = DistributedHerder::new();

    // Initially should have no connectors
    let connectors = herder.connectors_sync();
    assert!(connectors.is_empty());
}

#[test]
fn test_distributed_herder_connector_info_sync() {
    let herder = DistributedHerder::new();

    // Non-existent connector should return None
    let info = herder.connector_info_sync("non-existent".to_string());
    assert!(info.is_none());
}

#[test]
fn test_distributed_herder_connector_status() {
    let herder = DistributedHerder::new();

    // Non-existent connector should return None
    let status = herder.connector_status("non-existent".to_string());
    assert!(status.is_none());
}

#[test]
fn test_distributed_herder_task_status() {
    let herder = DistributedHerder::new();

    // Non-existent task should return Some (default implementation)
    let task_id = WorkerConnectorTaskId {
        connector: "test".to_string(),
        task: 0,
    };
    let status = herder.task_status(task_id);
    assert!(status.is_some());
}

#[test]
fn test_distributed_herder_connector_active_topics() {
    let herder = DistributedHerder::new();

    // Should return empty vector
    let topics = herder.connector_active_topics("test".to_string());
    assert!(topics.is_empty());
}

#[test]
fn test_distributed_herder_reset_connector_active_topics() {
    let herder = DistributedHerder::new();

    // Should not panic
    herder.reset_connector_active_topics("test".to_string());
}

#[test]
fn test_distributed_herder_kafka_cluster_id() {
    let herder = DistributedHerder::new();

    // Should return None
    let cluster_id = herder.kafka_cluster_id();
    assert!(cluster_id.is_none());
}

#[test]
fn test_distributed_herder_connector_plugin_config() {
    let herder = DistributedHerder::new();

    // Should return None
    let config = herder.connector_plugin_config("test-plugin".to_string());
    assert!(config.is_none());
}

#[test]
fn test_distributed_herder_connector_plugin_config_with_version() {
    let herder = DistributedHerder::new();

    // Should return None
    let version_range = VersionRange {
        min_version: Some("1.0.0".to_string()),
        max_version: Some("2.0.0".to_string()),
    };
    let config =
        herder.connector_plugin_config_with_version("test-plugin".to_string(), version_range);
    assert!(config.is_none());
}

#[test]
fn test_distributed_herder_logger_level() {
    let herder = DistributedHerder::new();

    // Should return None
    let level = herder.logger_level("test.logger".to_string());
    assert!(level.is_none());
}

#[test]
fn test_distributed_herder_all_logger_levels() {
    let herder = DistributedHerder::new();

    // Should return empty map
    let levels = herder.all_logger_levels();
    assert!(levels.is_empty());
}

#[test]
fn test_distributed_herder_set_worker_logger_level() {
    let herder = DistributedHerder::new();

    // Should succeed
    let result = herder.set_worker_logger_level("test.namespace".to_string(), "INFO".to_string());
    assert!(result.is_ok());
}

#[test]
fn test_distributed_herder_set_cluster_logger_level() {
    let herder = DistributedHerder::new();

    // Should succeed
    let result = herder.set_cluster_logger_level("test.namespace".to_string(), "INFO".to_string());
    assert!(result.is_ok());
}

#[test]
fn test_task_state_equality() {
    use connect_runtime_core::herder::TaskState;
    assert_eq!(TaskState::Running, TaskState::Running);
    assert_eq!(TaskState::Stopped, TaskState::Stopped);
    assert_eq!(TaskState::Paused, TaskState::Paused);
    assert_ne!(TaskState::Running, TaskState::Stopped);
}

#[test]
fn test_connector_state_equality() {
    assert_eq!(WorkerConnectorState::Running, WorkerConnectorState::Running);
    assert_eq!(WorkerConnectorState::Stopped, WorkerConnectorState::Stopped);
    assert_eq!(WorkerConnectorState::Paused, WorkerConnectorState::Paused);
    assert_ne!(WorkerConnectorState::Running, WorkerConnectorState::Stopped);
}

#[test]
fn test_created_struct() {
    let created = Created {
        created: true,
        value: ConnectorInfo {
            name: "test".to_string(),
            config: HashMap::new(),
            tasks: Vec::new(),
        },
    };

    assert!(created.created);
    assert_eq!(created.value.name, "test");
}

#[test]
fn test_internal_request_signature() {
    let signature = InternalRequestSignature {
        signature: "test-signature".to_string(),
    };

    assert_eq!(signature.signature, "test-signature");
}

#[test]
fn test_version_range() {
    let version_range = VersionRange {
        min_version: Some("1.0.0".to_string()),
        max_version: Some("2.0.0".to_string()),
    };

    assert_eq!(version_range.min_version, Some("1.0.0".to_string()));
    assert_eq!(version_range.max_version, Some("2.0.0".to_string()));
}

#[test]
fn test_restart_request() {
    let request = RestartRequest {
        connector_name: "test-connector".to_string(),
        include_tasks: true,
        only_failed: false,
    };

    assert_eq!(request.connector_name, "test-connector");
    assert!(request.include_tasks);
    assert!(!request.only_failed);
}

#[test]
fn test_connector_state_info() {
    let state_info = ConnectorStateInfo {
        name: "test-connector".to_string(),
        state: WorkerConnectorState::Running,
        task_states: HashMap::new(),
    };

    assert_eq!(state_info.name, "test-connector");
    assert_eq!(state_info.state, WorkerConnectorState::Running);
}

#[test]
fn test_memory_status_backing_store() {
    let mut store = MemoryStatusBackingStore::new();

    // Start should succeed
    let result = store.start();
    assert!(result.is_ok());

    // Put connector status
    let status = ConnectorStatus {
        name: "test-connector".to_string(),
        state: connect_runtime_core::storage::ConnectorState::Running,
        trace: "test trace".to_string(),
        worker_id: "worker-1".to_string(),
    };
    let result = store.put(status.clone());
    assert!(result.is_ok());

    // Get connector status
    let retrieved = store.get("test-connector".to_string());
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name, "test-connector");

    // Stop should succeed
    let result = store.stop();
    assert!(result.is_ok());
}

#[test]
fn test_memory_status_backing_store_task_status() {
    let mut store = MemoryStatusBackingStore::new();

    // Start should succeed
    let _ = store.start();

    // Put task status
    let task_id = StorageConnectorTaskId {
        connector: "test-connector".to_string(),
        task: 0,
    };
    let task_status = TaskStatus {
        id: task_id.clone(),
        state: StorageTaskState::Running,
        trace: "test trace".to_string(),
        worker_id: "worker-1".to_string(),
    };
    let result = store.put_task(task_status.clone());
    assert!(result.is_ok());

    // Get task status
    let retrieved = store.get_task(task_id);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().id.connector, "test-connector");

    // Get all tasks for connector
    let all_tasks = store.get_all("test-connector".to_string());
    assert_eq!(all_tasks.len(), 1);
}
