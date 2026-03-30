//! Worker module tests

use connect_runtime_core::worker::ConnectorConfig;
use connect_runtime_core::worker::WorkerImpl;
use connect_runtime_core::{
    Worker, WorkerConnectorState as ConnectorState, WorkerConnectorStatus as ConnectorStatus,
    WorkerConnectorTaskId as ConnectorTaskId, WorkerTargetState as TargetState,
    WorkerWorkerConfig as WorkerConfig,
};
use std::collections::HashMap;

#[test]
fn test_worker_impl_new() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let worker = WorkerImpl::new(worker_id.clone(), config);

    // Worker should be created successfully
    assert_eq!(worker.config().worker_id, worker_id);
}

#[test]
fn test_worker_impl_start() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let mut worker = WorkerImpl::new(worker_id, config);

    // Start should succeed
    let result = worker.start();
    assert!(result.is_ok());

    // Starting again should fail
    let result = worker.start();
    assert!(result.is_err());
}

#[test]
fn test_worker_impl_stop() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let mut worker = WorkerImpl::new(worker_id, config);

    // Stop before start should fail
    let result = worker.stop();
    assert!(result.is_err());

    // Start then stop should succeed
    let _ = worker.start();
    let result = worker.stop();
    assert!(result.is_ok());
}

#[test]
fn test_worker_impl_connector_names() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let worker = WorkerImpl::new(worker_id, config);

    // Initially should have no connectors
    let names = worker.connector_names();
    assert!(names.is_empty());
}

#[test]
fn test_worker_impl_task_ids() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let worker = WorkerImpl::new(worker_id, config);

    // Initially should have no tasks
    let task_ids = worker.task_ids();
    assert!(task_ids.is_empty());
}

#[test]
fn test_worker_impl_is_running() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let worker = WorkerImpl::new(worker_id, config);

    // Non-existent connector should not be running
    assert!(!worker.is_running("non-existent".to_string()));
}

#[test]
fn test() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let worker = WorkerImpl::new(worker_id, config);

    // Non-existent connector should have no version
    assert!(worker
        .connector_version("non-existent".to_string())
        .is_none());
}

#[test]
fn test_worker_impl_task_version() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let worker = WorkerImpl::new(worker_id, config);

    // Non-existent task should have no version
    let task_id = ConnectorTaskId {
        connector: "non-existent".to_string(),
        task: 0,
    };
    assert!(worker.task_version(task_id).is_none());
}

#[test]
fn test_worker_impl_is_sink_connector() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let worker = WorkerImpl::new(worker_id, config);

    // Non-existent connector should not be a sink connector
    assert!(!worker.is_sink_connector("non-existent".to_string()));
}

#[test]
fn test_worker_impl_connector_task_configs() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let worker = WorkerImpl::new(worker_id, config);

    // Non-existent connector should have no task configs
    let task_configs = worker.connector_task_configs(
        "non-existent".to_string(),
        ConnectorConfig {
            props: HashMap::new(),
        },
    );
    assert!(task_configs.is_empty());
}

#[test]
fn test_worker_impl_config() {
    let worker_id = "test-worker".to_string();
    let mut config_map = HashMap::new();
    config_map.insert("key1".to_string(), "value1".to_string());

    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: config_map.clone(),
    };

    let worker = WorkerImpl::new(worker_id, config);

    // Config should be retrievable
    let retrieved_config = worker.config();
    assert_eq!(retrieved_config.worker_id, "test-worker");
    assert_eq!(retrieved_config.config, config_map);
}

#[test]
fn test_worker_impl_metrics() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let worker = WorkerImpl::new(worker_id, config);

    // Metrics should be retrievable
    let metrics = worker.metrics();
    let snapshot = metrics.snapshot();
    assert!(snapshot.contains_key("records-produced"));
    assert!(snapshot.contains_key("records-consumed"));
    assert!(snapshot.contains_key("records-failed"));
    assert!(snapshot.contains_key("records-skipped"));
}

#[test]
fn test_worker_impl_fence_zombies() {
    let worker_id = "test-worker".to_string();
    let config = WorkerConfig {
        worker_id: worker_id.clone(),
        config: HashMap::new(),
    };

    let mut worker = WorkerImpl::new(worker_id, config);

    // fence_zombies should succeed
    let result = worker.fence_zombies("test-connector".to_string(), 1, HashMap::new());
    assert!(result.is_ok());
}

#[test]
fn test_connector_state_equality() {
    assert_eq!(ConnectorState::Running, ConnectorState::Running);
    assert_eq!(ConnectorState::Stopped, ConnectorState::Stopped);
    assert_eq!(ConnectorState::Paused, ConnectorState::Paused);
    assert_ne!(ConnectorState::Running, ConnectorState::Stopped);
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
