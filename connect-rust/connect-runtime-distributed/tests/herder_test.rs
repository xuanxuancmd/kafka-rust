//! Tests for the herder module
//!
//! This module tests the DistributedHerder trait implementation
//! and related structures.

use connect_runtime_distributed::herder::{
    Callback, ConnectorInfo, ConnectorState, ConnectorType, DistributedHerder,
    DistributedHerderImpl, HerderConfig, HerderMetrics, TargetState, TaskInfo,
};
use std::collections::HashMap;
use std::error::Error;

/// Test callback implementation
struct TestCallback<T> {
    result: std::sync::Arc<std::sync::Mutex<Option<T>>>,
    error: std::sync::Arc<std::sync::Mutex<Option<String>>>,
}

impl<T: Clone> TestCallback<T> {
    fn new() -> Self {
        TestCallback {
            result: std::sync::Arc::new(std::sync::Mutex::new(None)),
            error: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }

    fn get_result(&self) -> Option<T> {
        let result = self.result.lock().unwrap();
        result.clone()
    }

    fn get_error(&self) -> Option<String> {
        let error = self.error.lock().unwrap();
        error.clone()
    }
}

impl<T: Clone + 'static> Callback<T> for TestCallback<T> {
    fn on_completion(&self, result: T) {
        let mut res = self.result.lock().unwrap();
        *res = Some(result);
    }

    fn on_error(&self, error: Box<dyn Error>) {
        let mut err = self.error.lock().unwrap();
        *err = Some(error.to_string());
    }
}

impl<T: Clone> Clone for TestCallback<T> {
    fn clone(&self) -> Self {
        TestCallback {
            result: std::sync::Arc::clone(&self.result),
            error: std::sync::Arc::clone(&self.error),
        }
    }
}

#[test]
fn test_herder_config_default() {
    let config = HerderConfig::default();
    assert_eq!(config.worker_group_id, "connect-cluster");
    assert_eq!(config.worker_sync_timeout_ms, 5000);
    assert_eq!(config.worker_unsync_backoff_ms, 1000);
    assert_eq!(config.key_rotation_interval_ms, 3600000);
    assert_eq!(config.request_signature_algorithm, "HmacSHA256");
    assert!(!config.topic_tracking_enabled);
}

#[test]
fn test_herder_metrics_default() {
    let metrics = HerderMetrics::default();
    assert_eq!(metrics.active_connectors, 0);
    assert_eq!(metrics.active_tasks, 0);
    assert_eq!(metrics.failed_connectors, 0);
    assert_eq!(metrics.failed_tasks, 0);
    assert_eq!(metrics.rebalance_count, 0);
    assert_eq!(metrics.time_since_last_rebalance_ms, 0);
}

#[test]
fn test_distributed_herder_creation() {
    let config = HerderConfig::default();
    let herder = DistributedHerderImpl::new(config);

    assert!(!herder.is_ready());
    assert_eq!(herder.generation(), 0);
    assert!(!herder.is_leader());
    assert!(herder.leader_url().is_none());
}

#[test]
fn test_distributed_herder_get_metrics() {
    let config = HerderConfig::default();
    let herder = DistributedHerderImpl::new(config);

    let metrics = herder.get_metrics();
    assert_eq!(metrics.active_connectors, 0);
    assert_eq!(metrics.active_tasks, 0);
}

#[test]
fn test_distributed_herder_connectors_empty() {
    let config = HerderConfig::default();
    let herder = DistributedHerderImpl::new(config);

    let callback = TestCallback::new();
    let result = herder.connectors(Box::new(callback.clone()));

    assert!(result.is_ok());
    let connectors = callback.get_result();
    assert!(connectors.is_some());
    assert!(connectors.unwrap().is_empty());
}

#[test]
fn test_distributed_herder_put_connector_config() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let mut connector_config = HashMap::new();
    connector_config.insert("name".to_string(), "test-connector".to_string());
    connector_config.insert("connector.class".to_string(), "TestConnector".to_string());

    let callback = TestCallback::new();
    let result = herder.put_connector_config(
        "test-connector",
        connector_config,
        false,
        Box::new(callback.clone()),
    );

    assert!(result.is_ok());
    let info = callback.get_result();
    assert!(info.is_some());
    let connector_info = info.unwrap();
    assert_eq!(connector_info.name, "test-connector");
}

#[test]
fn test_distributed_herder_put_connector_config_duplicate() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let mut connector_config = HashMap::new();
    connector_config.insert("name".to_string(), "test-connector".to_string());

    // First insert should succeed
    let callback1 = TestCallback::new();
    let result1 = herder.put_connector_config(
        "test-connector",
        connector_config.clone(),
        false,
        Box::new(callback1),
    );
    assert!(result1.is_ok());

    // Second insert without allow_replace should fail
    let callback2 = TestCallback::new();
    let result2 = herder.put_connector_config(
        "test-connector",
        connector_config,
        false,
        Box::new(callback2),
    );
    assert!(result2.is_err());
}

#[test]
fn test_distributed_herder_put_connector_config_with_replace() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let mut connector_config1 = HashMap::new();
    connector_config1.insert("name".to_string(), "test-connector".to_string());
    connector_config1.insert("version".to_string(), "1".to_string());

    // First insert
    let callback1 = TestCallback::new();
    let _ = herder.put_connector_config(
        "test-connector",
        connector_config1,
        false,
        Box::new(callback1),
    );

    let mut connector_config2 = HashMap::new();
    connector_config2.insert("name".to_string(), "test-connector".to_string());
    connector_config2.insert("version".to_string(), "2".to_string());

    // Second insert with allow_replace should succeed
    let callback2 = TestCallback::new();
    let result2 = herder.put_connector_config(
        "test-connector",
        connector_config2,
        true,
        Box::new(callback2),
    );
    assert!(result2.is_ok());
}

#[test]
fn test_distributed_herder_put_connector_config_with_target_state() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let connector_config = HashMap::new();

    let callback = TestCallback::new();
    let result = herder.put_connector_config_with_target_state(
        "test-connector",
        connector_config,
        TargetState::Paused,
        false,
        Box::new(callback.clone()),
    );

    assert!(result.is_ok());
    let info = callback.get_result();
    assert!(info.is_some());
    let connector_info = info.unwrap();
    assert_eq!(connector_info.state, TargetState::Paused);
}

#[test]
fn test_distributed_herder_connector_info() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let connector_config = HashMap::new();

    let callback1 = TestCallback::new();
    let _ = herder.put_connector_config(
        "test-connector",
        connector_config,
        false,
        Box::new(callback1),
    );

    let callback2 = TestCallback::new();
    let result = herder.connector_info("test-connector", Box::new(callback2.clone()));

    assert!(result.is_ok());
    let info = callback2.get_result();
    assert!(info.is_some());
    let connector_info = info.unwrap();
    assert_eq!(connector_info.name, "test-connector");
}

#[test]
fn test_distributed_herder_connector_info_not_found() {
    let config = HerderConfig::default();
    let herder = DistributedHerderImpl::new(config);

    let callback = TestCallback::new();
    let result = herder.connector_info("nonexistent", Box::new(callback.clone()));

    assert!(result.is_ok());
    let info = callback.get_result();
    assert!(info.is_some());
    let connector_info = info.unwrap();
    assert_eq!(connector_info.name, "nonexistent");
}

#[test]
fn test_distributed_herder_connector_config() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let mut connector_config = HashMap::new();
    connector_config.insert("key1".to_string(), "value1".to_string());

    let callback1 = TestCallback::new();
    let _ = herder.put_connector_config(
        "test-connector",
        connector_config.clone(),
        false,
        Box::new(callback1),
    );

    let callback2 = TestCallback::new();
    let result = herder.connector_config("test-connector", Box::new(callback2.clone()));

    assert!(result.is_ok());
    let retrieved_config = callback2.get_result();
    assert!(retrieved_config.is_some());
    let config_map = retrieved_config.unwrap();
    assert_eq!(config_map.get("key1"), Some(&"value1".to_string()));
}

#[test]
fn test_distributed_herder_patch_connector_config() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let mut connector_config = HashMap::new();
    connector_config.insert("key1".to_string(), "value1".to_string());

    let callback1 = TestCallback::new();
    let _ = herder.put_connector_config(
        "test-connector",
        connector_config,
        false,
        Box::new(callback1),
    );

    let mut patch = HashMap::new();
    patch.insert("key2".to_string(), "value2".to_string());

    let callback2 = TestCallback::new();
    let result =
        herder.patch_connector_config("test-connector", patch, Box::new(callback2.clone()));

    assert!(result.is_ok());
    let info = callback2.get_result();
    assert!(info.is_some());
    let connector_info = info.unwrap();
    assert_eq!(connector_info.config.len(), 2);
}

#[test]
fn test_distributed_herder_delete_connector_config() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let connector_config = HashMap::new();

    let callback1 = TestCallback::new();
    let _ = herder.put_connector_config(
        "test-connector",
        connector_config,
        false,
        Box::new(callback1),
    );

    let callback2 = TestCallback::new();
    let result = herder.delete_connector_config("test-connector", Box::new(callback2.clone()));

    assert!(result.is_ok());
    let info = callback2.get_result();
    assert!(info.is_some());
}

#[test]
fn test_distributed_herder_task_configs() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let connector_config = HashMap::new();

    let callback1 = TestCallback::new();
    let _ = herder.put_connector_config(
        "test-connector",
        connector_config,
        false,
        Box::new(callback1),
    );

    let callback2 = TestCallback::new();
    let result = herder.task_configs("test-connector", Box::new(callback2.clone()));

    assert!(result.is_ok());
    let tasks = callback2.get_result();
    assert!(tasks.is_some());
    assert!(tasks.unwrap().is_empty());
}

#[test]
fn test_distributed_herder_put_task_configs() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let connector_config = HashMap::new();

    let callback1 = TestCallback::new();
    let _ = herder.put_connector_config(
        "test-connector",
        connector_config,
        false,
        Box::new(callback1),
    );

    let task_configs = vec![HashMap::new(), HashMap::new()];

    let callback2 = TestCallback::new();
    let result = herder.put_task_configs("test-connector", task_configs, Box::new(callback2));

    assert!(result.is_ok());
}

#[test]
fn test_distributed_herder_connector_state() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let connector_config = HashMap::new();

    let callback1 = TestCallback::new();
    let _ = herder.put_connector_config(
        "test-connector",
        connector_config,
        false,
        Box::new(callback1),
    );

    let state = herder.connector_state("test-connector");
    assert!(state.is_some());
    let connector_state = state.unwrap();
    assert_eq!(connector_state.name, "test-connector");
}

#[test]
fn test_distributed_herder_connector_active_topics() {
    let config = HerderConfig::default();
    let herder = DistributedHerderImpl::new(config);

    let topics = herder.connector_active_topics("test-connector");
    assert!(topics.is_empty());
}

#[test]
fn test_distributed_herder_pause_resume_connector() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let connector_config = HashMap::new();

    let callback1 = TestCallback::new();
    let _ = herder.put_connector_config(
        "test-connector",
        connector_config,
        false,
        Box::new(callback1),
    );

    let pause_result = herder.pause_connector("test-connector");
    assert!(pause_result.is_ok());

    let state = herder.connector_state("test-connector");
    assert!(state.is_some());
    assert_eq!(state.unwrap().state, TargetState::Paused);

    let resume_result = herder.resume_connector("test-connector");
    assert!(resume_result.is_ok());

    let state = herder.connector_state("test-connector");
    assert!(state.is_some());
    assert_eq!(state.unwrap().state, TargetState::Running);
}

#[test]
fn test_distributed_herder_kafka_cluster_id() {
    let config = HerderConfig::default();
    let herder = DistributedHerderImpl::new(config);

    let cluster_id = herder.kafka_cluster_id();
    assert!(cluster_id.is_none());
}

#[test]
fn test_distributed_herder_logger_levels() {
    let config = HerderConfig::default();
    let herder = DistributedHerderImpl::new(config);

    let level = herder.logger_level("test.logger");
    assert!(level.is_none());

    let all_levels = herder.all_logger_levels();
    assert!(all_levels.is_empty());
}

#[test]
fn test_distributed_herder_set_logger_levels() {
    let config = HerderConfig::default();
    let mut herder = DistributedHerderImpl::new(config);

    let result = herder.set_worker_logger_level("test", "INFO");
    assert!(result.is_ok());

    let result = herder.set_cluster_logger_level("test", "INFO");
    assert!(result.is_ok());
}

#[test]
fn test_target_state_equality() {
    assert_eq!(TargetState::Running, TargetState::Running);
    assert_eq!(TargetState::Paused, TargetState::Paused);
    assert_eq!(TargetState::Stopped, TargetState::Stopped);

    assert_ne!(TargetState::Running, TargetState::Paused);
    assert_ne!(TargetState::Paused, TargetState::Stopped);
}

#[test]
fn test_connector_type_equality() {
    assert_eq!(ConnectorType::Source, ConnectorType::Source);
    assert_eq!(ConnectorType::Sink, ConnectorType::Sink);

    assert_ne!(ConnectorType::Source, ConnectorType::Sink);
}
