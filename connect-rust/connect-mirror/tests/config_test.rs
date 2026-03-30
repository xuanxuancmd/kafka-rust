//! Unit tests for configuration traits

use connect_api::ConfigDef;
use connect_mirror::config::{MirrorCheckpointConfig, MirrorHeartbeatConfig, MirrorSourceConfig};
use std::collections::HashMap;

/// Mock implementation of MirrorSourceConfig for testing
struct MockMirrorSourceConfig {
    upstream_cluster_alias: String,
    downstream_cluster_alias: String,
    topic_config: HashMap<String, String>,
    replication_policy: String,
    offset_sync_topic: String,
    heartbeat_topic: String,
    replication_enabled: bool,
}

impl MockMirrorSourceConfig {
    fn new() -> Self {
        let mut topic_config = HashMap::new();
        topic_config.insert("key1".to_string(), "value1".to_string());

        MockMirrorSourceConfig {
            upstream_cluster_alias: "source1".to_string(),
            downstream_cluster_alias: "target1".to_string(),
            topic_config,
            replication_policy: "DefaultReplicationPolicy".to_string(),
            offset_sync_topic: "mm2-offsets.source1.internal".to_string(),
            heartbeat_topic: "heartbeats".to_string(),
            replication_enabled: true,
        }
    }
}

impl MirrorSourceConfig for MockMirrorSourceConfig {
    fn upstream_cluster_alias(&self) -> String {
        self.upstream_cluster_alias.clone()
    }

    fn downstream_cluster_alias(&self) -> String {
        self.downstream_cluster_alias.clone()
    }

    fn topic_config(&self) -> HashMap<String, String> {
        self.topic_config.clone()
    }

    fn replication_policy(&self) -> String {
        self.replication_policy.clone()
    }

    fn offset_sync_topic(&self) -> String {
        self.offset_sync_topic.clone()
    }

    fn heartbeat_topic(&self) -> String {
        self.heartbeat_topic.clone()
    }

    fn replication_enabled(&self) -> bool {
        self.replication_enabled
    }

    fn config_def(&self) -> ConfigDef {
        // Return an empty ConfigDef for testing
        ConfigDef::new()
    }
}

/// Mock implementation of MirrorCheckpointConfig for testing
struct MockMirrorCheckpointConfig {
    upstream_cluster_alias: String,
    downstream_cluster_alias: String,
    checkpoint_topic: String,
    sync_group_offset_timeout_ms: i64,
    checkpoint_sync_enabled: bool,
}

impl MockMirrorCheckpointConfig {
    fn new() -> Self {
        MockMirrorCheckpointConfig {
            upstream_cluster_alias: "source1".to_string(),
            downstream_cluster_alias: "target1".to_string(),
            checkpoint_topic: "mm2-checkpoints.internal".to_string(),
            sync_group_offset_timeout_ms: 60000,
            checkpoint_sync_enabled: true,
        }
    }
}

impl MirrorCheckpointConfig for MockMirrorCheckpointConfig {
    fn upstream_cluster_alias(&self) -> String {
        self.upstream_cluster_alias.clone()
    }

    fn downstream_cluster_alias(&self) -> String {
        self.downstream_cluster_alias.clone()
    }

    fn checkpoint_topic(&self) -> String {
        self.checkpoint_topic.clone()
    }

    fn sync_group_offset_timeout_ms(&self) -> i64 {
        self.sync_group_offset_timeout_ms
    }

    fn checkpoint_sync_enabled(&self) -> bool {
        self.checkpoint_sync_enabled
    }

    fn config_def(&self) -> ConfigDef {
        // Return an empty ConfigDef for testing
        ConfigDef::new()
    }
}

/// Mock implementation of MirrorHeartbeatConfig for testing
struct MockMirrorHeartbeatConfig {
    upstream_cluster_alias: String,
    downstream_cluster_alias: String,
    heartbeat_topic: String,
    heartbeat_interval_ms: i64,
    heartbeat_enabled: bool,
}

impl MockMirrorHeartbeatConfig {
    fn new() -> Self {
        MockMirrorHeartbeatConfig {
            upstream_cluster_alias: "source1".to_string(),
            downstream_cluster_alias: "target1".to_string(),
            heartbeat_topic: "heartbeats".to_string(),
            heartbeat_interval_ms: 5000,
            heartbeat_enabled: true,
        }
    }
}

impl MirrorHeartbeatConfig for MockMirrorHeartbeatConfig {
    fn upstream_cluster_alias(&self) -> String {
        self.upstream_cluster_alias.clone()
    }

    fn downstream_cluster_alias(&self) -> String {
        self.downstream_cluster_alias.clone()
    }

    fn heartbeat_topic(&self) -> String {
        self.heartbeat_topic.clone()
    }

    fn heartbeat_interval_ms(&self) -> i64 {
        self.heartbeat_interval_ms
    }

    fn heartbeat_enabled(&self) -> bool {
        self.heartbeat_enabled
    }

    fn config_def(&self) -> ConfigDef {
        // Return an empty ConfigDef for testing
        ConfigDef::new()
    }
}

#[test]
fn test_mirror_source_config_upstream_cluster_alias() {
    let config = MockMirrorSourceConfig::new();
    assert_eq!(config.upstream_cluster_alias(), "source1");
}

#[test]
fn test_mirror_source_config_downstream_cluster_alias() {
    let config = MockMirrorSourceConfig::new();
    assert_eq!(config.downstream_cluster_alias(), "target1");
}

#[test]
fn test_mirror_source_config_topic_config() {
    let config = MockMirrorSourceConfig::new();
    let topic_config = config.topic_config();
    assert_eq!(topic_config.len(), 1);
    assert_eq!(topic_config.get("key1"), Some(&"value1".to_string()));
}

#[test]
fn test_mirror_source_config_replication_policy() {
    let config = MockMirrorSourceConfig::new();
    assert_eq!(config.replication_policy(), "DefaultReplicationPolicy");
}

#[test]
fn test_mirror_source_config_offset_sync_topic() {
    let config = MockMirrorSourceConfig::new();
    assert_eq!(config.offset_sync_topic(), "mm2-offsets.source1.internal");
}

#[test]
fn test_mirror_source_config_heartbeat_topic() {
    let config = MockMirrorSourceConfig::new();
    assert_eq!(config.heartbeat_topic(), "heartbeats");
}

#[test]
fn test_mirror_source_config_replication_enabled() {
    let config = MockMirrorSourceConfig::new();
    assert!(config.replication_enabled());
}

#[test]
fn test_mirror_source_config_config_def() {
    let config = MockMirrorSourceConfig::new();
    let config_def = config.config_def();
    // Just verify it returns a ConfigDef
    let _ = config_def;
}

#[test]
fn test_mirror_checkpoint_config_upstream_cluster_alias() {
    let config = MockMirrorCheckpointConfig::new();
    assert_eq!(config.upstream_cluster_alias(), "source1");
}

#[test]
fn test_mirror_checkpoint_config_downstream_cluster_alias() {
    let config = MockMirrorCheckpointConfig::new();
    assert_eq!(config.downstream_cluster_alias(), "target1");
}

#[test]
fn test_mirror_checkpoint_config_checkpoint_topic() {
    let config = MockMirrorCheckpointConfig::new();
    assert_eq!(config.checkpoint_topic(), "mm2-checkpoints.internal");
}

#[test]
fn test_mirror_checkpoint_config_sync_group_offset_timeout_ms() {
    let config = MockMirrorCheckpointConfig::new();
    assert_eq!(config.sync_group_offset_timeout_ms(), 60000);
}

#[test]
fn test_mirror_checkpoint_config_checkpoint_sync_enabled() {
    let config = MockMirrorCheckpointConfig::new();
    assert!(config.checkpoint_sync_enabled());
}

#[test]
fn test_mirror_checkpoint_config_config_def() {
    let config = MockMirrorCheckpointConfig::new();
    let config_def = config.config_def();
    // Just verify it returns a ConfigDef
    let _ = config_def;
}

#[test]
fn test_mirror_heartbeat_config_upstream_cluster_alias() {
    let config = MockMirrorHeartbeatConfig::new();
    assert_eq!(config.upstream_cluster_alias(), "source1");
}

#[test]
fn test_mirror_heartbeat_config_downstream_cluster_alias() {
    let config = MockMirrorHeartbeatConfig::new();
    assert_eq!(config.downstream_cluster_alias(), "target1");
}

#[test]
fn test_mirror_heartbeat_config_heartbeat_topic() {
    let config = MockMirrorHeartbeatConfig::new();
    assert_eq!(config.heartbeat_topic(), "heartbeats");
}

#[test]
fn test_mirror_heartbeat_config_heartbeat_interval_ms() {
    let config = MockMirrorHeartbeatConfig::new();
    assert_eq!(config.heartbeat_interval_ms(), 5000);
}

#[test]
fn test_mirror_heartbeat_config_heartbeat_enabled() {
    let config = MockMirrorHeartbeatConfig::new();
    assert!(config.heartbeat_enabled());
}

#[test]
fn test_mirror_heartbeat_config_config_def() {
    let config = MockMirrorHeartbeatConfig::new();
    let config_def = config.config_def();
    // Just verify it returns a ConfigDef
    let _ = config_def;
}

#[test]
fn test_mock_mirror_source_config_custom_values() {
    let mut topic_config = HashMap::new();
    topic_config.insert("custom_key".to_string(), "custom_value".to_string());

    let config = MockMirrorSourceConfig {
        upstream_cluster_alias: "custom_source".to_string(),
        downstream_cluster_alias: "custom_target".to_string(),
        topic_config,
        replication_policy: "CustomReplicationPolicy".to_string(),
        offset_sync_topic: "custom-offsets".to_string(),
        heartbeat_topic: "custom-heartbeats".to_string(),
        replication_enabled: false,
    };

    assert_eq!(config.upstream_cluster_alias(), "custom_source");
    assert_eq!(config.downstream_cluster_alias(), "custom_target");
    assert_eq!(config.replication_policy(), "CustomReplicationPolicy");
    assert_eq!(config.offset_sync_topic(), "custom-offsets");
    assert_eq!(config.heartbeat_topic(), "custom-heartbeats");
    assert!(!config.replication_enabled());

    let topic_config = config.topic_config();
    assert_eq!(topic_config.len(), 1);
    assert_eq!(
        topic_config.get("custom_key"),
        Some(&"custom_value".to_string())
    );
}

#[test]
fn test_mock_mirror_checkpoint_config_custom_values() {
    let config = MockMirrorCheckpointConfig {
        upstream_cluster_alias: "custom_source".to_string(),
        downstream_cluster_alias: "custom_target".to_string(),
        checkpoint_topic: "custom-checkpoints".to_string(),
        sync_group_offset_timeout_ms: 120000,
        checkpoint_sync_enabled: false,
    };

    assert_eq!(config.upstream_cluster_alias(), "custom_source");
    assert_eq!(config.downstream_cluster_alias(), "custom_target");
    assert_eq!(config.checkpoint_topic(), "custom-checkpoints");
    assert_eq!(config.sync_group_offset_timeout_ms(), 120000);
    assert!(!config.checkpoint_sync_enabled());
}

#[test]
fn test_mock_mirror_heartbeat_config_custom_values() {
    let config = MockMirrorHeartbeatConfig {
        upstream_cluster_alias: "custom_source".to_string(),
        downstream_cluster_alias: "custom_target".to_string(),
        heartbeat_topic: "custom-heartbeats".to_string(),
        heartbeat_interval_ms: 10000,
        heartbeat_enabled: false,
    };

    assert_eq!(config.upstream_cluster_alias(), "custom_source");
    assert_eq!(config.downstream_cluster_alias(), "custom_target");
    assert_eq!(config.heartbeat_topic(), "custom-heartbeats");
    assert_eq!(config.heartbeat_interval_ms(), 10000);
    assert!(!config.heartbeat_enabled());
}
