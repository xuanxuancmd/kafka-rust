//! Unit tests for MirrorMaker implementation

use connect_mirror::maker::{ConnectorState, MirrorMaker, TaskConfig};
use connect_mirror::maker_impl::{
    ClusterConfig, ConnectorConfig, MirrorMakerConfig, MirrorMakerImpl, SourceAndTarget,
};
use std::collections::{HashMap, HashSet};

#[test]
fn test_source_and_target_creation() {
    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    assert_eq!(source_and_target.source_cluster_alias, "source1");
    assert_eq!(source_and_target.target_cluster_alias, "target1");
}

#[test]
fn test_source_and_target_equality() {
    let sat1 = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    let sat2 = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    let sat3 = SourceAndTarget {
        source_cluster_alias: "source2".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    assert_eq!(sat1, sat2);
    assert_ne!(sat1, sat3);
}

#[test]
fn test_source_and_target_clone() {
    let sat1 = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    let sat2 = sat1.clone();

    assert_eq!(sat1, sat2);
}

#[test]
fn test_mirrormaker_impl_new() {
    let maker = MirrorMakerImpl::new();

    // Verify initial state
    assert!(!maker.is_shutdown());
    assert_eq!(maker.source_and_targets().len(), 0);
}

#[test]
fn test_mirrormaker_impl_default() {
    let maker = MirrorMakerImpl::default();

    // Verify default implementation works
    assert!(!maker.is_shutdown());
    assert_eq!(maker.source_and_targets().len(), 0);
}

#[test]
fn test_mirrormaker_impl_with_config() {
    let mut clusters = HashMap::new();
    clusters.insert(
        "cluster1".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );

    let mut connectors = HashMap::new();
    connectors.insert(
        "connector1".to_string(),
        ConnectorConfig {
            class_name: "MirrorSourceConnector".to_string(),
            properties: HashMap::new(),
        },
    );

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    let mut cluster_set = HashSet::new();
    cluster_set.insert("cluster1".to_string());

    let maker = MirrorMakerImpl::with_config(config, cluster_set);

    // Verify configuration is set
    assert!(!maker.is_shutdown());
}

#[test]
fn test_mirrormaker_impl_add_herder() {
    let mut maker = MirrorMakerImpl::new();

    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    let result = maker.add_herder(source_and_target.clone());
    assert!(result.is_ok());

    // Verify herder was added
    let source_and_targets = maker.source_and_targets();
    assert_eq!(source_and_targets.len(), 1);
    assert_eq!(source_and_targets[0], source_and_target);
}

#[test]
fn test_mirrormaker_impl_add_multiple_herders() {
    let mut maker = MirrorMakerImpl::new();

    let sat1 = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    let sat2 = SourceAndTarget {
        source_cluster_alias: "source2".to_string(),
        target_cluster_alias: "target2".to_string(),
    };

    assert!(maker.add_herder(sat1.clone()).is_ok());
    assert!(maker.add_herder(sat2.clone()).is_ok());

    let source_and_targets = maker.source_and_targets();
    assert_eq!(source_and_targets.len(), 2);
}

#[test]
fn test_mirrormaker_impl_start() {
    let mut maker = MirrorMakerImpl::new();

    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    assert!(maker.add_herder(source_and_target).is_ok());

    // Start should succeed
    let result = maker.start();
    assert!(result.is_ok());
}

#[test]
fn test_mirrormaker_impl_start_shutdown() {
    let mut maker = MirrorMakerImpl::new();

    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    assert!(maker.add_herder(source_and_target).is_ok());

    // Start and stop
    assert!(maker.start().is_ok());
    assert!(maker.stop().is_ok());

    // Verify shutdown flag is set
    assert!(maker.is_shutdown());
}

#[test]
fn test_mirrormaker_impl_start_already_shutdown() {
    let mut maker = MirrorMakerImpl::new();

    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    assert!(maker.add_herder(source_and_target).is_ok());
    assert!(maker.start().is_ok());
    assert!(maker.stop().is_ok());

    // Try to start again - should fail
    let result = maker.start();
    assert!(result.is_err());
}

#[test]
fn test_mirrormaker_impl_await_stop() {
    let mut maker = MirrorMakerImpl::new();

    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    assert!(maker.add_herder(source_and_target).is_ok());
    assert!(maker.start().is_ok());

    // await_stop should succeed
    let result = maker.await_stop();
    assert!(result.is_ok());
}

#[test]
fn test_mirrormaker_impl_connector_status() {
    let mut maker = MirrorMakerImpl::new();

    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    assert!(maker.add_herder(source_and_target).is_ok());
    assert!(maker.start().is_ok());

    // Get connector status
    let status = maker.connector_status("mock".to_string());
    assert!(status.is_some());
    assert_eq!(status.unwrap(), ConnectorState::RUNNING);
}

#[test]
fn test_mirrormaker_impl_connector_status_stopped() {
    let mut maker = MirrorMakerImpl::new();

    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    assert!(maker.add_herder(source_and_target).is_ok());
    // Don't start - should be stopped

    let status = maker.connector_status("mock".to_string());
    assert!(status.is_some());
    assert_eq!(status.unwrap(), ConnectorState::STOPPED);
}

#[test]
fn test_mirrormaker_impl_task_configs() {
    let mut maker = MirrorMakerImpl::new();

    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    assert!(maker.add_herder(source_and_target).is_ok());
    assert!(maker.start().is_ok());

    // Get task configs
    let configs = maker.task_configs("mock".to_string());
    assert!(configs.is_some());

    let configs = configs.unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].task_id, 0);
}

#[test]
fn test_mirrormaker_impl_task_configs_stopped() {
    let mut maker = MirrorMakerImpl::new();

    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };

    assert!(maker.add_herder(source_and_target).is_ok());
    // Don't start - should return None

    let configs = maker.task_configs("mock".to_string());
    assert!(configs.is_none());
}

#[test]
fn test_mirrormaker_impl_main() {
    let maker = MirrorMakerImpl::new();

    // main should succeed (it's a skeleton implementation)
    let result = maker.main();
    assert!(result.is_ok());
}

#[test]
fn test_cluster_config_creation() {
    let config = ClusterConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        is_source: true,
        is_target: false,
    };

    assert_eq!(config.bootstrap_servers, "localhost:9092");
    assert!(config.is_source);
    assert!(!config.is_target);
}

#[test]
fn test_connector_config_creation() {
    let mut properties = HashMap::new();
    properties.insert("key1".to_string(), "value1".to_string());

    let config = ConnectorConfig {
        class_name: "MirrorSourceConnector".to_string(),
        properties,
    };

    assert_eq!(config.class_name, "MirrorSourceConnector");
    assert_eq!(config.properties.len(), 1);
    assert_eq!(config.properties.get("key1"), Some(&"value1".to_string()));
}

#[test]
fn test_mirrormaker_config_creation() {
    let mut clusters = HashMap::new();
    clusters.insert(
        "cluster1".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );

    let mut connectors = HashMap::new();
    connectors.insert(
        "connector1".to_string(),
        ConnectorConfig {
            class_name: "MirrorSourceConnector".to_string(),
            properties: HashMap::new(),
        },
    );

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    assert_eq!(config.clusters.len(), 1);
    assert_eq!(config.connectors.len(), 1);
    assert!(config.emit_heartbeats_enabled);
    assert!(config.checkpoint_sync_enabled);
}

#[test]
fn test_connector_state_variants() {
    // Test all ConnectorState variants can be created and compared
    let created = ConnectorState::CREATED;
    let running = ConnectorState::RUNNING;
    let paused = ConnectorState::PAUSED;
    let stopped = ConnectorState::STOPPED;
    let failed = ConnectorState::FAILED;
    let destroyed = ConnectorState::DESTROYED;

    assert_eq!(created, ConnectorState::CREATED);
    assert_eq!(running, ConnectorState::RUNNING);
    assert_eq!(paused, ConnectorState::PAUSED);
    assert_eq!(stopped, ConnectorState::STOPPED);
    assert_eq!(failed, ConnectorState::FAILED);
    assert_eq!(destroyed, ConnectorState::DESTROYED);

    assert_ne!(running, stopped);
}

#[test]
fn test_task_config_creation() {
    let mut config = HashMap::new();
    config.insert("key1".to_string(), "value1".to_string());

    let task_config = TaskConfig {
        task_id: 42,
        config,
    };

    assert_eq!(task_config.task_id, 42);
    assert_eq!(task_config.config.len(), 1);
}

#[test]
fn test_task_config_clone() {
    let mut config = HashMap::new();
    config.insert("key1".to_string(), "value1".to_string());

    let task_config1 = TaskConfig {
        task_id: 42,
        config,
    };

    let task_config2 = task_config1.clone();

    assert_eq!(task_config1.task_id, task_config2.task_id);
    assert_eq!(task_config1.config.len(), task_config2.config.len());
}
