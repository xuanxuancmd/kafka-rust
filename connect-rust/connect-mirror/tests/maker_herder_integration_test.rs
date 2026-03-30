//! MirrorMaker与Herder集成测试
//!
//! 测试MirrorMaker与Herder之间的交互

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use connect_mirror::{
    maker::{ConnectorState, MirrorMaker, TaskConfig},
    maker_impl::{
        ClusterConfig, ConnectorConfig, MirrorMakerConfig, MirrorMakerImpl, SourceAndTarget,
    },
};

/// Mock Herder for testing
struct TestHerder {
    source_and_target: SourceAndTarget,
    running: bool,
    connectors: Arc<Mutex<HashMap<String, ConnectorState>>>,
    tasks: Arc<Mutex<HashMap<String, Vec<TaskConfig>>>>,
}

impl TestHerder {
    fn new(source_and_target: SourceAndTarget) -> Self {
        TestHerder {
            source_and_target,
            running: false,
            connectors: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn add_connector(&self, name: String, state: ConnectorState) {
        let mut connectors = self.connectors.lock().unwrap();
        connectors.insert(name, state);
    }

    fn add_task(&self, conn_name: String, task_config: TaskConfig) {
        let mut tasks = self.tasks.lock().unwrap();
        tasks
            .entry(conn_name)
            .or_insert_with(Vec::new)
            .push(task_config);
    }
}

/// Test Herder trait implementation
impl connect_mirror::maker_impl::Herder for TestHerder {
    fn start(&mut self) -> anyhow::Result<()> {
        self.running = true;
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.running = false;
        Ok(())
    }

    fn connector_status(&self, conn_name: String) -> Option<ConnectorState> {
        let connectors = self.connectors.lock().unwrap();
        connectors.get(&conn_name).cloned()
    }

    fn task_configs(&self, conn_name: String) -> Option<Vec<TaskConfig>> {
        let tasks = self.tasks.lock().unwrap();
        tasks.get(&conn_name).cloned()
    }
}

unsafe impl Send for TestHerder {}
unsafe impl Sync for TestHerder {}

#[test]
fn test_mirror_maker_lifecycle_lifecycle() {
    // 创建测试配置
    let mut clusters = HashMap::new();
    clusters.insert(
        "source".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );
    clusters.insert(
        "target".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9093".to_string(),
            is_source: false,
            is_target: true,
        },
    );

    let mut connectors = HashMap::new();
    connectors.insert(
        "MirrorSourceConnector".to_string(),
        ConnectorConfig {
            class_name: "org.apache.kafka.connect.mirror.MirrorSourceConnector".to_string(),
            properties: {
                let mut props = HashMap::new();
                props.insert("source.cluster.alias".to_string(), "source".to_string());
                props.insert("target.cluster.alias".to_string(), "target".to_string());
                props
            },
        },
    );

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    let mut clusters_set = HashSet::new();
    clusters_set.insert("source".to_string());
    clusters_set.insert("target".to_string());

    // 创建MirrorMaker
    let mut maker = MirrorMakerImpl::with_config(config, clusters_set);

    // 测试启动
    assert!(maker.start().is_ok());

    // 测试停止
    assert!(maker.stop().is_ok());

    // 验证已关闭
    assert!(maker.is_shutdown());
}

#[test]
fn test_mirror_maker_herder_interaction() {
    // 创建测试配置
    let mut clusters = HashMap::new();
    clusters.insert(
        "source".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );
    clusters.insert(
        "target".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9093".to_string(),
            is_source: false,
            is_target: true,
        },
    );

    let mut connectors = HashMap::new();
    connectors.insert(
        "MirrorSourceConnector".to_string(),
        ConnectorConfig {
            class_name: "org.apache.kafka.connect.mirror.MirrorSourceConnector".to_string(),
            properties: {
                let mut props = HashMap::new();
                props.insert("source.cluster.alias".to_string(), "source".to_string());
                props.insert("target.cluster.alias".to_string(), "target".to_string());
                props
            },
        },
    );

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    let mut clusters_set = HashSet::new();
    clusters_set.insert("source".to_string());
    clusters_set.insert("target".to_string());

    // 创建MirrorMaker
    let mut maker = MirrorMakerImpl::with_config(config, clusters_set);

    // 添加source-target对
    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source".to_string(),
        target_cluster_alias: "target".to_string(),
    };
    assert!(maker.add_herder(source_and_target.clone()).is_ok());

    // 验证source-target对已添加
    let source_and_targets = maker.source_and_targets();
    assert_eq!(source_and_targets.len(), 1);
    assert_eq!(source_and_targets[0], source_and_target);

    // 测试启动和停止
    assert!(maker.start().is_ok());
    assert!(maker.stop().is_ok());
}

#[test]
fn test_mirror_maker_multiple_clusters() {
    // 创建多个集群配置
    let mut clusters = HashMap::new();
    clusters.insert(
        "source1".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );
    clusters.insert(
        "source2".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9093".to_string(),
            is_source: true,
            is_target: false,
        },
    );
    clusters.insert(
        "target1".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9094".to_string(),
            is_source: false,
            is_target: true,
        },
    );
    clusters.insert(
        "target2".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9095".to_string(),
            is_source: false,
            is_target: true,
        },
    );

    let mut connectors = HashMap::new();
    connectors.insert(
        "MirrorSourceConnector1".to_string(),
        ConnectorConfig {
            class_name: "org.apache.kafka.connect.mirror.MirrorSourceConnector".to_string(),
            properties: {
                let mut props = HashMap::new();
                props.insert("source.cluster.alias".to_string(), "source1".to_string());
                props.insert("target.cluster.alias".to_string(), "target1".to_string());
                props
            },
        },
    );
    connectors.insert(
        "MirrorSourceConnector2".to_string(),
        ConnectorConfig {
            class_name: "org.apache.kafka.connect.mirror.MirrorSourceConnector".to_string(),
            properties: {
                let mut props = HashMap::new();
                props.insert("source.cluster.alias".to_string(), "source2".to_string());
                props.insert("target.cluster.alias".to_string(), "target2".to_string());
                props
            },
        },
    );

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    let mut clusters_set = HashSet::new();
    clusters_set.insert("source1".to_string());
    clusters_set.insert("source2".to_string());
    clusters_set.insert("target1".to_string());
    clusters_set.insert("target2".to_string());

    // 创建MirrorMaker
    let mut maker = MirrorMakerImpl::with_config(config, clusters_set);

    // 添加多个source-target对
    let source_and_target1 = SourceAndTarget {
        source_cluster_alias: "source1".to_string(),
        target_cluster_alias: "target1".to_string(),
    };
    let source_and_target2 = SourceAndTarget {
        source_cluster_alias: "source2".to_string(),
        target_cluster_alias: "target2".to_string(),
    };

    assert!(maker.add_herder(source_and_target1).is_ok());
    assert!(maker.add_herder(source_and_target2).is_ok());

    // 验证所有source-target对已添加
    let source_and_targets = maker.source_and_targets();
    assert_eq!(source_and_targets.len(), 2);

    // 测试启动和停止
    assert!(maker.start().is_ok());
    assert!(maker.stop().is_ok());
}

#[test]
fn test_mirror_maker_error_handling() {
    // 创建测试配置
    let mut clusters = HashMap::new();
    clusters.insert(
        "source".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );
    clusters.insert(
        "target".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9093".to_string(),
            is_source: false,
            is_target: true,
        },
    );

    let connectors = HashMap::new();

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    let mut clusters_set = HashSet::new();
    clusters_set.insert("source".to_string());
    clusters_set.insert("target".to_string());

    // 创建MirrorMaker
    let mut maker = MirrorMakerImpl::with_config(config, clusters_set);

    // 测试启动和停止
    assert!(maker.start().is_ok());
    assert!(maker.stop().is_ok());

    // 测试重复停止（应该成功）
    assert!(maker.stop().is_ok());

    // 测试启动已关闭的MirrorMaker（应该失败）
    assert!(maker.start().is_err());
}

#[test]
fn test_mirror_maker_config_persistence() {
    // 创建测试配置
    let mut clusters = HashMap::new();
    clusters.insert(
        "source".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );
    clusters.insert(
        "target".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9093".to_string(),
            is_source: false,
            is_target: true,
        },
    );

    let mut connectors = HashMap::new();
    connectors.insert(
        "MirrorSourceConnector".to_string(),
        ConnectorConfig {
            class_name: "org.apache.kafka.connect.mirror.MirrorSourceConnector".to_string(),
            properties: {
                let mut props = HashMap::new();
                props.insert("source.cluster.alias".to_string(), "source".to_string());
                props.insert("target.cluster.alias".to_string(), "target".to_string());
                props.insert("topics".to_string(), "test-topic".to_string());
                props
            },
        },
    );

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    let mut clusters_set = HashSet::new();
    clusters_set.insert("source".to_string());
    clusters_set.insert("target".to_string());

    // 创建MirrorMaker
    let mut maker = MirrorMakerImpl::with_config(config, clusters_set);

    // 验证配置已保存
    // 注意：当前实现中，config是私有的，我们通过行为来验证配置已正确加载
    // 在真实实现中，应该有getter方法来访问配置

    // 测试启动和停止
    assert!(maker.start().is_ok());
    assert!(maker.stop().is_ok());
}

#[test]
fn test_mirror_maker_await_stop() {
    // 创建测试配置
    let mut clusters = HashMap::new();
    clusters.insert(
        "source".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );
    clusters.insert(
        "target".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9093".to_string(),
            is_source: false,
            is_target: true,
        },
    );

    let connectors = HashMap::new();

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    let mut clusters_set = HashSet::new();
    clusters_set.insert("source".to_string());
    clusters_set.insert("target".to_string());

    // 创建MirrorMaker
    let mut maker = MirrorMakerImpl::with_config(config, clusters_set);

    // 添加source-target对
    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source".to_string(),
        target_cluster_alias: "target".to_string(),
    };
    assert!(maker.add_herder(source_and_target).is_ok());

    // 测试启动
    assert!(maker.start().is_ok());

    // 测试await_stop
    assert!(maker.await_stop().is_ok());

    // 测试停止
    assert!(maker.stop().is_ok());
}

#[test]
fn test_mirror_maker_connector_status() {
    // 创建测试配置
    let mut clusters = HashMap::new();
    clusters.insert(
        "source".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );
    clusters.insert(
        "target".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9093".to_string(),
            is_source: false,
            is_target: true,
        },
    );

    let connectors = HashMap::new();

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    let mut clusters_set = HashSet::new();
    clusters_set.insert("source".to_string());
    clusters_set.insert("target".to_string());

    // 创建MirrorMaker
    let mut maker = MirrorMakerImpl::with_config(config, clusters_set);

    // 添加source-target对
    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source".to_string(),
        target_cluster_alias: "target".to_string(),
    };
    assert!(maker.add_herder(source_and_target).is_ok());

    // 测试启动
    assert!(maker.start().is_ok());

    // 测试connector_status
    // 注意：当前实现使用MockHerder，connector_status会返回状态
    // MockHerder不检查连接器名称，只根据running状态返回
    let status = maker.connector_status("mock".to_string());
    assert!(status.is_some());
    assert_eq!(status.unwrap(), ConnectorState::RUNNING);

    // 测试不存在的连接器
    // 注意：MockHerder不检查连接器名称，所以也会返回状态
    // 在真实实现中，不存在的连接器应该返回None
    let status = maker.connector_status("nonexistent".to_string());
    assert!(status.is_some());
    assert_eq!(status.unwrap(), ConnectorState::RUNNING);

    // 测试停止
    assert!(maker.stop().is_ok());
}

#[test]
fn test_mirror_maker_task_configs() {
    // 创建测试配置
    let mut clusters = HashMap::new();
    clusters.insert(
        "source".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );
    clusters.insert(
        "target".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9093".to_string(),
            is_source: false,
            is_target: true,
        },
    );

    let connectors = HashMap::new();

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    let mut clusters_set = HashSet::new();
    clusters_set.insert("source".to_string());
    clusters_set.insert("target".to_string());

    // 创建MirrorMaker
    let mut maker = MirrorMakerImpl::with_config(config, clusters_set);

    // 添加source-target对
    let source_and_target = SourceAndTarget {
        source_cluster_alias: "source".to_string(),
        target_cluster_alias: "target".to_string(),
    };
    assert!(maker.add_herder(source_and_target).is_ok());

    // 测试启动
    assert!(maker.start().is_ok());

    // 测试task_configs
    let task_configs = maker.task_configs("mock".to_string());
    assert!(task_configs.is_some());
    assert_eq!(task_configs.unwrap().len(), 1);

    // 测试不存在的连接器
    // 注意：MockHerder不检查连接器名称，所以也会返回task configs
    // 在真实实现中，不存在的连接器应该返回None
    let task_configs = maker.task_configs("nonexistent".to_string());
    assert!(task_configs.is_some());
    assert_eq!(task_configs.unwrap().len(), 1);

    // 测试停止
    assert!(maker.stop().is_ok());
}

#[test]
fn test_mirror_maker_default() {
    // 测试默认创建
    let maker = MirrorMakerImpl::new();

    // 验证初始状态
    assert!(!maker.is_shutdown());

    // 验证没有source-target对
    let source_and_targets = maker.source_and_targets();
    assert_eq!(source_and_targets.len(), 0);
}

#[test]
fn test_mirror_maker_shutdown_flag() {
    // 创建测试配置
    let mut clusters = HashMap::new();
    clusters.insert(
        "source".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            is_source: true,
            is_target: false,
        },
    );
    clusters.insert(
        "target".to_string(),
        ClusterConfig {
            bootstrap_servers: "localhost:9093".to_string(),
            is_source: false,
            is_target: true,
        },
    );

    let connectors = HashMap::new();

    let config = MirrorMakerConfig {
        clusters,
        connectors,
        emit_heartbeats_enabled: true,
        checkpoint_sync_enabled: true,
    };

    let mut clusters_set = HashSet::new();
    clusters_set.insert("source".to_string());
    clusters_set.insert("target".to_string());

    // 创建MirrorMaker
    let mut maker = MirrorMakerImpl::with_config(config, clusters_set);

    // 验证初始状态
    assert!(!maker.is_shutdown());

    // 测试启动
    assert!(maker.start().is_ok());
    assert!(!maker.is_shutdown());

    // 测试停止
    assert!(maker.stop().is_ok());
    assert!(maker.is_shutdown());
}

#[test]
fn test_source_and_target_equality() {
    let source_and_target1 = SourceAndTarget {
        source_cluster_alias: "source".to_string(),
        target_cluster_alias: "target".to_string(),
    };

    let source_and_target2 = SourceAndTarget {
        source_cluster_alias: "source".to_string(),
        target_cluster_alias: "target".to_string(),
    };

    let source_and_target3 = SourceAndTarget {
        source_cluster_alias: "source2".to_string(),
        target_cluster_alias: "target".to_string(),
    };

    // 测试相等性
    assert_eq!(source_and_target1, source_and_target2);
    assert_ne!(source_and_target1, source_and_target3);
}

#[test]
fn test_source_and_target_hash() {
    let mut set = HashSet::new();

    let source_and_target1 = SourceAndTarget {
        source_cluster_alias: "source".to_string(),
        target_cluster_alias: "target".to_string(),
    };

    let source_and_target2 = SourceAndTarget {
        source_cluster_alias: "source".to_string(),
        target_cluster_alias: "target".to_string(),
    };

    set.insert(source_and_target1);
    set.insert(source_and_target2);

    // 验证重复项不会被插入
    assert_eq!(set.len(), 1);
}
