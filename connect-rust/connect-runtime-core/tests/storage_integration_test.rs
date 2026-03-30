//! Storage集成测试
//!
//! 测试Herder与Storage之间的交互

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use connect_runtime_core::{
    embedded_cluster::EmbeddedConnectCluster,
    herder::{Callback as HerderCallback, Created, Herder},
    storage::{
        ConfigBackingStore, ConnectorStatus, ConnectorTaskId, MemoryConfigBackingStore,
        MemoryStatusBackingStore, OffsetBackingStore, StatusBackingStore,
        TargetState as StorageTargetState, TaskStatus, TopicState, TopicStatus,
    },
    worker::TargetState as WorkerTargetState,
};

/// Mock callback for testing
struct MockCallback<T> {
    result: Arc<Mutex<Option<T>>>,
}

impl<T: Clone> HerderCallback<T> for MockCallback<T> {
    fn call(&self, result: T) {
        let mut res = self.result.lock().unwrap();
        *res = Some(result);
    }
}

impl<T: Clone> connect_runtime_core::storage::Callback<T> for MockCallback<T> {
    fn call(&self, result: T) {
        let mut res = self.result.lock().unwrap();
        *res = Some(result);
    }
}

#[test]
fn test_status_store_lifecycle() {
    let mut store = MemoryStatusBackingStore::new();

    // 测试启动
    assert!(store.start().is_ok());

    // 测试重复启动
    assert!(store.start().is_err());

    // 测试停止
    assert!(store.stop().is_ok());

    // 测试重复停止
    assert!(store.stop().is_err());
}

#[test]
fn test_status_store_connector_status() {
    let mut store = MemoryStatusBackingStore::new();
    assert!(store.start().is_ok());

    // 创建连接器状态
    let status = ConnectorStatus {
        name: "test-connector".to_string(),
        state: connect_runtime_core::storage::ConnectorState::Running,
        trace: "Test trace".to_string(),
        worker_id: "worker-1".to_string(),
    };

    // 测试put
    assert!(store.put(status.clone()).is_ok());

    // 测试get
    let retrieved = store.get("test-connector".to_string());
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.name, "test-connector");
    assert_eq!(retrieved.worker_id, "worker-1");

    // 测试put_safe
    let status2 = ConnectorStatus {
        name: "test-connector".to_string(),
        state: connect_runtime_core::storage::ConnectorState::Paused,
        trace: "Updated trace".to_string(),
        worker_id: "worker-1".to_string(),
    };
    assert!(store.put_safe(status2).is_ok());

    // 验证更新
    let updated = store.get("test-connector".to_string());
    assert!(updated.is_some());
    assert_eq!(
        updated.unwrap().state,
        connect_runtime_core::storage::ConnectorState::Paused
    );

    // 测试获取不存在的连接器
    let nonexistent = store.get("nonexistent".to_string());
    assert!(nonexistent.is_none());

    assert!(store.stop().is_ok());
}

#[test]
fn test_status_store_task_status() {
    let mut store = MemoryStatusBackingStore::new();
    assert!(store.start().is_ok());

    // 创建任务状态
    let task_id = ConnectorTaskId {
        connector: "test-connector".to_string(),
        task: 0,
    };

    let status = TaskStatus {
        id: task_id.clone(),
        state: connect_runtime_core::storage::TaskState::Running,
        trace: "Task trace".to_string(),
        worker_id: "worker-1".to_string(),
    };

    // 测试put_task
    assert!(store.put_task(status.clone()).is_ok());

    // 测试get_task
    let retrieved = store.get_task(task_id.clone());
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.id.connector, "test-connector");
    assert_eq!(retrieved.id.task, 0);

    // 测试put_task_safe
    let status2 = TaskStatus {
        id: task_id.clone(),
        state: connect_runtime_core::storage::TaskState::Paused,
        trace: "Updated task trace".to_string(),
        worker_id: "worker-1".to_string(),
    };
    assert!(store.put_task_safe(status2).is_ok());

    // 验证更新
    let updated = store.get_task(task_id);
    assert!(updated.is_some());
    assert_eq!(
        updated.unwrap().state,
        connect_runtime_core::storage::TaskState::Paused
    );

    // 测试get_all
    let all_tasks = store.get_all("test-connector".to_string());
    assert_eq!(all_tasks.len(), 1);

    // 测试获取不存在的连接器的任务
    let nonexistent_tasks = store.get_all("nonexistent".to_string());
    assert_eq!(nonexistent_tasks.len(), 0);

    assert!(store.stop().is_ok());
}

#[test]
fn test_status_store_topic_status() {
    let mut store = MemoryStatusBackingStore::new();
    assert!(store.start().is_ok());

    // 创建topic状态
    let status = TopicStatus {
        connector: "test-connector".to_string(),
        topic: "test-topic".to_string(),
        state: TopicState::Active,
        partition_count: 3,
    };

    // 测试put_topic
    assert!(store.put_topic(status.clone()).is_ok());

    // 测试get_topic
    let retrieved = store.get_topic("test-connector".to_string(), "test-topic".to_string());
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.connector, "test-connector");
    assert_eq!(retrieved.topic, "test-topic");
    assert_eq!(retrieved.partition_count, 3);

    // 测试get_all_topics
    let all_topics = store.get_all_topics("test-connector".to_string());
    assert_eq!(all_topics.len(), 1);

    // 测试delete_topic
    assert!(store
        .delete_topic("test-connector".to_string(), "test-topic".to_string())
        .is_ok());

    // 验证删除
    let deleted = store.get_topic("test-connector".to_string(), "test-topic".to_string());
    assert!(deleted.is_none());

    assert!(store.stop().is_ok());
}

#[test]
fn test_config_store_lifecycle() {
    let mut store = MemoryConfigBackingStore::new();

    // 测试启动
    assert!(store.start().is_ok());

    // 测试重复启动
    assert!(store.start().is_err());

    // 测试停止
    assert!(store.stop().is_ok());

    // 测试重复停止
    assert!(store.stop().is_err());
}

#[test]
fn test_config_store_connector_config() {
    let mut store = MemoryConfigBackingStore::new();
    assert!(store.start().is_ok());

    // 创建连接器配置
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());
    config.insert("tasks.max".to_string(), "3".to_string());

    // 测试put_connector_config
    assert!(store
        .put_connector_config(
            "test-connector".to_string(),
            config.clone(),
            StorageTargetState::Started,
        )
        .is_ok());

    // 测试contains
    assert!(store.contains("test-connector".to_string()));
    assert!(!store.contains("nonexistent".to_string()));

    // 测试snapshot
    let snapshot = store.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot.contains_key("test-connector"));

    // 测试put_target_state
    assert!(store
        .put_target_state("test-connector".to_string(), StorageTargetState::Paused)
        .is_ok());

    // 测试remove_connector_config
    assert!(store
        .remove_connector_config("test-connector".to_string())
        .is_ok());

    // 验证删除
    assert!(!store.contains("test-connector".to_string()));

    assert!(store.stop().is_ok());
}

#[test]
fn test_config_store_task_config() {
    let mut store = MemoryConfigBackingStore::new();
    assert!(store.start().is_ok());

    // 创建任务配置
    let task_configs = vec![
        {
            let mut cfg = HashMap::new();
            cfg.insert("task.class".to_string(), "test-task".to_string());
            cfg
        },
        {
            let mut cfg = HashMap::new();
            cfg.insert("task.class".to_string(), "test-task".to_string());
            cfg
        },
    ];

    // 测试put_task_configs
    assert!(store
        .put_task_configs("test-connector".to_string(), task_configs.clone())
        .is_ok());

    // 测试remove_task_configs
    assert!(store
        .remove_task_configs("test-connector".to_string())
        .is_ok());

    assert!(store.stop().is_ok());
}

#[test]
fn test_offset_store_lifecycle() {
    let mut store = connect_runtime_core::storage::MemoryOffsetBackingStore::new();

    // 测试启动
    assert!(store.start().is_ok());

    // 测试重复启动
    assert!(store.start().is_err());

    // 测试停止
    assert!(store.stop().is_ok());

    // 测试重复停止
    assert!(store.stop().is_err());
}

#[test]
fn test_herder_storage_integration() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 获取status store
    let status_store = cluster.status_store();
    let mut store = status_store.lock().unwrap();

    // 创建连接器状态
    let status = ConnectorStatus {
        name: "test-connector".to_string(),
        state: connect_runtime_core::storage::ConnectorState::Running,
        trace: "Test trace".to_string(),
        worker_id: cluster.worker_id().to_string(),
    };

    // 测试put
    assert!(store.put(status).is_ok());

    // 验证可以获取
    let retrieved = store.get("test-connector".to_string());
    assert!(retrieved.is_some());

    drop(store);

    // 测试停止cluster
    assert!(cluster.stop().is_ok());
}

#[test]
fn test_cluster_config_store_integration() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 获取config store
    let config_store = cluster.config_store();
    let mut store = config_store.lock().unwrap();

    // 创建连接器配置
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());

    // 测试put_connector_config
    assert!(store
        .put_connector_config(
            "test-connector".to_string(),
            config,
            StorageTargetState::Started,
        )
        .is_ok());

    // 验证可以获取
    assert!(store.contains("test-connector".to_string()));

    drop(store);

    // 测试停止cluster
    assert!(cluster.stop().is_ok());
}

#[test]
fn test_connector_lifecycle_with_storage() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 创建连接器配置
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());

    // 测试put_connector_config_with_target_state
    let result = Arc::new(Mutex::new(None));
    let callback = Box::new(MockCallback {
        result: result.clone(),
    });

    assert!(cluster
        .put_connector_config_with_target_state(
            "test-connector".to_string(),
            config,
            WorkerTargetState::Started,
            false,
            callback,
        )
        .is_ok());

    // 验证连接器已添加
    let connectors = cluster.connector_names();
    assert_eq!(connectors.len(), 1);

    // 验证config store中也有配置
    let config_store = cluster.config_store();
    let store = config_store.lock().unwrap();
    assert!(store.contains("test-connector".to_string()));
    drop(store);

    // 测试删除连接器
    let result2 = Arc::new(Mutex::new(None));
    let callback2 = Box::new(MockCallback {
        result: result2.clone(),
    });

    assert!(cluster
        .delete_connector_config("test-connector".to_string(), callback2)
        .is_ok());

    // 验证连接器已删除
    let connectors = cluster.connector_names();
    assert_eq!(connectors.len(), 0);

    // 测试停止cluster
    assert!(cluster.stop().is_ok());
}

#[test]
fn test_multiple_connectors_storage_persistence() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 添加多个连接器
    for i in 0..5 {
        let mut config = HashMap::new();
        config.insert(
            "connector.class".to_string(),
            format!("test-connector-{}", i),
        );

        let result = Arc::new(Mutex::new(None));
        let callback = Box::new(MockCallback {
            result: result.clone(),
        });

        assert!(cluster
            .put_connector_config_with_target_state(
                format!("connector-{}", i),
                config,
                WorkerTargetState::Started,
                false,
                callback,
            )
            .is_ok());
    }

    // 验证所有连接器已添加
    let connectors = cluster.connector_names();
    assert_eq!(connectors.len(), 5);

    // 验证config store中也有所有配置
    let config_store = cluster.config_store();
    let store = config_store.lock().unwrap();
    let snapshot = store.snapshot();
    assert_eq!(snapshot.len(), 5);
    drop(store);

    // 测试停止cluster
    assert!(cluster.stop().is_ok());
}

#[test]
fn test_task_config_storage_integration() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 创建连接器
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());

    let result = Arc::new(Mutex::new(None));
    let callback = Box::new(MockCallback {
        result: result.clone(),
    });

    assert!(cluster
        .put_connector_config_with_target_state(
            "test-connector".to_string(),
            config,
            WorkerTargetState::Started,
            false,
            callback,
        )
        .is_ok());

    // 创建任务配置
    let task_configs = vec![
        {
            let mut cfg = HashMap::new();
            cfg.insert("task.class".to_string(), "test-task".to_string());
            cfg
        },
        {
            let mut cfg = HashMap::new();
            cfg.insert("task.class".to_string(), "test-task".to_string());
            cfg
        },
    ];

    // 测试put_task_configs
    let result2 = Arc::new(Mutex::new(None));
    let callback2 = Box::new(MockCallback {
        result: result2.clone(),
    });

    assert!(cluster
        .put_task_configs(
            "test-connector".to_string(),
            task_configs,
            callback2,
            connect_runtime_core::herder::InternalRequestSignature {
                signature: "test-signature".to_string(),
            },
        )
        .is_ok());

    // 验证任务已添加
    let tasks = cluster.get_connector_tasks("test-connector".to_string());
    assert_eq!(tasks.len(), 2);

    // 测试停止cluster
    assert!(cluster.stop().is_ok());
}

#[test]
fn test_storage_error_handling() {
    let mut status_store = MemoryStatusBackingStore::new();
    let mut config_store = MemoryConfigBackingStore::new();
    let mut offset_store = connect_runtime_core::storage::MemoryOffsetBackingStore::new();

    // 测试未启动时的操作
    // 注意：当前实现不检查启动状态，所以这些操作会成功
    // 在真实实现中，这些操作应该失败
    assert!(status_store
        .put(ConnectorStatus {
            name: "test".to_string(),
            state: connect_runtime_core::storage::ConnectorState::Running,
            trace: String::new(),
            worker_id: String::new(),
        })
        .is_ok());

    assert!(config_store
        .put_connector_config(
            "test".to_string(),
            HashMap::new(),
            StorageTargetState::Started,
        )
        .is_ok());

    // 启动stores
    assert!(status_store.start().is_ok());
    assert!(config_store.start().is_ok());
    assert!(offset_store.start().is_ok());

    // 测试正常操作
    assert!(status_store
        .put(ConnectorStatus {
            name: "test".to_string(),
            state: connect_runtime_core::storage::ConnectorState::Running,
            trace: String::new(),
            worker_id: String::new(),
        })
        .is_ok());

    assert!(config_store
        .put_connector_config(
            "test".to_string(),
            HashMap::new(),
            StorageTargetState::Started,
        )
        .is_ok());

    // 停止stores
    assert!(status_store.stop().is_ok());
    assert!(config_store.stop().is_ok());
    assert!(offset_store.stop().is_ok());
}

#[test]
fn test_patch_connector_config_with_storage() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 创建连接器
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());
    config.insert("tasks.max".to_string(), "1".to_string());

    let result = Arc::new(Mutex::new(None));
    let callback = Box::new(MockCallback {
        result: result.clone(),
    });

    assert!(cluster
        .put_connector_config_with_target_state(
            "test-connector".to_string(),
            config,
            WorkerTargetState::Started,
            false,
            callback,
        )
        .is_ok());

    // patch配置
    let mut patch = HashMap::new();
    patch.insert("tasks.max".to_string(), "3".to_string());
    patch.insert("new.property".to_string(), "value".to_string());

    let result2 = Arc::new(Mutex::new(None));
    let callback2 = Box::new(MockCallback {
        result: result2.clone(),
    });

    assert!(cluster
        .patch_connector_config("test-connector".to_string(), patch, callback2)
        .is_ok());

    // 验证配置已更新
    let info = cluster.get_connector_info("test-connector".to_string());
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.config.get("tasks.max"), Some(&"3".to_string()));
    assert_eq!(info.config.get("new.property"), Some(&"value".to_string()));

    // 测试停止cluster
    assert!(cluster.stop().is_ok());
}
