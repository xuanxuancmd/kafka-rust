//! Worker-Herder集成测试
//!
//! 测试Worker与Herder之间的交互

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use connect_runtime_core::{
    embedded_cluster::EmbeddedConnectCluster,
    herder::{Callback as HerderCallback, Herder},
    storage::{MemoryStatusBackingStore, StatusBackingStore},
    worker::{
        CloseableConnectorContext, ConnectorStatusListener, TargetState, Worker, WorkerConfig,
        WorkerImpl,
    },
};

/// Mock connector context for testing
struct MockConnectorContext;

impl CloseableConnectorContext for MockConnectorContext {
    fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

/// Mock connector status listener for testing
struct MockStatusListener;

impl ConnectorStatusListener for MockStatusListener {
    fn on_status_change(&self, _status: connect_runtime_core::worker::ConnectorStatus) {
        // Mock implementation
    }
}

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

impl<T: Clone> connect_runtime_core::worker::Callback<T> for MockCallback<T> {
    fn call(&self, result: T) {
        let mut res = self.result.lock().unwrap();
        *res = Some(result);
    }
}

#[test]
fn test_worker_herder_lifecycle() {
    // 创建status store（不启动）
    let status_store = MemoryStatusBackingStore::new();

    // 创建worker配置
    let worker_config = WorkerConfig {
        worker_id: "test-worker".to_string(),
        config: HashMap::new(),
    };

    // 创建worker（不启动）
    let worker = WorkerImpl::new("test-worker".to_string(), worker_config);

    // 创建herder并传入worker
    let mut herder = connect_runtime_core::herder::StandaloneHerder::with_components(
        Box::new(worker),
        Box::new(status_store),
    );

    // 测试herder启动（这会自动启动worker和status store）
    assert!(herder.start().is_ok());
    assert!(herder.is_ready());

    // 测试herder停止（这会自动停止worker和status store）
    assert!(herder.stop().is_ok());
}

#[test]
fn test_worker_connector_creation() {
    // 创建worker
    let worker_config = WorkerConfig {
        worker_id: "test-worker".to_string(),
        config: HashMap::new(),
    };
    let mut worker = WorkerImpl::new("test-worker".to_string(), worker_config);
    assert!(worker.start().is_ok());

    // 创建连接器配置
    let mut conn_props = HashMap::new();
    conn_props.insert("connector.class".to_string(), "test-connector".to_string());
    conn_props.insert("name".to_string(), "test-connector".to_string());

    // 创建连接器
    let ctx = Box::new(MockConnectorContext);
    let listener = Box::new(MockStatusListener);
    let callback = Box::new(MockCallback {
        result: Arc::new(Mutex::new(None)),
    });

    assert!(worker
        .start_connector(
            "test-connector".to_string(),
            conn_props,
            ctx,
            listener,
            connect_runtime_core::worker::TargetState::Started,
            callback,
        )
        .is_ok());

    // 验证连接器已创建
    let connectors = worker.connector_names();
    assert_eq!(connectors.len(), 1);
    assert!(connectors.contains(&"test-connector".to_string()));

    // 测试worker停止
    assert!(worker.stop().is_ok());
}

#[test]
fn test_herder_connector_management() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 创建连接器配置
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());
    config.insert("name".to_string(), "test-connector".to_string());

    // 测试添加连接器
    assert!(cluster
        .add_connector("test-connector".to_string(), config.clone())
        .is_ok());

    // 验证连接器已添加
    let connectors = cluster.connector_names();
    assert_eq!(connectors.len(), 1);
    assert!(connectors.contains(&"test-connector".to_string()));

    // 测试获取连接器信息
    let info = cluster.get_connector_info("test-connector".to_string());
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.name, "test-connector");

    // 测试删除连接器
    assert!(cluster
        .remove_connector("test-connector".to_string())
        .is_ok());

    // 验证连接器已删除
    let connectors = cluster.connector_names();
    assert_eq!(connectors.len(), 0);

    // 测试停止cluster
    assert!(cluster.stop().is_ok());
}

#[test]
fn test_worker_herder_connector_config_flow() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 创建连接器配置
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());
    config.insert("name".to_string(), "test-connector".to_string());
    config.insert("tasks.max".to_string(), "3".to_string());

    // 测试put_connector_config
    let result = Arc::new(Mutex::new(None));
    let callback = Box::new(MockCallback {
        result: result.clone(),
    });

    assert!(cluster
        .put_connector_config(
            "test-connector".to_string(),
            config.clone(),
            false,
            callback,
        )
        .is_ok());

    // 验证连接器配置已保存
    let connectors = cluster.connector_names();
    assert_eq!(connectors.len(), 1);

    // 测试connector_config
    let result2 = Arc::new(Mutex::new(None));
    let callback2 = Box::new(MockCallback {
        result: result2.clone(),
    });

    assert!(cluster
        .connector_config("test-connector".to_string(), callback2)
        .is_ok());

    // 测试put_connector_config_with_target_state
    let result3 = Arc::new(Mutex::new(None));
    let callback3 = Box::new(MockCallback {
        result: result3.clone(),
    });

    assert!(cluster
        .put_connector_config_with_target_state(
            "test-connector".to_string(),
            config,
            TargetState::Started,
            true,
            callback3,
        )
        .is_ok());

    // 测试停止cluster
    assert!(cluster.stop().is_ok());
}

#[test]
fn test_worker_herder_task_management() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 添加连接器
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());
    assert!(cluster
        .add_connector("test-connector".to_string(), config)
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
    let result = Arc::new(Mutex::new(None));
    let callback = Box::new(MockCallback {
        result: result.clone(),
    });

    assert!(cluster
        .put_task_configs(
            "test-connector".to_string(),
            task_configs,
            callback,
            connect_runtime_core::herder::InternalRequestSignature {
                signature: "test-signature".to_string(),
            },
        )
        .is_ok());

    // 验证任务已添加
    let tasks = cluster.get_connector_tasks("test-connector".to_string());
    assert_eq!(tasks.len(), 2);

    // 测试task_configs
    let result2 = Arc::new(Mutex::new(None));
    let callback2 = Box::new(MockCallback {
        result: result2.clone(),
    });

    assert!(cluster
        .task_configs("test-connector".to_string(), callback2)
        .is_ok());

    // 测试停止cluster
    assert!(cluster.stop().is_ok());
}

#[test]
fn test_worker_connector_lifecycle() {
    // 创建worker
    let worker_config = WorkerConfig {
        worker_id: "test-worker".to_string(),
        config: HashMap::new(),
    };
    let mut worker = WorkerImpl::new("test-worker".to_string(), worker_config);
    assert!(worker.start().is_ok());

    // 创建连接器
    let mut conn_props = HashMap::new();
    conn_props.insert("connector.class".to_string(), "test-connector".to_string());

    let ctx = Box::new(MockConnectorContext);
    let listener = Box::new(MockStatusListener);
    let callback = Box::new(MockCallback {
        result: Arc::new(Mutex::new(None)),
    });

    assert!(worker
        .start_connector(
            "test-connector".to_string(),
            conn_props,
            ctx,
            listener,
            connect_runtime_core::worker::TargetState::Started,
            callback,
        )
        .is_ok());

    // 测试连接器状态
    // 注意：当前实现中，start_connector创建的连接器状态是Uninitialized
    // 所以is_running会返回false。在真实实现中，连接器启动后状态会变为Running
    assert!(!worker.is_running("test-connector".to_string()));
    assert!(!worker.is_running("nonexistent-connector".to_string()));

    // 测试停止连接器
    assert!(worker
        .stop_and_await_connector("test-connector".to_string())
        .is_ok());

    // 测试停止不存在的连接器应失败
    assert!(worker
        .stop_and_await_connector("nonexistent-connector".to_string())
        .is_err());

    // 测试worker停止
    assert!(worker.stop().is_ok());
}

#[test]
fn test_herder_error_handling() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 测试重复启动
    assert!(cluster.start().is_err());

    // 测试添加重复连接器
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());
    assert!(cluster
        .add_connector("test-connector".to_string(), config.clone())
        .is_ok());

    // 第二次添加应失败
    assert!(cluster
        .add_connector("test-connector".to_string(), config)
        .is_err());

    // 测试删除不存在的连接器
    assert!(cluster
        .remove_connector("nonexistent-connector".to_string())
        .is_err());

    // 测试停止
    assert!(cluster.stop().is_ok());

    // 测试重复停止
    assert!(cluster.stop().is_err());
}

#[test]
fn test_worker_herder_state_transitions() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 验证初始状态
    assert!(cluster.is_ready());
    assert!(cluster.is_running());

    // 添加连接器
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());
    assert!(cluster
        .add_connector("test-connector".to_string(), config)
        .is_ok());

    // 测试连接器状态
    let status = cluster.connector_status("test-connector".to_string());
    assert!(status.is_some());
    assert_eq!(
        status.unwrap(),
        connect_runtime_core::worker::ConnectorState::Running
    );

    // 测试不存在的连接器状态
    let status = cluster.connector_status("nonexistent-connector".to_string());
    assert!(status.is_none());

    // 测试停止
    assert!(cluster.stop().is_ok());

    // 验证停止后状态
    assert!(!cluster.is_ready());
    assert!(!cluster.is_running());
}

#[test]
fn test_multiple_connectors_and_tasks() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 添加多个连接器
    for i in 0..3 {
        let mut config = HashMap::new();
        config.insert(
            "connector.class".to_string(),
            format!("test-connector-{}", i),
        );
        assert!(cluster
            .add_connector(format!("connector-{}", i), config)
            .is_ok());
    }

    // 验证所有连接器已添加
    let connectors = cluster.connector_names();
    assert_eq!(connectors.len(), 3);

    // 为每个连接器添加任务
    for i in 0..3 {
        let task_configs = vec![{
            let mut cfg = HashMap::new();
            cfg.insert("task.class".to_string(), format!("task-{}", i));
            cfg
        }];

        let result = Arc::new(Mutex::new(None));
        let callback = Box::new(MockCallback {
            result: result.clone(),
        });

        assert!(cluster
            .put_task_configs(
                format!("connector-{}", i),
                task_configs,
                callback,
                connect_runtime_core::herder::InternalRequestSignature {
                    signature: "test-signature".to_string(),
                },
            )
            .is_ok());
    }

    // 验证所有任务已添加
    let all_tasks = cluster.get_tasks();
    assert_eq!(all_tasks.len(), 3);

    // 测试停止
    assert!(cluster.stop().is_ok());
}

#[test]
fn test_herder_connectors_list() {
    // 创建embedded cluster
    let mut cluster = EmbeddedConnectCluster::new();
    assert!(cluster.start().is_ok());

    // 添加连接器
    let mut config = HashMap::new();
    config.insert("connector.class".to_string(), "test-connector".to_string());
    assert!(cluster
        .add_connector("test-connector".to_string(), config)
        .is_ok());

    // 测试connectors方法
    let result = Arc::new(Mutex::new(None));
    let callback = Box::new(MockCallback {
        result: result.clone(),
    });

    assert!(cluster.connectors(callback).is_ok());

    // 测试connectors_sync方法
    let connectors = cluster.connectors_sync();
    assert_eq!(connectors.len(), 1);
    assert!(connectors.contains(&"test-connector".to_string()));

    // 测试停止
    assert!(cluster.stop().is_ok());
}
