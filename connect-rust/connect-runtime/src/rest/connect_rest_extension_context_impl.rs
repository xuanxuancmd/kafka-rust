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

//! ConnectRestExtensionContextImpl - Implementation of REST extension context.
//!
//! This implementation provides access to the REST configurable (Router wrapper)
//! and cluster state information for REST extensions.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.ConnectRestExtensionContextImpl` in Java.

use crate::health::ConnectClusterStateImpl;
use crate::rest::ConnectRestConfigurable;
use common_trait::herder::Herder;

/// Implementation of ConnectRestExtensionContext for REST extensions.
///
/// This struct provides the context needed by REST extensions to:
/// - Register additional REST resources via the configurable
/// - Access cluster state and connector health information
///
/// In Java, this is a simple record class implementing the ConnectRestExtensionContext interface.
/// The Rust adaptation uses a struct with the same core fields.
///
/// # Type Parameters
///
/// * `H` - The Herder type used for cluster state queries
pub struct ConnectRestExtensionContextImpl<H: Herder> {
    /// The REST configurable for registering additional resources.
    configurable: ConnectRestConfigurable,
    /// The cluster state for accessing connector and task health info.
    cluster_state: ConnectClusterStateImpl<H>,
}

impl<H: Herder> ConnectRestExtensionContextImpl<H> {
    /// Creates a new ConnectRestExtensionContextImpl.
    ///
    /// # Arguments
    ///
    /// * `configurable` - The REST configurable for resource registration
    /// * `cluster_state` - The cluster state for health information
    pub fn new(
        configurable: ConnectRestConfigurable,
        cluster_state: ConnectClusterStateImpl<H>,
    ) -> Self {
        ConnectRestExtensionContextImpl {
            configurable,
            cluster_state,
        }
    }

    /// Creates a new context with default configurable.
    ///
    /// # Arguments
    ///
    /// * `cluster_state` - The cluster state for health information
    pub fn with_cluster_state(cluster_state: ConnectClusterStateImpl<H>) -> Self {
        ConnectRestExtensionContextImpl {
            configurable: ConnectRestConfigurable::new(),
            cluster_state,
        }
    }

    /// Returns the REST configurable for registering additional resources.
    ///
    /// REST extensions can use this to add their own routes to the Connect REST API.
    /// The configurable prevents duplicate registrations automatically.
    pub fn configurable(&self) -> &ConnectRestConfigurable {
        &self.configurable
    }

    /// Returns a mutable reference to the REST configurable.
    ///
    /// This allows extensions to modify the configurable for registration.
    pub fn configurable_mut(&mut self) -> &mut ConnectRestConfigurable {
        &mut self.configurable
    }

    /// Returns the cluster state for accessing connector and task health.
    ///
    /// REST extensions can query the cluster state to build health-related
    /// endpoints or informational responses.
    pub fn cluster_state(&self) -> &ConnectClusterStateImpl<H> {
        &self.cluster_state
    }

    /// Returns the underlying Router from the configurable.
    ///
    /// This is a convenience method to access the Router directly.
    pub fn router(&self) -> &axum::Router {
        self.configurable.router()
    }

    /// Takes ownership of the Router from the configurable.
    ///
    /// After calling this method, the configurable should not be used.
    pub fn into_router(self) -> axum::Router {
        self.configurable.into_router()
    }
}

impl<H: Herder> Clone for ConnectRestExtensionContextImpl<H> {
    fn clone(&self) -> Self {
        ConnectRestExtensionContextImpl {
            configurable: self.configurable.clone(),
            cluster_state: self.cluster_state.clone(),
        }
    }
}

impl<H: Herder> std::fmt::Debug for ConnectRestExtensionContextImpl<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectRestExtensionContextImpl")
            .field("configurable", &self.configurable)
            .field("cluster_state", &self.cluster_state)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::ConnectClusterDetailsImpl;
    use common_trait::herder::{
        ActiveTopicsInfo, Callback, ConfigInfos, ConfigKeyInfo, ConnectorInfo, ConnectorOffsets,
        ConnectorState, ConnectorStateInfo, ConnectorTaskId, Created, HerderRequest,
        InternalRequestSignature, LoggerLevel, Message, PluginDesc, Plugins, RestartRequest,
        StatusBackingStore, TargetState, TaskInfo, TaskStateInfo, VersionRange,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    // Mock Herder for testing
    struct MockHerder;

    // Minimal mock implementations for callback
    struct MockCallback<T>(std::marker::PhantomData<T>);
    impl<T> Callback<T> for MockCallback<T> {
        fn on_completion(&self, _result: T) {}
        fn on_error(&self, _error: String) {}
    }

    struct MockPlugins;
    impl Plugins for MockPlugins {
        fn connector_plugins(&self) -> Vec<String> {
            Vec::new()
        }
        fn converter_plugins(&self) -> Vec<String> {
            Vec::new()
        }
        fn transformation_plugins(&self) -> Vec<String> {
            Vec::new()
        }
        fn connector_plugin_desc(&self, _class_name: &str) -> Option<PluginDesc> {
            None
        }
        fn converter_plugin_desc(&self, _class_name: &str) -> Option<PluginDesc> {
            None
        }
    }

    struct MockStatusBackingStore;
    impl StatusBackingStore for MockStatusBackingStore {
        fn get_connector_state(&self, _connector: &str) -> Option<ConnectorState> {
            None
        }
        fn get_task_state(&self, _id: &ConnectorTaskId) -> Option<TaskStateInfo> {
            None
        }
        fn put_connector_state(&self, _connector: &str, _state: ConnectorState) {}
        fn put_task_state(&self, _id: &ConnectorTaskId, _state: TaskStateInfo) {}
    }

    struct MockHerderRequest;
    impl HerderRequest for MockHerderRequest {
        fn cancel(&self) {}
        fn is_completed(&self) -> bool {
            false
        }
    }

    // Mock for herder::ConnectMetrics trait (used by MockHerder::connect_metrics)
    struct MockConnectMetrics;
    impl common_trait::herder::ConnectMetrics for MockConnectMetrics {
        fn registry(&self) -> &dyn common_trait::herder::MetricsRegistry {
            &MockMetricsRegistry
        }
        fn stop(&self) {}
    }

    struct MockMetricsRegistry;
    impl common_trait::herder::MetricsRegistry for MockMetricsRegistry {
        fn worker_group_name(&self) -> &str {
            "test-group"
        }
    }

    impl Herder for MockHerder {
        fn start(&mut self) {}
        fn stop(&mut self) {}
        fn is_ready(&self) -> bool {
            true
        }
        fn health_check(&self, _callback: Box<dyn Callback<()>>) {}

        fn connectors_async(&self, callback: Box<dyn Callback<Vec<String>>>) {
            callback.on_completion(Vec::new());
        }
        fn connector_info_async(
            &self,
            _conn_name: &str,
            callback: Box<dyn Callback<ConnectorInfo>>,
        ) {
            callback.on_completion(ConnectorInfo {
                name: "test".to_string(),
                config: HashMap::new(),
                tasks: Vec::new(),
            });
        }
        fn connector_config_async(
            &self,
            _conn_name: &str,
            callback: Box<dyn Callback<HashMap<String, String>>>,
        ) {
            callback.on_completion(HashMap::new());
        }

        fn put_connector_config(
            &self,
            _conn_name: &str,
            _config: HashMap<String, String>,
            _allow_replace: bool,
            callback: Box<dyn Callback<Created<ConnectorInfo>>>,
        ) {
            callback.on_completion(Created {
                created: true,
                info: ConnectorInfo {
                    name: "test".to_string(),
                    config: HashMap::new(),
                    tasks: Vec::new(),
                },
            });
        }
        fn put_connector_config_with_state(
            &self,
            _conn_name: &str,
            _config: HashMap<String, String>,
            _target_state: TargetState,
            _allow_replace: bool,
            callback: Box<dyn Callback<Created<ConnectorInfo>>>,
        ) {
            callback.on_completion(Created {
                created: true,
                info: ConnectorInfo {
                    name: "test".to_string(),
                    config: HashMap::new(),
                    tasks: Vec::new(),
                },
            });
        }
        fn patch_connector_config(
            &self,
            _conn_name: &str,
            _config_patch: HashMap<String, String>,
            callback: Box<dyn Callback<Created<ConnectorInfo>>>,
        ) {
            callback.on_completion(Created {
                created: true,
                info: ConnectorInfo {
                    name: "test".to_string(),
                    config: HashMap::new(),
                    tasks: Vec::new(),
                },
            });
        }
        fn delete_connector_config(
            &self,
            _conn_name: &str,
            callback: Box<dyn Callback<Created<ConnectorInfo>>>,
        ) {
            callback.on_completion(Created {
                created: false,
                info: ConnectorInfo {
                    name: "test".to_string(),
                    config: HashMap::new(),
                    tasks: Vec::new(),
                },
            });
        }

        fn request_task_reconfiguration(&self, _conn_name: &str) {}
        fn task_configs_async(&self, _conn_name: &str, callback: Box<dyn Callback<Vec<TaskInfo>>>) {
            callback.on_completion(Vec::new());
        }
        fn put_task_configs(
            &self,
            _conn_name: &str,
            _configs: Vec<HashMap<String, String>>,
            callback: Box<dyn Callback<()>>,
            _request_signature: InternalRequestSignature,
        ) {
            callback.on_completion(());
        }
        fn fence_zombie_source_tasks(
            &self,
            _conn_name: &str,
            callback: Box<dyn Callback<()>>,
            _request_signature: InternalRequestSignature,
        ) {
            callback.on_completion(());
        }

        fn connectors_sync(&self) -> Vec<String> {
            Vec::new()
        }
        fn connector_info_sync(&self, _conn_name: &str) -> ConnectorInfo {
            ConnectorInfo {
                name: "test".to_string(),
                config: HashMap::new(),
                tasks: Vec::new(),
            }
        }
        fn connector_status(&self, _name: &str) -> ConnectorStateInfo {
            ConnectorStateInfo {
                name: "test".to_string(),
                connector: ConnectorState::Running,
                tasks: Vec::new(),
            }
        }
        fn connector_active_topics(&self, _conn_name: &str) -> ActiveTopicsInfo {
            ActiveTopicsInfo {
                connector: "test".to_string(),
                topics: Vec::new(),
            }
        }
        fn reset_connector_active_topics(&self, _conn_name: &str) {}

        fn status_backing_store(&self) -> Box<dyn StatusBackingStore> {
            Box::new(MockStatusBackingStore)
        }
        fn task_status(&self, _id: &ConnectorTaskId) -> TaskStateInfo {
            TaskStateInfo {
                id: ConnectorTaskId::new("test".to_string(), 0),
                state: ConnectorState::Running,
                worker_id: "worker1".to_string(),
                trace: None,
            }
        }

        fn validate_connector_config_async(
            &self,
            _connector_config: HashMap<String, String>,
            callback: Box<dyn Callback<ConfigInfos>>,
        ) {
            callback.on_completion(ConfigInfos {
                name: "test".to_string(),
                error_count: 0,
                group_count: 0,
                configs: Vec::new(),
            });
        }
        fn validate_connector_config_async_with_log(
            &self,
            _connector_config: HashMap<String, String>,
            callback: Box<dyn Callback<ConfigInfos>>,
            _do_log: bool,
        ) {
            callback.on_completion(ConfigInfos {
                name: "test".to_string(),
                error_count: 0,
                group_count: 0,
                configs: Vec::new(),
            });
        }

        fn restart_task(&self, _id: &ConnectorTaskId, callback: Box<dyn Callback<()>>) {
            callback.on_completion(());
        }
        fn restart_connector_async(&self, _conn_name: &str, callback: Box<dyn Callback<()>>) {
            callback.on_completion(());
        }
        fn restart_connector_delayed(
            &self,
            _delay_ms: u64,
            _conn_name: &str,
            _callback: Box<dyn Callback<()>>,
        ) -> Box<dyn HerderRequest> {
            Box::new(MockHerderRequest)
        }

        fn pause_connector(&self, _connector: &str) {}
        fn resume_connector(&self, _connector: &str) {}
        fn stop_connector_async(&self, _connector: &str, callback: Box<dyn Callback<()>>) {
            callback.on_completion(());
        }

        fn restart_connector_and_tasks(
            &self,
            _request: &RestartRequest,
            callback: Box<dyn Callback<ConnectorStateInfo>>,
        ) {
            callback.on_completion(ConnectorStateInfo {
                name: "test".to_string(),
                connector: ConnectorState::Running,
                tasks: Vec::new(),
            });
        }

        fn plugins(&self) -> Box<dyn Plugins> {
            Box::new(MockPlugins)
        }
        fn kafka_cluster_id(&self) -> String {
            "test-cluster".to_string()
        }
        fn connector_plugin_config(&self, _plugin_name: &str) -> Vec<ConfigKeyInfo> {
            Vec::new()
        }
        fn connector_plugin_config_with_version(
            &self,
            _plugin_name: &str,
            _version: VersionRange,
        ) -> Vec<ConfigKeyInfo> {
            Vec::new()
        }

        fn connector_offsets_async(
            &self,
            _conn_name: &str,
            callback: Box<dyn Callback<ConnectorOffsets>>,
        ) {
            callback.on_completion(ConnectorOffsets {
                offsets: HashMap::new(),
            });
        }
        fn alter_connector_offsets(
            &self,
            _conn_name: &str,
            _offsets: HashMap<
                HashMap<String, serde_json::Value>,
                HashMap<String, serde_json::Value>,
            >,
            callback: Box<dyn Callback<Message>>,
        ) {
            callback.on_completion(Message {
                code: 0,
                message: "success".to_string(),
            });
        }
        fn reset_connector_offsets(&self, _conn_name: &str, callback: Box<dyn Callback<Message>>) {
            callback.on_completion(Message {
                code: 0,
                message: "success".to_string(),
            });
        }

        fn logger_level(&self, _logger: &str) -> LoggerLevel {
            LoggerLevel {
                logger: "test".to_string(),
                level: "INFO".to_string(),
                effective_level: "INFO".to_string(),
            }
        }
        fn all_logger_levels(&self) -> HashMap<String, LoggerLevel> {
            HashMap::new()
        }
        fn set_worker_logger_level(&self, _namespace: &str, _level: &str) -> Vec<String> {
            Vec::new()
        }
        fn set_cluster_logger_level(&self, _namespace: &str, _level: &str) {}

        fn connect_metrics(&self) -> Box<dyn common_trait::herder::ConnectMetrics> {
            Box::new(MockConnectMetrics)
        }
    }

    fn create_test_cluster_state() -> ConnectClusterStateImpl<MockHerder> {
        let cluster_details = ConnectClusterDetailsImpl::new("test-cluster".to_string());
        ConnectClusterStateImpl::new(1000, cluster_details, Arc::new(MockHerder))
    }

    #[test]
    fn test_new() {
        let configurable = ConnectRestConfigurable::new();
        let cluster_state = create_test_cluster_state();
        let context = ConnectRestExtensionContextImpl::new(configurable, cluster_state);

        assert!(context.configurable().registered_types().is_empty());
    }

    #[test]
    fn test_with_cluster_state() {
        let cluster_state = create_test_cluster_state();
        let context = ConnectRestExtensionContextImpl::with_cluster_state(cluster_state);

        assert!(context.configurable().registered_types().is_empty());
    }

    #[test]
    fn test_configurable_mut() {
        let cluster_state = create_test_cluster_state();
        let mut context = ConnectRestExtensionContextImpl::with_cluster_state(cluster_state);

        context.configurable_mut().register_type("TestResource");
        assert!(context.configurable().is_registered("TestResource"));
    }

    #[test]
    fn test_cluster_state() {
        let cluster_state = create_test_cluster_state();
        let context = ConnectRestExtensionContextImpl::with_cluster_state(cluster_state);

        // Verify we can access cluster state
        let state = context.cluster_state();
        assert_eq!(state.connectors().unwrap_or_default().len(), 0);
    }

    #[test]
    fn test_clone() {
        let cluster_state = create_test_cluster_state();
        let mut context = ConnectRestExtensionContextImpl::with_cluster_state(cluster_state);
        context.configurable_mut().register_type("Resource1");

        let cloned = context.clone();
        assert!(cloned.configurable().is_registered("Resource1"));
    }
}
