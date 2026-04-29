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

//! REST API entities (DTOs) for Kafka Connect.
//!
//! These entities represent the JSON structures used in REST API responses.
//! Corresponds to `org.apache.kafka.connect.runtime.rest.entities` in Java.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Information about a connector.
///
/// Corresponds to `ConnectorInfo` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorInfo {
    /// The name of the connector.
    pub name: String,
    /// The connector type (source or sink).
    #[serde(rename = "type")]
    pub connector_type: ConnectorType,
    /// The configuration of the connector.
    pub config: HashMap<String, String>,
    /// The tasks assigned to this connector.
    pub tasks: Vec<ConnectorTaskId>,
}

impl ConnectorInfo {
    /// Creates a new ConnectorInfo.
    pub fn new(
        name: String,
        connector_type: ConnectorType,
        config: HashMap<String, String>,
        tasks: Vec<ConnectorTaskId>,
    ) -> Self {
        Self {
            name,
            connector_type,
            config,
            tasks,
        }
    }
}

/// The type of a connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorType {
    Source,
    Sink,
}

impl std::fmt::Display for ConnectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorType::Source => write!(f, "source"),
            ConnectorType::Sink => write!(f, "sink"),
        }
    }
}

/// Unique identifier for a connector task.
///
/// Corresponds to `ConnectorTaskId` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectorTaskId {
    /// The name of the connector.
    pub connector: String,
    /// The task number within the connector.
    pub task: i32,
}

impl ConnectorTaskId {
    /// Creates a new ConnectorTaskId.
    pub fn new(connector: String, task: i32) -> Self {
        Self { connector, task }
    }
}

impl std::fmt::Display for ConnectorTaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.connector, self.task)
    }
}

/// Information about a task.
///
/// Corresponds to `TaskInfo` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInfo {
    /// The ID of the task.
    pub id: ConnectorTaskId,
    /// The configuration of the task.
    pub config: HashMap<String, String>,
}

impl TaskInfo {
    /// Creates a new TaskInfo.
    pub fn new(id: ConnectorTaskId, config: HashMap<String, String>) -> Self {
        Self { id, config }
    }
}

/// Status of a connector or task.
///
/// Corresponds to `ConnectorState` and `TaskState` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConnectorState {
    Unassigned,
    Running,
    Paused,
    Failed,
    Destroyed,
    Restarting,
}

impl std::fmt::Display for ConnectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorState::Unassigned => write!(f, "UNASSIGNED"),
            ConnectorState::Running => write!(f, "RUNNING"),
            ConnectorState::Paused => write!(f, "PAUSED"),
            ConnectorState::Failed => write!(f, "FAILED"),
            ConnectorState::Destroyed => write!(f, "DESTROYED"),
            ConnectorState::Restarting => write!(f, "RESTARTING"),
        }
    }
}

/// Status of a connector with worker assignment.
///
/// Corresponds to `ConnectorStateInfo` in Java (not ConnectorStatus).
/// Note: In Java, ConnectorStateInfo includes name, connector state, tasks, and type.
/// In Rust, this is named ConnectorStatus to avoid confusion with the simplified ConnectorStateInfo.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorStatus {
    /// The name of the connector.
    pub name: String,
    /// The current state of the connector.
    pub connector: ConnectorStateInfo,
    /// The status of the tasks.
    pub tasks: Vec<TaskStatus>,
    /// The type of the connector (source or sink).
    #[serde(rename = "type")]
    pub connector_type: ConnectorType,
}

impl ConnectorStatus {
    /// Creates a new ConnectorStatus.
    pub fn new(
        name: String,
        connector: ConnectorStateInfo,
        tasks: Vec<TaskStatus>,
        connector_type: ConnectorType,
    ) -> Self {
        Self {
            name,
            connector,
            tasks,
            connector_type,
        }
    }
}

/// State information for a connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorStateInfo {
    /// The current state.
    pub state: ConnectorState,
    /// The worker ID assigned to this connector.
    pub worker_id: String,
}

impl ConnectorStateInfo {
    /// Creates a new ConnectorStateInfo.
    pub fn new(state: ConnectorState, worker_id: String) -> Self {
        Self { state, worker_id }
    }
}

/// Status of a task.
///
/// Corresponds to `TaskStatus` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStatus {
    /// The ID of the task.
    pub id: i32,
    /// The current state of the task.
    pub state: ConnectorState,
    /// The worker ID assigned to this task.
    pub worker_id: String,
    /// Optional trace message if the task failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
}

impl TaskStatus {
    /// Creates a new TaskStatus.
    pub fn new(id: i32, state: ConnectorState, worker_id: String, trace: Option<String>) -> Self {
        Self {
            id,
            state,
            worker_id,
            trace,
        }
    }
}

/// Error message returned by the REST API.
///
/// Corresponds to `ErrorMessage` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorMessage {
    /// The error code (HTTP status code).
    pub error_code: i32,
    /// The error message.
    pub message: String,
}

impl ErrorMessage {
    /// Creates a new ErrorMessage.
    pub fn new(error_code: i32, message: String) -> Self {
        Self {
            error_code,
            message,
        }
    }
}

/// Create or update connector request.
///
/// Corresponds to request body for POST /connectors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateConnectorRequest {
    /// The name of the connector (optional, can be specified in config).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The configuration of the connector.
    pub config: HashMap<String, String>,
}

impl CreateConnectorRequest {
    /// Creates a new CreateConnectorRequest.
    pub fn new(config: HashMap<String, String>) -> Self {
        Self { name: None, config }
    }

    /// Creates a new CreateConnectorRequest with a name.
    pub fn with_name(name: String, config: HashMap<String, String>) -> Self {
        Self {
            name: Some(name),
            config,
        }
    }
}

/// Response for connector config validation.
///
/// Corresponds to `ConfigInfos` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigInfos {
    /// The name of the connector.
    pub name: String,
    /// The number of errors found.
    pub error_count: i32,
    /// The list of config group names.
    #[serde(default)]
    pub groups: Vec<String>,
    /// The validation results for each config key.
    pub configs: Vec<ConfigInfo>,
}

impl ConfigInfos {
    /// Creates a new ConfigInfos.
    pub fn new(
        name: String,
        error_count: i32,
        groups: Vec<String>,
        configs: Vec<ConfigInfo>,
    ) -> Self {
        Self {
            name,
            error_count,
            groups,
            configs,
        }
    }

    /// Creates a new ConfigInfos without groups.
    pub fn without_groups(name: String, configs: Vec<ConfigInfo>, error_count: i32) -> Self {
        Self {
            name,
            error_count,
            groups: Vec::new(),
            configs,
        }
    }
}

/// Validation result for a single config key.
///
/// Corresponds to `ConfigInfo` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigInfo {
    /// The definition of the config key.
    pub definition: ConfigKeyDefinition,
    /// The value of the config.
    pub value: ConfigValueInfo,
}

impl ConfigInfo {
    /// Creates a new ConfigInfo.
    pub fn new(definition: ConfigKeyDefinition, value: ConfigValueInfo) -> Self {
        Self { definition, value }
    }
}

/// Definition of a config key.
///
/// Corresponds to `ConfigKeyInfo` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigKeyDefinition {
    /// The name of the config key.
    pub name: String,
    /// The type of the config value.
    #[serde(rename = "type")]
    pub config_type: ConfigDefType,
    /// Whether this config is required.
    pub required: bool,
    /// The default value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    /// The importance level.
    pub importance: ConfigDefImportance,
    /// The documentation string.
    pub documentation: String,
    /// The group name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// The order within the group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<i32>,
    /// The width for display.
    pub width: ConfigDefWidth,
    /// Display name for UI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Dependents (other configs that depend on this one).
    #[serde(default)]
    pub dependents: Vec<String>,
}

impl ConfigKeyDefinition {
    /// Creates a new ConfigKeyDefinition.
    pub fn new(
        name: String,
        config_type: ConfigDefType,
        required: bool,
        default_value: Option<String>,
        importance: ConfigDefImportance,
        documentation: String,
        width: ConfigDefWidth,
    ) -> Self {
        Self {
            name,
            config_type,
            required,
            default_value,
            importance,
            documentation,
            group: None,
            order: None,
            width,
            display_name: None,
            dependents: Vec::new(),
        }
    }
}

/// Type of a config definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigDefType {
    String,
    Int,
    Long,
    Double,
    Boolean,
    List,
    Class,
    Password,
}

impl std::fmt::Display for ConfigDefType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigDefType::String => write!(f, "string"),
            ConfigDefType::Int => write!(f, "int"),
            ConfigDefType::Long => write!(f, "long"),
            ConfigDefType::Double => write!(f, "double"),
            ConfigDefType::Boolean => write!(f, "boolean"),
            ConfigDefType::List => write!(f, "list"),
            ConfigDefType::Class => write!(f, "class"),
            ConfigDefType::Password => write!(f, "password"),
        }
    }
}

/// Importance level of a config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigDefImportance {
    High,
    Medium,
    Low,
}

impl std::fmt::Display for ConfigDefImportance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigDefImportance::High => write!(f, "high"),
            ConfigDefImportance::Medium => write!(f, "medium"),
            ConfigDefImportance::Low => write!(f, "low"),
        }
    }
}

/// Width of a config for display purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ConfigDefWidth {
    None,
    Short,
    Medium,
    Long,
}

impl std::fmt::Display for ConfigDefWidth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigDefWidth::None => write!(f, "NONE"),
            ConfigDefWidth::Short => write!(f, "SHORT"),
            ConfigDefWidth::Medium => write!(f, "MEDIUM"),
            ConfigDefWidth::Long => write!(f, "LONG"),
        }
    }
}

/// Value info for a config.
///
/// Corresponds to `ConfigValueInfo` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValueInfo {
    /// The name of the config key.
    pub name: String,
    /// The current value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Recommended values.
    #[serde(default)]
    pub recommended_values: Vec<String>,
    /// Error messages.
    #[serde(default)]
    pub errors: Vec<String>,
    /// Whether the value is visible.
    pub visible: bool,
}

impl ConfigValueInfo {
    /// Creates a new ConfigValueInfo.
    pub fn new(name: String, value: Option<String>, visible: bool) -> Self {
        Self {
            name,
            value,
            recommended_values: Vec::new(),
            errors: Vec::new(),
            visible,
        }
    }
}

/// Worker information.
///
/// Corresponds to `WorkerInfo` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerInfo {
    /// The version of the worker.
    pub version: String,
    /// The commit ID of the worker.
    pub commit: String,
    /// The Kafka cluster ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kafka_cluster_id: Option<String>,
}

impl WorkerInfo {
    /// Creates a new WorkerInfo.
    pub fn new(version: String, commit: String) -> Self {
        Self {
            version,
            commit,
            kafka_cluster_id: None,
        }
    }
}

/// Server info.
///
/// Used in the root endpoint response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfo {
    /// The version of Kafka Connect.
    pub version: String,
    /// The commit ID.
    pub commit: String,
    /// The Kafka cluster ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kafka_cluster_id: Option<String>,
}

impl ServerInfo {
    /// Creates a new ServerInfo.
    pub fn new(version: String, commit: String) -> Self {
        Self {
            version,
            commit,
            kafka_cluster_id: None,
        }
    }
}

/// List of active connectors.
///
/// Response for GET /connectors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorList {
    /// The list of connector names.
    pub connectors: Vec<String>,
}

impl ConnectorList {
    /// Creates a new ConnectorList.
    pub fn new(connectors: Vec<String>) -> Self {
        Self { connectors }
    }
}

impl From<Vec<String>> for ConnectorList {
    fn from(connectors: Vec<String>) -> Self {
        Self::new(connectors)
    }
}

/// Connector offsets info.
///
/// Corresponds to `ConnectorOffsets` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorOffsets {
    /// The offsets for each task.
    pub offsets: Vec<TaskOffset>,
}

impl ConnectorOffsets {
    /// Creates a new ConnectorOffsets.
    pub fn new(offsets: Vec<TaskOffset>) -> Self {
        Self { offsets }
    }
}

/// Offset for a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskOffset {
    /// The task ID.
    pub task: ConnectorTaskId,
    /// The offset key-value pairs.
    pub offset: HashMap<String, serde_json::Value>,
}

impl TaskOffset {
    /// Creates a new TaskOffset.
    pub fn new(task: ConnectorTaskId, offset: HashMap<String, serde_json::Value>) -> Self {
        Self { task, offset }
    }
}

/// Worker status for health check endpoint.
///
/// Corresponds to `WorkerStatus` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerStatus {
    /// The status string (e.g., "healthy", "unhealthy", "starting").
    pub status: String,
    /// The status message with details.
    pub message: String,
}

impl WorkerStatus {
    /// Creates a healthy worker status.
    pub fn healthy() -> Self {
        WorkerStatus {
            status: "healthy".to_string(),
            message: "Worker has completed startup and is ready to handle requests.".to_string(),
        }
    }

    /// Creates a starting worker status.
    ///
    /// # Arguments
    ///
    /// * `status_details` - Optional details about the startup status
    pub fn starting(status_details: Option<&str>) -> Self {
        let message = if let Some(details) = status_details {
            format!("Worker is still starting up. {}", details)
        } else {
            "Worker is still starting up.".to_string()
        };
        WorkerStatus {
            status: "starting".to_string(),
            message,
        }
    }

    /// Creates an unhealthy worker status.
    ///
    /// # Arguments
    ///
    /// * `status_details` - Optional details about the unhealthy status
    pub fn unhealthy(status_details: Option<&str>) -> Self {
        let message = if let Some(details) = status_details {
            format!("Worker was unable to handle this request and may be unable to handle other requests. {}", details)
        } else {
            "Worker was unable to handle this request and may be unable to handle other requests."
                .to_string()
        };
        WorkerStatus {
            status: "unhealthy".to_string(),
            message,
        }
    }

    /// Creates a new WorkerStatus with custom values.
    pub fn new(status: String, message: String) -> Self {
        WorkerStatus { status, message }
    }
}

/// Logger level information.
///
/// Corresponds to `LoggerLevel` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoggerLevel {
    /// The log level (e.g., "DEBUG", "INFO", "WARN", "ERROR").
    pub level: String,
    /// Last modified timestamp (Unix epoch milliseconds).
    #[serde(rename = "last_modified")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<i64>,
}

impl LoggerLevel {
    /// Creates a new LoggerLevel.
    ///
    /// # Arguments
    ///
    /// * `level` - The log level string
    /// * `last_modified` - Optional last modified timestamp
    pub fn new(level: String, last_modified: Option<i64>) -> Self {
        LoggerLevel {
            level,
            last_modified,
        }
    }

    /// Creates a LoggerLevel without last_modified.
    pub fn with_level(level: String) -> Self {
        LoggerLevel::new(level, None)
    }
}

/// Active topics information for a connector.
///
/// Corresponds to `ActiveTopicsInfo` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveTopicsInfo {
    /// The connector name.
    pub connector: String,
    /// Active topics for the connector.
    #[serde(default)]
    pub topics: Vec<String>,
}

impl ActiveTopicsInfo {
    /// Creates a new ActiveTopicsInfo.
    pub fn new(connector: String, topics: Vec<String>) -> Self {
        ActiveTopicsInfo { connector, topics }
    }
}

/// Plugin information for a connector plugin.
///
/// Corresponds to `PluginInfo` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginInfo {
    /// The plugin class name.
    pub class: String,
    /// Plugin type (source, sink, connector).
    #[serde(rename = "type")]
    pub plugin_type: String,
    /// Plugin version.
    pub version: String,
}

impl PluginInfo {
    /// Creates a new PluginInfo.
    pub fn new(class: String, plugin_type: String, version: String) -> Self {
        PluginInfo {
            class,
            plugin_type,
            version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_type_display() {
        assert_eq!(ConnectorType::Source.to_string(), "source");
        assert_eq!(ConnectorType::Sink.to_string(), "sink");
    }

    #[test]
    fn test_connector_task_id_display() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        assert_eq!(id.to_string(), "test-connector-0");
    }

    #[test]
    fn test_connector_state_display() {
        assert_eq!(ConnectorState::Running.to_string(), "RUNNING");
        assert_eq!(ConnectorState::Paused.to_string(), "PAUSED");
        assert_eq!(ConnectorState::Failed.to_string(), "FAILED");
    }

    #[test]
    fn test_connector_info_serialization() {
        let info = ConnectorInfo::new(
            "test".to_string(),
            ConnectorType::Source,
            HashMap::from([("name".to_string(), "test".to_string())]),
            vec![ConnectorTaskId::new("test".to_string(), 0)],
        );
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"name\":\"test\""));
        assert!(json.contains("\"type\":\"source\""));
    }

    #[test]
    fn test_connector_info_deserialization() {
        let json =
            r#"{"name":"test","type":"sink","config":{},"tasks":[{"connector":"test","task":0}]}"#;
        let info: ConnectorInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.name, "test");
        assert_eq!(info.connector_type, ConnectorType::Sink);
        assert_eq!(info.tasks.len(), 1);
    }

    #[test]
    fn test_error_message() {
        let error = ErrorMessage::new(404, "Connector not found".to_string());
        assert_eq!(error.error_code, 404);
        assert_eq!(error.message, "Connector not found");
    }

    #[test]
    fn test_create_connector_request() {
        let req = CreateConnectorRequest::with_name(
            "test".to_string(),
            HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]),
        );
        assert_eq!(req.name, Some("test".to_string()));
        assert!(req.config.contains_key("connector.class"));
    }

    #[test]
    fn test_config_def_type_serialization() {
        assert_eq!(
            serde_json::to_string(&ConfigDefType::String).unwrap(),
            "\"string\""
        );
        assert_eq!(
            serde_json::to_string(&ConfigDefType::Boolean).unwrap(),
            "\"boolean\""
        );
    }

    #[test]
    fn test_connector_list_from_vec() {
        let list = ConnectorList::from(vec!["conn1".to_string(), "conn2".to_string()]);
        assert_eq!(list.connectors.len(), 2);
    }

    #[test]
    fn test_worker_info() {
        let info = WorkerInfo::new("3.0.0".to_string(), "abc123".to_string());
        assert_eq!(info.version, "3.0.0");
        assert!(info.kafka_cluster_id.is_none());
    }

    #[test]
    fn test_worker_status_healthy() {
        let status = WorkerStatus::healthy();
        assert_eq!(status.status, "healthy");
        assert!(status.message.contains("ready"));
    }

    #[test]
    fn test_worker_status_starting() {
        let status = WorkerStatus::starting(Some("Waiting for leader"));
        assert_eq!(status.status, "starting");
        assert!(status.message.contains("Waiting for leader"));
    }

    #[test]
    fn test_worker_status_unhealthy() {
        let status = WorkerStatus::unhealthy(None);
        assert_eq!(status.status, "unhealthy");
        assert!(status.message.contains("unable to handle"));
    }

    #[test]
    fn test_logger_level() {
        let level = LoggerLevel::new("INFO".to_string(), Some(1234567890));
        assert_eq!(level.level, "INFO");
        assert_eq!(level.last_modified, Some(1234567890));

        let level_no_time = LoggerLevel::with_level("DEBUG".to_string());
        assert_eq!(level_no_time.level, "DEBUG");
        assert!(level_no_time.last_modified.is_none());
    }

    #[test]
    fn test_logger_level_serialization() {
        let level = LoggerLevel::new("WARN".to_string(), Some(1000));
        let json = serde_json::to_string(&level).unwrap();
        assert!(json.contains("\"level\":\"WARN\""));
        assert!(json.contains("\"last_modified\":1000"));
    }

    #[test]
    fn test_active_topics_info() {
        let info = ActiveTopicsInfo::new(
            "test-connector".to_string(),
            vec!["topic1".to_string(), "topic2".to_string()],
        );
        assert_eq!(info.connector, "test-connector");
        assert_eq!(info.topics.len(), 2);
    }

    #[test]
    fn test_plugin_info() {
        let plugin = PluginInfo::new(
            "org.apache.kafka.connect.file.FileStreamSourceConnector".to_string(),
            "source".to_string(),
            "1.0.0".to_string(),
        );
        assert_eq!(
            plugin.class,
            "org.apache.kafka.connect.file.FileStreamSourceConnector"
        );
        assert_eq!(plugin.plugin_type, "source");
        assert_eq!(plugin.version, "1.0.0");
    }
}
