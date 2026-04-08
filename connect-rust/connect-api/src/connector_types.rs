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

//! Connector types for Kafka Connect API.
//! This module contains type definitions that are shared across connector implementations.

use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================================
// Connector Type
// ============================================================================================

/// Enum definition that identifies the type of the connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorType {
    /// Identifies a source connector
    Source,
    /// Identifies a sink connector
    Sink,
    /// Identifies a connector whose type could not be inferred
    Unknown,
}

impl std::fmt::Display for ConnectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorType::Source => write!(f, "source"),
            ConnectorType::Sink => write!(f, "sink"),
            ConnectorType::Unknown => write!(f, "unknown"),
        }
    }
}

impl Default for ConnectorType {
    fn default() -> Self {
        ConnectorType::Unknown
    }
}

// ============================================================================================
// Connector Info
// ============================================================================================

/// Information about a connector.
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    /// The name of the connector
    pub name: String,
    /// The connector class
    pub connector_class: String,
    /// The type of the connector
    pub connector_type: ConnectorType,
    /// The current state of the connector
    pub state: ConnectorState,
    /// The worker id
    pub worker_id: String,
    /// The generated configuration
    pub config: HashMap<String, String>,
}

impl ConnectorInfo {
    /// Create a new ConnectorInfo.
    pub fn new(
        name: String,
        connector_class: String,
        connector_type: ConnectorType,
        state: ConnectorState,
        worker_id: String,
        config: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            connector_class,
            connector_type,
            state,
            worker_id,
            config,
        }
    }
}

/// The state of a connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorState {
    /// The connector is running
    Running,
    /// The connector is paused
    Paused,
    /// The connector is restarting
    Restarting,
    /// The connector has failed
    Failed,
    /// The connector is destroyed
    Destroyed,
    /// The connector state is unassigned
    Unassigned,
}

impl std::fmt::Display for ConnectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorState::Running => write!(f, "RUNNING"),
            ConnectorState::Paused => write!(f, "PAUSED"),
            ConnectorState::Restarting => write!(f, "RESTARTING"),
            ConnectorState::Failed => write!(f, "FAILED"),
            ConnectorState::Destroyed => write!(f, "DESTROYED"),
            ConnectorState::Unassigned => write!(f, "UNASSIGNED"),
        }
    }
}

impl Default for ConnectorState {
    fn default() -> Self {
        ConnectorState::Unassigned
    }
}

// ============================================================================================
// Task Info
// ============================================================================================

/// Information about a task.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    /// The id of the task
    pub id: TaskId,
    /// The state of the task
    pub state: TaskState,
    /// The worker id
    pub worker_id: String,
    /// The task configuration
    pub config: HashMap<String, String>,
}

impl TaskInfo {
    /// Create a new TaskInfo.
    pub fn new(
        id: TaskId,
        state: TaskState,
        worker_id: String,
        config: HashMap<String, String>,
    ) -> Self {
        Self {
            id,
            state,
            worker_id,
            config,
        }
    }
}

/// Task identifier containing connector name and task id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskId {
    /// The connector name
    pub connector: String,
    /// The task id
    pub task: i32,
}

impl TaskId {
    /// Create a new TaskId.
    pub fn new(connector: String, task: i32) -> Self {
        Self { connector, task }
    }

    /// Parse a TaskId from a string in the format "connector-name#task-id".
    pub fn parse(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('#').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid task id format: {}", s));
        }
        let task = parts[1]
            .parse::<i32>()
            .map_err(|_| format!("Invalid task id: {}", s))?;
        Ok(Self::new(parts[0].to_string(), task))
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.connector, self.task)
    }
}

/// The state of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// The task is running
    Running,
    /// The task is paused
    Paused,
    /// The task is restarting
    Restarting,
    /// The task has failed
    Failed,
    /// The task is destroyed
    Destroyed,
    /// The task is unassigned
    Unassigned,
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskState::Running => write!(f, "RUNNING"),
            TaskState::Paused => write!(f, "PAUSED"),
            TaskState::Restarting => write!(f, "RESTARTING"),
            TaskState::Failed => write!(f, "FAILED"),
            TaskState::Destroyed => write!(f, "DESTROYED"),
            TaskState::Unassigned => write!(f, "UNASSIGNED"),
        }
    }
}

impl Default for TaskState {
    fn default() -> Self {
        TaskState::Unassigned
    }
}

// ============================================================================================
// Plugin Description
// ============================================================================================

/// Description of a plugin (connector or transformation).
#[derive(Debug, Clone)]
pub struct PluginDescription {
    /// The class name of the plugin
    pub class_name: String,
    /// The type of the plugin
    pub plugin_type: PluginType,
    /// The version of the plugin
    pub version: String,
    /// The name of the plugin
    pub name: Option<String>,
}

impl PluginDescription {
    /// Create a new PluginDescription.
    pub fn new(class_name: String, plugin_type: PluginType, version: String) -> Self {
        Self {
            class_name,
            plugin_type,
            version,
            name: None,
        }
    }

    /// Set the name of the plugin.
    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }
}

/// The type of a plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginType {
    /// Source connector
    SourceConnector,
    /// Sink connector
    SinkConnector,
    /// Task
    Task,
    /// Transformation
    Transformation,
    /// Converter
    Converter,
    /// Header converter
    HeaderConverter,
    /// Predicate
    Predicate,
}

impl std::fmt::Display for PluginType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginType::SourceConnector => write!(f, "source-connector"),
            PluginType::SinkConnector => write!(f, "sink-connector"),
            PluginType::Task => write!(f, "task"),
            PluginType::Transformation => write!(f, "transformation"),
            PluginType::Converter => write!(f, "converter"),
            PluginType::HeaderConverter => write!(f, "header-converter"),
            PluginType::Predicate => write!(f, "predicate"),
        }
    }
}

// ============================================================================================
// Restart Request / Plan
// ============================================================================================

/// Request to restart a connector or task.
#[derive(Debug, Clone)]
pub struct RestartRequest {
    /// The name of the connector
    pub connector: Option<String>,
    /// The task id (if restarting a specific task)
    pub task: Option<TaskId>,
    /// Whether to restart even if the connector/task is not running
    pub force: bool,
    /// Reason for the restart
    pub reason: Option<String>,
}

impl RestartRequest {
    /// Create a request to restart a connector.
    pub fn for_connector(connector: String) -> Self {
        Self {
            connector: Some(connector),
            task: None,
            force: false,
            reason: None,
        }
    }

    /// Create a request to restart a specific task.
    pub fn for_task(task: TaskId) -> Self {
        Self {
            connector: None,
            task: Some(task),
            force: false,
            reason: None,
        }
    }

    /// Create a request to restart with force.
    pub fn with_force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    /// Set the reason for the restart.
    pub fn with_reason(mut self, reason: String) -> Self {
        self.reason = Some(reason);
        self
    }
}

/// Plan for restarting connectors or tasks.
#[derive(Debug, Clone)]
pub struct RestartPlan {
    /// The connectors to restart
    pub connectors: Vec<String>,
    /// The tasks to restart
    pub tasks: Vec<TaskId>,
    /// Whether to restart all
    pub restart_all: bool,
}

impl RestartPlan {
    /// Create a new RestartPlan.
    pub fn new() -> Self {
        Self {
            connectors: vec![],
            tasks: vec![],
            restart_all: false,
        }
    }

    /// Create a plan that restarts everything.
    pub fn restart_all() -> Self {
        Self {
            connectors: vec![],
            tasks: vec![],
            restart_all: true,
        }
    }

    /// Add a connector to restart.
    pub fn add_connector(mut self, connector: String) -> Self {
        self.connectors.push(connector);
        self
    }

    /// Add a task to restart.
    pub fn add_task(mut self, task: TaskId) -> Self {
        self.tasks.push(task);
        self
    }
}

impl Default for RestartPlan {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================================
// Connector Offsets
// ============================================================================================

/// Offset format version for connector-managed offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetFormatVersion {
    /// Version 1: offset values as strings
    Version1,
    /// Version 2: offset values with type information
    Version2,
}

impl Default for OffsetFormatVersion {
    fn default() -> Self {
        OffsetFormatVersion::Version1
    }
}

/// Connector offsets information.
#[derive(Debug)]
pub struct ConnectorOffsets {
    /// The connector name
    pub connector_name: String,
    /// The offsets
    pub offsets: Vec<ConnectorOffset>,
    /// The offset format version
    pub offset_format_version: OffsetFormatVersion,
}

impl Clone for ConnectorOffsets {
    fn clone(&self) -> Self {
        Self {
            connector_name: self.connector_name.clone(),
            offsets: vec![], // Cannot clone Vec<ConnectorOffset> because ConnectorOffset contains Box<dyn Any>
            offset_format_version: self.offset_format_version,
        }
    }
}

impl ConnectorOffsets {
    /// Create a new ConnectorOffsets.
    pub fn new(
        connector_name: String,
        offsets: Vec<ConnectorOffset>,
        offset_format_version: OffsetFormatVersion,
    ) -> Self {
        Self {
            connector_name,
            offsets,
            offset_format_version,
        }
    }
}

/// A single connector offset entry.
#[derive(Debug)]
pub struct ConnectorOffset {
    /// The partition identifier
    pub partition: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    /// The offset value
    pub offset: Option<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>,
}

impl ConnectorOffset {
    /// Create a new ConnectorOffset
    pub fn new(partition: HashMap<String, Box<dyn std::any::Any + Send + Sync>>) -> Self {
        Self {
            partition,
            offset: None,
        }
    }

    /// Set the offset
    pub fn with_offset(
        mut self,
        offset: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    ) -> Self {
        self.offset = Some(offset);
        self
    }
}

// ============================================================================================
// Abstract Status
// ============================================================================================

/// Abstract status for connectors and tasks.
#[derive(Debug, Clone)]
pub struct AbstractStatus<T> {
    /// The id of the connector/task
    pub id: T,
    /// The state
    pub state: String,
    /// The worker id
    pub worker_id: String,
    /// Timestamp of the last state change
    pub trace: Option<String>,
}

impl<T> AbstractStatus<T> {
    /// Create a new AbstractStatus.
    pub fn new(id: T, state: String, worker_id: String) -> Self {
        Self {
            id,
            state,
            worker_id,
            trace: None,
        }
    }

    /// Set the trace message.
    pub fn with_trace(mut self, trace: String) -> Self {
        self.trace = Some(trace);
        self
    }
}

// ============================================================================================
// Connector Health
// ============================================================================================

/// Health information for a connector.
#[derive(Debug, Clone)]
pub struct ConnectorHealth {
    /// The connector name
    pub connector_name: String,
    /// The connector type
    pub connector_type: ConnectorType,
    /// The current state
    pub state: ConnectorState,
    /// The worker id
    pub worker_id: String,
    /// The tasks information
    pub tasks: Vec<TaskState>,
    /// Error trace (if failed)
    pub error_trace: Option<String>,
}

impl ConnectorHealth {
    /// Create a new ConnectorHealth.
    pub fn new(
        connector_name: String,
        connector_type: ConnectorType,
        state: ConnectorState,
        worker_id: String,
    ) -> Self {
        Self {
            connector_name,
            connector_type,
            state,
            worker_id,
            tasks: vec![],
            error_trace: None,
        }
    }

    /// Add a task state.
    pub fn add_task(mut self, task_state: TaskState) -> Self {
        self.tasks.push(task_state);
        self
    }

    /// Set the error trace.
    pub fn with_error_trace(mut self, error_trace: String) -> Self {
        self.error_trace = Some(error_trace);
        self
    }
}

// ============================================================================================
// Abstract State
// ============================================================================================

/// Abstract state for connectors.
#[derive(Debug, Clone)]
pub struct AbstractState {
    /// The state
    pub state: ConnectorState,
    /// The worker id
    pub worker_id: Option<String>,
}

impl AbstractState {
    /// Create a new AbstractState.
    pub fn new(state: ConnectorState, worker_id: Option<String>) -> Self {
        Self { state, worker_id }
    }
}

impl Default for AbstractState {
    fn default() -> Self {
        Self {
            state: ConnectorState::Unassigned,
            worker_id: None,
        }
    }
}

// ============================================================================================
// Connect Record (base trait)
// ============================================================================================

use crate::data::Headers;

/// ConnectRecord is the base trait for records in Kafka Connect.
pub trait ConnectRecord<T>: Send + Sync {
    /// Returns the topic.
    fn topic(&self) -> &str;

    /// Returns the Kafka partition.
    fn kafka_partition(&self) -> Option<i32>;

    /// Returns the key.
    fn key(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>>;

    /// Returns the value.
    fn value(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>>;

    /// Returns the timestamp.
    fn timestamp(&self) -> Option<i64>;

    /// Returns the headers.
    fn headers(&self) -> Arc<dyn Headers>;
}
