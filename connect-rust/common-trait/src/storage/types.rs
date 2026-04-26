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

//! Basic types needed for storage backing store traits.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Unique ID for a single task.
/// Includes a unique connector name and a task ID that is unique within the connector.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConnectorTaskId {
    pub connector: String,
    pub task: u32,
}

impl ConnectorTaskId {
    pub fn new(connector: String, task: u32) -> Self {
        Self { connector, task }
    }

    pub fn connector(&self) -> &str {
        &self.connector
    }

    pub fn task(&self) -> u32 {
        self.task
    }
}

impl fmt::Display for ConnectorTaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.connector, self.task)
    }
}

/// The state of a connector or task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum State {
    Unassigned,
    Running,
    Paused,
    Failed,
    Destroyed,
    Restarting,
    Stopped,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Unassigned => write!(f, "UNASSIGNED"),
            State::Running => write!(f, "RUNNING"),
            State::Paused => write!(f, "PAUSED"),
            State::Failed => write!(f, "FAILED"),
            State::Destroyed => write!(f, "DESTROYED"),
            State::Restarting => write!(f, "RESTARTING"),
            State::Stopped => write!(f, "STOPPED"),
        }
    }
}

/// Target state for a connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TargetState {
    Started,
    Stopped,
    Paused,
}

impl Default for TargetState {
    fn default() -> Self {
        TargetState::Started
    }
}

impl fmt::Display for TargetState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TargetState::Started => write!(f, "STARTED"),
            TargetState::Stopped => write!(f, "STOPPED"),
            TargetState::Paused => write!(f, "PAUSED"),
        }
    }
}

/// Session key for inter-worker communication validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionKey {
    pub key: String,
    pub creation_timestamp: i64,
}

impl SessionKey {
    pub fn new(key: String, creation_timestamp: i64) -> Self {
        Self {
            key,
            creation_timestamp,
        }
    }
}

/// Request to restart a connector and optionally its tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartRequest {
    pub connector: String,
    pub only_failed: bool,
    pub include_tasks: bool,
}

impl RestartRequest {
    pub fn new(connector: String, only_failed: bool, include_tasks: bool) -> Self {
        Self {
            connector,
            only_failed,
            include_tasks,
        }
    }

    pub fn connector(&self) -> &str {
        &self.connector
    }

    pub fn only_failed(&self) -> bool {
        self.only_failed
    }

    pub fn include_tasks(&self) -> bool {
        self.include_tasks
    }
}

/// Status of a connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorStatus {
    pub id: String,
    pub state: State,
    pub worker_id: String,
    pub generation: u32,
    pub trace: Option<String>,
    pub version: Option<String>,
}

impl ConnectorStatus {
    pub fn new(
        id: String,
        state: State,
        worker_id: String,
        generation: u32,
        trace: Option<String>,
        version: Option<String>,
    ) -> Self {
        Self {
            id,
            state,
            worker_id,
            generation,
            trace,
            version,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn trace(&self) -> Option<&str> {
        self.trace.as_deref()
    }

    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

/// Status of a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStatus {
    pub id: ConnectorTaskId,
    pub state: State,
    pub worker_id: String,
    pub generation: u32,
    pub trace: Option<String>,
    pub version: Option<String>,
}

impl TaskStatus {
    pub fn new(
        id: ConnectorTaskId,
        state: State,
        worker_id: String,
        generation: u32,
        trace: Option<String>,
        version: Option<String>,
    ) -> Self {
        Self {
            id,
            state,
            worker_id,
            generation,
            trace,
            version,
        }
    }

    pub fn id(&self) -> &ConnectorTaskId {
        &self.id
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn trace(&self) -> Option<&str> {
        self.trace.as_deref()
    }

    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

/// Status of a topic used by a connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicStatus {
    pub connector: String,
    pub topic: String,
    pub state: State,
}

impl TopicStatus {
    pub fn new(connector: String, topic: String, state: State) -> Self {
        Self {
            connector,
            topic,
            state,
        }
    }

    pub fn connector(&self) -> &str {
        &self.connector
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn state(&self) -> State {
        self.state
    }
}

/// An immutable snapshot of the configuration state of connectors and tasks in a Kafka Connect cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfigState {
    pub offset: i64,
    pub session_key: Option<SessionKey>,
    pub connector_task_counts: HashMap<String, u32>,
    pub connector_configs: HashMap<String, HashMap<String, String>>,
    pub connector_target_states: HashMap<String, TargetState>,
    pub task_configs: HashMap<ConnectorTaskId, HashMap<String, String>>,
    pub connector_task_count_records: HashMap<String, u32>,
    pub connector_task_config_generations: HashMap<String, u32>,
    pub applied_connector_configs: HashMap<String, HashMap<String, String>>,
    pub connectors_pending_fencing: Vec<String>,
    pub inconsistent_connectors: Vec<String>,
}

impl ClusterConfigState {
    pub const NO_OFFSET: i64 = -1;

    pub fn empty() -> Self {
        Self {
            offset: Self::NO_OFFSET,
            session_key: None,
            connector_task_counts: HashMap::new(),
            connector_configs: HashMap::new(),
            connector_target_states: HashMap::new(),
            task_configs: HashMap::new(),
            connector_task_count_records: HashMap::new(),
            connector_task_config_generations: HashMap::new(),
            applied_connector_configs: HashMap::new(),
            connectors_pending_fencing: Vec::new(),
            inconsistent_connectors: Vec::new(),
        }
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn session_key(&self) -> Option<&SessionKey> {
        self.session_key.as_ref()
    }

    pub fn contains(&self, connector: &str) -> bool {
        self.connector_configs.contains_key(connector)
    }

    pub fn connectors(&self) -> Vec<&String> {
        self.connector_configs.keys().collect()
    }

    pub fn connector_config(&self, connector: &str) -> Option<&HashMap<String, String>> {
        self.connector_configs.get(connector)
    }

    pub fn target_state(&self, connector: &str) -> Option<TargetState> {
        self.connector_target_states.get(connector).copied()
    }

    pub fn task_config(&self, task: &ConnectorTaskId) -> Option<&HashMap<String, String>> {
        self.task_configs.get(task)
    }

    pub fn task_count(&self, connector_name: &str) -> u32 {
        self.connector_task_counts
            .get(connector_name)
            .copied()
            .unwrap_or(0)
    }

    pub fn pending_fencing(&self, connector_name: &str) -> bool {
        self.connectors_pending_fencing
            .contains(&connector_name.to_string())
    }

    pub fn tasks(&self, connector_name: &str) -> Vec<ConnectorTaskId> {
        if self
            .inconsistent_connectors
            .contains(&connector_name.to_string())
        {
            return Vec::new();
        }

        let num_tasks = self
            .connector_task_counts
            .get(connector_name)
            .copied()
            .unwrap_or(0);
        let mut task_ids = Vec::with_capacity(num_tasks as usize);
        for task_index in 0..num_tasks {
            task_ids.push(ConnectorTaskId::new(connector_name.to_string(), task_index));
        }
        task_ids
    }

    pub fn task_count_record(&self, connector: &str) -> Option<u32> {
        self.connector_task_count_records.get(connector).copied()
    }

    pub fn task_config_generation(&self, connector: &str) -> Option<u32> {
        self.connector_task_config_generations
            .get(connector)
            .copied()
    }

    pub fn inconsistent_connectors(&self) -> &[String] {
        &self.inconsistent_connectors
    }
}

impl Default for ClusterConfigState {
    fn default() -> Self {
        Self::empty()
    }
}

/// Applied connector config with generation tracking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedConnectorConfig {
    pub config: HashMap<String, String>,
    pub generation: u32,
}

impl AppliedConnectorConfig {
    pub fn new(config: HashMap<String, String>, generation: u32) -> Self {
        Self { config, generation }
    }
}

/// WorkerConfig placeholder type.
/// This should be defined in connect-runtime with full configuration options.
/// For trait definition purposes, we use a minimal placeholder.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub config: HashMap<String, String>,
}

impl WorkerConfig {
    pub fn new(config: HashMap<String, String>) -> Self {
        Self { config }
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }
}
