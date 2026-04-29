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

//! Connect protocol module for Kafka Connect distributed runtime.
//!
//! This module implements the protocol for Kafka Connect workers in a group.
//! It includes:
//! - `ConnectProtocolCompatibility`: Enumeration of protocol compatibility modes
//! - `ConnectProtocol`: Protocol implementation with serialization/deserialization
//! - `WorkerState`: Captures the deserialized form of a worker's metadata
//! - `Assignment`: Represents assignment of connectors and tasks to workers
//!
//! Corresponds to Java classes:
//! - `org.apache.kafka.connect.runtime.distributed.ConnectProtocol`
//! - `org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility`

use std::collections::HashMap;
use std::fmt;

/// Key name constants for protocol serialization (V0)
pub const VERSION_KEY_NAME: &str = "version";
pub const URL_KEY_NAME: &str = "url";
pub const CONFIG_OFFSET_KEY_NAME: &str = "config-offset";
pub const CONNECTOR_KEY_NAME: &str = "connector";
pub const LEADER_KEY_NAME: &str = "leader";
pub const LEADER_URL_KEY_NAME: &str = "leader-url";
pub const ERROR_KEY_NAME: &str = "error";
pub const TASKS_KEY_NAME: &str = "tasks";
pub const ASSIGNMENT_KEY_NAME: &str = "assignment";

/// Sentinel task ID indicating the connector itself (not a task)
pub const CONNECTOR_TASK: i32 = -1;

/// Protocol version constants
pub const CONNECT_PROTOCOL_V0: i16 = 0;
pub const CONNECT_PROTOCOL_V1: i16 = 1;
pub const CONNECT_PROTOCOL_V2: i16 = 2;

/// An enumeration of the modes available to the worker to signal which Connect protocols are
/// enabled at any time.
///
/// `EAGER` signifies that this worker only supports prompt release of assigned connectors
/// and tasks in every rebalance. Corresponds to Connect protocol V0.
///
/// `COMPATIBLE` signifies that this worker supports both eager and incremental cooperative
/// Connect protocols and will use the version that is elected by the Kafka broker coordinator
/// during rebalance.
///
/// `SESSIONED` signifies that this worker supports all of the above protocols in addition to
/// a protocol that uses incremental cooperative rebalancing for worker assignment and uses session
/// keys distributed via the config topic to verify internal REST requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectProtocolCompatibility {
    /// Eager protocol - prompt release of connectors and tasks in every rebalance (V0)
    Eager,
    /// Compatible protocol - supports both eager and incremental cooperative (V1)
    Compatible,
    /// Sessioned protocol - supports session key verification (V2)
    Sessioned,
}

impl ConnectProtocolCompatibility {
    /// Returns the protocol name for this compatibility mode.
    ///
    /// # Returns
    /// The protocol name string used in ProtocolMetadata
    pub fn protocol(&self) -> &'static str {
        match self {
            ConnectProtocolCompatibility::Eager => "default",
            ConnectProtocolCompatibility::Compatible => "compatible",
            ConnectProtocolCompatibility::Sessioned => "sessioned",
        }
    }

    /// Returns the protocol version for this compatibility mode.
    ///
    /// # Returns
    /// The protocol version number (i16)
    pub fn protocol_version(&self) -> i16 {
        match self {
            ConnectProtocolCompatibility::Eager => CONNECT_PROTOCOL_V0,
            ConnectProtocolCompatibility::Compatible => CONNECT_PROTOCOL_V1,
            ConnectProtocolCompatibility::Sessioned => CONNECT_PROTOCOL_V2,
        }
    }

    /// Returns the enum that corresponds to the name that is given as an argument.
    /// If no mapping is found, `IllegalArgumentException` is thrown (panics in Rust).
    ///
    /// # Arguments
    /// * `name` - The name of the protocol compatibility mode
    ///
    /// # Returns
    /// The enum that corresponds to the protocol compatibility mode
    ///
    /// # Panics
    /// Panics if the name is not recognized
    pub fn compatibility(name: &str) -> ConnectProtocolCompatibility {
        match name.to_uppercase().as_str() {
            "EAGER" => ConnectProtocolCompatibility::Eager,
            "COMPATIBLE" => ConnectProtocolCompatibility::Compatible,
            "SESSIONED" => ConnectProtocolCompatibility::Sessioned,
            _ => panic!("Unknown Connect protocol compatibility mode: {}", name),
        }
    }

    /// Returns the enum that corresponds to the Connect protocol version.
    /// If no mapping is found, panics.
    ///
    /// # Arguments
    /// * `protocol_version` - The version of the protocol
    ///
    /// # Returns
    /// The enum that corresponds to the protocol compatibility mode
    ///
    /// # Panics
    /// Panics if the protocol version is not recognized
    pub fn from_protocol_version(protocol_version: i16) -> ConnectProtocolCompatibility {
        match protocol_version {
            CONNECT_PROTOCOL_V0 => ConnectProtocolCompatibility::Eager,
            CONNECT_PROTOCOL_V1 => ConnectProtocolCompatibility::Compatible,
            CONNECT_PROTOCOL_V2 => ConnectProtocolCompatibility::Sessioned,
            _ => panic!("Unknown Connect protocol version: {}", protocol_version),
        }
    }

    /// Returns the enum that corresponds to the protocol name.
    /// If no mapping is found, panics.
    ///
    /// # Arguments
    /// * `protocol_name` - The name of the connect protocol
    ///
    /// # Returns
    /// The enum that corresponds to the protocol compatibility mode
    ///
    /// # Panics
    /// Panics if the protocol name is not recognized
    pub fn from_protocol(protocol_name: &str) -> ConnectProtocolCompatibility {
        match protocol_name.to_lowercase().as_str() {
            "default" => ConnectProtocolCompatibility::Eager,
            "compatible" => ConnectProtocolCompatibility::Compatible,
            "sessioned" => ConnectProtocolCompatibility::Sessioned,
            _ => panic!(
                "Not found Connect protocol compatibility mode for protocol: {}",
                protocol_name
            ),
        }
    }
}

impl fmt::Display for ConnectProtocolCompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectProtocolCompatibility::Eager => write!(f, "eager"),
            ConnectProtocolCompatibility::Compatible => write!(f, "compatible"),
            ConnectProtocolCompatibility::Sessioned => write!(f, "sessioned"),
        }
    }
}

/// A class that captures the deserialized form of a worker's metadata.
///
/// This represents the state of a worker including its URL and the most
/// up-to-date configuration offset known to it.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerState {
    /// The URL of the worker
    url: String,
    /// The most up-to-date (maximum) configuration offset known to this worker
    offset: i64,
}

impl WorkerState {
    /// Creates a new WorkerState with the given URL and config offset.
    ///
    /// # Arguments
    /// * `url` - The worker's URL
    /// * `offset` - The configuration offset
    pub fn new(url: String, offset: i64) -> Self {
        WorkerState { url, offset }
    }

    /// Returns the worker's URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the most up-to-date (maximum) configuration offset known to this worker.
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl fmt::Display for WorkerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WorkerState{{url='{}', offset={}}}",
            self.url, self.offset
        )
    }
}

/// Connector task ID identifying a specific task for a connector.
/// Corresponds to `org.apache.kafka.connect.util.ConnectorTaskId` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectorTaskId {
    /// The connector name
    connector: String,
    /// The task ID (task number)
    task: i32,
}

impl ConnectorTaskId {
    /// Creates a new ConnectorTaskId.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    /// * `task` - The task ID
    pub fn new(connector: String, task: i32) -> Self {
        ConnectorTaskId { connector, task }
    }

    /// Returns the connector name.
    pub fn connector(&self) -> &str {
        &self.connector
    }

    /// Returns the task ID.
    pub fn task(&self) -> i32 {
        self.task
    }
}

impl fmt::Display for ConnectorTaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.connector, self.task)
    }
}

/// Error code constant: no error during assignment
pub const ASSIGNMENT_NO_ERROR: i16 = 0;
/// Error code constant: configuration offsets mismatched in a way that the leader could not resolve
pub const ASSIGNMENT_CONFIG_MISMATCH: i16 = 1;

/// The basic assignment of connectors and tasks introduced with V0 version of the Connect protocol.
///
/// This represents the assignment of connectors and tasks to a worker, including:
/// - Error code (0 for success)
/// - Leader information
/// - Configuration offset
/// - Assigned connector IDs and task IDs
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// Error code for this assignment
    error: i16,
    /// Connect group's leader ID
    leader: String,
    /// Connect group's leader URL
    leader_url: String,
    /// Most up-to-date configuration offset
    offset: i64,
    /// List of connectors that the worker should instantiate and run
    connector_ids: Vec<String>,
    /// List of task IDs that the worker should instantiate and run
    task_ids: Vec<ConnectorTaskId>,
}

impl Assignment {
    /// Creates a new Assignment indicating responsibility for the given connector instances and task IDs.
    ///
    /// # Arguments
    /// * `error` - Error code for this assignment; NO_ERROR (0) indicates no error
    /// * `leader` - Connect group's leader ID; may be empty for empty assignment
    /// * `leader_url` - Connect group's leader URL; may be empty for empty assignment
    /// * `config_offset` - The most up-to-date configuration offset
    /// * `connector_ids` - List of connectors to instantiate; must not be null
    /// * `task_ids` - List of task IDs to instantiate; must not be null
    pub fn new(
        error: i16,
        leader: String,
        leader_url: String,
        config_offset: i64,
        connector_ids: Vec<String>,
        task_ids: Vec<ConnectorTaskId>,
    ) -> Self {
        Assignment {
            error,
            leader,
            leader_url,
            offset: config_offset,
            connector_ids,
            task_ids,
        }
    }

    /// Returns the error code of this assignment.
    /// 0 signals successful assignment (NO_ERROR).
    pub fn error(&self) -> i16 {
        self.error
    }

    /// Returns the ID of the leader Connect worker in this assignment.
    pub fn leader(&self) -> &str {
        &self.leader
    }

    /// Returns the URL to which the leader accepts requests from other members.
    pub fn leader_url(&self) -> &str {
        &self.leader_url
    }

    /// Returns true if this assignment failed.
    pub fn failed(&self) -> bool {
        self.error != ASSIGNMENT_NO_ERROR
    }

    /// Returns the most up-to-date offset in the configuration topic.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the connectors included in this assignment.
    pub fn connectors(&self) -> &[String] {
        &self.connector_ids
    }

    /// Returns the tasks included in this assignment.
    pub fn tasks(&self) -> &[ConnectorTaskId] {
        &self.task_ids
    }

    /// Converts this assignment to a map of connector to task IDs.
    /// Uses CONNECTOR_TASK sentinel (-1) to indicate the connector itself.
    fn as_map(&self) -> HashMap<String, Vec<i32>> {
        let mut task_map: HashMap<String, Vec<i32>> = HashMap::new();

        // Add connector assignments with CONNECTOR_TASK sentinel
        for connector_id in &self.connector_ids {
            task_map
                .entry(connector_id.clone())
                .or_insert_with(Vec::new)
                .push(CONNECTOR_TASK);
        }

        // Add task assignments
        for task_id in &self.task_ids {
            task_map
                .entry(task_id.connector.clone())
                .or_insert_with(Vec::new)
                .push(task_id.task);
        }

        task_map
    }
}

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Assignment{{error={}, leader='{}', leaderUrl='{}', offset={}, connectorIds={:?}, taskIds={:?}}}",
            self.error,
            self.leader,
            self.leader_url,
            self.offset,
            self.connector_ids,
            self.task_ids
        )
    }
}

/// ConnectProtocol implements the protocol for Kafka Connect workers in a group.
///
/// This class provides serialization and deserialization for:
/// - Worker metadata (WorkerState)
/// - Worker assignments (Assignment)
///
/// All serialization follows the V0 protocol format.
pub struct ConnectProtocol;

impl ConnectProtocol {
    /// Returns the collection of Connect protocols that are supported by this version
    /// along with their serialized metadata. The V0 protocol only supports EAGER mode.
    ///
    /// # Arguments
    /// * `worker_state` - The current state of the worker metadata
    ///
    /// # Returns
    /// A vector containing a single tuple (protocol_name, metadata_bytes) for EAGER mode
    pub fn metadata_request(worker_state: &WorkerState) -> Vec<(String, Vec<u8>)> {
        let metadata = Self::serialize_metadata(worker_state);
        vec![(
            ConnectProtocolCompatibility::Eager.protocol().to_string(),
            metadata,
        )]
    }

    /// Serializes worker state metadata to a byte buffer.
    ///
    /// Protocol V0 format:
    /// ```text
    /// Version            => Int16
    /// Url                => String
    /// ConfigOffset       => Int64
    /// ```
    ///
    /// # Arguments
    /// * `worker_state` - The current state of the worker metadata
    ///
    /// # Returns
    /// Serialized byte buffer containing the worker metadata
    pub fn serialize_metadata(worker_state: &WorkerState) -> Vec<u8> {
        // V0 header: version (i16)
        // V0 config state: url (string) + offset (i64)
        let url_bytes = worker_state.url.as_bytes();
        let url_len = url_bytes.len() as i32;

        // Calculate total size: version(2) + url_len(4) + url_bytes + offset(8)
        let total_size = 2 + 4 + url_bytes.len() + 8;

        let mut buffer = Vec::with_capacity(total_size);

        // Write version (i16)
        buffer.extend_from_slice(&CONNECT_PROTOCOL_V0.to_be_bytes());

        // Write url length (i32) and url bytes
        buffer.extend_from_slice(&url_len.to_be_bytes());
        buffer.extend_from_slice(url_bytes);

        // Write config offset (i64)
        buffer.extend_from_slice(&worker_state.offset.to_be_bytes());

        buffer
    }

    /// Deserializes worker metadata from a byte buffer.
    ///
    /// # Arguments
    /// * `buffer` - A buffer containing the serialized protocol metadata
    ///
    /// # Returns
    /// The deserialized WorkerState
    ///
    /// # Panics
    /// Panics if the protocol version is incompatible or buffer is malformed
    pub fn deserialize_metadata(buffer: &[u8]) -> WorkerState {
        // Read version (i16)
        let version = i16::from_be_bytes([buffer[0], buffer[1]]);
        Self::check_version_compatibility(version);

        // Read url length (i32) at offset 2
        let url_len = i32::from_be_bytes([buffer[2], buffer[3], buffer[4], buffer[5]]) as usize;
        let url_start = 6;
        let url_end = url_start + url_len;

        // Read url bytes
        let url = String::from_utf8(buffer[url_start..url_end].to_vec())
            .expect("Invalid UTF-8 in worker URL");

        // Read config offset (i64) at offset after url
        let offset_start = url_end;
        let offset = i64::from_be_bytes([
            buffer[offset_start],
            buffer[offset_start + 1],
            buffer[offset_start + 2],
            buffer[offset_start + 3],
            buffer[offset_start + 4],
            buffer[offset_start + 5],
            buffer[offset_start + 6],
            buffer[offset_start + 7],
        ]);

        WorkerState::new(url, offset)
    }

    /// Serializes an assignment to a byte buffer.
    ///
    /// Protocol V0 format:
    /// ```text
    /// Version            => Int16
    /// Error              => Int16
    /// Leader             => String
    /// LeaderUrl          => String
    /// ConfigOffset       => Int64
    /// Assignment         => [Connector Assignment]
    /// ```
    ///
    /// Connector Assignment:
    /// ```text
    /// Connector          => String
    /// Tasks              => [Int32]
    /// ```
    ///
    /// # Arguments
    /// * `assignment` - The assignment to serialize
    ///
    /// # Returns
    /// Serialized byte buffer containing the assignment
    pub fn serialize_assignment(assignment: &Assignment) -> Vec<u8> {
        let mut buffer = Vec::new();

        // Write version (i16)
        buffer.extend_from_slice(&CONNECT_PROTOCOL_V0.to_be_bytes());

        // Write error (i16)
        buffer.extend_from_slice(&assignment.error.to_be_bytes());

        // Write leader (string)
        let leader_bytes = assignment.leader.as_bytes();
        let leader_len = leader_bytes.len() as i32;
        buffer.extend_from_slice(&leader_len.to_be_bytes());
        buffer.extend_from_slice(leader_bytes);

        // Write leader_url (string)
        let leader_url_bytes = assignment.leader_url.as_bytes();
        let leader_url_len = leader_url_bytes.len() as i32;
        buffer.extend_from_slice(&leader_url_len.to_be_bytes());
        buffer.extend_from_slice(leader_url_bytes);

        // Write config offset (i64)
        buffer.extend_from_slice(&assignment.offset.to_be_bytes());

        // Write assignment array
        let task_map = assignment.as_map();
        let assignment_count = task_map.len() as i32;
        buffer.extend_from_slice(&assignment_count.to_be_bytes());

        for (connector, tasks) in &task_map {
            // Write connector name (string)
            let connector_bytes = connector.as_bytes();
            let connector_len = connector_bytes.len() as i32;
            buffer.extend_from_slice(&connector_len.to_be_bytes());
            buffer.extend_from_slice(connector_bytes);

            // Write tasks array
            let tasks_count = tasks.len() as i32;
            buffer.extend_from_slice(&tasks_count.to_be_bytes());
            for task_id in tasks {
                buffer.extend_from_slice(&task_id.to_be_bytes());
            }
        }

        buffer
    }

    /// Deserializes an assignment from a byte buffer.
    ///
    /// # Arguments
    /// * `buffer` - A buffer containing the serialized assignment
    ///
    /// # Returns
    /// The deserialized Assignment
    ///
    /// # Panics
    /// Panics if the protocol version is incompatible or buffer is malformed
    pub fn deserialize_assignment(buffer: &[u8]) -> Assignment {
        // Read version (i16)
        let version = i16::from_be_bytes([buffer[0], buffer[1]]);
        Self::check_version_compatibility(version);

        // Read error (i16) at offset 2
        let error = i16::from_be_bytes([buffer[2], buffer[3]]);

        // Read leader (string) at offset 4
        let (leader, offset_after_leader) = Self::read_string(&buffer[4..]);

        // Read leader_url (string)
        let (leader_url, offset_after_leader_url) =
            Self::read_string(&buffer[4 + offset_after_leader..]);
        let leader_url_start = 4 + offset_after_leader;

        // Read config offset (i64)
        let offset_start = 4 + offset_after_leader + offset_after_leader_url;
        let config_offset = i64::from_be_bytes([
            buffer[offset_start],
            buffer[offset_start + 1],
            buffer[offset_start + 2],
            buffer[offset_start + 3],
            buffer[offset_start + 4],
            buffer[offset_start + 5],
            buffer[offset_start + 6],
            buffer[offset_start + 7],
        ]);

        // Read assignment array
        let assignment_start = offset_start + 8;
        let assignment_count = i32::from_be_bytes([
            buffer[assignment_start],
            buffer[assignment_start + 1],
            buffer[assignment_start + 2],
            buffer[assignment_start + 3],
        ]) as usize;

        let mut connector_ids: Vec<String> = Vec::new();
        let mut task_ids: Vec<ConnectorTaskId> = Vec::new();

        let mut pos = assignment_start + 4;
        for _ in 0..assignment_count {
            // Read connector name
            let (connector, connector_read_len) = Self::read_string(&buffer[pos..]);
            pos += connector_read_len;

            // Read tasks array count
            let tasks_count = i32::from_be_bytes([
                buffer[pos],
                buffer[pos + 1],
                buffer[pos + 2],
                buffer[pos + 3],
            ]) as usize;
            pos += 4;

            // Read each task ID
            for _ in 0..tasks_count {
                let task_id_val = i32::from_be_bytes([
                    buffer[pos],
                    buffer[pos + 1],
                    buffer[pos + 2],
                    buffer[pos + 3],
                ]);
                pos += 4;

                if task_id_val == CONNECTOR_TASK {
                    connector_ids.push(connector.clone());
                } else {
                    task_ids.push(ConnectorTaskId::new(connector.clone(), task_id_val));
                }
            }
        }

        Assignment::new(
            error,
            leader,
            leader_url,
            config_offset,
            connector_ids,
            task_ids,
        )
    }

    /// Reads a string from a buffer starting at offset 0.
    /// Returns the string and the number of bytes read.
    fn read_string(buffer: &[u8]) -> (String, usize) {
        let len = i32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        let string_bytes = &buffer[4..4 + len];
        let string = String::from_utf8(string_bytes.to_vec()).expect("Invalid UTF-8 string");
        (string, 4 + len)
    }

    /// Checks if the protocol version is compatible.
    ///
    /// # Panics
    /// Panics if the version is incompatible
    fn check_version_compatibility(version: i16) {
        if version < CONNECT_PROTOCOL_V0 {
            panic!("Unsupported subscription version: {}", version);
        }
        // Assume versions can be parsed as V0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_protocol_compatibility_protocol() {
        assert_eq!(ConnectProtocolCompatibility::Eager.protocol(), "default");
        assert_eq!(
            ConnectProtocolCompatibility::Compatible.protocol(),
            "compatible"
        );
        assert_eq!(
            ConnectProtocolCompatibility::Sessioned.protocol(),
            "sessioned"
        );
    }

    #[test]
    fn test_connect_protocol_compatibility_version() {
        assert_eq!(
            ConnectProtocolCompatibility::Eager.protocol_version(),
            CONNECT_PROTOCOL_V0
        );
        assert_eq!(
            ConnectProtocolCompatibility::Compatible.protocol_version(),
            CONNECT_PROTOCOL_V1
        );
        assert_eq!(
            ConnectProtocolCompatibility::Sessioned.protocol_version(),
            CONNECT_PROTOCOL_V2
        );
    }

    #[test]
    fn test_connect_protocol_compatibility_from_name() {
        assert_eq!(
            ConnectProtocolCompatibility::compatibility("EAGER"),
            ConnectProtocolCompatibility::Eager
        );
        assert_eq!(
            ConnectProtocolCompatibility::compatibility("eager"),
            ConnectProtocolCompatibility::Eager
        );
        assert_eq!(
            ConnectProtocolCompatibility::compatibility("COMPATIBLE"),
            ConnectProtocolCompatibility::Compatible
        );
        assert_eq!(
            ConnectProtocolCompatibility::compatibility("SESSIONED"),
            ConnectProtocolCompatibility::Sessioned
        );
    }

    #[test]
    fn test_connect_protocol_compatibility_from_version() {
        assert_eq!(
            ConnectProtocolCompatibility::from_protocol_version(0),
            ConnectProtocolCompatibility::Eager
        );
        assert_eq!(
            ConnectProtocolCompatibility::from_protocol_version(1),
            ConnectProtocolCompatibility::Compatible
        );
        assert_eq!(
            ConnectProtocolCompatibility::from_protocol_version(2),
            ConnectProtocolCompatibility::Sessioned
        );
    }

    #[test]
    fn test_connect_protocol_compatibility_from_protocol_name() {
        assert_eq!(
            ConnectProtocolCompatibility::from_protocol("default"),
            ConnectProtocolCompatibility::Eager
        );
        assert_eq!(
            ConnectProtocolCompatibility::from_protocol("compatible"),
            ConnectProtocolCompatibility::Compatible
        );
        assert_eq!(
            ConnectProtocolCompatibility::from_protocol("sessioned"),
            ConnectProtocolCompatibility::Sessioned
        );
    }

    #[test]
    fn test_worker_state() {
        let state = WorkerState::new("http://localhost:8083".to_string(), 100);
        assert_eq!(state.url(), "http://localhost:8083");
        assert_eq!(state.offset(), 100);
    }

    #[test]
    fn test_connector_task_id() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 5);
        assert_eq!(task_id.connector(), "test-connector");
        assert_eq!(task_id.task(), 5);
        assert_eq!(format!("{}", task_id), "test-connector-5");
    }

    #[test]
    fn test_assignment() {
        let connector_ids = vec!["connector1".to_string(), "connector2".to_string()];
        let task_ids = vec![
            ConnectorTaskId::new("connector1".to_string(), 0),
            ConnectorTaskId::new("connector1".to_string(), 1),
        ];

        let assignment = Assignment::new(
            ASSIGNMENT_NO_ERROR,
            "worker1".to_string(),
            "http://worker1:8083".to_string(),
            100,
            connector_ids,
            task_ids,
        );

        assert_eq!(assignment.error(), ASSIGNMENT_NO_ERROR);
        assert_eq!(assignment.leader(), "worker1");
        assert_eq!(assignment.leader_url(), "http://worker1:8083");
        assert_eq!(assignment.offset(), 100);
        assert!(!assignment.failed());
        assert_eq!(assignment.connectors().len(), 2);
        assert_eq!(assignment.tasks().len(), 2);
    }

    #[test]
    fn test_assignment_failed() {
        let assignment = Assignment::new(
            ASSIGNMENT_CONFIG_MISMATCH,
            "".to_string(),
            "".to_string(),
            0,
            Vec::new(),
            Vec::new(),
        );
        assert!(assignment.failed());
    }

    #[test]
    fn test_serialize_deserialize_metadata() {
        let worker_state = WorkerState::new("http://localhost:8083".to_string(), 12345);
        let serialized = ConnectProtocol::serialize_metadata(&worker_state);
        let deserialized = ConnectProtocol::deserialize_metadata(&serialized);

        assert_eq!(deserialized.url(), worker_state.url());
        assert_eq!(deserialized.offset(), worker_state.offset());
    }

    #[test]
    fn test_serialize_deserialize_assignment() {
        let connector_ids = vec!["connector1".to_string()];
        let task_ids = vec![
            ConnectorTaskId::new("connector1".to_string(), 0),
            ConnectorTaskId::new("connector1".to_string(), 1),
        ];

        let assignment = Assignment::new(
            ASSIGNMENT_NO_ERROR,
            "leader1".to_string(),
            "http://leader1:8083".to_string(),
            500,
            connector_ids,
            task_ids,
        );

        let serialized = ConnectProtocol::serialize_assignment(&assignment);
        let deserialized = ConnectProtocol::deserialize_assignment(&serialized);

        assert_eq!(deserialized.error(), assignment.error());
        assert_eq!(deserialized.leader(), assignment.leader());
        assert_eq!(deserialized.leader_url(), assignment.leader_url());
        assert_eq!(deserialized.offset(), assignment.offset());
        assert_eq!(
            deserialized.connectors().len(),
            assignment.connectors().len()
        );
        assert_eq!(deserialized.tasks().len(), assignment.tasks().len());
    }

    #[test]
    fn test_assignment_as_map() {
        let connector_ids = vec!["connector1".to_string()];
        let task_ids = vec![
            ConnectorTaskId::new("connector1".to_string(), 0),
            ConnectorTaskId::new("connector1".to_string(), 1),
        ];

        let assignment = Assignment::new(
            ASSIGNMENT_NO_ERROR,
            "".to_string(),
            "".to_string(),
            0,
            connector_ids,
            task_ids,
        );

        let map = assignment.as_map();
        assert!(map.contains_key("connector1"));
        let tasks = map.get("connector1").unwrap();
        assert!(tasks.contains(&CONNECTOR_TASK)); // connector itself
        assert!(tasks.contains(&0)); // task 0
        assert!(tasks.contains(&1)); // task 1
    }
}
