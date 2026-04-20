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

//! This class implements a group protocol for Kafka Connect workers that support
//! incremental and cooperative rebalancing of connectors and tasks.
//!
//! It includes:
//! - The format of worker state used when joining the group and distributing assignments
//! - The format of assignments of connectors and tasks to workers
//! - Serialization and deserialization methods for both metadata and assignments
//!
//! The V1 protocol supports incremental cooperative rebalancing, which allows workers
//! to keep their current assignments during rebalance rather than releasing everything
//! and re-assigning from scratch.
//!
//! The V2 protocol is schematically identical to V1, but signifies that internal request
//! verification and distribution of session keys is enabled (KIP-507).
//!
//! Corresponds to `org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol` in Java.

use crate::distributed::connect_protocol::{
    ConnectProtocolCompatibility, ConnectorTaskId, CONNECTOR_TASK, CONNECT_PROTOCOL_V0,
    CONNECT_PROTOCOL_V1, CONNECT_PROTOCOL_V2,
};
use crate::distributed::extended_assignment::{ExtendedAssignment, CONFIG_MISMATCH, NO_ERROR};
use crate::distributed::extended_worker_state::ExtendedWorkerState;

/// Key name for allocation field in protocol serialization
pub const ALLOCATION_KEY_NAME: &str = "allocation";

/// Key name for revoked field in protocol serialization
pub const REVOKED_KEY_NAME: &str = "revoked";

/// Key name for scheduled delay field in protocol serialization
pub const SCHEDULED_DELAY_KEY_NAME: &str = "delay";

/// Whether to tolerate missing fields with defaults during serialization
pub const TOLERATE_MISSING_FIELDS_WITH_DEFAULTS: bool = true;

/// Protocol that supports incremental cooperative rebalancing for Kafka Connect workers.
///
/// This provides:
/// - Serialization/deserialization of worker metadata (ExtendedWorkerState)
/// - Serialization/deserialization of assignments (ExtendedAssignment)
/// - Protocol metadata request generation for joining groups
pub struct IncrementalCooperativeConnectProtocol;

impl IncrementalCooperativeConnectProtocol {
    /// Serializes worker state metadata to a byte buffer.
    ///
    /// Subscription V1/V2 format:
    /// ```text
    /// Version            => Int16
    /// Url                => String
    /// ConfigOffset       => Int64
    /// Current Assignment => [Byte]
    /// ```
    ///
    /// # Arguments
    /// * `worker_state` - The current state of the worker metadata
    /// * `sessioned` - Whether to use V2 (sessioned) or V1 protocol
    ///
    /// # Returns
    /// Serialized byte buffer containing the worker metadata
    pub fn serialize_metadata(worker_state: &ExtendedWorkerState, sessioned: bool) -> Vec<u8> {
        let version = if sessioned {
            CONNECT_PROTOCOL_V2
        } else {
            CONNECT_PROTOCOL_V1
        };

        // Serialize the current assignment
        let assignment_bytes = Self::serialize_assignment(worker_state.assignment(), sessioned);

        let url_bytes = worker_state.url().as_bytes();
        let url_len = url_bytes.len() as i32;

        // Calculate total size:
        // version(2) + url_len(4) + url_bytes + offset(8) + assignment
        let assignment_len = assignment_bytes.len() as i32;
        let total_size = 2 + 4 + url_bytes.len() + 8 + 4 + assignment_bytes.len();

        let mut buffer = Vec::with_capacity(total_size);

        // Write version (i16)
        buffer.extend_from_slice(&version.to_be_bytes());

        // Write url length (i32) and url bytes
        buffer.extend_from_slice(&url_len.to_be_bytes());
        buffer.extend_from_slice(url_bytes);

        // Write config offset (i64)
        buffer.extend_from_slice(&worker_state.offset().to_be_bytes());

        // Write allocation (assignment bytes)
        // The allocation is written as nullable bytes: length (i32) + bytes
        // If null, length would be -1, but we always serialize as bytes here
        buffer.extend_from_slice(&assignment_len.to_be_bytes());
        if !assignment_bytes.is_empty() {
            buffer.extend_from_slice(&assignment_bytes);
        }

        buffer
    }

    /// Returns the collection of Connect protocols that are supported by this version
    /// along with their serialized metadata. The protocols are ordered by preference.
    ///
    /// # Arguments
    /// * `worker_state` - The current state of the worker metadata
    /// * `sessioned` - Whether the SESSIONED protocol should be included
    ///
    /// # Returns
    /// A vector of tuples containing (protocol_name, metadata_bytes)
    pub fn metadata_request(
        worker_state: &ExtendedWorkerState,
        sessioned: bool,
    ) -> Vec<(String, Vec<u8>)> {
        let mut protocols = Vec::new();

        // Order matters in terms of protocol preference
        if sessioned {
            // SESSIONED protocol with V2 metadata
            let sessioned_metadata = Self::serialize_metadata(worker_state, true);
            protocols.push((
                ConnectProtocolCompatibility::Sessioned
                    .protocol()
                    .to_string(),
                sessioned_metadata,
            ));
        }

        // COMPATIBLE protocol with V1 metadata
        let compatible_metadata = Self::serialize_metadata(worker_state, false);
        protocols.push((
            ConnectProtocolCompatibility::Compatible
                .protocol()
                .to_string(),
            compatible_metadata,
        ));

        // EAGER protocol with V0 metadata (from ConnectProtocol)
        let eager_metadata = Self::serialize_metadata_v0(worker_state);
        protocols.push((
            ConnectProtocolCompatibility::Eager.protocol().to_string(),
            eager_metadata,
        ));

        protocols
    }

    /// Serializes worker state metadata using V0 protocol format (for EAGER compatibility).
    ///
    /// V0 format:
    /// ```text
    /// Version            => Int16
    /// Url                => String
    /// ConfigOffset       => Int64
    /// ```
    fn serialize_metadata_v0(worker_state: &ExtendedWorkerState) -> Vec<u8> {
        let url_bytes = worker_state.url().as_bytes();
        let url_len = url_bytes.len() as i32;

        let total_size = 2 + 4 + url_bytes.len() + 8;
        let mut buffer = Vec::with_capacity(total_size);

        // Write version (i16)
        buffer.extend_from_slice(&CONNECT_PROTOCOL_V0.to_be_bytes());

        // Write url length (i32) and url bytes
        buffer.extend_from_slice(&url_len.to_be_bytes());
        buffer.extend_from_slice(url_bytes);

        // Write config offset (i64)
        buffer.extend_from_slice(&worker_state.offset().to_be_bytes());

        buffer
    }

    /// Deserializes worker metadata from a byte buffer.
    ///
    /// # Arguments
    /// * `buffer` - A buffer containing the serialized protocol metadata
    ///
    /// # Returns
    /// The deserialized ExtendedWorkerState
    ///
    /// # Panics
    /// Panics if the protocol version is incompatible or buffer is malformed
    pub fn deserialize_metadata(buffer: &[u8]) -> ExtendedWorkerState {
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

        // Read allocation (assignment bytes) if present
        let allocation_start = offset_start + 8;
        let assignment = if buffer.len() > allocation_start {
            // Read allocation length (i32)
            let allocation_len = i32::from_be_bytes([
                buffer[allocation_start],
                buffer[allocation_start + 1],
                buffer[allocation_start + 2],
                buffer[allocation_start + 3],
            ]) as usize;

            if allocation_len > 0 && buffer.len() >= allocation_start + 4 + allocation_len {
                let allocation_bytes =
                    &buffer[allocation_start + 4..allocation_start + 4 + allocation_len];
                Self::deserialize_assignment(allocation_bytes)
            } else {
                ExtendedAssignment::empty()
            }
        } else {
            ExtendedAssignment::empty()
        };

        ExtendedWorkerState::new(url, config_offset, Some(assignment))
    }

    /// Serializes an extended assignment to a byte buffer.
    ///
    /// Complete Assignment V1/V2 format:
    /// ```text
    /// Version            => Int16
    /// Error              => Int16
    /// Leader             => String
    /// LeaderUrl          => String
    /// ConfigOffset       => Int64
    /// Assignment         => [Connector Assignment]
    /// Revoked            => [Connector Assignment]
    /// ScheduledDelay     => Int32
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
    /// * `sessioned` - Whether to use V2 (sessioned) or V1 protocol
    ///
    /// # Returns
    /// Serialized byte buffer containing the assignment, or empty if null/empty assignment
    pub fn serialize_assignment(assignment: &ExtendedAssignment, sessioned: bool) -> Vec<u8> {
        // Return empty for null or empty assignment
        if assignment == &ExtendedAssignment::empty() {
            return Vec::new();
        }

        let version = if sessioned {
            CONNECT_PROTOCOL_V2
        } else {
            CONNECT_PROTOCOL_V1
        };

        let mut buffer = Vec::new();

        // Write version (i16)
        buffer.extend_from_slice(&version.to_be_bytes());

        // Write error (i16)
        buffer.extend_from_slice(&assignment.error().to_be_bytes());

        // Write leader (string, nullable)
        Self::write_nullable_string(&mut buffer, assignment.leader());

        // Write leader_url (string, nullable)
        Self::write_nullable_string(&mut buffer, assignment.leader_url());

        // Write config offset (i64)
        buffer.extend_from_slice(&assignment.offset().to_be_bytes());

        // Write assignment array (connector assignments)
        Self::write_connector_assignments(&mut buffer, assignment);

        // Write revoked array (connector assignments)
        Self::write_revoked_connector_assignments(&mut buffer, assignment);

        // Write scheduled delay (i32)
        buffer.extend_from_slice(&assignment.delay().to_be_bytes());

        buffer
    }

    /// Deserializes an extended assignment from a byte buffer.
    ///
    /// # Arguments
    /// * `buffer` - A buffer containing the serialized assignment
    ///
    /// # Returns
    /// The deserialized ExtendedAssignment, or empty assignment if buffer is empty
    ///
    /// # Panics
    /// Panics if the protocol version is incompatible or buffer is malformed
    pub fn deserialize_assignment(buffer: &[u8]) -> ExtendedAssignment {
        if buffer.is_empty() {
            return ExtendedAssignment::empty();
        }

        // Read version (i16)
        let version = i16::from_be_bytes([buffer[0], buffer[1]]);
        Self::check_version_compatibility(version);

        // Read error (i16) at offset 2
        let error = i16::from_be_bytes([buffer[2], buffer[3]]);

        // Read leader (nullable string) at offset 4
        let (leader, leader_bytes_read) = Self::read_nullable_string(&buffer[4..]);

        // Read leader_url (nullable string)
        let leader_url_start = 4 + leader_bytes_read;
        let (leader_url, leader_url_bytes_read) =
            Self::read_nullable_string(&buffer[leader_url_start..]);

        // Read config offset (i64)
        let offset_start = leader_url_start + leader_url_bytes_read;
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
        let (connector_ids, task_ids, assignment_bytes_read) =
            Self::read_connector_assignments(&buffer[assignment_start..]);

        // Read revoked array
        let revoked_start = assignment_start + assignment_bytes_read;
        let (revoked_connector_ids, revoked_task_ids, revoked_bytes_read) =
            Self::read_connector_assignments(&buffer[revoked_start..]);

        // Read scheduled delay (i32)
        let delay_start = revoked_start + revoked_bytes_read;
        let delay = i32::from_be_bytes([
            buffer[delay_start],
            buffer[delay_start + 1],
            buffer[delay_start + 2],
            buffer[delay_start + 3],
        ]);

        ExtendedAssignment::new(
            version,
            error,
            leader,
            leader_url,
            config_offset,
            connector_ids,
            task_ids,
            revoked_connector_ids,
            revoked_task_ids,
            delay,
        )
    }

    /// Checks if the protocol version is compatible.
    ///
    /// # Panics
    /// Panics if the version is incompatible (less than V0)
    fn check_version_compatibility(version: i16) {
        if version < CONNECT_PROTOCOL_V0 {
            panic!("Unsupported subscription version: {}", version);
        }
        // Assume versions can be parsed
    }

    /// Writes a nullable string to the buffer.
    /// Null is represented as length -1.
    fn write_nullable_string(buffer: &mut Vec<u8>, value: Option<&str>) {
        match value {
            Some(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len() as i32;
                buffer.extend_from_slice(&len.to_be_bytes());
                buffer.extend_from_slice(bytes);
            }
            None => {
                // Null string: length -1
                buffer.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }
    }

    /// Reads a nullable string from the buffer.
    /// Returns (Option<String>, bytes_read).
    fn read_nullable_string(buffer: &[u8]) -> (Option<String>, usize) {
        let len = i32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        if len < 0 {
            // Null string
            return (None, 4);
        }
        let len = len as usize;
        let string_bytes = &buffer[4..4 + len];
        let string = String::from_utf8(string_bytes.to_vec()).expect("Invalid UTF-8 string");
        (Some(string), 4 + len)
    }

    /// Writes connector assignments to the buffer.
    /// Format: count (i32) + [connector (string) + tasks_count (i32) + [task_id (i32)]]
    fn write_connector_assignments(buffer: &mut Vec<u8>, assignment: &ExtendedAssignment) {
        // Create a map from connector to tasks
        let task_map = assignment.as_map();

        // Write count
        let count = task_map.len() as i32;
        buffer.extend_from_slice(&count.to_be_bytes());

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
    }

    /// Writes revoked connector assignments to the buffer.
    fn write_revoked_connector_assignments(buffer: &mut Vec<u8>, assignment: &ExtendedAssignment) {
        let revoked_map = assignment.revoked_as_map();

        // Write count (nullable - if no revoked, write 0 or handle null)
        match revoked_map {
            Some(task_map) => {
                let count = task_map.len() as i32;
                buffer.extend_from_slice(&count.to_be_bytes());

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
            }
            None => {
                // Write empty array (count = 0)
                buffer.extend_from_slice(&0i32.to_be_bytes());
            }
        }
    }

    /// Reads connector assignments from the buffer.
    /// Returns (connector_ids, task_ids, bytes_read).
    fn read_connector_assignments(buffer: &[u8]) -> (Vec<String>, Vec<ConnectorTaskId>, usize) {
        let mut connector_ids: Vec<String> = Vec::new();
        let mut task_ids: Vec<ConnectorTaskId> = Vec::new();

        // Read count
        let count = i32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        let mut pos = 4;

        for _ in 0..count {
            // Read connector name
            let connector_len = i32::from_be_bytes([
                buffer[pos],
                buffer[pos + 1],
                buffer[pos + 2],
                buffer[pos + 3],
            ]) as usize;
            pos += 4;
            let connector = String::from_utf8(buffer[pos..pos + connector_len].to_vec())
                .expect("Invalid UTF-8 connector name");
            pos += connector_len;

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
                    // This is the connector itself
                    connector_ids.push(connector.clone());
                } else {
                    // This is a task
                    task_ids.push(ConnectorTaskId::new(connector.clone(), task_id_val));
                }
            }
        }

        (connector_ids, task_ids, pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_assignment() {
        let assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            NO_ERROR,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["connector-a".to_string()],
            vec![ConnectorTaskId::new("connector-a".to_string(), 0)],
            vec!["connector-b".to_string()],
            vec![ConnectorTaskId::new("connector-b".to_string(), 1)],
            5000,
        );

        let serialized =
            IncrementalCooperativeConnectProtocol::serialize_assignment(&assignment, false);
        let deserialized =
            IncrementalCooperativeConnectProtocol::deserialize_assignment(&serialized);

        assert_eq!(deserialized.version(), assignment.version());
        assert_eq!(deserialized.error(), assignment.error());
        assert_eq!(deserialized.leader(), assignment.leader());
        assert_eq!(deserialized.leader_url(), assignment.leader_url());
        assert_eq!(deserialized.offset(), assignment.offset());
        assert_eq!(
            deserialized.connectors().len(),
            assignment.connectors().len()
        );
        assert_eq!(deserialized.tasks().len(), assignment.tasks().len());
        assert_eq!(
            deserialized.revoked_connectors().len(),
            assignment.revoked_connectors().len()
        );
        assert_eq!(
            deserialized.revoked_tasks().len(),
            assignment.revoked_tasks().len()
        );
        assert_eq!(deserialized.delay(), assignment.delay());
    }

    #[test]
    fn test_serialize_deserialize_empty_assignment() {
        let empty = ExtendedAssignment::empty();
        let serialized = IncrementalCooperativeConnectProtocol::serialize_assignment(&empty, false);
        assert!(serialized.is_empty());

        let deserialized =
            IncrementalCooperativeConnectProtocol::deserialize_assignment(&serialized);
        assert_eq!(deserialized, ExtendedAssignment::empty());
    }

    #[test]
    fn test_serialize_deserialize_metadata() {
        let assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            NO_ERROR,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["connector-a".to_string()],
            vec![ConnectorTaskId::new("connector-a".to_string(), 0)],
            vec![],
            vec![],
            0,
        );

        let worker_state =
            ExtendedWorkerState::new("http://worker:8083".to_string(), 100, Some(assignment));

        let serialized =
            IncrementalCooperativeConnectProtocol::serialize_metadata(&worker_state, false);
        let deserialized = IncrementalCooperativeConnectProtocol::deserialize_metadata(&serialized);

        assert_eq!(deserialized.url(), worker_state.url());
        assert_eq!(deserialized.offset(), worker_state.offset());
        assert_eq!(
            deserialized.assignment().connectors().len(),
            worker_state.assignment().connectors().len()
        );
    }

    #[test]
    fn test_metadata_request_with_sessioned() {
        let assignment = ExtendedAssignment::empty();
        let worker_state =
            ExtendedWorkerState::new("http://worker:8083".to_string(), 100, Some(assignment));

        let protocols =
            IncrementalCooperativeConnectProtocol::metadata_request(&worker_state, true);

        // Should have 3 protocols: sessioned, compatible, eager
        assert_eq!(protocols.len(), 3);
        assert_eq!(protocols[0].0, "sessioned");
        assert_eq!(protocols[1].0, "compatible");
        assert_eq!(protocols[2].0, "default");
    }

    #[test]
    fn test_metadata_request_without_sessioned() {
        let assignment = ExtendedAssignment::empty();
        let worker_state =
            ExtendedWorkerState::new("http://worker:8083".to_string(), 100, Some(assignment));

        let protocols =
            IncrementalCooperativeConnectProtocol::metadata_request(&worker_state, false);

        // Should have 2 protocols: compatible, eager
        assert_eq!(protocols.len(), 2);
        assert_eq!(protocols[0].0, "compatible");
        assert_eq!(protocols[1].0, "default");
    }

    #[test]
    fn test_serialize_assignment_with_sessioned() {
        let assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V2,
            NO_ERROR,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["connector-a".to_string()],
            vec![ConnectorTaskId::new("connector-a".to_string(), 0)],
            vec![],
            vec![],
            0,
        );

        let serialized =
            IncrementalCooperativeConnectProtocol::serialize_assignment(&assignment, true);
        let deserialized =
            IncrementalCooperativeConnectProtocol::deserialize_assignment(&serialized);

        // Version should be V2
        assert_eq!(deserialized.version(), CONNECT_PROTOCOL_V2);
    }

    #[test]
    fn test_assignment_with_null_leader() {
        let assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            NO_ERROR,
            None,
            None,
            -1,
            vec![],
            vec![],
            vec![],
            vec![],
            0,
        );

        let serialized =
            IncrementalCooperativeConnectProtocol::serialize_assignment(&assignment, false);
        let deserialized =
            IncrementalCooperativeConnectProtocol::deserialize_assignment(&serialized);

        assert!(deserialized.leader().is_none());
        assert!(deserialized.leader_url().is_none());
    }

    #[test]
    fn test_assignment_with_error() {
        let assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            CONFIG_MISMATCH,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec![],
            vec![],
            vec![],
            vec![],
            0,
        );

        let serialized =
            IncrementalCooperativeConnectProtocol::serialize_assignment(&assignment, false);
        let deserialized =
            IncrementalCooperativeConnectProtocol::deserialize_assignment(&serialized);

        assert!(deserialized.failed());
        assert_eq!(deserialized.error(), CONFIG_MISMATCH);
    }
}
