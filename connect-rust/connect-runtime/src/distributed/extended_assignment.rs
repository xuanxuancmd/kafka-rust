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

//! Extended assignment of connectors and tasks that includes revoked connectors and tasks
//! as well as a scheduled rebalancing delay.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.distributed.ExtendedAssignment` in Java.

use crate::distributed::connect_protocol::ConnectorTaskId;
use std::collections::HashMap;

/// Sentinel task ID used to indicate the connector itself (as opposed to a task).
/// A task ID of -1 indicates that the assignment includes responsibility for running
/// the Connector instance in addition to any tasks it generates.
pub const CONNECTOR_TASK: i32 = -1;

/// Error code indicating no error during assignment.
pub const NO_ERROR: i16 = 0;

/// Error code indicating configuration offsets mismatched in a way that the leader
/// could not resolve. Workers should read to the end of the config log and try to re-join.
pub const CONFIG_MISMATCH: i16 = 1;

/// Connect protocol version 1 (incremental cooperative).
pub const CONNECT_PROTOCOL_V1: i16 = 1;

/// The extended assignment of connectors and tasks that includes revoked connectors and tasks
/// as well as a scheduled rebalancing delay.
///
/// This extends the basic Assignment with additional fields for incremental cooperative rebalancing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendedAssignment {
    /// Connect protocol version
    version: i16,
    /// Error code for this assignment; NO_ERROR indicates no error during assignment
    error: i16,
    /// Connect group's leader Id; may be null only on the empty assignment
    leader: Option<String>,
    /// Connect group's leader URL; may be null only on the empty assignment
    leader_url: Option<String>,
    /// The offset in the config topic that this assignment is corresponding to
    config_offset: i64,
    /// List of connectors that the worker should instantiate and run
    connector_ids: Vec<String>,
    /// List of task IDs that the worker should instantiate and run
    task_ids: Vec<ConnectorTaskId>,
    /// List of connectors that the worker should stop running
    revoked_connector_ids: Vec<String>,
    /// List of task IDs that the worker should stop running
    revoked_task_ids: Vec<ConnectorTaskId>,
    /// The scheduled delay after which the worker should rejoin the group
    delay: i32,
}

impl ExtendedAssignment {
    /// Create an assignment indicating responsibility for the given connector instances and task Ids.
    ///
    /// # Arguments
    /// * `version` - Connect protocol version
    /// * `error` - Error code for this assignment; NO_ERROR indicates no error during assignment
    /// * `leader` - Connect group's leader Id; may be null only on the empty assignment
    /// * `leader_url` - Connect group's leader URL; may be null only on the empty assignment
    /// * `config_offset` - The offset in the config topic that this assignment is corresponding to
    /// * `connector_ids` - List of connectors that the worker should instantiate and run
    /// * `task_ids` - List of task IDs that the worker should instantiate and run
    /// * `revoked_connector_ids` - List of connectors that the worker should stop running
    /// * `revoked_task_ids` - List of task IDs that the worker should stop running
    /// * `delay` - The scheduled delay after which the worker should rejoin the group
    pub fn new(
        version: i16,
        error: i16,
        leader: Option<String>,
        leader_url: Option<String>,
        config_offset: i64,
        connector_ids: Vec<String>,
        task_ids: Vec<ConnectorTaskId>,
        revoked_connector_ids: Vec<String>,
        revoked_task_ids: Vec<ConnectorTaskId>,
        delay: i32,
    ) -> Self {
        ExtendedAssignment {
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
        }
    }

    /// Create a duplicate of the given assignment with deep copies of all collections.
    pub fn duplicate(assignment: &ExtendedAssignment) -> ExtendedAssignment {
        ExtendedAssignment::new(
            assignment.version(),
            assignment.error(),
            assignment.leader().map(|s| s.to_string()),
            assignment.leader_url().map(|s| s.to_string()),
            assignment.offset(),
            assignment.connectors().iter().cloned().collect(),
            assignment.tasks().iter().cloned().collect(),
            assignment.revoked_connectors().iter().cloned().collect(),
            assignment.revoked_tasks().iter().cloned().collect(),
            assignment.delay(),
        )
    }

    /// Return an empty assignment.
    pub fn empty() -> ExtendedAssignment {
        static EMPTY: std::sync::OnceLock<ExtendedAssignment> = std::sync::OnceLock::new();
        EMPTY
            .get_or_init(|| {
                ExtendedAssignment::new(
                    CONNECT_PROTOCOL_V1,
                    NO_ERROR,
                    None,
                    None,
                    -1,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    0,
                )
            })
            .clone()
    }

    /// Return the version of the connect protocol that this assignment belongs to.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Return the error code of this assignment; 0 signals successful assignment.
    pub fn error(&self) -> i16 {
        self.error
    }

    /// Return the ID of the leader Connect worker in this assignment.
    pub fn leader(&self) -> Option<&str> {
        self.leader.as_deref()
    }

    /// Return the URL to which the leader accepts requests from other members of the group.
    pub fn leader_url(&self) -> Option<&str> {
        self.leader_url.as_deref()
    }

    /// Check if this assignment failed.
    pub fn failed(&self) -> bool {
        self.error != NO_ERROR
    }

    /// Return the most up-to-date offset in the configuration topic according to this assignment.
    pub fn offset(&self) -> i64 {
        self.config_offset
    }

    /// Return the connectors included in this assignment.
    pub fn connectors(&self) -> &[String] {
        &self.connector_ids
    }

    /// Return the tasks included in this assignment.
    pub fn tasks(&self) -> &[ConnectorTaskId] {
        &self.task_ids
    }

    /// Return the IDs of the connectors that are revoked by this assignment.
    pub fn revoked_connectors(&self) -> &[String] {
        &self.revoked_connector_ids
    }

    /// Return the IDs of the tasks that are revoked by this assignment.
    pub fn revoked_tasks(&self) -> &[ConnectorTaskId] {
        &self.revoked_task_ids
    }

    /// Return the delay for the rebalance that is scheduled by this assignment.
    pub fn delay(&self) -> i32 {
        self.delay
    }

    /// Convert the assignment to a map structure for serialization.
    /// Returns a map from connector ID to a collection of task IDs (including CONNECTOR_TASK for connectors).
    pub fn as_map(&self) -> HashMap<String, Vec<i32>> {
        let mut task_map: HashMap<String, Vec<i32>> = HashMap::new();

        // Add connector instances (using CONNECTOR_TASK sentinel)
        for connector_id in &self.connector_ids {
            task_map
                .entry(connector_id.clone())
                .or_insert_with(Vec::new)
                .push(CONNECTOR_TASK);
        }

        // Add task IDs
        for task_id in &self.task_ids {
            task_map
                .entry(task_id.connector().to_string())
                .or_insert_with(Vec::new)
                .push(task_id.task());
        }

        task_map
    }

    /// Convert the revoked assignments to a map structure for serialization.
    /// Returns a map from connector ID to a collection of task IDs (including CONNECTOR_TASK for connectors).
    pub fn revoked_as_map(&self) -> Option<HashMap<String, Vec<i32>>> {
        if self.revoked_connector_ids.is_empty() && self.revoked_task_ids.is_empty() {
            return None;
        }

        let mut task_map: HashMap<String, Vec<i32>> = HashMap::new();

        // Add revoked connector instances (using CONNECTOR_TASK sentinel)
        for connector_id in &self.revoked_connector_ids {
            task_map
                .entry(connector_id.clone())
                .or_insert_with(Vec::new)
                .push(CONNECTOR_TASK);
        }

        // Add revoked task IDs
        for task_id in &self.revoked_task_ids {
            task_map
                .entry(task_id.connector().to_string())
                .or_insert_with(Vec::new)
                .push(task_id.task());
        }

        Some(task_map)
    }
}

impl std::fmt::Display for ExtendedAssignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Assignment{{error={}, leader='{}', leaderUrl='{}', offset={}, connectorIds={:?}, taskIds={:?}, revokedConnectorIds={:?}, revokedTaskIds={:?}, delay={}}}",
            self.error,
            self.leader.as_deref().unwrap_or("null"),
            self.leader_url.as_deref().unwrap_or("null"),
            self.config_offset,
            self.connector_ids,
            self.task_ids,
            self.revoked_connector_ids,
            self.revoked_task_ids,
            self.delay
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_assignment() {
        let empty = ExtendedAssignment::empty();
        assert_eq!(empty.version(), CONNECT_PROTOCOL_V1);
        assert_eq!(empty.error(), NO_ERROR);
        assert!(empty.leader().is_none());
        assert!(empty.leader_url().is_none());
        assert_eq!(empty.offset(), -1);
        assert!(empty.connectors().is_empty());
        assert!(empty.tasks().is_empty());
        assert!(empty.revoked_connectors().is_empty());
        assert!(empty.revoked_tasks().is_empty());
        assert_eq!(empty.delay(), 0);
    }

    #[test]
    fn test_assignment_creation() {
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

        assert_eq!(assignment.version(), CONNECT_PROTOCOL_V1);
        assert_eq!(assignment.error(), NO_ERROR);
        assert_eq!(assignment.leader(), Some("leader-1"));
        assert_eq!(assignment.leader_url(), Some("http://leader:8083"));
        assert_eq!(assignment.offset(), 100);
        assert_eq!(assignment.connectors().len(), 1);
        assert_eq!(assignment.tasks().len(), 1);
        assert_eq!(assignment.revoked_connectors().len(), 1);
        assert_eq!(assignment.revoked_tasks().len(), 1);
        assert_eq!(assignment.delay(), 5000);
        assert!(!assignment.failed());
    }

    #[test]
    fn test_duplicate() {
        let original = ExtendedAssignment::new(
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

        let duplicate = ExtendedAssignment::duplicate(&original);
        assert_eq!(original, duplicate);
    }

    #[test]
    fn test_as_map() {
        let assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            NO_ERROR,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["connector-a".to_string(), "connector-b".to_string()],
            vec![
                ConnectorTaskId::new("connector-a".to_string(), 0),
                ConnectorTaskId::new("connector-a".to_string(), 1),
            ],
            vec![],
            vec![],
            0,
        );

        let map = assignment.as_map();
        assert_eq!(map.len(), 2);

        // Check connector-a has CONNECTOR_TASK and tasks 0, 1
        let connector_a_tasks = map.get("connector-a").unwrap();
        assert!(connector_a_tasks.contains(&CONNECTOR_TASK));
        assert!(connector_a_tasks.contains(&0));
        assert!(connector_a_tasks.contains(&1));

        // Check connector-b has only CONNECTOR_TASK (no tasks)
        let connector_b_tasks = map.get("connector-b").unwrap();
        assert!(connector_b_tasks.contains(&CONNECTOR_TASK));
        assert_eq!(connector_b_tasks.len(), 1);
    }

    #[test]
    fn test_revoked_as_map() {
        let assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            NO_ERROR,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec![],
            vec![],
            vec!["connector-c".to_string()],
            vec![ConnectorTaskId::new("connector-c".to_string(), 0)],
            0,
        );

        let map = assignment.revoked_as_map();
        assert!(map.is_some());

        let map = map.unwrap();
        assert_eq!(map.len(), 1);

        let connector_c_tasks = map.get("connector-c").unwrap();
        assert!(connector_c_tasks.contains(&CONNECTOR_TASK));
        assert!(connector_c_tasks.contains(&0));
    }

    #[test]
    fn test_failed_assignment() {
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

        assert!(assignment.failed());
    }

    #[test]
    fn test_display() {
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

        let display_str = format!("{}", assignment);
        assert!(display_str.contains("error=0"));
        assert!(display_str.contains("leader='leader-1'"));
        assert!(display_str.contains("offset=100"));
        assert!(display_str.contains("delay=5000"));
    }
}
