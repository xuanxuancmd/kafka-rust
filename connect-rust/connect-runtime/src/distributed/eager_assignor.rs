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

//! Eager assignor for Kafka Connect distributed runtime.
//!
//! An assignor that computes a unweighted round-robin distribution of connectors and tasks.
//! The connectors are assigned to the workers first, followed by the tasks. This is to avoid
//! load imbalance when several 1-task connectors are running, given that a connector is usually
//! more lightweight than a task.
//!
//! Note that this class is NOT thread-safe.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.distributed.EagerAssignor` in Java.

use crate::distributed::connect_assignor::{
    ConnectAssignor, JoinGroupResponseMember, WorkerCoordinatorState,
};
use crate::distributed::connect_protocol::{
    Assignment, ConnectProtocol, ConnectProtocolCompatibility, ConnectorTaskId,
    ASSIGNMENT_CONFIG_MISMATCH, ASSIGNMENT_NO_ERROR,
};
use crate::distributed::extended_worker_state::ExtendedWorkerState;
use std::collections::HashMap;

/// Circular iterator that wraps around a collection indefinitely.
///
/// This is used for round-robin assignment of connectors and tasks.
struct CircularIterator<T> {
    items: Vec<T>,
    index: usize,
}

impl<T> CircularIterator<T> {
    /// Creates a new circular iterator from a vector of items.
    fn new(items: Vec<T>) -> Self {
        CircularIterator { items, index: 0 }
    }

    /// Returns the next item in the circular iteration.
    /// After reaching the last item, it wraps back to the first.
    fn next(&mut self) -> Option<&T> {
        if self.items.is_empty() {
            return None;
        }
        let item = &self.items[self.index];
        self.index = (self.index + 1) % self.items.len();
        Some(item)
    }

    /// Returns the number of items in the iterator.
    fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true if the iterator has no items.
    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// An assignor that computes a unweighted round-robin distribution of connectors and tasks.
///
/// The connectors are assigned to the workers first, followed by the tasks. This is to avoid
/// load imbalance when several 1-task connectors are running, given that a connector is usually
/// more lightweight than a task.
///
/// Note that this class is NOT thread-safe.
pub struct EagerAssignor {
    /// Logger (placeholder - in Rust we typically use log crate)
    _log_context: String,
}

impl EagerAssignor {
    /// Creates a new EagerAssignor.
    ///
    /// # Arguments
    /// * `log_context` - Context string for logging (simplified compared to Java's LogContext)
    pub fn new(log_context: String) -> Self {
        EagerAssignor {
            _log_context: log_context,
        }
    }

    /// Performs task assignment based on the member metadata and worker coordinator state.
    ///
    /// This method:
    /// 1. Parses member metadata to get each worker's state
    /// 2. Finds the maximum config offset among all members
    /// 3. Ensures the leader has the latest config
    /// 4. Performs round-robin assignment of connectors and tasks
    /// 5. Serializes and returns the assignments
    ///
    /// # Arguments
    /// * `leader_id` - The leader of the group
    /// * `protocol` - The protocol type (should be Eager for this assignor)
    /// * `all_member_metadata` - The metadata of all the active workers of the group
    /// * `coordinator` - The worker coordinator state
    ///
    /// # Returns
    /// A map from member ID to serialized assignment bytes
    fn perform_assignment_internal(
        &self,
        leader_id: &str,
        protocol: ConnectProtocolCompatibility,
        all_member_metadata: &[JoinGroupResponseMember],
        coordinator: &dyn WorkerCoordinatorState,
        connectors: &[String],
        tasks_by_connector: &HashMap<String, Vec<ConnectorTaskId>>,
    ) -> HashMap<String, Vec<u8>> {
        // Parse member metadata to get ExtendedWorkerState for each member
        let member_configs: HashMap<String, ExtendedWorkerState> = all_member_metadata
            .iter()
            .map(|member| {
                let state = Self::deserialize_member_metadata(&member.metadata);
                (member.member_id.clone(), state)
            })
            .collect();

        // Find the maximum config offset among all members
        let max_offset = Self::find_max_member_config_offset(&member_configs, coordinator);

        // Ensure leader has the latest config
        let leader_offset_result = self.ensure_leader_config(max_offset, coordinator);

        // If leader doesn't have latest config, return CONFIG_MISMATCH assignment
        if leader_offset_result.is_none() {
            return self.fill_assignments_and_serialize(
                &member_configs.keys().cloned().collect::<Vec<_>>(),
                ASSIGNMENT_CONFIG_MISMATCH,
                leader_id,
                member_configs.get(leader_id).map(|s| s.url()).unwrap_or(""),
                max_offset,
                HashMap::new(),
                HashMap::new(),
            );
        }

        // Perform actual task assignment
        self.perform_task_assignment(
            leader_id,
            max_offset,
            &member_configs,
            connectors,
            tasks_by_connector,
        )
    }

    /// Deserializes member metadata to ExtendedWorkerState.
    ///
    /// This handles both V0 (WorkerState) and V1/V2 (ExtendedWorkerState) metadata formats.
    fn deserialize_member_metadata(metadata: &[u8]) -> ExtendedWorkerState {
        // Try to parse as V0 first (simple format)
        if metadata.len() >= 2 {
            let version = i16::from_be_bytes([metadata[0], metadata[1]]);
            if version == 0 {
                // V0 format: version (i16) + url (string) + offset (i64)
                let worker_state = ConnectProtocol::deserialize_metadata(metadata);
                // Convert WorkerState to ExtendedWorkerState with empty assignment
                return ExtendedWorkerState::new(
                    worker_state.url().to_string(),
                    worker_state.offset(),
                    None,
                );
            }
        }
        // Try V1/V2 format (ExtendedWorkerState with assignment)
        // For now, use a simplified parsing - this will be properly implemented
        // when IncrementalCooperativeConnectProtocol is fully implemented
        Self::deserialize_extended_metadata(metadata)
    }

    /// Deserializes V1/V2 extended metadata format.
    fn deserialize_extended_metadata(metadata: &[u8]) -> ExtendedWorkerState {
        // V1/V2 format: version (i16) + url (string) + offset (i64) + allocation bytes
        if metadata.len() < 6 {
            // Minimum: version(2) + url_len(4)
            return ExtendedWorkerState::new("".to_string(), 0, None);
        }

        let version = i16::from_be_bytes([metadata[0], metadata[1]]);

        // Read url length and url
        let url_len =
            i32::from_be_bytes([metadata[2], metadata[3], metadata[4], metadata[5]]) as usize;
        let url_start = 6;
        let url_end = url_start + url_len;

        if metadata.len() < url_end + 8 {
            return ExtendedWorkerState::new("".to_string(), 0, None);
        }

        let url = String::from_utf8(metadata[url_start..url_end].to_vec())
            .unwrap_or_else(|_| "".to_string());

        // Read offset (i64)
        let offset = i64::from_be_bytes([
            metadata[url_end],
            metadata[url_end + 1],
            metadata[url_end + 2],
            metadata[url_end + 3],
            metadata[url_end + 4],
            metadata[url_end + 5],
            metadata[url_end + 6],
            metadata[url_end + 7],
        ]);

        // For V1/V2, there may be an allocation field with assignment data
        // For eager protocol, we typically don't include assignment in metadata
        // So we use empty assignment
        ExtendedWorkerState::new(url, offset, None)
    }

    /// Finds the maximum config offset among all member configs.
    ///
    /// The new config offset is the maximum seen by any member. We always perform assignment
    /// using this offset, even if some members have fallen behind. The config offset used
    /// to generate the assignment is included in the response so members that have fallen
    /// behind will not use the assignment until they have caught up.
    fn find_max_member_config_offset(
        member_configs: &HashMap<String, ExtendedWorkerState>,
        coordinator: &dyn WorkerCoordinatorState,
    ) -> i64 {
        let mut max_offset: Option<i64> = None;

        for state in member_configs.values() {
            let member_offset = state.offset();
            max_offset = Some(match max_offset {
                None => member_offset,
                Some(current) => current.max(member_offset),
            });
        }

        let max_offset = max_offset.unwrap_or(0);

        // In a real implementation, we would log this
        // log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
        //           max_offset, coordinator.config_offset());

        max_offset
    }

    /// Ensures the leader has the latest configuration.
    ///
    /// If this leader is behind some other members, we can't do assignment.
    /// Returns None if the leader cannot catch up, otherwise returns the offset to use.
    fn ensure_leader_config(
        &self,
        max_offset: i64,
        coordinator: &dyn WorkerCoordinatorState,
    ) -> Option<i64> {
        let current_offset = coordinator.config_offset();

        if current_offset < max_offset {
            // We might be able to take a new snapshot to catch up immediately
            // In this simplified implementation, we return None indicating mismatch
            // A real implementation would try to get a fresh snapshot
            //
            // log.info("Was selected to perform assignments, but do not have latest config
            //          found in sync request. Returning an empty configuration to trigger re-sync.");
            None
        } else {
            Some(max_offset)
        }
    }

    /// Performs round-robin task assignment.
    ///
    /// Assigns all connectors first, then all tasks. This avoids load imbalance in common cases
    /// (e.g., for connectors that generate only 1 task each).
    fn perform_task_assignment(
        &self,
        leader_id: &str,
        max_offset: i64,
        member_configs: &HashMap<String, ExtendedWorkerState>,
        connectors: &[String],
        tasks_by_connector: &HashMap<String, Vec<ConnectorTaskId>>,
    ) -> HashMap<String, Vec<u8>> {
        // Initialize assignment maps
        let mut connector_assignments: HashMap<String, Vec<String>> = HashMap::new();
        let mut task_assignments: HashMap<String, Vec<ConnectorTaskId>> = HashMap::new();

        // Sort connectors for deterministic assignment
        let connectors_sorted = Self::sorted(connectors);

        // Sort member IDs for deterministic circular iteration
        let members_sorted = Self::sorted(&member_configs.keys().cloned().collect::<Vec<_>>());
        let mut member_it = CircularIterator::new(members_sorted.clone());

        // Assign connectors using round-robin
        for connector_id in &connectors_sorted {
            if let Some(connector_assigned_to) = member_it.next() {
                connector_assignments
                    .entry(connector_assigned_to.clone())
                    .or_insert_with(Vec::new)
                    .push(connector_id.clone());
            }
        }

        // Assign tasks using round-robin (continues from where connector assignment ended)
        for connector_id in &connectors_sorted {
            let tasks = tasks_by_connector.get(connector_id);
            if let Some(tasks) = tasks {
                let tasks_sorted = Self::sorted(tasks);
                for task_id in &tasks_sorted {
                    if let Some(task_assigned_to) = member_it.next() {
                        task_assignments
                            .entry(task_assigned_to.clone())
                            .or_insert_with(Vec::new)
                            .push(task_id.clone());
                    }
                }
            }
        }

        // In a real implementation, we would update the coordinator's leader state
        // coordinator.leader_state(new LeaderState(member_configs, connector_assignments, task_assignments));

        // Fill assignments and serialize
        self.fill_assignments_and_serialize(
            &members_sorted,
            ASSIGNMENT_NO_ERROR,
            leader_id,
            member_configs.get(leader_id).map(|s| s.url()).unwrap_or(""),
            max_offset,
            connector_assignments,
            task_assignments,
        )
    }

    /// Fills assignments for all members and serializes them.
    ///
    /// Creates an Assignment for each member with their assigned connectors and tasks,
    /// then serializes using ConnectProtocol.
    fn fill_assignments_and_serialize(
        &self,
        members: &[String],
        error: i16,
        leader_id: &str,
        leader_url: &str,
        max_offset: i64,
        connector_assignments: HashMap<String, Vec<String>>,
        task_assignments: HashMap<String, Vec<ConnectorTaskId>>,
    ) -> HashMap<String, Vec<u8>> {
        let mut group_assignment: HashMap<String, Vec<u8>> = HashMap::new();

        for member in members {
            let connectors = connector_assignments
                .get(member)
                .cloned()
                .unwrap_or_default();

            let tasks = task_assignments.get(member).cloned().unwrap_or_default();

            let assignment = Assignment::new(
                error,
                leader_id.to_string(),
                leader_url.to_string(),
                max_offset,
                connectors,
                tasks,
            );

            // In a real implementation, we would log this
            // log.debug("Assignment: {} -> {}", member, assignment);

            let serialized = ConnectProtocol::serialize_assignment(&assignment);
            group_assignment.insert(member.clone(), serialized);
        }

        // In a real implementation, we would log this
        // log.debug("Finished assignment");

        group_assignment
    }

    /// Sorts a collection for deterministic ordering.
    fn sorted<T: Ord + Clone>(items: &[T]) -> Vec<T> {
        let mut result = items.to_vec();
        result.sort();
        result
    }
}

impl ConnectAssignor for EagerAssignor {
    /// Based on the member metadata and the information stored in the worker coordinator,
    /// this method computes an assignment of connectors and tasks among the members
    /// of the worker group.
    ///
    /// This implementation performs a round-robin distribution of connectors and tasks.
    /// Connectors are assigned first, then tasks, to avoid load imbalance.
    fn perform_assignment(
        &self,
        leader_id: &str,
        protocol: ConnectProtocolCompatibility,
        all_member_metadata: &[JoinGroupResponseMember],
        coordinator: &dyn WorkerCoordinatorState,
    ) -> HashMap<String, Vec<u8>> {
        // In a real implementation, we would get connectors and tasks from coordinator.config_snapshot()
        // For now, we use empty lists as the coordinator trait doesn't provide this
        // This will be properly implemented when WorkerCoordinator is complete

        let connectors: Vec<String> = Vec::new();
        let tasks_by_connector: HashMap<String, Vec<ConnectorTaskId>> = HashMap::new();

        self.perform_assignment_internal(
            leader_id,
            protocol,
            all_member_metadata,
            coordinator,
            &connectors,
            &tasks_by_connector,
        )
    }

    /// Returns the protocol version string for this assignor.
    ///
    /// For EagerAssignor, this returns "default" which corresponds to protocol V0.
    fn version(&self) -> String {
        ConnectProtocolCompatibility::Eager.protocol().to_string()
    }

    /// Returns the protocol type this assignor supports.
    ///
    /// EagerAssignor supports the EAGER protocol (V0).
    fn protocol(&self) -> ConnectProtocolCompatibility {
        ConnectProtocolCompatibility::Eager
    }
}

/// Extended version of perform_assignment that accepts connectors and tasks directly.
///
/// This is used when the caller has direct access to configuration state,
/// bypassing the need for a full WorkerCoordinator implementation.
pub struct EagerAssignorContext {
    assignor: EagerAssignor,
}

impl EagerAssignorContext {
    /// Creates a new EagerAssignorContext.
    pub fn new() -> Self {
        EagerAssignorContext {
            assignor: EagerAssignor::new("EagerAssignor".to_string()),
        }
    }

    /// Performs assignment with explicit connector and task data.
    ///
    /// # Arguments
    /// * `leader_id` - The leader of the group
    /// * `all_member_metadata` - The metadata of all the active workers
    /// * `coordinator_offset` - The coordinator's current config offset
    /// * `coordinator_url` - The coordinator's URL
    /// * `connectors` - List of connector IDs to assign
    /// * `tasks_by_connector` - Map from connector ID to its task IDs
    ///
    /// # Returns
    /// A map from member ID to serialized assignment bytes
    pub fn perform_assignment_with_data(
        &self,
        leader_id: &str,
        all_member_metadata: &[JoinGroupResponseMember],
        coordinator_offset: i64,
        coordinator_url: &str,
        connectors: &[String],
        tasks_by_connector: &HashMap<String, Vec<ConnectorTaskId>>,
    ) -> HashMap<String, Vec<u8>> {
        // Parse member metadata
        let member_configs: HashMap<String, ExtendedWorkerState> = all_member_metadata
            .iter()
            .map(|member| {
                let state = EagerAssignor::deserialize_member_metadata(&member.metadata);
                (member.member_id.clone(), state)
            })
            .collect();

        // Find max offset
        let max_offset = EagerAssignor::find_max_member_config_offset_simple(&member_configs);

        // Check if coordinator has latest config
        if coordinator_offset < max_offset {
            // Return CONFIG_MISMATCH
            return self.assignor.fill_assignments_and_serialize(
                &member_configs.keys().cloned().collect::<Vec<_>>(),
                ASSIGNMENT_CONFIG_MISMATCH,
                leader_id,
                coordinator_url,
                max_offset,
                HashMap::new(),
                HashMap::new(),
            );
        }

        // Perform round-robin assignment
        self.assignor.perform_task_assignment(
            leader_id,
            max_offset,
            &member_configs,
            connectors,
            tasks_by_connector,
        )
    }
}

impl EagerAssignor {
    /// Simplified version of find_max_member_config_offset for use without coordinator.
    fn find_max_member_config_offset_simple(
        member_configs: &HashMap<String, ExtendedWorkerState>,
    ) -> i64 {
        let mut max_offset: Option<i64> = None;

        for state in member_configs.values() {
            let member_offset = state.offset();
            max_offset = Some(match max_offset {
                None => member_offset,
                Some(current) => current.max(member_offset),
            });
        }

        max_offset.unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::connect_protocol::CONNECT_PROTOCOL_V0;

    /// Mock coordinator for testing
    struct MockCoordinator {
        config_offset: i64,
        member_id: String,
        url: String,
    }

    impl MockCoordinator {
        fn new(config_offset: i64, member_id: String, url: String) -> Self {
            MockCoordinator {
                config_offset,
                member_id,
                url,
            }
        }
    }

    impl WorkerCoordinatorState for MockCoordinator {
        fn config_offset(&self) -> i64 {
            self.config_offset
        }

        fn member_id(&self) -> &str {
            &self.member_id
        }

        fn is_leader(&self) -> bool {
            true
        }

        fn url(&self) -> &str {
            &self.url
        }

        fn assigned_connectors(&self) -> &[String] {
            &[]
        }

        fn assigned_tasks(&self) -> &[ConnectorTaskId] {
            &[]
        }
    }

    #[test]
    fn test_circular_iterator_basic() {
        let items = vec!["a", "b", "c"];
        let mut it = CircularIterator::new(items);

        assert_eq!(it.next(), Some(&"a"));
        assert_eq!(it.next(), Some(&"b"));
        assert_eq!(it.next(), Some(&"c"));
        // Wraps around
        assert_eq!(it.next(), Some(&"a"));
        assert_eq!(it.next(), Some(&"b"));
    }

    #[test]
    fn test_circular_iterator_empty() {
        let items: Vec<i32> = vec![];
        let mut it = CircularIterator::new(items);

        assert!(it.is_empty());
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_circular_iterator_single() {
        let items = vec!["only"];
        let mut it = CircularIterator::new(items);

        assert_eq!(it.next(), Some(&"only"));
        assert_eq!(it.next(), Some(&"only"));
        assert_eq!(it.next(), Some(&"only"));
    }

    #[test]
    fn test_eager_assignor_creation() {
        let assignor = EagerAssignor::new("test-context".to_string());
        assert_eq!(assignor.version(), "default");
        assert_eq!(assignor.protocol(), ConnectProtocolCompatibility::Eager);
    }

    #[test]
    fn test_sorted() {
        let items = vec!["c", "a", "b"];
        let sorted = EagerAssignor::sorted(&items);
        assert_eq!(sorted, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_deserialize_member_metadata_v0() {
        // Create V0 metadata
        let worker_state = crate::distributed::connect_protocol::WorkerState::new(
            "http://localhost:8083".to_string(),
            100,
        );
        let metadata = ConnectProtocol::serialize_metadata(&worker_state);

        let result = EagerAssignor::deserialize_member_metadata(&metadata);

        assert_eq!(result.url(), "http://localhost:8083");
        assert_eq!(result.offset(), 100);
    }

    #[test]
    fn test_fill_assignments_and_serialize() {
        let assignor = EagerAssignor::new("test".to_string());

        let members = vec!["member1".to_string(), "member2".to_string()];
        let connector_assignments: HashMap<String, Vec<String>> = {
            let mut map = HashMap::new();
            map.insert("member1".to_string(), vec!["connector1".to_string()]);
            map.insert("member2".to_string(), vec!["connector2".to_string()]);
            map
        };
        let task_assignments: HashMap<String, Vec<ConnectorTaskId>> = {
            let mut map = HashMap::new();
            map.insert(
                "member1".to_string(),
                vec![ConnectorTaskId::new("connector1".to_string(), 0)],
            );
            map.insert(
                "member2".to_string(),
                vec![ConnectorTaskId::new("connector2".to_string(), 0)],
            );
            map
        };

        let result = assignor.fill_assignments_and_serialize(
            &members,
            ASSIGNMENT_NO_ERROR,
            "leader1",
            "http://leader:8083",
            100,
            connector_assignments,
            task_assignments,
        );

        assert_eq!(result.len(), 2);
        assert!(result.contains_key("member1"));
        assert!(result.contains_key("member2"));

        // Deserialize and verify
        let assignment1 = ConnectProtocol::deserialize_assignment(result.get("member1").unwrap());
        assert_eq!(assignment1.error(), ASSIGNMENT_NO_ERROR);
        assert_eq!(assignment1.leader(), "leader1");
        assert_eq!(assignment1.connectors().len(), 1);
        assert_eq!(assignment1.tasks().len(), 1);
    }

    #[test]
    fn test_config_mismatch_assignment() {
        let assignor = EagerAssignor::new("test".to_string());

        let members = vec!["member1".to_string()];
        let result = assignor.fill_assignments_and_serialize(
            &members,
            ASSIGNMENT_CONFIG_MISMATCH,
            "",
            "",
            0,
            HashMap::new(),
            HashMap::new(),
        );

        let assignment = ConnectProtocol::deserialize_assignment(result.get("member1").unwrap());
        assert_eq!(assignment.error(), ASSIGNMENT_CONFIG_MISMATCH);
        assert!(assignment.failed());
        assert!(assignment.connectors().is_empty());
        assert!(assignment.tasks().is_empty());
    }

    #[test]
    fn test_perform_assignment_empty() {
        let assignor = EagerAssignor::new("test".to_string());
        let coordinator =
            MockCoordinator::new(100, "leader".to_string(), "http://leader:8083".to_string());

        // Create member metadata
        let worker_state = crate::distributed::connect_protocol::WorkerState::new(
            "http://worker:8083".to_string(),
            100,
        );
        let metadata = ConnectProtocol::serialize_metadata(&worker_state);
        let members = vec![JoinGroupResponseMember::new("leader".to_string(), metadata)];

        let result = assignor.perform_assignment(
            "leader",
            ConnectProtocolCompatibility::Eager,
            &members,
            &coordinator,
        );

        // With no connectors/tasks, should still return valid assignment
        assert_eq!(result.len(), 1);
        let assignment = ConnectProtocol::deserialize_assignment(result.get("leader").unwrap());
        assert_eq!(assignment.error(), ASSIGNMENT_NO_ERROR);
    }

    #[test]
    fn test_eager_assignor_context() {
        let context = EagerAssignorContext::new();

        // Create member metadata
        let worker_state = crate::distributed::connect_protocol::WorkerState::new(
            "http://worker1:8083".to_string(),
            100,
        );
        let metadata1 = ConnectProtocol::serialize_metadata(&worker_state);

        let worker_state2 = crate::distributed::connect_protocol::WorkerState::new(
            "http://worker2:8083".to_string(),
            100,
        );
        let metadata2 = ConnectProtocol::serialize_metadata(&worker_state2);

        let members = vec![
            JoinGroupResponseMember::new("member1".to_string(), metadata1),
            JoinGroupResponseMember::new("member2".to_string(), metadata2),
        ];

        let connectors = vec![
            "conn1".to_string(),
            "conn2".to_string(),
            "conn3".to_string(),
        ];
        let tasks_by_connector: HashMap<String, Vec<ConnectorTaskId>> = {
            let mut map = HashMap::new();
            map.insert(
                "conn1".to_string(),
                vec![
                    ConnectorTaskId::new("conn1".to_string(), 0),
                    ConnectorTaskId::new("conn1".to_string(), 1),
                ],
            );
            map.insert(
                "conn2".to_string(),
                vec![ConnectorTaskId::new("conn2".to_string(), 0)],
            );
            map.insert(
                "conn3".to_string(),
                vec![ConnectorTaskId::new("conn3".to_string(), 0)],
            );
            map
        };

        let result = context.perform_assignment_with_data(
            "member1",
            &members,
            100,
            "http://worker1:8083",
            &connectors,
            &tasks_by_connector,
        );

        assert_eq!(result.len(), 2);

        // Verify assignments
        for (_, serialized) in &result {
            let assignment = ConnectProtocol::deserialize_assignment(serialized);
            assert_eq!(assignment.error(), ASSIGNMENT_NO_ERROR);
        }
    }

    #[test]
    fn test_round_robin_distribution() {
        let assignor = EagerAssignor::new("test".to_string());

        // Create member configs
        let member_configs: HashMap<String, ExtendedWorkerState> = {
            let mut map = HashMap::new();
            map.insert(
                "m1".to_string(),
                ExtendedWorkerState::new("url1".to_string(), 100, None),
            );
            map.insert(
                "m2".to_string(),
                ExtendedWorkerState::new("url2".to_string(), 100, None),
            );
            map.insert(
                "m3".to_string(),
                ExtendedWorkerState::new("url3".to_string(), 100, None),
            );
            map
        };

        // 6 connectors, should be distributed evenly
        let connectors: Vec<String> = vec!["c1", "c2", "c3", "c4", "c5", "c6"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        let tasks_by_connector: HashMap<String, Vec<ConnectorTaskId>> = HashMap::new();

        let result = assignor.perform_task_assignment(
            "m1",
            100,
            &member_configs,
            &connectors,
            &tasks_by_connector,
        );

        // Count connectors per member
        let mut counts: HashMap<String, usize> = HashMap::new();
        for (_, serialized) in &result {
            let assignment = ConnectProtocol::deserialize_assignment(serialized);
            counts.insert(
                assignment.leader().to_string(), // Using leader field to check member assignment
                assignment.connectors().len(),
            );
        }

        // Each member should have exactly 2 connectors (6 connectors / 3 members)
        // Note: The actual assignment is based on member IDs, not leader field
        // We need to check by deserializing each member's assignment
        for member_id in member_configs.keys() {
            let serialized = result.get(member_id).unwrap();
            let assignment = ConnectProtocol::deserialize_assignment(serialized);
            assert_eq!(assignment.connectors().len(), 2);
        }
    }
}
