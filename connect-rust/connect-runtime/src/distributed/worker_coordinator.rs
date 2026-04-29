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

//! Worker coordinator for Kafka Connect distributed runtime.
//!
//! This class manages the coordination process with brokers for the Connect cluster group membership.
//! It ties together the coordinator, which implements the group member protocol, with all the other
//! pieces needed to drive the connection to the group coordinator broker.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.distributed.WorkerCoordinator` in Java.

use crate::distributed::connect_assignor::{
    ConnectAssignor, JoinGroupResponseMember, WorkerCoordinatorState,
};
use crate::distributed::connect_protocol::{
    ConnectProtocol, ConnectProtocolCompatibility, ConnectorTaskId,
};
use crate::distributed::eager_assignor::EagerAssignor;
use crate::distributed::extended_assignment::ExtendedAssignment;
use crate::distributed::extended_worker_state::ExtendedWorkerState;
use crate::distributed::incremental_cooperative_assignor::IncrementalCooperativeAssignor;
use crate::distributed::incremental_cooperative_connect_protocol::IncrementalCooperativeConnectProtocol;
use crate::distributed::worker_group_member::WorkerCoordinator as WorkerCoordinatorTrait;
use crate::distributed::worker_rebalance_listener::WorkerRebalanceListener;
use common_trait::storage::{ClusterConfigState, ConfigBackingStore};
use kafka_clients_mock::coordinator::{Generation, LeaderState};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Unknown member ID constant (matches Java JoinGroupRequest.UNKNOWN_MEMBER_ID)
pub const UNKNOWN_MEMBER_ID: &str = "";

/// No generation constant (matches Java Generation.NO_GENERATION.generationId)
pub const NO_GENERATION: i32 = -1;

/// Worker coordinator for managing group membership and assignment.
///
/// This implements the group member protocol for Connect workers, coordinating
/// with the Kafka broker to join groups, sync assignments, and handle rebalancing.
///
/// The WorkerCoordinator is responsible for:
/// - Managing the group membership lifecycle (join, sync, heartbeat, leave)
/// - Tracking the current assignment of connectors and tasks
/// - Handling protocol compatibility between different worker versions
/// - Providing leader state for assignment computation
pub struct WorkerCoordinator {
    /// Worker REST API URL
    rest_url: String,
    /// Configuration backing store for accessing cluster config state
    config_storage: Arc<dyn ConfigBackingStore>,
    /// Current assignment snapshot (volatile in Java)
    assignment_snapshot: Option<ExtendedAssignment>,
    /// Cluster configuration state snapshot
    config_snapshot: Option<ClusterConfigState>,
    /// Rebalance listener for assignment/revocation callbacks
    listener: Option<Arc<Mutex<dyn WorkerRebalanceListener>>>,
    /// Protocol compatibility mode
    protocol_compatibility: ConnectProtocolCompatibility,
    /// Leader state (only valid for leader workers)
    leader_state: Option<LeaderState>,
    /// Flag indicating if rejoin has been requested
    rejoin_requested: bool,
    /// Current connect protocol version (volatile in Java)
    current_connect_protocol: ConnectProtocolCompatibility,
    /// Last completed generation ID (volatile in Java)
    last_completed_generation_id: i32,
    /// Eager mode assignor (V0 protocol)
    eager_assignor: EagerAssignor,
    /// Incremental cooperative assignor (V1/V2 protocol)
    incremental_assignor: IncrementalCooperativeAssignor,
    /// Coordinator discovery timeout in milliseconds
    coordinator_discovery_timeout_ms: i32,
    /// Current generation information
    generation: Generation,
    /// Member state
    state: MemberState,
}

/// Member state in the coordinator group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberState {
    /// Member is not part of any group
    Unjoined,
    /// Member is waiting for join group response
    Joining,
    /// Member is waiting for sync group response
    Syncing,
    /// Member has stable assignment
    Stable,
    /// Member is leaving the group
    Leaving,
}

impl WorkerCoordinator {
    /// Create a new WorkerCoordinator.
    ///
    /// # Arguments
    /// * `rest_url` - The REST URL of this worker
    /// * `config_storage` - The configuration backing store
    /// * `protocol_compatibility` - The protocol compatibility mode
    /// * `max_delay` - Maximum delay for incremental cooperative rebalancing
    /// * `coordinator_discovery_timeout_ms` - Timeout for coordinator discovery
    ///
    /// # Returns
    /// A new WorkerCoordinator instance
    pub fn new(
        rest_url: String,
        config_storage: Arc<dyn ConfigBackingStore>,
        protocol_compatibility: ConnectProtocolCompatibility,
        max_delay: i32,
        coordinator_discovery_timeout_ms: i32,
    ) -> Self {
        WorkerCoordinator {
            rest_url,
            config_storage,
            assignment_snapshot: None,
            config_snapshot: None,
            listener: None,
            protocol_compatibility,
            leader_state: None,
            rejoin_requested: false,
            current_connect_protocol: protocol_compatibility,
            last_completed_generation_id: NO_GENERATION,
            eager_assignor: EagerAssignor::new("WorkerCoordinator".to_string()),
            incremental_assignor: IncrementalCooperativeAssignor::new(max_delay),
            coordinator_discovery_timeout_ms,
            generation: Generation::no_generation(),
            state: MemberState::Unjoined,
        }
    }

    /// Set the rebalance listener.
    ///
    /// # Arguments
    /// * `listener` - The rebalance listener
    pub fn set_listener(&mut self, listener: Arc<Mutex<dyn WorkerRebalanceListener>>) {
        self.listener = Some(listener);
    }

    /// Get the REST URL of this worker.
    pub fn rest_url(&self) -> &str {
        &self.rest_url
    }

    /// Get the current generation ID.
    pub fn generation_id(&self) -> i32 {
        self.generation.generation_id()
    }

    /// Get the last completed generation ID.
    pub fn last_completed_generation_id(&self) -> i32 {
        self.last_completed_generation_id
    }

    /// Get the current member state.
    pub fn state(&self) -> MemberState {
        self.state
    }

    /// Get the current assignment snapshot.
    pub fn assignment(&self) -> Option<&ExtendedAssignment> {
        self.assignment_snapshot.as_ref()
    }

    /// Get the current configuration snapshot.
    pub fn config_snapshot(&self) -> Option<&ClusterConfigState> {
        self.config_snapshot.as_ref()
    }

    /// Set the cluster config state snapshot.
    pub fn set_config_snapshot(&mut self, snapshot: Option<ClusterConfigState>) {
        self.config_snapshot = snapshot;
    }

    /// Get a fresh configuration snapshot from the backing store.
    pub fn config_fresh_snapshot(&mut self) -> ClusterConfigState {
        self.config_snapshot = Some(self.config_storage.snapshot());
        self.config_snapshot
            .clone()
            .unwrap_or(ClusterConfigState::empty())
    }

    /// Get the leader state.
    pub fn leader_state(&self) -> Option<&LeaderState> {
        self.leader_state.as_ref()
    }

    /// Check if this worker is the leader.
    pub fn is_leader(&self) -> bool {
        self.leader_state.is_some()
    }

    /// Set the leader state.
    pub fn set_leader_state(&mut self, leader_state: Option<LeaderState>) {
        self.leader_state = leader_state;
    }

    /// Set the assignment.
    pub fn set_assignment(&mut self, assignment: Option<ExtendedAssignment>) {
        self.assignment_snapshot = assignment;
    }

    /// Check if rejoin is needed or pending.
    pub fn rejoin_needed_or_pending(&self) -> bool {
        self.rejoin_requested
            || self
                .assignment_snapshot
                .as_ref()
                .map_or(false, |a| a.failed())
    }

    /// Get the current protocol version as i16.
    pub fn current_protocol_version_i16(&self) -> i16 {
        self.current_connect_protocol.protocol_version()
    }

    /// Check if coordinator is unknown.
    pub fn coordinator_unknown(&self) -> bool {
        self.state == MemberState::Unjoined || self.generation.generation_id() == NO_GENERATION
    }

    /// Get the protocol type (always "connect" for Connect workers).
    pub fn protocol_type(&self) -> &'static str {
        "connect"
    }

    /// Get the protocol name based on current compatibility mode.
    pub fn protocol_name(&self) -> &'static str {
        self.protocol_compatibility.protocol()
    }

    /// Get the eager assignor.
    pub fn eager_assignor(&self) -> &EagerAssignor {
        &self.eager_assignor
    }

    /// Get the incremental assignor.
    pub fn incremental_assignor(&self) -> &IncrementalCooperativeAssignor {
        &self.incremental_assignor
    }

    // ========================================================================
    // AbstractCoordinator Callback Methods (P1-2 to P1-8)
    // ========================================================================

    /// Generate join group metadata based on protocol compatibility.
    ///
    /// This method is invoked when the worker needs to send its metadata to the
    /// group coordinator during the join group phase. The metadata includes:
    /// - The worker's REST URL
    /// - The config offset known to this worker
    /// - The current assignment (for incremental cooperative protocol)
    ///
    /// Java reference: WorkerCoordinator.java:182-191
    ///
    /// # Returns
    /// A vector of (protocol_name, serialized_metadata) tuples for each supported protocol
    pub fn metadata(&mut self) -> Vec<(String, Vec<u8>)> {
        // Get fresh snapshot from config storage
        self.config_snapshot = Some(self.config_storage.snapshot());

        // Create ExtendedWorkerState with current assignment
        let local_assignment_snapshot = self.assignment_snapshot.clone();
        let worker_state = ExtendedWorkerState::new(
            self.rest_url.clone(),
            self.config_snapshot.as_ref().map_or(-1, |s| s.offset()),
            local_assignment_snapshot,
        );

        // Select serialization method based on protocol compatibility
        match self.protocol_compatibility {
            ConnectProtocolCompatibility::Eager => {
                // EAGER mode uses V0 protocol (ConnectProtocol)
                // Convert ExtendedWorkerState to WorkerState for V0
                let worker_state_v0 = crate::distributed::connect_protocol::WorkerState::new(
                    worker_state.url().to_string(),
                    worker_state.offset(),
                );
                ConnectProtocol::metadata_request(&worker_state_v0)
            }
            ConnectProtocolCompatibility::Compatible => {
                // COMPATIBLE mode uses V1 protocol with sessioned=false
                IncrementalCooperativeConnectProtocol::metadata_request(&worker_state, false)
            }
            ConnectProtocolCompatibility::Sessioned => {
                // SESSIONED mode uses V2 protocol with sessioned=true
                IncrementalCooperativeConnectProtocol::metadata_request(&worker_state, true)
            }
        }
    }

    /// Handle assignment after join group completes.
    ///
    /// This callback is invoked when the worker receives its assignment from the
    /// group coordinator after successfully joining the group.
    ///
    /// For incremental cooperative protocol:
    /// - First processes revoked connectors/tasks by calling listener.onRevoked
    /// - Merges the remaining local assignment with the new assignment
    /// - Updates the assignment snapshot
    ///
    /// For eager protocol:
    /// - Directly updates the assignment snapshot
    ///
    /// Java reference: WorkerCoordinator.java:194-221
    ///
    /// # Arguments
    /// * `generation` - The generation ID of the group
    /// * `member_id` - The member ID assigned to this worker
    /// * `protocol` - The protocol name that was selected
    /// * `assignment` - The serialized assignment data
    pub fn on_join_complete(
        &mut self,
        generation: i32,
        member_id: String,
        protocol: String,
        assignment: Vec<u8>,
    ) {
        // Deserialize the assignment
        let new_assignment =
            IncrementalCooperativeConnectProtocol::deserialize_assignment(&assignment);

        // Update current protocol from the selected protocol
        self.current_connect_protocol = ConnectProtocolCompatibility::from_protocol(&protocol);

        // Clear rejoin request flag
        self.rejoin_requested = false;

        // Handle incremental cooperative protocol specific logic
        if self.current_connect_protocol != ConnectProtocolCompatibility::Eager {
            // Call onRevoked for revoked connectors/tasks first
            if !new_assignment.revoked_connectors().is_empty()
                || !new_assignment.revoked_tasks().is_empty()
            {
                if let Some(listener) = &self.listener {
                    let mut listener = listener.lock().unwrap();
                    listener.on_revoked(
                        new_assignment.leader().unwrap_or_default().to_string(),
                        new_assignment.revoked_connectors().to_vec(),
                        new_assignment.revoked_tasks().to_vec(),
                    );
                }
            }

            // Merge local snapshot with new assignment
            let local_assignment_snapshot = self.assignment_snapshot.clone();
            if let Some(local) = local_assignment_snapshot {
                // Create a mutable copy of new assignment for merging
                let merged_assignment = ExtendedAssignment::duplicate(&new_assignment);

                // Remove revoked items from local assignment and merge
                // Note: In Rust we need to create new vectors since ExtendedAssignment is immutable
                let mut merged_connectors: Vec<String> = new_assignment.connectors().to_vec();
                let mut merged_tasks: Vec<ConnectorTaskId> = new_assignment.tasks().to_vec();

                // Add remaining local connectors (not revoked)
                for connector in local.connectors() {
                    if !new_assignment
                        .revoked_connectors()
                        .contains(&connector.to_string())
                    {
                        if !merged_connectors.contains(&connector.to_string()) {
                            merged_connectors.push(connector.to_string());
                        }
                    }
                }

                // Add remaining local tasks (not revoked)
                for task in local.tasks() {
                    if !new_assignment.revoked_tasks().contains(task) {
                        if !merged_tasks.contains(task) {
                            merged_tasks.push(task.clone());
                        }
                    }
                }

                // Update assignment with merged values
                self.assignment_snapshot = Some(ExtendedAssignment::new(
                    merged_assignment.version(),
                    merged_assignment.error(),
                    merged_assignment.leader().map(|s| s.to_string()),
                    merged_assignment.leader_url().map(|s| s.to_string()),
                    merged_assignment.offset(),
                    merged_connectors,
                    merged_tasks,
                    merged_assignment.revoked_connectors().to_vec(),
                    merged_assignment.revoked_tasks().to_vec(),
                    merged_assignment.delay(),
                ));
            } else {
                self.assignment_snapshot = Some(new_assignment);
            }
        } else {
            // Eager mode: directly use new assignment
            self.assignment_snapshot = Some(new_assignment);
        }

        // Update last completed generation
        self.last_completed_generation_id = generation;

        // Update generation info
        self.generation = Generation::new(generation, member_id, Some(protocol));

        // Notify listener about the assignment
        if let Some(listener) = &self.listener {
            let mut listener = listener.lock().unwrap();
            listener.on_assigned(
                self.assignment_snapshot
                    .clone()
                    .unwrap_or_else(|| ExtendedAssignment::empty()),
                generation,
            );
        }

        // Update state to Stable
        self.state = MemberState::Stable;
    }

    /// Perform assignment when this worker is elected as leader.
    ///
    /// This callback is invoked when this worker becomes the leader of the group.
    /// The leader is responsible for computing the assignment for all members.
    ///
    /// For EAGER protocol: uses EagerAssignor
    /// For COMPATIBLE/SESSIONED protocol: uses IncrementalCooperativeAssignor
    ///
    /// Java reference: WorkerCoordinator.java:224-235
    ///
    /// # Arguments
    /// * `leader_id` - The leader member ID
    /// * `protocol` - The protocol name that was selected
    /// * `members` - List of (member_id, metadata_bytes) for all group members
    ///
    /// # Returns
    /// A map from member_id to serialized assignment bytes
    pub fn on_leader_elected(
        &mut self,
        leader_id: String,
        protocol: String,
        members: Vec<(String, Vec<u8>)>,
    ) -> HashMap<String, Vec<u8>> {
        // Determine protocol compatibility from selected protocol
        let protocol_compatibility = ConnectProtocolCompatibility::from_protocol(&protocol);

        // Convert Vec<(String, Vec<u8>)> to Vec<JoinGroupResponseMember>
        let join_members: Vec<JoinGroupResponseMember> = members
            .iter()
            .map(|(member_id, metadata)| {
                JoinGroupResponseMember::new(member_id.clone(), metadata.clone())
            })
            .collect();

        // Select the appropriate assignor based on protocol
        if protocol_compatibility == ConnectProtocolCompatibility::Eager {
            // Use EagerAssignor for EAGER protocol
            ConnectAssignor::perform_assignment(
                &self.eager_assignor,
                &leader_id,
                protocol_compatibility,
                &join_members,
                self,
            )
        } else {
            // Use IncrementalCooperativeAssignor for COMPATIBLE/SESSIONED protocols
            ConnectAssignor::perform_assignment(
                &self.incremental_assignor,
                &leader_id,
                protocol_compatibility,
                &join_members,
                self,
            )
        }
    }

    /// Prepare for join group.
    ///
    /// This callback is invoked before the worker joins the group.
    /// For EAGER protocol: revokes all previous assignments
    /// For incremental cooperative protocol: keeps current assignment
    ///
    /// Java reference: WorkerCoordinator.java:238-249
    ///
    /// # Arguments
    /// * `generation` - The current generation ID
    /// * `member_id` - The current member ID
    ///
    /// # Returns
    /// true to continue with the join, false to abort
    pub fn on_join_prepare(&mut self, generation: i32, member_id: String) -> bool {
        // Clear leader state
        self.leader_state = None;

        // Handle protocol-specific preparation
        if self.current_connect_protocol == ConnectProtocolCompatibility::Eager {
            // Eager mode: revoke previous assignment before rejoin
            if let Some(local_assignment) = &self.assignment_snapshot {
                if !local_assignment.failed() {
                    if let Some(listener) = &self.listener {
                        let mut listener = listener.lock().unwrap();
                        listener.on_revoked(
                            local_assignment.leader().unwrap_or_default().to_string(),
                            local_assignment.connectors().to_vec(),
                            local_assignment.tasks().to_vec(),
                        );
                    }
                }
            }
        }
        // For incremental cooperative mode: keep assignment until explicitly revoked

        // Log for debugging
        let _ = generation;
        let _ = member_id;

        // Always return true to continue with the join
        true
    }

    /// Ensure coordinator is ready for communication.
    ///
    /// This method checks if the coordinator is known and ready for communication.
    /// If the coordinator is unknown, it attempts to discover the coordinator.
    ///
    /// Java reference: AbstractCoordinator.ensureCoordinatorReady()
    ///
    /// # Arguments
    /// * `timeout_ms` - Timeout in milliseconds for coordinator discovery
    ///
    /// # Returns
    /// true if coordinator is ready, false if discovery failed
    pub fn ensure_coordinator_ready(&mut self, timeout_ms: i64) -> bool {
        // Check if coordinator is already known
        if !self.coordinator_unknown() {
            return true;
        }

        // In mock/simplified implementation, we simulate coordinator discovery
        // by transitioning to Joining state
        self.state = MemberState::Joining;

        // Simulate successful discovery (in real implementation would poll network client)
        let _ = timeout_ms;
        true
    }

    /// Handle poll timeout expiry.
    ///
    /// This callback is invoked when the worker experiences a poll timeout,
    /// indicating that it has lost contact with the group coordinator.
    ///
    /// Java reference: WorkerCoordinator.java:268-278
    pub fn handle_poll_timeout_expiry(&mut self) {
        // Notify listener about the timeout
        if let Some(listener) = &self.listener {
            let mut listener = listener.lock().unwrap();
            listener.on_poll_timeout_expiry();
        }

        // Reset state since we've lost coordinator contact
        self.state = MemberState::Unjoined;
        self.generation = Generation::no_generation();
    }

    /// Check if rejoin is needed or pending.
    ///
    /// This method determines whether the worker needs to rejoin the group.
    /// A rejoin is needed when:
    /// - The parent class determines rejoin is needed (state not stable)
    /// - The assignment snapshot is null or failed
    /// - A rejoin has been explicitly requested
    ///
    /// Java reference: WorkerCoordinator.java:279-284
    ///
    /// # Returns
    /// true if rejoin is needed, false otherwise
    pub fn rejoin_needed_or_pending_full(&self) -> bool {
        // Check state (equivalent to super.rejoinNeededOrPending())
        let state_needs_rejoin = self.state != MemberState::Stable;

        // Check assignment snapshot
        let assignment_needs_rejoin = self.assignment_snapshot.is_none()
            || self
                .assignment_snapshot
                .as_ref()
                .map_or(false, |a| a.failed());

        // Check if rejoin was explicitly requested
        state_needs_rejoin || assignment_needs_rejoin || self.rejoin_requested
    }

    /// Invert an assignment mapping from (owner -> items) to (item -> owner).
    ///
    /// This is a utility method used during assignment computation to convert
    /// the assignment format from member-based to item-based lookup.
    ///
    /// Java reference: WorkerCoordinator.java:351-361
    ///
    /// # Arguments
    /// * `assignment` - Map from owner (member) to list of items (connectors/tasks)
    ///
    /// # Returns
    /// A map from item to owner
    pub fn invert_assignment(assignment: &HashMap<String, Vec<String>>) -> HashMap<String, String> {
        let mut inverted = HashMap::new();
        for (owner, items) in assignment.iter() {
            for item in items.iter() {
                inverted.insert(item.clone(), owner.clone());
            }
        }
        inverted
    }
}

impl WorkerCoordinatorState for WorkerCoordinator {
    fn config_offset(&self) -> i64 {
        self.config_snapshot
            .as_ref()
            .map_or(ClusterConfigState::NO_OFFSET, |s| s.offset())
    }

    fn member_id(&self) -> &str {
        self.generation.member_id()
    }

    fn is_leader(&self) -> bool {
        self.leader_state.is_some()
    }

    fn url(&self) -> &str {
        &self.rest_url
    }

    fn assigned_connectors(&self) -> &[String] {
        self.assignment_snapshot
            .as_ref()
            .map_or(&[], |a| a.connectors())
    }

    fn assigned_tasks(&self) -> &[ConnectorTaskId] {
        self.assignment_snapshot.as_ref().map_or(&[], |a| a.tasks())
    }
}

impl WorkerCoordinatorTrait for WorkerCoordinator {
    fn poll(&mut self, timeout: i64, on_poll: Option<Arc<dyn Fn() + Send + Sync>>) {
        // In the real implementation, this would:
        // 1. Check if coordinator is unknown and discover it
        // 2. Check if rejoin is needed and ensure active group
        // 3. Run heartbeat
        // This is a simplified implementation for the framework
        if self.coordinator_unknown() {
            // Attempt to discover coordinator
            self.state = MemberState::Joining;
        }

        if self.rejoin_needed_or_pending() {
            // Ensure active group
            self.rejoin_requested = false;
            self.state = MemberState::Stable;
        }

        // Invoke on_poll callback if provided
        if let Some(callback) = on_poll {
            callback();
        }

        // Heartbeat logic would go here in full implementation
        // For now, we just track the timeout
        let _ = timeout;
    }

    fn member_id(&self) -> String {
        self.generation.member_id().to_string()
    }

    fn request_rejoin(&mut self, reason: &str) {
        self.rejoin_requested = true;
        // In real implementation, would log the reason
        let _ = reason;
    }

    fn maybe_leave_group(&mut self, leave_reason: &str) {
        if self.state != MemberState::Unjoined {
            self.state = MemberState::Leaving;
            // In real implementation, would send LeaveGroupRequest
            self.state = MemberState::Unjoined;
            self.generation = Generation::no_generation();
            self.assignment_snapshot = None;
            self.leader_state = None;
        }
        let _ = leave_reason;
    }

    fn owner_url(&self, connector: &str) -> String {
        self.leader_state
            .as_ref()
            .and_then(|ls| ls.owner_url_for_connector(connector))
            .unwrap_or_default()
    }

    fn owner_url_for_task(&self, task: &ConnectorTaskId) -> String {
        // Convert ConnectorTaskId to mock ConnectorTaskId since LeaderState uses mock types
        let mock_task_id = kafka_clients_mock::coordinator::ConnectorTaskId::new(
            task.connector().to_string(),
            task.task(),
        );
        self.leader_state
            .as_ref()
            .and_then(|ls| ls.owner_url_for_task(&mock_task_id))
            .unwrap_or_default()
    }

    fn current_protocol_version(&self) -> i16 {
        self.current_connect_protocol.protocol_version()
    }

    fn revoke_assignment(&mut self, assignment: &ExtendedAssignment) {
        if let Some(listener) = &self.listener {
            let mut listener = listener.lock().unwrap();
            listener.on_revoked(
                assignment.leader().unwrap_or_default().to_string(),
                assignment.revoked_connectors().to_vec(),
                assignment.revoked_tasks().to_vec(),
            );
        }
    }

    fn close(&mut self) {
        self.maybe_leave_group("coordinator closed");
        self.assignment_snapshot = None;
        self.config_snapshot = None;
        self.leader_state = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::extended_assignment::{CONNECT_PROTOCOL_V1, NO_ERROR};
    use common_trait::storage::{ClusterConfigState, ConfigBackingStore};
    use std::collections::HashMap;

    /// Mock ConfigBackingStore for testing
    struct MockConfigBackingStore;

    impl ConfigBackingStore for MockConfigBackingStore {
        fn start(&mut self) {}

        fn stop(&mut self) {}

        fn snapshot(&self) -> ClusterConfigState {
            ClusterConfigState::empty()
        }

        fn contains(&self, _connector: &str) -> bool {
            false
        }

        fn put_connector_config(
            &mut self,
            _connector: &str,
            _properties: HashMap<String, String>,
            _target_state: Option<common_trait::storage::TargetState>,
        ) {
        }

        fn remove_connector_config(&mut self, _connector: &str) {}

        fn put_task_configs(&mut self, _connector: &str, _configs: Vec<HashMap<String, String>>) {}

        fn remove_task_configs(&mut self, _connector: &str) {}

        fn refresh(&mut self, _timeout: std::time::Duration) -> Result<(), std::io::Error> {
            Ok(())
        }

        fn put_target_state(
            &mut self,
            _connector: &str,
            _state: common_trait::storage::TargetState,
        ) {
        }

        fn put_session_key(&mut self, _key: common_trait::storage::SessionKey) {}

        fn put_restart_request(&mut self, _request: common_trait::storage::RestartRequest) {}

        fn put_task_count_record(&mut self, _connector: &str, _task_count: u32) {}

        fn put_logger_level(&mut self, _namespace: &str, _level: &str) {}

        fn set_update_listener(
            &mut self,
            _listener: Box<dyn common_trait::storage::ConfigBackingStoreUpdateListener>,
        ) {
        }
    }

    #[test]
    fn test_worker_coordinator_creation() {
        let config_storage = Arc::new(MockConfigBackingStore);
        let coordinator = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage,
            ConnectProtocolCompatibility::Compatible,
            100,
            5000,
        );

        assert_eq!(coordinator.rest_url(), "http://localhost:8083");
        assert_eq!(coordinator.protocol_type(), "connect");
        assert_eq!(coordinator.protocol_name(), "compatible");
        assert_eq!(coordinator.generation_id(), NO_GENERATION);
        assert_eq!(coordinator.last_completed_generation_id(), NO_GENERATION);
        assert!(!coordinator.is_leader());
        assert!(coordinator.assignment().is_none());
        assert!(coordinator.coordinator_unknown());
    }

    #[test]
    fn test_protocol_compatibility() {
        let config_storage = Arc::new(MockConfigBackingStore);

        let coordinator_eager = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage.clone(),
            ConnectProtocolCompatibility::Eager,
            100,
            5000,
        );
        assert_eq!(coordinator_eager.current_protocol_version(), 0);

        let coordinator_compatible = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage.clone(),
            ConnectProtocolCompatibility::Compatible,
            100,
            5000,
        );
        assert_eq!(coordinator_compatible.current_protocol_version(), 1);

        let coordinator_sessioned = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage,
            ConnectProtocolCompatibility::Sessioned,
            100,
            5000,
        );
        assert_eq!(coordinator_sessioned.current_protocol_version(), 2);
    }

    #[test]
    fn test_request_rejoin() {
        let config_storage = Arc::new(MockConfigBackingStore);
        let mut coordinator = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage,
            ConnectProtocolCompatibility::Compatible,
            100,
            5000,
        );

        assert!(!coordinator.rejoin_needed_or_pending());
        coordinator.request_rejoin("test reason");
        assert!(coordinator.rejoin_needed_or_pending());
    }

    #[test]
    fn test_set_assignment() {
        let config_storage = Arc::new(MockConfigBackingStore);
        let mut coordinator = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage,
            ConnectProtocolCompatibility::Compatible,
            100,
            5000,
        );

        assert!(coordinator.assignment().is_none());

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

        coordinator.set_assignment(Some(assignment));
        assert!(coordinator.assignment().is_some());
        assert_eq!(coordinator.assignment().unwrap().connectors().len(), 1);
    }

    #[test]
    fn test_failed_assignment_triggers_rejoin() {
        let config_storage = Arc::new(MockConfigBackingStore);
        let mut coordinator = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage,
            ConnectProtocolCompatibility::Compatible,
            100,
            5000,
        );

        // Set a failed assignment (CONFIG_MISMATCH)
        let failed_assignment = ExtendedAssignment::new(
            CONNECT_PROTOCOL_V1,
            crate::distributed::extended_assignment::CONFIG_MISMATCH,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            -1,
            vec![],
            vec![],
            vec![],
            vec![],
            0,
        );

        coordinator.set_assignment(Some(failed_assignment));
        assert!(coordinator.assignment().unwrap().failed());
        assert!(coordinator.rejoin_needed_or_pending());
    }

    #[test]
    fn test_maybe_leave_group() {
        let config_storage = Arc::new(MockConfigBackingStore);
        let mut coordinator = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage,
            ConnectProtocolCompatibility::Compatible,
            100,
            5000,
        );

        // Initially in Unjoined state
        assert_eq!(coordinator.state(), MemberState::Unjoined);

        // Simulate join
        coordinator.state = MemberState::Stable;
        coordinator.generation =
            Generation::new(1, "member-1".to_string(), Some("compatible".to_string()));

        // Leave group
        coordinator.maybe_leave_group("test leave");
        assert_eq!(coordinator.state(), MemberState::Unjoined);
        assert!(coordinator.assignment().is_none());
    }

    #[test]
    fn test_poll_when_coordinator_unknown() {
        let config_storage = Arc::new(MockConfigBackingStore);
        let mut coordinator = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage,
            ConnectProtocolCompatibility::Compatible,
            100,
            5000,
        );

        assert!(coordinator.coordinator_unknown());
        coordinator.poll(1000, None);
        // Should transition to Joining state
        assert_eq!(coordinator.state(), MemberState::Joining);
    }

    #[test]
    fn test_config_fresh_snapshot() {
        let config_storage = Arc::new(MockConfigBackingStore);
        let mut coordinator = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage,
            ConnectProtocolCompatibility::Compatible,
            100,
            5000,
        );

        assert!(coordinator.config_snapshot().is_none());
        let snapshot = coordinator.config_fresh_snapshot();
        assert_eq!(snapshot.offset(), ClusterConfigState::NO_OFFSET);
        assert!(coordinator.config_snapshot().is_some());
    }

    #[test]
    fn test_worker_coordinator_state_trait() {
        let config_storage = Arc::new(MockConfigBackingStore);
        let coordinator = WorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            config_storage,
            ConnectProtocolCompatibility::Compatible,
            100,
            5000,
        );

        assert_eq!(coordinator.config_offset(), ClusterConfigState::NO_OFFSET);
        assert_eq!(
            WorkerCoordinatorState::member_id(&coordinator),
            UNKNOWN_MEMBER_ID
        );
        assert!(!coordinator.is_leader());
        assert_eq!(coordinator.url(), "http://localhost:8083");
        assert!(coordinator.assigned_connectors().is_empty());
        assert!(coordinator.assigned_tasks().is_empty());
    }
}
