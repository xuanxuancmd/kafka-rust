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

//! Mock implementation of WorkerCoordinator for testing purposes.
//!
//! This is a simplified version that provides the core coordinator functionality
//! needed by Kafka Connect's distributed runtime without the complexity of
//! AbstractCoordinator's full heartbeat and rebalance mechanisms.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.distributed.WorkerCoordinator` in Java,
//! but simplified for mock testing scenarios.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Unknown member ID constant (matches Java JoinGroupRequest.UNKNOWN_MEMBER_ID)
pub const UNKNOWN_MEMBER_ID: &str = "";

/// No generation constant (matches Java Generation.NO_GENERATION.generationId)
pub const NO_GENERATION: i32 = -1;

/// Protocol type for Connect workers
pub const PROTOCOL_TYPE: &str = "connect";

/// Member state in the coordinator group
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

/// Generation information for the coordinator
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generation {
    /// The generation ID
    generation_id: i32,
    /// The member ID assigned by the coordinator
    member_id: String,
    /// The protocol name selected by the group
    protocol: Option<String>,
}

impl Generation {
    /// Creates a new Generation.
    pub fn new(generation_id: i32, member_id: String, protocol: Option<String>) -> Self {
        Generation {
            generation_id,
            member_id,
            protocol,
        }
    }

    /// Returns the generation ID.
    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// Returns the member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the protocol name.
    pub fn protocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }

    /// No generation sentinel value
    pub fn no_generation() -> Self {
        Generation::new(NO_GENERATION, UNKNOWN_MEMBER_ID.to_string(), None)
    }
}

impl Default for Generation {
    fn default() -> Self {
        Self::no_generation()
    }
}

/// Connector task ID (re-exported from connect-runtime distributed module for convenience)
/// This is a simplified version for mock use.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectorTaskId {
    connector: String,
    task: i32,
}

impl ConnectorTaskId {
    pub fn new(connector: String, task: i32) -> Self {
        ConnectorTaskId { connector, task }
    }

    pub fn connector(&self) -> &str {
        &self.connector
    }

    pub fn task(&self) -> i32 {
        self.task
    }
}

impl std::fmt::Display for ConnectorTaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.connector, self.task)
    }
}

/// Extended assignment of connectors and tasks (simplified version for mock)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendedAssignment {
    /// Connect protocol version
    version: i16,
    /// Error code for this assignment
    error: i16,
    /// Connect group's leader Id
    leader: Option<String>,
    /// Connect group's leader URL
    leader_url: Option<String>,
    /// The offset in the config topic
    config_offset: i64,
    /// List of connectors assigned to this worker
    connectors: Vec<String>,
    /// List of tasks assigned to this worker
    tasks: Vec<ConnectorTaskId>,
    /// List of connectors revoked from this worker
    revoked_connectors: Vec<String>,
    /// List of tasks revoked from this worker
    revoked_tasks: Vec<ConnectorTaskId>,
    /// Delay before rejoin
    delay: i32,
}

impl ExtendedAssignment {
    /// No error constant
    pub const NO_ERROR: i16 = 0;
    /// Config mismatch error constant
    pub const CONFIG_MISMATCH: i16 = 1;
    /// Connector task sentinel (indicates running the connector instance)
    pub const CONNECTOR_TASK: i32 = -1;
    /// Protocol version 1
    pub const CONNECT_PROTOCOL_V1: i16 = 1;

    pub fn new(
        version: i16,
        error: i16,
        leader: Option<String>,
        leader_url: Option<String>,
        config_offset: i64,
        connectors: Vec<String>,
        tasks: Vec<ConnectorTaskId>,
        revoked_connectors: Vec<String>,
        revoked_tasks: Vec<ConnectorTaskId>,
        delay: i32,
    ) -> Self {
        ExtendedAssignment {
            version,
            error,
            leader,
            leader_url,
            config_offset,
            connectors,
            tasks,
            revoked_connectors,
            revoked_tasks,
            delay,
        }
    }

    pub fn empty() -> Self {
        ExtendedAssignment::new(
            Self::CONNECT_PROTOCOL_V1,
            Self::NO_ERROR,
            None,
            None,
            -1,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
        )
    }

    pub fn version(&self) -> i16 {
        self.version
    }

    pub fn error(&self) -> i16 {
        self.error
    }

    pub fn leader(&self) -> Option<&str> {
        self.leader.as_deref()
    }

    pub fn leader_url(&self) -> Option<&str> {
        self.leader_url.as_deref()
    }

    pub fn failed(&self) -> bool {
        self.error != Self::NO_ERROR
    }

    pub fn offset(&self) -> i64 {
        self.config_offset
    }

    pub fn connectors(&self) -> &[String] {
        &self.connectors
    }

    pub fn tasks(&self) -> &[ConnectorTaskId] {
        &self.tasks
    }

    pub fn revoked_connectors(&self) -> &[String] {
        &self.revoked_connectors
    }

    pub fn revoked_tasks(&self) -> &[ConnectorTaskId] {
        &self.revoked_tasks
    }

    pub fn delay(&self) -> i32 {
        self.delay
    }
}

impl Default for ExtendedAssignment {
    fn default() -> Self {
        Self::empty()
    }
}

impl std::fmt::Display for ExtendedAssignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Assignment{{error={}, leader='{}', leaderUrl='{}', offset={}, connectors={:?}, tasks={:?}, revokedConnectors={:?}, revokedTasks={:?}, delay={}}}",
            self.error,
            self.leader.as_deref().unwrap_or("null"),
            self.leader_url.as_deref().unwrap_or("null"),
            self.config_offset,
            self.connectors,
            self.tasks,
            self.revoked_connectors,
            self.revoked_tasks,
            self.delay
        )
    }
}

/// Worker state for extended protocol
#[derive(Debug, Clone)]
pub struct ExtendedWorkerState {
    url: String,
    config_offset: i64,
    assignment: Option<ExtendedAssignment>,
}

impl ExtendedWorkerState {
    pub fn new(url: String, config_offset: i64, assignment: Option<ExtendedAssignment>) -> Self {
        ExtendedWorkerState {
            url,
            config_offset,
            assignment,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn config_offset(&self) -> i64 {
        self.config_offset
    }

    pub fn assignment(&self) -> Option<&ExtendedAssignment> {
        self.assignment.as_ref()
    }
}

/// Leader state containing information about all members and their assignments
#[derive(Debug, Clone)]
pub struct LeaderState {
    all_members: HashMap<String, ExtendedWorkerState>,
    connector_owners: HashMap<String, String>,
    task_owners: HashMap<ConnectorTaskId, String>,
}

impl LeaderState {
    pub fn new(
        all_members: HashMap<String, ExtendedWorkerState>,
        connector_assignment: HashMap<String, Vec<String>>,
        task_assignment: HashMap<String, Vec<ConnectorTaskId>>,
    ) -> Self {
        let connector_owners = Self::invert_assignment(&connector_assignment);
        let task_owners = Self::invert_assignment(&task_assignment);
        LeaderState {
            all_members,
            connector_owners,
            task_owners,
        }
    }

    fn invert_assignment<K, V>(assignment: &HashMap<K, Vec<V>>) -> HashMap<V, K>
    where
        K: Clone + std::hash::Hash + Eq,
        V: Clone + std::hash::Hash + Eq,
    {
        let mut inverted = HashMap::new();
        for (key, values) in assignment {
            for value in values {
                inverted.insert(value.clone(), key.clone());
            }
        }
        inverted
    }

    pub fn owner_url_for_connector(&self, connector: &str) -> Option<String> {
        let owner_id = self.connector_owners.get(connector)?;
        self.all_members.get(owner_id)?.url().to_string().into()
    }

    pub fn owner_url_for_task(&self, task: &ConnectorTaskId) -> Option<String> {
        let owner_id = self.task_owners.get(task)?;
        self.all_members.get(owner_id)?.url().to_string().into()
    }

    pub fn all_members(&self) -> &HashMap<String, ExtendedWorkerState> {
        &self.all_members
    }

    pub fn connector_owners(&self) -> &HashMap<String, String> {
        &self.connector_owners
    }

    pub fn task_owners(&self) -> &HashMap<ConnectorTaskId, String> {
        &self.task_owners
    }
}

/// Connectors and tasks container
#[derive(Debug, Clone, Default)]
pub struct ConnectorsAndTasks {
    connectors: Vec<String>,
    tasks: Vec<ConnectorTaskId>,
}

impl ConnectorsAndTasks {
    pub fn new(connectors: Vec<String>, tasks: Vec<ConnectorTaskId>) -> Self {
        ConnectorsAndTasks { connectors, tasks }
    }

    pub fn empty() -> Self {
        ConnectorsAndTasks::new(Vec::new(), Vec::new())
    }

    pub fn connectors(&self) -> &[String] {
        &self.connectors
    }

    pub fn tasks(&self) -> &[ConnectorTaskId] {
        &self.tasks
    }

    pub fn size(&self) -> usize {
        self.connectors.len() + self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty() && self.tasks.is_empty()
    }
}

/// Worker load information
#[derive(Debug, Clone)]
pub struct WorkerLoad {
    worker: String,
    connectors: Vec<String>,
    tasks: Vec<ConnectorTaskId>,
}

impl WorkerLoad {
    pub fn new(worker: String, connectors: Vec<String>, tasks: Vec<ConnectorTaskId>) -> Self {
        WorkerLoad {
            worker,
            connectors,
            tasks,
        }
    }

    pub fn worker(&self) -> &str {
        &self.worker
    }

    pub fn connectors(&self) -> &[String] {
        &self.connectors
    }

    pub fn tasks(&self) -> &[ConnectorTaskId] {
        &self.tasks
    }

    pub fn size(&self) -> usize {
        self.connectors.len() + self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty() && self.tasks.is_empty()
    }

    pub fn assign_connector(&mut self, connector: String) {
        self.connectors.push(connector);
    }

    pub fn assign_task(&mut self, task: ConnectorTaskId) {
        self.tasks.push(task);
    }
}

/// Connect protocol compatibility modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectProtocolCompatibility {
    Eager,
    Compatible,
    Sessioned,
}

impl ConnectProtocolCompatibility {
    pub fn protocol(&self) -> &'static str {
        match self {
            ConnectProtocolCompatibility::Eager => "default",
            ConnectProtocolCompatibility::Compatible => "compatible",
            ConnectProtocolCompatibility::Sessioned => "sessioned",
        }
    }

    pub fn protocol_version(&self) -> i16 {
        match self {
            ConnectProtocolCompatibility::Eager => 0,
            ConnectProtocolCompatibility::Compatible => 1,
            ConnectProtocolCompatibility::Sessioned => 2,
        }
    }

    pub fn from_protocol(protocol: &str) -> Self {
        match protocol.to_lowercase().as_str() {
            "default" => ConnectProtocolCompatibility::Eager,
            "compatible" => ConnectProtocolCompatibility::Compatible,
            "sessioned" => ConnectProtocolCompatibility::Sessioned,
            _ => ConnectProtocolCompatibility::Eager,
        }
    }
}

impl Default for ConnectProtocolCompatibility {
    fn default() -> Self {
        ConnectProtocolCompatibility::Compatible
    }
}

/// Response from join group operation
#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    /// Generation ID assigned to the member
    generation_id: i32,
    /// Member ID assigned by the coordinator
    member_id: String,
    /// Leader ID of the group
    leader_id: String,
    /// Members in the group
    members: Vec<GroupMember>,
    /// Protocol selected for the group
    protocol: String,
}

impl JoinGroupResponse {
    pub fn new(
        generation_id: i32,
        member_id: String,
        leader_id: String,
        members: Vec<GroupMember>,
        protocol: String,
    ) -> Self {
        JoinGroupResponse {
            generation_id,
            member_id,
            leader_id,
            members,
            protocol,
        }
    }

    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    pub fn leader_id(&self) -> &str {
        &self.leader_id
    }

    pub fn is_leader(&self) -> bool {
        self.member_id == self.leader_id
    }

    pub fn members(&self) -> &[GroupMember] {
        &self.members
    }

    pub fn protocol(&self) -> &str {
        &self.protocol
    }
}

/// Group member information
#[derive(Debug, Clone)]
pub struct GroupMember {
    member_id: String,
    metadata: Vec<u8>,
}

impl GroupMember {
    pub fn new(member_id: String, metadata: Vec<u8>) -> Self {
        GroupMember {
            member_id,
            metadata,
        }
    }

    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }
}

/// Response from sync_group operation
#[derive(Debug, Clone)]
pub struct SyncGroupResponse {
    /// Assignment data for this member
    assignment: Vec<u8>,
    /// Error code (0 for success)
    error: i16,
}

impl SyncGroupResponse {
    pub fn new(assignment: Vec<u8>, error: i16) -> Self {
        SyncGroupResponse { assignment, error }
    }

    pub fn assignment(&self) -> &[u8] {
        &self.assignment
    }

    pub fn error(&self) -> i16 {
        self.error
    }

    pub fn is_success(&self) -> bool {
        self.error == ExtendedAssignment::NO_ERROR
    }
}

/// Worker rebalance listener trait (callback interface)
pub trait WorkerRebalanceListener: Send + Sync {
    fn on_assigned(&mut self, assignment: ExtendedAssignment, generation: i32);
    fn on_revoked(&mut self, leader: String, connectors: Vec<String>, tasks: Vec<ConnectorTaskId>);
    fn on_poll_timeout_expiry(&mut self);
}

/// Mock implementation of WorkerRebalanceListener for testing
pub struct MockWorkerRebalanceListener {
    assigned_count: i32,
    revoked_count: i32,
    timeout_count: i32,
    last_assignment: Option<ExtendedAssignment>,
    last_generation: i32,
}

impl MockWorkerRebalanceListener {
    pub fn new() -> Self {
        MockWorkerRebalanceListener {
            assigned_count: 0,
            revoked_count: 0,
            timeout_count: 0,
            last_assignment: None,
            last_generation: 0,
        }
    }

    pub fn assigned_count(&self) -> i32 {
        self.assigned_count
    }

    pub fn revoked_count(&self) -> i32 {
        self.revoked_count
    }

    pub fn timeout_count(&self) -> i32 {
        self.timeout_count
    }

    pub fn last_assignment(&self) -> Option<&ExtendedAssignment> {
        self.last_assignment.as_ref()
    }

    pub fn last_generation(&self) -> i32 {
        self.last_generation
    }

    pub fn reset(&mut self) {
        self.assigned_count = 0;
        self.revoked_count = 0;
        self.timeout_count = 0;
        self.last_assignment = None;
        self.last_generation = 0;
    }
}

impl Default for MockWorkerRebalanceListener {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerRebalanceListener for MockWorkerRebalanceListener {
    fn on_assigned(&mut self, assignment: ExtendedAssignment, generation: i32) {
        self.assigned_count += 1;
        self.last_assignment = Some(assignment);
        self.last_generation = generation;
    }

    fn on_revoked(
        &mut self,
        _leader: String,
        _connectors: Vec<String>,
        _tasks: Vec<ConnectorTaskId>,
    ) {
        self.revoked_count += 1;
    }

    fn on_poll_timeout_expiry(&mut self) {
        self.timeout_count += 1;
    }
}

/// Mock Worker Coordinator - simplified implementation for testing
///
/// This provides a mock implementation of the coordinator functionality needed
/// by Kafka Connect's distributed runtime. It simulates:
/// - Join group operations
/// - Sync group operations for assignment
/// - Leave group operations
/// - Basic poll heartbeat simulation
/// - Rebalance triggering
///
/// The implementation uses in-memory state and does not actually communicate
/// with a Kafka broker coordinator.
pub struct MockWorkerCoordinator {
    /// Worker's REST URL
    rest_url: String,
    /// Current assignment snapshot
    assignment_snapshot: Option<ExtendedAssignment>,
    /// Current generation
    generation: Generation,
    /// Current member state
    state: MemberState,
    /// Whether rejoin has been requested
    rejoin_requested: bool,
    /// Protocol compatibility mode
    protocol_compatibility: ConnectProtocolCompatibility,
    /// Current connect protocol version
    current_connect_protocol: ConnectProtocolCompatibility,
    /// Last completed generation ID
    last_completed_generation_id: i32,
    /// Leader state (if this worker is leader)
    leader_state: Option<LeaderState>,
    /// Rebalance listener
    listener: Option<Arc<Mutex<dyn WorkerRebalanceListener>>>,
    /// Group ID
    group_id: String,
    /// Config offset
    config_offset: i64,
    /// Mock group members (for testing)
    mock_members: Vec<GroupMember>,
    /// Mock assignment data (for testing)
    mock_assignment_data: Vec<u8>,
}

impl MockWorkerCoordinator {
    /// Create a new MockWorkerCoordinator
    ///
    /// # Arguments
    /// * `rest_url` - Worker's REST URL
    /// * `group_id` - Consumer group ID for coordination
    /// * `protocol_compatibility` - Connect protocol compatibility mode
    pub fn new(
        rest_url: String,
        group_id: String,
        protocol_compatibility: ConnectProtocolCompatibility,
    ) -> Self {
        MockWorkerCoordinator {
            rest_url,
            assignment_snapshot: None,
            generation: Generation::no_generation(),
            state: MemberState::Unjoined,
            rejoin_requested: false,
            protocol_compatibility,
            current_connect_protocol: protocol_compatibility,
            last_completed_generation_id: NO_GENERATION,
            leader_state: None,
            listener: None,
            group_id,
            config_offset: 0,
            mock_members: Vec::new(),
            mock_assignment_data: Vec::new(),
        }
    }

    /// Set the rebalance listener
    pub fn set_listener(&mut self, listener: Arc<Mutex<dyn WorkerRebalanceListener>>) {
        self.listener = Some(listener);
    }

    /// Set mock members for testing join_group
    pub fn set_mock_members(&mut self, members: Vec<GroupMember>) {
        self.mock_members = members;
    }

    /// Set mock assignment data for testing sync_group
    pub fn set_mock_assignment_data(&mut self, data: Vec<u8>) {
        self.mock_assignment_data = data;
    }

    /// Set the config offset
    pub fn set_config_offset(&mut self, offset: i64) {
        self.config_offset = offset;
    }

    /// Protocol type (always "connect" for Connect workers)
    pub fn protocol_type(&self) -> &'static str {
        PROTOCOL_TYPE
    }

    /// Get current member ID
    pub fn member_id(&self) -> &str {
        self.generation.member_id()
    }

    /// Get current generation ID
    pub fn generation_id(&self) -> i32 {
        self.generation.generation_id()
    }

    /// Get last completed generation ID
    pub fn last_completed_generation_id(&self) -> i32 {
        self.last_completed_generation_id
    }

    /// Get current member state
    pub fn state(&self) -> MemberState {
        self.state
    }

    /// Get current assignment
    pub fn assignment(&self) -> Option<&ExtendedAssignment> {
        self.assignment_snapshot.as_ref()
    }

    /// Check if coordinator is unknown (not joined)
    pub fn coordinator_unknown(&self) -> bool {
        self.state == MemberState::Unjoined || self.state == MemberState::Leaving
    }

    /// Check if this worker is the leader
    pub fn is_leader(&self) -> bool {
        match &self.assignment_snapshot {
            Some(assignment) => assignment.leader() == Some(self.member_id()),
            None => false,
        }
    }

    /// Get leader state (if leader)
    pub fn leader_state(&self) -> Option<&LeaderState> {
        self.leader_state.as_ref()
    }

    /// Set leader state
    pub fn set_leader_state(&mut self, state: Option<LeaderState>) {
        self.leader_state = state;
    }

    /// Get owner URL for a connector
    pub fn owner_url_for_connector(&self, connector: &str) -> Option<String> {
        if self.rejoin_needed_or_pending() || !self.is_leader() {
            return None;
        }
        self.leader_state()?.owner_url_for_connector(connector)
    }

    /// Get owner URL for a task
    pub fn owner_url_for_task(&self, task: &ConnectorTaskId) -> Option<String> {
        if self.rejoin_needed_or_pending() || !self.is_leader() {
            return None;
        }
        self.leader_state()?.owner_url_for_task(task)
    }

    /// Get current protocol version
    pub fn current_protocol_version(&self) -> i16 {
        self.current_connect_protocol.protocol_version()
    }

    /// Request rejoin of the group
    pub fn request_rejoin(&mut self, reason: &str) {
        // Debug: Request joining group due to reason
        self.rejoin_requested = true;
    }

    /// Check if rejoin is needed or pending
    pub fn rejoin_needed_or_pending(&self) -> bool {
        self.state != MemberState::Stable
            || self.assignment_snapshot.is_none()
            || self
                .assignment_snapshot
                .as_ref()
                .map(|a| a.failed())
                .unwrap_or(false)
            || self.rejoin_requested
    }

    /// Join the coordination group
    ///
    /// This simulates the join group operation. In mock mode, it generates
    /// a member ID and assigns this member as leader if no mock members are set.
    pub fn join_group(&mut self) -> Result<JoinGroupResponse, String> {
        if self.state != MemberState::Unjoined && self.state != MemberState::Leaving {
            return Err(
                "Cannot join group while in state: ".to_string() + &format!("{:?}", self.state)
            );
        }

        self.state = MemberState::Joining;

        // Generate member ID (mock: simple incrementing)
        let member_id = if self.mock_members.is_empty() {
            format!(
                "worker-{}",
                self.rest_url.replace("http://", "").replace(":", "-")
            )
        } else {
            format!("member-{}", self.mock_members.len() + 1)
        };

        // Determine leader (mock: first member or this worker if alone)
        let leader_id = if self.mock_members.is_empty() {
            member_id.clone()
        } else {
            self.mock_members.first().unwrap().member_id().to_string()
        };

        // Add this member to mock members
        let self_member = GroupMember::new(member_id.clone(), self.serialize_metadata());

        let mut members = self.mock_members.clone();
        members.push(self_member);

        // Use the configured protocol
        let protocol = self.protocol_compatibility.protocol().to_string();

        // Assign generation
        let generation_id = 1; // Mock: always start with generation 1
        self.generation = Generation::new(generation_id, member_id.clone(), Some(protocol.clone()));

        self.state = MemberState::Syncing;

        Ok(JoinGroupResponse::new(
            generation_id,
            member_id,
            leader_id,
            members,
            protocol,
        ))
    }

    /// Sync the group after join
    ///
    /// This simulates receiving assignment from the leader. In mock mode,
    /// if this worker is the leader, it assigns all connectors/tasks to itself.
    /// If mock_assignment_data is set, it uses that data.
    pub fn sync_group(&mut self) -> Result<SyncGroupResponse, String> {
        if self.state != MemberState::Syncing {
            return Err(
                "Cannot sync group while in state: ".to_string() + &format!("{:?}", self.state)
            );
        }

        // Mock: create assignment data
        let assignment_data = if !self.mock_assignment_data.is_empty() {
            self.mock_assignment_data.clone()
        } else {
            self.serialize_assignment()
        };

        // Parse assignment from data
        let assignment = self.parse_assignment(&assignment_data);

        // Update state
        self.assignment_snapshot = Some(assignment.clone());
        self.last_completed_generation_id = self.generation.generation_id();
        self.state = MemberState::Stable;
        self.rejoin_requested = false;

        // Notify listener
        if let Some(listener) = &self.listener {
            if let Ok(mut l) = listener.lock() {
                l.on_assigned(assignment, self.generation.generation_id());
            }
        }

        Ok(SyncGroupResponse::new(
            assignment_data,
            ExtendedAssignment::NO_ERROR,
        ))
    }

    /// Leave the coordination group
    pub fn leave_group(&mut self) -> Result<(), String> {
        if self.state == MemberState::Unjoined {
            return Ok(()); // Already not in group
        }

        self.state = MemberState::Leaving;

        // Notify listener about revocation
        if let Some(listener) = &self.listener {
            if let Some(assignment) = &self.assignment_snapshot {
                if let Ok(mut l) = listener.lock() {
                    l.on_revoked(
                        assignment.leader().unwrap_or("unknown").to_string(),
                        assignment.connectors().to_vec(),
                        assignment.tasks().to_vec(),
                    );
                }
            }
        }

        // Reset state
        self.assignment_snapshot = None;
        self.generation = Generation::no_generation();
        self.last_completed_generation_id = NO_GENERATION;
        self.leader_state = None;
        self.state = MemberState::Unjoined;
        self.rejoin_requested = false;

        Ok(())
    }

    /// Poll for heartbeat (simplified mock implementation)
    ///
    /// In the real implementation, this would:
    /// 1. Check coordinator readiness
    /// 2. Send heartbeat if needed
    /// 3. Process any pending rebalance
    ///
    /// In mock mode, it just checks if rejoin is needed and triggers it.
    pub fn poll(&mut self, timeout_ms: i64) -> Result<(), String> {
        // Check if coordinator is unknown
        if self.coordinator_unknown() {
            return Err("Coordinator unknown during poll".to_string());
        }

        // Check if rejoin is needed
        if self.rejoin_needed_or_pending() {
            self.ensure_active_group()?;
        }

        // Mock: simulate heartbeat by just returning success
        Ok(())
    }

    /// Ensure the worker has an active group membership
    ///
    /// This performs join_group and sync_group if needed.
    pub fn ensure_active_group(&mut self) -> Result<(), String> {
        // If rejoin is requested, need to go through join/sync flow again
        // This matches Java WorkerCoordinator behavior where rejoinRequested triggers full rejoin
        if self.rejoin_requested && self.state == MemberState::Stable {
            // Reset state to trigger rejoin flow
            // In Java, this happens via onJoinPrepare -> join_group -> sync_group
            self.state = MemberState::Unjoined;
            self.rejoin_requested = false; // Clear before rejoin to avoid infinite loop
        }

        // Check if already in stable state with no rejoin needed
        if self.state == MemberState::Stable && !self.rejoin_needed_or_pending() {
            return Ok(());
        }

        // Join group if needed
        if self.state == MemberState::Unjoined || self.state == MemberState::Leaving {
            self.join_group()?;
        }

        // Sync group if in joining state
        if self.state == MemberState::Syncing {
            self.sync_group()?;
        }

        // Now should be in stable state
        if self.state != MemberState::Stable {
            return Err("Failed to establish active group".to_string());
        }

        Ok(())
    }

    /// Revoke the current assignment
    pub fn revoke_assignment(&mut self, assignment: &ExtendedAssignment) {
        if let Some(listener) = &self.listener {
            if let Ok(mut l) = listener.lock() {
                l.on_revoked(
                    assignment.leader().unwrap_or("unknown").to_string(),
                    assignment.connectors().to_vec(),
                    assignment.tasks().to_vec(),
                );
            }
        }
    }

    /// Serialize worker metadata (mock implementation)
    fn serialize_metadata(&self) -> Vec<u8> {
        // Mock: simple serialization of URL and config offset
        let mut data = Vec::new();

        // Version (i16)
        data.extend_from_slice(&self.current_protocol_version().to_be_bytes());

        // URL length (i32) + URL bytes
        let url_bytes = self.rest_url.as_bytes();
        data.extend_from_slice(&(url_bytes.len() as i32).to_be_bytes());
        data.extend_from_slice(url_bytes);

        // Config offset (i64)
        data.extend_from_slice(&self.config_offset.to_be_bytes());

        data
    }

    /// Serialize assignment (mock implementation)
    fn serialize_assignment(&self) -> Vec<u8> {
        // Mock: create empty assignment
        let assignment = ExtendedAssignment::new(
            self.current_protocol_version(),
            ExtendedAssignment::NO_ERROR,
            Some(self.member_id().to_string()),
            Some(self.rest_url.clone()),
            self.config_offset,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
        );

        self.serialize_extended_assignment(&assignment)
    }

    /// Serialize extended assignment to bytes
    fn serialize_extended_assignment(&self, assignment: &ExtendedAssignment) -> Vec<u8> {
        let mut data = Vec::new();

        // Version (i16)
        data.extend_from_slice(&assignment.version().to_be_bytes());

        // Error (i16)
        data.extend_from_slice(&assignment.error().to_be_bytes());

        // Leader (string)
        let leader = assignment.leader().unwrap_or("");
        data.extend_from_slice(&(leader.len() as i32).to_be_bytes());
        data.extend_from_slice(leader.as_bytes());

        // Leader URL (string)
        let leader_url = assignment.leader_url().unwrap_or("");
        data.extend_from_slice(&(leader_url.len() as i32).to_be_bytes());
        data.extend_from_slice(leader_url.as_bytes());

        // Config offset (i64)
        data.extend_from_slice(&assignment.offset().to_be_bytes());

        // Connectors count (i32) + connectors
        data.extend_from_slice(&(assignment.connectors().len() as i32).to_be_bytes());
        for conn in assignment.connectors() {
            data.extend_from_slice(&(conn.len() as i32).to_be_bytes());
            data.extend_from_slice(conn.as_bytes());
        }

        // Tasks count (i32) + tasks
        data.extend_from_slice(&(assignment.tasks().len() as i32).to_be_bytes());
        for task in assignment.tasks() {
            data.extend_from_slice(&(task.connector().len() as i32).to_be_bytes());
            data.extend_from_slice(task.connector().as_bytes());
            data.extend_from_slice(&task.task().to_be_bytes());
        }

        // Revoked connectors count + revoked connectors
        data.extend_from_slice(&(assignment.revoked_connectors().len() as i32).to_be_bytes());
        for conn in assignment.revoked_connectors() {
            data.extend_from_slice(&(conn.len() as i32).to_be_bytes());
            data.extend_from_slice(conn.as_bytes());
        }

        // Revoked tasks count + revoked tasks
        data.extend_from_slice(&(assignment.revoked_tasks().len() as i32).to_be_bytes());
        for task in assignment.revoked_tasks() {
            data.extend_from_slice(&(task.connector().len() as i32).to_be_bytes());
            data.extend_from_slice(task.connector().as_bytes());
            data.extend_from_slice(&task.task().to_be_bytes());
        }

        // Delay (i32)
        data.extend_from_slice(&assignment.delay().to_be_bytes());

        data
    }

    /// Parse assignment from bytes (mock implementation)
    fn parse_assignment(&self, data: &[u8]) -> ExtendedAssignment {
        if data.is_empty() {
            return ExtendedAssignment::empty();
        }

        // Mock: simple parsing - if data starts with version, parse basic fields
        let version = i16::from_be_bytes([data[0], data[1]]);
        let error = if data.len() >= 4 {
            i16::from_be_bytes([data[2], data[3]])
        } else {
            ExtendedAssignment::NO_ERROR
        };

        ExtendedAssignment::new(
            version,
            error,
            Some(self.member_id().to_string()),
            Some(self.rest_url.clone()),
            self.config_offset,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
        )
    }

    /// Set assignment directly (for testing)
    pub fn set_assignment(&mut self, assignment: ExtendedAssignment) {
        self.assignment_snapshot = Some(assignment);
        self.state = MemberState::Stable;
        self.rejoin_requested = false;
    }

    /// Get the rest URL
    pub fn rest_url(&self) -> &str {
        &self.rest_url
    }

    /// Get the group ID
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Get protocol compatibility
    pub fn protocol_compatibility(&self) -> ConnectProtocolCompatibility {
        self.protocol_compatibility
    }
}

impl std::fmt::Display for MockWorkerCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MockWorkerCoordinator{{url='{}', group='{}', state={:?}, generation={}, memberId='{}'}}",
            self.rest_url,
            self.group_id,
            self.state,
            self.generation.generation_id(),
            self.member_id()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generation_creation() {
        let gen = Generation::new(1, "member-1".to_string(), Some("compatible".to_string()));
        assert_eq!(gen.generation_id(), 1);
        assert_eq!(gen.member_id(), "member-1");
        assert_eq!(gen.protocol(), Some("compatible"));
    }

    #[test]
    fn test_generation_no_generation() {
        let gen = Generation::no_generation();
        assert_eq!(gen.generation_id(), NO_GENERATION);
        assert_eq!(gen.member_id(), UNKNOWN_MEMBER_ID);
        assert_eq!(gen.protocol(), None);
    }

    #[test]
    fn test_connector_task_id() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 5);
        assert_eq!(id.connector(), "test-connector");
        assert_eq!(id.task(), 5);
        assert_eq!(format!("{}", id), "test-connector-5");
    }

    #[test]
    fn test_extended_assignment_empty() {
        let empty = ExtendedAssignment::empty();
        assert_eq!(empty.version(), ExtendedAssignment::CONNECT_PROTOCOL_V1);
        assert_eq!(empty.error(), ExtendedAssignment::NO_ERROR);
        assert!(empty.leader().is_none());
        assert!(empty.connectors().is_empty());
        assert!(empty.tasks().is_empty());
        assert!(!empty.failed());
    }

    #[test]
    fn test_extended_assignment_creation() {
        let assignment = ExtendedAssignment::new(
            1,
            0,
            Some("leader-1".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["conn-a".to_string()],
            vec![ConnectorTaskId::new("conn-a".to_string(), 0)],
            vec!["conn-b".to_string()],
            vec![ConnectorTaskId::new("conn-b".to_string(), 1)],
            5000,
        );

        assert_eq!(assignment.version(), 1);
        assert_eq!(assignment.error(), 0);
        assert_eq!(assignment.leader(), Some("leader-1"));
        assert_eq!(assignment.offset(), 100);
        assert_eq!(assignment.connectors().len(), 1);
        assert_eq!(assignment.tasks().len(), 1);
        assert_eq!(assignment.revoked_connectors().len(), 1);
        assert_eq!(assignment.revoked_tasks().len(), 1);
        assert_eq!(assignment.delay(), 5000);
    }

    #[test]
    fn test_extended_assignment_failed() {
        let assignment = ExtendedAssignment::new(
            1,
            ExtendedAssignment::CONFIG_MISMATCH,
            None,
            None,
            0,
            vec![],
            vec![],
            vec![],
            vec![],
            0,
        );
        assert!(assignment.failed());
    }

    #[test]
    fn test_connectors_and_tasks() {
        let cat = ConnectorsAndTasks::new(
            vec!["conn-1".to_string(), "conn-2".to_string()],
            vec![ConnectorTaskId::new("conn-1".to_string(), 0)],
        );
        assert_eq!(cat.size(), 3);
        assert!(!cat.is_empty());
    }

    #[test]
    fn test_worker_load() {
        let mut load = WorkerLoad::new(
            "worker-1".to_string(),
            vec!["conn-1".to_string()],
            vec![ConnectorTaskId::new("conn-1".to_string(), 0)],
        );
        assert_eq!(load.worker(), "worker-1");
        assert_eq!(load.size(), 2);

        load.assign_connector("conn-2".to_string());
        load.assign_task(ConnectorTaskId::new("conn-2".to_string(), 0));
        assert_eq!(load.size(), 4);
    }

    #[test]
    fn test_connect_protocol_compatibility() {
        assert_eq!(ConnectProtocolCompatibility::Eager.protocol(), "default");
        assert_eq!(
            ConnectProtocolCompatibility::Compatible.protocol(),
            "compatible"
        );
        assert_eq!(
            ConnectProtocolCompatibility::Sessioned.protocol(),
            "sessioned"
        );

        assert_eq!(ConnectProtocolCompatibility::Eager.protocol_version(), 0);
        assert_eq!(
            ConnectProtocolCompatibility::Compatible.protocol_version(),
            1
        );
        assert_eq!(
            ConnectProtocolCompatibility::Sessioned.protocol_version(),
            2
        );

        assert_eq!(
            ConnectProtocolCompatibility::from_protocol("default"),
            ConnectProtocolCompatibility::Eager
        );
        assert_eq!(
            ConnectProtocolCompatibility::from_protocol("compatible"),
            ConnectProtocolCompatibility::Compatible
        );
    }

    #[test]
    fn test_mock_worker_rebalance_listener() {
        let mut listener = MockWorkerRebalanceListener::new();
        let assignment = ExtendedAssignment::empty();

        listener.on_assigned(assignment.clone(), 1);
        assert_eq!(listener.assigned_count(), 1);
        assert_eq!(listener.last_generation(), 1);

        listener.on_revoked("leader".to_string(), vec!["conn".to_string()], vec![]);
        assert_eq!(listener.revoked_count(), 1);

        listener.on_poll_timeout_expiry();
        assert_eq!(listener.timeout_count(), 1);

        listener.reset();
        assert_eq!(listener.assigned_count(), 0);
        assert_eq!(listener.revoked_count(), 0);
        assert_eq!(listener.timeout_count(), 0);
    }

    #[test]
    fn test_mock_worker_coordinator_creation() {
        let coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        assert_eq!(coordinator.rest_url(), "http://localhost:8083");
        assert_eq!(coordinator.group_id(), "connect-group");
        assert_eq!(coordinator.state(), MemberState::Unjoined);
        assert_eq!(coordinator.generation_id(), NO_GENERATION);
        assert_eq!(coordinator.member_id(), UNKNOWN_MEMBER_ID);
        assert!(coordinator.assignment().is_none());
        assert!(coordinator.coordinator_unknown());
    }

    #[test]
    fn test_mock_worker_coordinator_join_group() {
        let mut coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        let result = coordinator.join_group();
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.generation_id(), 1);
        assert!(!response.member_id().is_empty());
        assert!(response.is_leader()); // First member becomes leader
        assert_eq!(coordinator.state(), MemberState::Syncing);
    }

    #[test]
    fn test_mock_worker_coordinator_sync_group() {
        let mut coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        // Must join first
        coordinator.join_group().unwrap();

        let result = coordinator.sync_group();
        assert!(result.is_ok());

        let response = result.unwrap();
        assert!(response.is_success());
        assert_eq!(coordinator.state(), MemberState::Stable);
        assert!(coordinator.assignment().is_some());
        assert!(!coordinator.rejoin_needed_or_pending());
    }

    #[test]
    fn test_mock_worker_coordinator_leave_group() {
        let mut coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        // Join and sync first
        coordinator.join_group().unwrap();
        coordinator.sync_group().unwrap();

        let result = coordinator.leave_group();
        assert!(result.is_ok());

        assert_eq!(coordinator.state(), MemberState::Unjoined);
        assert!(coordinator.assignment().is_none());
        assert_eq!(coordinator.generation_id(), NO_GENERATION);
    }

    #[test]
    fn test_mock_worker_coordinator_ensure_active_group() {
        let mut coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        let result = coordinator.ensure_active_group();
        assert!(result.is_ok());

        assert_eq!(coordinator.state(), MemberState::Stable);
        assert!(coordinator.assignment().is_some());
    }

    #[test]
    fn test_mock_worker_coordinator_request_rejoin() {
        let mut coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        coordinator.ensure_active_group().unwrap();
        assert!(!coordinator.rejoin_needed_or_pending());

        coordinator.request_rejoin("test reason");
        assert!(coordinator.rejoin_needed_or_pending());
    }

    #[test]
    fn test_mock_worker_coordinator_poll() {
        let mut coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        // Poll should fail when coordinator is unknown
        let result = coordinator.poll(1000);
        assert!(result.is_err());

        // After ensuring active group, poll should succeed
        coordinator.ensure_active_group().unwrap();
        let result = coordinator.poll(1000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_worker_coordinator_with_listener() {
        let mut coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        let listener = Arc::new(Mutex::new(MockWorkerRebalanceListener::new()));
        coordinator.set_listener(listener.clone());

        coordinator.ensure_active_group().unwrap();

        let listener_guard = listener.lock().unwrap();
        assert_eq!(listener_guard.assigned_count(), 1);
        assert!(listener_guard.last_assignment().is_some());
    }

    #[test]
    fn test_mock_worker_coordinator_is_leader() {
        let mut coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        coordinator.ensure_active_group().unwrap();

        // First member becomes leader in mock
        assert!(coordinator.is_leader());
    }

    #[test]
    fn test_mock_worker_coordinator_with_mock_members() {
        let mut coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        // Add existing members
        coordinator.set_mock_members(vec![GroupMember::new(
            "existing-member".to_string(),
            vec![],
        )]);

        let result = coordinator.join_group();
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.members().len(), 2);
        // Existing member should be leader
        assert_eq!(response.leader_id(), "existing-member");
        assert!(!response.is_leader());
    }

    #[test]
    fn test_leader_state() {
        let all_members = HashMap::new();
        let mut connector_assignment = HashMap::new();
        connector_assignment.insert("worker-1".to_string(), vec!["conn-1".to_string()]);

        let mut task_assignment = HashMap::new();
        task_assignment.insert(
            "worker-1".to_string(),
            vec![ConnectorTaskId::new("conn-1".to_string(), 0)],
        );

        let leader_state = LeaderState::new(all_members, connector_assignment, task_assignment);
        assert!(leader_state.connector_owners().contains_key("conn-1"));
        assert!(leader_state
            .task_owners()
            .contains_key(&ConnectorTaskId::new("conn-1".to_string(), 0)));
    }

    #[test]
    fn test_join_group_response() {
        let response = JoinGroupResponse::new(
            1,
            "leader-1".to_string(),
            "leader-1".to_string(),
            vec![GroupMember::new("leader-1".to_string(), vec![])],
            "compatible".to_string(),
        );

        assert_eq!(response.generation_id(), 1);
        assert_eq!(response.member_id(), "leader-1");
        assert_eq!(response.leader_id(), "leader-1");
        assert_eq!(response.protocol(), "compatible");
        assert!(response.is_leader());
    }

    #[test]
    fn test_sync_group_response() {
        let response = SyncGroupResponse::new(vec![1, 2, 3], 0);
        assert!(response.is_success());
        assert_eq!(response.error(), 0);
        assert_eq!(response.assignment(), &[1, 2, 3]);

        let error_response = SyncGroupResponse::new(vec![], 1);
        assert!(!error_response.is_success());
    }

    #[test]
    fn test_set_assignment() {
        let mut coordinator = MockWorkerCoordinator::new(
            "http://localhost:8083".to_string(),
            "connect-group".to_string(),
            ConnectProtocolCompatibility::Compatible,
        );

        let assignment = ExtendedAssignment::new(
            1,
            0,
            Some("leader".to_string()),
            Some("http://leader:8083".to_string()),
            100,
            vec!["conn-1".to_string()],
            vec![ConnectorTaskId::new("conn-1".to_string(), 0)],
            vec![],
            vec![],
            0,
        );

        coordinator.set_assignment(assignment.clone());
        assert_eq!(coordinator.state(), MemberState::Stable);
        assert!(coordinator.assignment().is_some());
        assert_eq!(
            coordinator.assignment().unwrap().connectors(),
            &["conn-1".to_string() as String]
        );
    }
}
