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

//! DistributedHerder - Distributed herder for Kafka Connect distributed mode.
//!
//! This module implements the Herder trait for distributed mode, where
//! multiple workers coordinate via Kafka's group membership protocol.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.distributed.DistributedHerder`
//! in Java (~1100 lines, ~60 methods).
//!
//! **P4-2a scope**: Basic structure and lifecycle methods.
//! **P4-2b scope**: In-memory config/state management (Kafka backing store is P2).
//! **P4-2c scope**: Rebalance processing, WorkerGroupMember, ExtendedAssignment.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use common_trait::herder::TargetState as HerderTargetState;
use common_trait::herder::{
    ActiveTopicsInfo, Callback, ConfigInfo, ConfigInfos, ConfigKeyInfo, ConnectorInfo,
    ConnectorOffsets, ConnectorState, ConnectorStateInfo, ConnectorTaskId, Created, Herder,
    HerderRequest, InternalRequestSignature, LoggerLevel, Message, PluginDesc, PluginType, Plugins,
    StatusBackingStore, TaskInfo, TaskStateInfo, VersionRange,
};
use common_trait::storage::{
    ClusterConfigState, ConnectorTaskId as StorageConnectorTaskId, TargetState,
};
use serde_json::Value;

use crate::worker::Worker;

// ===== HerderMetrics - Metrics tracking for DistributedHerder =====

/// HerderMetrics - Metrics tracking for herder operations.
///
/// Tracks request counts, rebalance events, and error counts.
/// Uses atomic counters for thread-safe updates.
///
/// P4-2e: Basic metrics implementation for tracking herder operations.
#[derive(Debug, Default)]
pub struct HerderMetrics {
    /// Total request count processed by the herder.
    pub request_count: AtomicU64,
    /// Total rebalance count (rebalance completed events).
    pub rebalance_count: AtomicU64,
    /// Total error count (failed operations).
    pub error_count: AtomicU64,
}

impl HerderMetrics {
    /// Create new HerderMetrics with zero counters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment request count.
    pub fn increment_request_count(&self) {
        self.request_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Increment rebalance count.
    pub fn increment_rebalance_count(&self) {
        self.rebalance_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Increment error count.
    pub fn increment_error_count(&self) {
        self.error_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Get current request count.
    pub fn request_count_value(&self) -> u64 {
        self.request_count.load(Ordering::SeqCst)
    }

    /// Get current rebalance count.
    pub fn rebalance_count_value(&self) -> u64 {
        self.rebalance_count.load(Ordering::SeqCst)
    }

    /// Get current error count.
    pub fn error_count_value(&self) -> u64 {
        self.error_count.load(Ordering::SeqCst)
    }
}

// ===== PendingRequest - Internal request tracking =====

/// PendingRequest - Internal request for delayed operations.
///
/// Represents a request that has been queued for processing.
/// Used for delayed connector restart and other async operations.
///
/// P4-2e: Request queue implementation.
pub struct PendingRequest {
    /// Request type.
    pub request_type: PendingRequestType,
    /// Target connector name.
    pub connector_name: String,
    /// Delay in milliseconds before processing.
    pub delay_ms: u64,
    /// Creation timestamp.
    pub created_at: Instant,
    /// Request handle for tracking.
    pub handle: Arc<DistributedHerderRequest>,
    /// Callback to invoke on completion.
    pub callback: Option<Arc<dyn CallbackWithResult>>,
}

/// Pending request types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingRequestType {
    /// Delayed connector restart.
    RestartConnector,
    /// Task reconfiguration.
    TaskReconfiguration,
}

/// Callback trait that can be stored in Arc.
pub trait CallbackWithResult: Send + Sync {
    /// Called on successful completion.
    fn on_success(&self);

    /// Called on error.
    fn on_error(&self, error: String);
}

/// Simple callback result storage for testing.
pub struct SimpleCallbackResult {
    success: AtomicBool,
    error: RwLock<Option<String>>,
}

impl SimpleCallbackResult {
    pub fn new() -> Self {
        Self {
            success: AtomicBool::new(false),
            error: RwLock::new(None),
        }
    }

    pub fn was_successful(&self) -> bool {
        self.success.load(Ordering::SeqCst)
    }

    pub fn error(&self) -> Option<String> {
        self.error.read().unwrap().clone()
    }
}

impl CallbackWithResult for SimpleCallbackResult {
    fn on_success(&self) {
        self.success.store(true, Ordering::SeqCst);
    }

    fn on_error(&self, error: String) {
        *self.error.write().unwrap() = Some(error);
    }
}

/// SimpleCallback - A callback implementation that captures the result.
/// Uses Arc internally so Clone shares storage.
pub struct SimpleCallback<T: Clone> {
    state: Arc<SharedCallbackState<T>>,
}

struct SharedCallbackState<T: Clone> {
    result: RwLock<Option<T>>,
    error: RwLock<Option<String>>,
}

impl<T: Clone> Clone for SimpleCallback<T> {
    fn clone(&self) -> Self {
        SimpleCallback {
            state: self.state.clone(),
        }
    }
}

impl<T: Clone> SimpleCallback<T> {
    pub fn new() -> Self {
        SimpleCallback {
            state: Arc::new(SharedCallbackState {
                result: RwLock::new(None),
                error: RwLock::new(None),
            }),
        }
    }

    pub fn result(&self) -> Option<T> {
        self.state.result.read().unwrap().clone()
    }

    pub fn error(&self) -> Option<String> {
        self.state.error.read().unwrap().clone()
    }
}

impl<T: Clone> Callback<T> for SimpleCallback<T> {
    fn on_completion(&self, result: T) {
        *self.state.result.write().unwrap() = Some(result);
    }

    fn on_error(&self, error: String) {
        *self.state.error.write().unwrap() = Some(error);
    }
}

// ===== ExtendedAssignment - Assignment data structure =====

/// ExtendedAssignment - Connector/task assignment data structure.
///
/// Encapsulates the details of a worker's assignment during a rebalance.
/// Contains assigned and revoked connectors/tasks, along with rebalance metadata.
///
/// Corresponds to `org.apache.kafka.connect.runtime.distributed.ExtendedAssignment`
/// in Java (~200 lines).
#[derive(Debug, Clone)]
pub struct ExtendedAssignment {
    /// Protocol version for this assignment.
    version: i32,
    /// Error code if assignment failed (0 = no error).
    error: i32,
    /// Leader worker ID.
    leader_id: String,
    /// Leader worker REST URL.
    leader_url: String,
    /// Configuration offset the worker should be caught up to.
    offset: i64,
    /// Assigned connector names.
    connectors: Vec<String>,
    /// Assigned task IDs.
    tasks: Vec<StorageConnectorTaskId>,
    /// Revoked connector names (for cooperative rebalancing).
    revoked_connectors: Vec<String>,
    /// Revoked task IDs (for cooperative rebalancing).
    revoked_tasks: Vec<StorageConnectorTaskId>,
    /// Delay before next scheduled rebalance (milliseconds).
    delay: i64,
}

/// Assignment error codes.
pub const ASSIGNMENT_ERROR_NONE: i32 = 0;
pub const ASSIGNMENT_ERROR_UNKNOWN_MEMBER: i32 = 1;
pub const ASSIGNMENT_ERROR_DUPLICATE_MEMBER: i32 = 2;
pub const ASSIGNMENT_ERROR_INCONSISTENT_STATE: i32 = 3;
pub const ASSIGNMENT_ERROR_NO_ASSIGNMENT: i32 = 4;
pub const ASSIGNMENT_ERROR_ASSIGNMENT_TIMEOUT: i32 = 5;

impl ExtendedAssignment {
    /// Create a new empty assignment.
    pub fn empty() -> Self {
        ExtendedAssignment {
            version: 1,
            error: ASSIGNMENT_ERROR_NONE,
            leader_id: String::new(),
            leader_url: String::new(),
            offset: -1,
            connectors: Vec::new(),
            tasks: Vec::new(),
            revoked_connectors: Vec::new(),
            revoked_tasks: Vec::new(),
            delay: 0,
        }
    }

    /// Create a new assignment with the given parameters.
    pub fn new(
        version: i32,
        error: i32,
        leader_id: String,
        leader_url: String,
        offset: i64,
        connectors: Vec<String>,
        tasks: Vec<StorageConnectorTaskId>,
        revoked_connectors: Vec<String>,
        revoked_tasks: Vec<StorageConnectorTaskId>,
        delay: i64,
    ) -> Self {
        ExtendedAssignment {
            version,
            error,
            leader_id,
            leader_url,
            offset,
            connectors,
            tasks,
            revoked_connectors,
            revoked_tasks,
            delay,
        }
    }

    /// Get protocol version.
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Get error code.
    pub fn error(&self) -> i32 {
        self.error
    }

    /// Check if there's an error.
    pub fn has_error(&self) -> bool {
        self.error != ASSIGNMENT_ERROR_NONE
    }

    /// Get leader worker ID.
    pub fn leader_id(&self) -> &str {
        &self.leader_id
    }

    /// Get leader REST URL.
    pub fn leader_url(&self) -> &str {
        &self.leader_url
    }

    /// Get configuration offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Get assigned connectors.
    pub fn connectors(&self) -> &[String] {
        &self.connectors
    }

    /// Get assigned tasks.
    pub fn tasks(&self) -> &[StorageConnectorTaskId] {
        &self.tasks
    }

    /// Get revoked connectors.
    pub fn revoked_connectors(&self) -> &[String] {
        &self.revoked_connectors
    }

    /// Get revoked tasks.
    pub fn revoked_tasks(&self) -> &[StorageConnectorTaskId] {
        &self.revoked_tasks
    }

    /// Get scheduled rebalance delay.
    pub fn delay(&self) -> i64 {
        self.delay
    }

    /// Check if assignment contains any connectors or tasks.
    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty() && self.tasks.is_empty()
    }

    /// Total count of assigned connectors and tasks.
    pub fn total_count(&self) -> usize {
        self.connectors.len() + self.tasks.len()
    }
}

impl Default for ExtendedAssignment {
    fn default() -> Self {
        Self::empty()
    }
}

// ===== RebalanceListener trait =====

/// RebalanceListener trait - Callback for rebalance events.
///
/// Handles onAssigned and onRevoked events during Kafka Connect rebalance.
/// Corresponds to `org.apache.kafka.connect.runtime.distributed.WorkerRebalanceListener`
/// in Java.
pub trait RebalanceListener: Send + Sync {
    /// Called when connectors and tasks are assigned to this worker.
    ///
    /// Args:
    /// - assignment: The new assignment containing assigned connectors/tasks
    /// - generation: The group generation ID for this rebalance
    fn on_assigned(&mut self, assignment: ExtendedAssignment, generation: i32);

    /// Called when connectors and tasks are revoked from this worker.
    ///
    /// Args:
    /// - leader_id: The leader worker ID (may be empty if leader not known)
    /// - connectors: Connector names being revoked
    /// - tasks: Task IDs being revoked
    fn on_revoked(
        &mut self,
        leader_id: &str,
        connectors: Vec<String>,
        tasks: Vec<StorageConnectorTaskId>,
    );
}

// ===== WorkerGroupMember (Simplified Version) =====

/// WorkerGroupMember - Group membership management (simplified version).
///
/// In P4-2c, this is a simplified/mock version that simulates group membership
/// without actual Kafka consumer group protocol. The full implementation
/// with KafkaConsumer group is P4-3~P4-6 scope.
///
/// Corresponds to `org.apache.kafka.connect.runtime.distributed.WorkerGroupMember`
/// in Java (~400 lines).
///
/// **Thread safety**: Uses atomic flags for membership state.
pub struct WorkerGroupMember {
    /// Worker ID for this member.
    worker_id: String,
    /// Whether the member is active in the group.
    is_active: AtomicBool,
    /// Whether a rebalance is in progress.
    rebalance_in_progress: AtomicBool,
    /// Current generation ID.
    generation: AtomicI32,
    /// Current assignment (stored for rebalance handling).
    current_assignment: RwLock<ExtendedAssignment>,
    /// Rebalance listener callback.
    rebalance_listener: RwLock<Option<Arc<dyn RebalanceListener>>>,
    /// Pending assignment to apply on next tick.
    pending_assignment: RwLock<Option<ExtendedAssignment>>,
    /// Pending generation for pending assignment.
    pending_generation: AtomicI32,
    /// Whether to trigger a simulated rebalance (for testing).
    trigger_rebalance: AtomicBool,
}

impl WorkerGroupMember {
    /// Create a new WorkerGroupMember with the given worker ID.
    pub fn new(worker_id: String) -> Self {
        WorkerGroupMember {
            worker_id,
            is_active: AtomicBool::new(false),
            rebalance_in_progress: AtomicBool::new(false),
            generation: AtomicI32::new(-1),
            current_assignment: RwLock::new(ExtendedAssignment::empty()),
            rebalance_listener: RwLock::new(None),
            pending_assignment: RwLock::new(None),
            pending_generation: AtomicI32::new(-1),
            trigger_rebalance: AtomicBool::new(false),
        }
    }

    /// Set the rebalance listener.
    pub fn set_rebalance_listener(&self, listener: Arc<dyn RebalanceListener>) {
        *self.rebalance_listener.write().unwrap() = Some(listener);
    }

    /// Ensure the member is active in the group.
    ///
    /// This method triggers group join/rejoin if needed.
    /// In simplified version, we simulate the group membership protocol.
    pub fn ensure_active(&self) {
        if self.is_active.load(Ordering::SeqCst) {
            return;
        }
        self.rebalance_in_progress.store(true, Ordering::SeqCst);
    }

    /// Poll for group membership events.
    ///
    /// In simplified version, this processes pending assignments and
    /// invokes rebalance listener callbacks.
    ///
    /// Args:
    /// - timeout: Timeout for the poll operation (not used in simplified version)
    pub fn poll(&self, timeout: Duration) {
        // Process pending assignment if available
        let pending = self.pending_assignment.write().unwrap().take();
        if let Some(assignment) = pending {
            let generation = self.pending_generation.load(Ordering::SeqCst);
            self.apply_assignment(assignment, generation);
        }

        // Check for rebalance trigger (testing)
        if self.trigger_rebalance.load(Ordering::SeqCst) {
            self.trigger_rebalance.store(false, Ordering::SeqCst);
            self.rebalance_in_progress.store(true, Ordering::SeqCst);
        }
    }

    /// Apply a new assignment and invoke on_assigned callback.
    fn apply_assignment(&self, assignment: ExtendedAssignment, generation: i32) {
        // Update current assignment
        *self.current_assignment.write().unwrap() = assignment.clone();
        self.generation.store(generation, Ordering::SeqCst);

        // Mark as active after receiving assignment
        self.is_active.store(true, Ordering::SeqCst);
        self.rebalance_in_progress.store(false, Ordering::SeqCst);

        // Invoke listener callback
        let listener_guard = self.rebalance_listener.read().unwrap();
        if let Some(listener) = listener_guard.as_ref() {
            // Note: We can't call mutable method on Arc, so we use a workaround
            // In production, this would use proper mutability handling
            // For simplified version, we'll handle this differently
        }
    }

    /// Trigger a simulated rebalance (for testing).
    ///
    /// Sets a flag that will be processed on next poll() call.
    pub fn trigger_rebalance_for_test(&self) {
        self.trigger_rebalance.store(true, Ordering::SeqCst);
    }

    /// Set a pending assignment to be applied on next poll().
    ///
    /// This is used by tests to simulate receiving an assignment.
    pub fn set_pending_assignment(&self, assignment: ExtendedAssignment, generation: i32) {
        *self.pending_assignment.write().unwrap() = Some(assignment);
        self.pending_generation.store(generation, Ordering::SeqCst);
    }

    /// Check if member is active.
    pub fn is_active(&self) -> bool {
        self.is_active.load(Ordering::SeqCst)
    }

    /// Get current generation.
    pub fn generation(&self) -> i32 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Get current assignment.
    pub fn assignment(&self) -> ExtendedAssignment {
        self.current_assignment.read().unwrap().clone()
    }

    /// Get worker ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Request rejoin (for cooperative rebalancing).
    pub fn request_rejoin(&self) {
        self.is_active.store(false, Ordering::SeqCst);
        self.rebalance_in_progress.store(true, Ordering::SeqCst);
    }

    /// Check if rebalance is in progress.
    pub fn rebalance_in_progress(&self) -> bool {
        self.rebalance_in_progress.load(Ordering::SeqCst)
    }
}

// ===== DistributedHerder struct =====

/// DistributedHerder - Distributed herder implementation.
///
/// Coordinates multiple workers via Kafka's group membership protocol.
/// One worker is elected as leader and handles configuration reading,
/// task assignment, and rebalance coordination.
///
/// **Thread safety**: Uses atomic flags for lifecycle state.
/// Uses RwLock for config_state and assignment to allow concurrent reads.
///
/// **P4-2b**: In-memory config storage (Kafka backing store is P2 scope).
/// **P4-2c**: Rebalance processing with WorkerGroupMember.
pub struct DistributedHerder {
    /// Worker instance that executes connectors and tasks.
    worker: Arc<Worker>,
    /// Kafka cluster ID for this Connect cluster.
    kafka_cluster_id: String,
    /// Whether the herder has been started.
    started: AtomicBool,
    /// Whether the herder is in the process of stopping.
    stopping: AtomicBool,
    /// Whether the herder has completed initialization and received assignment.
    ready: AtomicBool,
    /// Whether rebalance resolution is in progress.
    rebalance_resolving: AtomicBool,

    /// Immutable snapshot of cluster configuration state.
    config_state: RwLock<ClusterConfigState>,

    /// In-memory connector configurations.
    connector_configs: RwLock<HashMap<String, HashMap<String, String>>>,

    /// In-memory task configurations.
    task_configs: RwLock<HashMap<StorageConnectorTaskId, HashMap<String, String>>>,

    /// In-memory target states for connectors.
    target_states: RwLock<HashMap<String, TargetState>>,

    /// In-memory connector task counts.
    connector_task_counts: RwLock<HashMap<String, u32>>,

    /// In-memory connector offsets storage.
    connector_offsets: RwLock<HashMap<String, ConnectorOffsets>>,

    /// In-memory connector active topics tracking.
    /// Maps connector name to set of active topics.
    connector_active_topics: RwLock<HashMap<String, Vec<String>>>,

    /// Current assignment from rebalance.
    assignment: RwLock<ExtendedAssignment>,

    /// Running assignment - connectors and tasks currently executing.
    /// Updated on on_assigned and on_revoked.
    running_assignment: RwLock<ExtendedAssignment>,

    /// Group generation ID from last rebalance.
    generation: AtomicI32,

    /// Leader worker ID (empty if not leader, or leader unknown).
    leader_id: RwLock<String>,

    /// Whether this worker is the leader.
    is_leader: AtomicBool,

    /// WorkerGroupMember for group membership management.
    member: Arc<WorkerGroupMember>,

    /// Last tick time for periodic operations.
    last_tick: RwLock<Option<Instant>>,

    /// Pending requests queue for delayed operations.
    /// Requests are processed on tick() with delay handling.
    pending_requests: RwLock<VecDeque<PendingRequest>>,

    /// Metrics tracking for herder operations.
    metrics: HerderMetrics,
}

impl DistributedHerder {
    /// Create a new DistributedHerder with the given worker and cluster ID.
    pub fn new(worker: Arc<Worker>, kafka_cluster_id: String) -> Self {
        let worker_id = worker.config().worker_id().to_string();
        let member = Arc::new(WorkerGroupMember::new(worker_id.clone()));

        DistributedHerder {
            worker,
            kafka_cluster_id,
            started: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            rebalance_resolving: AtomicBool::new(false),
            config_state: RwLock::new(ClusterConfigState::empty()),
            connector_configs: RwLock::new(HashMap::new()),
            task_configs: RwLock::new(HashMap::new()),
            target_states: RwLock::new(HashMap::new()),
            connector_task_counts: RwLock::new(HashMap::new()),
            connector_offsets: RwLock::new(HashMap::new()),
            connector_active_topics: RwLock::new(HashMap::new()),
            assignment: RwLock::new(ExtendedAssignment::empty()),
            running_assignment: RwLock::new(ExtendedAssignment::empty()),
            generation: AtomicI32::new(-1),
            leader_id: RwLock::new(String::new()),
            is_leader: AtomicBool::new(false),
            member,
            last_tick: RwLock::new(None),
            pending_requests: RwLock::new(VecDeque::new()),
            metrics: HerderMetrics::new(),
        }
    }

    /// Check if the herder is in the process of stopping.
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Check if this worker is the leader.
    ///
    /// Leader is determined by comparing leader_id with worker_id.
    /// Corresponds to Java `DistributedHerder.isLeader()`.
    pub fn is_leader(&self) -> bool {
        let leader_id = self.leader_id.read().unwrap();
        let leader_id_str: &str = &*leader_id;
        !leader_id_str.is_empty() && leader_id_str == self.worker.config().worker_id()
    }

    /// Get current leader ID.
    pub fn leader_id(&self) -> String {
        self.leader_id.read().unwrap().clone()
    }

    /// Get current group generation.
    pub fn generation(&self) -> i32 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Get current assignment.
    pub fn assignment(&self) -> ExtendedAssignment {
        self.assignment.read().unwrap().clone()
    }

    /// Get running assignment (currently executing connectors/tasks).
    pub fn running_assignment(&self) -> ExtendedAssignment {
        self.running_assignment.read().unwrap().clone()
    }

    /// Get WorkerGroupMember.
    pub fn member(&self) -> &Arc<WorkerGroupMember> {
        &self.member
    }

    /// Get HerderMetrics reference.
    pub fn metrics(&self) -> &HerderMetrics {
        &self.metrics
    }

    /// Add a pending request to the queue.
    ///
    /// P4-2e: Request queue management.
    fn add_pending_request(&self, request: PendingRequest) {
        let mut pending = self.pending_requests.write().unwrap();
        pending.push_back(request);
        self.metrics.increment_request_count();
    }

    /// Process pending requests that are ready.
    ///
    /// P4-2e: Request processing on tick.
    fn process_pending_requests(&self) {
        let now = Instant::now();
        let mut pending = self.pending_requests.write().unwrap();

        // Process requests that have elapsed their delay
        while let Some(request) = pending.front() {
            let elapsed = now.duration_since(request.created_at);
            if elapsed.as_millis() as u64 >= request.delay_ms {
                let request = pending.pop_front().unwrap();

                // Process the request based on type
                match request.request_type {
                    PendingRequestType::RestartConnector => {
                        self.process_restart_connector_request(&request);
                    }
                    PendingRequestType::TaskReconfiguration => {
                        self.process_task_reconfiguration_request(&request);
                    }
                }
            } else {
                // First request hasn't elapsed, stop processing
                break;
            }
        }
    }

    /// Process a delayed restart connector request.
    fn process_restart_connector_request(&self, request: &PendingRequest) {
        // Check if request was cancelled
        if request.handle.is_completed() {
            return;
        }

        // In simplified version, just mark as completed
        // Real implementation would restart the connector via worker
        request.handle.mark_completed();

        if let Some(callback) = &request.callback {
            callback.on_success();
        }
    }

    /// Process a task reconfiguration request.
    fn process_task_reconfiguration_request(&self, request: &PendingRequest) {
        // Check if request was cancelled
        if request.handle.is_completed() {
            return;
        }

        // In simplified version, just mark as completed
        request.handle.mark_completed();

        if let Some(callback) = &request.callback {
            callback.on_success();
        }
    }

    /// Main tick method - drives group membership and handles rebalance.
    ///
    /// This method:
    /// 1. Ensures group is active (member.ensure_active())
    /// 2. Processes pending rebalance assignments
    /// 3. Handles rebalance completion (starting/stopping connectors/tasks)
    /// 4. Polls for membership events (member.poll())
    /// 5. Processes pending requests from the request queue
    ///
    /// Corresponds to Java `DistributedHerder.tick()` (~100 lines).
    pub fn tick(&self) {
        // Record tick time
        *self.last_tick.write().unwrap() = Some(Instant::now());

        // Skip if stopping
        if self.is_stopping() {
            return;
        }

        // Ensure group membership is active
        self.member.ensure_active();

        // Process any pending assignment
        let pending = self.member.pending_assignment.write().unwrap().take();
        if let Some(assignment) = pending {
            let generation = self.member.pending_generation.load(Ordering::SeqCst);
            self.handle_assignment_received(assignment, generation);
        }

        // Handle rebalance completion if needed
        if self.rebalance_resolving.load(Ordering::SeqCst) {
            self.handle_rebalance_completed();
        }

        // Poll for membership events
        self.member.poll(Duration::from_millis(0));

        // Process pending requests from the queue
        self.process_pending_requests();
    }

    /// Handle assignment received from rebalance.
    ///
    /// Called when member receives a new assignment. This sets the assignment
    /// and marks that rebalance resolution is needed.
    fn handle_assignment_received(&self, assignment: ExtendedAssignment, generation: i32) {
        // Update assignment and generation
        *self.assignment.write().unwrap() = assignment.clone();
        self.generation.store(generation, Ordering::SeqCst);

        // Update leader info
        *self.leader_id.write().unwrap() = assignment.leader_id.clone();
        self.is_leader.store(
            assignment.leader_id == self.worker.config().worker_id(),
            Ordering::SeqCst,
        );

        // Mark rebalance resolution as needed
        self.rebalance_resolving.store(true, Ordering::SeqCst);

        // Mark as ready (first assignment received)
        self.ready.store(true, Ordering::SeqCst);
    }

    /// Handle rebalance completion.
    ///
    /// This method:
    /// 1. Stops revoked connectors and tasks (from assignment.revoked_*)
    /// 2. Starts newly assigned connectors and tasks (from assignment.*)
    /// 3. Updates running_assignment to reflect current state
    ///
    /// Corresponds to Java `DistributedHerder.handleRebalanceCompleted()`.
    fn handle_rebalance_completed(&self) {
        let assignment = self.assignment.read().unwrap().clone();
        let running = self.running_assignment.read().unwrap().clone();

        // Calculate revoked items (in running but not in new assignment)
        let revoked_connectors: Vec<String> = running
            .connectors
            .iter()
            .filter(|c| !assignment.connectors.contains(c))
            .cloned()
            .collect();

        let revoked_tasks: Vec<StorageConnectorTaskId> = running
            .tasks
            .iter()
            .filter(|t| !assignment.tasks.contains(t))
            .cloned()
            .collect();

        // Calculate new items (in assignment but not in running)
        let new_connectors: Vec<String> = assignment
            .connectors
            .iter()
            .filter(|c| !running.connectors.contains(c))
            .cloned()
            .collect();

        let new_tasks: Vec<StorageConnectorTaskId> = assignment
            .tasks
            .iter()
            .filter(|t| !running.tasks.contains(t))
            .cloned()
            .collect();

        // Handle revoked items (stop them)
        self.handle_revoked_items(&revoked_connectors, &revoked_tasks);

        // Handle new items (start them)
        self.handle_new_items(&new_connectors, &new_tasks);

        // Update running assignment
        *self.running_assignment.write().unwrap() = assignment;

        // Mark rebalance resolution as complete
        self.rebalance_resolving.store(false, Ordering::SeqCst);
    }

    /// Handle revoked connectors and tasks - stop them.
    fn handle_revoked_items(&self, connectors: &[String], tasks: &[StorageConnectorTaskId]) {
        // Stop revoked connectors
        for connector_name in connectors {
            self.worker.stop_connector(connector_name);
        }

        // Stop revoked tasks - use storage ConnectorTaskId directly
        for task_id in tasks {
            // Convert to worker ConnectorTaskId
            let worker_task_id = common_trait::worker::ConnectorTaskId::new(
                task_id.connector.clone(),
                task_id.task as i32,
            );
            self.worker.stop_task(&worker_task_id);
        }
    }

    /// Handle newly assigned connectors and tasks - start them.
    fn handle_new_items(&self, connectors: &[String], tasks: &[StorageConnectorTaskId]) {
        use crate::task::MockTaskStatusListener;
        use crate::worker::MockConnectorStatusListener;
        use common_trait::worker::TargetState as WorkerTargetState;

        // Get current config state for starting
        let config_state = self.config_state.read().unwrap().clone();

        // Start new connectors
        for connector_name in connectors {
            if let Some(conn_config) = config_state.connector_config(connector_name) {
                let target_state = config_state
                    .target_state(connector_name)
                    .unwrap_or(TargetState::Started);

                let worker_target_state = match target_state {
                    TargetState::Started => WorkerTargetState::Started,
                    TargetState::Paused => WorkerTargetState::Paused,
                    TargetState::Stopped => WorkerTargetState::Stopped,
                };

                if target_state == TargetState::Started {
                    let status_listener = Arc::new(MockConnectorStatusListener::new());
                    self.worker
                        .start_connector(
                            connector_name,
                            conn_config.clone(),
                            status_listener,
                            worker_target_state,
                        )
                        .ok();
                }
            }
        }

        // Start new tasks
        for task_id in tasks {
            if let Some(task_config) = config_state.task_config(task_id) {
                // Convert to worker ConnectorTaskId
                let worker_task_id = common_trait::worker::ConnectorTaskId::new(
                    task_id.connector.clone(),
                    task_id.task as i32,
                );

                let task_status_listener = Arc::new(MockTaskStatusListener::new());

                // Get connector config for conn_props
                let conn_props = config_state
                    .connector_config(&task_id.connector)
                    .cloned()
                    .unwrap_or_default();

                // For simplicity, assume source task (is_sink = false)
                // Real implementation would check connector type
                self.worker
                    .start_task(
                        &worker_task_id,
                        conn_props,
                        task_config.clone(),
                        task_status_listener,
                        WorkerTargetState::Started,
                        false, // is_sink
                        None,  // consumer
                        None,  // producer
                    )
                    .ok();
            }
        }
    }

    /// Trigger a simulated rebalance for testing.
    ///
    /// This sets up a pending assignment that will be processed on next tick.
    pub fn trigger_rebalance_for_test(&self, assignment: ExtendedAssignment, generation: i32) {
        self.member.set_pending_assignment(assignment, generation);
    }

    /// Set leader ID directly for testing.
    pub fn set_leader_id_for_test(&self, leader_id: String) {
        let is_this_leader = leader_id == self.worker.config().worker_id();
        *self.leader_id.write().unwrap() = leader_id;
        self.is_leader.store(is_this_leader, Ordering::SeqCst);
    }

    /// Set ready state directly for testing.
    pub fn set_ready_for_test(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    fn update_config_state_snapshot(&self) {
        let connector_configs = self.connector_configs.read().unwrap().clone();
        let task_configs = self.task_configs.read().unwrap().clone();
        let target_states = self.target_states.read().unwrap().clone();
        let task_counts = self.connector_task_counts.read().unwrap().clone();

        let mut state = self.config_state.write().unwrap();
        state.connector_configs = connector_configs;
        state.task_configs = task_configs;
        state.connector_target_states = target_states;
        state.connector_task_counts = task_counts;
    }

    fn get_task_infos(&self, connector_name: &str) -> Vec<TaskInfo> {
        let task_counts = self.connector_task_counts.read().unwrap();
        let num_tasks = task_counts.get(connector_name).copied().unwrap_or(0);
        let task_configs = self.task_configs.read().unwrap();

        let mut tasks = Vec::with_capacity(num_tasks as usize);
        for task_index in 0..num_tasks {
            let task_id = StorageConnectorTaskId::new(connector_name.to_string(), task_index);
            let config = task_configs.get(&task_id).cloned().unwrap_or_default();
            tasks.push(TaskInfo {
                id: ConnectorTaskId {
                    connector: connector_name.to_string(),
                    task: task_index,
                },
                config,
            });
        }
        tasks
    }

    fn validate_connector_config(&self, config: &HashMap<String, String>) -> Option<String> {
        if !config.contains_key("connector.class") {
            return Some("Connector config must contain 'connector.class' field".to_string());
        }
        None
    }
}

/// DistributedHerderRequest - Request handle for async operations.
pub struct DistributedHerderRequest {
    cancelled: AtomicBool,
    completed: AtomicBool,
}

impl DistributedHerderRequest {
    pub fn new() -> Self {
        DistributedHerderRequest {
            cancelled: AtomicBool::new(false),
            completed: AtomicBool::new(false),
        }
    }

    pub fn mark_completed(&self) {
        self.completed.store(true, Ordering::SeqCst);
    }
}

impl Default for DistributedHerderRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl HerderRequest for DistributedHerderRequest {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

/// DistributedHerderRequestWrapper - Request handle with callback result.
///
/// P4-2e: Wraps internal request handle and callback result for external callers.
pub struct DistributedHerderRequestWrapper {
    inner: Arc<DistributedHerderRequest>,
    result: Arc<SimpleCallbackResult>,
}

impl HerderRequest for DistributedHerderRequestWrapper {
    fn cancel(&self) {
        self.inner.cancel();
    }

    fn is_completed(&self) -> bool {
        self.inner.is_completed()
    }
}

impl DistributedHerderRequestWrapper {
    pub fn was_successful(&self) -> bool {
        self.result.was_successful()
    }

    pub fn error(&self) -> Option<String> {
        self.result.error()
    }
}

/// DistributedStatusBackingStore - Status backing store for distributed mode.
///
/// Stores connector and task states in memory for distributed mode.
/// In production, this would be backed by Kafka status topics.
#[derive(Debug, Default)]
pub struct DistributedStatusBackingStore {
    /// In-memory connector states storage.
    connector_states: RwLock<HashMap<String, ConnectorState>>,
    /// In-memory task states storage.
    task_states: RwLock<HashMap<ConnectorTaskId, TaskStateInfo>>,
}

impl DistributedStatusBackingStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StatusBackingStore for DistributedStatusBackingStore {
    fn get_connector_state(&self, connector: &str) -> Option<ConnectorState> {
        let states = self.connector_states.read().unwrap();
        states.get(connector).copied()
    }

    fn get_task_state(&self, id: &ConnectorTaskId) -> Option<TaskStateInfo> {
        let states = self.task_states.read().unwrap();
        states.get(id).cloned()
    }

    fn put_connector_state(&self, connector: &str, state: ConnectorState) {
        let mut states = self.connector_states.write().unwrap();
        states.insert(connector.to_string(), state);
    }

    fn put_task_state(&self, id: &ConnectorTaskId, state: TaskStateInfo) {
        let mut states = self.task_states.write().unwrap();
        states.insert(id.clone(), state);
    }
}

/// DistributedPlugins - Plugin discovery for distributed mode.
///
/// Provides plugin information for connector, converter, and transformation plugins.
/// In this simplified implementation, returns built-in connector class names.
///
/// P4-4d: Returns real connector class names with PluginDesc information.
#[derive(Debug, Default)]
pub struct DistributedPlugins {}

impl DistributedPlugins {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a ConfigKeyInfo for a required configuration key.
    fn create_required_config_key(
        name: &str,
        display_name: &str,
        documentation: &str,
        group: Option<&str>,
    ) -> ConfigKeyInfo {
        ConfigKeyInfo {
            name: name.to_string(),
            config_type: "STRING".to_string(),
            default_value: None,
            required: true,
            importance: "HIGH".to_string(),
            documentation: documentation.to_string(),
            group: group.map(|s| s.to_string()),
            order_in_group: Some(1),
            width: "LONG".to_string(),
            display_name: display_name.to_string(),
            dependents: vec![],
            validators: vec![],
        }
    }

    /// Create a ConfigKeyInfo for an optional configuration key.
    fn create_optional_config_key(
        name: &str,
        display_name: &str,
        documentation: &str,
        default_value: Option<&str>,
        group: Option<&str>,
        order_in_group: Option<u32>,
    ) -> ConfigKeyInfo {
        ConfigKeyInfo {
            name: name.to_string(),
            config_type: "STRING".to_string(),
            default_value: default_value.map(|s| s.to_string()),
            required: false,
            importance: "MEDIUM".to_string(),
            documentation: documentation.to_string(),
            group: group.map(|s| s.to_string()),
            order_in_group,
            width: "MEDIUM".to_string(),
            display_name: display_name.to_string(),
            dependents: vec![],
            validators: vec![],
        }
    }

    /// Validate a connector configuration.
    ///
    /// Performs basic validation of required fields:
    /// - "connector.class" must be present and non-empty
    /// - "name" must be present and non-empty (if provided in config)
    ///
    /// Returns ConfigInfos with validation results.
    pub fn validate_connector_config(&self, config: &HashMap<String, String>) -> ConfigInfos {
        let mut errors: Vec<String> = Vec::new();
        let mut configs: Vec<ConfigInfo> = Vec::new();

        // Create config key definitions for validation
        let connector_class_key = Self::create_required_config_key(
            "connector.class",
            "Connector Class",
            "The Java or Rust class name for the connector implementation",
            Some("Common"),
        );

        let name_key = Self::create_required_config_key(
            "name",
            "Connector Name",
            "The unique name for this connector instance",
            Some("Common"),
        );

        let tasks_max_key = Self::create_optional_config_key(
            "tasks.max",
            "Maximum Tasks",
            "The maximum number of tasks for this connector",
            Some("1"),
            Some("Common"),
            Some(3),
        );

        // Validate connector.class
        let connector_class_value = config.get("connector.class").cloned().unwrap_or_default();
        let connector_class_errors: Vec<String> = if connector_class_value.is_empty() {
            vec!["Missing required configuration: connector.class".to_string()]
        } else {
            // Check if connector class is known
            if !self.connector_plugins().contains(&connector_class_value) {
                vec![format!(
                    "Unknown connector class: '{}'. Known classes: {}",
                    connector_class_value,
                    self.connector_plugins().join(", ")
                )]
            } else {
                vec![]
            }
        };

        if !connector_class_errors.is_empty() {
            errors.extend(connector_class_errors.clone());
        }

        configs.push(ConfigInfo {
            definition: connector_class_key,
            value: common_trait::config::ConfigValue::with_value(
                "connector.class",
                Value::String(connector_class_value),
            ),
            errors: connector_class_errors,
        });

        // Validate name
        let name_value = config.get("name").cloned().unwrap_or_default();
        let name_errors: Vec<String> = if name_value.is_empty() {
            vec!["Missing required configuration: name".to_string()]
        } else {
            vec![]
        };

        if !name_errors.is_empty() {
            errors.extend(name_errors.clone());
        }

        configs.push(ConfigInfo {
            definition: name_key,
            value: common_trait::config::ConfigValue::with_value("name", Value::String(name_value)),
            errors: name_errors,
        });

        // Validate tasks.max (optional)
        let tasks_max_value = config
            .get("tasks.max")
            .cloned()
            .unwrap_or_else(|| "1".to_string());
        let tasks_max_errors: Vec<String> = {
            let parsed: Result<u32, _> = tasks_max_value.parse();
            match parsed {
                Ok(v) if v == 0 => vec!["tasks.max must be at least 1".to_string()],
                Ok(_) => vec![],
                Err(_) => vec![format!(
                    "Invalid value for tasks.max: '{}'",
                    tasks_max_value
                )],
            }
        };

        if !tasks_max_errors.is_empty() {
            errors.extend(tasks_max_errors.clone());
        }

        configs.push(ConfigInfo {
            definition: tasks_max_key,
            value: common_trait::config::ConfigValue::with_value(
                "tasks.max",
                Value::String(tasks_max_value),
            ),
            errors: tasks_max_errors,
        });

        // Calculate name from config
        let config_name = config
            .get("name")
            .cloned()
            .unwrap_or_else(|| "connector".to_string());

        ConfigInfos {
            name: config_name,
            error_count: errors.len() as u32,
            group_count: 1, // Common group
            configs,
        }
    }

    /// Get ConfigKeyInfo list for a connector plugin.
    ///
    /// Returns the configuration keys required/optional for the given connector class.
    pub fn get_connector_config_keys(&self, class_name: &str) -> Vec<ConfigKeyInfo> {
        // Base configuration keys common to all connectors
        let base_keys = vec![
            Self::create_required_config_key(
                "connector.class",
                "Connector Class",
                "The class name for the connector implementation",
                Some("Common"),
            ),
            Self::create_required_config_key(
                "name",
                "Connector Name",
                "The unique name for this connector instance",
                Some("Common"),
            ),
            Self::create_optional_config_key(
                "tasks.max",
                "Maximum Tasks",
                "The maximum number of tasks for this connector",
                Some("1"),
                Some("Common"),
                Some(3),
            ),
        ];

        // Add connector-specific keys based on class
        if class_name.contains("FileStreamSource") || class_name == "FileStreamSource" {
            base_keys
                .into_iter()
                .chain(vec![
                    Self::create_required_config_key(
                        "file",
                        "Source File",
                        "The file to read as the source",
                        Some("File"),
                    ),
                    Self::create_optional_config_key(
                        "topic",
                        "Output Topic",
                        "The Kafka topic to write records to",
                        None,
                        Some("File"),
                        Some(2),
                    ),
                ])
                .collect()
        } else if class_name.contains("FileStreamSink") || class_name == "FileStreamSink" {
            base_keys
                .into_iter()
                .chain(vec![
                    Self::create_required_config_key(
                        "file",
                        "Sink File",
                        "The file to write records to",
                        Some("File"),
                    ),
                    Self::create_required_config_key(
                        "topics",
                        "Input Topics",
                        "The Kafka topics to read records from",
                        Some("File"),
                    ),
                ])
                .collect()
        } else {
            // Return base keys for unknown connectors
            base_keys
        }
    }
}

impl Plugins for DistributedPlugins {
    fn connector_plugins(&self) -> Vec<String> {
        // Built-in connector types (corresponding to Kafka's file connectors)
        vec![
            "FileStreamSource".to_string(),
            "FileStreamSink".to_string(),
            "org.apache.kafka.connect.file.FileStreamSourceConnector".to_string(),
            "org.apache.kafka.connect.file.FileStreamSinkConnector".to_string(),
        ]
    }

    fn converter_plugins(&self) -> Vec<String> {
        vec![
            "org.apache.kafka.connect.storage.StringConverter".to_string(),
            "org.apache.kafka.connect.storage.JsonConverter".to_string(),
            "org.apache.kafka.connect.storage.ByteArrayConverter".to_string(),
        ]
    }

    fn transformation_plugins(&self) -> Vec<String> {
        vec![
            "org.apache.kafka.connect.transforms.InsertField".to_string(),
            "org.apache.kafka.connect.transforms.ReplaceField".to_string(),
            "org.apache.kafka.connect.transforms.TimestampRouter".to_string(),
        ]
    }

    fn connector_plugin_desc(&self, class_name: &str) -> Option<PluginDesc> {
        // Determine plugin type based on class name
        let plugin_type = if class_name.contains("Source") || class_name == "FileStreamSource" {
            PluginType::Source
        } else if class_name.contains("Sink") || class_name == "FileStreamSink" {
            PluginType::Sink
        } else {
            PluginType::Source // Default to source
        };

        // Return descriptor for known connectors
        if self.connector_plugins().contains(&class_name.to_string()) {
            Some(PluginDesc {
                class_name: class_name.to_string(),
                plugin_type,
                version: "1.0.0".to_string(),
                documentation: if plugin_type == PluginType::Source {
                    "File-based source connector that reads lines from a file and produces them to Kafka"
                } else {
                    "File-based sink connector that writes records from Kafka to a file"
                }.to_string(),
            })
        } else {
            None
        }
    }

    fn converter_plugin_desc(&self, class_name: &str) -> Option<PluginDesc> {
        if self.converter_plugins().contains(&class_name.to_string()) {
            Some(PluginDesc {
                class_name: class_name.to_string(),
                plugin_type: PluginType::Converter,
                version: "1.0.0".to_string(),
                documentation: format!("Converter for {}", class_name),
            })
        } else {
            None
        }
    }
}

// ===== Herder trait implementation =====

impl Herder for DistributedHerder {
    fn start(&mut self) {
        if self.started.load(Ordering::SeqCst) {
            return;
        }
        self.started.store(true, Ordering::SeqCst);
        self.stopping.store(false, Ordering::SeqCst);
    }

    fn stop(&mut self) {
        if self.stopping.load(Ordering::SeqCst) {
            return;
        }
        self.stopping.store(true, Ordering::SeqCst);
        self.started.store(false, Ordering::SeqCst);
        self.ready.store(false, Ordering::SeqCst);
    }

    fn is_ready(&self) -> bool {
        self.started.load(Ordering::SeqCst)
            && !self.stopping.load(Ordering::SeqCst)
            && self.ready.load(Ordering::SeqCst)
    }

    fn health_check(&self, callback: Box<dyn Callback<()>>) {
        if self.is_ready() {
            callback.on_completion(());
        } else {
            callback
                .on_error("DistributedHerder not ready: rebalance not yet completed".to_string());
        }
    }

    fn connectors_async(&self, callback: Box<dyn Callback<Vec<String>>>) {
        let state = self.config_state.read().unwrap();
        let connectors: Vec<String> = state.connector_configs.keys().cloned().collect();
        callback.on_completion(connectors);
    }

    fn connector_info_async(&self, conn_name: &str, callback: Box<dyn Callback<ConnectorInfo>>) {
        let state = self.config_state.read().unwrap();
        if !state.contains(conn_name) {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            return;
        }
        let config = state
            .connector_config(conn_name)
            .cloned()
            .unwrap_or_default();
        let tasks = self.get_task_infos(conn_name);
        callback.on_completion(ConnectorInfo {
            name: conn_name.to_string(),
            config,
            tasks,
        });
    }

    fn connector_config_async(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<HashMap<String, String>>>,
    ) {
        let state = self.config_state.read().unwrap();
        match state.connector_config(conn_name) {
            Some(config) => callback.on_completion(config.clone()),
            None => callback.on_error(format!("Connector '{}' does not exist", conn_name)),
        }
    }

    fn put_connector_config(
        &self,
        conn_name: &str,
        config: HashMap<String, String>,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        // Leader check - only leader can write connector configs
        if !self.is_leader() {
            callback.on_error(format!(
                "NotLeader: only leader can write connector configs. Current leader: '{}'",
                self.leader_id()
            ));
            return;
        }

        if let Some(error) = self.validate_connector_config(&config) {
            callback.on_error(error);
            return;
        }

        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if exists && !allow_replace {
            callback.on_error(format!("Connector '{}' already exists", conn_name));
            return;
        }

        {
            let mut configs = self.connector_configs.write().unwrap();
            configs.insert(conn_name.to_string(), config.clone());
        }

        {
            let mut states = self.target_states.write().unwrap();
            if !states.contains_key(conn_name) {
                states.insert(conn_name.to_string(), TargetState::Started);
            }
        }

        self.update_config_state_snapshot();
        let tasks = self.get_task_infos(conn_name);

        callback.on_completion(Created {
            created: !exists,
            info: ConnectorInfo {
                name: conn_name.to_string(),
                config,
                tasks,
            },
        });
    }

    fn put_connector_config_with_state(
        &self,
        conn_name: &str,
        config: HashMap<String, String>,
        target_state: HerderTargetState,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        // Leader check - only leader can write connector configs with state
        if !self.is_leader() {
            callback.on_error(format!(
                "NotLeader: only leader can write connector configs. Current leader: '{}'",
                self.leader_id()
            ));
            return;
        }

        if let Some(error) = self.validate_connector_config(&config) {
            callback.on_error(error);
            return;
        }

        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if exists && !allow_replace {
            callback.on_error(format!("Connector '{}' already exists", conn_name));
            return;
        }

        let storage_target_state = match target_state {
            HerderTargetState::Started => TargetState::Started,
            HerderTargetState::Paused => TargetState::Paused,
            HerderTargetState::Stopped => TargetState::Stopped,
        };

        {
            let mut configs = self.connector_configs.write().unwrap();
            configs.insert(conn_name.to_string(), config.clone());
        }

        {
            let mut states = self.target_states.write().unwrap();
            states.insert(conn_name.to_string(), storage_target_state);
        }

        self.update_config_state_snapshot();
        let tasks = self.get_task_infos(conn_name);

        callback.on_completion(Created {
            created: !exists,
            info: ConnectorInfo {
                name: conn_name.to_string(),
                config,
                tasks,
            },
        });
    }

    fn patch_connector_config(
        &self,
        conn_name: &str,
        config_patch: HashMap<String, String>,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        // Leader check - only leader can patch connector configs
        if !self.is_leader() {
            callback.on_error(format!(
                "NotLeader: only leader can patch connector configs. Current leader: '{}'",
                self.leader_id()
            ));
            return;
        }

        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if !exists {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            return;
        }

        {
            let mut configs = self.connector_configs.write().unwrap();
            if let Some(existing_config) = configs.get_mut(conn_name) {
                for (key, value) in config_patch {
                    existing_config.insert(key, value);
                }
            }
        }

        self.update_config_state_snapshot();

        let (config, tasks) = {
            let configs = self.connector_configs.read().unwrap();
            let config = configs.get(conn_name).cloned().unwrap_or_default();
            let tasks = self.get_task_infos(conn_name);
            (config, tasks)
        };

        callback.on_completion(Created {
            created: false,
            info: ConnectorInfo {
                name: conn_name.to_string(),
                config,
                tasks,
            },
        });
    }

    fn delete_connector_config(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        // Leader check - only leader can delete connector configs
        if !self.is_leader() {
            callback.on_error(format!(
                "NotLeader: only leader can delete connector configs. Current leader: '{}'",
                self.leader_id()
            ));
            return;
        }

        let config = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.get(conn_name).cloned()
        };

        if config.is_none() {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            return;
        }

        let config = config.unwrap();

        {
            let mut configs = self.connector_configs.write().unwrap();
            configs.remove(conn_name);
        }
        {
            let mut states = self.target_states.write().unwrap();
            states.remove(conn_name);
        }
        {
            let mut counts = self.connector_task_counts.write().unwrap();
            counts.remove(conn_name);
        }
        {
            let mut task_configs = self.task_configs.write().unwrap();
            task_configs.retain(|id, _| id.connector != conn_name);
        }
        {
            let mut offsets = self.connector_offsets.write().unwrap();
            offsets.remove(conn_name);
        }

        self.update_config_state_snapshot();

        callback.on_completion(Created {
            created: false,
            info: ConnectorInfo {
                name: conn_name.to_string(),
                config,
                tasks: vec![],
            },
        });
    }

    fn request_task_reconfiguration(&self, conn_name: &str) {
        // Check connector exists
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if !exists {
            return;
        }

        // Create request handle
        let handle = Arc::new(DistributedHerderRequest::new());

        // Create pending request for task reconfiguration
        let request = PendingRequest {
            request_type: PendingRequestType::TaskReconfiguration,
            connector_name: conn_name.to_string(),
            delay_ms: 0, // Immediate reconfiguration request
            created_at: Instant::now(),
            handle,
            callback: None, // No callback for reconfiguration request
        };

        // Add to pending requests queue
        self.add_pending_request(request);
    }

    fn task_configs_async(&self, conn_name: &str, callback: Box<dyn Callback<Vec<TaskInfo>>>) {
        let state = self.config_state.read().unwrap();
        if !state.contains(conn_name) {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            return;
        }
        let tasks = self.get_task_infos(conn_name);
        callback.on_completion(tasks);
    }

    fn put_task_configs(
        &self,
        conn_name: &str,
        configs: Vec<HashMap<String, String>>,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) {
        // Leader check - only leader can write task configs
        if !self.is_leader() {
            callback.on_error(format!(
                "NotLeader: only leader can write task configs for '{}'. Current leader: '{}'",
                conn_name,
                self.leader_id()
            ));
            self.metrics.increment_error_count();
            return;
        }

        // Check connector exists
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if !exists {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            self.metrics.increment_error_count();
            return;
        }

        // Write task configs
        {
            let mut task_configs = self.task_configs.write().unwrap();
            for (index, config) in configs.iter().enumerate() {
                let task_id = StorageConnectorTaskId::new(conn_name.to_string(), index as u32);
                task_configs.insert(task_id, config.clone());
            }
        }

        // Update task count
        {
            let mut counts = self.connector_task_counts.write().unwrap();
            counts.insert(conn_name.to_string(), configs.len() as u32);
        }

        // Update config state snapshot
        self.update_config_state_snapshot();

        callback.on_completion(());
    }

    fn fence_zombie_source_tasks(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) {
        // Leader check - only leader can fence zombie tasks
        if !self.is_leader() {
            callback.on_error(format!(
                "NotLeader: only leader can fence zombie tasks for '{}'. Current leader: '{}'",
                conn_name,
                self.leader_id()
            ));
            self.metrics.increment_error_count();
            return;
        }

        // Check connector exists
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if !exists {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            self.metrics.increment_error_count();
            return;
        }

        // P4-2e: Simplified version - just return success
        // Full implementation with AdminClient.fenceProducers() is P4-3 scope
        callback.on_completion(());
    }

    fn connectors_sync(&self) -> Vec<String> {
        let state = self.config_state.read().unwrap();
        state.connector_configs.keys().cloned().collect()
    }

    fn connector_info_sync(&self, conn_name: &str) -> ConnectorInfo {
        let state = self.config_state.read().unwrap();
        let config = state
            .connector_config(conn_name)
            .cloned()
            .unwrap_or_default();
        let tasks = self.get_task_infos(conn_name);
        ConnectorInfo {
            name: conn_name.to_string(),
            config,
            tasks,
        }
    }

    fn connector_status(&self, conn_name: &str) -> ConnectorStateInfo {
        let state = self.config_state.read().unwrap();
        let target_state = state
            .target_state(conn_name)
            .unwrap_or(TargetState::Started);

        let connector_state = match target_state {
            TargetState::Started => ConnectorState::Running,
            TargetState::Paused => ConnectorState::Paused,
            TargetState::Stopped => ConnectorState::Unassigned,
        };

        let tasks = self.get_task_infos(conn_name);
        let task_states = tasks
            .iter()
            .map(|task| TaskStateInfo {
                id: task.id.clone(),
                state: connector_state,
                worker_id: self.worker.config().worker_id().to_string(),
                trace: None,
            })
            .collect();

        ConnectorStateInfo {
            name: conn_name.to_string(),
            connector: connector_state,
            tasks: task_states,
        }
    }

    fn connector_active_topics(&self, conn_name: &str) -> ActiveTopicsInfo {
        let topics = self.connector_active_topics.read().unwrap();
        let topics_list = topics.get(conn_name).cloned().unwrap_or_default();
        ActiveTopicsInfo {
            connector: conn_name.to_string(),
            topics: topics_list,
        }
    }

    fn reset_connector_active_topics(&self, conn_name: &str) {
        let mut topics = self.connector_active_topics.write().unwrap();
        topics.remove(conn_name);
    }

    fn status_backing_store(&self) -> Box<dyn StatusBackingStore> {
        Box::new(DistributedStatusBackingStore::new())
    }

    fn task_status(&self, id: &ConnectorTaskId) -> TaskStateInfo {
        TaskStateInfo {
            id: ConnectorTaskId {
                connector: id.connector.clone(),
                task: id.task,
            },
            state: ConnectorState::Unassigned,
            worker_id: self.worker.config().worker_id().to_string(),
            trace: None,
        }
    }

    fn validate_connector_config_async(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
    ) {
        // Use DistributedPlugins for validation
        let plugins = DistributedPlugins::new();
        let result = plugins.validate_connector_config(&connector_config);
        callback.on_completion(result);
    }

    fn validate_connector_config_async_with_log(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
        _do_log: bool,
    ) {
        // For simplicity, same as validate_connector_config_async
        self.validate_connector_config_async(connector_config, callback);
    }

    fn restart_task(&self, id: &ConnectorTaskId, callback: Box<dyn Callback<()>>) {
        let storage_id = StorageConnectorTaskId::new(id.connector.clone(), id.task);
        let exists = {
            let task_configs = self.task_configs.read().unwrap();
            task_configs.contains_key(&storage_id)
        };

        if !exists {
            callback.on_error(format!("Task {}-{} does not exist", id.connector, id.task));
            return;
        }

        callback.on_error(format!(
            "Task restart for {}-{} requires worker management (P4-2c)",
            id.connector, id.task
        ));
    }

    fn restart_connector_async(&self, conn_name: &str, callback: Box<dyn Callback<()>>) {
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if !exists {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            return;
        }

        callback.on_error(format!(
            "Connector restart for '{}' requires worker management (P4-2c)",
            conn_name
        ));
    }

    fn restart_connector_delayed(
        &self,
        delay_ms: u64,
        conn_name: &str,
        callback: Box<dyn Callback<()>>,
    ) -> Box<dyn HerderRequest> {
        // Check connector exists
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if !exists {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            return Box::new(DistributedHerderRequest::new());
        }

        // Create request handle
        let handle = Arc::new(DistributedHerderRequest::new());

        // Store callback result for notification
        let callback_result = Arc::new(SimpleCallbackResult::new());

        // Create pending request
        let request = PendingRequest {
            request_type: PendingRequestType::RestartConnector,
            connector_name: conn_name.to_string(),
            delay_ms,
            created_at: Instant::now(),
            handle: handle.clone(),
            callback: Some(callback_result.clone()),
        };

        // Add to pending requests queue
        self.add_pending_request(request);

        // Return handle to caller
        Box::new(DistributedHerderRequestWrapper {
            inner: handle,
            result: callback_result,
        })
    }

    fn pause_connector(&self, connector: &str) {
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(connector)
        };

        if !exists {
            return;
        }

        {
            let mut states = self.target_states.write().unwrap();
            states.insert(connector.to_string(), TargetState::Paused);
        }

        self.update_config_state_snapshot();
    }

    fn resume_connector(&self, connector: &str) {
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(connector)
        };

        if !exists {
            return;
        }

        {
            let mut states = self.target_states.write().unwrap();
            states.insert(connector.to_string(), TargetState::Started);
        }

        self.update_config_state_snapshot();
    }

    fn plugins(&self) -> Box<dyn Plugins> {
        Box::new(DistributedPlugins::new())
    }

    fn kafka_cluster_id(&self) -> String {
        self.kafka_cluster_id.clone()
    }

    fn connector_plugin_config(&self, plugin_name: &str) -> Vec<ConfigKeyInfo> {
        // Use DistributedPlugins to get config keys
        let plugins = DistributedPlugins::new();
        plugins.get_connector_config_keys(plugin_name)
    }

    fn connector_plugin_config_with_version(
        &self,
        plugin_name: &str,
        _version: VersionRange,
    ) -> Vec<ConfigKeyInfo> {
        // For simplicity, ignore version range and return all config keys
        let plugins = DistributedPlugins::new();
        plugins.get_connector_config_keys(plugin_name)
    }

    fn connector_offsets_async(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<ConnectorOffsets>>,
    ) {
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if !exists {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            return;
        }

        let offsets = self.connector_offsets.read().unwrap();
        match offsets.get(conn_name) {
            Some(conn_offsets) => callback.on_completion(conn_offsets.clone()),
            None => callback.on_completion(ConnectorOffsets {
                offsets: HashMap::new(),
            }),
        }
    }

    fn alter_connector_offsets(
        &self,
        conn_name: &str,
        offsets: HashMap<HashMap<String, Value>, HashMap<String, Value>>,
        callback: Box<dyn Callback<Message>>,
    ) {
        // Check connector exists
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if !exists {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            self.metrics.increment_error_count();
            return;
        }

        // P4-2e: Update connector_offsets in-memory HashMap
        // No leader check needed - offsets is local storage
        {
            let mut stored_offsets = self.connector_offsets.write().unwrap();
            let mut new_offsets = HashMap::new();
            for (partition, offset) in offsets {
                new_offsets.insert(
                    Value::Object(partition.into_iter().map(|(k, v)| (k, v)).collect()),
                    Value::Object(offset.into_iter().map(|(k, v)| (k, v)).collect()),
                );
            }
            stored_offsets.insert(
                conn_name.to_string(),
                ConnectorOffsets {
                    offsets: new_offsets,
                },
            );
        }

        callback.on_completion(Message {
            code: 0,
            message: format!("Offsets altered successfully for connector '{}'", conn_name),
        });
    }

    fn logger_level(&self, logger: &str) -> LoggerLevel {
        LoggerLevel {
            logger: logger.to_string(),
            level: "INFO".to_string(),
            effective_level: "INFO".to_string(),
        }
    }

    fn set_worker_logger_level(&self, _namespace: &str, _level: &str) -> Vec<String> {
        vec![]
    }

    // === New Herder trait methods ===

    fn stop_connector_async(&self, connector: &str, callback: Box<dyn Callback<()>>) {
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(connector)
        };

        if !exists {
            callback.on_error(format!("Connector '{}' does not exist", connector));
            self.metrics.increment_error_count();
            return;
        }

        // Update target state to stopped
        {
            let mut states = self.target_states.write().unwrap();
            states.insert(connector.to_string(), TargetState::Stopped);
        }

        self.update_config_state_snapshot();

        // In distributed mode, stopping a connector requires leader coordination
        if !self.is_leader() {
            callback.on_error(format!(
                "NotLeader: only leader can stop connector '{}'. Current leader: '{}'",
                connector,
                self.leader_id()
            ));
            self.metrics.increment_error_count();
            return;
        }

        callback.on_completion(());
    }

    fn restart_connector_and_tasks(
        &self,
        request: &common_trait::herder::RestartRequest,
        callback: Box<dyn Callback<ConnectorStateInfo>>,
    ) {
        let conn_name = request.connector();

        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if !exists {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            self.metrics.increment_error_count();
            return;
        }

        // Get current connector status
        let connector_state_info = self.connector_status(conn_name);

        // If only_failed is true, check if connector is actually failed
        if request.only_failed() && connector_state_info.connector != ConnectorState::Failed {
            // Connector is not failed, no restart needed
            callback.on_completion(connector_state_info);
            return;
        }

        // Restart connector
        let restart_callback = SimpleCallback::<()>::new();
        self.restart_connector_async(conn_name, Box::new(restart_callback.clone()));

        match restart_callback.result() {
            Some(_) => callback.on_completion(self.connector_status(conn_name)),
            None => callback.on_error("Failed to restart connector".to_string()),
        }
    }

    fn reset_connector_offsets(&self, conn_name: &str, callback: Box<dyn Callback<Message>>) {
        let exists = {
            let connector_configs = self.connector_configs.read().unwrap();
            connector_configs.contains_key(conn_name)
        };

        if !exists {
            callback.on_error(format!("Connector '{}' does not exist", conn_name));
            self.metrics.increment_error_count();
            return;
        }

        // Clear all offsets for this connector
        {
            let mut stored_offsets = self.connector_offsets.write().unwrap();
            stored_offsets.remove(conn_name);
        }

        callback.on_completion(Message {
            code: 0,
            message: format!("Offsets reset successfully for connector '{}'", conn_name),
        });
    }

    fn all_logger_levels(&self) -> HashMap<String, LoggerLevel> {
        // Return empty map since we don't track loggers in distributed mode's in-memory state
        // Real implementation would query LogContext.getLoggers()
        HashMap::new()
    }

    fn set_cluster_logger_level(&self, _namespace: &str, _level: &str) {
        // In distributed mode, this should write to the config topic
        // Simplified implementation - no actual cluster-wide logging support
    }

    fn connect_metrics(&self) -> Box<dyn common_trait::herder::ConnectMetrics> {
        // Return a reference to the worker's metrics
        Box::new(DistributedConnectMetrics::new(
            self.worker.config().worker_id().to_string(),
        ))
    }
}

// ===== DistributedConnectMetrics implementation =====

/// ConnectMetrics implementation for DistributedHerder.
struct DistributedConnectMetrics {
    worker_id: String,
}

impl DistributedConnectMetrics {
    fn new(worker_id: String) -> Self {
        DistributedConnectMetrics { worker_id }
    }
}

impl common_trait::herder::ConnectMetrics for DistributedConnectMetrics {
    fn registry(&self) -> &dyn common_trait::herder::MetricsRegistry {
        // Note: We use a static registry since trait objects need stable memory location
        // The actual worker_group_name is stored in self, not in the static registry
        static REGISTRY: DistributedMetricsRegistry = DistributedMetricsRegistry::new_static();
        &REGISTRY
    }

    fn stop(&self) {
        // No actual metrics to stop in simplified implementation
    }
}

/// MetricsRegistry implementation for DistributedHerder.
struct DistributedMetricsRegistry {
    worker_id: String,
}

impl DistributedMetricsRegistry {
    fn new(worker_id: String) -> Self {
        DistributedMetricsRegistry { worker_id }
    }

    const fn new_static() -> Self {
        DistributedMetricsRegistry {
            worker_id: String::new(),
        }
    }
}

impl common_trait::herder::MetricsRegistry for DistributedMetricsRegistry {
    fn worker_group_name(&self) -> &str {
        // For static registry, return empty string as it's only used for trait object stability
        // The actual worker_id is tracked through DistributedConnectMetrics
        &self.worker_id
    }
}

// ===== Unit tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct TestCallback<T: Clone + std::fmt::Debug> {
        result: Arc<Mutex<Option<T>>>,
        error: Arc<Mutex<Option<String>>>,
    }

    impl<T: Clone + std::fmt::Debug> Clone for TestCallback<T> {
        fn clone(&self) -> Self {
            TestCallback {
                result: self.result.clone(),
                error: self.error.clone(),
            }
        }
    }

    impl<T: Clone + std::fmt::Debug> TestCallback<T> {
        fn new() -> Self {
            TestCallback {
                result: Arc::new(Mutex::new(None)),
                error: Arc::new(Mutex::new(None)),
            }
        }

        fn result(&self) -> Option<T> {
            self.result.lock().unwrap().clone()
        }

        fn error(&self) -> Option<String> {
            self.error.lock().unwrap().clone()
        }
    }

    impl<T: Clone + std::fmt::Debug> Callback<T> for TestCallback<T> {
        fn on_completion(&self, result: T) {
            *self.result.lock().unwrap() = Some(result);
        }

        fn on_error(&self, error: String) {
            *self.error.lock().unwrap() = Some(error);
        }
    }

    fn create_test_worker() -> Arc<Worker> {
        Arc::new(Worker::with_defaults("test-worker-1".to_string()))
    }

    fn create_test_config() -> HashMap<String, String> {
        let mut config = HashMap::new();
        config.insert("name".to_string(), "test-connector".to_string());
        config.insert("connector.class".to_string(), "TestConnector".to_string());
        config
    }

    #[test]
    fn test_distributed_herder_creation() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster-id".to_string());
        assert_eq!(herder.kafka_cluster_id(), "test-cluster-id");
        assert!(!herder.is_ready());
        assert!(!herder.is_stopping());
    }

    #[test]
    fn test_start_stop_lifecycle() {
        let worker = create_test_worker();
        let mut herder = DistributedHerder::new(worker, "test-cluster".to_string());
        assert!(!herder.is_ready());
        herder.start();
        assert!(!herder.is_ready());
        herder.stop();
        assert!(!herder.is_ready());
        assert!(herder.is_stopping());
    }

    #[test]
    fn test_health_check_not_ready() {
        let worker = create_test_worker();
        let mut herder = DistributedHerder::new(worker, "test-cluster".to_string());
        herder.start();
        let callback = TestCallback::<()>::new();
        herder.health_check(Box::new(callback.clone()));
        assert!(callback.error().is_some());
    }

    #[test]
    fn test_connectors_sync_empty() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let connectors = herder.connectors_sync();
        assert!(connectors.is_empty());
    }

    #[test]
    fn test_put_connector_config_success() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );

        assert!(callback.result().is_some());
        let created = callback.result().unwrap();
        assert!(created.created);
        assert_eq!(created.info.name, "test-conn");
        assert_eq!(created.info.config, config);

        let connectors = herder.connectors_sync();
        assert!(connectors.contains(&"test-conn".to_string()));
    }

    #[test]
    fn test_put_connector_config_already_exists() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.error().is_some());
    }

    #[test]
    fn test_put_connector_config_validation_error() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = HashMap::new();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));
        assert!(callback.error().is_some());
    }

    #[test]
    fn test_put_connector_config_allow_replace() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().unwrap().created);

        let mut new_config = HashMap::new();
        new_config.insert("name".to_string(), "updated".to_string());
        new_config.insert(
            "connector.class".to_string(),
            "UpdatedConnector".to_string(),
        );

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            new_config.clone(),
            true,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());
        let created = callback.result().unwrap();
        assert!(!created.created);
        assert_eq!(created.info.config, new_config);
    }

    #[test]
    fn test_put_connector_config_with_state() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config_with_state(
            "test-conn",
            config.clone(),
            HerderTargetState::Paused,
            false,
            Box::new(callback.clone()),
        );

        assert!(callback.result().is_some());
        let state = herder.config_state.read().unwrap();
        assert_eq!(state.target_state("test-conn"), Some(TargetState::Paused));
    }

    #[test]
    fn test_delete_connector_config_success() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.delete_connector_config("test-conn", Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let deleted = callback.result().unwrap();
        assert!(!deleted.created);
        assert_eq!(deleted.info.name, "test-conn");

        let connectors = herder.connectors_sync();
        assert!(!connectors.contains(&"test-conn".to_string()));
    }

    #[test]
    fn test_delete_connector_config_not_found() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.delete_connector_config("nonexistent", Box::new(callback.clone()));

        assert!(callback.error().is_some());
    }

    #[test]
    fn test_connectors_async_with_connector() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("conn1", config.clone(), false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("conn2", config.clone(), false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        let callback = TestCallback::<Vec<String>>::new();
        herder.connectors_async(Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let connectors = callback.result().unwrap();
        assert_eq!(connectors.len(), 2);
        assert!(connectors.contains(&"conn1".to_string()));
        assert!(connectors.contains(&"conn2".to_string()));
    }

    #[test]
    fn test_connector_info_async_success() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        let callback = TestCallback::<ConnectorInfo>::new();
        herder.connector_info_async("test-conn", Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let info = callback.result().unwrap();
        assert_eq!(info.name, "test-conn");
        assert_eq!(info.config, config);
    }

    #[test]
    fn test_connector_info_async_not_found() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let callback = TestCallback::<ConnectorInfo>::new();
        herder.connector_info_async("nonexistent", Box::new(callback.clone()));

        assert!(callback.error().is_some());
    }

    #[test]
    fn test_connector_config_async_success() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        let callback = TestCallback::<HashMap<String, String>>::new();
        herder.connector_config_async("test-conn", Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let result_config = callback.result().unwrap();
        assert_eq!(result_config, config);
    }

    #[test]
    fn test_patch_connector_config_success() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        let mut patch = HashMap::new();
        patch.insert("tasks.max".to_string(), "3".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.patch_connector_config("test-conn", patch.clone(), Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let updated = callback.result().unwrap();
        assert!(!updated.created);
        assert!(updated.info.config.contains_key("tasks.max"));
    }

    #[test]
    fn test_pause_connector() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        herder.pause_connector("test-conn");

        let state = herder.config_state.read().unwrap();
        assert_eq!(state.target_state("test-conn"), Some(TargetState::Paused));
    }

    #[test]
    fn test_resume_connector() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config_with_state(
            "test-conn",
            config.clone(),
            HerderTargetState::Paused,
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        herder.resume_connector("test-conn");

        let state = herder.config_state.read().unwrap();
        assert_eq!(state.target_state("test-conn"), Some(TargetState::Started));
    }

    #[test]
    fn test_connector_status() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        let status = herder.connector_status("test-conn");
        assert_eq!(status.name, "test-conn");
        assert_eq!(status.connector, ConnectorState::Running);

        herder.pause_connector("test-conn");
        let status = herder.connector_status("test-conn");
        assert_eq!(status.connector, ConnectorState::Paused);
    }

    #[test]
    fn test_task_configs_async() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        let callback = TestCallback::<Vec<TaskInfo>>::new();
        herder.task_configs_async("test-conn", Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let tasks = callback.result().unwrap();
        assert!(tasks.is_empty());
    }

    #[test]
    fn test_connector_offsets_async() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        let callback = TestCallback::<ConnectorOffsets>::new();
        herder.connector_offsets_async("test-conn", Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let offsets = callback.result().unwrap();
        assert!(offsets.offsets.is_empty());
    }

    #[test]
    fn test_connector_offsets_async_not_found() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let callback = TestCallback::<ConnectorOffsets>::new();
        herder.connector_offsets_async("nonexistent", Box::new(callback.clone()));

        assert!(callback.error().is_some());
    }

    #[test]
    fn test_restart_connector_async_error() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        let callback = TestCallback::<()>::new();
        herder.restart_connector_async("test-conn", Box::new(callback.clone()));

        assert!(callback.error().is_some());
    }

    #[test]
    fn test_restart_task_error() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let callback = TestCallback::<()>::new();
        herder.restart_task(
            &ConnectorTaskId {
                connector: "test-conn".to_string(),
                task: 0,
            },
            Box::new(callback.clone()),
        );

        assert!(callback.error().is_some());
    }

    #[test]
    fn test_herder_request() {
        let request = DistributedHerderRequest::new();
        assert!(!request.is_completed());
        request.cancel();
        assert!(request.cancelled.load(Ordering::SeqCst));
        request.mark_completed();
        assert!(request.is_completed());
    }

    #[test]
    fn test_status_backing_store() {
        let store = DistributedStatusBackingStore::new();
        assert!(store.get_connector_state("test").is_none());
        assert!(store
            .get_task_state(&ConnectorTaskId {
                connector: "test".to_string(),
                task: 0,
            })
            .is_none());
    }

    #[test]
    fn test_plugins() {
        let plugins = DistributedPlugins::new();
        // P4-4d: Now returns real connector classes instead of empty
        assert!(!plugins.connector_plugins().is_empty());
        assert!(plugins.converter_plugins().is_empty() == false); // Not empty
    }

    #[test]
    fn test_config_state_snapshot_updates() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader for write operations
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("conn1", config.clone(), false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        let state = herder.config_state.read().unwrap();
        assert!(state.contains("conn1"));
        assert!(state.connector_config("conn1").is_some());
    }

    // ===== P4-2c: ExtendedAssignment tests =====

    #[test]
    fn test_extended_assignment_empty() {
        let assignment = ExtendedAssignment::empty();
        assert_eq!(assignment.version(), 1);
        assert_eq!(assignment.error(), ASSIGNMENT_ERROR_NONE);
        assert!(!assignment.has_error());
        assert!(assignment.is_empty());
        assert_eq!(assignment.total_count(), 0);
        assert!(assignment.leader_id().is_empty());
        assert!(assignment.leader_url().is_empty());
        assert_eq!(assignment.offset(), -1);
        assert!(assignment.connectors().is_empty());
        assert!(assignment.tasks().is_empty());
        assert!(assignment.revoked_connectors().is_empty());
        assert!(assignment.revoked_tasks().is_empty());
        assert_eq!(assignment.delay(), 0);
    }

    #[test]
    fn test_extended_assignment_new() {
        let connectors = vec!["conn1".to_string(), "conn2".to_string()];
        let tasks = vec![
            StorageConnectorTaskId::new("conn1".to_string(), 0),
            StorageConnectorTaskId::new("conn1".to_string(), 1),
        ];
        let revoked_connectors = vec!["old-conn".to_string()];
        let revoked_tasks = vec![StorageConnectorTaskId::new("old-conn".to_string(), 0)];

        let assignment = ExtendedAssignment::new(
            2,
            ASSIGNMENT_ERROR_NONE,
            "worker-leader".to_string(),
            "http://leader:8083".to_string(),
            42,
            connectors.clone(),
            tasks.clone(),
            revoked_connectors.clone(),
            revoked_tasks.clone(),
            1000,
        );

        assert_eq!(assignment.version(), 2);
        assert!(!assignment.has_error());
        assert_eq!(assignment.leader_id(), "worker-leader");
        assert_eq!(assignment.leader_url(), "http://leader:8083");
        assert_eq!(assignment.offset(), 42);
        assert_eq!(assignment.connectors().len(), 2);
        assert_eq!(assignment.tasks().len(), 2);
        assert_eq!(assignment.revoked_connectors().len(), 1);
        assert_eq!(assignment.revoked_tasks().len(), 1);
        assert_eq!(assignment.delay(), 1000);
        assert!(!assignment.is_empty());
        assert_eq!(assignment.total_count(), 4);
    }

    #[test]
    fn test_extended_assignment_error_codes() {
        let assignment_error = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_UNKNOWN_MEMBER,
            "".to_string(),
            "".to_string(),
            -1,
            vec![],
            vec![],
            vec![],
            vec![],
            0,
        );
        assert!(assignment_error.has_error());
        assert_eq!(assignment_error.error(), ASSIGNMENT_ERROR_UNKNOWN_MEMBER);

        let assignment_timeout = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_ASSIGNMENT_TIMEOUT,
            "".to_string(),
            "".to_string(),
            -1,
            vec![],
            vec![],
            vec![],
            vec![],
            0,
        );
        assert!(assignment_timeout.has_error());
        assert_eq!(
            assignment_timeout.error(),
            ASSIGNMENT_ERROR_ASSIGNMENT_TIMEOUT
        );
    }

    #[test]
    fn test_extended_assignment_clone() {
        let original = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_NONE,
            "leader".to_string(),
            "url".to_string(),
            10,
            vec!["conn".to_string()],
            vec![StorageConnectorTaskId::new("conn".to_string(), 0)],
            vec![],
            vec![],
            0,
        );
        let cloned = original.clone();
        assert_eq!(cloned.leader_id(), original.leader_id());
        assert_eq!(cloned.connectors().len(), original.connectors().len());
    }

    // ===== P4-2c: WorkerGroupMember tests =====

    #[test]
    fn test_worker_group_member_creation() {
        let member = WorkerGroupMember::new("worker-1".to_string());
        assert_eq!(member.worker_id(), "worker-1");
        assert!(!member.is_active());
        assert!(!member.rebalance_in_progress());
        assert_eq!(member.generation(), -1);
        assert!(member.assignment().is_empty());
    }

    #[test]
    fn test_worker_group_member_ensure_active() {
        let member = WorkerGroupMember::new("worker-1".to_string());
        assert!(!member.rebalance_in_progress());
        member.ensure_active();
        assert!(member.rebalance_in_progress());
    }

    #[test]
    fn test_worker_group_member_set_pending_assignment() {
        let member = WorkerGroupMember::new("worker-1".to_string());
        let assignment = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_NONE,
            "leader".to_string(),
            "url".to_string(),
            10,
            vec!["conn".to_string()],
            vec![],
            vec![],
            vec![],
            0,
        );
        member.set_pending_assignment(assignment.clone(), 5);

        // After poll, assignment should be applied
        member.poll(Duration::from_millis(0));
        assert!(member.is_active());
        assert_eq!(member.generation(), 5);
        assert_eq!(member.assignment().leader_id(), "leader");
        assert!(!member.rebalance_in_progress());
    }

    #[test]
    fn test_worker_group_member_trigger_rebalance() {
        let member = WorkerGroupMember::new("worker-1".to_string());
        // First activate with an assignment
        member.set_pending_assignment(ExtendedAssignment::empty(), 1);
        member.poll(Duration::from_millis(0));
        assert!(member.is_active());
        assert!(!member.rebalance_in_progress());

        // Trigger rebalance - sets trigger_rebalance flag
        member.trigger_rebalance_for_test();
        // rebalance_in_progress is still false before poll
        assert!(!member.rebalance_in_progress());

        // Poll should process the trigger and set rebalance_in_progress
        member.poll(Duration::from_millis(0));
        // After poll, rebalance_in_progress should be true (trigger was processed)
        assert!(member.rebalance_in_progress());
    }

    #[test]
    fn test_worker_group_member_request_rejoin() {
        let member = WorkerGroupMember::new("worker-1".to_string());
        member.set_pending_assignment(ExtendedAssignment::empty(), 1);
        member.poll(Duration::from_millis(0));
        assert!(member.is_active());

        member.request_rejoin();
        assert!(!member.is_active());
        assert!(member.rebalance_in_progress());
    }

    // ===== P4-2c: DistributedHerder rebalance tests =====

    #[test]
    fn test_distributed_herder_is_leader() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        // Initially not leader (no leader_id set)
        assert!(!herder.is_leader());

        // Set leader_id to different worker
        herder.set_leader_id_for_test("other-worker".to_string());
        assert!(!herder.is_leader());

        // Set leader_id to this worker
        herder.set_leader_id_for_test("test-worker-1".to_string());
        assert!(herder.is_leader());
    }

    #[test]
    fn test_distributed_herder_generation() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        assert_eq!(herder.generation(), -1);

        // Trigger rebalance with assignment
        let assignment = ExtendedAssignment::empty();
        herder.trigger_rebalance_for_test(assignment, 10);
        herder.tick();

        assert_eq!(herder.generation(), 10);
    }

    #[test]
    fn test_distributed_herder_assignment() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        assert!(herder.assignment().is_empty());

        // Create assignment with connectors
        let assignment = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_NONE,
            "test-worker-1".to_string(),
            "http://localhost:8083".to_string(),
            0,
            vec!["conn1".to_string()],
            vec![StorageConnectorTaskId::new("conn1".to_string(), 0)],
            vec![],
            vec![],
            0,
        );
        herder.trigger_rebalance_for_test(assignment.clone(), 1);
        herder.tick();

        let current = herder.assignment();
        assert_eq!(current.connectors().len(), 1);
        assert_eq!(current.connectors()[0], "conn1");
        assert_eq!(current.tasks().len(), 1);
    }

    #[test]
    fn test_distributed_herder_running_assignment() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        assert!(herder.running_assignment().is_empty());

        // Trigger rebalance
        let assignment = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_NONE,
            "test-worker-1".to_string(),
            "url".to_string(),
            0,
            vec!["conn1".to_string()],
            vec![],
            vec![],
            vec![],
            0,
        );
        herder.trigger_rebalance_for_test(assignment, 1);
        herder.tick();

        // Running assignment should be updated after rebalance completes
        let running = herder.running_assignment();
        assert_eq!(running.connectors().len(), 1);
    }

    #[test]
    fn test_distributed_herder_tick_sets_ready() {
        let worker = create_test_worker();
        let mut herder = DistributedHerder::new(worker, "test-cluster".to_string());
        herder.start();

        // Initially not ready (no assignment)
        assert!(!herder.is_ready());

        // Trigger rebalance with assignment
        let assignment = ExtendedAssignment::empty();
        herder.trigger_rebalance_for_test(assignment, 1);
        herder.tick();

        // Should be ready after receiving first assignment
        assert!(herder.is_ready());
    }

    #[test]
    fn test_distributed_herder_tick_stops_if_stopping() {
        let worker = create_test_worker();
        let mut herder = DistributedHerder::new(worker, "test-cluster".to_string());
        herder.start();

        // Set pending assignment
        let assignment = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_NONE,
            "leader".to_string(),
            "url".to_string(),
            0,
            vec!["conn".to_string()],
            vec![],
            vec![],
            vec![],
            0,
        );
        herder.trigger_rebalance_for_test(assignment.clone(), 1);

        // Stop before tick
        herder.stop();
        herder.tick();

        // Assignment should not be applied (still stopping)
        assert!(herder.assignment().is_empty());
        assert_eq!(herder.generation(), -1);
    }

    #[test]
    fn test_distributed_herder_member_accessor() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let member = herder.member();
        assert_eq!(member.worker_id(), "test-worker-1");
    }

    #[test]
    fn test_distributed_herder_leader_updates_on_assignment() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        // Trigger rebalance with assignment where this worker is leader
        let assignment = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_NONE,
            "test-worker-1".to_string(), // This worker is leader
            "http://localhost:8083".to_string(),
            0,
            vec![],
            vec![],
            vec![],
            vec![],
            0,
        );
        herder.trigger_rebalance_for_test(assignment, 1);
        herder.tick();

        assert_eq!(herder.leader_id(), "test-worker-1");
        assert!(herder.is_leader());

        // Now trigger rebalance with different leader
        let assignment2 = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_NONE,
            "other-worker".to_string(), // Different leader
            "http://other:8083".to_string(),
            0,
            vec!["conn".to_string()],
            vec![],
            vec![],
            vec![],
            0,
        );
        herder.trigger_rebalance_for_test(assignment2, 2);
        herder.tick();

        assert_eq!(herder.leader_id(), "other-worker");
        assert!(!herder.is_leader());
    }

    #[test]
    fn test_rebalance_resolving_flag() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        // Set pending assignment
        let assignment = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_NONE,
            "leader".to_string(),
            "url".to_string(),
            0,
            vec!["conn".to_string()],
            vec![],
            vec![],
            vec![],
            0,
        );
        herder.trigger_rebalance_for_test(assignment, 1);

        // Tick should process assignment and set rebalance_resolving
        herder.tick();

        // After tick, rebalance_resolving should be cleared
        // (handle_rebalance_completed runs and clears it)
        assert!(!herder.rebalance_resolving.load(Ordering::SeqCst));
    }

    #[test]
    fn test_assignment_error_handling() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        // Trigger rebalance with error assignment
        let assignment = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_INCONSISTENT_STATE,
            "".to_string(),
            "".to_string(),
            -1,
            vec![],
            vec![],
            vec![],
            vec![],
            0,
        );
        herder.trigger_rebalance_for_test(assignment, 1);
        herder.tick();

        // Assignment should still be applied (error handling is minimal in simplified version)
        assert!(herder.assignment().has_error());
        assert_eq!(
            herder.assignment().error(),
            ASSIGNMENT_ERROR_INCONSISTENT_STATE
        );
    }

    #[test]
    fn test_running_assignment_diff_calculation() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        // First assignment: connector1
        let assignment1 = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_NONE,
            "leader".to_string(),
            "url".to_string(),
            0,
            vec!["connector1".to_string()],
            vec![StorageConnectorTaskId::new("connector1".to_string(), 0)],
            vec![],
            vec![],
            0,
        );
        herder.trigger_rebalance_for_test(assignment1, 1);
        herder.tick();

        let running1 = herder.running_assignment();
        assert_eq!(running1.connectors().len(), 1);
        assert_eq!(running1.connectors()[0], "connector1");

        // Second assignment: connector2 instead of connector1 (revokes connector1)
        let assignment2 = ExtendedAssignment::new(
            1,
            ASSIGNMENT_ERROR_NONE,
            "leader".to_string(),
            "url".to_string(),
            0,
            vec!["connector2".to_string()], // New connector
            vec![StorageConnectorTaskId::new("connector2".to_string(), 0)],
            vec!["connector1".to_string()], // Revoked connector
            vec![StorageConnectorTaskId::new("connector1".to_string(), 0)],
            0,
        );
        herder.trigger_rebalance_for_test(assignment2, 2);
        herder.tick();

        let running2 = herder.running_assignment();
        assert_eq!(running2.connectors().len(), 1);
        assert_eq!(running2.connectors()[0], "connector2");
        // connector1 should be removed
        assert!(!running2.connectors().contains(&"connector1".to_string()));
    }

    // ===== P4-4d: Leader check tests =====

    #[test]
    fn test_put_connector_config_not_leader() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as NOT leader
        herder.set_leader_id_for_test("other-worker".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));

        // Should get NotLeader error
        assert!(callback.error().is_some());
        let error = callback.error().unwrap();
        assert!(error.contains("NotLeader"));
        assert!(error.contains("other-worker"));
    }

    #[test]
    fn test_put_connector_config_as_leader() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Set this worker as leader
        herder.set_leader_id_for_test("test-worker-1".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-conn",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );

        // Should succeed as leader
        assert!(callback.result().is_some());
        let created = callback.result().unwrap();
        assert!(created.created);
    }

    #[test]
    fn test_delete_connector_config_not_leader() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // First create connector as leader
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Now switch to non-leader and try to delete
        herder.set_leader_id_for_test("other-worker".to_string());
        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.delete_connector_config("test-conn", Box::new(callback.clone()));

        // Should get NotLeader error
        assert!(callback.error().is_some());
        let error = callback.error().unwrap();
        assert!(error.contains("NotLeader"));
    }

    #[test]
    fn test_patch_connector_config_not_leader() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // First create connector as leader
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Now switch to non-leader and try to patch
        herder.set_leader_id_for_test("other-worker".to_string());
        let mut patch = HashMap::new();
        patch.insert("tasks.max".to_string(), "5".to_string());

        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.patch_connector_config("test-conn", patch, Box::new(callback.clone()));

        // Should get NotLeader error
        assert!(callback.error().is_some());
        let error = callback.error().unwrap();
        assert!(error.contains("NotLeader"));
    }

    // ===== P4-4d: validate_connector_config_async tests =====

    #[test]
    fn test_validate_connector_config_async_success() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let mut config = HashMap::new();
        config.insert("name".to_string(), "test-connector".to_string());
        config.insert(
            "connector.class".to_string(),
            "FileStreamSource".to_string(),
        );

        let callback = TestCallback::<ConfigInfos>::new();
        herder.validate_connector_config_async(config, Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let result = callback.result().unwrap();
        assert_eq!(result.error_count, 0);
        assert_eq!(result.configs.len(), 3); // connector.class, name, tasks.max
    }

    #[test]
    fn test_validate_connector_config_async_missing_class() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let mut config = HashMap::new();
        config.insert("name".to_string(), "test-connector".to_string());
        // Missing connector.class

        let callback = TestCallback::<ConfigInfos>::new();
        herder.validate_connector_config_async(config, Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let result = callback.result().unwrap();
        assert!(result.error_count > 0);
        // Check that connector.class has error
        let class_config = result
            .configs
            .iter()
            .find(|c| c.definition.name == "connector.class");
        assert!(class_config.is_some());
        assert!(!class_config.unwrap().errors.is_empty());
    }

    #[test]
    fn test_validate_connector_config_async_missing_name() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let mut config = HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "FileStreamSource".to_string(),
        );
        // Missing name

        let callback = TestCallback::<ConfigInfos>::new();
        herder.validate_connector_config_async(config, Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let result = callback.result().unwrap();
        assert!(result.error_count > 0);
        // Check that name has error
        let name_config = result.configs.iter().find(|c| c.definition.name == "name");
        assert!(name_config.is_some());
        assert!(!name_config.unwrap().errors.is_empty());
    }

    #[test]
    fn test_validate_connector_config_async_unknown_class() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let mut config = HashMap::new();
        config.insert("name".to_string(), "test-connector".to_string());
        config.insert(
            "connector.class".to_string(),
            "UnknownConnectorClass".to_string(),
        );

        let callback = TestCallback::<ConfigInfos>::new();
        herder.validate_connector_config_async(config, Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let result = callback.result().unwrap();
        assert!(result.error_count > 0);
        // Check that connector.class has unknown class error
        let class_config = result
            .configs
            .iter()
            .find(|c| c.definition.name == "connector.class");
        assert!(class_config.is_some());
        let errors = &class_config.unwrap().errors;
        assert!(errors.iter().any(|e| e.contains("Unknown connector class")));
    }

    #[test]
    fn test_validate_connector_config_async_invalid_tasks_max() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let mut config = HashMap::new();
        config.insert("name".to_string(), "test-connector".to_string());
        config.insert(
            "connector.class".to_string(),
            "FileStreamSource".to_string(),
        );
        config.insert("tasks.max".to_string(), "0".to_string()); // Invalid: must be >= 1

        let callback = TestCallback::<ConfigInfos>::new();
        herder.validate_connector_config_async(config, Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let result = callback.result().unwrap();
        assert!(result.error_count > 0);
        // Check that tasks.max has error
        let tasks_config = result
            .configs
            .iter()
            .find(|c| c.definition.name == "tasks.max");
        assert!(tasks_config.is_some());
        assert!(!tasks_config.unwrap().errors.is_empty());
    }

    // ===== P4-4d: DistributedPlugins tests =====

    #[test]
    fn test_distributed_plugins_connector_plugins() {
        let plugins = DistributedPlugins::new();
        let connectors = plugins.connector_plugins();
        assert!(!connectors.is_empty());
        assert!(connectors.contains(&"FileStreamSource".to_string()));
        assert!(connectors.contains(&"FileStreamSink".to_string()));
    }

    #[test]
    fn test_distributed_plugins_converter_plugins() {
        let plugins = DistributedPlugins::new();
        let converters = plugins.converter_plugins();
        assert!(!converters.is_empty());
        assert!(
            converters.contains(&"org.apache.kafka.connect.storage.StringConverter".to_string())
        );
    }

    #[test]
    fn test_distributed_plugins_transformation_plugins() {
        let plugins = DistributedPlugins::new();
        let transformations = plugins.transformation_plugins();
        assert!(!transformations.is_empty());
    }

    #[test]
    fn test_distributed_plugins_connector_plugin_desc() {
        let plugins = DistributedPlugins::new();

        // Test known connector
        let desc = plugins.connector_plugin_desc("FileStreamSource");
        assert!(desc.is_some());
        let desc = desc.unwrap();
        assert_eq!(desc.class_name, "FileStreamSource");
        assert_eq!(desc.plugin_type, PluginType::Source);
        assert_eq!(desc.version, "1.0.0");

        // Test known sink connector
        let desc = plugins.connector_plugin_desc("FileStreamSink");
        assert!(desc.is_some());
        let desc = desc.unwrap();
        assert_eq!(desc.plugin_type, PluginType::Sink);

        // Test unknown connector
        let desc = plugins.connector_plugin_desc("UnknownConnector");
        assert!(desc.is_none());
    }

    #[test]
    fn test_distributed_plugins_converter_plugin_desc() {
        let plugins = DistributedPlugins::new();

        let desc = plugins.converter_plugin_desc("org.apache.kafka.connect.storage.JsonConverter");
        assert!(desc.is_some());
        let desc = desc.unwrap();
        assert_eq!(desc.plugin_type, PluginType::Converter);
    }

    // ===== P4-4d: connector_plugin_config tests =====

    #[test]
    fn test_connector_plugin_config_basic() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let config_keys = herder.connector_plugin_config("FileStreamSource");
        assert!(!config_keys.is_empty());

        // Should have at least connector.class, name, tasks.max
        let names: Vec<&str> = config_keys.iter().map(|k| k.name.as_str()).collect();
        assert!(names.contains(&"connector.class"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"tasks.max"));
    }

    #[test]
    fn test_connector_plugin_config_source_specific() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let config_keys = herder.connector_plugin_config("FileStreamSource");

        // Should have file config for FileStreamSource
        let names: Vec<&str> = config_keys.iter().map(|k| k.name.as_str()).collect();
        assert!(names.contains(&"file"));
    }

    #[test]
    fn test_connector_plugin_config_sink_specific() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let config_keys = herder.connector_plugin_config("FileStreamSink");

        // Should have topics config for FileStreamSink
        let names: Vec<&str> = config_keys.iter().map(|k| k.name.as_str()).collect();
        assert!(names.contains(&"topics"));
    }

    #[test]
    fn test_connector_plugin_config_required_fields() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let config_keys = herder.connector_plugin_config("FileStreamSource");

        // connector.class and name should be required
        let class_key = config_keys.iter().find(|k| k.name == "connector.class");
        assert!(class_key.is_some());
        assert!(class_key.unwrap().required);

        let name_key = config_keys.iter().find(|k| k.name == "name");
        assert!(name_key.is_some());
        assert!(name_key.unwrap().required);

        // tasks.max should be optional
        let tasks_key = config_keys.iter().find(|k| k.name == "tasks.max");
        assert!(tasks_key.is_some());
        assert!(!tasks_key.unwrap().required);
    }

    #[test]
    fn test_connector_plugin_config_with_version() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        // Version range should be ignored in simplified implementation
        let version_range = VersionRange::new(Some("1.0.0".to_string()), None, true, false);
        let config_keys =
            herder.connector_plugin_config_with_version("FileStreamSource", version_range);

        assert!(!config_keys.is_empty());
    }

    #[test]
    fn test_config_key_info_properties() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let config_keys = herder.connector_plugin_config("FileStreamSource");

        // Check properties of ConfigKeyInfo
        let class_key = config_keys
            .iter()
            .find(|k| k.name == "connector.class")
            .unwrap();
        assert_eq!(class_key.config_type, "STRING");
        assert_eq!(class_key.importance, "HIGH");
        assert!(!class_key.documentation.is_empty());
        assert!(!class_key.display_name.is_empty());
    }

    #[test]
    fn test_plugins_integration_with_herder() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        // Herder.plugins() should return a working Plugins instance
        let plugins = herder.plugins();
        let connectors = plugins.connector_plugins();
        assert!(!connectors.is_empty());

        let desc = plugins.connector_plugin_desc("FileStreamSource");
        assert!(desc.is_some());
    }

    // ===== P4-2e: HerderMetrics tests =====

    #[test]
    fn test_herder_metrics_creation() {
        let metrics = HerderMetrics::new();
        assert_eq!(metrics.request_count_value(), 0);
        assert_eq!(metrics.rebalance_count_value(), 0);
        assert_eq!(metrics.error_count_value(), 0);
    }

    #[test]
    fn test_herder_metrics_increment() {
        let metrics = HerderMetrics::new();
        metrics.increment_request_count();
        assert_eq!(metrics.request_count_value(), 1);
        metrics.increment_request_count();
        assert_eq!(metrics.request_count_value(), 2);

        metrics.increment_rebalance_count();
        assert_eq!(metrics.rebalance_count_value(), 1);

        metrics.increment_error_count();
        assert_eq!(metrics.error_count_value(), 1);
        metrics.increment_error_count();
        assert_eq!(metrics.error_count_value(), 2);
    }

    #[test]
    fn test_herder_metrics_accessor() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let metrics = herder.metrics();
        assert_eq!(metrics.request_count_value(), 0);
        assert_eq!(metrics.rebalance_count_value(), 0);
        assert_eq!(metrics.error_count_value(), 0);
    }

    // ===== P4-2e: put_task_configs tests =====

    #[test]
    fn test_put_task_configs_not_leader() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Create connector first as leader
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Switch to non-leader and try put_task_configs
        herder.set_leader_id_for_test("other-worker".to_string());
        let callback = TestCallback::<()>::new();
        herder.put_task_configs(
            "test-conn",
            vec![HashMap::new()],
            Box::new(callback.clone()),
            InternalRequestSignature {
                key: "test".to_string(),
                signature: "sig".to_string(),
            },
        );

        assert!(callback.error().is_some());
        let error = callback.error().unwrap();
        assert!(error.contains("NotLeader"));
    }

    #[test]
    fn test_put_task_configs_connector_not_found() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        // Set as leader but connector doesn't exist
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<()>::new();
        herder.put_task_configs(
            "nonexistent",
            vec![HashMap::new()],
            Box::new(callback.clone()),
            InternalRequestSignature {
                key: "test".to_string(),
                signature: "sig".to_string(),
            },
        );

        assert!(callback.error().is_some());
        let error = callback.error().unwrap();
        assert!(error.contains("does not exist"));
    }

    #[test]
    fn test_put_task_configs_success() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Create connector as leader
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Put task configs as leader
        let mut task_config1 = HashMap::new();
        task_config1.insert("task.class".to_string(), "TestTask".to_string());
        let mut task_config2 = HashMap::new();
        task_config2.insert("task.class".to_string(), "TestTask".to_string());

        let callback = TestCallback::<()>::new();
        herder.put_task_configs(
            "test-conn",
            vec![task_config1.clone(), task_config2.clone()],
            Box::new(callback.clone()),
            InternalRequestSignature {
                key: "test".to_string(),
                signature: "sig".to_string(),
            },
        );

        assert!(callback.result().is_some());

        // Verify task configs were stored
        let task_configs = herder.task_configs.read().unwrap();
        assert_eq!(task_configs.len(), 2);

        // Verify task count was updated
        let counts = herder.connector_task_counts.read().unwrap();
        assert_eq!(counts.get("test-conn"), Some(&2));
    }

    // ===== P4-2e: fence_zombie_source_tasks tests =====

    #[test]
    fn test_fence_zombie_source_tasks_not_leader() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Create connector as leader
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Switch to non-leader and try fence
        herder.set_leader_id_for_test("other-worker".to_string());
        let callback = TestCallback::<()>::new();
        herder.fence_zombie_source_tasks(
            "test-conn",
            Box::new(callback.clone()),
            InternalRequestSignature {
                key: "test".to_string(),
                signature: "sig".to_string(),
            },
        );

        assert!(callback.error().is_some());
        assert!(callback.error().unwrap().contains("NotLeader"));
    }

    #[test]
    fn test_fence_zombie_source_tasks_success() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Create connector as leader
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Fence as leader (simplified - returns success)
        let callback = TestCallback::<()>::new();
        herder.fence_zombie_source_tasks(
            "test-conn",
            Box::new(callback.clone()),
            InternalRequestSignature {
                key: "test".to_string(),
                signature: "sig".to_string(),
            },
        );

        assert!(callback.result().is_some());
    }

    #[test]
    fn test_fence_zombie_source_tasks_connector_not_found() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        // Set as leader but connector doesn't exist
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<()>::new();
        herder.fence_zombie_source_tasks(
            "nonexistent",
            Box::new(callback.clone()),
            InternalRequestSignature {
                key: "test".to_string(),
                signature: "sig".to_string(),
            },
        );

        assert!(callback.error().is_some());
    }

    // ===== P4-2e: alter_connector_offsets tests =====

    #[test]
    fn test_alter_connector_offsets_connector_not_found() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let callback = TestCallback::<Message>::new();
        herder.alter_connector_offsets("nonexistent", HashMap::new(), Box::new(callback.clone()));

        assert!(callback.error().is_some());
    }

    #[test]
    fn test_alter_connector_offsets_success() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Create connector as leader
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Alter offsets - use empty offsets for test (HashMap as key requires Hash trait)
        // The trait signature uses HashMap<HashMap<String, Value>, HashMap<String, Value>>
        // which requires the inner HashMap to implement Hash (not available in std)
        // For testing, we pass empty offsets
        let offsets = HashMap::new();

        let callback = TestCallback::<Message>::new();
        herder.alter_connector_offsets("test-conn", offsets, Box::new(callback.clone()));

        assert!(callback.result().is_some());
        let message = callback.result().unwrap();
        assert_eq!(message.code, 0);
        assert!(message.message.contains("successfully"));

        // Verify offsets were stored (empty offsets)
        let stored_offsets = herder.connector_offsets.read().unwrap();
        assert!(stored_offsets.contains_key("test-conn"));
        assert!(stored_offsets.get("test-conn").unwrap().offsets.is_empty());
    }

    // ===== P4-2e: restart_connector_delayed tests =====

    #[test]
    fn test_restart_connector_delayed_connector_not_found() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());

        let callback = TestCallback::<()>::new();
        let request =
            herder.restart_connector_delayed(1000, "nonexistent", Box::new(callback.clone()));

        assert!(callback.error().is_some());
        let error = callback.error().unwrap();
        assert!(error.contains("does not exist"));
    }

    #[test]
    fn test_restart_connector_delayed_success() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Create connector as leader
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Request delayed restart
        let callback = TestCallback::<()>::new();
        let request =
            herder.restart_connector_delayed(100, "test-conn", Box::new(callback.clone()));

        // Request should be created
        assert!(!request.is_completed());

        // Verify request was added to queue
        let pending = herder.pending_requests.read().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].connector_name, "test-conn");
        assert_eq!(pending[0].delay_ms, 100);
        assert_eq!(
            pending[0].request_type,
            PendingRequestType::RestartConnector
        );

        // Metrics should be incremented
        assert_eq!(herder.metrics().request_count_value(), 1);
    }

    #[test]
    fn test_restart_connector_delayed_cancel() {
        let worker = create_test_worker();
        let herder = DistributedHerder::new(worker, "test-cluster".to_string());
        let config = create_test_config();

        // Create connector as leader
        herder.set_leader_id_for_test("test-worker-1".to_string());
        let callback = TestCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-conn", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Request delayed restart
        let callback = TestCallback::<()>::new();
        let request = herder.restart_connector_delayed(
            10000, // Long delay
            "test-conn",
            Box::new(callback.clone()),
        );

        // Cancel the request
        request.cancel();
        assert!(!request.is_completed());

        // Process pending requests - cancelled request should be skipped
        herder.process_pending_requests();

        // Request should still not be completed (cancelled)
        assert!(!request.is_completed());
    }

    #[test]
    fn test_pending_request_type_enum() {
        assert_eq!(
            PendingRequestType::RestartConnector,
            PendingRequestType::RestartConnector
        );
        assert_ne!(
            PendingRequestType::RestartConnector,
            PendingRequestType::TaskReconfiguration
        );
    }

    #[test]
    fn test_simple_callback_result() {
        let result = SimpleCallbackResult::new();
        assert!(!result.was_successful());
        assert!(result.error().is_none());

        result.on_success();
        assert!(result.was_successful());

        result.on_error("test error".to_string());
        assert!(result.error().is_some());
        assert_eq!(result.error().unwrap(), "test error");
    }

    #[test]
    fn test_distributed_herder_request_wrapper() {
        let inner = Arc::new(DistributedHerderRequest::new());
        let result = Arc::new(SimpleCallbackResult::new());
        let wrapper = DistributedHerderRequestWrapper {
            inner: inner.clone(),
            result: result.clone(),
        };

        assert!(!wrapper.is_completed());
        assert!(!wrapper.was_successful());

        inner.mark_completed();
        assert!(wrapper.is_completed());

        result.on_success();
        assert!(wrapper.was_successful());
    }
}
