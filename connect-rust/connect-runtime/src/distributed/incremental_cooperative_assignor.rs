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

//! An assignor that computes a distribution of connectors and tasks according to the incremental
//! cooperative strategy for rebalancing.
//!
//! Note that this class is NOT thread-safe.
//!
//! See KIP-415 for a description of the assignment policy:
//! <https://cwiki.apache.org/confluence/display/KAFKA/KIP-415%3A+Incremental+Cooperative+Rebalancing+in+Kafka+Connect>
//!
//! Corresponds to `org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeAssignor` in Java.

use crate::distributed::connect_assignor::{
    ConnectAssignor, JoinGroupResponseMember, WorkerCoordinatorState,
};
use crate::distributed::connect_protocol::{
    ConnectProtocolCompatibility, ConnectorTaskId, ASSIGNMENT_CONFIG_MISMATCH, ASSIGNMENT_NO_ERROR,
    CONNECT_PROTOCOL_V1, CONNECT_PROTOCOL_V2,
};
use crate::distributed::extended_assignment::{ExtendedAssignment, CONFIG_MISMATCH, NO_ERROR};
use crate::distributed::extended_worker_state::ExtendedWorkerState;
use crate::distributed::incremental_cooperative_connect_protocol::IncrementalCooperativeConnectProtocol;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

/// Represents a collection of connectors and tasks.
///
/// This is used to track assignments, revocations, and other collections
/// of connectors and tasks throughout the assignment process.
///
/// Corresponds to `WorkerCoordinator.ConnectorsAndTasks` in Java.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorsAndTasks {
    connectors: BTreeSet<String>,
    tasks: BTreeSet<ConnectorTaskId>,
}

impl ConnectorsAndTasks {
    /// Empty connectors and tasks instance.
    pub const EMPTY: ConnectorsAndTasks = ConnectorsAndTasks {
        connectors: BTreeSet::new(),
        tasks: BTreeSet::new(),
    };

    /// Creates a new ConnectorsAndTasks with the given connectors and tasks.
    pub fn new(connectors: BTreeSet<String>, tasks: BTreeSet<ConnectorTaskId>) -> Self {
        ConnectorsAndTasks { connectors, tasks }
    }

    /// Creates an empty ConnectorsAndTasks.
    pub fn empty() -> Self {
        ConnectorsAndTasks::EMPTY.clone()
    }

    /// Returns the connectors.
    pub fn connectors(&self) -> &BTreeSet<String> {
        &self.connectors
    }

    /// Returns the tasks.
    pub fn tasks(&self) -> &BTreeSet<ConnectorTaskId> {
        &self.tasks
    }

    /// Returns true if this collection is empty.
    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty() && self.tasks.is_empty()
    }

    /// Returns the total size (connectors + tasks).
    pub fn size(&self) -> usize {
        self.connectors.len() + self.tasks.len()
    }

    /// Adds a connector.
    pub fn add_connector(&mut self, connector: String) {
        self.connectors.insert(connector);
    }

    /// Adds a task.
    pub fn add_task(&mut self, task: ConnectorTaskId) {
        self.tasks.insert(task);
    }

    /// Adds all connectors and tasks from another instance.
    pub fn add_all(&mut self, other: &ConnectorsAndTasks) {
        for connector in other.connectors.iter() {
            self.connectors.insert(connector.clone());
        }
        for task in other.tasks.iter() {
            self.tasks.insert(task.clone());
        }
    }

    /// Removes all connectors and tasks from another instance.
    pub fn remove_all(&mut self, other: &ConnectorsAndTasks) {
        for connector in other.connectors.iter() {
            self.connectors.remove(connector);
        }
        for task in other.tasks.iter() {
            self.tasks.remove(task);
        }
    }

    /// Returns true if this collection contains the given connector.
    pub fn contains_connector(&self, connector: &str) -> bool {
        self.connectors.contains(connector)
    }

    /// Returns true if this collection contains the given task.
    pub fn contains_task(&self, task: &ConnectorTaskId) -> bool {
        self.tasks.contains(task)
    }

    /// Clears all connectors and tasks.
    pub fn clear(&mut self) {
        self.connectors.clear();
        self.tasks.clear();
    }
}

impl std::fmt::Display for ConnectorsAndTasks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConnectorsAndTasks{{connectors={:?}, tasks={:?}}}",
            self.connectors, self.tasks
        )
    }
}

/// Builder for creating ConnectorsAndTasks instances.
pub struct ConnectorsAndTasksBuilder {
    connectors: BTreeSet<String>,
    tasks: BTreeSet<ConnectorTaskId>,
}

impl ConnectorsAndTasksBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        ConnectorsAndTasksBuilder {
            connectors: BTreeSet::new(),
            tasks: BTreeSet::new(),
        }
    }

    /// Adds connectors to the builder.
    pub fn with_connectors(mut self, connectors: impl IntoIterator<Item = String>) -> Self {
        for connector in connectors {
            self.connectors.insert(connector);
        }
        self
    }

    /// Adds tasks to the builder.
    pub fn with_tasks(mut self, tasks: impl IntoIterator<Item = ConnectorTaskId>) -> Self {
        for task in tasks {
            self.tasks.insert(task);
        }
        self
    }

    /// Adds connectors and tasks from collections.
    pub fn with(
        mut self,
        connectors: impl IntoIterator<Item = String>,
        tasks: impl IntoIterator<Item = ConnectorTaskId>,
    ) -> Self {
        for connector in connectors {
            self.connectors.insert(connector);
        }
        for task in tasks {
            self.tasks.insert(task);
        }
        self
    }

    /// Adds all from another ConnectorsAndTasks.
    pub fn add_all(mut self, other: &ConnectorsAndTasks) -> Self {
        self.connectors.extend(other.connectors.iter().cloned());
        self.tasks.extend(other.tasks.iter().cloned());
        self
    }

    /// Adds a single connector.
    pub fn add_connector(mut self, connector: String) -> Self {
        self.connectors.insert(connector);
        self
    }

    /// Adds a single task.
    pub fn add_task(mut self, task: ConnectorTaskId) -> Self {
        self.tasks.insert(task);
        self
    }

    /// Builds the ConnectorsAndTasks instance.
    pub fn build(self) -> ConnectorsAndTasks {
        ConnectorsAndTasks::new(self.connectors, self.tasks)
    }

    /// Adds connectors to the builder in-place (mutable).
    pub fn add_connectors_mut(&mut self, connectors: impl IntoIterator<Item = String>) {
        for connector in connectors {
            self.connectors.insert(connector);
        }
    }

    /// Adds tasks to the builder in-place (mutable).
    pub fn add_tasks_mut(&mut self, tasks: impl IntoIterator<Item = ConnectorTaskId>) {
        for task in tasks {
            self.tasks.insert(task);
        }
    }

    /// Adds all from another ConnectorsAndTasks in-place (mutable).
    pub fn add_all_mut(&mut self, other: &ConnectorsAndTasks) {
        self.connectors.extend(other.connectors.iter().cloned());
        self.tasks.extend(other.tasks.iter().cloned());
    }
}

impl Default for ConnectorsAndTasksBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents the load on a worker including assigned connectors and tasks.
///
/// Corresponds to `WorkerCoordinator.WorkerLoad` in Java.
#[derive(Debug, Clone)]
pub struct WorkerLoad {
    worker_id: String,
    connectors: Vec<String>,
    tasks: Vec<ConnectorTaskId>,
}

impl WorkerLoad {
    /// Creates a new WorkerLoad for the given worker.
    pub fn new(worker_id: String) -> Self {
        WorkerLoad {
            worker_id,
            connectors: Vec::new(),
            tasks: Vec::new(),
        }
    }

    /// Creates a WorkerLoad with existing assignments.
    pub fn with_assignments(
        worker_id: String,
        connectors: Vec<String>,
        tasks: Vec<ConnectorTaskId>,
    ) -> Self {
        WorkerLoad {
            worker_id,
            connectors,
            tasks,
        }
    }

    /// Returns the worker ID.
    pub fn worker(&self) -> &str {
        &self.worker_id
    }

    /// Returns the connectors.
    pub fn connectors(&self) -> &[String] {
        &self.connectors
    }

    /// Returns the tasks.
    pub fn tasks(&self) -> &[ConnectorTaskId] {
        &self.tasks
    }

    /// Returns the number of connectors.
    pub fn connectors_size(&self) -> usize {
        self.connectors.len()
    }

    /// Returns the number of tasks.
    pub fn tasks_size(&self) -> usize {
        self.tasks.len()
    }

    /// Returns true if this worker has no assignments.
    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty() && self.tasks.is_empty()
    }

    /// Assigns a connector to this worker.
    pub fn assign_connector(&mut self, connector: String) {
        self.connectors.push(connector);
    }

    /// Assigns a task to this worker.
    pub fn assign_task(&mut self, task: ConnectorTaskId) {
        self.tasks.push(task);
    }

    /// Removes all connectors from a set.
    pub fn remove_connectors(&mut self, to_remove: &BTreeSet<String>) {
        self.connectors.retain(|c| !to_remove.contains(c));
    }

    /// Removes all tasks from a set.
    pub fn remove_tasks(&mut self, to_remove: &BTreeSet<ConnectorTaskId>) {
        self.tasks.retain(|t| !to_remove.contains(t));
    }

    /// Comparator for sorting by connector count (ascending).
    pub fn connector_comparator(a: &WorkerLoad, b: &WorkerLoad) -> std::cmp::Ordering {
        a.connectors_size().cmp(&b.connectors_size())
    }

    /// Comparator for sorting by task count (ascending).
    pub fn task_comparator(a: &WorkerLoad, b: &WorkerLoad) -> std::cmp::Ordering {
        a.tasks_size().cmp(&b.tasks_size())
    }
}

/// Builder for creating WorkerLoad instances.
pub struct WorkerLoadBuilder {
    worker_id: String,
    connectors: Vec<String>,
    tasks: Vec<ConnectorTaskId>,
}

impl WorkerLoadBuilder {
    /// Creates a new builder for the given worker.
    pub fn new(worker_id: String) -> Self {
        WorkerLoadBuilder {
            worker_id,
            connectors: Vec::new(),
            tasks: Vec::new(),
        }
    }

    /// Sets the connectors.
    pub fn with_connectors(mut self, connectors: Vec<String>) -> Self {
        self.connectors = connectors;
        self
    }

    /// Sets the tasks.
    pub fn with_tasks(mut self, tasks: Vec<ConnectorTaskId>) -> Self {
        self.tasks = tasks;
        self
    }

    /// Sets both connectors and tasks.
    pub fn with(mut self, connectors: Vec<String>, tasks: Vec<ConnectorTaskId>) -> Self {
        self.connectors = connectors;
        self.tasks = tasks;
        self
    }

    /// Builds the WorkerLoad instance.
    pub fn build(self) -> WorkerLoad {
        WorkerLoad::with_assignments(self.worker_id, self.connectors, self.tasks)
    }
}

/// Represents the cluster assignment state after performing incremental cooperative assignment.
///
/// Contains:
/// - Newly assigned connectors and tasks for each worker
/// - Newly revoked connectors and tasks for each worker
/// - All assigned connectors and tasks across the cluster
///
/// Corresponds to `IncrementalCooperativeAssignor.ClusterAssignment` in Java.
#[derive(Debug, Clone)]
pub struct ClusterAssignment {
    newly_assigned_connectors: HashMap<String, Vec<String>>,
    newly_assigned_tasks: HashMap<String, Vec<ConnectorTaskId>>,
    newly_revoked_connectors: HashMap<String, Vec<String>>,
    newly_revoked_tasks: HashMap<String, Vec<ConnectorTaskId>>,
    all_assigned_connectors: HashMap<String, Vec<String>>,
    all_assigned_tasks: HashMap<String, Vec<ConnectorTaskId>>,
    all_workers: HashSet<String>,
}

impl ClusterAssignment {
    /// Empty cluster assignment instance.
    pub fn empty() -> Self {
        ClusterAssignment {
            newly_assigned_connectors: HashMap::new(),
            newly_assigned_tasks: HashMap::new(),
            newly_revoked_connectors: HashMap::new(),
            newly_revoked_tasks: HashMap::new(),
            all_assigned_connectors: HashMap::new(),
            all_assigned_tasks: HashMap::new(),
            all_workers: HashSet::new(),
        }
    }

    /// Creates a new ClusterAssignment with all fields.
    pub fn new(
        newly_assigned_connectors: HashMap<String, Vec<String>>,
        newly_assigned_tasks: HashMap<String, Vec<ConnectorTaskId>>,
        newly_revoked_connectors: HashMap<String, Vec<String>>,
        newly_revoked_tasks: HashMap<String, Vec<ConnectorTaskId>>,
        all_assigned_connectors: HashMap<String, Vec<String>>,
        all_assigned_tasks: HashMap<String, Vec<ConnectorTaskId>>,
    ) -> Self {
        // Collect all workers from all maps
        let all_workers: HashSet<String> = newly_assigned_connectors
            .keys()
            .chain(newly_assigned_tasks.keys())
            .chain(newly_revoked_connectors.keys())
            .chain(newly_revoked_tasks.keys())
            .chain(all_assigned_connectors.keys())
            .chain(all_assigned_tasks.keys())
            .cloned()
            .collect();

        ClusterAssignment {
            newly_assigned_connectors,
            newly_assigned_tasks,
            newly_revoked_connectors,
            newly_revoked_tasks,
            all_assigned_connectors,
            all_assigned_tasks,
            all_workers,
        }
    }

    /// Returns the newly assigned connectors for all workers.
    pub fn newly_assigned_connectors(&self) -> &HashMap<String, Vec<String>> {
        &self.newly_assigned_connectors
    }

    /// Returns the newly assigned connectors for a specific worker.
    pub fn newly_assigned_connectors_for(&self, worker: &str) -> Vec<String> {
        self.newly_assigned_connectors
            .get(worker)
            .cloned()
            .unwrap_or_default()
    }

    /// Returns the newly assigned tasks for all workers.
    pub fn newly_assigned_tasks(&self) -> &HashMap<String, Vec<ConnectorTaskId>> {
        &self.newly_assigned_tasks
    }

    /// Returns the newly assigned tasks for a specific worker.
    pub fn newly_assigned_tasks_for(&self, worker: &str) -> Vec<ConnectorTaskId> {
        self.newly_assigned_tasks
            .get(worker)
            .cloned()
            .unwrap_or_default()
    }

    /// Returns the newly revoked connectors for all workers.
    pub fn newly_revoked_connectors(&self) -> &HashMap<String, Vec<String>> {
        &self.newly_revoked_connectors
    }

    /// Returns the newly revoked connectors for a specific worker.
    pub fn newly_revoked_connectors_for(&self, worker: &str) -> Vec<String> {
        self.newly_revoked_connectors
            .get(worker)
            .cloned()
            .unwrap_or_default()
    }

    /// Returns the newly revoked tasks for all workers.
    pub fn newly_revoked_tasks(&self) -> &HashMap<String, Vec<ConnectorTaskId>> {
        &self.newly_revoked_tasks
    }

    /// Returns the newly revoked tasks for a specific worker.
    pub fn newly_revoked_tasks_for(&self, worker: &str) -> Vec<ConnectorTaskId> {
        self.newly_revoked_tasks
            .get(worker)
            .cloned()
            .unwrap_or_default()
    }

    /// Returns all assigned connectors across the cluster.
    pub fn all_assigned_connectors(&self) -> &HashMap<String, Vec<String>> {
        &self.all_assigned_connectors
    }

    /// Returns all assigned tasks across the cluster.
    pub fn all_assigned_tasks(&self) -> &HashMap<String, Vec<ConnectorTaskId>> {
        &self.all_assigned_tasks
    }

    /// Returns all workers involved in this assignment.
    pub fn all_workers(&self) -> &HashSet<String> {
        &self.all_workers
    }
}

/// Exponential backoff for consecutive revoking rebalances.
///
/// Corresponds to `org.apache.kafka.common.utils.ExponentialBackoff` in Java.
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    initial_interval: i64,
    multiplier: f64,
    max_interval: i64,
    max_attempts: i32,
}

impl ExponentialBackoff {
    /// Creates a new ExponentialBackoff.
    ///
    /// # Arguments
    /// * `initial_interval` - Initial backoff interval in milliseconds
    /// * `multiplier` - Multiplier for each subsequent backoff
    /// * `max_interval` - Maximum backoff interval in milliseconds
    /// * `max_attempts` - Maximum number of attempts (unused in this implementation)
    pub fn new(
        initial_interval: i64,
        multiplier: f64,
        max_interval: i64,
        max_attempts: i32,
    ) -> Self {
        ExponentialBackoff {
            initial_interval,
            multiplier,
            max_interval,
            max_attempts,
        }
    }

    /// Calculates the backoff time for a given number of attempts.
    ///
    /// Returns the backoff time in milliseconds.
    pub fn backoff(&self, num_attempts: i32) -> i64 {
        if num_attempts <= 0 {
            return 0;
        }

        let mut current_interval = self.initial_interval;
        for _ in 1..num_attempts {
            current_interval = (current_interval as f64 * self.multiplier) as i64;
            if current_interval > self.max_interval {
                current_interval = self.max_interval;
                break;
            }
        }
        current_interval
    }
}

/// Configuration snapshot for the assignment.
///
/// This is a simplified version of ClusterConfigState from Java.
/// In a full implementation, this would be obtained from the coordinator.
#[derive(Debug, Clone)]
pub struct ConfigSnapshot {
    offset: i64,
    connectors: BTreeSet<String>,
    tasks_by_connector: HashMap<String, Vec<ConnectorTaskId>>,
}

impl ConfigSnapshot {
    /// Creates a new ConfigSnapshot.
    pub fn new(
        offset: i64,
        connectors: BTreeSet<String>,
        tasks_by_connector: HashMap<String, Vec<ConnectorTaskId>>,
    ) -> Self {
        ConfigSnapshot {
            offset,
            connectors,
            tasks_by_connector,
        }
    }

    /// Creates an empty ConfigSnapshot.
    pub fn empty() -> Self {
        ConfigSnapshot {
            offset: 0,
            connectors: BTreeSet::new(),
            tasks_by_connector: HashMap::new(),
        }
    }

    /// Returns the config offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the set of connector names.
    pub fn connectors(&self) -> &BTreeSet<String> {
        &self.connectors
    }

    /// Returns the tasks for a specific connector.
    pub fn tasks(&self, connector: &str) -> Vec<ConnectorTaskId> {
        self.tasks_by_connector
            .get(connector)
            .cloned()
            .unwrap_or_default()
    }

    /// Returns all tasks across all connectors.
    pub fn all_tasks(&self) -> BTreeSet<ConnectorTaskId> {
        self.tasks_by_connector
            .values()
            .flat_map(|tasks| tasks.iter().cloned())
            .collect()
    }
}

/// An assignor that computes a distribution of connectors and tasks according to the incremental
/// cooperative strategy for rebalancing.
///
/// Note that this class is NOT thread-safe.
pub struct IncrementalCooperativeAssignor {
    /// Maximum delay for scheduled rebalance in milliseconds
    max_delay: i32,
    /// Previous assignment from the last rebalance
    previous_assignment: ConnectorsAndTasks,
    /// Previous revocation that may not have taken effect
    previous_revocation: ConnectorsAndTasks,
    /// Whether revocation occurred in previous rebalance
    revoked_in_previous: bool,
    /// Candidate workers for reassignment during delayed rebalance (ordered set simulation)
    candidate_workers_for_reassignment: Vec<String>,
    /// Scheduled rebalance time (milliseconds since epoch)
    scheduled_rebalance: i64,
    /// Current delay for delayed rebalance
    delay: i32,
    /// Previous generation ID
    previous_generation_id: i32,
    /// Previous member IDs
    previous_members: HashSet<String>,
    /// Exponential backoff for consecutive revoking rebalances
    consecutive_revoking_rebalances_backoff: ExponentialBackoff,
    /// Number of successive revoking rebalances
    num_successive_revoking_rebalances: i32,
}

impl IncrementalCooperativeAssignor {
    /// Creates a new IncrementalCooperativeAssignor.
    ///
    /// # Arguments
    /// * `max_delay` - Maximum delay for scheduled rebalance in milliseconds
    pub fn new(max_delay: i32) -> Self {
        // By default, initial interval is 1. The only corner case is when the user has set
        // maxDelay to 0 in which case, the exponential backoff delay should be 0 which would
        // return the backoff delay to be 0 always.
        let initial_interval = if max_delay == 0 { 0 } else { 1 };

        IncrementalCooperativeAssignor {
            max_delay,
            previous_assignment: ConnectorsAndTasks::empty(),
            previous_revocation: ConnectorsAndTasksBuilder::new().build(),
            revoked_in_previous: false,
            candidate_workers_for_reassignment: Vec::new(),
            scheduled_rebalance: 0,
            delay: 0,
            previous_generation_id: -1,
            previous_members: HashSet::new(),
            consecutive_revoking_rebalances_backoff: ExponentialBackoff::new(
                initial_interval,
                40.0,
                max_delay as i64,
                0,
            ),
            num_successive_revoking_rebalances: 0,
        }
    }

    /// Performs assignment based on member metadata and coordinator state.
    fn perform_assignment_internal(
        &mut self,
        leader_id: &str,
        protocol: ConnectProtocolCompatibility,
        all_member_metadata: &[JoinGroupResponseMember],
        coordinator: &dyn WorkerCoordinatorState,
        config_snapshot: &ConfigSnapshot,
        last_completed_generation_id: i32,
        current_generation_id: i32,
    ) -> HashMap<String, Vec<u8>> {
        // Parse member metadata to get ExtendedWorkerState for each member
        let member_configs: HashMap<String, ExtendedWorkerState> = all_member_metadata
            .iter()
            .map(|member| {
                let state =
                    IncrementalCooperativeConnectProtocol::deserialize_metadata(&member.metadata);
                (member.member_id.clone(), state)
            })
            .collect();

        // Find the maximum config offset among all members
        let max_offset = self.find_max_member_config_offset(&member_configs, coordinator);

        // Ensure leader has the latest config
        let leader_offset_result = self.ensure_leader_config(max_offset, coordinator);

        // If leader doesn't have latest config, return CONFIG_MISMATCH assignment
        if leader_offset_result.is_none() {
            let leader_state = member_configs.get(leader_id);
            let leader_url = leader_state.map(|s| s.url()).unwrap_or("");
            return self.fill_assignments_and_serialize(
                &member_configs.keys().cloned().collect::<Vec<_>>(),
                ASSIGNMENT_CONFIG_MISMATCH,
                leader_id,
                leader_url,
                max_offset,
                ClusterAssignment::empty(),
                0,
                protocol.protocol_version(),
            );
        }

        // Build member assignments from member configs
        let member_assignments: HashMap<String, ConnectorsAndTasks> = member_configs
            .iter()
            .map(|(member_id, state)| {
                let assignment = state.assignment();
                let connectors: BTreeSet<String> =
                    assignment.connectors().iter().cloned().collect();
                let tasks: BTreeSet<ConnectorTaskId> = assignment.tasks().iter().cloned().collect();
                (
                    member_id.clone(),
                    ConnectorsAndTasks::new(connectors, tasks),
                )
            })
            .collect();

        // Perform task assignment
        let cluster_assignment = self.perform_task_assignment(
            config_snapshot,
            last_completed_generation_id,
            current_generation_id,
            &member_assignments,
        );

        // Fill assignments and serialize
        let leader_url = member_configs.get(leader_id).map(|s| s.url()).unwrap_or("");
        self.fill_assignments_and_serialize(
            &member_configs.keys().cloned().collect::<Vec<_>>(),
            ASSIGNMENT_NO_ERROR,
            leader_id,
            leader_url,
            max_offset,
            cluster_assignment,
            self.delay,
            protocol.protocol_version(),
        )
    }

    /// Finds the maximum config offset among all member configs.
    fn find_max_member_config_offset(
        &self,
        member_configs: &HashMap<String, ExtendedWorkerState>,
        _coordinator: &dyn WorkerCoordinatorState,
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

    /// Ensures the leader has the latest configuration.
    fn ensure_leader_config(
        &self,
        max_offset: i64,
        coordinator: &dyn WorkerCoordinatorState,
    ) -> Option<i64> {
        let current_offset = coordinator.config_offset();

        if current_offset < max_offset {
            // In this simplified implementation, we return None indicating mismatch
            // A real implementation would try to get a fresh snapshot
            None
        } else {
            Some(max_offset)
        }
    }

    /// Performs task assignment based on the incremental cooperative connect protocol.
    fn perform_task_assignment(
        &mut self,
        config_snapshot: &ConfigSnapshot,
        last_completed_generation_id: i32,
        current_generation_id: i32,
        member_assignments: &HashMap<String, ConnectorsAndTasks>,
    ) -> ClusterAssignment {
        // Base set: The previous assignment of connectors-and-tasks
        // Check generation ID mismatch
        if self.previous_generation_id != last_completed_generation_id {
            // Clear the view of previous assignments due to generation mismatch
            self.previous_assignment = ConnectorsAndTasks::empty();
        }

        // Get configured connectors and tasks
        let configured_connectors = config_snapshot.connectors().clone();
        let configured_tasks = config_snapshot.all_tasks();

        // Base set: The set of configured connectors-and-tasks
        let configured = ConnectorsAndTasksBuilder::new()
            .with_connectors(configured_connectors.iter().cloned())
            .with_tasks(configured_tasks.iter().cloned())
            .build();

        // Base set: The set of active connectors-and-tasks
        let active_assignments = self.assignment(member_assignments);

        // Check if previous revocation did not take effect
        if !self.previous_revocation.is_empty() {
            let prev_revocation_has_active = self
                .previous_revocation
                .connectors()
                .iter()
                .any(|c| active_assignments.contains_connector(c))
                || self
                    .previous_revocation
                    .tasks()
                    .iter()
                    .any(|t| active_assignments.contains_task(t));

            if prev_revocation_has_active {
                self.previous_assignment = active_assignments.clone();
            }
            self.previous_revocation.clear();
        }

        // Derived set: The set of deleted connectors-and-tasks (previous - configured)
        let deleted = self.diff_connectors_and_tasks(&self.previous_assignment, &[&configured]);

        // The connectors and tasks that are currently running on more than one worker each
        let duplicated = self.duplicated_assignments(member_assignments);

        // Derived set: The set of lost or unaccounted connectors-and-tasks
        // (previous - active - deleted)
        let lost_assignments = self
            .diff_connectors_and_tasks(&self.previous_assignment, &[&active_assignments, &deleted]);

        // Derived set: The set of new connectors-and-tasks (configured - previous - active)
        let created = self.diff_connectors_and_tasks(
            &configured,
            &[&self.previous_assignment, &active_assignments],
        );

        // A collection of the current assignment excluding deleted
        let current_worker_assignment = self.worker_assignment(member_assignments, &deleted);

        // Build toRevoke map
        let mut to_revoke: HashMap<String, ConnectorsAndTasksBuilder> = HashMap::new();

        // Calculate deleted to revoke from each worker
        let deleted_to_revoke = self.intersection(&deleted, member_assignments);
        self.add_all_to_revoke(&mut to_revoke, &deleted_to_revoke);

        // Calculate duplicated to revoke from each worker
        let duplicated_to_revoke = self.intersection(&duplicated, member_assignments);
        self.add_all_to_revoke(&mut to_revoke, &duplicated_to_revoke);

        // Compute the assignment that will be applied across the cluster
        let mut next_worker_assignment = self.worker_loads(member_assignments);
        self.remove_all_from_worker_loads(&mut next_worker_assignment, &deleted_to_revoke);
        self.remove_all_from_worker_loads(&mut next_worker_assignment, &duplicated_to_revoke);

        // Handle lost assignments
        let mut lost_assignments_to_reassign_builder = ConnectorsAndTasksBuilder::new();
        self.handle_lost_assignments(
            &lost_assignments,
            &mut lost_assignments_to_reassign_builder,
            &mut next_worker_assignment,
        );
        let lost_assignments_to_reassign = lost_assignments_to_reassign_builder.build();

        // Do not revoke resources for re-assignment while a delayed rebalance is active
        if self.delay == 0 {
            let load_balancing_revocations =
                self.perform_load_balancing_revocations(&configured, &next_worker_assignment);

            // If this round and the previous round involved revocation
            if self.revoked_in_previous && !load_balancing_revocations.is_empty() {
                self.num_successive_revoking_rebalances += 1;
                self.delay = self
                    .consecutive_revoking_rebalances_backoff
                    .backoff(self.num_successive_revoking_rebalances)
                    as i32;

                if self.delay != 0 {
                    self.scheduled_rebalance = self.current_time_ms() + self.delay as i64;
                } else {
                    self.add_all_to_revoke(&mut to_revoke, &load_balancing_revocations);
                    self.remove_all_from_worker_loads(
                        &mut next_worker_assignment,
                        &load_balancing_revocations,
                    );
                }
            } else if !load_balancing_revocations.is_empty() {
                self.add_all_to_revoke(&mut to_revoke, &load_balancing_revocations);
                self.remove_all_from_worker_loads(
                    &mut next_worker_assignment,
                    &load_balancing_revocations,
                );
                self.revoked_in_previous = true;
            } else if self.revoked_in_previous {
                self.revoked_in_previous = false;
                self.num_successive_revoking_rebalances = 0;
            }
        } else {
            self.revoked_in_previous = false;
        }

        // The complete set of connectors and tasks that should be newly-assigned
        let to_assign = ConnectorsAndTasksBuilder::new()
            .add_all(&created)
            .add_all(&lost_assignments_to_reassign)
            .build();

        // Assign connectors and tasks
        self.assign_connectors(
            &mut next_worker_assignment,
            to_assign.connectors.iter().cloned().collect(),
        );
        self.assign_tasks(
            &mut next_worker_assignment,
            to_assign.tasks.iter().cloned().collect(),
        );

        // Build next connector and task assignments maps
        let next_connector_assignments: HashMap<String, Vec<String>> = next_worker_assignment
            .iter()
            .map(|wl| {
                (
                    wl.worker().to_string(),
                    wl.connectors().iter().cloned().collect(),
                )
            })
            .collect();

        let next_task_assignments: HashMap<String, Vec<ConnectorTaskId>> = next_worker_assignment
            .iter()
            .map(|wl| {
                (
                    wl.worker().to_string(),
                    wl.tasks().iter().cloned().collect(),
                )
            })
            .collect();

        // Build current connector and task assignments maps
        let current_connector_assignments: HashMap<String, Vec<String>> = current_worker_assignment
            .iter()
            .map(|wl| {
                (
                    wl.worker().to_string(),
                    wl.connectors().iter().cloned().collect(),
                )
            })
            .collect();

        let current_task_assignments: HashMap<String, Vec<ConnectorTaskId>> =
            current_worker_assignment
                .iter()
                .map(|wl| {
                    (
                        wl.worker().to_string(),
                        wl.tasks().iter().cloned().collect(),
                    )
                })
                .collect();

        // Compute incremental assignments
        let incremental_connector_assignments =
            self.diff_assignment_maps(&next_connector_assignments, &current_connector_assignments);
        let incremental_task_assignments =
            self.diff_task_assignment_maps(&next_task_assignments, &current_task_assignments);

        // Build revoked map
        let revoked = self.build_all_revoke(&to_revoke);

        // Compute previous assignment for next round
        self.previous_assignment = self.compute_previous_assignment(
            &revoked,
            &next_connector_assignments,
            &next_task_assignments,
            &lost_assignments,
        );
        self.previous_generation_id = current_generation_id;
        self.previous_members = member_assignments.keys().cloned().collect();

        // Build revoked connector and task maps
        let revoked_connectors: HashMap<String, Vec<String>> = revoked
            .iter()
            .map(|(k, v)| (k.clone(), v.connectors.iter().cloned().collect()))
            .collect();

        let revoked_tasks: HashMap<String, Vec<ConnectorTaskId>> = revoked
            .iter()
            .map(|(k, v)| (k.clone(), v.tasks.iter().cloned().collect()))
            .collect();

        // Build all assigned connector and task maps (next - revoked)
        let all_assigned_connectors =
            self.diff_assignment_maps(&next_connector_assignments, &revoked_connectors);
        let all_assigned_tasks =
            self.diff_task_assignment_maps(&next_task_assignments, &revoked_tasks);

        ClusterAssignment::new(
            incremental_connector_assignments,
            incremental_task_assignments,
            revoked_connectors,
            revoked_tasks,
            all_assigned_connectors,
            all_assigned_tasks,
        )
    }

    /// Handles lost assignments during delayed rebalance.
    fn handle_lost_assignments(
        &mut self,
        lost_assignments: &ConnectorsAndTasks,
        lost_assignments_to_reassign: &mut ConnectorsAndTasksBuilder,
        complete_worker_assignment: &mut Vec<WorkerLoad>,
    ) {
        // There are no lost assignments and there have been no successive revoking rebalances
        if lost_assignments.is_empty() && !self.revoked_in_previous {
            self.reset_delay();
            return;
        }

        let now = self.current_time_ms();

        let active_members: HashSet<String> = complete_worker_assignment
            .iter()
            .map(|wl| wl.worker().to_string())
            .collect();

        if self.scheduled_rebalance <= 0 && active_members.is_superset(&self.previous_members) {
            // No worker seems to have departed the group during the rebalance
            lost_assignments_to_reassign.add_all_mut(lost_assignments);
            return;
        } else if self.max_delay == 0 {
            // Scheduled rebalance delays are disabled
            lost_assignments_to_reassign.add_all_mut(lost_assignments);
            return;
        }

        if self.scheduled_rebalance > 0 && now >= self.scheduled_rebalance {
            // Delayed rebalance expired and it's time to assign resources
            let candidate_worker_load: Vec<WorkerLoad> =
                if !self.candidate_workers_for_reassignment.is_empty() {
                    self.pick_candidate_worker_for_reassignment(complete_worker_assignment)
                } else {
                    Vec::new()
                };

            if !candidate_worker_load.is_empty() {
                // Assign lost connectors and tasks to candidate workers
                let mut candidate_iterator = candidate_worker_load.iter().cycle();

                for connector in lost_assignments.connectors.iter() {
                    if let Some(worker) = candidate_iterator.next() {
                        let worker_mut = complete_worker_assignment
                            .iter_mut()
                            .find(|w| w.worker() == worker.worker());
                        if let Some(w) = worker_mut {
                            w.assign_connector(connector.clone());
                        }
                    }
                }

                candidate_iterator = candidate_worker_load.iter().cycle();
                for task in lost_assignments.tasks.iter() {
                    if let Some(worker) = candidate_iterator.next() {
                        let worker_mut = complete_worker_assignment
                            .iter_mut()
                            .find(|w| w.worker() == worker.worker());
                        if let Some(w) = worker_mut {
                            w.assign_task(task.clone());
                        }
                    }
                }
            } else {
                lost_assignments_to_reassign.add_all_mut(lost_assignments);
            }

            self.reset_delay();
            self.revoked_in_previous = false;
        } else {
            // Add candidate workers for reassignment (maintain insertion order and uniqueness)
            let candidates =
                self.candidate_workers_for_reassignment_internal(complete_worker_assignment);
            for c in candidates {
                if !self.candidate_workers_for_reassignment.contains(&c) {
                    self.candidate_workers_for_reassignment.push(c);
                }
            }

            if now < self.scheduled_rebalance {
                self.delay = self.calculate_delay(now);
            } else {
                // scheduledRebalance == 0
                self.delay = self.max_delay;
            }
            self.scheduled_rebalance = now + self.delay as i64;
        }
    }

    /// Resets the delayed rebalance state.
    fn reset_delay(&mut self) {
        self.candidate_workers_for_reassignment.clear();
        self.scheduled_rebalance = 0;
        self.delay = 0;
    }

    /// Gets candidate workers for reassignment (empty workers).
    fn candidate_workers_for_reassignment_internal(
        &self,
        complete_worker_assignment: &[WorkerLoad],
    ) -> Vec<String> {
        complete_worker_assignment
            .iter()
            .filter(|wl| wl.is_empty())
            .map(|wl| wl.worker().to_string())
            .collect()
    }

    /// Picks candidate workers for reassignment from the tracked set.
    fn pick_candidate_worker_for_reassignment(
        &self,
        complete_worker_assignment: &[WorkerLoad],
    ) -> Vec<WorkerLoad> {
        let active_workers: HashMap<&str, &WorkerLoad> = complete_worker_assignment
            .iter()
            .map(|wl| (wl.worker(), wl))
            .collect();

        self.candidate_workers_for_reassignment
            .iter()
            .filter_map(|worker_id| active_workers.get(worker_id.as_str()).cloned())
            .cloned()
            .collect()
    }

    /// Calculates delay based on current time.
    fn calculate_delay(&self, now: i64) -> i32 {
        let diff = self.scheduled_rebalance - now;
        if diff > 0 {
            (diff as i32).min(self.max_delay)
        } else {
            0
        }
    }

    /// Gets current time in milliseconds.
    fn current_time_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    /// Performs load balancing revocations.
    fn perform_load_balancing_revocations(
        &self,
        configured: &ConnectorsAndTasks,
        workers: &[WorkerLoad],
    ) -> HashMap<String, ConnectorsAndTasks> {
        if workers.iter().all(|wl| wl.is_empty()) {
            return HashMap::new();
        }

        if configured.is_empty() {
            return HashMap::new();
        }

        let mut result: HashMap<String, ConnectorsAndTasksBuilder> = HashMap::new();

        let connector_revocations = self.load_balancing_revocations_internal(
            "connector",
            configured.connectors.len(),
            workers,
            |wl| wl.connectors(),
        );

        let task_revocations = self.load_balancing_revocations_internal(
            "task",
            configured.tasks.len(),
            workers,
            |wl| wl.tasks(),
        );

        for (worker, revoked) in connector_revocations {
            result
                .entry(worker)
                .or_insert_with(ConnectorsAndTasksBuilder::new)
                .add_connectors_mut(revoked);
        }

        for (worker, revoked) in task_revocations {
            result
                .entry(worker)
                .or_insert_with(ConnectorsAndTasksBuilder::new)
                .add_tasks_mut(revoked);
        }

        self.build_all_revoke(&result)
    }

    /// Internal method for load balancing revocations.
    fn load_balancing_revocations_internal<E: Clone + Eq + std::hash::Hash + Ord>(
        &self,
        _allocated_resource_name: &str,
        total_to_allocate: usize,
        workers: &[WorkerLoad],
        worker_allocation: fn(&WorkerLoad) -> &[E],
    ) -> HashMap<String, BTreeSet<E>> {
        let total_workers = workers.len();
        if total_workers == 0 {
            return HashMap::new();
        }

        let min_allocated_per_worker = total_to_allocate / total_workers;
        let workers_to_allocate_extra = total_to_allocate % total_workers;

        let workers_allocated_minimum = workers
            .iter()
            .filter(|wl| worker_allocation(wl).len() == min_allocated_per_worker)
            .count();

        let workers_allocated_single_extra = workers
            .iter()
            .filter(|wl| worker_allocation(wl).len() == min_allocated_per_worker + 1)
            .count();

        if workers_allocated_single_extra == workers_to_allocate_extra
            && workers_allocated_minimum + workers_allocated_single_extra == total_workers
        {
            // No load-balancing revocations required
            return HashMap::new();
        }

        let mut result: HashMap<String, BTreeSet<E>> = HashMap::new();
        let mut allocated_extras = 0;

        for worker in workers.iter() {
            let current_allocation_size_for_worker = worker_allocation(worker).len();

            if current_allocation_size_for_worker <= min_allocated_per_worker {
                continue;
            }

            let max_allocation_for_worker = if allocated_extras < workers_to_allocate_extra {
                allocated_extras += 1;
                if current_allocation_size_for_worker == min_allocated_per_worker + 1 {
                    continue;
                }
                min_allocated_per_worker + 1
            } else {
                min_allocated_per_worker
            };

            let mut revoked_from_worker: BTreeSet<E> = BTreeSet::new();
            result.insert(worker.worker().to_string(), revoked_from_worker.clone());

            let current_worker_allocation = worker_allocation(worker);
            let mut num_revoked = 0;

            for item in current_worker_allocation.iter() {
                if current_allocation_size_for_worker - num_revoked <= max_allocation_for_worker {
                    break;
                }
                result
                    .get_mut(worker.worker())
                    .unwrap()
                    .insert(item.clone());
                num_revoked += 1;
            }
        }

        result
    }

    /// Assigns connectors using round-robin with load balancing.
    fn assign_connectors(&self, worker_assignment: &mut Vec<WorkerLoad>, connectors: Vec<String>) {
        if worker_assignment.is_empty() || connectors.is_empty() {
            return;
        }

        worker_assignment.sort_by(WorkerLoad::connector_comparator);

        let mut connector_iter = connectors.into_iter();

        while let Some(first) = worker_assignment.first() {
            let first_load = first.connectors_size();

            // Find workers with same load as first
            let up_to = worker_assignment
                .iter()
                .position(|wl| wl.connectors_size() > first_load)
                .unwrap_or(worker_assignment.len());

            for worker in worker_assignment.iter_mut().take(up_to) {
                if let Some(connector) = connector_iter.next() {
                    worker.assign_connector(connector);
                } else {
                    return;
                }
            }
        }
    }

    /// Assigns tasks using round-robin with load balancing.
    fn assign_tasks(&self, worker_assignment: &mut Vec<WorkerLoad>, tasks: Vec<ConnectorTaskId>) {
        if worker_assignment.is_empty() || tasks.is_empty() {
            return;
        }

        worker_assignment.sort_by(WorkerLoad::task_comparator);

        let mut task_iter = tasks.into_iter();

        while let Some(first) = worker_assignment.first() {
            let first_load = first.tasks_size();

            let up_to = worker_assignment
                .iter()
                .position(|wl| wl.tasks_size() > first_load)
                .unwrap_or(worker_assignment.len());

            for worker in worker_assignment.iter_mut().take(up_to) {
                if let Some(task) = task_iter.next() {
                    worker.assign_task(task);
                } else {
                    return;
                }
            }
        }
    }

    /// Computes difference between two ConnectorsAndTasks.
    fn diff_connectors_and_tasks(
        &self,
        base: &ConnectorsAndTasks,
        to_subtract: &[&ConnectorsAndTasks],
    ) -> ConnectorsAndTasks {
        let mut connectors: BTreeSet<String> = base.connectors.clone();
        let mut tasks: BTreeSet<ConnectorTaskId> = base.tasks.clone();

        for sub in to_subtract {
            for connector in sub.connectors.iter() {
                connectors.remove(connector);
            }
            for task in sub.tasks.iter() {
                tasks.remove(task);
            }
        }

        ConnectorsAndTasksBuilder::new()
            .with_connectors(connectors.iter().cloned())
            .with_tasks(tasks.iter().cloned())
            .build()
    }

    /// Computes difference between two assignment maps.
    fn diff_assignment_maps(
        &self,
        base: &HashMap<String, Vec<String>>,
        to_subtract: &HashMap<String, Vec<String>>,
    ) -> HashMap<String, Vec<String>> {
        let mut incremental: HashMap<String, Vec<String>> = HashMap::new();

        for (worker, values) in base.iter() {
            let mut new_values: Vec<String> = values.clone();
            if let Some(subtract_values) = to_subtract.get(worker) {
                for v in subtract_values {
                    if let Some(pos) = new_values.iter().position(|x| x == v) {
                        new_values.remove(pos);
                    }
                }
            }
            incremental.insert(worker.clone(), new_values);
        }

        incremental
    }

    /// Computes difference between two task assignment maps.
    fn diff_task_assignment_maps(
        &self,
        base: &HashMap<String, Vec<ConnectorTaskId>>,
        to_subtract: &HashMap<String, Vec<ConnectorTaskId>>,
    ) -> HashMap<String, Vec<ConnectorTaskId>> {
        let mut incremental: HashMap<String, Vec<ConnectorTaskId>> = HashMap::new();

        for (worker, values) in base.iter() {
            let mut new_values: Vec<ConnectorTaskId> = values.clone();
            if let Some(subtract_values) = to_subtract.get(worker) {
                for v in subtract_values {
                    if let Some(pos) = new_values.iter().position(|x| x == v) {
                        new_values.remove(pos);
                    }
                }
            }
            incremental.insert(worker.clone(), new_values);
        }

        incremental
    }

    /// Combines all member assignments into one ConnectorsAndTasks.
    fn assignment(
        &self,
        member_assignments: &HashMap<String, ConnectorsAndTasks>,
    ) -> ConnectorsAndTasks {
        let mut builder = ConnectorsAndTasksBuilder::new();

        for cat in member_assignments.values() {
            builder = builder.add_all(cat);
        }

        builder.build()
    }

    /// Computes intersection between ConnectorsAndTasks and member assignments.
    fn intersection(
        &self,
        connectors_and_tasks: &ConnectorsAndTasks,
        assignments: &HashMap<String, ConnectorsAndTasks>,
    ) -> HashMap<String, ConnectorsAndTasks> {
        assignments
            .iter()
            .map(|(worker, assignment)| {
                let mut connectors: BTreeSet<String> = BTreeSet::new();
                for c in assignment.connectors.iter() {
                    if connectors_and_tasks.connectors.contains(c) {
                        connectors.insert(c.clone());
                    }
                }

                let mut tasks: BTreeSet<ConnectorTaskId> = BTreeSet::new();
                for t in assignment.tasks.iter() {
                    if connectors_and_tasks.tasks.contains(t) {
                        tasks.insert(t.clone());
                    }
                }

                (
                    worker.clone(),
                    ConnectorsAndTasksBuilder::new()
                        .with_connectors(connectors.iter().cloned())
                        .with_tasks(tasks.iter().cloned())
                        .build(),
                )
            })
            .collect()
    }

    /// Gets worker assignment excluding specified connectors/tasks.
    fn worker_assignment(
        &self,
        member_assignments: &HashMap<String, ConnectorsAndTasks>,
        to_exclude: &ConnectorsAndTasks,
    ) -> Vec<WorkerLoad> {
        member_assignments
            .iter()
            .map(|(worker_id, cat)| {
                let connectors: Vec<String> = cat
                    .connectors
                    .iter()
                    .filter(|c| !to_exclude.connectors.contains(*c))
                    .cloned()
                    .collect();

                let tasks: Vec<ConnectorTaskId> = cat
                    .tasks
                    .iter()
                    .filter(|t| !to_exclude.tasks.contains(*t))
                    .cloned()
                    .collect();

                WorkerLoadBuilder::new(worker_id.clone())
                    .with(connectors, tasks)
                    .build()
            })
            .collect()
    }

    /// Gets worker loads from member assignments.
    fn worker_loads(
        &self,
        member_assignments: &HashMap<String, ConnectorsAndTasks>,
    ) -> Vec<WorkerLoad> {
        member_assignments
            .iter()
            .map(|(worker_id, cat)| {
                WorkerLoadBuilder::new(worker_id.clone())
                    .with(
                        cat.connectors.iter().cloned().collect(),
                        cat.tasks.iter().cloned().collect(),
                    )
                    .build()
            })
            .collect()
    }

    /// Adds all from one map to the revoke map.
    fn add_all_to_revoke(
        &self,
        base: &mut HashMap<String, ConnectorsAndTasksBuilder>,
        to_add: &HashMap<String, ConnectorsAndTasks>,
    ) {
        for (worker, assignment) in to_add.iter() {
            base.entry(worker.clone())
                .or_insert_with(ConnectorsAndTasksBuilder::new)
                .add_all_mut(assignment);
        }
    }

    /// Builds all revoke map from builders.
    fn build_all_revoke<K: std::hash::Hash + Eq + Clone>(
        &self,
        builders: &HashMap<K, ConnectorsAndTasksBuilder>,
    ) -> HashMap<K, ConnectorsAndTasks> {
        builders
            .iter()
            .map(|(k, builder)| {
                let mut new_builder = ConnectorsAndTasksBuilder::new();
                new_builder
                    .connectors
                    .extend(builder.connectors.iter().cloned());
                new_builder.tasks.extend(builder.tasks.iter().cloned());
                (k.clone(), new_builder.build())
            })
            .collect()
    }

    /// Removes all from worker loads based on a map.
    fn remove_all_from_worker_loads(
        &self,
        worker_loads: &mut Vec<WorkerLoad>,
        to_remove: &HashMap<String, ConnectorsAndTasks>,
    ) {
        for worker_load in worker_loads.iter_mut() {
            let worker = worker_load.worker();
            if let Some(to_remove_from_worker) = to_remove.get(worker) {
                worker_load.remove_connectors(&to_remove_from_worker.connectors);
                worker_load.remove_tasks(&to_remove_from_worker.tasks);
            }
        }
    }

    /// Finds duplicated assignments across workers.
    fn duplicated_assignments(
        &self,
        member_assignments: &HashMap<String, ConnectorsAndTasks>,
    ) -> ConnectorsAndTasks {
        // Count connector instances
        let connector_counts: HashMap<String, usize> = member_assignments
            .values()
            .flat_map(|cat| cat.connectors.iter())
            .fold(HashMap::new(), |mut acc, c| {
                *acc.entry(c.clone()).or_insert(0) += 1;
                acc
            });

        let duplicated_connectors: BTreeSet<String> = connector_counts
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(c, _)| c)
            .collect();

        // Count task instances
        let task_counts: HashMap<ConnectorTaskId, usize> = member_assignments
            .values()
            .flat_map(|cat| cat.tasks.iter())
            .fold(HashMap::new(), |mut acc, t| {
                *acc.entry(t.clone()).or_insert(0) += 1;
                acc
            });

        let duplicated_tasks: BTreeSet<ConnectorTaskId> = task_counts
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(t, _)| t)
            .collect();

        ConnectorsAndTasksBuilder::new()
            .with_connectors(duplicated_connectors.iter().cloned())
            .with_tasks(duplicated_tasks.iter().cloned())
            .build()
    }

    /// Computes previous assignment for next round.
    fn compute_previous_assignment(
        &mut self,
        to_revoke: &HashMap<String, ConnectorsAndTasks>,
        connector_assignments: &HashMap<String, Vec<String>>,
        task_assignments: &HashMap<String, Vec<ConnectorTaskId>>,
        lost_assignments: &ConnectorsAndTasks,
    ) -> ConnectorsAndTasks {
        // Combine all connector and task assignments
        let all_connectors: BTreeSet<String> = connector_assignments
            .values()
            .flat_map(|v| v.iter().cloned())
            .collect();

        let all_tasks: BTreeSet<ConnectorTaskId> = task_assignments
            .values()
            .flat_map(|v| v.iter().cloned())
            .collect();

        let mut previous_assignment = ConnectorsAndTasksBuilder::new()
            .with_connectors(all_connectors.iter().cloned())
            .with_tasks(all_tasks.iter().cloned())
            .build();

        // Remove revoked and add to previous_revocation
        for revoked in to_revoke.values() {
            previous_assignment.remove_all(revoked);
            self.previous_revocation.add_all(revoked);
        }

        // Add lost assignments
        previous_assignment.add_all(lost_assignments);

        previous_assignment
    }

    /// Fills assignments for all members and serializes them.
    fn fill_assignments_and_serialize(
        &self,
        members: &[String],
        error: i16,
        leader_id: &str,
        leader_url: &str,
        max_offset: i64,
        cluster_assignment: ClusterAssignment,
        delay: i32,
        protocol_version: i16,
    ) -> HashMap<String, Vec<u8>> {
        let mut group_assignment: HashMap<String, Vec<u8>> = HashMap::new();

        for member in members {
            let connectors_to_start = cluster_assignment.newly_assigned_connectors_for(member);
            let tasks_to_start = cluster_assignment.newly_assigned_tasks_for(member);
            let connectors_to_stop = cluster_assignment.newly_revoked_connectors_for(member);
            let tasks_to_stop = cluster_assignment.newly_revoked_tasks_for(member);

            let assignment = ExtendedAssignment::new(
                protocol_version,
                error,
                Some(leader_id.to_string()),
                Some(leader_url.to_string()),
                max_offset,
                connectors_to_start,
                tasks_to_start,
                connectors_to_stop,
                tasks_to_stop,
                delay,
            );

            let sessioned = protocol_version >= CONNECT_PROTOCOL_V2;
            let serialized =
                IncrementalCooperativeConnectProtocol::serialize_assignment(&assignment, sessioned);
            group_assignment.insert(member.clone(), serialized);
        }

        group_assignment
    }
}

impl ConnectAssignor for IncrementalCooperativeAssignor {
    /// Based on the member metadata and the information stored in the worker coordinator,
    /// this method computes an assignment of connectors and tasks among the members
    /// of the worker group using the incremental cooperative strategy.
    fn perform_assignment(
        &self,
        leader_id: &str,
        protocol: ConnectProtocolCompatibility,
        all_member_metadata: &[JoinGroupResponseMember],
        coordinator: &dyn WorkerCoordinatorState,
    ) -> HashMap<String, Vec<u8>> {
        // In a real implementation, we would get config_snapshot from coordinator
        // For now, we use an empty snapshot
        let config_snapshot = ConfigSnapshot::empty();
        let last_completed_generation_id = -1;
        let current_generation_id = 0;

        // Note: This is a simplified implementation that doesn't mutate state
        // A full implementation would need mutable self
        let mut assignor = self.clone();
        assignor.perform_assignment_internal(
            leader_id,
            protocol,
            all_member_metadata,
            coordinator,
            &config_snapshot,
            last_completed_generation_id,
            current_generation_id,
        )
    }

    /// Returns the protocol version string for this assignor.
    fn version(&self) -> String {
        ConnectProtocolCompatibility::Compatible
            .protocol()
            .to_string()
    }

    /// Returns the protocol type this assignor supports.
    fn protocol(&self) -> ConnectProtocolCompatibility {
        ConnectProtocolCompatibility::Compatible
    }
}

impl Clone for IncrementalCooperativeAssignor {
    fn clone(&self) -> Self {
        IncrementalCooperativeAssignor {
            max_delay: self.max_delay,
            previous_assignment: self.previous_assignment.clone(),
            previous_revocation: self.previous_revocation.clone(),
            revoked_in_previous: self.revoked_in_previous,
            candidate_workers_for_reassignment: self.candidate_workers_for_reassignment.clone(),
            scheduled_rebalance: self.scheduled_rebalance,
            delay: self.delay,
            previous_generation_id: self.previous_generation_id,
            previous_members: self.previous_members.clone(),
            consecutive_revoking_rebalances_backoff: self
                .consecutive_revoking_rebalances_backoff
                .clone(),
            num_successive_revoking_rebalances: self.num_successive_revoking_rebalances,
        }
    }
}

/// Extended version of perform_assignment that accepts config data directly.
pub struct IncrementalCooperativeAssignorContext {
    assignor: IncrementalCooperativeAssignor,
}

impl IncrementalCooperativeAssignorContext {
    /// Creates a new context with the given max delay.
    pub fn new(max_delay: i32) -> Self {
        IncrementalCooperativeAssignorContext {
            assignor: IncrementalCooperativeAssignor::new(max_delay),
        }
    }

    /// Performs assignment with explicit connector and task data.
    ///
    /// # Arguments
    /// * `leader_id` - The leader of the group
    /// * `all_member_metadata` - The metadata of all the active workers
    /// * `last_completed_generation_id` - The last completed generation ID
    /// * `current_generation_id` - The current generation ID
    /// * `config_snapshot` - The configuration snapshot with connectors and tasks
    /// * `coordinator_offset` - The coordinator's current config offset
    /// * `coordinator_url` - The coordinator's URL
    ///
    /// # Returns
    /// A map from member ID to serialized assignment bytes
    pub fn perform_assignment_with_data(
        &mut self,
        leader_id: &str,
        all_member_metadata: &[JoinGroupResponseMember],
        last_completed_generation_id: i32,
        current_generation_id: i32,
        config_snapshot: &ConfigSnapshot,
        coordinator_offset: i64,
        coordinator_url: &str,
    ) -> HashMap<String, Vec<u8>> {
        // Parse member metadata
        let member_configs: HashMap<String, ExtendedWorkerState> = all_member_metadata
            .iter()
            .map(|member| {
                let state =
                    IncrementalCooperativeConnectProtocol::deserialize_metadata(&member.metadata);
                (member.member_id.clone(), state)
            })
            .collect();

        // Find max offset
        let max_offset = self.find_max_member_config_offset_simple(&member_configs);

        // Check if coordinator has latest config
        if coordinator_offset < max_offset {
            // Return CONFIG_MISMATCH
            return self.assignor.fill_assignments_and_serialize(
                &member_configs.keys().cloned().collect::<Vec<_>>(),
                ASSIGNMENT_CONFIG_MISMATCH,
                leader_id,
                coordinator_url,
                max_offset,
                ClusterAssignment::empty(),
                0,
                CONNECT_PROTOCOL_V1,
            );
        }

        // Build member assignments from member configs
        let member_assignments: HashMap<String, ConnectorsAndTasks> = member_configs
            .iter()
            .map(|(member_id, state)| {
                let assignment = state.assignment();
                let connectors: BTreeSet<String> =
                    assignment.connectors().iter().cloned().collect();
                let tasks: BTreeSet<ConnectorTaskId> = assignment.tasks().iter().cloned().collect();
                (
                    member_id.clone(),
                    ConnectorsAndTasks::new(connectors, tasks),
                )
            })
            .collect();

        // Perform task assignment
        let cluster_assignment = self.assignor.perform_task_assignment(
            config_snapshot,
            last_completed_generation_id,
            current_generation_id,
            &member_assignments,
        );

        // Fill assignments and serialize
        self.assignor.fill_assignments_and_serialize(
            &member_configs.keys().cloned().collect::<Vec<_>>(),
            ASSIGNMENT_NO_ERROR,
            leader_id,
            coordinator_url,
            max_offset,
            cluster_assignment,
            self.assignor.delay,
            CONNECT_PROTOCOL_V1,
        )
    }

    /// Simplified version of find_max_member_config_offset for use without coordinator.
    fn find_max_member_config_offset_simple(
        &self,
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

    #[test]
    fn test_connectors_and_tasks_empty() {
        let cat = ConnectorsAndTasks::empty();
        assert!(cat.is_empty());
        assert_eq!(cat.size(), 0);
    }

    #[test]
    fn test_connectors_and_tasks_builder() {
        let cat = ConnectorsAndTasksBuilder::new()
            .add_connector("conn1".to_string())
            .add_task(ConnectorTaskId::new("conn1".to_string(), 0))
            .build();

        assert!(!cat.is_empty());
        assert_eq!(cat.connectors.len(), 1);
        assert_eq!(cat.tasks.len(), 1);
        assert!(cat.contains_connector("conn1"));
    }

    #[test]
    fn test_connectors_and_tasks_add_remove() {
        let mut cat = ConnectorsAndTasks::empty();
        cat.add_connector("conn1".to_string());
        cat.add_task(ConnectorTaskId::new("conn1".to_string(), 0));

        assert!(!cat.is_empty());

        let other = ConnectorsAndTasksBuilder::new()
            .add_connector("conn1".to_string())
            .build();

        cat.remove_all(&other);
        assert!(cat.connectors.is_empty());
        assert_eq!(cat.tasks.len(), 1);
    }

    #[test]
    fn test_worker_load() {
        let mut wl = WorkerLoad::new("worker1".to_string());
        assert!(wl.is_empty());

        wl.assign_connector("conn1".to_string());
        wl.assign_task(ConnectorTaskId::new("conn1".to_string(), 0));

        assert!(!wl.is_empty());
        assert_eq!(wl.connectors_size(), 1);
        assert_eq!(wl.tasks_size(), 1);
    }

    #[test]
    fn test_worker_load_remove() {
        let mut wl = WorkerLoad::with_assignments(
            "worker1".to_string(),
            vec!["conn1".to_string(), "conn2".to_string()],
            vec![ConnectorTaskId::new("conn1".to_string(), 0)],
        );

        let to_remove: BTreeSet<String> = vec!["conn1".to_string()].into_iter().collect();
        wl.remove_connectors(&to_remove);

        assert_eq!(wl.connectors_size(), 1);
        assert_eq!(wl.connectors()[0], "conn2");
    }

    #[test]
    fn test_worker_load_comparator() {
        let wl1 =
            WorkerLoad::with_assignments("worker1".to_string(), vec!["conn1".to_string()], vec![]);
        let wl2 = WorkerLoad::with_assignments(
            "worker2".to_string(),
            vec!["conn1".to_string(), "conn2".to_string()],
            vec![],
        );

        assert_eq!(
            WorkerLoad::connector_comparator(&wl1, &wl2),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn test_cluster_assignment_empty() {
        let ca = ClusterAssignment::empty();
        assert!(ca.newly_assigned_connectors().is_empty());
        assert!(ca.newly_assigned_tasks().is_empty());
        assert!(ca.all_workers().is_empty());
    }

    #[test]
    fn test_cluster_assignment_with_data() {
        let mut newly_assigned_connectors: HashMap<String, Vec<String>> = HashMap::new();
        newly_assigned_connectors.insert("worker1".to_string(), vec!["conn1".to_string()]);

        let mut newly_assigned_tasks: HashMap<String, Vec<ConnectorTaskId>> = HashMap::new();
        newly_assigned_tasks.insert(
            "worker1".to_string(),
            vec![ConnectorTaskId::new("conn1".to_string(), 0)],
        );

        let ca = ClusterAssignment::new(
            newly_assigned_connectors,
            newly_assigned_tasks,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );

        assert_eq!(ca.newly_assigned_connectors_for("worker1").len(), 1);
        assert_eq!(ca.newly_assigned_tasks_for("worker1").len(), 1);
        assert!(ca.all_workers().contains("worker1"));
    }

    #[test]
    fn test_exponential_backoff() {
        let backoff = ExponentialBackoff::new(1, 2.0, 100, 0);

        assert_eq!(backoff.backoff(0), 0);
        assert_eq!(backoff.backoff(1), 1);
        assert_eq!(backoff.backoff(2), 2);
        assert_eq!(backoff.backoff(3), 4);
        assert_eq!(backoff.backoff(10), 100); // capped at max
    }

    #[test]
    fn test_exponential_backoff_zero_initial() {
        let backoff = ExponentialBackoff::new(0, 2.0, 100, 0);

        assert_eq!(backoff.backoff(1), 0);
        assert_eq!(backoff.backoff(10), 0);
    }

    #[test]
    fn test_incremental_cooperative_assignor_creation() {
        let assignor = IncrementalCooperativeAssignor::new(60000);
        assert_eq!(assignor.max_delay, 60000);
        assert_eq!(assignor.version(), "compatible");
        assert_eq!(
            assignor.protocol(),
            ConnectProtocolCompatibility::Compatible
        );
    }

    #[test]
    fn test_incremental_cooperative_assignor_zero_delay() {
        let assignor = IncrementalCooperativeAssignor::new(0);
        assert_eq!(assignor.max_delay, 0);
        assert_eq!(
            assignor.consecutive_revoking_rebalances_backoff.backoff(1),
            0
        );
    }

    #[test]
    fn test_diff_connectors_and_tasks() {
        let assignor = IncrementalCooperativeAssignor::new(60000);

        let base = ConnectorsAndTasksBuilder::new()
            .add_connector("conn1".to_string())
            .add_connector("conn2".to_string())
            .add_task(ConnectorTaskId::new("conn1".to_string(), 0))
            .build();

        let to_subtract = ConnectorsAndTasksBuilder::new()
            .add_connector("conn1".to_string())
            .build();

        let result = assignor.diff_connectors_and_tasks(&base, &[&to_subtract]);

        assert!(!result.contains_connector("conn1"));
        assert!(result.contains_connector("conn2"));
        assert!(result.contains_task(&ConnectorTaskId::new("conn1".to_string(), 0)));
    }

    #[test]
    fn test_intersection() {
        let assignor = IncrementalCooperativeAssignor::new(60000);

        let cat = ConnectorsAndTasksBuilder::new()
            .add_connector("conn1".to_string())
            .add_connector("conn2".to_string())
            .build();

        let mut assignments: HashMap<String, ConnectorsAndTasks> = HashMap::new();
        assignments.insert(
            "worker1".to_string(),
            ConnectorsAndTasksBuilder::new()
                .add_connector("conn1".to_string())
                .build(),
        );
        assignments.insert(
            "worker2".to_string(),
            ConnectorsAndTasksBuilder::new()
                .add_connector("conn3".to_string())
                .build(),
        );

        let result = assignor.intersection(&cat, &assignments);

        assert_eq!(result.get("worker1").unwrap().connectors.len(), 1);
        assert!(result.get("worker1").unwrap().contains_connector("conn1"));
        assert_eq!(result.get("worker2").unwrap().connectors.len(), 0);
    }

    #[test]
    fn test_duplicated_assignments() {
        let assignor = IncrementalCooperativeAssignor::new(60000);

        let mut assignments: HashMap<String, ConnectorsAndTasks> = HashMap::new();
        assignments.insert(
            "worker1".to_string(),
            ConnectorsAndTasksBuilder::new()
                .add_connector("conn1".to_string())
                .build(),
        );
        assignments.insert(
            "worker2".to_string(),
            ConnectorsAndTasksBuilder::new()
                .add_connector("conn1".to_string())
                .build(),
        );

        let duplicated = assignor.duplicated_assignments(&assignments);

        assert!(duplicated.contains_connector("conn1"));
        assert_eq!(duplicated.connectors.len(), 1);
    }

    #[test]
    fn test_assign_connectors() {
        let assignor = IncrementalCooperativeAssignor::new(60000);

        let mut worker_assignment: Vec<WorkerLoad> = vec![
            WorkerLoad::new("worker1".to_string()),
            WorkerLoad::new("worker2".to_string()),
        ];

        let connectors = vec![
            "conn1".to_string(),
            "conn2".to_string(),
            "conn3".to_string(),
        ];

        assignor.assign_connectors(&mut worker_assignment, connectors);

        // Both workers should have some connectors
        assert!(!worker_assignment[0].is_empty() || !worker_assignment[1].is_empty());
        // Total connectors assigned should equal input
        let total_assigned = worker_assignment
            .iter()
            .map(|wl| wl.connectors_size())
            .sum::<usize>();
        assert_eq!(total_assigned, 3);
    }

    #[test]
    fn test_assign_tasks() {
        let assignor = IncrementalCooperativeAssignor::new(60000);

        let mut worker_assignment: Vec<WorkerLoad> = vec![
            WorkerLoad::new("worker1".to_string()),
            WorkerLoad::new("worker2".to_string()),
        ];

        let tasks = vec![
            ConnectorTaskId::new("conn1".to_string(), 0),
            ConnectorTaskId::new("conn1".to_string(), 1),
            ConnectorTaskId::new("conn2".to_string(), 0),
        ];

        assignor.assign_tasks(&mut worker_assignment, tasks);

        let total_assigned = worker_assignment
            .iter()
            .map(|wl| wl.tasks_size())
            .sum::<usize>();
        assert_eq!(total_assigned, 3);
    }

    #[test]
    fn test_config_snapshot() {
        let mut tasks_by_connector: HashMap<String, Vec<ConnectorTaskId>> = HashMap::new();
        tasks_by_connector.insert(
            "conn1".to_string(),
            vec![
                ConnectorTaskId::new("conn1".to_string(), 0),
                ConnectorTaskId::new("conn1".to_string(), 1),
            ],
        );

        let snapshot = ConfigSnapshot::new(
            100,
            vec!["conn1".to_string()].into_iter().collect(),
            tasks_by_connector,
        );

        assert_eq!(snapshot.offset(), 100);
        assert_eq!(snapshot.connectors().len(), 1);
        assert_eq!(snapshot.tasks("conn1").len(), 2);
        assert_eq!(snapshot.all_tasks().len(), 2);
    }

    #[test]
    fn test_perform_load_balancing_revocations_empty() {
        let assignor = IncrementalCooperativeAssignor::new(60000);

        let configured = ConnectorsAndTasks::empty();
        let workers: Vec<WorkerLoad> = vec![];

        let result = assignor.perform_load_balancing_revocations(&configured, &workers);
        assert!(result.is_empty());
    }

    #[test]
    fn test_perform_load_balancing_revocations_balanced() {
        let assignor = IncrementalCooperativeAssignor::new(60000);

        let configured = ConnectorsAndTasksBuilder::new()
            .add_connector("conn1".to_string())
            .add_connector("conn2".to_string())
            .build();

        let workers: Vec<WorkerLoad> = vec![
            WorkerLoad::with_assignments("worker1".to_string(), vec!["conn1".to_string()], vec![]),
            WorkerLoad::with_assignments("worker2".to_string(), vec!["conn2".to_string()], vec![]),
        ];

        let result = assignor.perform_load_balancing_revocations(&configured, &workers);
        // Already balanced, should return empty
        assert!(result.is_empty());
    }

    #[test]
    fn test_reset_delay() {
        let mut assignor = IncrementalCooperativeAssignor::new(60000);
        assignor.delay = 5000;
        assignor.scheduled_rebalance = 100000;
        assignor
            .candidate_workers_for_reassignment
            .push("worker1".to_string());

        assignor.reset_delay();

        assert_eq!(assignor.delay, 0);
        assert_eq!(assignor.scheduled_rebalance, 0);
        assert!(assignor.candidate_workers_for_reassignment.is_empty());
    }

    #[test]
    fn test_assignment_combination() {
        let assignor = IncrementalCooperativeAssignor::new(60000);

        let mut member_assignments: HashMap<String, ConnectorsAndTasks> = HashMap::new();
        member_assignments.insert(
            "worker1".to_string(),
            ConnectorsAndTasksBuilder::new()
                .add_connector("conn1".to_string())
                .build(),
        );
        member_assignments.insert(
            "worker2".to_string(),
            ConnectorsAndTasksBuilder::new()
                .add_connector("conn2".to_string())
                .build(),
        );

        let combined = assignor.assignment(&member_assignments);

        assert_eq!(combined.connectors.len(), 2);
        assert!(combined.contains_connector("conn1"));
        assert!(combined.contains_connector("conn2"));
    }
}
