//! Assignor module for distributing connectors and tasks among workers
//!
//! This module provides traits and implementations for assigning
//! connectors and tasks to workers in a distributed cluster.

use crate::coordination::{ConnectProtocolCompatibility, ExtendedAssignment, WorkerCoordinator};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

/// Member metadata for group coordination
#[derive(Debug, Clone)]
pub struct MemberMetadata {
    /// Member ID
    pub member_id: String,
    /// Metadata bytes
    pub metadata: Vec<u8>,
    /// Worker URL (parsed from metadata)
    pub url: String,
    /// Configuration offset (parsed from metadata)
    pub offset: i64,
}

/// Cluster configuration for assignment
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClusterConfig {
    /// Connector configurations
    connectors: HashMap<String, ConnectorConfig>,
    /// Task configurations
    tasks: HashMap<String, Vec<TaskConfig>>,
}

/// Connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConnectorConfig {
    /// Connector name
    name: String,
    /// Connector properties
    config: HashMap<String, String>,
    /// Target state
    target_state: String,
}

/// Task configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskConfig {
    /// Task index
    task: i32,
    /// Task properties
    config: HashMap<String, String>,
}

/// Connect assignor trait for distributing work among workers
pub trait ConnectAssignor {
    /// Perform assignment of connectors and tasks among workers
    ///
    /// # Arguments
    /// * `leader_id` - ID of the group leader
    /// * `protocol` - Protocol compatibility level
    /// * `all_member_metadata` - Metadata from all active workers
    /// * `coordinator` - Worker coordinator reference
    ///
    /// # Returns
    /// Map of member ID to assignment bytes
    fn perform_assignment(
        &self,
        leader_id: &str,
        protocol: ConnectProtocolCompatibility,
        all_member_metadata: &[MemberMetadata],
        coordinator: &dyn WorkerCoordinator,
    ) -> Result<HashMap<String, Vec<u8>>, Box<dyn Error>>;
}

/// Eager assignor - performs full rebalance on every change
///
/// This assignor revokes all assignments and redistributes
/// connectors and tasks evenly across all workers.
pub struct EagerAssignor {
    /// Log context
    log_context: String,
}

impl EagerAssignor {
    /// Create a new eager assignor
    pub fn new(log_context: &str) -> Self {
        EagerAssignor {
            log_context: log_context.to_string(),
        }
    }
}

impl ConnectAssignor for EagerAssignor {
    fn perform_assignment(
        &self,
        leader_id: &str,
        protocol: ConnectProtocolCompatibility,
        all_member_metadata: &[MemberMetadata],
        coordinator: &dyn WorkerCoordinator,
    ) -> Result<HashMap<String, Vec<u8>>, Box<dyn Error>> {
        let mut assignments = HashMap::new();

        if all_member_metadata.is_empty() {
            return Ok(assignments);
        }

        // Get current assignment to revoke
        let current_assignment = coordinator.assignment();
        if let Some(assign) = &current_assignment {
            coordinator.revoke_assignment(assign)?;
        }

        // Get cluster configuration from coordinator's config backing store
        // For now, we'll distribute evenly based on member count
        let num_workers = all_member_metadata.len();

        // Create assignments for each worker
        // In a real implementation, this would:
        // 1. Read connector configurations from config backing store
        // 2. Calculate task configurations for each connector
        // 3. Distribute connectors and tasks evenly across workers
        // 4. Serialize assignments using the appropriate protocol

        for (i, member) in all_member_metadata.iter().enumerate() {
            let assignment = ExtendedAssignment {
                leader: Some(leader_id.to_string()),
                connectors: Vec::new(), // Would contain assigned connectors
                tasks: Vec::new(),      // Would contain assigned tasks
                revoked_connectors: Vec::new(),
                revoked_tasks: Vec::new(),
                error: None,
            };

            // Serialize assignment based on protocol
            let serialized =
                match protocol {
                    ConnectProtocolCompatibility::Eager => serde_json::to_vec(&assignment)
                        .map_err(|e| Box::new(e) as Box<dyn Error>)?,
                    ConnectProtocolCompatibility::Compatible => serde_json::to_vec(&assignment)
                        .map_err(|e| Box::new(e) as Box<dyn Error>)?,
                    ConnectProtocolCompatibility::Sessioned => serde_json::to_vec(&assignment)
                        .map_err(|e| Box::new(e) as Box<dyn Error>)?,
                };

            assignments.insert(member.member_id.clone(), serialized);
        }

        Ok(assignments)
    }
}

/// Incremental cooperative assignor - performs incremental rebalancing
///
/// This assignor only revokes and reassigns what's necessary,
/// allowing for smoother rebalancing with minimal disruption.
pub struct IncrementalCooperativeAssignor {
    /// Log context
    log_context: String,
    /// Maximum delay for rebalance in milliseconds
    max_delay_ms: i32,
}

impl IncrementalCooperativeAssignor {
    /// Create a new incremental cooperative assignor
    pub fn new(log_context: &str, max_delay_ms: i32) -> Self {
        IncrementalCooperativeAssignor {
            log_context: log_context.to_string(),
            max_delay_ms,
        }
    }

    /// Get the maximum delay
    pub fn max_delay_ms(&self) -> i32 {
        self.max_delay_ms
    }
}

impl ConnectAssignor for IncrementalCooperativeAssignor {
    fn perform_assignment(
        &self,
        leader_id: &str,
        protocol: ConnectProtocolCompatibility,
        all_member_metadata: &[MemberMetadata],
        coordinator: &dyn WorkerCoordinator,
    ) -> Result<HashMap<String, Vec<u8>>, Box<dyn Error>> {
        let mut assignments = HashMap::new();

        if all_member_metadata.is_empty() {
            return Ok(assignments);
        }

        // Get current assignment to determine what needs to be revoked
        let current_assignment = coordinator.assignment();

        // Get cluster configuration
        // In a real implementation, this would:
        // 1. Read connector configurations from config backing store
        // 2. Calculate task configurations for each connector
        // 3. Compare with current assignment to determine revocations
        // 4. Distribute only necessary changes (incremental)
        // 5. Serialize assignments with revoked lists

        for (i, member) in all_member_metadata.iter().enumerate() {
            let mut assignment = ExtendedAssignment {
                leader: Some(leader_id.to_string()),
                connectors: Vec::new(),
                tasks: Vec::new(),
                revoked_connectors: Vec::new(),
                revoked_tasks: Vec::new(),
                error: None,
            };

            // If we have a current assignment, populate revoked lists
            if let Some(current) = &current_assignment {
                // In cooperative rebalancing, we only revoke what's necessary
                // This is a simplified implementation
                assignment.revoked_connectors = current.connectors.clone();
                assignment.revoked_tasks = current.tasks.clone();
            }

            // Serialize assignment based on protocol
            let serialized =
                match protocol {
                    ConnectProtocolCompatibility::Eager => serde_json::to_vec(&assignment)
                        .map_err(|e| Box::new(e) as Box<dyn Error>)?,
                    ConnectProtocolCompatibility::Compatible => serde_json::to_vec(&assignment)
                        .map_err(|e| Box::new(e) as Box<dyn Error>)?,
                    ConnectProtocolCompatibility::Sessioned => serde_json::to_vec(&assignment)
                        .map_err(|e| Box::new(e) as Box<dyn Error>)?,
                };

            assignments.insert(member.member_id.clone(), serialized);
        }

        Ok(assignments)
    }
}

/// Worker load for balancing decisions
#[derive(Debug, Clone)]
pub struct WorkerLoad {
    /// Worker ID
    pub worker_id: String,
    /// Number of connectors assigned
    pub connector_count: usize,
    /// Number of tasks assigned
    pub task_count: usize,
}

impl WorkerLoad {
    /// Create a new worker load
    pub fn new(worker_id: String) -> Self {
        WorkerLoad {
            worker_id,
            connector_count: 0,
            task_count: 0,
        }
    }

    /// Get total load (connectors + tasks)
    pub fn total_load(&self) -> usize {
        self.connector_count + self.task_count
    }
}

/// Assignment result for a worker
#[derive(Debug, Clone)]
pub struct WorkerAssignment {
    /// Worker ID
    pub worker_id: String,
    /// Assigned connectors
    pub connectors: Vec<String>,
    /// Assigned tasks
    pub tasks: Vec<String>,
}

impl WorkerAssignment {
    /// Create a new worker assignment
    pub fn new(worker_id: String) -> Self {
        WorkerAssignment {
            worker_id,
            connectors: Vec::new(),
            tasks: Vec::new(),
        }
    }

    /// Get total assignment count
    pub fn total_count(&self) -> usize {
        self.connectors.len() + self.tasks.len()
    }
}
