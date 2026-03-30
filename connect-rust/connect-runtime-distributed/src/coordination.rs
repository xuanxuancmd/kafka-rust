//! Coordination module for distributed worker coordination
//!
//! This module provides traits and structures for coordinating workers
//! in a distributed Kafka Connect cluster.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

/// Protocol compatibility levels for Connect coordination
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectProtocolCompatibility {
    /// Eager protocol - full rebalance on every change
    Eager,
    /// Compatible protocol - incremental cooperative rebalance
    Compatible,
    /// Sessioned protocol - incremental cooperative with session tracking
    Sessioned,
}

impl ConnectProtocolCompatibility {
    /// Get the protocol version
    pub fn protocol_version(&self) -> u16 {
        match self {
            ConnectProtocolCompatibility::Eager => 0,
            ConnectProtocolCompatibility::Compatible => 1,
            ConnectProtocolCompatibility::Sessioned => 2,
        }
    }

    /// Get protocol name
    pub fn protocol_name(&self) -> &'static str {
        match self {
            ConnectProtocolCompatibility::Eager => "eager",
            ConnectProtocolCompatibility::Compatible => "compatible",
            ConnectProtocolCompatibility::Sessioned => "sessioned",
        }
    }

    /// Parse protocol from name
    pub fn from_protocol(name: &str) -> Option<Self> {
        match name {
            "eager" => Some(ConnectProtocolCompatibility::Eager),
            "compatible" => Some(ConnectProtocolCompatibility::Compatible),
            "sessioned" => Some(ConnectProtocolCompatibility::Sessioned),
            _ => None,
        }
    }
}

/// Extended assignment for distributed workers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendedAssignment {
    /// Leader URL
    pub leader: Option<String>,
    /// Assigned connectors
    pub connectors: Vec<String>,
    /// Assigned tasks
    pub tasks: Vec<String>,
    /// Revoked connectors (for cooperative rebalance)
    pub revoked_connectors: Vec<String>,
    /// Revoked tasks (for cooperative rebalance)
    pub revoked_tasks: Vec<String>,
    /// Error message if assignment failed
    pub error: Option<String>,
}

impl ExtendedAssignment {
    /// Create an empty assignment
    pub fn empty() -> Self {
        ExtendedAssignment {
            leader: None,
            connectors: Vec::new(),
            tasks: Vec::new(),
            revoked_connectors: Vec::new(),
            revoked_tasks: Vec::new(),
            error: None,
        }
    }

    /// Check if assignment failed
    pub fn failed(&self) -> bool {
        self.error.is_some()
    }
}

/// Extended worker state
#[derive(Debug, Clone)]
pub struct ExtendedWorkerState {
    /// Worker URL
    pub url: String,
    /// Configuration offset
    pub offset: i64,
    /// Current assignment
    pub assignment: Option<ExtendedAssignment>,
}

/// Connect protocol trait for serialization/deserialization
pub trait ConnectProtocol {
    /// Create metadata request from worker state
    fn metadata_request(worker_state: &ExtendedWorkerState) -> Result<Vec<u8>, Box<dyn Error>>;

    /// Deserialize assignment from bytes
    fn deserialize_assignment(data: &[u8]) -> Result<ExtendedAssignment, Box<dyn Error>>;

    /// Serialize assignment to bytes
    fn serialize_assignment(assignment: &ExtendedAssignment) -> Result<Vec<u8>, Box<dyn Error>>;
}

/// Eager protocol implementation
pub struct EagerProtocol;

impl ConnectProtocol for EagerProtocol {
    fn metadata_request(worker_state: &ExtendedWorkerState) -> Result<Vec<u8>, Box<dyn Error>> {
        let metadata = WorkerMetadata {
            url: worker_state.url.clone(),
            offset: worker_state.offset,
        };
        serde_json::to_vec(&metadata).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn deserialize_assignment(data: &[u8]) -> Result<ExtendedAssignment, Box<dyn Error>> {
        serde_json::from_slice(data).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn serialize_assignment(assignment: &ExtendedAssignment) -> Result<Vec<u8>, Box<dyn Error>> {
        serde_json::to_vec(assignment).map_err(|e| Box::new(e) as Box<dyn Error>)
    }
}

/// Compatible protocol implementation
pub struct CompatibleProtocol;

impl ConnectProtocol for CompatibleProtocol {
    fn metadata_request(worker_state: &ExtendedWorkerState) -> Result<Vec<u8>, Box<dyn Error>> {
        let metadata = WorkerMetadata {
            url: worker_state.url.clone(),
            offset: worker_state.offset,
        };
        serde_json::to_vec(&metadata).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn deserialize_assignment(data: &[u8]) -> Result<ExtendedAssignment, Box<dyn Error>> {
        serde_json::from_slice(data).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn serialize_assignment(assignment: &ExtendedAssignment) -> Result<Vec<u8>, Box<dyn Error>> {
        serde_json::to_vec(assignment).map_err(|e| Box::new(e) as Box<dyn Error>)
    }
}

/// Sessioned protocol implementation
pub struct SessionedProtocol;

impl ConnectProtocol for SessionedProtocol {
    fn metadata_request(worker_state: &ExtendedWorkerState) -> Result<Vec<u8>, Box<dyn Error>> {
        let metadata = WorkerMetadata {
            url: worker_state.url.clone(),
            offset: worker_state.offset,
        };
        serde_json::to_vec(&metadata).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn deserialize_assignment(data: &[u8]) -> Result<ExtendedAssignment, Box<dyn Error>> {
        serde_json::from_slice(data).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn serialize_assignment(assignment: &ExtendedAssignment) -> Result<Vec<u8>, Box<dyn Error>> {
        serde_json::to_vec(assignment).map_err(|e| Box::new(e) as Box<dyn Error>)
    }
}

/// Worker metadata for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerMetadata {
    url: String,
    offset: i64,
}

/// Worker coordinator trait for managing distributed coordination
pub trait WorkerCoordinator {
    /// Request a group rejoin
    fn request_rejoin(&self, reason: &str);

    /// Get protocol type
    fn protocol_type(&self) -> &str;

    /// Poll for coordinator events
    fn poll(&self, timeout_ms: i64) -> Result<(), Box<dyn Error>>;

    /// Get current member ID
    fn member_id(&self) -> Option<String>;

    /// Get current generation ID
    fn generation_id(&self) -> i32;

    /// Get last completed generation ID
    fn last_completed_generation_id(&self) -> i32;

    /// Check if rejoin is needed
    fn rejoin_needed(&self) -> bool;

    /// Get current assignment
    fn assignment(&self) -> Option<ExtendedAssignment>;

    /// Get current protocol version
    fn current_protocol_version(&self) -> u16;

    /// Get owner URL for a connector
    fn owner_url_for_connector(&self, connector: &str) -> Option<String>;

    /// Get owner URL for a task
    fn owner_url_for_task(&self, task: &str) -> Option<String>;

    /// Revoke assignment
    fn revoke_assignment(&self, assignment: &ExtendedAssignment) -> Result<(), Box<dyn Error>>;
}

/// Default worker coordinator implementation
pub struct DefaultWorkerCoordinator {
    /// Member ID
    member_id: Arc<Mutex<Option<String>>>,
    /// Generation ID
    generation_id: Arc<Mutex<i32>>,
    /// Last completed generation ID
    last_completed_generation_id: Arc<Mutex<i32>>,
    /// Current assignment
    assignment: Arc<Mutex<Option<ExtendedAssignment>>>,
    /// Rejoin needed flag
    rejoin_needed: Arc<Mutex<bool>>,
    /// Protocol type
    protocol_type: String,
    /// Protocol version
    protocol_version: ConnectProtocolCompatibility,
    /// Connector owner mapping
    connector_owners: Arc<Mutex<HashMap<String, String>>>,
    /// Task owner mapping
    task_owners: Arc<Mutex<HashMap<String, String>>>,
}

impl DefaultWorkerCoordinator {
    /// Create a new default worker coordinator
    pub fn new(protocol_type: String, protocol_version: ConnectProtocolCompatibility) -> Self {
        DefaultWorkerCoordinator {
            member_id: Arc::new(Mutex::new(None)),
            generation_id: Arc::new(Mutex::new(0)),
            last_completed_generation_id: Arc::new(Mutex::new(-1)),
            assignment: Arc::new(Mutex::new(None)),
            rejoin_needed: Arc::new(Mutex::new(false)),
            protocol_type,
            protocol_version,
            connector_owners: Arc::new(Mutex::new(HashMap::new())),
            task_owners: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Set member ID
    pub fn set_member_id(&self, member_id: String) {
        let mut id = self.member_id.lock().unwrap();
        *id = Some(member_id);
    }

    /// Set generation ID
    pub fn set_generation_id(&self, generation_id: i32) {
        let mut gen = self.generation_id.lock().unwrap();
        *gen = generation_id;
    }

    /// Set assignment
    pub fn set_assignment(&self, assignment: ExtendedAssignment) {
        let mut assign = self.assignment.lock().unwrap();
        *assign = Some(assignment);
    }

    /// Update owner mappings from assignment
    pub fn update_owner_mappings(&self, worker_url: &str, assignment: &ExtendedAssignment) {
        let mut connector_owners = self.connector_owners.lock().unwrap();
        let mut task_owners = self.task_owners.lock().unwrap();

        for connector in &assignment.connectors {
            connector_owners.insert(connector.clone(), worker_url.to_string());
        }

        for task in &assignment.tasks {
            task_owners.insert(task.clone(), worker_url.to_string());
        }
    }
}

impl WorkerCoordinator for DefaultWorkerCoordinator {
    fn request_rejoin(&self, reason: &str) {
        let mut rejoin = self.rejoin_needed.lock().unwrap();
        *rejoin = true;
        // Log rejoin reason in real implementation
        let _ = reason;
    }

    fn protocol_type(&self) -> &str {
        &self.protocol_type
    }

    fn poll(&self, _timeout_ms: i64) -> Result<(), Box<dyn Error>> {
        // In real implementation, this would poll for coordinator events
        Ok(())
    }

    fn member_id(&self) -> Option<String> {
        let id = self.member_id.lock().unwrap();
        id.clone()
    }

    fn generation_id(&self) -> i32 {
        let gen = self.generation_id.lock().unwrap();
        *gen
    }

    fn last_completed_generation_id(&self) -> i32 {
        let gen = self.last_completed_generation_id.lock().unwrap();
        *gen
    }

    fn rejoin_needed(&self) -> bool {
        let rejoin = self.rejoin_needed.lock().unwrap();
        *rejoin
    }

    fn assignment(&self) -> Option<ExtendedAssignment> {
        let assign = self.assignment.lock().unwrap();
        assign.clone()
    }

    fn current_protocol_version(&self) -> u16 {
        self.protocol_version.protocol_version()
    }

    fn owner_url_for_connector(&self, connector: &str) -> Option<String> {
        let owners = self.connector_owners.lock().unwrap();
        owners.get(connector).cloned()
    }

    fn owner_url_for_task(&self, task: &str) -> Option<String> {
        let owners = self.task_owners.lock().unwrap();
        owners.get(task).cloned()
    }

    fn revoke_assignment(&self, assignment: &ExtendedAssignment) -> Result<(), Box<dyn Error>> {
        let mut connector_owners = self.connector_owners.lock().unwrap();
        let mut task_owners = self.task_owners.lock().unwrap();

        for connector in &assignment.connectors {
            connector_owners.remove(connector);
        }

        for task in &assignment.tasks {
            task_owners.remove(task);
        }

        Ok(())
    }
}

/// Worker group member trait for participating in distributed group
pub trait WorkerGroupMember {
    /// Start the group member
    fn start(&mut self) -> Result<(), Box<dyn Error>>;

    /// Stop the group member
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    /// Request to leave group
    fn request_leave(&self) -> Result<(), Box<dyn Error>>;

    /// Check if member is ready
    fn is_ready(&self) -> bool;

    /// Get coordinator
    fn coordinator(&self) -> Option<Box<dyn WorkerCoordinator>>;

    /// Get worker URL
    fn worker_url(&self) -> &str;
}

/// Default worker group member implementation
pub struct DefaultWorkerGroupMember {
    /// Worker URL
    worker_url: String,
    /// Coordinator
    coordinator: Option<Box<dyn WorkerCoordinator>>,
    /// Ready state
    ready: Arc<Mutex<bool>>,
    /// Running state
    running: Arc<Mutex<bool>>,
    /// Current worker state
    worker_state: Arc<Mutex<Option<ExtendedWorkerState>>>,
}

impl DefaultWorkerGroupMember {
    /// Create a new default worker group member
    pub fn new(worker_url: String) -> Self {
        DefaultWorkerGroupMember {
            worker_url,
            coordinator: None,
            ready: Arc::new(Mutex::new(false)),
            running: Arc::new(Mutex::new(false)),
            worker_state: Arc::new(Mutex::new(None)),
        }
    }

    /// Create with coordinator
    pub fn with_coordinator(worker_url: String, coordinator: Box<dyn WorkerCoordinator>) -> Self {
        DefaultWorkerGroupMember {
            worker_url,
            coordinator: Some(coordinator),
            ready: Arc::new(Mutex::new(false)),
            running: Arc::new(Mutex::new(false)),
            worker_state: Arc::new(Mutex::new(None)),
        }
    }

    /// Set worker state
    pub fn set_worker_state(&self, state: ExtendedWorkerState) {
        let mut worker_state = self.worker_state.lock().unwrap();
        *worker_state = Some(state);
    }

    /// Get worker state
    pub fn worker_state(&self) -> Option<ExtendedWorkerState> {
        let worker_state = self.worker_state.lock().unwrap();
        worker_state.clone()
    }
}

impl WorkerGroupMember for DefaultWorkerGroupMember {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to acquire running lock: {}", e),
            )) as Box<dyn Error>
        })?;

        if *running {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "WorkerGroupMember is already running",
            )) as Box<dyn Error>);
        }

        *running = true;

        {
            let mut ready = self.ready.lock().map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to acquire ready lock: {}", e),
                )) as Box<dyn Error>
            })?;
            *ready = true;
        }

        Ok(())
    }

    fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to acquire running lock: {}", e),
            )) as Box<dyn Error>
        })?;

        if !*running {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "WorkerGroupMember is not running",
            )) as Box<dyn Error>);
        }

        *running = false;

        {
            let mut ready = self.ready.lock().map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to acquire ready lock: {}", e),
                )) as Box<dyn Error>
            })?;
            *ready = false;
        }

        Ok(())
    }

    fn request_leave(&self) -> Result<(), Box<dyn Error>> {
        if let Some(coordinator) = &self.coordinator {
            coordinator.request_rejoin("Member requesting leave");
        }
        Ok(())
    }

    fn is_ready(&self) -> bool {
        let ready = self.ready.lock();
        if let Ok(ready) = ready {
            *ready
        } else {
            false
        }
    }

    fn coordinator(&self) -> Option<Box<dyn WorkerCoordinator>> {
        // Note: This is a simplified implementation
        // In real code, we would need to clone the coordinator
        None
    }

    fn worker_url(&self) -> &str {
        &self.worker_url
    }
}
