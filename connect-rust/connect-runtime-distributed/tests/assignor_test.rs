//! Tests for the assignor module
//!
//! This module tests the ConnectAssignor trait implementations
//! and related structures.

use connect_runtime_distributed::assignor::{
    ConnectAssignor, EagerAssignor, IncrementalCooperativeAssignor, MemberMetadata,
    WorkerAssignment, WorkerLoad,
};
use connect_runtime_distributed::coordination::{
    ConnectProtocolCompatibility, ExtendedAssignment, WorkerCoordinator,
};
use std::collections::HashMap;
use std::error::Error;

/// Mock worker coordinator for testing
struct MockWorkerCoordinator {
    assignment: Option<ExtendedAssignment>,
}

impl MockWorkerCoordinator {
    fn new() -> Self {
        MockWorkerCoordinator { assignment: None }
    }

    fn set_assignment(&mut self, assignment: ExtendedAssignment) {
        self.assignment = Some(assignment);
    }
}

impl WorkerCoordinator for MockWorkerCoordinator {
    fn request_rejoin(&self, _reason: &str) {
        // Mock implementation
    }

    fn protocol_type(&self) -> &str {
        "eager"
    }

    fn poll(&self, _timeout_ms: i64) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn member_id(&self) -> Option<String> {
        Some("member-1".to_string())
    }

    fn generation_id(&self) -> i32 {
        1
    }

    fn last_completed_generation_id(&self) -> i32 {
        0
    }

    fn rejoin_needed(&self) -> bool {
        false
    }

    fn assignment(&self) -> Option<ExtendedAssignment> {
        self.assignment.clone()
    }

    fn current_protocol_version(&self) -> u16 {
        0
    }

    fn owner_url_for_connector(&self, _connector: &str) -> Option<String> {
        None
    }

    fn owner_url_for_task(&self, _task: &str) -> Option<String> {
        None
    }

    fn revoke_assignment(&self, _assignment: &ExtendedAssignment) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

#[test]
fn test_member_metadata_creation() {
    let metadata = MemberMetadata {
        member_id: "member-1".to_string(),
        metadata: vec![1, 2, 3],
        url: "http://localhost:8083".to_string(),
        offset: 100,
    };

    assert_eq!(metadata.member_id, "member-1");
    assert_eq!(metadata.url, "http://localhost:8083");
    assert_eq!(metadata.offset, 100);
}

#[test]
fn test_eager_assignor_creation() {
    let assignor = EagerAssignor::new("test-assignor");
    // Just verify creation succeeds
    let _ = assignor;
}

#[test]
fn test_eager_assignor_perform_assignment_empty() {
    let assignor = EagerAssignor::new("test-assignor");
    let coordinator = MockWorkerCoordinator::new();
    let result = assignor.perform_assignment(
        "leader-1",
        ConnectProtocolCompatibility::Eager,
        &[],
        &coordinator,
    );

    assert!(result.is_ok());
    let assignments = result.unwrap();
    assert!(assignments.is_empty());
}

#[test]
fn test_eager_assignor_perform_assignment_with_members() {
    let assignor = EagerAssignor::new("test-assignor");
    let coordinator = MockWorkerCoordinator::new();

    let members = vec![MemberMetadata {
        member_id: "member-1".to_string(),
        metadata: vec![],
        url: "http://localhost:8083".to_string(),
        offset: 0,
    }];

    let result = assignor.perform_assignment(
        "leader-1",
        ConnectProtocolCompatibility::Eager,
        &members,
        &coordinator,
    );

    assert!(result.is_ok());
    let assignments = result.unwrap();
    assert_eq!(assignments.len(), 1);
    assert!(assignments.contains_key("member-1"));
}

#[test]
fn test_incremental_cooperative_assignor_creation() {
    let assignor = IncrementalCooperativeAssignor::new("test-assignor", 5000);
    assert_eq!(assignor.max_delay_ms(), 5000);
}

#[test]
fn test_incremental_cooperative_assignor_perform_assignment_empty() {
    let assignor = IncrementalCooperativeAssignor::new("test-assignor", 5000);
    let coordinator = MockWorkerCoordinator::new();
    let result = assignor.perform_assignment(
        "leader-1",
        ConnectProtocolCompatibility::Compatible,
        &[],
        &coordinator,
    );

    assert!(result.is_ok());
    let assignments = result.unwrap();
    assert!(assignments.is_empty());
}

#[test]
fn test_incremental_cooperative_assignor_perform_assignment_with_members() {
    let assignor = IncrementalCooperativeAssignor::new("test-assignor", 5000);
    let coordinator = MockWorkerCoordinator::new();

    let members = vec![MemberMetadata {
        member_id: "member-1".to_string(),
        metadata: vec![],
        url: "http://localhost:8083".to_string(),
        offset: 0,
    }];

    let result = assignor.perform_assignment(
        "leader-1",
        ConnectProtocolCompatibility::Compatible,
        &members,
        &coordinator,
    );

    assert!(result.is_ok());
    let assignments = result.unwrap();
    assert_eq!(assignments.len(), 1);
    assert!(assignments.contains_key("member-1"));
}

#[test]
fn test_worker_load_creation() {
    let load = WorkerLoad::new("worker-1".to_string());
    assert_eq!(load.worker_id, "worker-1");
    assert_eq!(load.connector_count, 0);
    assert_eq!(load.task_count, 0);
    assert_eq!(load.total_load(), 0);
}

#[test]
fn test_worker_assignment_creation() {
    let assignment = WorkerAssignment::new("worker-1".to_string());
    assert_eq!(assignment.worker_id, "worker-1");
    assert!(assignment.connectors.is_empty());
    assert!(assignment.tasks.is_empty());
    assert_eq!(assignment.total_count(), 0);
}

#[test]
fn test_connect_protocol_compatibility_version() {
    assert_eq!(ConnectProtocolCompatibility::Eager.protocol_version(), 0);
    assert_eq!(
        ConnectProtocolCompatibility::Compatible.protocol_version(),
        1
    );
    assert_eq!(
        ConnectProtocolCompatibility::Sessioned.protocol_version(),
        2
    );
}

#[test]
fn test_connect_protocol_compatibility_name() {
    assert_eq!(ConnectProtocolCompatibility::Eager.protocol_name(), "eager");
    assert_eq!(
        ConnectProtocolCompatibility::Compatible.protocol_name(),
        "compatible"
    );
    assert_eq!(
        ConnectProtocolCompatibility::Sessioned.protocol_name(),
        "sessioned"
    );
}

#[test]
fn test_connect_protocol_compatibility_from_name() {
    assert_eq!(
        ConnectProtocolCompatibility::from_protocol("eager"),
        Some(ConnectProtocolCompatibility::Eager)
    );
    assert_eq!(
        ConnectProtocolCompatibility::from_protocol("compatible"),
        Some(ConnectProtocolCompatibility::Compatible)
    );
    assert_eq!(
        ConnectProtocolCompatibility::from_protocol("sessioned"),
        Some(ConnectProtocolCompatibility::Sessioned)
    );
    assert_eq!(ConnectProtocolCompatibility::from_protocol("invalid"), None);
}
