//! Tests for the coordination module
//!
//! This module tests the WorkerCoordinator and WorkerGroupMember traits
//! and their default implementations.

use connect_runtime_distributed::coordination::{
    ConnectProtocol, ConnectProtocolCompatibility, DefaultWorkerCoordinator,
    DefaultWorkerGroupMember, ExtendedAssignment, ExtendedWorkerState, WorkerCoordinator,
    WorkerGroupMember,
};
use std::error::Error;

#[test]
fn test_connect_protocol_compatibility_eager() {
    let protocol = ConnectProtocolCompatibility::Eager;
    assert_eq!(protocol.protocol_version(), 0);
    assert_eq!(protocol.protocol_name(), "eager");
}

#[test]
fn test_connect_protocol_compatibility_compatible() {
    let protocol = ConnectProtocolCompatibility::Compatible;
    assert_eq!(protocol.protocol_version(), 1);
    assert_eq!(protocol.protocol_name(), "compatible");
}

#[test]
fn test_connect_protocol_compatibility_sessioned() {
    let protocol = ConnectProtocolCompatibility::Sessioned;
    assert_eq!(protocol.protocol_version(), 2);
    assert_eq!(protocol.protocol_name(), "sessioned");
}

#[test]
fn test_connect_protocol_compatibility_from_protocol() {
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

#[test]
fn test_extended_assignment_empty() {
    let assignment = ExtendedAssignment::empty();
    assert!(assignment.leader.is_none());
    assert!(assignment.connectors.is_empty());
    assert!(assignment.tasks.is_empty());
    assert!(assignment.revoked_connectors.is_empty());
    assert!(assignment.revoked_tasks.is_empty());
    assert!(assignment.error.is_none());
}

#[test]
fn test_extended_assignment_failed() {
    let mut assignment = ExtendedAssignment::empty();
    assert!(!assignment.failed());

    assignment.error = Some("Test error".to_string());
    assert!(assignment.failed());
}

#[test]
fn test_extended_assignment_with_data() {
    let assignment = ExtendedAssignment {
        leader: Some("http://leader:8083".to_string()),
        connectors: vec!["connector-1".to_string(), "connector-2".to_string()],
        tasks: vec!["connector-1:0".to_string(), "connector-2:0".to_string()],
        revoked_connectors: vec![],
        revoked_tasks: vec![],
        error: None,
    };

    assert_eq!(assignment.leader, Some("http://leader:8083".to_string()));
    assert_eq!(assignment.connectors.len(), 2);
    assert_eq!(assignment.tasks.len(), 2);
    assert!(!assignment.failed());
}

#[test]
fn test_extended_worker_state() {
    let state = ExtendedWorkerState {
        url: "http://worker:8083".to_string(),
        offset: 100,
        assignment: None,
    };

    assert_eq!(state.url, "http://worker:8083");
    assert_eq!(state.offset, 100);
    assert!(state.assignment.is_none());
}

#[test]
fn test_eager_protocol_metadata_request() {
    let state = ExtendedWorkerState {
        url: "http://worker:8083".to_string(),
        offset: 100,
        assignment: None,
    };

    let result = connect_runtime_distributed::coordination::EagerProtocol::metadata_request(&state);
    assert!(result.is_ok());
    let metadata = result.unwrap();
    assert!(!metadata.is_empty());
}

#[test]
fn test_eager_protocol_serialize_deserialize_assignment() {
    let assignment = ExtendedAssignment {
        leader: Some("http://leader:8083".to_string()),
        connectors: vec!["connector-1".to_string()],
        tasks: vec!["connector-1:0".to_string()],
        revoked_connectors: vec![],
        revoked_tasks: vec![],
        error: None,
    };

    let serialized =
        connect_runtime_distributed::coordination::EagerProtocol::serialize_assignment(&assignment);
    assert!(serialized.is_ok());

    let deserialized =
        connect_runtime_distributed::coordination::EagerProtocol::deserialize_assignment(
            &serialized.unwrap(),
        );
    assert!(deserialized.is_ok());

    let result = deserialized.unwrap();
    assert_eq!(result.leader, assignment.leader);
    assert_eq!(result.connectors, assignment.connectors);
    assert_eq!(result.tasks, assignment.tasks);
}

#[test]
fn test_compatible_protocol_metadata_request() {
    let state = ExtendedWorkerState {
        url: "http://worker:8083".to_string(),
        offset: 100,
        assignment: None,
    };

    let result =
        connect_runtime_distributed::coordination::CompatibleProtocol::metadata_request(&state);
    assert!(result.is_ok());
    let metadata = result.unwrap();
    assert!(!metadata.is_empty());
}

#[test]
fn test_compatible_protocol_serialize_deserialize_assignment() {
    let assignment = ExtendedAssignment {
        leader: Some("http://leader:8083".to_string()),
        connectors: vec!["connector-1".to_string()],
        tasks: vec!["connector-1:0".to_string()],
        revoked_connectors: vec![],
        revoked_tasks: vec![],
        error: None,
    };

    let serialized =
        connect_runtime_distributed::coordination::CompatibleProtocol::serialize_assignment(
            &assignment,
        );
    assert!(serialized.is_ok());

    let deserialized =
        connect_runtime_distributed::coordination::CompatibleProtocol::deserialize_assignment(
            &serialized.unwrap(),
        );
    assert!(deserialized.is_ok());

    let result = deserialized.unwrap();
    assert_eq!(result.leader, assignment.leader);
    assert_eq!(result.connectors, assignment.connectors);
}

#[test]
fn test_sessioned_protocol_metadata_request() {
    let state = ExtendedWorkerState {
        url: "http://worker:8083".to_string(),
        offset: 100,
        assignment: None,
    };

    let result =
        connect_runtime_distributed::coordination::SessionedProtocol::metadata_request(&state);
    assert!(result.is_ok());
    let metadata = result.unwrap();
    assert!(!metadata.is_empty());
}

#[test]
fn test_sessioned_protocol_serialize_deserialize_assignment() {
    let assignment = ExtendedAssignment {
        leader: Some("http://leader:8083".to_string()),
        connectors: vec!["connector-1".to_string()],
        tasks: vec!["connector-1:0".to_string()],
        revoked_connectors: vec![],
        revoked_tasks: vec![],
        error: None,
    };

    let serialized =
        connect_runtime_distributed::coordination::SessionedProtocol::serialize_assignment(
            &assignment,
        );
    assert!(serialized.is_ok());

    let deserialized =
        connect_runtime_distributed::coordination::SessionedProtocol::deserialize_assignment(
            &serialized.unwrap(),
        );
    assert!(deserialized.is_ok());

    let result = deserialized.unwrap();
    assert_eq!(result.leader, assignment.leader);
    assert_eq!(result.connectors, assignment.connectors);
}

#[test]
fn test_default_worker_coordinator_creation() {
    let coordinator =
        DefaultWorkerCoordinator::new("eager".to_string(), ConnectProtocolCompatibility::Eager);

    assert_eq!(coordinator.protocol_type(), "eager");
    assert_eq!(coordinator.current_protocol_version(), 0);
    assert_eq!(coordinator.generation_id(), 0);
    assert_eq!(coordinator.last_completed_generation_id(), -1);
    assert!(!coordinator.rejoin_needed());
    assert!(coordinator.member_id().is_none());
    assert!(coordinator.assignment().is_none());
}

#[test]
fn test_default_worker_coordinator_set_member_id() {
    let coordinator =
        DefaultWorkerCoordinator::new("eager".to_string(), ConnectProtocolCompatibility::Eager);

    coordinator.set_member_id("member-1".to_string());
    assert_eq!(coordinator.member_id(), Some("member-1".to_string()));
}

#[test]
fn test_default_worker_coordinator_set_generation_id() {
    let coordinator =
        DefaultWorkerCoordinator::new("eager".to_string(), ConnectProtocolCompatibility::Eager);

    coordinator.set_generation_id(5);
    assert_eq!(coordinator.generation_id(), 5);
}

#[test]
fn test_default_worker_coordinator_set_assignment() {
    let coordinator =
        DefaultWorkerCoordinator::new("eager".to_string(), ConnectProtocolCompatibility::Eager);

    let assignment = ExtendedAssignment {
        leader: Some("http://leader:8083".to_string()),
        connectors: vec!["connector-1".to_string()],
        tasks: vec!["connector-1:0".to_string()],
        revoked_connectors: vec![],
        revoked_tasks: vec![],
        error: None,
    };

    coordinator.set_assignment(assignment.clone());
    let retrieved = coordinator.assignment();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.connectors, assignment.connectors);
}

#[test]
fn test_default_worker_coordinator_update_owner_mappings() {
    let coordinator =
        DefaultWorkerCoordinator::new("eager".to_string(), ConnectProtocolCompatibility::Eager);

    let assignment = ExtendedAssignment {
        leader: Some("http://leader:8083".to_string()),
        connectors: vec!["connector-1".to_string(), "connector-2".to_string()],
        tasks: vec!["connector-1:0".to_string(), "connector-2:0".to_string()],
        revoked_connectors: vec![],
        revoked_tasks: vec![],
        error: None,
    };

    coordinator.update_owner_mappings("http://worker:8083", &assignment);

    assert_eq!(
        coordinator.owner_url_for_connector("connector-1"),
        Some("http://worker:8083".to_string())
    );
    assert_eq!(
        coordinator.owner_url_for_connector("connector-2"),
        Some("http://worker:8083".to_string())
    );
    assert_eq!(
        coordinator.owner_url_for_task("connector-1:0"),
        Some("http://worker:8083".to_string())
    );
    assert_eq!(
        coordinator.owner_url_for_task("connector-2:0"),
        Some("http://worker:8083".to_string())
    );
}

#[test]
fn test_default_worker_coordinator_revoke_assignment() {
    let coordinator =
        DefaultWorkerCoordinator::new("eager".to_string(), ConnectProtocolCompatibility::Eager);

    let assignment = ExtendedAssignment {
        leader: Some("http://leader:8083".to_string()),
        connectors: vec!["connector-1".to_string()],
        tasks: vec!["connector-1:0".to_string()],
        revoked_connectors: vec![],
        revoked_tasks: vec![],
        error: None,
    };

    coordinator.update_owner_mappings("http://worker:8083", &assignment);

    assert!(coordinator.owner_url_for_connector("connector-1").is_some());

    let result = coordinator.revoke_assignment(&assignment);
    assert!(result.is_ok());

    assert!(coordinator.owner_url_for_connector("connector-1").is_none());
}

#[test]
fn test_default_worker_coordinator_request_rejoin() {
    let coordinator =
        DefaultWorkerCoordinator::new("eager".to_string(), ConnectProtocolCompatibility::Eager);

    assert!(!coordinator.rejoin_needed());
    coordinator.request_rejoin("Test reason");
    assert!(coordinator.rejoin_needed());
}

#[test]
fn test_default_worker_coordinator_poll() {
    let coordinator =
        DefaultWorkerCoordinator::new("eager".to_string(), ConnectProtocolCompatibility::Eager);

    let result = coordinator.poll(1000);
    assert!(result.is_ok());
}

#[test]
fn test_default_worker_group_member_creation() {
    let member = DefaultWorkerGroupMember::new("http://worker:8083".to_string());

    assert_eq!(member.worker_url(), "http://worker:8083");
    assert!(!member.is_ready());
}

#[test]
fn test_default_worker_group_member_start_stop() {
    let mut member = DefaultWorkerGroupMember::new("http://worker:8083".to_string());

    assert!(!member.is_ready());

    let start_result = member.start();
    assert!(start_result.is_ok());
    assert!(member.is_ready());

    let stop_result = member.stop();
    assert!(stop_result.is_ok());
    assert!(!member.is_ready());
}

#[test]
fn test_default_worker_group_member_start_twice_fails() {
    let mut member = DefaultWorkerGroupMember::new("http://worker:8083".to_string());

    let start_result1 = member.start();
    assert!(start_result1.is_ok());

    let start_result2 = member.start();
    assert!(start_result2.is_err());
}

#[test]
fn test_default_worker_group_member_stop_without_start_fails() {
    let mut member = DefaultWorkerGroupMember::new("http://worker:8083".to_string());

    let stop_result = member.stop();
    assert!(stop_result.is_err());
}

#[test]
fn test_default_worker_group_member_request_leave() {
    let mut member = DefaultWorkerGroupMember::new("http://worker:8083".to_string());

    let start_result = member.start();
    assert!(start_result.is_ok());

    let leave_result = member.request_leave();
    assert!(leave_result.is_ok());
}

#[test]
fn test_default_worker_group_member_set_worker_state() {
    let member = DefaultWorkerGroupMember::new("http://worker:8083".to_string());

    let state = ExtendedWorkerState {
        url: "http://worker:8083".to_string(),
        offset: 100,
        assignment: None,
    };

    member.set_worker_state(state.clone());
    let retrieved = member.worker_state();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.url, state.url);
    assert_eq!(retrieved.offset, state.offset);
}

#[test]
fn test_default_worker_group_member_with_coordinator() {
    let coordinator = Box::new(DefaultWorkerCoordinator::new(
        "eager".to_string(),
        ConnectProtocolCompatibility::Eager,
    ));

    let mut member =
        DefaultWorkerGroupMember::with_coordinator("http://worker:8083".to_string(), coordinator);

    assert_eq!(member.worker_url(), "http://worker:8083");

    let start_result = member.start();
    assert!(start_result.is_ok());
    assert!(member.is_ready());
}
