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

//! Connect assignor trait for Kafka Connect distributed runtime.
//!
//! This module defines the `ConnectAssignor` trait that computes a distribution
//! of connectors and tasks among the workers of the group that performs rebalancing.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.distributed.ConnectAssignor` in Java.

use crate::distributed::connect_protocol::ConnectProtocolCompatibility;
use std::collections::HashMap;

/// Join group response member metadata.
/// Contains the member ID and metadata for a worker in the group.
///
/// Corresponds to `JoinGroupResponseData.JoinGroupResponseMember` in Java.
#[derive(Debug, Clone)]
pub struct JoinGroupResponseMember {
    /// The member ID assigned by the group coordinator
    pub member_id: String,
    /// The group instance ID (optional)
    pub group_instance_id: Option<String>,
    /// The member's metadata bytes
    pub metadata: Vec<u8>,
}

impl JoinGroupResponseMember {
    /// Creates a new JoinGroupResponseMember.
    pub fn new(member_id: String, metadata: Vec<u8>) -> Self {
        JoinGroupResponseMember {
            member_id,
            group_instance_id: None,
            metadata,
        }
    }

    /// Creates a new JoinGroupResponseMember with group instance ID.
    pub fn with_group_instance_id(
        member_id: String,
        group_instance_id: Option<String>,
        metadata: Vec<u8>,
    ) -> Self {
        JoinGroupResponseMember {
            member_id,
            group_instance_id,
            metadata,
        }
    }

    /// Returns the member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the group instance ID.
    pub fn group_instance_id(&self) -> Option<&String> {
        self.group_instance_id.as_ref()
    }

    /// Returns the metadata bytes.
    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }
}

/// Worker coordinator state used by the assignor.
///
/// This provides the assignor with access to the worker coordinator's current state
/// including configuration offsets, assigned connectors/tasks, and pending assignments.
///
/// Corresponds to `WorkerCoordinator` in Java (simplified interface for assignment).
pub trait WorkerCoordinatorState {
    /// Returns the current configuration offset.
    fn config_offset(&self) -> i64;

    /// Returns the member ID of this coordinator.
    fn member_id(&self) -> &str;

    /// Returns whether this coordinator is the leader.
    fn is_leader(&self) -> bool;

    /// Returns the URL of this worker.
    fn url(&self) -> &str;

    /// Returns the assigned connectors for this worker.
    fn assigned_connectors(&self) -> &[String];

    /// Returns the assigned tasks for this worker.
    fn assigned_tasks(&self) -> &[crate::distributed::connect_protocol::ConnectorTaskId];
}

/// An assignor that computes a distribution of connectors and tasks among the workers
/// of the group that performs rebalancing.
///
/// Implementations of this trait compute assignments based on member metadata and
/// the information stored in the worker coordinator.
///
/// Corresponds to `org.apache.kafka.connect.runtime.distributed.ConnectAssignor` in Java.
pub trait ConnectAssignor {
    /// Based on the member metadata and the information stored in the worker coordinator,
    /// this method computes an assignment of connectors and tasks among the members
    /// of the worker group.
    ///
    /// # Arguments
    /// * `leader_id` - The leader of the group
    /// * `protocol` - The protocol type (EAGER, COMPATIBLE, or SESSIONED)
    /// * `all_member_metadata` - The metadata of all the active workers of the group
    /// * `coordinator` - The worker coordinator that runs this assignor
    ///
    /// # Returns
    /// A map from member ID to serialized assignment bytes
    fn perform_assignment(
        &self,
        leader_id: &str,
        protocol: ConnectProtocolCompatibility,
        all_member_metadata: &[JoinGroupResponseMember],
        coordinator: &dyn WorkerCoordinatorState,
    ) -> HashMap<String, Vec<u8>>;

    /// Returns the protocol version string for this assignor.
    ///
    /// This is used to identify the assignor's protocol version during group joining.
    fn version(&self) -> String;

    /// Returns the protocol type this assignor supports.
    ///
    /// This indicates which protocol compatibility mode this assignor works with.
    fn protocol(&self) -> ConnectProtocolCompatibility;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_group_response_member_creation() {
        let member = JoinGroupResponseMember::new("member-1".to_string(), vec![1, 2, 3]);
        assert_eq!(member.member_id(), "member-1");
        assert_eq!(member.metadata(), &[1, 2, 3]);
        assert!(member.group_instance_id().is_none());
    }

    #[test]
    fn test_join_group_response_member_with_group_instance_id() {
        let member = JoinGroupResponseMember::with_group_instance_id(
            "member-1".to_string(),
            Some("instance-1".to_string()),
            vec![1, 2, 3],
        );
        assert_eq!(member.member_id(), "member-1");
        assert_eq!(member.group_instance_id(), Some(&"instance-1".to_string()));
        assert_eq!(member.metadata(), &[1, 2, 3]);
    }
}
