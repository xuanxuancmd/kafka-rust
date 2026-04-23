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

//! Listener for rebalance events in the worker group.
//!
//! This trait defines callbacks that are invoked when a worker's assignment changes
//! during a rebalance operation. Implementers receive notifications when:
//! - A new assignment is created (on_assigned)
//! - A rebalance starts and connectors/tasks are revoked (on_revoked)
//! - A worker experiences a poll timeout expiry (on_poll_timeout_expiry)
//!
//! Corresponds to `org.apache.kafka.connect.runtime.distributed.WorkerRebalanceListener` in Java.

use crate::distributed::connect_protocol::ConnectorTaskId;
use crate::distributed::extended_assignment::ExtendedAssignment;

/// Listener for rebalance events in the worker group.
///
/// This trait provides callbacks for:
/// - Assignment notifications when joining the Connect worker group
/// - Revocation notifications when a rebalance operation starts
/// - Poll timeout expiry notifications
///
/// Implementers of this trait should handle these events to properly manage
/// connectors and tasks on the worker.
pub trait WorkerRebalanceListener {
    /// Invoked when a new assignment is created by joining the Connect worker group.
    ///
    /// This is invoked for both successful and unsuccessful assignments.
    /// The implementer should:
    /// - Start newly assigned connectors and tasks
    /// - Handle any assignment errors appropriately
    ///
    /// # Arguments
    /// * `assignment` - The new assignment containing connectors and tasks to run
    /// * `generation` - The generation ID of the consumer group
    fn on_assigned(&mut self, assignment: ExtendedAssignment, generation: i32);

    /// Invoked when a rebalance operation starts, revoking ownership for the set of
    /// connectors and tasks.
    ///
    /// Depending on the Connect protocol version, the collection of revoked connectors
    /// or tasks might refer to all or some of the connectors and tasks running on the worker.
    /// The implementer should:
    /// - Stop the revoked connectors and tasks
    /// - Clean up any resources associated with them
    ///
    /// # Arguments
    /// * `leader` - The leader worker ID that initiated the rebalance
    /// * `connectors` - Collection of connector IDs that are being revoked
    /// * `tasks` - Collection of task IDs that are being revoked
    fn on_revoked(&mut self, leader: String, connectors: Vec<String>, tasks: Vec<ConnectorTaskId>);

    /// Invoked when a worker experiences a poll timeout expiry.
    ///
    /// This indicates that the worker has lost contact with the group coordinator
    /// due to missing polls. The implementer should handle this situation
    /// appropriately, such as by attempting to rejoin the group.
    fn on_poll_timeout_expiry(&mut self);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock implementation for testing
    struct MockRebalanceListener {
        assigned_count: i32,
        revoked_count: i32,
        timeout_count: i32,
        last_assignment: Option<ExtendedAssignment>,
        last_generation: i32,
    }

    impl MockRebalanceListener {
        fn new() -> Self {
            MockRebalanceListener {
                assigned_count: 0,
                revoked_count: 0,
                timeout_count: 0,
                last_assignment: None,
                last_generation: 0,
            }
        }
    }

    impl WorkerRebalanceListener for MockRebalanceListener {
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

    #[test]
    fn test_on_assigned() {
        let mut listener = MockRebalanceListener::new();
        let assignment = ExtendedAssignment::empty();

        listener.on_assigned(assignment.clone(), 1);

        assert_eq!(listener.assigned_count, 1);
        assert_eq!(listener.last_generation, 1);
        assert!(listener.last_assignment.is_some());
    }

    #[test]
    fn test_on_revoked() {
        let mut listener = MockRebalanceListener::new();

        listener.on_revoked(
            "leader-1".to_string(),
            vec!["connector-a".to_string()],
            vec![ConnectorTaskId::new("connector-a".to_string(), 0)],
        );

        assert_eq!(listener.revoked_count, 1);
    }

    #[test]
    fn test_on_poll_timeout_expiry() {
        let mut listener = MockRebalanceListener::new();

        listener.on_poll_timeout_expiry();

        assert_eq!(listener.timeout_count, 1);
    }

    #[test]
    fn test_multiple_events() {
        let mut listener = MockRebalanceListener::new();

        // Simulate a rebalance cycle
        listener.on_revoked(
            "leader-1".to_string(),
            vec!["connector-a".to_string()],
            vec![ConnectorTaskId::new("connector-a".to_string(), 0)],
        );

        listener.on_assigned(ExtendedAssignment::empty(), 2);

        // Simulate timeout
        listener.on_poll_timeout_expiry();

        assert_eq!(listener.revoked_count, 1);
        assert_eq!(listener.assigned_count, 1);
        assert_eq!(listener.timeout_count, 1);
    }
}
