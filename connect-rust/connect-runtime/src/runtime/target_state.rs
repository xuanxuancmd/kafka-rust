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

//! Target state for connectors in Kafka Connect.
//!
//! The target state of a connector is its desired state as indicated by the user
//! through interaction with the REST API. When a connector is first created, its
//! target state is "STARTED." This does not mean it has actually started, just that
//! the Connect framework will attempt to start it after its tasks have been assigned.
//!
//! After the connector has been paused, the target state will change to PAUSED,
//! and all the tasks will stop doing work. A target state of STOPPED is similar to
//! PAUSED, but is also accompanied by a full shutdown of the connector's tasks,
//! including deallocation of any Kafka clients, SMTs, and other resources brought
//! up for or by that task.
//!
//! Target states are persisted in the config topic, which is read by all of the
//! workers in the group. When a worker sees a new target state for a connector which
//! is running, it will transition any tasks which it owns (i.e. which have been
//! assigned to it by the leader) into the desired target state. Upon completion of
//! a task rebalance, the worker will start the task in the last known target state.
//!
//! Corresponds to Java: `org.apache.kafka.connect.runtime.TargetState`

use serde::{Deserialize, Serialize};

/// The target state of a connector.
///
/// Represents the desired state of a connector as indicated by the user
/// through interaction with the REST API.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.TargetState`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TargetState {
    /// The connector should be started.
    /// When a connector is first created, its target state is STARTED.
    /// This does not mean it has actually started, just that the Connect
    /// framework will attempt to start it after its tasks have been assigned.
    STARTED,

    /// The connector should be paused.
    /// After the connector has been paused, the target state will change to PAUSED,
    /// and all the tasks will stop doing work.
    PAUSED,

    /// The connector should be stopped.
    /// A target state of STOPPED is similar to PAUSED, but is also accompanied by
    /// a full shutdown of the connector's tasks, including deallocation of any
    /// Kafka clients, SMTs, and other resources brought up for or by that task.
    STOPPED,
}

impl Default for TargetState {
    fn default() -> Self {
        TargetState::STARTED
    }
}

impl std::fmt::Display for TargetState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetState::STARTED => write!(f, "STARTED"),
            TargetState::PAUSED => write!(f, "PAUSED"),
            TargetState::STOPPED => write!(f, "STOPPED"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_target_state_values() {
        assert_eq!(TargetState::STARTED.to_string(), "STARTED");
        assert_eq!(TargetState::PAUSED.to_string(), "PAUSED");
        assert_eq!(TargetState::STOPPED.to_string(), "STOPPED");
    }

    #[test]
    fn test_default_target_state() {
        assert_eq!(TargetState::default(), TargetState::STARTED);
    }

    #[test]
    fn test_target_state_equality() {
        assert_eq!(TargetState::STARTED, TargetState::STARTED);
        assert_ne!(TargetState::STARTED, TargetState::PAUSED);
        assert_ne!(TargetState::PAUSED, TargetState::STOPPED);
    }

    #[test]
    fn test_target_state_serialization() {
        let started = TargetState::STARTED;
        let serialized = serde_json::to_string(&started).unwrap();
        assert_eq!(serialized, "\"STARTED\"");

        let deserialized: TargetState = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, TargetState::STARTED);
    }
}
