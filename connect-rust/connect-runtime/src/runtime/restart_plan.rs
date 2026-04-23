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

//! RestartPlan - Plan for restarting a connector and its tasks.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.RestartPlan` in Java.

use super::restart_request::RestartRequest;
use common_trait::herder::{ConnectorState, ConnectorStateInfo, ConnectorTaskId, TaskStateInfo};

/// RestartPlan - An immutable restart plan per connector.
///
/// Created from a RestartRequest and current connector status.
/// Contains the restart request and the resulting connector state info
/// where items to restart have their state set to RESTARTING.
///
/// Corresponds to `org.apache.kafka.connect.runtime.RestartPlan` in Java.
#[derive(Debug, Clone)]
pub struct RestartPlan {
    /// The original restart request.
    request: RestartRequest,
    /// The connector state info showing what will be restarted (with RESTARTING states).
    state_info: ConnectorStateInfo,
    /// The task IDs to stop and restart (may be empty).
    ids_to_restart: Vec<ConnectorTaskId>,
}

impl RestartPlan {
    /// Create a new RestartPlan.
    ///
    /// The connector state info should have RESTARTING state set for
    /// items that will be restarted based on the restart request.
    ///
    /// # Arguments
    /// * `request` - The restart request; may not be null
    /// * `state_info` - The current state info for the connector; may not be null
    pub fn new(request: RestartRequest, state_info: ConnectorStateInfo) -> Self {
        // Collect the task IDs to stop and restart (may be none)
        let ids_to_restart: Vec<ConnectorTaskId> = state_info
            .tasks
            .iter()
            .filter(|task| is_restarting(&task.state))
            .map(|task_state| {
                ConnectorTaskId::new(request.connector_name().to_string(), task_state.id.task)
            })
            .collect();
        RestartPlan {
            request,
            state_info,
            ids_to_restart,
        }
    }

    /// Get the connector name.
    pub fn connector_name(&self) -> &str {
        self.request.connector_name()
    }

    /// Get the original RestartRequest.
    pub fn restart_request(&self) -> &RestartRequest {
        &self.request
    }

    /// Get the ConnectorStateInfo that reflects the current state of the connector
    /// except with the status set to RESTARTING for the connector and any tasks
    /// that are to be restarted, based upon the restart request.
    pub fn restart_connector_state_info(&self) -> &ConnectorStateInfo {
        &self.state_info
    }

    /// Get the immutable collection of ConnectorTaskId for all tasks to be restarted.
    pub fn task_ids_to_restart(&self) -> &[ConnectorTaskId] {
        &self.ids_to_restart
    }

    /// Determine whether the connector instance is to be restarted.
    pub fn should_restart_connector(&self) -> bool {
        is_restarting(&self.state_info.connector)
    }

    /// Determine whether at least one task instance is to be restarted.
    pub fn should_restart_tasks(&self) -> bool {
        !self.ids_to_restart.is_empty()
    }

    /// Get the number of connector tasks that are to be restarted.
    pub fn restart_task_count(&self) -> usize {
        self.ids_to_restart.len()
    }

    /// Get the total number of tasks in the connector.
    pub fn total_task_count(&self) -> usize {
        self.state_info.tasks.len()
    }
}

/// Helper function to check if a state is RESTARTING.
fn is_restarting(state: &ConnectorState) -> bool {
    *state == ConnectorState::Restarting
}

impl std::fmt::Display for RestartPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.should_restart_connector() {
            write!(
                f,
                "plan to restart connector and {} of {} tasks for {}",
                self.restart_task_count(),
                self.total_task_count(),
                self.request
            )
        } else {
            write!(
                f,
                "plan to restart {} of {} tasks for {}",
                self.restart_task_count(),
                self.total_task_count(),
                self.request
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_restart_plan_new() {
        let request = RestartRequest::new("test".to_string(), false, true);
        let state_info = ConnectorStateInfo {
            name: "test".to_string(),
            connector: ConnectorState::Restarting,
            tasks: vec![],
        };
        let plan = RestartPlan::new(request, state_info);

        assert_eq!(plan.connector_name(), "test");
        assert_eq!(plan.total_task_count(), 0);
        assert!(plan.should_restart_connector());
        assert!(!plan.should_restart_tasks());
    }

    #[test]
    fn test_restart_plan_no_connector_restart() {
        let request = RestartRequest::new("test".to_string(), true, true);
        let state_info = ConnectorStateInfo {
            name: "test".to_string(),
            connector: ConnectorState::Running, // Not restarting
            tasks: vec![],
        };
        let plan = RestartPlan::new(request, state_info);

        assert!(!plan.should_restart_connector());
        assert!(!plan.should_restart_tasks());
    }

    #[test]
    fn test_restart_plan_with_tasks() {
        let request = RestartRequest::new("test".to_string(), false, true);
        let state_info = ConnectorStateInfo {
            name: "test".to_string(),
            connector: ConnectorState::Restarting,
            tasks: vec![
                TaskStateInfo {
                    id: ConnectorTaskId::new("test".to_string(), 0),
                    state: ConnectorState::Restarting,
                    worker_id: "worker1".to_string(),
                    trace: None,
                },
                TaskStateInfo {
                    id: ConnectorTaskId::new("test".to_string(), 1),
                    state: ConnectorState::Running, // Not restarting
                    worker_id: "worker1".to_string(),
                    trace: None,
                },
                TaskStateInfo {
                    id: ConnectorTaskId::new("test".to_string(), 2),
                    state: ConnectorState::Restarting,
                    worker_id: "worker1".to_string(),
                    trace: Some("error".to_string()),
                },
            ],
        };
        let plan = RestartPlan::new(request, state_info);

        assert_eq!(plan.total_task_count(), 3);
        assert_eq!(plan.restart_task_count(), 2);
        assert!(plan.should_restart_connector());
        assert!(plan.should_restart_tasks());

        let ids = plan.task_ids_to_restart();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0].task, 0);
        assert_eq!(ids[1].task, 2);
    }

    #[test]
    fn test_restart_plan_display_with_connector() {
        let request = RestartRequest::new("test".to_string(), false, true);
        let state_info = ConnectorStateInfo {
            name: "test".to_string(),
            connector: ConnectorState::Restarting,
            tasks: vec![TaskStateInfo {
                id: ConnectorTaskId::new("test".to_string(), 0),
                state: ConnectorState::Restarting,
                worker_id: "worker1".to_string(),
                trace: None,
            }],
        };
        let plan = RestartPlan::new(request, state_info);

        assert_eq!(
            plan.to_string(),
            "plan to restart connector and 1 of 1 tasks for restart request for {connectorName='test', onlyFailed=false, includeTasks=true}"
        );
    }

    #[test]
    fn test_restart_plan_display_without_connector() {
        let request = RestartRequest::new("test".to_string(), true, true);
        let state_info = ConnectorStateInfo {
            name: "test".to_string(),
            connector: ConnectorState::Running, // Not restarting
            tasks: vec![TaskStateInfo {
                id: ConnectorTaskId::new("test".to_string(), 0),
                state: ConnectorState::Restarting,
                worker_id: "worker1".to_string(),
                trace: None,
            }],
        };
        let plan = RestartPlan::new(request, state_info);

        assert_eq!(
            plan.to_string(),
            "plan to restart 1 of 1 tasks for restart request for {connectorName='test', onlyFailed=true, includeTasks=true}"
        );
    }
}
