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

//! RestartRequest - Request to restart a connector and optionally its tasks.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.RestartRequest` in Java.

use super::status::{ConnectorStatus, State, TaskStatus};

/// RestartRequest - Request to restart a connector and optionally its tasks.
///
/// Specifies which connector to restart and whether to include tasks.
/// Corresponds to `org.apache.kafka.connect.runtime.RestartRequest` in Java.
///
/// The natural order is based first upon the connector name and then requested
/// restart behaviors. If two requests have the same connector name, then the
/// requests are ordered based on the probable number of tasks/connector this
/// request is going to restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartRequest {
    /// Connector name.
    connector_name: String,
    /// Whether to restart only failed instances.
    only_failed: bool,
    /// Whether to include tasks in the restart.
    include_tasks: bool,
}

impl RestartRequest {
    /// Create a new RestartRequest.
    ///
    /// # Arguments
    /// * `connector_name` - The name of the connector; may not be null/empty
    /// * `only_failed` - true if only failed instances should be restarted
    /// * `include_tasks` - true if tasks should be restarted, or false if only the connector
    pub fn new(connector_name: String, only_failed: bool, include_tasks: bool) -> Self {
        assert!(
            !connector_name.is_empty(),
            "Connector name may not be empty"
        );
        RestartRequest {
            connector_name,
            only_failed,
            include_tasks,
        }
    }

    /// Get the connector name.
    /// Alias for connector_name() to match Java naming.
    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }

    /// Get the connector name (alias for connector_name).
    pub fn connector(&self) -> &str {
        &self.connector_name
    }

    /// Check if only failed instances should be restarted.
    pub fn only_failed(&self) -> bool {
        self.only_failed
    }

    /// Check if tasks should be included in restart.
    pub fn include_tasks(&self) -> bool {
        self.include_tasks
    }

    /// Determine whether the connector with the given status is to be restarted.
    ///
    /// # Arguments
    /// * `status` - The connector status; may not be null
    ///
    /// # Returns
    /// true if the connector is to be restarted, or false otherwise
    pub fn should_restart_connector(&self, status: &ConnectorStatus) -> bool {
        !self.only_failed || status.state() == State::Failed
    }

    /// Determine whether only the connector instance is to be restarted even if not failed.
    ///
    /// # Returns
    /// true if only the connector instance is to be restarted even if not failed
    pub fn force_restart_connector_only(&self) -> bool {
        !self.only_failed && !self.include_tasks
    }

    /// Determine whether the task instance with the given status is to be restarted.
    ///
    /// # Arguments
    /// * `status` - The task status; may not be null
    ///
    /// # Returns
    /// true if the task is to be restarted, or false otherwise
    pub fn should_restart_task(&self, status: &TaskStatus) -> bool {
        self.include_tasks && (!self.only_failed || status.state() == State::Failed)
    }

    /// Calculate an internal rank for the restart request based on the probable
    /// number of tasks/connector this request is going to restart.
    fn impact_rank(&self) -> i32 {
        if self.only_failed && !self.include_tasks {
            // restarts only failed connector so least impactful
            0
        } else if self.only_failed && self.include_tasks {
            // restarts only failed connector and tasks
            1
        } else if !self.only_failed && !self.include_tasks {
            // restart connector in any state but no tasks
            2
        } else {
            // onlyFailed==false && includeTasks==true
            // restarts both connector and tasks in any state so highest impact
            3
        }
    }
}

/// Implement natural ordering for RestartRequest.
/// The natural order is based first upon the connector name and then requested
/// restart behaviors.
impl Ord for RestartRequest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let result = self.connector_name.cmp(&other.connector_name);
        if result == std::cmp::Ordering::Equal {
            self.impact_rank().cmp(&other.impact_rank())
        } else {
            result
        }
    }
}

impl PartialOrd for RestartRequest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for RestartRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "restart request for {{connectorName='{}', onlyFailed={}, includeTasks={}}}",
            self.connector_name, self.only_failed, self.include_tasks
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_trait::storage::ConnectorTaskId;

    #[test]
    fn test_restart_request_new() {
        let request = RestartRequest::new("test-connector".to_string(), true, true);
        assert_eq!(request.connector_name(), "test-connector");
        assert_eq!(request.connector(), "test-connector");
        assert!(request.only_failed());
        assert!(request.include_tasks());
    }

    #[test]
    fn test_should_restart_connector_not_only_failed() {
        let request = RestartRequest::new("test".to_string(), false, true);
        let status = ConnectorStatus::running("test".to_string(), "worker1".to_string(), 1, None);
        assert!(request.should_restart_connector(&status));
    }

    #[test]
    fn test_should_restart_connector_only_failed_running() {
        let request = RestartRequest::new("test".to_string(), true, true);
        let status = ConnectorStatus::running("test".to_string(), "worker1".to_string(), 1, None);
        assert!(!request.should_restart_connector(&status));
    }

    #[test]
    fn test_should_restart_connector_only_failed_failed() {
        let request = RestartRequest::new("test".to_string(), true, true);
        let status = ConnectorStatus::failed(
            "test".to_string(),
            "worker1".to_string(),
            1,
            "error".to_string(),
            None,
        );
        assert!(request.should_restart_connector(&status));
    }

    #[test]
    fn test_force_restart_connector_only() {
        // force_restart_connector_only returns true only when !onlyFailed && !includeTasks
        let request = RestartRequest::new("test".to_string(), false, false);
        assert!(request.force_restart_connector_only());

        let request = RestartRequest::new("test".to_string(), true, false);
        assert!(!request.force_restart_connector_only());

        let request = RestartRequest::new("test".to_string(), false, true);
        assert!(!request.force_restart_connector_only());

        let request = RestartRequest::new("test".to_string(), true, true);
        assert!(!request.force_restart_connector_only());
    }

    #[test]
    fn test_should_restart_task_not_include_tasks() {
        let request = RestartRequest::new("test".to_string(), false, false);
        let task_id = ConnectorTaskId::new("test".to_string(), 0);
        let status = TaskStatus::running(task_id, "worker1".to_string(), 1, None);
        assert!(!request.should_restart_task(&status));
    }

    #[test]
    fn test_should_restart_task_include_tasks() {
        let request = RestartRequest::new("test".to_string(), false, true);
        let task_id = ConnectorTaskId::new("test".to_string(), 0);
        let status = TaskStatus::running(task_id, "worker1".to_string(), 1, None);
        assert!(request.should_restart_task(&status));
    }

    #[test]
    fn test_should_restart_task_only_failed() {
        let request = RestartRequest::new("test".to_string(), true, true);
        let task_id = ConnectorTaskId::new("test".to_string(), 0);
        let running_status = TaskStatus::running(task_id.clone(), "worker1".to_string(), 1, None);
        assert!(!request.should_restart_task(&running_status));

        let failed_status =
            TaskStatus::failed(task_id, "worker1".to_string(), 1, "error".to_string(), None);
        assert!(request.should_restart_task(&failed_status));
    }

    #[test]
    fn test_restart_request_ordering() {
        // Test ordering by connector name first
        let request1 = RestartRequest::new("abc".to_string(), true, true);
        let request2 = RestartRequest::new("xyz".to_string(), true, true);
        assert!(request1 < request2);

        // Test ordering by impact rank when connector names are equal
        // impact_rank: 0 (onlyFailed=true, includeTasks=false)
        let request_least = RestartRequest::new("test".to_string(), true, false);
        // impact_rank: 1 (onlyFailed=true, includeTasks=true)
        let request_medium1 = RestartRequest::new("test".to_string(), true, true);
        // impact_rank: 2 (onlyFailed=false, includeTasks=false)
        let request_medium2 = RestartRequest::new("test".to_string(), false, false);
        // impact_rank: 3 (onlyFailed=false, includeTasks=true)
        let request_highest = RestartRequest::new("test".to_string(), false, true);

        assert!(request_least < request_medium1);
        assert!(request_medium1 < request_medium2);
        assert!(request_medium2 < request_highest);
    }

    #[test]
    fn test_restart_request_display() {
        let request = RestartRequest::new("test".to_string(), true, false);
        assert_eq!(
            request.to_string(),
            "restart request for {connectorName='test', onlyFailed=true, includeTasks=false}"
        );
    }

    #[test]
    #[should_panic(expected = "Connector name may not be empty")]
    fn test_restart_request_empty_name() {
        RestartRequest::new("".to_string(), true, true);
    }
}
