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

//! Status listener interfaces for connectors and tasks.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.ConnectorStatus.Listener` and
//! `org.apache.kafka.connect.runtime.TaskStatus.Listener` in Java.

use crate::errors::ConnectError;
use crate::worker::ConnectorTaskId;

/// Status state enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusState {
    /// The connector/task is starting.
    Starting,
    /// The connector/task is running.
    Running,
    /// The connector/task is paused.
    Paused,
    /// The connector/task is stopping.
    Stopping,
    /// The connector/task has stopped.
    Stopped,
    /// The connector/task has failed.
    Failed,
    /// The connector/task is restarting.
    Restarting,
}

/// Listener for connector status transitions.
///
/// This is used to receive notifications about connector lifecycle events.
pub trait ConnectorStatusListener {
    /// Called when the connector starts.
    fn on_startup(&self, connector: &str);

    /// Called when the connector is running.
    fn on_running(&self, connector: &str);

    /// Called when the connector is paused.
    fn on_pause(&self, connector: &str);

    /// Called when the connector is stopping.
    fn on_shutdown(&self, connector: &str);

    /// Called when the connector has stopped.
    fn on_stopped(&self, connector: &str);

    /// Called when the connector fails.
    fn on_failure(&self, connector: &str, error: ConnectError);

    /// Called when the connector is restarting.
    fn on_restart(&self, connector: &str);
}

/// Listener for task status transitions.
///
/// This is used to receive notifications about task lifecycle events.
pub trait TaskStatusListener {
    /// Called when the task starts.
    fn on_startup(&self, id: &ConnectorTaskId);

    /// Called when the task is running.
    fn on_running(&self, id: &ConnectorTaskId);

    /// Called when the task is paused.
    fn on_pause(&self, id: &ConnectorTaskId);

    /// Called when the task is stopping.
    fn on_shutdown(&self, id: &ConnectorTaskId);

    /// Called when the task has stopped.
    fn on_stopped(&self, id: &ConnectorTaskId);

    /// Called when the task fails.
    fn on_failure(&self, id: &ConnectorTaskId, error: ConnectError);

    /// Called when the task is restarting.
    fn on_restart(&self, id: &ConnectorTaskId);
}
