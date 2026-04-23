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

//! Connector task ID for identifying tasks in a connector.
//!
//! Corresponds to `org.apache.kafka.connect.util.ConnectorTaskId` in Java.

use serde::{Deserialize, Serialize};

/// Unique identifier for a connector task.
///
/// A task is identified by its connector name and task ID number.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectorTaskId {
    /// The name of the connector.
    connector: String,
    /// The task ID number within the connector.
    task: i32,
}

impl ConnectorTaskId {
    /// Creates a new connector task ID.
    pub fn new(connector: String, task: i32) -> Self {
        ConnectorTaskId { connector, task }
    }

    /// Returns the connector name.
    pub fn connector(&self) -> &str {
        &self.connector
    }

    /// Returns the task ID number.
    pub fn task(&self) -> i32 {
        self.task
    }
}

impl std::fmt::Display for ConnectorTaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.connector, self.task)
    }
}
