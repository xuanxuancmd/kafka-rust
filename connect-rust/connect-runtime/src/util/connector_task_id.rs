/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use serde::{Deserialize, Serialize};

/// Unique ID for a single task. It includes a unique connector ID and a task ID that is unique within
/// the connector.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectorTaskId {
    /// Connector name
    pub connector: String,
    /// Task number
    pub task: i32,
}

impl ConnectorTaskId {
    /// Create a new connector task ID
    pub fn new(connector: String, task: i32) -> Self {
        Self { connector, task }
    }

    /// Get connector name
    pub fn connector(&self) -> &str {
        &self.connector
    }

    /// Get task number
    pub fn task(&self) -> i32 {
        self.task
    }
}

impl std::fmt::Display for ConnectorTaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.connector, self.task)
    }
}

impl std::cmp::Ord for ConnectorTaskId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.connector.cmp(&other.connector) {
            std::cmp::Ordering::Equal => self.task.cmp(&other.task),
            other => other,
        }
    }
}

impl std::cmp::PartialOrd for ConnectorTaskId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
