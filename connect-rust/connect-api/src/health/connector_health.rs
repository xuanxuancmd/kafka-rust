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

use crate::health::{AbstractState, ConnectorState, ConnectorType, TaskState};

/// ConnectorHealth for connector health.
pub struct ConnectorHealth {
    name: String,
    connector_type: ConnectorType,
    connector_state: ConnectorState,
    tasks: std::collections::HashMap<i32, TaskState>,
}

impl ConnectorHealth {
    pub fn new(
        name: impl Into<String>,
        connector_type: ConnectorType,
        connector_state: ConnectorState,
    ) -> Self {
        ConnectorHealth {
            name: name.into(),
            connector_type,
            connector_state,
            tasks: std::collections::HashMap::new(),
        }
    }

    pub fn add_task(&mut self, task_id: i32, state: TaskState) {
        self.tasks.insert(task_id, state);
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn connector_type(&self) -> ConnectorType {
        self.connector_type
    }

    pub fn connector_state(&self) -> &ConnectorState {
        &self.connector_state
    }

    pub fn tasks(&self) -> &std::collections::HashMap<i32, TaskState> {
        &self.tasks
    }
}

impl AbstractState for ConnectorHealth {
    fn state(&self) -> &str {
        self.connector_state.state()
    }

    fn is_healthy(&self) -> bool {
        self.connector_state.is_healthy() && self.tasks.values().all(|t| t.is_healthy())
    }
}
