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

use crate::health::{AbstractState, ConnectorHealth};

/// ConnectClusterState for cluster health state.
pub struct ConnectClusterState {
    connectors: std::collections::HashMap<String, ConnectorHealth>,
}

impl ConnectClusterState {
    pub fn new() -> Self {
        ConnectClusterState {
            connectors: std::collections::HashMap::new(),
        }
    }

    pub fn add_connector(&mut self, name: impl Into<String>, health: ConnectorHealth) {
        self.connectors.insert(name.into(), health);
    }

    pub fn connectors(&self) -> &std::collections::HashMap<String, ConnectorHealth> {
        &self.connectors
    }

    pub fn is_healthy(&self) -> bool {
        self.connectors.values().all(|h| h.is_healthy())
    }
}

impl Default for ConnectClusterState {
    fn default() -> Self {
        Self::new()
    }
}
