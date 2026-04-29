// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by official law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Mock connector for testing connector behaviors.
//!
//! This corresponds to `org.apache.kafka.connect.tools.MockConnector` in Java.

use common_trait::config::{Config, ConfigDef, ConfigValueEntry};
use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext};
use connect_api::errors::ConnectError;
use std::collections::HashMap;

pub const MOCK_MODE_KEY: &str = "mock_mode";
pub const DELAY_MS_KEY: &str = "delay_ms";
pub const CONNECTOR_FAILURE: &str = "connector-failure";
pub const TASK_FAILURE: &str = "task-failure";
pub const DEFAULT_FAILURE_DELAY_MS: u64 = 15000;

pub struct MockConfigDef;
impl ConfigDef for MockConfigDef {
    fn config_def(&self) -> HashMap<String, ConfigValueEntry> {
        HashMap::new()
    }
}

pub struct MockConnector {
    config: HashMap<String, String>,
    context: Option<Box<dyn ConnectorContext>>,
}

impl std::fmt::Debug for MockConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockConnector")
            .field("config", &self.config)
            .field(
                "context",
                &self.context.as_ref().map(|_| "ConnectorContext"),
            )
            .finish()
    }
}

impl MockConnector {
    pub fn new() -> Self {
        MockConnector {
            config: HashMap::new(),
            context: None,
        }
    }
}

impl Default for MockConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Versioned for MockConnector {
    fn version() -> &'static str {
        "0.1.0"
    }
}

static MOCK_CONNECTOR_CONFIG_DEF: MockConfigDef = MockConfigDef;

impl Connector for MockConnector {
    fn context(&self) -> &dyn ConnectorContext {
        self.context
            .as_ref()
            .map(|c| c.as_ref())
            .unwrap_or_else(|| panic!("context not initialized"))
    }
    fn initialize(&mut self, context: Box<dyn ConnectorContext>) {
        self.context = Some(context);
    }
    fn initialize_with_task_configs(
        &mut self,
        context: Box<dyn ConnectorContext>,
        _: Vec<HashMap<String, String>>,
    ) {
        self.context = Some(context);
    }
    fn start(&mut self, props: HashMap<String, String>) {
        self.config = props;
    }
    fn stop(&mut self) {
        self.config.clear();
    }
    fn task_class(&self) -> &'static str {
        panic!("use MockSourceConnector or MockSinkConnector");
    }
    fn task_configs(&self, max_tasks: i32) -> Result<Vec<HashMap<String, String>>, ConnectError> {
        Ok((0..max_tasks).map(|_| self.config.clone()).collect())
    }
    fn validate(&self, _: HashMap<String, String>) -> Config {
        Config::new(vec![])
    }
    fn config(&self) -> &'static dyn ConfigDef {
        &MOCK_CONNECTOR_CONFIG_DEF
    }
}
