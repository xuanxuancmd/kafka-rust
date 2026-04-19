// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_trait::config::{Config, ConfigDef, ConfigValue};
use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext, Task};
use connect_api::data::{Schema, SchemaBuilder};
use std::collections::HashMap;

/// Mock connector for testing
struct MockConnector {
    version: String,
}

impl MockConnector {
    fn new() -> Self {
        MockConnector {
            version: "1.0.0".to_string(),
        }
    }
}

impl Versioned for MockConnector {
    fn version(&self) -> &str {
        &self.version
    }
}

impl Connector for MockConnector {
    fn initialize(&mut self, _context: ConnectorContext) {
        // Initialize the connector
    }

    fn start(&mut self, _props: HashMap<String, String>) {
        // Start the connector
    }

    fn stop(&mut self) {
        // Stop the connector
    }

    fn task_class(&self) -> &str {
        "MockTask"
    }

    fn task_configs(&self, max_tasks: i32) -> Vec<HashMap<String, String>> {
        let mut configs = Vec::new();
        for _ in 0..max_tasks {
            configs.push(HashMap::new());
        }
        configs
    }

    fn config(&self) -> ConfigDef {
        ConfigDef::new()
    }

    fn validate(&self, _configs: HashMap<String, String>) -> Config {
        Config::new(Vec::new())
    }
}

#[test]
fn test_connector_version() {
    let connector = MockConnector::new();
    assert_eq!("1.0.0", connector.version());
}

#[test]
fn test_connector_task_configs() {
    let connector = MockConnector::new();
    let configs = connector.task_configs(3);
    assert_eq!(3, configs.len());
}

#[test]
fn test_connector_start_stop() {
    let mut connector = MockConnector::new();
    let props = HashMap::new();

    connector.start(props);
    connector.stop();
}

#[test]
fn test_connector_config() {
    let connector = MockConnector::new();
    let config_def = connector.config();
    // ConfigDef should be empty for mock
}
