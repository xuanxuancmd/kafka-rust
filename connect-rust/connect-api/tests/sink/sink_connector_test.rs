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

use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext};
use connect_api::sink::{SinkConnector, SinkTask, SinkTaskContext};
use std::collections::HashMap;

/// Mock sink connector for testing
struct MockSinkConnector {
    version: String,
}

impl MockSinkConnector {
    fn new() -> Self {
        MockSinkConnector {
            version: "1.0.0".to_string(),
        }
    }
}

impl Versioned for MockSinkConnector {
    fn version(&self) -> &str {
        &self.version
    }
}

impl Connector for MockSinkConnector {
    fn initialize(&mut self, _context: ConnectorContext) {}
    fn start(&mut self, _props: HashMap<String, String>) {}
    fn stop(&mut self) {}
    fn task_class(&self) -> &str {
        "MockSinkTask"
    }
    fn task_configs(&self, max_tasks: i32) -> Vec<HashMap<String, String>> {
        vec![HashMap::new(); max_tasks as usize]
    }
    fn config(&self) -> common_trait::config::ConfigDef {
        common_trait::config::ConfigDef::new()
    }
    fn validate(&self, _configs: HashMap<String, String>) -> common_trait::config::Config {
        common_trait::config::Config::new(Vec::new())
    }
}

impl SinkConnector for MockSinkConnector {
    fn task_configs_with_topics(
        &self,
        max_tasks: i32,
        topics: &[String],
    ) -> Vec<HashMap<String, String>> {
        let mut configs = Vec::new();
        for i in 0..max_tasks {
            let mut config = HashMap::new();
            config.insert("topics".to_string(), topics.join(","));
            configs.push(config);
        }
        configs
    }
}

#[test]
fn test_sink_connector_version() {
    let connector = MockSinkConnector::new();
    assert_eq!("1.0.0", connector.version());
}

#[test]
fn test_sink_connector_task_configs_with_topics() {
    let connector = MockSinkConnector::new();
    let topics = vec!["topic1".to_string(), "topic2".to_string()];
    let configs = connector.task_configs_with_topics(2, &topics);

    assert_eq!(2, configs.len());
    for config in configs {
        assert!(config.contains_key("topics"));
    }
}
