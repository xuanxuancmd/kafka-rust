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
use connect_api::source::{SourceConnector, SourceTask, SourceTaskContext};
use std::collections::HashMap;

/// Mock source connector for testing
struct MockSourceConnector {
    version: String,
}

impl MockSourceConnector {
    fn new() -> Self {
        MockSourceConnector {
            version: "1.0.0".to_string(),
        }
    }
}

impl Versioned for MockSourceConnector {
    fn version(&self) -> &str {
        &self.version
    }
}

impl Connector for MockSourceConnector {
    fn initialize(&mut self, _context: ConnectorContext) {}
    fn start(&mut self, _props: HashMap<String, String>) {}
    fn stop(&mut self) {}
    fn task_class(&self) -> &str {
        "MockSourceTask"
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

impl SourceConnector for MockSourceConnector {
    fn exactly_once_support(&self) -> connect_api::source::ExactlyOnceSupport {
        connect_api::source::ExactlyOnceSupport::Unsupported
    }

    fn transaction_boundaries(&self) -> connect_api::source::ConnectorTransactionBoundaries {
        connect_api::source::ConnectorTransactionBoundaries::Unsupported
    }
}

#[test]
fn test_source_connector_version() {
    let connector = MockSourceConnector::new();
    assert_eq!("1.0.0", connector.version());
}

#[test]
fn test_source_connector_exactly_once_support() {
    let connector = MockSourceConnector::new();
    assert_eq!(
        connect_api::source::ExactlyOnceSupport::Unsupported,
        connector.exactly_once_support()
    );
}

#[test]
fn test_source_connector_transaction_boundaries() {
    let connector = MockSourceConnector::new();
    assert_eq!(
        connect_api::source::ConnectorTransactionBoundaries::Unsupported,
        connector.transaction_boundaries()
    );
}
