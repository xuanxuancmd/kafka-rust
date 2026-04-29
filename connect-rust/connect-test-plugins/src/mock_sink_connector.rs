// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0

use crate::mock_connector::MockConnector;
use common_trait::config::{Config, ConfigDef, ConfigValueEntry};
use common_trait::TopicPartition;
use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext};
use connect_api::errors::ConnectError;
use connect_api::sink::SinkConnector;
use std::collections::HashMap;

pub struct MockSinkConfigDef;
impl ConfigDef for MockSinkConfigDef {
    fn config_def(&self) -> HashMap<String, ConfigValueEntry> {
        HashMap::new()
    }
}

#[derive(Debug)]
pub struct MockSinkConnector {
    delegate: MockConnector,
}

impl MockSinkConnector {
    pub fn new() -> Self {
        MockSinkConnector {
            delegate: MockConnector::new(),
        }
    }
}

impl Default for MockSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}
impl Versioned for MockSinkConnector {
    fn version() -> &'static str {
        MockConnector::version()
    }
}

static MOCK_SINK_CONNECTOR_CONFIG_DEF: MockSinkConfigDef = MockSinkConfigDef;

impl Connector for MockSinkConnector {
    fn context(&self) -> &dyn ConnectorContext {
        self.delegate.context()
    }
    fn initialize(&mut self, ctx: Box<dyn ConnectorContext>) {
        self.delegate.initialize(ctx);
    }
    fn initialize_with_task_configs(
        &mut self,
        ctx: Box<dyn ConnectorContext>,
        cfgs: Vec<HashMap<String, String>>,
    ) {
        self.delegate.initialize_with_task_configs(ctx, cfgs);
    }
    fn start(&mut self, props: HashMap<String, String>) {
        self.delegate.start(props);
    }
    fn reconfigure(&mut self, props: HashMap<String, String>) {
        self.delegate.reconfigure(props);
    }
    fn stop(&mut self) {
        self.delegate.stop();
    }
    fn task_class(&self) -> &'static str {
        "MockSinkTask"
    }
    fn task_configs(&self, max: i32) -> Result<Vec<HashMap<String, String>>, ConnectError> {
        self.delegate.task_configs(max)
    }
    fn validate(&self, cfgs: HashMap<String, String>) -> Config {
        self.delegate.validate(cfgs)
    }
    fn config(&self) -> &'static dyn ConfigDef {
        &MOCK_SINK_CONNECTOR_CONFIG_DEF
    }
}

impl SinkConnector for MockSinkConnector {
    fn task_configs_for_topics(&self, _: &[TopicPartition]) -> Vec<HashMap<String, String>> {
        vec![]
    }
    fn alter_offsets(
        &self,
        _: HashMap<String, String>,
        _: HashMap<TopicPartition, Option<i64>>,
    ) -> Result<bool, ConnectError> {
        Ok(false)
    }
}
