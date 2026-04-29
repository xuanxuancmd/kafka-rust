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
use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext};
use connect_api::errors::ConnectError;
use connect_api::source::{ConnectorTransactionBoundaries, ExactlyOnceSupport, SourceConnector};
use serde_json::Value;
use std::collections::HashMap;

pub struct MockSourceConfigDef;
impl ConfigDef for MockSourceConfigDef {
    fn config_def(&self) -> HashMap<String, ConfigValueEntry> {
        HashMap::new()
    }
}

#[derive(Debug)]
pub struct MockSourceConnector {
    delegate: MockConnector,
}

impl MockSourceConnector {
    pub fn new() -> Self {
        MockSourceConnector {
            delegate: MockConnector::new(),
        }
    }
}

impl Default for MockSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}
impl Versioned for MockSourceConnector {
    fn version() -> &'static str {
        MockConnector::version()
    }
}

static MOCK_SOURCE_CONNECTOR_CONFIG_DEF: MockSourceConfigDef = MockSourceConfigDef;

impl Connector for MockSourceConnector {
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
        "MockSourceTask"
    }
    fn task_configs(&self, max: i32) -> Result<Vec<HashMap<String, String>>, ConnectError> {
        self.delegate.task_configs(max)
    }
    fn validate(&self, cfgs: HashMap<String, String>) -> Config {
        self.delegate.validate(cfgs)
    }
    fn config(&self) -> &'static dyn ConfigDef {
        &MOCK_SOURCE_CONNECTOR_CONFIG_DEF
    }
}

impl SourceConnector for MockSourceConnector {
    fn exactly_once_support(&self, _: HashMap<String, String>) -> ExactlyOnceSupport {
        ExactlyOnceSupport::Unsupported
    }
    fn can_define_transaction_boundaries(
        &self,
        _: HashMap<String, String>,
    ) -> ConnectorTransactionBoundaries {
        ConnectorTransactionBoundaries::CoordinatorDefined
    }
    fn alter_offsets(
        &self,
        _: HashMap<String, String>,
        _: HashMap<HashMap<String, Value>, HashMap<String, Value>>,
    ) -> Result<bool, ConnectError> {
        Ok(false)
    }
}
