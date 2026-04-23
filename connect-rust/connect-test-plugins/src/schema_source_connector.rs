// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0

use common_trait::config::{Config, ConfigDef, ConfigValueEntry};
use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext};
use connect_api::errors::ConnectError;
use connect_api::source::{ConnectorTransactionBoundaries, ExactlyOnceSupport, SourceConnector};
use serde_json::Value;
use std::collections::HashMap;

pub struct SchemaSourceConfigDef;
impl ConfigDef for SchemaSourceConfigDef {
    fn config_def(&self) -> HashMap<String, ConfigValueEntry> {
        HashMap::new()
    }
}

#[derive(Debug)]
pub struct SchemaSourceConnector {
    config: HashMap<String, String>,
}

impl SchemaSourceConnector {
    pub fn new() -> Self {
        SchemaSourceConnector {
            config: HashMap::new(),
        }
    }
}

impl Default for SchemaSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}
impl Versioned for SchemaSourceConnector {
    fn version() -> &'static str {
        "0.1.0"
    }
}

static SCHEMA_SOURCE_CONNECTOR_CONFIG_DEF: SchemaSourceConfigDef = SchemaSourceConfigDef;

impl Connector for SchemaSourceConnector {
    fn context(&self) -> &dyn ConnectorContext {
        panic!("context not initialized")
    }
    fn initialize(&mut self, _: Box<dyn ConnectorContext>) {}
    fn initialize_with_task_configs(
        &mut self,
        _: Box<dyn ConnectorContext>,
        _: Vec<HashMap<String, String>>,
    ) {
    }
    fn start(&mut self, props: HashMap<String, String>) {
        self.config = props;
    }
    fn stop(&mut self) {}
    fn task_class(&self) -> &'static str {
        "SchemaSourceTask"
    }
    fn task_configs(&self, max: i32) -> Result<Vec<HashMap<String, String>>, ConnectError> {
        Ok((0..max)
            .map(|i| {
                let mut props = self.config.clone();
                props.insert(
                    crate::schema_source_task::ID_CONFIG.to_string(),
                    i.to_string(),
                );
                props
            })
            .collect())
    }
    fn validate(&self, _: HashMap<String, String>) -> Config {
        Config::new(vec![])
    }
    fn config(&self) -> &'static dyn ConfigDef {
        &SCHEMA_SOURCE_CONNECTOR_CONFIG_DEF
    }
}

impl SourceConnector for SchemaSourceConnector {
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
