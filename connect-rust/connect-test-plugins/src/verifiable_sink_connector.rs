// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0

use common_trait::config::{Config, ConfigDef, ConfigValueEntry};
use common_trait::TopicPartition;
use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext};
use connect_api::errors::ConnectError;
use connect_api::sink::SinkConnector;
use std::collections::HashMap;

pub struct VerifiableSinkConfigDef;
impl ConfigDef for VerifiableSinkConfigDef {
    fn config_def(&self) -> HashMap<String, ConfigValueEntry> {
        HashMap::new()
    }
}

#[derive(Debug)]
pub struct VerifiableSinkConnector {
    config: HashMap<String, String>,
}

impl VerifiableSinkConnector {
    pub fn new() -> Self {
        VerifiableSinkConnector {
            config: HashMap::new(),
        }
    }
}

impl Default for VerifiableSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}
impl Versioned for VerifiableSinkConnector {
    fn version() -> &'static str {
        "0.1.0"
    }
}

static VERIFIABLE_SINK_CONNECTOR_CONFIG_DEF: VerifiableSinkConfigDef = VerifiableSinkConfigDef;

impl Connector for VerifiableSinkConnector {
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
        "VerifiableSinkTask"
    }
    fn task_configs(&self, max: i32) -> Result<Vec<HashMap<String, String>>, ConnectError> {
        Ok((0..max)
            .map(|i| {
                let mut props = self.config.clone();
                props.insert(
                    crate::verifiable_sink_task::ID_CONFIG.to_string(),
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
        &VERIFIABLE_SINK_CONNECTOR_CONFIG_DEF
    }
}

impl SinkConnector for VerifiableSinkConnector {
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
