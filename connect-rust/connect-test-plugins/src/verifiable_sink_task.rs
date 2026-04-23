// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0

use common_trait::TopicPartition;
use connect_api::components::Versioned;
use connect_api::connector::Task;
use connect_api::errors::ConnectError;
use connect_api::sink::{ErrantRecordReporter, SinkRecord, SinkTask, SinkTaskContext};
use kafka_clients_mock::OffsetAndMetadata;
use std::collections::HashMap;

pub const NAME_CONFIG: &str = "name";
pub const ID_CONFIG: &str = "id";

#[derive(Debug, Clone)]
pub struct VerifiableSinkTask {
    name: Option<String>,
    id: i32,
}

impl VerifiableSinkTask {
    pub fn new() -> Self {
        VerifiableSinkTask { name: None, id: 0 }
    }
}

impl Default for VerifiableSinkTask {
    fn default() -> Self {
        Self::new()
    }
}

impl Task for VerifiableSinkTask {
    fn version(&self) -> &str {
        crate::verifiable_sink_connector::VerifiableSinkConnector::version()
    }
    fn start(&mut self, props: HashMap<String, String>) {
        self.name = props.get(NAME_CONFIG).cloned();
        self.id = props
            .get(ID_CONFIG)
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
    }
    fn stop(&mut self) {}
}

impl SinkTask for VerifiableSinkTask {
    fn initialize(&mut self, _: impl SinkTaskContext) {}
    fn put(&mut self, _: Vec<SinkRecord>) -> Result<(), ConnectError> {
        Ok(())
    }
    fn flush(&mut self, _: HashMap<TopicPartition, OffsetAndMetadata>) -> Result<(), ConnectError> {
        Ok(())
    }
    fn pre_commit(
        &mut self,
        offs: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, ConnectError> {
        self.flush(offs.clone())?;
        Ok(offs)
    }
    fn open(&mut self, _: Vec<TopicPartition>) -> Result<(), ConnectError> {
        Ok(())
    }
    fn close(&mut self, _: Vec<TopicPartition>) -> Result<(), ConnectError> {
        Ok(())
    }
    fn errant_record_reporter(&self) -> Option<&dyn ErrantRecordReporter> {
        None
    }
}
