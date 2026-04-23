// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0

use connect_api::components::Versioned;
use connect_api::connector::Task;
use connect_api::errors::ConnectError;
use connect_api::source::{SourceRecord, SourceTask, SourceTaskContext};
use kafka_clients_mock::RecordMetadata;
use serde_json::Value;
use std::collections::HashMap;

pub const NAME_CONFIG: &str = "name";
pub const ID_CONFIG: &str = "id";
pub const TOPIC_CONFIG: &str = "topic";
pub const THROUGHPUT_CONFIG: &str = "throughput";
pub const COMPLETE_RECORD_DATA_CONFIG: &str = "complete.record.data";

#[derive(Debug, Clone)]
pub struct VerifiableSourceTask {
    name: Option<String>,
    id: i32,
    topic: Option<String>,
    seqno: u64,
}

impl VerifiableSourceTask {
    pub fn new() -> Self {
        VerifiableSourceTask {
            name: None,
            id: 0,
            topic: None,
            seqno: 0,
        }
    }
}

impl Default for VerifiableSourceTask {
    fn default() -> Self {
        Self::new()
    }
}

impl Task for VerifiableSourceTask {
    fn version(&self) -> &str {
        crate::verifiable_source_connector::VerifiableSourceConnector::version()
    }
    fn start(&mut self, props: HashMap<String, String>) {
        self.name = props.get(NAME_CONFIG).cloned();
        self.id = props
            .get(ID_CONFIG)
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
        self.topic = props.get(TOPIC_CONFIG).cloned();
    }
    fn stop(&mut self) {}
}

impl SourceTask for VerifiableSourceTask {
    fn initialize(&mut self, _: impl SourceTaskContext) {}
    fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError> {
        let topic = self.topic.clone().unwrap_or_else(|| "test".to_string());
        let mut sp = HashMap::new();
        sp.insert("id".to_string(), Value::Number(self.id.into()));
        let mut so = HashMap::new();
        so.insert("seqno".to_string(), Value::Number(self.seqno.into()));
        let r = SourceRecord::new(
            sp,
            so,
            topic,
            None,
            Some(Value::Number(self.seqno.into())),
            Value::Number(self.seqno.into()),
        );
        self.seqno += 1;
        Ok(vec![r])
    }
    fn commit(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }
    fn commit_record(&mut self, _: &SourceRecord, _: &RecordMetadata) -> Result<(), ConnectError> {
        Ok(())
    }
}
