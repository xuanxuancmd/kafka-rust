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
pub const NUM_MSGS_CONFIG: &str = "num.messages";
pub const THROUGHPUT_CONFIG: &str = "throughput";
pub const MULTIPLE_SCHEMA_CONFIG: &str = "multiple.schema";
pub const PARTITION_COUNT_CONFIG: &str = "partition.count";

#[derive(Debug, Clone)]
pub struct SchemaSourceTask {
    id: i32,
    topic: Option<String>,
    seqno: u64,
    count: u64,
    max_num_msgs: u64,
}

impl SchemaSourceTask {
    pub fn new() -> Self {
        SchemaSourceTask {
            id: 0,
            topic: None,
            seqno: 0,
            count: 0,
            max_num_msgs: 100,
        }
    }
}

impl Default for SchemaSourceTask {
    fn default() -> Self {
        Self::new()
    }
}

impl Task for SchemaSourceTask {
    fn version(&self) -> &str {
        crate::schema_source_connector::SchemaSourceConnector::version()
    }
    fn start(&mut self, props: HashMap<String, String>) {
        self.id = props
            .get(ID_CONFIG)
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
        self.topic = props.get(TOPIC_CONFIG).cloned();
        self.max_num_msgs = props
            .get(NUM_MSGS_CONFIG)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(100);
    }
    fn stop(&mut self) {}
}

impl SourceTask for SchemaSourceTask {
    fn initialize(&mut self, _: impl SourceTaskContext) {}
    fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError> {
        if self.count >= self.max_num_msgs {
            return Ok(vec![]);
        }
        let topic = self.topic.clone().unwrap_or_else(|| "test".to_string());
        let mut sp = HashMap::new();
        sp.insert("id".to_string(), Value::Number(self.id.into()));
        let mut so = HashMap::new();
        so.insert("seqno".to_string(), Value::Number(self.seqno.into()));
        let r = SourceRecord::new(
            sp,
            so,
            topic,
            Some(self.id),
            Some(Value::String("key".to_string())),
            Value::Number(self.seqno.into()),
        );
        self.seqno += 1;
        self.count += 1;
        Ok(vec![r])
    }
    fn commit(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }
    fn commit_record(&mut self, _: &SourceRecord, _: &RecordMetadata) -> Result<(), ConnectError> {
        Ok(())
    }
}
