// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0

use crate::mock_connector::{DEFAULT_FAILURE_DELAY_MS, DELAY_MS_KEY, MOCK_MODE_KEY, TASK_FAILURE};
use common_trait::TopicPartition;
use connect_api::components::Versioned;
use connect_api::connector::Task;
use connect_api::errors::ConnectError;
use connect_api::sink::{ErrantRecordReporter, SinkRecord, SinkTask, SinkTaskContext};
use kafka_clients_mock::OffsetAndMetadata;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct MockSinkTask {
    mock_mode: Option<String>,
    start_time_ms: Option<u64>,
    failure_delay_ms: u64,
}

impl MockSinkTask {
    pub fn new() -> Self {
        MockSinkTask {
            mock_mode: None,
            start_time_ms: None,
            failure_delay_ms: DEFAULT_FAILURE_DELAY_MS,
        }
    }
}

impl Default for MockSinkTask {
    fn default() -> Self {
        Self::new()
    }
}

impl Task for MockSinkTask {
    fn version(&self) -> &str {
        crate::mock_connector::MockConnector::version()
    }
    fn start(&mut self, props: HashMap<String, String>) {
        self.mock_mode = props.get(MOCK_MODE_KEY).cloned();
        if Some(TASK_FAILURE.to_string()) == self.mock_mode {
            self.start_time_ms = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            );
            self.failure_delay_ms = props
                .get(DELAY_MS_KEY)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(DEFAULT_FAILURE_DELAY_MS);
        }
    }
    fn stop(&mut self) {}
}

struct DummyReporter;
impl ErrantRecordReporter for DummyReporter {
    fn report(&mut self, _: &SinkRecord, _: ConnectError) -> Result<(), ConnectError> {
        Ok(())
    }
}

impl SinkTask for MockSinkTask {
    fn initialize(&mut self, _: impl SinkTaskContext) {}
    fn put(&mut self, _: Vec<SinkRecord>) -> Result<(), ConnectError> {
        if Some(TASK_FAILURE.to_string()) == self.mock_mode {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            if let Some(start) = self.start_time_ms {
                if now - start > self.failure_delay_ms {
                    return Err(ConnectError::general("MockSinkTask failure"));
                }
            }
        }
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
