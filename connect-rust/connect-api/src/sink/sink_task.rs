// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::connector::Task;
use crate::errors::ConnectError;
use crate::sink::{ErrantRecordReporter, SinkRecord, SinkTaskContext};
use common_trait::TopicPartition;
use kafka_clients_mock::OffsetAndMetadata;
use std::collections::HashMap;

/// The configuration key that provides the list of topics that are inputs for this SinkTask.
pub const TOPICS_CONFIG: &str = "topics";

/// The configuration key that provides a regex specifying which topics to include as inputs for this SinkTask.
pub const TOPICS_REGEX_CONFIG: &str = "topics.regex";

/// SinkTask trait for sink tasks.
///
/// This corresponds to `org.apache.kafka.connect.sink.SinkTask` in Java.
pub trait SinkTask: Task {
    /// Initializes this task with the given context.
    fn initialize(&mut self, context: impl SinkTaskContext);

    /// Puts the given records.
    fn put(&mut self, records: Vec<SinkRecord>) -> Result<(), ConnectError>;

    /// Flushes the records. Default implementation does nothing.
    fn flush(
        &mut self,
        _offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), ConnectError> {
        Ok(())
    }

    /// Pre-commits the offsets. Default implementation calls flush and returns offsets.
    fn pre_commit(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, ConnectError> {
        self.flush(offsets.clone())?;
        Ok(offsets)
    }

    /// Opens the given partitions. Default implementation does nothing.
    fn open(&mut self, _partitions: Vec<TopicPartition>) -> Result<(), ConnectError> {
        Ok(())
    }

    /// Closes the given partitions. Default implementation does nothing.
    fn close(&mut self, _partitions: Vec<TopicPartition>) -> Result<(), ConnectError> {
        Ok(())
    }

    /// Returns the errant record reporter.
    fn errant_record_reporter(&self) -> Option<&dyn ErrantRecordReporter>;
}
