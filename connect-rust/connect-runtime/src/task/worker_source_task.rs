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

//! WorkerSourceTask - Source task execution in Kafka Connect.
//!
//! This module implements the WorkerSourceTask that polls records from
//! a SourceTask, converts them to ProducerRecords, and sends them to Kafka.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.WorkerSourceTask` in Java.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common_trait::errors::ConnectError;
use common_trait::worker::{ConnectorTaskId, TargetState};
use connect_api::connector::ConnectRecord;
use connect_api::source::SourceRecord;
use kafka_clients_mock::{MockKafkaProducer, ProducerRecord, RecordMetadata};

use super::{TaskConfig, TaskMetrics, ThreadSafeTaskStatusListener, WorkerTask};

/// Offset state for tracking committed offsets.
///
/// This corresponds to the offset tracking in WorkerSourceTask.
#[derive(Debug, Clone)]
pub struct OffsetState {
    /// The source partition.
    partition: HashMap<String, serde_json::Value>,
    /// The source offset.
    offset: HashMap<String, serde_json::Value>,
}

impl OffsetState {
    /// Creates a new OffsetState.
    pub fn new(
        partition: HashMap<String, serde_json::Value>,
        offset: HashMap<String, serde_json::Value>,
    ) -> Self {
        OffsetState { partition, offset }
    }

    /// Returns the partition.
    pub fn partition(&self) -> &HashMap<String, serde_json::Value> {
        &self.partition
    }

    /// Returns the offset.
    pub fn offset(&self) -> &HashMap<String, serde_json::Value> {
        &self.offset
    }
}

/// Submitted record tracking.
///
/// Tracks a record that has been submitted to the producer
/// and is awaiting acknowledgment.
#[derive(Debug)]
pub struct SubmittedRecord {
    /// The source record that was submitted.
    source_record: SourceRecord,
    /// The topic partition and offset metadata.
    metadata: Option<RecordMetadata>,
    /// Whether the record has been acknowledged.
    acknowledged: AtomicBool,
    /// Unique identifier for this record (topic + approximate offset).
    identifier: String,
}

impl SubmittedRecord {
    /// Creates a new SubmittedRecord.
    pub fn new(source_record: SourceRecord) -> Self {
        let identifier = format!(
            "{}:{}:{}",
            source_record.topic(),
            source_record.kafka_partition().unwrap_or(-1),
            source_record.source_offset().len()
        );
        SubmittedRecord {
            source_record,
            metadata: None,
            acknowledged: AtomicBool::new(false),
            identifier,
        }
    }

    /// Creates a SubmittedRecord with metadata.
    pub fn with_metadata(source_record: SourceRecord, metadata: RecordMetadata) -> Self {
        let identifier = format!(
            "{}:{}:{}",
            source_record.topic(),
            metadata.partition(),
            metadata.offset()
        );
        SubmittedRecord {
            source_record,
            metadata: Some(metadata),
            acknowledged: AtomicBool::new(false),
            identifier,
        }
    }

    /// Returns the source record.
    pub fn source_record(&self) -> &SourceRecord {
        &self.source_record
    }

    /// Returns the unique identifier.
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Returns the metadata if available.
    pub fn metadata(&self) -> Option<&RecordMetadata> {
        self.metadata.as_ref()
    }

    /// Marks the record as acknowledged.
    pub fn acknowledge(&self) {
        self.acknowledged.store(true, Ordering::SeqCst);
    }

    /// Returns whether the record is acknowledged.
    pub fn is_acknowledged(&self) -> bool {
        self.acknowledged.load(Ordering::SeqCst)
    }

    /// Returns the committable offset state if acknowledged.
    pub fn committable_offset(&self) -> Option<OffsetState> {
        if self.is_acknowledged() {
            Some(OffsetState::new(
                self.source_record.source_partition().clone(),
                self.source_record.source_offset().clone(),
            ))
        } else {
            None
        }
    }
}

/// Submitted records collection.
///
/// Tracks all records submitted to the producer.
pub struct SubmittedRecords {
    /// List of submitted records awaiting acknowledgment.
    pending: Mutex<Vec<Arc<SubmittedRecord>>>,
    /// List of acknowledged records ready for commit.
    committable: Mutex<Vec<Arc<SubmittedRecord>>>,
}

impl SubmittedRecords {
    /// Creates a new SubmittedRecords.
    pub fn new() -> Self {
        SubmittedRecords {
            pending: Mutex::new(Vec::new()),
            committable: Mutex::new(Vec::new()),
        }
    }

    /// Adds a new submitted record.
    pub fn submit(&self, record: Arc<SubmittedRecord>) {
        self.pending.lock().unwrap().push(record);
    }

    /// Marks a record as acknowledged by identifier and moves it to committable.
    pub fn acknowledge_by_id(&self, identifier: &str) {
        let mut pending = self.pending.lock().unwrap();
        let mut committable = self.committable.lock().unwrap();

        // Move from pending to committable by matching identifier
        pending.retain(|r| {
            if r.identifier() == identifier {
                r.acknowledge();
                committable.push(r.clone());
                false
            } else {
                true
            }
        });
    }

    /// Marks all pending records as acknowledged.
    pub fn acknowledge_all(&self) {
        let mut pending = self.pending.lock().unwrap();
        let mut committable = self.committable.lock().unwrap();

        for r in pending.iter() {
            r.acknowledge();
        }
        committable.extend(pending.drain(..));
    }

    /// Returns committable offsets.
    pub fn committable_offsets(&self) -> Vec<OffsetState> {
        let committable = self.committable.lock().unwrap();
        committable
            .iter()
            .filter_map(|r| r.committable_offset())
            .collect()
    }

    /// Clears all committable records after commit.
    pub fn clear_committable(&self) {
        self.committable.lock().unwrap().clear();
    }

    /// Returns the number of pending records.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// Returns the number of committable records.
    pub fn committable_count(&self) -> usize {
        self.committable.lock().unwrap().len()
    }

    /// Clears all records.
    pub fn clear(&self) {
        self.pending.lock().unwrap().clear();
        self.committable.lock().unwrap().clear();
    }
}

impl Default for SubmittedRecords {
    fn default() -> Self {
        SubmittedRecords::new()
    }
}

/// Internal state for WorkerSourceTask.
struct WorkerSourceTaskState {
    /// Current target state.
    target_state: TargetState,
    /// Whether the task is stopping.
    stopping: bool,
    /// Whether the task has failed.
    failed: bool,
    /// Whether the task has started.
    started: bool,
    /// Whether the task is paused.
    paused: bool,
    /// Stored error message if task failed.
    error_message: Option<Arc<String>>,
    /// Producer send exception.
    producer_send_exception: Option<String>,
}

impl Default for WorkerSourceTaskState {
    fn default() -> Self {
        WorkerSourceTaskState {
            target_state: TargetState::Started,
            stopping: false,
            failed: false,
            started: false,
            paused: false,
            error_message: None,
            producer_send_exception: None,
        }
    }
}

/// Object-safe source task poller trait.
///
/// This trait is defined in connect-runtime to be object-safe
/// (no generic parameters), allowing dynamic dispatch for
/// source task poll operations.
pub trait ThreadSafeSourceTaskPoller: Send + Sync {
    /// Polls for new records.
    fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError>;

    /// Commits the current batch of records.
    fn commit(&mut self) -> Result<(), ConnectError>;

    /// Commits a single record.
    fn commit_record(
        &mut self,
        record: &SourceRecord,
        metadata: &RecordMetadata,
    ) -> Result<(), ConnectError>;
}

/// WorkerSourceTask - executes source tasks.
///
/// This struct implements the source task execution logic:
/// - Polls records from the underlying SourceTask
/// - Converts SourceRecords to ProducerRecords
/// - Sends ProducerRecords to Kafka via the producer
/// - Tracks submitted records and manages offset commits
///
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerSourceTask` in Java.
pub struct WorkerSourceTask {
    /// Task ID.
    id: ConnectorTaskId,
    /// Task version (obtained from connector's task class).
    version: String,
    /// Status listener.
    status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
    /// Task metrics.
    metrics: TaskMetrics,
    /// Internal state.
    state: Mutex<WorkerSourceTaskState>,
    /// Stop condvar.
    stop_condvar: Condvar,
    /// The underlying SourceTask poller.
    source_task_poller: Mutex<Option<Box<dyn ThreadSafeSourceTaskPoller>>>,
    /// The Kafka producer.
    producer: Option<Arc<MockKafkaProducer>>,
    /// Submitted records tracker.
    submitted_records: SubmittedRecords,
    /// Whether the task has been initialized.
    initialized: AtomicBool,
    /// Whether close was called.
    close_called: AtomicBool,
    /// Key converter (for serialization).
    key_converter: Option<Arc<dyn Converter>>,
    /// Value converter (for serialization).
    value_converter: Option<Arc<dyn Converter>>,
    /// Header converter (for serialization).
    header_converter: Option<Arc<dyn HeaderConverter>>,
}

/// Converter trait for converting connect data to bytes.
///
/// Simplified version of the Converter trait from common-trait
/// for use in WorkerSourceTask.
pub trait Converter: Send + Sync {
    /// Converts connect data to bytes.
    fn from_connect_data(&self, topic: &str, value: &serde_json::Value) -> Vec<u8>;
}

/// Header converter trait.
///
/// Simplified version for WorkerSourceTask.
pub trait HeaderConverter: Send + Sync {
    /// Converts headers to bytes map.
    fn from_connect_headers(
        &self,
        headers: &connect_api::header::ConnectHeaders,
    ) -> HashMap<String, Vec<u8>>;
}

impl WorkerSourceTask {
    /// Creates a new WorkerSourceTask.
    pub fn new(
        id: ConnectorTaskId,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
        producer: Option<Arc<MockKafkaProducer>>,
    ) -> Self {
        WorkerSourceTask {
            id,
            version: "1.0.0".to_string(),
            status_listener,
            metrics: TaskMetrics::new(),
            state: Mutex::new(WorkerSourceTaskState::default()),
            stop_condvar: Condvar::new(),
            source_task_poller: Mutex::new(None),
            producer,
            submitted_records: SubmittedRecords::new(),
            initialized: AtomicBool::new(false),
            close_called: AtomicBool::new(false),
            key_converter: None,
            value_converter: None,
            header_converter: None,
        }
    }

    /// Creates a WorkerSourceTask with a specific version.
    pub fn with_version(
        id: ConnectorTaskId,
        version: String,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
        producer: Option<Arc<MockKafkaProducer>>,
    ) -> Self {
        WorkerSourceTask {
            id,
            version,
            status_listener,
            metrics: TaskMetrics::new(),
            state: Mutex::new(WorkerSourceTaskState::default()),
            stop_condvar: Condvar::new(),
            source_task_poller: Mutex::new(None),
            producer,
            submitted_records: SubmittedRecords::new(),
            initialized: AtomicBool::new(false),
            close_called: AtomicBool::new(false),
            key_converter: None,
            value_converter: None,
            header_converter: None,
        }
    }

    /// Creates a WorkerSourceTask with converters.
    pub fn with_converters(
        id: ConnectorTaskId,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
        producer: Option<Arc<MockKafkaProducer>>,
        key_converter: Option<Arc<dyn Converter>>,
        value_converter: Option<Arc<dyn Converter>>,
        header_converter: Option<Arc<dyn HeaderConverter>>,
    ) -> Self {
        WorkerSourceTask {
            id,
            version: "1.0.0".to_string(),
            status_listener,
            metrics: TaskMetrics::new(),
            state: Mutex::new(WorkerSourceTaskState::default()),
            stop_condvar: Condvar::new(),
            source_task_poller: Mutex::new(None),
            producer,
            submitted_records: SubmittedRecords::new(),
            initialized: AtomicBool::new(false),
            close_called: AtomicBool::new(false),
            key_converter,
            value_converter,
            header_converter,
        }
    }

    /// Returns the task ID.
    pub fn id(&self) -> &ConnectorTaskId {
        &self.id
    }

    /// Returns the task version.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Sets the source task poller.
    pub fn set_source_task_poller(&self, poller: Box<dyn ThreadSafeSourceTaskPoller>) {
        *self.source_task_poller.lock().unwrap() = Some(poller);
    }

    /// Returns whether initialize was called.
    pub fn was_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    /// Returns whether close was called.
    pub fn was_closed(&self) -> bool {
        self.close_called.load(Ordering::SeqCst)
    }

    /// Returns the submitted records count.
    pub fn submitted_count(&self) -> usize {
        self.submitted_records.pending_count()
    }

    /// Returns the committable records count.
    pub fn committable_count(&self) -> usize {
        self.submitted_records.committable_count()
    }

    /// Returns the producer send exception if any.
    pub fn producer_send_exception(&self) -> Option<String> {
        self.state.lock().unwrap().producer_send_exception.clone()
    }

    /// Sets the producer send exception.
    pub fn set_producer_send_exception(&self, error: String) {
        self.state.lock().unwrap().producer_send_exception = Some(error);
    }

    /// Clears the producer send exception.
    pub fn clear_producer_send_exception(&self) {
        self.state.lock().unwrap().producer_send_exception = None;
    }

    /// Polls records from the underlying SourceTask.
    ///
    /// Corresponds to `poll()` in Java WorkerSourceTask.
    pub fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError> {
        // Check if task is stopping
        if self.is_stopping() {
            return Ok(Vec::new());
        }

        // Check if paused
        if self.is_paused() {
            return Ok(Vec::new());
        }

        // Get source task poller
        let mut poller_guard = self.source_task_poller.lock().unwrap();
        let poller = poller_guard.as_mut();

        if poller.is_none() {
            return Err(ConnectError::general("SourceTask not initialized"));
        }

        // Poll from source task
        poller.unwrap().poll()
    }

    /// Sends records to Kafka.
    ///
    /// Corresponds to `sendRecords()` in Java WorkerSourceTask.
    pub fn send_records(&mut self, records: Vec<SourceRecord>) -> Result<(), ConnectError> {
        if records.is_empty() {
            return Ok(());
        }

        // Convert and send each record
        let producer_records = self.convert_to_send_records(records)?;

        // Record batch size
        self.record_batch(producer_records.len());

        // Send each producer record
        for (source_record, producer_record) in producer_records {
            self.send_to_producer(source_record, producer_record)?;
        }

        Ok(())
    }

    /// Converts SourceRecords to ProducerRecords.
    ///
    /// Corresponds to `convertToSendRecords()` in Java.
    pub fn convert_to_send_records(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<(SourceRecord, ProducerRecord)>, ConnectError> {
        let mut result = Vec::new();

        for source_record in records {
            let producer_record = self.convert_record(&source_record)?;
            result.push((source_record, producer_record));
        }

        Ok(result)
    }

    /// Converts a single SourceRecord to ProducerRecord.
    fn convert_record(&self, source_record: &SourceRecord) -> Result<ProducerRecord, ConnectError> {
        // Convert key
        let key_bytes = if let Some(key) = source_record.key() {
            if let Some(converter) = &self.key_converter {
                converter.from_connect_data(source_record.topic(), key)
            } else {
                // Default: serialize JSON value to bytes
                serde_json::to_vec(key).unwrap_or_default()
            }
        } else {
            vec![]
        };

        // Convert value
        let value_bytes = {
            let value = source_record.value();
            if let Some(converter) = &self.value_converter {
                converter.from_connect_data(source_record.topic(), value)
            } else {
                // Default: serialize JSON value to bytes
                serde_json::to_vec(value).unwrap_or_default()
            }
        };

        // Convert headers
        let headers = if let Some(converter) = &self.header_converter {
            converter.from_connect_headers(source_record.headers())
        } else {
            HashMap::new()
        };

        // Create producer record
        let producer_record = ProducerRecord::with_all(
            source_record.topic(),
            source_record.kafka_partition(),
            Some(key_bytes),
            Some(value_bytes),
            source_record.timestamp(),
            headers,
        );

        Ok(producer_record)
    }

    /// Sends a ProducerRecord to the producer.
    ///
    /// Corresponds to `sendToProducer()` in Java.
    pub fn send_to_producer(
        &self,
        source_record: SourceRecord,
        producer_record: ProducerRecord,
    ) -> Result<(), ConnectError> {
        // Check for producer
        let producer = match &self.producer {
            Some(p) => p,
            None => return Err(ConnectError::general("Producer not configured")),
        };

        // Check for existing send exception
        if let Some(error) = self.producer_send_exception() {
            return Err(ConnectError::general(error));
        }

        // Create submitted record
        let submitted = Arc::new(SubmittedRecord::new(source_record));
        let identifier = submitted.identifier().to_string();

        // Track the submitted record
        self.submitted_records.submit(submitted.clone());

        // Send the record (no callback - synchronous handling)
        let result = producer.send(&producer_record);

        // Handle result synchronously
        match result {
            Ok(_metadata) => {
                // Mark as acknowledged on success
                self.submitted_records.acknowledge_by_id(&identifier);
                Ok(())
            }
            Err(e) => {
                let error_msg = e.to_string();
                self.set_producer_send_exception(error_msg.clone());
                Err(ConnectError::general(error_msg))
            }
        }
    }

    /// Commits a single record.
    ///
    /// Called when a SourceRecord has been successfully sent to Kafka.
    /// Corresponds to `commitRecord()` in Java WorkerSourceTask.
    pub fn commit_record(
        &self,
        record: &SourceRecord,
        metadata: &RecordMetadata,
    ) -> Result<(), ConnectError> {
        let mut poller_guard = self.source_task_poller.lock().unwrap();
        if let Some(poller) = poller_guard.as_mut() {
            poller.commit_record(record, metadata)?;
        }
        Ok(())
    }

    /// Commits a single source record.
    ///
    /// Called when a SourceRecord has been successfully sent to Kafka.
    /// This is an alias for commit_record() that explicitly indicates
    /// the record is a source record.
    /// Corresponds to `commitSourceRecord()` in Java WorkerSourceTask.
    pub fn commit_source_record(
        &self,
        record: &SourceRecord,
        metadata: &RecordMetadata,
    ) -> Result<(), ConnectError> {
        // Delegate to commit_record - same semantics
        self.commit_record(record, metadata)
    }

    /// Commits the source task.
    ///
    /// Triggers commit() on the underlying SourceTask.
    /// Corresponds to `commitSourceTask()` in Java.
    pub fn commit_source_task(&self) -> Result<(), ConnectError> {
        let mut poller_guard = self.source_task_poller.lock().unwrap();
        if let Some(poller) = poller_guard.as_mut() {
            poller.commit()?;
        }
        Ok(())
    }

    /// Commits offsets.
    ///
    /// Persists offsets of successfully processed records.
    /// Corresponds to `commitOffsets()` in Java WorkerSourceTask.
    pub fn commit_offsets_internal(&mut self) -> Result<(), ConnectError> {
        // Check for stopping
        if self.is_stopping() {
            return Ok(());
        }

        // Get committable offsets from submitted records
        let committable = self.submitted_records.committable_offsets();

        if committable.is_empty() {
            return Ok(());
        }

        // Commit to source task
        self.commit_source_task()?;

        // Clear committable records
        self.submitted_records.clear_committable();

        // Record commit metrics
        self.record_commit_success(5);

        Ok(())
    }

    /// Flushes the producer.
    pub fn flush_producer(&self) -> Result<(), ConnectError> {
        if let Some(producer) = &self.producer {
            producer
                .flush()
                .map_err(|e| ConnectError::general(e.to_string()))?;
        }
        Ok(())
    }

    /// Signals that the task has stopped.
    pub fn signal_stopped(&self) {
        self.stop_condvar.notify_all();
    }

    /// Called when producer send fails.
    pub fn producer_send_failed(&self, error: &str) {
        self.set_producer_send_exception(error.to_string());
    }
}

impl WorkerTask for WorkerSourceTask {
    fn id(&self) -> &ConnectorTaskId {
        &self.id
    }

    fn status_listener(&self) -> &dyn ThreadSafeTaskStatusListener {
        self.status_listener.as_ref()
    }

    fn metrics(&mut self) -> &mut TaskMetrics {
        &mut self.metrics
    }

    fn initialize(&mut self, _config: TaskConfig) -> Result<(), ConnectError> {
        self.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn do_start(&mut self) -> Result<(), ConnectError> {
        // Check producer
        if self.producer.is_none() {
            return Err(ConnectError::general(
                "Producer not configured for source task",
            ));
        }
        Ok(())
    }

    fn execute(&mut self) -> Result<(), ConnectError> {
        // Check for stopping
        if self.is_stopping() {
            return Ok(());
        }

        // Check for producer send exception
        if let Some(error) = self.producer_send_exception() {
            return Err(ConnectError::general(error));
        }

        // Poll records
        let records = self.poll()?;

        if records.is_empty() {
            return Ok(());
        }

        // Send records
        self.send_records(records)?;

        Ok(())
    }

    fn close(&mut self) -> Result<(), ConnectError> {
        self.close_called.store(true, Ordering::SeqCst);

        // Flush producer
        self.flush_producer()?;

        // Clear submitted records
        self.submitted_records.clear();

        // Clear producer send exception
        self.clear_producer_send_exception();

        Ok(())
    }

    fn commit_offsets(&mut self) -> Result<(), ConnectError> {
        self.commit_offsets_internal()
    }

    fn is_stopping(&self) -> bool {
        self.state.lock().unwrap().stopping
    }

    fn is_paused(&self) -> bool {
        self.state.lock().unwrap().paused
    }

    fn is_started(&self) -> bool {
        self.state.lock().unwrap().started
    }

    fn is_failed(&self) -> bool {
        self.state.lock().unwrap().failed
    }

    fn target_state(&self) -> TargetState {
        self.state.lock().unwrap().target_state
    }

    fn set_target_state(&mut self, state: TargetState) {
        self.state.lock().unwrap().target_state = state;
    }

    fn set_stopping(&mut self, stopping: bool) {
        self.state.lock().unwrap().stopping = stopping;
        if stopping {
            self.stop_condvar.notify_all();
        }
    }

    fn set_started(&mut self, started: bool) {
        self.state.lock().unwrap().started = started;
    }

    fn set_paused(&mut self, paused: bool) {
        self.state.lock().unwrap().paused = paused;
    }

    fn set_failed(&mut self, failed: bool, error_message: Option<String>) {
        let mut state = self.state.lock().unwrap();
        state.failed = failed;
        state.error_message = error_message.map(|s| Arc::new(s));
    }

    fn get_error_message(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap()
            .error_message
            .as_ref()
            .map(|s| s.to_string())
    }

    fn await_stop(&self, timeout: Duration) -> bool {
        let state = self.state.lock().unwrap();
        let result = self
            .stop_condvar
            .wait_timeout_while(state, timeout, |s| !s.stopping);
        match result {
            Ok((_state, timeout_result)) => !timeout_result.timed_out(),
            Err(_) => false,
        }
    }
}

// ===== Test helpers =====

/// A simple mock SourceTask poller for testing.
pub struct MockSourceTaskPoller {
    records: Mutex<Vec<SourceRecord>>,
    poll_count: Mutex<u32>,
    commit_count: Mutex<u32>,
    commit_record_count: Mutex<u32>,
    should_fail: AtomicBool,
    fail_message: Mutex<Option<String>>,
}

impl MockSourceTaskPoller {
    /// Creates a new MockSourceTaskPoller.
    pub fn new() -> Self {
        MockSourceTaskPoller {
            records: Mutex::new(Vec::new()),
            poll_count: Mutex::new(0),
            commit_count: Mutex::new(0),
            commit_record_count: Mutex::new(0),
            should_fail: AtomicBool::new(false),
            fail_message: Mutex::new(None),
        }
    }

    /// Sets the records to return.
    pub fn set_records(&self, records: Vec<SourceRecord>) {
        *self.records.lock().unwrap() = records;
    }

    /// Returns the poll count.
    pub fn poll_count(&self) -> u32 {
        *self.poll_count.lock().unwrap()
    }

    /// Returns the commit count.
    pub fn commit_count(&self) -> u32 {
        *self.commit_count.lock().unwrap()
    }

    /// Returns the commit record count.
    pub fn commit_record_count(&self) -> u32 {
        *self.commit_record_count.lock().unwrap()
    }

    /// Sets whether to fail on poll.
    pub fn set_should_fail(&self, fail: bool, message: Option<String>) {
        self.should_fail.store(fail, Ordering::SeqCst);
        *self.fail_message.lock().unwrap() = message;
    }

    /// Clears all counters.
    pub fn clear(&self) {
        *self.poll_count.lock().unwrap() = 0;
        *self.commit_count.lock().unwrap() = 0;
        *self.commit_record_count.lock().unwrap() = 0;
    }
}

impl Default for MockSourceTaskPoller {
    fn default() -> Self {
        MockSourceTaskPoller::new()
    }
}

impl ThreadSafeSourceTaskPoller for MockSourceTaskPoller {
    fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError> {
        *self.poll_count.lock().unwrap() += 1;

        if self.should_fail.load(Ordering::SeqCst) {
            let msg = self.fail_message.lock().unwrap().clone();
            return Err(ConnectError::general(
                msg.unwrap_or_else(|| "Mock poll error".to_string()),
            ));
        }

        let records = self.records.lock().unwrap().clone();
        Ok(records)
    }

    fn commit(&mut self) -> Result<(), ConnectError> {
        *self.commit_count.lock().unwrap() += 1;
        Ok(())
    }

    fn commit_record(
        &mut self,
        _record: &SourceRecord,
        _metadata: &RecordMetadata,
    ) -> Result<(), ConnectError> {
        *self.commit_record_count.lock().unwrap() += 1;
        Ok(())
    }
}

/// A simple converter for testing.
pub struct SimpleConverter;

impl Converter for SimpleConverter {
    fn from_connect_data(&self, _topic: &str, value: &serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(value).unwrap_or_default()
    }
}

/// A simple header converter for testing.
pub struct SimpleHeaderConverter;

impl HeaderConverter for SimpleHeaderConverter {
    fn from_connect_headers(
        &self,
        _headers: &connect_api::header::ConnectHeaders,
    ) -> HashMap<String, Vec<u8>> {
        HashMap::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connect_api::header::ConnectHeaders;
    use std::collections::HashMap as StdHashMap;

    fn create_test_source_record(topic: &str, value: &str) -> SourceRecord {
        let partition = StdHashMap::from([(
            "partition".to_string(),
            serde_json::Value::String("p1".to_string()),
        )]);
        let offset = StdHashMap::from([(
            "offset".to_string(),
            serde_json::Value::Number(serde_json::Number::from(1)),
        )]);
        SourceRecord::new(
            partition,
            offset,
            topic,
            None,
            Some(serde_json::Value::String(value.to_string())),
            serde_json::Value::String(value.to_string()),
        )
    }

    fn create_test_task() -> WorkerSourceTask {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let producer = Some(Arc::new(MockKafkaProducer::new()));
        WorkerSourceTask::new(id, listener, producer)
    }

    fn create_test_task_with_converters() -> WorkerSourceTask {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let producer = Some(Arc::new(MockKafkaProducer::new()));
        WorkerSourceTask::with_converters(
            id,
            listener,
            producer,
            Some(Arc::new(SimpleConverter)),
            Some(Arc::new(SimpleConverter)),
            Some(Arc::new(SimpleHeaderConverter)),
        )
    }

    #[test]
    fn test_task_creation() {
        let task = create_test_task();
        assert_eq!(task.id().connector(), "test-connector");
        assert_eq!(task.id().task(), 0);
        assert!(!task.was_initialized());
        assert!(!task.was_closed());
    }

    #[test]
    fn test_initialize() {
        let mut task = create_test_task();
        let config = TaskConfig::default();
        task.initialize(config).unwrap();
        assert!(task.was_initialized());
    }

    #[test]
    fn test_do_start_requires_producer() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let mut task = WorkerSourceTask::new(id, listener, None);

        let result = task.do_start();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains("Producer not configured"));
    }

    #[test]
    fn test_poll_without_source_task() {
        let mut task = create_test_task();
        let result = task.poll();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains("SourceTask not initialized"));
    }

    #[test]
    fn test_poll_with_source_task() {
        let mut task = create_test_task();
        let poller = Box::new(MockSourceTaskPoller::new());

        // Set records on the poller before setting it
        {
            let mock = poller.as_ref() as &dyn std::any::Any;
            if let Some(mock_poller) = mock.downcast_ref::<MockSourceTaskPoller>() {
                let records = vec![create_test_source_record("test-topic", "test-value")];
                mock_poller.set_records(records);
            }
        }

        task.set_source_task_poller(poller);

        // Poll
        let result = task.poll().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].topic(), "test-topic");
    }

    #[test]
    fn test_poll_when_stopping() {
        let mut task = create_test_task();
        task.set_stopping(true);

        let result = task.poll().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_poll_when_paused() {
        let mut task = create_test_task();
        task.set_paused(true);

        let result = task.poll().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_convert_to_send_records() {
        let task = create_test_task_with_converters();

        let records = vec![
            create_test_source_record("topic1", "value1"),
            create_test_source_record("topic2", "value2"),
        ];

        let result = task.convert_to_send_records(records).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1.topic(), "topic1");
        assert_eq!(result[1].1.topic(), "topic2");
    }

    #[test]
    fn test_send_records_empty() {
        let mut task = create_test_task();
        let result = task.send_records(Vec::new());
        assert!(result.is_ok());
    }

    #[test]
    fn test_send_records_without_producer() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let task = WorkerSourceTask::new(id, listener, None);

        let records = vec![create_test_source_record("topic", "value")];
        let converted = task.convert_to_send_records(records).unwrap();

        let result = task.send_to_producer(converted[0].0.clone(), converted[0].1.clone());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains("Producer not configured"));
    }

    #[test]
    fn test_send_to_producer_success() {
        let task = create_test_task();

        let record = create_test_source_record("test-topic", "test-value");
        let producer_record = task.convert_record(&record).unwrap();

        let result = task.send_to_producer(record, producer_record);
        assert!(result.is_ok());
        // After successful send, record is acknowledged and moved to committable
        assert_eq!(task.submitted_count(), 0); // pending count is 0
        assert_eq!(task.committable_count(), 1); // committable count is 1
    }

    #[test]
    fn test_commit_record() {
        let task = create_test_task();
        let poller = Box::new(MockSourceTaskPoller::new());
        task.set_source_task_poller(poller);

        let record = create_test_source_record("test-topic", "test-value");
        let tp = common_trait::TopicPartition::new("test-topic", 0);
        let metadata = RecordMetadata::new(tp, 0, 0, 1234567890, 0, 0);

        task.commit_record(&record, &metadata).unwrap();

        // Verify commit_record was called on poller
        let poller_guard = task.source_task_poller.lock().unwrap();
        if let Some(p) = poller_guard.as_ref() {
            let mock = p as &dyn std::any::Any;
            if let Some(mock_poller) = mock.downcast_ref::<MockSourceTaskPoller>() {
                assert_eq!(mock_poller.commit_record_count(), 1);
            }
        }
    }

    #[test]
    fn test_commit_source_record() {
        let task = create_test_task();
        let poller = Box::new(MockSourceTaskPoller::new());
        task.set_source_task_poller(poller);

        let record = create_test_source_record("test-topic", "test-value");
        let tp = common_trait::TopicPartition::new("test-topic", 0);
        let metadata = RecordMetadata::new(tp, 0, 0, 1234567890, 0, 0);

        // Test commit_source_record method
        task.commit_source_record(&record, &metadata).unwrap();

        // Verify commit_record was called on poller (same semantics)
        let poller_guard = task.source_task_poller.lock().unwrap();
        if let Some(p) = poller_guard.as_ref() {
            let mock = p as &dyn std::any::Any;
            if let Some(mock_poller) = mock.downcast_ref::<MockSourceTaskPoller>() {
                assert_eq!(mock_poller.commit_record_count(), 1);
            }
        }
    }

    #[test]
    fn test_commit_source_task() {
        let task = create_test_task();
        let poller = Box::new(MockSourceTaskPoller::new());
        task.set_source_task_poller(poller);

        task.commit_source_task().unwrap();

        // Verify commit was called on poller
        let poller_guard = task.source_task_poller.lock().unwrap();
        if let Some(p) = poller_guard.as_ref() {
            let mock = p as &dyn std::any::Any;
            if let Some(mock_poller) = mock.downcast_ref::<MockSourceTaskPoller>() {
                assert_eq!(mock_poller.commit_count(), 1);
            }
        }
    }

    #[test]
    fn test_commit_offsets() {
        let mut task = create_test_task();
        task.initialize(TaskConfig::default()).unwrap();

        // Add some committable records
        task.submitted_records.acknowledge_all();

        let result = task.commit_offsets();
        assert!(result.is_ok());
    }

    #[test]
    fn test_commit_offsets_when_stopping() {
        let mut task = create_test_task();
        task.set_stopping(true);

        let result = task.commit_offsets();
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_with_records() {
        let mut task = create_test_task_with_converters();
        task.initialize(TaskConfig::default()).unwrap();

        let poller = Box::new(MockSourceTaskPoller::new());
        let records = vec![create_test_source_record("test-topic", "test-value")];

        // Set records on poller before setting it
        {
            let poller_ref = poller.as_ref() as &dyn std::any::Any;
            if let Some(mock_poller) = poller_ref.downcast_ref::<MockSourceTaskPoller>() {
                mock_poller.set_records(records);
            }
        }
        task.set_source_task_poller(poller);

        let result = task.execute();
        assert!(result.is_ok());

        // Verify poll was called
        let poller_guard = task.source_task_poller.lock().unwrap();
        if let Some(p) = poller_guard.as_ref() {
            let mock = p as &dyn std::any::Any;
            if let Some(mock_poller) = mock.downcast_ref::<MockSourceTaskPoller>() {
                assert_eq!(mock_poller.poll_count(), 1);
            }
        }
    }

    #[test]
    fn test_execute_when_stopping() {
        let mut task = create_test_task();
        task.set_stopping(true);

        let result = task.execute();
        assert!(result.is_ok());
    }

    #[test]
    fn test_close() {
        let mut task = create_test_task();
        task.initialize(TaskConfig::default()).unwrap();

        task.close().unwrap();
        assert!(task.was_closed());
    }

    #[test]
    fn test_state_transitions() {
        let mut task = create_test_task();

        // Initial state
        assert_eq!(task.target_state(), TargetState::Started);
        assert!(!task.is_stopping());
        assert!(!task.is_paused());

        // Transition to paused
        task.transition_to(TargetState::Paused);
        assert_eq!(task.target_state(), TargetState::Paused);
        assert!(task.is_paused());

        // Transition to stopped
        task.transition_to(TargetState::Stopped);
        assert_eq!(task.target_state(), TargetState::Stopped);
        assert!(task.is_stopping());
    }

    #[test]
    fn test_producer_send_exception() {
        let task = create_test_task();

        // Initially no exception
        assert!(task.producer_send_exception().is_none());

        // Set exception
        task.producer_send_failed("Send failed");
        assert!(task.producer_send_exception().is_some());
        assert!(task
            .producer_send_exception()
            .unwrap()
            .contains("Send failed"));

        // Clear exception
        task.clear_producer_send_exception();
        assert!(task.producer_send_exception().is_none());
    }

    #[test]
    fn test_submitted_records() {
        let sr = SubmittedRecords::new();
        let record = create_test_source_record("test-topic", "test-value");
        let submitted = Arc::new(SubmittedRecord::new(record));

        // Submit
        sr.submit(submitted.clone());
        assert_eq!(sr.pending_count(), 1);
        assert_eq!(sr.committable_count(), 0);

        // Acknowledge by id
        sr.acknowledge_by_id(submitted.identifier());
        assert_eq!(sr.pending_count(), 0);
        assert_eq!(sr.committable_count(), 1);

        // Get committable offsets
        let offsets = sr.committable_offsets();
        assert_eq!(offsets.len(), 1);

        // Clear committable
        sr.clear_committable();
        assert_eq!(sr.committable_count(), 0);
    }

    #[test]
    fn test_submitted_records_acknowledge_all() {
        let sr = SubmittedRecords::new();
        let r1 = Arc::new(SubmittedRecord::new(create_test_source_record(
            "topic", "v1",
        )));
        let r2 = Arc::new(SubmittedRecord::new(create_test_source_record(
            "topic", "v2",
        )));

        sr.submit(r1);
        sr.submit(r2);
        assert_eq!(sr.pending_count(), 2);

        sr.acknowledge_all();
        assert_eq!(sr.pending_count(), 0);
        assert_eq!(sr.committable_count(), 2);
    }

    #[test]
    fn test_offset_state() {
        let partition = HashMap::from([(
            "key".to_string(),
            serde_json::Value::String("p".to_string()),
        )]);
        let offset = HashMap::from([(
            "pos".to_string(),
            serde_json::Value::Number(serde_json::Number::from(1)),
        )]);
        let state = OffsetState::new(partition.clone(), offset.clone());

        assert_eq!(state.partition(), &partition);
        assert_eq!(state.offset(), &offset);
    }

    #[test]
    fn test_flush_producer() {
        let task = create_test_task();
        let result = task.flush_producer();
        assert!(result.is_ok());
    }

    #[test]
    fn test_metrics_tracking() {
        let mut task = create_test_task_with_converters();
        task.initialize(TaskConfig::default()).unwrap();

        let poller = Box::new(MockSourceTaskPoller::new());
        let records = vec![
            create_test_source_record("test-topic", "value1"),
            create_test_source_record("test-topic", "value2"),
        ];

        {
            let poller_ref = poller.as_ref() as &dyn std::any::Any;
            if let Some(mock_poller) = poller_ref.downcast_ref::<MockSourceTaskPoller>() {
                mock_poller.set_records(records);
            }
        }
        task.set_source_task_poller(poller);

        // Execute should record batch
        task.execute().unwrap();
        assert_eq!(task.metrics().batch_count(), 1);
    }

    #[test]
    fn test_error_message_storage() {
        let mut task = create_test_task();

        // Initially no error
        assert!(task.get_error_message().is_none());

        // Set error
        task.set_failed(true, Some("Test error".to_string()));
        assert!(task.is_failed());
        assert_eq!(task.get_error_message(), Some("Test error".to_string()));

        // Clear error
        task.set_failed(false, None);
        assert!(task.get_error_message().is_none());
    }

    #[test]
    fn test_standard_startup_shutdown() {
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let producer = Some(Arc::new(MockKafkaProducer::new()));
        let mut task = WorkerSourceTask::new(id, listener.clone(), producer);

        // Initialize and start
        task.initialize(TaskConfig::default()).unwrap();
        task.start().unwrap();

        assert!(task.was_initialized());
        assert!(task.is_started());
        assert_eq!(listener.count_event_type("startup"), 1);
        assert_eq!(listener.count_event_type("running"), 1);

        // Stop and close
        task.stop();
        task.close().unwrap();

        assert!(task.is_stopping());
        assert!(task.was_closed());
    }

    #[test]
    fn test_execute_with_producer_exception() {
        let mut task = create_test_task();
        task.initialize(TaskConfig::default()).unwrap();

        // Set producer send exception
        task.set_producer_send_exception("Previous send failed".to_string());

        // Execute should fail due to exception
        let result = task.execute();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains("Previous send failed"));
    }

    #[test]
    fn test_poll_failure() {
        let mut task = create_test_task();
        let poller = Box::new(MockSourceTaskPoller::new());

        {
            let poller_ref = poller.as_ref() as &dyn std::any::Any;
            if let Some(mock_poller) = poller_ref.downcast_ref::<MockSourceTaskPoller>() {
                mock_poller.set_should_fail(true, Some("Poll failed".to_string()));
            }
        }
        task.set_source_task_poller(poller);

        let result = task.poll();
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("Poll failed"));
    }

    #[test]
    fn test_thread_safe_source_task_poller() {
        let mut poller = MockSourceTaskPoller::new();
        let records = vec![create_test_source_record("topic", "value")];
        poller.set_records(records.clone());

        let result = poller.poll();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);

        poller.commit().unwrap();
        assert_eq!(poller.commit_count(), 1);
    }
}
