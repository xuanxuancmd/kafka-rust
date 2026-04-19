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

//! WorkerSinkTask - Sink task execution in Kafka Connect.
//!
//! This module implements the WorkerSinkTask that consumes records from Kafka,
//! converts them to SinkRecords, and delivers them to a SinkTask.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.WorkerSinkTask` in Java.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common_trait::errors::ConnectError;
use common_trait::record::TimestampType;
use common_trait::worker::{ConnectorTaskId, TargetState};
use common_trait::TopicPartition;
use connect_api::connector::ConnectRecord;
use connect_api::header::ConnectHeaders;
use connect_api::sink::SinkRecord;
use kafka_clients_mock::{ConsumerRecord, ConsumerRecords, MockKafkaConsumer, OffsetAndMetadata};

use super::{TaskConfig, TaskMetrics, ThreadSafeTaskStatusListener, WorkerTask};

/// Object-safe sink task executor trait.
///
/// This trait is defined in connect-runtime to be object-safe
/// (no generic parameters), allowing dynamic dispatch for
/// sink task operations.
///
/// Corresponds to SinkTask operations in Java.
pub trait ThreadSafeSinkTaskExecutor: Send + Sync {
    /// Puts records to the sink system.
    fn put(&mut self, records: Vec<SinkRecord>) -> Result<(), ConnectError>;

    /// Flushes pending records.
    fn flush(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), ConnectError>;

    /// Pre-commits offsets - allows SinkTask to filter offsets.
    fn pre_commit(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, ConnectError>;

    /// Opens partitions for consumption.
    fn open(&mut self, partitions: Vec<TopicPartition>) -> Result<(), ConnectError>;

    /// Closes partitions.
    fn close(&mut self, partitions: Vec<TopicPartition>) -> Result<(), ConnectError>;
}

/// Converter trait for converting bytes to Connect data.
///
/// Simplified version for WorkerSinkTask.
pub trait SinkConverter: Send + Sync {
    /// Converts bytes to Connect data (SchemaAndValue represented as serde_json::Value).
    fn to_connect_data(&self, topic: &str, data: Option<&[u8]>) -> serde_json::Value;
}

/// Header converter trait for sink tasks.
///
/// Simplified version for WorkerSinkTask.
pub trait SinkHeaderConverter: Send + Sync {
    /// Converts bytes headers to Connect headers.
    fn to_connect_headers(&self, headers: &HashMap<String, Vec<u8>>) -> ConnectHeaders;
}

/// Transformation trait for Single Message Transformations (SMT).
///
/// Simplified version for WorkerSinkTask.
pub trait Transformation: Send + Sync {
    /// Applies transformation to a SinkRecord.
    fn apply(&self, record: &SinkRecord) -> Result<SinkRecord, ConnectError>;
}

/// Transformation chain that applies multiple transformations.
pub struct TransformationChain {
    transformations: Vec<Arc<dyn Transformation>>,
}

impl TransformationChain {
    /// Creates a new empty transformation chain.
    pub fn new() -> Self {
        TransformationChain {
            transformations: Vec::new(),
        }
    }

    /// Creates a chain with transformations.
    pub fn with_transformations(transformations: Vec<Arc<dyn Transformation>>) -> Self {
        TransformationChain { transformations }
    }

    /// Adds a transformation to the chain.
    pub fn add(&mut self, transformation: Arc<dyn Transformation>) {
        self.transformations.push(transformation);
    }

    /// Applies all transformations in order.
    pub fn apply(&self, record: &SinkRecord) -> Result<SinkRecord, ConnectError> {
        let mut current = record.clone();
        for transformation in &self.transformations {
            current = transformation.apply(&current)?;
        }
        Ok(current)
    }

    /// Returns the number of transformations.
    pub fn len(&self) -> usize {
        self.transformations.len()
    }

    /// Returns whether the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.transformations.is_empty()
    }
}

impl Default for TransformationChain {
    fn default() -> Self {
        TransformationChain::new()
    }
}

/// Internal state for WorkerSinkTask.
struct WorkerSinkTaskState {
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
    /// Current offsets being processed.
    current_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    /// Assigned partitions.
    assigned_partitions: Vec<TopicPartition>,
    /// Whether consumer is paused due to retriable error.
    consumer_paused: bool,
    /// Pending message batch for re-delivery.
    pending_batch: Vec<SinkRecord>,
}

impl Default for WorkerSinkTaskState {
    fn default() -> Self {
        WorkerSinkTaskState {
            target_state: TargetState::Started,
            stopping: false,
            failed: false,
            started: false,
            paused: false,
            error_message: None,
            current_offsets: HashMap::new(),
            assigned_partitions: Vec::new(),
            consumer_paused: false,
            pending_batch: Vec::new(),
        }
    }
}

/// WorkerSinkTask - executes sink tasks.
///
/// This struct implements the sink task execution logic:
/// - Polls records from Kafka via consumer
/// - Converts ConsumerRecords to SinkRecords
/// - Applies transformations (SMTs)
/// - Delivers SinkRecords to the underlying SinkTask
/// - Commits offsets back to Kafka
///
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerSinkTask` in Java.
pub struct WorkerSinkTask {
    /// Task ID.
    id: ConnectorTaskId,
    /// Task version (obtained from connector's task class).
    version: String,
    /// Status listener.
    status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
    /// Task metrics.
    metrics: TaskMetrics,
    /// Internal state.
    state: Mutex<WorkerSinkTaskState>,
    /// Stop condvar.
    stop_condvar: Condvar,
    /// The Kafka consumer.
    consumer: Option<Arc<MockKafkaConsumer>>,
    /// The underlying SinkTask executor.
    sink_task_executor: Mutex<Option<Box<dyn ThreadSafeSinkTaskExecutor>>>,
    /// Key converter (for deserialization).
    key_converter: Option<Arc<dyn SinkConverter>>,
    /// Value converter (for deserialization).
    value_converter: Option<Arc<dyn SinkConverter>>,
    /// Header converter (for deserialization).
    header_converter: Option<Arc<dyn SinkHeaderConverter>>,
    /// Transformation chain (SMTs).
    transformation_chain: TransformationChain,
    /// Whether the task has been initialized.
    initialized: AtomicBool,
    /// Whether close was called.
    close_called: AtomicBool,
    /// Current message batch (converted SinkRecords).
    message_batch: Mutex<Vec<SinkRecord>>,
    /// Commit sequence number (for async commit tracking).
    commit_seqno: Mutex<u64>,
}

impl WorkerSinkTask {
    /// Creates a new WorkerSinkTask.
    pub fn new(
        id: ConnectorTaskId,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
        consumer: Option<Arc<MockKafkaConsumer>>,
    ) -> Self {
        WorkerSinkTask {
            id,
            version: "1.0.0".to_string(),
            status_listener,
            metrics: TaskMetrics::new(),
            state: Mutex::new(WorkerSinkTaskState::default()),
            stop_condvar: Condvar::new(),
            consumer,
            sink_task_executor: Mutex::new(None),
            key_converter: None,
            value_converter: None,
            header_converter: None,
            transformation_chain: TransformationChain::new(),
            initialized: AtomicBool::new(false),
            close_called: AtomicBool::new(false),
            message_batch: Mutex::new(Vec::new()),
            commit_seqno: Mutex::new(0),
        }
    }

    /// Creates a WorkerSinkTask with a specific version.
    pub fn with_version(
        id: ConnectorTaskId,
        version: String,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
        consumer: Option<Arc<MockKafkaConsumer>>,
    ) -> Self {
        WorkerSinkTask {
            id,
            version,
            status_listener,
            metrics: TaskMetrics::new(),
            state: Mutex::new(WorkerSinkTaskState::default()),
            stop_condvar: Condvar::new(),
            consumer,
            sink_task_executor: Mutex::new(None),
            key_converter: None,
            value_converter: None,
            header_converter: None,
            transformation_chain: TransformationChain::new(),
            initialized: AtomicBool::new(false),
            close_called: AtomicBool::new(false),
            message_batch: Mutex::new(Vec::new()),
            commit_seqno: Mutex::new(0),
        }
    }

    /// Creates a WorkerSinkTask with converters.
    pub fn with_converters(
        id: ConnectorTaskId,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
        consumer: Option<Arc<MockKafkaConsumer>>,
        key_converter: Option<Arc<dyn SinkConverter>>,
        value_converter: Option<Arc<dyn SinkConverter>>,
        header_converter: Option<Arc<dyn SinkHeaderConverter>>,
        transformation_chain: TransformationChain,
    ) -> Self {
        WorkerSinkTask {
            id,
            version: "1.0.0".to_string(),
            status_listener,
            metrics: TaskMetrics::new(),
            state: Mutex::new(WorkerSinkTaskState::default()),
            stop_condvar: Condvar::new(),
            consumer,
            sink_task_executor: Mutex::new(None),
            key_converter,
            value_converter,
            header_converter,
            transformation_chain,
            initialized: AtomicBool::new(false),
            close_called: AtomicBool::new(false),
            message_batch: Mutex::new(Vec::new()),
            commit_seqno: Mutex::new(0),
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

    /// Sets the sink task executor.
    pub fn set_sink_task_executor(&self, executor: Box<dyn ThreadSafeSinkTaskExecutor>) {
        *self.sink_task_executor.lock().unwrap() = Some(executor);
    }

    /// Returns whether initialize was called.
    pub fn was_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    /// Returns whether close was called.
    pub fn was_closed(&self) -> bool {
        self.close_called.load(Ordering::SeqCst)
    }

    /// Returns the current message batch size.
    pub fn message_batch_size(&self) -> usize {
        self.message_batch.lock().unwrap().len()
    }

    /// Returns the current offsets.
    pub fn current_offsets(&self) -> HashMap<TopicPartition, OffsetAndMetadata> {
        self.state.lock().unwrap().current_offsets.clone()
    }

    /// Sets assigned partitions.
    pub fn set_assigned_partitions(&self, partitions: Vec<TopicPartition>) {
        self.state.lock().unwrap().assigned_partitions = partitions;
    }

    /// Returns assigned partitions.
    pub fn assigned_partitions(&self) -> Vec<TopicPartition> {
        self.state.lock().unwrap().assigned_partitions.clone()
    }

    /// Returns whether consumer is paused.
    pub fn is_consumer_paused(&self) -> bool {
        self.state.lock().unwrap().consumer_paused
    }

    /// Sets consumer paused state.
    pub fn set_consumer_paused(&self, paused: bool) {
        self.state.lock().unwrap().consumer_paused = paused;
    }

    /// Polls for messages from Kafka.
    ///
    /// Corresponds to `poll()` in Java WorkerSinkTask.
    pub fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords, ConnectError> {
        // Check if task is stopping
        if self.is_stopping() {
            return Ok(ConsumerRecords::empty());
        }

        // Check if paused
        if self.is_paused() {
            return Ok(ConsumerRecords::empty());
        }

        // Get consumer
        let consumer = match &self.consumer {
            Some(c) => c,
            None => return Err(ConnectError::general("Consumer not configured")),
        };

        // Poll from consumer
        let records = consumer.poll(timeout);

        // Record metrics
        self.record_batch(records.count());

        Ok(records)
    }

    /// Converts ConsumerRecords to SinkRecords.
    ///
    /// Iterates through ConsumerRecords and converts each to a SinkRecord
    /// using the configured converters and transformation chain.
    /// Corresponds to `convertMessages()` in Java WorkerSinkTask.
    pub fn convert_messages(
        &self,
        consumer_records: ConsumerRecords,
    ) -> Result<Vec<SinkRecord>, ConnectError> {
        if consumer_records.is_empty() {
            return Ok(Vec::new());
        }

        let mut sink_records = Vec::new();

        for consumer_record in consumer_records.iter() {
            let sink_record = self.convert_and_transform_record(consumer_record)?;
            sink_records.push(sink_record);
        }

        // Store in message batch
        *self.message_batch.lock().unwrap() = sink_records.clone();

        Ok(sink_records)
    }

    /// Converts and transforms a single ConsumerRecord to SinkRecord.
    ///
    /// This method:
    /// 1. Calls converter to convert key/value bytes to Connect data
    /// 2. Converts headers using header converter
    /// 3. Applies transformation chain (SMTs)
    /// Corresponds to `convertAndTransformRecord()` in Java.
    fn convert_and_transform_record(
        &self,
        consumer_record: &ConsumerRecord,
    ) -> Result<SinkRecord, ConnectError> {
        // Convert key using key converter
        let key = self.call_converter_key(consumer_record.topic(), consumer_record.key());

        // Convert value using value converter
        let value = self.call_converter_value(consumer_record.topic(), consumer_record.value());

        // Convert headers using header converter
        let headers = self.call_converter_headers(consumer_record.headers());

        // Create initial SinkRecord
        let sink_record = SinkRecord::new(
            consumer_record.topic(),
            Some(consumer_record.partition()),
            key,
            value,
            consumer_record.offset(),
            TimestampType::CreateTime,
        )
        .with_headers(headers);

        // Apply transformation chain
        self.call_transformation_chain(&sink_record)
    }

    /// Calls the key converter to convert bytes to Connect data.
    ///
    /// Corresponds to key conversion in `convertAndTransformRecord()` in Java.
    pub fn call_converter_key(
        &self,
        topic: &str,
        key_bytes: Option<&Vec<u8>>,
    ) -> Option<serde_json::Value> {
        if key_bytes.is_none() {
            return None;
        }

        if let Some(converter) = &self.key_converter {
            let data = key_bytes.map(|b| b.as_slice());
            Some(converter.to_connect_data(topic, data))
        } else {
            // Default: parse as JSON
            key_bytes.and_then(|b| serde_json::from_slice(b).ok())
        }
    }

    /// Calls the value converter to convert bytes to Connect data.
    ///
    /// Corresponds to value conversion in `convertAndTransformRecord()` in Java.
    pub fn call_converter_value(
        &self,
        topic: &str,
        value_bytes: Option<&Vec<u8>>,
    ) -> serde_json::Value {
        if value_bytes.is_none() {
            return serde_json::Value::Null;
        }

        if let Some(converter) = &self.value_converter {
            let data = value_bytes.map(|b| b.as_slice());
            converter.to_connect_data(topic, data)
        } else {
            // Default: parse as JSON
            value_bytes
                .and_then(|b| serde_json::from_slice(b).ok())
                .unwrap_or(serde_json::Value::Null)
        }
    }

    /// Calls the header converter to convert bytes headers to Connect headers.
    ///
    /// Corresponds to header conversion in `convertAndTransformRecord()` in Java.
    pub fn call_converter_headers(&self, headers: &HashMap<String, Vec<u8>>) -> ConnectHeaders {
        if let Some(converter) = &self.header_converter {
            converter.to_connect_headers(headers)
        } else {
            ConnectHeaders::new()
        }
    }

    /// Applies the transformation chain to a SinkRecord.
    ///
    /// Corresponds to transformation chain application in Java.
    pub fn call_transformation_chain(
        &self,
        record: &SinkRecord,
    ) -> Result<SinkRecord, ConnectError> {
        if self.transformation_chain.is_empty() {
            return Ok(record.clone());
        }

        self.transformation_chain.apply(record)
    }

    /// Delivers messages to the SinkTask.
    ///
    /// Takes the converted SinkRecords and passes them to the
    /// user's SinkTask via put(). Handles RetriableException by
    /// pausing the consumer for re-delivery.
    /// Corresponds to `deliverMessages()` in Java WorkerSinkTask.
    pub fn deliver_messages(&mut self, records: Vec<SinkRecord>) -> Result<(), ConnectError> {
        if records.is_empty() {
            return Ok(());
        }

        // Get sink task executor
        let mut executor_guard = self.sink_task_executor.lock().unwrap();
        let executor = match executor_guard.as_mut() {
            Some(e) => e,
            None => return Err(ConnectError::general("SinkTask not initialized")),
        };

        // Update current offsets
        {
            let mut state = self.state.lock().unwrap();
            for record in &records {
                let tp = TopicPartition::new(record.topic(), record.kafka_partition().unwrap_or(0));
                state
                    .current_offsets
                    .insert(tp, OffsetAndMetadata::new(record.kafka_offset() + 1));
            }
        }

        // Deliver to sink task
        let result = executor.put(records.clone());

        match result {
            Ok(()) => {
                // Clear pending batch on success
                self.state.lock().unwrap().pending_batch.clear();
                self.set_consumer_paused(false);
                Ok(())
            }
            Err(e) => {
                // Check if retriable
                if is_retriable_error(&e) {
                    // Store for re-delivery
                    self.state.lock().unwrap().pending_batch = records;
                    self.set_consumer_paused(true);
                    Err(e)
                } else {
                    // Unrecoverable error - will cause task to fail
                    Err(e)
                }
            }
        }
    }

    /// Commits offsets to Kafka.
    ///
    /// Initiates the offset commit process:
    /// 1. Filters offsets to only assigned partitions
    /// 2. Calls SinkTask.preCommit() for offset filtering
    /// 3. Performs sync or async commit
    /// Corresponds to `commitOffsets()` in Java WorkerSinkTask.
    pub fn commit_offsets_internal(&mut self) -> Result<(), ConnectError> {
        // Check for stopping
        if self.is_stopping() {
            return Ok(());
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Get current offsets filtered to assigned partitions
        let offsets_to_commit = self.prepare_offsets_for_commit();

        if offsets_to_commit.is_empty() {
            return Ok(());
        }

        // Call SinkTask.preCommit() for filtering
        let filtered_offsets = self.pre_commit(offsets_to_commit)?;

        if filtered_offsets.is_empty() {
            return Ok(());
        }

        // Perform commit
        let seqno = self.get_next_commit_seqno();
        self.offset_commit_helper(filtered_offsets, false, seqno)?;

        // Record commit metrics
        let duration = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
            - now;
        self.record_commit_success(duration);

        Ok(())
    }

    /// Prepares offsets for commit by filtering to assigned partitions.
    fn prepare_offsets_for_commit(&self) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let state = self.state.lock().unwrap();
        let assigned: HashSet<TopicPartition> = state.assigned_partitions.iter().cloned().collect();

        state
            .current_offsets
            .iter()
            .filter(|(tp, _)| assigned.contains(tp))
            .map(|(tp, offset)| (tp.clone(), offset.clone()))
            .collect()
    }

    /// Calls SinkTask.preCommit() to get offsets safe to commit.
    ///
    /// Corresponds to `preCommit()` call in Java WorkerSinkTask.
    pub fn pre_commit(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, ConnectError> {
        let mut executor_guard = self.sink_task_executor.lock().unwrap();
        if let Some(executor) = executor_guard.as_mut() {
            executor.pre_commit(offsets)
        } else {
            Ok(offsets)
        }
    }

    /// Helper for performing offset commit.
    ///
    /// Performs either synchronous or asynchronous commit
    /// based on configuration. Handles commit callbacks.
    /// Corresponds to `doCommit()` in Java WorkerSinkTask.
    pub fn offset_commit_helper(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        closing: bool,
        seqno: u64,
    ) -> Result<(), ConnectError> {
        let consumer = match &self.consumer {
            Some(c) => c,
            None => return Err(ConnectError::general("Consumer not configured")),
        };

        // Use synchronous commit for simplicity
        // In production, async commit would be used with callback tracking
        consumer
            .commit_sync_offsets(Some(offsets))
            .map_err(|e| ConnectError::general(e.to_string()))?;

        // Track commit sequence
        *self.commit_seqno.lock().unwrap() = seqno;

        Ok(())
    }

    /// Gets the next commit sequence number.
    fn get_next_commit_seqno(&self) -> u64 {
        let mut seqno = self.commit_seqno.lock().unwrap();
        *seqno += 1;
        *seqno
    }

    /// Handles partition assignment changes.
    ///
    /// Called when partitions are assigned to this consumer.
    /// Opens new partitions on the SinkTask.
    pub fn on_partitions_assigned(
        &mut self,
        partitions: Vec<TopicPartition>,
    ) -> Result<(), ConnectError> {
        // Update assigned partitions
        self.set_assigned_partitions(partitions.clone());

        // Open partitions on sink task
        let mut executor_guard = self.sink_task_executor.lock().unwrap();
        if let Some(executor) = executor_guard.as_mut() {
            executor.open(partitions)?;
        }

        Ok(())
    }

    /// Handles partition revocation.
    ///
    /// Called when partitions are revoked from this consumer.
    /// Closes revoked partitions on the SinkTask and commits offsets.
    pub fn on_partitions_revoked(
        &mut self,
        partitions: Vec<TopicPartition>,
    ) -> Result<(), ConnectError> {
        // Close partitions on sink task
        {
            let mut executor_guard = self.sink_task_executor.lock().unwrap();
            if let Some(executor) = executor_guard.as_mut() {
                executor.close(partitions.clone())?;
            }
        } // executor_guard dropped here

        // Remove from assigned partitions
        {
            let mut state = self.state.lock().unwrap();
            state
                .assigned_partitions
                .retain(|p| !partitions.contains(p));
        }

        // Commit offsets for revoked partitions
        self.commit_offsets_internal()?;

        Ok(())
    }

    /// Signals that the task has stopped.
    pub fn signal_stopped(&self) {
        self.stop_condvar.notify_all();
    }
}

/// Checks if an error is retriable.
///
/// Corresponds to error classification in Java.
fn is_retriable_error(error: &ConnectError) -> bool {
    // In Java, RetriableException is checked
    // For simplicity, check if message contains retriable indicators
    let msg = error.message();
    msg.contains("retriable") || msg.contains("timeout") || msg.contains("temporary")
}

impl WorkerTask for WorkerSinkTask {
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
        // Check consumer
        if self.consumer.is_none() {
            return Err(ConnectError::general(
                "Consumer not configured for sink task",
            ));
        }

        // Check sink task executor
        {
            let executor_guard = self.sink_task_executor.lock().unwrap();
            if executor_guard.is_none() {
                return Err(ConnectError::general("SinkTask executor not configured"));
            }
        }

        Ok(())
    }

    fn execute(&mut self) -> Result<(), ConnectError> {
        // Check for stopping
        if self.is_stopping() {
            return Ok(());
        }

        // Check for consumer paused (re-deliver pending batch)
        if self.is_consumer_paused() {
            let pending = self.state.lock().unwrap().pending_batch.clone();
            if !pending.is_empty() {
                self.deliver_messages(pending)?;
            }
            return Ok(());
        }

        // Poll for new records
        let consumer_records = self.poll(Duration::from_millis(100))?;

        if consumer_records.is_empty() {
            return Ok(());
        }

        // Convert to SinkRecords
        let sink_records = self.convert_messages(consumer_records)?;

        // Deliver to SinkTask
        self.deliver_messages(sink_records)?;

        Ok(())
    }

    fn close(&mut self) -> Result<(), ConnectError> {
        self.close_called.store(true, Ordering::SeqCst);

        // Commit final offsets
        self.commit_offsets_internal()?;

        // Close all assigned partitions
        let partitions = self.assigned_partitions();
        {
            let mut executor_guard = self.sink_task_executor.lock().unwrap();
            if let Some(executor) = executor_guard.as_mut() {
                executor.close(partitions)?;
            }
        }

        // Clear message batch
        self.message_batch.lock().unwrap().clear();

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

/// A simple mock SinkTask executor for testing.
pub struct MockSinkTaskExecutor {
    put_count: Mutex<u32>,
    flush_count: Mutex<u32>,
    pre_commit_count: Mutex<u32>,
    open_count: Mutex<u32>,
    close_count: Mutex<u32>,
    should_fail_put: AtomicBool,
    fail_message: Mutex<Option<String>>,
    last_records: Mutex<Vec<SinkRecord>>,
    pre_commit_filter:
        Mutex<Option<Box<dyn Fn(&TopicPartition, &OffsetAndMetadata) -> bool + Send>>>,
}

impl MockSinkTaskExecutor {
    /// Creates a new MockSinkTaskExecutor.
    pub fn new() -> Self {
        MockSinkTaskExecutor {
            put_count: Mutex::new(0),
            flush_count: Mutex::new(0),
            pre_commit_count: Mutex::new(0),
            open_count: Mutex::new(0),
            close_count: Mutex::new(0),
            should_fail_put: AtomicBool::new(false),
            fail_message: Mutex::new(None),
            last_records: Mutex::new(Vec::new()),
            pre_commit_filter: Mutex::new(None),
        }
    }

    /// Returns the put count.
    pub fn put_count(&self) -> u32 {
        *self.put_count.lock().unwrap()
    }

    /// Returns the flush count.
    pub fn flush_count(&self) -> u32 {
        *self.flush_count.lock().unwrap()
    }

    /// Returns the pre_commit count.
    pub fn pre_commit_count(&self) -> u32 {
        *self.pre_commit_count.lock().unwrap()
    }

    /// Returns the open count.
    pub fn open_count(&self) -> u32 {
        *self.open_count.lock().unwrap()
    }

    /// Returns the close count.
    pub fn close_count(&self) -> u32 {
        *self.close_count.lock().unwrap()
    }

    /// Sets whether put should fail.
    pub fn set_should_fail_put(&self, fail: bool, message: Option<String>) {
        self.should_fail_put.store(fail, Ordering::SeqCst);
        *self.fail_message.lock().unwrap() = message;
    }

    /// Returns the last records received.
    pub fn last_records(&self) -> Vec<SinkRecord> {
        self.last_records.lock().unwrap().clone()
    }

    /// Clears all counters.
    pub fn clear(&self) {
        *self.put_count.lock().unwrap() = 0;
        *self.flush_count.lock().unwrap() = 0;
        *self.pre_commit_count.lock().unwrap() = 0;
        *self.open_count.lock().unwrap() = 0;
        *self.close_count.lock().unwrap() = 0;
    }
}

impl Default for MockSinkTaskExecutor {
    fn default() -> Self {
        MockSinkTaskExecutor::new()
    }
}

impl ThreadSafeSinkTaskExecutor for MockSinkTaskExecutor {
    fn put(&mut self, records: Vec<SinkRecord>) -> Result<(), ConnectError> {
        *self.put_count.lock().unwrap() += 1;
        *self.last_records.lock().unwrap() = records;

        if self.should_fail_put.load(Ordering::SeqCst) {
            let msg = self.fail_message.lock().unwrap().clone();
            return Err(ConnectError::general(
                msg.unwrap_or_else(|| "Mock put error".to_string()),
            ));
        }

        Ok(())
    }

    fn flush(
        &mut self,
        _offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), ConnectError> {
        *self.flush_count.lock().unwrap() += 1;
        Ok(())
    }

    fn pre_commit(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, ConnectError> {
        *self.pre_commit_count.lock().unwrap() += 1;

        // Apply filter if set
        let filter_guard = self.pre_commit_filter.lock().unwrap();
        if let Some(filter) = filter_guard.as_ref() {
            Ok(offsets
                .into_iter()
                .filter(|(tp, offset)| filter(tp, offset))
                .collect())
        } else {
            Ok(offsets)
        }
    }

    fn open(&mut self, _partitions: Vec<TopicPartition>) -> Result<(), ConnectError> {
        *self.open_count.lock().unwrap() += 1;
        Ok(())
    }

    fn close(&mut self, _partitions: Vec<TopicPartition>) -> Result<(), ConnectError> {
        *self.close_count.lock().unwrap() += 1;
        Ok(())
    }
}

/// A simple sink converter for testing.
pub struct SimpleSinkConverter;

impl SinkConverter for SimpleSinkConverter {
    fn to_connect_data(&self, _topic: &str, data: Option<&[u8]>) -> serde_json::Value {
        data.and_then(|b| serde_json::from_slice(b).ok())
            .unwrap_or(serde_json::Value::Null)
    }
}

/// A simple sink header converter for testing.
pub struct SimpleSinkHeaderConverter;

impl SinkHeaderConverter for SimpleSinkHeaderConverter {
    fn to_connect_headers(&self, _headers: &HashMap<String, Vec<u8>>) -> ConnectHeaders {
        ConnectHeaders::new()
    }
}

/// A simple transformation for testing.
pub struct SimpleTransformation;

impl Transformation for SimpleTransformation {
    fn apply(&self, record: &SinkRecord) -> Result<SinkRecord, ConnectError> {
        // No transformation - return clone
        Ok(record.clone())
    }
}

use std::collections::HashSet;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;

    fn create_test_consumer_record(
        topic: &str,
        partition: i32,
        offset: i64,
        value: &[u8],
    ) -> ConsumerRecord {
        ConsumerRecord::new(
            topic,
            partition,
            offset,
            None,
            Some(value.to_vec()),
            1234567890,
        )
    }

    fn create_test_task() -> WorkerSinkTask {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        WorkerSinkTask::new(id, listener, consumer)
    }

    fn create_test_task_with_converters() -> WorkerSinkTask {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        WorkerSinkTask::with_converters(
            id,
            listener,
            consumer,
            Some(Arc::new(SimpleSinkConverter)),
            Some(Arc::new(SimpleSinkConverter)),
            Some(Arc::new(SimpleSinkHeaderConverter)),
            TransformationChain::new(),
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
    fn test_do_start_requires_consumer() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let mut task = WorkerSinkTask::new(id, listener, None);

        let result = task.do_start();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains("Consumer not configured"));
    }

    #[test]
    fn test_do_start_requires_sink_task_executor() {
        let mut task = create_test_task();
        // No sink task executor set

        let result = task.do_start();
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("SinkTask executor"));
    }

    #[test]
    fn test_poll_without_consumer() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let mut task = WorkerSinkTask::new(id, listener, None);

        let result = task.poll(Duration::from_millis(100));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains("Consumer not configured"));
    }

    #[test]
    fn test_poll_when_stopping() {
        let mut task = create_test_task();
        task.set_stopping(true);

        let result = task.poll(Duration::from_millis(100)).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_poll_when_paused() {
        let mut task = create_test_task();
        task.set_paused(true);

        let result = task.poll(Duration::from_millis(100)).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_poll_with_records() {
        let mut task = create_test_task();

        // Subscribe and add records
        let consumer = task.consumer.as_ref().unwrap();
        consumer.subscribe(vec!["test-topic".to_string()]);
        let tp = TopicPartition::new("test-topic", 0);
        let r1 = create_test_consumer_record("test-topic", 0, 0, b"value1");
        let r2 = create_test_consumer_record("test-topic", 0, 1, b"value2");
        consumer.add_records(StdHashMap::from([(tp.clone(), vec![r1, r2])]));

        let result = task.poll(Duration::from_millis(100)).unwrap();
        assert_eq!(result.count(), 2);
    }

    #[test]
    fn test_convert_messages_empty() {
        let task = create_test_task();
        let records = ConsumerRecords::empty();

        let result = task.convert_messages(records).unwrap();
        assert!(result.is_empty());
        assert_eq!(task.message_batch_size(), 0);
    }

    #[test]
    fn test_convert_messages_with_records() {
        let task = create_test_task_with_converters();

        let tp = TopicPartition::new("test-topic", 0);
        let r1 = ConsumerRecord::new(
            "test-topic",
            0,
            0,
            None,
            Some(br#"{"key":"value"}"#.to_vec()),
            1000,
        );
        let r2 = ConsumerRecord::new(
            "test-topic",
            0,
            1,
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            1001,
        );

        let consumer_records = ConsumerRecords::new(StdHashMap::from([(tp.clone(), vec![r1, r2])]));

        let result = task.convert_messages(consumer_records).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(task.message_batch_size(), 2);

        // Verify first record
        assert_eq!(result[0].topic(), "test-topic");
        assert_eq!(result[0].kafka_partition(), Some(0));
        assert_eq!(result[0].kafka_offset(), 0);
    }

    #[test]
    fn test_call_converter_key() {
        let task = create_test_task_with_converters();

        // None key
        assert!(task.call_converter_key("topic", None).is_none());

        // With key bytes
        let key_bytes = Some(br#"{"key":"value"}"#.to_vec());
        let result = task.call_converter_key("topic", key_bytes.as_ref());
        assert!(result.is_some());
    }

    #[test]
    fn test_call_converter_value() {
        let task = create_test_task_with_converters();

        // None value
        let result = task.call_converter_value("topic", None);
        assert!(result.is_null());

        // With value bytes
        let value_bytes = Some(br#"{"data":"test"}"#.to_vec());
        let result = task.call_converter_value("topic", value_bytes.as_ref());
        assert!(!result.is_null());
    }

    #[test]
    fn test_call_transformation_chain_empty() {
        let task = create_test_task();

        let record = SinkRecord::new(
            "topic",
            Some(0),
            None,
            serde_json::Value::String("value".to_string()),
            0,
            TimestampType::CreateTime,
        );

        let result = task.call_transformation_chain(&record).unwrap();
        assert_eq!(result.topic(), "topic");
    }

    #[test]
    fn test_call_transformation_chain_with_transformation() {
        let mut chain = TransformationChain::new();
        chain.add(Arc::new(SimpleTransformation));

        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let task = WorkerSinkTask::with_converters(
            id,
            listener,
            consumer,
            Some(Arc::new(SimpleSinkConverter)),
            Some(Arc::new(SimpleSinkConverter)),
            Some(Arc::new(SimpleSinkHeaderConverter)),
            chain,
        );

        let record = SinkRecord::new(
            "topic",
            Some(0),
            None,
            serde_json::Value::String("value".to_string()),
            0,
            TimestampType::CreateTime,
        );

        let result = task.call_transformation_chain(&record).unwrap();
        assert_eq!(result.topic(), "topic");
    }

    #[test]
    fn test_deliver_messages_empty() {
        let mut task = create_test_task();
        task.set_sink_task_executor(Box::new(MockSinkTaskExecutor::new()));

        let result = task.deliver_messages(Vec::new());
        assert!(result.is_ok());
    }

    #[test]
    fn test_deliver_messages_without_executor() {
        let mut task = create_test_task();
        // No executor set

        let records = vec![SinkRecord::new(
            "topic",
            Some(0),
            None,
            serde_json::Value::String("value".to_string()),
            0,
            TimestampType::CreateTime,
        )];

        let result = task.deliver_messages(records);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains("SinkTask not initialized"));
    }

    #[test]
    fn test_deliver_messages_success() {
        let mut task = create_test_task();
        let executor = Box::new(MockSinkTaskExecutor::new());
        task.set_sink_task_executor(executor);

        let records = vec![
            SinkRecord::new(
                "topic",
                Some(0),
                None,
                serde_json::Value::String("v1".to_string()),
                0,
                TimestampType::CreateTime,
            ),
            SinkRecord::new(
                "topic",
                Some(0),
                None,
                serde_json::Value::String("v2".to_string()),
                1,
                TimestampType::CreateTime,
            ),
        ];

        task.deliver_messages(records.clone()).unwrap();

        // Verify executor received records
        let executor_guard = task.sink_task_executor.lock().unwrap();
        let mock = executor_guard.as_ref().unwrap() as &dyn std::any::Any;
        if let Some(mock_executor) = mock.downcast_ref::<MockSinkTaskExecutor>() {
            assert_eq!(mock_executor.put_count(), 1);
            assert_eq!(mock_executor.last_records().len(), 2);
        }
    }

    #[test]
    fn test_deliver_messages_retriable_error() {
        let mut task = create_test_task();

        // Create executor and configure it to fail before setting
        let executor = MockSinkTaskExecutor::new();
        executor.set_should_fail_put(true, Some("retriable timeout error".to_string()));
        task.set_sink_task_executor(Box::new(executor));

        let records = vec![SinkRecord::new(
            "topic",
            Some(0),
            None,
            serde_json::Value::String("v".to_string()),
            0,
            TimestampType::CreateTime,
        )];

        let result = task.deliver_messages(records);
        assert!(result.is_err());

        // Consumer should be paused for re-delivery
        assert!(task.is_consumer_paused());
    }

    #[test]
    fn test_pre_commit() {
        let task = create_test_task();
        let executor = Box::new(MockSinkTaskExecutor::new());
        task.set_sink_task_executor(executor);

        let tp = TopicPartition::new("topic", 0);
        let offsets = StdHashMap::from([(tp.clone(), OffsetAndMetadata::new(10))]);

        let result = task.pre_commit(offsets.clone()).unwrap();
        assert_eq!(result.len(), 1);

        // Verify pre_commit was called
        let executor_guard = task.sink_task_executor.lock().unwrap();
        let mock = executor_guard.as_ref().unwrap() as &dyn std::any::Any;
        if let Some(mock_executor) = mock.downcast_ref::<MockSinkTaskExecutor>() {
            assert_eq!(mock_executor.pre_commit_count(), 1);
        }
    }

    #[test]
    fn test_offset_commit_helper() {
        let task = create_test_task();

        // Subscribe consumer
        let consumer = task.consumer.as_ref().unwrap();
        consumer.subscribe(vec!["topic".to_string()]);

        let tp = TopicPartition::new("topic", 0);
        let offsets = StdHashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);

        let result = task.offset_commit_helper(offsets, false, 1);
        assert!(result.is_ok());

        // Verify committed
        let committed = consumer.committed(&tp).unwrap();
        assert_eq!(committed.offset(), 100);
    }

    #[test]
    fn test_commit_offsets() {
        let mut task = create_test_task_with_converters();
        task.initialize(TaskConfig::default()).unwrap();
        task.set_sink_task_executor(Box::new(MockSinkTaskExecutor::new()));

        // Subscribe and add records
        let consumer = task.consumer.as_ref().unwrap();
        consumer.subscribe(vec!["topic".to_string()]);
        let tp = TopicPartition::new("topic", 0);
        let r1 = ConsumerRecord::new("topic", 0, 0, None, Some(b"v".to_vec()), 1000);
        consumer.add_records(StdHashMap::from([(tp.clone(), vec![r1])]));

        // Set assigned partitions
        task.set_assigned_partitions(vec![tp.clone()]);

        // Poll and convert
        let records = task.poll(Duration::from_millis(100)).unwrap();
        task.convert_messages(records).unwrap();

        // Commit
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
    fn test_on_partitions_assigned() {
        let mut task = create_test_task();
        task.set_sink_task_executor(Box::new(MockSinkTaskExecutor::new()));

        let tp = TopicPartition::new("topic", 0);
        let result = task.on_partitions_assigned(vec![tp.clone()]);
        assert!(result.is_ok());

        // Verify assigned
        assert!(task.assigned_partitions().contains(&tp));

        // Verify open was called
        let executor_guard = task.sink_task_executor.lock().unwrap();
        let mock = executor_guard.as_ref().unwrap() as &dyn std::any::Any;
        if let Some(mock_executor) = mock.downcast_ref::<MockSinkTaskExecutor>() {
            assert_eq!(mock_executor.open_count(), 1);
        }
    }

    #[test]
    fn test_on_partitions_revoked() {
        let mut task = create_test_task();
        task.initialize(TaskConfig::default()).unwrap();
        task.set_sink_task_executor(Box::new(MockSinkTaskExecutor::new()));

        let tp = TopicPartition::new("topic", 0);
        task.set_assigned_partitions(vec![tp.clone()]);

        // Subscribe consumer
        let consumer = task.consumer.as_ref().unwrap();
        consumer.subscribe(vec!["topic".to_string()]);

        let result = task.on_partitions_revoked(vec![tp.clone()]);
        assert!(result.is_ok());

        // Verify removed from assigned
        assert!(!task.assigned_partitions().contains(&tp));

        // Verify close was called
        let executor_guard = task.sink_task_executor.lock().unwrap();
        let mock = executor_guard.as_ref().unwrap() as &dyn std::any::Any;
        if let Some(mock_executor) = mock.downcast_ref::<MockSinkTaskExecutor>() {
            assert_eq!(mock_executor.close_count(), 1);
        }
    }

    #[test]
    fn test_execute_with_records() {
        let mut task = create_test_task_with_converters();
        task.initialize(TaskConfig::default()).unwrap();
        task.set_sink_task_executor(Box::new(MockSinkTaskExecutor::new()));

        // Subscribe and add records
        let consumer = task.consumer.as_ref().unwrap();
        consumer.subscribe(vec!["topic".to_string()]);
        let tp = TopicPartition::new("topic", 0);
        let r1 = ConsumerRecord::new("topic", 0, 0, None, Some(b"value".to_vec()), 1000);
        consumer.add_records(StdHashMap::from([(tp.clone(), vec![r1])]));

        // Set assigned partitions
        task.set_assigned_partitions(vec![tp.clone()]);

        let result = task.execute();
        assert!(result.is_ok());
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
        let mut task = create_test_task_with_converters();
        task.initialize(TaskConfig::default()).unwrap();
        task.set_sink_task_executor(Box::new(MockSinkTaskExecutor::new()));

        // Subscribe consumer
        let consumer = task.consumer.as_ref().unwrap();
        consumer.subscribe(vec!["topic".to_string()]);
        let tp = TopicPartition::new("topic", 0);
        task.set_assigned_partitions(vec![tp.clone()]);

        task.close().unwrap();
        assert!(task.was_closed());
        assert_eq!(task.message_batch_size(), 0);
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
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let mut task = WorkerSinkTask::new(id, listener.clone(), consumer);

        // Subscribe consumer
        task.consumer
            .as_ref()
            .unwrap()
            .subscribe(vec!["topic".to_string()]);

        // Set executor
        task.set_sink_task_executor(Box::new(MockSinkTaskExecutor::new()));

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
    fn test_transformation_chain() {
        let mut chain = TransformationChain::new();
        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);

        chain.add(Arc::new(SimpleTransformation));
        assert!(!chain.is_empty());
        assert_eq!(chain.len(), 1);

        let record = SinkRecord::new(
            "topic",
            Some(0),
            None,
            serde_json::Value::String("value".to_string()),
            0,
            TimestampType::CreateTime,
        );
        let result = chain.apply(&record).unwrap();
        assert_eq!(result.topic(), "topic");
    }

    #[test]
    fn test_metrics_tracking() {
        let mut task = create_test_task_with_converters();
        task.initialize(TaskConfig::default()).unwrap();
        task.set_sink_task_executor(Box::new(MockSinkTaskExecutor::new()));

        // Subscribe and add records
        let consumer = task.consumer.as_ref().unwrap();
        consumer.subscribe(vec!["topic".to_string()]);
        let tp = TopicPartition::new("topic", 0);
        let r1 = ConsumerRecord::new("topic", 0, 0, None, Some(b"v".to_vec()), 1000);
        consumer.add_records(StdHashMap::from([(tp.clone(), vec![r1])]));

        task.set_assigned_partitions(vec![tp.clone()]);

        // Execute should record batch
        task.execute().unwrap();
        assert!(task.metrics().batch_count() >= 1);
    }

    #[test]
    fn test_is_retriable_error() {
        let error1 = ConnectError::general("retriable connection timeout");
        assert!(is_retriable_error(&error1));

        let error2 = ConnectError::general("permanent failure");
        assert!(!is_retriable_error(&error2));
    }

    #[test]
    fn test_thread_safe_sink_task_executor() {
        let mut executor = MockSinkTaskExecutor::new();

        let records = vec![SinkRecord::new(
            "topic",
            Some(0),
            None,
            serde_json::Value::String("v".to_string()),
            0,
            TimestampType::CreateTime,
        )];
        executor.put(records).unwrap();
        assert_eq!(executor.put_count(), 1);

        let tp = TopicPartition::new("topic", 0);
        let offsets = StdHashMap::from([(tp.clone(), OffsetAndMetadata::new(10))]);
        executor.flush(offsets).unwrap();
        assert_eq!(executor.flush_count(), 1);

        executor.open(vec![tp.clone()]).unwrap();
        assert_eq!(executor.open_count(), 1);

        executor.close(vec![tp]).unwrap();
        assert_eq!(executor.close_count(), 1);
    }

    #[test]
    fn test_current_offsets_tracking() {
        let mut task = create_test_task();
        task.set_sink_task_executor(Box::new(MockSinkTaskExecutor::new()));

        let records = vec![
            SinkRecord::new(
                "topic",
                Some(0),
                None,
                serde_json::Value::String("v1".to_string()),
                0,
                TimestampType::CreateTime,
            ),
            SinkRecord::new(
                "topic",
                Some(0),
                None,
                serde_json::Value::String("v2".to_string()),
                1,
                TimestampType::CreateTime,
            ),
        ];

        task.deliver_messages(records).unwrap();

        // Verify offsets tracked
        let offsets = task.current_offsets();
        assert!(offsets.contains_key(&TopicPartition::new("topic", 0)));
    }

    #[test]
    fn test_prepare_offsets_for_commit() {
        let task = create_test_task();

        // Set assigned partitions
        let tp1 = TopicPartition::new("topic1", 0);
        let tp2 = TopicPartition::new("topic2", 0);
        task.set_assigned_partitions(vec![tp1.clone()]);

        // Set current offsets (including unassigned)
        {
            let mut state = task.state.lock().unwrap();
            state
                .current_offsets
                .insert(tp1.clone(), OffsetAndMetadata::new(10));
            state
                .current_offsets
                .insert(tp2.clone(), OffsetAndMetadata::new(20));
        }

        // Prepare should filter to only assigned
        let prepared = task.prepare_offsets_for_commit();
        assert!(prepared.contains_key(&tp1));
        assert!(!prepared.contains_key(&tp2));
    }
}
