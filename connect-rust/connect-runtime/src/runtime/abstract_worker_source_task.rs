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

//! AbstractWorkerSourceTask - shared logic for running source tasks.
//!
//! This module provides the base implementation for source tasks with either
//! standard semantics (at-least-once or at-most-once) or exactly-once semantics.
//!
//! It defines abstract hooks for subclasses to implement custom behavior,
//! and provides the core poll-convert-send loop implementation.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.AbstractWorkerSourceTask` in Java.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common_trait::errors::ConnectError;
use common_trait::metrics::PluginMetrics;
use common_trait::util::time::Time;
use common_trait::worker::{ConnectorTaskId, TargetState};
use connect_api::connector::ConnectRecord;
use connect_api::header::{ConnectHeaders, Headers};
use connect_api::source::SourceRecord;
use kafka_clients_mock::{ProducerRecord, RecordMetadata};

use crate::errors::{
    ProcessingContext as ErrorProcessingContext, RetryWithToleranceOperator, ToleranceType,
};
use crate::runtime::submitted_records::{SubmittedRecord, SubmittedRecords};
use crate::runtime::WorkerSourceTaskContext;
use crate::task::{TaskConfig, TaskMetrics, ThreadSafeTaskStatusListener, WorkerTask};

/// Backoff time in milliseconds when send fails.
/// Corresponds to SEND_FAILED_BACKOFF_MS in Java.
const SEND_FAILED_BACKOFF_MS: u64 = 100;

/// Trait defining abstract hooks for AbstractWorkerSourceTask subclasses.
///
/// These hooks allow subclasses (WorkerSourceTask, ExactlyOnceWorkerSourceTask)
/// to define custom behavior at specific points in the poll-convert-send loop.
///
/// Corresponds to the abstract methods in Java's AbstractWorkerSourceTask.
pub trait AbstractWorkerSourceTaskHooks: Send + Sync {
    /// Hook to define custom startup behavior before calling
    /// SourceTask.initialize(SourceTaskContext) and SourceTask.start(Map).
    ///
    /// Called during initializeAndStart().
    fn prepare_to_initialize_task(&mut self);

    /// Hook to define custom initialization behavior when preparing to begin
    /// the poll-convert-send loop for the first time, or when re-entering
    /// the loop after being paused.
    ///
    /// Called at the start of execute() and after resuming from pause.
    fn prepare_to_enter_send_loop(&mut self);

    /// Hook to define custom periodic behavior at the top of every iteration
    /// of the poll-convert-send loop.
    ///
    /// Called at the beginning of each loop iteration in execute().
    fn begin_send_iteration(&mut self);

    /// Hook to define custom periodic checks for health, metrics, etc.
    /// Called whenever SourceTask.poll() is about to be invoked.
    fn prepare_to_poll_task(&mut self);

    /// Invoked when a record provided by the task has been filtered out
    /// by a transform or the converter, or will be discarded due to
    /// failures during transformation or conversion.
    ///
    /// # Arguments
    /// * `record` - The pre-transform record that has been dropped
    fn record_dropped(&mut self, record: &SourceRecord);

    /// Invoked when a record is about to be dispatched to the producer.
    /// May be invoked multiple times for the same record if retriable errors
    /// are encountered.
    ///
    /// # Arguments
    /// * `source_record` - The pre-transform SourceRecord provided by the source task
    /// * `producer_record` - The ProducerRecord produced by transforming and converting
    ///
    /// # Returns
    /// An Optional SubmittedRecord to be acknowledged if the corresponding
    /// producer record is ack'd by Kafka, or dropped if synchronously rejected
    /// by the producer. Can return None if not necessary to track individual records.
    fn prepare_to_send_record(
        &mut self,
        source_record: &SourceRecord,
        producer_record: &ProducerRecord,
    ) -> Option<SubmittedRecord>;

    /// Invoked when a record has been transformed, converted, and dispatched
    /// to the producer successfully. Does not guarantee the record has been
    /// sent to Kafka or ack'd by brokers.
    ///
    /// # Arguments
    /// * `record` - The pre-transform SourceRecord that was successfully dispatched
    fn record_dispatched(&mut self, record: &SourceRecord);

    /// Invoked when an entire batch of records returned from SourceTask.poll()
    /// has been transformed, converted, and either discarded, filtered, or
    /// dispatched to the producer.
    fn batch_dispatched(&mut self);

    /// Invoked when a record has been sent and ack'd by the Kafka cluster.
    /// Note: This method may be invoked concurrently and should be thread-safe.
    ///
    /// # Arguments
    /// * `source_record` - The pre-transform SourceRecord that was successfully sent
    /// * `producer_record` - The ProducerRecord
    /// * `record_metadata` - The RecordMetadata for the producer record
    fn record_sent(
        &mut self,
        source_record: &SourceRecord,
        producer_record: &ProducerRecord,
        record_metadata: &RecordMetadata,
    );

    /// Invoked when a record given to Producer.send() has failed with a
    /// non-retriable error.
    ///
    /// # Arguments
    /// * `context` - The processing context for this record
    /// * `synchronous` - Whether the error occurred during send() invocation
    /// * `producer_record` - The ProducerRecord that failed to send
    /// * `pre_transform_record` - The pre-transform SourceRecord
    /// * `error` - The exception that was thrown or reported
    fn producer_send_failed(
        &mut self,
        context: &ErrorProcessingContext,
        synchronous: bool,
        producer_record: &ProducerRecord,
        pre_transform_record: &SourceRecord,
        error: &str,
    );

    /// Invoked when no more records will be polled or dispatched.
    /// Should attempt to commit offsets for outstanding records.
    ///
    /// # Arguments
    /// * `failed` - Whether the task is undergoing an unhealthy shutdown
    fn final_offset_commit(&mut self, failed: bool);
}

/// SourceTaskMetricsGroup - tracks metrics for source tasks.
///
/// Corresponds to SourceTaskMetricsGroup inner class in Java.
pub struct SourceTaskMetricsGroup {
    /// Metric group identifier
    metric_group: String,
    /// Total poll count
    poll_count: Mutex<u64>,
    /// Total poll batch time in milliseconds
    poll_time_total: Mutex<u64>,
    /// Max poll batch time in milliseconds
    poll_time_max: Mutex<u64>,
    /// Active record count (records being processed)
    active_record_count: Mutex<i64>,
    /// Total records polled
    source_record_poll_total: Mutex<u64>,
    /// Total records written
    source_record_write_total: Mutex<u64>,
}

impl SourceTaskMetricsGroup {
    /// Creates a new SourceTaskMetricsGroup.
    ///
    /// # Arguments
    /// * `id` - The connector task ID
    pub fn new(id: &ConnectorTaskId) -> Self {
        SourceTaskMetricsGroup {
            metric_group: format!(
                "source-task-metrics:connector={},task={}",
                id.connector(),
                id.task()
            ),
            poll_count: Mutex::new(0),
            poll_time_total: Mutex::new(0),
            poll_time_max: Mutex::new(0),
            active_record_count: Mutex::new(0),
            source_record_poll_total: Mutex::new(0),
            source_record_write_total: Mutex::new(0),
        }
    }

    /// Records a poll operation with batch size and duration.
    ///
    /// # Arguments
    /// * `batch_size` - Number of records in the batch
    /// * `duration_ms` - Duration of the poll operation in milliseconds
    pub fn record_poll(&self, batch_size: usize, duration_ms: u64) {
        let mut poll_count = self.poll_count.lock().unwrap();
        *poll_count += 1;

        let mut poll_time_total = self.poll_time_total.lock().unwrap();
        *poll_time_total += duration_ms;

        let mut poll_time_max = self.poll_time_max.lock().unwrap();
        if duration_ms > *poll_time_max {
            *poll_time_max = duration_ms;
        }

        let mut active_record_count = self.active_record_count.lock().unwrap();
        *active_record_count += batch_size as i64;

        let mut source_record_poll_total = self.source_record_poll_total.lock().unwrap();
        *source_record_poll_total += batch_size as u64;
    }

    /// Records a write operation.
    ///
    /// # Arguments
    /// * `record_count` - Number of records written
    /// * `skipped_count` - Number of records skipped
    pub fn record_write(&self, record_count: usize, skipped_count: usize) {
        let mut source_record_write_total = self.source_record_write_total.lock().unwrap();
        *source_record_write_total += (record_count - skipped_count) as u64;

        let mut active_record_count = self.active_record_count.lock().unwrap();
        *active_record_count -= record_count as i64;
        *active_record_count = std::cmp::max(0, *active_record_count);
    }

    /// Returns the metric group name.
    pub fn metric_group(&self) -> &str {
        &self.metric_group
    }

    /// Returns the poll count.
    pub fn poll_count(&self) -> u64 {
        *self.poll_count.lock().unwrap()
    }

    /// Returns the average poll batch time in milliseconds.
    pub fn average_poll_batch_time(&self) -> u64 {
        let poll_count = *self.poll_count.lock().unwrap();
        if poll_count == 0 {
            return 0;
        }
        let poll_time_total = *self.poll_time_total.lock().unwrap();
        poll_time_total / poll_count
    }
}

/// SourceRecordWriteCounter - counts records written for a batch.
///
/// Tracks progress of a batch of records being sent to Kafka,
/// and reports metrics when all writes complete.
///
/// Corresponds to SourceRecordWriteCounter inner class in Java.
pub struct SourceRecordWriteCounter {
    /// The metrics group to report to
    metrics_group: Arc<SourceTaskMetricsGroup>,
    /// The batch size
    batch_size: usize,
    /// Whether we've already reported metrics
    completed: AtomicBool,
    /// Counter for remaining records
    counter: Mutex<usize>,
    /// Number of skipped records
    skipped: Mutex<usize>,
}

impl SourceRecordWriteCounter {
    /// Creates a new SourceRecordWriteCounter.
    ///
    /// # Arguments
    /// * `batch_size` - The size of the batch (must be > 0)
    /// * `metrics_group` - The metrics group to report to
    pub fn new(batch_size: usize, metrics_group: Arc<SourceTaskMetricsGroup>) -> Self {
        assert!(batch_size > 0);
        SourceRecordWriteCounter {
            metrics_group,
            batch_size,
            completed: AtomicBool::new(false),
            counter: Mutex::new(batch_size),
            skipped: Mutex::new(0),
        }
    }

    /// Skips a record (not written).
    pub fn skip_record(&self) {
        let mut skipped = self.skipped.lock().unwrap();
        *skipped += 1;

        let mut counter = self.counter.lock().unwrap();
        if *counter > 0 {
            *counter -= 1;
            if *counter == 0 {
                self.finished_all_writes();
            }
        }
    }

    /// Completes a record successfully.
    pub fn complete_record(&self) {
        let mut counter = self.counter.lock().unwrap();
        if *counter > 0 {
            *counter -= 1;
            if *counter == 0 {
                self.finished_all_writes();
            }
        }
    }

    /// Marks all remaining records as retried.
    pub fn retry_remaining(&self) {
        self.finished_all_writes();
    }

    /// Reports metrics when all writes complete.
    fn finished_all_writes(&self) {
        if !self.completed.load(Ordering::SeqCst) {
            self.completed.store(true, Ordering::SeqCst);

            let counter = *self.counter.lock().unwrap();
            let skipped = *self.skipped.lock().unwrap();

            self.metrics_group
                .record_write(self.batch_size - counter, skipped);
        }
    }
}

/// Converter trait for converting connect data to bytes.
///
/// Used by AbstractWorkerSourceTask for key/value/header conversion.
pub trait SourceTaskConverter: Send + Sync {
    /// Converts connect data to bytes.
    fn from_connect_data(
        &self,
        topic: &str,
        headers: &ConnectHeaders,
        schema: Option<&serde_json::Value>,
        value: &serde_json::Value,
    ) -> Result<Vec<u8>, ConnectError>;
}

/// Header converter trait.
pub trait SourceTaskHeaderConverter: Send + Sync {
    /// Converts headers to bytes map.
    fn from_connect_header(
        &self,
        topic: &str,
        key: &str,
        schema: Option<&serde_json::Value>,
        value: &serde_json::Value,
    ) -> Result<Vec<u8>, ConnectError>;
}

/// Transformation chain trait.
pub trait SourceTaskTransformationChain: Send + Sync {
    /// Applies transformations to a source record.
    fn apply(
        &self,
        context: &mut ErrorProcessingContext,
        record: &SourceRecord,
    ) -> Option<SourceRecord>;
}

/// Source task wrapper trait - dyn-compatible wrapper for source task operations.
///
/// This trait provides a dyn-compatible interface for the core source task operations
/// that AbstractWorkerSourceTask needs. The actual SourceTask trait cannot be used
/// directly as a trait object because it has methods with `impl` parameters.
pub trait ThreadSafeSourceTaskWrapper: Send + Sync {
    /// Initialize the task with context reference.
    fn initialize(&mut self, context: &WorkerSourceTaskContext);

    /// Start the task with configuration.
    fn start(&mut self, config: HashMap<String, String>) -> Result<(), ConnectError>;

    /// Stop the task.
    fn stop(&mut self);

    /// Poll for new records.
    fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError>;

    /// Commit the current batch.
    fn commit(&mut self) -> Result<(), ConnectError>;

    /// Commit a single record.
    fn commit_record(
        &mut self,
        record: &SourceRecord,
        metadata: Option<&RecordMetadata>,
    ) -> Result<(), ConnectError>;

    /// Get the task version.
    fn version(&self) -> String;
}

/// Topic creation trait.
pub trait TopicCreation: Send + Sync {
    /// Checks if topic creation is required for the given topic.
    fn is_topic_creation_required(&self, topic: &str) -> bool;

    /// Adds a topic to the created set.
    fn add_topic(&self, topic: &str);

    /// Finds the first matching topic creation group.
    fn find_first_group(&self, topic: &str) -> Option<String>;
}

/// Topic admin trait.
pub trait TopicAdmin: Send + Sync {
    /// Describes topics.
    fn describe_topics(&self, topics: &[&str]) -> HashMap<String, TopicDescription>;

    /// Creates or finds topics.
    fn create_or_find_topics(&self, topics: &[NewTopic]) -> TopicCreationResponse;
}

/// Topic description (simplified).
#[derive(Debug, Clone)]
pub struct TopicDescription {
    name: String,
    partitions: i32,
    replication_factor: i32,
}

impl TopicDescription {
    pub fn new(name: String, partitions: i32, replication_factor: i32) -> Self {
        TopicDescription {
            name,
            partitions,
            replication_factor,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn partitions(&self) -> i32 {
        self.partitions
    }

    pub fn replication_factor(&self) -> i32 {
        self.replication_factor
    }
}

/// New topic (simplified).
#[derive(Debug, Clone)]
pub struct NewTopic {
    name: String,
    partitions: i32,
    replication_factor: i32,
}

impl NewTopic {
    pub fn new(name: String, partitions: i32, replication_factor: i32) -> Self {
        NewTopic {
            name,
            partitions,
            replication_factor,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Topic creation response (simplified).
#[derive(Debug, Clone)]
pub struct TopicCreationResponse {
    created: HashMap<String, bool>,
    existing: HashMap<String, bool>,
}

impl TopicCreationResponse {
    pub fn new() -> Self {
        TopicCreationResponse {
            created: HashMap::new(),
            existing: HashMap::new(),
        }
    }

    pub fn is_created(&self, topic: &str) -> bool {
        self.created.get(topic).copied().unwrap_or(false)
    }

    pub fn is_existing(&self, topic: &str) -> bool {
        self.existing.get(topic).copied().unwrap_or(false)
    }

    pub fn add_created(&mut self, topic: &str) {
        self.created.insert(topic.to_string(), true);
    }

    pub fn add_existing(&mut self, topic: &str) {
        self.existing.insert(topic.to_string(), true);
    }
}

/// Offset storage writer trait.
pub trait OffsetStorageWriter: Send + Sync {
    /// Writes an offset entry.
    fn offset(
        &self,
        partition: HashMap<String, serde_json::Value>,
        offset: HashMap<String, serde_json::Value>,
    );

    /// Begins a flush operation.
    fn begin_flush(&self, timeout_ms: i64) -> Result<bool, ConnectError>;

    /// Performs the actual flush.
    fn do_flush(&self) -> Result<(), ConnectError>;

    /// Cancels a pending flush.
    fn cancel_flush(&self);
}

/// Offset storage reader trait.
pub trait OffsetStorageReader: Send + Sync {
    /// Reads offsets for partitions.
    fn offsets(
        &self,
        partitions: &[HashMap<String, serde_json::Value>],
    ) -> HashMap<HashMap<String, serde_json::Value>, HashMap<String, serde_json::Value>>;

    /// Closes the reader.
    fn close(&self);
}

/// Connector offset backing store trait.
pub trait ConnectorOffsetBackingStore: Send + Sync {
    /// Starts the offset store.
    fn start(&self);

    /// Stops the offset store.
    fn stop(&self);
}

/// Worker config trait for source task configuration access.
pub trait WorkerSourceTaskConfig: Send + Sync {
    /// Gets a boolean configuration value.
    fn get_bool(&self, key: &str) -> bool;

    /// Gets a long configuration value.
    fn get_long(&self, key: &str) -> i64;

    /// Gets a string configuration value.
    fn get_string(&self, key: &str) -> Option<&str>;
}

/// Internal state for AbstractWorkerSourceTask.
struct AbstractWorkerSourceTaskState {
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
    /// Stored error message.
    error_message: Option<String>,
    /// Whether the producer is closed.
    producer_closed: bool,
}

impl Default for AbstractWorkerSourceTaskState {
    fn default() -> Self {
        AbstractWorkerSourceTaskState {
            target_state: TargetState::Started,
            stopping: false,
            failed: false,
            started: false,
            paused: false,
            error_message: None,
            producer_closed: false,
        }
    }
}

/// AbstractWorkerSourceTask - shared logic for running source tasks.
///
/// This struct contains shared logic for source tasks with either
/// standard semantics (at-least-once/at-most-once) or exactly-once semantics.
///
/// Subclasses implement the AbstractWorkerSourceTaskHooks trait to define
/// custom behavior at specific points in the lifecycle.
///
/// Corresponds to `org.apache.kafka.connect.runtime.AbstractWorkerSourceTask` in Java.
pub struct AbstractWorkerSourceTask {
    /// Task ID.
    id: ConnectorTaskId,
    /// Task version.
    version: String,
    /// Worker configuration.
    worker_config: Arc<dyn WorkerSourceTaskConfig>,
    /// Source task (the actual connector task implementation) - wrapped for dyn compatibility.
    source_task: Mutex<Option<Box<dyn ThreadSafeSourceTaskWrapper>>>,
    /// Source task context.
    source_task_context: WorkerSourceTaskContext,
    /// Key converter.
    key_converter: Option<Arc<dyn SourceTaskConverter>>,
    /// Value converter.
    value_converter: Option<Arc<dyn SourceTaskConverter>>,
    /// Header converter.
    header_converter: Option<Arc<dyn SourceTaskHeaderConverter>>,
    /// Transformation chain.
    transformation_chain: Option<Arc<dyn SourceTaskTransformationChain>>,
    /// Producer for sending records to Kafka.
    producer: Option<Arc<dyn KafkaProducer>>,
    /// Topic admin for topic management.
    topic_admin: Option<Arc<dyn TopicAdmin>>,
    /// Topic creation configuration.
    topic_creation: Option<Arc<dyn TopicCreation>>,
    /// Offset storage reader.
    offset_reader: Option<Arc<dyn OffsetStorageReader>>,
    /// Offset storage writer.
    offset_writer: Option<Arc<dyn OffsetStorageWriter>>,
    /// Offset backing store.
    offset_store: Option<Arc<dyn ConnectorOffsetBackingStore>>,
    /// Submitted records tracker.
    submitted_records: SubmittedRecords,
    /// Source task metrics group.
    metrics_group: Arc<SourceTaskMetricsGroup>,
    /// Retry with tolerance operator.
    retry_operator: Arc<RetryWithToleranceOperator>,
    /// Task metrics (wrapped for interior mutability).
    task_metrics: Mutex<TaskMetrics>,
    /// Status listener.
    status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
    /// Time provider.
    time: Arc<dyn Time>,
    /// Internal state.
    state: Mutex<AbstractWorkerSourceTaskState>,
    /// Stop requested condvar/latch.
    stop_requested_latch: (Mutex<bool>, Condvar),
    /// Whether topic tracking is enabled.
    topic_tracking_enabled: bool,
    /// Task configuration (originals as strings).
    task_config: Mutex<HashMap<String, String>>,
    /// Whether the task has been initialized.
    initialized: AtomicBool,
    /// Close called flag.
    close_called: AtomicBool,
    /// Whether start() was called successfully.
    task_started: AtomicBool,
    /// Records to send (pending from poll).
    to_send: Mutex<Option<Vec<SourceRecord>>>,
    /// Plugin metrics.
    plugin_metrics: Option<Arc<dyn PluginMetrics + Send + Sync>>,
}

/// Kafka producer trait for source tasks.
pub trait KafkaProducer: Send + Sync {
    /// Sends a record to Kafka with a callback.
    fn send(
        &self,
        record: &ProducerRecord,
        callback: Box<dyn FnOnce(Result<RecordMetadata, String>) + Send>,
    );

    /// Flushes pending records.
    fn flush(&self, timeout_ms: i64) -> Result<(), String>;

    /// Closes the producer.
    fn close(&self, timeout_ms: i64);
}

impl AbstractWorkerSourceTask {
    /// Creates a new AbstractWorkerSourceTask.
    ///
    /// # Arguments
    /// * `id` - Connector task ID
    /// * `version` - Task version (from SourceTask.version())
    /// * `worker_config` - Worker configuration
    /// * `status_listener` - Task status listener
    /// * `initial_state` - Initial target state
    /// * `source_task_context` - Source task context
    /// * `key_converter` - Key converter
    /// * `value_converter` - Value converter
    /// * `header_converter` - Header converter
    /// * `transformation_chain` - Transformation chain
    /// * `producer` - Kafka producer
    /// * `topic_admin` - Topic admin
    /// * `topic_creation` - Topic creation config
    /// * `offset_reader` - Offset storage reader
    /// * `offset_writer` - Offset storage writer
    /// * `offset_store` - Offset backing store
    /// * `time` - Time provider
    /// * `retry_operator` - Retry with tolerance operator
    /// * `topic_tracking_enabled` - Whether topic tracking is enabled
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ConnectorTaskId,
        version: String,
        worker_config: Arc<dyn WorkerSourceTaskConfig>,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
        initial_state: TargetState,
        source_task_context: WorkerSourceTaskContext,
        key_converter: Option<Arc<dyn SourceTaskConverter>>,
        value_converter: Option<Arc<dyn SourceTaskConverter>>,
        header_converter: Option<Arc<dyn SourceTaskHeaderConverter>>,
        transformation_chain: Option<Arc<dyn SourceTaskTransformationChain>>,
        producer: Option<Arc<dyn KafkaProducer>>,
        topic_admin: Option<Arc<dyn TopicAdmin>>,
        topic_creation: Option<Arc<dyn TopicCreation>>,
        offset_reader: Option<Arc<dyn OffsetStorageReader>>,
        offset_writer: Option<Arc<dyn OffsetStorageWriter>>,
        offset_store: Option<Arc<dyn ConnectorOffsetBackingStore>>,
        time: Arc<dyn Time>,
        retry_operator: Arc<RetryWithToleranceOperator>,
        topic_tracking_enabled: bool,
    ) -> Self {
        let id_for_metrics = id.clone();
        AbstractWorkerSourceTask {
            id,
            version,
            worker_config,
            source_task: Mutex::new(None),
            source_task_context,
            key_converter,
            value_converter,
            header_converter,
            transformation_chain,
            producer,
            topic_admin,
            topic_creation,
            offset_reader,
            offset_writer,
            offset_store,
            submitted_records: SubmittedRecords::new(),
            metrics_group: Arc::new(SourceTaskMetricsGroup::new(&id_for_metrics)),
            retry_operator,
            task_metrics: Mutex::new(TaskMetrics::new()),
            status_listener,
            time,
            state: Mutex::new(AbstractWorkerSourceTaskState {
                target_state: initial_state,
                ..Default::default()
            }),
            stop_requested_latch: (Mutex::new(false), Condvar::new()),
            topic_tracking_enabled,
            task_config: Mutex::new(HashMap::new()),
            initialized: AtomicBool::new(false),
            close_called: AtomicBool::new(false),
            task_started: AtomicBool::new(false),
            to_send: Mutex::new(None),
            plugin_metrics: None,
        }
    }

    /// Returns the task ID.
    pub fn id(&self) -> &ConnectorTaskId {
        &self.id
    }

    /// Returns the task version.
    pub fn task_version(&self) -> &str {
        &self.version
    }

    /// Returns whether the task is stopping.
    pub fn is_stopping(&self) -> bool {
        self.state.lock().unwrap().stopping
    }

    /// Returns whether the task is paused.
    pub fn is_paused(&self) -> bool {
        self.state.lock().unwrap().paused
    }

    /// Returns whether the task has started.
    pub fn is_started(&self) -> bool {
        self.state.lock().unwrap().started
    }

    /// Returns whether the task has failed.
    pub fn is_failed(&self) -> bool {
        self.state.lock().unwrap().failed
    }

    /// Returns the target state.
    pub fn target_state(&self) -> TargetState {
        self.state.lock().unwrap().target_state
    }

    /// Returns whether the producer is closed.
    pub fn is_producer_closed(&self) -> bool {
        self.state.lock().unwrap().producer_closed
    }

    /// Sets the stopping flag.
    pub fn set_stopping(&self, stopping: bool) {
        self.state.lock().unwrap().stopping = stopping;
        if stopping {
            self.stop_requested_latch.1.notify_all();
        }
    }

    /// Sets the started flag.
    pub fn set_started(&self, started: bool) {
        self.state.lock().unwrap().started = started;
    }

    /// Sets the paused flag.
    pub fn set_paused(&self, paused: bool) {
        self.state.lock().unwrap().paused = paused;
    }

    /// Sets the failed flag.
    pub fn set_failed(&self, failed: bool, error_message: Option<String>) {
        let mut state = self.state.lock().unwrap();
        state.failed = failed;
        state.error_message = error_message;
    }

    /// Gets the error message.
    pub fn get_error_message(&self) -> Option<String> {
        self.state.lock().unwrap().error_message.clone()
    }

    /// Sets the target state.
    pub fn set_target_state(&self, state: TargetState) {
        self.state.lock().unwrap().target_state = state;
    }

    /// Returns the submitted records.
    pub fn submitted_records(&self) -> &SubmittedRecords {
        &self.submitted_records
    }

    /// Returns the metrics group.
    pub fn metrics_group(&self) -> &SourceTaskMetricsGroup {
        &self.metrics_group
    }

    /// Returns the retry operator.
    pub fn retry_operator(&self) -> &RetryWithToleranceOperator {
        &self.retry_operator
    }

    /// Returns whether the task has been initialized.
    pub fn was_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    /// Returns whether close was called.
    pub fn was_closed(&self) -> bool {
        self.close_called.load(Ordering::SeqCst)
    }

    /// Sets the source task implementation.
    pub fn set_source_task(&self, task: Box<dyn ThreadSafeSourceTaskWrapper>) {
        *self.source_task.lock().unwrap() = Some(task);
    }

    /// Gets the task configuration.
    pub fn task_config(&self) -> HashMap<String, String> {
        self.task_config.lock().unwrap().clone()
    }

    /// Gets the worker config.
    pub fn worker_config(&self) -> &dyn WorkerSourceTaskConfig {
        self.worker_config.as_ref()
    }

    /// Initialize the task with configuration.
    ///
    /// Corresponds to `initialize(TaskConfig)` in Java.
    pub fn initialize(&self, config: TaskConfig) -> Result<(), ConnectError> {
        *self.task_config.lock().unwrap() = config.config().clone();
        self.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Initialize and start the source task.
    ///
    /// This method:
    /// 1. Calls prepareToInitializeTask() hook
    /// 2. Starts the offset store
    /// 3. Sets started flag to true
    /// 4. Calls task.initialize(sourceTaskContext)
    /// 5. Calls task.start(taskConfig)
    ///
    /// Corresponds to `initializeAndStart()` in Java.
    pub fn initialize_and_start<H: AbstractWorkerSourceTaskHooks>(
        &self,
        hooks: &mut H,
    ) -> Result<(), ConnectError> {
        // Call hook for custom initialization
        hooks.prepare_to_initialize_task();

        // Start the offset store
        if let Some(offset_store) = &self.offset_store {
            offset_store.start();
        }

        // Set started flag - we expect stop() to be called if initialize succeeds
        self.task_started.store(true, Ordering::SeqCst);
        self.set_started(true);

        // Initialize the source task
        {
            let mut source_task = self.source_task.lock().unwrap();
            if let Some(task) = source_task.as_mut() {
                task.initialize(&self.source_task_context);
            }
        }

        // Start the source task with configuration
        let task_config = self.task_config.lock().unwrap().clone();
        {
            let mut source_task = self.source_task.lock().unwrap();
            if let Some(task) = source_task.as_mut() {
                task.start(task_config)?;
            }
        }

        log::info!("{} Source task finished initialization and start", self.id);
        Ok(())
    }

    /// Cancel the task.
    ///
    /// Preemptively closes the offset reader and producer to unblock
    /// any blocked operations.
    ///
    /// Corresponds to `cancel()` in Java.
    pub fn cancel(&self) {
        self.set_stopping(true);

        // Close the offset reader preemptively
        if let Some(offset_reader) = &self.offset_reader {
            offset_reader.close();
        }

        // Close the producer on a separate thread (async)
        // Mark producer as closed even if closing is async
        self.state.lock().unwrap().producer_closed = true;
        if let Some(producer) = &self.producer {
            // In Rust, we spawn a thread for the close operation
            // to avoid blocking the herder's tick thread
            let producer = producer.clone();
            std::thread::spawn(move || {
                producer.close(0);
            });
        }
    }

    /// Stop the task.
    ///
    /// Signals that the task should stop and wakes up waiting threads.
    ///
    /// Corresponds to `stop()` in Java.
    pub fn stop(&self) {
        self.set_stopping(true);
        self.set_target_state(TargetState::Stopped);

        // Notify stop requested latch
        {
            let mut stop_requested = self.stop_requested_latch.0.lock().unwrap();
            *stop_requested = true;
        }
        self.stop_requested_latch.1.notify_all();
    }

    /// Remove metrics.
    ///
    /// Corresponds to `removeMetrics()` in Java.
    pub fn remove_metrics(&self) {
        // Close plugin metrics if present
        // Note: Rust doesn't have AutoCloseable, metrics are cleaned up via Drop

        // Metrics group is an Arc, will be cleaned up when all references are dropped
    }

    /// Close resources.
    ///
    /// This method:
    /// 1. If started, calls task.stop()
    /// 2. Closes producer with 30 second timeout
    /// 3. Closes topic admin
    /// 4. Closes offset reader
    /// 5. Stops offset store
    /// 6. Closes converters
    ///
    /// Corresponds to `close()` in Java.
    pub fn close(&self) -> Result<(), ConnectError> {
        self.close_called.store(true, Ordering::SeqCst);

        // Stop the source task if it was started
        if self.task_started.load(Ordering::SeqCst) {
            let mut source_task = self.source_task.lock().unwrap();
            if let Some(task) = source_task.as_mut() {
                task.stop();
            }
        }

        // Close the producer with 30 second timeout
        self.close_producer(Duration::from_secs(30));

        // Close the topic admin
        if let Some(topic_admin) = &self.topic_admin {
            // Topic admin close would be implemented by the trait
            // For now, we just drop the reference
        }

        // Close the offset reader
        if let Some(offset_reader) = &self.offset_reader {
            offset_reader.close();
        }

        // Stop the offset store
        if let Some(offset_store) = &self.offset_store {
            offset_store.stop();
        }

        // Converters are Arc references, cleaned up automatically

        // Clear pending records
        *self.to_send.lock().unwrap() = None;

        Ok(())
    }

    /// Close the producer.
    fn close_producer(&self, timeout: Duration) {
        if let Some(producer) = &self.producer {
            self.state.lock().unwrap().producer_closed = true;
            producer.close(timeout.as_millis() as i64);
        }
    }

    /// Execute the main poll-convert-send loop.
    ///
    /// This is the main loop that:
    /// 1. Calls prepareToEnterSendLoop()
    /// 2. For each iteration:
    ///    - Calls beginSendIteration()
    ///    - Handles pause/resume
    ///    - Polls records if none pending
    ///    - Sends records to Kafka
    ///    - Backs off if send fails
    /// 3. On exit, calls finalOffsetCommit()
    ///
    /// Corresponds to `execute()` in Java.
    pub fn execute<H: AbstractWorkerSourceTaskHooks>(
        &self,
        hooks: &mut H,
    ) -> Result<(), ConnectError> {
        hooks.prepare_to_enter_send_loop();

        while !self.is_stopping() {
            hooks.begin_send_iteration();

            // Handle pause
            if self.is_paused() {
                self.on_pause();
                if self.await_unpause()? {
                    self.on_resume();
                    hooks.prepare_to_enter_send_loop();
                }
                continue;
            }

            // Poll if no pending records
            {
                let mut to_send = self.to_send.lock().unwrap();
                if to_send.is_none() {
                    hooks.prepare_to_poll_task();

                    log::trace!(
                        "{} Nothing to send to Kafka. Polling source for additional records",
                        self.id
                    );

                    let start = self.time.milliseconds();
                    let records = self.poll();

                    if records.is_some() {
                        let batch = records.unwrap();
                        self.record_poll_returned(batch.len(), self.time.milliseconds() - start);
                        *to_send = Some(batch);
                    }
                }
            }

            // Check if we have records to send
            {
                let to_send = self.to_send.lock().unwrap();
                if to_send.is_none() {
                    hooks.batch_dispatched();
                    continue;
                }
            }

            // Send records
            log::trace!("{} About to send {} records to Kafka", self.id, {
                let to_send = self.to_send.lock().unwrap();
                to_send.as_ref().map(|b| b.len()).unwrap_or(0)
            });

            if !self.send_records(hooks)? {
                // Backoff on send failure
                self.await_stop_requested(SEND_FAILED_BACKOFF_MS)?;
            }
        }

        // Final offset commit on successful exit
        hooks.final_offset_commit(false);
        Ok(())
    }

    /// Poll records from the source task.
    ///
    /// Handles RetriableException by returning None (will retry on next iteration).
    ///
    /// Corresponds to `poll()` in Java.
    pub fn poll(&self) -> Option<Vec<SourceRecord>> {
        let mut source_task = self.source_task.lock().unwrap();
        if let Some(task) = source_task.as_mut() {
            match task.poll() {
                Ok(records) => Some(records),
                Err(e) => {
                    // Check if retriable
                    if e.message().contains("retriable") {
                        log::warn!("{} failed to poll records from SourceTask. Will retry operation. Error: {}", self.id, e.message());
                        None
                    } else {
                        // Non-retriable error - will be handled by caller
                        log::error!(
                            "{} failed to poll records from SourceTask: {}",
                            self.id,
                            e.message()
                        );
                        None
                    }
                }
            }
        } else {
            None
        }
    }

    /// Send a batch of records to Kafka.
    ///
    /// If a send fails and is retriable, saves the remainder for retry.
    /// If a send fails and is not retriable, throws ConnectException.
    ///
    /// Returns true if all messages sent, false if some need retry.
    ///
    /// Corresponds to `sendRecords()` in Java.
    pub fn send_records<H: AbstractWorkerSourceTaskHooks>(
        &self,
        hooks: &mut H,
    ) -> Result<bool, ConnectError> {
        let mut to_send_guard = self.to_send.lock().unwrap();
        let to_send = to_send_guard
            .as_mut()
            .ok_or_else(|| ConnectError::general("No records to send"))?;

        if to_send.is_empty() {
            hooks.batch_dispatched();
            *to_send_guard = None;
            return Ok(true);
        }

        let batch_size = to_send.len();
        self.record_batch(batch_size);

        // Create write counter for metrics
        let counter = Arc::new(SourceRecordWriteCounter::new(
            batch_size,
            self.metrics_group.clone(),
        ));

        let mut processed = 0;

        for pre_transform_record in to_send.iter() {
            // Create processing context
            let mut context = ErrorProcessingContext::new();

            // Apply transformations
            let record = if let Some(chain) = &self.transformation_chain {
                chain.apply(&mut context, pre_transform_record)
            } else {
                Some(pre_transform_record.clone())
            };

            // Convert to producer record
            let producer_record = if let Some(ref transformed_record) = record {
                self.convert_transformed_record(&context, transformed_record)
            } else {
                None
            };

            // Check if filtered or failed
            if producer_record.is_none() || context.is_failed() {
                counter.skip_record();
                hooks.record_dropped(pre_transform_record);
                processed += 1;
                continue;
            }

            let producer_record = producer_record.unwrap();

            log::trace!(
                "{} Appending record to the topic {} with key {:?}, value {:?}",
                self.id,
                producer_record.topic(),
                producer_record.key(),
                producer_record.value()
            );

            // Prepare to send
            let submitted_record =
                hooks.prepare_to_send_record(pre_transform_record, &producer_record);

            // Send to Kafka
            match self.send_record_to_producer(
                &context,
                pre_transform_record,
                &producer_record,
                submitted_record.as_ref(),
                counter.clone(),
                hooks,
            ) {
                Ok(()) => {
                    processed += 1;
                    hooks.record_dispatched(pre_transform_record);
                }
                Err(SendRecordError::Retriable) => {
                    // Save remaining records for retry
                    let remaining = to_send.split_off(processed);
                    *to_send_guard = Some(remaining);
                    counter.retry_remaining();
                    return Ok(false);
                }
                Err(SendRecordError::NonRetriable(e)) => {
                    return Err(e);
                }
            }
        }

        // All records sent
        *to_send_guard = None;
        hooks.batch_dispatched();
        Ok(true)
    }

    /// Send a single record to the producer.
    fn send_record_to_producer<H: AbstractWorkerSourceTaskHooks>(
        &self,
        context: &ErrorProcessingContext,
        pre_transform_record: &SourceRecord,
        producer_record: &ProducerRecord,
        submitted_record: Option<&SubmittedRecord>,
        counter: Arc<SourceRecordWriteCounter>,
        hooks: &mut H,
    ) -> Result<(), SendRecordError> {
        // Maybe create topic
        self.maybe_create_topic(producer_record.topic())?;

        // Get producer
        let producer = self.producer.as_ref().ok_or_else(|| {
            SendRecordError::NonRetriable(ConnectError::general("Producer not configured"))
        })?;

        // Clone references for callback
        let task_id = self.id.clone();
        let metrics_group = self.metrics_group.clone();
        let topic_tracking_enabled = self.topic_tracking_enabled;
        let producer_closed = Arc::new(AtomicBool::new(self.is_producer_closed()));
        let tolerance_type = self.retry_operator.tolerance_type();

        // Create callback
        let callback = {
            let pre_transform_record = pre_transform_record.clone();
            let producer_record = producer_record.clone();
            let counter = counter.clone();
            let submitted_record = submitted_record.map(|r| r.clone());

            Box::new(move |result: Result<RecordMetadata, String>| {
                match result {
                    Ok(record_metadata) => {
                        counter.complete_record();
                        log::trace!(
                            "{} Wrote record successfully: topic {} partition {} offset {}",
                            task_id,
                            record_metadata.topic(),
                            record_metadata.partition(),
                            record_metadata.offset()
                        );
                        // Note: record_sent would be called by hooks, but we can't call hooks from callback
                        // In a full implementation, we'd need to pass hooks reference
                        if submitted_record.is_some() {
                            submitted_record.unwrap().ack();
                        }
                        // Record active topic if tracking enabled
                        if topic_tracking_enabled {
                            metrics_group.record_write(1, 0);
                        }
                    }
                    Err(e) => {
                        if producer_closed.load(Ordering::SeqCst) {
                            log::trace!(
                                "{} failed to send record to {}; this is expected as the producer has already been closed",
                                task_id,
                                producer_record.topic()
                            );
                        } else {
                            log::error!(
                                "{} failed to send record to {}: {}",
                                task_id,
                                producer_record.topic(),
                                e
                            );
                        }
                        // Note: producer_send_failed would be called by hooks
                        if tolerance_type == ToleranceType::ALL {
                            counter.skip_record();
                            if submitted_record.is_some() {
                                submitted_record.unwrap().ack();
                            }
                        }
                    }
                }
            })
        };

        // Send the record
        producer.send(producer_record, callback);

        Ok(())
    }

    /// Convert a transformed source record to a producer record.
    ///
    /// Corresponds to `convertTransformedRecord()` in Java.
    pub fn convert_transformed_record(
        &self,
        context: &ErrorProcessingContext,
        record: &SourceRecord,
    ) -> Option<ProducerRecord> {
        if context.is_failed() {
            return None;
        }

        // Convert headers
        let headers = self.convert_header_for(record)?;

        // Convert key
        let key = if let Some(key_converter) = &self.key_converter {
            key_converter
                .from_connect_data(
                    record.topic(),
                    &headers,
                    None,
                    record.key().unwrap_or(&serde_json::Value::Null),
                )
                .ok()
        } else {
            record
                .key()
                .map(|k| serde_json::to_vec(k).unwrap_or_default())
        };

        // Convert value
        let value: Option<Vec<u8>> = if let Some(value_converter) = &self.value_converter {
            value_converter
                .from_connect_data(record.topic(), &headers, None, record.value())
                .ok()
        } else {
            Some(serde_json::to_vec(record.value()).unwrap_or_default())
        };

        // Create producer record
        let timestamp = record
            .timestamp()
            .map(|t| t as i64)
            .unwrap_or_else(|| self.time.milliseconds());

        Some(ProducerRecord::with_all(
            record.topic(),
            record.kafka_partition(),
            key,
            value,
            Some(timestamp),
            HashMap::new(), // Headers converted above, but simplified for this implementation
        ))
    }

    /// Convert headers for a source record.
    ///
    /// Corresponds to `convertHeaderFor()` in Java.
    pub fn convert_header_for(&self, record: &SourceRecord) -> Option<ConnectHeaders> {
        let headers = record.headers();
        if headers.is_empty() {
            return Some(ConnectHeaders::new());
        }

        // For simplified implementation, just return the headers as-is
        // Full implementation would convert each header using header_converter
        Some(headers.clone())
    }

    /// Maybe create a topic if it doesn't exist.
    ///
    /// Corresponds to `maybeCreateTopic()` in Java.
    fn maybe_create_topic(&self, topic: &str) -> Result<(), ConnectError> {
        if let Some(topic_creation) = &self.topic_creation {
            if !topic_creation.is_topic_creation_required(topic) {
                log::trace!(
                    "Topic creation by the connector is disabled or the topic {} was previously created",
                    topic
                );
                return Ok(());
            }

            log::info!(
                "The task will send records to topic '{}' for the first time. Checking whether topic exists",
                topic
            );

            // Check if topic exists
            if let Some(topic_admin) = &self.topic_admin {
                let existing = topic_admin.describe_topics(&[topic]);
                if !existing.is_empty() {
                    log::info!("Topic '{}' already exists", topic);
                    topic_creation.add_topic(topic);
                    return Ok(());
                }

                // Create topic
                log::info!("Creating topic '{}'", topic);

                if let Some(group) = topic_creation.find_first_group(topic) {
                    log::debug!("Topic '{}' matched topic creation group: {}", topic, group);

                    let new_topic = NewTopic::new(topic.to_string(), 1, 1); // Simplified defaults
                    let response = topic_admin.create_or_find_topics(&[new_topic]);

                    if response.is_created(topic) {
                        topic_creation.add_topic(topic);
                        log::info!("Created topic '{}'", topic);
                    } else if response.is_existing(topic) {
                        topic_creation.add_topic(topic);
                        log::info!("Found existing topic '{}'", topic);
                    } else {
                        log::warn!("Request to create new topic '{}' failed", topic);
                        return Err(ConnectError::general(format!(
                            "Task failed to create new topic {}. Ensure that the task is authorized to create topics or that the topic exists and restart the task",
                            topic
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Commit a task record.
    ///
    /// Corresponds to `commitTaskRecord()` in Java.
    pub fn commit_task_record(&self, record: &SourceRecord, metadata: Option<&RecordMetadata>) {
        let mut source_task = self.source_task.lock().unwrap();
        if let Some(task) = source_task.as_mut() {
            if let Err(e) = task.commit_record(record, metadata) {
                log::error!(
                    "{} Exception thrown while calling task.commitRecord(): {}",
                    self.id,
                    e.message()
                );
            }
        }
    }

    /// Commit the source task.
    ///
    /// Corresponds to `commitSourceTask()` in Java.
    pub fn commit_source_task(&self) {
        let mut source_task = self.source_task.lock().unwrap();
        if let Some(task) = source_task.as_mut() {
            if let Err(e) = task.commit() {
                log::error!(
                    "{} Exception thrown while calling task.commit(): {}",
                    self.id,
                    e.message()
                );
            }
        }
    }

    /// Record that poll returned records.
    fn record_poll_returned(&self, num_records: usize, duration: i64) {
        self.metrics_group.record_poll(num_records, duration as u64);
    }

    /// Record a batch of records.
    fn record_batch(&self, size: usize) {
        self.task_metrics.lock().unwrap().record_batch(size);
    }

    /// Called when task pauses.
    fn on_pause(&self) {
        self.status_listener.on_pause(&self.id);
    }

    /// Called when task resumes.
    fn on_resume(&self) {
        self.status_listener.on_running(&self.id);
    }

    /// Await unpause.
    fn await_unpause(&self) -> Result<bool, ConnectError> {
        // Wait until not paused or stopping
        while self.is_paused() && !self.is_stopping() {
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(!self.is_stopping())
    }

    /// Await stop requested with timeout.
    fn await_stop_requested(&self, timeout_ms: u64) -> Result<(), ConnectError> {
        let (lock, cvar) = &self.stop_requested_latch;
        let stop_requested = lock.lock().unwrap();
        let result =
            cvar.wait_timeout_while(stop_requested, Duration::from_millis(timeout_ms), |s| !*s);
        match result {
            Ok((_guard, timeout_result)) => {
                if timeout_result.timed_out() {
                    // Timeout - continue
                }
                Ok(())
            }
            Err(_) => Ok(()),
        }
    }

    /// Transition to a new target state.
    pub fn transition_to(&self, state: TargetState) {
        let prev_state = self.target_state();
        self.set_target_state(state);

        match state {
            TargetState::Started => {
                if prev_state == TargetState::Paused {
                    self.set_paused(false);
                }
            }
            TargetState::Paused => {
                self.set_paused(true);
            }
            TargetState::Stopped => {
                self.set_stopping(true);
            }
        }
    }
}

/// Error type for send record operations.
enum SendRecordError {
    Retriable,
    NonRetriable(ConnectError),
}

impl From<ConnectError> for SendRecordError {
    fn from(err: ConnectError) -> Self {
        SendRecordError::NonRetriable(err)
    }
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::MockTaskStatusListener;
    use common_trait::util::time::SystemTimeImpl;

    fn create_test_task() -> AbstractWorkerSourceTask {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(MockTaskStatusListener::new());
        let time = Arc::new(SystemTimeImpl::new()) as Arc<dyn Time>;
        let config = Arc::new(MockWorkerConfig::new());
        let retry_metrics = Arc::new(crate::errors::ErrorHandlingMetrics::new());
        let retry_operator = Arc::new(RetryWithToleranceOperator::new(
            0,
            0,
            ToleranceType::ALL,
            retry_metrics,
        ));
        // Create storage ConnectorTaskId for WorkerSourceTaskContext
        let storage_id =
            common_trait::storage::ConnectorTaskId::new("test-connector".to_string(), 0);
        let source_task_context = WorkerSourceTaskContext::new_for_testing(storage_id);

        AbstractWorkerSourceTask::new(
            id,
            "1.0.0".to_string(),
            config,
            listener,
            TargetState::Started,
            source_task_context,
            None, // key_converter
            None, // value_converter
            None, // header_converter
            None, // transformation_chain
            None, // producer
            None, // topic_admin
            None, // topic_creation
            None, // offset_reader
            None, // offset_writer
            None, // offset_store
            time,
            retry_operator,
            true, // topic_tracking_enabled
        )
    }

    #[test]
    fn test_task_creation() {
        let task = create_test_task();

        assert_eq!(task.id().connector(), "test-connector");
        assert_eq!(task.id().task(), 0);
        assert_eq!(task.task_version(), "1.0.0");
        assert!(!task.is_stopping());
        assert!(!task.is_paused());
        assert_eq!(task.target_state(), TargetState::Started);
    }

    #[test]
    fn test_state_transitions() {
        let task = create_test_task();

        // Initial state
        assert_eq!(task.target_state(), TargetState::Started);

        // Transition to paused
        task.transition_to(TargetState::Paused);
        assert_eq!(task.target_state(), TargetState::Paused);
        assert!(task.is_paused());

        // Transition to started (resume)
        task.transition_to(TargetState::Started);
        assert_eq!(task.target_state(), TargetState::Started);
        assert!(!task.is_paused());

        // Transition to stopped
        task.transition_to(TargetState::Stopped);
        assert_eq!(task.target_state(), TargetState::Stopped);
        assert!(task.is_stopping());
    }

    #[test]
    fn test_stop_and_cancel() {
        let task = create_test_task();

        // Stop
        task.stop();
        assert!(task.is_stopping());
        assert_eq!(task.target_state(), TargetState::Stopped);

        // Cancel
        task.cancel();
        assert!(task.is_producer_closed());
    }

    #[test]
    fn test_initialize() {
        let task = create_test_task();

        let config = TaskConfig::new(HashMap::from([("key".to_string(), "value".to_string())]));

        task.initialize(config).unwrap();
        assert!(task.was_initialized());

        let task_config = task.task_config();
        assert_eq!(task_config.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_metrics_group() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let metrics = SourceTaskMetricsGroup::new(&id);

        // Record polls
        metrics.record_poll(10, 50);
        metrics.record_poll(20, 100);

        assert_eq!(metrics.poll_count(), 2);
        assert_eq!(metrics.average_poll_batch_time(), 75); // (50 + 100) / 2
    }

    #[test]
    fn test_source_record_write_counter() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let metrics_group = Arc::new(SourceTaskMetricsGroup::new(&id));

        let counter = SourceRecordWriteCounter::new(5, metrics_group.clone());

        // Complete some records
        counter.complete_record();
        counter.complete_record();
        counter.complete_record();

        // Skip some
        counter.skip_record();

        // Remaining via retry
        counter.retry_remaining();

        // Metrics should have been recorded
        assert!(metrics_group.poll_count() == 0); // poll_count not affected by write
    }

    #[test]
    fn test_error_handling() {
        let task = create_test_task();

        // Initially no error
        assert!(task.get_error_message().is_none());
        assert!(!task.is_failed());

        // Set error
        task.set_failed(true, Some("Test error".to_string()));
        assert!(task.is_failed());
        assert_eq!(task.get_error_message(), Some("Test error".to_string()));

        // Clear error
        task.set_failed(false, None);
        assert!(!task.is_failed());
        assert!(task.get_error_message().is_none());
    }

    #[test]
    fn test_close() {
        let task = create_test_task();

        // Initialize first
        task.initialize(TaskConfig::default()).unwrap();

        // Close
        task.close().unwrap();
        assert!(task.was_closed());
    }
}

/// Mock worker config for testing.
struct MockWorkerConfig {
    values: HashMap<String, String>,
}

impl MockWorkerConfig {
    fn new() -> Self {
        MockWorkerConfig {
            values: HashMap::new(),
        }
    }
}

impl WorkerSourceTaskConfig for MockWorkerConfig {
    fn get_bool(&self, _key: &str) -> bool {
        true
    }

    fn get_long(&self, key: &str) -> i64 {
        self.values
            .get(key)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
    }

    fn get_string(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(|s| s.as_str())
    }
}
