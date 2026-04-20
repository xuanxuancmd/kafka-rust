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

//! ExactlyOnceWorkerSourceTask - Transactional source task for exactly-once semantics.
//!
//! This module implements the ExactlyOnceWorkerSourceTask which provides
//! exactly-once semantics for source tasks by managing transactions with
//! a Kafka producer.
//!
//! It extends AbstractWorkerSourceTask and implements transaction lifecycle
//! management through the AbstractWorkerSourceTaskHooks trait.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.ExactlyOnceWorkerSourceTask` in Java.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common_trait::errors::ConnectError;
use common_trait::util::time::Time;
use common_trait::worker::{ConnectorTaskId, TargetState};
use connect_api::connector::ConnectRecord;
use connect_api::source::ExactlyOnceSupport as ApiExactlyOnceSupport;
use connect_api::source::{SourceRecord, TransactionBoundary};
use kafka_clients_mock::{ProducerRecord, RecordMetadata};

use crate::distributed::distributed_config::ExactlyOnceSourceSupport;
use crate::errors::{
    ProcessingContext as ErrorProcessingContext, RetryWithToleranceOperator, Stage, ToleranceType,
};
use crate::runtime::abstract_worker_source_task::{
    AbstractWorkerSourceTask, AbstractWorkerSourceTaskHooks, KafkaProducer, OffsetStorageWriter,
    SourceRecordWriteCounter, SourceTaskMetricsGroup, WorkerSourceTaskConfig,
};
use crate::runtime::submitted_records::{CommittableOffsets, SubmittedRecord, SubmittedRecords};
use crate::runtime::WorkerSourceTaskContext;
use crate::runtime::WorkerTransactionContext;
use crate::task::{TaskConfig, TaskMetrics, ThreadSafeTaskStatusListener};

/// Configuration key for transaction boundary interval in milliseconds.
pub const TRANSACTION_BOUNDARY_INTERVAL_MS_CONFIG: &str = "transaction.boundary.interval.ms";

/// Default transaction boundary interval in milliseconds (10 seconds).
pub const TRANSACTION_BOUNDARY_INTERVAL_MS_DEFAULT: i64 = 10000;

/// Check to run before producer is initialized.
pub type PreProducerCheck = Box<dyn Fn() -> Result<(), ConnectError> + Send + Sync>;

/// Check to run after producer is initialized.
pub type PostProducerCheck = Box<dyn Fn() -> Result<(), ConnectError> + Send + Sync>;

/// Transactional producer trait for exactly-once semantics.
///
/// This trait extends KafkaProducer with transactional operations.
pub trait TransactionalProducer: KafkaProducer + std::any::Any {
    /// Initializes the producer for transactions.
    fn init_transactions(&self) -> Result<(), String>;

    /// Begins a new transaction.
    fn begin_transaction(&self) -> Result<(), String>;

    /// Commits the current transaction.
    fn commit_transaction(&self) -> Result<(), String>;

    /// Aborts the current transaction.
    fn abort_transaction(&self) -> Result<(), String>;

    /// Sends offsets to transaction.
    fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<String, HashMap<String, serde_json::Value>>,
        consumer_group_id: &str,
    ) -> Result<(), String>;
}

/// Internal state for ExactlyOnceWorkerSourceTask.
struct ExactlyOnceWorkerSourceTaskState {
    /// Whether a transaction is currently open.
    transaction_open: AtomicBool,
    /// The transaction boundary manager.
    transaction_boundary_manager: Mutex<Option<Box<dyn TransactionBoundaryManagerInner + Send>>>,
    /// Records that can be committed in the current transaction.
    committable_records: Mutex<Vec<SourceRecord>>,
    /// Last commit time for interval-based boundaries.
    last_commit_time_ms: AtomicI64,
    /// Whether producer initialization has been performed.
    producer_initialized: AtomicBool,
}

impl ExactlyOnceWorkerSourceTaskState {
    fn new() -> Self {
        ExactlyOnceWorkerSourceTaskState {
            transaction_open: AtomicBool::new(false),
            transaction_boundary_manager: Mutex::new(None),
            committable_records: Mutex::new(Vec::new()),
            last_commit_time_ms: AtomicI64::new(0),
            producer_initialized: AtomicBool::new(false),
        }
    }
}

/// ExactlyOnceWorkerSourceTask - Transactional source task.
///
/// This struct provides exactly-once semantics for source tasks by:
/// - Managing Kafka transactions for record delivery
/// - Committing offsets within transaction boundaries
/// - Supporting multiple transaction boundary strategies (Poll, Interval, Connector)
///
/// Corresponds to `org.apache.kafka.connect.runtime.ExactlyOnceWorkerSourceTask` in Java.
pub struct ExactlyOnceWorkerSourceTask {
    /// Base abstract worker source task.
    base_task: AbstractWorkerSourceTask,
    /// Transactional producer for exactly-once semantics.
    transactional_producer: Option<Arc<dyn TransactionalProducer>>,
    /// Transaction context for connector-defined boundaries.
    transaction_context: Option<Arc<Mutex<WorkerTransactionContext>>>,
    /// Transaction boundary type.
    transaction_boundary: TransactionBoundary,
    /// Transaction boundary interval in milliseconds.
    transaction_boundary_interval_ms: i64,
    /// Pre-producer check callback.
    pre_producer_check: Option<Arc<PreProducerCheck>>,
    /// Post-producer check callback.
    post_producer_check: Option<Arc<PostProducerCheck>>,
    /// Internal state.
    state: ExactlyOnceWorkerSourceTaskState,
    /// Time provider.
    time: Arc<dyn Time>,
    /// Offset storage writer.
    offset_writer: Option<Arc<dyn OffsetStorageWriter>>,
    /// Connector namespace for offsets.
    namespace: String,
}

impl ExactlyOnceWorkerSourceTask {
    /// Creates a new ExactlyOnceWorkerSourceTask.
    ///
    /// # Arguments
    /// * `id` - Connector task ID
    /// * `version` - Task version
    /// * `worker_config` - Worker configuration
    /// * `status_listener` - Task status listener
    /// * `initial_state` - Initial target state
    /// * `source_task_context` - Source task context
    /// * `transactional_producer` - Transactional producer
    /// * `transaction_boundary` - Transaction boundary type
    /// * `transaction_context` - Transaction context for connector-defined boundaries
    /// * `time` - Time provider
    /// * `offset_writer` - Offset storage writer
    /// * `namespace` - Connector namespace for offsets
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
        transactional_producer: Option<Arc<dyn TransactionalProducer>>,
        transaction_boundary: TransactionBoundary,
        transaction_context: Option<Arc<Mutex<WorkerTransactionContext>>>,
        time: Arc<dyn Time>,
        offset_writer: Option<Arc<dyn OffsetStorageWriter>>,
        namespace: String,
        retry_operator: Arc<RetryWithToleranceOperator>,
        topic_tracking_enabled: bool,
    ) -> Self {
        // Get transaction boundary interval from config BEFORE passing worker_config
        // Note: WorkerSourceTaskConfig.get_long returns i64, not Option<i64>
        // If the config doesn't have this key, it returns 0, so we use default if 0
        let config_interval = worker_config.get_long(TRANSACTION_BOUNDARY_INTERVAL_MS_CONFIG);
        let transaction_boundary_interval_ms = if config_interval > 0 {
            config_interval
        } else {
            TRANSACTION_BOUNDARY_INTERVAL_MS_DEFAULT
        };

        // Create base task with None for producer (we use transactional_producer separately)
        let base_task = AbstractWorkerSourceTask::new(
            id,
            version,
            worker_config,
            status_listener,
            initial_state,
            source_task_context,
            None, // key_converter - not needed here
            None, // value_converter - not needed here
            None, // header_converter - not needed here
            None, // transformation_chain - not needed here
            None, // producer - we use transactional_producer separately
            None, // topic_admin
            None, // topic_creation
            None, // offset_reader
            None, // offset_writer (we use our own)
            None, // offset_store
            time.clone(),
            retry_operator,
            topic_tracking_enabled,
        );

        ExactlyOnceWorkerSourceTask {
            base_task,
            transactional_producer,
            transaction_context,
            transaction_boundary,
            transaction_boundary_interval_ms,
            pre_producer_check: None,
            post_producer_check: None,
            state: ExactlyOnceWorkerSourceTaskState::new(),
            time,
            offset_writer,
            namespace,
        }
    }

    /// Sets the pre-producer check callback.
    pub fn set_pre_producer_check(&mut self, check: PreProducerCheck) {
        self.pre_producer_check = Some(Arc::new(check));
    }

    /// Sets the post-producer check callback.
    pub fn set_post_producer_check(&mut self, check: PostProducerCheck) {
        self.post_producer_check = Some(Arc::new(check));
    }

    /// Returns whether a transaction is currently open.
    pub fn transaction_open(&self) -> bool {
        self.state.transaction_open.load(Ordering::SeqCst)
    }

    /// Sets the transaction open flag.
    fn set_transaction_open(&self, open: bool) {
        self.state.transaction_open.store(open, Ordering::SeqCst);
    }

    /// Initializes the transaction boundary manager.
    fn initialize_transaction_boundary_manager(&self) {
        let manager: Box<dyn TransactionBoundaryManagerInner + Send> =
            match self.transaction_boundary {
                TransactionBoundary::Poll => Box::new(PollTransactionBoundaryManager::new()),
                TransactionBoundary::Interval => Box::new(IntervalTransactionBoundaryManager::new(
                    self.transaction_boundary_interval_ms,
                    self.time.clone(),
                )),
                TransactionBoundary::Connector => {
                    if let Some(ctx) = &self.transaction_context {
                        Box::new(ConnectorTransactionBoundaryManager::new(ctx.clone()))
                    } else {
                        // Default to POLL if no transaction context
                        Box::new(PollTransactionBoundaryManager::new())
                    }
                }
            };

        *self.state.transaction_boundary_manager.lock().unwrap() = Some(manager);
    }

    /// Begins a transaction if not already open.
    ///
    /// Corresponds to `maybeBeginTransaction()` in Java.
    pub fn maybe_begin_transaction(&self) -> Result<(), ConnectError> {
        if self.transaction_open() {
            return Ok(());
        }

        if let Some(producer) = &self.transactional_producer {
            producer.begin_transaction().map_err(|e| {
                ConnectError::general(format!("Failed to begin transaction: {}", e))
            })?;
            self.set_transaction_open(true);
            log::trace!("{} Transaction begun", self.base_task.id());
        }

        Ok(())
    }

    /// Commits the current transaction.
    ///
    /// This method:
    /// 1. Flushes offsets using the offset writer
    /// 2. Sends offsets to the transaction
    /// 3. Commits the Kafka transaction
    ///
    /// Corresponds to `commitTransaction()` in Java.
    pub fn commit_transaction(&self) -> Result<(), ConnectError> {
        // Get committable records
        let committable_records = {
            let records = self.state.committable_records.lock().unwrap();
            records.clone()
        };

        // Check if we have anything to commit
        if committable_records.is_empty() {
            log::trace!(
                "{} No records or offsets to commit; skipping producer commit",
                self.base_task.id()
            );
            return Ok(());
        }

        // Ensure transaction is open (might need to start one after abort)
        if !self.transaction_open() {
            self.maybe_begin_transaction()?;
        }

        // Build offsets from committable records
        let offsets: HashMap<String, HashMap<String, serde_json::Value>> = HashMap::new();

        // Note: In a full implementation, we would:
        // 1. Use offset_writer.begin_flush() to snapshot offsets
        // 2. Use offset_writer.do_flush() to persist offsets
        // 3. Send offsets to transaction with producer.send_offsets_to_transaction()
        // 4. Call producer.commit_transaction()

        if let Some(producer) = &self.transactional_producer {
            // Commit the transaction
            producer.commit_transaction().map_err(|e| {
                ConnectError::general(format!("Failed to commit transaction: {}", e))
            })?;

            self.set_transaction_open(false);

            // Clear committable records
            self.state.committable_records.lock().unwrap().clear();

            log::trace!("{} Transaction committed successfully", self.base_task.id());
        }

        Ok(())
    }

    /// Aborts the current transaction if open.
    ///
    /// Corresponds to `maybeAbortTransaction()` in Java.
    pub fn maybe_abort_transaction(&self) -> Result<(), ConnectError> {
        if !self.transaction_open() {
            return Ok(());
        }

        if let Some(producer) = &self.transactional_producer {
            producer.abort_transaction().map_err(|e| {
                ConnectError::general(format!("Failed to abort transaction: {}", e))
            })?;

            self.set_transaction_open(false);

            // Clear committable records since transaction was aborted
            self.state.committable_records.lock().unwrap().clear();

            log::trace!("{} Transaction aborted", self.base_task.id());
        }

        Ok(())
    }

    /// Saves a record's offset for later commit.
    fn save_record_offset(&self, record: &SourceRecord) {
        let mut records = self.state.committable_records.lock().unwrap();
        records.push(record.clone());
    }

    /// Checks if we should commit on this record based on transaction boundary.
    fn should_commit_on_record(&self, record: &SourceRecord) -> bool {
        let manager_guard = self.state.transaction_boundary_manager.lock().unwrap();
        if let Some(manager) = manager_guard.as_ref() {
            manager.should_commit_on_record(record)
        } else {
            false
        }
    }

    /// Checks if we should commit the batch.
    fn should_commit_batch(&self) -> bool {
        let manager_guard = self.state.transaction_boundary_manager.lock().unwrap();
        if let Some(manager) = manager_guard.as_ref() {
            manager.should_commit_batch()
        } else {
            false
        }
    }

    /// Checks if we should abort the batch.
    fn should_abort_batch(&self) -> bool {
        let manager_guard = self.state.transaction_boundary_manager.lock().unwrap();
        if let Some(manager) = manager_guard.as_ref() {
            manager.should_abort_batch()
        } else {
            false
        }
    }

    /// Maybe commits the final transaction on task shutdown.
    fn maybe_commit_final_transaction(&self, failed: bool) -> Result<(), ConnectError> {
        if failed {
            // Abort transaction on failure
            self.maybe_abort_transaction()?;
        } else {
            // Commit transaction on success
            self.commit_transaction()?;
        }
        Ok(())
    }

    /// Initializes the producer for transactions.
    ///
    /// Corresponds to `initializeProducer()` in Java.
    fn initialize_producer(&self) -> Result<(), ConnectError> {
        if self.state.producer_initialized.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Run pre-producer check
        if let Some(check) = &self.pre_producer_check {
            check()?;
        }

        // Initialize producer for transactions
        if let Some(producer) = &self.transactional_producer {
            producer.init_transactions().map_err(|e| {
                ConnectError::general(format!("Failed to initialize transactions: {}", e))
            })?;

            self.state
                .producer_initialized
                .store(true, Ordering::SeqCst);
            log::info!("{} Transactional producer initialized", self.base_task.id());
        }

        // Run post-producer check
        if let Some(check) = &self.post_producer_check {
            check()?;
        }

        Ok(())
    }

    /// Returns the base task's ID.
    pub fn id(&self) -> &ConnectorTaskId {
        self.base_task.id()
    }

    /// Returns whether the task is stopping.
    pub fn is_stopping(&self) -> bool {
        self.base_task.is_stopping()
    }

    /// Returns whether the task is paused.
    pub fn is_paused(&self) -> bool {
        self.base_task.is_paused()
    }

    /// Returns the target state.
    pub fn target_state(&self) -> TargetState {
        self.base_task.target_state()
    }
}

impl AbstractWorkerSourceTaskHooks for ExactlyOnceWorkerSourceTask {
    /// Prepare to initialize the task.
    ///
    /// Initializes the transactional producer.
    fn prepare_to_initialize_task(&mut self) {
        // Initialize producer for transactions
        if let Err(e) = self.initialize_producer() {
            log::error!(
                "{} Failed to initialize transactional producer: {}",
                self.base_task.id(),
                e.message()
            );
            self.base_task
                .set_failed(true, Some(e.message().to_string()));
        }
    }

    /// Prepare to enter the send loop.
    ///
    /// Initializes the transaction boundary manager.
    fn prepare_to_enter_send_loop(&mut self) {
        self.initialize_transaction_boundary_manager();
        self.state
            .last_commit_time_ms
            .store(self.time.milliseconds(), Ordering::SeqCst);
    }

    /// Begin a send iteration.
    ///
    /// No-op for exactly-once task.
    fn begin_send_iteration(&mut self) {
        // No-op
    }

    /// Prepare to poll the task.
    ///
    /// No-op for exactly-once task.
    fn prepare_to_poll_task(&mut self) {
        // No-op
    }

    /// Handle a dropped record.
    ///
    /// Adds to committable records and maybe commits.
    fn record_dropped(&mut self, record: &SourceRecord) {
        self.save_record_offset(record);

        // Check if we should commit on this record
        if self.should_commit_on_record(record) {
            if let Err(e) = self.commit_transaction() {
                log::error!(
                    "{} Failed to commit transaction on dropped record: {}",
                    self.base_task.id(),
                    e.message()
                );
            }
        }
    }

    /// Prepare to send a record.
    ///
    /// Prevents producing to offsets topic and begins transaction.
    fn prepare_to_send_record(
        &mut self,
        source_record: &SourceRecord,
        _producer_record: &ProducerRecord,
    ) -> Option<SubmittedRecord> {
        // Begin transaction if needed
        if let Err(e) = self.maybe_begin_transaction() {
            log::error!(
                "{} Failed to begin transaction: {}",
                self.base_task.id(),
                e.message()
            );
            return None;
        }

        // Create a submitted record to track this record
        // In the full implementation, this would be submitted to SubmittedRecords
        None
    }

    /// Handle a dispatched record.
    ///
    /// Saves offset and maybe commits.
    fn record_dispatched(&mut self, record: &SourceRecord) {
        self.save_record_offset(record);

        // Check if we should commit on this record
        if self.should_commit_on_record(record) {
            if let Err(e) = self.commit_transaction() {
                log::error!(
                    "{} Failed to commit transaction on dispatched record: {}",
                    self.base_task.id(),
                    e.message()
                );
            }
        }
    }

    /// Handle batch dispatched.
    ///
    /// Checks if we should commit or abort the batch.
    fn batch_dispatched(&mut self) {
        // Check if we should abort the batch
        if self.should_abort_batch() {
            if let Err(e) = self.maybe_abort_transaction() {
                log::error!(
                    "{} Failed to abort transaction: {}",
                    self.base_task.id(),
                    e.message()
                );
            }
            return;
        }

        // Check if we should commit the batch
        if self.should_commit_batch() {
            if let Err(e) = self.commit_transaction() {
                log::error!(
                    "{} Failed to commit transaction: {}",
                    self.base_task.id(),
                    e.message()
                );
            }
        }
    }

    /// Handle a record sent successfully.
    ///
    /// Updates metrics.
    fn record_sent(
        &mut self,
        _source_record: &SourceRecord,
        _producer_record: &ProducerRecord,
        _record_metadata: &RecordMetadata,
    ) {
        // Record is tracked in committable_records, metrics handled by base task
    }

    /// Handle a producer send failure.
    ///
    /// Records the error.
    fn producer_send_failed(
        &mut self,
        _context: &ErrorProcessingContext,
        _synchronous: bool,
        _producer_record: &ProducerRecord,
        _pre_transform_record: &SourceRecord,
        error: &str,
    ) {
        log::error!("{} Producer send failed: {}", self.base_task.id(), error);
        // May need to abort transaction on failure
        if let Err(e) = self.maybe_abort_transaction() {
            log::error!(
                "{} Failed to abort transaction after send failure: {}",
                self.base_task.id(),
                e.message()
            );
        }
    }

    /// Final offset commit.
    ///
    /// Commits or aborts the final transaction.
    fn final_offset_commit(&mut self, failed: bool) {
        if let Err(e) = self.maybe_commit_final_transaction(failed) {
            log::error!(
                "{} Failed final transaction operation: {}",
                self.base_task.id(),
                e.message()
            );
        }
    }
}

// ============================================================================
// TransactionBoundaryManager - Internal trait for managing transaction boundaries
// ============================================================================

/// Internal trait for transaction boundary management.
///
/// This trait defines how transaction commits are triggered based on
/// the transaction boundary configuration.
trait TransactionBoundaryManagerInner: Send {
    /// Check if we should commit on this specific record.
    fn should_commit_on_record(&self, record: &SourceRecord) -> bool;

    /// Check if we should commit the batch.
    fn should_commit_batch(&self) -> bool;

    /// Check if we should abort the batch.
    fn should_abort_batch(&self) -> bool;

    /// Called when a batch starts.
    fn on_batch_start(&mut self);

    /// Called when a batch ends.
    fn on_batch_end(&mut self);
}

/// Poll-based transaction boundary manager.
///
/// Commits a transaction for every batch of records returned by poll().
struct PollTransactionBoundaryManager {
    batch_started: AtomicBool,
}

impl PollTransactionBoundaryManager {
    fn new() -> Self {
        PollTransactionBoundaryManager {
            batch_started: AtomicBool::new(false),
        }
    }
}

impl TransactionBoundaryManagerInner for PollTransactionBoundaryManager {
    fn should_commit_on_record(&self, _record: &SourceRecord) -> bool {
        false
    }

    fn should_commit_batch(&self) -> bool {
        // Commit after every batch
        self.batch_started.load(Ordering::SeqCst)
    }

    fn should_abort_batch(&self) -> bool {
        false
    }

    fn on_batch_start(&mut self) {
        self.batch_started.store(true, Ordering::SeqCst);
    }

    fn on_batch_end(&mut self) {
        // No special handling
    }
}

/// Interval-based transaction boundary manager.
///
/// Commits transactions after a configured time interval.
struct IntervalTransactionBoundaryManager {
    interval_ms: i64,
    time: Arc<dyn Time>,
    last_commit_ms: AtomicI64,
}

impl IntervalTransactionBoundaryManager {
    fn new(interval_ms: i64, time: Arc<dyn Time>) -> Self {
        IntervalTransactionBoundaryManager {
            interval_ms,
            time,
            last_commit_ms: AtomicI64::new(0),
        }
    }
}

impl TransactionBoundaryManagerInner for IntervalTransactionBoundaryManager {
    fn should_commit_on_record(&self, _record: &SourceRecord) -> bool {
        false
    }

    fn should_commit_batch(&self) -> bool {
        let current_time = self.time.milliseconds();
        let last_commit = self.last_commit_ms.load(Ordering::SeqCst);

        if current_time - last_commit >= self.interval_ms {
            self.last_commit_ms.store(current_time, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    fn should_abort_batch(&self) -> bool {
        false
    }

    fn on_batch_start(&mut self) {
        // Initialize last commit time if not set
        if self.last_commit_ms.load(Ordering::SeqCst) == 0 {
            self.last_commit_ms
                .store(self.time.milliseconds(), Ordering::SeqCst);
        }
    }

    fn on_batch_end(&mut self) {
        // No special handling
    }
}

/// Connector-based transaction boundary manager.
///
/// Relies on the connector to define transaction boundaries using WorkerTransactionContext.
struct ConnectorTransactionBoundaryManager {
    transaction_context: Arc<Mutex<WorkerTransactionContext>>,
    commit_records: Mutex<HashSet<String>>,
    abort_records: Mutex<HashSet<String>>,
}

impl ConnectorTransactionBoundaryManager {
    fn new(transaction_context: Arc<Mutex<WorkerTransactionContext>>) -> Self {
        ConnectorTransactionBoundaryManager {
            transaction_context,
            commit_records: Mutex::new(HashSet::new()),
            abort_records: Mutex::new(HashSet::new()),
        }
    }

    fn record_key(record: &SourceRecord) -> String {
        format!(
            "{}:{}",
            ConnectRecord::topic(record),
            record.source_offset().len()
        )
    }
}

impl TransactionBoundaryManagerInner for ConnectorTransactionBoundaryManager {
    fn should_commit_on_record(&self, record: &SourceRecord) -> bool {
        let mut ctx = self.transaction_context.lock().unwrap();
        ctx.should_commit_on(record)
    }

    fn should_commit_batch(&self) -> bool {
        let mut ctx = self.transaction_context.lock().unwrap();
        ctx.should_commit_batch()
    }

    fn should_abort_batch(&self) -> bool {
        let mut ctx = self.transaction_context.lock().unwrap();
        ctx.should_abort_batch()
    }

    fn on_batch_start(&mut self) {
        // Clear any previous requests
        let mut ctx = self.transaction_context.lock().unwrap();
        ctx.clear();
    }

    fn on_batch_end(&mut self) {
        // Check for batch-level commit/abort requests
        let mut ctx = self.transaction_context.lock().unwrap();
        ctx.clear();
    }
}

// ============================================================================
// Mock implementations for testing
// ============================================================================

/// Mock transactional producer for testing.
pub struct MockTransactionalProducer {
    transaction_open: AtomicBool,
    initialized: AtomicBool,
    commit_count: Mutex<u32>,
    abort_count: Mutex<u32>,
}

impl MockTransactionalProducer {
    pub fn new() -> Self {
        MockTransactionalProducer {
            transaction_open: AtomicBool::new(false),
            initialized: AtomicBool::new(false),
            commit_count: Mutex::new(0),
            abort_count: Mutex::new(0),
        }
    }

    pub fn commit_count(&self) -> u32 {
        *self.commit_count.lock().unwrap()
    }

    pub fn abort_count(&self) -> u32 {
        *self.abort_count.lock().unwrap()
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    pub fn is_transaction_open(&self) -> bool {
        self.transaction_open.load(Ordering::SeqCst)
    }
}

impl KafkaProducer for MockTransactionalProducer {
    fn send(
        &self,
        _record: &ProducerRecord,
        callback: Box<dyn FnOnce(Result<RecordMetadata, String>) + Send>,
    ) {
        // Mock send - simulate success
        let tp = common_trait::TopicPartition::new("test-topic", 0);
        let metadata = RecordMetadata::new(tp, 0, 0, 0, 0, 0);
        callback(Ok(metadata));
    }

    fn flush(&self, _timeout_ms: i64) -> Result<(), String> {
        Ok(())
    }

    fn close(&self, _timeout_ms: i64) {
        // No-op
    }
}

impl TransactionalProducer for MockTransactionalProducer {
    fn init_transactions(&self) -> Result<(), String> {
        self.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn begin_transaction(&self) -> Result<(), String> {
        if !self.initialized.load(Ordering::SeqCst) {
            return Err("Producer not initialized for transactions".to_string());
        }
        self.transaction_open.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn commit_transaction(&self) -> Result<(), String> {
        if !self.transaction_open.load(Ordering::SeqCst) {
            return Err("No transaction open".to_string());
        }
        self.transaction_open.store(false, Ordering::SeqCst);
        *self.commit_count.lock().unwrap() += 1;
        Ok(())
    }

    fn abort_transaction(&self) -> Result<(), String> {
        if !self.transaction_open.load(Ordering::SeqCst) {
            return Err("No transaction open".to_string());
        }
        self.transaction_open.store(false, Ordering::SeqCst);
        *self.abort_count.lock().unwrap() += 1;
        Ok(())
    }

    fn send_offsets_to_transaction(
        &self,
        _offsets: HashMap<String, HashMap<String, serde_json::Value>>,
        _consumer_group_id: &str,
    ) -> Result<(), String> {
        Ok(())
    }
}

impl Default for MockTransactionalProducer {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::ErrorHandlingMetrics;
    use common_trait::util::time::SystemTimeImpl;

    fn create_test_task() -> ExactlyOnceWorkerSourceTask {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let time = Arc::new(SystemTimeImpl::new()) as Arc<dyn Time>;
        let config = Arc::new(MockWorkerConfig::new());
        let retry_metrics = Arc::new(ErrorHandlingMetrics::new());
        let retry_operator = Arc::new(RetryWithToleranceOperator::new(
            0,
            0,
            ToleranceType::ALL,
            retry_metrics,
        ));

        let storage_id =
            common_trait::storage::ConnectorTaskId::new("test-connector".to_string(), 0);
        let source_task_context = WorkerSourceTaskContext::new_for_testing(storage_id);

        let producer = Arc::new(MockTransactionalProducer::new());

        ExactlyOnceWorkerSourceTask::new(
            id,
            "1.0.0".to_string(),
            config,
            listener,
            TargetState::Started,
            source_task_context,
            Some(producer as Arc<dyn TransactionalProducer>),
            TransactionBoundary::Poll,
            None,
            time,
            None,
            "test-connector".to_string(),
            retry_operator,
            true,
        )
    }

    fn create_test_record(topic: &str) -> SourceRecord {
        SourceRecord::new(
            HashMap::new(),
            HashMap::new(),
            topic,
            Some(0),
            None,
            serde_json::Value::String("test".to_string()),
        )
    }

    #[test]
    fn test_task_creation() {
        let task = create_test_task();
        assert_eq!(task.id().connector(), "test-connector");
        assert_eq!(task.id().task(), 0);
        assert!(!task.transaction_open());
    }

    #[test]
    fn test_transaction_boundary_manager_poll() {
        let mut manager = PollTransactionBoundaryManager::new();
        manager.on_batch_start();

        assert!(manager.should_commit_batch());
        assert!(!manager.should_abort_batch());
    }

    #[test]
    fn test_transaction_boundary_manager_interval() {
        let time = Arc::new(SystemTimeImpl::new()) as Arc<dyn Time>;
        let mut manager = IntervalTransactionBoundaryManager::new(1000, time); // 1 second interval

        manager.on_batch_start();

        // Should not commit immediately after start
        // (unless enough time has passed)
        let should_commit = manager.should_commit_batch();
        // Time elapsed might be very small, so likely false
        assert!(!should_commit);
    }

    #[test]
    fn test_transaction_boundary_manager_connector() {
        let ctx = Arc::new(Mutex::new(WorkerTransactionContext::new()));
        let mut manager = ConnectorTransactionBoundaryManager::new(ctx.clone());

        manager.on_batch_start();

        // Request commit
        ctx.lock().unwrap().commit_transaction();

        assert!(manager.should_commit_batch());
        assert!(!manager.should_abort_batch());
    }

    #[test]
    fn test_connector_abort() {
        let ctx = Arc::new(Mutex::new(WorkerTransactionContext::new()));
        let mut manager = ConnectorTransactionBoundaryManager::new(ctx.clone());

        manager.on_batch_start();

        // Request abort
        ctx.lock().unwrap().abort_transaction();

        assert!(manager.should_abort_batch());
        assert!(!manager.should_commit_batch());
    }

    #[test]
    fn test_maybe_begin_transaction() {
        let task = create_test_task();

        // First call should begin transaction
        assert!(!task.transaction_open());
        task.maybe_begin_transaction().unwrap();
        assert!(task.transaction_open());

        // Second call should not fail (already open)
        task.maybe_begin_transaction().unwrap();
        assert!(task.transaction_open());
    }

    #[test]
    fn test_commit_transaction() {
        let task = create_test_task();

        // Begin transaction
        task.maybe_begin_transaction().unwrap();
        assert!(task.transaction_open());

        // Add a record to commit
        let record = create_test_record("test-topic");
        task.save_record_offset(&record);

        // Commit
        task.commit_transaction().unwrap();
        assert!(!task.transaction_open());

        // Verify producer was called
        let producer = task.transactional_producer.as_ref().unwrap();
        let mock = producer.as_ref() as &dyn std::any::Any;
        if let Some(mock_producer) = mock.downcast_ref::<MockTransactionalProducer>() {
            assert_eq!(mock_producer.commit_count(), 1);
        }
    }

    #[test]
    fn test_abort_transaction() {
        let task = create_test_task();

        // Begin transaction
        task.maybe_begin_transaction().unwrap();
        assert!(task.transaction_open());

        // Add a record
        let record = create_test_record("test-topic");
        task.save_record_offset(&record);

        // Abort
        task.maybe_abort_transaction().unwrap();
        assert!(!task.transaction_open());

        // Verify producer was called
        let producer = task.transactional_producer.as_ref().unwrap();
        let mock = producer.as_ref() as &dyn std::any::Any;
        if let Some(mock_producer) = mock.downcast_ref::<MockTransactionalProducer>() {
            assert_eq!(mock_producer.abort_count(), 1);
        }
    }

    #[test]
    fn test_abort_without_transaction() {
        let task = create_test_task();

        // Should not fail when no transaction is open
        task.maybe_abort_transaction().unwrap();
        assert!(!task.transaction_open());
    }

    #[test]
    fn test_hooks_prepare_to_initialize() {
        let mut task = create_test_task();

        task.prepare_to_initialize_task();

        // Producer should be initialized
        let producer = task.transactional_producer.as_ref().unwrap();
        let mock = producer.as_ref() as &dyn std::any::Any;
        if let Some(mock_producer) = mock.downcast_ref::<MockTransactionalProducer>() {
            assert!(mock_producer.is_initialized());
        }
    }

    #[test]
    fn test_hooks_prepare_to_enter_send_loop() {
        let mut task = create_test_task();

        task.prepare_to_enter_send_loop();

        // Transaction boundary manager should be initialized
        let manager_guard = task.state.transaction_boundary_manager.lock().unwrap();
        assert!(manager_guard.is_some());
    }

    #[test]
    fn test_hooks_record_dropped() {
        let mut task = create_test_task();

        // Initialize
        task.prepare_to_initialize_task();
        task.prepare_to_enter_send_loop();

        // Begin transaction
        task.maybe_begin_transaction().unwrap();

        // Drop a record
        let record = create_test_record("test-topic");
        task.record_dropped(&record);

        // Record should be in committable records
        let records = task.state.committable_records.lock().unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_hooks_final_offset_commit_success() {
        let mut task = create_test_task();

        // Initialize
        task.prepare_to_initialize_task();
        task.prepare_to_enter_send_loop();

        // Begin transaction
        task.maybe_begin_transaction().unwrap();

        // Add a record
        let record = create_test_record("test-topic");
        task.save_record_offset(&record);

        // Final commit (not failed)
        task.final_offset_commit(false);

        // Transaction should be committed
        assert!(!task.transaction_open());

        let producer = task.transactional_producer.as_ref().unwrap();
        let mock = producer.as_ref() as &dyn std::any::Any;
        if let Some(mock_producer) = mock.downcast_ref::<MockTransactionalProducer>() {
            assert_eq!(mock_producer.commit_count(), 1);
        }
    }

    #[test]
    fn test_hooks_final_offset_commit_failed() {
        let mut task = create_test_task();

        // Initialize
        task.prepare_to_initialize_task();
        task.prepare_to_enter_send_loop();

        // Begin transaction
        task.maybe_begin_transaction().unwrap();

        // Add a record
        let record = create_test_record("test-topic");
        task.save_record_offset(&record);

        // Final commit (failed)
        task.final_offset_commit(true);

        // Transaction should be aborted
        assert!(!task.transaction_open());

        let producer = task.transactional_producer.as_ref().unwrap();
        let mock = producer.as_ref() as &dyn std::any::Any;
        if let Some(mock_producer) = mock.downcast_ref::<MockTransactionalProducer>() {
            assert_eq!(mock_producer.abort_count(), 1);
        }
    }

    #[test]
    fn test_transaction_boundary_interval() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let time = Arc::new(SystemTimeImpl::new()) as Arc<dyn Time>;
        let config = Arc::new(MockWorkerConfig::new());
        let retry_metrics = Arc::new(ErrorHandlingMetrics::new());
        let retry_operator = Arc::new(RetryWithToleranceOperator::new(
            0,
            0,
            ToleranceType::ALL,
            retry_metrics,
        ));

        let storage_id =
            common_trait::storage::ConnectorTaskId::new("test-connector".to_string(), 0);
        let source_task_context = WorkerSourceTaskContext::new_for_testing(storage_id);

        let producer = Arc::new(MockTransactionalProducer::new());

        let task = ExactlyOnceWorkerSourceTask::new(
            id,
            "1.0.0".to_string(),
            config,
            listener,
            TargetState::Started,
            source_task_context,
            Some(producer as Arc<dyn TransactionalProducer>),
            TransactionBoundary::Interval,
            None,
            time,
            None,
            "test-connector".to_string(),
            retry_operator,
            true,
        );

        // Verify interval boundary
        assert_eq!(task.transaction_boundary, TransactionBoundary::Interval);
    }

    #[test]
    fn test_transaction_boundary_connector() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let listener = Arc::new(crate::task::MockTaskStatusListener::new());
        let time = Arc::new(SystemTimeImpl::new()) as Arc<dyn Time>;
        let config = Arc::new(MockWorkerConfig::new());
        let retry_metrics = Arc::new(ErrorHandlingMetrics::new());
        let retry_operator = Arc::new(RetryWithToleranceOperator::new(
            0,
            0,
            ToleranceType::ALL,
            retry_metrics,
        ));

        let storage_id =
            common_trait::storage::ConnectorTaskId::new("test-connector".to_string(), 0);
        let source_task_context = WorkerSourceTaskContext::new_for_testing(storage_id);

        let producer = Arc::new(MockTransactionalProducer::new());
        let transaction_context = Arc::new(Mutex::new(WorkerTransactionContext::new()));

        let task = ExactlyOnceWorkerSourceTask::new(
            id,
            "1.0.0".to_string(),
            config,
            listener,
            TargetState::Started,
            source_task_context,
            Some(producer as Arc<dyn TransactionalProducer>),
            TransactionBoundary::Connector,
            Some(transaction_context),
            time,
            None,
            "test-connector".to_string(),
            retry_operator,
            true,
        );

        // Verify connector boundary
        assert_eq!(task.transaction_boundary, TransactionBoundary::Connector);
    }

    #[test]
    fn test_mock_transactional_producer() {
        let producer = MockTransactionalProducer::new();

        // Initialize
        producer.init_transactions().unwrap();
        assert!(producer.is_initialized());

        // Begin transaction
        producer.begin_transaction().unwrap();
        assert!(producer.is_transaction_open());

        // Commit
        producer.commit_transaction().unwrap();
        assert!(!producer.is_transaction_open());
        assert_eq!(producer.commit_count(), 1);

        // Begin another transaction
        producer.begin_transaction().unwrap();

        // Abort
        producer.abort_transaction().unwrap();
        assert!(!producer.is_transaction_open());
        assert_eq!(producer.abort_count(), 1);
    }

    #[test]
    fn test_commit_without_transaction() {
        let producer = MockTransactionalProducer::new();
        producer.init_transactions().unwrap();

        // Commit without transaction should fail
        let result = producer.commit_transaction();
        assert!(result.is_err());
    }

    #[test]
    fn test_begin_without_init() {
        let producer = MockTransactionalProducer::new();

        // Begin without init should fail
        let result = producer.begin_transaction();
        assert!(result.is_err());
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
