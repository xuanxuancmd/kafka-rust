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

//! A TransactionContext that can be given to tasks and then queried by the worker.
//!
//! This class is thread safe and is designed to accommodate concurrent use
//! without external synchronization.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.WorkerTransactionContext` in Java.

use connect_api::connector::ConnectRecord;
use connect_api::SourceRecord;
use std::collections::HashSet;

/// A TransactionContext that can be given to tasks and then queried by the worker
/// to check on requests to abort and commit transactions.
///
/// This class is thread safe and is designed to accommodate concurrent use
/// without external synchronization.
///
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerTransactionContext` in Java.
pub struct WorkerTransactionContext {
    /// Records that have been requested to be committed.
    committable_records: HashSet<SourceRecord>,
    /// Records that have been requested to be aborted.
    abortable_records: HashSet<SourceRecord>,
    /// Whether a batch commit has been requested.
    batch_commit_requested: bool,
    /// Whether a batch abort has been requested.
    batch_abort_requested: bool,
}

impl WorkerTransactionContext {
    /// Creates a new WorkerTransactionContext.
    pub fn new() -> Self {
        WorkerTransactionContext {
            committable_records: HashSet::new(),
            abortable_records: HashSet::new(),
            batch_commit_requested: false,
            batch_abort_requested: false,
        }
    }

    /// Request to commit the current transaction for the entire batch.
    ///
    /// Corresponds to Java: `public synchronized void commitTransaction()`
    pub fn commit_transaction(&mut self) {
        self.batch_commit_requested = true;
    }

    /// Request to commit the transaction at a specific record boundary.
    ///
    /// # Arguments
    /// * `record` - The source record used to define transaction boundaries; may not be null
    ///
    /// Corresponds to Java: `public synchronized void commitTransaction(SourceRecord record)`
    pub fn commit_transaction_on_record(&mut self, record: SourceRecord) {
        self.committable_records.insert(record);
    }

    /// Request to abort the current transaction for the entire batch.
    ///
    /// Corresponds to Java: `public synchronized void abortTransaction()`
    pub fn abort_transaction(&mut self) {
        self.batch_abort_requested = true;
    }

    /// Request to abort the transaction at a specific record boundary.
    ///
    /// # Arguments
    /// * `record` - The source record used to define transaction boundaries; may not be null
    ///
    /// Corresponds to Java: `public synchronized void abortTransaction(SourceRecord record)`
    pub fn abort_transaction_on_record(&mut self, record: SourceRecord) {
        self.abortable_records.insert(record);
    }

    /// Check if a batch commit has been requested and reset the flag.
    ///
    /// # Returns
    /// True if a batch commit was requested, false otherwise. The flag is reset after this call.
    ///
    /// Corresponds to Java: `public synchronized boolean shouldCommitBatch()`
    pub fn should_commit_batch(&mut self) -> bool {
        self.check_batch_requests_consistency();
        let result = self.batch_commit_requested;
        self.batch_commit_requested = false;
        result
    }

    /// Check if a batch abort has been requested and reset the flag.
    ///
    /// # Returns
    /// True if a batch abort was requested, false otherwise. The flag is reset after this call.
    ///
    /// Corresponds to Java: `public synchronized boolean shouldAbortBatch()`
    pub fn should_abort_batch(&mut self) -> bool {
        self.check_batch_requests_consistency();
        let result = self.batch_abort_requested;
        self.batch_abort_requested = false;
        result
    }

    /// Check if a commit was requested at a specific record boundary.
    ///
    /// If the record is in both committable and abortable sets, an error is raised.
    /// This check is performed here instead of in the connector-facing methods because
    /// the connector might swallow exceptions.
    ///
    /// # Arguments
    /// * `record` - The source record to check
    ///
    /// # Returns
    /// True if a commit was requested for this record, false otherwise.
    ///
    /// Corresponds to Java: `public synchronized boolean shouldCommitOn(SourceRecord record)`
    pub fn should_commit_on(&mut self, record: &SourceRecord) -> bool {
        self.check_record_request_consistency(record);
        self.committable_records.remove(record)
    }

    /// Check if an abort was requested at a specific record boundary.
    ///
    /// # Arguments
    /// * `record` - The source record to check
    ///
    /// # Returns
    /// True if an abort was requested for this record, false otherwise.
    ///
    /// Corresponds to Java: `public synchronized boolean shouldAbortOn(SourceRecord record)`
    pub fn should_abort_on(&mut self, record: &SourceRecord) -> bool {
        self.check_record_request_consistency(record);
        self.abortable_records.remove(record)
    }

    /// Check consistency of batch-level commit/abort requests.
    ///
    /// Throws an error if both commit and abort were requested for the same batch.
    fn check_batch_requests_consistency(&self) {
        if self.batch_commit_requested && self.batch_abort_requested {
            panic!("Connector requested both commit and abort of same transaction");
        }
    }

    /// Check consistency of record-level commit/abort requests.
    ///
    /// Throws an error if both commit and abort were requested for the same record.
    fn check_record_request_consistency(&self, record: &SourceRecord) {
        if self.committable_records.contains(record) && self.abortable_records.contains(record) {
            log::trace!(
                "Connector will fail as it has requested both commit and abort of transaction for same record: {:?}",
                record
            );
            panic!(
                "Connector requested both commit and abort of same record against topic/partition {:?}/{:?}",
                record.topic(),
                record.kafka_partition()
            );
        }
    }

    /// Clear all pending commit/abort requests.
    pub fn clear(&mut self) {
        self.committable_records.clear();
        self.abortable_records.clear();
        self.batch_commit_requested = false;
        self.batch_abort_requested = false;
    }
}

impl Default for WorkerTransactionContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::HashMap;

    fn create_test_record(topic: &str) -> SourceRecord {
        SourceRecord::new(
            HashMap::new(),
            HashMap::new(),
            topic,
            Some(0),
            None,
            Value::String("test".to_string()),
        )
    }

    #[test]
    fn test_new_context() {
        let ctx = WorkerTransactionContext::new();
        assert!(!ctx.batch_commit_requested);
        assert!(!ctx.batch_abort_requested);
        assert!(ctx.committable_records.is_empty());
        assert!(ctx.abortable_records.is_empty());
    }

    #[test]
    fn test_commit_transaction_batch() {
        let mut ctx = WorkerTransactionContext::new();
        ctx.commit_transaction();
        assert!(ctx.should_commit_batch());
        // Flag should be reset
        assert!(!ctx.should_commit_batch());
    }

    #[test]
    fn test_abort_transaction_batch() {
        let mut ctx = WorkerTransactionContext::new();
        ctx.abort_transaction();
        assert!(ctx.should_abort_batch());
        // Flag should be reset
        assert!(!ctx.should_abort_batch());
    }

    #[test]
    fn test_commit_transaction_on_record() {
        let mut ctx = WorkerTransactionContext::new();
        let record = create_test_record("test-topic");

        ctx.commit_transaction_on_record(record.clone());

        // Check that the record is now committable
        assert!(ctx.committable_records.contains(&record));

        // should_commit_on should return true and remove the record
        assert!(ctx.should_commit_on(&record));
        assert!(!ctx.committable_records.contains(&record));

        // Second call should return false
        assert!(!ctx.should_commit_on(&record));
    }

    #[test]
    fn test_abort_transaction_on_record() {
        let mut ctx = WorkerTransactionContext::new();
        let record = create_test_record("test-topic");

        ctx.abort_transaction_on_record(record.clone());

        assert!(ctx.abortable_records.contains(&record));
        assert!(ctx.should_abort_on(&record));
        assert!(!ctx.abortable_records.contains(&record));
        assert!(!ctx.should_abort_on(&record));
    }

    #[test]
    #[should_panic(expected = "Connector requested both commit and abort of same transaction")]
    fn test_batch_consistency_check() {
        let mut ctx = WorkerTransactionContext::new();
        ctx.commit_transaction();
        ctx.abort_transaction();
        ctx.should_commit_batch(); // Should panic
    }

    #[test]
    #[should_panic(expected = "Connector requested both commit and abort of same record")]
    fn test_record_consistency_check() {
        let mut ctx = WorkerTransactionContext::new();
        let record = create_test_record("test-topic");

        ctx.commit_transaction_on_record(record.clone());
        ctx.abort_transaction_on_record(record.clone());

        ctx.should_commit_on(&record); // Should panic
    }

    #[test]
    fn test_clear() {
        let mut ctx = WorkerTransactionContext::new();
        ctx.commit_transaction();
        ctx.abort_transaction_on_record(create_test_record("topic1"));
        ctx.commit_transaction_on_record(create_test_record("topic2"));

        ctx.clear();

        assert!(!ctx.batch_commit_requested);
        assert!(!ctx.batch_abort_requested);
        assert!(ctx.committable_records.is_empty());
        assert!(ctx.abortable_records.is_empty());
    }
}
