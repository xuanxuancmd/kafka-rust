//! MirrorSourceTask implementation
//!
//! This module provides concrete implementation of MirrorSourceTask.

use anyhow::Result;
use connect_api::error::ConnectException;
use connect_api::{RecordMetadata, SourceRecord, SourceTask, TopicPartition};
use connect_mirror_client::ReplicationPolicy;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::task::{ConsumerRecord, MirrorSourceTask as MirrorSourceTaskTrait};

/// Concrete implementation of MirrorSourceTask
///
/// This task replicates records from upstream cluster to downstream cluster.
pub struct MirrorSourceTaskImpl {
    /// Source cluster alias
    source_cluster_alias: Option<String>,
    /// Poll timeout
    poll_timeout: Duration,
    /// Replication policy for topic naming
    replication_policy: Option<Arc<dyn ReplicationPolicy>>,
    /// Whether task is stopping
    stopping: Arc<Mutex<bool>>,
    /// Assigned topic partitions
    assigned_topic_partitions: Arc<Mutex<Vec<TopicPartition>>>,
    /// Current offsets for each partition
    current_offsets: Arc<Mutex<HashMap<TopicPartition, i64>>>,
    /// Injected records for testing (bypasses consumer)
    injected_records: Arc<Mutex<Vec<ConsumerRecord>>>,
}

impl MirrorSourceTaskImpl {
    /// Create a new MirrorSourceTaskImpl
    pub fn new() -> Self {
        MirrorSourceTaskImpl {
            source_cluster_alias: None,
            poll_timeout: Duration::from_millis(1000),
            replication_policy: None,
            stopping: Arc::new(Mutex::new(false)),
            assigned_topic_partitions: Arc::new(Mutex::new(Vec::new())),
            current_offsets: Arc::new(Mutex::new(HashMap::new())),
            injected_records: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Create a new MirrorSourceTaskImpl with custom parameters
    pub fn with_params(source_cluster_alias: String, poll_timeout_ms: u64) -> Self {
        MirrorSourceTaskImpl {
            source_cluster_alias: Some(source_cluster_alias),
            poll_timeout: Duration::from_millis(poll_timeout_ms),
            replication_policy: None,
            stopping: Arc::new(Mutex::new(false)),
            assigned_topic_partitions: Arc::new(Mutex::new(Vec::new())),
            current_offsets: Arc::new(Mutex::new(HashMap::new())),
            injected_records: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Set the replication policy
    pub fn with_replication_policy(mut self, policy: Arc<dyn ReplicationPolicy>) -> Self {
        self.replication_policy = Some(policy);
        self
    }

    /// Inject records for testing (synchronous, no async runtime needed)
    pub fn inject_records(&self, records: Vec<ConsumerRecord>) {
        let mut injected = self.injected_records.lock().unwrap();
        *injected = records;
    }

    /// Initialize consumer with assigned topic partitions
    pub fn initialize_consumer(&self, task_topic_partitions: Vec<TopicPartition>) -> Result<()> {
        // In a real implementation, this would:
        // 1. Load previously committed offsets for the assigned topic-partitions
        // 2. Seek the consumer to the appropriate offset for each partition

        let mut assigned = self.assigned_topic_partitions.lock().unwrap();
        *assigned = task_topic_partitions;

        Ok(())
    }

    /// Load offsets for assigned partitions
    pub fn load_offsets(&self) -> Result<HashMap<TopicPartition, i64>> {
        // In a real implementation, this would load offsets from OffsetStorageReader
        let offsets = self.current_offsets.lock().unwrap();
        Ok(offsets.clone())
    }

    /// Seek consumer to specific offset
    pub fn seek(&self, partition: TopicPartition, offset: i64) -> Result<()> {
        let mut offsets = self.current_offsets.lock().unwrap();
        offsets.insert(partition, offset);
        Ok(())
    }
}

impl MirrorSourceTaskTrait for MirrorSourceTaskImpl {
    fn convert_record(&self, consumer_record: ConsumerRecord) -> Result<SourceRecord> {
        // Convert a consumer record to a source record
        // In a real implementation, this would:
        // 1. Format the target topic name using the replication_policy
        // 2. Convert headers
        // 3. Create SourceRecord with appropriate source partition and offset

        let source_partition = HashMap::new();
        let mut source_offset = HashMap::new();
        source_offset.insert(
            "partition".to_string(),
            consumer_record.partition.to_string(),
        );
        source_offset.insert("offset".to_string(), consumer_record.offset.to_string());

        // Use builder pattern since SourceRecord fields are private
        let builder = SourceRecord::builder()
            .topic(consumer_record.topic)
            .partition(Some(consumer_record.partition));

        let mut builder = builder;

        // Handle optional key
        if let Some(key) = consumer_record.key {
            builder = builder.key(Box::new(key));
        }

        // Handle optional value
        if let Some(value) = consumer_record.value {
            builder = builder.value(Box::new(value));
        }

        // Handle optional timestamp
        if let Some(ts) = consumer_record.timestamp {
            builder = builder.timestamp(ts);
        }

        builder = builder
            .headers(Box::new(connect_api::connector_impl::SimpleHeaders::new()))
            .source_partition(source_partition)
            .source_offset(source_offset);

        Ok(builder.build())
    }
}

impl connect_api::Versioned for MirrorSourceTaskImpl {
    fn version(&self) -> String {
        "1.0.0".to_string()
    }
}

impl connect_api::Task for MirrorSourceTaskImpl {
    fn start(&self, props: HashMap<String, String>) -> Result<(), ConnectException> {
        // Parse configuration
        let source_alias = props.get("source.cluster.alias").cloned();

        // Note: In a real implementation, we would need interior mutability
        // to modify source_cluster_alias. For now, we just log it.
        let _ = source_alias;

        // In a real implementation, this would:
        // 1. Parse configuration using MirrorSourceTaskConfig
        // 2. Set up metrics
        // 3. Set up ReplicationPolicy
        // 4. Set up OffsetSyncWriter if enabled
        // 5. Create KafkaConsumer
        // 6. Assign it the topic-partitions specified in the task configuration

        Ok(())
    }

    fn stop(&self) {
        // Set stopping flag
        let mut stopping = self.stopping.lock().unwrap();
        *stopping = true;

        // In a real implementation, this would:
        // 1. Wake up the consumer
        // 2. Quietly close the consumer, offsetSyncWriter, and metrics reporters
    }
}

impl SourceTask for MirrorSourceTaskImpl {
    fn poll(&self) -> Result<Option<Vec<SourceRecord>>, ConnectException> {
        // Check if stopping
        let stopping = self.stopping.lock().unwrap();
        if *stopping {
            return Ok(None);
        }

        // Check for injected records first (for testing)
        {
            let injected = self.injected_records.lock().unwrap();
            if !injected.is_empty() {
                let records: Vec<ConsumerRecord> = injected.clone();
                // Clear the injected records after returning them
                drop(injected);
                let mut injected_guard = self.injected_records.lock().unwrap();
                injected_guard.clear();

                // Convert injected records to source records
                let mut source_records = Vec::new();
                for record in records {
                    match self.convert_record(record) {
                        Ok(source_record) => source_records.push(source_record),
                        Err(e) => {
                            // Convert anyhow::Error to ConnectException
                            return Err(ConnectException::new(e.to_string()));
                        }
                    }
                }
                return Ok(Some(source_records));
            }
        }

        // No records available, return None
        Ok(None)
    }

    fn commit(&self) -> Result<(), ConnectException> {
        // In a real implementation, this would:
        // 1. Handle delayed and pending offset synchronization messages
        //    by calling promoteDelayedOffsetSyncs() and firePendingOffsetSyncs()
        //    on the offsetSyncWriter

        Ok(())
    }

    fn commit_record(
        &self,
        _record: &SourceRecord,
        _metadata: Option<RecordMetadata>,
    ) -> Result<(), ConnectException> {
        // In a real implementation, this would:
        // 1. Record replication latency and counts
        // 2. If an offsetSyncWriter is present, queue offset synchronization
        //    messages for the source topic-partition

        Ok(())
    }
}

impl Default for MirrorSourceTaskImpl {
    fn default() -> Self {
        Self::new()
    }
}
