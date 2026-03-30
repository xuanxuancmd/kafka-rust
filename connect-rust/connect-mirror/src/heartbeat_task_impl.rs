//! MirrorHeartbeatTask implementation
//!
//! This module provides concrete implementation of MirrorHeartbeatTask.

use anyhow::Result;
use connect_api::{RecordMetadata, SourceRecord, SourceTask};
use connect_mirror_client::Heartbeat;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::task::MirrorHeartbeatTask as MirrorHeartbeatTaskTrait;

/// Concrete implementation of MirrorHeartbeatTask
///
/// This task publishes heartbeat records to monitor replication health.
pub struct MirrorHeartbeatTaskImpl {
    /// Source cluster alias
    source_cluster_alias: Option<String>,
    /// Target cluster alias
    target_cluster_alias: Option<String>,
    /// Heartbeats topic name
    heartbeats_topic: Option<String>,
    /// Interval between heartbeats
    interval: Duration,
    /// Last heartbeat timestamp
    last_heartbeat: Arc<Mutex<Option<Instant>>>,
    /// Whether task is stopped
    stopped: Arc<Mutex<bool>>,
}

impl MirrorHeartbeatTaskImpl {
    /// Create a new MirrorHeartbeatTaskImpl
    pub fn new() -> Self {
        MirrorHeartbeatTaskImpl {
            source_cluster_alias: None,
            target_cluster_alias: None,
            heartbeats_topic: None,
            interval: Duration::from_secs(5), // Default 5 seconds
            last_heartbeat: Arc::new(Mutex::new(None)),
            stopped: Arc::new(Mutex::new(false)),
        }
    }

    /// Create a new MirrorHeartbeatTaskImpl with custom parameters
    pub fn with_params(
        source_cluster_alias: String,
        target_cluster_alias: String,
        heartbeats_topic: String,
        interval_ms: u64,
    ) -> Self {
        MirrorHeartbeatTaskImpl {
            source_cluster_alias: Some(source_cluster_alias),
            target_cluster_alias: Some(target_cluster_alias),
            heartbeats_topic: Some(heartbeats_topic),
            interval: Duration::from_millis(interval_ms),
            last_heartbeat: Arc::new(Mutex::new(None)),
            stopped: Arc::new(Mutex::new(false)),
        }
    }

    /// Create a heartbeat record
    pub fn create_heartbeat(&self) -> Result<Heartbeat> {
        let source_alias = self.source_cluster_alias.clone().unwrap_or_default();
        let target_alias = self.target_cluster_alias.clone().unwrap_or_default();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64;

        Ok(Heartbeat {
            source_cluster_alias: source_alias,
            target_cluster_alias: target_alias,
            timestamp,
        })
    }

    /// Convert a heartbeat to a source record
    pub fn heartbeat_record(&self, heartbeat: &Heartbeat) -> Result<SourceRecord> {
        let source_partition = HashMap::new();
        let mut source_offset = HashMap::new();
        source_offset.insert("timestamp".to_string(), heartbeat.timestamp);

        // Serialize heartbeat to bytes
        let heartbeat_bytes = serde_json::to_vec(heartbeat).unwrap_or_default();

        Ok(SourceRecord {
            topic: self.heartbeats_topic.clone().unwrap_or_default(),
            kafka_partition: Some(0),
            key_schema: None,
            key: Some(Box::new(heartbeat_bytes) as Box<dyn std::any::Any>),
            value_schema: None,
            value: Some(Box::new(heartbeat_bytes) as Box<dyn std::any::Any>),
            timestamp: Some(heartbeat.timestamp),
            headers: connect_api::data::Headers::new(),
            source_partition,
            source_offset,
        })
    }

    /// Check if it's time to send a heartbeat
    pub fn should_send_heartbeat(&self) -> bool {
        let last_heartbeat = self.last_heartbeat.lock().unwrap();

        match *last_heartbeat {
            Some(last) => {
                let elapsed = last.elapsed();
                elapsed >= self.interval
            }
            None => true,
        }
    }

    /// Update last heartbeat timestamp
    pub fn update_last_heartbeat(&self) {
        let mut last_heartbeat = self.last_heartbeat.lock().unwrap();
        *last_heartbeat = Some(Instant::now());
    }
}

impl MirrorHeartbeatTaskTrait for MirrorHeartbeatTaskImpl {
    // Heartbeat task primarily uses the poll() method from SourceTask
}

impl SourceTask for MirrorHeartbeatTaskImpl {
    fn initialize(&mut self, _context: Box<dyn connect_api::SourceTaskContext>) {
        // Initialize task with context
    }

    fn start(&mut self, props: HashMap<String, String>) {
        // Parse configuration
        let source_alias = props.get("source.cluster.alias").cloned();
        let target_alias = props.get("target.cluster.alias").cloned();
        let heartbeat_topic = props.get("heartbeats.topic").cloned();
        let interval_ms = props
            .get("heartbeats.interval.ms")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5000);

        self.source_cluster_alias = source_alias;
        self.target_cluster_alias = target_alias;
        self.heartbeats_topic = heartbeat_topic;
        self.interval = Duration::from_millis(interval_ms);

        // Create a CountDownLatch for stopping the task
        let mut stopped = self.stopped.lock().unwrap();
        *stopped = false;
    }

    fn poll(&mut self) -> Result<Vec<SourceRecord>, Box<dyn std::error::Error>> {
        // Check if task is stopped
        let stopped = self.stopped.lock().unwrap();
        if *stopped {
            return Ok(Vec::new());
        }

        // Check if it's time to send a heartbeat
        if !self.should_send_heartbeat() {
            return Ok(Vec::new());
        }

        // Create heartbeat record
        let heartbeat = self.create_heartbeat()?;
        let record = self.heartbeat_record(&heartbeat)?;

        // Update last heartbeat timestamp
        self.update_last_heartbeat();

        Ok(vec![record])
    }

    fn commit(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Heartbeat task does not need to commit offsets
        Ok(())
    }

    fn commit_record(
        &mut self,
        _record: SourceRecord,
        _metadata: RecordMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Heartbeat task does not need to commit individual records
        Ok(())
    }

    fn stop(&mut self) {
        // Signal the task to stop by counting down the stopped latch
        let mut stopped = self.stopped.lock().unwrap();
        *stopped = true;
    }
}

impl Default for MirrorHeartbeatTaskImpl {
    fn default() -> Self {
        Self::new()
    }
}
