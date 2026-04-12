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

//! Connector implementation traits and types for Kafka Connect.

use crate::connector_types::ConnectRecord;
use crate::data::{ConcreteHeaders, Schema, SchemaAndValue};
use crate::error::ConnectException;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================================
// Versioned trait - for components that have a version
// ============================================================================================

/// Trait for components that have a version string.
/// Connect requires some components implement this interface to define a version string.
pub trait Versioned {
    /// Get the version of this component.
    fn version(&self) -> String;
}

// ============================================================================================
// Connector Context traits
// ============================================================================================

/// ConnectorContext allows Connectors to proactively interact with the Kafka Connect runtime.
pub trait ConnectorContext: Send + Sync {
    /// Requests that the runtime reconfigure the Tasks for this source.
    /// This should be used to indicate to the runtime that something about the input/output
    /// has changed (e.g. partitions added/removed) and the running Tasks will need to be modified.
    fn request_task_reconfiguration(&self);

    /// Raise an unrecoverable exception to the Connect framework.
    /// This will cause the status of the connector to transition to FAILED.
    fn raise_error(&self, error: ConnectException);
}

/// SourceConnectorContext extends ConnectorContext with source-specific functionality.
pub trait SourceConnectorContext: ConnectorContext {
    /// Returns the OffsetStorageReader for this SourceConnectorContext.
    fn offset_storage_reader(&self) -> Arc<dyn OffsetStorageReader>;
}

/// SinkConnectorContext extends ConnectorContext with sink-specific functionality.
pub trait SinkConnectorContext: ConnectorContext {}

/// OffsetStorageReader provides access to the offset storage used by sources.
pub trait OffsetStorageReader: Send + Sync {
    /// Get the offset for the specified partition.
    fn offset(
        &self,
        partition: &HashMap<String, String>,
    ) -> Option<HashMap<String, Box<dyn Any + Send + Sync>>>;

    /// Get a set of offsets for the specified partition identifiers.
    fn offsets(
        &self,
        partitions: &[HashMap<String, String>],
    ) -> HashMap<HashMap<String, String>, HashMap<String, Box<dyn Any + Send + Sync>>>;
}

// ============================================================================================
// Task Context traits
// ============================================================================================

/// SourceTaskContext is provided to SourceTasks to allow them to interact with the underlying runtime.
pub trait SourceTaskContext: Send + Sync {
    /// Get the Task configuration.
    fn configs(&self) -> HashMap<String, String>;

    /// Get the OffsetStorageReader for this SourceTask.
    fn offset_storage_reader(&self) -> Arc<dyn OffsetStorageReader>;
}

/// SinkTaskContext allows SinkTasks to access utilities in the Kafka Connect runtime.
pub trait SinkTaskContext: Send + Sync {
    /// Get the Task configuration.
    fn configs(&self) -> HashMap<String, String>;

    /// Reset the consumer offsets for the given topic partitions.
    fn offset(&self, offsets: HashMap<TopicPartition, i64>);

    /// Reset the consumer offset for a single topic partition.
    fn offset_single(&self, tp: TopicPartition, offset: i64);

    /// Set the timeout in milliseconds.
    fn timeout(&self, timeout_ms: i64);

    /// Get the current set of assigned TopicPartitions for this task.
    fn assignment(&self) -> Vec<TopicPartition>;

    /// Pause consumption of messages from the specified TopicPartitions.
    fn pause(&self, partitions: &[TopicPartition]);

    /// Resume consumption of messages from previously paused TopicPartitions.
    fn resume(&self, partitions: &[TopicPartition]);

    /// Request an offset commit.
    fn request_commit(&self);
}

// ============================================================================================
// TopicPartition
// ============================================================================================

/// Represents a Kafka topic partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    /// The topic name
    pub topic: String,
    /// The partition number
    pub partition: i32,
}

impl TopicPartition {
    /// Create a new TopicPartition.
    pub fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }
}

impl std::fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

// ============================================================================================
// ExactlyOnce Support
// ============================================================================================

/// Represents whether a connector supports exactly-once semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactlyOnceSupport {
    /// The connector can provide exactly-once support
    Supported,
    /// The connector cannot provide exactly-once support
    Unsupported,
}

/// Represents whether a connector can define transaction boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorTransactionBoundaries {
    /// The connector will define its own transaction boundaries
    Supported,
    /// The connector cannot define transaction boundaries
    Unsupported,
}

/// Transaction boundary style for source connectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionBoundary {
    /// A new transaction will be started and committed for every batch of records returned by poll().
    Poll,
    /// Transactions will be started and committed on a user-defined time interval.
    Interval,
    /// Transactions will be defined by the connector itself, via a TransactionContext.
    Connector,
}

impl Default for TransactionBoundary {
    fn default() -> Self {
        TransactionBoundary::Poll
    }
}

// ============================================================================================
// Transaction Context
// ============================================================================================

/// TransactionContext can be used to define producer transaction boundaries
/// when exactly-once support is enabled for the connector.
pub trait TransactionContext: Send + Sync {
    /// Signal the start of a new transaction.
    fn begin_transaction(&self) -> Result<(), ConnectException>;

    /// Signal a commit of the current transaction.
    fn commit_transaction(&self) -> Result<(), ConnectException>;

    /// Signal an abort of the current transaction.
    fn abort_transaction(&self) -> Result<(), ConnectException>;
}

// ============================================================================================
// Record metadata
// ============================================================================================

/// Metadata for a produced record.
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// The topic the record was sent to
    pub topic: String,
    /// The partition the record was sent to
    pub partition: i32,
    /// The offset of the record in the partition
    pub offset: i64,
    /// The timestamp of the record
    pub timestamp: Option<i64>,
}

impl RecordMetadata {
    /// Create a new RecordMetadata.
    pub fn new(topic: String, partition: i32, offset: i64) -> Self {
        Self {
            topic,
            partition,
            offset,
            timestamp: None,
        }
    }
}

// ============================================================================================
// Config validation
// ============================================================================================

/// Configuration validation result.
#[derive(Debug, Clone)]
pub struct Config {
    /// The validated configuration values
    pub config_values: Vec<ConfigKey>,
}

impl Config {
    /// Create a new Config.
    pub fn new(config_values: Vec<ConfigKey>) -> Self {
        Self { config_values }
    }
}

// ============================================================================================
// Connector base trait
// ============================================================================================

/// Base trait for all connectors.
/// Connectors manage integration of Kafka Connect with another system, either as an input
/// that ingests data into Kafka or an output that passes data to an external system.
pub trait Connector: Versioned + Send + Sync {
    /// Initialize this connector with a context.
    fn initialize(&mut self, ctx: Arc<dyn ConnectorContext>);

    /// Start this Connector.
    fn start(&self, props: HashMap<String, String>) -> Result<(), ConnectException>;

    /// Reconfigure this Connector.
    fn reconfigure(&self, props: HashMap<String, String>) -> Result<(), ConnectException> {
        self.stop()?;
        self.start(props)
    }

    /// Returns the Task implementation class for this Connector.
    fn task_class(&self) -> String;

    /// Returns a set of configurations for Tasks based on the current configuration.
    fn task_configs(
        &self,
        max_tasks: i32,
    ) -> Result<Vec<HashMap<String, String>>, ConnectException>;

    /// Stop this connector.
    fn stop(&self) -> Result<(), ConnectException>;

    /// Validate the connector configuration values against configuration definitions.
    fn validate(&self, connector_configs: &HashMap<String, String>) -> Config {
        // Default implementation - subclasses should override if they have custom config
        Config::new(vec![])
    }

    /// Get the connector type (Source or Sink).
    fn connector_type(&self) -> ConnectorTypeKind;
}

/// Connector type kind enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorTypeKind {
    /// Source connector
    Source,
    /// Sink connector
    Sink,
}

// ============================================================================================
// SourceConnector trait
// ============================================================================================

/// SourceConnectors implement the connector interface to pull data from another system and send it to Kafka.
pub trait SourceConnector: Connector {
    /// Signals whether the connector supports exactly-once semantics with a proposed configuration.
    fn exactly_once_support(
        &self,
        _connector_config: &HashMap<String, String>,
    ) -> Option<ExactlyOnceSupport> {
        None
    }

    /// Signals whether the connector implementation is capable of defining the transaction boundaries.
    fn can_define_transaction_boundaries(
        &self,
        _connector_config: &HashMap<String, String>,
    ) -> ConnectorTransactionBoundaries {
        ConnectorTransactionBoundaries::Unsupported
    }

    /// Invoked when users request to manually alter/reset the offsets for this connector.
    fn alter_offsets(
        &self,
        _connector_config: &HashMap<String, String>,
        _offsets: &HashMap<Box<dyn Any + Send + Sync>, Option<Box<dyn Any + Send + Sync>>>,
    ) -> Result<bool, ConnectException> {
        Ok(false)
    }
}

// ============================================================================================
// SinkConnector trait
// ============================================================================================

/// SinkConnectors implement the Connector interface to send Kafka data to another system.
pub trait SinkConnector: Connector {
    /// Invoked when users request to manually alter/reset the offsets for this connector.
    fn alter_offsets(
        &self,
        _connector_config: &HashMap<String, String>,
        _offsets: &HashMap<TopicPartition, Option<i64>>,
    ) -> Result<bool, ConnectException> {
        Ok(false)
    }
}

// ============================================================================================
// Task base trait
// ============================================================================================

/// Task contains the code that actually copies data to/from another system.
pub trait Task: Versioned + Send + Sync {
    /// Start the Task.
    fn start(&self, props: HashMap<String, String>) -> Result<(), ConnectException>;

    /// Stop this task.
    fn stop(&self);
}

// ============================================================================================
// SourceTask trait
// ============================================================================================

/// SourceTask is a Task that pulls records from another system for storage in Kafka.
pub trait SourceTask: Task {
    /// Poll this source task for new records.
    fn poll(&self) -> Result<Option<Vec<SourceRecord>>, ConnectException>;

    /// This method is invoked periodically when offsets are committed for this source task.
    fn commit(&self) -> Result<(), ConnectException> {
        // Default implementation does nothing
        Ok(())
    }

    /// Commit an individual SourceRecord when the callback from the producer client is received.
    fn commit_record(
        &self,
        _record: &SourceRecord,
        _metadata: Option<RecordMetadata>,
    ) -> Result<(), ConnectException> {
        // Default implementation does nothing
        Ok(())
    }
}

/// SimpleHeaders is a simple implementation of HeadersTrait
#[derive(Debug, Default)]
pub struct SimpleHeaders {
    headers: std::collections::HashMap<String, Vec<u8>>,
}

impl SimpleHeaders {
    /// Create a new SimpleHeaders
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if headers are empty
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    /// Get the number of headers
    pub fn len(&self) -> usize {
        self.headers.len()
    }

    /// Add a header
    pub fn add(&mut self, key: String, value: Vec<u8>) {
        self.headers.insert(key, value);
    }

    /// Get a header value by key
    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.headers.get(key).map(|v| v.as_slice())
    }

    /// Remove a header by key
    pub fn remove(&mut self, key: &str) {
        self.headers.remove(key);
    }

    /// List all header keys
    pub fn list(&self) -> Vec<String> {
        self.headers.keys().cloned().collect()
    }
}

impl HeadersTrait for SimpleHeaders {
    /// Get iterator over headers.
    fn iter(&self) -> Box<dyn Iterator<Item = Box<dyn HeaderTrait>> + '_> {
        Box::new(self.headers.iter().map(|(key, value)| {
            Box::new(SimpleHeader::new(key.clone(), value.clone())) as Box<dyn HeaderTrait>
        }))
    }

    /// Get headers with the specified key.
    fn with_key(&self, key: &str) -> Vec<Box<dyn HeaderTrait>> {
        self.headers
            .iter()
            .filter(|(k, _)| *k == key)
            .map(|(k, v)| Box::new(SimpleHeader::new(k.clone(), v.clone())) as Box<dyn HeaderTrait>)
            .collect()
    }
}

/// SimpleHeader is a simple implementation of HeaderTrait
#[derive(Debug, Clone)]
pub struct SimpleHeader {
    key: String,
    value: Vec<u8>,
}

impl SimpleHeader {
    /// Create a new SimpleHeader
    pub fn new(key: String, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}

impl HeaderTrait for SimpleHeader {
    /// Get the header key.
    fn key(&self) -> &str {
        &self.key
    }

    /// Get the schema.
    fn schema(&self) -> Option<Arc<dyn Schema>> {
        None
    }

    /// Get the value.
    fn value(&self) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        Some(Box::new(self.value.clone()))
    }
}

impl crate::data::Header for SimpleHeader {
    fn key(&self) -> &str {
        &self.key
    }

    fn schema(&self) -> Arc<dyn Schema> {
        Arc::new(crate::data::ConnectSchema::new(crate::data::Type::Bytes))
    }

    fn value(&self) -> Arc<dyn std::any::Any + Send + Sync> {
        Arc::new(self.value.clone())
    }

    fn with(
        &self,
        schema: Arc<dyn Schema>,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Box<dyn crate::data::Header> {
        let bytes = value
            .downcast_ref::<Vec<u8>>()
            .map(|v| v.clone())
            .unwrap_or_default();
        Box::new(SimpleHeader::new(self.key.clone(), bytes))
    }

    fn rename(&self, key: &str) -> Box<dyn crate::data::Header> {
        Box::new(SimpleHeader::new(key.to_string(), self.value.clone()))
    }
}

impl crate::data::Headers for SimpleHeaders {
    fn size(&self) -> usize {
        self.headers.len()
    }

    fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    fn all_with_name(&self, key: &str) -> Vec<Box<dyn crate::data::Header>> {
        self.headers
            .get(key)
            .map(|v| {
                vec![Box::new(SimpleHeader::new(key.to_string(), v.clone()))
                    as Box<dyn crate::data::Header>]
            })
            .unwrap_or_default()
    }

    fn last_with_name(&self, key: &str) -> Option<Box<dyn crate::data::Header>> {
        self.headers.get(key).map(|v| {
            Box::new(SimpleHeader::new(key.to_string(), v.clone())) as Box<dyn crate::data::Header>
        })
    }

    fn add(&mut self, header: Box<dyn crate::data::Header>) {
        let bytes: Vec<u8> = header
            .value()
            .downcast::<Vec<u8>>()
            .map(|v| (*v).clone())
            .unwrap_or_default();
        self.headers.insert(header.key().to_string(), bytes);
    }

    fn remove_headers(&mut self, _keys: &[String]) -> Box<dyn crate::data::Headers> {
        Box::new(SimpleHeaders::new())
    }

    fn remove(&mut self, key: &str) -> Box<dyn crate::data::Headers> {
        let mut new_headers = SimpleHeaders::new();
        for (k, v) in &self.headers {
            if k != key {
                new_headers.headers.insert(k.clone(), v.clone());
            }
        }
        Box::new(new_headers)
    }

    fn duplicate(&self) -> Box<dyn crate::data::Headers> {
        let mut new_headers = SimpleHeaders::new();
        new_headers.headers = self.headers.clone();
        Box::new(new_headers)
    }

    fn iter(&self) -> Box<dyn Iterator<Item = &dyn crate::data::Header> + '_> {
        // This is a simple implementation
        // Note: This is a simplified implementation that returns an empty iterator
        // In real code, you'd want to properly iterate over headers
        Box::new(std::iter::empty())
    }
}

///' SourceRecord represents a record read from a source system.
pub struct SourceRecord {
    /// The topic the record came from
    pub topic: String,
    /// The partition the record came from
    pub partition: Option<i32>,
    /// The source partition identifier
    pub source_partition: HashMap<String, String>,
    /// The source offset
    pub source_offset: HashMap<String, String>,
    /// The key schema
    pub key_schema: Option<Arc<dyn crate::data::Schema>>,
    /// The record key
    pub key: Option<Box<dyn std::any::Any + Send + Sync>>,
    /// The value schema
    pub value_schema: Option<Arc<dyn crate::data::Schema>>,
    /// The record value
    pub value: Option<Box<dyn std::any::Any + Send + Sync>>,
    /// The timestamp
    pub timestamp: Option<i64>,
    /// The headers
    pub headers: Box<dyn crate::data::Headers>,
}

impl std::fmt::Debug for SourceRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceRecord")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("source_partition", &self.source_partition)
            .field("source_offset", &self.source_offset)
            .field("key", &"Box<dyn Any>")
            .field("value", &"Box<dyn Any>")
            .field("timestamp", &self.timestamp)
            .field("headers", &"Box<dyn Headers>")
            .finish()
    }
}

impl Clone for SourceRecord {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            partition: self.partition,
            source_partition: self.source_partition.clone(),
            source_offset: self.source_offset.clone(),
            key_schema: self.key_schema.clone(),
            key: None, // Cannot clone Box<dyn Any>
            value_schema: self.value_schema.clone(),
            value: None, // Cannot clone Box<dyn Any>
            timestamp: self.timestamp,
            headers: Box::new(SimpleHeaders::new()), // Cannot clone Box<dyn HeadersTrait>
        }
    }
}

impl SourceRecord {
    /// Create a new SourceRecord.
    pub fn new(
        topic: String,
        source_partition: HashMap<String, String>,
        source_offset: HashMap<String, String>,
    ) -> Self {
        Self {
            topic,
            partition: None,
            source_partition,
            source_offset,
            key_schema: None,
            key: None,
            value_schema: None,
            value: None,
            timestamp: None,
            headers: Box::new(SimpleHeaders::new()),
        }
    }

    /// Builder for SourceRecord.
    pub fn builder() -> SourceRecordBuilder {
        SourceRecordBuilder::new()
    }
}

/// Builder for SourceRecord.
#[derive(Default)]
pub struct SourceRecordBuilder {
    topic: Option<String>,
    partition: Option<i32>,
    source_partition: HashMap<String, String>,
    source_offset: HashMap<String, String>,
    key_schema: Option<Arc<dyn crate::data::Schema>>,
    key: Option<Box<dyn std::any::Any + Send + Sync>>,
    value_schema: Option<Arc<dyn crate::data::Schema>>,
    value: Option<Box<dyn std::any::Any + Send + Sync>>,
    timestamp: Option<i64>,
    headers: Option<Box<dyn crate::data::Headers>>,
}

impl SourceRecordBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the topic.
    pub fn topic(mut self, topic: String) -> Self {
        self.topic = Some(topic);
        self
    }

    /// Set the partition.
    pub fn partition(mut self, partition: Option<i32>) -> Self {
        self.partition = partition;
        self
    }

    /// Set the source partition.
    pub fn source_partition(mut self, source_partition: HashMap<String, String>) -> Self {
        self.source_partition = source_partition;
        self
    }

    /// Set the source offset.
    pub fn source_offset(mut self, source_offset: HashMap<String, String>) -> Self {
        self.source_offset = source_offset;
        self
    }

    /// Set the key schema.
    pub fn key_schema(mut self, key_schema: Arc<dyn crate::data::Schema>) -> Self {
        self.key_schema = Some(key_schema);
        self
    }

    /// Set the key.
    pub fn key(mut self, key: Box<dyn std::any::Any + Send + Sync>) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the value schema.
    pub fn value_schema(mut self, value_schema: Arc<dyn crate::data::Schema>) -> Self {
        self.value_schema = Some(value_schema);
        self
    }

    /// Set the value.
    pub fn value(mut self, value: Box<dyn std::any::Any + Send + Sync>) -> Self {
        self.value = Some(value);
        self
    }

    /// Set the timestamp.
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set the headers.
    pub fn headers(mut self, headers: Box<dyn crate::data::Headers>) -> Self {
        self.headers = Some(headers);
        self
    }

    /// Build the SourceRecord.
    pub fn build(self) -> SourceRecord {
        SourceRecord {
            topic: self.topic.unwrap_or_default(),
            partition: self.partition,
            source_partition: self.source_partition,
            source_offset: self.source_offset,
            key_schema: self.key_schema,
            key: self.key,
            value_schema: self.value_schema,
            value: self.value,
            timestamp: self.timestamp,
            headers: self
                .headers
                .unwrap_or_else(|| Box::new(SimpleHeaders::new())),
        }
    }
}

impl ConnectRecord<SourceRecord> for SourceRecord {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn kafka_partition(&self) -> Option<i32> {
        self.partition
    }

    fn key_schema(&self) -> Option<Arc<dyn crate::data::Schema>> {
        self.key_schema.clone()
    }

    fn key(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        // Cannot convert Box<dyn Any> to Arc<dyn Any> without cloning the inner value
        // Return None as Box cannot be converted to Arc directly
        None
    }

    fn value_schema(&self) -> Option<Arc<dyn crate::data::Schema>> {
        self.value_schema.clone()
    }

    fn value(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        // Cannot convert Box<dyn Any> to Arc<dyn Any> without cloning the inner value
        // Return None as Box cannot be converted to Arc directly
        None
    }

    fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    fn headers(&self) -> &dyn crate::data::Headers {
        // Convert Box<dyn HeadersTrait> to &dyn Headers
        // This requires HeadersTrait to be compatible with Headers trait
        // For now, return a reference to a default ConcreteHeaders
        static DEFAULT_HEADERS: std::sync::OnceLock<ConcreteHeaders> = std::sync::OnceLock::new();
        DEFAULT_HEADERS.get_or_init(|| ConcreteHeaders::new())
    }

    fn new_record(
        self,
        topic: Option<&str>,
        partition: Option<i32>,
        key_schema: Option<Arc<dyn crate::data::Schema>>,
        key: Option<Arc<dyn std::any::Any + Send + Sync>>,
        value_schema: Option<Arc<dyn crate::data::Schema>>,
        value: Option<Arc<dyn std::any::Any + Send + Sync>>,
        timestamp: Option<i64>,
        headers: Option<Box<dyn crate::data::Headers>>,
    ) -> SourceRecord {
        // Create a new SourceRecord with the provided values
        // Use self values as defaults if None is provided
        SourceRecord {
            topic: topic.unwrap_or(&self.topic).to_string(),
            partition: partition.or(self.partition),
            source_partition: self.source_partition,
            source_offset: self.source_offset,
            key_schema: key_schema.or(self.key_schema),
            key: None,
            value_schema: value_schema.or(self.value_schema),
            value: None,
            timestamp: timestamp.or(self.timestamp),
            headers: headers.unwrap_or_else(|| Box::new(SimpleHeaders::new())),
        }
    }
}

// ============================================================================================
// SinkTask trait
// ============================================================================================

/// SinkTask is a Task that takes records loaded from Kafka and sends them to another system.
pub trait SinkTask: Task {
    /// Put the records in the sink.
    fn put(&self, records: &[SinkRecord]) -> Result<(), ConnectException>;

    /// Flush all records that have been put for the specified topic-partitions.
    fn flush(
        &self,
        _current_offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), ConnectException> {
        // Default implementation does nothing
        Ok(())
    }

    /// Pre-commit hook invoked prior to an offset commit.
    fn pre_commit(
        &self,
        current_offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, ConnectException> {
        self.flush(current_offsets)?;
        Ok(current_offsets.clone())
    }

    /// Open writers for newly assigned partitions.
    fn open(&self, _partitions: &[TopicPartition]) -> Result<(), ConnectException> {
        // Default implementation does nothing
        Ok(())
    }

    /// Close writers for partitions that are no longer assigned.
    fn close(&self, _partitions: &[TopicPartition]) -> Result<(), ConnectException> {
        // Default implementation does nothing
        Ok(())
    }
}

/// SinkRecord represents a record read from Kafka and destined for a sink system.
#[derive(Debug)]
pub struct SinkRecord {
    /// The topic
    pub topic: String,
    /// The Kafka partition
    pub kafka_partition: i32,
    /// The record key
    pub key: Option<Box<dyn std::any::Any + Send + Sync>>,
    /// The record value
    pub value: Option<Box<dyn std::any::Any + Send + Sync>>,
    /// The timestamp
    pub timestamp: Option<i64>,
    /// The Kafka offset
    pub kafka_offset: i64,
    /// The original topic (before transformations)
    pub original_topic: Option<String>,
    /// The original Kafka partition (before transformations)
    pub original_kafka_partition: Option<i32>,
    /// The original Kafka offset (before transformations)
    pub original_kafka_offset: Option<i64>,
}

impl Clone for SinkRecord {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            kafka_partition: self.kafka_partition,
            key: None,   // Cannot clone Box<dyn Any>
            value: None, // Cannot clone Box<dyn Any>
            timestamp: self.timestamp,
            kafka_offset: self.kafka_offset,
            original_topic: self.original_topic.clone(),
            original_kafka_partition: self.original_kafka_partition,
            original_kafka_offset: self.original_kafka_offset,
        }
    }
}

impl SinkRecord {
    /// Create a new SinkRecord.
    pub fn new(topic: String, kafka_partition: i32, kafka_offset: i64) -> Self {
        Self {
            topic,
            kafka_partition,
            key: None,
            value: None,
            timestamp: None,
            kafka_offset,
            original_topic: None,
            original_kafka_partition: None,
            original_kafka_offset: None,
        }
    }

    /// Builder for SinkRecord.
    pub fn builder() -> SinkRecordBuilder {
        SinkRecordBuilder::new()
    }
}

/// Builder for SinkRecord.
#[derive(Default)]
pub struct SinkRecordBuilder {
    topic: Option<String>,
    kafka_partition: Option<i32>,
    key: Option<Box<dyn std::any::Any + Send + Sync>>,
    value: Option<Box<dyn std::any::Any + Send + Sync>>,
    timestamp: Option<i64>,
    kafka_offset: Option<i64>,
}

impl SinkRecordBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the topic.
    pub fn topic(mut self, topic: String) -> Self {
        self.topic = Some(topic);
        self
    }

    /// Set the Kafka partition.
    pub fn kafka_partition(mut self, kafka_partition: i32) -> Self {
        self.kafka_partition = Some(kafka_partition);
        self
    }

    /// Set the key.
    pub fn key(mut self, key: Box<dyn std::any::Any + Send + Sync>) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the value.
    pub fn value(mut self, value: Box<dyn std::any::Any + Send + Sync>) -> Self {
        self.value = Some(value);
        self
    }

    /// Set the timestamp.
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set the Kafka offset.
    pub fn kafka_offset(mut self, kafka_offset: i64) -> Self {
        self.kafka_offset = Some(kafka_offset);
        self
    }

    /// Build the SinkRecord.
    pub fn build(self) -> SinkRecord {
        SinkRecord {
            topic: self.topic.unwrap_or_default(),
            kafka_partition: self.kafka_partition.unwrap_or(0),
            key: self.key,
            value: self.value,
            timestamp: self.timestamp,
            kafka_offset: self.kafka_offset.unwrap_or(0),
            original_topic: None,
            original_kafka_partition: None,
            original_kafka_offset: None,
        }
    }
}

/// Offset and metadata for a topic partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetAndMetadata {
    /// The offset
    pub offset: i64,
    /// The metadata
    pub metadata: Option<String>,
    /// The leader epoch
    pub leader_epoch: Option<i32>,
}

impl OffsetAndMetadata {
    /// Create a new OffsetAndMetadata.
    pub fn new(offset: i64) -> Self {
        Self {
            offset,
            metadata: None,
            leader_epoch: None,
        }
    }

    /// Create a new OffsetAndMetadata with metadata.
    pub fn with_metadata(offset: i64, metadata: String) -> Self {
        Self {
            offset,
            metadata: Some(metadata),
            leader_epoch: None,
        }
    }

    /// Create a new OffsetAndMetadata with full options.
    pub fn with_all(offset: i64, metadata: Option<String>, leader_epoch: Option<i32>) -> Self {
        Self {
            offset,
            metadata,
            leader_epoch,
        }
    }
}

// ============================================================================================
// Converter trait
// ============================================================================================

/// The Converter interface provides support for translating between Kafka Connect's runtime data format
/// and byte arrays.
pub trait Converter: Send + Sync {
    /// Configure this converter.
    fn configure(&self, configs: HashMap<String, String>, is_key: bool);

    /// Convert a Kafka Connect data object to a native object for serialization.
    fn from_connect_data(
        &self,
        topic: &str,
        schema: Option<&dyn Schema>,
        value: &dyn std::any::Any,
    ) -> Result<Vec<u8>, ConnectException>;

    /// Convert a Kafka Connect data object to a native object for serialization with headers.
    fn from_connect_data_with_headers(
        &self,
        topic: &str,
        headers: &dyn HeadersTrait,
        schema: Option<&dyn Schema>,
        value: &dyn std::any::Any,
    ) -> Result<Vec<u8>, ConnectException> {
        // Default implementation ignores headers
        self.from_connect_data(topic, schema, value)
    }

    /// Convert a native object to a Kafka Connect data object for deserialization.
    fn to_connect_data(
        &self,
        topic: &str,
        value: &[u8],
    ) -> Result<SchemaAndValue, ConnectException>;

    /// Convert a native object to a Kafka Connect data object for deserialization with headers.
    fn to_connect_data_with_headers(
        &self,
        topic: &str,
        headers: &dyn HeadersTrait,
        value: &[u8],
    ) -> Result<SchemaAndValue, ConnectException> {
        // Default implementation ignores headers
        self.to_connect_data(topic, value)
    }
}

/// Headers trait for converter interface.
pub trait HeadersTrait: Send + Sync {
    /// Get iterator over headers.
    fn iter(&self) -> Box<dyn Iterator<Item = Box<dyn HeaderTrait>> + '_>;

    /// Get headers with the specified key.
    fn with_key(&self, key: &str) -> Vec<Box<dyn HeaderTrait>>;
}

/// Header trait for converter interface.
pub trait HeaderTrait: Send + Sync {
    /// Get the header key.
    fn key(&self) -> &str;

    /// Get the schema.
    fn schema(&self) -> Option<Arc<dyn Schema>>;

    /// Get the value.
    fn value(&self) -> Option<Box<dyn std::any::Any + Send + Sync>>;
}

// ============================================================================================
// HeaderConverter trait
// ============================================================================================

/// The HeaderConverter interface provides support for translating between Kafka Connect's runtime data format
/// and byte arrays for Headers.
pub trait HeaderConverter: Send + Sync {
    /// Convert the header name and byte array value into a Header object.
    fn to_connect_header(
        &self,
        topic: &str,
        header_key: &str,
        value: &[u8],
    ) -> Result<
        (
            Option<Arc<dyn Schema>>,
            Box<dyn std::any::Any + Send + Sync>,
        ),
        ConnectException,
    >;

    /// Convert the Header's value into its byte array representation.
    fn from_connect_header(
        &self,
        topic: &str,
        header_key: &str,
        schema: Option<&dyn Schema>,
        value: &dyn std::any::Any,
    ) -> Result<Option<Vec<u8>>, ConnectException>;
}

// ============================================================================================
// Transformation trait
// ============================================================================================

/// Single message transformation for Kafka Connect record types.
pub trait Transformation<R: Send + Sync>: Send + Sync {
    /// Apply transformation to record and return another record object or None to drop.
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn std::error::Error>>;

    /// Get the configuration definition for this transformation.
    fn config(&self) -> ConfigDef;
}

/// Configurable transformation that can be configured.
pub trait ConfigurableTransformation<R: Send + Sync>: Transformation<R> {
    /// Configure this transformation.
    fn configure(&self, configs: HashMap<String, String>) -> Result<(), ConnectException>;
}

// ============================================================================================
// Predicate trait
// ============================================================================================

/// Predicate for conditional transformations.
pub trait Predicate: Send + Sync {
    /// Test if the record matches the predicate.
    fn test(&self, record: &dyn ConnectRecordTrait) -> bool;
}

/// ConnectRecord trait for predicate evaluation.
pub trait ConnectRecordTrait: Send + Sync {
    /// Get the topic.
    fn topic(&self) -> &str;

    /// Get the key.
    fn key(&self) -> Option<&dyn std::any::Any>;

    /// Get the value.
    fn value(&self) -> Option<&dyn std::any::Any>;

    /// Get the headers.
    fn headers(&self) -> &dyn crate::data::Headers;
}

// ============================================================================================
// Errant Record Reporter
// ============================================================================================

/// Reporter for problematic or failed records.
pub trait ErrantRecordReporter: Send + Sync {
    /// Report a failed record.
    fn report(&self, record: SinkRecord, error: ConnectException) -> Result<(), ConnectException>;
}

// ============================================================================================
// ConfigDef types (simplified)
// ============================================================================================

/// Configuration definition.
/// This is a simplified version - full implementation would be in connect-runtime.
pub struct ConfigDef {
    /// Config keys
    keys: Vec<ConfigKey>,
}

impl ConfigDef {
    /// Create a new empty ConfigDef.
    pub fn new() -> Self {
        Self { keys: vec![] }
    }
}

impl Default for ConfigDef {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration key definition.
#[derive(Debug, Clone)]
pub struct ConfigKey {
    /// Name of the configuration
    pub name: String,
    /// Type of the configuration
    pub type_: ConfigKeyType,
    /// Documentation
    pub doc: Option<String>,
    /// Default value
    pub default_value: Option<String>,
    /// Required
    pub required: bool,
}

/// Configuration key type.
#[derive(Debug, Clone, Copy)]
pub enum ConfigKeyType {
    /// String type
    String,
    /// Int type
    Int,
    /// Long type
    Long,
    /// Boolean type
    Boolean,
    /// List type
    List,
    /// Password type
    Password,
    /// Class type
    Class,
}

// ============================================================================================
// ConfigValue enum for configuration values
// ============================================================================================

/// Configuration value enum.
#[derive(Debug, Clone)]
pub enum ConfigValue {
    /// String value
    String(String),
    /// Int value
    Int(i32),
    /// Long value
    Long(i64),
    /// Boolean value
    Boolean(bool),
    /// List value
    List(Vec<String>),
}

impl ConfigValue {
    /// Create a new String ConfigValue.
    pub fn new_string(value: String) -> Self {
        ConfigValue::String(value)
    }

    /// Create a new Int ConfigValue.
    pub fn new_int(value: i32) -> Self {
        ConfigValue::Int(value)
    }

    /// Create a new Long ConfigValue.
    pub fn new_long(value: i64) -> Self {
        ConfigValue::Long(value)
    }

    /// Create a new Boolean ConfigValue.
    pub fn new_boolean(value: bool) -> Self {
        ConfigValue::Boolean(value)
    }

    /// Create a new List ConfigValue.
    pub fn new_list(value: Vec<String>) -> Self {
        ConfigValue::List(value)
    }
}

// Add convenience methods for ConfigDef
impl ConfigDef {
    /// Add a configuration to the definition.
    pub fn add_config(&mut self, name: String, value: ConfigValue) {
        self.keys.push(ConfigKey {
            name,
            type_: match value {
                ConfigValue::String(_) => ConfigKeyType::String,
                ConfigValue::Int(_) => ConfigKeyType::Int,
                ConfigValue::Long(_) => ConfigKeyType::Long,
                ConfigValue::Boolean(_) => ConfigKeyType::Boolean,
                ConfigValue::List(_) => ConfigKeyType::List,
            },
            doc: None,
            default_value: None,
            required: false,
        });
    }

    /// Get a configuration by name.
    pub fn get_config(&self, name: &str) -> Option<&ConfigKey> {
        self.keys.iter().find(|k| k.name == name)
    }
}

// ============================================================================================
// Closeable trait
// ============================================================================================

/// Trait for components that can be closed.
pub trait Closeable {
    /// Close this component and release any resources.
    fn close(&mut self) -> Result<(), Box<dyn std::error::Error>>;
}

// ============================================================================================
// Configurable trait
// ============================================================================================

/// Trait for components that can be configured.
pub trait Configurable {
    /// Configure this component with the provided configuration.
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>);
}
