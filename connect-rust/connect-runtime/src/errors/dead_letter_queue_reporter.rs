//! Dead letter queue reporter module
//!
//! Provides a DeadLetterQueueReporter for writing original consumed records into a dead letter queue.

use connect_runtime_core::errors::ConnectRuntimeError;
use crate::errors::error_handling_metrics::ErrorHandlingMetrics;
use crate::errors::error_reporter::ErrorReporter;
use crate::errors::processing_context::ProcessingContext;
use crate::errors::stage::Stage;
use async_trait::async_trait;
use kafka_clients_trait::admin::AdminClient;
use kafka_clients_trait::producer::{Producer, ProducerRecord, RecordMetadata};
use std::sync::Arc;
use tracing::error;

/// Number of desired partitions for DLQ topic
const DLQ_NUM_DESIRED_PARTITIONS: i32 = 1;

/// Header prefix for error headers
const HEADER_PREFIX: &str = "__connect.errors.";

/// Error header for original topic
const ERROR_HEADER_ORIG_TOPIC: &str = "__connect.errors.topic";

/// Error header for original partition
const ERROR_HEADER_ORIG_PARTITION: &str = "__connect.errors.partition";

/// Error header for original offset
const ERROR_HEADER_ORIG_OFFSET: &str = "__connect.errors.offset";

/// Error header for connector name
const ERROR_HEADER_CONNECTOR_NAME: &str = "__connect.errors.connector.name";

/// Error header for task ID
const ERROR_HEADER_TASK_ID: &str = "__connect.errors.task.id";

/// Error header for stage
const ERROR_HEADER_STAGE: &str = "__connect.errors.stage";

/// Error header for executing class
const ERROR_HEADER_EXECUTING_CLASS: &str = "__connect.errors.class.name";

/// Error header for exception class name
const ERROR_HEADER_EXCEPTION: &str = "__connect.errors.exception.class.name";

/// Error header for exception message
const ERROR_HEADER_EXCEPTION_MESSAGE: &str = "__connect.errors.exception.message";

/// Error header for exception stack trace
const ERROR_HEADER_EXCEPTION_STACK_TRACE: &str = "__connect.errors.exception.stacktrace";

/// Write the original consumed record into a dead letter queue. The dead letter queue is a Kafka topic located
/// on the same cluster used by the worker to maintain internal topics. Each connector is typically configured
/// with its own Kafka topic dead letter queue. By default, the topic name is not set, and if the
/// connector config doesn't specify one, this feature is disabled.
pub struct DeadLetterQueueReporter<P, A>
where
    P: Producer<Vec<u8>, Vec<u8>> + Send + Sync,
    A: AdminClient + Send + Sync,
{
    /// Connector task ID
    connector_task_id: String,

    /// DLQ topic name
    dlq_topic_name: String,

    /// Whether DLQ context headers are enabled
    dlq_context_headers_enabled: bool,

    /// Error handling metrics
    error_handling_metrics: Arc<ErrorHandlingMetrics>,

    /// Kafka producer
    kafka_producer: Arc<P>,

    /// Admin client
    admin_client: Arc<A>,
}

impl<P, A> DeadLetterQueueReporter<P, A>
where
    P: Producer<Vec<u8>, Vec<u8>> + Send + Sync,
    A: AdminClient + Send + Sync,
{
    /// Create and setup a new dead letter queue reporter
    ///
    /// # Arguments
    ///
    /// * `connector_task_id` - connector task ID
    /// * `dlq_topic_name` - DLQ topic name
    /// * `dlq_context_headers_enabled` - whether DLQ context headers are enabled
    /// * `dlq_topic_replication_factor` - DLQ topic replication factor
    /// * `error_handling_metrics` - error handling metrics
    /// * `kafka_producer` - Kafka producer
    /// * `admin_client` - admin client
    ///
    /// # Returns
    ///
    /// Result containing the dead letter queue reporter or an error
    pub async fn create_and_setup(
        connector_task_id: String,
        dlq_topic_name: String,
        dlq_context_headers: bool,
        dlq_topic_replication_factor: i16,
        error_handling_metrics: Arc<ErrorHandlingMetrics>,
        kafka_producer: Arc<P>,
        admin_client: Arc<A>,
    ) -> Result<Self, ConnectRuntimeError> {
        let topic = dlq_topic_name.trim().to_string();

        // Check if topic exists, create if not
        let topics = admin_client.list_topics().await.map_err(|e| {
            ConnectRuntimeError::connector_error(format!("Failed to list topics: {}", e))
        })?;

        if !topics.contains(&topic) {
            error!("Topic {} doesn't exist. Will attempt to create topic.", topic);
            admin_client
                .create_topic(&topic, DLQ_NUM_DESIRED_PARTITIONS, dlq_topic_replication_factor)
                .await
                .map_err(|e| {
                    ConnectRuntimeError::connector_error(format!(
                        "Could not initialize dead letter queue with topic={}: {}",
                        topic, e
                    ))
                })?;
        }

        Ok(Self {
            connector_task_id,
            dlq_topic_name: topic,
            dlq_context_headers_enabled: dlq_context_headers,
            error_handling_metrics,
            kafka_producer,
            admin_client,
        })
    }

    /// Write the raw records into a Kafka topic and return the producer future
    ///
    /// # Arguments
    ///
    /// * `context` - processing context containing the raw record at ProcessingContext::original()
    ///
    /// # Returns
    ///
    /// Result containing the record metadata or an error
    async fn report_internal(
        &self,
        context: &ProcessingContext<Vec<u8>>,
    ) -> Result<Option<RecordMetadata>, ConnectRuntimeError> {
        if self.dlq_topic_name.is_empty() {
            return Ok(None);
        }

        self.error_handling_metrics.record_dead_letter_queue_produce_request();

        if context.original().is_empty() {
            self.error_handling_metrics.record_dead_letter_queue_produce_failed();
            return Ok(None);
        }

        let original_message = context.original();

        let producer_record = ProducerRecord::builder()
            .topic(&self.dlq_topic_name)
            .key(original_message.clone())
            .value(original_message.clone())
            .build();

        let mut producer_record = producer_record;

        if self.dlq_context_headers_enabled {
            self.populate_context_headers(&mut producer_record, context);
        }

        match self.kafka_producer.send(producer_record).await {
            Ok(metadata) => Ok(Some(metadata)),
            Err(e) => {
                error!(
                    "Could not produce message to dead letter queue. topic={}",
                    self.dlq_topic_name
                );
                self.error_handling_metrics.record_dead_letter_queue_produce_failed();
                Err(ConnectRuntimeError::connector_error(format!(
                    "Failed to produce to DLQ: {}",
                    e
                )))
            }
        }
    }

    /// Populate context headers
    ///
    /// # Arguments
    ///
    /// * `producer_record` - producer record
    /// * `context` - processing context
    fn populate_context_headers(
        &self,
        producer_record: &mut ProducerRecord<Vec<u8>, Vec<u8>>,
        context: &ProcessingContext<Vec<u8>>,
    ) {
        // Add original topic, partition, offset if available
        producer_record.add_header(ERROR_HEADER_ORIG_TOPIC, b"unknown".to_vec());
        producer_record.add_header(ERROR_HEADER_ORIG_PARTITION, b"0".to_vec());
        producer_record.add_header(ERROR_HEADER_ORIG_OFFSET, b"0".to_vec());

        // Add connector name and task ID
        producer_record.add_header(
            ERROR_HEADER_CONNECTOR_NAME,
            self.connector_task_id.as_bytes().to_vec(),
        );
        producer_record.add_header(ERROR_HEADER_TASK_ID, b"0".to_vec());

        // Add stage
        if let Some(stage) = context.stage() {
            producer_record.add_header(ERROR_HEADER_STAGE, stage.name().as_bytes().to_vec());
        }

        // Add executing class
        if let Some(type_id) = context.executing_class() {
            producer_record.add_header(
                ERROR_HEADER_EXECUTING_CLASS,
                format!("{:?}", type_id).as_bytes().to_vec(),
            );
        }

        // Add error information if available
        if let Some(err) = context.error() {
            producer_record.add_header(
                ERROR_HEADER_EXCEPTION,
                std::any::type_name_of_val(err).as_bytes().to_vec(),
            );
            producer_record.add_header(
                ERROR_HEADER_EXCEPTION_MESSAGE,
                err.to_string().as_bytes().to_vec(),
            );
            // Stack trace is not easily available in Rust
        }
    }
}

#[async_trait]
impl<P, A> ErrorReporter<Vec<u8>> for DeadLetterQueueReporter<P, A>
where
    P: Producer<Vec<u8>, Vec<u8>> + Send + Sync,
    A: AdminClient + Send + Sync,
{
    async fn report(&self, context: &ProcessingContext<Vec<u8>>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.report_internal(context).await?;
        Ok(())
    }
}
