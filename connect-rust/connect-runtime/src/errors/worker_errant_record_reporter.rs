//! Worker errant record reporter module
//!
//! Provides a WorkerErrantRecordReporter for reporting errant records to error handlers.

use crate::errors::error_handling_metrics::ErrorHandlingMetrics;
use crate::errors::processing_context::ProcessingContext;
use crate::errors::retry_with_tolerance_operator::RetryWithToleranceOperator;
use crate::errors::stage::Stage;
use connect_runtime_core::errors::ConnectRuntimeError;
use async_trait::async_trait;
use dashmap::DashMap;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use tracing::{error, warn};

/// Worker errant record reporter
pub struct WorkerErrantRecordReporter {
    /// Retry with tolerance operator
    retry_with_tolerance_operator: Arc<RetryWithToleranceOperator<Vec<u8>>>,

    /// Error handling metrics
    error_handling_metrics: Arc<ErrorHandlingMetrics>,

    /// Futures for error reports
    futures: Arc<DashMap<String, Vec<tokio::task::JoinHandle<()>>>>,

    /// Task put exception
    task_put_exception: Arc<Mutex<Option<Box<dyn std::error::Error + Send + Sync>>>>,

    /// Whether to stop reporting
    stopping: Arc<AtomicBool>,
}

impl WorkerErrantRecordReporter {
    /// Create a new worker errant record reporter
    ///
    /// # Arguments
    ///
    /// * `retry_with_tolerance_operator` - retry with tolerance operator
    /// * `error_handling_metrics` - error handling metrics
    ///
    /// # Returns
    ///
    /// New worker errant record reporter
    pub fn new(
        retry_with_tolerance_operator: Arc<RetryWithToleranceOperator<Vec<u8>>>,
        error_handling_metrics: Arc<ErrorHandlingMetrics>,
    ) -> Self {
        Self {
            retry_with_tolerance_operator,
            error_handling_metrics,
            futures: Arc::new(DashMap::new()),
            task_put_exception: Arc::new(Mutex::new(None)),
            stopping: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Report an errant record
    ///
    /// # Arguments
    ///
    /// * `record` - sink record
    /// * `error` - error
    ///
    /// # Returns
    ///
    /// Result containing () or an error
    pub async fn report(
        &self,
        record: Vec<u8>,
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> Result<(), ConnectRuntimeError> {
        let context = ProcessingContext::new(record);

        let future = self
            .retry_with_tolerance_operator
            .execute_failed(
                // We need a mutable context, but we can't get one from the method signature
                // This is a limitation of the current design
                &mut ProcessingContext::new(record),
                Stage::TaskPut,
                TypeId::of::<Self>(),
                error,
            )
            .await?;

        let mut task_exception = self.task_put_exception.lock().await;
        if task_exception.is_none() {
            *task_exception = Some(error);
        }

        Ok(())
    }

    /// Await completion of all error reports for a given set of topic partitions
    ///
    /// # Arguments
    ///
    /// * `topic_partitions` - topic partitions to await reporter completion for
    pub async fn await_futures(&self, topic_partitions: &[String]) {
        for partition in topic_partitions {
            if let Some(mut futures) = self.futures.remove(partition) {
                for future in futures.drain(..) {
                    if let Err(e) = future.await {
                        error!("Encountered an error while awaiting an errant record future's completion: {}", e);
                    }
                }
            }
        }
    }

    /// Cancel all active error reports for a given set of topic partitions
    ///
    /// # Arguments
    ///
    /// * `topic_partitions` - topic partitions to cancel reporting for
    pub async fn cancel_futures(&self, topic_partitions: &[String]) {
        for partition in topic_partitions {
            if let Some(mut futures) = self.futures.remove(partition) {
                for future in futures.drain(..) {
                    future.abort();
                }
            }
        }
    }

    /// Maybe throw async error
    ///
    /// # Returns
    ///
    /// Result containing () or an error if tolerance is exceeded
    pub async fn maybe_throw_async_error(&self) -> Result<(), ConnectRuntimeError> {
        let task_exception = self.task_put_exception.lock().await;
        if task_exception.is_some()
            && !self.retry_with_tolerance_operator.within_tolerance_limits()
        {
            if let Some(ref e) = *task_exception {
                return Err(ConnectRuntimeError::connector_error(format!(
                    "Tolerance exceeded in error handler: {}",
                    e
                )));
            }
        }
        Ok(())
    }

    /// Stop reporting
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::Relaxed);
    }

    /// Check if stopping
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Relaxed)
    }
}

/// Errant record reporter trait
#[async_trait]
pub trait ErrantRecordReporter: Send + Sync {
    /// Report an errant record
    ///
    /// # Arguments
    ///
    /// * `record` - sink record
    /// * `error` - error
    ///
    /// # Returns
    ///
    /// Result containing () or an error
    async fn report(
        &self,
        record: Vec<u8>,
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> Result<(), ConnectRuntimeError>;

    /// Await completion of all error reports for a given set of topic partitions
    ///
    /// # Arguments
    ///
    /// * `topic_partitions` - topic partitions to await reporter completion for
    async fn await_futures(&self, topic_partitions: &[String]);

    /// Cancel all active error reports for a given set of topic partitions
    ///
    /// # Arguments
    ///
    /// * `topic_partitions` - topic partitions to cancel reporting for
    async fn cancel_futures(&self, topic_partitions: &[String]);

    /// Maybe throw async error
    ///
    /// # Returns
    ///
    /// Result containing () or an error if tolerance is exceeded
    async fn maybe_throw_async_error(&self) -> Result<(), ConnectRuntimeError>;
}

#[async_trait]
impl ErrantRecordReporter for WorkerErrantRecordReporter {
    async fn report(
        &self,
        record: Vec<u8>,
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> Result<(), ConnectRuntimeError> {
        self.report(record, error).await
    }

    async fn await_futures(&self, topic_partitions: &[String]) {
        self.await_futures(topic_partitions).await
    }

    async fn cancel_futures(&self, topic_partitions: &[String]) {
        self.cancel_futures(topic_partitions).await
    }

    async fn maybe_throw_async_error(&self) -> Result<(), ConnectRuntimeError> {
        self.maybe_throw_async_error().await
    }
}
