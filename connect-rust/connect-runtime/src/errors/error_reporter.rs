//! Error reporter module
//!
//! Provides the ErrorReporter trait for reporting errors.

use async_trait::async_trait;

/// Report an error using the information contained in the ProcessingContext.
#[async_trait]
pub trait ErrorReporter<T>: Send + Sync {
    /// Report an error and return the producer future.
    ///
    /// # Arguments
    ///
    /// * `context` - the processing context (cannot be null)
    ///
    /// # Returns
    ///
    /// Future result from the producer sending a record to Kafka
    async fn report(&self, context: &crate::errors::processing_context::ProcessingContext<T>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Default no-op error reporter
pub struct NoOpErrorReporter;

#[async_trait]
impl<T> ErrorReporter<T> for NoOpErrorReporter {
    async fn report(&self, _context: &crate::errors::processing_context::ProcessingContext<T>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}
