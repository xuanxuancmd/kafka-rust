//! Processing context module
//!
//! Provides the ProcessingContext struct for metadata related to currently evaluating operations.

use crate::errors::stage::Stage;
use std::any::TypeId;

/// Contains all the metadata related to the currently evaluating operation, and associated with a particular
/// sink or source record from the consumer or task, respectively. This struct is not thread safe, and so once an
/// instance is passed to a new thread, it should no longer be accessed by the previous thread.
pub struct ProcessingContext<T> {
    /// The original record before processing, as received from either Kafka or a Source Task
    original: T,

    /// The stage in the connector pipeline which is currently executing
    position: Option<Stage>,

    /// The class which is going to execute the current operation
    klass: Option<TypeId>,

    /// The number of attempts made to execute the current operation
    attempt: i32,

    /// The error (if any) which was encountered while processing the current stage
    error: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl<T> ProcessingContext<T> {
    /// Construct a context associated with the processing of a particular record
    ///
    /// # Arguments
    ///
    /// * `original` - The original record before processing, as received from either Kafka or a Source Task
    pub fn new(original: T) -> Self {
        Self {
            original,
            position: None,
            klass: None,
            attempt: 0,
            error: None,
        }
    }

    /// Get the original record before processing, as received from either Kafka or a Source Task
    pub fn original(&self) -> &T {
        &self.original
    }

    /// Get the original record as mutable
    pub fn original_mut(&mut self) -> &mut T {
        &mut self.original
    }

    /// Set the stage in the connector pipeline which is currently executing
    ///
    /// # Arguments
    ///
    /// * `position` - the stage
    pub fn position(&mut self, position: Stage) {
        self.position = Some(position);
    }

    /// Get the stage in the connector pipeline which is currently executing
    pub fn stage(&self) -> Option<Stage> {
        self.position
    }

    /// Get the class which is going to execute the current operation
    pub fn executing_class(&self) -> Option<TypeId> {
        self.klass
    }

    /// Set the class which is currently executing
    ///
    /// # Arguments
    ///
    /// * `klass` - set the class which is currently executing
    pub fn executing_class(&mut self, klass: TypeId) {
        self.klass = Some(klass);
    }

    /// A helper method to set both the stage and the class
    ///
    /// # Arguments
    ///
    /// * `stage` - the stage
    /// * `klass` - the class which will execute the operation in this stage
    pub fn current_context(&mut self, stage: Stage, klass: TypeId) {
        self.position(stage);
        self.executing_class(klass);
    }

    /// Set the number of attempts made to execute the current operation
    ///
    /// # Arguments
    ///
    /// * `attempt` - the number of attempts made to execute the current operation
    pub fn attempt(&mut self, attempt: i32) {
        self.attempt = attempt;
    }

    /// Get the number of attempts made to execute the current operation
    pub fn get_attempt(&self) -> i32 {
        self.attempt
    }

    /// Get the error (if any) which was encountered while processing the current stage
    pub fn error(&self) -> Option<&(dyn std::error::Error + Send + Sync)> {
        self.error.as_ref().map(|e| e.as_ref())
    }

    /// Set the error (if any) which was encountered while processing the current stage
    ///
    /// # Arguments
    ///
    /// * `error` - the error
    pub fn set_error(&mut self, error: Box<dyn std::error::Error + Send + Sync>) {
        self.error = Some(error);
    }

    /// Return true if the last operation encountered an error; false otherwise
    pub fn failed(&self) -> bool {
        self.error.is_some()
    }
}
