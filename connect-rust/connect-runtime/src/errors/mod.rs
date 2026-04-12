//! Errors module
//!
//! Provides error handling and reporting functionality for Connect runtime.

pub mod dead_letter_queue_reporter;
pub mod error_handling_metrics;
pub mod error_reporter;
pub mod log_reporter;
pub mod operation;
pub mod processing_context;
pub mod retry_with_tolerance_operator;
pub mod stage;
pub mod tolerance_type;
pub mod worker_errant_record_reporter;

// Re-exports
pub use dead_letter_queue_reporter::DeadLetterQueueReporter;
pub use error_handling_metrics::ErrorHandlingMetrics;
pub use error_reporter::{ErrorReporter, NoOpErrorReporter};
pub use log_reporter::{LogReporter, SinkLogReporter, SourceLogReporter};
pub use operation::{Operation, FunctionOperation};
pub use processing_context::ProcessingContext;
pub use retry_with_tolerance_operator::RetryWithToleranceOperator;
pub use stage::Stage;
pub use tolerance_type::ToleranceType;
pub use worker_errant_record_reporter::{WorkerErrantRecordReporter, ErrantRecordReporter};

// Import errors from connect-runtime-core
use connect_runtime_core::errors::ConnectRuntimeError;
