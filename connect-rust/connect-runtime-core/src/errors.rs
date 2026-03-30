//! Errors module
//!
//! Provides error types for Connect runtime.

use std::error::Error;
use std::fmt;

/// Connect runtime error
#[derive(Debug)]
pub enum ConnectRuntimeError {
    /// Worker error
    WorkerError(String),

    /// Herder error
    HerderError(String),

    /// Storage error
    StorageError(String),

    /// Config error
    ConfigError(String),

    /// Metrics error
    MetricsError(String),

    /// Isolation error
    IsolationError(String),

    /// Connector error
    ConnectorError(String),

    /// Task error
    TaskError(String),

    /// Offset error
    OffsetError(String),

    /// Timeout error
    TimeoutError(String),

    /// I/O error
    IoError(String),

    /// Serialization error
    SerializationError(String),

    /// Deserialization error
    DeserializationError(String),

    /// Unknown error
    UnknownError(String),
}

impl fmt::Display for ConnectRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectRuntimeError::WorkerError(msg) => write!(f, "Worker error: {}", msg),
            ConnectRuntimeError::HerderError(msg) => write!(f, "Herder error: {}", msg),
            ConnectRuntimeError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            ConnectRuntimeError::ConfigError(msg) => write!(f, "Config error: {}", msg),
            ConnectRuntimeError::MetricsError(msg) => write!(f, "Metrics error: {}", msg),
            ConnectRuntimeError::IsolationError(msg) => write!(f, "Isolation error: {}", msg),
            ConnectRuntimeError::ConnectorError(msg) => write!(f, "Connector error: {}", msg),
            ConnectRuntimeError::TaskError(msg) => write!(f, "Task error: {}", msg),
            ConnectRuntimeError::OffsetError(msg) => write!(f, "Offset error: {}", msg),
            ConnectRuntimeError::TimeoutError(msg) => write!(f, "Timeout error: {}", msg),
            ConnectRuntimeError::IoError(msg) => write!(f, "I/O error: {}", msg),
            ConnectRuntimeError::SerializationError(msg) => {
                write!(f, "Serialization error: {}", msg)
            }
            ConnectRuntimeError::DeserializationError(msg) => {
                write!(f, "Deserialization error: {}", msg)
            }
            ConnectRuntimeError::UnknownError(msg) => write!(f, "Unknown error: {}", msg),
        }
    }
}

impl Error for ConnectRuntimeError {}

impl ConnectRuntimeError {
    /// Create a worker error
    pub fn worker_error(msg: String) -> Self {
        ConnectRuntimeError::WorkerError(msg)
    }

    /// Create a herder error
    pub fn herder_error(msg: String) -> Self {
        ConnectRuntimeError::HerderError(msg)
    }

    /// Create a storage error
    pub fn storage_error(msg: String) -> Self {
        ConnectRuntimeError::StorageError(msg)
    }

    /// Create a config error
    pub fn config_error(msg: String) -> Self {
        ConnectRuntimeError::ConfigError(msg)
    }

    /// Create a metrics error
    pub fn metrics_error(msg: String) -> Self {
        ConnectRuntimeError::MetricsError(msg)
    }

    /// Create an isolation error
    pub fn isolation_error(msg: String) -> Self {
        ConnectRuntimeError::IsolationError(msg)
    }

    /// Create a connector error
    pub fn connector_error(msg: String) -> Self {
        ConnectRuntimeError::ConnectorError(msg)
    }

    /// Create a task error
    pub fn task_error(msg: String) -> Self {
        ConnectRuntimeError::TaskError(msg)
    }

    /// Create an offset error
    pub fn offset_error(msg: String) -> Self {
        ConnectRuntimeError::OffsetError(msg)
    }

    /// Create a timeout error
    pub fn timeout_error(msg: String) -> Self {
        ConnectRuntimeError::TimeoutError(msg)
    }

    /// Create an I/O error
    pub fn io_error(msg: String) -> Self {
        ConnectRuntimeError::IoError(msg)
    }

    /// Create a serialization error
    pub fn serialization_error(msg: String) -> Self {
        ConnectRuntimeError::SerializationError(msg)
    }

    /// Create a deserialization error
    pub fn deserialization_error(msg: String) -> Self {
        ConnectRuntimeError::DeserializationError(msg)
    }

    /// Create an unknown error
    pub fn unknown_error(msg: String) -> Self {
        ConnectRuntimeError::UnknownError(msg)
    }
}

/// Result type for Connect runtime operations
pub type ConnectRuntimeResult<T> = Result<T, ConnectRuntimeError>;

/// Error code
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Success
    Success = 0,

    /// Unknown error
    Unknown = 1,

    /// Invalid config
    InvalidConfig = 2,

    /// Connector not found
    ConnectorNotFound = 3,

    /// Task not found
    TaskNotFound = 4,

    /// Connector already exists
    ConnectorAlreadyExists = 5,

    /// Connector not running
    ConnectorNotRunning = 6,

    /// Task not running
    TaskNotRunning = 7,

    /// Timeout
    Timeout = 8,

    /// I/O error
    IoError = 9,

    /// Serialization error
    SerializationError = 10,

    /// Deserialization error
    DeserializationError = 11,

    /// Offset error
    OffsetError = 12,

    /// Metrics error
    MetricsError = 13,

    /// Isolation error
    IsolationError = 14,
}

impl ErrorCode {
    /// Get error code as integer
    pub fn as_int(&self) -> i32 {
        *self as i32
    }

    /// Get error code from integer
    pub fn from_int(code: i32) -> Self {
        match code {
            0 => ErrorCode::Success,
            1 => ErrorCode::Unknown,
            2 => ErrorCode::InvalidConfig,
            3 => ErrorCode::ConnectorNotFound,
            4 => ErrorCode::TaskNotFound,
            5 => ErrorCode::ConnectorAlreadyExists,
            6 => ErrorCode::ConnectorNotRunning,
            7 => ErrorCode::TaskNotRunning,
            8 => ErrorCode::Timeout,
            9 => ErrorCode::IoError,
            10 => ErrorCode::SerializationError,
            11 => ErrorCode::DeserializationError,
            12 => ErrorCode::OffsetError,
            13 => ErrorCode::MetricsError,
            14 => ErrorCode::IsolationError,
            _ => ErrorCode::Unknown,
        }
    }
}
