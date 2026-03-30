//! Error types and traits for Kafka Connect

use std::fmt;

/// Core error type for Kafka Connect operations
#[derive(Debug, Clone)]
pub enum ConnectError {
    /// Configuration error
    ConfigError(String),
    /// Data conversion error
    ConversionError(String),
    /// Task execution error
    TaskError(String),
    /// Connector error
    ConnectorError(String),
    /// Offset storage error
    OffsetStorageError(String),
    /// Serialization error
    SerializationError(String),
    /// Deserialization error
    DeserializationError(String),
    /// IO error
    IoError(String),
    /// Generic error
    Other(String),
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectError::ConfigError(msg) => write!(f, "Config error: {}", msg),
            ConnectError::ConversionError(msg) => write!(f, "Conversion error: {}", msg),
            ConnectError::TaskError(msg) => write!(f, "Task error: {}", msg),
            ConnectError::ConnectorError(msg) => write!(f, "Connector error: {}", msg),
            ConnectError::OffsetStorageError(msg) => write!(f, "Offset storage error: {}", msg),
            ConnectError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            ConnectError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            ConnectError::IoError(msg) => write!(f, "IO error: {}", msg),
            ConnectError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for ConnectError {}

/// Trait for errors that can be retried
pub trait RetriableError {
    /// Returns true if the error can be retried
    fn retriable(&self) -> bool;
}

impl RetriableError for ConnectError {
    fn retriable(&self) -> bool {
        match self {
            ConnectError::ConfigError(_) => false,
            ConnectError::ConversionError(_) => false,
            ConnectError::TaskError(_) => true,
            ConnectError::ConnectorError(_) => true,
            ConnectError::OffsetStorageError(_) => true,
            ConnectError::SerializationError(_) => false,
            ConnectError::DeserializationError(_) => false,
            ConnectError::IoError(_) => true,
            ConnectError::Other(_) => false,
        }
    }
}

/// Result type alias for Connect operations
pub type ConnectResult<T> = Result<T, ConnectError>;

/// Data exception for schema and data validation errors
#[derive(Debug, Clone)]
pub struct DataException {
    message: String,
}

impl DataException {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for DataException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Data exception: {}", self.message)
    }
}

impl std::error::Error for DataException {}

/// Connect exception for general connector errors
#[derive(Debug, Clone)]
pub struct ConnectException {
    message: String,
}

impl ConnectException {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ConnectException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connect exception: {}", self.message)
    }
}

impl std::error::Error for ConnectException {}
