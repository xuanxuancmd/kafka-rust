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
    /// Not found exception for missing connector or task errors
    NotFound(NotFoundException),
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
            ConnectError::NotFound(err) => write!(f, "{}", err),
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
            ConnectError::NotFound(_) => false,
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

/// Schema builder exception for schema construction errors
#[derive(Debug, Clone)]
pub struct SchemaBuilderException {
    message: String,
}

impl SchemaBuilderException {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SchemaBuilderException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Schema builder exception: {}", self.message)
    }
}

impl std::error::Error for SchemaBuilderException {}

/// Schema projector exception for schema projection errors
#[derive(Debug, Clone)]
pub struct SchemaProjectorException {
    message: String,
}

impl SchemaProjectorException {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SchemaProjectorException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Schema projector exception: {}", self.message)
    }
}

impl std::error::Error for SchemaProjectorException {}

/// Retriable exception for operations that can be retried
#[derive(Debug, Clone)]
pub struct RetriableException {
    message: String,
}

impl RetriableException {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for RetriableException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Retriable exception: {}", self.message)
    }
}

impl std::error::Error for RetriableException {}

/// Already exists exception for duplicate resource errors
#[derive(Debug, Clone)]
pub struct AlreadyExistsException {
    message: String,
}

impl AlreadyExistsException {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for AlreadyExistsException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Already exists exception: {}", self.message)
    }
}

impl std::error::Error for AlreadyExistsException {}

/// Illegal worker state exception for invalid state errors
#[derive(Debug, Clone)]
pub struct IllegalWorkerStateException {
    message: String,
}

impl IllegalWorkerStateException {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for IllegalWorkerStateException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Illegal worker state exception: {}", self.message)
    }
}

impl std::error::Error for IllegalWorkerStateException {}

/// Not found exception for missing connector or task errors
#[derive(Debug, Clone)]
pub struct NotFoundException {
    message: String,
}

impl NotFoundException {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for NotFoundException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Not found exception: {}", self.message)
    }
}

impl std::error::Error for NotFoundException {}

/// From trait for converting NotFoundException to ConnectError
impl From<NotFoundException> for ConnectError {
    fn from(err: NotFoundException) -> Self {
        ConnectError::NotFound(err)
    }
}
