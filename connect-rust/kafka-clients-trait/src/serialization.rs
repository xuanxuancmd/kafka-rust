//! Kafka Serialization Trait
//!
//! Defines the core interface for Kafka serialization and deserialization.

use std::error::Error;
use std::fmt;

/// Serialization error
#[derive(Debug)]
pub enum SerializationError {
    InvalidData(String),
    UnsupportedType(String),
    IoError(String),
}

impl fmt::Display for SerializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SerializationError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            SerializationError::UnsupportedType(msg) => write!(f, "Unsupported type: {}", msg),
            SerializationError::IoError(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

impl Error for SerializationError {}

/// Deserialization error
#[derive(Debug)]
pub enum DeserializationError {
    InvalidData(String),
    UnsupportedType(String),
    IoError(String),
}

impl fmt::Display for DeserializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeserializationError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            DeserializationError::UnsupportedType(msg) => write!(f, "Unsupported type: {}", msg),
            DeserializationError::IoError(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

impl Error for DeserializationError {}

/// Core Serializer trait
pub trait Serializer<T>: Send + Sync {
    /// Serializes a value into bytes
    fn serialize(&self, value: &T) -> Result<Vec<u8>, SerializationError>;

    /// Closes the serializer and releases resources
    fn close(&self);
}

/// Core Deserializer trait
pub trait Deserializer<T>: Send + Sync {
    /// Deserializes bytes into a value
    fn deserialize(&self, data: &[u8]) -> Result<T, DeserializationError>;

    /// Closes the deserializer and releases resources
    fn close(&self);
}

/// Default byte array serializer (no-op)
pub struct ByteArraySerializer;

impl Serializer<Vec<u8>> for ByteArraySerializer {
    fn serialize(&self, value: &Vec<u8>) -> Result<Vec<u8>, SerializationError> {
        Ok(value.clone())
    }

    fn close(&self) {}
}

/// Default byte array deserializer (no-op)
pub struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, data: &[u8]) -> Result<Vec<u8>, DeserializationError> {
        Ok(data.to_vec())
    }

    fn close(&self) {}
}

/// String serializer
pub struct StringSerializer;

impl Serializer<String> for StringSerializer {
    fn serialize(&self, value: &String) -> Result<Vec<u8>, SerializationError> {
        Ok(value.as_bytes().to_vec())
    }

    fn close(&self) {}
}

/// String deserializer
pub struct StringDeserializer;

impl Deserializer<String> for StringDeserializer {
    fn deserialize(&self, data: &[u8]) -> Result<String, DeserializationError> {
        String::from_utf8(data.to_vec())
            .map_err(|e| DeserializationError::InvalidData(e.to_string()))
    }

    fn close(&self) {}
}
