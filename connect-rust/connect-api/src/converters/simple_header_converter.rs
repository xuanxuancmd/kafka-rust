//! Simple Header Converter Module
//!
//! This module provides simple header conversion for Kafka Connect.

use crate::data::{ConnectSchema, Schema, Type};
use crate::error::ConnectError;
use std::any::Any;
use std::sync::Arc;

/// Simple header converter
pub struct SimpleHeaderConverter;

impl SimpleHeaderConverter {
    /// Create a new simple header converter
    pub fn new() -> Self {
        Self
    }

    /// Configure the converter
    pub fn configure(&mut self, _configs: std::collections::HashMap<String, String>) {
        // No special configuration needed
    }

    /// Convert header value to bytes
    pub fn from_connect_header(
        &self,
        _topic: &str,
        _header_key: &str,
        _schema: Option<&dyn Schema>,
        value: &dyn Any,
    ) -> Result<Option<Vec<u8>>, ConnectError> {
        if let Some(s) = value.downcast_ref::<String>() {
            Ok(Some(s.as_bytes().to_vec()))
        } else if let Some(bytes) = value.downcast_ref::<Vec<u8>>() {
            Ok(Some(bytes.clone()))
        } else {
            Err(ConnectError::Other(
                "Header value must be String or Vec<u8>".to_string(),
            ))
        }
    }

    /// Convert bytes to header value
    pub fn to_connect_header(
        &self,
        _topic: &str,
        _header_key: &str,
        value: &[u8],
    ) -> Result<(Option<Arc<dyn Schema>>, Box<dyn Any + Send + Sync>), ConnectError> {
        if value.is_empty() {
            return Ok((None, Box::new(())));
        }

        let schema = Arc::new(ConnectSchema::new(Type::String)) as Arc<dyn Schema>;
        let string_value = String::from_utf8_lossy(value).to_string();
        Ok((Some(schema), Box::new(string_value)))
    }
}

impl Default for SimpleHeaderConverter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_header_converter_new() {
        let converter = SimpleHeaderConverter::new();
        // Converter is stateless, no verification needed
    }

    #[test]
    fn test_from_connect_header_string() {
        let converter = SimpleHeaderConverter::new();
        let value = "test value".to_string();
        let result = converter.from_connect_header("test-topic", "key", None, &value);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("test value".as_bytes().to_vec()));
    }

    #[test]
    fn test_to_connect_header() {
        let converter = SimpleHeaderConverter::new();
        let value = "test value".as_bytes();
        let result = converter.to_connect_header("test-topic", "key", value);
        assert!(result.is_ok());
        let (schema, boxed_value) = result.unwrap();
        assert!(schema.is_some());
        let value_str = boxed_value.downcast_ref::<String>().unwrap();
        assert_eq!(value_str, "test value");
    }
}
