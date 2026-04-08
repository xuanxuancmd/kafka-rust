//! String Converter Module
//!
//! This module provides string serialization/deserialization for Kafka Connect.

use crate::error::{ConnectError, ConnectException};
use std::any::Any;
use std::sync::Arc;

/// String converter configuration
#[derive(Debug, Clone)]
pub struct StringConverterConfig {
    /// Encoding configuration
    encoding: Option<String>,
}

impl Default for StringConverterConfig {
    fn default() -> Self {
        Self { encoding: None }
    }
}

/// String converter
pub struct StringConverter {
    config: StringConverterConfig,
}

impl StringConverter {
    /// Create a new string converter
    pub fn new() -> Self {
        Self {
            config: StringConverterConfig::default(),
        }
    }

    /// Create a new string converter with configuration
    pub fn with_config(config: StringConverterConfig) -> Self {
        Self { config }
    }

    /// Configure the converter
    pub fn configure(&mut self, configs: std::collections::HashMap<String, String>) {
        if let Some(encoding) = configs.get("encoding") {
            self.config.encoding = Some(encoding.clone());
        }
    }

    /// Convert bytes to SchemaAndValue
    pub fn to_connect_data(
        &self,
        _topic: &str,
        value: &[u8],
    ) -> Result<crate::data::SchemaAndValue, ConnectError> {
        if value.is_empty() {
            return Ok(crate::data::SchemaAndValue::new(None, None));
        }

        match std::str::from_utf8(value) {
            Ok(s) => Ok(crate::data::SchemaAndValue::new(
                Some(Arc::new(crate::data::ConnectSchema::new(
                    crate::data::Type::String,
                ))),
                Some(Box::new(s.to_string())),
            )),
            Err(_) => {
                // Return lossy string for invalid UTF-8
                Ok(crate::data::SchemaAndValue::new(
                    Some(Arc::new(crate::data::ConnectSchema::new(
                        crate::data::Type::String,
                    ))),
                    Some(Box::new(String::from_utf8_lossy(value).to_string())),
                ))
            }
        }
    }

    /// Convert SchemaAndValue to bytes
    pub fn from_connect_data(
        &self,
        _topic: &str,
        _schema: Option<&dyn crate::data::Schema>,
        value: &dyn Any,
    ) -> Result<Vec<u8>, ConnectException> {
        if let Some(s) = value.downcast_ref::<String>() {
            Ok(s.as_bytes().to_vec())
        } else {
            Err(ConnectException::new("Value is not a String".to_string()))
        }
    }
}

impl Default for StringConverter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_converter_new() {
        let converter = StringConverter::new();
        assert_eq!(converter.config.encoding, None);
    }

    #[test]
    fn test_to_connect_data_empty() {
        let converter = StringConverter::new();
        let result = converter.to_connect_data("test-topic", &[]);
        assert!(result.is_ok());
        let sav = result.unwrap();
        assert!(sav.value().is_none());
    }

    #[test]
    fn test_to_connect_data_valid_utf8() {
        let converter = StringConverter::new();
        let input = "hello world".as_bytes();
        let result = converter.to_connect_data("test-topic", input);
        assert!(result.is_ok());
    }

    #[test]
    fn test_from_connect_data() {
        let converter = StringConverter::new();
        let result = converter.from_connect_data("test-topic", None, &"hello".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello".as_bytes().to_vec());
    }
}
