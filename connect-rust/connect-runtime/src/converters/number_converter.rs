// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Number Converter
//!
//! Converter and HeaderConverter implementation that supports serializing to and
//! deserializing from number values.
//!
//! This implementation currently does nothing with the topic names or header keys.

use crate::converters::number_converter_config::NumberConverterConfig;
use connect_api::data::{ConnectSchema, Schema, SchemaAndValue};
use connect_api::error::{ConnectException, DataException};
use connect_api::storage::{Converter, ConverterType, HeaderConverter};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Converter and HeaderConverter implementation that supports serializing to and
/// deserializing from number values.
pub struct NumberConverter<T> {
    /// Configuration
    config: NumberConverterConfig,
    /// Type name for error messages
    type_name: String,
    /// Schema for deserialized values
    schema: Arc<ConnectSchema>,
    /// Phantom data for generic type
    _phantom: std::marker::PhantomData<T>,
}

impl<T> NumberConverter<T> {
    /// Create a new NumberConverter.
    ///
    /// # Arguments
    ///
    /// * `type_name` - the displayable name of the type
    /// * `schema` - the optional schema to be used for all deserialized forms
    pub fn new(type_name: String, schema: Arc<ConnectSchema>) -> Self {
        Self {
            config: NumberConverterConfig::default(),
            type_name,
            schema,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a new NumberConverter with configuration.
    ///
    /// # Arguments
    ///
    /// * `type_name` - the displayable name of the type
    /// * `schema` - the optional schema to be used for all deserialized forms
    /// * `config` - converter configuration
    pub fn with_config(
        type_name: String,
        schema: Arc<ConnectSchema>,
        config: NumberConverterConfig,
    ) -> Self {
        Self {
            config,
            type_name,
            schema,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get type name.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Get schema.
    pub fn schema(&self) -> Arc<ConnectSchema> {
        Arc::clone(&self.schema)
    }
}

impl<T: Clone + Send + Sync + 'static> Default for NumberConverter<T> {
    fn default() -> Self {
        Self {
            config: NumberConverterConfig::default(),
            type_name: "number".to_string(),
            schema: Arc::new(ConnectSchema::new(connect_api::data::Type::Int64)),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Converter for NumberConverter<T> {
    fn configure(&mut self, configs: HashMap<String, String>, is_key: bool) {
        self.config = NumberConverterConfig::new_with_type(configs, is_key);
    }

    fn from_connect_data(
        &self,
        _topic: &str,
        _schema: Option<&dyn Schema>,
        value: &dyn Any,
    ) -> Result<Vec<u8>, ConnectException> {
        // Try to cast value to the expected type
        if value.is::<()>() {
            // Null value - return empty bytes
            return Ok(vec![]);
        }

        // Serialize based on type
        self.serialize_value(value)
    }

    fn to_connect_data(
        &self,
        _topic: &str,
        value: &[u8],
    ) -> Result<SchemaAndValue, ConnectException> {
        let deserialized = self.deserialize_value(value)?;
        Ok(SchemaAndValue::new(
            Some(Arc::clone(&self.schema)),
            deserialized,
        ))
    }
}

impl<T: Clone + Send + Sync + 'static> HeaderConverter for NumberConverter<T> {
    fn to_connect_header(
        &self,
        topic: &str,
        _header_key: &str,
        value: &[u8],
    ) -> Result<SchemaAndValue, ConnectException> {
        self.to_connect_data(topic, value)
    }

    fn from_connect_header(
        &self,
        topic: &str,
        _header_key: &str,
        schema: Option<&dyn Schema>,
        value: &dyn Any,
    ) -> Result<Option<Vec<u8>>, ConnectException> {
        let bytes = self.from_connect_data(topic, schema, value)?;
        Ok(Some(bytes))
    }
}

impl NumberConverter<i32> {
    fn serialize_value(&self, value: &dyn Any) -> Result<Vec<u8>, ConnectException> {
        if let Some(v) = value.downcast_ref::<i32>() {
            Ok(v.to_le_bytes().to_vec())
        } else {
            Err(DataException::new(format!(
                "Failed to serialize to {} (was unknown type)",
                self.type_name
            ))
            .into())
        }
    }

    fn deserialize_value(
        &self,
        value: &[u8],
    ) -> Result<Option<Box<dyn Any + Send + Sync>>, ConnectException> {
        if value.is_empty() {
            return Ok(None);
        }

        if value.len() < 4 {
            return Err(ConnectException::new(format!(
                "Failed to deserialize {}: insufficient bytes",
                self.type_name
            )));
        }

        let bytes = [value[0], value[1], value[2], value[3]];
        let v = i32::from_le_bytes(bytes);
        Ok(Some(Box::new(v) as Box<dyn Any + Send + Sync>))
    }
}

impl NumberConverter<i64> {
    fn serialize_value(&self, value: &dyn Any) -> Result<Vec<u8>, ConnectException> {
        if let Some(v) = value.downcast_ref::<i64>() {
            Ok(v.to_le_bytes().to_vec())
        } else {
            Err(DataException::new(format!(
                "Failed to serialize to {} (was unknown type)",
                self.type_name
            ))
            .into())
        }
    }

    fn deserialize_value(
        &self,
        value: &[u8],
    ) -> Result<Option<Box<dyn Any + Send + Sync>>, ConnectException> {
        if value.is_empty() {
            return Ok(None);
        }

        if value.len() < 8 {
            return Err(ConnectException::new(format!(
                "Failed to deserialize {}: insufficient bytes",
                self.type_name
            )));
        }

        let bytes = [
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
        ];
        let v = i64::from_le_bytes(bytes);
        Ok(Some(Box::new(v) as Box<dyn Any + Send + Sync>))
    }
}

impl NumberConverter<i16> {
    fn serialize_value(&self, value: &dyn Any) -> Result<Vec<u8>, ConnectException> {
        if let Some(v) = value.downcast_ref::<i16>() {
            Ok(v.to_le_bytes().to_vec())
        } else {
            Err(DataException::new(format!(
                "Failed to serialize to {} (was unknown type)",
                self.type_name
            ))
            .into())
        }
    }

    fn deserialize_value(
        &self,
        value: &[u8],
    ) -> Result<Option<Box<dyn Any + Send + Sync>>, ConnectException> {
        if value.is_empty() {
            return Ok(None);
        }

        if value.len() < 2 {
            return Err(ConnectException::new(format!(
                "Failed to deserialize {}: insufficient bytes",
                self.type_name
            )));
        }

        let bytes = [value[0], value[1]];
        let v = i16::from_le_bytes(bytes);
        Ok(Some(Box::new(v) as Box<dyn Any + Send + Sync>))
    }
}

impl NumberConverter<f64> {
    fn serialize_value(&self, value: &dyn Any) -> Result<Vec<u8>, ConnectException> {
        if let Some(v) = value.downcast_ref::<f64>() {
            Ok(v.to_le_bytes().to_vec())
        } else {
            Err(DataException::new(format!(
                "Failed to serialize to {} (was unknown type)",
                self.type_name
            ))
            .into())
        }
    }

    fn deserialize_value(
        &self,
        value: &[u8],
    ) -> Result<Option<Box<dyn Any + Send + Sync>>, ConnectException> {
        if value.is_empty() {
            return Ok(None);
        }

        if value.len() < 8 {
            return Err(ConnectException::new(format!(
                "Failed to deserialize {}: insufficient bytes",
                self.type_name
            )));
        }

        let bytes = [
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
        ];
        let v = f64::from_le_bytes(bytes);
        Ok(Some(Box::new(v) as Box<dyn Any + Send + Sync>))
    }
}

impl NumberConverter<f32> {
    fn serialize_value(&self, value: &dyn Any) -> Result<Vec<u8>, ConnectException> {
        if let Some(v) = value.downcast_ref::<f32>() {
            Ok(v.to_le_bytes().to_vec())
        } else {
            Err(DataException::new(format!(
                "Failed to serialize to {} (was unknown type)",
                self.type_name
            ))
            .into())
        }
    }

    fn deserialize_value(
        &self,
        value: &[u8],
    ) -> Result<Option<Box<dyn Any + Send + Sync>>, ConnectException> {
        if value.is_empty() {
            return Ok(None);
        }

        if value.len() < 4 {
            return Err(ConnectException::new(format!(
                "Failed to deserialize {}: insufficient bytes",
                self.type_name
            )));
        }

        let bytes = [value[0], value[1], value[2], value[3]];
        let v = f32::from_le_bytes(bytes);
        Ok(Some(Box::new(v) as Box<dyn Any + Send + Sync>))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connect_api::data::Type;

    #[test]
    fn test_number_converter_new() {
        let schema = Arc::new(ConnectSchema::new(Type::Int64));
        let converter = NumberConverter::<i64>::new("long".to_string(), schema);
        assert_eq!(converter.type_name(), "long");
    }

    #[test]
    fn test_i32_serialize() {
        let schema = Arc::new(ConnectSchema::new(Type::Int32));
        let converter = NumberConverter::<i32>::new("integer".to_string(), schema);
        let value: i32 = 42;
        let result = converter.serialize_value(&value);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42i32.to_le_bytes().to_vec());
    }

    #[test]
    fn test_i32_deserialize() {
        let schema = Arc::new(ConnectSchema::new(Type::Int32));
        let converter = NumberConverter::<i32>::new("integer".to_string(), schema);
        let bytes = 42i32.to_le_bytes().to_vec();
        let result = converter.deserialize_value(&bytes);
        assert!(result.is_ok());
        if let Some(v) = result.unwrap() {
            if let Some(i) = v.downcast_ref::<i32>() {
                assert_eq!(*i, 42);
            } else {
                panic!("Failed to downcast to i32");
            }
        } else {
            panic!("Deserialized value is None");
        }
    }

    #[test]
    fn test_i64_serialize() {
        let schema = Arc::new(ConnectSchema::new(Type::Int64));
        let converter = NumberConverter::<i64>::new("long".to_string(), schema);
        let value: i64 = 42;
        let result = converter.serialize_value(&value);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42i64.to_le_bytes().to_vec());
    }

    #[test]
    fn test_f64_serialize() {
        let schema = Arc::new(ConnectSchema::new(Type::Float64));
        let converter = NumberConverter::<f64>::new("double".to_string(), schema);
        let value: f64 = 42.5;
        let result = converter.serialize_value(&value);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42.5f64.to_le_bytes().to_vec());
    }

    #[test]
    fn test_f32_serialize() {
        let schema = Arc::new(ConnectSchema::new(Type::Float32));
        let converter = NumberConverter::<f32>::new("float".to_string(), schema);
        let value: f32 = 42.5;
        let result = converter.serialize_value(&value);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42.5f32.to_le_bytes().to_vec());
    }
}
