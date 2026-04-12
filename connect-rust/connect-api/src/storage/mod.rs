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

//! Storage module for Kafka Connect API.
//! This module contains storage-related types and traits.

use crate::config::Configurable;
use crate::connector_impl::Closeable;
use crate::data::{Headers, Schema, SchemaAndValue};
use crate::error::ConnectException;
use std::collections::HashMap;

/// Converter type enumeration for key/value converters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConverterType {
    /// Key converter
    Key,
    /// Value converter
    Value,
    /// Header converter
    Header,
}

impl ConverterType {
    /// Convert to string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            ConverterType::Key => "key",
            ConverterType::Value => "value",
            ConverterType::Header => "header",
        }
    }
}

/// The Converter interface provides support for translating between Kafka Connect's runtime data format
/// and byte arrays. Internally, this likely includes an intermediate step to the format used by
/// the serialization layer (e.g. JsonNode, GenericRecord, Message).
pub trait Converter: Configurable + Closeable + Send + Sync {
    /// Configure this class.
    ///
    /// # Arguments
    ///
    /// * `configs` - configs in key/value pairs
    /// * `isKey` - whether this converter is for a key or a value
    fn configure(&mut self, configs: HashMap<String, String>, is_key: bool);

    /// Convert a Kafka Connect data object to a native object for serialization.
    ///
    /// # Arguments
    ///
    /// * `topic` - the topic associated with the data
    /// * `schema` - the schema for the value
    /// * `value` - the value to convert
    ///
    /// # Returns
    ///
    /// the serialized value
    fn from_connect_data(
        &self,
        topic: &str,
        schema: Option<&dyn Schema>,
        value: &dyn std::any::Any,
    ) -> Result<Vec<u8>, ConnectException>;

    /// Convert a Kafka Connect data object to a native object for serialization,
    /// potentially using the supplied topic and headers in the record as necessary.
    ///
    /// # Arguments
    ///
    /// * `topic` - the topic associated with the data
    /// * `headers` - the headers associated with the data
    /// * `schema` - the schema for the value
    /// * `value` - the value to convert
    ///
    /// # Returns
    ///
    /// the serialized value
    fn from_connect_data_with_headers(
        &self,
        topic: &str,
        _headers: &dyn Headers,
        schema: Option<&dyn Schema>,
        value: &dyn std::any::Any,
    ) -> Result<Vec<u8>, ConnectException> {
        // Default implementation ignores headers
        self.from_connect_data(topic, schema, value)
    }

    /// Convert a native object to a Kafka Connect data object for deserialization.
    ///
    /// # Arguments
    ///
    /// * `topic` - the topic associated with the data
    /// * `value` - the value to convert
    ///
    /// # Returns
    ///
    /// an object containing the Schema and the converted value
    fn to_connect_data(
        &self,
        topic: &str,
        value: &[u8],
    ) -> Result<SchemaAndValue, ConnectException>;

    /// Convert a native object to a Kafka Connect data object for deserialization,
    /// potentially using the supplied topic and headers in the record as necessary.
    ///
    /// # Arguments
    ///
    /// * `topic` - the topic associated with the data
    /// * `headers` - the headers associated with the data
    /// * `value` - the value to convert
    ///
    /// # Returns
    ///
    /// an object containing the Schema and the converted value
    fn to_connect_data_with_headers(
        &self,
        topic: &str,
        _headers: &dyn Headers,
        value: &[u8],
    ) -> Result<SchemaAndValue, ConnectException> {
        // Default implementation ignores headers
        self.to_connect_data(topic, value)
    }
}

/// The HeaderConverter interface provides support for translating between Kafka Connect's runtime data format
/// and byte arrays for Headers.
pub trait HeaderConverter: Configurable + Closeable + Send + Sync {
    /// Convert the header name and byte array value into a Header object.
    ///
    /// # Arguments
    ///
    /// * `topic` - the name of the topic for the record containing the header
    /// * `header_key` - the header's key
    /// * `value` - the header's raw value
    ///
    /// # Returns
    ///
    /// the SchemaAndValue
    fn to_connect_header(
        &self,
        topic: &str,
        header_key: &str,
        value: &[u8],
    ) -> Result<SchemaAndValue, ConnectException>;

    /// Convert the Header's value into its byte array representation.
    ///
    /// # Arguments
    ///
    /// * `topic` - the name of the topic for the record containing the header
    /// * `header_key` - the header's key
    /// * `schema` - the schema for the header's value
    /// * `value` - the header's value to convert
    ///
    /// # Returns
    ///
    /// the byte array form of the Header's value; may be null if the value is null
    fn from_connect_header(
        &self,
        topic: &str,
        header_key: &str,
        schema: Option<&dyn Schema>,
        value: &dyn std::any::Any,
    ) -> Result<Option<Vec<u8>>, ConnectException>;
}

/// Offset storage reader interface for reading stored offsets.
pub trait OffsetStorageReader: Send + Sync {
    /// Get the offset for the specified partition.
    ///
    /// # Arguments
    ///
    /// * `partition` - the partition to get offset for
    ///
    /// # Returns
    ///
    /// The offset data, or None if not found
    fn offset(
        &self,
        partition: &HashMap<String, String>,
    ) -> Option<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>;

    /// Get a set of offsets for the specified partition identifiers.
    ///
    /// # Arguments
    ///
    /// * `partitions` - the list of partitions to get offsets for
    ///
    /// # Returns
    ///
    /// A map from partition to offset data
    fn offsets(
        &self,
        partitions: &[HashMap<String, String>],
    ) -> HashMap<HashMap<String, String>, HashMap<String, Box<dyn std::any::Any + Send + Sync>>>;
}

/// Offset storage writer interface for storing offsets.
pub trait OffsetStorageWriter: Send + Sync {
    /// Save the offset.
    ///
    /// # Arguments
    ///
    /// * `partition` - the partition
    /// * `offset` - the offset data
    fn save_offset(
        &mut self,
        partition: HashMap<String, String>,
        offset: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    );

    /// Flush all pending offsets.
    fn flush(&mut self) -> Result<(), ConnectException>;
}

/// Converter configuration for key/value converters.
#[derive(Debug, Clone)]
pub struct ConverterConfig {
    /// Configuration properties
    configs: HashMap<String, String>,
    /// Whether this is a key converter
    is_key: bool,
}

impl ConverterConfig {
    /// Create a new ConverterConfig.
    pub fn new(configs: HashMap<String, String>, is_key: bool) -> Self {
        Self { configs, is_key }
    }

    /// Get the configuration value.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.configs.get(key)
    }

    /// Check if this is a key converter.
    pub fn is_key(&self) -> bool {
        self.is_key
    }

    /// Check if this is a value converter.
    pub fn is_value(&self) -> bool {
        !self.is_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_converter_type_as_str() {
        assert_eq!(ConverterType::Key.as_str(), "key");
        assert_eq!(ConverterType::Value.as_str(), "value");
        assert_eq!(ConverterType::Header.as_str(), "header");
    }

    #[test]
    fn test_converter_config() {
        let mut configs = HashMap::new();
        configs.insert("schema.enabled".to_string(), "true".to_string());
        let config = ConverterConfig::new(configs, true);

        assert!(config.is_key());
        assert!(!config.is_value());
        assert_eq!(config.get("schema.enabled"), Some(&"true".to_string()));
    }
}
