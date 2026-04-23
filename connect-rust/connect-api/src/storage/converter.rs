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

//! Converter trait for Kafka Connect storage converters.
//!
//! This corresponds to `org.apache.kafka.connect.storage.Converter` in Java.

use crate::data::{Schema, SchemaAndValue};
use crate::errors::ConnectError;
use common_trait::header::Headers;
use serde_json::Value;
use std::collections::HashMap;

/// Converter trait for converting between Kafka Connect's runtime data format and byte[].
///
/// This corresponds to `org.apache.kafka.connect.storage.Converter` in Java Kafka Connect.
/// The Converter interface provides support for translating between Kafka Connect's runtime
/// data format and byte[]. Internally, this likely includes an intermediate step to the
/// format used by the serialization layer (e.g. JsonNode, GenericRecord, Message).
///
/// Kafka Connect may discover implementations of this interface using a service provider
/// mechanism. Implementations should be properly registered for discovery.
pub trait Converter {
    /// Configure this class.
    ///
    /// # Arguments
    /// * `configs` - configuration in key/value pairs
    /// * `is_key` - whether this converter is for a key or a value
    ///
    /// Corresponds to `configure(Map<String, ?> configs, boolean isKey)` in Java.
    fn configure(&mut self, configs: HashMap<String, Value>, is_key: bool);

    /// Convert a Kafka Connect data object to a native object for serialization.
    ///
    /// # Arguments
    /// * `topic` - the topic associated with the data
    /// * `schema` - the schema for the value; may be None
    /// * `value` - the value to convert; may be None (tombstone record)
    ///
    /// # Returns
    /// The serialized bytes, or None if the value is null (tombstone record).
    ///
    /// Corresponds to `fromConnectData(String topic, Schema schema, Object value)` in Java.
    /// In Java, this method returns byte[] which can be null for tombstone records.
    fn from_connect_data(
        &self,
        topic: &str,
        schema: Option<&dyn Schema>,
        value: Option<&Value>,
    ) -> Result<Option<Vec<u8>>, ConnectError>;

    /// Convert a Kafka Connect data object to a native object for serialization,
    /// potentially using the supplied topic and headers in the record as necessary.
    ///
    /// Connect uses this method directly, and for backward compatibility reasons this method
    /// by default will call the [`from_connect_data`](Converter::from_connect_data) method.
    /// Override this method to make use of the supplied headers.
    ///
    /// # Arguments
    /// * `topic` - the topic associated with the data
    /// * `headers` - the headers associated with the data; any changes done to the headers
    ///               are applied to the message sent to the broker
    /// * `schema` - the schema for the value; may be None
    /// * `value` - the value to convert; may be None (tombstone record)
    ///
    /// # Returns
    /// The serialized bytes, or None if the value is null (tombstone record).
    ///
    /// Corresponds to `fromConnectData(String topic, Headers headers, Schema schema, Object value)` in Java.
    fn from_connect_data_with_headers<H>(
        &self,
        topic: &str,
        headers: &mut H,
        schema: Option<&dyn Schema>,
        value: Option<&Value>,
    ) -> Result<Option<Vec<u8>>, ConnectError>
    where
        H: Headers,
    {
        // Default implementation: delegate to from_connect_data without headers
        self.from_connect_data(topic, schema, value)
    }

    /// Convert a native object to a Kafka Connect data object for deserialization.
    ///
    /// # Arguments
    /// * `topic` - the topic associated with the data
    /// * `value` - the value to convert; may be None (tombstone record)
    ///
    /// # Returns
    /// An object containing the Schema and the converted value.
    /// For null input, returns SchemaAndValue representing null value.
    ///
    /// Corresponds to `toConnectData(String topic, byte[] value)` in Java.
    /// In Java, this method returns SchemaAndValue, and for null input returns
    /// SchemaAndValue.NULL (schema is null, value is null).
    fn to_connect_data(
        &self,
        topic: &str,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError>;

    /// Convert a native object to a Kafka Connect data object for deserialization,
    /// potentially using the supplied topic and headers in the record as necessary.
    ///
    /// Connect uses this method directly, and for backward compatibility reasons this method
    /// by default will call the [`to_connect_data`](Converter::to_connect_data) method.
    /// Override this method to make use of the supplied headers.
    ///
    /// # Arguments
    /// * `topic` - the topic associated with the data
    /// * `headers` - the headers associated with the data
    /// * `value` - the value to convert; may be None (tombstone record)
    ///
    /// # Returns
    /// An object containing the Schema and the converted value.
    ///
    /// Corresponds to `toConnectData(String topic, Headers headers, byte[] value)` in Java.
    fn to_connect_data_with_headers<H>(
        &self,
        topic: &str,
        headers: &H,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError>
    where
        H: Headers,
    {
        // Default implementation: delegate to to_connect_data without headers
        self.to_connect_data(topic, value)
    }

    /// Configuration specification for this converter.
    ///
    /// # Returns
    /// The configuration specification; may not be null
    ///
    /// Corresponds to `config()` in Java (default method returning new ConfigDef()).
    fn config(&self) -> &'static dyn common_trait::config::ConfigDef;

    /// Close this converter and release any resources.
    ///
    /// Corresponds to `close()` in Java (default method from Closeable interface).
    fn close(&mut self) {}
}
