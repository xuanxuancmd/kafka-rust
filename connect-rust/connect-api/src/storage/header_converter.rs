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

use crate::data::{Schema, SchemaAndValue};
use crate::errors::ConnectError;
use common_trait::config::ConfigDef;
use serde_json::Value;
use std::collections::HashMap;

/// HeaderConverter trait for header converters.
///
/// This corresponds to `org.apache.kafka.connect.storage.HeaderConverter` in Java.
///
/// The HeaderConverter interface provides support for translating between Kafka Connect's
/// runtime data format and byte[]. This is similar to the Converter interface, but
/// specifically for Headers.
pub trait HeaderConverter {
    /// Configures this converter.
    ///
    /// This corresponds to `configure(Map<String, ?> configs, boolean isKey)` in Java.
    ///
    /// # Arguments
    /// * `configs` - the configuration settings
    /// * `is_key` - whether this is for a key or value header
    fn configure(&mut self, configs: HashMap<String, String>, is_key: bool);

    /// Convert the header's value into its byte array representation.
    ///
    /// This corresponds to `fromConnectHeader(String topic, String headerKey, Schema schema, Object value)` in Java.
    ///
    /// # Arguments
    /// * `topic` - the name of the topic for the record containing the header
    /// * `header_key` - the header's key; may not be null
    /// * `schema` - the schema for the header's value; may be null
    /// * `value` - the header's value to convert; may be null
    ///
    /// # Returns
    /// The byte array form of the Header's value; may be null if the value is null
    fn from_connect_header(
        &self,
        topic: &str,
        header_key: &str,
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<Vec<u8>>, ConnectError>;

    /// Convert the header name and byte array value into a SchemaAndValue object.
    ///
    /// This corresponds to `toConnectHeader(String topic, String headerKey, byte[] value)` in Java.
    ///
    /// # Arguments
    /// * `topic` - the name of the topic for the record containing the header
    /// * `header_key` - the header's key; may not be null
    /// * `value` - the header's raw value; may be null
    ///
    /// # Returns
    /// The SchemaAndValue; may not be null
    fn to_connect_header(
        &self,
        topic: &str,
        header_key: &str,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError>;

    /// Configuration specification for this set of header converters.
    ///
    /// This corresponds to `config()` in Java.
    ///
    /// # Returns
    /// The configuration specification; may not be null
    fn config(&self) -> &'static dyn ConfigDef;

    /// Closes this converter.
    ///
    /// This corresponds to `close()` in Java.
    /// Implementations should release any resources held by this converter.
    fn close(&mut self);
}
