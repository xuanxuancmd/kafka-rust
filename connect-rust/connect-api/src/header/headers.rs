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

use crate::data::Schema;
use crate::header::Header;
use serde_json::Value;

/// Headers trait for managing multiple headers in a Connect record.
///
/// This corresponds to `org.apache.kafka.connect.header.Headers` in Java.
pub trait Headers {
    /// Adds a header with the given key and value.
    fn add(&mut self, key: impl Into<String>, value: Value) -> &mut Self;

    /// Adds a header with a boolean value.
    fn add_bool(&mut self, key: impl Into<String>, value: bool) -> &mut Self;

    /// Adds a header with an Int8 value.
    fn add_int8(&mut self, key: impl Into<String>, value: i8) -> &mut Self;

    /// Adds a header with an Int16 value.
    fn add_int16(&mut self, key: impl Into<String>, value: i16) -> &mut Self;

    /// Adds a header with an Int32 value.
    fn add_int32(&mut self, key: impl Into<String>, value: i32) -> &mut Self;

    /// Adds a header with an Int64 value.
    fn add_int64(&mut self, key: impl Into<String>, value: i64) -> &mut Self;

    /// Adds a header with a Float32 value.
    fn add_float32(&mut self, key: impl Into<String>, value: f32) -> &mut Self;

    /// Adds a header with a Float64 value.
    fn add_float64(&mut self, key: impl Into<String>, value: f64) -> &mut Self;

    /// Adds a header with a string value.
    fn add_string(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self;

    /// Adds a header with bytes value.
    fn add_bytes(&mut self, key: impl Into<String>, value: &[u8]) -> &mut Self;

    /// Adds a header with a List value and schema.
    fn add_list(
        &mut self,
        key: impl Into<String>,
        value: Vec<Value>,
        schema: &dyn Schema,
    ) -> &mut Self;

    /// Adds a header with a Map value and schema.
    fn add_map(
        &mut self,
        key: impl Into<String>,
        value: std::collections::HashMap<String, Value>,
        schema: &dyn Schema,
    ) -> &mut Self;

    /// Adds a header with a Struct value.
    fn add_struct(&mut self, key: impl Into<String>, value: crate::data::Struct) -> &mut Self;

    /// Adds a header with a Decimal value.
    fn add_decimal(&mut self, key: impl Into<String>, value: bigdecimal::BigDecimal) -> &mut Self;

    /// Adds a header with a Date value.
    fn add_date(&mut self, key: impl Into<String>, value: chrono::NaiveDate) -> &mut Self;

    /// Adds a header with a Time value.
    fn add_time(&mut self, key: impl Into<String>, value: chrono::NaiveTime) -> &mut Self;

    /// Adds a header with a Timestamp value.
    fn add_timestamp(
        &mut self,
        key: impl Into<String>,
        value: chrono::DateTime<chrono::Utc>,
    ) -> &mut Self;

    /// Adds a header object directly.
    fn add_header(&mut self, header: crate::header::ConnectHeader) -> &mut Self;

    /// Removes all headers with the given key.
    fn remove(&mut self, key: &str) -> &mut Self;

    /// Retains only the latest header for a specific key.
    fn retain_latest_by_key(&mut self, key: &str) -> &mut Self;

    /// Retains only the latest header for each key.
    fn retain_latest(&mut self) -> &mut Self;

    /// Clears all headers.
    fn clear(&mut self) -> &mut Self;

    /// Creates a duplicate of this headers.
    fn duplicate(&self) -> Self;

    /// Applies a transformation to all headers.
    fn apply(&mut self, transform: &dyn HeaderTransform) -> &mut Self;

    /// Applies a transformation to headers with a specific key.
    fn apply_by_key(&mut self, key: &str, transform: &dyn HeaderTransform) -> &mut Self;

    /// Returns the last header with the given key.
    fn last_header(&self, key: &str) -> Option<&dyn Header>;

    /// Returns all headers with the given key.
    fn headers_with_key(&self, key: &str) -> Vec<&dyn Header>;

    /// Returns an iterator over all headers.
    fn iter(&self) -> Box<dyn Iterator<Item = &dyn Header> + '_>;

    /// Returns the number of headers.
    fn size(&self) -> usize;

    /// Returns whether there are no headers.
    fn is_empty(&self) -> bool;
}

/// HeaderTransform trait for transforming headers.
///
/// This corresponds to `org.apache.kafka.connect.header.HeaderTransform` in Java.
pub trait HeaderTransform {
    /// Transforms the header.
    fn transform(&self, header: &mut crate::header::ConnectHeader);
}
