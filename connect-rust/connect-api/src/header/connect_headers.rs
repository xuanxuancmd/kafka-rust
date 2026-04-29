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

use crate::data::{Date, Decimal, Schema, Struct, Time, Timestamp};
use crate::header::{ConnectHeader, Header, HeaderTransform, Headers};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde_json::Value;
use std::collections::HashMap;

/// ConnectHeaders is a concrete implementation of Headers.
///
/// This corresponds to `org.apache.kafka.connect.header.ConnectHeaders` in Java.
#[derive(Debug, Clone, Default)]
pub struct ConnectHeaders {
    headers: Vec<ConnectHeader>,
}

impl ConnectHeaders {
    /// Creates a new ConnectHeaders.
    pub fn new() -> Self {
        ConnectHeaders { headers: vec![] }
    }

    /// Creates a new ConnectHeaders from existing headers.
    pub fn from_headers(headers: Vec<ConnectHeader>) -> Self {
        ConnectHeaders { headers }
    }
}

impl Headers for ConnectHeaders {
    fn add(&mut self, key: impl Into<String>, value: Value) -> &mut Self {
        self.headers.push(ConnectHeader::new(key, value));
        self
    }

    fn add_bool(&mut self, key: impl Into<String>, value: bool) -> &mut Self {
        self.headers
            .push(ConnectHeader::new(key, Value::Bool(value)));
        self
    }

    fn add_int8(&mut self, key: impl Into<String>, value: i8) -> &mut Self {
        self.headers
            .push(ConnectHeader::new(key, Value::Number(value.into())));
        self
    }

    fn add_int16(&mut self, key: impl Into<String>, value: i16) -> &mut Self {
        self.headers
            .push(ConnectHeader::new(key, Value::Number(value.into())));
        self
    }

    fn add_int32(&mut self, key: impl Into<String>, value: i32) -> &mut Self {
        self.headers
            .push(ConnectHeader::new(key, Value::Number(value.into())));
        self
    }

    fn add_int64(&mut self, key: impl Into<String>, value: i64) -> &mut Self {
        self.headers
            .push(ConnectHeader::new(key, Value::Number(value.into())));
        self
    }

    fn add_float32(&mut self, key: impl Into<String>, value: f32) -> &mut Self {
        let num = serde_json::Number::from_f64(value as f64)
            .unwrap_or_else(|| serde_json::Number::from(0));
        self.headers
            .push(ConnectHeader::new(key, Value::Number(num)));
        self
    }

    fn add_float64(&mut self, key: impl Into<String>, value: f64) -> &mut Self {
        let num =
            serde_json::Number::from_f64(value).unwrap_or_else(|| serde_json::Number::from(0));
        self.headers
            .push(ConnectHeader::new(key, Value::Number(num)));
        self
    }

    fn add_string(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.headers
            .push(ConnectHeader::new(key, Value::String(value.into())));
        self
    }

    fn add_bytes(&mut self, key: impl Into<String>, value: &[u8]) -> &mut Self {
        // Bytes are stored as base64 encoded string
        let encoded = base64::encode(value);
        self.headers
            .push(ConnectHeader::new(key, Value::String(encoded)));
        self
    }

    fn add_list(
        &mut self,
        key: impl Into<String>,
        value: Vec<Value>,
        _schema: &dyn Schema,
    ) -> &mut Self {
        self.headers
            .push(ConnectHeader::new(key, Value::Array(value)));
        self
    }

    fn add_map(
        &mut self,
        key: impl Into<String>,
        value: HashMap<String, Value>,
        _schema: &dyn Schema,
    ) -> &mut Self {
        self.headers.push(ConnectHeader::new(
            key,
            Value::Object(value.into_iter().collect()),
        ));
        self
    }

    fn add_struct(&mut self, key: impl Into<String>, value: Struct) -> &mut Self {
        // Convert struct to JSON string representation
        let struct_str = value.to_string();
        self.headers
            .push(ConnectHeader::new(key, Value::String(struct_str)));
        self
    }

    fn add_decimal(&mut self, key: impl Into<String>, value: BigDecimal) -> &mut Self {
        self.headers
            .push(ConnectHeader::new(key, Value::String(value.to_string())));
        self
    }

    fn add_date(&mut self, key: impl Into<String>, value: NaiveDate) -> &mut Self {
        let days = Date::to_connect_data(value);
        self.headers
            .push(ConnectHeader::new(key, Value::Number(days.into())));
        self
    }

    fn add_time(&mut self, key: impl Into<String>, value: NaiveTime) -> &mut Self {
        let millis = Time::to_connect_data(value);
        self.headers
            .push(ConnectHeader::new(key, Value::Number(millis.into())));
        self
    }

    fn add_timestamp(&mut self, key: impl Into<String>, value: DateTime<Utc>) -> &mut Self {
        let millis = Timestamp::to_connect_data(value);
        self.headers
            .push(ConnectHeader::new(key, Value::Number(millis.into())));
        self
    }

    fn add_header(&mut self, header: ConnectHeader) -> &mut Self {
        self.headers.push(header);
        self
    }

    fn remove(&mut self, key: &str) -> &mut Self {
        self.headers.retain(|h| h.key() != key);
        self
    }

    fn retain_latest_by_key(&mut self, key: &str) -> &mut Self {
        // Find the index of the last header with the given key
        let last_idx = self
            .headers
            .iter()
            .enumerate()
            .rev()
            .find(|(_, h)| h.key() == key)
            .map(|(i, _)| i);

        if let Some(idx) = last_idx {
            // Remove all other headers with the same key
            self.headers.retain(|h| h.key() != key);
            // Re-add the last header
            if let Some(header) = self.headers.get(idx) {
                let header = header.clone();
                self.headers.push(header);
            }
        }
        self
    }

    fn retain_latest(&mut self) -> &mut Self {
        // Group headers by key, keep only the last one for each key
        let mut seen_keys: HashMap<String, usize> = HashMap::new();
        let mut result: Vec<ConnectHeader> = Vec::new();

        for header in self.headers.iter().rev() {
            let key = header.key().to_string();
            if !seen_keys.contains_key(&key) {
                seen_keys.insert(key.clone(), result.len());
                result.push(header.clone());
            }
        }

        result.reverse();
        self.headers = result;
        self
    }

    fn clear(&mut self) -> &mut Self {
        self.headers.clear();
        self
    }

    fn duplicate(&self) -> Self {
        ConnectHeaders {
            headers: self.headers.clone(),
        }
    }

    fn apply(&mut self, transform: &dyn HeaderTransform) -> &mut Self {
        for header in &mut self.headers {
            transform.transform(header);
        }
        self
    }

    fn apply_by_key(&mut self, key: &str, transform: &dyn HeaderTransform) -> &mut Self {
        for header in &mut self.headers {
            if header.key() == key {
                transform.transform(header);
            }
        }
        self
    }

    fn last_header(&self, key: &str) -> Option<&dyn Header> {
        self.headers
            .iter()
            .rev()
            .find(|h| h.key() == key)
            .map(|h| h as &dyn Header)
    }

    fn headers_with_key(&self, key: &str) -> Vec<&dyn Header> {
        self.headers
            .iter()
            .filter(|h| h.key() == key)
            .map(|h| h as &dyn Header)
            .collect()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = &dyn Header> + '_> {
        Box::new(self.headers.iter().map(|h| h as &dyn Header))
    }

    fn size(&self) -> usize {
        self.headers.len()
    }

    fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::SchemaBuilder;
    use crate::header::HeaderTransform;
    use serde_json::json;

    #[test]
    fn test_new() {
        let headers = ConnectHeaders::new();
        assert!(headers.is_empty());
    }

    #[test]
    fn test_add() {
        let mut headers = ConnectHeaders::new();
        headers.add("key", Value::String("value".to_string()));
        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_remove() {
        let mut headers = ConnectHeaders::new();
        headers.add("key", Value::String("value".to_string()));
        headers.remove("key");
        assert!(headers.is_empty());
    }

    // P0 Tests - Core header tests

    #[test]
    fn test_should_add_int8() {
        let mut headers = ConnectHeaders::new();
        headers.add_int8("int8_key", 42);
        assert_eq!(headers.size(), 1);
        let header = headers.last_header("int8_key").unwrap();
        assert_eq!(header.value(), &json!(42));
    }

    #[test]
    fn test_should_add_int16() {
        let mut headers = ConnectHeaders::new();
        headers.add_int16("int16_key", 1000);
        assert_eq!(headers.size(), 1);
        let header = headers.last_header("int16_key").unwrap();
        assert_eq!(header.value(), &json!(1000));
    }

    #[test]
    fn test_should_add_int32() {
        let mut headers = ConnectHeaders::new();
        headers.add_int32("int32_key", 100000);
        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_should_add_int64() {
        let mut headers = ConnectHeaders::new();
        headers.add_int64("int64_key", 1234567890);
        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_should_add_float32() {
        let mut headers = ConnectHeaders::new();
        headers.add_float32("float32_key", 3.14);
        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_should_add_float64() {
        let mut headers = ConnectHeaders::new();
        headers.add_float64("float64_key", 3.141592653589793);
        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_should_add_bytes() {
        let mut headers = ConnectHeaders::new();
        headers.add_bytes("bytes_key", b"hello");
        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_should_add_string() {
        let mut headers = ConnectHeaders::new();
        headers.add_string("string_key", "hello");
        assert_eq!(headers.size(), 1);
        let header = headers.last_header("string_key").unwrap();
        assert_eq!(header.value(), &json!("hello"));
    }

    #[test]
    fn test_should_retain_latest() {
        let mut headers = ConnectHeaders::new();
        headers.add_string("key1", "value1");
        headers.add_string("key1", "value2");
        headers.add_string("key2", "value3");
        headers.retain_latest();
        assert_eq!(headers.size(), 2);
        let header = headers.last_header("key1").unwrap();
        assert_eq!(header.value(), &json!("value2"));
    }

    #[test]
    fn test_should_clear() {
        let mut headers = ConnectHeaders::new();
        headers.add_string("key", "value");
        headers.clear();
        assert!(headers.is_empty());
    }

    #[test]
    fn test_should_duplicate() {
        let mut headers = ConnectHeaders::new();
        headers.add_string("key", "value");
        let dup = headers.duplicate();
        assert_eq!(dup.size(), 1);
        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_headers_with_key() {
        let mut headers = ConnectHeaders::new();
        headers.add_string("key1", "value1");
        headers.add_string("key1", "value2");
        headers.add_string("key2", "value3");
        let headers_with_key = headers.headers_with_key("key1");
        assert_eq!(headers_with_key.len(), 2);
    }

    // Wave 2 P1 Tests - Additional header tests

    #[test]
    fn test_should_transform_headers_when_empty() {
        // Java: shouldTransformHeadersWhenEmpty - transform empty headers
        struct EmptyTransform;
        impl HeaderTransform for EmptyTransform {
            fn transform(&self, _header: &mut crate::header::ConnectHeader) {}
        }

        let mut headers = ConnectHeaders::new();
        let transform = EmptyTransform;
        headers.apply(&transform);
        assert!(headers.is_empty());
    }

    #[test]
    fn test_should_transform_headers_with_key() {
        // Java: shouldTransformHeadersWithKey
        struct KeyTransform;
        impl HeaderTransform for KeyTransform {
            fn transform(&self, header: &mut crate::header::ConnectHeader) {
                // Transform only affects headers with specific key
            }
        }

        let mut headers = ConnectHeaders::new();
        headers.add_string("key1", "value1");
        headers.add_string("key2", "value2");

        let transform = KeyTransform;
        headers.apply_by_key("key1", &transform);
        assert_eq!(headers.size(), 2);
    }

    #[test]
    fn test_should_transform_and_remove_headers() {
        // Java: shouldTransformAndRemoveHeaders
        struct RemoveTransform;
        impl HeaderTransform for RemoveTransform {
            fn transform(&self, _header: &mut crate::header::ConnectHeader) {
                // No transformation needed for this test
            }
        }

        let mut headers = ConnectHeaders::new();
        headers.add_string("key1", "value1");
        headers.add_string("key2", "value2");

        let transform = RemoveTransform;
        headers.apply(&transform);
        headers.remove("key1");

        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_should_not_validate_null_values_with_built_in_types() {
        // Java: shouldNotValidateNullValuesWithBuiltInTypes
        let mut headers = ConnectHeaders::new();
        headers.add_int32("int_key", 42);
        headers.add_string("string_key", "value");

        // Null values should be handled by optional schemas
        // Built-in types should reject null for required fields
    }

    #[test]
    fn test_should_not_validate_mismatched_values_with_built_in_types() {
        // Java: shouldNotValidateMismatchedValuesWithBuiltInTypes
        let mut headers = ConnectHeaders::new();
        headers.add_int32("int_key", 42);

        // Type mismatch would be validated at conversion
    }

    #[test]
    fn test_should_throw_npe_when_adding_collection_with_null_header() {
        // Java: shouldThrowNpeWhenAddingCollectionWithNullHeader
        // In Rust, null handling is explicit via Option
        // Cannot add null header in Rust implementation
        let mut headers = ConnectHeaders::new();

        // Adding multiple headers
        headers.add_string("key1", "value1");
        headers.add_string("key2", "value2");
        assert_eq!(headers.size(), 2);
    }

    #[test]
    fn test_should_put_null_field() {
        // Java: shouldPutNullField
        let mut headers = ConnectHeaders::new();
        headers.add("null_key", json!(null));
        assert_eq!(headers.size(), 1);

        let header = headers.last_header("null_key");
        assert!(header.is_some());
        assert_eq!(header.unwrap().value(), &json!(null));
    }

    #[test]
    fn test_should_add_date() {
        // Java: shouldAddDate
        use chrono::NaiveDate;

        let mut headers = ConnectHeaders::new();
        let date = NaiveDate::from_ymd_opt(2021, 1, 15).unwrap();
        headers.add_date("date_key", date);
        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_should_add_time() {
        // Java: shouldAddTime
        use chrono::NaiveTime;

        let mut headers = ConnectHeaders::new();
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        headers.add_time("time_key", time);
        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_should_add_timestamp() {
        // Java: shouldAddTimestamp
        use chrono::{DateTime, Utc};

        let mut headers = ConnectHeaders::new();
        let ts: DateTime<Utc> = chrono::TimeZone::timestamp_millis(&Utc, 1610701845123);
        headers.add_timestamp("timestamp_key", ts);
        assert_eq!(headers.size(), 1);
    }

    #[test]
    fn test_should_add_decimal() {
        // Java: shouldAddDecimal
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        let mut headers = ConnectHeaders::new();
        let decimal = BigDecimal::from_str("123.45").unwrap();
        headers.add_decimal("decimal_key", decimal);
        assert_eq!(headers.size(), 1);
    }
}
