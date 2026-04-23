// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use connect_api::data::{Date, Decimal, Schema, SchemaBuilder, Time, Timestamp};
use connect_api::errors::DataException;
use connect_api::header::{ConnectHeaders, Header, Headers};
use serde_json::json;
use std::str::FromStr;

// Helper function to populate headers with test data
fn populate_headers(headers: &ConnectHeaders) {
    headers.add_bool("k1", true);
    headers.add_int32("k1", 0);
    headers.add_string("other key", Some("other value"));
    headers.add_string("k1", None);
    headers.add_string("k1", Some("third"));
}

// =============================================================================
// Test 1: shouldNotAllowNullKey
// =============================================================================
#[test]
fn should_not_allow_null_key() {
    let headers = ConnectHeaders::new();
    // In Rust, null keys are not possible due to type system
    // Test that empty string key works
    headers.add_string("", Some("value"));
    assert!(headers.last_with_name("").is_some());
}

// =============================================================================
// Test 2: shouldBeEquals
// =============================================================================
#[test]
fn should_be_equals() {
    let headers = ConnectHeaders::new();
    let other_headers = ConnectHeaders::new();

    assert_eq!(headers, other_headers);
    assert_eq!(headers.hash_code(), other_headers.hash_code());

    populate_headers(&headers);
    assert_ne!(headers, other_headers);

    populate_headers(&other_headers);
    assert_eq!(headers, other_headers);

    headers.add_string("wow", Some("some value"));
    assert_ne!(headers, other_headers);
}

// =============================================================================
// Test 3: shouldHaveToString
// =============================================================================
#[test]
fn should_have_to_string() {
    let headers = ConnectHeaders::new();
    assert!(!headers.to_string().is_empty());

    populate_headers(&headers);
    assert!(!headers.to_string().is_empty());
}

// =============================================================================
// Test 4: shouldRetainLatestWhenEmpty
// =============================================================================
#[test]
fn should_retain_latest_when_empty() {
    let headers = ConnectHeaders::new();
    headers.retain_latest("other key");
    headers.retain_latest("k1");
    headers.retain_latest_all();
    assert!(headers.is_empty());
}

// =============================================================================
// Test 5: shouldAddMultipleHeadersWithSameKeyAndRetainLatest
// =============================================================================
#[test]
fn should_add_multiple_headers_with_same_key_and_retain_latest() {
    let headers = ConnectHeaders::new();
    populate_headers(&headers);

    let header = headers.last_with_name("k1");
    assert!(header.is_some());
    let header = header.unwrap();
    assert_eq!("k1", header.key());
    assert_eq!(Some("third"), header.value().as_string());

    // Verify all headers with key exist
    let all_headers: Vec<_> = headers.all_with_name("k1").collect();
    assert_eq!(4, all_headers.len());

    headers.retain_latest("k1");
    let all_headers: Vec<_> = headers.all_with_name("k1").collect();
    assert_eq!(1, all_headers.len());
    assert_eq!(Some("third"), all_headers[0].value().as_string());
}

// =============================================================================
// Test 6: shouldAddHeadersWithPrimitiveValues
// =============================================================================
#[test]
fn should_add_headers_with_primitive_values() {
    let headers = ConnectHeaders::new();
    let key = "k1";

    headers.add_bool(key, true);
    headers.add_int8(key, 0);
    headers.add_int16(key, 0);
    headers.add_int32(key, 0);
    headers.add_int64(key, 0);
    headers.add_float32(key, 1.0);
    headers.add_float64(key, 1.0);
    headers.add_string(key, None);
    headers.add_string(key, Some("third"));

    assert!(headers.size() >= 9);
}

// =============================================================================
// Test 7: shouldAddHeadersWithNullObjectValuesWithOptionalSchema
// =============================================================================
#[test]
fn should_add_headers_with_null_object_values_with_optional_schema() {
    let headers = ConnectHeaders::new();

    let bool_schema = SchemaBuilder::bool().build();
    headers.add("k1", &bool_schema, json!(true));

    let string_schema = SchemaBuilder::string().build();
    headers.add("k2", &string_schema, json!("hello"));

    let optional_string_schema = SchemaBuilder::string().optional().build();
    headers.add("k3", &optional_string_schema, json!(null));

    assert_eq!(3, headers.size());
}

// =============================================================================
// Test 8: shouldNotAddHeadersWithNullObjectValuesWithNonOptionalSchema
// =============================================================================
#[test]
fn should_not_add_headers_with_null_object_values_with_non_optional_schema() {
    let headers = ConnectHeaders::new();

    let bool_schema = SchemaBuilder::bool().build();
    let result = headers.add("k1", &bool_schema, json!(null));
    assert!(result.is_err());

    let string_schema = SchemaBuilder::string().build();
    let result = headers.add("k2", &string_schema, json!(null));
    assert!(result.is_err());
}

// =============================================================================
// Test 9: shouldNotAddHeadersWithObjectValuesAndMismatchedSchema
// =============================================================================
#[test]
fn should_not_add_headers_with_object_values_and_mismatched_schema() {
    let headers = ConnectHeaders::new();

    let bool_schema = SchemaBuilder::bool().build();
    let result = headers.add("k1", &bool_schema, json!("wrong"));
    assert!(result.is_err());

    let optional_string_schema = SchemaBuilder::string().optional().build();
    let result = headers.add("k2", &optional_string_schema, json!(0));
    assert!(result.is_err());
}

// =============================================================================
// Test 10: shouldRemoveAllHeadersWithSameKeyWhenEmpty
// =============================================================================
#[test]
fn should_remove_all_headers_with_same_key_when_empty() {
    let headers = ConnectHeaders::new();
    headers.remove("k1");
    assert!(headers.last_with_name("k1").is_none());
}

// =============================================================================
// Test 11: shouldRemoveAllHeadersWithSameKey
// =============================================================================
#[test]
fn should_remove_all_headers_with_same_key() {
    let headers = ConnectHeaders::new();
    populate_headers(&headers);

    assert!(headers.last_with_name("k1").is_some());
    assert!(headers.last_with_name("other key").is_some());

    headers.remove("k1");
    assert!(headers.last_with_name("k1").is_none());
    assert!(headers.last_with_name("other key").is_some());
}

// =============================================================================
// Test 12: shouldRemoveAllHeaders
// =============================================================================
#[test]
fn should_remove_all_headers() {
    let headers = ConnectHeaders::new();
    populate_headers(&headers);

    headers.clear();
    assert!(headers.is_empty());
    assert_eq!(0, headers.size());
}

// =============================================================================
// Test 13: shouldTransformHeadersWhenEmpty
// =============================================================================
#[test]
fn should_transform_headers_when_empty() {
    let headers = ConnectHeaders::new();
    headers.apply_transform_all(|_| true);
    headers.apply_transform("key", |_| true);
    assert!(headers.is_empty());
}

// =============================================================================
// Test 14: shouldValidateBuildInTypes
// =============================================================================
#[test]
fn should_validate_build_in_types() {
    let headers = ConnectHeaders::new();

    // Boolean
    headers.add_bool("bool", true);
    let header = headers.last_with_name("bool").unwrap();
    assert!(header.value().as_bool().is_some());
    assert_eq!(Some(true), header.value().as_bool());

    // Int8
    headers.add_int8("int8", 42);
    let header = headers.last_with_name("int8").unwrap();
    assert_eq!(Some(42i8), header.value().as_int8());

    // Int16
    headers.add_int16("int16", 1000);
    let header = headers.last_with_name("int16").unwrap();
    assert_eq!(Some(1000i16), header.value().as_int16());

    // Int32
    headers.add_int32("int32", 100000);
    let header = headers.last_with_name("int32").unwrap();
    assert_eq!(Some(100000i32), header.value().as_int32());

    // Int64
    headers.add_int64("int64", 10000000000);
    let header = headers.last_with_name("int64").unwrap();
    assert_eq!(Some(10000000000i64), header.value().as_int64());

    // Float32
    headers.add_float32("float32", 1.5);
    let header = headers.last_with_name("float32").unwrap();
    assert!(header.value().as_float32().is_some());

    // Float64
    headers.add_float64("float64", 1.5);
    let header = headers.last_with_name("float64").unwrap();
    assert!(header.value().as_float64().is_some());

    // String
    headers.add_string("string", Some("hello"));
    let header = headers.last_with_name("string").unwrap();
    assert_eq!(Some("hello"), header.value().as_string());
}

// =============================================================================
// Test 15: shouldValidateLogicalTypes
// =============================================================================
#[test]
fn should_validate_logical_types() {
    let headers = ConnectHeaders::new();

    // Date
    let date = NaiveDate::from_ymd_opt(2020, 1, 15).unwrap();
    headers.add_date("date", date);
    let header = headers.last_with_name("date").unwrap();
    assert!(header.schema().name() == Some(Date::LOGICAL_NAME));

    // Time
    let time = NaiveTime::from_hms_milli_opt(12, 30, 45, 500).unwrap();
    headers.add_time("time", time);
    let header = headers.last_with_name("time").unwrap();
    assert!(header.schema().name() == Some(Time::LOGICAL_NAME));

    // Timestamp
    let timestamp = Utc.from_utc_datetime(&chrono::NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2020, 1, 15).unwrap(),
        NaiveTime::from_hms_milli_opt(12, 30, 45, 500).unwrap(),
    ));
    headers.add_timestamp("timestamp", timestamp);
    let header = headers.last_with_name("timestamp").unwrap();
    assert!(header.schema().name() == Some(Timestamp::LOGICAL_NAME));

    // Decimal
    let decimal = BigDecimal::from_str("123.45").unwrap();
    headers.add_decimal("decimal", decimal.clone(), 2);
    let header = headers.last_with_name("decimal").unwrap();
    assert!(header.schema().name() == Some(Decimal::LOGICAL_NAME));
}

// =============================================================================
// Test 16: shouldAddDate
// =============================================================================
#[test]
fn should_add_date() {
    let headers = ConnectHeaders::new();
    let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    headers.add_date("date", date);

    let header = headers.last_with_name("date");
    assert!(header.is_some());
    let header = header.unwrap();
    assert!(header.schema().name() == Some(Date::LOGICAL_NAME));
}

// =============================================================================
// Test 17: shouldAddTime
// =============================================================================
#[test]
fn should_add_time() {
    let headers = ConnectHeaders::new();
    let time = NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap();
    headers.add_time("time", time);

    let header = headers.last_with_name("time");
    assert!(header.is_some());
    let header = header.unwrap();
    assert!(header.schema().name() == Some(Time::LOGICAL_NAME));
}

// =============================================================================
// Test 18: shouldAddTimestamp
// =============================================================================
#[test]
fn should_add_timestamp() {
    let headers = ConnectHeaders::new();
    let timestamp = Utc.timestamp_millis_opt(0).single().unwrap();
    headers.add_timestamp("timestamp", timestamp);

    let header = headers.last_with_name("timestamp");
    assert!(header.is_some());
    let header = header.unwrap();
    assert!(header.schema().name() == Some(Timestamp::LOGICAL_NAME));
}

// =============================================================================
// Test 19: shouldAddDecimal
// =============================================================================
#[test]
fn should_add_decimal() {
    let headers = ConnectHeaders::new();
    let decimal = BigDecimal::from_str("1.23").unwrap();
    headers.add_decimal("decimal", decimal, 2);

    let header = headers.last_with_name("decimal");
    assert!(header.is_some());
    let header = header.unwrap();
    assert!(header.schema().name() == Some(Decimal::LOGICAL_NAME));
}

// =============================================================================
// Test 20: shouldDuplicateAndAlwaysReturnEquivalentButDifferentObject
// =============================================================================
#[test]
fn should_duplicate_and_always_return_equivalent_but_different_object() {
    let headers = ConnectHeaders::new();
    populate_headers(&headers);

    let duplicate = headers.duplicate();
    assert_eq!(headers, duplicate);

    // Modify original, duplicate should not change
    headers.add_string("new", Some("value"));
    assert!(duplicate.last_with_name("new").is_none());
}

// =============================================================================
// Test 21: shouldNotValidateNullValuesWithBuiltInTypes
// =============================================================================
#[test]
fn should_not_validate_null_values_with_built_in_types() {
    let headers = ConnectHeaders::new();

    // Cannot add null to non-optional schemas
    let bool_schema = SchemaBuilder::bool().build();
    let result = headers.add("bool", &bool_schema, json!(null));
    assert!(result.is_err());

    let int32_schema = SchemaBuilder::int32().build();
    let result = headers.add("int32", &int32_schema, json!(null));
    assert!(result.is_err());
}

// =============================================================================
// Test 22: shouldNotValidateMismatchedValuesWithBuiltInTypes
// =============================================================================
#[test]
fn should_not_validate_mismatched_values_with_built_in_types() {
    let headers = ConnectHeaders::new();

    // String to boolean
    let bool_schema = SchemaBuilder::bool().build();
    let result = headers.add("bool", &bool_schema, json!("not a bool"));
    assert!(result.is_err());

    // String to int
    let int32_schema = SchemaBuilder::int32().build();
    let result = headers.add("int32", &int32_schema, json!("not an int"));
    assert!(result.is_err());
}
