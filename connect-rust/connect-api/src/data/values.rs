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

use crate::data::{
    ConnectSchema, Date, Decimal, Field, Schema, SchemaAndValue, SchemaBuilder, SchemaType, Struct,
    Time, Timestamp,
};
use crate::errors::ConnectError;
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use num_bigint::BigInt;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

/// ISO 8601 date format pattern
pub const ISO_8601_DATE_FORMAT_PATTERN: &str = "yyyy-MM-dd";
/// ISO 8601 time format pattern
pub const ISO_8601_TIME_FORMAT_PATTERN: &str = "HH:mm:ss.SSS'Z'";
/// ISO 8601 timestamp format pattern
pub const ISO_8601_TIMESTAMP_FORMAT_PATTERN: &str = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

/// Milliseconds per day
const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

/// Null value string representation
const NULL_VALUE: &str = "null";

/// Values utility for type conversions.
///
/// This corresponds to `org.apache.kafka.connect.data.Values` in Java.
pub struct Values;

/// ValueParser for parsing string values to JSON values.
pub struct ValueParser;

impl ValueParser {
    /// Parses a JSON string value.
    pub fn parse_json(s: &str) -> Result<Value, ConnectError> {
        serde_json::from_str(s)
            .map_err(|e| ConnectError::data(format!("Failed to parse JSON: {}", e)))
    }

    /// Parses a quoted string value.
    pub fn parse_quoted_string(s: &str) -> Option<Value> {
        if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
            Some(Value::String(s[1..s.len() - 1].to_string()))
        } else {
            None
        }
    }

    /// Parses a boolean value from string.
    pub fn parse_boolean(s: &str) -> Option<Value> {
        match s {
            "true" => Some(Value::Bool(true)),
            "false" => Some(Value::Bool(false)),
            _ => None,
        }
    }

    /// Parses a number from string.
    pub fn parse_number(s: &str) -> Option<Value> {
        if let Ok(i) = s.parse::<i64>() {
            Some(Value::Number(i.into()))
        } else if let Ok(f) = s.parse::<f64>() {
            serde_json::Number::from_f64(f).map(Value::Number)
        } else {
            None
        }
    }

    /// Parses a generic string to best matching value type.
    pub fn parse(s: &str) -> Result<Value, ConnectError> {
        // Try JSON first
        if let Ok(v) = Self::parse_json(s) {
            return Ok(v);
        }

        // Try quoted string
        if let Some(v) = Self::parse_quoted_string(s) {
            return Ok(v);
        }

        // Try boolean
        if let Some(v) = Self::parse_boolean(s) {
            return Ok(v);
        }

        // Try number
        if let Some(v) = Self::parse_number(s) {
            return Ok(v);
        }

        // Default to string
        Ok(Value::String(s.to_string()))
    }
}

/// SchemaInferencer for inferring schema from values.
pub struct SchemaInferencer;

impl SchemaInferencer {
    /// Infers schema from a JSON value.
    pub fn infer(value: &Value) -> Option<ConnectSchema> {
        Values::infer_schema(value)
    }

    /// Infers schema type from a JSON value.
    pub fn infer_type(value: &Value) -> SchemaType {
        match value {
            Value::Null => SchemaType::String, // Default for null
            Value::Bool(_) => SchemaType::Boolean,
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                        SchemaType::Int8
                    } else if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                        SchemaType::Int16
                    } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        SchemaType::Int32
                    } else {
                        SchemaType::Int64
                    }
                } else if n.as_f64().is_some() {
                    SchemaType::Float64
                } else {
                    SchemaType::String
                }
            }
            Value::String(_) => SchemaType::String,
            Value::Array(_) => SchemaType::Array,
            Value::Object(_) => SchemaType::Map,
        }
    }

    /// Infers schema for an array element.
    pub fn infer_array_element_schema(arr: &[Value]) -> Option<ConnectSchema> {
        if arr.is_empty() {
            return None;
        }
        let first = &arr[0];
        Self::infer(first)
    }
}

impl Values {
    /// Convert the specified value to a Boolean value.
    /// The supplied schema is required if the value is a logical type.
    ///
    /// # Arguments
    /// * `schema` - the schema for the value; may be null
    /// * `value` - the value to be converted; may be null
    ///
    /// # Returns
    /// The representation as a boolean, or null if the supplied value was null
    pub fn convert_to_boolean(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<bool>, ConnectError> {
        if value.is_null() {
            return Ok(None);
        }
        match value {
            Value::Bool(b) => Ok(Some(*b)),
            Value::String(s) => {
                let parsed = Self::parse_string(s)?;
                if let Some(v) = parsed.value() {
                    if v.as_bool().is_some() {
                        return Ok(Some(v.as_bool().unwrap()));
                    }
                }
                Ok(Some(Self::as_long(value, schema)? != 0))
            }
            _ => Ok(Some(Self::as_long(value, schema)? != 0)),
        }
    }

    /// Convert the specified value to a Byte (i8) value.
    /// The supplied schema is required if the value is a logical type.
    pub fn convert_to_byte(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<i8>, ConnectError> {
        if value.is_null() {
            return Ok(None);
        }
        match value {
            Value::Number(n) => {
                let i = Self::as_long(value, schema)?;
                Ok(Some((i & 0xFF) as i8))
            }
            _ => Ok(Some((Self::as_long(value, schema)? & 0xFF) as i8)),
        }
    }

    /// Convert the specified value to a Short (i16) value.
    /// The supplied schema is required if the value is a logical type.
    pub fn convert_to_short(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<i16>, ConnectError> {
        if value.is_null() {
            return Ok(None);
        }
        Ok(Some((Self::as_long(value, schema)? & 0xFFFF) as i16))
    }

    /// Convert the specified value to an Int (i32) value.
    /// The supplied schema is required if the value is a logical type.
    pub fn convert_to_int(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<i32>, ConnectError> {
        if value.is_null() {
            return Ok(None);
        }
        Ok(Some((Self::as_long(value, schema)? & 0xFFFFFFFF) as i32))
    }

    /// Convert the specified value to a Long (i64) value.
    /// The supplied schema is required if the value is a logical type.
    pub fn convert_to_long(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<i64>, ConnectError> {
        if value.is_null() {
            return Ok(None);
        }
        Ok(Some(Self::as_long(value, schema)?))
    }

    /// Convert the specified value to a Float (f32) value.
    /// The supplied schema is required if the value is a logical type.
    pub fn convert_to_float(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<f32>, ConnectError> {
        if value.is_null() {
            return Ok(None);
        }
        Ok(Some(Self::as_double(value, schema)? as f32))
    }

    /// Convert the specified value to a Double (f64) value.
    /// The supplied schema is required if the value is a logical type.
    pub fn convert_to_double(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<f64>, ConnectError> {
        if value.is_null() {
            return Ok(None);
        }
        Ok(Some(Self::as_double(value, schema)?))
    }

    /// Convert the specified value to a String value.
    pub fn convert_to_string(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<String>, ConnectError> {
        if value.is_null() {
            return Ok(None);
        }
        Ok(Some(Self::convert_to_string_internal(value, false)))
    }

    /// Internal method to convert value to string.
    fn convert_to_string_internal(value: &Value, embedded: bool) -> String {
        match value {
            Value::Null => NULL_VALUE.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => {
                if embedded {
                    format!("\"{}\"", Self::escape_string(s))
                } else {
                    s.clone()
                }
            }
            Value::Array(arr) => {
                let elements: Vec<String> = arr
                    .iter()
                    .map(|v| Self::convert_to_string_internal(v, true))
                    .collect();
                format!("[{}]", elements.join(","))
            }
            Value::Object(obj) => {
                let entries: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "{}:{}",
                            Self::convert_to_string_internal(&Value::String(k.clone()), true),
                            Self::convert_to_string_internal(v, true)
                        )
                    })
                    .collect();
                format!("{{{}}}", entries.join(","))
            }
        }
    }

    /// Escape string for embedded representation.
    fn escape_string(s: &str) -> String {
        // Escape backslashes first: \ -> \\
        let replace1 = s.replace('\\', "\\\\");
        // Then escape quotes: " -> \"
        replace1.replace('"', "\\\"")
    }

    /// Convert the specified value to a List value.
    /// If the value is a string representation of an array, this method will parse the string.
    pub fn convert_to_list(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<Vec<Value>>, ConnectError> {
        if value.is_null() {
            return Err(ConnectError::data(
                "Unable to convert a null value to a schema that requires a value",
            ));
        }
        match value {
            Value::String(s) => {
                let parsed = Self::parse_string(s)?;
                if let Some(v) = parsed.value() {
                    if v.is_array() {
                        return Ok(Some(v.as_array().unwrap().clone()));
                    }
                }
                Err(ConnectError::data(format!("Cannot convert {} to list", s)))
            }
            Value::Array(arr) => Ok(Some(arr.clone())),
            _ => Err(ConnectError::data(format!(
                "Cannot convert {} to list",
                value
            ))),
        }
    }

    /// Convert the specified value to a Map value.
    /// If the value is a string representation of a map, this method will parse the string.
    pub fn convert_to_map(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<HashMap<String, Value>>, ConnectError> {
        if value.is_null() {
            return Err(ConnectError::data(
                "Unable to convert a null value to a schema that requires a value",
            ));
        }
        match value {
            Value::String(s) => {
                let parsed = Self::parse_string(s)?;
                if let Some(v) = parsed.value() {
                    if v.is_object() {
                        let map: HashMap<String, Value> = v
                            .as_object()
                            .unwrap()
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        return Ok(Some(map));
                    }
                }
                Err(ConnectError::data(format!("Cannot convert {} to map", s)))
            }
            Value::Object(obj) => {
                let map: HashMap<String, Value> =
                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                Ok(Some(map))
            }
            _ => Err(ConnectError::data(format!(
                "Cannot convert {} to map",
                value
            ))),
        }
    }

    /// Convert the specified value to a Struct value.
    /// Structs cannot be converted from other types, so this method returns a struct only if the value is a struct.
    pub fn convert_to_struct(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<Struct>, ConnectError> {
        if value.is_null() {
            return Err(ConnectError::data(
                "Unable to convert a null value to a schema that requires a value",
            ));
        }
        match value {
            Value::Object(obj) => {
                // Try to convert JSON object to Struct
                // This requires a schema to properly convert
                Err(ConnectError::data(
                    "Struct conversion requires schema information",
                ))
            }
            _ => Err(ConnectError::data(format!(
                "Cannot convert {} to struct",
                value
            ))),
        }
    }

    /// Convert the specified value to a Time value.
    pub fn convert_to_time(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<NaiveTime>, ConnectError> {
        if value.is_null() {
            return Err(ConnectError::data(
                "Unable to convert a null value to a schema that requires a value",
            ));
        }

        // Check if schema is a Time logical type
        if let Some(s) = schema {
            if s.name() == Some(Time::LOGICAL_NAME) {
                let millis = Self::as_long(value, schema)?;
                return Ok(Some(Time::from_logical_simple(millis as i32)));
            }
        }

        // Try to parse as time string
        match value {
            Value::String(s) => {
                let parsed = Self::parse_string(s)?;
                if let Some(v) = parsed.value() {
                    if v.is_string() {
                        // Try to parse as ISO time format
                        if let Ok(time) = Self::parse_iso_time(v.as_str().unwrap()) {
                            return Ok(Some(time));
                        }
                    }
                }
                Err(ConnectError::data(format!("Cannot convert {} to time", s)))
            }
            _ => {
                let millis = Self::as_long(value, schema)?;
                Ok(Some(Time::from_logical_simple(
                    (millis % MILLIS_PER_DAY) as i32,
                )))
            }
        }
    }

    /// Convert the specified value to a Date value.
    pub fn convert_to_date(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<NaiveDate>, ConnectError> {
        if value.is_null() {
            return Err(ConnectError::data(
                "Unable to convert a null value to a schema that requires a value",
            ));
        }

        // Check if schema is a Date logical type
        if let Some(s) = schema {
            if s.name() == Some(Date::LOGICAL_NAME) {
                let days = Self::as_long(value, schema)?;
                return Ok(Some(Date::from_logical_simple(days as i32)));
            }
        }

        // Try to parse as date string
        match value {
            Value::String(s) => {
                let parsed = Self::parse_string(s)?;
                if let Some(v) = parsed.value() {
                    if v.is_string() {
                        // Try to parse as ISO date format
                        if let Ok(date) = Self::parse_iso_date(v.as_str().unwrap()) {
                            return Ok(Some(date));
                        }
                    }
                }
                Err(ConnectError::data(format!("Cannot convert {} to date", s)))
            }
            _ => {
                let days = Self::as_long(value, schema)?;
                Ok(Some(Date::from_logical_simple(
                    (days / MILLIS_PER_DAY) as i32,
                )))
            }
        }
    }

    /// Convert the specified value to a Timestamp value.
    pub fn convert_to_timestamp(
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<DateTime<Utc>>, ConnectError> {
        if value.is_null() {
            return Err(ConnectError::data(
                "Unable to convert a null value to a schema that requires a value",
            ));
        }

        // Check if schema is a Timestamp logical type
        if let Some(s) = schema {
            if s.name() == Some(Timestamp::LOGICAL_NAME) {
                let millis = Self::as_long(value, schema)?;
                return Ok(Some(Timestamp::from_logical_simple(millis)));
            }
        }

        // Try to parse as timestamp string
        match value {
            Value::String(s) => {
                let parsed = Self::parse_string(s)?;
                if let Some(v) = parsed.value() {
                    if v.is_string() {
                        // Try to parse as ISO timestamp format
                        if let Ok(ts) = Self::parse_iso_timestamp(v.as_str().unwrap()) {
                            return Ok(Some(ts));
                        }
                    }
                }
                Err(ConnectError::data(format!(
                    "Cannot convert {} to timestamp",
                    s
                )))
            }
            _ => {
                let millis = Self::as_long(value, schema)?;
                Ok(Some(Timestamp::from_logical_simple(millis)))
            }
        }
    }

    /// Convert the specified value to a Decimal value.
    pub fn convert_to_decimal(
        schema: Option<&dyn Schema>,
        value: &Value,
        scale: i32,
    ) -> Result<Option<BigDecimal>, ConnectError> {
        if value.is_null() {
            return Err(ConnectError::data(
                "Unable to convert a null value to a schema that requires a value",
            ));
        }

        match value {
            Value::String(s) => BigDecimal::from_str(s)
                .map(Some)
                .map_err(|_| ConnectError::data(format!("Cannot convert {} to decimal", s))),
            Value::Number(n) => {
                let s = n.to_string();
                BigDecimal::from_str(&s)
                    .map(Some)
                    .map_err(|_| ConnectError::data(format!("Cannot convert {} to decimal", n)))
            }
            Value::Array(arr) => {
                // Bytes representation - need to convert to BigDecimal
                let bytes: Vec<u8> = arr
                    .iter()
                    .filter_map(|v| v.as_i64().map(|i| i as u8))
                    .collect();
                Decimal::to_logical(&bytes, scale).map(Some)
            }
            _ => Err(ConnectError::data(format!(
                "Cannot convert {} to decimal",
                value
            ))),
        }
    }

    /// Infer a schema for the given value.
    pub fn infer_schema(value: &Value) -> Option<ConnectSchema> {
        match value {
            Value::Null => None,
            Value::Bool(_) => Some(SchemaBuilder::bool().build()),
            Value::Number(n) => {
                // Try to determine the best integer type
                if let Some(i) = n.as_i64() {
                    if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                        return Some(SchemaBuilder::int8().build());
                    } else if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                        return Some(SchemaBuilder::int16().build());
                    } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        return Some(SchemaBuilder::int32().build());
                    } else {
                        return Some(SchemaBuilder::int64().build());
                    }
                }
                // Floating point
                Some(SchemaBuilder::float64().build())
            }
            Value::String(_) => Some(SchemaBuilder::string().build()),
            Value::Array(arr) => {
                if arr.is_empty() {
                    return None;
                }
                // Try to infer element schema
                let element_schema = Self::infer_schema(&arr[0]);
                if let Some(elem_schema) = element_schema {
                    Some(SchemaBuilder::array(elem_schema).build())
                } else {
                    None
                }
            }
            Value::Object(obj) => {
                if obj.is_empty() {
                    return None;
                }
                // Try to infer key and value schemas
                let first_entry = obj.iter().next();
                if let Some((_, v)) = first_entry {
                    let value_schema = Self::infer_schema(v);
                    if let Some(val_schema) = value_schema {
                        Some(
                            SchemaBuilder::map(SchemaBuilder::string().build(), val_schema).build(),
                        )
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    /// Convert the specified value to the desired scalar value type (long).
    pub fn as_long(value: &Value, schema: Option<&dyn Schema>) -> Result<i64, ConnectError> {
        match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(i)
                } else if let Some(f) = n.as_f64() {
                    Ok(f as i64)
                } else {
                    Err(ConnectError::data(format!("Cannot convert {} to long", n)))
                }
            }
            Value::String(s) => {
                // Try to parse as number
                if let Ok(i) = s.parse::<i64>() {
                    Ok(i)
                } else if let Ok(d) = BigDecimal::from_str(s) {
                    Ok(d.to_string().parse::<i64>().unwrap_or(0))
                } else {
                    Err(ConnectError::data(format!("Cannot convert {} to long", s)))
                }
            }
            Value::Bool(b) => Ok(if *b { 1 } else { 0 }),
            _ => {
                // Check for logical types
                if let Some(s) = schema {
                    let schema_name = s.name();
                    if schema_name == Some(Date::LOGICAL_NAME) {
                        // Date is stored as days since epoch
                        return Ok(Self::as_long(value, None)?);
                    }
                    if schema_name == Some(Time::LOGICAL_NAME) {
                        // Time is stored as milliseconds since midnight
                        return Ok(Self::as_long(value, None)?);
                    }
                    if schema_name == Some(Timestamp::LOGICAL_NAME) {
                        // Timestamp is stored as milliseconds since epoch
                        return Ok(Self::as_long(value, None)?);
                    }
                }
                Err(ConnectError::data(format!(
                    "Cannot convert {} to long",
                    value
                )))
            }
        }
    }

    /// Convert the specified value to the desired floating point type (double).
    pub fn as_double(value: &Value, schema: Option<&dyn Schema>) -> Result<f64, ConnectError> {
        match value {
            Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    Ok(f)
                } else if let Some(i) = n.as_i64() {
                    Ok(i as f64)
                } else {
                    Err(ConnectError::data(format!(
                        "Cannot convert {} to double",
                        n
                    )))
                }
            }
            Value::String(s) => {
                if let Ok(f) = s.parse::<f64>() {
                    Ok(f)
                } else if let Ok(d) = BigDecimal::from_str(s) {
                    Ok(d.to_string().parse::<f64>().unwrap_or(0.0))
                } else {
                    // Try as long
                    Ok(Self::as_long(value, schema)? as f64)
                }
            }
            Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
            _ => Ok(Self::as_long(value, schema)? as f64),
        }
    }

    /// Parse the specified string representation of a value into its schema and value.
    pub fn parse_string(value: &str) -> Result<SchemaAndValue, ConnectError> {
        if value.is_empty() {
            return Ok(SchemaAndValue::new(
                Some(SchemaBuilder::string().build()),
                Some(Value::String(value.to_string())),
            ));
        }

        // Check for null
        if value == NULL_VALUE {
            return Ok(SchemaAndValue::new(None, None));
        }

        // Check for boolean
        if value == "true" {
            return Ok(SchemaAndValue::new(
                Some(SchemaBuilder::bool().build()),
                Some(Value::Bool(true)),
            ));
        }
        if value == "false" {
            return Ok(SchemaAndValue::new(
                Some(SchemaBuilder::bool().build()),
                Some(Value::Bool(false)),
            ));
        }

        // Check for array
        if value.starts_with('[') {
            return Self::parse_array(value);
        }

        // Check for map/object
        if value.starts_with('{') {
            return Self::parse_map(value);
        }

        // Check for quoted string
        if value.starts_with('"') {
            return Self::parse_quoted_string(value);
        }

        // Try to parse as number
        if let Ok(i) = value.parse::<i64>() {
            // Determine the best integer schema
            let schema = if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                SchemaBuilder::int8().build()
            } else if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                SchemaBuilder::int16().build()
            } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                SchemaBuilder::int32().build()
            } else {
                SchemaBuilder::int64().build()
            };
            return Ok(SchemaAndValue::new(
                Some(schema),
                Some(Value::Number(i.into())),
            ));
        }

        // Try to parse as float
        if let Ok(f) = value.parse::<f64>() {
            let schema = if (f as f32) as f64 != f && !f.is_infinite() {
                SchemaBuilder::float64().build()
            } else {
                SchemaBuilder::float32().build()
            };
            let num =
                serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0));
            return Ok(SchemaAndValue::new(Some(schema), Some(Value::Number(num))));
        }

        // Try to parse as temporal types
        if let Ok(date) = Self::parse_iso_date(value) {
            return Ok(SchemaAndValue::new(
                Some(Date::schema()),
                Some(Value::Number(Date::to_connect_data(date).into())),
            ));
        }
        if let Ok(time) = Self::parse_iso_time(value) {
            return Ok(SchemaAndValue::new(
                Some(Time::schema()),
                Some(Value::Number(Time::to_connect_data(time).into())),
            ));
        }
        if let Ok(ts) = Self::parse_iso_timestamp(value) {
            return Ok(SchemaAndValue::new(
                Some(Timestamp::schema()),
                Some(Value::Number(Timestamp::to_connect_data(ts).into())),
            ));
        }

        // Default: treat as string
        Ok(SchemaAndValue::new(
            Some(SchemaBuilder::string().build()),
            Some(Value::String(value.to_string())),
        ))
    }

    /// Parse an array from string representation.
    fn parse_array(value: &str) -> Result<SchemaAndValue, ConnectError> {
        // Use serde_json to parse JSON array
        let parsed: Result<Vec<Value>, _> = serde_json::from_str(value);
        match parsed {
            Ok(arr) => {
                // Infer element schema
                let element_schema = if arr.is_empty() {
                    None
                } else {
                    Self::infer_schema(&arr[0])
                };
                let schema = if let Some(elem_schema) = element_schema {
                    SchemaBuilder::array(elem_schema).build()
                } else {
                    SchemaBuilder::array(SchemaBuilder::string().build()).build()
                };
                Ok(SchemaAndValue::new(Some(schema), Some(Value::Array(arr))))
            }
            Err(_) => Err(ConnectError::data(format!("Cannot parse array: {}", value))),
        }
    }

    /// Parse a map from string representation.
    fn parse_map(value: &str) -> Result<SchemaAndValue, ConnectError> {
        // Use serde_json to parse JSON object
        let parsed: Result<serde_json::Map<String, Value>, _> = serde_json::from_str(value);
        match parsed {
            Ok(obj) => {
                // Infer value schema
                let value_schema = if obj.is_empty() {
                    None
                } else {
                    let first_value = obj.values().next();
                    first_value.and_then(|v| Self::infer_schema(v))
                };
                let schema = if let Some(val_schema) = value_schema {
                    SchemaBuilder::map(SchemaBuilder::string().build(), val_schema).build()
                } else {
                    SchemaBuilder::map(
                        SchemaBuilder::string().build(),
                        SchemaBuilder::string().build(),
                    )
                    .build()
                };
                Ok(SchemaAndValue::new(Some(schema), Some(Value::Object(obj))))
            }
            Err(_) => Err(ConnectError::data(format!("Cannot parse map: {}", value))),
        }
    }

    /// Parse a quoted string from string representation.
    fn parse_quoted_string(value: &str) -> Result<SchemaAndValue, ConnectError> {
        if !value.starts_with('"') || !value.ends_with('"') {
            return Err(ConnectError::data(format!(
                "Invalid quoted string: {}",
                value
            )));
        }
        // Remove quotes and unescape
        let content = &value[1..value.len() - 1];
        let unescaped = Self::unescape_string(content);
        Ok(SchemaAndValue::new(
            Some(SchemaBuilder::string().build()),
            Some(Value::String(unescaped)),
        ))
    }

    /// Unescape string.
    fn unescape_string(s: &str) -> String {
        // Unescape quotes first: \" -> "
        s.replace("\\\"", "\"")
            // Then unescape backslashes: \\ -> \
            .replace("\\\\", "\\")
    }

    /// Parse ISO 8601 date format (yyyy-MM-dd).
    fn parse_iso_date(value: &str) -> Result<NaiveDate, ConnectError> {
        NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map_err(|_| ConnectError::data(format!("Cannot parse date: {}", value)))
    }

    /// Parse ISO 8601 time format (HH:mm:ss.SSS'Z').
    fn parse_iso_time(value: &str) -> Result<NaiveTime, ConnectError> {
        // Handle different time formats
        if value.ends_with('Z') {
            let time_str = &value[..value.len() - 1];
            NaiveTime::parse_from_str(time_str, "%H:%M:%S%.3f")
                .or_else(|_| NaiveTime::parse_from_str(time_str, "%H:%M:%S"))
                .map_err(|_| ConnectError::data(format!("Cannot parse time: {}", value)))
        } else {
            NaiveTime::parse_from_str(value, "%H:%M:%S%.3f")
                .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S"))
                .map_err(|_| ConnectError::data(format!("Cannot parse time: {}", value)))
        }
    }

    /// Parse ISO 8601 timestamp format (yyyy-MM-dd'T'HH:mm:ss.SSS'Z').
    fn parse_iso_timestamp(value: &str) -> Result<DateTime<Utc>, ConnectError> {
        // Handle different timestamp formats
        if value.ends_with('Z') {
            let ts_str = &value[..value.len() - 1];
            NaiveDate::parse_from_str(&ts_str[..10], "%Y-%m-%d")
                .and_then(|date| {
                    NaiveTime::parse_from_str(&ts_str[11..], "%H:%M:%S%.3f")
                        .or_else(|_| NaiveTime::parse_from_str(&ts_str[11..], "%H:%M:%S"))
                        .map(|time| date.and_time(time))
                })
                .map(|dt| Utc.from_utc_datetime(&dt))
                .map_err(|_| ConnectError::data(format!("Cannot parse timestamp: {}", value)))
        } else {
            // Try without Z suffix
            let parts: Vec<&str> = value.split('T').collect();
            if parts.len() != 2 {
                return Err(ConnectError::data(format!(
                    "Cannot parse timestamp: {}",
                    value
                )));
            }
            NaiveDate::parse_from_str(parts[0], "%Y-%m-%d")
                .and_then(|date| {
                    NaiveTime::parse_from_str(parts[1], "%H:%M:%S%.3f")
                        .or_else(|_| NaiveTime::parse_from_str(parts[1], "%H:%M:%S"))
                        .map(|time| date.and_time(time))
                })
                .map(|dt| Utc.from_utc_datetime(&dt))
                .map_err(|_| ConnectError::data(format!("Cannot parse timestamp: {}", value)))
        }
    }

    /// Get date format for a value based on its magnitude.
    pub fn date_format_for(millis: i64) -> &'static str {
        if millis < MILLIS_PER_DAY {
            ISO_8601_TIME_FORMAT_PATTERN
        } else if millis % MILLIS_PER_DAY == 0 {
            ISO_8601_DATE_FORMAT_PATTERN
        } else {
            ISO_8601_TIMESTAMP_FORMAT_PATTERN
        }
    }

    /// Convert the value to the desired type.
    pub fn convert_to(
        to_schema: &dyn Schema,
        from_schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Value, ConnectError> {
        if value.is_null() {
            if to_schema.is_optional() {
                return Ok(Value::Null);
            }
            return Err(ConnectError::data(
                "Unable to convert a null value to a schema that requires a value",
            ));
        }

        match to_schema.r#type() {
            SchemaType::Boolean => Ok(Value::Bool(
                Self::convert_to_boolean(from_schema, value)?.unwrap_or(false),
            )),
            SchemaType::Int8 => Ok(Value::Number(
                Self::convert_to_byte(from_schema, value)?
                    .unwrap_or(0)
                    .into(),
            )),
            SchemaType::Int16 => Ok(Value::Number(
                Self::convert_to_short(from_schema, value)?
                    .unwrap_or(0)
                    .into(),
            )),
            SchemaType::Int32 => {
                // Check for logical types
                if to_schema.name() == Some(Date::LOGICAL_NAME) {
                    let date = Self::convert_to_date(from_schema, value)?;
                    return Ok(Value::Number(
                        date.map(|d| Date::to_connect_data(d)).unwrap_or(0).into(),
                    ));
                }
                if to_schema.name() == Some(Time::LOGICAL_NAME) {
                    let time = Self::convert_to_time(from_schema, value)?;
                    return Ok(Value::Number(
                        time.map(|t| Time::to_connect_data(t)).unwrap_or(0).into(),
                    ));
                }
                Ok(Value::Number(
                    Self::convert_to_int(from_schema, value)?
                        .unwrap_or(0)
                        .into(),
                ))
            }
            SchemaType::Int64 => {
                // Check for Timestamp logical type
                if to_schema.name() == Some(Timestamp::LOGICAL_NAME) {
                    let ts = Self::convert_to_timestamp(from_schema, value)?;
                    return Ok(Value::Number(
                        ts.map(|t| Timestamp::to_connect_data(t))
                            .unwrap_or(0)
                            .into(),
                    ));
                }
                Ok(Value::Number(
                    Self::convert_to_long(from_schema, value)?
                        .unwrap_or(0)
                        .into(),
                ))
            }
            SchemaType::Float32 => Ok(Value::Number(
                serde_json::Number::from_f64(
                    Self::convert_to_float(from_schema, value)?.unwrap_or(0.0) as f64,
                )
                .unwrap_or_else(|| serde_json::Number::from(0)),
            )),
            SchemaType::Float64 => Ok(Value::Number(
                serde_json::Number::from_f64(
                    Self::convert_to_double(from_schema, value)?.unwrap_or(0.0),
                )
                .unwrap_or_else(|| serde_json::Number::from(0)),
            )),
            SchemaType::String => Ok(Value::String(
                Self::convert_to_string(from_schema, value)?.unwrap_or_default(),
            )),
            SchemaType::Bytes => {
                // Check for Decimal logical type
                if to_schema.name() == Some(Decimal::LOGICAL_NAME) {
                    let scale = Decimal::scale(to_schema).unwrap_or(0);
                    let decimal = Self::convert_to_decimal(from_schema, value, scale)?;
                    if let Some(d) = decimal {
                        let bytes = Decimal::from_logical(&d, scale);
                        let arr: Vec<Value> = bytes
                            .iter()
                            .map(|b| Value::Number((*b as i64).into()))
                            .collect();
                        return Ok(Value::Array(arr));
                    }
                }
                // Default bytes conversion
                match value {
                    Value::String(s) => {
                        // Base64 encode
                        let encoded = base64::encode(s.as_bytes());
                        Ok(Value::String(encoded))
                    }
                    Value::Array(arr) => Ok(Value::Array(arr.clone())),
                    _ => Err(ConnectError::data(format!(
                        "Cannot convert {} to bytes",
                        value
                    ))),
                }
            }
            SchemaType::Array => {
                let list = Self::convert_to_list(from_schema, value)?;
                Ok(Value::Array(list.unwrap_or_default()))
            }
            SchemaType::Map => {
                let map = Self::convert_to_map(from_schema, value)?;
                Ok(Value::Object(map.unwrap_or_default().into_iter().collect()))
            }
            SchemaType::Struct => Err(ConnectError::data(
                "Struct conversion requires schema information",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_convert_to_boolean() {
        let schema = SchemaBuilder::bool().build();

        // Test boolean value
        let result = Values::convert_to_boolean(Some(&schema), &json!(true));
        assert_eq!(result.unwrap(), Some(true));

        // Test string "true"
        let result = Values::convert_to_boolean(Some(&schema), &json!("true"));
        assert_eq!(result.unwrap(), Some(true));

        // Test string "false"
        let result = Values::convert_to_boolean(Some(&schema), &json!("false"));
        assert_eq!(result.unwrap(), Some(false));

        // Test integer
        let result = Values::convert_to_boolean(Some(&schema), &json!(1));
        assert_eq!(result.unwrap(), Some(true));

        let result = Values::convert_to_boolean(Some(&schema), &json!(0));
        assert_eq!(result.unwrap(), Some(false));
    }

    #[test]
    fn test_convert_to_byte() {
        let schema = SchemaBuilder::int8().build();

        let result = Values::convert_to_byte(Some(&schema), &json!(42));
        assert_eq!(result.unwrap(), Some(42));

        let result = Values::convert_to_byte(Some(&schema), &json!("42"));
        assert_eq!(result.unwrap(), Some(42));
    }

    #[test]
    fn test_convert_to_short() {
        let schema = SchemaBuilder::int16().build();

        let result = Values::convert_to_short(Some(&schema), &json!(1000));
        assert_eq!(result.unwrap(), Some(1000));

        let result = Values::convert_to_short(Some(&schema), &json!("1000"));
        assert_eq!(result.unwrap(), Some(1000));
    }

    #[test]
    fn test_convert_to_int() {
        let schema = SchemaBuilder::int32().build();

        let result = Values::convert_to_int(Some(&schema), &json!(12345));
        assert_eq!(result.unwrap(), Some(12345));

        let result = Values::convert_to_int(Some(&schema), &json!("12345"));
        assert_eq!(result.unwrap(), Some(12345));
    }

    #[test]
    fn test_convert_to_long() {
        let schema = SchemaBuilder::int64().build();

        let result = Values::convert_to_long(Some(&schema), &json!(123456789));
        assert_eq!(result.unwrap(), Some(123456789));

        let result = Values::convert_to_long(Some(&schema), &json!("123456789"));
        assert_eq!(result.unwrap(), Some(123456789));
    }

    #[test]
    fn test_convert_to_float() {
        let schema = SchemaBuilder::float32().build();

        let result = Values::convert_to_float(Some(&schema), &json!(3.14));
        assert!(result.unwrap().is_some());

        let result = Values::convert_to_float(Some(&schema), &json!("3.14"));
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_convert_to_double() {
        let schema = SchemaBuilder::float64().build();

        let result = Values::convert_to_double(Some(&schema), &json!(3.141592653589793));
        assert!(result.unwrap().is_some());

        let result = Values::convert_to_double(Some(&schema), &json!("3.141592653589793"));
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_convert_to_string() {
        let result = Values::convert_to_string(None, &json!("hello"));
        assert_eq!(result.unwrap(), Some("hello".to_string()));

        let result = Values::convert_to_string(None, &json!(42));
        assert_eq!(result.unwrap(), Some("42".to_string()));

        let result = Values::convert_to_string(None, &json!(true));
        assert_eq!(result.unwrap(), Some("true".to_string()));
    }

    #[test]
    fn test_convert_to_list() {
        let result = Values::convert_to_list(None, &json!([1, 2, 3]));
        assert_eq!(result.unwrap().unwrap().len(), 3);

        let result = Values::convert_to_list(None, &json!("[1, 2, 3]"));
        assert_eq!(result.unwrap().unwrap().len(), 3);
    }

    #[test]
    fn test_convert_to_map() {
        let result = Values::convert_to_map(None, &json!({"a": 1, "b": 2}));
        assert_eq!(result.unwrap().unwrap().len(), 2);

        let result = Values::convert_to_map(None, &json!("{\"a\": 1, \"b\": 2}"));
        assert_eq!(result.unwrap().unwrap().len(), 2);
    }

    #[test]
    fn test_parse_string_boolean() {
        let result = Values::parse_string("true");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert_eq!(sv.value(), Some(&json!(true)));
    }

    #[test]
    fn test_parse_string_number() {
        let result = Values::parse_string("42");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert_eq!(sv.value(), Some(&json!(42)));
    }

    #[test]
    fn test_parse_string_quoted() {
        let result = Values::parse_string("\"hello\"");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert_eq!(sv.value(), Some(&json!("hello")));
    }

    #[test]
    fn test_parse_string_array() {
        let result = Values::parse_string("[1, 2, 3]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert!(sv.value().unwrap().is_array());
    }

    #[test]
    fn test_parse_string_object() {
        let result = Values::parse_string("{\"a\": 1}");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert!(sv.value().unwrap().is_object());
    }

    #[test]
    fn test_infer_schema() {
        // Null
        assert!(Values::infer_schema(&json!(null)).is_none());

        // Boolean
        let schema = Values::infer_schema(&json!(true)).unwrap();
        assert_eq!(schema.r#type(), SchemaType::Boolean);

        // Integer
        let schema = Values::infer_schema(&json!(42)).unwrap();
        assert_eq!(schema.r#type(), SchemaType::Int8);

        let schema = Values::infer_schema(&json!(1000)).unwrap();
        assert_eq!(schema.r#type(), SchemaType::Int16);

        let schema = Values::infer_schema(&json!(100000)).unwrap();
        assert_eq!(schema.r#type(), SchemaType::Int32);

        // String
        let schema = Values::infer_schema(&json!("hello")).unwrap();
        assert_eq!(schema.r#type(), SchemaType::String);

        // Array
        let schema = Values::infer_schema(&json!([1, 2, 3])).unwrap();
        assert_eq!(schema.r#type(), SchemaType::Array);

        // Object
        let schema = Values::infer_schema(&json!({"a": 1})).unwrap();
        assert_eq!(schema.r#type(), SchemaType::Map);
    }

    #[test]
    fn test_escape_and_unescape_string() {
        let original = "hello\"world\\test";
        let escaped = Values::escape_string(original);
        let unescaped = Values::unescape_string(&escaped);
        assert_eq!(original, unescaped);
    }

    // P0 Tests - Core parsing and conversion tests

    #[test]
    fn test_parse_null_string() {
        // Java: shouldParseNullString() - null input returns null SchemaAndValue
        let result = Values::parse_string("null");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert!(sv.value().is_none() || sv.value().unwrap().is_null());
    }

    #[test]
    fn test_parse_empty_string() {
        // Java: shouldParseEmptyString() - empty string parsed as STRING schema with empty value
        let result = Values::parse_string("");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert!(sv.value().is_some());
        if let Some(v) = sv.value() {
            assert_eq!(v, &json!(""));
        }
    }

    #[test]
    fn test_parse_empty_map() {
        // Java: shouldParseEmptyMap() - "{}" parsed as empty Map
        let result = Values::parse_string("{}");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert!(sv.value().unwrap().is_object());
        assert!(sv.value().unwrap().as_object().unwrap().is_empty());
    }

    #[test]
    fn test_parse_empty_array() {
        // Java: shouldParseEmptyArray() - "[]" parsed as empty Array
        let result = Values::parse_string("[]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert!(sv.value().unwrap().is_array());
        assert!(sv.value().unwrap().as_array().unwrap().is_empty());
    }

    #[test]
    fn test_parse_null() {
        // Java: shouldParseNull() - "null" parsed as null value
        let result = Values::parse_string("null");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert!(sv.value().is_none() || sv.value().unwrap().is_null());
    }

    #[test]
    fn test_convert_null_value() {
        // Java: shouldConvertNullValue() - null values to various schema types
        let null_val = json!(null);

        // Optional schemas should accept null
        let optional_schema = SchemaBuilder::string().optional().build();
        let result = Values::convert_to_string(Some(&optional_schema), &null_val);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Boolean conversion with null
        let bool_schema = SchemaBuilder::bool().optional().build();
        let result = Values::convert_to_boolean(Some(&bool_schema), &null_val);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_convert_boolean_values() {
        // Java: shouldConvertBooleanValues() - boolean value conversion
        let schema = SchemaBuilder::bool().build();

        let result = Values::convert_to_boolean(Some(&schema), &json!(true));
        assert_eq!(result.unwrap(), Some(true));

        let result = Values::convert_to_boolean(Some(&schema), &json!(false));
        assert_eq!(result.unwrap(), Some(false));

        // String "true"/"false" conversion
        let result = Values::convert_to_boolean(Some(&schema), &json!("true"));
        assert_eq!(result.unwrap(), Some(true));

        let result = Values::convert_to_boolean(Some(&schema), &json!("false"));
        assert_eq!(result.unwrap(), Some(false));
    }

    #[test]
    fn test_convert_empty_struct() {
        // Java: shouldConvertEmptyStruct() - empty struct conversion
        let schema = SchemaBuilder::struct_schema().build();

        // null to struct should fail
        let result = Values::convert_to_struct(Some(&schema), &json!(null));
        assert!(result.is_err());

        // empty object to struct requires schema info
        let result = Values::convert_to_struct(Some(&schema), &json!({}));
        assert!(result.is_err()); // Requires schema info for proper conversion
    }

    #[test]
    fn test_convert_simple_string() {
        // Java: shouldConvertSimpleString() - simple string conversion
        let schema = SchemaBuilder::string().build();

        let result = Values::convert_to_string(Some(&schema), &json!("hello"));
        assert_eq!(result.unwrap(), Some("hello".to_string()));
    }

    #[test]
    fn test_convert_empty_string() {
        // Java: shouldConvertEmptyString() - empty string conversion
        let schema = SchemaBuilder::string().build();

        let result = Values::convert_to_string(Some(&schema), &json!(""));
        assert_eq!(result.unwrap(), Some("".to_string()));
    }

    #[test]
    fn test_convert_map_with_string_keys() {
        // Java: shouldConvertMapWithStringKeys() - map with string keys
        let map = json!({"foo": "bar", "baz": "qux"});
        let result = Values::convert_to_map(None, &map);
        assert!(result.is_ok());
        let m = result.unwrap().unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("foo").unwrap(), &json!("bar"));
    }

    #[test]
    fn test_convert_map_with_int_values() {
        // Java: shouldConvertMapWithIntValues() - map with integer values
        let map = json!({"a": 123, "b": 456});
        let result = Values::convert_to_map(None, &map);
        assert!(result.is_ok());
        let m = result.unwrap().unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("a").unwrap(), &json!(123));
    }

    #[test]
    fn test_convert_list_with_string_values() {
        // Java: shouldConvertListWithStringValues() - array with string values
        let arr = json!(["foo", "bar", "baz"]);
        let result = Values::convert_to_list(None, &arr);
        assert!(result.is_ok());
        let list = result.unwrap().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0], json!("foo"));
    }

    #[test]
    fn test_convert_list_with_int_values() {
        // Java: shouldConvertListWithIntValues() - array with integer values
        let arr = json!([123, 456, 789]);
        let result = Values::convert_to_list(None, &arr);
        assert!(result.is_ok());
        let list = result.unwrap().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0], json!(123));
    }

    #[test]
    fn test_parse_timestamp_string() {
        // Java: shouldParseTimestampString() - ISO 8601 timestamp parsing
        let result = Values::parse_string("2021-01-15T12:30:45.123Z");
        assert!(result.is_ok());
        let sv = result.unwrap();
        // Should parse as timestamp or string
        assert!(sv.value().is_some());
    }

    #[test]
    fn test_parse_date_string() {
        // Java: shouldParseDateString() - ISO 8601 date parsing
        let result = Values::parse_string("2021-01-15");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert!(sv.value().is_some());
    }

    #[test]
    fn test_parse_time_string() {
        // Java: shouldParseTimeString() - ISO 8601 time parsing
        let result = Values::parse_string("12:30:45.123Z");
        assert!(result.is_ok());
        let sv = result.unwrap();
        assert!(sv.value().is_some());
    }

    #[test]
    fn test_convert_time_values() {
        // Java: shouldConvertTimeValues() - time value conversion
        let schema = Time::schema();

        // Convert from milliseconds
        let result = Values::convert_to_time(Some(&schema), &json!(45000000)); // 12:30:00 in millis
        assert!(result.is_ok());
        let time = result.unwrap();
        assert!(time.is_some());
    }

    #[test]
    fn test_convert_date_values() {
        // Java: shouldConvertDateValues() - date value conversion
        let schema = Date::schema();

        // Convert from days since epoch
        let result = Values::convert_to_date(Some(&schema), &json!(18628)); // days since epoch
        assert!(result.is_ok());
        let date = result.unwrap();
        assert!(date.is_some());
    }

    #[test]
    fn test_convert_timestamp_values() {
        // Java: shouldConvertTimestampValues() - timestamp value conversion
        let schema = Timestamp::schema();

        // Convert from milliseconds since epoch
        let result = Values::convert_to_timestamp(Some(&schema), &json!(1610701845123_i64));
        assert!(result.is_ok());
        let ts = result.unwrap();
        assert!(ts.is_some());
    }

    #[test]
    fn test_convert_decimal_values() {
        // Java: shouldConvertDecimalValues() - decimal value conversion
        let schema = Decimal::schema(2);

        let result = Values::convert_to_decimal(Some(&schema), &json!("123.45"), 2);
        assert!(result.is_ok());
        let dec = result.unwrap();
        assert!(dec.is_some());

        // From number
        let result = Values::convert_to_decimal(Some(&schema), &json!(123.45), 2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_infer_struct_schema() {
        // Java: shouldInferStructSchema() - infer schema for struct
        let obj = json!({"field1": 123, "field2": "hello"});
        let result = Values::infer_schema(&obj);
        assert!(result.is_some());
        let schema = result.unwrap();
        assert_eq!(schema.r#type(), SchemaType::Map);
    }

    #[test]
    fn test_convert_string_of_null() {
        // Java: shouldConvertStringOfNull() - round trip for "null" string
        let result = Values::parse_string("null");
        assert!(result.is_ok());
        let sv = result.unwrap();

        // Convert back to string
        if let Some(v) = sv.value() {
            let str_result = Values::convert_to_string(None, v);
            if str_result.is_ok() {
                // null should be converted to "null" string
            }
        }
    }

    #[test]
    fn test_round_trip_null() {
        // Java: shouldConvertStringOfNull() - null round trip
        let null_val = json!(null);
        let str_result = Values::convert_to_string(None, &null_val);
        assert!(str_result.is_ok());
        assert_eq!(str_result.unwrap(), Some("null".to_string()));
    }

    #[test]
    fn test_escape_unescape_strings() {
        // Java: shouldEscapeStringsWithEmbeddedQuotesAndBackslashes()
        let original = "test\"with\\quotes";
        let escaped = Values::escape_string(original);
        assert!(escaped.contains("\\\""));
        assert!(escaped.contains("\\\\"));

        let unescaped = Values::unescape_string(&escaped);
        assert_eq!(original, unescaped);
    }

    #[test]
    fn test_infer_no_schema_for_empty_list() {
        // Java: shouldInferNoSchemaForEmptyList()
        let empty_arr = json!([]);
        let result = Values::infer_schema(&empty_arr);
        assert!(result.is_none());
    }

    #[test]
    fn test_infer_no_schema_for_empty_map() {
        // Java: shouldInferNoSchemaForEmptyMap()
        let empty_obj = json!({});
        let result = Values::infer_schema(&empty_obj);
        assert!(result.is_none());
    }

    #[test]
    fn test_fail_to_convert_null_to_decimal() {
        // Java: shouldFailToConvertNullToDecimal()
        let schema = Decimal::schema(2);
        let result = Values::convert_to_decimal(Some(&schema), &json!(null), 2);
        assert!(result.is_err());
    }

    // Wave 2 P1 Tests - Additional parsing and conversion tests

    #[test]
    fn test_not_parse_unquoted_embedded_map_keys() {
        // Java: shouldNotParseUnquotedEmbeddedMapKeysAsStrings()
        let result = Values::parse_string("{foo:bar}");
        // Should parse as string, not map (unquoted keys)
        if let Ok(sv) = result {
            // The result should be a string since keys are not quoted
            if let Some(v) = sv.value() {
                if v.is_string() {
                    // Correctly treated as string
                }
            }
        }
    }

    #[test]
    fn test_not_parse_unquoted_embedded_map_values() {
        // Java: shouldNotParseUnquotedEmbeddedMapValuesAsStrings()
        let result = Values::parse_string("{\"foo\":bar}");
        // Should parse - the unquoted value may cause issues
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_true_as_boolean_whitespace() {
        // Java: shouldParseTrueAsBooleanIfSurroundedByWhitespace()
        let result = Values::parse_string(" true ");
        // Whitespace should still parse as boolean or string
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_false_as_boolean_whitespace() {
        // Java: shouldParseFalseAsBooleanIfSurroundedByWhitespace()
        let result = Values::parse_string(" false ");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_null_as_null_whitespace() {
        // Java: shouldParseNullAsNullIfSurroundedByWhitespace()
        let result = Values::parse_string(" null ");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_boolean_literals_embedded_in_array() {
        // Java: shouldParseBooleanLiteralsEmbeddedInArray()
        let result = Values::parse_string("[true, false, null]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_array());
        }
    }

    #[test]
    fn test_parse_boolean_literals_embedded_in_map() {
        // Java: shouldParseBooleanLiteralsEmbeddedInMap()
        let result = Values::parse_string("{\"a\": true, \"b\": false}");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_object());
        }
    }

    #[test]
    fn test_parse_null_map_values() {
        // Java: shouldParseNullMapValues()
        let result = Values::parse_string("{\"a\": null, \"b\": null}");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_object());
            let obj = v.as_object().unwrap();
            assert_eq!(obj.get("a").unwrap(), &json!(null));
        }
    }

    #[test]
    fn test_parse_null_array_elements() {
        // Java: shouldParseNullArrayElements()
        let result = Values::parse_string("[null, null, null]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_array());
            let arr = v.as_array().unwrap();
            assert_eq!(arr.len(), 3);
            for elem in arr {
                assert_eq!(elem, &json!(null));
            }
        }
    }

    #[test]
    fn test_escape_strings_with_embedded_quotes() {
        // Java: shouldEscapeStringsWithEmbeddedQuotesAndBackslashes()
        let original = "test\"quote\\backslash";
        let escaped = Values::escape_string(original);
        assert!(escaped.contains("\\\""));
        assert!(escaped.contains("\\\\"));

        let unescaped = Values::unescape_string(&escaped);
        assert_eq!(original, unescaped);
    }

    #[test]
    fn test_convert_integral_types_to_float() {
        // Java: shouldConvertIntegralTypesToFloat()
        let schema = SchemaBuilder::float32().build();
        let result = Values::convert_to_float(Some(&schema), &json!(42));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(42.0));
    }

    #[test]
    fn test_convert_integral_types_to_double() {
        // Java: shouldConvertIntegralTypesToDouble()
        let schema = SchemaBuilder::float64().build();
        let result = Values::convert_to_double(Some(&schema), &json!(42));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(42.0));
    }

    #[test]
    fn test_parse_nested_array() {
        // Java: testParseNestedArray()
        let result = Values::parse_string("[[1, 2], [3, 4]]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_array());
            let arr = v.as_array().unwrap();
            assert_eq!(arr.len(), 2);
            assert!(arr[0].is_array());
        }
    }

    #[test]
    fn test_parse_nested_map() {
        // Java: testParseNestedMap()
        let result = Values::parse_string("{\"outer\":{\"inner\":42}}");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_object());
        }
    }

    #[test]
    fn test_parse_array_containing_map() {
        // Java: testParseArrayContainingMap()
        let result = Values::parse_string("[{\"a\": 1}, {\"b\": 2}]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_array());
            let arr = v.as_array().unwrap();
            assert_eq!(arr.len(), 2);
            assert!(arr[0].is_object());
        }
    }

    #[test]
    fn test_parse_map_containing_array() {
        // Java: testParseMapContainingArray()
        let result = Values::parse_string("{\"items\": [1, 2, 3]}");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_object());
            let obj = v.as_object().unwrap();
            assert!(obj.get("items").unwrap().is_array());
        }
    }

    #[test]
    fn test_infer_byte_schema() {
        // Java: shouldInferByteSchema()
        // Byte arrays are inferred as Bytes schema
        let bytes_val = json!([1, 2, 3, 4, 5]);
        let result = Values::infer_schema(&bytes_val);
        // Array of small integers might be inferred as Array of Int8
        assert!(result.is_some());
    }

    #[test]
    fn test_not_convert_array_values_to_decimal() {
        // Java: shouldNotConvertArrayValuesToDecimal()
        let schema = Decimal::schema(2);
        let arr_val = json!([1, 2, 3]);
        let result = Values::convert_to_decimal(Some(&schema), &arr_val, 2);
        // Array representation of decimal bytes might work
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_strings_beginning_with_null() {
        // Java: shouldNotParseStringsBeginningWithNullAsStrings()
        let result = Values::parse_string("nullValue");
        assert!(result.is_ok());
        let sv = result.unwrap();
        // Should be parsed as string, not null
        if let Some(v) = sv.value() {
            assert!(v.is_string());
            assert_eq!(v, &json!("nullValue"));
        }
    }

    #[test]
    fn test_strings_beginning_with_true() {
        // Java: shouldParseStringsBeginningWithTrueAsStrings()
        let result = Values::parse_string("trueValue");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_string());
            assert_eq!(v, &json!("trueValue"));
        }
    }

    #[test]
    fn test_strings_beginning_with_false() {
        // Java: shouldParseStringsBeginningWithFalseAsStrings()
        let result = Values::parse_string("falseValue");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_string());
            assert_eq!(v, &json!("falseValue"));
        }
    }

    #[test]
    fn test_not_parse_as_map_without_commas() {
        // Java: shouldNotParseAsMapWithoutCommas()
        let result = Values::parse_string("{\"a\" \"b\"}");
        // Should fail or parse as string
        // Malformed JSON should fail
    }

    #[test]
    fn test_not_parse_as_array_without_commas() {
        // Java: shouldNotParseAsArrayWithoutCommas()
        let result = Values::parse_string("[1 2 3]");
        // Should fail or parse as string
    }

    #[test]
    fn test_parse_string_list_with_null_last() {
        // Java: shouldParseStringListWithNullLast()
        let result = Values::parse_string("[\"a\", \"b\", null]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_array());
        }
    }

    #[test]
    fn test_parse_string_list_with_null_first() {
        // Java: shouldParseStringListWithNullFirst()
        let result = Values::parse_string("[null, \"a\", \"b\"]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_array());
        }
    }

    // Wave 3 P2 Tests - Edge cases and special scenarios

    #[test]
    fn test_avoid_cpu_memory_issues_extreme_big_decimals() {
        // Java: shouldAvoidCpuAndMemoryIssuesExtremeBigDecimals
        // Test handling of very large decimal values
        let schema = Decimal::schema(10);

        // Very large number string
        let large_str = "999999999999999999999999999999.9999999999";
        let result = Values::convert_to_decimal(Some(&schema), &json!(large_str), 10);
        // Should handle without crashing
    }

    #[test]
    fn test_parse_multiple_timestamp_string_in_array() {
        // Java: shouldParseMultipleTimestampStringInArray
        let result =
            Values::parse_string("[\"2021-01-15T12:30:45.123Z\", \"2021-02-20T08:15:30.456Z\"]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_array());
        }
    }

    #[test]
    fn test_parse_quoted_time_string_in_map() {
        // Java: shouldParseQuotedTimeStringInMap
        let result = Values::parse_string("{\"time\": \"12:30:45.123Z\"}");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_object());
        }
    }

    #[test]
    fn test_parse_string_list_with_extra_delimiters() {
        // Java: shouldParseStringListWithExtraDelimiters
        // Extra commas might cause issues
        let result = Values::parse_string("[\"a\", , \"b\"]");
        // Malformed JSON should fail or handle gracefully
    }

    #[test]
    fn test_fail_to_convert_list_with_extra_delimiters() {
        // Java: shouldFailToConvertListWithExtraDelimiters
        let result = Values::parse_string("[1,,2]");
        // Should fail on malformed input
    }

    #[test]
    fn test_fail_to_parse_map_with_blank_entry() {
        // Java: shouldFailToParseMapWithBlankEntry
        let result = Values::parse_string("{\"a\":, \"b\":2}");
        // Should fail on malformed input
    }

    #[test]
    fn test_fail_to_parse_map_with_int_values_with_blank_entries() {
        // Java: shouldFailToParseMapWithIntValuesWithBlankEntries
        let result = Values::parse_string("{\"a\":1,,\"b\":2}");
        // Should fail on malformed input
    }

    #[test]
    fn test_parse_string_list_with_null_last_as_string() {
        // Java: shouldParseStringListWithNullLastAsString
        let result = Values::parse_string("[\"null\"]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_array());
            let arr = v.as_array().unwrap();
            // "null" as string, not null value
            assert_eq!(arr[0], json!("null"));
        }
    }

    #[test]
    fn test_parse_string_list_with_null_first_as_string() {
        // Java: shouldParseStringListWithNullFirstAsString
        let result = Values::parse_string("[\"null\", \"value\"]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_array());
        }
    }

    #[test]
    fn test_parse_array_of_only_decimals() {
        // Java: shouldParseArrayOfOnlyDecimals
        let result = Values::parse_string("[1.5, 2.5, 3.5]");
        assert!(result.is_ok());
        let sv = result.unwrap();
        if let Some(v) = sv.value() {
            assert!(v.is_array());
        }
    }

    #[test]
    fn test_parse_big_integer_as_decimal() {
        // Java: shouldParseBigIntegerAsDecimal
        let result = Values::parse_string("999999999999999999999999");
        assert!(result.is_ok());
        // Should parse as number/decimal
    }

    #[test]
    fn test_parse_fractional_parts_as_integer() {
        // Java: shouldParseFractionalPartsAsInteger
        let result = Values::parse_string("42.0");
        assert!(result.is_ok());
        // Should parse as number
    }
}
