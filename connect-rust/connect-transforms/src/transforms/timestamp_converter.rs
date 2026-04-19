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

//! TimestampConverter Transformation - converts timestamps between various formats.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.TimestampConverter` in Java.
//!
//! TimestampConverter allows you to convert timestamps between different formats:
//! - Unix epoch timestamps (with configurable precision)
//! - String timestamps (with configurable format pattern)
//! - Kafka Connect Date, Time, and Timestamp logical types
//!
//! # Configuration
//!
//! * `field` - The field containing the timestamp. If empty, the entire value is converted.
//!   Default: empty (whole value conversion).
//! * `target.type` - The desired output type. Valid values: "string", "unix", "Date", "Time", "Timestamp".
//!   Required.
//! * `format` - A SimpleDateFormat-compatible pattern for string conversion. Required when
//!   target.type is "string" or when converting from string.
//! * `unix.precision` - The precision for Unix timestamps. Valid values: "seconds", "milliseconds",
//!   "microseconds", "nanoseconds". Default: "milliseconds".
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::timestamp_converter::{TimestampConverter, TimestampConverterTarget};
//! use connect_api::transforms::Transformation;
//! use connect_api::source::SourceRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Create a TimestampConverter to convert a timestamp field to string format
//! let mut transform = TimestampConverter::new(TimestampConverterTarget::Value);
//! let mut configs = HashMap::new();
//! configs.insert("field".to_string(), json!("ts"));
//! configs.insert("target.type".to_string(), json!("string"));
//! configs.insert("format".to_string(), json!("yyyy-MM-dd HH:mm:ss"));
//! transform.configure(configs);
//!
//! // Create a record with Unix timestamp
//! let record = SourceRecord::new(
//!     HashMap::new(),
//!     HashMap::new(),
//!     "test-topic".to_string(),
//!     None,
//!     None,
//!     json!({"ts": 1609459200000, "name": "test"}),
//! );
//!
//! // Transform will convert timestamp to string
//! let result = transform.transform(record).unwrap().unwrap();
//! assert!(result.value()["ts"].is_string());
//! ```

use crate::transforms::util::{Requirements, SimpleConfig};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use common_trait::cache::{Cache, SynchronizedCache};
use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType, ConfigKeyDef};
use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

// Configuration keys
const FIELD_CONFIG: &str = "field";
const FIELD_DEFAULT: &str = "";

const TARGET_TYPE_CONFIG: &str = "target.type";

const FORMAT_CONFIG: &str = "format";
const FORMAT_DEFAULT: &str = "";

const UNIX_PRECISION_CONFIG: &str = "unix.precision";
const UNIX_PRECISION_DEFAULT: &str = "milliseconds";

const REPLACE_NULL_WITH_DEFAULT_CONFIG: &str = "replace.null.with.default";
const REPLACE_NULL_WITH_DEFAULT_DEFAULT: bool = true;

/// Purpose string for error messages.
const PURPOSE: &str = "convert timestamp types";

/// Overview documentation for TimestampConverter.
pub const OVERVIEW_DOC: &str = "Convert timestamps between different formats such as Unix epoch, \
    strings, and Connect's Date/Time/Timestamp types. Use the concrete transformation type designed \
    for the record key (TimestampConverter.Key) or value (TimestampConverter.Value).";

/// Target type enum for timestamp conversion output.
///
/// This corresponds to the target type options in Java's TimestampConverter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetType {
    /// String representation with configurable format.
    String,
    /// Unix epoch timestamp as a Long value.
    Unix,
    /// Date logical type (year, month, day only).
    Date,
    /// Time logical type (hour, minute, second, millisecond only).
    Time,
    /// Timestamp logical type (full date and time).
    Timestamp,
}

impl TargetType {
    /// Parses a target type from a string.
    ///
    /// # Arguments
    /// * `s` - The string representation of the target type
    ///
    /// # Returns
    /// Returns the parsed TargetType or an error.
    pub fn from_string(s: &str) -> Result<TargetType, ConnectError> {
        match s.to_lowercase().as_str() {
            "string" => Ok(TargetType::String),
            "unix" => Ok(TargetType::Unix),
            "date" => Ok(TargetType::Date),
            "time" => Ok(TargetType::Time),
            "timestamp" => Ok(TargetType::Timestamp),
            _ => Err(ConnectError::data(format!(
                "Invalid target type '{}'. Valid values are: string, unix, Date, Time, Timestamp",
                s
            ))),
        }
    }

    /// Returns the string representation of this target type.
    pub fn as_str(&self) -> &'static str {
        match self {
            TargetType::String => "string",
            TargetType::Unix => "unix",
            TargetType::Date => "Date",
            TargetType::Time => "Time",
            TargetType::Timestamp => "Timestamp",
        }
    }
}

impl fmt::Display for TargetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Unix precision enum for Unix timestamp conversion.
///
/// This corresponds to the unix.precision configuration in Java's TimestampConverter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnixPrecision {
    /// Seconds since epoch.
    Seconds,
    /// Milliseconds since epoch.
    Milliseconds,
    /// Microseconds since epoch.
    Microseconds,
    /// Nanoseconds since epoch.
    Nanoseconds,
}

impl UnixPrecision {
    /// Parses a unix precision from a string.
    ///
    /// # Arguments
    /// * `s` - The string representation of the precision
    ///
    /// # Returns
    /// Returns the parsed UnixPrecision or an error.
    pub fn from_string(s: &str) -> Result<UnixPrecision, ConnectError> {
        match s.to_lowercase().as_str() {
            "seconds" => Ok(UnixPrecision::Seconds),
            "milliseconds" => Ok(UnixPrecision::Milliseconds),
            "microseconds" => Ok(UnixPrecision::Microseconds),
            "nanoseconds" => Ok(UnixPrecision::Nanoseconds),
            _ => Err(ConnectError::data(format!(
                "Invalid unix precision '{}'. Valid values are: seconds, milliseconds, microseconds, nanoseconds",
                s
            ))),
        }
    }

    /// Returns the string representation of this precision.
    pub fn as_str(&self) -> &'static str {
        match self {
            UnixPrecision::Seconds => "seconds",
            UnixPrecision::Milliseconds => "milliseconds",
            UnixPrecision::Microseconds => "microseconds",
            UnixPrecision::Nanoseconds => "nanoseconds",
        }
    }
}

impl fmt::Display for UnixPrecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// TimestampConverterTarget enum representing Key or Value operation.
///
/// This replaces Java's inner classes `TimestampConverter.Key` and `TimestampConverter.Value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampConverterTarget {
    /// Operate on record key.
    Key,
    /// Operate on record value.
    Value,
}

/// TimestampConverter transformation that converts timestamps between formats.
///
/// This corresponds to `org.apache.kafka.connect.transforms.TimestampConverter<R>` in Java.
///
/// The TimestampConverter transformation allows you to:
/// - Convert between Unix epoch, string, Date, Time, and Timestamp formats
/// - Convert individual fields or the entire record value
/// - Configure Unix timestamp precision (seconds, milliseconds, microseconds, nanoseconds)
/// - Use custom date/time format patterns for string conversions
///
/// # Configuration
///
/// - `field`: The field to convert (empty for whole-value conversion)
/// - `target.type`: The desired output type (required)
/// - `format`: SimpleDateFormat pattern for string conversion
/// - `unix.precision`: Precision for Unix timestamps (default: milliseconds)
///
/// # Thread Safety
///
/// Thread-safe using SynchronizedCache for schema caching.
pub struct TimestampConverter {
    /// The target (Key or Value).
    target: TimestampConverterTarget,
    /// The field containing the timestamp (empty for whole value).
    field: String,
    /// The target type for conversion.
    target_type: TargetType,
    /// The format pattern for string conversion.
    format: Option<String>,
    /// The precision for Unix timestamps.
    unix_precision: UnixPrecision,
    /// Whether to replace null values with default values.
    replace_null_with_default: bool,
    /// Schema cache for storing updated schemas.
    schema_cache: SynchronizedCache<String, String>,
}

impl TimestampConverter {
    /// Creates a new TimestampConverter for the specified target.
    ///
    /// # Arguments
    /// * `target` - The target (Key or Value)
    pub fn new(target: TimestampConverterTarget) -> Self {
        TimestampConverter {
            target,
            field: FIELD_DEFAULT.to_string(),
            target_type: TargetType::Timestamp,
            format: None,
            unix_precision: UnixPrecision::Milliseconds,
            replace_null_with_default: REPLACE_NULL_WITH_DEFAULT_DEFAULT,
            schema_cache: SynchronizedCache::with_default_capacity(),
        }
    }

    /// Returns the version of this transformation.
    pub fn version() -> &'static str {
        "3.9.0"
    }

    /// Returns the configuration definition for this transformation.
    pub fn config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                FIELD_CONFIG,
                ConfigDefType::String,
                Some(Value::String(FIELD_DEFAULT.to_string())),
                ConfigDefImportance::High,
                "The field containing the timestamp, or empty if the entire value is a timestamp",
            )
            .define(
                TARGET_TYPE_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The desired timestamp representation: string, unix, Date, Time, or Timestamp",
            )
            .define(
                FORMAT_CONFIG,
                ConfigDefType::String,
                Some(Value::String(FORMAT_DEFAULT.to_string())),
                ConfigDefImportance::High,
                "A SimpleDateFormat-compatible format string for the target type 'string' or source type 'string'. \
                 Required when converting to/from strings.",
            )
            .define(
                UNIX_PRECISION_CONFIG,
                ConfigDefType::String,
                Some(Value::String(UNIX_PRECISION_DEFAULT.to_string())),
                ConfigDefImportance::Medium,
                "The desired Unix timestamp precision: seconds, milliseconds, microseconds, or nanoseconds",
            )
            .define(
                REPLACE_NULL_WITH_DEFAULT_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(REPLACE_NULL_WITH_DEFAULT_DEFAULT)),
                ConfigDefImportance::Medium,
                "Whether to replace null values with the default value from the schema",
            )
            .build()
    }

    /// Returns the target of this transformation.
    pub fn target(&self) -> TimestampConverterTarget {
        self.target
    }

    /// Returns the field being converted.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Returns the target type.
    pub fn target_type(&self) -> TargetType {
        self.target_type
    }

    /// Returns the format pattern.
    pub fn format(&self) -> Option<&str> {
        self.format.as_deref()
    }

    /// Returns the Unix precision.
    pub fn unix_precision(&self) -> UnixPrecision {
        self.unix_precision
    }

    /// Converts a SimpleDateFormat pattern to chrono format.
    ///
    /// SimpleDateFormat uses different format specifiers than chrono:
    /// - yyyy -> %Y (year)
    /// - MM -> %m (month)
    /// - dd -> %d (day)
    /// - HH -> %H (hour 0-23)
    /// - mm -> %M (minute)
    /// - ss -> %S (second)
    /// - SSS -> %3f (milliseconds, 3 digits)
    /// - S -> %f (fractional seconds)
    fn convert_simple_date_format_to_chrono(pattern: &str) -> String {
        let mut result = String::new();
        let mut chars = pattern.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                'y' => {
                    // yyyy -> %Y
                    let count = Self::count_repeated_chars(&mut chars, 'y', c);
                    if count >= 4 {
                        result.push_str("%Y");
                    } else if count >= 2 {
                        result.push_str("%y");
                    } else {
                        result.push_str("%Y");
                    }
                }
                'M' => {
                    let count = Self::count_repeated_chars(&mut chars, 'M', c);
                    if count >= 4 {
                        // MMMM - full month name (not supported in chrono basic)
                        result.push_str("%B");
                    } else if count >= 3 {
                        // MMM - abbreviated month name
                        result.push_str("%b");
                    } else if count >= 2 {
                        result.push_str("%m");
                    } else {
                        result.push_str("%-m");
                    }
                }
                'd' => {
                    let count = Self::count_repeated_chars(&mut chars, 'd', c);
                    if count >= 2 {
                        result.push_str("%d");
                    } else {
                        result.push_str("%-d");
                    }
                }
                'H' => {
                    let count = Self::count_repeated_chars(&mut chars, 'H', c);
                    if count >= 2 {
                        result.push_str("%H");
                    } else {
                        result.push_str("%-H");
                    }
                }
                'h' => {
                    let count = Self::count_repeated_chars(&mut chars, 'h', c);
                    if count >= 2 {
                        result.push_str("%I");
                    } else {
                        result.push_str("%-I");
                    }
                }
                'm' => {
                    let count = Self::count_repeated_chars(&mut chars, 'm', c);
                    if count >= 2 {
                        result.push_str("%M");
                    } else {
                        result.push_str("%-M");
                    }
                }
                's' => {
                    let count = Self::count_repeated_chars(&mut chars, 's', c);
                    if count >= 2 {
                        result.push_str("%S");
                    } else {
                        result.push_str("%-S");
                    }
                }
                'S' => {
                    let count = Self::count_repeated_chars(&mut chars, 'S', c);
                    if count >= 3 {
                        result.push_str("%3f");
                    } else if count >= 2 {
                        result.push_str("%2f");
                    } else {
                        result.push_str("%f");
                    }
                }
                'a' => {
                    result.push_str("%P");
                }
                'z' | 'Z' => {
                    result.push_str("%Z");
                }
                '\'' => {
                    // Handle quoted text in SimpleDateFormat
                    result.push('\'');
                    while let Some(&next) = chars.peek() {
                        if next == '\'' {
                            chars.next();
                            result.push('\'');
                            break;
                        }
                        chars.next();
                        result.push(next);
                    }
                }
                _ => {
                    result.push(c);
                }
            }
        }

        result
    }

    /// Counts repeated characters starting from the current position.
    fn count_repeated_chars(
        chars: &mut std::iter::Peekable<std::str::Chars>,
        target: char,
        _first: char,
    ) -> usize {
        let mut count = 1;
        while let Some(&next) = chars.peek() {
            if next == target {
                chars.next();
                count += 1;
            } else {
                break;
            }
        }
        count
    }

    /// Converts a value to a DateTime<Utc> (raw timestamp).
    ///
    /// This corresponds to `TimestampTranslator.toRaw()` in Java.
    ///
    /// # Arguments
    /// * `value` - The input value to convert
    /// * `format` - The format pattern (for string conversion)
    /// * `unix_precision` - The Unix precision (for unix conversion)
    ///
    /// # Returns
    /// Returns a DateTime<Utc> or an error.
    fn to_raw(
        value: &Value,
        format: Option<&str>,
        unix_precision: UnixPrecision,
    ) -> Result<DateTime<Utc>, ConnectError> {
        if value.is_null() {
            return Err(ConnectError::data("Cannot convert null timestamp"));
        }

        // Determine the source type and convert accordingly
        if value.is_string() {
            // String to DateTime using format
            let format_str = format.ok_or_else(|| {
                ConnectError::data(
                    "Format pattern is required when converting from string timestamps",
                )
            })?;
            let chrono_format = Self::convert_simple_date_format_to_chrono(format_str);
            let str_val = value.as_str().unwrap();

            // Try to parse with the format
            let parsed = NaiveDateTime::parse_from_str(str_val, &chrono_format).map_err(|e| {
                ConnectError::data(format!(
                    "Failed to parse timestamp string '{}' with format '{}': {}",
                    str_val, format_str, e
                ))
            })?;

            return Ok(DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc));
        }

        if value.is_i64() || value.is_u64() {
            // Unix timestamp conversion
            let unix_val = if value.is_i64() {
                value.as_i64().unwrap()
            } else {
                value.as_u64().unwrap() as i64
            };

            let dt = match unix_precision {
                UnixPrecision::Seconds => {
                    Utc.timestamp_opt(unix_val, 0).single().ok_or_else(|| {
                        ConnectError::data(format!(
                            "Invalid Unix timestamp (seconds): {}",
                            unix_val
                        ))
                    })?
                }
                UnixPrecision::Milliseconds => {
                    Utc.timestamp_millis_opt(unix_val).single().ok_or_else(|| {
                        ConnectError::data(format!(
                            "Invalid Unix timestamp (milliseconds): {}",
                            unix_val
                        ))
                    })?
                }
                UnixPrecision::Microseconds => {
                    let secs = unix_val / 1_000_000;
                    let nanos = (unix_val % 1_000_000) * 1_000;
                    Utc.timestamp_opt(secs, nanos as u32)
                        .single()
                        .ok_or_else(|| {
                            ConnectError::data(format!(
                                "Invalid Unix timestamp (microseconds): {}",
                                unix_val
                            ))
                        })?
                }
                UnixPrecision::Nanoseconds => {
                    let secs = unix_val / 1_000_000_000;
                    let nanos = (unix_val % 1_000_000_000) as u32;
                    Utc.timestamp_opt(secs, nanos).single().ok_or_else(|| {
                        ConnectError::data(format!(
                            "Invalid Unix timestamp (nanoseconds): {}",
                            unix_val
                        ))
                    })?
                }
            };

            return Ok(dt);
        }

        if value.is_f64() {
            // Float could be Unix timestamp (less common)
            let float_val = value.as_f64().unwrap();
            let unix_val = float_val as i64;
            return Self::to_raw(&Value::Number(unix_val.into()), format, unix_precision);
        }

        // Check if value is an object that might contain timestamp data
        if value.is_object() {
            // In our JSON representation, a timestamp might be stored as an object
            // with specific fields. For now, we return an error.
            return Err(ConnectError::data(format!(
                "Cannot convert object to timestamp. Expected string, number, or timestamp value."
            )));
        }

        Err(ConnectError::data(format!(
            "Unsupported timestamp type: {:?}",
            value
        )))
    }

    /// Converts a DateTime<Utc> to the target type.
    ///
    /// This corresponds to `TimestampTranslator.toType()` in Java.
    ///
    /// # Arguments
    /// * `dt` - The DateTime to convert
    /// * `target_type` - The target type
    /// * `format` - The format pattern (for string conversion)
    /// * `unix_precision` - The Unix precision (for unix conversion)
    ///
    /// # Returns
    /// Returns a JSON Value representing the converted timestamp.
    fn to_type(
        dt: DateTime<Utc>,
        target_type: TargetType,
        format: Option<&str>,
        unix_precision: UnixPrecision,
    ) -> Result<Value, ConnectError> {
        match target_type {
            TargetType::String => {
                let format_str = format.ok_or_else(|| {
                    ConnectError::data("Format pattern is required when converting to string type")
                })?;
                let chrono_format = Self::convert_simple_date_format_to_chrono(format_str);
                let formatted = dt.format(&chrono_format).to_string();
                Ok(Value::String(formatted))
            }
            TargetType::Unix => {
                let unix_val = match unix_precision {
                    UnixPrecision::Seconds => dt.timestamp(),
                    UnixPrecision::Milliseconds => dt.timestamp_millis(),
                    UnixPrecision::Microseconds => dt.timestamp_micros(),
                    UnixPrecision::Nanoseconds => dt.timestamp_nanos_opt().unwrap_or_else(|| {
                        // Fallback for timestamps that don't fit in i64 nanoseconds
                        // Use microseconds as an approximation
                        dt.timestamp_micros() * 1000
                    }),
                };
                Ok(Value::Number(unix_val.into()))
            }
            TargetType::Date => {
                // Date: only year, month, day (no time component)
                // Return as days since epoch (Kafka Connect Date representation)
                let naive_date = dt.date_naive();
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = (naive_date - epoch).num_days() as i64;
                Ok(Value::Number(days.into()))
            }
            TargetType::Time => {
                // Time: only hour, minute, second, millisecond (no date component)
                // Return as milliseconds since midnight (Kafka Connect Time representation)
                let time = dt.time();
                let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                let millis = (time - midnight).num_milliseconds() as i64;
                Ok(Value::Number(millis.into()))
            }
            TargetType::Timestamp => {
                // Timestamp: full timestamp as milliseconds since epoch
                Ok(Value::Number(dt.timestamp_millis().into()))
            }
        }
    }

    /// Converts a timestamp value to the target type.
    ///
    /// This corresponds to `TimestampConverter.convertTimestamp()` in Java.
    ///
    /// # Arguments
    /// * `value` - The input timestamp value
    ///
    /// # Returns
    /// Returns the converted value.
    fn convert_timestamp(&self, value: &Value) -> Result<Value, ConnectError> {
        // First, convert to raw DateTime
        let dt = Self::to_raw(value, self.format.as_deref(), self.unix_precision)?;

        // Then, convert to target type
        Self::to_type(
            dt,
            self.target_type,
            self.format.as_deref(),
            self.unix_precision,
        )
    }

    /// Applies the transformation in schemaless mode.
    fn apply_schemaless(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        let operating_value = match self.target {
            TimestampConverterTarget::Key => record.key(),
            TimestampConverterTarget::Value => Some(record.value()),
        };

        if operating_value.is_none() {
            return Ok(Some(record));
        }

        let value = operating_value.unwrap();
        if value.is_null() {
            return Ok(Some(record));
        }

        // Whole value conversion
        if self.field.is_empty() {
            let converted = self.convert_timestamp(value)?;
            return self.new_record(record, None, converted);
        }

        // Field-level conversion
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Map objects supported in absence of schema for [{}], found: {}",
                PURPOSE,
                Requirements::null_safe_type_name(value)
            )));
        }

        let value_map = Requirements::require_map(value, PURPOSE)?;
        let mut updated_map = HashMap::new();

        for (field_name, field_value) in value_map.iter() {
            if field_name == &self.field {
                let converted = self.convert_timestamp(field_value)?;
                updated_map.insert(field_name.clone(), converted);
            } else {
                updated_map.insert(field_name.clone(), field_value.clone());
            }
        }

        let updated_value =
            serde_json::to_value(updated_map).unwrap_or(Value::Object(serde_json::Map::new()));

        self.new_record(record, None, updated_value)
    }

    /// Applies the transformation in schema-aware mode.
    fn apply_with_schema(
        &self,
        record: SourceRecord,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        let operating_value = match self.target {
            TimestampConverterTarget::Key => record.key(),
            TimestampConverterTarget::Value => Some(record.value()),
        };

        if operating_value.is_none() {
            return Ok(Some(record));
        }

        let value = operating_value.unwrap();
        if value.is_null() {
            return Ok(Some(record));
        }

        let original_schema = match self.target {
            TimestampConverterTarget::Key => record.key_schema(),
            TimestampConverterTarget::Value => record.value_schema(),
        };

        // Whole value conversion
        if self.field.is_empty() {
            let updated_schema = self.get_target_schema_for_type();
            let converted = self.convert_timestamp(value)?;
            return self.new_record(record, Some(updated_schema), converted);
        }

        // Field-level conversion
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Struct objects supported for [{} with schema], found: {}",
                PURPOSE,
                Requirements::null_safe_type_name(value)
            )));
        }

        // Get or build updated schema
        let updated_schema = self.get_or_build_schema(original_schema);

        let value_obj = value.as_object().unwrap();
        let mut updated_obj = serde_json::Map::new();

        for (field_name, field_value) in value_obj.iter() {
            if field_name == &self.field {
                if field_value.is_null() && self.replace_null_with_default {
                    // Use default value or null
                    updated_obj.insert(field_name.clone(), Value::Null);
                } else {
                    let converted = self.convert_timestamp(field_value)?;
                    updated_obj.insert(field_name.clone(), converted);
                }
            } else {
                updated_obj.insert(field_name.clone(), field_value.clone());
            }
        }

        let updated_value = Value::Object(updated_obj);
        self.new_record(record, Some(updated_schema), updated_value)
    }

    /// Gets the target schema for a specific target type.
    fn get_target_schema_for_type(&self) -> String {
        match self.target_type {
            TargetType::String => "string".to_string(),
            TargetType::Unix => "int64".to_string(),
            TargetType::Date => "date".to_string(),
            TargetType::Time => "time".to_string(),
            TargetType::Timestamp => "timestamp".to_string(),
        }
    }

    /// Gets or builds the updated schema from the cache.
    fn get_or_build_schema(&self, original_schema: Option<&str>) -> String {
        let schema_key = original_schema.unwrap_or("schemaless").to_string();

        if let Some(cached) = self.schema_cache.get(&schema_key) {
            return cached;
        }

        // Build updated schema with the target field converted
        let target_field_schema = self.get_target_schema_for_type();
        let updated_schema = format!("{}:{}:{}", schema_key, self.field, target_field_schema);

        self.schema_cache
            .put(schema_key.clone(), updated_schema.clone());
        updated_schema
    }

    /// Creates a new record with the updated value and schema.
    fn new_record(
        &self,
        record: SourceRecord,
        _updated_schema: Option<String>,
        updated_value: Value,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        match self.target {
            TimestampConverterTarget::Key => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                Some(updated_value),
                record.value().clone(),
            ))),
            TimestampConverterTarget::Value => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                record.key().cloned(),
                updated_value,
            ))),
        }
    }
}

impl Default for TimestampConverter {
    fn default() -> Self {
        TimestampConverter::new(TimestampConverterTarget::Value)
    }
}

impl Transformation<SourceRecord> for TimestampConverter {
    /// Configures this transformation with the given configuration map.
    fn configure(&mut self, configs: HashMap<String, Value>) {
        let config_def = Self::config_def();
        let simple_config = SimpleConfig::new(config_def, configs)
            .expect("Failed to parse configuration for TimestampConverter");

        // Parse field
        self.field = simple_config
            .get_string(FIELD_CONFIG)
            .map(|s| s.to_string())
            .unwrap_or_else(|| FIELD_DEFAULT.to_string());

        // Parse target.type (required)
        let target_type_str = simple_config
            .get_string(TARGET_TYPE_CONFIG)
            .expect("target.type is required");
        self.target_type =
            TargetType::from_string(target_type_str).expect("Invalid target.type value");

        // Parse format
        let format_str = simple_config.get_string(FORMAT_CONFIG);
        self.format = if format_str.is_some() && !format_str.unwrap().is_empty() {
            Some(format_str.unwrap().to_string())
        } else {
            None
        };

        // Validate format requirement for string type
        if self.target_type == TargetType::String && self.format.is_none() {
            panic!("Format pattern is required when target.type is 'string'");
        }

        // Parse unix.precision
        let precision_str = simple_config
            .get_string(UNIX_PRECISION_CONFIG)
            .unwrap_or(UNIX_PRECISION_DEFAULT);
        self.unix_precision =
            UnixPrecision::from_string(precision_str).expect("Invalid unix.precision value");

        // Parse replace.null.with.default
        self.replace_null_with_default = simple_config
            .get_bool(REPLACE_NULL_WITH_DEFAULT_CONFIG)
            .unwrap_or(REPLACE_NULL_WITH_DEFAULT_DEFAULT);
    }

    /// Transforms the given record by converting timestamps.
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        let operating_value = match self.target {
            TimestampConverterTarget::Key => record.key(),
            TimestampConverterTarget::Value => Some(record.value()),
        };

        if operating_value.is_none() || operating_value.unwrap().is_null() {
            return Ok(Some(record));
        }

        let has_schema = match self.target {
            TimestampConverterTarget::Key => record.key_schema().is_some(),
            TimestampConverterTarget::Value => record.value_schema().is_some(),
        };

        if has_schema {
            self.apply_with_schema(record)
        } else {
            self.apply_schemaless(record)
        }
    }

    /// Closes this transformation and releases any resources.
    fn close(&mut self) {
        self.field = FIELD_DEFAULT.to_string();
        self.format = None;
        self.unix_precision = UnixPrecision::Milliseconds;
        self.replace_null_with_default = REPLACE_NULL_WITH_DEFAULT_DEFAULT;
    }
}

impl fmt::Display for TimestampConverter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TimestampConverter{{target={}, field='{}', target_type={}, format={}, unix_precision={}, replace_null_with_default={}}}",
            match self.target {
                TimestampConverterTarget::Key => "Key",
                TimestampConverterTarget::Value => "Value",
            },
            self.field,
            self.target_type,
            self.format.as_deref().unwrap_or("None"),
            self.unix_precision,
            self.replace_null_with_default
        )
    }
}

impl fmt::Debug for TimestampConverter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimestampConverter")
            .field("target", &self.target)
            .field("field", &self.field)
            .field("target_type", &self.target_type)
            .field("format", &self.format)
            .field("unix_precision", &self.unix_precision)
            .field("replace_null_with_default", &self.replace_null_with_default)
            .field("schema_cache", &"<SynchronizedCache>")
            .finish()
    }
}

/// TimestampConverter.Key - convenience constructor for Key target.
pub fn timestamp_converter_key() -> TimestampConverter {
    TimestampConverter::new(TimestampConverterTarget::Key)
}

/// TimestampConverter.Value - convenience constructor for Value target.
pub fn timestamp_converter_value() -> TimestampConverter {
    TimestampConverter::new(TimestampConverterTarget::Value)
}
