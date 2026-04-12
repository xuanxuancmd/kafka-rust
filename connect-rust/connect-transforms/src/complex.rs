//! Complex transformations
//!
//! This module provides complex built-in transformation implementations.

use connect_api::connector_types::ConnectRecord;
use connect_api::data::{ConnectSchema, Field, Schema, SchemaBuilder, Struct, Type};
use connect_api::Closeable;
use connect_api::ConfigDef;
use connect_api::ConfigValue;
use connect_api::Configurable;
use connect_api::Transformation;
use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

/// TimestampConverter transformation
///
/// Converts timestamp fields between different formats.
pub struct TimestampConverter<R: ConnectRecord<R>> {
    field: String,
    target_type: String,
    format: String,
    unix_precision: String,
    replace_null_with_default: bool,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> TimestampConverter<R> {
    pub fn new() -> Self {
        Self {
            field: String::new(),
            target_type: String::new(),
            format: String::new(),
            unix_precision: String::new(),
            replace_null_with_default: false,
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for TimestampConverter<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(field) = configs.get("field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.field = f.clone();
            }
        }

        if let Some(target) = configs.get("target.type") {
            if let Some(t) = target.downcast_ref::<String>() {
                self.target_type = t.clone();
            }
        }

        if let Some(format) = configs.get("format") {
            if let Some(f) = format.downcast_ref::<String>() {
                self.format = f.clone();
            }
        }

        if let Some(precision) = configs.get("unix.precision") {
            if let Some(p) = precision.downcast_ref::<String>() {
                self.unix_precision = p.clone();
            }
        }

        if let Some(replace) = configs.get("replace.null.with.default") {
            if let Some(r) = replace.downcast_ref::<bool>() {
                self.replace_null_with_default = *r;
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for TimestampConverter<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.field.clear();
        self.target_type.clear();
        self.format.clear();
        self.unix_precision.clear();
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for TimestampConverter<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Schema, Struct};
        use std::sync::Arc;

        let value_schema = record.value_schema();
        let value = record.value();

        if value_schema.is_none() {
            // Schemaless
            self.apply_schemaless(record)
        } else {
            // Schema-based
            self.apply_with_schema(record)
        }
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("field".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            "target.type".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config("format".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            "unix.precision".to_string(),
            ConfigValue::String("milliseconds".to_string()),
        );
        config.add_config(
            "replace.null.with.default".to_string(),
            ConfigValue::Boolean(false),
        );
        config
    }
}

impl<R: ConnectRecord<R>> TimestampConverter<R> {
    fn apply_schemaless(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use std::collections::HashMap;
        use std::sync::Arc;

        let value = record.value();
        if value.is_none() || self.field.is_empty() {
            // Convert entire value
            let converted = self.convert_timestamp(value.as_ref())?;
            let converted_arc = if let Some(c) = converted {
                Some(Arc::new(c) as Arc<dyn std::any::Any + Send + Sync>)
            } else {
                None
            };
            return Ok(Some(record.new_record(
                None,
                None,
                None,
                None,
                None,
                converted_arc,
                None,
                None,
            )));
        } else {
            // Convert specific field in HashMap
            if let Some(value_arc) = value {
                if let Some(value_any) = value_arc
                    .downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
                {
                    // Manually clone the HashMap
                    let mut updated_value: HashMap<String, Box<dyn std::any::Any + Send + Sync>> =
                        HashMap::new();
                    for (k, v) in value_any.iter() {
                        if let Some(s) = v.downcast_ref::<String>() {
                            updated_value.insert(k.clone(), Box::new(s.clone()));
                        } else if let Some(i) = v.downcast_ref::<i32>() {
                            updated_value.insert(k.clone(), Box::new(*i));
                        } else if let Some(i) = v.downcast_ref::<i64>() {
                            updated_value.insert(k.clone(), Box::new(*i));
                        } else if let Some(b) = v.downcast_ref::<bool>() {
                            updated_value.insert(k.clone(), Box::new(*b));
                        } else if let Some(f) = v.downcast_ref::<f64>() {
                            updated_value.insert(k.clone(), Box::new(*f));
                        }
                    }

                    let field_value = value_any.get(&self.field);
                    let converted = if let Some(fv) = field_value {
                        self.convert_timestamp_with_type(fv, &self.infer_timestamp_type(fv)?)
                    } else {
                        Ok(None)
                    }?;
                    if let Some(c) = converted {
                        updated_value.insert(self.field.clone(), c);
                    } else {
                        updated_value.remove(&self.field);
                    }
                    let updated_value_arc =
                        Arc::new(Box::new(updated_value) as Box<dyn std::any::Any + Send + Sync>);
                    return Ok(Some(record.new_record(
                        None,
                        None,
                        None,
                        None,
                        None,
                        Some(updated_value_arc),
                        None,
                        None,
                    )));
                }
            }
        }
        Ok(Some(record))
    }

    fn apply_with_schema(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Field, Schema, SchemaBuilder, Struct};
        use std::sync::Arc;

        let value = record.value();
        let value_schema = record.value_schema();

        if let (Some(value_arc), Some(schema_arc)) = (value, value_schema) {
            if let (Some(value_any), Some(schema_ref)) = (
                value_arc.downcast_ref::<Struct>(),
                schema_arc.as_connect_schema(),
            ) {
                if self.field.is_empty() {
                    // Convert entire value
                    let source_type = self.timestamp_type_from_schema(schema_ref)?;
                    let converted = self.convert_timestamp_with_type(value_any, &source_type)?;
                    let converted_arc = if let Some(c) = converted {
                        Some(Arc::new(c) as Arc<dyn std::any::Any + Send + Sync>)
                    } else {
                        None
                    };
                    let target_schema = self.target_type_schema(schema_ref.is_optional())?;
                    let target_schema_arc = Arc::clone(&target_schema) as Arc<dyn Schema>;
                    return Ok(Some(record.new_record(
                        None,
                        None,
                        None,
                        None,
                        Some(target_schema_arc),
                        converted_arc,
                        None,
                        None,
                    )));
                } else {
                    // Convert specific field in Struct
                    let field = schema_ref.field(&self.field)?;
                    let source_type = self.timestamp_type_from_schema(field.schema().as_ref())?;

                    // Build updated schema
                    let mut builder = SchemaBuilder::struct_();
                    for f in schema_ref.fields()? {
                        if f.name() == self.field {
                            let target_schema =
                                self.target_type_schema(f.schema().is_optional())?;
                            builder = builder.field(f.name().to_string(), target_schema);
                        } else {
                            builder = builder.field(
                                f.name().to_string(),
                                Arc::new(f.schema().as_connect_schema().unwrap().clone()),
                            );
                        }
                    }
                    if schema_ref.is_optional() {
                        builder = builder.optional();
                    }
                    let updated_schema =
                        builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;

                    // Build updated struct
                    let mut updated_struct = Struct::new(updated_schema.clone())
                        .map_err(|e| Box::new(e) as Box<dyn Error>)?;

                    for f in schema_ref.fields()? {
                        let field_name = f.name();
                        let field_value = if self.replace_null_with_default {
                            value_any.get(field_name)
                        } else {
                            value_any.get(field_name)
                        };

                        if field_name == self.field {
                            if let Some(fv) = field_value {
                                let converted =
                                    self.convert_timestamp_with_type(fv, &source_type)?;
                                if let Some(c) = converted {
                                    updated_struct.put_without_validation(field_name, c);
                                }
                            }
                        } else {
                            if let Some(fv) = field_value {
                                // Clone the value
                                if let Some(s) = fv.downcast_ref::<String>() {
                                    updated_struct
                                        .put_without_validation(field_name, Box::new(s.clone()));
                                } else if let Some(i) = fv.downcast_ref::<i32>() {
                                    updated_struct.put_without_validation(field_name, Box::new(*i));
                                } else if let Some(i) = fv.downcast_ref::<i64>() {
                                    updated_struct.put_without_validation(field_name, Box::new(*i));
                                } else if let Some(b) = fv.downcast_ref::<bool>() {
                                    updated_struct.put_without_validation(field_name, Box::new(*b));
                                } else if let Some(f) = fv.downcast_ref::<f64>() {
                                    updated_struct.put_without_validation(field_name, Box::new(*f));
                                }
                            }
                        }
                    }

                    let updated_value_arc =
                        Arc::new(Box::new(updated_struct) as Box<dyn std::any::Any + Send + Sync>);
                    let updated_schema_arc = Arc::clone(&updated_schema) as Arc<dyn Schema>;
                    return Ok(Some(record.new_record(
                        None,
                        None,
                        None,
                        None,
                        Some(updated_schema_arc),
                        Some(updated_value_arc),
                        None,
                        None,
                    )));
                }
            }
        }
        Ok(Some(record))
    }

    fn timestamp_type_from_schema(&self, schema: &dyn Schema) -> Result<String, Box<dyn Error>> {
        use connect_api::data::{Date, Time, Timestamp};

        if let Some(name) = schema.name() {
            if name == Timestamp::LOGICAL_NAME {
                return Ok("Timestamp".to_string());
            } else if name == Date::LOGICAL_NAME {
                return Ok("Date".to_string());
            } else if name == Time::LOGICAL_NAME {
                return Ok("Time".to_string());
            }
        }

        match schema.type_() {
            connect_api::data::Type::String => Ok("string".to_string()),
            connect_api::data::Type::Int64 => Ok("unix".to_string()),
            connect_api::data::Type::Int32 => {
                // Could be Date or Time, check schema name
                if let Some(name) = schema.name() {
                    if name == Date::LOGICAL_NAME {
                        Ok("Date".to_string())
                    } else if name == Time::LOGICAL_NAME {
                        Ok("Time".to_string())
                    } else {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Schema {} does not correspond to a known timestamp type format",
                                schema.type_()
                            ),
                        )));
                    }
                } else {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Int32 schema without logical name cannot be inferred as timestamp type",
                    )));
                }
            }
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Schema {} does not correspond to a known timestamp type format",
                        schema.type_()
                    ),
                )))
            }
        }
    }

    fn infer_timestamp_type(&self, value: &dyn std::any::Any) -> Result<String, Box<dyn Error>> {
        if value.is::<String>() {
            Ok("string".to_string())
        } else if value.is::<i64>() {
            Ok("unix".to_string())
        } else if value.is::<i32>() {
            // Date and Time both use i32, default to Timestamp for simplicity
            Ok("Timestamp".to_string())
        } else {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "TimestampConverter does not support {} objects as timestamps",
                    if value.is::<()>() { "null" } else { "unknown" }
                ),
            )));
        }
    }

    fn convert_timestamp(
        &self,
        value: Option<&Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Option<Box<dyn std::any::Any + Send + Sync>>, Box<dyn Error>> {
        if value.is_none() {
            return Ok(None);
        }

        let source_type = self.infer_timestamp_type(value.unwrap().as_ref())?;
        self.convert_timestamp_with_type(value.unwrap().as_ref(), &source_type)
    }

    fn convert_timestamp_with_type(
        &self,
        value: &dyn std::any::Any,
        source_type: &str,
    ) -> Result<Option<Box<dyn std::any::Any + Send + Sync>>, Box<dyn Error>> {
        // Convert source to milliseconds since epoch
        let millis = self.to_millis(value, source_type)?;

        // Convert milliseconds to target type
        self.from_millis(millis, &self.target_type)
    }

    fn to_millis(
        &self,
        value: &dyn std::any::Any,
        source_type: &str,
    ) -> Result<i64, Box<dyn Error>> {
        use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

        match source_type {
            "string" => {
                if let Some(s) = value.downcast_ref::<String>() {
                    // Parse string using format pattern
                    let format_str = if self.format.is_empty() {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "TimestampConverter requires format option to be specified when using string timestamps",
                        )));
                    } else {
                        &self.format
                    };

                    // Convert SimpleDateFormat pattern to chrono format
                    let chrono_format = self.simple_date_format_to_chrono(format_str)?;

                    let dt = NaiveDateTime::parse_from_str(s, &chrono_format).map_err(|e| {
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Could not parse timestamp: value ({}) does not match pattern ({})",
                                s, format_str
                            ),
                        ))
                    })?;

                    Ok(dt.and_utc().timestamp_millis())
                } else {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Expected string timestamp to be a String",
                    )));
                }
            }
            "unix" => {
                if let Some(unix_time) = value.downcast_ref::<i64>() {
                    match self.unix_precision.as_str() {
                        "seconds" => Ok(unix_time * 1000),
                        "microseconds" => Ok(unix_time / 1000),
                        "nanoseconds" => Ok(unix_time / 1_000_000),
                        _ => Ok(*unix_time), // milliseconds (default)
                    }
                } else {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Expected Unix timestamp to be a Long (i64)",
                    )));
                }
            }
            "Date" => {
                if let Some(days) = value.downcast_ref::<i32>() {
                    // Date is days since epoch
                    Ok(*days as i64 * connect_api::data::Date::MILLIS_PER_DAY)
                } else {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Expected Date to be an i32 (days since epoch)",
                    )));
                }
            }
            "Time" => {
                if let Some(millis) = value.downcast_ref::<i32>() {
                    // Time is milliseconds since midnight
                    Ok(*millis as i64)
                } else {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Expected Time to be an i32 (milliseconds since midnight)",
                    )));
                }
            }
            "Timestamp" => {
                if let Some(millis) = value.downcast_ref::<i64>() {
                    Ok(*millis)
                } else {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Expected Timestamp to be an i64 (milliseconds since epoch)",
                    )));
                }
            }
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unsupported timestamp type: {}", source_type),
                )))
            }
        }
    }

    fn from_millis(
        &self,
        millis: i64,
        target_type: &str,
    ) -> Result<Option<Box<dyn std::any::Any + Send + Sync>>, Box<dyn Error>> {
        use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

        match target_type {
            "string" => {
                let format_str = if self.format.is_empty() {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "TimestampConverter requires format option to be specified when using string timestamps",
                    )));
                } else {
                    &self.format
                };

                let chrono_format = self.simple_date_format_to_chrono(format_str)?;
                let dt = DateTime::<Utc>::from_timestamp_millis(millis).ok_or_else(|| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid timestamp value",
                    ))
                })?;

                let formatted = dt.format(&chrono_format).to_string();
                Ok(Some(Box::new(formatted)))
            }
            "unix" => {
                let unix_time = match self.unix_precision.as_str() {
                    "seconds" => millis / 1000,
                    "microseconds" => millis * 1000,
                    "nanoseconds" => millis * 1_000_000,
                    _ => millis, // milliseconds (default)
                };
                Ok(Some(Box::new(unix_time)))
            }
            "Date" => {
                let days = (millis / connect_api::data::Date::MILLIS_PER_DAY) as i32;
                Ok(Some(Box::new(days)))
            }
            "Time" => {
                // Time is milliseconds since midnight
                let time_millis = (millis % 86_400_000) as i32; // 24 * 60 * 60 * 1000
                Ok(Some(Box::new(time_millis)))
            }
            "Timestamp" => Ok(Some(Box::new(millis))),
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unsupported timestamp type: {}", target_type),
                )))
            }
        }
    }

    fn target_type_schema(
        &self,
        is_optional: bool,
    ) -> Result<Arc<connect_api::data::ConnectSchema>, Box<dyn Error>> {
        use connect_api::data::{Date, SchemaBuilder, Time, Timestamp};

        let builder = match self.target_type.as_str() {
            "string" => SchemaBuilder::string(),
            "unix" => SchemaBuilder::int64(),
            "Date" => SchemaBuilder::int32()
                .name(Date::LOGICAL_NAME.to_string())
                .version(1),
            "Time" => SchemaBuilder::int32()
                .name(Time::LOGICAL_NAME.to_string())
                .version(1),
            "Timestamp" => SchemaBuilder::int64()
                .name(Timestamp::LOGICAL_NAME.to_string())
                .version(1),
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unsupported timestamp type: {}", self.target_type),
                )))
            }
        };

        let schema = if is_optional {
            builder.optional().build()
        } else {
            builder.build()
        };

        schema.map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    /// Convert SimpleDateFormat pattern to chrono format
    /// This is a simplified conversion - full SimpleDateFormat support would be more complex
    fn simple_date_format_to_chrono(&self, pattern: &str) -> Result<String, Box<dyn Error>> {
        let mut result = String::new();
        let chars: Vec<char> = pattern.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            if i + 1 < chars.len() && chars[i] == chars[i + 1] {
                // Check for repeated characters (SimpleDateFormat uses repeated chars)
                match chars[i] {
                    'y' => {
                        // Year
                        let count = self.count_repeated(&chars, i, 'y');
                        if count >= 4 {
                            result.push_str("%Y");
                        } else {
                            result.push_str("%y");
                        }
                        i += count;
                    }
                    'M' => {
                        // Month
                        let count = self.count_repeated(&chars, i, 'M');
                        if count >= 3 {
                            result.push_str("%B"); // Full month name
                        } else if count == 2 {
                            result.push_str("%m"); // Zero-padded month
                        } else {
                            result.push_str("%-m"); // Month without padding
                        }
                        i += count;
                    }
                    'd' => {
                        // Day
                        let count = self.count_repeated(&chars, i, 'd');
                        if count >= 2 {
                            result.push_str("%d"); // Zero-padded day
                        } else {
                            result.push_str("%-d"); // Day without padding
                        }
                        i += count;
                    }
                    'H' => {
                        // Hour (24-hour)
                        let count = self.count_repeated(&chars, i, 'H');
                        if count >= 2 {
                            result.push_str("%H"); // Zero-padded hour
                        } else {
                            result.push_str("%-H"); // Hour without padding
                        }
                        i += count;
                    }
                    'h' => {
                        // Hour (12-hour)
                        let count = self.count_repeated(&chars, i, 'h');
                        if count >= 2 {
                            result.push_str("%I"); // Zero-padded hour (12-hour)
                        } else {
                            result.push_str("%-I"); // Hour without padding (12-hour)
                        }
                        i += count;
                    }
                    'm' => {
                        // Minute
                        let count = self.count_repeated(&chars, i, 'm');
                        if count >= 2 {
                            result.push_str("%M"); // Zero-padded minute
                        } else {
                            result.push_str("%-M"); // Minute without padding
                        }
                        i += count;
                    }
                    's' => {
                        // Second
                        let count = self.count_repeated(&chars, i, 's');
                        if count >= 2 {
                            result.push_str("%S"); // Zero-padded second
                        } else {
                            result.push_str("%-S"); // Second without padding
                        }
                        i += count;
                    }
                    'S' => {
                        // Millisecond
                        let count = self.count_repeated(&chars, i, 'S');
                        if count == 3 {
                            result.push_str("%3f"); // Milliseconds (3 digits)
                        } else if count == 6 {
                            result.push_str("%6f"); // Microseconds (6 digits)
                        } else if count == 9 {
                            result.push_str("%9f"); // Nanoseconds (9 digits)
                        } else {
                            result.push_str("%3f"); // Default to milliseconds
                        }
                        i += count;
                    }
                    'a' => {
                        result.push_str("%P"); // AM/PM lowercase
                        i += 2;
                    }
                    'A' => {
                        result.push_str("%p"); // AM/PM uppercase
                        i += 2;
                    }
                    'z' => {
                        result.push_str("%z"); // Timezone offset
                        i += 2;
                    }
                    'Z' => {
                        result.push_str("%Z"); // Timezone name
                        i += 2;
                    }
                    _ => {
                        result.push(chars[i]);
                        i += 1;
                    }
                }
            } else {
                // Single character, just add it
                result.push(chars[i]);
                i += 1;
            }
        }

        Ok(result)
    }

    fn count_repeated(&self, chars: &[char], start: usize, c: char) -> usize {
        let mut count = 0;
        for i in start..chars.len() {
            if chars[i] == c {
                count += 1;
            } else {
                break;
            }
        }
        count
    }
}

/// ReplaceField transformation
///
/// Replaces, renames, or excludes fields in records.
pub struct ReplaceField<R: ConnectRecord<R>> {
    exclude: Vec<String>,
    include: Vec<String>,
    renames: HashMap<String, String>,
    replace_null_with_default: bool,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> ReplaceField<R> {
    pub fn new() -> Self {
        Self {
            exclude: Vec::new(),
            include: Vec::new(),
            renames: HashMap::new(),
            replace_null_with_default: false,
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for ReplaceField<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(exclude) = configs.get("exclude") {
            if let Some(e) = exclude.downcast_ref::<String>() {
                self.exclude = e.split(",").map(|s| s.trim().to_string()).collect();
            }
        }

        if let Some(include) = configs.get("include") {
            if let Some(i) = include.downcast_ref::<String>() {
                self.include = i.split(",").map(|s| s.trim().to_string()).collect();
            }
        }

        if let Some(renames) = configs.get("renames") {
            if let Some(r) = renames.downcast_ref::<String>() {
                // Parse renames in format "old1:new1,old2:new2"
                for pair in r.split(",") {
                    if let Some((old, new)) = pair.split_once(':') {
                        self.renames
                            .insert(old.trim().to_string(), new.trim().to_string());
                    }
                }
            }
        }

        if let Some(replace) = configs.get("replace.null.with.default") {
            if let Some(r) = replace.downcast_ref::<bool>() {
                self.replace_null_with_default = *r;
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for ReplaceField<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.exclude.clear();
        self.include.clear();
        self.renames.clear();
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for ReplaceField<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        // Handle null value
        let value = record.value();
        if value.is_none() {
            return Ok(Some(record));
        }

        let value_schema = record.value_schema();

        if value_schema.is_none() {
            // SchemalessREPLACE
            self.apply_schemaless(record)
        } else {
            // Schema-based REPLACE
            self.apply_with_schema(record)
        }
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("exclude".to_string(), ConfigValue::String(String::new()));
        config.add_config("include".to_string(), ConfigValue::String(String::new()));
        config.add_config("renames".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            "replace.null.with.default".to_string(),
            ConfigValue::Boolean(false),
        );
        config
    }
}

impl<R: ConnectRecord<R>> ReplaceField<R> {
    fn clone_box(
        &self,
        value: &dyn std::any::Any,
    ) -> Box<dyn std::any::Any + Send + Sync + 'static> {
        if let Some(s) = value.downcast_ref::<String>() {
            Box::new(s.clone())
        } else if let Some(i) = value.downcast_ref::<i8>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i16>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i32>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i64>() {
            Box::new(*i)
        } else if let Some(f) = value.downcast_ref::<f32>() {
            Box::new(*f)
        } else if let Some(f) = value.downcast_ref::<f64>() {
            Box::new(*f)
        } else if let Some(b) = value.downcast_ref::<bool>() {
            Box::new(*b)
        } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
            Box::new(v.clone())
        } else if let Some(v) = value.downcast_ref::<Vec<Box<dyn std::any::Any + Send + Sync>>>() {
            let mut cloned_vec = Vec::new();
            for item in v {
                cloned_vec.push(self.clone_box(item.as_ref()));
            }
            Box::new(cloned_vec)
        } else if let Some(v) =
            value.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
        {
            let mut cloned_map = HashMap::new();
            for (k, val) in v.iter() {
                cloned_map.insert(k.clone(), self.clone_box(val.as_ref()));
            }
            Box::new(cloned_map)
        } else {
            Box::new(())
        }
    }

    fn filter(&self, field_name: &str) -> bool {
        !self.exclude.contains(&field_name.to_string())
            && (self.include.is_empty() || self.contains(field_name))
    }

    fn contains(&self, field_name: &str) -> bool {
        self.include.iter().any(|f| f == field_name)
    }

    fn renamed(&self, field_name: &str) -> String {
        match self.renames.get(field_name) {
            Some(new_name) => new_name.clone(),
            None => field_name.to_string(),
        }
    }

    fn reverse_renamed(&self, field_name: &str) -> String {
        // Find original name from renames map
        for (old, new) in &self.renames {
            if new == field_name {
                return old.clone();
            }
        }
        field_name.to_string()
    }

    fn apply_schemaless(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::Struct;
        use std::sync::Arc;

        let value = record.value();
        if let Some(value_arc) = value {
            if let Some(value_any) =
                value_arc.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
            {
                let mut updated_value = HashMap::new();

                for (field_name, field_value) in value_any.iter() {
                    if self.filter(field_name) {
                        let new_name = self.renamed(field_name);
                        // Use clone_box to properly clone the dynamic value
                        let cloned = self.clone_box(field_value);
                        updated_value.insert(new_name, cloned);
                    }
                }

                let boxed: Box<HashMap<String, Box<dyn std::any::Any + Send + Sync>>> =
                    Box::new(updated_value);
                let updated_value_arc = Arc::new(boxed as Box<dyn std::any::Any + Send + Sync>);
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(updated_value_arc),
                    None,
                    None,
                )));
            }
        }
        Ok(Some(record))
    }

    fn apply_with_schema(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Field, Schema, SchemaBuilder, Struct, Type};
        use std::sync::Arc;

        let value = record.value();
        let value_schema = record.value_schema();

        if let (Some(value_arc), Some(schema_arc)) = (value, value_schema) {
            if let (Some(value_any), Some(schema_ref)) = (
                value_arc.downcast_ref::<Struct>(),
                schema_arc.as_connect_schema(),
            ) {
                // Build updated schema
                let mut builder = SchemaBuilder::struct_();
                for field in schema_ref.fields().unwrap_or_default() {
                    if self.filter(field.name()) {
                        let new_name = self.renamed(field.name());
                        builder = builder.field(
                            new_name,
                            Arc::new(field.schema().as_connect_schema().unwrap().clone()),
                        );
                    }
                }
                let updated_schema = builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;

                // Build updated struct
                let mut updated_struct = Struct::new(updated_schema.clone())
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;
                for field in updated_schema.fields().unwrap_or_default() {
                    let original_name = self.reverse_renamed(field.name());
                    let field_value = if self.replace_null_with_default {
                        value_any.get(&original_name)
                    } else {
                        value_any.get(&original_name)
                    };
                    if let Some(fv) = field_value {
                        // Clone the value
                        if let Some(s) = fv.downcast_ref::<String>() {
                            updated_struct
                                .put_without_validation(field.name(), Box::new(s.clone()));
                        } else if let Some(i) = fv.downcast_ref::<i32>() {
                            updated_struct.put_without_validation(field.name(), Box::new(*i));
                        } else if let Some(i) = fv.downcast_ref::<i64>() {
                            updated_struct.put_without_validation(field.name(), Box::new(*i));
                        } else if let Some(b) = fv.downcast_ref::<bool>() {
                            updated_struct.put_without_validation(field.name(), Box::new(*b));
                        } else if let Some(f) = fv.downcast_ref::<f64>() {
                            updated_struct.put_without_validation(field.name(), Box::new(*f));
                        }
                    }
                }

                let updated_value_arc =
                    Arc::new(Box::new(updated_struct) as Box<dyn std::any::Any + Send + Sync>);
                let updated_schema_arc = Arc::clone(&updated_schema) as Arc<dyn Schema>;
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    Some(updated_schema_arc),
                    Some(updated_value_arc),
                    None,
                    None,
                )));
            }
        }
        Ok(Some(record))
    }
}

/// MaskField transformation
///
/// Masks specified fields in records.
pub struct MaskField<R: ConnectRecord<R>> {
    fields: Vec<String>,
    replacement: Option<String>,
    replace_null_with_default: bool,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> MaskField<R> {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            replacement: None,
            replace_null_with_default: false,
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for MaskField<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(fields) = configs.get("fields") {
            if let Some(f) = fields.downcast_ref::<String>() {
                self.fields = f.split(",").map(|s| s.trim().to_string()).collect();
            }
        }

        if let Some(replacement) = configs.get("replacement") {
            if let Some(r) = replacement.downcast_ref::<String>() {
                self.replacement = Some(r.clone());
            }
        }

        if let Some(replace) = configs.get("replace.null.with.default") {
            if let Some(r) = replace.downcast_ref::<bool>() {
                self.replace_null_with_default = *r;
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for MaskField<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.fields.clear();
        self.replacement = None;
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for MaskField<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        let value_schema = record.value_schema();

        if value_schema.is_none() {
            // Schemaless
            self.apply_schemaless(record)
        } else {
            // Schema-based
            self.apply_with_schema(record)
        }
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("fields".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            "replacement".to_string(),
            ConfigValue::String("****".to_string()),
        );
        config.add_config(
            "replace.null.with.default".to_string(),
            ConfigValue::Boolean(false),
        );
        config
    }
}

impl<R: ConnectRecord<R>> MaskField<R> {
    fn clone_box(
        &self,
        value: &dyn std::any::Any,
    ) -> Box<dyn std::any::Any + Send + Sync + 'static> {
        if let Some(s) = value.downcast_ref::<String>() {
            Box::new(s.clone())
        } else if let Some(i) = value.downcast_ref::<i8>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i16>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i32>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i64>() {
            Box::new(*i)
        } else if let Some(f) = value.downcast_ref::<f32>() {
            Box::new(*f)
        } else if let Some(f) = value.downcast_ref::<f64>() {
            Box::new(*f)
        } else if let Some(b) = value.downcast_ref::<bool>() {
            Box::new(*b)
        } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
            Box::new(v.clone())
        } else if let Some(v) = value.downcast_ref::<Vec<Box<dyn std::any::Any + Send + Sync>>>() {
            // For Vec<Box<dyn Any>>, we need to downcast each element to a cloneable type
            // Return empty vec as fallback since we can't easily clone Box<dyn Any>
            let mut cloned = Vec::new();
            for item in v {
                if let Some(s) = item.downcast_ref::<String>() {
                    cloned.push(Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(b) = item.downcast_ref::<bool>() {
                    cloned.push(Box::new(*b) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(i) = item.downcast_ref::<i32>() {
                    cloned.push(Box::new(*i) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(i) = item.downcast_ref::<i64>() {
                    cloned.push(Box::new(*i) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(f) = item.downcast_ref::<f64>() {
                    cloned.push(Box::new(*f) as Box<dyn std::any::Any + Send + Sync>);
                }
                // Skip elements we can't clone
            }
            Box::new(cloned)
        } else if let Some(v) =
            value.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
        {
            // For HashMap<String, Box<dyn Any>>, clone the values we can
            let mut cloned = HashMap::new();
            for (k, val) in v.iter() {
                if let Some(s) = val.downcast_ref::<String>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(b) = val.downcast_ref::<bool>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*b) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(i) = val.downcast_ref::<i32>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*i) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(i) = val.downcast_ref::<i64>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*i) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(f) = val.downcast_ref::<f64>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*f) as Box<dyn std::any::Any + Send + Sync>,
                    );
                }
                // Skip values we can't clone
            }
            Box::new(cloned)
        } else {
            Box::new(())
        }
    }

    fn masked(&self, value: &dyn std::any::Any) -> Box<dyn std::any::Any + Send + Sync> {
        if let Some(replacement) = &self.replacement {
            // Use custom replacement string
            Box::new(replacement.clone())
        } else {
            // Use primitive null values based on type
            if value.is::<bool>() {
                Box::new(false)
            } else if value.is::<i8>() {
                Box::new(0i8)
            } else if value.is::<i16>() {
                Box::new(0i16)
            } else if value.is::<i32>() {
                Box::new(0i32)
            } else if value.is::<i64>() {
                Box::new(0i64)
            } else if value.is::<f32>() {
                Box::new(0.0f32)
            } else if value.is::<f64>() {
                Box::new(0.0f64)
            } else if value.is::<String>() {
                Box::new(String::new())
            } else {
                // Default to empty string for unknown types
                Box::new(String::new())
            }
        }
    }

    fn apply_schemaless(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use std::sync::Arc;

        let value = record.value();
        if let Some(value_arc) = value {
            if let Some(value_any) =
                value_arc.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
            {
                // Create new HashMap by cloning all entries
                let mut updated_value: HashMap<String, Box<dyn std::any::Any + Send + Sync>> =
                    HashMap::new();
                for (k, v) in value_any.iter() {
                    updated_value.insert(k.clone(), self.clone_box(v.as_ref()));
                }

                for field_name in &self.fields {
                    if let Some(field_value) = updated_value.get(field_name) {
                        let masked_value = self.masked(field_value.as_ref());
                        updated_value.insert(field_name.clone(), masked_value);
                    }
                }

                let updated_value_arc =
                    Arc::new(Box::new(updated_value) as Box<dyn std::any::Any + Send + Sync>);
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(updated_value_arc),
                    None,
                    None,
                )));
            }
        }
        Ok(Some(record))
    }

    fn apply_with_schema(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Field, Schema, Struct};
        use std::sync::Arc;

        let value = record.value();
        let value_schema = record.value_schema();

        if let (Some(value_arc), Some(schema_arc)) = (value, value_schema) {
            if let (Some(value_any), Some(schema_ref)) = (
                value_arc.downcast_ref::<Struct>(),
                schema_arc.as_connect_schema(),
            ) {
                let mut updated_struct =
                    Struct::new(Arc::new(schema_arc.as_connect_schema().unwrap().clone()))
                        .map_err(|e| Box::new(e) as Box<dyn Error>)?;

                for field in schema_ref.fields().unwrap_or_default() {
                    let field_name = field.name();
                    let field_value = if self.replace_null_with_default {
                        value_any.get(field_name)
                    } else {
                        value_any.get(field_name)
                    };

                    let masked_value: Option<Box<dyn std::any::Any + Send + Sync>> =
                        if self.fields.contains(&field_name.to_string()) {
                            if let Some(fv) = field_value {
                                Some(self.masked(fv))
                            } else {
                                None
                            }
                        } else {
                            if let Some(fv) = field_value {
                                // Clone original value
                                if let Some(s) = fv.downcast_ref::<String>() {
                                    Some(Box::new(s.clone()))
                                } else if let Some(i) = fv.downcast_ref::<i32>() {
                                    Some(Box::new(*i))
                                } else if let Some(i) = fv.downcast_ref::<i64>() {
                                    Some(Box::new(*i))
                                } else if let Some(b) = fv.downcast_ref::<bool>() {
                                    Some(Box::new(*b))
                                } else if let Some(f) = fv.downcast_ref::<f64>() {
                                    Some(Box::new(*f))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        };

                    if let Some(mv) = masked_value {
                        updated_struct.put_without_validation(field_name, mv);
                    }
                }

                let updated_value_arc =
                    Arc::new(Box::new(updated_struct) as Box<dyn std::any::Any + Send + Sync>);
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    Some(schema_arc.clone()),
                    Some(updated_value_arc),
                    None,
                    None,
                )));
            }
        }
        Ok(Some(record))
    }
}

/// InsertField transformation
///
/// Inserts metadata fields into records.
pub struct InsertField<R: ConnectRecord<R>> {
    topic_field: Option<String>,
    partition_field: Option<String>,
    offset_field: Option<String>,
    timestamp_field: Option<String>,
    static_field: Option<String>,
    static_value: Option<String>,
    replace_null_with_default: bool,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> InsertField<R> {
    pub fn new() -> Self {
        Self {
            topic_field: None,
            partition_field: None,
            offset_field: None,
            timestamp_field: None,
            static_field: None,
            static_value: None,
            replace_null_with_default: false,
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for InsertField<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(field) = configs.get("topic.field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.topic_field = Some(f.clone());
            }
        }

        if let Some(field) = configs.get("partition.field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.partition_field = Some(f.clone());
            }
        }

        if let Some(field) = configs.get("offset.field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.offset_field = Some(f.clone());
            }
        }

        if let Some(field) = configs.get("timestamp.field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.timestamp_field = Some(f.clone());
            }
        }

        if let Some(field) = configs.get("static.field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.static_field = Some(f.clone());
            }
        }

        if let Some(value) = configs.get("static.value") {
            if let Some(v) = value.downcast_ref::<String>() {
                self.static_value = Some(v.clone());
            }
        }

        if let Some(replace) = configs.get("replace.null.with.default") {
            if let Some(r) = replace.downcast_ref::<bool>() {
                self.replace_null_with_default = *r;
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for InsertField<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.topic_field = None;
        self.partition_field = None;
        self.offset_field = None;
        self.timestamp_field = None;
        self.static_field = None;
        self.static_value = None;
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for InsertField<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        let value = record.value();
        if value.is_none() {
            return Ok(Some(record));
        }

        let value_schema = record.value_schema();

        if value_schema.is_none() {
            // Schemaless
            self.apply_schemaless(record)
        } else {
            // Schema-based
            self.apply_with_schema(record)
        }
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config(
            "topic.field".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config(
            "partition.field".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config(
            "offset.field".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config(
            "timestamp.field".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config(
            "static.field".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config(
            "static.value".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config(
            "replace.null.with.default".to_string(),
            ConfigValue::Boolean(false),
        );
        config
    }
}

impl<R: ConnectRecord<R>> InsertField<R> {
    fn clone_box(
        &self,
        value: &dyn std::any::Any,
    ) -> Box<dyn std::any::Any + Send + Sync + 'static> {
        if let Some(s) = value.downcast_ref::<String>() {
            Box::new(s.clone())
        } else if let Some(i) = value.downcast_ref::<i8>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i16>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i32>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i64>() {
            Box::new(*i)
        } else if let Some(f) = value.downcast_ref::<f32>() {
            Box::new(*f)
        } else if let Some(f) = value.downcast_ref::<f64>() {
            Box::new(*f)
        } else if let Some(b) = value.downcast_ref::<bool>() {
            Box::new(*b)
        } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
            Box::new(v.clone())
        } else if let Some(v) = value.downcast_ref::<Vec<Box<dyn std::any::Any + Send + Sync>>>() {
            // For Vec<Box<dyn Any>>, we need to downcast each element to a cloneable type
            // Return empty vec as fallback since we can't easily clone Box<dyn Any>
            let mut cloned = Vec::new();
            for item in v {
                if let Some(s) = item.downcast_ref::<String>() {
                    cloned.push(Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(b) = item.downcast_ref::<bool>() {
                    cloned.push(Box::new(*b) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(i) = item.downcast_ref::<i32>() {
                    cloned.push(Box::new(*i) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(i) = item.downcast_ref::<i64>() {
                    cloned.push(Box::new(*i) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(f) = item.downcast_ref::<f64>() {
                    cloned.push(Box::new(*f) as Box<dyn std::any::Any + Send + Sync>);
                }
                // Skip elements we can't clone
            }
            Box::new(cloned)
        } else if let Some(v) =
            value.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
        {
            // For HashMap<String, Box<dyn Any>>, clone the values we can
            let mut cloned = HashMap::new();
            for (k, val) in v.iter() {
                if let Some(s) = val.downcast_ref::<String>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(b) = val.downcast_ref::<bool>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*b) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(i) = val.downcast_ref::<i32>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*i) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(i) = val.downcast_ref::<i64>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*i) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(f) = val.downcast_ref::<f64>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*f) as Box<dyn std::any::Any + Send + Sync>,
                    );
                }
                // Skip values we can't clone
            }
            Box::new(cloned)
        } else {
            Box::new(())
        }
    }

    fn parse_field_spec(spec: &Option<String>) -> Option<(String, bool)> {
        spec.as_ref().map(|s| {
            if s.ends_with('?') {
                (s[..s.len() - 1].to_string(), true)
            } else if s.ends_with('!') {
                (s[..s.len() - 1].to_string(), false)
            } else {
                (s.clone(), true)
            }
        })
    }

    fn apply_schemaless(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use std::sync::Arc;

        let value = record.value();
        if let Some(value_arc) = value {
            if let Some(value_any) =
                value_arc.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
            {
                // Create new HashMap by cloning all entries
                let mut updated_value: HashMap<String, Box<dyn std::any::Any + Send + Sync>> =
                    HashMap::new();
                for (k, v) in value_any.iter() {
                    updated_value.insert(k.clone(), self.clone_box(v.as_ref()));
                }

                // Insert topic
                if let Some((field_name, _)) = Self::parse_field_spec(&self.topic_field) {
                    updated_value.insert(field_name, Box::new(record.topic().to_string()));
                }

                // Insert partition
                if let Some((field_name, _)) = Self::parse_field_spec(&self.partition_field) {
                    if let Some(partition) = record.kafka_partition() {
                        updated_value.insert(field_name, Box::new(partition));
                    }
                }

                // Insert timestamp
                if let Some((field_name, _)) = Self::parse_field_spec(&self.timestamp_field) {
                    if let Some(timestamp) = record.timestamp() {
                        updated_value.insert(field_name, Box::new(timestamp));
                    }
                }

                // Insert static value
                if let (Some((field_name, _)), Some(static_value)) = (
                    Self::parse_field_spec(&self.static_field),
                    &self.static_value,
                ) {
                    updated_value.insert(field_name, Box::new(static_value.clone()));
                }

                let updated_value_arc =
                    Arc::new(Box::new(updated_value) as Box<dyn std::any::Any + Send + Sync>);
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(updated_value_arc),
                    None,
                    None,
                )));
            }
        }
        Ok(Some(record))
    }

    fn apply_with_schema(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Field, Schema, SchemaBuilder, Struct};
        use std::sync::Arc;

        let value = record.value();
        let value_schema = record.value_schema();

        if let (Some(value_arc), Some(schema_arc)) = (value, value_schema) {
            if let (Some(value_any), Some(schema_ref)) = (
                value_arc.downcast_ref::<Struct>(),
                schema_arc.as_connect_schema(),
            ) {
                // Build updated schema
                let mut builder = SchemaBuilder::struct_();
                for field in schema_ref.fields().unwrap_or_default() {
                    builder = builder.field(
                        field.name().to_string(),
                        Arc::new(field.schema().as_connect_schema().unwrap().clone()),
                    );
                }

                // Add new fields to schema
                if let Some((field_name, optional)) = Self::parse_field_spec(&self.topic_field) {
                    let schema = SchemaBuilder::string();
                    let schema = if optional { schema.optional() } else { schema };
                    let field_schema = schema.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;
                    builder = builder.field(field_name, field_schema);
                }

                if let Some((field_name, optional)) = Self::parse_field_spec(&self.partition_field)
                {
                    let schema = SchemaBuilder::int32();
                    let schema = if optional { schema.optional() } else { schema };
                    let field_schema = schema.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;
                    builder = builder.field(field_name, field_schema);
                }

                if let Some((field_name, optional)) = Self::parse_field_spec(&self.offset_field) {
                    let schema = SchemaBuilder::int64();
                    let schema = if optional { schema.optional() } else { schema };
                    let field_schema = schema.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;
                    builder = builder.field(field_name, field_schema);
                }

                if let Some((field_name, optional)) = Self::parse_field_spec(&self.timestamp_field)
                {
                    let schema = SchemaBuilder::int64();
                    let schema = if optional { schema.optional() } else { schema };
                    let field_schema = schema.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;
                    builder = builder.field(field_name, field_schema);
                }

                if let Some((field_name, optional)) = Self::parse_field_spec(&self.static_field) {
                    let schema = SchemaBuilder::string();
                    let schema = if optional { schema.optional() } else { schema };
                    let field_schema = schema.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;
                    builder = builder.field(field_name, field_schema);
                }

                let updated_schema = builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;

                // Build updated struct
                let mut updated_struct = Struct::new(updated_schema.clone())
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;

                // Copy existing fields
                for field in schema_ref.fields().unwrap_or_default() {
                    let field_value = if self.replace_null_with_default {
                        value_any.get(field.name())
                    } else {
                        value_any.get(field.name())
                    };
                    if let Some(fv) = field_value {
                        if let Some(s) = fv.downcast_ref::<String>() {
                            updated_struct
                                .put_without_validation(field.name(), Box::new(s.clone()));
                        } else if let Some(i) = fv.downcast_ref::<i32>() {
                            updated_struct.put_without_validation(field.name(), Box::new(*i));
                        } else if let Some(i) = fv.downcast_ref::<i64>() {
                            updated_struct.put_without_validation(field.name(), Box::new(*i));
                        } else if let Some(b) = fv.downcast_ref::<bool>() {
                            updated_struct.put_without_validation(field.name(), Box::new(*b));
                        } else if let Some(f) = fv.downcast_ref::<f64>() {
                            updated_struct.put_without_validation(field.name(), Box::new(*f));
                        }
                    }
                }

                // Insert new field values
                if let Some((field_name, _)) = Self::parse_field_spec(&self.topic_field) {
                    updated_struct
                        .put_without_validation(&field_name, Box::new(record.topic().to_string()));
                }

                if let Some((field_name, _)) = Self::parse_field_spec(&self.partition_field) {
                    if let Some(partition) = record.kafka_partition() {
                        updated_struct.put_without_validation(&field_name, Box::new(partition));
                    }
                }

                if let Some((field_name, _)) = Self::parse_field_spec(&self.timestamp_field) {
                    if let Some(timestamp) = record.timestamp() {
                        updated_struct.put_without_validation(&field_name, Box::new(timestamp));
                    }
                }

                if let (Some((field_name, _)), Some(static_value)) = (
                    Self::parse_field_spec(&self.static_field),
                    &self.static_value,
                ) {
                    updated_struct
                        .put_without_validation(&field_name, Box::new(static_value.clone()));
                }

                let updated_value_arc =
                    Arc::new(Box::new(updated_struct) as Box<dyn std::any::Any + Send + Sync>);
                let updated_schema_arc = Arc::clone(&updated_schema) as Arc<dyn Schema>;
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    Some(updated_schema_arc),
                    Some(updated_value_arc),
                    None,
                    None,
                )));
            }
        }
        Ok(Some(record))
    }
}

/// HoistField transformation
///
/// Hoists a single field to become the entire value.
pub struct HoistField<R: ConnectRecord<R>> {
    field: String,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> HoistField<R> {
    pub fn new() -> Self {
        Self {
            field: String::new(),
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for HoistField<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(field) = configs.get("field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.field = f.clone();
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for HoistField<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.field.clear();
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for HoistField<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        let value_schema = record.value_schema();
        let value = record.value();

        if value_schema.is_none() {
            // Schemaless: create HashMap with field name -> value
            self.apply_schemaless(record, value)
        } else {
            // Schema-based: create Struct with field name -> value
            self.apply_with_schema(record, value_schema, value)
        }
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("field".to_string(), ConfigValue::String(String::new()));
        config
    }
}

impl<R: ConnectRecord<R>> HoistField<R> {
    fn apply_schemaless(
        &self,
        record: R,
        value: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Option<R>, Box<dyn Error>> {
        use std::sync::Arc;

        let mut updated_value = HashMap::new();
        if let Some(value_arc) = value {
            updated_value.insert(self.field.clone(), value_arc);
        }

        let updated_value_arc =
            Arc::new(Box::new(updated_value) as Box<dyn std::any::Any + Send + Sync>);
        Ok(Some(record.new_record(
            None,
            None,
            None,
            None,
            None,
            Some(updated_value_arc),
            None,
            None,
        )))
    }

    fn apply_with_schema(
        &self,
        record: R,
        value_schema: Option<Arc<dyn Schema>>,
        value: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Schema, SchemaBuilder, Struct};
        use std::sync::Arc;

        if let Some(schema_arc) = value_schema {
            // Create new schema with single field
            let builder = SchemaBuilder::struct_().field(
                self.field.clone(),
                Arc::new(schema_arc.as_connect_schema().unwrap().clone()),
            );
            let updated_schema = builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;

            // Create new struct with single field
            let mut updated_struct =
                Struct::new(updated_schema.clone()).map_err(|e| Box::new(e) as Box<dyn Error>)?;
            if let Some(value_arc) = value {
                // Convert Arc to Box by downcasting and cloning
                let boxed_value: Box<dyn std::any::Any + Send + Sync> =
                    if let Some(s) = value_arc.downcast_ref::<String>() {
                        Box::new(s.clone())
                    } else if let Some(i) = value_arc.downcast_ref::<i32>() {
                        Box::new(*i)
                    } else if let Some(i) = value_arc.downcast_ref::<i64>() {
                        Box::new(*i)
                    } else if let Some(b) = value_arc.downcast_ref::<bool>() {
                        Box::new(*b)
                    } else if let Some(f) = value_arc.downcast_ref::<f64>() {
                        Box::new(*f)
                    } else {
                        Box::new(())
                    };
                updated_struct.put_without_validation(&self.field, boxed_value);
            }

            let updated_value_arc =
                Arc::new(Box::new(updated_struct) as Box<dyn std::any::Any + Send + Sync>);
            let updated_schema_arc = Arc::clone(&updated_schema) as Arc<dyn Schema>;
            Ok(Some(record.new_record(
                None,
                None,
                None,
                None,
                Some(updated_schema_arc),
                Some(updated_value_arc),
                None,
                None,
            )))
        } else {
            Ok(Some(record))
        }
    }
}

/// HeaderFrom transformation
///
/// Copies or moves fields to/from headers.
pub struct HeaderFrom<R: ConnectRecord<R>> {
    fields: Vec<String>,
    headers: Vec<String>,
    operation: String,
    replace_null_with_default: bool,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> HeaderFrom<R> {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            headers: Vec::new(),
            operation: String::new(),
            replace_null_with_default: false,
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for HeaderFrom<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(fields) = configs.get("fields") {
            if let Some(f) = fields.downcast_ref::<String>() {
                self.fields = f.split(",").map(|s| s.trim().to_string()).collect();
            }
        }

        if let Some(headers) = configs.get("headers") {
            if let Some(h) = headers.downcast_ref::<String>() {
                self.headers = h.split(",").map(|s| s.trim().to_string()).collect();
            }
        }

        if let Some(operation) = configs.get("operation") {
            if let Some(op) = operation.downcast_ref::<String>() {
                self.operation = op.clone();
            }
        }

        if let Some(replace) = configs.get("replace.null.with.default") {
            if let Some(r) = replace.downcast_ref::<bool>() {
                self.replace_null_with_default = *r;
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for HeaderFrom<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.fields.clear();
        self.headers.clear();
        self.operation.clear();
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for HeaderFrom<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        let operating_value = record.value();
        let operating_schema = record.value_schema();

        if operating_schema.is_none() {
            // Schemaless
            self.apply_schemaless(record)
        } else {
            // Schema-based
            self.apply_with_schema(record)
        }
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("fields".to_string(), ConfigValue::String(String::new()));
        config.add_config("headers".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            "operation".to_string(),
            ConfigValue::String("copy".to_string()),
        );
        config.add_config(
            "replace.null.with.default".to_string(),
            ConfigValue::Boolean(false),
        );
        config
    }
}

impl<R: ConnectRecord<R>> HeaderFrom<R> {
    fn clone_box(
        &self,
        value: &dyn std::any::Any,
    ) -> Box<dyn std::any::Any + Send + Sync + 'static> {
        if let Some(s) = value.downcast_ref::<String>() {
            Box::new(s.clone())
        } else if let Some(i) = value.downcast_ref::<i8>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i16>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i32>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i64>() {
            Box::new(*i)
        } else if let Some(f) = value.downcast_ref::<f32>() {
            Box::new(*f)
        } else if let Some(f) = value.downcast_ref::<f64>() {
            Box::new(*f)
        } else if let Some(b) = value.downcast_ref::<bool>() {
            Box::new(*b)
        } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
            Box::new(v.clone())
        } else if let Some(v) = value.downcast_ref::<Vec<Box<dyn std::any::Any + Send + Sync>>>() {
            // For Vec<Box<dyn Any>>, we need to downcast each element to a cloneable type
            // Return empty vec as fallback since we can't easily clone Box<dyn Any>
            let mut cloned = Vec::new();
            for item in v {
                if let Some(s) = item.downcast_ref::<String>() {
                    cloned.push(Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(b) = item.downcast_ref::<bool>() {
                    cloned.push(Box::new(*b) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(i) = item.downcast_ref::<i32>() {
                    cloned.push(Box::new(*i) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(i) = item.downcast_ref::<i64>() {
                    cloned.push(Box::new(*i) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(f) = item.downcast_ref::<f64>() {
                    cloned.push(Box::new(*f) as Box<dyn std::any::Any + Send + Sync>);
                }
                // Skip elements we can't clone
            }
            Box::new(cloned)
        } else if let Some(v) =
            value.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
        {
            // For HashMap<String, Box<dyn Any>>, clone the values we can
            let mut cloned = HashMap::new();
            for (k, val) in v.iter() {
                if let Some(s) = val.downcast_ref::<String>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(b) = val.downcast_ref::<bool>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*b) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(i) = val.downcast_ref::<i32>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*i) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(i) = val.downcast_ref::<i64>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*i) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(f) = val.downcast_ref::<f64>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*f) as Box<dyn std::any::Any + Send + Sync>,
                    );
                }
                // Skip values we can't clone
            }
            Box::new(cloned)
        } else {
            Box::new(())
        }
    }

    fn apply_schemaless(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::ConcreteHeader;
        use std::sync::Arc;

        let value = record.value();
        if let Some(value_arc) = value {
            if let Some(value_any) =
                value_arc.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
            {
                // Duplicate headers
                let mut updated_headers = record.headers().duplicate();

                // Create updated value map
                let mut updated_value: HashMap<String, Box<dyn std::any::Any + Send + Sync>> =
                    HashMap::new();
                for (k, v) in value_any.iter() {
                    updated_value.insert(k.clone(), self.clone_box(v.as_ref()));
                }

                // Process each field/header pair
                for i in 0..self.fields.len() {
                    if i >= self.headers.len() {
                        break;
                    }

                    let field_name = &self.fields[i];
                    let header_name = &self.headers[i];

                    // Get field value from original map
                    let field_value = value_any.get(field_name);

                    if let Some(fv) = field_value {
                        // Convert Box<Any> to Arc<Any>
                        let field_value_arc: Arc<dyn std::any::Any + Send + Sync> =
                            if let Some(s) = fv.downcast_ref::<String>() {
                                Arc::new(Box::new(s.clone()))
                            } else if let Some(i) = fv.downcast_ref::<i32>() {
                                Arc::new(Box::new(*i))
                            } else if let Some(i) = fv.downcast_ref::<i64>() {
                                Arc::new(Box::new(*i))
                            } else if let Some(b) = fv.downcast_ref::<bool>() {
                                Arc::new(Box::new(*b))
                            } else if let Some(f) = fv.downcast_ref::<f64>() {
                                Arc::new(Box::new(*f))
                            } else {
                                continue;
                            };

                        // Add header (schemaless has null schema)
                        let null_schema = Arc::new(connect_api::data::ConnectSchema::new(
                            connect_api::data::Type::String,
                        ));
                        let header = Box::new(ConcreteHeader::new(
                            header_name.clone(),
                            null_schema,
                            field_value_arc,
                        ));
                        updated_headers.add(header);

                        // For MOVE operation, remove field from value
                        if self.operation == "move" {
                            updated_value.remove(field_name);
                        }
                    }
                }

                // Create new record with updated value and headers
                let updated_value_arc =
                    Arc::new(Box::new(updated_value) as Box<dyn std::any::Any + Send + Sync>);
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(updated_value_arc),
                    None,
                    Some(updated_headers),
                )));
            }
        }
        Ok(Some(record))
    }

    fn apply_with_schema(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::{
            ConcreteHeader, ConnectSchema, Field, Schema, SchemaBuilder, Struct,
        };
        use std::sync::Arc;

        let value = record.value();
        let value_schema = record.value_schema();

        if let (Some(value_arc), Some(schema_arc)) = (value, value_schema) {
            if let (Some(value_any), Some(schema_ref)) = (
                value_arc.downcast_ref::<Struct>(),
                schema_arc.as_connect_schema(),
            ) {
                // Duplicate headers
                let mut updated_headers = record.headers().duplicate();

                let (updated_schema, updated_value) = if self.operation == "move" {
                    // Build new schema without moved fields
                    let new_schema = self.move_schema(schema_arc.clone())?;
                    let new_schema_connect =
                        Arc::new(new_schema.as_connect_schema().unwrap().clone());

                    // Build new struct without moved fields
                    let mut new_struct = Struct::new(new_schema_connect.clone())
                        .map_err(|e| Box::new(e) as Box<dyn Error>)?;

                    for field in new_schema.fields().unwrap_or_default() {
                        let field_value = self.get_field_value(value_any, &field);
                        if let Some(fv) = field_value {
                            new_struct.put_without_validation(field.name(), fv);
                        }
                    }

                    (new_schema, new_struct)
                } else {
                    // For COPY, keep original schema and value
                    (schema_arc.clone(), value_any.clone())
                };

                // Add headers from fields
                for i in 0..self.fields.len() {
                    if i >= self.headers.len() {
                        break;
                    }

                    let field_name = &self.fields[i];
                    let header_name = &self.headers[i];

                    // Get field from schema
                    if let Ok(field) = schema_ref.field(field_name) {
                        // Get field value
                        let field_value = self.get_field_value(value_any, &field);

                        if let Some(fv) = field_value {
                            // Convert Box<Any> to Arc<Any>
                            let field_value_arc: Arc<dyn std::any::Any + Send + Sync> =
                                if let Some(s) = fv.downcast_ref::<String>() {
                                    Arc::new(Box::new(s.clone()))
                                } else if let Some(i) = fv.downcast_ref::<i32>() {
                                    Arc::new(Box::new(*i))
                                } else if let Some(i) = fv.downcast_ref::<i64>() {
                                    Arc::new(Box::new(*i))
                                } else if let Some(b) = fv.downcast_ref::<bool>() {
                                    Arc::new(Box::new(*b))
                                } else if let Some(f) = fv.downcast_ref::<f64>() {
                                    Arc::new(Box::new(*f))
                                } else {
                                    continue;
                                };

                            // Add header with field schema
                            let field_schema = field.schema();
                            let header = Box::new(ConcreteHeader::new(
                                header_name.clone(),
                                field_schema,
                                field_value_arc,
                            ));
                            updated_headers.add(header);
                        }
                    }
                }

                // Create new record
                let updated_value_arc =
                    Arc::new(Box::new(updated_value) as Box<dyn std::any::Any + Send + Sync>);
                let updated_schema_arc = Arc::clone(&updated_schema) as Arc<dyn Schema>;
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    Some(updated_schema_arc),
                    Some(updated_value_arc),
                    None,
                    Some(updated_headers),
                )));
            }
        }
        Ok(Some(record))
    }

    fn get_field_value(
        &self,
        value: &Struct,
        field: &Field,
    ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        // Use get() which returns field value
        // Note: Rust implementation doesn't have getWithoutDefault like Java
        // So we always use get() for now
        let raw_value = value.get(field.name());
        if let Some(rv) = raw_value {
            if let Some(s) = rv.downcast_ref::<String>() {
                return Some(Box::new(s.clone()));
            } else if let Some(i) = rv.downcast_ref::<i32>() {
                return Some(Box::new(*i));
            } else if let Some(i) = rv.downcast_ref::<i64>() {
                return Some(Box::new(*i));
            } else if let Some(b) = rv.downcast_ref::<bool>() {
                return Some(Box::new(*b));
            } else if let Some(f) = rv.downcast_ref::<f64>() {
                return Some(Box::new(*f));
            }
        }
        None
    }

    fn move_schema(
        &self,
        operating_schema: Arc<dyn Schema>,
    ) -> Result<Arc<dyn Schema>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Field, SchemaBuilder};
        use std::sync::Arc;

        let schema_ref = operating_schema.as_connect_schema().ok_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid schema type".to_string(),
            ))
        })?;

        // Build new schema excluding fields that will be moved
        let mut builder = SchemaBuilder::struct_();
        for field in schema_ref.fields().unwrap_or_default() {
            if !self.fields.contains(&field.name().to_string()) {
                builder = builder.field(
                    field.name().to_string(),
                    Arc::new(field.schema().as_connect_schema().unwrap().clone()),
                );
            }
        }

        let new_schema = builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;
        Ok(Arc::clone(&new_schema) as Arc<dyn Schema>)
    }
}

/// Flatten transformation
///
/// Flattens nested structures.
pub struct Flatten<R: ConnectRecord<R>> {
    delimiter: String,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> Flatten<R> {
    pub fn new() -> Self {
        Self {
            delimiter: ".".to_string(),
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for Flatten<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(delimiter) = configs.get("delimiter") {
            if let Some(d) = delimiter.downcast_ref::<String>() {
                self.delimiter = d.clone();
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for Flatten<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.delimiter.clear();
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for Flatten<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        let operating_value = record.value();
        if operating_value.is_none() {
            return Ok(Some(record));
        }

        let operating_schema = record.value_schema();
        if operating_schema.is_none() {
            self.apply_schemaless(record)
        } else {
            self.apply_with_schema(record)
        }
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config(
            "delimiter".to_string(),
            ConfigValue::String(".".to_string()),
        );
        config
    }
}

impl<R: ConnectRecord<R>> Flatten<R> {
    fn apply_schemaless(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use std::sync::Arc;

        let value = record.value();
        if let Some(value_arc) = value {
            if let Some(value_any) =
                value_arc.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
            {
                let mut new_value = HashMap::new();
                self.apply_schemaless_recursive(value_any, "", &mut new_value);

                let new_value_arc =
                    Arc::new(Box::new(new_value) as Box<dyn std::any::Any + Send + Sync>);
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(new_value_arc),
                    None,
                    None,
                )));
            }
        }
        Ok(Some(record))
    }

    fn apply_schemaless_recursive(
        &self,
        original_record: &HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
        field_name_prefix: &str,
        new_record: &mut HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    ) {
        for (key, value) in original_record.iter() {
            let field_name = self.field_name(field_name_prefix, key);

            if value.is::<()>() {
                new_record.insert(field_name, Box::new(()));
                continue;
            }

            let inferred_type = self.infer_schema_type(value);
            if inferred_type.is_none() {
                continue;
            }

            match inferred_type.unwrap() {
                Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Float32
                | Type::Float64
                | Type::Boolean
                | Type::String
                | Type::Bytes
                | Type::Array => {
                    new_record.insert(field_name, self.clone_value(value));
                }
                Type::Map => {
                    if let Some(map_value) = value
                        .downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
                    {
                        self.apply_schemaless_recursive(map_value, &field_name, new_record);
                    }
                }
                Type::Struct => {
                    if let Some(map_value) = value
                        .downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
                    {
                        self.apply_schemaless_recursive(map_value, &field_name, new_record);
                    }
                }
            }
        }
    }

    fn infer_schema_type(&self, value: &Box<dyn std::any::Any + Send + Sync>) -> Option<Type> {
        if value.is::<i8>() {
            Some(Type::Int8)
        } else if value.is::<i16>() {
            Some(Type::Int16)
        } else if value.is::<i32>() {
            Some(Type::Int32)
        } else if value.is::<i64>() {
            Some(Type::Int64)
        } else if value.is::<f32>() {
            Some(Type::Float32)
        } else if value.is::<f64>() {
            Some(Type::Float64)
        } else if value.is::<bool>() {
            Some(Type::Boolean)
        } else if value.is::<String>() {
            Some(Type::String)
        } else if value.is::<Vec<u8>>() {
            Some(Type::Bytes)
        } else if value.is::<Vec<Box<dyn std::any::Any + Send + Sync>>>() {
            Some(Type::Array)
        } else if value.is::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>() {
            Some(Type::Map)
        } else {
            None
        }
    }

    fn clone_value(
        &self,
        value: &Box<dyn std::any::Any + Send + Sync>,
    ) -> Box<dyn std::any::Any + Send + Sync> {
        if let Some(s) = value.downcast_ref::<String>() {
            Box::new(s.clone())
        } else if let Some(i) = value.downcast_ref::<i32>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i64>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i16>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i8>() {
            Box::new(*i)
        } else if let Some(f) = value.downcast_ref::<f32>() {
            Box::new(*f)
        } else if let Some(f) = value.downcast_ref::<f64>() {
            Box::new(*f)
        } else if let Some(b) = value.downcast_ref::<bool>() {
            Box::new(*b)
        } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
            Box::new(v.clone())
        } else if let Some(v) = value.downcast_ref::<Vec<Box<dyn std::any::Any + Send + Sync>>>() {
            // For Vec<Box<dyn Any>>, we need to downcast each element to a cloneable type
            // Return empty vec as fallback since we can't easily clone Box<dyn Any>
            let mut cloned = Vec::new();
            for item in v {
                if let Some(s) = item.downcast_ref::<String>() {
                    cloned.push(Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(b) = item.downcast_ref::<bool>() {
                    cloned.push(Box::new(*b) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(i) = item.downcast_ref::<i32>() {
                    cloned.push(Box::new(*i) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(i) = item.downcast_ref::<i64>() {
                    cloned.push(Box::new(*i) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(f) = item.downcast_ref::<f64>() {
                    cloned.push(Box::new(*f) as Box<dyn std::any::Any + Send + Sync>);
                }
                // Skip elements we can't clone
            }
            Box::new(cloned)
        } else if let Some(v) =
            value.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
        {
            // For HashMap<String, Box<dyn Any>>, clone the values we can
            let mut cloned = HashMap::new();
            for (k, val) in v.iter() {
                if let Some(s) = val.downcast_ref::<String>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(b) = val.downcast_ref::<bool>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*b) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(i) = val.downcast_ref::<i32>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*i) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(i) = val.downcast_ref::<i64>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*i) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(f) = val.downcast_ref::<f64>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*f) as Box<dyn std::any::Any + Send + Sync>,
                    );
                }
                // Skip values we can't clone
            }
            Box::new(cloned)
        } else {
            Box::new(())
        }
    }

    fn apply_with_schema(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Schema, Struct};
        use std::sync::Arc;

        let value = record.value();
        let value_schema = record.value_schema();

        if let (Some(value_arc), Some(schema_arc)) = (value, value_schema) {
            if let Some(schema_ref) = schema_arc.as_connect_schema() {
                let updated_schema = self.build_updated_schema(
                    schema_arc.clone(),
                    "",
                    schema_ref.is_optional(),
                    None,
                )?;

                if let Some(struct_value) = value_arc.downcast_ref::<Struct>() {
                    let mut updated_value = Struct::new(updated_schema.clone())
                        .map_err(|e| Box::new(e) as Box<dyn Error>)?;
                    self.build_with_schema(struct_value, "", &mut updated_value);

                    let updated_value_arc =
                        Arc::new(Box::new(updated_value) as Box<dyn std::any::Any + Send + Sync>);
                    let updated_schema_arc = Arc::clone(&updated_schema) as Arc<dyn Schema>;
                    return Ok(Some(record.new_record(
                        None,
                        None,
                        None,
                        None,
                        Some(updated_schema_arc),
                        Some(updated_value_arc),
                        None,
                        None,
                    )));
                } else {
                    let updated_schema_arc = Arc::clone(&updated_schema) as Arc<dyn Schema>;
                    return Ok(Some(record.new_record(
                        None,
                        None,
                        None,
                        None,
                        Some(updated_schema_arc),
                        None,
                        None,
                        None,
                    )));
                }
            }
        }
        Ok(Some(record))
    }

    fn build_updated_schema(
        &self,
        schema: Arc<dyn Schema>,
        field_name_prefix: &str,
        optional: bool,
        default_from_parent: Option<&Struct>,
    ) -> Result<Arc<ConnectSchema>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, SchemaBuilder};

        let schema_ref = schema.as_connect_schema().ok_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid schema type",
            ))
        })?;

        let mut builder = SchemaBuilder::struct_();

        for field in schema_ref.fields().unwrap_or_default() {
            let field_name = self.field_name(field_name_prefix, field.name());
            let field_schema_ref = field.schema();
            let field_is_optional = optional || field_schema_ref.is_optional();

            let field_default_value = if let Some(default_val) = field_schema_ref.default_value() {
                Some(default_val)
            } else if let Some(parent_default) = default_from_parent {
                parent_default.get(field.name())
            } else {
                None
            };

            match field_schema_ref.type_() {
                Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Float32
                | Type::Float64
                | Type::Boolean
                | Type::String
                | Type::Bytes
                | Type::Array => {
                    let converted_schema = self.convert_field_schema(
                        Arc::new(field_schema_ref.as_connect_schema().unwrap().clone()),
                        field_is_optional,
                        field_default_value,
                    )?;
                    builder = builder.field(field_name, converted_schema);
                }
                Type::Struct => {
                    let nested_default = if let Some(default_val) = field_default_value {
                        default_val.downcast_ref::<()>()
                    } else {
                        None
                    };
                    let nested_schema = self.build_updated_schema(
                        Arc::clone(&field_schema_ref) as Arc<dyn Schema>,
                        &field_name,
                        field_is_optional,
                        None,
                    )?;
                    for nested_field in nested_schema.fields().unwrap_or_default() {
                        builder = builder.field(
                            nested_field.name().to_string(),
                            Arc::new(nested_field.schema().as_connect_schema().unwrap().clone()),
                        );
                    }
                }
                Type::Map => {
                    let converted_schema = self.convert_field_schema(
                        field.schema(),
                        field_is_optional,
                        field_default_value,
                    )?;
                    builder = builder.field(field_name, converted_schema);
                }
            }
        }

        builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn convert_field_schema(
        &self,
        orig: Arc<ConnectSchema>,
        optional: bool,
        _default_from_parent: Option<&dyn std::any::Any>,
    ) -> Result<Arc<ConnectSchema>, Box<dyn Error>> {
        use connect_api::data::SchemaBuilder;

        let mut builder = match orig.type_() {
            Type::Int8 => SchemaBuilder::int8(),
            Type::Int16 => SchemaBuilder::int16(),
            Type::Int32 => SchemaBuilder::int32(),
            Type::Int64 => SchemaBuilder::int64(),
            Type::Float32 => SchemaBuilder::float32(),
            Type::Float64 => SchemaBuilder::float64(),
            Type::Boolean => SchemaBuilder::boolean(),
            Type::String => SchemaBuilder::string(),
            Type::Bytes => SchemaBuilder::bytes(),
            Type::Array => {
                let value_schema = orig.value_schema().map_err(|e| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Array must have value schema: {}", e),
                    )) as Box<dyn Error>
                })?;
                SchemaBuilder::array(Arc::new(value_schema.as_connect_schema().unwrap().clone()))
            }
            Type::Map => {
                let key_schema = orig.key_schema().map_err(|e| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Map must have key schema: {}", e),
                    )) as Box<dyn Error>
                })?;
                let value_schema = orig.value_schema().map_err(|e| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Map must have value schema: {}", e),
                    )) as Box<dyn Error>
                })?;
                SchemaBuilder::map(
                    Arc::new(key_schema.as_connect_schema().unwrap().clone()),
                    Arc::new(value_schema.as_connect_schema().unwrap().clone()),
                )
            }
            Type::Struct => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Cannot convert struct schema directly",
                )));
            }
        };

        if optional {
            builder = builder.optional();
        }

        builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn build_with_schema(&self, record: &Struct, field_name_prefix: &str, new_record: &mut Struct) {
        for field in record.schema().fields().unwrap_or_default() {
            let field_name = self.field_name(field_name_prefix, field.name());

            match field.schema().type_() {
                Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Float32
                | Type::Float64
                | Type::Boolean
                | Type::String
                | Type::Bytes
                | Type::Array => {
                    if let Some(field_value) = record.get(field.name()) {
                        if let Some(cloned_value) = self.clone_any_value(field_value) {
                            new_record.put_without_validation(&field_name, cloned_value);
                        }
                    }
                }
                Type::Struct => {
                    if let Some(nested_struct) = record
                        .get(field.name())
                        .and_then(|v| v.downcast_ref::<Struct>())
                    {
                        self.build_with_schema(nested_struct, &field_name, new_record);
                    }
                }
                Type::Map => {
                    if let Some(field_value) = record.get(field.name()) {
                        if let Some(cloned_value) = self.clone_any_value(field_value) {
                            new_record.put_without_validation(&field_name, cloned_value);
                        }
                    }
                }
            }
        }
    }

    fn clone_any_value(
        &self,
        value: &dyn std::any::Any,
    ) -> Option<Box<dyn std::any::Any + Send + Sync + 'static>> {
        if let Some(s) = value.downcast_ref::<String>() {
            Some(Box::new(s.clone()))
        } else if let Some(i) = value.downcast_ref::<i32>() {
            Some(Box::new(*i))
        } else if let Some(i) = value.downcast_ref::<i64>() {
            Some(Box::new(*i))
        } else if let Some(i) = value.downcast_ref::<i16>() {
            Some(Box::new(*i))
        } else if let Some(i) = value.downcast_ref::<i8>() {
            Some(Box::new(*i))
        } else if let Some(f) = value.downcast_ref::<f32>() {
            Some(Box::new(*f))
        } else if let Some(f) = value.downcast_ref::<f64>() {
            Some(Box::new(*f))
        } else if let Some(b) = value.downcast_ref::<bool>() {
            Some(Box::new(*b))
        } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
            Some(Box::new(v.clone()))
        } else {
            // For complex types, return None (can't clone)
            None
        }
    }

    fn field_name(&self, prefix: &str, field_name: &str) -> String {
        if prefix.is_empty() {
            field_name.to_string()
        } else {
            format!("{}{}{}", prefix, self.delimiter, field_name)
        }
    }
}

/// ExtractField transformation
///
/// Extracts a field from the value and uses it as the entire value.
pub struct ExtractField<R: ConnectRecord<R>> {
    field: String,
    replace_null_with_default: bool,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> ExtractField<R> {
    pub fn new() -> Self {
        Self {
            field: String::new(),
            replace_null_with_default: false,
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for ExtractField<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(field) = configs.get("field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.field = f.clone();
            }
        }

        if let Some(replace) = configs.get("replace.null.with.default") {
            if let Some(r) = replace.downcast_ref::<bool>() {
                self.replace_null_with_default = *r;
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for ExtractField<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.field.clear();
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for ExtractField<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        let value_schema = record.value_schema();
        let value = record.value();

        if value_schema.is_none() {
            // Schemaless
            self.apply_schemaless(record, value)
        } else {
            // Schema-based
            self.apply_with_schema(record, value_schema, value)
        }
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("field".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            "replace.null.with.default".to_string(),
            ConfigValue::Boolean(false),
        );
        config
    }
}

impl<R: ConnectRecord<R>> ExtractField<R> {
    fn clone_box(
        &self,
        value: &dyn std::any::Any,
    ) -> Box<dyn std::any::Any + Send + Sync + 'static> {
        if let Some(s) = value.downcast_ref::<String>() {
            Box::new(s.clone())
        } else if let Some(i) = value.downcast_ref::<i8>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i16>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i32>() {
            Box::new(*i)
        } else if let Some(i) = value.downcast_ref::<i64>() {
            Box::new(*i)
        } else if let Some(f) = value.downcast_ref::<f32>() {
            Box::new(*f)
        } else if let Some(f) = value.downcast_ref::<f64>() {
            Box::new(*f)
        } else if let Some(b) = value.downcast_ref::<bool>() {
            Box::new(*b)
        } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
            Box::new(v.clone())
        } else if let Some(v) = value.downcast_ref::<Vec<Box<dyn std::any::Any + Send + Sync>>>() {
            // For Vec<Box<dyn Any>>, we need to downcast each element to a cloneable type
            // Return empty vec as fallback since we can't easily clone Box<dyn Any>
            let mut cloned = Vec::new();
            for item in v {
                if let Some(s) = item.downcast_ref::<String>() {
                    cloned.push(Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(b) = item.downcast_ref::<bool>() {
                    cloned.push(Box::new(*b) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(i) = item.downcast_ref::<i32>() {
                    cloned.push(Box::new(*i) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(i) = item.downcast_ref::<i64>() {
                    cloned.push(Box::new(*i) as Box<dyn std::any::Any + Send + Sync>);
                } else if let Some(f) = item.downcast_ref::<f64>() {
                    cloned.push(Box::new(*f) as Box<dyn std::any::Any + Send + Sync>);
                }
                // Skip elements we can't clone
            }
            Box::new(cloned)
        } else if let Some(v) =
            value.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
        {
            // For HashMap<String, Box<dyn Any>>, clone the values we can
            let mut cloned = HashMap::new();
            for (k, val) in v.iter() {
                if let Some(s) = val.downcast_ref::<String>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(b) = val.downcast_ref::<bool>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*b) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(i) = val.downcast_ref::<i32>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*i) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(i) = val.downcast_ref::<i64>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*i) as Box<dyn std::any::Any + Send + Sync>,
                    );
                } else if let Some(f) = val.downcast_ref::<f64>() {
                    cloned.insert(
                        k.clone(),
                        Box::new(*f) as Box<dyn std::any::Any + Send + Sync>,
                    );
                }
                // Skip values we can't clone
            }
            Box::new(cloned)
        } else {
            Box::new(())
        }
    }

    fn apply_schemaless(
        &self,
        record: R,
        value: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Option<R>, Box<dyn Error>> {
        use std::sync::Arc;

        if let Some(value_arc) = value {
            if let Some(value_any) =
                value_arc.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
            {
                if let Some(field_value) = value_any.get(&self.field) {
                    // Extract field value and use it as entire value
                    let extracted_value = Arc::new(self.clone_box(field_value.as_ref()));
                    return Ok(Some(record.new_record(
                        None,
                        None,
                        None,
                        None,
                        None,
                        Some(extracted_value),
                        None,
                        None,
                    )));
                }
            }
        }
        Ok(Some(record))
    }

    fn apply_with_schema(
        &self,
        record: R,
        value_schema: Option<Arc<dyn Schema>>,
        value: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Field, Schema, Struct};
        use std::sync::Arc;

        if let (Some(value_arc), Some(schema_arc)) = (value, value_schema) {
            if let (Some(value_any), Some(schema_ref)) = (
                value_arc.downcast_ref::<Struct>(),
                schema_arc.as_connect_schema(),
            ) {
                // Get field from schema
                let field = schema_ref.field(&self.field);
                if let Ok(field) = field {
                    // Get field value from struct
                    let field_value = if self.replace_null_with_default {
                        value_any.get(&self.field)
                    } else {
                        value_any.get(&self.field)
                    };

                    if let Some(fv) = field_value {
                        // Clone the field value
                        let cloned_value: Box<dyn std::any::Any + Send + Sync> =
                            if let Some(s) = fv.downcast_ref::<String>() {
                                Box::new(s.clone())
                            } else if let Some(i) = fv.downcast_ref::<i32>() {
                                Box::new(*i)
                            } else if let Some(i) = fv.downcast_ref::<i64>() {
                                Box::new(*i)
                            } else if let Some(b) = fv.downcast_ref::<bool>() {
                                Box::new(*b)
                            } else if let Some(f) = fv.downcast_ref::<f64>() {
                                Box::new(*f)
                            } else {
                                return Ok(Some(record));
                            };

                        let extracted_value_arc = Arc::new(cloned_value);
                        let field_schema = field.schema();
                        return Ok(Some(record.new_record(
                            None,
                            None,
                            None,
                            None,
                            Some(field_schema),
                            Some(extracted_value_arc),
                            None,
                            None,
                        )));
                    }
                }
            }
        }
        Ok(Some(record))
    }
}

/// Cast transformation
///

/// Cast transformation
///
/// Casts fields to specified types.
pub struct Cast<R: ConnectRecord<R>> {
    spec: Vec<String>,
    replace_null_with_default: bool,
    casts: HashMap<String, connect_api::data::Type>,
    whole_value_cast_type: Option<connect_api::data::Type>,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> Cast<R> {
    pub fn new() -> Self {
        Self {
            spec: Vec::new(),
            replace_null_with_default: false,
            casts: HashMap::new(),
            whole_value_cast_type: None,
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for Cast<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(spec) = configs.get("spec") {
            if let Some(s) = spec.downcast_ref::<String>() {
                self.spec = s.split(",").map(|s| s.trim().to_string()).collect();
                // Parse spec into casts map
                let parsed = Self::parse_field_types(&self.spec);
                self.casts = parsed.0;
                self.whole_value_cast_type = parsed.1;
            }
        }

        if let Some(replace) = configs.get("replace.null.with.default") {
            if let Some(r) = replace.downcast_ref::<bool>() {
                self.replace_null_with_default = *r;
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for Cast<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.spec.clear();
        self.casts.clear();
        self.whole_value_cast_type = None;
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for Cast<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        use std::sync::Arc;

        // Get operating value and schema (for value transformation)
        let operating_value = record.value();
        let operating_schema = record.value_schema();

        // Handle null value
        if operating_value.is_none() {
            return Ok(Some(record));
        }

        if operating_schema.is_none() {
            // Schemaless transformation
            self.apply_schemaless(record, operating_value)
        } else {
            // Schema-based transformation
            self.apply_with_schema(record, operating_schema, operating_value)
        }
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("spec".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            "replace.null.with.default".to_string(),
            ConfigValue::Boolean(false),
        );
        config
    }
}

impl<R: ConnectRecord<R>> Cast<R> {
    /// Parse field types from spec
    /// Returns (casts map, whole value cast type)
    fn parse_field_types(
        spec: &[String],
    ) -> (
        HashMap<String, connect_api::data::Type>,
        Option<connect_api::data::Type>,
    ) {
        let mut casts = HashMap::new();
        let mut whole_value_cast_type = None;

        for mapping in spec {
            let parts: Vec<&str> = mapping.split(':').collect();

            if parts.len() > 2 {
                panic!("Invalid cast mapping: {}", mapping);
            }

            if parts.len() == 1 {
                // Whole value cast
                let type_str = parts[0].trim().to_uppercase();
                let target_type = Self::parse_type(&type_str);
                whole_value_cast_type = Some(target_type);
            } else {
                // Field cast
                let field_name = parts[0].trim().to_string();
                let type_str = parts[1].trim().to_uppercase();
                let target_type = Self::parse_type(&type_str);
                casts.insert(field_name, target_type);
            }
        }

        if whole_value_cast_type.is_some() && spec.len() > 1 {
            panic!("Cast transformations that specify a type to cast entire' value to may only specify a single cast in their spec");
        }

        (casts, whole_value_cast_type)
    }

    /// Parse type string to Type' enum
    fn parse_type(type_str: &str) -> connect_api::data::Type {
        match type_str {
            "INT8" => connect_api::data::Type::Int8,
            "INT16" => connect_api::data::Type::Int16,
            "INT32" => connect_api::data::Type::Int32,
            "INT64" => connect_api::data::Type::Int64,
            "FLOAT32" => connect_api::data::Type::Float32,
            "FLOAT64" => connect_api::data::Type::Float64,
            "BOOLEAN" => connect_api::data::Type::Boolean,
            "STRING" => connect_api::data::Type::String,
            _ => panic!("Invalid type found in casting spec: {}", type_str),
        }
    }

    /// Apply schemaless transformation
    fn apply_schemaless(
        &self,
        record: R,
        operating_value: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Option<R>, Box<dyn Error>> {
        use std::sync::Arc;

        // Whole value casting
        if let Some(whole_value_type) = self.whole_value_cast_type {
            if let Some(value_arc) = operating_value {
                let casted_value =
                    self.cast_value_to_type(None, value_arc.as_ref(), whole_value_type)?;
                let casted_value_arc = Arc::new(casted_value);
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(casted_value_arc),
                    None,
                    None,
                )));
            }
        }

        // Field casting
        if let Some(value_arc) = operating_value {
            if let Some(value_any) =
                value_arc.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
            {
                let mut updated_value: HashMap<String, Box<dyn std::any::Any + Send + Sync>> =
                    HashMap::new();

                for (field_name, field_value) in value_any.iter() {
                    if let Some(target_type) = self.casts.get(field_name) {
                        let casted_value =
                            self.cast_value_to_type(None, field_value.as_ref(), *target_type)?;
                        updated_value.insert(field_name.clone(), casted_value);
                    } else {
                        // No cast needed, clone original value
                        updated_value
                            .insert(field_name.clone(), self.clone_value(field_value.as_ref()));
                    }
                }

                let updated_value_arc =
                    Arc::new(Box::new(updated_value) as Box<dyn std::any::Any + Send + Sync>);
                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(updated_value_arc),
                    None,
                    None,
                )));
            }
        }

        Ok(Some(record))
    }

    /// Apply schema-based transformation
    fn apply_with_schema(
        &self,
        record: R,
        operating_schema: Option<Arc<dyn connect_api::data::Schema>>,
        operating_value: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Option<R>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Field, Schema, SchemaBuilder, Struct};
        use std::sync::Arc;

        // Whole value casting
        if let Some(whole_value_type) = self.whole_value_cast_type {
            if let Some(ref value_arc) = operating_value {
                if let Some(ref schema_arc) = operating_schema {
                    let casted_value = self.cast_value_to_type(
                        Some(schema_arc.as_ref()),
                        value_arc.as_ref(),
                        whole_value_type,
                    )?;
                    let casted_value_arc = Arc::new(casted_value);

                    // Build new schema for whole value
                    let new_schema = Arc::new(ConnectSchema::new(whole_value_type));

                    return Ok(Some(record.new_record(
                        None,
                        None,
                        None,
                        None,
                        Some(new_schema as Arc<dyn Schema>),
                        Some(casted_value_arc),
                        None,
                        None,
                    )));
                }
            }
        }

        // Field casting
        if let (Some(value_arc), Some(schema_arc)) = (operating_value, operating_schema) {
            if let (Some(value_any), Some(schema_ref)) = (
                value_arc.downcast_ref::<Struct>(),
                schema_arc.as_connect_schema(),
            ) {
                // Build updated schema
                let updated_schema = self.build_updated_schema(schema_arc.clone())?;

                // Build updated struct
                let mut updated_struct = Struct::new(updated_schema.clone())
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;

                for field in schema_ref.fields().unwrap_or_default() {
                    let field_name = field.name();
                    let field_value = value_any.get(field_name);

                    let new_field_value = if let Some(target_type) = self.casts.get(field_name) {
                        if let Some(fv) = field_value {
                            Some(self.cast_value_to_type(
                                Some(field.schema().as_ref()),
                                fv,
                                *target_type,
                            )?)
                        } else {
                            None
                        }
                    } else {
                        // No cast needed, copy original value
                        if let Some(fv) = field_value {
                            Some(self.clone_value(fv))
                        } else {
                            None
                        }
                    };

                    if let Some(nfv) = new_field_value {
                        updated_struct.put_without_validation(field_name, nfv);
                    }
                }

                let updated_value_arc =
                    Arc::new(Box::new(updated_struct) as Box<dyn std::any::Any + Send + Sync>);
                let updated_schema_arc = Arc::clone(&updated_schema) as Arc<dyn Schema>;

                return Ok(Some(record.new_record(
                    None,
                    None,
                    None,
                    None,
                    Some(updated_schema_arc),
                    Some(updated_value_arc),
                    None,
                    None,
                )));
            }
        }

        Ok(Some(record))
    }

    /// Build updated schema with cast field types
    fn build_updated_schema(
        &self,
        original_schema: Arc<dyn connect_api::data::Schema>,
    ) -> Result<Arc<connect_api::data::ConnectSchema>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Field, SchemaBuilder};

        let schema_ref = match original_schema.as_connect_schema() {
            Some(s) => s,
            None => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid schema type",
                )));
            }
        };

        let mut builder = SchemaBuilder::struct_();

        for field in schema_ref.fields().unwrap_or_default() {
            let field_name = field.name();
            let field_schema = field.schema();

            if let Some(target_type) = self.casts.get(field_name) {
                // Build new schema with cast type
                let mut type_builder = Self::convert_field_type(*target_type);
                if field_schema.is_optional() {
                    type_builder = type_builder.optional();
                }
                // Note: Default value handling omitted for simplicity
                let new_field_schema = type_builder
                    .build()
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;
                builder = builder.field(field_name.to_string(), new_field_schema);
            } else {
                // Keep original field schema
                builder = builder.field(
                    field_name.to_string(),
                    Arc::new(field_schema.as_connect_schema().unwrap().clone()),
                );
            }
        }

        builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    /// Convert field type to SchemaBuilder
    fn convert_field_type(type_: connect_api::data::Type) -> SchemaBuilder {
        match type_ {
            connect_api::data::Type::Int8 => SchemaBuilder::int8(),
            connect_api::data::Type::Int16 => SchemaBuilder::int16(),
            connect_api::data::Type::Int32 => SchemaBuilder::int32(),
            connect_api::data::Type::Int64 => SchemaBuilder::int64(),
            connect_api::data::Type::Float32 => SchemaBuilder::float32(),
            connect_api::data::Type::Float64 => SchemaBuilder::float64(),
            connect_api::data::Type::Boolean => SchemaBuilder::boolean(),
            connect_api::data::Type::String => SchemaBuilder::string(),
            _ => panic!("Unexpected type in Cast transformation: {:?}", type_),
        }
    }

    /// Cast value to target type
    fn cast_value_to_type(
        &self,
        schema: Option<&dyn connect_api::data::Schema>,
        value: &dyn std::any::Any,
        target_type: connect_api::data::Type,
    ) -> Result<Box<dyn std::any::Any + Send + Sync>, Box<dyn Error>> {
        match target_type {
            connect_api::data::Type::Int8 => Ok(Box::new(self.cast_to_int8(value)?)),
            connect_api::data::Type::Int16 => Ok(Box::new(self.cast_to_int16(value)?)),
            connect_api::data::Type::Int32 => Ok(Box::new(self.cast_to_int32(value)?)),
            connect_api::data::Type::Int64 => Ok(Box::new(self.cast_to_int64(value)?)),
            connect_api::data::Type::Float32 => Ok(Box::new(self.cast_to_float32(value)?)),
            connect_api::data::Type::Float64 => Ok(Box::new(self.cast_to_float64(value)?)),
            connect_api::data::Type::Boolean => Ok(Box::new(self.cast_to_boolean(value)?)),
            connect_api::data::Type::String => Ok(Box::new(self.cast_to_string(value)?)),
            _ => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{:?} is not supported in Cast transformation", target_type),
            ))),
        }
    }

    /// Cast to i8
    fn cast_to_int8(&self, value: &dyn std::any::Any) -> Result<i8, Box<dyn Error>> {
        if let Some(v) = value.downcast_ref::<i8>() {
            Ok(*v)
        } else if let Some(v) = value.downcast_ref::<i16>() {
            Ok(*v as i8)
        } else if let Some(v) = value.downcast_ref::<i32>() {
            Ok(*v as i8)
        } else if let Some(v) = value.downcast_ref::<i64>() {
            Ok(*v as i8)
        } else if let Some(v) = value.downcast_ref::<f32>() {
            Ok(*v as i8)
        } else if let Some(v) = value.downcast_ref::<f64>() {
            Ok(*v as i8)
        } else if let Some(v) = value.downcast_ref::<bool>() {
            Ok(if *v { 1 } else { 0 })
        } else if let Some(v) = value.downcast_ref::<String>() {
            v.parse::<i8>().map_err(|e| Box::new(e) as Box<dyn Error>)
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unexpected type in Cast transformation"),
            )))
        }
    }

    /// Cast to i16
    fn cast_to_int16(&self, value: &dyn std::any::Any) -> Result<i16, Box<dyn Error>> {
        if let Some(v) = value.downcast_ref::<i8>() {
            Ok(*v as i16)
        } else if let Some(v) = value.downcast_ref::<i16>() {
            Ok(*v)
        } else if let Some(v) = value.downcast_ref::<i32>() {
            Ok(*v as i16)
        } else if let Some(v) = value.downcast_ref::<i64>() {
            Ok(*v as i16)
        } else if let Some(v) = value.downcast_ref::<f32>() {
            Ok(*v as i16)
        } else if let Some(v) = value.downcast_ref::<f64>() {
            Ok(*v as i16)
        } else if let Some(v) = value.downcast_ref::<bool>() {
            Ok(if *v { 1 } else { 0 })
        } else if let Some(v) = value.downcast_ref::<String>() {
            v.parse::<i16>().map_err(|e| Box::new(e) as Box<dyn Error>)
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unexpected type in Cast transformation"),
            )))
        }
    }

    /// Cast to i32
    fn cast_to_int32(&self, value: &dyn std::any::Any) -> Result<i32, Box<dyn Error>> {
        if let Some(v) = value.downcast_ref::<i8>() {
            Ok(*v as i32)
        } else if let Some(v) = value.downcast_ref::<i16>() {
            Ok(*v as i32)
        } else if let Some(v) = value.downcast_ref::<i32>() {
            Ok(*v)
        } else if let Some(v) = value.downcast_ref::<i64>() {
            Ok(*v as i32)
        } else if let Some(v) = value.downcast_ref::<f32>() {
            Ok(*v as i32)
        } else if let Some(v) = value.downcast_ref::<f64>() {
            Ok(*v as i32)
        } else if let Some(v) = value.downcast_ref::<bool>() {
            Ok(if *v { 1 } else { 0 })
        } else if let Some(v) = value.downcast_ref::<String>() {
            v.parse::<i32>().map_err(|e| Box::new(e) as Box<dyn Error>)
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unexpected type in Cast transformation"),
            )))
        }
    }

    /// Cast to i64
    fn cast_to_int64(&self, value: &dyn std::any::Any) -> Result<i64, Box<dyn Error>> {
        if let Some(v) = value.downcast_ref::<i8>() {
            Ok(*v as i64)
        } else if let Some(v) = value.downcast_ref::<i16>() {
            Ok(*v as i64)
        } else if let Some(v) = value.downcast_ref::<i32>() {
            Ok(*v as i64)
        } else if let Some(v) = value.downcast_ref::<i64>() {
            Ok(*v)
        } else if let Some(v) = value.downcast_ref::<f32>() {
            Ok(*v as i64)
        } else if let Some(v) = value.downcast_ref::<f64>() {
            Ok(*v as i64)
        } else if let Some(v) = value.downcast_ref::<bool>() {
            Ok(if *v { 1 } else { 0 })
        } else if let Some(v) = value.downcast_ref::<String>() {
            v.parse::<i64>().map_err(|e| Box::new(e) as Box<dyn Error>)
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unexpected type in Cast transformation"),
            )))
        }
    }

    /// Cast to f32
    fn cast_to_float32(&self, value: &dyn std::any::Any) -> Result<f32, Box<dyn Error>> {
        if let Some(v) = value.downcast_ref::<i8>() {
            Ok(*v as f32)
        } else if let Some(v) = value.downcast_ref::<i16>() {
            Ok(*v as f32)
        } else if let Some(v) = value.downcast_ref::<i32>() {
            Ok(*v as f32)
        } else if let Some(v) = value.downcast_ref::<i64>() {
            Ok(*v as f32)
        } else if let Some(v) = value.downcast_ref::<f32>() {
            Ok(*v)
        } else if let Some(v) = value.downcast_ref::<f64>() {
            Ok(*v as f32)
        } else if let Some(v) = value.downcast_ref::<bool>() {
            Ok(if *v { 1.0 } else { 0.0 })
        } else if let Some(v) = value.downcast_ref::<String>() {
            v.parse::<f32>().map_err(|e| Box::new(e) as Box<dyn Error>)
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unexpected type in Cast transformation"),
            )))
        }
    }

    /// Cast to f64
    fn cast_to_float64(&self, value: &dyn std::any::Any) -> Result<f64, Box<dyn Error>> {
        if let Some(v) = value.downcast_ref::<i8>() {
            Ok(*v as f64)
        } else if let Some(v) = value.downcast_ref::<i16>() {
            Ok(*v as f64)
        } else if let Some(v) = value.downcast_ref::<i32>() {
            Ok(*v as f64)
        } else if let Some(v) = value.downcast_ref::<i64>() {
            Ok(*v as f64)
        } else if let Some(v) = value.downcast_ref::<f32>() {
            Ok(*v as f64)
        } else if let Some(v) = value.downcast_ref::<f64>() {
            Ok(*v)
        } else if let Some(v) = value.downcast_ref::<bool>() {
            Ok(if *v { 1.0 } else { 0.0 })
        } else if let Some(v) = value.downcast_ref::<String>() {
            v.parse::<f64>().map_err(|e| Box::new(e) as Box<dyn Error>)
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unexpected type in Cast transformation"),
            )))
        }
    }

    /// Cast to bool
    fn cast_to_boolean(&self, value: &dyn std::any::Any) -> Result<bool, Box<dyn Error>> {
        if let Some(v) = value.downcast_ref::<i8>() {
            Ok(*v != 0)
        } else if let Some(v) = value.downcast_ref::<i16>() {
            Ok(*v != 0)
        } else if let Some(v) = value.downcast_ref::<i32>() {
            Ok(*v != 0)
        } else if let Some(v) = value.downcast_ref::<i64>() {
            Ok(*v != 0)
        } else if let Some(v) = value.downcast_ref::<f32>() {
            Ok(*v != 0.0)
        } else if let Some(v) = value.downcast_ref::<f64>() {
            Ok(*v != 0.0)
        } else if let Some(v) = value.downcast_ref::<bool>() {
            Ok(*v)
        } else if let Some(v) = value.downcast_ref::<String>() {
            Ok(v.parse::<bool>().unwrap_or(false))
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unexpected type in Cast transformation"),
            )))
        }
    }

    /// Cast to String
    fn cast_to_string(&self, value: &dyn std::any::Any) -> Result<String, Box<dyn Error>> {
        if let Some(v) = value.downcast_ref::<i8>() {
            Ok(v.to_string())
        } else if let Some(v) = value.downcast_ref::<i16>() {
            Ok(v.to_string())
        } else if let Some(v) = value.downcast_ref::<i32>() {
            Ok(v.to_string())
        } else if let Some(v) = value.downcast_ref::<i64>() {
            Ok(v.to_string())
        } else if let Some(v) = value.downcast_ref::<f32>() {
            Ok(v.to_string())
        } else if let Some(v) = value.downcast_ref::<f64>() {
            Ok(v.to_string())
        } else if let Some(v) = value.downcast_ref::<bool>() {
            Ok(v.to_string())
        } else if let Some(v) = value.downcast_ref::<String>() {
            Ok(v.clone())
        } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
            // Base64 encode bytes
            use base64::{engine::general_purpose::STANDARD, Engine as _};
            Ok(STANDARD.encode(v))
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unexpected type in Cast transformation"),
            )))
        }
    }

    /// Clone a value
    fn clone_value(&self, value: &dyn std::any::Any) -> Box<dyn std::any::Any + Send + Sync> {
        if let Some(v) = value.downcast_ref::<String>() {
            Box::new(v.clone())
        } else if let Some(v) = value.downcast_ref::<i8>() {
            Box::new(*v)
        } else if let Some(v) = value.downcast_ref::<i16>() {
            Box::new(*v)
        } else if let Some(v) = value.downcast_ref::<i32>() {
            Box::new(*v)
        } else if let Some(v) = value.downcast_ref::<i64>() {
            Box::new(*v)
        } else if let Some(v) = value.downcast_ref::<f32>() {
            Box::new(*v)
        } else if let Some(v) = value.downcast_ref::<f64>() {
            Box::new(*v)
        } else if let Some(v) = value.downcast_ref::<bool>() {
            Box::new(*v)
        } else {
            panic!("Unsupported type for cloning");
        }
    }
}
