//! Simple transformations
//!
//! This module provides simple built-in transformation implementations.

use chrono::DateTime;
use connect_api::connector_types::ConnectRecord;
use connect_api::data::{ConcreteHeaders, Schema as ConnectSchemaTrait};
use connect_api::error::DataException;
use connect_api::{
    Closeable, ConfigDef, ConfigValue, Configurable, Headers, Schema, Transformation,
};
use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

/// ValueToKey transformation
///
/// Extracts fields from the value and uses them as the key.
pub struct ValueToKey<R: ConnectRecord<R>> {
    fields: Vec<String>,
    replace_null_with_default: bool,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> ValueToKey<R> {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            replace_null_with_default: true, // Default to true as per Java
            _phantom: PhantomData,
        }
    }

    /// Extract key fields from schemaless record (Map-based)
    fn extract_key_from_schemaless(
        &self,
        value: Option<&Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Option<(
        HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
        Vec<String>,
    )> {
        let value = value?;
        let value_map =
            value.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()?;

        let mut key_map = HashMap::new();
        let mut missing_fields = Vec::new();

        for field in &self.fields {
            if let Some(field_value) = value_map.get(field) {
                // Clone the value
                let cloned = if let Some(s) = field_value.downcast_ref::<String>() {
                    Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(i) = field_value.downcast_ref::<i32>() {
                    Box::new(*i) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(i) = field_value.downcast_ref::<i64>() {
                    Box::new(*i) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(b) = field_value.downcast_ref::<bool>() {
                    Box::new(*b) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(f) = field_value.downcast_ref::<f64>() {
                    Box::new(*f) as Box<dyn std::any::Any + Send + Sync>
                } else {
                    // For other types, use empty string as fallback
                    Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>
                };
                key_map.insert(field.clone(), cloned);
            } else if self.replace_null_with_default {
                // Use default value (null for now)
                missing_fields.push(field.clone());
            }
        }

        Some((key_map, missing_fields))
    }

    /// Simplified version that extracts key without building schema
    fn extract_key_from_schema_based_simple(
        &self,
        value: Option<&Arc<dyn std::any::Any + Send + Sync>>,
        _value_schema: Option<&Arc<dyn connect_api::data::Schema>>,
    ) -> Option<(
        HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
        Vec<String>,
    )> {
        let value = value?;
        let value_struct = value.downcast_ref::<connect_api::data::Struct>()?;

        let mut key_map = HashMap::new();
        let mut missing_fields = Vec::new();

        for field_name in &self.fields {
            let field_value = value_struct.get(field_name);

            if let Some(fv) = field_value {
                let cloned = if let Some(s) = fv.downcast_ref::<String>() {
                    Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(i) = fv.downcast_ref::<i32>() {
                    Box::new(*i) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(i) = fv.downcast_ref::<i64>() {
                    Box::new(*i) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(b) = fv.downcast_ref::<bool>() {
                    Box::new(*b) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(f) = fv.downcast_ref::<f64>() {
                    Box::new(*f) as Box<dyn std::any::Any + Send + Sync>
                } else {
                    Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>
                };
                key_map.insert(field_name.clone(), cloned);
            } else if self.replace_null_with_default {
                missing_fields.push(field_name.clone());
            }
        }

        Some((key_map, missing_fields))
    }
}

impl<R: ConnectRecord<R>> Configurable for ValueToKey<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(fields) = configs.get("fields") {
            if let Some(fields_str) = fields.downcast_ref::<String>() {
                self.fields = fields_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
            } else if let Some(fields_vec) = fields.downcast_ref::<Vec<String>>() {
                self.fields = fields_vec.clone();
            }
        }

        if let Some(replace) = configs.get("replace.null.with.default") {
            if let Some(r) = replace.downcast_ref::<bool>() {
                self.replace_null_with_default = *r;
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for ValueToKey<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.fields.clear();
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for ValueToKey<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        // Check if fields are configured
        if self.fields.is_empty() {
            return Ok(Some(record));
        }

        // Collect all needed values before moving record
        let topic = record.topic().to_string();
        let kafka_partition = record.kafka_partition();
        let value_schema = record.value_schema();
        let value = record.value();
        let timestamp = record.timestamp();

        if value_schema.is_none() {
            // Schemaless: use HashMap-based key
            if let Some((key_map, _)) = self.extract_key_from_schemaless(value.as_ref()) {
                let key_arc = Arc::new(Box::new(key_map) as Box<dyn std::any::Any + Send + Sync>);
                let new_record = record.new_record(
                    Some(&topic),
                    kafka_partition,
                    None,
                    Some(key_arc),
                    None,
                    value,
                    timestamp,
                    None,
                );
                return Ok(Some(new_record));
            }
        } else {
            // Schema-based: for now, just use the key value without schema
            // (Full implementation would build key schema from value schema)
            if let Some((key_map, _)) =
                self.extract_key_from_schema_based_simple(value.as_ref(), value_schema.as_ref())
            {
                let key_arc = Arc::new(Box::new(key_map) as Box<dyn std::any::Any + Send + Sync>);
                let new_record = record.new_record(
                    Some(&topic),
                    kafka_partition,
                    None,
                    Some(key_arc),
                    value_schema,
                    value,
                    timestamp,
                    None,
                );
                return Ok(Some(new_record));
            }
        }

        Ok(Some(record))
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("fields".to_string(), ConfigValue::List(Vec::new()));
        config.add_config(
            "replace.null.with.default".to_string(),
            ConfigValue::Boolean(true),
        );
        config
    }
}

/// TimestampRouter transformation
///
/// Routes records to topics based on timestamp.
pub struct TimestampRouter<R: ConnectRecord<R>> {
    topic_format: String,
    timestamp_format: String,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> TimestampRouter<R> {
    pub fn new() -> Self {
        Self {
            topic_format: "${topic}-${timestamp}".to_string(),
            timestamp_format: "yyyyMMdd".to_string(),
            _phantom: PhantomData,
        }
    }

    /// Convert SimpleDateFormat pattern to chrono strftime pattern
    /// This is a simplified conversion that handles common patterns
    fn convert_simple_date_format_to_strftime(&self, pattern: &str) -> String {
        let mut result = pattern.to_string();

        // Common SimpleDateFormat patterns to strftime patterns
        // Note: This is not a complete conversion, but handles the most common cases
        result = result.replace("yyyy", "%Y"); // 4-digit year
        result = result.replace("yy", "%y"); // 2-digit year
        result = result.replace("MM", "%m"); // month (01-12)
        result = result.replace("dd", "%d"); // day of month (01-31)
        result = result.replace("HH", "%H"); // hour (00-23)
        result = result.replace("hh", "%I"); // hour (01-12)
        result = result.replace("mm", "%M"); // minute (00-59)
        result = result.replace("ss", "%S"); // second (00-59)
        result = result.replace("SSS", "%3f"); // milliseconds
        result = result.replace("EEE", "%a"); // abbreviated weekday name
        result = result.replace("EEEE", "%A"); // full weekday name

        result
    }

    /// Format timestamp according to configured timestamp_format
    fn format_timestamp(&self, timestamp: i64) -> Result<String, Box<dyn Error>> {
        // Convert timestamp (milliseconds since epoch) to DateTime
        let secs = timestamp / 1000;
        let nsecs = ((timestamp % 1000) * 1_000_000) as u32;

        let dt = DateTime::from_timestamp(secs, nsecs)
            .ok_or_else(|| DataException::new("Invalid timestamp value".to_string()))?;

        // Convert SimpleDateFormat pattern to strftime pattern
        let strftime_pattern = self.convert_simple_date_format_to_strftime(&self.timestamp_format);

        // Format datetime
        let formatted = dt.format(&strftime_pattern).to_string();

        Ok(formatted)
    }

    /// Format topic name by replacing placeholders with actual values
    fn format_topic_name(&self, topic: &str, formatted_timestamp: &str) -> String {
        self.topic_format
            .replace("${topic}", topic)
            .replace("${timestamp}", formatted_timestamp)
    }
}

impl<R: ConnectRecord<R>> Configurable for TimestampRouter<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(format) = configs.get("topic.format") {
            if let Some(f) = format.downcast_ref::<String>() {
                self.topic_format = f.clone();
            }
        }

        if let Some(format) = configs.get("timestamp.format") {
            if let Some(f) = format.downcast_ref::<String>() {
                self.timestamp_format = f.clone();
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for TimestampRouter<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

impl<R> Transformation<R> for TimestampRouter<R>
where
    R: ConnectRecord<R>,
{
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        // Get timestamp from record
        let timestamp = record.timestamp().ok_or_else(|| {
            DataException::new(format!(
                "Timestamp missing on record with topic: {}",
                record.topic()
            ))
        })?;

        // Format timestamp according to configured format
        let formatted_timestamp = self.format_timestamp(timestamp)?;

        // Get original topic name
        let original_topic = record.topic();

        // Format new topic name by replacing placeholders
        let new_topic = self.format_topic_name(original_topic, &formatted_timestamp);

        // Collect all needed values before moving record
        let kafka_partition = record.kafka_partition();
        let key_schema = record.key_schema();
        let key = record.key();
        let value_schema = record.value_schema();
        let value = record.value();

        // Create a new record with updated topic
        let new_record = record.new_record(
            Some(&new_topic),
            kafka_partition,
            key_schema,
            key,
            value_schema,
            value,
            Some(timestamp),
            None,
        );

        Ok(Some(new_record))
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config(
            "topic.format".to_string(),
            ConfigValue::String("${topic}-${timestamp}".to_string()),
        );
        config.add_config(
            "timestamp.format".to_string(),
            ConfigValue::String("yyyyMMdd".to_string()),
        );
        config
    }
}

/// InsertHeader transformation
///
/// Inserts a header with a static value into records.
pub struct InsertHeader<R: ConnectRecord<R>> {
    header: String,
    value: Option<String>,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> InsertHeader<R> {
    pub fn new() -> Self {
        Self {
            header: String::new(),
            value: None,
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for InsertHeader<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        // Match Java config keys: "header" and "value.literal"
        if let Some(header) = configs.get("header") {
            if let Some(h) = header.downcast_ref::<String>() {
                self.header = h.clone();
            }
        }

        if let Some(value) = configs.get("value.literal") {
            if let Some(v) = value.downcast_ref::<String>() {
                self.value = Some(v.clone());
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for InsertHeader<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.header.clear();
        self.value = None;
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for InsertHeader<R> {
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        // Check if header and value are configured
        if self.header.is_empty() || self.value.is_none() {
            return Ok(Some(record));
        }

        // Collect all needed values before moving record
        let topic = record.topic().to_string();
        let kafka_partition = record.kafka_partition();
        let key_schema = record.key_schema();
        let key = record.key();
        let value_schema = record.value_schema();
        let value = record.value();
        let timestamp = record.timestamp();

        // Get existing headers and duplicate them
        let existing_headers = record.headers();
        let mut updated_headers = existing_headers.duplicate();

        // Create a header with string schema
        let header_schema = Arc::new(connect_api::data::ConnectSchema::new(
            connect_api::data::Type::String,
        ));
        let header_value =
            Arc::new(Box::new(self.value.clone().unwrap()) as Box<dyn std::any::Any + Send + Sync>);

        // Create and add the header
        let header = Box::new(connect_api::data::ConcreteHeader::new(
            self.header.clone(),
            header_schema,
            header_value,
        ));
        updated_headers.add(header);

        // Create new record with updated headers
        let new_record = record.new_record(
            Some(&topic),
            kafka_partition,
            key_schema,
            key,
            value_schema,
            value,
            timestamp,
            Some(updated_headers),
        );

        Ok(Some(new_record))
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("header".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            "value.literal".to_string(),
            ConfigValue::String(String::new()),
        );
        config
    }
}

/// Filter transformation
///
/// Filters out all records (always returns None).
pub struct Filter<R: ConnectRecord<R>> {
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> Filter<R> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for Filter<R> {
    fn configure(&mut self, _configs: HashMap<String, Box<dyn std::any::Any>>) {
        // Filter doesn't need configuration for now
    }
}

impl<R: ConnectRecord<R>> Closeable for Filter<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for Filter<R> {
    fn apply(&mut self, _record: R) -> Result<Option<R>, Box<dyn Error>> {
        // Filter out all records
        Ok(None)
    }

    fn config(&self) -> ConfigDef {
        // Filter doesn't require any configuration
        ConfigDef::new()
    }
}

/// DropHeaders transformation
///
/// Drops specified headers from records.
pub struct DropHeaders<R: ConnectRecord<R>> {
    headers: Vec<String>,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> DropHeaders<R> {
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for DropHeaders<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(headers) = configs.get("headers") {
            if let Some(h) = headers.downcast_ref::<String>() {
                self.headers = h.split(',').map(|s| s.trim().to_string()).collect();
            } else if let Some(h_vec) = headers.downcast_ref::<Vec<String>>() {
                self.headers = h_vec.clone();
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for DropHeaders<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.headers.clear();
        Ok(())
    }
}

impl<R> Transformation<R> for DropHeaders<R>
where
    R: ConnectRecord<R>,
{
    fn apply(&mut self, mut record: R) -> Result<Option<R>, Box<dyn Error>> {
        // Get current headers from record
        let current_headers = record.headers();

        // If no headers to drop or no headers in record, return as-is
        if self.headers.is_empty() || current_headers.is_empty() {
            return Ok(Some(record));
        }

        // Collect all needed values before moving record
        let topic = record.topic().to_string();
        let kafka_partition = record.kafka_partition();
        let key_schema = record.key_schema();
        let key = record.key();
        let value_schema = record.value_schema();
        let value = record.value();
        let timestamp = record.timestamp();

        // Create new headers by filtering out the ones to drop
        let mut new_headers = ConcreteHeaders::new();
        for header in current_headers.iter() {
            let header_key = header.key();
            // Only add headers that are not in the drop list
            if !self.headers.contains(&header_key.to_string()) {
                new_headers.add(header.with(header.schema(), header.value()));
            }
        }

        // Create new record with filtered headers
        let new_record = record.new_record(
            Some(&topic),
            kafka_partition,
            key_schema,
            key,
            value_schema,
            value,
            timestamp,
            Some(Box::new(new_headers) as Box<dyn connect_api::data::Headers>),
        );

        Ok(Some(new_record))
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("headers".to_string(), ConfigValue::String(String::new()));
        config
    }
}
