//! HeaderFrom transformation
//!
//! Moves or copies fields in key/value of a record into that record's headers.
//!
//! Use concrete transformation type designed for:
//! - key (<code>Key.class.getName()</code>) - extracts from key and puts into headers
//! - value (<code>Value.class.getName()</code>) - extracts from value and puts into headers
//!
//! ## Configuration
//!
//! - <code>fields</code> (required): List of field names to move/copy
//! - <code>operation</code> (required): Operation type, either "move" or "copy"
//! - <code>headers</code> (optionalList): List of header names to operate on (same order as field names)
//! - <code>replace.null.with.default</code> (optional): Whether to replace fields that have a default value and that are null to default value. When set to true, default value is used, otherwise null is used.
//!
//! ## Example
//!
//! ```toml
//! [connect.transforms.HeaderFrom]
//! fields=firstname,lastname
//! operation=copy
//! replace.null.with.default=true
//!
//! [connect.transforms.HeaderFrom]
//! fields=username
//! operation=move
//! ```
//!
//! This will copy 'firstname' and 'lastname' fields from key into headers, and move 'username' field from value into headers.

use connect_api::connector_types::ConnectRecord;
use connect_api::data::{ConcreteHeader, Schema};
use connect_api::{Closeable, ConfigDef, ConfigValue, Configurable, Transformation};
use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

/// Operation type for HeaderFrom transformation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Move operation - removes field from key/value
    Move,
    /// Copy operation - copies field from key/value to headers
    Copy,
}

/// HeaderFrom transformation
///
/// Moves or copies fields in key/value of a record into that record's headers.
pub struct HeaderFrom<R: ConnectRecord<R>> {
    /// List of field names to operate on
    fields: Vec<String>,
    /// Operation type (move or copy)
    operation: Operation,
    /// Whether to replace null fields with default value
    replace_null_with_default: bool,
    /// Headers to operate on (same order as fields)
    headers: Vec<String>,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> HeaderFrom<R> {
    /// Creates a new HeaderFrom transformation
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            operation: Operation::Copy,
            replace_null_with_default: false,
            headers: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Creates a new HeaderFrom transformation with specified fields
    pub fn with_fields(fields: Vec<String>) -> Self {
        let mut transform = Self::new();
        transform.fields = fields;
        transform
    }

    /// Creates a new HeaderFrom transformation with specified operation
    pub fn with_operation(operation: Operation) -> Self {
        let mut transform = Self::new();
        transform.operation = operation;
        transform
    }

    /// Creates a new HeaderFrom transformation with specified replace null with default flag
    pub fn with_replace_null_with_default(mut self, replace: bool) -> Self {
        self.replace_null_with_default = replace;
        self
    }

    /// Creates a new HeaderFrom transformation with specified headers
    pub fn with_headers(mut self, headers: Vec<String>) -> Self {
        self.headers = headers;
        self
    }
}

impl<R: ConnectRecord<R>> Configurable for HeaderFrom<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(fields) = configs.get("fields") {
            if let Some(f) = fields.downcast_ref::<Vec<String>>() {
                self.fields = f.clone();
            } else if let Some(f) = fields.downcast_ref::<String>() {
                self.fields = f.split(',').map(|s| s.trim().to_string()).collect();
            }
        }

        if let Some(op) = configs.get("operation") {
            if let Some(o) = op.downcast_ref::<String>() {
                if o == "move" {
                    self.operation = Operation::Move;
                } else if o == "copy" {
                    self.operation = Operation::Copy;
                }
            }
        }

        if let Some(replace) = configs.get("replace.null.with.default") {
            if let Some(r) = replace.downcast_ref::<bool>() {
                self.replace_null_with_default = *r;
            }
        }

        if let Some(headers) = configs.get("headers") {
            if let Some(h) = headers.downcast_ref::<Vec<String>>() {
                self.headers = h.clone();
            } else if let Some(h) = headers.downcast_ref::<String>() {
                self.headers = h.split(',').map(|s| s.trim().to_string()).collect();
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for HeaderFrom<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.fields.clear();
        self.headers.clear();
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for HeaderFrom<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        let operating_value = self.operating_value(&record);

        if operating_value.is_none() {
            return Ok(Some(record));
        }

        let operating_schema = self.operating_schema(&record);

        if operating_schema.is_none() {
            // Schemaless case
            if let Some(value) = operating_value {
                if let Some(value_any) =
                    value.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
                {
                    return self.apply_schemaless(&record, value_any);
                }
            }
        } else {
            // Schema-based case
            if let (Some(value), Some(schema)) = (operating_value, operating_schema) {
                if let Some(value_any) = value.downcast_ref::<connect_api::data::Struct>() {
                    if let Some(schema_ref) = schema.as_connect_schema() {
                        return self.apply_with_schema(&record, value_any, schema_ref);
                    }
                }
            }
        }

        Ok(Some(record))
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("fields".to_string(), ConfigValue::List(Vec::new()));
        config.add_config(
            "operation".to_string(),
            ConfigValue::String("copy".to_string()),
        );
        config.add_config(
            "replace.null.with.default".to_string(),
            ConfigValue::Boolean(false),
        );
        config.add_config("headers".to_string(), ConfigValue::List(Vec::new()));
        config
    }
}

impl<R: ConnectRecord<R>> HeaderFrom<R> {
    /// Schemaless application
    fn apply_schemaless(
        &self,
        record: &R,
        value_map: &HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Option<R>, Box<dyn Error>> {
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

        // Create a string schema for headers
        let header_schema = Arc::new(connect_api::data::ConnectSchema::new(
            connect_api::data::Type::String,
        ));

        // Add fields to headers
        for (field_name, header_name) in self.field_and_header_iter() {
            if let Some(field_value) = value_map.get(&field_name) {
                // Clone the value
                let cloned_value = self.clone_box(field_value.as_ref());
                let header_value = Arc::new(cloned_value) as Arc<dyn std::any::Any + Send + Sync>;

                // Create and add header
                let header = Box::new(ConcreteHeader::new(
                    header_name.clone(),
                    header_schema.clone(),
                    header_value,
                ));
                updated_headers.add(header);
            }
        }

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

    /// Schema-based application
    fn apply_with_schema(
        &self,
        record: &R,
        struct_value: &connect_api::data::Struct,
        schema_ref: &connect_api::data::ConnectSchema,
    ) -> Result<Option<R>, Box<dyn Error>> {
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

        // Create a string schema for headers
        let header_schema = Arc::new(connect_api::data::ConnectSchema::new(
            connect_api::data::Type::String,
        ));

        // Add fields to headers
        for (field_name, header_name) in self.field_and_header_iter() {
            let field_value = struct_value.get(&field_name);

            if let Some(fv) = field_value {
                let field_schema = schema_ref.field(&field_name);

                if field_schema.is_ok() {
                    // Clone the value
                    let cloned_value = self.clone_box(fv);
                    let header_value =
                        Arc::new(cloned_value) as Arc<dyn std::any::Any + Send + Sync>;

                    // Create and add header
                    let header = Box::new(ConcreteHeader::new(
                        header_name.clone(),
                        header_schema.clone(),
                        header_value,
                    ));
                    updated_headers.add(header);
                }
            }
        }

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

    /// Iterator over field names and header names
    fn field_and_header_iter(&self) -> Vec<(String, String)> {
        let mut result = Vec::new();

        for i in 0..self.fields.len() {
            let field_name = self.fields[i].clone();
            let header_name = if i < self.headers.len() {
                self.headers[i].clone()
            } else {
                format!("header{}", i)
            };
            result.push((field_name, header_name));
        }

        result
    }

    /// Clone a boxed value
    fn clone_box(&self, value: &dyn std::any::Any) -> Box<dyn std::any::Any + Send + Sync> {
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
        } else {
            // Default to empty string for unknown types
            Box::new(String::new())
        }
    }

    /// Abstract methods for subclasses
    fn operating_schema(&self, _record: &R) -> Option<Arc<dyn Schema>> {
        None
    }

    fn operating_value(&self, _record: &R) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
}

/// Key transformation - extracts from key
pub struct HeaderFromKey<R: ConnectRecord<R>> {
    inner: HeaderFrom<R>,
}

impl<R: ConnectRecord<R>> HeaderFromKey<R> {
    pub fn new() -> Self {
        Self {
            inner: HeaderFrom::new(),
        }
    }

    pub fn with_fields(fields: Vec<String>) -> Self {
        Self {
            inner: HeaderFrom::with_fields(fields),
        }
    }

    pub fn with_operation(operation: Operation) -> Self {
        Self {
            inner: HeaderFrom::with_operation(operation),
        }
    }

    pub fn with_replace_null_with_default(mut self, replace: bool) -> Self {
        Self {
            inner: self.inner.with_replace_null_with_default(replace),
        }
    }

    pub fn with_headers(mut self, headers: Vec<String>) -> Self {
        Self {
            inner: self.inner.with_headers(headers),
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for HeaderFromKey<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        self.inner.configure(configs);
    }
}

impl<R: ConnectRecord<R>> Closeable for HeaderFromKey<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.inner.close()
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for HeaderFromKey<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        self.inner.apply(record)
    }

    fn config(&self) -> ConfigDef {
        self.inner.config()
    }
}

impl<R: ConnectRecord<R>> HeaderFromKey<R> {
    fn operating_schema(&self, record: &R) -> Option<Arc<dyn Schema>> {
        record.key_schema()
    }

    fn operating_value(&self, record: &R) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        record.key()
    }
}

/// Value transformation - extracts from value
pub struct HeaderFromValue<R: ConnectRecord<R>> {
    inner: HeaderFrom<R>,
}

impl<R: ConnectRecord<R>> HeaderFromValue<R> {
    pub fn new() -> Self {
        Self {
            inner: HeaderFrom::new(),
        }
    }

    pub fn with_fields(fields: Vec<String>) -> Self {
        Self {
            inner: HeaderFrom::with_fields(fields),
        }
    }

    pub fn with_operation(operation: Operation) -> Self {
        Self {
            inner: HeaderFrom::with_operation(operation),
        }
    }

    pub fn with_replace_null_with_default(mut self, replace: bool) -> Self {
        Self {
            inner: self.inner.with_replace_null_with_default(replace),
        }
    }

    pub fn with_headers(mut self, headers: Vec<String>) -> Self {
        Self {
            inner: self.inner.with_headers(headers),
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for HeaderFromValue<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        self.inner.configure(configs);
    }
}

impl<R: ConnectRecord<R>> Closeable for HeaderFromValue<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.inner.close()
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for HeaderFromValue<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        self.inner.apply(record)
    }

    fn config(&self) -> ConfigDef {
        self.inner.config()
    }
}

impl<R: ConnectRecord<R>> HeaderFromValue<R> {
    fn operating_schema(&self, record: &R) -> Option<Arc<dyn Schema>> {
        record.value_schema()
    }

    fn operating_value(&self, record: &R) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        record.value()
    }
}
