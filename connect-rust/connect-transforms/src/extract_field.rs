//! ExtractField transformation
//!
//! Extracts specified field from a Struct when schema present, or a Map in case of schemaless data.
//! Any null values are passed through unmodified.

use connect_api::connector_types::ConnectRecord;
use connect_api::data::{ConnectSchema, Field, Schema, Struct};
use connect_api::error::DataException;
use connect_api::transforms::field::{FieldSyntaxVersion, SingleFieldPath};
use connect_api::transforms::util::{SimpleConfig, Requirements};
use connect_api::{Configurable, Transformation};
use connect_api::ConfigDef;
use connect_api::ConfigValue;
use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

/// ExtractField transformation
///
/// Extracts specified field from a Struct when schema present, or a Map
/// in case of schemaless data. Any null values are passed through unmodified.
pub struct ExtractField<R: ConnectRecord<R>> {
    /// The field path to extract
    field_path: SingleFieldFieldPath,
    /// Whether to replace null with default value
    replace_null_with_default: bool,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> ExtractField<R> {
    /// Creates a new ExtractField transformation
    pub fn new() -> Self {
        Self {
            field_path: SingleFieldPath::new(
                "field",
                FieldSyntaxVersion::V1,
            ),
            replace_null_with_default: true,
            _phantom: PhantomData,
        }
    }

    /// Creates a new ExtractField transformation with specified field path
    pub fn with_field_pathway(field_path: SingleFieldPath) -> Self {
        let mut transform = Self::new();
        transform.field_path = field_path;
        transform
    }

    /// Creates a new ExtractField transformation with specified replace null with default flag
    pub fn with_replace_null_with_default(mut self, replace: bool) -> Self {
        self.replace_null_with_default = replace;
        self
    }
}

impl<R: ConnectRecord<R>> Configurable for ExtractField<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(field) = configs.get("field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.field_path = SingleFieldPath::new(f, FieldSyntaxVersion::from_config(&self.config()));
            }
        }

        if let Some(replace) = configs.get("replace.null.with.default") {
            if let Some(r) = replace.downcast_ref::<bool>() {
                self.replace_null_with_default = *r;
            }
        }
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for ExtractField<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        let schema = self.operating_schema(&record);
        if schema.is_none() {
            // Schemaless case
            if let Some(value_any) = self.operating_value(&record) {
                if let Some(value_map) = value_any.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>() {
                    return self.apply_schemaless(&record, value_map);
                }
            }
        } else {
            // Schema-based case
            if let (Some(value_any), Some(schema_ref)) = (
                self.operating_value(&record),
                self.operating_schema(&record),
            ) {
                if let (Some(value_any), Some(schema_ref)) = (
                    value_any.downcast_ref::<Struct>(),
                    schema_ref.as_connect_schema(),
                ) {
                    return self.apply_with_schema(&record, value_any, schema_ref)?;
                }
            }
        }
        Ok(Some(record))
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config(
            "field".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config(
            "replace.null.with.default".to_string(),
            ConfigValue::Boolean(true),
        );
        config
    }

    /// Schemaless extraction
    fn apply_schemaless(
        &self,
        record: R,
        value_map: &HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Option<R>, Box<dyn Error>> {
        // Extract field from map
        let extracted_value = self.field_path.value_from_map(value_map);

        let new_record = record.new_record(
            None,
            None,
            None,
            Some(extracted_value),
            None,
            None,
            None,
        );

        Ok(Some(new_record))
    }

    /// Schema-based extraction
    fn apply_with_schema(
        &self,
        record: R,
        value_any: &dyn std::any::Any + Send + Sync>,
        schema_ref: &dyn connect_api::data::ConnectSchema,
    ) -> Result<Option<R>, Box<dyn Error>> {
        let struct_value = value_any.downcast_ref::<Struct>()?;

        // Get field from schema
        let field = self.field_path.field_from(schema_ref);

        if field.is_none() {
            return Err(DataException::new(format!(
                "Unknown field: {}",
                self.field_path
            )));
        }

        // Get field value with or without default
        let field_value = if self.replace_null_with_default {
            struct_value.get(&field.unwrap())
        } else {
            struct_value.get_without_default(&field.unwrap().name())
        };

        // Extract value from struct
        let extracted_value = self.field_path.value_from_struct(
            struct_value,
            self.replace_null_with_default,
        );

        let new_record = record.new_record(
            None,
            None,
            None,
            Some(extracted_value),
            field.map(|f| f.schema()),
            None,
            None,
        );

        Ok(Some(new_record))
    }

    // Abstract methods for subclasses
    fn operating_schema(&self, record: R) -> Option<Arc<dyn connect_api::data::Schema>> {
        None
    }

    fn operating_value(&self, record: R) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
}

/// Key transformation - extracts from key
pub struct ExtractFieldKey<R: ConnectRecord<R>> {
    inner: ExtractField<R>,
}

impl<R: ConnectRecord<R>> ExtractFieldKey<R> {
    pub fn new() -> Self {
        Self {
            inner: ExtractField::new(),
        }
    }

    pub fn with_field_pathway(field_path: SingleFieldPath) -> Self {
        Self {
            inner: ExtractField::with_field_pathway(field_path),
        }
    }

    pub fn with_replace_null_with_default(mut self, replace: bool) -> Self {
        Self {
            inner: self.inner.with_replace_null_with_default(replace),
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for ExtractFieldKey<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        self.inner.configure(configs);
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for ExtractFieldKey<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        self.inner.apply(record)
    }

    fn config(&self) -> ConfigDef {
        self.inner.config()
    }
}

impl<R: ConnectRecord<R>> ExtractFieldKey<R> {
    fn operating_schema(&self, record: R) -> Option<Arc<dyn connect_api::data::Schema>> {
        record.key_schema()
    }

    fn operating_value(&self, record: R) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        record.key()
    }
}

/// Value transformation - extracts from value
pub struct ExtractFieldValue<R: ConnectRecord<R>> {
    inner: ExtractField<R>,
}

impl<R: ConnectRecord<R>> ExtractFieldValue<R> {
    pub fn new() -> Self {
        Self {
            inner: ExtractField::new(),
        }
    }

    pub fn with_field_pathway(field_path: SingleFieldPath) -> Self {
        Self {
            inner: ExtractField::with_field_pathway(field_path),
        }
    }

    pub fn with_replace_null_with_default(mut self, replace: bool) -> Self {
        Self {
            inner: self.inner.with_replace_null_with_default(replace),
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for ExtractFieldValue<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        self.inner.configure(configs);
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for ExtractFieldValue<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        self.inner.apply(record)
    }

    fn config(&self) -> ConfigDef {
        self.inner.config()
    }
}

impl<R: ConnectRecord<R>> ExtractFieldValue<R> {
    fn operating_schema(&self, record: R) -> Option<Arc<dyn connect_api::data::Schema>> {
        record.value_schema()
    }

    fn operating_value(&self, record: R) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        record.value()
    }
}
