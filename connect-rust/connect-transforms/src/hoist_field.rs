//! HoistField transformation
//!
//! Hoists a field from a nested structure to the top level.
//!
//! ## Configuration
//!
//! - <code>field</code> (required): The field path to hoist (using dotted notation for nested fields)
//! - <code>dest</code> (optional): The destination field name. If not specified, the last part of the field path is used.
//!
//! ## Example
//!
//! ```toml
//! [connect.transforms.HoistField]
//! field=nested.address.city
//! dest=city
//! ```
//!
//! This will hoist the `city` field from `nested.address.city` to the top level as `city`.

use crate::field::{FieldSyntaxVersion, SingleFieldPath};
use connect_api::connector_types::ConnectRecord;
use connect_api::data::{ConnectSchema, Field, Schema, SchemaBuilder, Struct, Type};
use connect_api::error::DataException;
use connect_api::{Closeable, ConfigDef, ConfigValue, Configurable, Transformation};
use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

/// HoistField transformation
///
/// Hoists a field from a nested structure to the top level.
pub struct HoistField<R: ConnectRecord<R>> {
    /// The field path to hoist
    field: String,
    /// The destination field name
    dest: Option<String>,
    /// Field syntax version
    field_syntax_version: FieldSyntaxVersion,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> HoistField<R> {
    /// Creates a new HoistField transformation
    pub fn new() -> Self {
        Self {
            field: String::new(),
            dest: None,
            field_syntax_version: FieldSyntaxVersion::V1,
            _phantom: PhantomData,
        }
    }

    /// Creates a new HoistField transformation with specified field
    pub fn with_field(field: String) -> Self {
        let mut transform = Self::new();
        transform.field = field;
        transform
    }

    /// Creates a new HoistField transformation with specified destination
    pub fn with_dest(dest: String) -> Self {
        let mut transform = Self::new();
        transform.dest = Some(dest);
        transform
    }

    /// Creates a new HoistField transformation with specified field syntax version
    pub fn with_field_syntax_version(version: FieldSyntaxVersion) -> Self {
        let mut transform = Self::new();
        transform.field_syntax_version = version;
        transform
    }
}

impl<R: ConnectRecord<R>> Configurable for HoistField<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(field) = configs.get("field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.field = f.clone();
            }
        }

        if let Some(dest) = configs.get("dest") {
            if let Some(d) = dest.downcast_ref::<String>() {
                self.dest = Some(d.clone());
            }
        }

        if let Some(version) = configs.get(FieldSyntaxVersion::FIELD_SYNTAX_VERSION_CONFIG) {
            if let Some(v) = version.downcast_ref::<String>() {
                self.field_syntax_version = FieldSyntaxVersion::from_str(v);
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for HoistField<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.field.clear();
        self.dest = None;
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for HoistField<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        if self.field.is_empty() {
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
        config.add_config("field".to_string(), ConfigValue::String(String::new()));
        config.add_config("dest".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            FieldSyntaxVersion::FIELD_SYNTAX_VERSION_CONFIG.to_string(),
            ConfigValue::String(FieldSyntaxVersion::FIELD_SYNTAX_VERSION_DEFAULT_VALUE.to_string()),
        );
        config
    }
}

impl<R: ConnectRecord<R>> HoistField<R> {
    /// Apply transformation to a schemaless record
    fn apply_schemaless(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        let value = record.value();

        if let Some(value_arc) = value {
            if let Some(value_any) =
                value_arc.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
            {
                // Parse field path
                let field_path = SingleFieldPath::new(&self.field, self.field_syntax_version);

                // Get field value
                if let Some(field_value) = field_path.value_from_map(Some(value_any)) {
                    // Determine destination field name
                    let dest_field = self
                        .dest
                        .clone()
                        .unwrap_or_else(|| field_path.last_step().to_string());

                    // Clone map and add hoisted field
                    let mut updated_value = HashMap::new();
                    for (k, v) in value_any.iter() {
                        updated_value.insert(k.clone(), self.clone_box(v.as_ref()));
                    }
                    updated_value.insert(dest_field, field_value);

                    // Create new record
                    let updated_value_arc =
                        Arc::new(Box::new(updated_value) as Box<dyn std::any::Any + Send + Sync>);
                    let new_record = record.new_record(
                        None,
                        None,
                        None,
                        None,
                        None,
                        Some(updated_value_arc),
                        None,
                        None,
                    );
                    return Ok(Some(new_record));
                }
            }
        }

        Ok(Some(record))
    }

    /// Apply transformation to a schema-based record
    fn apply_with_schema(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        let value = record.value();
        let value_schema = record.value_schema();

        if let (Some(value_arc), Some(schema_arc)) = (value, value_schema) {
            if let (Some(value_any), Some(schema_ref)) = (
                value_arc.downcast_ref::<Struct>(),
                schema_arc.as_connect_schema(),
            ) {
                // Parse field path
                let field_path = SingleFieldPath::new(&self.field, self.field_syntax_version);

                // Get field schema and value
                let field_schema = field_path.field_from(Some(&schema_arc));
                let field_value = field_path.value_from_struct(Some(value_any), false);

                if let (Some(field_schema), Some(field_value)) = (field_schema, field_value) {
                    // Determine destination field name
                    let dest_field = self
                        .dest
                        .clone()
                        .unwrap_or_else(|| field_path.last_step().to_string());

                    // Build updated schema
                    let mut builder = SchemaBuilder::struct_();
                    for f in schema_ref.fields()? {
                        builder = builder.field(
                            f.name().to_string(),
                            Arc::new(f.schema().as_connect_schema().unwrap().clone()),
                        );
                    }
                    // Add hoisted field
                    builder = builder.field(
                        dest_field.clone(),
                        Arc::new(field_schema.schema().as_connect_schema().unwrap().clone()),
                    );
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
                        if let Some(field_value) = value_any.get(field_name) {
                            updated_struct
                                .put_without_validation(field_name, self.clone_box(field_value));
                        }
                    }
                    // Add hoisted field value
                    updated_struct.put_without_validation(&dest_field, field_value);

                    // Create new record
                    let updated_value_arc =
                        Arc::new(Box::new(updated_struct) as Box<dyn std::any::Any + Send + Sync>);
                    let updated_schema_arc = Arc::clone(&updated_schema) as Arc<dyn Schema>;
                    let new_record = record.new_record(
                        None,
                        None,
                        None,
                        None,
                        Some(updated_schema_arc),
                        Some(updated_value_arc),
                        None,
                        None,
                    );
                    return Ok(Some(new_record));
                }
            }
        }

        Ok(Some(record))
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
        } else if let Some(s) = value.downcast_ref::<Struct>() {
            // Clone struct by copying all fields
            let schema = Arc::clone(&s.schema());
            let mut cloned_struct = Struct::new(schema.clone())
                .map_err(|_| DataException::new("Failed to clone struct".to_string()))
                .unwrap();
            for field in schema.fields().unwrap_or_default() {
                let field_name = field.name();
                if let Some(field_value) = s.get(field_name) {
                    cloned_struct.put_without_validation(field_name, self.clone_box(field_value));
                }
            }
            Box::new(cloned_struct)
        } else {
            // Default to empty unit for unknown types
            Box::new(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connect_api::data::{ConnectSchema, SourceRecord, Struct};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_hoist_field_new() {
        let transform = HoistField::<SourceRecord>::new();
        assert!(transform.field.is_empty());
        assert!(transform.dest.is_none());
    }

    #[test]
    fn test_hoist_field_with_field() {
        let transform = HoistField::<SourceRecord>::with_field("nested.field".to_string());
        assert_eq!(transform.field, "nested.field");
    }

    #[test]
    fn test_hoist_field_with_dest() {
        let transform = HoistField::<SourceRecord>::with_dest("destination".to_string());
        assert_eq!(transform.dest, Some("destination".to_string()));
    }

    #[test]
    fn test_hoist_field_schemaless() {
        let mut transform = HoistField::<SourceRecord>::new();
        transform.field = "nested.field".to_string();

        // Create a schemaless record
        let mut value = HashMap::new();
        value.insert(
            "key".to_string(),
            Box::new("value1".to_string()) as Box<dyn std::any::Any + Send + Sync>,
        );

        let nested = HashMap::new();
        nested.insert(
            "field".to_string(),
            Box::new("nested_value".to_string()) as Box<dyn std::any::Any + Send + Sync>,
        );
        value.insert(
            "nested".to_string(),
            Box::new(nested) as Box<dyn std::any::Any + Send + Sync>,
        );

        let record = SourceRecord::builder()
            .topic("test-topic".to_string())
            .value(Arc::new(
                Box::new(value) as Box<dyn std::any::Any + Send + Sync>
            ))
            .build();

        // Apply transformation
        let result = transform.apply(record);
        assert!(result.is_ok());

        let transformed_record = result.unwrap().unwrap();
        let transformed_value = transformed_record.value().unwrap();
        let transformed_map = transformed_value
            .downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
            .unwrap();

        // Check that field was hoisted
        assert!(transformed_map.contains_key("field"));
        let hoisted_value = transformed_map
            .get("field")
            .unwrap()
            .downcast_ref::<String>()
            .unwrap();
        assert_eq!(hoisted_value, "nested_value");
    }

    #[test]
    fn test_hoist_field_with_schema() {
        let mut transform = HoistField::<SourceRecord>::new();
        transform.field = "nested.field".to_string();
        transform.dest = Some("hoisted".to_string());

        // Create a schema-based record
        let nested_schema = Arc::new(ConnectSchema::new(Type::String));
        let nested_field_schema = Arc::new(ConnectSchema::new(Type::String));

        let mut nested_builder = connect_api::data::SchemaBuilder::struct_();
        nested_builder = nested_builder.field("field".to_string(), nested_field_schema.clone());
        let nested_schema = nested_builder.build().unwrap();

        let mut builder = connect_api::data::SchemaBuilder::struct_();
        builder = builder.field(
            "key".to_string(),
            Arc::new(ConnectSchema::new(Type::String)),
        );
        builder = builder.field("nested".to_string(), nested_schema.clone());
        let schema = builder.build().unwrap();

        let mut nested_struct = Struct::new(nested_schema.clone()).unwrap();
        nested_struct.put_without_validation("field", Box::new("nested_value".to_string()));

        let mut value_struct = Struct::new(schema.clone()).unwrap();
        value_struct.put_without_validation("key", Box::new("value1".to_string()));
        value_struct.put_without_validation("nested", Box::new(nested_struct));

        let record = SourceRecord::builder()
            .topic("test-topic".to_string())
            .value_schema(schema.clone())
            .value(Arc::new(
                Box::new(value_struct) as Box<dyn std::any::Any + Send + Sync>
            ))
            .build();

        // Apply transformation
        let result = transform.apply(record);
        assert!(result.is_ok());

        let transformed_record = result.unwrap().unwrap();
        let transformed_value = transformed_record.value().unwrap();
        let transformed_struct = transformed_value.downcast_ref::<Struct>().unwrap();

        // Check that field was hoisted
        assert!(transformed_struct.schema().field("hoisted").is_ok());
        let hoisted_value = transformed_struct
            .get("hoisted")
            .unwrap()
            .downcast_ref::<String>()
            .unwrap();
        assert_eq!(hoisted_value, "nested_value");
    }
}
