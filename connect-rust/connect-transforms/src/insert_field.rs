//! InsertField transformation
//!
//! Inserts a field into a record with a specified value.
//!
//! ## Configuration
//!
//! - <code>field</code> (required): The field name to insert
//! - <code>value</code> (optional): The static value to insert (as a string)
//! - <code>value.schema.type</code> (optional): The schema type for the value (e.g., "STRING", "INT64", etc.)
//! - <code>value.schema.name</code> (optional): The schema name for the value (for logical types)
//! - <code>value.schema.version</code> (optional): The schema version for the value
//!
//! ## Example
//!
//! ```toml
//! [connect.transforms.InsertField]
//! field=timestamp
//! value=2023-01-01
//! value.schema.type=STRING
//! ```
//!
//! This will insert a `timestamp` field with value "2023-01-01" into each record.

use connect_api::connector_types::ConnectRecord;
use connect_api::data::{ConnectSchema, Field, Schema, SchemaBuilder, Struct, Type};
use connect_api::error::DataException;
use connect_api::{Closeable, ConfigDef, ConfigValue, Configurable, Transformation};
use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

/// InsertField transformation
///
/// Inserts a field into a record with a specified value.
pub struct InsertField<R: ConnectRecord<R>> {
    /// The field name to insert
    field: String,
    /// The static value to insert
    value: Option<String>,
    /// The schema type for the value
    value_schema_type: Option<String>,
    /// The schema name for the value (for logical types)
    value_schema_name: Option<String>,
    /// The schema version for the value
    value_schema_version: Option<i32>,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> InsertField<R> {
    /// Creates a new InsertField transformation
    pub fn new() -> Self {
        Self {
            field: String::new(),
            value: None,
            value_schema_type: None,
            value_schema_name: None,
            value_schema_version: None,
            _phantom: PhantomData,
        }
    }

    /// Creates a new InsertField transformation with specified field and value
    pub fn with_field_value(field: String, value: String) -> Self {
        let mut transform = Self::new();
        transform.field = field;
        transform.value = Some(value);
        transform
    }

    /// Creates a new InsertField transformation with specified field, value, and schema type
    pub fn with_field_value_schema_type(field: String, value: String, schema_type: String) -> Self {
        let mut transform = Self::new();
        transform.field = field;
        transform.value = Some(value);
        transform.value_schema_type = Some(schema_type);
        transform
    }
}

impl<R: ConnectRecord<R>> Configurable for InsertField<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(field) = configs.get("field") {
            if let Some(f) = field.downcast_ref::<String>() {
                self.field = f.clone();
            }
        }

        if let Some(value) = configs.get("value") {
            if let Some(v) = value.downcast_ref::<String>() {
                self.value = Some(v.clone());
            }
        }

        if let Some(schema_type) = configs.get("value.schema.type") {
            if let Some(st) = schema_type.downcast_ref::<String>() {
                self.value_schema_type = Some(st.clone());
            }
        }

        if let Some(schema_name) = configs.get("value.schema.name") {
            if let Some(sn) = schema_name.downcast_ref::<::String>() {
                self.value_schema_name = Some(sn.clone());
            }
        }

        if let Some(schema_version) = configs.get("value.schema.version") {
            if let Some(sv) = schema_version.downcast_ref::<i32>() {
                self.value_schema_version = Some(*sv);
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for InsertField<R> {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.field.clear();
        self.value = None;
        self.value_schema_type = None;
        self.value_schema_name = None;
        self.value_schema_version = None;
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for InsertField<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        if self.field.is_empty() || self.value.is_none() {
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
        config.add_config("value".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            "value.schema.type".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config(
            "value.schema.name".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config("value.schema.version".to_string(), ConfigValue::Int32(1));
        config
    }
}

impl<R: ConnectRecord<R>> InsertField<R> {
    /// Apply transformation to a schemaless record
    fn apply_schemaless(&self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        let value = record.value();

        if let Some(value_arc) = value {
            if let Some(value_any) =
                value_arc.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>()
            {
                // Clone map and insert new field
                let mut updated_value = HashMap::new();
                for (k, v) in value_any.iter() {
                    updated_value.insert(k.clone(), self.clone_box(v.as_ref()));
                }

                // Insert new field
                let field_value =
                    Box::new(self.value.clone().unwrap()) as Box<dyn std::any::Any + Send + Sync>;
                updated_value.insert(self.field.clone(), field_value);

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
                // Build the schema for the new field
                let field_schema = self.build_field_schema()?;

                // Build updated schema
                let mut builder = SchemaBuilder::struct_();
                for f in schema_ref.fields()? {
                    builder = builder.field(
                        f.name().to_string(),
                        Arc::new(f.schema().as_connect_schema().unwrap().clone()),
                    );
                }
                // Add new field
                builder = builder.field(self.field.clone(), field_schema.clone());
                if schema_ref.is_optional() {
                    builder = builder.optional();
                }
                let updated_schema = builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;

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
                // Insert new field value
                let field_value =
                    Box::new(self.value.clone().unwrap()) as Box<dyn std::any::Any + Send + Sync>;
                updated_struct.put_without_validation(&self.field, field_value);

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

        Ok(Some(record))
    }

    /// Build the schema for the new field
    fn build_field_schema(&self) -> Result<Arc<ConnectSchema>, Box<dyn Error>> {
        let schema_type = self
            .value_schema_type
            .as_ref()
            .map(|t| t.to_uppercase())
            .unwrap_or_else(|| "STRING".to_string());

        let mut builder = match schema_type.as_str() {
            "INT8" => SchemaBuilder::int8(),
            "INT16" => SchemaBuilder::int16(),
            "INT32" => SchemaBuilder::int32(),
            "INT64" => SchemaBuilder::int64(),
            "FLOAT32" => SchemaBuilder::float32(),
            "FLOAT64" => SchemaBuilder::float64(),
            "BOOLEAN" => SchemaBuilder::boolean(),
            "STRING" => SchemaBuilder::string(),
            "BYTES" => SchemaBuilder::bytes(),
            _ => {
                return Err(Box::new(DataException::new(format!(
                    "Unsupported schema type: {}",
                    schema_type
                ))));
            }
        };

        // Add schema name if specified
        if let Some(name) = &self.value_schema_name {
            builder = builder.name(name.clone());
        }

        // Add schema version if specified
        if let Some(version) = self.value_schema_version {
            builder = builder.version(*version);
        }

        let schema = builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;
        Ok(Arc::new(schema))
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
            let schema = s.schema();
            let mut cloned_struct = Struct::new(schema)
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
    fn test_insert_field_new() {
        let transform = InsertField::<SourceRecord>::new();
        assert!(transform.field.is_empty());
        assert!(transform.value.is_none());
    }

    #[test]
    fn test_insert_field_with_field_value() {
        let transform = InsertField::<SourceRecord>::with_field_value(
            "new_field".to_string(),
            "new_value".to_string(),
        );
        assert_eq!(transform.field, "new_field");
        assert_eq!(transform.value, Some("new_value".to_string()));
    }

    #[test]
    fn test_insert_field_schemaless() {
        let mut transform = InsertField::<SourceRecord>::new();
        transform.field = "timestamp".to_string();
        transform.value = Some("2023-01-01".to_string());

        // Create a schemaless record
        let mut value = HashMap::new();
        value.insert(
            "key".to_string(),
            Box::new("value1".to_string()) as Box<dyn std::ariy::Any + Send + Sync>,
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

        // Check that field was inserted
        assert!(transformed_map.contains_key("timestamp"));
        let inserted_value = transformed_map
            .get("timestamp")
            .unwrap()
            .downcast_ref::<String>()
            .unwrap();
        assert_eq!(inserted_value, "2023-01-01");
    }

    #[test]
    fn test_insert_field_with_schema() {
        let mut transform = InsertField::<SourceRecord>::new();
        transform.field = "new_field".to_string();
        transform.value = Some("new_value".to_string());
        transform.value_schema_type = Some("STRING".to_string());

        // Create a schema-based record
        let mut builder = connect_api::data::SchemaBuilder::struct_();
        builder = builder.field(
            "key".to_string(),
            Arc::new(ConnectSchema::new(Type::String)),
        );
        builder = builder.field(
            "value".to_string(),
            Arc::new(ConnectSchema::new(Type::String)),
        );
        let schema = builder.build().unwrap();

        let mut value_struct = Struct::new(schema.clone()).unwrap();
        value_struct.put_without_validation("key", Box::new("key_value".to_string()));
        value_struct.put_without_validation("value", Box::new("value_value".to_string()));

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

        // Check that field was inserted
        assert!(transformed_struct.schema().field("new_field").is_ok());
        let inserted_value = transformed_struct
            .get("new_field")
            .unwrap()
            .downcast_ref::<String>()
            .unwrap();
        assert_eq!(inserted_value, "new_value");
    }
}
