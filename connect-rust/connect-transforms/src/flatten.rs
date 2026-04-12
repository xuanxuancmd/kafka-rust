//! Flatten transformation
//!
//! Flattens a nested data structure, generating names for each field by concatenating field names at each
//! level with a configurable delimiter character. Applies to Struct when schema present, or a Map
//! in case of schemaless data. Array fields and their contents are not modified.
//! The default delimiter is '.'.

use connect_api::connector_types::ConnectRecord;
use connect_api::data::{ConnectSchema, Field, Schema, SchemaBuilder, Struct, Type};
use connect_api::error::DataException;
use connect_api::transforms::util::{SchemaUtil, SimpleConfig};
use connect_api::{Configurable, Transformation};
use connect_api::ConfigDef;
use connect_api::ConfigValue;
use connect_api::cache::{Cache, LRUCache, SynchronizedCache};
use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

/// Trait for operating on schema and value of a record
pub trait FlattenOperating<R: ConnectRecord<R> + Clone> {
    /// Get the operating schema (key schema or value schema)
    fn operating_schema(&self, record: &R) -> Option<Arc<dyn connect_api::data::Schema>>;
    
    /// Get the operating value (key or value)
    fn operating_value(&self, record: &R) -> Option<Arc<dyn std::any::Any + Send + Sync>>;
    
    /// Create a new record with updated schema and value
    fn new_record(
        &self,
        record: R,
        updated_schema: Option<Arc<dyn connect_api::data::Schema>>,
        updated_value: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> R;
}

/// Flatten transformation
///
/// Flattens a nested data structure, generating names for each field by concatenating
/// field names at each level with a configurable delimiter character.
pub struct Flatten<R: ConnectRecord<R>, O: FlattenOperating<R>> {
    /// Delimiter for field name concatenation
    delimiter: String,
    /// Cache for schema updates
    schema_update_cache: Option<Box<dyn Cache<Arc<dyn Schema>, Arc<dyn Schema>>>>>,
    /// Operating implementation (key or value)
    operating: O,
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>, O: FlattenOperating<R>> Flatten<R, O> {
    /// Creates a new Flatten transformation
    pub fn new(operating: O) -> Self {
        Self {
            delimiter: ".".to_string(),
            schema_update_cache: None,
            operating,
            _phantom: PhantomData,
        }
    }

    /// Creates a new Flatten transformation with specified delimiter
    pub fn with_delimiter(operating: O, delimiter: String) -> Self {
        let mut transform = Self::new(operating);
        transform.delimiter = delimiter;
        transform
    }

    /// Creates a new Flatten transformation with specified cache
    pub fn with_cache(
        operating: O,
        cache: Box<dyn Cache<Arc<dyn Schema>, Arc<dyn Schema>>>,
    ) -> Self {
        let mut transform = Self::new(operating);
        transform.schema_update_cache = Some(cache);
        transform
    }
}

impl<R: ConnectRecord<R>, O: FlattenOperating<R>> Configurable for Flatten<R, O> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(delim) = configs.get("delimiter") {
            if let Some(d) = delim.downcast_ref::<String>() {
                self.delimiter = d.clone();
            }
        }

        // Initialize schema cache if not already set
        if self.schema_update_cache.is_none() {
            self.schema_update_cache = Some(Box::new(SynchronizedCache::new(
                Box::new(LRUCache::new(16))
            )) as Box<dyn Cache<Arc<dyn Schema>, Arc<dyn Schema>>>);
        }
    }
}

impl<R: ConnectRecord<R>, O: FlattenOperating<R>> Transformation<R> for Flatten<R, O> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        let value = self.operating.operating_value(&record);
        
        if value.is_none() {
            // Pass through null values unchanged
            return Ok(Some(record));
        }

        let schema = self.operating.operating_schema(&record);
        
        if schema.is_none() {
            // Schemaless case
            if let Some(value_any) = value {
                if let Some(map) = value_any.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>() {
                    return self.apply_schemaless(&record, map);
                }
            }
        } else {
            // Schema-based case
            if let Some(value_any) = value {
                if let Some(struct_value) = value_any.downcast_ref::<Struct>() {
                    if let Some(schema_ref) = schema {
                        return self.apply_with_schema(&record, struct_value, schema_ref.as_ref());
                    }
                }
            }
        }

        Ok(Some(record))
    }

    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config(
            "delimiter".to_string(),
            ConfigValue::String(".".to_string()),
        );
        config
    }

    /// Creates a new Flatten transformation with specified delimiter
    pub fn with_delimiter(delimiter: String) -> Self {
        let mut transform = Self::new();
        transform.delimiter = delimiter;
        transform
    }

    /// Creates a new Flatten transformation with specified cache
    pub fn with_cache(
        cache: Box<dyn Cache<Arc<dyn Schema>, Arc<dyn Schema>>>,
    ) -> Self {
        let mut transform = Self::new();
        transform.schema_update_cache = Some(cache);
        transform
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

    /// Schemaless flattening
    fn apply_schemaless(
        &self,
        record: &R,
        value_map: &HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    ) -> Result<Option<R>, Box<dyn Error>> {
        let flattened_map = self.flatten_map(value_map, &"".to_string());
        let flattened_value = Arc::new(flattened_map) as Arc<dyn std::any::Any + Send + Sync>;
        
        let new_record = self.operating.new_record(
            record.clone(),
            None,
            Some(flattened_value),
        );

        Ok(Some(new_record))
    }

    /// Schema-based flattening
    fn apply_with_schema(
        &self,
        record: &R,
        value_any: &Struct,
        schema_ref: &dyn connect_api::data::ConnectSchema,
    ) -> Result<Option<R>, Box<dyn Error>> {
        // Get or compute flattened schema
        let flattened_schema = if let Some(cache) = &self.schema_update_cache {
            cache.get(schema_ref)
        } else {
            None
        };

        let flattened_schema = if let Some(cached_schema) = flattened_schema {
            cached_schema.clone()
        } else {
            // Compute flattened schema
            let computed_schema = self.compute_flattened_schema(schema_ref)?;
            
            // Cache the result
            if let Some(cache) = &self.schema_update_cache {
                cache.put(schema_ref.clone(), Arc::new(computed_schema));
            }
            
            Arc::new(computed_schema)
        };

        // Flatten the struct value
        let flattened_value = self.flatten_struct(value_any, &"".to_string(), Some(flattened_schema.clone()))?;
        
        let new_record = self.operating.new_record(
            record.clone(),
            Some(flattened_schema),
            Some(flattened_value),
        );

        Ok(Some(new_record))
    }

    /// Flatten a map value recursively
    fn flatten_map(
        &self,
        value_map: &HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
        prefix: &str,
    ) -> HashMap<String, Box<dyn std::any::Any + Send + Sync>> {
        let mut flattened = HashMap::new();
        
        for (key, value) in value_map {
            let new_key = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{}{}{}", prefix, key)
            };
            
            if let Some(map_value) = value.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>() {
                let flattened_submap = self.flatten_map(map_value, &new_key);
                for (subkey, subvalue) in flattened_submap {
                    flattened.insert(subkey, subvalue);
                }
            } else {
                flattened.insert(new_key, value.clone());
            }
        }
        
        flattened
    }

    /// Flatten a struct value recursively
    fn flatten_struct(
        &self,
        struct_value: &Struct,
        prefix: &str,
        schema: Option<Arc<dyn Schema>>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync>, Box<dyn Error>> {
        let fields = struct_value.schema().fields()?;
        let mut flattened = HashMap::new();
        
        for field in fields {
            let field_name = field.name();
            let field_value = struct_value.get(&field_name)?;
            
            let new_key = if prefix.is_empty() {
                field_name.clone()
            } else {
                format!("{}{}{}", prefix, field_name)
            };
            
            flattened.insert(new_key, field_value);
        }
        
        Ok(Box::new(flattened))
    }

    /// Compute flattened schema from original schema
    fn compute_flattened_schema(
        &self,
        schema_ref: &dyn connect_api::data::ConnectSchema,
    ) -> Result<Arc<dyn Schema>, Box<dyn Error>> {
        let fields = schema_ref.fields()?;
        let mut builder = SchemaBuilder::struct_();
        
        for field in fields {
            let field_name = field.name();
            let field_schema = field.schema();
            
            // For simple types, just copy the field
            if let Some(simple_schema) = field_schema.as_connect_schema() {
                match simple_schema.type_() {
                    Type::String => {
                        builder.field(field_name, SchemaBuilder::string());
                    }
                    Type::Int8 => {
                        builder.field(field_name, SchemaBuilder::int8());
                    }
                    Type::Int16 => {
                        builder.field(field_name, SchemaBuilder::int16());
                    }
                    Type::Int32 => {
                        builder.field(field_name, SchemaBuilder::int32());
                    }
                    Type::Int64 => {
                        builder.field(field_name, SchemaBuilder::int64());
                    }
                    Type::Float32 => {
                        builder.field(field_name, SchemaBuilder::float32());
                    }
                    Type::Float64 => {
                        builder.field(field_name, SchemaBuilder::float64());
                    }
                    Type::Boolean => {
                        builder.field(field_name, SchemaBuilder::boolean());
                    }
                    Type::Bytes => {
                        builder.field(field_name, SchemaBuilder::bytes());
                    }
                    Type::Array => {
                        builder.field(field_name, SchemaBuilder::array(field_schema.as_ref().unwrap()));
                    }
                    Type::Map => {
                        if let Some(map_schema) = field_schema.as_connect_schema() {
                            builder.field(field_name, SchemaBuilder::map(map_schema));
                        }
                    }
                    Type::Struct => {
                        if let Some(struct_schema) = field_schema.as_connect_schema() {
                            builder.field(field_name, SchemaBuilder::struct_(struct_schema));
                        }
                    }
                }
            }
        }
        
        Ok(Arc::new(builder.build()?))
    }
}

/// Operating implementation for key
pub struct FlattenKeyOperating;

impl<R: ConnectRecord<R>> FlattenOperating<R> for FlattenKeyOperating {
    fn operating_schema(&self, record: &R) -> Option<Arc<dyn connect_api::data::Schema>> {
        record.key_schema()
    }
    
    fn operating_value(&self, record: &R) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        record.key()
    }
    
    fn new_record(
        &self,
        record: R,
        updated_schema: Option<Arc<dyn connect_api::data::Schema>>,
        updated_value: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> R {
        record.new_record(
            None,
            None,
            updated_schema,
            updated_value,
            None,
            None,
            None,
        )
    }
}

/// Key transformation - flattens key
pub struct FlattenKey<R: ConnectRecord<R>> {
    inner: Flatten<R, FlattenKeyOperating>,
}

impl<R: ConnectRecord<R>> FlattenKey<R> {
    pub fn new() -> Self {
        Self {
            inner: Flatten::new(FlattenKeyOperating),
        }
    }

    pub fn with_delimiter(delimiter: String) -> Self {
        Self {
            inner: Flatten::with_delimiter(FlattenKeyOperating, delimiter),
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for FlattenKey<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        self.inner.configure(configs);
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for FlattenKey<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        self.inner.apply(record)
    }

    fn config(&self) -> ConfigDef {
        self.inner.config()
    }
}

/// Operating implementation for value
pub struct FlattenValueOperating;

impl<R: ConnectRecord<R>> FlattenOperating<R> for FlattenValueOperating {
    fn operating_schema(&self, record: &R) -> Option<Arc<dyn connect_api::data::Schema>> {
        record.value_schema()
    }
    
    fn operating_value(&self, record: &R) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        record.value()
    }
    
    fn new_record(
        &self,
        record: R,
        updated_schema: Option<Arc<dyn connect_api::data::Schema>>,
        updated_value: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> R {
        record.new_record(
            None,
            None,
            None,
            None,
            updated_schema,
            updated_value,
            None,
        )
    }
}

/// Value transformation - flattens value
pub struct FlattenValue<R: ConnectRecord<R>> {
    inner: Flatten<R, FlattenValueOperating>,
}

impl<R: ConnectRecord<R>> FlattenValue<R> {
    pub fn new() -> Self {
        Self {
            inner: Flatten::new(FlattenValueOperating),
        }
    }

    pub fn with_delimiter(delimiter: String) -> Self {
        Self {
            inner: Flatten::with_delimiter(FlattenValueOperating, delimiter),
        }
    }
}

impl<R: ConnectRecord<R>> Configurable for FlattenValue<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        self.inner.configure(configs);
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for FlattenValue<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        self.inner.apply(record)
    }

    fn config(&self) -> ConfigDef {
        self.inner.config()
    }
}
    }
}

impl<R: ConnectRecord<R>> Configurable for FlattenValue<R> {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        self.inner.configure(configs);
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for FlattenValue<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        self.inner.apply(record)
    }

    fn config(&self) -> ConfigDef {
        self.inner.config()
    }
}

impl<R: ConnectRecord<R>> FlattenValue<R> {
    fn operating_schema(&self, record: R) -> Option<Arc<dyn connect_api::data::Schema>> {
        record.value_schema()
    }

    fn operating_value(&self, record: R) -> Option<Arc<dyn std::any::Any + Send +>> {
        record.value()
    }
}
