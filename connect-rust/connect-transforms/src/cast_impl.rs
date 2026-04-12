
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
                self.spec = s.split(',').map(|s| s.trim().to_string()).collect();
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
    fn parse_field_types(spec: &[String]) -> (HashMap<String, connect_api::data::Type>, Option<connect_api::data::Type>) {
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
                let casted_value = self.cast_value_to_type(None, value_arc.as_ref(), whole_value_type)?;
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
            if let Some(value_any) = value_arc.downcast_ref::<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>() {
                let mut updated_value = value_any.clone();
                
                for (field_name, target_type) in &self.casts {
                    if let Some(field_value) = value_any.get(field_name) {
                        let casted_value = self.cast_value_to_type(None, field_value, *target_type)?;
                        updated_value.insert(field_name.clone(), casted_value);
                    }
                }
                
                let updated_value_arc = Arc::new(Box::new(updated_value) as Box<dyn std::any::Any + Send + Sync>);
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
            if let Some(value_arc) = operating_value {
                if let Some(schema_arc) = operating_schema {
                    let casted_value = self.cast_value_to_type(Some(schema_arc.as_ref()), value_arc.as_ref(), whole_value_type)?;
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
                schema_arc.as_any().downcast_ref::<ConnectSchema>(),
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
                            Some(self.cast_value_to_type(Some(field.schema().as_ref()), fv, *target_type)?)
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
                
                let updated_value_arc = Arc::new(Box::new(updated_struct) as Box<dyn std::any::Any + Send + Sync>);
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
    fn build_updated_schema(&self, original_schema: Arc<dyn connect_api::data::Schema>) -> Result<Arc<connect_api::data::ConnectSchema>, Box<dyn Error>> {
        use connect_api::data::{ConnectSchema, Field, SchemaBuilder};
        
        let schema_ref = original_schema
            .as_any()
            .downcast_ref::<ConnectSchema>()
            .ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Invalid schema type"))?;
        
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
                let new_field_schema = type_builder.build().map_err(|e| Box::new(e) as Box<dyn Error>)?;
                builder.field(field_name.to_string(), new_field_schema);
            } else {
                // Keep original field schema
                builder.field(field_name.to_string(), field_schema);
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
'            Ok(*v as f32)
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
            Ok(base64::encode(v))
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
