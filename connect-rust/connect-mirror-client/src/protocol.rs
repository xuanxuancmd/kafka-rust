//! Kafka Protocol Types
//!
//! Defines Kafka protocol types for serialization/deserialization of structured data.
//! This mirrors the Java implementation in org.apache.kafka.common.protocol.types

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

/// Protocol type error
#[derive(Debug)]
pub enum ProtocolError {
    InvalidData(String),
    UnsupportedType(String),
    IoError(String),
    BufferOverflow,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            ProtocolError::UnsupportedType(msg) => write!(f, "Unsupported type: {}", msg),
            ProtocolError::IoError(msg) => write!(f, "IO error: {}", msg),
            ProtocolError::BufferOverflow => write!(f, "Buffer overflow"),
        }
    }
}

impl Error for ProtocolError {}

/// Protocol types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    /// Boolean type
    BOOLEAN,
    /// 8-bit signed integer
    INT8,
    /// 16-bit signed integer
    INT16,
    /// 32-bit signed integer
    INT32,
    /// 64-bit signed integer
    INT64,
    /// Variable-length integer
    VARINT,
    /// Variable-length long
    VARLONG,
    /// Unsigned 8-bit integer
    UINT8,
    /// 32-bit unsigned integer
    UINT32,
    /// 64-bit unsigned integer
    UINT64,
    /// Floating-point 32-bit
    FLOAT64,
    /// String
    STRING,
    /// Bytes
    BYTES,
    /// Nullable string
    NullableString,
    /// Nullable bytes
    NullableBytes,
    /// Array
    ARRAY,
    /// Struct
    STRUCT,
}

/// Schema field definition
#[derive(Debug, Clone)]
pub struct Field {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: Type,
    /// Default value (optional)
    pub default_value: Option<FieldValue>,
    /// Documentation
    pub doc: Option<String>,
}

impl Field {
    /// Creates a new field
    pub fn new(name: &str, field_type: Type) -> Self {
        Field {
            name: name.to_string(),
            field_type,
            default_value: None,
            doc: None,
        }
    }

    /// Creates a field with default value
    pub fn with_default(name: &str, field_type: Type, default_value: FieldValue) -> Self {
        Field {
            name: name.to_string(),
            field_type,
            default_value: Some(default_value),
            doc: None,
        }
    }

    /// Creates a field with documentation
    pub fn with_doc(name: &str, field_type: Type, doc: &str) -> Self {
        Field {
            name: name.to_string(),
            field_type,
            default_value: None,
            doc: Some(doc.to_string()),
        }
    }
}

/// Field value
#[derive(Debug, Clone)]
pub enum FieldValue {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    String(String),
    Bytes(Vec<u8>),
}

/// Schema definition
#[derive(Debug, Clone)]
pub struct Schema {
    /// Schema fields
    pub fields: Vec<Field>,
}

impl Schema {
    /// Creates a new schema from fields
    pub fn new(fields: Vec<Field>) -> Self {
        Schema { fields }
    }

    /// Calculates the size of a struct
    pub fn size_of(&self, _struct: &Struct) -> usize {
        // Simplified size calculation
        // In a real implementation, this would calculate exact size
        let mut size = 0;
        for field in &self.fields {
            size += Self::field_size(field);
        }
        size
    }

    /// Calculates the size of a field
    fn field_size(field: &Field) -> usize {
        match field.field_type {
            Type::BOOLEAN => 1,
            Type::INT8 => 1,
            Type::INT16 => 2,
            Type::INT32 => 4,
            Type::INT64 => 8,
            Type::STRING => 4, // Length prefix
            Type::BYTES => 4,  // Length prefix
            _ => 0,            // Simplified
        }
    }

    /// Writes a struct to a buffer
    pub fn write(&self, buffer: &mut Vec<u8>, struct_data: &Struct) -> Result<(), ProtocolError> {
        for field in &self.fields {
            if let Some(value) = struct_data.get(&field.name) {
                Self::write_field(buffer, field, value)?;
            }
        }
        Ok(())
    }

    /// Writes a field value to a buffer
    fn write_field(
        buffer: &mut Vec<u8>,
        _field: &Field,
        value: &FieldValue,
    ) -> Result<(), ProtocolError> {
        match value {
            FieldValue::Boolean(b) => {
                if *b {
                    buffer.push(1);
                } else {
                    buffer.push(0);
                }
            }
            FieldValue::Int8(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            FieldValue::Int16(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            FieldValue::Int32(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            FieldValue::Int64(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            FieldValue::String(s) => {
                let bytes = s.as_bytes();
                buffer.extend_from_slice(&(bytes.len() as i32).to_le_bytes());
                buffer.extend_from_slice(bytes);
            }
            FieldValue::Bytes(b) => {
                buffer.extend_from_slice(&(b.len() as i32).to_le_bytes());
                buffer.extend_from_slice(b);
            }
        }
        Ok(())
    }

    /// Reads a struct from a buffer
    pub fn read(&self, buffer: &mut &[u8]) -> Result<Struct, ProtocolError> {
        let mut struct_data = Struct::new();
        for field in &self.fields {
            let value = Self::read_field(buffer, field)?;
            struct_data.set(&field.name, value);
        }
        Ok(struct_data)
    }

    /// Reads a field value from a buffer
    fn read_field(buffer: &mut &[u8], field: &Field) -> Result<FieldValue, ProtocolError> {
        match field.field_type {
            Type::BOOLEAN => {
                if buffer.is_empty() {
                    return Err(ProtocolError::InvalidData("Buffer too short".to_string()));
                }
                let b = buffer[0] != 0;
                *buffer = &buffer[1..];
                Ok(FieldValue::Boolean(b))
            }
            Type::INT8 => {
                if buffer.len() < 1 {
                    return Err(ProtocolError::InvalidData("Buffer too short".to_string()));
                }
                let v = i8::from_le_bytes([buffer[0]]);
                *buffer = &buffer[1..];
                Ok(FieldValue::Int8(v))
            }
            Type::INT16 => {
                if buffer.len() < 2 {
                    return Err(ProtocolError::InvalidData("Buffer too short".to_string()));
                }
                let v = i16::from_le_bytes([buffer[0], buffer[1]]);
                *buffer = &buffer[2..];
                Ok(FieldValue::Int16(v))
            }
            Type::INT32 => {
                if buffer.len() < 4 {
                    return Err(ProtocolError::InvalidData("Buffer too short".to_string()));
                }
                let v = i32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
                *buffer = &buffer[4..];
                Ok(FieldValue::Int32(v))
            }
            Type::INT64 => {
                if buffer.len() < 8 {
                    return Err(ProtocolError::InvalidData("Buffer too short".to_string()));
                }
                let v = i64::from_le_bytes([
                    buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6],
                    buffer[7],
                ]);
                *buffer = &buffer[8..];
                Ok(FieldValue::Int64(v))
            }
            Type::STRING => {
                if buffer.len() < 4 {
                    return Err(ProtocolError::InvalidData("Buffer too short".to_string()));
                }
                let len = i32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
                *buffer = &buffer[4..];
                if buffer.len() < len {
                    return Err(ProtocolError::InvalidData("Buffer too short".to_string()));
                }
                let s = String::from_utf8_lossy(&buffer[..len]).into_owned();
                *buffer = &buffer[len..];
                Ok(FieldValue::String(s))
            }
            _ => Err(ProtocolError::UnsupportedType(format!(
                "{:?}",
                field.field_type
            ))),
        }
    }
}

/// Struct data
#[derive(Debug, Clone)]
pub struct Struct {
    /// Field values
    values: HashMap<String, FieldValue>,
}

impl Struct {
    /// Creates a new empty struct
    pub fn new() -> Self {
        Struct {
            values: HashMap::new(),
        }
    }

    /// Sets a field value
    pub fn set(&mut self, name: &str, value: FieldValue) {
        self.values.insert(name.to_string(), value);
    }

    /// Gets a field value
    pub fn get(&self, name: &str) -> Option<&FieldValue> {
        self.values.get(name)
    }

    /// Gets a boolean field value
    pub fn get_boolean(&self, name: &str) -> Option<bool> {
        match self.get(name) {
            Some(FieldValue::Boolean(b)) => Some(*b),
            _ => None,
        }
    }

    /// Gets an i8 field value
    pub fn get_int8(&self, name: &str) -> Option<i8> {
        match self.get(name) {
            Some(FieldValue::Int8(v)) => Some(*v),
            _ => None,
        }
    }

    /// Gets an i16 field value
    pub fn get_int16(&self, name: &str) -> Option<i16> {
        match self.get(name) {
            Some(FieldValue::Int16(v)) => Some(*v),
            _ => None,
        }
    }

    /// Gets an i32 field value
    pub fn get_int32(&self, name: &str) -> Option<i32> {
        match self.get(name) {
            Some(FieldValue::Int32(v)) => Some(*v),
            _ => None,
        }
    }

    /// Gets an i64 field value
    pub fn get_int64(&self, name: &str) -> Option<i64> {
        match self.get(name) {
            Some(FieldValue::Int64(v)) => Some(*v),
            _ => None,
        }
    }

    /// Gets a string field value
    pub fn get_string(&self, name: &str) -> Option<String> {
        match self.get(name) {
            Some(FieldValue::String(s)) => Some(s.clone()),
            _ => None,
        }
    }

    /// Gets a bytes field value
    pub fn get_bytes(&self, name: &str) -> Option<Vec<u8>> {
        match self.get(name) {
            Some(FieldValue::Bytes(b)) => Some(b.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_creation() {
        let field = Field::new("test_field", Type::INT32);
        assert_eq!(field.name, "test_field");
        assert_eq!(field.field_type, Type::INT32);
    }

    #[test]
    fn test_schema_creation() {
        let schema = Schema::new(vec![
            Field::new("field1", Type::INT32),
            Field::new("field2", Type::STRING),
        ]);
        assert_eq!(schema.fields.len(), 2);
    }

    #[test]
    fn test_struct_operations() {
        let mut struct_data = Struct::new();
        struct_data.set("field1", FieldValue::Int32(42));
        struct_data.set(
            "field2",
            FieldValue::String("hello".to_string()),
        );

        assert_eq!(struct_data.get_int32("field1"), Some(42));
        assert_eq!(struct_data.get_string("field2"), Some("hello".to_string()));
    }

    #[test]
    fn test_schema_write_read() {
        let schema = {
            let mut s = Struct::new();
            s.set("field1", FieldValue::Int32(42));
            s.set(
                "field2",
                FieldValue::String("hello".to_string()),
            );
            s
        };

        let schema_def = Schema::new(vec![
            Field::new("field1", Type::INT32),
            Field::new("field2", Type::STRING),
        ]);

        let mut buffer = Vec::new();
        schema_def.write(&mut buffer, &schema).unwrap();

        let mut buffer_slice = buffer.as_slice();
        let read_schema = schema_def.read(&mut buffer_slice).unwrap();

        assert_eq!(read_schema.get_int32("field1"), Some(42));
        assert_eq!(read_schema.get_string("field2"), Some("hello".to_string()));
    }
}
