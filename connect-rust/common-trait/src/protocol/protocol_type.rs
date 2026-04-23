/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::io::{Cursor, Read};

use super::ProtocolValue;
use super::SchemaError;

/// A serializable type - corresponds to Java: org.apache.kafka.common.protocol.types.Type
pub trait Type: Send + Sync {
    /// Write the typed object to the buffer
    fn write(&self, buffer: &mut Vec<u8>, value: &ProtocolValue) -> Result<(), SchemaError>;

    /// Read the typed object from the buffer
    fn read(&self, buffer: &mut Cursor<&[u8]>) -> Result<ProtocolValue, SchemaError>;

    /// Validate the object
    fn validate(&self, value: &ProtocolValue) -> Result<(), SchemaError>;

    /// Return the size of the object in bytes
    fn size_of(&self, value: &ProtocolValue) -> usize;

    /// Check if the type supports null values
    fn is_nullable(&self) -> bool {
        false
    }

    /// Get the type name
    fn type_name(&self) -> &'static str;
}

/// Helper function to get remaining bytes in cursor
fn remaining(cursor: &Cursor<&[u8]>) -> usize {
    cursor.get_ref().len() - cursor.position() as usize
}

/// INT16 type - represents an integer between -2^15 and 2^15-1 inclusive.
/// Encoded using two bytes in network byte order (big-endian).
pub struct Int16Type;

impl Type for Int16Type {
    fn write(&self, buffer: &mut Vec<u8>, value: &ProtocolValue) -> Result<(), SchemaError> {
        let v = value
            .as_int16()
            .ok_or_else(|| SchemaError::new("Value is not an INT16"))?;
        buffer.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn read(&self, buffer: &mut Cursor<&[u8]>) -> Result<ProtocolValue, SchemaError> {
        let mut bytes = [0u8; 2];
        buffer.read_exact(&mut bytes)?;
        Ok(ProtocolValue::Int16(i16::from_be_bytes(bytes)))
    }

    fn size_of(&self, _value: &ProtocolValue) -> usize {
        2
    }

    fn validate(&self, value: &ProtocolValue) -> Result<(), SchemaError> {
        if value.as_int16().is_some() {
            Ok(())
        } else {
            Err(SchemaError::new("Value is not a Short"))
        }
    }

    fn type_name(&self) -> &'static str {
        "INT16"
    }
}

pub const INT16: Int16Type = Int16Type;

/// INT32 type - represents an integer between -2^31 and 2^31-1 inclusive.
/// Encoded using four bytes in network byte order (big-endian).
pub struct Int32Type;

impl Type for Int32Type {
    fn write(&self, buffer: &mut Vec<u8>, value: &ProtocolValue) -> Result<(), SchemaError> {
        let v = value
            .as_int32()
            .ok_or_else(|| SchemaError::new("Value is not an INT32"))?;
        buffer.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn read(&self, buffer: &mut Cursor<&[u8]>) -> Result<ProtocolValue, SchemaError> {
        let mut bytes = [0u8; 4];
        buffer.read_exact(&mut bytes)?;
        Ok(ProtocolValue::Int32(i32::from_be_bytes(bytes)))
    }

    fn size_of(&self, _value: &ProtocolValue) -> usize {
        4
    }

    fn validate(&self, value: &ProtocolValue) -> Result<(), SchemaError> {
        if value.as_int32().is_some() {
            Ok(())
        } else {
            Err(SchemaError::new("Value is not an Integer"))
        }
    }

    fn type_name(&self) -> &'static str {
        "INT32"
    }
}

pub const INT32: Int32Type = Int32Type;

/// INT64 type - represents an integer between -2^63 and 2^63-1 inclusive.
/// Encoded using eight bytes in network byte order (big-endian).
pub struct Int64Type;

impl Type for Int64Type {
    fn write(&self, buffer: &mut Vec<u8>, value: &ProtocolValue) -> Result<(), SchemaError> {
        let v = value
            .as_int64()
            .ok_or_else(|| SchemaError::new("Value is not an INT64"))?;
        buffer.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn read(&self, buffer: &mut Cursor<&[u8]>) -> Result<ProtocolValue, SchemaError> {
        let mut bytes = [0u8; 8];
        buffer.read_exact(&mut bytes)?;
        Ok(ProtocolValue::Int64(i64::from_be_bytes(bytes)))
    }

    fn size_of(&self, _value: &ProtocolValue) -> usize {
        8
    }

    fn validate(&self, value: &ProtocolValue) -> Result<(), SchemaError> {
        if value.as_int64().is_some() {
            Ok(())
        } else {
            Err(SchemaError::new("Value is not a Long"))
        }
    }

    fn type_name(&self) -> &'static str {
        "INT64"
    }
}

pub const INT64: Int64Type = Int64Type;

/// STRING type - represents a sequence of characters.
/// First the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding.
pub struct StringType;

impl Type for StringType {
    fn write(&self, buffer: &mut Vec<u8>, value: &ProtocolValue) -> Result<(), SchemaError> {
        let s = value
            .as_string()
            .ok_or_else(|| SchemaError::new("Value is not a String"))?;
        let bytes = s.as_bytes();
        if bytes.len() > i16::MAX as usize {
            return Err(SchemaError::new(format!(
                "String length {} is larger than the maximum string length.",
                bytes.len()
            )));
        }
        buffer.extend_from_slice(&(bytes.len() as i16).to_be_bytes());
        buffer.extend_from_slice(bytes);
        Ok(())
    }

    fn read(&self, buffer: &mut Cursor<&[u8]>) -> Result<ProtocolValue, SchemaError> {
        let mut len_bytes = [0u8; 2];
        buffer.read_exact(&mut len_bytes)?;
        let length = i16::from_be_bytes(len_bytes);
        if length < 0 {
            return Err(SchemaError::new(format!(
                "String length {} cannot be negative",
                length
            )));
        }
        let length = length as usize;
        let rem = remaining(buffer);
        if length > rem {
            return Err(SchemaError::new(format!(
                "Error reading string of length {}, only {} bytes available",
                length, rem
            )));
        }
        let mut bytes = vec![0u8; length];
        buffer.read_exact(&mut bytes)?;
        Ok(ProtocolValue::String(String::from_utf8(bytes).map_err(
            |e| SchemaError::new(format!("Invalid UTF-8: {}", e)),
        )?))
    }

    fn size_of(&self, value: &ProtocolValue) -> usize {
        value.as_string().map(|s| 2 + s.len()).unwrap_or(2)
    }

    fn validate(&self, value: &ProtocolValue) -> Result<(), SchemaError> {
        if value.as_string().is_some() {
            Ok(())
        } else {
            Err(SchemaError::new("Value is not a String"))
        }
    }

    fn type_name(&self) -> &'static str {
        "STRING"
    }
}

pub const STRING: StringType = StringType;
