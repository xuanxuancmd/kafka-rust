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

/// A typed value that can be serialized/deserialized
/// This enum provides a concrete representation of values that satisfies Clone + Send + Sync
#[derive(Debug, Clone)]
pub enum ProtocolValue {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    String(String),
    Struct(Vec<ProtocolValue>),
    Null,
}

impl ProtocolValue {
    pub fn as_int16(&self) -> Option<i16> {
        match self {
            ProtocolValue::Int16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_int32(&self) -> Option<i32> {
        match self {
            ProtocolValue::Int32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_int64(&self) -> Option<i64> {
        match self {
            ProtocolValue::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            ProtocolValue::String(v) => Some(v.as_str()),
            _ => None,
        }
    }

    pub fn as_struct(&self) -> Option<&Vec<ProtocolValue>> {
        match self {
            ProtocolValue::Struct(v) => Some(v),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ProtocolValue::Null)
    }
}

impl std::fmt::Display for ProtocolValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolValue::Int16(v) => write!(f, "{}", v),
            ProtocolValue::Int32(v) => write!(f, "{}", v),
            ProtocolValue::Int64(v) => write!(f, "{}", v),
            ProtocolValue::String(v) => write!(f, "{}", v),
            ProtocolValue::Struct(v) => {
                write!(f, "{{")?;
                for (i, val) in v.iter().enumerate() {
                    write!(f, "{}", val)?;
                    if i < v.len() - 1 {
                        write!(f, ",")?;
                    }
                }
                write!(f, "}}")?;
                Ok(())
            }
            ProtocolValue::Null => write!(f, "null"),
        }
    }
}
