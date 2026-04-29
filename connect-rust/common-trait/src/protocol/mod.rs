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

//! Protocol Types module - corresponds to Java: org.apache.kafka.common.protocol.types
//!
//! This module provides the serialization/deserialization infrastructure for Kafka protocol messages.
//! It implements the Type, Field, Schema, and Struct classes from Kafka's protocol types package.

pub mod field;
pub mod protocol_struct;
pub mod protocol_type;
pub mod protocol_value;
pub mod schema;
pub mod schema_error;

pub use field::{BoundField, Field};
pub use protocol_struct::Struct;
pub use protocol_type::{Type, INT16, INT32, INT64, STRING};
pub use protocol_value::ProtocolValue;
pub use schema::Schema;
pub use schema_error::SchemaError;
