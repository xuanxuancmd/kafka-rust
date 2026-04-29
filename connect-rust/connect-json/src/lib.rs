// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! JSON converter module for Kafka Connect.
//!
//! This module provides JSON serialization and deserialization for Kafka Connect.
//! It corresponds to `org.apache.kafka.connect.json` in Java Kafka Connect.
//!
//! ## Structure
//!
//! - `DecimalFormat`: Decimal format enum (BASE64 or NUMERIC)
//! - `JsonSchema`: JSON schema constants and envelope structure
//! - `JsonSerializer`: JSON serializer for Kafka Connect
//! - `JsonDeserializer`: JSON deserializer for Kafka Connect
//! - `JsonConverterConfig`: Configuration for JSON converter
//! - `JsonConverter`: Main JSON converter implementing Converter and HeaderConverter

pub mod decimal_format;
pub mod json_converter;
pub mod json_converter_config;
pub mod json_deserializer;
pub mod json_schema;
pub mod json_serializer;

pub use decimal_format::*;
pub use json_converter::*;
pub use json_converter_config::*;
pub use json_deserializer::*;
pub use json_schema::*;
pub use json_serializer::*;
