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

//! Kafka Connect API for Rust
//!
//! This crate provides core types and traits for implementing Kafka Connect connectors
//! in Rust.

pub mod config;
pub mod converters;
pub mod data;
pub mod error;
pub mod health;
pub mod interfaces;
pub mod policy;
pub mod rest;
pub mod storage;
pub mod transforms;
pub mod utils;

pub mod connector_impl;
pub mod connector_types;

// Re-export commonly used types from connector_impl
pub use connector_impl::*;

// Re-export data module types for convenience
pub use data::{Schema, SchemaAndValue, SchemaBuilder, SchemaProjector, SchemaType, Struct, Type};
