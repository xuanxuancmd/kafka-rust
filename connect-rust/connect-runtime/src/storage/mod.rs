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

//! Storage backing store implementations for Kafka Connect.
//!
//! This module provides implementations of storage interfaces used by Kafka Connect
//! to persist connector offsets, configurations, and status information.
//!
//! # Modules
//! - `memory_offset_backing_store`: In-memory implementation of OffsetBackingStore
//! - `memory_config_backing_store`: In-memory implementation of ConfigBackingStore
//! - `memory_status_backing_store`: In-memory implementation of StatusBackingStore
//! - `offset_storage_writer`: Buffered writer for offset data
//! - `offset_storage_reader_impl`: Implementation of OffsetStorageReader

mod memory_config_backing_store;
mod memory_offset_backing_store;
mod memory_status_backing_store;
mod offset_storage_reader_impl;
mod offset_storage_writer;
mod offset_utils;

pub use memory_config_backing_store::*;
pub use memory_offset_backing_store::*;
pub use memory_status_backing_store::*;
pub use offset_storage_reader_impl::*;
pub use offset_storage_writer::*;
pub use offset_utils::*;