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
//! - `file_offset_backing_store`: File-based implementation of OffsetBackingStore
//! - `kafka_offset_backing_store`: Kafka topic-based implementation of OffsetBackingStore
//! - `kafka_config_backing_store`: Kafka topic-based implementation of ConfigBackingStore
//! - `kafka_status_backing_store`: Kafka topic-based implementation of StatusBackingStore
//! - `kafka_topic_based_backing_store`: Base class for Kafka topic-based stores
//! - `connector_offset_backing_store`: Wrapper for connector-specific offset storage
//! - `offset_storage_writer`: Buffered writer for offset data
//! - `offset_storage_reader_impl`: Implementation of OffsetStorageReader
//! - `offset_utils`: Utility functions for offset storage
//! - `applied_connector_config`: Wrapper for applied connector configurations
//! - `closeable_offset_storage_reader`: Closeable offset storage reader
//! - `privileged_write_exception`: Exception for privileged write failures

mod applied_connector_config;
mod closeable_offset_storage_reader;
mod connector_offset_backing_store;
mod file_offset_backing_store;
mod kafka_config_backing_store;
mod kafka_offset_backing_store;
mod kafka_status_backing_store;
mod kafka_topic_based_backing_store;
mod memory_config_backing_store;
mod memory_offset_backing_store;
mod memory_status_backing_store;
mod offset_storage_reader_impl;
mod offset_storage_writer;
mod offset_utils;
mod privileged_write_exception;

pub use applied_connector_config::*;
pub use closeable_offset_storage_reader::*;
pub use connector_offset_backing_store::*;
pub use file_offset_backing_store::*;
pub use kafka_config_backing_store::*;
pub use kafka_offset_backing_store::*;
pub use kafka_status_backing_store::*;
pub use kafka_topic_based_backing_store::*;
pub use memory_config_backing_store::*;
pub use memory_offset_backing_store::*;
pub use memory_status_backing_store::*;
pub use offset_storage_reader_impl::*;
pub use offset_storage_writer::*;
pub use offset_utils::*;
pub use privileged_write_exception::*;