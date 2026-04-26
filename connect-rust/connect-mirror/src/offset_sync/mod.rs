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

//! Offset synchronization module for MirrorMaker 2.
//!
//! This module provides:
//! - OffsetSyncWriter: generates and writes offset synchronization records
//! - OffsetSyncStore: reads and stores offset synchronization records for translation
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.OffsetSyncWriter, OffsetSyncStore

pub mod offset_sync_store;
pub mod offset_sync_writer;

pub use offset_sync_store::*;
pub use offset_sync_writer::*;
