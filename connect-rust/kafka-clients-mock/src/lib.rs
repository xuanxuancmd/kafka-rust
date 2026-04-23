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

//! Mock implementations for Kafka clients.
//!
//! This module provides mock implementations for testing purposes.

pub mod admin;
pub mod consumer_record;
pub mod isolation_level;
pub mod kafka_consumer;
pub mod kafka_producer;
pub mod offset_and_metadata;
pub mod offset_commit_callback;
pub mod producer_record;
pub mod record_metadata;

pub use admin::*;
pub use consumer_record::*;
pub use isolation_level::*;
pub use kafka_consumer::*;
pub use kafka_producer::*;
pub use offset_and_metadata::*;
pub use offset_commit_callback::*;
pub use producer_record::*;
pub use record_metadata::*;
