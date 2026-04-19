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

//! Common traits for Kafka clients and connect modules.
//!
//! This module defines the common interfaces used by both kafka-clients and kafka-common.

pub mod cache;
pub mod config;
pub mod configurable;
pub mod errors;
pub mod header;
pub mod herder;
pub mod kafka_exception;
pub mod metrics;
pub mod protocol;
pub mod record;
pub mod serialization;
pub mod storage;
pub mod topic_partition;
pub mod util;
pub mod utils;
pub mod worker;

// Re-export common types
pub use cache::*;
pub use config::*;
pub use configurable::*;
pub use errors::*;
pub use header::*;
pub use herder::*;
pub use kafka_exception::*;
pub use metrics::*;
pub use protocol::*;
pub use record::*;
pub use serialization::*;
pub use storage::*;
pub use topic_partition::*;
pub use util::*;
pub use utils::*;
pub use worker::*;
