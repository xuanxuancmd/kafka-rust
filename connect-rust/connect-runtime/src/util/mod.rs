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

//! Utility module for Kafka Connect runtime.
//!
//! This module provides utility classes that correspond to
//! `org.apache.kafka.connect.util` in Java.
//!
//! # Components
//!
//! - **Callback**: Generic callback interface for async operations
//! - **ConnectorTaskId**: Unique identifier for connector tasks
//! - **Table**: Two-dimensional table structure
//! - **Stage**: Stage tracking for operations
//! - **TemporaryStage**: Auto-completing stage wrapper
//! - **ConnectUtils**: Connect utility functions
//! - **SinkUtils**: Sink connector utilities
//! - **RetryUtil**: Retry logic with timeout
//! - **TopicAdmin**: Topic management via Admin client
//! - **SharedTopicAdmin**: Lazy and atomic TopicAdmin holder
//! - **LoggingContext**: MDC logging context management
//! - **KafkaBasedLog**: Kafka-backed shared log storage

mod callback;
mod connect_utils;
mod connector_task_id;
mod kafka_based_log;
mod logging_context;
mod retry_util;
mod shared_topic_admin;
mod sink_utils;
mod stage;
mod table;
mod temporary_stage;
mod topic_admin;

pub use callback::*;
pub use connect_utils::*;
pub use connector_task_id::*;
pub use kafka_based_log::*;
pub use logging_context::*;
pub use retry_util::*;
pub use shared_topic_admin::*;
pub use sink_utils::*;
pub use stage::*;
pub use table::*;
pub use temporary_stage::*;
pub use topic_admin::*;
