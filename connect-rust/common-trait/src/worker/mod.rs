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

//! Worker module for Kafka Connect runtime.
//!
//! This module defines the core Worker trait that manages connectors and tasks
//! in a Kafka Connect cluster.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.Worker` in Java.

mod callback;
mod connector_offsets;
mod connector_task_id;
mod message;
mod status_listener;
mod target_state;
mod worker;

pub use callback::*;
pub use connector_offsets::*;
pub use connector_task_id::*;
pub use message::*;
pub use status_listener::*;
pub use target_state::*;
pub use worker::*;
