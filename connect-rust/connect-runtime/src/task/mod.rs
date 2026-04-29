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

//! Task module for Kafka Connect runtime.
//!
//! This module defines the core WorkerTask abstraction that manages
//! individual source and sink tasks in Kafka Connect.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.WorkerTask` in Java.

mod worker_sink_task;
mod worker_source_task;
mod worker_task;

pub use worker_sink_task::*;
pub use worker_source_task::*;
pub use worker_task::*;
