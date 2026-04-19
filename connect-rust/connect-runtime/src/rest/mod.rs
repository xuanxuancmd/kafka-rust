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

//! REST module for Kafka Connect runtime.
//!
//! This module provides REST server and client implementations for
//! Kafka Connect's HTTP API. The REST API allows users to:
//! - Manage connectors (create, delete, pause, resume)
//! - Monitor connector and task status
//! - Validate connector configurations
//! - List available connector plugins
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest` in Java.

mod entities;
mod rest_client;
mod rest_server;
pub mod resources;

pub use entities::*;
pub use rest_client::*;
pub use rest_server::*;