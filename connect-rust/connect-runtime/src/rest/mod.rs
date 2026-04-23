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
pub mod errors;
mod rest_client;
mod rest_server;
pub mod resources;
pub mod util;

// New REST core components
mod rest_request_timeout;
pub mod rest_server_config;
mod herder_request_handler;
mod internal_request_signature;

pub use entities::*;
pub use errors::*;
pub use rest_client::*;
// Note: rest_server exports InternalServerConfig (for internal server binding config)
// and RestServer. For full REST configuration, use rest_server_config::RestServerConfig
pub use rest_server::*;
pub use util::*;

// Export new components
pub use rest_request_timeout::*;
pub use rest_server_config::RestServerConfig;
pub use herder_request_handler::*;
pub use internal_request_signature::*;