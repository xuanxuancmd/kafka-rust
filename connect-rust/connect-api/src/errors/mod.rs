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

//! Errors module for Kafka Connect.
//!
//! This module provides exception types that correspond to
//! `org.apache.kafka.connect.errors` in Java.

mod already_exists_exception;
mod connect_exception;
mod data_exception;
mod illegal_worker_state_exception;
mod not_found_exception;
mod retriable_exception;
mod schema_builder_exception;
mod schema_projector_exception;

pub use already_exists_exception::*;
pub use connect_exception::*;
pub use data_exception::*;
pub use illegal_worker_state_exception::*;
pub use not_found_exception::*;
pub use retriable_exception::*;
pub use schema_builder_exception::*;
pub use schema_projector_exception::*;

// Legacy enum-based error type for backward compatibility
// This is deprecated and will be removed in future versions
mod connect_error;

pub use connect_error::ConnectError;
