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

//! REST errors module for Kafka Connect.
//!
//! This module provides REST-specific exception handling that corresponds to
//! `org.apache.kafka.connect.runtime.rest.errors` in Java.
//!
//! # Components
//!
//! - **ConnectRestException**: Base REST exception with HTTP status code and error code
//! - **BadRequestException**: Exception for 400 Bad Request errors
//! - **ConnectExceptionMapper**: Maps exceptions to HTTP responses

mod connect_rest_exception;
mod bad_request_exception;
mod connect_exception_mapper;

pub use connect_rest_exception::*;
pub use bad_request_exception::*;
pub use connect_exception_mapper::*;