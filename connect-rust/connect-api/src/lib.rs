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

//! Connect API module for Kafka Connect.
//!
//! This module provides the core API for Kafka Connect connectors.

pub mod errors;
pub mod components;
pub mod data;
pub mod header;
pub mod connector;
pub mod source;
pub mod sink;
pub mod storage;
pub mod health;
pub mod transforms;
pub mod rest;
pub mod util;

pub use errors::*;
pub use components::*;
pub use data::*;
pub use header::*;
pub use connector::*;
pub use source::*;
pub use sink::*;
pub use storage::*;
pub use health::*;
pub use transforms::*;
pub use rest::*;
pub use util::*;