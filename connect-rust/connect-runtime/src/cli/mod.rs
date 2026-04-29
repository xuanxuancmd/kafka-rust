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

//! CLI module for Kafka Connect.
//!
//! This module provides command line utilities for running Kafka Connect
//! in distributed or standalone mode.
//!
//! # Components
//!
//! - **AbstractConnectCli**: Common initialization logic for Kafka Connect
//! - **ConnectDistributed**: Runs Kafka Connect in distributed mode
//! - **ConnectStandalone**: Runs Kafka Connect in standalone mode
//!
//! Corresponds to `org.apache.kafka.connect.cli` in Java.

mod abstract_connect_cli;
mod connect_distributed;
mod connect_standalone;

pub use abstract_connect_cli::*;
pub use connect_distributed::*;
pub use connect_standalone::*;