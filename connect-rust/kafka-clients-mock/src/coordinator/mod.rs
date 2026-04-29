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

//! Mock coordinator implementations for Kafka Connect distributed runtime.
//!
//! This module provides simplified mock implementations of the coordinator
//! interfaces for testing purposes. The full AbstractCoordinator from
//! kafka-clients is too complex for testing, so this simplified version
//! supports only the core functionality needed by Connect.

pub mod worker_coordinator;

pub use worker_coordinator::*;
