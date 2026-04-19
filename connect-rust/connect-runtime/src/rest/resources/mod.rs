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

//! REST resources module for Kafka Connect.
//!
//! This module provides REST handlers (resources) for the Kafka Connect API.
//! Each resource corresponds to a set of related endpoints.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.resources` in Java.

mod connectors_resource;
mod tasks_resource;
mod connector_plugins_resource;

pub use connectors_resource::*;
pub use tasks_resource::*;
pub use connector_plugins_resource::*;