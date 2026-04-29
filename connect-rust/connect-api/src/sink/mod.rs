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

//! Sink module for Kafka Connect.

mod sink_connector_context;
mod sink_task_context;
mod errant_record_reporter;
mod sink_connector;
mod sink_task;
mod sink_record;

pub use sink_connector_context::*;
pub use sink_task_context::*;
pub use errant_record_reporter::*;
pub use sink_connector::*;
pub use sink_task::*;
pub use sink_record::*;