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

//! Message type for REST API responses.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.entities.Message` in Java.

use serde::{Deserialize, Serialize};

/// A message for REST API responses.
///
/// Used for simple text responses from REST API operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// The message text.
    message: String,
}

impl Message {
    /// Creates a new message.
    pub fn new(message: String) -> Self {
        Message { message }
    }

    /// Returns the message text.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
