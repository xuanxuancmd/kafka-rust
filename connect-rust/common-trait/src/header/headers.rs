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

use serde_json::Value;

/// Header represents a single header in a Kafka record.
///
/// This corresponds to `org.apache.kafka.common.header.Header` in Java.
pub trait Header {
    /// Returns the header key.
    fn key(&self) -> &str;

    /// Returns the header value as bytes.
    fn value(&self) -> &[u8];
}

/// Headers trait for managing multiple headers.
///
/// This corresponds to `org.apache.kafka.common.header.Headers` in Java.
pub trait Headers {
    /// Adds a header with the given key and value.
    fn add(&mut self, key: impl Into<String>, value: Vec<u8>) -> &mut Self;

    /// Adds a header with the given key and JSON value.
    fn add_json(&mut self, key: impl Into<String>, value: Value) -> &mut Self;

    /// Removes all headers with the given key.
    fn remove(&mut self, key: &str) -> &mut Self;

    /// Returns the last header with the given key.
    fn last_header(&self, key: &str) -> Option<&dyn Header>;

    /// Returns all headers with the given key.
    fn headers(&self, key: &str) -> Vec<&dyn Header>;

    /// Returns an iterator over all headers.
    fn iterator(&self) -> Box<dyn Iterator<Item = &dyn Header> + '_>;
}
