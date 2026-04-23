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

use crate::errors::ConnectError;
use serde_json::Value;

/// Transformation trait for transforming records.
///
/// This corresponds to `org.apache.kafka.connect.transforms.Transformation` in Java.
pub trait Transformation<R> {
    /// Configures this transformation.
    fn configure(&mut self, configs: std::collections::HashMap<String, Value>);

    /// Transforms the given record.
    ///
    /// Returns `Ok(None)` to indicate the record should be filtered/dropped.
    /// Returns `Ok(Some(record))` with the transformed record.
    /// Returns `Err(ConnectError)` on transformation errors.
    fn transform(&self, record: R) -> Result<Option<R>, ConnectError>;

    /// Closes this transformation.
    fn close(&mut self);
}
