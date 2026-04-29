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

//! Decimal format enum for JSON serialization.
//!
//! Corresponds to: `org.apache.kafka.connect.json.DecimalFormat` in Java Kafka Connect.
//!
//! Source: connect/json/src/main/java/org/apache/kafka/connect/json/DecimalFormat.java

use std::fmt;
use std::str::FromStr;

/// Decimal format enum for JSON serialization of decimal values.
///
/// Corresponds to: `org.apache.kafka.connect.json.DecimalFormat`
///
/// This enum defines how decimal values are serialized in JSON:
/// - `Base64`: Serialize as base64-encoded string (default in Java)
/// - `Numeric`: Serialize as JSON number
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecimalFormat {
    /// Serialize decimals as base64-encoded strings.
    /// This is the default format used by Kafka Connect.
    Base64,
    /// Serialize decimals as JSON numbers.
    Numeric,
}

impl FromStr for DecimalFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "BASE64" => Ok(DecimalFormat::Base64),
            "NUMERIC" => Ok(DecimalFormat::Numeric),
            _ => Err(format!(
                "Invalid DecimalFormat: {}, expected 'BASE64' or 'NUMERIC'",
                s
            )),
        }
    }
}

impl fmt::Display for DecimalFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecimalFormat::Base64 => write!(f, "BASE64"),
            DecimalFormat::Numeric => write!(f, "NUMERIC"),
        }
    }
}
