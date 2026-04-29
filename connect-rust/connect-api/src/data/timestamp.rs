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

use crate::data::{ConnectSchema, Schema, SchemaBuilder};
use chrono::{DateTime, Utc};

/// Timestamp logical type for Connect schemas.
///
/// This corresponds to `org.apache.kafka.connect.data.Timestamp` in Java.
pub struct Timestamp;

impl Timestamp {
    /// The logical type name for Timestamp.
    pub const LOGICAL_NAME: &'static str = "org.apache.kafka.connect.data.Timestamp";

    /// Creates a Timestamp schema builder.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::int64().name(Self::LOGICAL_NAME)
    }

    /// Creates a Timestamp schema.
    pub fn schema() -> ConnectSchema {
        Self::builder().build()
    }

    /// Creates an optional Timestamp schema.
    pub fn optional_schema() -> ConnectSchema {
        Self::builder().optional().build()
    }

    /// Checks if a schema is a Timestamp schema.
    pub fn is_timestamp(schema: &dyn Schema) -> bool {
        schema.name() == Some(Self::LOGICAL_NAME)
    }

    /// Converts a timestamp to a Connect value (milliseconds since epoch).
    pub fn to_connect_data(timestamp: DateTime<Utc>) -> i64 {
        timestamp.timestamp_millis()
    }

    /// Converts a Connect value to a timestamp.
    pub fn from_connect_data(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis)
            .unwrap_or_else(|| DateTime::from_timestamp_millis(0).unwrap())
    }

    /// Converts a logical value (milliseconds) to a timestamp.
    /// Alias for from_connect_data for compatibility with Java API.
    pub fn from_logical(schema: &dyn Schema, millis: i64) -> DateTime<Utc> {
        Self::from_connect_data(millis)
    }

    /// Converts a logical value (milliseconds) to a timestamp - simple version.
    pub fn from_logical_simple(millis: i64) -> DateTime<Utc> {
        Self::from_connect_data(millis)
    }

    /// Converts a timestamp to a logical value (milliseconds).
    /// Alias for to_connect_data for compatibility with Java API.
    pub fn to_logical(schema: &dyn Schema, timestamp: DateTime<Utc>) -> i64 {
        Self::to_connect_data(timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Schema;

    #[test]
    fn test_timestamp_schema() {
        let schema = Timestamp::schema();
        assert_eq!(schema.name(), Some(Timestamp::LOGICAL_NAME));
        assert_eq!(schema.r#type(), crate::data::SchemaType::Int64);
    }

    #[test]
    fn test_optional_timestamp_schema() {
        let schema = Timestamp::optional_schema();
        assert_eq!(schema.name(), Some(Timestamp::LOGICAL_NAME));
        assert!(schema.is_optional());
    }

    #[test]
    fn test_is_timestamp() {
        let schema = Timestamp::schema();
        assert!(Timestamp::is_timestamp(&schema));

        let other_schema = crate::data::SchemaBuilder::int64().build();
        assert!(!Timestamp::is_timestamp(&other_schema));
    }

    #[test]
    fn test_to_connect_data() {
        let epoch = DateTime::from_timestamp_millis(0).unwrap();
        let millis = Timestamp::to_connect_data(epoch);
        assert_eq!(millis, 0);

        let ts = DateTime::from_timestamp_millis(1234567890).unwrap();
        let millis = Timestamp::to_connect_data(ts);
        assert_eq!(millis, 1234567890);
    }

    #[test]
    fn test_from_connect_data() {
        let ts = Timestamp::from_connect_data(0);
        assert_eq!(ts.timestamp_millis(), 0);

        let ts = Timestamp::from_connect_data(1234567890);
        assert_eq!(ts.timestamp_millis(), 1234567890);
    }

    #[test]
    fn test_from_logical_simple() {
        let ts = Timestamp::from_logical_simple(0);
        assert_eq!(ts.timestamp_millis(), 0);
    }

    #[test]
    fn test_round_trip() {
        let original = DateTime::from_timestamp_millis(1609459200000).unwrap(); // 2021-01-01
        let millis = Timestamp::to_connect_data(original);
        let recovered = Timestamp::from_connect_data(millis);
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_current_timestamp() {
        let now = chrono::Utc::now();
        let millis = Timestamp::to_connect_data(now);
        let recovered = Timestamp::from_connect_data(millis);

        // Should be within a few milliseconds
        let diff = (now.timestamp_millis() - recovered.timestamp_millis()).abs();
        assert!(diff < 100);
    }
}
