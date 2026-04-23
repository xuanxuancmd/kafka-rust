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
use chrono::NaiveTime;

/// Time logical type for Connect schemas.
///
/// This corresponds to `org.apache.kafka.connect.data.Time` in Java.
pub struct Time;

impl Time {
    /// The logical type name for Time.
    pub const LOGICAL_NAME: &'static str = "org.apache.kafka.connect.data.Time";

    /// Creates a Time schema builder.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::int32().name(Self::LOGICAL_NAME)
    }

    /// Creates a Time schema.
    pub fn schema() -> ConnectSchema {
        Self::builder().build()
    }

    /// Creates an optional Time schema.
    pub fn optional_schema() -> ConnectSchema {
        Self::builder().optional().build()
    }

    /// Checks if a schema is a Time schema.
    pub fn is_time(schema: &dyn Schema) -> bool {
        schema.name() == Some(Self::LOGICAL_NAME)
    }

    /// Converts a time to a Connect value (milliseconds since midnight).
    pub fn to_connect_data(time: NaiveTime) -> i32 {
        // Milliseconds since midnight
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        (time - midnight).num_milliseconds() as i32
    }

    /// Converts a Connect value to a time.
    pub fn from_connect_data(millis: i32) -> NaiveTime {
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        // chrono::Duration doesn't support milliseconds directly, use microseconds
        midnight + chrono::Duration::microseconds(millis as i64 * 1000)
    }

    /// Converts a logical value (milliseconds) to a time.
    /// Alias for from_connect_data for compatibility with Java API.
    pub fn from_logical(schema: &dyn Schema, millis: i32) -> NaiveTime {
        Self::from_connect_data(millis)
    }

    /// Converts a logical value (milliseconds) to a time - simple version.
    pub fn from_logical_simple(millis: i32) -> NaiveTime {
        Self::from_connect_data(millis)
    }

    /// Converts a time to a logical value (milliseconds).
    /// Alias for to_connect_data for compatibility with Java API.
    pub fn to_logical(schema: &dyn Schema, time: NaiveTime) -> i32 {
        Self::to_connect_data(time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Schema;
    use chrono::Timelike;

    #[test]
    fn test_time_schema() {
        let schema = Time::schema();
        assert_eq!(schema.name(), Some(Time::LOGICAL_NAME));
        assert_eq!(schema.r#type(), crate::data::SchemaType::Int32);
    }

    #[test]
    fn test_optional_time_schema() {
        let schema = Time::optional_schema();
        assert_eq!(schema.name(), Some(Time::LOGICAL_NAME));
        assert!(schema.is_optional());
    }

    #[test]
    fn test_is_time() {
        let schema = Time::schema();
        assert!(Time::is_time(&schema));

        let other_schema = crate::data::SchemaBuilder::int32().build();
        assert!(!Time::is_time(&other_schema));
    }

    #[test]
    fn test_to_connect_data() {
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let millis = Time::to_connect_data(midnight);
        assert_eq!(millis, 0);

        let time = NaiveTime::from_hms_opt(1, 0, 0).unwrap();
        let millis = Time::to_connect_data(time);
        assert_eq!(millis, 3600000); // 1 hour in milliseconds

        let time = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let millis = Time::to_connect_data(time);
        assert_eq!(millis, 12 * 3600000);

        let time = NaiveTime::from_hms_milli_opt(0, 0, 1, 500).unwrap();
        let millis = Time::to_connect_data(time);
        assert_eq!(millis, 1500);
    }

    #[test]
    fn test_from_connect_data() {
        let time = Time::from_connect_data(0);
        assert_eq!(time, NaiveTime::from_hms_opt(0, 0, 0).unwrap());

        let time = Time::from_connect_data(3600000);
        assert_eq!(time, NaiveTime::from_hms_opt(1, 0, 0).unwrap());

        let time = Time::from_connect_data(1500);
        assert_eq!(time, NaiveTime::from_hms_milli_opt(0, 0, 1, 500).unwrap());
    }

    #[test]
    fn test_from_logical_simple() {
        let time = Time::from_logical_simple(0);
        assert_eq!(time, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    }

    #[test]
    fn test_round_trip() {
        let original = NaiveTime::from_hms_milli_opt(14, 30, 45, 123).unwrap();
        let millis = Time::to_connect_data(original);
        let recovered = Time::from_connect_data(millis);
        // Note: round trip may lose sub-millisecond precision
        assert_eq!(original.hour(), recovered.hour());
        assert_eq!(original.minute(), recovered.minute());
        assert_eq!(original.second(), recovered.second());
    }
}
