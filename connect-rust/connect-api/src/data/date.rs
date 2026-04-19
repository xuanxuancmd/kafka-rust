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
use chrono::NaiveDate;

/// Date logical type for Connect schemas.
///
/// This corresponds to `org.apache.kafka.connect.data.Date` in Java.
pub struct Date;

impl Date {
    /// The logical type name for Date.
    pub const LOGICAL_NAME: &'static str = "org.apache.kafka.connect.data.Date";

    /// Creates a Date schema builder.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::int32().name(Self::LOGICAL_NAME)
    }

    /// Creates a Date schema.
    pub fn schema() -> ConnectSchema {
        Self::builder().build()
    }

    /// Creates an optional Date schema.
    pub fn optional_schema() -> ConnectSchema {
        Self::builder().optional().build()
    }

    /// Checks if a schema is a Date schema.
    pub fn is_date(schema: &dyn Schema) -> bool {
        schema.name() == Some(Self::LOGICAL_NAME)
    }

    /// Converts a date to a Connect value (days since epoch).
    pub fn to_connect_data(date: NaiveDate) -> i32 {
        // Days since Unix epoch (1970-01-01)
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (date - epoch).num_days() as i32
    }

    /// Converts a Connect value to a date.
    pub fn from_connect_data(days: i32) -> NaiveDate {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        epoch + chrono::Duration::days(days as i64)
    }

    /// Converts a logical value (days) to a date.
    /// Alias for from_connect_data for compatibility with Java API.
    pub fn from_logical(schema: &dyn Schema, days: i32) -> NaiveDate {
        Self::from_connect_data(days)
    }

    /// Converts a logical value (days) to a date - simple version.
    pub fn from_logical_simple(days: i32) -> NaiveDate {
        Self::from_connect_data(days)
    }

    /// Converts a date to a logical value (days).
    /// Alias for to_connect_data for compatibility with Java API.
    pub fn to_logical(schema: &dyn Schema, date: NaiveDate) -> i32 {
        Self::to_connect_data(date)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Schema;

    #[test]
    fn test_date_schema() {
        let schema = Date::schema();
        assert_eq!(schema.name(), Some(Date::LOGICAL_NAME));
        assert_eq!(schema.r#type(), crate::data::SchemaType::Int32);
    }

    #[test]
    fn test_optional_date_schema() {
        let schema = Date::optional_schema();
        assert_eq!(schema.name(), Some(Date::LOGICAL_NAME));
        assert!(schema.is_optional());
    }

    #[test]
    fn test_is_date() {
        let schema = Date::schema();
        assert!(Date::is_date(&schema));

        let other_schema = crate::data::SchemaBuilder::int32().build();
        assert!(!Date::is_date(&other_schema));
    }

    #[test]
    fn test_to_connect_data() {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days = Date::to_connect_data(epoch);
        assert_eq!(days, 0);

        let date = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
        let days = Date::to_connect_data(date);
        assert_eq!(days, 1);

        let date = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let days = Date::to_connect_data(date);
        assert!(days > 0);
    }

    #[test]
    fn test_from_connect_data() {
        let date = Date::from_connect_data(0);
        assert_eq!(date, NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

        let date = Date::from_connect_data(1);
        assert_eq!(date, NaiveDate::from_ymd_opt(1970, 1, 2).unwrap());

        let date = Date::from_connect_data(365);
        assert_eq!(date, NaiveDate::from_ymd_opt(1971, 1, 1).unwrap());
    }

    #[test]
    fn test_from_logical_simple() {
        let date = Date::from_logical_simple(0);
        assert_eq!(date, NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    }

    #[test]
    fn test_round_trip() {
        let original = NaiveDate::from_ymd_opt(2020, 6, 15).unwrap();
        let days = Date::to_connect_data(original);
        let recovered = Date::from_connect_data(days);
        assert_eq!(original, recovered);
    }
}
