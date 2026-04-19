// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use connect_api::data::{Date, Schema, SchemaBuilder};
use connect_api::errors::ConnectError;

#[test]
fn test_builder() {
    let plain = Date::schema();
    assert_eq!(Date::LOGICAL_NAME, plain.name().unwrap_or(""));
    assert_eq!(Some(1), plain.version());
}

#[test]
fn test_from_logical() {
    // Epoch date (1970-01-01)
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let result = Date::from_logical(&Date::schema(), &epoch);
    assert!(result.is_ok());
    assert_eq!(0, result.unwrap());

    // 10000 days after epoch
    let epoch_plus_10000 =
        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::Duration::days(10000);
    let result = Date::from_logical(&Date::schema(), &epoch_plus_10000);
    assert!(result.is_ok());
    assert_eq!(10000, result.unwrap());
}

#[test]
fn test_from_logical_invalid_schema() {
    let invalid_schema = SchemaBuilder::int64().name("invalid").build();
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let result = Date::from_logical(&invalid_schema, &epoch);
    assert!(result.is_err());
}

#[test]
fn test_to_logical() {
    // Convert 0 days to epoch date
    let result = Date::to_logical(&Date::schema(), 0);
    assert!(result.is_ok());
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    assert_eq!(epoch, result.unwrap());

    // Convert 10000 days
    let result = Date::to_logical(&Date::schema(), 10000);
    assert!(result.is_ok());
    let expected = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::Duration::days(10000);
    assert_eq!(expected, result.unwrap());
}

#[test]
fn test_to_logical_invalid_schema() {
    let invalid_schema = SchemaBuilder::int64().name("invalid").build();
    let result = Date::to_logical(&invalid_schema, 0);
    assert!(result.is_err());
}
