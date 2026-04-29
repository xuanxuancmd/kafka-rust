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

use chrono::{DateTime, TimeZone, Utc};
use connect_api::data::{Schema, SchemaBuilder, Timestamp};
use connect_api::errors::ConnectError;

const NUM_MILLIS: i64 = 2000000000;
const TOTAL_MILLIS: i64 = NUM_MILLIS * 2;

#[test]
fn test_builder() {
    let plain = Timestamp::schema();
    assert_eq!(Timestamp::LOGICAL_NAME, plain.name().unwrap_or(""));
    assert_eq!(Some(1), plain.version());
}

#[test]
fn test_from_logical() {
    // Epoch timestamp (1970-01-01 00:00:00 UTC)
    let epoch = Utc.timestamp_millis_opt(0).single().unwrap();
    let result = Timestamp::from_logical(&Timestamp::schema(), &epoch);
    assert!(result.is_ok());
    assert_eq!(0, result.unwrap());

    // TOTAL_MILLIS after epoch
    let epoch_plus_millis = Utc.timestamp_millis_opt(TOTAL_MILLIS).single().unwrap();
    let result = Timestamp::from_logical(&Timestamp::schema(), &epoch_plus_millis);
    assert!(result.is_ok());
    assert_eq!(TOTAL_MILLIS, result.unwrap());
}

#[test]
fn test_from_logical_invalid_schema() {
    let invalid_schema = SchemaBuilder::int64().name("invalid").build();
    let epoch = Utc.timestamp_millis_opt(0).single().unwrap();
    let result = Timestamp::from_logical(&invalid_schema, &epoch);
    assert!(result.is_err());
}

#[test]
fn test_to_logical() {
    // Convert 0 milliseconds to epoch timestamp
    let result = Timestamp::to_logical(&Timestamp::schema(), 0);
    assert!(result.is_ok());
    let epoch = Utc.timestamp_millis_opt(0).single().unwrap();
    assert_eq!(epoch, result.unwrap());

    // Convert TOTAL_MILLIS milliseconds
    let result = Timestamp::to_logical(&Timestamp::schema(), TOTAL_MILLIS);
    assert!(result.is_ok());
    let expected = Utc.timestamp_millis_opt(TOTAL_MILLIS).single().unwrap();
    assert_eq!(expected, result.unwrap());
}

#[test]
fn test_to_logical_invalid_schema() {
    let invalid_schema = SchemaBuilder::int64().name("invalid").build();
    let result = Timestamp::to_logical(&invalid_schema, 0);
    assert!(result.is_err());
}
