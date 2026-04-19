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

use chrono::NaiveTime;
use connect_api::data::{Schema, SchemaBuilder, Time};
use connect_api::errors::ConnectError;

#[test]
fn test_builder() {
    let plain = Time::schema();
    assert_eq!(Time::LOGICAL_NAME, plain.name().unwrap_or(""));
    assert_eq!(Some(1), plain.version());
}

#[test]
fn test_from_logical() {
    // Epoch time (00:00:00)
    let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    let result = Time::from_logical(&Time::schema(), &epoch_time);
    assert!(result.is_ok());
    assert_eq!(0, result.unwrap());

    // 10000 milliseconds after epoch
    let time_plus_10000 = NaiveTime::from_hms_milli_opt(0, 0, 10, 0).unwrap();
    let result = Time::from_logical(&Time::schema(), &time_plus_10000);
    assert!(result.is_ok());
    assert_eq!(10000, result.unwrap());
}

#[test]
fn test_from_logical_invalid_schema() {
    let invalid_schema = SchemaBuilder::int32().name("invalid").build();
    let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    let result = Time::from_logical(&invalid_schema, &epoch_time);
    assert!(result.is_err());
}

#[test]
fn test_to_logical() {
    // Convert 0 milliseconds to epoch time
    let result = Time::to_logical(&Time::schema(), 0);
    assert!(result.is_ok());
    let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    assert_eq!(epoch_time, result.unwrap());

    // Convert 10000 milliseconds
    let result = Time::to_logical(&Time::schema(), 10000);
    assert!(result.is_ok());
    let expected = NaiveTime::from_hms_milli_opt(0, 0, 10, 0).unwrap();
    assert_eq!(expected, result.unwrap());
}

#[test]
fn test_to_logical_invalid_schema() {
    let invalid_schema = SchemaBuilder::int32().name("invalid").build();
    let result = Time::to_logical(&invalid_schema, 0);
    assert!(result.is_err());
}
