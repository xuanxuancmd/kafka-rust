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

use bigdecimal::BigDecimal;
use connect_api::data::{Decimal, Schema, SchemaBuilder};
use std::collections::HashMap;

const TEST_SCALE: i32 = 2;

#[test]
fn test_builder() {
    let plain = Decimal::builder(TEST_SCALE).build();
    assert_eq!(Decimal::LOGICAL_NAME, plain.name().unwrap_or(""));

    let params = plain.parameters();
    assert!(params.is_some());
    let params_map = params.unwrap();
    assert_eq!(
        Some("2"),
        params_map
            .get(&Decimal::SCALE_FIELD.to_string())
            .map(|s| s.as_str())
    );

    assert_eq!(Some(1), plain.version());
}

#[test]
fn test_from_logical() {
    let schema = Decimal::schema(TEST_SCALE);
    let test_decimal = BigDecimal::from(156) / BigDecimal::from(100);
    let encoded = Decimal::from_logical(&schema, &test_decimal);
    assert!(encoded.is_ok());

    let test_decimal_negative = BigDecimal::from(-156) / BigDecimal::from(100);
    let encoded_negative = Decimal::from_logical(&schema, &test_decimal_negative);
    assert!(encoded_negative.is_ok());
}

#[test]
fn test_to_logical() {
    let schema = Decimal::schema(2);
    let test_bytes: Vec<u8> = vec![0, 156]; // Represents 156 with scale 2
    let converted = Decimal::to_logical(&schema, &test_bytes);
    assert!(converted.is_ok());

    let test_bytes_negative: Vec<u8> = vec![255, 100]; // Represents -156 with scale 2
    let converted_negative = Decimal::to_logical(&schema, &test_bytes_negative);
    assert!(converted_negative.is_ok());
}
