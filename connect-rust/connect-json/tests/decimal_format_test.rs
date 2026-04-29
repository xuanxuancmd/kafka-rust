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

//! Tests for decimal_format module.

use connect_json::DecimalFormat;
use std::str::FromStr;

#[test]
fn test_from_str_base64() {
    assert_eq!(
        DecimalFormat::from_str("BASE64").unwrap(),
        DecimalFormat::Base64
    );
    assert_eq!(
        DecimalFormat::from_str("base64").unwrap(),
        DecimalFormat::Base64
    );
    assert_eq!(
        DecimalFormat::from_str("Base64").unwrap(),
        DecimalFormat::Base64
    );
}

#[test]
fn test_from_str_numeric() {
    assert_eq!(
        DecimalFormat::from_str("NUMERIC").unwrap(),
        DecimalFormat::Numeric
    );
    assert_eq!(
        DecimalFormat::from_str("numeric").unwrap(),
        DecimalFormat::Numeric
    );
    assert_eq!(
        DecimalFormat::from_str("Numeric").unwrap(),
        DecimalFormat::Numeric
    );
}

#[test]
fn test_from_str_invalid() {
    assert!(DecimalFormat::from_str("invalid").is_err());
    assert!(DecimalFormat::from_str("").is_err());
}

#[test]
fn test_display() {
    assert_eq!(format!("{}", DecimalFormat::Base64), "BASE64");
    assert_eq!(format!("{}", DecimalFormat::Numeric), "NUMERIC");
}
