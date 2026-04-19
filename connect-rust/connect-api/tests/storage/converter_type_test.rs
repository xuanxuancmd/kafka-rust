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

use connect_api::storage::ConverterType;

#[test]
fn test_converter_type_values() {
    assert_eq!(ConverterType::Key, ConverterType::Key);
    assert_eq!(ConverterType::Value, ConverterType::Value);
    assert_eq!(ConverterType::Header, ConverterType::Header);

    assert_ne!(ConverterType::Key, ConverterType::Value);
    assert_ne!(ConverterType::Key, ConverterType::Header);
    assert_ne!(ConverterType::Value, ConverterType::Header);
}

#[test]
fn test_converter_type_display() {
    assert_eq!("Key", ConverterType::Key.to_string());
    assert_eq!("Value", ConverterType::Value.to_string());
    assert_eq!("Header", ConverterType::Header.to_string());
}

#[test]
fn test_converter_type_all_variants() {
    let all_types = [
        ConverterType::Key,
        ConverterType::Value,
        ConverterType::Header,
    ];
    assert_eq!(3, all_types.len());
}
