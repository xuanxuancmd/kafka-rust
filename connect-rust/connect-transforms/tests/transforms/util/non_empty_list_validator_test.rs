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

use connect_transforms::transforms::util::NonEmptyListValidator;
use serde_json::json;

#[test]
fn test_valid_list() {
    let validator = NonEmptyListValidator::new();
    let result = validator.validate("foo", &json!(["item"]));
    assert!(result.is_ok());
}

#[test]
fn test_empty_list_fails() {
    let validator = NonEmptyListValidator::new();
    let result = validator.validate("foo", &json!([]));
    assert!(result.is_err());
}

#[test]
fn test_null_list_fails() {
    let validator = NonEmptyListValidator::new();
    let result = validator.validate("foo", &json!(null));
    assert!(result.is_err());
}
