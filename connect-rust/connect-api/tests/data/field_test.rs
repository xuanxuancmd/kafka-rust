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

use connect_api::data::{Field, Schema, SchemaBuilder};

#[test]
fn test_equality() {
    let field1 = Field::new("name", 0, SchemaBuilder::int8().build());
    let field2 = Field::new("name", 0, SchemaBuilder::int8().build());
    let different_name = Field::new("name2", 0, SchemaBuilder::int8().build());
    let different_index = Field::new("name", 1, SchemaBuilder::int8().build());
    let different_schema = Field::new("name", 0, SchemaBuilder::int16().build());

    assert_eq!(field1, field2);
    assert_ne!(field1, different_name);
    assert_ne!(field1, different_index);
    assert_ne!(field1, different_schema);
}
