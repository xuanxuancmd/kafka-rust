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

//! Tools module for Kafka Connect runtime.
//!
//! This module provides documentation generation tools for predicates and transformations.
//! It corresponds to `org.apache.kafka.connect.tools` in Java.
//!
//! ## Key Components
//!
//! - `PredicateDoc`: Generates HTML documentation for predicates
//! - `TransformationDoc`: Generates HTML documentation for transformations

pub mod predicate_doc;
pub mod transformation_doc;

pub use predicate_doc::*;
pub use transformation_doc::*;