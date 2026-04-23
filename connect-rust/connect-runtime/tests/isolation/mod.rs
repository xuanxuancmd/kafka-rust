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

//! Isolation tests for Kafka Connect runtime.
//!
//! ## Java Test Correspondence
//!
//! | Java Test Class | Rust Test Module | Description |
//! |-----------------|------------------|-------------|
//! | `PluginScannerTest` | `scanner_test` | Plugin scanning tests |
//! | `TestPluginsTest` | `test_plugins` | Test plugin loading tests |
//! | `PluginDiscoveryTest` | (future) | Plugin discovery tests |
//! | `PluginClassLoaderTest` | (future) | Class loader isolation tests |
//!
//! ## Naming Convention
//!
//! - Java PascalCase class names → Rust snake_case module names
//! - Example: `PluginScannerTest` → `scanner_test.rs`
//!
//! ## Evidence Output Convention
//!
//! Test results and artifacts should be written to:
//! - `target/test-evidence/isolation/<test_name>/`
//!
//! This mirrors Java's test output conventions.

mod scanner_test;
mod test_plugins;
