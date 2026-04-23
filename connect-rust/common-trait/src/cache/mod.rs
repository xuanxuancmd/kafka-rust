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

//! Cache module providing a generic cache interface.
//!
//! This module defines a semi-persistent map interface for storing key-value
//! mappings until either an eviction criteria is met or the entries are
//! manually invalidated.
//!
//! Available implementations:
//! - `LruCache`: A simple Least Recently Used cache
//! - `SynchronizedCache`: A thread-safe wrapper around a cache

mod cache;
mod lru_cache;
mod synchronized_cache;

pub use cache::Cache;
pub use lru_cache::{LruCache, DEFAULT_CAPACITY};
pub use synchronized_cache::SynchronizedCache;
