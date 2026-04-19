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

//! Tests for SourceAndTarget module
//! Corresponds to Java: org.apache.kafka.connect.mirror.SourceAndTarget

use connect_mirror_client::SourceAndTarget;
use std::collections::HashSet;

#[test]
fn test_new() {
    let sat = SourceAndTarget::new("source", "target");
    assert_eq!(sat.source(), "source");
    assert_eq!(sat.target(), "target");
}

#[test]
fn test_display() {
    let sat = SourceAndTarget::new("source", "target");
    assert_eq!(format!("{}", sat), "source->target");
}

#[test]
fn test_equals() {
    let sat1 = SourceAndTarget::new("source", "target");
    let sat2 = SourceAndTarget::new("source", "target");
    let sat3 = SourceAndTarget::new("other", "target");
    assert_eq!(sat1, sat2);
    assert_ne!(sat1, sat3);
}

#[test]
fn test_hash() {
    let mut set = HashSet::new();
    let sat = SourceAndTarget::new("source", "target");
    set.insert(sat.clone());
    assert!(set.contains(&sat));
}
