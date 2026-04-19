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

use connect_api::util::ConnectorUtils;
use std::collections::HashMap;

#[test]
fn test_task_configs_single_task() {
    let mut configs: HashMap<String, String> = HashMap::new();
    configs.insert("task.count", "1".to_string());
    configs.insert("key", "value".to_string());

    let task_configs = ConnectorUtils::task_configs(1, &configs);
    assert_eq!(1, task_configs.len());

    // Each task config should contain the original config
    for task_config in task_configs {
        assert_eq!(Some("value"), task_config.get("key"));
    }
}

#[test]
fn test_task_configs_multiple_tasks() {
    let mut configs: HashMap<String, String> = HashMap::new();
    configs.insert("task.count", "3".to_string());
    configs.insert("key", "value".to_string());

    let task_configs = ConnectorUtils::task_configs(3, &configs);
    assert_eq!(3, task_configs.len());

    // Each task config should contain the original config
    for task_config in task_configs {
        assert_eq!(Some("value"), task_config.get("key"));
    }
}

#[test]
fn test_task_configs_zero_tasks() {
    let configs: HashMap<String, String> = HashMap::new();

    let task_configs = ConnectorUtils::task_configs(0, &configs);
    assert_eq!(0, task_configs.len());
}
