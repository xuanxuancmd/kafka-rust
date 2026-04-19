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

//! StatusBackingStore trait for storing connector and task statuses.

use std::collections::HashSet;

use super::types::{ConnectorStatus, ConnectorTaskId, TaskStatus, TopicStatus, WorkerConfig};

/// StatusBackingStore is an interface to store and retrieve connector / task statuses.
pub trait StatusBackingStore: Send + Sync {
    /// Configure class with the given key-value pairs.
    ///
    /// # Arguments
    /// * `config` - Config for StatusBackingStore
    fn configure(&mut self, config: &WorkerConfig);

    /// Start dependent services (if needed).
    fn start(&mut self);

    /// Stop dependent services (if needed).
    fn stop(&mut self);

    /// Set the state of the connector to the given value.
    ///
    /// # Arguments
    /// * `status` - The status of the connector
    fn put_connector_status(&mut self, status: ConnectorStatus);

    /// Safely set the state of the connector to the given value.
    /// What is considered "safe" depends on the implementation, but basically it
    /// means that the store can provide higher assurance that another worker
    /// hasn't concurrently written any conflicting data.
    ///
    /// # Arguments
    /// * `status` - The status of the connector
    fn put_connector_status_safe(&mut self, status: ConnectorStatus);

    /// Set the state of the task to the given value.
    ///
    /// # Arguments
    /// * `status` - The status of the task
    fn put_task_status(&mut self, status: TaskStatus);

    /// Safely set the state of the task to the given value.
    /// What is considered "safe" depends on the implementation, but basically it
    /// means that the store can provide higher assurance that another worker
    /// hasn't concurrently written any conflicting data.
    ///
    /// # Arguments
    /// * `status` - The status of the task
    fn put_task_status_safe(&mut self, status: TaskStatus);

    /// Set the state of a connector's topic to the given value.
    ///
    /// # Arguments
    /// * `status` - The status of the topic used by a connector
    fn put_topic_status(&mut self, status: TopicStatus);

    /// Get the current state of the task.
    ///
    /// # Arguments
    /// * `id` - The id of the task
    ///
    /// # Returns
    /// The state or None if there is none
    fn get_task_status(&self, id: &ConnectorTaskId) -> Option<TaskStatus>;

    /// Get the current state of the connector.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    ///
    /// # Returns
    /// The state or None if there is none
    fn get_connector_status(&self, connector: &str) -> Option<ConnectorStatus>;

    /// Get the states of all tasks for the given connector.
    ///
    /// # Arguments
    /// * `connector` - The connector name
    ///
    /// # Returns
    /// A collection of task statuses
    fn get_all_task_statuses(&self, connector: &str) -> Vec<TaskStatus>;

    /// Get the status of a connector's topic if the connector is actively using this topic.
    ///
    /// # Arguments
    /// * `connector` - The connector name; never null
    /// * `topic` - The topic name; never null
    ///
    /// # Returns
    /// The state or None if there is none
    fn get_topic_status(&self, connector: &str, topic: &str) -> Option<TopicStatus>;

    /// Get the states of all topics that a connector is using.
    ///
    /// # Arguments
    /// * `connector` - The connector name; never null
    ///
    /// # Returns
    /// A collection of topic statuses or an empty collection if there is none
    fn get_all_topic_statuses(&self, connector: &str) -> Vec<TopicStatus>;

    /// Delete this topic from the connector's set of active topics.
    ///
    /// # Arguments
    /// * `connector` - The connector name; never null
    /// * `topic` - The topic name; never null
    fn delete_topic(&mut self, connector: &str, topic: &str);

    /// Get all cached connectors.
    ///
    /// # Returns
    /// The set of connector names
    fn connectors(&self) -> HashSet<String>;

    /// Flush any pending writes.
    fn flush(&mut self);
}
