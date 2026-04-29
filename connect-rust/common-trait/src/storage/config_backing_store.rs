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

//! ConfigBackingStore trait for storing connector configurations.

use std::collections::HashMap;
use std::time::Duration;

use super::types::{ClusterConfigState, ConnectorTaskId, RestartRequest, SessionKey, TargetState};

/// ConfigBackingStore is an interface to store and retrieve (via snapshot) configuration
/// information that is created during runtime (i.e., not static configuration like the worker config).
///
/// This configuration information includes connector configs, task configs, connector target states etc.
pub trait ConfigBackingStore: Send + Sync {
    /// Start the backing store.
    fn start(&mut self);

    /// Stop the backing store.
    fn stop(&mut self);

    /// Get a snapshot of the current configuration state including all connector and task configurations.
    ///
    /// # Returns
    /// The cluster config state
    fn snapshot(&self) -> ClusterConfigState;

    /// Check if the store has configuration for a connector.
    ///
    /// # Arguments
    /// * `connector` - Name of the connector
    ///
    /// # Returns
    /// true if the backing store contains configuration for the connector
    fn contains(&self, connector: &str) -> bool;

    /// Update the configuration for a connector.
    ///
    /// # Arguments
    /// * `connector` - Name of the connector
    /// * `properties` - The connector configuration
    /// * `target_state` - The desired target state for the connector; may be None if no target state change is desired.
    ///                    Note that the default target state is STARTED if no target state exists previously.
    fn put_connector_config(
        &mut self,
        connector: &str,
        properties: HashMap<String, String>,
        target_state: Option<TargetState>,
    );

    /// Remove configuration for a connector.
    ///
    /// # Arguments
    /// * `connector` - Name of the connector
    fn remove_connector_config(&mut self, connector: &str);

    /// Update the task configurations for a connector.
    ///
    /// # Arguments
    /// * `connector` - Name of the connector
    /// * `configs` - The new task configs for the connector
    fn put_task_configs(&mut self, connector: &str, configs: Vec<HashMap<String, String>>);

    /// Remove the task configs associated with a connector.
    ///
    /// # Arguments
    /// * `connector` - Name of the connector
    fn remove_task_configs(&mut self, connector: &str);

    /// Refresh the backing store.
    /// This forces the store to ensure that it has the latest configs that have been written.
    ///
    /// # Arguments
    /// * `timeout` - Max time to wait for the refresh to complete
    ///
    /// # Returns
    /// Ok(()) if refresh completed, Err if timeout expired
    fn refresh(&mut self, timeout: Duration) -> Result<(), std::io::Error>;

    /// Transition a connector to a new target state (e.g., paused).
    ///
    /// # Arguments
    /// * `connector` - Name of the connector
    /// * `state` - The state to transition to
    fn put_target_state(&mut self, connector: &str, state: TargetState);

    /// Store a new SessionKey that can be used to validate internal inter-worker communication.
    ///
    /// # Arguments
    /// * `session_key` - The session key to store
    fn put_session_key(&mut self, session_key: SessionKey);

    /// Request a restart of a connector and optionally its tasks.
    ///
    /// # Arguments
    /// * `restart_request` - The restart request details
    fn put_restart_request(&mut self, restart_request: RestartRequest);

    /// Record the number of tasks for the connector after a successful round of zombie fencing.
    ///
    /// # Arguments
    /// * `connector` - Name of the connector
    /// * `task_count` - Number of tasks used by the connector
    fn put_task_count_record(&mut self, connector: &str, task_count: u32);

    /// Prepare to write to the backing config store.
    /// May be required by some implementations (such as those that only permit a single writer
    /// at a time across a cluster of workers) before performing mutating operations like
    /// writing configurations, target states, etc.
    ///
    /// The default implementation is a no-op; it is the responsibility of the implementing
    /// class to override this and document any expectations for when it must be invoked.
    fn claim_write_privileges(&mut self) {
        // Default: no-op
    }

    /// Emit a new level for the specified logging namespace (and all of its children).
    /// This level should be applied by all workers currently in the cluster, but not to
    /// workers that join after it is stored.
    ///
    /// # Arguments
    /// * `namespace` - The namespace to adjust; may not be null
    /// * `level` - The new level for the namespace; may not be null
    fn put_logger_level(&mut self, namespace: &str, level: &str);

    /// Set an update listener to get notifications when there are new records written to the backing store.
    ///
    /// # Arguments
    /// * `listener` - Non-null listener
    fn set_update_listener(&mut self, listener: Box<dyn ConfigBackingStoreUpdateListener>);
}

/// Listener for configuration backing store updates.
pub trait ConfigBackingStoreUpdateListener: Send + Sync {
    /// Invoked when a connector configuration has been removed.
    ///
    /// # Arguments
    /// * `connector` - Name of the connector
    fn on_connector_config_remove(&self, connector: &str);

    /// Invoked when a connector configuration has been updated.
    ///
    /// # Arguments
    /// * `connector` - Name of the connector
    fn on_connector_config_update(&self, connector: &str);

    /// Invoked when task configs are updated.
    ///
    /// # Arguments
    /// * `tasks` - All the tasks whose configs have been updated
    fn on_task_config_update(&self, tasks: &[ConnectorTaskId]);

    /// Invoked when the user has set a new target state (e.g., paused).
    ///
    /// # Arguments
    /// * `connector` - Name of the connector
    fn on_connector_target_state_change(&self, connector: &str);

    /// Invoked when the leader has distributed a new session key.
    ///
    /// # Arguments
    /// * `session_key` - The session key
    fn on_session_key_update(&self, session_key: &SessionKey);

    /// Invoked when a connector and possibly its tasks have been requested to be restarted.
    ///
    /// # Arguments
    /// * `restart_request` - The restart request
    fn on_restart_request(&self, restart_request: &RestartRequest);

    /// Invoked when a dynamic log level adjustment has been read.
    ///
    /// # Arguments
    /// * `namespace` - The namespace to adjust; never null
    /// * `level` - The level to set the namespace to; never null
    fn on_logging_level_update(&self, namespace: &str, level: &str);
}
