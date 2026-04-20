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

//! A holder of a TopicAdmin object that is lazily and atomically created.
//!
//! This corresponds to `org.apache.kafka.connect.util.SharedTopicAdmin` in Java.

use super::topic_admin::{TopicAdmin, TopicAdminError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Default close duration (essentially unlimited).
const DEFAULT_CLOSE_DURATION_MS: i64 = i64::MAX;

/// A holder of a TopicAdmin object that is lazily and atomically created.
///
/// This corresponds to `org.apache.kafka.connect.util.SharedTopicAdmin` in Java.
/// As soon as one of the getters is called, all getters will return the same
/// shared TopicAdmin instance until this SharedTopicAdmin is closed.
pub struct SharedTopicAdmin {
    admin_props: HashMap<String, String>,
    admin: Arc<std::sync::Mutex<Option<Arc<TopicAdmin>>>>,
    closed: AtomicBool,
    factory: Arc<dyn Fn(HashMap<String, String>) -> TopicAdmin + Send + Sync>,
}

impl SharedTopicAdmin {
    /// Creates a new SharedTopicAdmin with the given admin properties.
    pub fn new(admin_props: HashMap<String, String>) -> Self {
        Self::with_factory(admin_props, Arc::new(|props| TopicAdmin::new(props)))
    }

    /// Creates a new SharedTopicAdmin with a custom factory function.
    pub fn with_factory(
        admin_props: HashMap<String, String>,
        factory: Arc<dyn Fn(HashMap<String, String>) -> TopicAdmin + Send + Sync>,
    ) -> Self {
        SharedTopicAdmin {
            admin_props,
            admin: Arc::new(std::sync::Mutex::new(None)),
            closed: AtomicBool::new(false),
            factory,
        }
    }

    /// Get the shared TopicAdmin instance.
    ///
    /// # Errors
    /// Returns an error if this object has already been closed.
    pub fn get(&self) -> Result<Arc<TopicAdmin>, TopicAdminError> {
        self.topic_admin()
    }

    /// Get the shared TopicAdmin instance.
    ///
    /// # Errors
    /// Returns an error if this object has already been closed.
    pub fn topic_admin(&self) -> Result<Arc<TopicAdmin>, TopicAdminError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TopicAdminError::Other(format!(
                "The {} has already been closed and cannot be used.",
                self
            )));
        }

        let mut admin_guard = self.admin.lock().unwrap();
        if admin_guard.is_none() {
            let new_admin = Arc::new(self.factory.clone()(self.admin_props.clone()));
            *admin_guard = Some(new_admin);
        }

        Ok(admin_guard.clone().unwrap())
    }

    /// Get the bootstrap servers string.
    pub fn bootstrap_servers(&self) -> String {
        self.admin_props
            .get("bootstrap.servers")
            .cloned()
            .unwrap_or_else(|| "<unknown>".to_string())
    }

    /// Close the underlying TopicAdmin instance.
    ///
    /// Once this method is called, the get() and topic_admin() methods
    /// may not be used.
    pub fn close(&self) {
        self.close_with_timeout(Duration::from_millis(DEFAULT_CLOSE_DURATION_MS as u64))
    }

    /// Close the underlying TopicAdmin instance with a timeout.
    pub fn close_with_timeout(&self, timeout: Duration) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let mut admin_guard = self.admin.lock().unwrap();
            if let Some(admin) = admin_guard.take() {
                // In Rust, drop handles the cleanup
                log::debug!(
                    "Closing SharedTopicAdmin admin for brokers at {} with timeout {}ms",
                    self.bootstrap_servers(),
                    timeout.as_millis()
                );
            }
        }
    }

    /// Check if this SharedTopicAdmin has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for SharedTopicAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "admin client for brokers at {}",
            self.bootstrap_servers()
        )
    }
}

impl Drop for SharedTopicAdmin {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_topic_admin_creation() {
        let props = HashMap::from([(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        )]);
        let shared_admin = SharedTopicAdmin::new(props);

        assert_eq!(shared_admin.bootstrap_servers(), "localhost:9092");
        assert!(!shared_admin.is_closed());
    }

    #[test]
    fn test_shared_topic_admin_close() {
        let props = HashMap::from([(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        )]);
        let shared_admin = SharedTopicAdmin::new(props);

        shared_admin.close();
        assert!(shared_admin.is_closed());

        // Should fail to get after close
        let result = shared_admin.get();
        assert!(result.is_err());
    }

    #[test]
    fn test_shared_topic_admin_singleton() {
        let props = HashMap::from([(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        )]);
        let shared_admin = SharedTopicAdmin::new(props);

        // Get admin twice - should return the same instance
        let admin1 = shared_admin.topic_admin();
        let admin2 = shared_admin.topic_admin();

        // Both should succeed and point to same bootstrap servers
        assert!(admin1.is_ok());
        assert!(admin2.is_ok());
        assert_eq!(
            admin1.unwrap().bootstrap_servers(),
            admin2.unwrap().bootstrap_servers()
        );
    }
}
