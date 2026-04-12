/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::errors::ConnectException;
use crate::util::TopicAdmin;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, atomic::{AtomicBool, AtomicPtr}};
use std::ops::Function;

/// A holder of a TopicAdmin object that is lazily and atomically created when needed
pub struct SharedTopicAdmin {
    admin_props: HashMap<String, Box<dyn std::any::Any>>,
    admin: Arc<Mutex<Option<TopicAdmin>>>,
    closed: Arc<AtomicBool>,
    factory: Arc<dyn Fn(HashMap<String, Box<dyn std::any::Any>>) -> TopicAdmin + Send + Sync>>,
}

impl SharedTopicAdmin {
    /// Default close duration
    pub const DEFAULT_CLOSE_DURATION: Duration = Duration::from_millis(i64::MAX);

    /// Create a new SharedTopicAdmin
    pub fn new(admin_props: HashMap<String, Box<dyn std::any::Any>>) -> Self {
        Self::with_factory(admin_props, Arc::new(|props| TopicAdmin::new(props)))
    }

    /// Create a new SharedTopicAdmin with custom factory
    pub fn with_factory(
        admin_props: HashMap<String, Box<dyn std::any::Any>>,
        factory: Arc<dyn Fn(HashMap<String, Box<dyn std::any::Any>>) -> TopicAdmin + Send + Sync>>,
    ) -> Self {
        Self {
            admin_props,
            admin: Arc::new(Mutex::new(None)),
            closed: Arc::new(AtomicBool::new(false)),
            factory,
        }
    }

    /// Get the shared TopicAdmin instance
    pub fn topic_admin(&self) -> TopicAdmin {
        let mut admin = self.admin.lock().unwrap();
        if admin.is_none() {
            if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
                let msg = format!("The {} has already been closed and cannot be used.", self);
                panic!("{}", msg);
            }
            *admin = Some((self.factory)(self.admin_props.clone()));
        }
        admin.as_ref().unwrap().clone()
    }

    /// Get bootstrap servers
    pub fn bootstrap_servers(&self) -> String {
        self.admin_props
            .get("bootstrap.servers")
            .map(|v| v.to_string())
            .unwrap_or_else(|| "<unknown>".to_string())
    }

    /// Close the underlying TopicAdmin instance
    pub fn close(&self) {
        self.close_with_timeout(Self::DEFAULT_CLOSE_DURATION);
    }

    /// Close the underlying TopicAdmin instance with timeout
    pub fn close_with_timeout(&self, _timeout: Duration) {
        if self.closed.compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        ) {
            let mut admin = self.admin.lock().unwrap();
            if let Some(admin_instance) = admin.take() {
                // TODO: Close admin instance with timeout
            }
        }
    }
}

impl Drop for SharedTopicAdmin {
    fn drop(&mut self) {
        self.close();
    }
}

impl std::fmt::Display for SharedTopicAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "admin client for brokers at {}", self.bootstrap_servers())
    }
}
