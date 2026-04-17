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
use crate::util::Callback;
use crate::util::TopicAdmin;
use common_trait::consumer::Consumer;
use common_trait::producer::Producer;
use common_trait::time::Time;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

/// KafkaBasedLog provides a generic implementation of a shared, a compacted log of records stored in Kafka
pub struct KafkaBasedLog<K, V> {
    topic: String,
    partition_count: i32,
    producer_configs: HashMap<String, Box<dyn std::any::Any>>,
    consumer_configs: HashMap<String, Box<dyn std::any::Any>>,
    consumed_callback: Box<dyn Callback<(K, V)>>,
    topic_admin_supplier: Arc<Mutex<Option<TopicAdmin>>>,
    require_admin_for_offsets: bool,
    consumer: Arc<Mutex<Option<Box<dyn Consumer<K, V>>>>,
    producer: Arc<Mutex<Option<Box<dyn Producer<K, V>>>>,
    admin: Arc<Mutex<Option<TopicAdmin>>>,
    stop_requested: Arc<Mutex<bool>>,
    read_log_end_offset_callbacks: Arc<Mutex<Vec<Box<dyn Callback<()>>>>,
    report_errors_to_callback: Arc<Mutex<bool>>,
    time: Arc<dyn Time>,
}

impl<K, V> KafkaBasedLog<K, V> {
    /// Create a new KafkaBasedLog object
    pub fn new(
        topic: String,
        producer_configs: HashMap<String, Box<dyn std::any::Any>>,
        consumer_configs: HashMap<String, Box<dyn std::any::Any>>,
        topic_admin_supplier: Arc<Mutex<Option<TopicAdmin>>>,
        consumed_callback: Box<dyn Callback<(K, V)>>,
        time: Arc<dyn Time>,
    ) -> Self {
        // Check if isolation.level = read_committed
        let require_admin_for_offsets = consumer_configs
            .get("isolation.level")
            .map(|v| v.to_string().eq_ignore_ascii_case("read_committed"))
            .unwrap_or(false);

        Self {
            topic,
            partition_count: 0,
            producer_configs,
            consumer_configs,
            consumed_callback,
            topic_admin_supplier,
            require_admin_for_offsets,
            consumer: Arc::new(Mutex::new(None)),
            producer: Arc::new(Mutex::new(None)),
            admin: Arc::new(Mutex::new(None)),
            stop_requested: Arc::new(Mutex::new(false)),
            read_log_end_offset_callbacks: Arc::new(Mutex::new(Vec::new())),
            report_errors_to_callback: Arc::new(Mutex::new(false)),
            time,
        }
    }

    /// Start the log
    pub async fn start(&self) -> Result<(), ConnectException> {
        // TODO: Implement start logic
        Ok(())
    }

    /// Stop the log
    pub async fn stop(&self) {
        let mut stop_requested = self.stop_requested.lock().await;
        *stop_requested = true;
    }

    /// Read to end
    pub async fn read_to_end(&self) -> Result<(), ConnectException> {
        // TODO: Implement read_to_end logic
        Ok(())
    }

    /// Send a record
    pub async fn send(&self, key: K, value: V) -> Result<(), ConnectException> {
        // TODO: Implement send logic
        Ok(())
    }
}