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
use crate::runtime::distributed::DistributedConfig;
use crate::runtime::WorkerConfig;
use common_trait::common_client_configs::CommonClientConfigs;
use common_trait::record_batch::RecordBatch;
use std::collections::HashMap;
use std::ops::Function;

/// Utility methods for Connect
pub struct ConnectUtils;

impl ConnectUtils {
    /// Check and convert timestamp
    pub fn check_and_convert_timestamp(timestamp: Option<i64>) -> Option<i64> {
        match timestamp {
            None | Some(0..) => timestamp,
            Some(t) if t == RecordBatch::NO_TIMESTAMP => None,
            Some(t) => {
                let msg = format!("Invalid record timestamp {}", t);
                let error = crate::common::InvalidRecordException::new(msg);
                Err(error).unwrap()
            }
        }
    }

    /// Ensure that the properties contain an expected value for the given key
    pub fn ensure_property(
        props: &mut HashMap<String, String>,
        key: &str,
        expected_value: &str,
        justification: Option<&str>,
        case_sensitive: bool,
    ) {
        if let Some(warning) = Self::ensure_property_and_get_warning(
            props,
            key,
            expected_value,
            justification,
            case_sensitive,
        ) {
            log::warn!("{}", warning);
        }
    }

    /// Ensure that a given key has an expected value in the properties
    pub fn ensure_property_and_get_warning(
        props: &mut HashMap<String, String>,
        key: &str,
        expected_value: &str,
        justification: Option<&str>,
        case_sensitive: bool,
    ) -> Option<String> {
        if !props.contains_key(key) {
            // Insert the expected value
            props.insert(key.to_string(), expected_value.to_string());
            // But don't issue a warning to the user
            return None;
        }

        let value = props.get(key).unwrap().clone();
        let matches_expected_value = if case_sensitive {
            expected_value == &value
        } else {
            expected_value.eq_ignore_ascii_case(&value)
        };

        if matches_expected_value {
            return None;
        }

        // Insert the expected value
        props.insert(key.to_string(), expected_value.to_string());

        let justification = justification.map(|j| format!(" {}", j)).unwrap_or_default();
        let warning = format!(
            "The value '{}' for the '{}' property will be ignored as it cannot be overridden{}. The value '{}' will be used instead.",
            value, key, justification, expected_value
        );
        Some(warning)
    }

    /// Add Connect metrics context properties
    pub fn add_metrics_context_properties(
        prop: &mut HashMap<String, Box<dyn std::any::Any>>,
        config: &dyn WorkerConfig,
        cluster_id: &str,
    ) {
        // Add all properties predefined with "metrics.context."
        for (k, v) in
            config.originals_with_prefix(CommonClientConfigs::METRICS_CONTEXT_PREFIX, false)
        {
            prop.insert(k, v);
        }
        // Add connect properties
        prop.insert(
            format!(
                "{}{}",
                CommonClientConfigs::METRICS_CONTEXT_PREFIX,
                WorkerConfig::CONNECT_KAFKA_CLUSTER_ID
            ),
            Box::new(cluster_id.to_string()),
        );

        if let Some(group_id) = config.originals().get(DistributedConfig::GROUP_ID_CONFIG) {
            prop.insert(
                format!(
                    "{}{}",
                    CommonClientConfigs::METRICS_CONTEXT_PREFIX,
                    WorkerConfig::CONNECT_GROUP_ID
                ),
                group_id.clone(),
            );
        }
    }

    /// Check if connector is a sink connector
    pub fn is_sink_connector(connector: &dyn Connector) -> bool {
        connector.as_sink_connector().is_some()
    }

    /// Check if connector is a source connector
    pub fn is_source_connector(connector: &dyn Connector) -> bool {
        connector.as_source_connector().is_some()
    }

    /// Transform values in a map
    pub fn transform_values<K, I, O, F>(map: &HashMap<K, I>, transformation: F) -> HashMap<K, O>
    where
        K: Clone + Eq + std::hash::Hash,
        I: Clone,
        O: Clone,
        F: Fn(&I) -> O,
    {
        map.iter()
            .map(|(k, v)| (k.clone(), transformation(v)))
            .collect()
    }

    /// Combine collections
    pub fn combine_collections<I>(collections: Vec<Vec<I>>) -> Vec<I> {
        Self::combine_collections_with_extractor(collections, |c| c)
    }

    /// Combine collections with extractor
    pub fn combine_collections_with_extractor<I, T, F>(
        collection: Vec<I>,
        extract_collection: F,
    ) -> Vec<T>
    where
        F: Fn(&I) -> Vec<T>,
    {
        collection
            .iter()
            .flat_map(|item| extract_collection(item).into_iter())
            .collect()
    }

    /// Maybe wrap throwable as ConnectException
    pub fn maybe_wrap(
        t: Option<Box<dyn std::error::Error + Send>>,
        message: &str,
    ) -> Option<ConnectException> {
        match t {
            None => None,
            Some(err) => {
                if let Some(connect_err) = err.downcast_ref::<ConnectException>() {
                    Some(connect_err.clone())
                } else {
                    Some(ConnectException::new(message.to_string(), Some(err)))
                }
            }
        }
    }

    /// Create the base of a client ID
    pub fn client_id_base(config: &dyn WorkerConfig) -> String {
        let result = config.group_id().unwrap_or_else(|| "connect".to_string());
        let user_specified_client_id = config.get_string(CommonClientConfigs::CLIENT_ID_CONFIG);
        if let Some(user_specified_client_id) = user_specified_client_id {
            if !user_specified_client_id.trim().is_empty() {
                result = format!("{}-{}", result, user_specified_client_id);
            }
        }
        format!("{}-", result)
    }

    /// Get the class name for an object
    pub fn class_name(o: Option<&dyn std::any::Any>) -> String {
        match o {
            None => "null".to_string(),
            Some(obj) => format!("{:?}", std::any::type_name_of_val(obj)),
        }
    }

    /// Apply a patch on a connector config
    pub fn patch_config(
        config: &HashMap<String, String>,
        patch: &HashMap<String, Option<String>>,
    ) -> HashMap<String, String> {
        let mut result = config.clone();
        for (k, v) in patch {
            match v {
                Some(value) => {
                    result.insert(k.clone(), value.clone());
                }
                None => {
                    result.remove(k);
                }
            }
        }
        result
    }
}
