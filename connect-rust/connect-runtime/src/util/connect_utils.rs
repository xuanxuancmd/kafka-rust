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

//! Connect utility functions.
//!
//! This corresponds to `org.apache.kafka.connect.util.ConnectUtils` in Java.

use std::collections::HashMap;

/// Connect utility functions.
///
/// This corresponds to `org.apache.kafka.connect.util.ConnectUtils` in Java.
pub struct ConnectUtils;

impl ConnectUtils {
    /// Checks and converts a timestamp value.
    ///
    /// # Arguments
    /// * `timestamp` - The timestamp value to check
    ///
    /// # Returns
    /// * Some(timestamp) if valid and non-negative
    /// * None if timestamp equals NO_TIMESTAMP (-1)
    /// * Throws error for invalid negative timestamps
    pub fn check_and_convert_timestamp(timestamp: Option<i64>) -> Result<Option<i64>, String> {
        match timestamp {
            None => Ok(None),
            Some(ts) if ts >= 0 => Ok(Some(ts)),
            Some(-1) => Ok(None), // NO_TIMESTAMP
            Some(ts) => Err(format!("Invalid record timestamp {}", ts)),
        }
    }

    /// Ensures that a property has the expected value in the given map.
    ///
    /// If the property doesn't exist, it will be added with the expected value.
    /// If the property exists with a different value, a warning will be logged
    /// and the value will be overridden.
    ///
    /// # Arguments
    /// * `props` - The properties map to modify
    /// * `key` - The property key to check
    /// * `expected_value` - The expected value for the property
    /// * `justification` - Optional reason why this property cannot be overridden
    /// * `case_sensitive` - Whether the value comparison should be case-sensitive
    ///
    /// # Returns
    /// An optional warning message if a user-supplied value was overridden
    pub fn ensure_property_and_get_warning(
        props: &mut HashMap<String, String>,
        key: &str,
        expected_value: &str,
        justification: Option<&str>,
        case_sensitive: bool,
    ) -> Option<String> {
        if !props.contains_key(key) {
            props.insert(key.to_string(), expected_value.to_string());
            return None;
        }

        let current_value = props.get(key).unwrap().clone();
        let matches = if case_sensitive {
            current_value == expected_value
        } else {
            current_value.eq_ignore_ascii_case(expected_value)
        };

        if matches {
            return None;
        }

        // Override the value
        props.insert(key.to_string(), expected_value.to_string());

        // Generate warning
        let justification_text = justification.map(|j| format!(" {}", j)).unwrap_or_default();
        Some(format!(
            "The value '{}' for the '{}' property will be ignored as it cannot be overridden{}. The value '{}' will be used instead.",
            current_value, key, justification_text, expected_value
        ))
    }

    /// Ensures that a property has the expected value in the given map.
    ///
    /// Similar to `ensure_property_and_get_warning` but doesn't return the warning.
    pub fn ensure_property(
        props: &mut HashMap<String, String>,
        key: &str,
        expected_value: &str,
        justification: Option<&str>,
        case_sensitive: bool,
    ) {
        let warning = Self::ensure_property_and_get_warning(
            props,
            key,
            expected_value,
            justification,
            case_sensitive,
        );
        if let Some(w) = warning {
            log::warn!("{}", w);
        }
    }

    /// Transforms all values in a map using the given function.
    ///
    /// # Arguments
    /// * `map` - The map to transform
    /// * `f` - The transformation function
    pub fn transform_values<K, I, O, F>(map: HashMap<K, I>, f: F) -> HashMap<K, O>
    where
        K: std::hash::Hash + Eq + Clone,
        F: Fn(I) -> O,
    {
        map.into_iter().map(|(k, v)| (k, f(v))).collect()
    }

    /// Combines multiple collections into a single list.
    pub fn combine_collections<I, T, F>(collections: Vec<I>, extract: F) -> Vec<T>
    where
        F: Fn(&I) -> Vec<T>,
    {
        collections.iter().flat_map(extract).collect()
    }

    /// Wraps an exception in a ConnectException if needed.
    ///
    /// # Arguments
    /// * `error` - The original error
    /// * `message` - The message for the ConnectException if wrapping is needed
    pub fn maybe_wrap<E: std::error::Error + 'static>(
        error: Option<E>,
        message: &str,
    ) -> Option<Box<dyn std::error::Error + Send + Sync>> {
        error.map(|e| {
            // Check if it's already a ConnectException-like error
            // In Rust we just wrap it in a generic error
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{}: {}", message, e),
            )) as Box<dyn std::error::Error + Send + Sync>
        })
    }

    /// Returns the class name for an object in a null-safe manner.
    pub fn class_name<T>(_obj: &T) -> &'static str {
        std::any::type_name::<T>()
    }

    /// Applies a patch on a connector config.
    ///
    /// In the output, values from the patch will override values from the config.
    /// Null values will cause the corresponding key to be removed.
    pub fn patch_config(
        config: HashMap<String, String>,
        patch: HashMap<String, Option<String>>,
    ) -> HashMap<String, String> {
        let mut result = config;
        for (k, v) in patch {
            if let Some(value) = v {
                result.insert(k, value);
            } else {
                result.remove(&k);
            }
        }
        result
    }

    /// Creates a client ID base for Kafka clients.
    pub fn client_id_base(group_id: Option<&str>, user_client_id: Option<&str>) -> String {
        let base = group_id.unwrap_or("connect");
        let result = if let Some(cid) = user_client_id {
            if !cid.trim().is_empty() {
                format!("{}-{}", base, cid.trim())
            } else {
                base.to_string()
            }
        } else {
            base.to_string()
        };
        format!("{}-", result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_and_convert_timestamp() {
        assert_eq!(ConnectUtils::check_and_convert_timestamp(None), Ok(None));
        assert_eq!(
            ConnectUtils::check_and_convert_timestamp(Some(1000)),
            Ok(Some(1000))
        );
        assert_eq!(
            ConnectUtils::check_and_convert_timestamp(Some(-1)),
            Ok(None)
        );
        assert!(ConnectUtils::check_and_convert_timestamp(Some(-2)).is_err());
    }

    #[test]
    fn test_ensure_property_missing() {
        let mut props = HashMap::new();
        let warning =
            ConnectUtils::ensure_property_and_get_warning(&mut props, "key", "value", None, true);
        assert!(warning.is_none());
        assert_eq!(props.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_ensure_property_matches() {
        let mut props = HashMap::new();
        props.insert("key".to_string(), "value".to_string());
        let warning =
            ConnectUtils::ensure_property_and_get_warning(&mut props, "key", "value", None, true);
        assert!(warning.is_none());
    }

    #[test]
    fn test_ensure_property_override() {
        let mut props = HashMap::new();
        props.insert("key".to_string(), "wrong".to_string());
        let warning = ConnectUtils::ensure_property_and_get_warning(
            &mut props,
            "key",
            "value",
            Some("it's critical"),
            true,
        );
        assert!(warning.is_some());
        assert_eq!(props.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_transform_values() {
        let map: HashMap<String, i32> = [("a".to_string(), 1), ("b".to_string(), 2)]
            .into_iter()
            .collect();
        let transformed = ConnectUtils::transform_values(map, |v| v * 2);
        assert_eq!(transformed.get("a"), Some(&2));
        assert_eq!(transformed.get("b"), Some(&4));
    }

    #[test]
    fn test_patch_config() {
        let config: HashMap<String, String> = [
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]
        .into_iter()
        .collect();

        let patch: HashMap<String, Option<String>> = [
            ("key1".to_string(), Some("new1".to_string())),
            ("key2".to_string(), None), // Remove this key
            ("key3".to_string(), Some("value3".to_string())),
        ]
        .into_iter()
        .collect();

        let result = ConnectUtils::patch_config(config, patch);
        assert_eq!(result.get("key1"), Some(&"new1".to_string()));
        assert!(!result.contains_key("key2"));
        assert_eq!(result.get("key3"), Some(&"value3".to_string()));
    }

    #[test]
    fn test_client_id_base() {
        assert_eq!(ConnectUtils::client_id_base(None, None), "connect-");
        assert_eq!(
            ConnectUtils::client_id_base(Some("group1"), None),
            "group1-"
        );
        assert_eq!(
            ConnectUtils::client_id_base(Some("group1"), Some("client1")),
            "group1-client1-"
        );
        assert_eq!(
            ConnectUtils::client_id_base(None, Some("  ")), // Empty client ID
            "connect-"
        );
    }
}
