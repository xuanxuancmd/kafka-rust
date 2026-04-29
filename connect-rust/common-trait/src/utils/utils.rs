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

/// Utility functions for Kafka clients.
///
/// This corresponds to `org.apache.kafka.common.utils.Utils` in Java.

/// Checks if a string is blank (empty or contains only whitespace).
pub fn is_blank(s: &str) -> bool {
    s.trim().is_empty()
}

/// Checks if a string is empty.
pub fn is_empty(s: &str) -> bool {
    s.is_empty()
}

/// Returns the host and port from a host:port string.
pub fn get_host_port(host_port: &str) -> Option<(String, u16)> {
    let parts: Vec<&str> = host_port.split(':').collect();
    if parts.len() == 2 {
        let host = parts[0].to_string();
        let port: u16 = parts[1].parse().ok()?;
        Some((host, port))
    } else {
        None
    }
}

/// Joins a list of strings with a separator.
pub fn join<T: AsRef<str>>(items: &[T], separator: &str) -> String {
    items
        .iter()
        .map(|s| s.as_ref())
        .collect::<Vec<&str>>()
        .join(separator)
}

use std::collections::HashMap;
use std::io::Write;

/// Quietly closes a resource, ignoring any errors.
///
/// In Rust, resources are typically auto-closed via the `Drop` trait.
/// This function flushes the resource before dropping it, and logs any errors.
///
/// Corresponds to `org.apache.kafka.common.utils.Utils.closeQuietly(AutoCloseable, String)`
/// in Kafka source: `clients/src/main/java/org/apache/kafka/common/utils/Utils.java`
pub fn close_quietly<W: Write>(resource: Option<W>, name: &str) {
    if let Some(mut r) = resource {
        if let Err(e) = r.flush() {
            println!("Failed to close {}: {}", name, e);
        }
        // Resource will be auto-closed via Drop when it goes out of scope
    }
}

/// Loads properties from a file into a HashMap.
///
/// Properties file format:
/// - Lines starting with `#` or `!` are comments and ignored
/// - Empty lines are ignored
/// - Key-value pairs are separated by `=`
/// - Leading/trailing whitespace on keys and values is trimmed
///
/// Corresponds to `org.apache.kafka.common.utils.Utils.loadProps(String)`
/// in Kafka source: `clients/src/main/java/org/apache/kafka/common/utils/Utils.java`
pub fn load_props(file_path: &str) -> Result<HashMap<String, String>, std::io::Error> {
    let content = std::fs::read_to_string(file_path)?;
    let mut props = HashMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            props.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    Ok(props)
}

/// Converts a HashMap of properties to a string map (identity function in Rust).
///
/// In Java, this converts `Properties` to `Map<String, String>`.
/// In Rust, HashMap<String, String> is already a string map, so this is an identity function.
///
/// Corresponds to `org.apache.kafka.common.utils.Utils.propsToStringMap(Properties)`
/// in Kafka source: `clients/src/main/java/org/apache/kafka/common/utils/Utils.java`
pub fn props_to_string_map(props: HashMap<String, String>) -> HashMap<String, String> {
    props
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_blank() {
        assert!(is_blank(""));
        assert!(is_blank("   "));
        assert!(is_blank("\t\n"));
        assert!(!is_blank("hello"));
    }

    #[test]
    fn test_is_empty() {
        assert!(is_empty(""));
        assert!(!is_empty("hello"));
    }

    #[test]
    fn test_get_host_port() {
        let result = get_host_port("localhost:9092");
        assert_eq!(result, Some(("localhost".to_string(), 9092)));

        let result = get_host_port("invalid");
        assert_eq!(result, None);
    }

    #[test]
    fn test_join() {
        let items = vec!["a", "b", "c"];
        assert_eq!(join(&items, ","), "a,b,c");
    }

    #[test]
    fn test_close_quietly() {
        // Test with None - should not panic
        close_quietly(None::<std::fs::File>, "test_file");

        // Test with Some - should flush and close
        let temp_path = std::env::temp_dir().join("close_quietly_test.txt");
        let mut file = std::fs::File::create(&temp_path).unwrap();
        file.write_all(b"test data").unwrap();
        close_quietly(Some(file), "temp_file");
        // Clean up
        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_load_props() {
        let temp_path = std::env::temp_dir().join("load_props_test.properties");
        std::fs::write(
            &temp_path,
            "key1=value1\nkey2=value2\n# comment\n! another comment\n",
        )
        .unwrap();
        let path = temp_path.to_str().unwrap();

        let props = load_props(path).unwrap();
        assert_eq!(props.get("key1"), Some(&"value1".to_string()));
        assert_eq!(props.get("key2"), Some(&"value2".to_string()));
        assert_eq!(props.len(), 2);
        // Clean up
        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_props_to_string_map() {
        let mut props = HashMap::new();
        props.insert("key1".to_string(), "value1".to_string());
        props.insert("key2".to_string(), "value2".to_string());

        let map = props_to_string_map(props.clone());
        assert_eq!(map, props);
    }
}
