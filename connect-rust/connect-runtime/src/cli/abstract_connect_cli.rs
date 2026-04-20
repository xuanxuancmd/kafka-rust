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

//! AbstractConnectCli - common initialization logic for Kafka Connect.
//!
//! Corresponds to `org.apache.kafka.connect.cli.AbstractConnectCli` in Java.

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use common_trait::herder::Herder;

/// ConnectCliOptions - options for running Kafka Connect CLI.
#[derive(Debug, Clone)]
pub struct ConnectCliOptions {
    /// Worker properties file path.
    pub worker_props_file: String,
    /// Extra arguments (connector config files for standalone mode).
    pub extra_args: Vec<String>,
}

impl ConnectCliOptions {
    /// Creates new options from command line arguments.
    pub fn from_args(args: &[String]) -> Result<Self, String> {
        if args.is_empty() || args.contains(&"--help".to_string()) {
            return Err("Usage: <worker.properties> [extra args]".to_string());
        }

        Ok(ConnectCliOptions {
            worker_props_file: args[0].clone(),
            extra_args: args[1..].to_vec(),
        })
    }
}

/// AbstractConnectCli - common initialization logic for Kafka Connect.
///
/// This abstract base class provides common functionality for CLI utilities
/// that run Kafka Connect in different modes (distributed, standalone).
///
/// Corresponds to `org.apache.kafka.connect.cli.AbstractConnectCli` in Java.
pub trait AbstractConnectCli: Sized {
    /// The type of Herder to use.
    type HerderType: Herder;

    /// Returns the usage string for this CLI.
    fn usage() -> String;

    /// Creates the herder for this mode.
    ///
    /// # Arguments
    ///
    /// * `worker_props` - Worker properties
    /// * `worker_id` - Worker identifier
    fn create_herder(worker_props: HashMap<String, String>, worker_id: String) -> Self::HerderType;

    /// Processes extra arguments beyond the worker properties file.
    fn process_extra_args(extra_args: &[String]) -> Result<(), String> {
        // Default: no extra arguments to process
        Ok(())
    }

    /// Runs the CLI with the given arguments.
    fn run(args: &[String]) -> Result<(), String> {
        let options = ConnectCliOptions::from_args(args)?;

        // Log initialization start
        let init_start = Instant::now();

        // Load worker properties
        let worker_props = load_properties_file(&options.worker_props_file)?;

        // Get worker ID from properties or default
        let worker_id = worker_props
            .get("worker.id")
            .cloned()
            .unwrap_or_else(|| "localhost:8083".to_string());

        // Create and start herder
        let _herder = Self::create_herder(worker_props, worker_id);

        // Process extra arguments
        Self::process_extra_args(&options.extra_args)?;

        // Log initialization complete
        let init_time = init_start.elapsed();
        // log::info!("Kafka Connect worker initialization took {}ms", init_time.as_millis());

        Ok(())
    }
}

/// Loads properties from a file.
///
/// # Arguments
///
/// * `path` - Path to the properties file
///
/// # Returns
///
/// A HashMap of properties.
fn load_properties_file(path: &str) -> Result<HashMap<String, String>, String> {
    if path.is_empty() {
        return Ok(HashMap::new());
    }

    let file_path = Path::new(path);
    if !file_path.exists() {
        return Err(format!("Worker properties file not found: {}", path));
    }

    // In a real implementation, we would parse the properties file
    // For now, return an empty map as placeholder
    // This would use a proper properties parser
    Ok(HashMap::new())
}

/// Helper function to parse Java-style properties file content.
pub fn parse_properties_content(content: &str) -> HashMap<String, String> {
    let mut props = HashMap::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            if !key.is_empty() {
                props.insert(key.to_string(), value.to_string());
            }
        }
    }

    props
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_options_from_args() {
        let args = vec!["worker.properties".to_string()];
        let options = ConnectCliOptions::from_args(&args).unwrap();
        assert_eq!(options.worker_props_file, "worker.properties");
        assert!(options.extra_args.is_empty());
    }

    #[test]
    fn test_cli_options_with_extra_args() {
        let args = vec![
            "worker.properties".to_string(),
            "connector1.properties".to_string(),
        ];
        let options = ConnectCliOptions::from_args(&args).unwrap();
        assert_eq!(options.worker_props_file, "worker.properties");
        assert_eq!(options.extra_args.len(), 1);
    }

    #[test]
    fn test_cli_options_empty_args() {
        let args: Vec<String> = vec![];
        let result = ConnectCliOptions::from_args(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_options_help() {
        let args = vec!["--help".to_string()];
        let result = ConnectCliOptions::from_args(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_properties_content() {
        let content = "key1=value1\nkey2=value2\n#comment\nkey3=value3";
        let props = parse_properties_content(content);
        assert_eq!(props.get("key1"), Some(&"value1".to_string()));
        assert_eq!(props.get("key2"), Some(&"value2".to_string()));
        assert_eq!(props.get("key3"), Some(&"value3".to_string()));
        assert_eq!(props.len(), 3);
    }

    #[test]
    fn test_parse_properties_with_whitespace() {
        let content = "key1 = value1 \n  key2=value2  ";
        let props = parse_properties_content(content);
        assert_eq!(props.get("key1"), Some(&"value1".to_string()));
        assert_eq!(props.get("key2"), Some(&"value2".to_string()));
    }
}
