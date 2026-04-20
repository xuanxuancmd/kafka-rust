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

//! Connect Worker system and runtime information.
//!
//! This module provides system and runtime information about the worker.
//! Corresponds to `org.apache.kafka.connect.runtime.WorkerInfo` in Java.

use log::info;
use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

/// Connect Worker system and runtime information.
///
/// Collects and logs JVM/runtime and OS/system information for debugging and monitoring.
/// This is useful for troubleshooting worker issues and understanding the environment
/// in which the worker is running.
///
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerInfo` in Java.
pub struct WorkerInfo {
    /// Collected information values.
    values: HashMap<String, String>,
}

impl WorkerInfo {
    /// Creates a new WorkerInfo instance and collects system/runtime information.
    ///
    /// Corresponds to Java: `public WorkerInfo()`
    pub fn new() -> Self {
        let mut values = HashMap::new();
        Self::add_runtime_info(&mut values);
        Self::add_system_info(&mut values);
        WorkerInfo { values }
    }

    /// Log all collected values at INFO level.
    ///
    /// Equivalent to `logAll` in AbstractConfig.
    /// Corresponds to Java: `public void logAll()`
    pub fn log_all(&self) {
        let mut output = String::from("WorkerInfo values:\n");
        for (key, value) in &self.values {
            output.push_str(&format!("\t{} = {}\n", key, Self::format_value(value)));
        }
        info!("{}", output);
    }

    /// Format a value for display, returning "NA" for null values.
    fn format_value(value: &str) -> &str {
        if value.is_empty() {
            "NA"
        } else {
            value
        }
    }

    /// Collect general runtime information.
    ///
    /// Collects JVM arguments, specification, and classpath information.
    /// In Rust, we collect analogous information about the Rust runtime.
    /// Corresponds to Java: `protected final void addRuntimeInfo()`
    fn add_runtime_info(values: &mut HashMap<String, String>) {
        // Collect environment and process information
        // Rust version and target info
        let rust_version = env::var("RUST_VERSION").unwrap_or_else(|_| {
            format!(
                "{}.{}",
                env!("CARGO_PKG_VERSION_MAJOR"),
                env!("CARGO_PKG_VERSION_MINOR")
            )
        });
        values.insert("runtime.version".to_string(), rust_version);

        // Process arguments (similar to JVM input arguments)
        let args: Vec<String> = env::args().collect();
        values.insert("runtime.args".to_string(), args.join(", "));

        // Target specification
        let target = env::var("TARGET").unwrap_or_else(|_| {
            #[cfg(target_os = "linux")]
            {
                "linux".to_string()
            }
            #[cfg(target_os = "windows")]
            {
                "windows".to_string()
            }
            #[cfg(target_os = "macos")]
            {
                "macos".to_string()
            }
            #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
            {
                "unknown".to_string()
            }
        });
        values.insert("runtime.target".to_string(), target);

        // Build profile (debug/release)
        let profile = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        values.insert("runtime.profile".to_string(), profile.to_string());

        // Package name and version
        values.insert(
            "runtime.package".to_string(),
            env!("CARGO_PKG_NAME").to_string(),
        );
        values.insert(
            "runtime.package_version".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        );

        // Current working directory (analogous to classpath concept)
        if let Ok(cwd) = env::current_dir() {
            values.insert("runtime.cwd".to_string(), cwd.to_string_lossy().to_string());
        }
    }

    /// Collect system information.
    ///
    /// Collects OS name, architecture, version, and available processors.
    /// Corresponds to Java: `protected final void addSystemInfo()`
    fn add_system_info(values: &mut HashMap<String, String>) {
        // OS information
        #[cfg(target_os = "linux")]
        let os_name = "Linux";
        #[cfg(target_os = "windows")]
        let os_name = "Windows";
        #[cfg(target_os = "macos")]
        let os_name = "Mac OS X";
        #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
        let os_name = "Unknown";

        #[cfg(target_arch = "x86_64")]
        let arch = "x86_64";
        #[cfg(target_arch = "x86")]
        let arch = "x86";
        #[cfg(target_arch = "aarch64")]
        let arch = "aarch64";
        #[cfg(target_arch = "arm")]
        let arch = "arm";
        #[cfg(not(any(
            target_arch = "x86_64",
            target_arch = "x86",
            target_arch = "aarch64",
            target_arch = "arm"
        )))]
        let arch = "unknown";

        // Combine OS spec info (name, arch, version)
        let os_version = env::var("OS_VERSION")
            .or_else(|_| env::var("OSTYPE"))
            .unwrap_or_else(|_| "unknown".to_string());
        let os_spec = format!("{}, {}, {}", os_name, arch, os_version);
        values.insert("os.spec".to_string(), os_spec);

        // Number of available processors (vcpus)
        let available_processors = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        values.insert("os.vcpus".to_string(), available_processors.to_string());

        // Current time as Unix timestamp
        if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
            values.insert("os.epoch_ms".to_string(), now.as_millis().to_string());
        }

        // Memory info if available
        #[cfg(target_os = "linux")]
        {
            if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
                // Extract total memory (MemTotal line)
                for line in meminfo.lines() {
                    if line.starts_with("MemTotal:") {
                        values.insert(
                            "os.total_memory_kb".to_string(),
                            line.split(':')
                                .nth(1)
                                .map(|s| s.trim())
                                .unwrap_or("unknown")
                                .to_string(),
                        );
                        break;
                    }
                }
            }
        }

        #[cfg(target_os = "windows")]
        {
            // Windows: use environment variables or system info
            values.insert(
                "os.total_memory_kb".to_string(),
                env::var("TOTALMEMORYKB").unwrap_or_else(|_| "unknown".to_string()),
            );
        }
    }

    /// Get a specific value by key.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.values.get(key)
    }

    /// Get all collected values.
    pub fn values(&self) -> &HashMap<String, String> {
        &self.values
    }
}

impl Default for WorkerInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_info_new() {
        let info = WorkerInfo::new();
        assert!(!info.values.is_empty());

        // Should have runtime info
        assert!(info.get("runtime.package").is_some());
        assert!(info.get("runtime.package_version").is_some());

        // Should have system info
        assert!(info.get("os.spec").is_some());
        assert!(info.get("os.vcpus").is_some());
    }

    #[test]
    fn test_worker_info_values() {
        let info = WorkerInfo::new();
        let values = info.values();

        // Check that vcpus is a valid number
        let vcpus = values.get("os.vcpus").unwrap();
        let vcpus_num: usize = vcpus.parse().unwrap();
        assert!(vcpus_num > 0);
    }

    #[test]
    fn test_format_value() {
        assert_eq!(WorkerInfo::format_value(""), "NA");
        assert_eq!(WorkerInfo::format_value("test"), "test");
    }
}
