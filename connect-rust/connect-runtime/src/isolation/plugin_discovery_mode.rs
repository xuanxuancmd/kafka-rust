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

//! Plugin discovery mode enumeration for Kafka Connect.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode` in Java.
//! See KIP-898: Modernize Connect plugin discovery.
//!
//! Strategy to use to discover plugins usable on a Connect worker.

use std::fmt;
use std::str::FromStr;

/// Strategy to use to discover plugins usable on a Connect worker.
///
/// This corresponds to `org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode` in Java.
/// See <https://cwiki.apache.org/confluence/display/KAFKA/KIP-898%3A+Modernize+Connect+plugin+discovery>
///
/// # Discovery Modes
///
/// - `OnlyScan`: Scan for plugins reflectively only. This corresponds to the legacy behavior
///   of Connect prior to KIP-898. Note: ConfigProvider, ConnectRestExtension, and
///   ConnectorClientConfigOverridePolicy are still loaded using ServiceLoader in this mode.
///
/// - `HybridWarn`: Scan for plugins reflectively and via ServiceLoader.
///   Emit warnings if one or more plugins is not available via ServiceLoader.
///
/// - `HybridFail`: Scan for plugins reflectively and via ServiceLoader.
///   Fail worker during startup if one or more plugins is not available via ServiceLoader.
///
/// - `ServiceLoad`: Discover plugins via ServiceLoader only.
///   Plugins may not be usable if they are not available via ServiceLoader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PluginDiscoveryMode {
    /// Scan for plugins reflectively only.
    /// This corresponds to the legacy behavior of Connect prior to KIP-898.
    /// Note: ConfigProvider, ConnectRestExtension, and ConnectorClientConfigOverridePolicy
    /// are still loaded using ServiceLoader in this mode.
    OnlyScan,
    /// Scan for plugins reflectively and via ServiceLoader.
    /// Emit warnings if one or more plugins is not available via ServiceLoader.
    HybridWarn,
    /// Scan for plugins reflectively and via ServiceLoader.
    /// Fail worker during startup if one or more plugins is not available via ServiceLoader.
    HybridFail,
    /// Discover plugins via ServiceLoader only.
    /// Plugins may not be usable if they are not available via ServiceLoader.
    ServiceLoad,
}

impl PluginDiscoveryMode {
    /// Returns whether this mode uses reflective scanning.
    ///
    /// This corresponds to `reflectivelyScan()` in Java.
    /// Returns true for all modes except `ServiceLoad`.
    ///
    /// # Examples
    /// ```
    /// use connect_runtime::isolation::PluginDiscoveryMode;
    ///
    /// assert!(PluginDiscoveryMode::OnlyScan.reflectively_scan());
    /// assert!(PluginDiscoveryMode::HybridWarn.reflectively_scan());
    /// assert!(PluginDiscoveryMode::HybridFail.reflectively_scan());
    /// assert!(!PluginDiscoveryMode::ServiceLoad.reflectively_scan());
    /// ```
    pub fn reflectively_scan(&self) -> bool {
        match self {
            PluginDiscoveryMode::OnlyScan => true,
            PluginDiscoveryMode::HybridWarn => true,
            PluginDiscoveryMode::HybridFail => true,
            PluginDiscoveryMode::ServiceLoad => false,
        }
    }

    /// Returns whether this mode uses ServiceLoader.
    ///
    /// This corresponds to `serviceLoad()` in Java.
    /// Returns true for all modes except `OnlyScan`.
    ///
    /// # Examples
    /// ```
    /// use connect_runtime::isolation::PluginDiscoveryMode;
    ///
    /// assert!(!PluginDiscoveryMode::OnlyScan.service_load());
    /// assert!(PluginDiscoveryMode::HybridWarn.service_load());
    /// assert!(PluginDiscoveryMode::HybridFail.service_load());
    /// assert!(PluginDiscoveryMode::ServiceLoad.service_load());
    /// ```
    pub fn service_load(&self) -> bool {
        match self {
            PluginDiscoveryMode::OnlyScan => false,
            PluginDiscoveryMode::HybridWarn => true,
            PluginDiscoveryMode::HybridFail => true,
            PluginDiscoveryMode::ServiceLoad => true,
        }
    }

    /// Returns whether this mode is a hybrid mode.
    ///
    /// Hybrid modes use both reflective scanning and ServiceLoader.
    pub fn is_hybrid(&self) -> bool {
        matches!(
            self,
            PluginDiscoveryMode::HybridWarn | PluginDiscoveryMode::HybridFail
        )
    }

    /// Returns whether this mode should fail on ServiceLoader issues.
    ///
    /// Only `HybridFail` mode will cause the worker to fail during startup
    /// if plugins are not available via ServiceLoader.
    pub fn should_fail_on_service_loader_issues(&self) -> bool {
        matches!(self, PluginDiscoveryMode::HybridFail)
    }

    /// Returns whether this mode should warn on ServiceLoader issues.
    ///
    /// `HybridWarn` mode will emit warnings if plugins are not available via ServiceLoader.
    pub fn should_warn_on_service_loader_issues(&self) -> bool {
        matches!(self, PluginDiscoveryMode::HybridWarn)
    }

    /// Parses a discovery mode from a configuration string.
    ///
    /// The comparison is case-insensitive and supports various formats:
    /// - "only_scan", "only-scan", "onlyscan" -> OnlyScan
    /// - "hybrid_warn", "hybrid-warn", "hybridwarn" -> HybridWarn
    /// - "hybrid_fail", "hybrid-fail", "hybridfail" -> HybridFail
    /// - "service_load", "service-load", "serviceload" -> ServiceLoad
    pub fn from_config_str(s: &str) -> Option<Self> {
        match s.to_lowercase().replace('_', "-").as_str() {
            "only-scan" | "onlyscan" => Some(PluginDiscoveryMode::OnlyScan),
            "hybrid-warn" | "hybridwarn" => Some(PluginDiscoveryMode::HybridWarn),
            "hybrid-fail" | "hybridfail" => Some(PluginDiscoveryMode::HybridFail),
            "service-load" | "serviceload" => Some(PluginDiscoveryMode::ServiceLoad),
            _ => None,
        }
    }

    /// Returns all available discovery modes.
    pub fn all() -> [PluginDiscoveryMode; 4] {
        [
            PluginDiscoveryMode::OnlyScan,
            PluginDiscoveryMode::HybridWarn,
            PluginDiscoveryMode::HybridFail,
            PluginDiscoveryMode::ServiceLoad,
        ]
    }
}

impl fmt::Display for PluginDiscoveryMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Match Java's toString() which uses lowercase
        match self {
            PluginDiscoveryMode::OnlyScan => write!(f, "only_scan"),
            PluginDiscoveryMode::HybridWarn => write!(f, "hybrid_warn"),
            PluginDiscoveryMode::HybridFail => write!(f, "hybrid_fail"),
            PluginDiscoveryMode::ServiceLoad => write!(f, "service_load"),
        }
    }
}

impl FromStr for PluginDiscoveryMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_config_str(s).ok_or_else(|| format!("Unknown plugin discovery mode: {}", s))
    }
}

impl Default for PluginDiscoveryMode {
    /// Default discovery mode is `OnlyScan` (legacy behavior).
    fn default() -> Self {
        PluginDiscoveryMode::OnlyScan
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reflectively_scan() {
        assert!(PluginDiscoveryMode::OnlyScan.reflectively_scan());
        assert!(PluginDiscoveryMode::HybridWarn.reflectively_scan());
        assert!(PluginDiscoveryMode::HybridFail.reflectively_scan());
        assert!(!PluginDiscoveryMode::ServiceLoad.reflectively_scan());
    }

    #[test]
    fn test_service_load() {
        assert!(!PluginDiscoveryMode::OnlyScan.service_load());
        assert!(PluginDiscoveryMode::HybridWarn.service_load());
        assert!(PluginDiscoveryMode::HybridFail.service_load());
        assert!(PluginDiscoveryMode::ServiceLoad.service_load());
    }

    #[test]
    fn test_is_hybrid() {
        assert!(!PluginDiscoveryMode::OnlyScan.is_hybrid());
        assert!(PluginDiscoveryMode::HybridWarn.is_hybrid());
        assert!(PluginDiscoveryMode::HybridFail.is_hybrid());
        assert!(!PluginDiscoveryMode::ServiceLoad.is_hybrid());
    }

    #[test]
    fn test_should_fail_on_service_loader_issues() {
        assert!(!PluginDiscoveryMode::OnlyScan.should_fail_on_service_loader_issues());
        assert!(!PluginDiscoveryMode::HybridWarn.should_fail_on_service_loader_issues());
        assert!(PluginDiscoveryMode::HybridFail.should_fail_on_service_loader_issues());
        assert!(!PluginDiscoveryMode::ServiceLoad.should_fail_on_service_loader_issues());
    }

    #[test]
    fn test_should_warn_on_service_loader_issues() {
        assert!(!PluginDiscoveryMode::OnlyScan.should_warn_on_service_loader_issues());
        assert!(PluginDiscoveryMode::HybridWarn.should_warn_on_service_loader_issues());
        assert!(!PluginDiscoveryMode::HybridFail.should_warn_on_service_loader_issues());
        assert!(!PluginDiscoveryMode::ServiceLoad.should_warn_on_service_loader_issues());
    }

    #[test]
    fn test_display() {
        assert_eq!(PluginDiscoveryMode::OnlyScan.to_string(), "only_scan");
        assert_eq!(PluginDiscoveryMode::HybridWarn.to_string(), "hybrid_warn");
        assert_eq!(PluginDiscoveryMode::HybridFail.to_string(), "hybrid_fail");
        assert_eq!(PluginDiscoveryMode::ServiceLoad.to_string(), "service_load");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            "only_scan".parse::<PluginDiscoveryMode>().unwrap(),
            PluginDiscoveryMode::OnlyScan
        );
        assert_eq!(
            "ONLY_SCAN".parse::<PluginDiscoveryMode>().unwrap(),
            PluginDiscoveryMode::OnlyScan
        );
        assert_eq!(
            "hybrid_warn".parse::<PluginDiscoveryMode>().unwrap(),
            PluginDiscoveryMode::HybridWarn
        );
        assert_eq!(
            "hybrid-fail".parse::<PluginDiscoveryMode>().unwrap(),
            PluginDiscoveryMode::HybridFail
        );
        assert_eq!(
            "serviceload".parse::<PluginDiscoveryMode>().unwrap(),
            PluginDiscoveryMode::ServiceLoad
        );
        assert!("unknown".parse::<PluginDiscoveryMode>().is_err());
    }

    #[test]
    fn test_from_config_str() {
        assert_eq!(
            PluginDiscoveryMode::from_config_str("only_scan"),
            Some(PluginDiscoveryMode::OnlyScan)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("only-scan"),
            Some(PluginDiscoveryMode::OnlyScan)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("onlyscan"),
            Some(PluginDiscoveryMode::OnlyScan)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("ONLY_SCAN"),
            Some(PluginDiscoveryMode::OnlyScan)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("hybrid_warn"),
            Some(PluginDiscoveryMode::HybridWarn)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("hybrid-warn"),
            Some(PluginDiscoveryMode::HybridWarn)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("hybridwarn"),
            Some(PluginDiscoveryMode::HybridWarn)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("hybrid_fail"),
            Some(PluginDiscoveryMode::HybridFail)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("hybrid-fail"),
            Some(PluginDiscoveryMode::HybridFail)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("hybridfail"),
            Some(PluginDiscoveryMode::HybridFail)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("service_load"),
            Some(PluginDiscoveryMode::ServiceLoad)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("service-load"),
            Some(PluginDiscoveryMode::ServiceLoad)
        );
        assert_eq!(
            PluginDiscoveryMode::from_config_str("serviceload"),
            Some(PluginDiscoveryMode::ServiceLoad)
        );
        assert_eq!(PluginDiscoveryMode::from_config_str("invalid"), None);
    }

    #[test]
    fn test_default() {
        assert_eq!(
            PluginDiscoveryMode::default(),
            PluginDiscoveryMode::OnlyScan
        );
    }

    #[test]
    fn test_all() {
        let all_modes = PluginDiscoveryMode::all();
        assert_eq!(all_modes.len(), 4);
    }
}
