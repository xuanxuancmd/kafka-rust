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

//! Plugin type enumeration for Kafka Connect.
//!
//! This module defines the types of plugins supported by Kafka Connect.

use std::fmt;

/// Enumeration of all plugin types supported by Kafka Connect.
///
/// Each variant corresponds to a specific plugin interface in the Connect framework.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PluginType {
    /// Source connector plugin type.
    Source,
    /// Sink connector plugin type.
    Sink,
    /// Converter plugin type for message conversion.
    Converter,
    /// Header converter plugin type for header conversion.
    HeaderConverter,
    /// Transformation plugin type (Single Message Transforms).
    Transformation,
    /// Predicate plugin type for conditional transformations.
    Predicate,
    /// Config provider plugin type for external configuration.
    ConfigProvider,
    /// REST extension plugin type for REST API extensions.
    RestExtension,
    /// Connector client config override policy plugin type.
    ConnectorClientConfigOverridePolicy,
}

impl PluginType {
    /// Returns the simple name of the plugin interface class.
    ///
    /// This corresponds to the simple class name in Java (without package).
    pub fn simple_name(&self) -> &'static str {
        match self {
            PluginType::Source => "SourceConnector",
            PluginType::Sink => "SinkConnector",
            PluginType::Converter => "Converter",
            PluginType::HeaderConverter => "HeaderConverter",
            PluginType::Transformation => "Transformation",
            PluginType::Predicate => "Predicate",
            PluginType::ConfigProvider => "ConfigProvider",
            PluginType::RestExtension => "ConnectRestExtension",
            PluginType::ConnectorClientConfigOverridePolicy => {
                "ConnectorClientConfigOverridePolicy"
            }
        }
    }

    /// Returns the fully qualified class name of the plugin interface.
    ///
    /// This matches the Java class names used in the Kafka Connect framework.
    pub fn super_class(&self) -> &'static str {
        match self {
            PluginType::Source => "org.apache.kafka.connect.source.SourceConnector",
            PluginType::Sink => "org.apache.kafka.connect.sink.SinkConnector",
            PluginType::Converter => "org.apache.kafka.connect.storage.Converter",
            PluginType::HeaderConverter => "org.apache.kafka.connect.storage.HeaderConverter",
            PluginType::Transformation => "org.apache.kafka.connect.transforms.Transformation",
            PluginType::Predicate => "org.apache.kafka.connect.transforms.predicates.Predicate",
            PluginType::ConfigProvider => "org.apache.kafka.common.config.provider.ConfigProvider",
            PluginType::RestExtension => "org.apache.kafka.connect.rest.ConnectRestExtension",
            PluginType::ConnectorClientConfigOverridePolicy => {
                "org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy"
            }
        }
    }

    /// Parses a plugin type from its string representation.
    ///
    /// The comparison is case-insensitive.
    pub fn from_str_ignore_case(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "source" => Some(PluginType::Source),
            "sink" => Some(PluginType::Sink),
            "converter" => Some(PluginType::Converter),
            "headerconverter" | "header_converter" | "header-converter" => {
                Some(PluginType::HeaderConverter)
            }
            "transformation" => Some(PluginType::Transformation),
            "predicate" => Some(PluginType::Predicate),
            "configprovider" | "config_provider" | "config-provider" => {
                Some(PluginType::ConfigProvider)
            }
            "restextension" | "rest_extension" | "rest-extension" => {
                Some(PluginType::RestExtension)
            }
            "connectorclientconfigoverridepolicy"
            | "connector_client_config_override_policy"
            | "connector-client-config-override-policy" => {
                Some(PluginType::ConnectorClientConfigOverridePolicy)
            }
            _ => None,
        }
    }
}

impl fmt::Display for PluginType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Match Java's toString() which uses lowercase
        match self {
            PluginType::Source => write!(f, "source"),
            PluginType::Sink => write!(f, "sink"),
            PluginType::Converter => write!(f, "converter"),
            PluginType::HeaderConverter => write!(f, "header_converter"),
            PluginType::Transformation => write!(f, "transformation"),
            PluginType::Predicate => write!(f, "predicate"),
            PluginType::ConfigProvider => write!(f, "configprovider"),
            PluginType::RestExtension => write!(f, "rest_extension"),
            PluginType::ConnectorClientConfigOverridePolicy => {
                write!(f, "connector_client_config_override_policy")
            }
        }
    }
}

impl std::str::FromStr for PluginType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_ignore_case(s).ok_or_else(|| format!("Unknown plugin type: {}", s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_name() {
        assert_eq!(PluginType::Source.simple_name(), "SourceConnector");
        assert_eq!(PluginType::Sink.simple_name(), "SinkConnector");
        assert_eq!(PluginType::Converter.simple_name(), "Converter");
        assert_eq!(PluginType::HeaderConverter.simple_name(), "HeaderConverter");
        assert_eq!(PluginType::Transformation.simple_name(), "Transformation");
        assert_eq!(PluginType::Predicate.simple_name(), "Predicate");
        assert_eq!(PluginType::ConfigProvider.simple_name(), "ConfigProvider");
        assert_eq!(
            PluginType::RestExtension.simple_name(),
            "ConnectRestExtension"
        );
        assert_eq!(
            PluginType::ConnectorClientConfigOverridePolicy.simple_name(),
            "ConnectorClientConfigOverridePolicy"
        );
    }

    #[test]
    fn test_super_class() {
        assert_eq!(
            PluginType::Source.super_class(),
            "org.apache.kafka.connect.source.SourceConnector"
        );
        assert_eq!(
            PluginType::Sink.super_class(),
            "org.apache.kafka.connect.sink.SinkConnector"
        );
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", PluginType::Source), "source");
        assert_eq!(format!("{}", PluginType::Sink), "sink");
        assert_eq!(format!("{}", PluginType::Converter), "converter");
        assert_eq!(
            format!("{}", PluginType::HeaderConverter),
            "header_converter"
        );
    }

    #[test]
    fn test_from_str() {
        assert_eq!("source".parse::<PluginType>().unwrap(), PluginType::Source);
        assert_eq!("SOURCE".parse::<PluginType>().unwrap(), PluginType::Source);
        assert_eq!("Sink".parse::<PluginType>().unwrap(), PluginType::Sink);
        assert!("unknown".parse::<PluginType>().is_err());
    }

    #[test]
    fn test_ordering() {
        assert!(PluginType::Source < PluginType::Sink);
        assert!(PluginType::Sink < PluginType::Converter);
    }
}
