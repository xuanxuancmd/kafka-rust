//! RegexRouter transformation
//!
//! This module provides the RegexRouter transformation which routes records
//! to dynamically named topics based on a regular expression.

use connect_api::{Closeable, ConfigDef, ConfigValue, Configurable, ConnectRecord, Transformation};
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;

/// RegexRouter transformation
///
/// Routes records to dynamically named topics based on a regular expression.
/// The transformation can replace the topic name regex with a replacement string,
/// supporting capture groups and special regex patterns.
///
/// # Configuration
///
/// - `regex`: The regular expression pattern to match against the topic name
/// - `replacement`: The replacement string, can include capture group references
/// - `source`: The source field to apply the regex to (optional, defaults to topic)
///
/// # Example
///
/// ```rust
/// use connect_transforms::regex_router::RegexRouter;
/// use connect_api::data::SourceRecord;
/// use connect_api::Configurable;
/// use std::collections::HashMap;
///
/// let mut router = RegexRouter::<SourceRecord>::new();
/// let mut configs = HashMap::new();
/// configs.insert("regex".to_string(), Box::new("test-(\\d+)".to_string()) as Box<dyn std::any::Any>);
/// configs.insert("replacement".to_string(), Box::new("production-$1".to_string()) as Box<dyn std::any::Any>);
/// router.configure(configs);
/// ```
pub struct RegexRouter<R: ConnectRecord<R>> {
    /// The regular expression pattern to match
    pattern: Option<Regex>,
    /// The replacement string
    replacement: String,
    /// The source field to apply the regex to
    source: String,
    /// Phantom data for the record type
    _phantom: PhantomData<R>,
}

impl<R: ConnectRecord<R>> RegexRouter<R> {
    /// Create a new RegexRouter transformation
    ///
    /// # Returns
    ///
    /// A new RegexRouter instance with default configuration
    pub fn new() -> Self {
        Self {
            pattern: None,
            replacement: String::new(),
            source: String::new(),
            _phantom: PhantomData,
        }
    }

    /// Create a new RegexRouter with a specific pattern and replacement
    ///
    /// # Arguments
    ///
    /// * `pattern` - The regular expression pattern
    /// * `replacement` - The replacement string
    ///
    /// # Returns
    ///
    /// A new RegexRouter instance with the specified pattern and replacement
    ///
    /// # Errors
    ///
    /// Returns an error if the pattern is invalid
    pub fn with_pattern(pattern: &str, replacement: &str) -> Result<Self, Box<dyn Error>> {
        let regex = Regex::new(pattern)?;
        Ok(Self {
            pattern: Some(regex),
            replacement: replacement.to_string(),
            source: String::new(),
            _phantom: PhantomData,
        })
    }

    /// Apply the regex transformation to a string
    ///
    /// # Arguments
    ///
    /// * `input` - The input string to transform
    ///
    /// # Returns
    ///
    /// The transformed string, or None if the pattern doesn't match
    fn apply_regex(&self, input: &str) -> Option<String> {
        if let Some(ref pattern) = self.pattern {
            Some(pattern.replace(input, &self.replacement).to_string())
        } else {
            Some(input.to_string())
        }
    }

    /// Validate the regex pattern
    ///
    /// # Arguments
    ///
    /// * `pattern` - The pattern string to validate
    ///
    /// # Returns
    ///
    /// Ok(()) if the pattern is valid, Err otherwise
    fn validate_pattern(pattern: &str) -> Result<(), Box<dyn Error>> {
        Regex::new(pattern)?;
        Ok(())
    }

    /// Check if the transformation is configured
    ///
    /// # Returns
    ///
    /// true if the pattern is set, false otherwise
    pub fn is_configured(&self) -> bool {
        self.pattern.is_some()
    }

    /// Get the current replacement string
    ///
    /// # Returns
    ///
    /// The replacement string
    pub fn replacement(&self) -> &str {
        &self.replacement
    }

    /// Get the current source field
    ///
    /// # Returns
    ///
    /// The source field
    pub fn source(&self) -> &str {
        &self.source
    }
}

impl<R: ConnectRecord<R>> Default for RegexRouter<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: ConnectRecord<R>> Configurable for RegexRouter<R> {
    /// Configure the RegexRouter transformation
    ///
    /// # Arguments
    ///
    /// * `configs` - A HashMap of configuration parameters
    ///
    /// # Configuration Parameters
    ///
    /// - `regex`: The regular expression pattern (String)
    /// - `replacement`: The replacement string (String)
    /// - `source`: The source field to apply the regex to (String, optional)
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(regex) = configs.get("regex") {
            if let Some(regex_str) = regex.downcast_ref::<String>() {
                match Regex::new(regex_str) {
                    Ok(pattern) => {
                        self.pattern = Some(pattern);
                    }
                    Err(_) => {
                        // Invalid regex pattern, keep the old pattern or set to None
                        self.pattern = None;
                    }
                }
            }
        }

        if let Some(replacement) = configs.get("replacement") {
            if let Some(repl) = replacement.downcast_ref::<String>() {
                self.replacement = repl.clone();
            }
        }

        if let Some(source) = configs.get("source") {
            if let Some(src) = source.downcast_ref::<String>() {
                self.source = src.clone();
            }
        }
    }
}

impl<R: ConnectRecord<R>> Closeable for RegexRouter<R> {
    /// Close the RegexRouter transformation
    ///
    /// # Returns
    ///
    /// Ok(()) if successful
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.pattern = None;
        self.replacement.clear();
        self.source.clear();
        Ok(())
    }
}

impl<R: ConnectRecord<R>> Transformation<R> for RegexRouter<R> {
    /// Apply the regex transformation to a record
    ///
    /// # Arguments
    ///
    /// * `record` - The record to transform
    ///
    /// # Returns
    ///
    /// Ok(Some(record)) if the transformation was applied, Ok(None) if the record was filtered
    ///
    /// # Errors
    ///
    /// Returns an error if the transformation fails
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>> {
        // For now, we return the record as-is since we can't easily modify the topic
        // without more API support. In a full implementation, this would:
        // 1. Get the topic name from the record
        // 2. Apply the regex transformation
        // 3. Update the record with the new topic name
        Ok(Some(record))
    }

    /// Get the configuration definition for this transformation
    ///
    /// # Returns
    ///
    /// A ConfigDef describing the required and optional configuration parameters
    fn config(&self) -> ConfigDef {
        let mut config = ConfigDef::new();
        config.add_config("regex".to_string(), ConfigValue::String(String::new()));
        config.add_config(
            "replacement".to_string(),
            ConfigValue::String(String::new()),
        );
        config.add_config("source".to_string(), ConfigValue::String(String::new()));
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connect_api::data::SourceRecord;

    #[test]
    fn test_regex_router_new() {
        let router: RegexRouter<SourceRecord> = RegexRouter::new();
        assert!(!router.is_configured());
        assert_eq!(router.replacement(), "");
        assert_eq!(router.source(), "");
    }

    #[test]
    fn test_regex_router_default() {
        let router: RegexRouter<SourceRecord> = RegexRouter::default();
        assert!(!router.is_configured());
    }

    #[test]
    fn test_regex_router_with_pattern_valid() {
        let result = RegexRouter::<SourceRecord>::with_pattern("test-(\\d+)", "production-$1");
        assert!(result.is_ok());
        let router = result.unwrap();
        assert!(router.is_configured());
        assert_eq!(router.replacement(), "production-$1");
    }

    #[test]
    fn test_regex_router_with_pattern_invalid() {
        let result = RegexRouter::<SourceRecord>::with_pattern("[invalid(", "replacement");
        assert!(result.is_err());
    }

    #[test]
    fn test_regex_router_configure() {
        let mut router: RegexRouter<SourceRecord> = RegexRouter::new();
        let mut configs = HashMap::new();
        configs.insert(
            "regex".to_string(),
            Box::new("test-(\\d+)".to_string()) as Box<dyn std::any::Any>,
        );
        configs.insert(
            "replacement".to_string(),
            Box::new("production-$1".to_string()) as Box<dyn std::any::Any>,
        );
        router.configure(configs);
        assert!(router.is_configured());
        assert_eq!(router.replacement(), "production-$1");
    }

    #[test]
    fn test_regex_router_configure_invalid_regex() {
        let mut router: RegexRouter<SourceRecord> = RegexRouter::new();
        let mut configs = HashMap::new();
        configs.insert(
            "regex".to_string(),
            Box::new("[invalid(".to_string()) as Box<dyn std::any::Any>,
        );
        router.configure(configs);
        assert!(!router.is_configured());
    }

    #[test]
    fn test_regex_router_close() {
        let mut router: RegexRouter<SourceRecord> = RegexRouter::new();
        let mut configs = HashMap::new();
        configs.insert(
            "regex".to_string(),
            Box::new("test-(\\d+)".to_string()) as Box<dyn std::any::Any>,
        );
        router.configure(configs);
        assert!(router.is_configured());
        let result = router.close();
        assert!(result.is_ok());
        assert!(!router.is_configured());
    }

    #[test]
    fn test_regex_router_config() {
        let router: RegexRouter<SourceRecord> = RegexRouter::new();
        let config = router.config();
        assert!(config.get_config("regex").is_some());
        assert!(config.get_config("replacement").is_some());
        assert!(config.get_config("source").is_some());
    }

    #[test]
    fn test_regex_router_validate_pattern_valid() {
        let result = RegexRouter::<SourceRecord>::validate_pattern("test-(\\d+)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_regex_router_validate_pattern_invalid() {
        let result = RegexRouter::<SourceRecord>::validate_pattern("[invalid(");
        assert!(result.is_err());
    }
}
