//! Connector Utilities
//!
//! This module provides utility functions for connector implementations.

use std::collections::HashMap;

/// Utility functions for connector implementations
pub struct ConnectorUtils;

impl ConnectorUtils {
    /// Given a list of elements and a target number of groups, generates list of groups of
    /// elements to match the target number of groups, spreading them evenly among groups.
    ///
    /// This generates groups with contiguous elements, which results in intuitive ordering if
    /// your elements are also ordered (e.g. alphabetical lists of table names if you sort
    /// table names alphabetically to generate raw partitions) or can result in efficient
    /// partitioning if elements are sorted according to some criteria that affects performance
    /// (e.g. topic partitions with the same leader).
    ///
    /// # Arguments
    ///
    /// * `elements` - list of elements to partition
    /// * `num_groups` - number of output groups to generate
    ///
    /// # Returns
    ///
    /// List of groups, where each group is a list of elements
    ///
    /// # Examples
    ///
    /// ```
    /// use connect_api::utils::ConnectorUtils;
    ///
    /// let elements = vec!["topic1", "topic2", "topic3", "topic4", "topic5", "topic6"];
    /// let groups = ConnectorUtils::group_partitions(elements, 3);
    /// // groups = [["topic1", "topic2"], ["topic3", "topic4"], ["topic5", "topic6"]]
    /// ```
    pub fn group_partitions<T: Clone>(elements: Vec<T>, num_groups: usize) -> Vec<Vec<T>> {
        if num_groups == 0 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(num_groups);
        let per_group = elements.len() / num_groups;
        let leftover = elements.len() - (num_groups * per_group);

        let mut assigned = 0;
        for group in 0..num_groups {
            let num_this_group = if group < leftover {
                per_group + 1
            } else {
                per_group
            };

            let mut group_list = Vec::with_capacity(num_this_group);
            for _ in 0..num_this_group {
                if assigned < elements.len() {
                    group_list.push(elements[assigned].clone());
                    assigned += 1;
                }
            }

            result.push(group_list);
        }

        result
    }

    /// Convert a connector type to a string representation
    ///
    /// # Arguments
    ///
    /// * `connector_type` - the connector type
    ///
    /// # Returns
    ///
    /// String representation of the connector type
    ///
    /// # Examples
    ///
    /// ```
    /// use connect_api::utils::ConnectorUtils;
    ///
    /// let type_str = ConnectorUtils::connector_type_string("source");
    /// // type_str = "source"
    /// ```
    pub fn connector_type_string(connector_type: &str) -> String {
        connector_type.to_string()
    }

    /// Validate connector configuration
    ///
    /// # Arguments
    ///
    /// * `config` - configuration map
    ///
    /// # Returns
    ///
    /// Result indicating whether configuration is valid
    ///
    /// # Examples
    ///
    /// ```
    /// use connect_api::utils::ConnectorUtils;
    /// use std::collections::HashMap;
    ///
    /// let mut config = HashMap::new();
    /// config.insert("name".to_string(), "test".to_string());
    /// let valid = ConnectorUtils::validate_config(&config);
    /// ```
    pub fn validate_config(config: &HashMap<String, String>) -> Result<(), String> {
        if !config.contains_key("name") {
            return Err("Connector name is required".to_string());
        }

        if config.get("name").map(|s| s.is_empty()).unwrap_or(true) {
            return Err("Connector name cannot be empty".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_partitions_basic() {
        let elements = vec!["a", "b", "c", "d", "e", "f"];
        let groups = ConnectorUtils::group_partitions(elements, 3);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0], vec!["a", "b"]);
        assert_eq!(groups[1], vec!["c", "d"]);
        assert_eq!(groups[2], vec!["e", "f"]);
    }

    #[test]
    fn test_group_partitions_leftover() {
        let elements = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
        let groups = ConnectorUtils::group_partitions(elements, 3);
        assert_eq!(groups.len(), 3);
        // 8 elements / 3 groups = 2 per group with 2 leftover
        // First 2 groups get extra element: 3, 3, 2
        assert_eq!(groups[0].len(), 3);
        assert_eq!(groups[1].len(), 3);
        assert_eq!(groups[2].len(), 2);
    }

    #[test]
    fn test_group_partitions_zero_groups() {
        let elements = vec!["a", "b", "c"];
        let groups = ConnectorUtils::group_partitions(elements, 0);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn test_connector_type_string() {
        assert_eq!(ConnectorUtils::connector_type_string("source"), "source");
        assert_eq!(ConnectorUtils::connector_type_string("sink"), "sink");
    }

    #[test]
    fn test_validate_config_valid() {
        let mut config = HashMap::new();
        config.insert("name".to_string(), "test".to_string());
        assert!(ConnectorUtils::validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_missing_name() {
        let config = HashMap::new();
        assert!(ConnectorUtils::validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_empty_name() {
        let mut config = HashMap::new();
        config.insert("name".to_string(), "".to_string());
        assert!(ConnectorUtils::validate_config(&config).is_err());
    }
}
