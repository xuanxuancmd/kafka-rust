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

//! Version range parsing for plugin compatibility.
//!
//! This module provides Version and VersionRange structs that correspond to
//! Java's org.apache.maven.artifact.versioning.VersionRange.
//!
//! Supported syntax:
//! - `[1.0]` - exact version (1.0)
//! - `[1.0,2.0)` - 1.0 <= version < 2.0 (lower inclusive, upper exclusive)
//! - `[1.0,2.0]` - 1.0 <= version <= 2.0 (both inclusive)
//! - `[1.0,]` - version >= 1.0 (lower bound only)
//! - `(,2.0)` - version < 2.0 (upper bound only)
//! - `latest` - special keyword for latest version

use std::cmp::Ordering;
use std::fmt;

/// Error type for version parsing failures.
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// Invalid version string format.
    InvalidVersion(String),
    /// Invalid range syntax.
    InvalidRangeSyntax(String),
    /// Empty version string.
    EmptyVersion,
    /// Missing bracket in range specification.
    MissingBracket,
    /// Invalid bracket combination.
    InvalidBracketCombination,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::InvalidVersion(s) => write!(f, "Invalid version: {}", s),
            ParseError::InvalidRangeSyntax(s) => write!(f, "Invalid range syntax: {}", s),
            ParseError::EmptyVersion => write!(f, "Empty version string"),
            ParseError::MissingBracket => write!(f, "Missing bracket in range specification"),
            ParseError::InvalidBracketCombination => write!(f, "Invalid bracket combination"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Represents a version with major, minor, and patch components.
///
/// Version format: `major.minor.patch` or `major.minor`.
/// Missing components are treated as 0.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Version {
    /// Major version number.
    pub major: u32,
    /// Minor version number.
    pub minor: u32,
    /// Patch version number.
    pub patch: u32,
}

impl Version {
    /// Create a new version.
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Parse a version string like "1.0.0" or "1.0".
    ///
    /// # Examples
    /// ```
    /// let v1 = Version::parse("1.0.0")?;
    /// let v2 = Version::parse("1.0")?;
    /// let v3 = Version::parse("2")?;
    /// ```
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseError::EmptyVersion);
        }

        // Check that the entire string is valid (only digits and dots)
        for c in s.chars() {
            if !c.is_ascii_digit() && c != '.' {
                return Err(ParseError::InvalidVersion(s.to_string()));
            }
        }

        let parts: Vec<&str> = s.split('.').collect();

        // Ensure no empty parts (like "1." or ".0")
        for part in &parts {
            if part.is_empty() {
                return Err(ParseError::InvalidVersion(s.to_string()));
            }
        }

        let major = parts
            .first()
            .and_then(|p| p.parse::<u32>().ok())
            .ok_or_else(|| ParseError::InvalidVersion(s.to_string()))?;

        // For minor and patch, we need to check if they exist and are valid
        let minor = if parts.len() > 1 {
            parts
                .get(1)
                .and_then(|p| p.parse::<u32>().ok())
                .ok_or_else(|| ParseError::InvalidVersion(s.to_string()))?
        } else {
            0
        };

        let patch = if parts.len() > 2 {
            parts
                .get(2)
                .and_then(|p| p.parse::<u32>().ok())
                .ok_or_else(|| ParseError::InvalidVersion(s.to_string()))?
        } else {
            0
        };

        Ok(Self::new(major, minor, patch))
    }

    /// Convert version to string representation.
    pub fn to_string_full(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.patch == 0 {
            write!(f, "{}.{}", self.major, self.minor)
        } else {
            write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
        }
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.major.cmp(&other.major) {
            Ordering::Equal => match self.minor.cmp(&other.minor) {
                Ordering::Equal => self.patch.cmp(&other.patch),
                ord => ord,
            },
            ord => ord,
        }
    }
}

/// Represents a version range with optional lower and upper bounds.
///
/// Supports Maven-style version range syntax:
/// - `[1.0]` - exact version
/// - `[1.0,2.0)` - soft upper bound (exclusive)
/// - `[1.0,2.0]` - hard upper bound (inclusive)
/// - `[1.0,]` - lower bound only (version >= 1.0)
/// - `(,2.0)` - upper bound only (version < 2.0)
/// - `latest` - matches any version
#[derive(Debug, Clone, PartialEq)]
pub struct VersionRange {
    /// Lower bound version (None means no lower bound).
    pub lower: Option<Version>,
    /// Upper bound version (None means no upper bound).
    pub upper: Option<Version>,
    /// Is lower bound inclusive (true for `[`, false for `(`).
    pub lower_inclusive: bool,
    /// Is upper bound inclusive (true for `]`, false for `)`).
    pub upper_inclusive: bool,
    /// Is this a "latest" range (matches any version).
    pub is_latest: bool,
}

impl VersionRange {
    /// Create a new version range.
    pub fn new(
        lower: Option<Version>,
        upper: Option<Version>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    ) -> Self {
        Self {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
            is_latest: false,
        }
    }

    /// Create a "latest" version range that matches any version.
    pub fn latest() -> Self {
        Self {
            lower: None,
            upper: None,
            lower_inclusive: false,
            upper_inclusive: false,
            is_latest: true,
        }
    }

    /// Parse a version range specification string.
    ///
    /// # Supported Syntax
    /// - `[1.0]` - exact version (1.0)
    /// - `[1.0,2.0)` - 1.0 <= version < 2.0
    /// - `[1.0,2.0]` - 1.0 <= version <= 2.0
    /// - `[1.0,]` - version >= 1.0
    /// - `(,2.0)` - version < 2.0
    /// - `latest` - matches any version
    ///
    /// # Examples
    /// ```
    /// let range1 = VersionRange::parse("[1.0]")?;
    /// let range2 = VersionRange::parse("[1.0,2.0)")?;
    /// let range3 = VersionRange::parse("latest")?;
    /// ```
    pub fn parse(spec: &str) -> Result<Self, ParseError> {
        let spec = spec.trim();

        // Handle "latest" keyword
        if spec == "latest" {
            return Ok(Self::latest());
        }

        // Handle simple version without brackets (treated as recommended)
        if !spec.starts_with('[') && !spec.starts_with('(') {
            let version = Version::parse(spec)?;
            return Ok(Self::new(Some(version.clone()), Some(version), true, true));
        }

        // Must have closing bracket
        if !spec.ends_with(']') && !spec.ends_with(')') {
            return Err(ParseError::MissingBracket);
        }

        let lower_bracket = spec.chars().next().unwrap();
        let upper_bracket = spec.chars().last().unwrap();

        // Validate bracket combination
        if lower_bracket != '[' && lower_bracket != '(' {
            return Err(ParseError::InvalidBracketCombination);
        }
        if upper_bracket != ']' && upper_bracket != ')' {
            return Err(ParseError::InvalidBracketCombination);
        }

        // Extract content between brackets
        let content = &spec[1..spec.len() - 1];

        // Handle exact version: [1.0]
        if !content.contains(',') {
            let version = Version::parse(content)?;
            return Ok(Self::new(Some(version.clone()), Some(version), true, true));
        }

        // Split by comma
        let parts: Vec<&str> = content.splitn(2, ',').collect();
        if parts.len() != 2 {
            return Err(ParseError::InvalidRangeSyntax(spec.to_string()));
        }

        let lower_str = parts[0].trim();
        let upper_str = parts[1].trim();

        // Parse lower bound
        let lower = if lower_str.is_empty() {
            None
        } else {
            Some(Version::parse(lower_str)?)
        };

        // Parse upper bound
        let upper = if upper_str.is_empty() {
            None
        } else {
            Some(Version::parse(upper_str)?)
        };

        // Validate: at least one bound must exist
        if lower.is_none() && upper.is_none() {
            return Err(ParseError::InvalidRangeSyntax(spec.to_string()));
        }

        let lower_inclusive = lower_bracket == '[';
        let upper_inclusive = upper_bracket == ']';

        Ok(Self::new(lower, upper, lower_inclusive, upper_inclusive))
    }

    /// Check if a version string matches this range.
    ///
    /// # Examples
    /// ```
    /// let range = VersionRange::parse("[1.0,2.0)")?;
    /// assert!(range.matches("1.0"));
    /// assert!(range.matches("1.5"));
    /// assert!(!range.matches("2.0"));
    /// ```
    pub fn matches(&self, version: &str) -> bool {
        // "latest" matches any non-empty version
        if self.is_latest {
            return !version.trim().is_empty();
        }

        let v = Version::parse(version.trim());
        if v.is_err() {
            return false;
        }
        let v = v.unwrap();

        // Check lower bound
        if let Some(lower) = &self.lower {
            let cmp = v.cmp(lower);
            if cmp == Ordering::Less {
                return false;
            }
            if cmp == Ordering::Equal && !self.lower_inclusive {
                return false;
            }
        }

        // Check upper bound
        if let Some(upper) = &self.upper {
            let cmp = v.cmp(upper);
            if cmp == Ordering::Greater {
                return false;
            }
            if cmp == Ordering::Equal && !self.upper_inclusive {
                return false;
            }
        }

        true
    }

    /// Check if this range has any restrictions defined.
    ///
    /// A range has restrictions if it has at least one bound defined
    /// (and is not "latest").
    ///
    /// # Examples
    /// ```
    /// let range1 = VersionRange::parse("[1.0,]")?;
    /// assert!(range1.has_restrictions());
    /// let range2 = VersionRange::parse("latest")?;
    /// assert!(!range2.has_restrictions());
    /// ```
    pub fn has_restrictions(&self) -> bool {
        if self.is_latest {
            return false;
        }
        self.lower.is_some() || self.upper.is_some()
    }

    /// Get the recommended version from this range.
    ///
    /// The recommended version is typically:
    /// - For exact version `[1.0]`: returns 1.0
    /// - For range with upper bound: returns upper bound (if inclusive)
    /// - For range with only lower bound: returns lower bound
    /// - For "latest": returns None (no specific recommendation)
    ///
    /// # Examples
    /// ```
    /// let range1 = VersionRange::parse("[1.0]")?;
    /// assert_eq!(range1.recommended_version(), Some(Version::parse("1.0")?));
    /// let range2 = VersionRange::parse("[1.0,2.0]")?;
    /// assert_eq!(range2.recommended_version(), Some(Version::parse("2.0")?));
    /// ```
    pub fn recommended_version(&self) -> Option<Version> {
        if self.is_latest {
            return None;
        }

        // Exact version case
        if self.lower.is_some() && self.upper.is_some() {
            if self.lower == self.upper && self.lower_inclusive && self.upper_inclusive {
                return self.lower.clone();
            }
        }

        // Upper bound exists and is inclusive: recommend upper
        if self.upper.is_some() && self.upper_inclusive {
            return self.upper.clone();
        }

        // Lower bound exists: recommend lower
        if self.lower.is_some() {
            return self.lower.clone();
        }

        // Only upper bound exists (exclusive): return default version 0.0
        // This matches Maven behavior where an open lower bound defaults to 0.0
        Some(Version::new(0, 0, 0))
    }

    /// Check if this is an exact version range (single version).
    pub fn is_exact(&self) -> bool {
        self.lower.is_some()
            && self.upper.is_some()
            && self.lower == self.upper
            && self.lower_inclusive
            && self.upper_inclusive
    }

    /// Check if this range allows any version (no bounds).
    pub fn is_open(&self) -> bool {
        self.is_latest || (self.lower.is_none() && self.upper.is_none())
    }
}

impl fmt::Display for VersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_latest {
            return write!(f, "latest");
        }

        let lower_bracket = if self.lower_inclusive { '[' } else { '(' };
        let upper_bracket = if self.upper_inclusive { ']' } else { ')' };

        let lower_str = self
            .lower
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_default();

        let upper_str = self
            .upper
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_default();

        // Exact version format
        if self.is_exact() {
            return write!(f, "[{}]", lower_str);
        }

        write!(
            f,
            "{}{},{}{}",
            lower_bracket, lower_str, upper_str, upper_bracket
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_parse_basic() {
        let v = Version::parse("1.0.0").unwrap();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 0);
        assert_eq!(v.patch, 0);
    }

    #[test]
    fn test_version_parse_short() {
        let v1 = Version::parse("1.0").unwrap();
        assert_eq!(v1.major, 1);
        assert_eq!(v1.minor, 0);
        assert_eq!(v1.patch, 0);

        let v2 = Version::parse("2").unwrap();
        assert_eq!(v2.major, 2);
        assert_eq!(v2.minor, 0);
        assert_eq!(v2.patch, 0);
    }

    #[test]
    fn test_version_comparison() {
        let v1 = Version::parse("1.0.0").unwrap();
        let v2 = Version::parse("1.0.1").unwrap();
        let v3 = Version::parse("1.1.0").unwrap();
        let v4 = Version::parse("2.0.0").unwrap();

        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v3 < v4);
        assert!(v1 < v4);
    }

    #[test]
    fn test_version_parse_invalid() {
        assert!(Version::parse("").is_err());
        assert!(Version::parse("abc").is_err());
        assert!(Version::parse("1.a").is_err());
    }

    #[test]
    fn test_range_exact_version() {
        let range = VersionRange::parse("[1.0]").unwrap();
        assert!(range.is_exact());
        assert!(range.matches("1.0"));
        assert!(range.matches("1.0.0"));
        assert!(!range.matches("0.9"));
        assert!(!range.matches("1.1"));
        assert!(range.has_restrictions());
        assert_eq!(
            range.recommended_version(),
            Some(Version::parse("1.0").unwrap())
        );
    }

    #[test]
    fn test_range_lower_inclusive_upper_exclusive() {
        let range = VersionRange::parse("[1.0,2.0)").unwrap();
        assert!(range.matches("1.0"));
        assert!(range.matches("1.5"));
        assert!(!range.matches("2.0"));
        assert!(!range.matches("0.9"));
        assert!(!range.matches("2.1"));
        assert!(range.has_restrictions());
    }

    #[test]
    fn test_range_both_inclusive() {
        let range = VersionRange::parse("[1.0,2.0]").unwrap();
        assert!(range.matches("1.0"));
        assert!(range.matches("1.5"));
        assert!(range.matches("2.0"));
        assert!(!range.matches("0.9"));
        assert!(!range.matches("2.1"));
        assert!(range.has_restrictions());
        assert_eq!(
            range.recommended_version(),
            Some(Version::parse("2.0").unwrap())
        );
    }

    #[test]
    fn test_range_lower_bound_only() {
        let range = VersionRange::parse("[1.0,]").unwrap();
        assert!(range.matches("1.0"));
        assert!(range.matches("1.5"));
        assert!(range.matches("10.0"));
        assert!(!range.matches("0.9"));
        assert!(range.has_restrictions());
        assert_eq!(
            range.recommended_version(),
            Some(Version::parse("1.0").unwrap())
        );
    }

    #[test]
    fn test_range_upper_bound_only() {
        let range = VersionRange::parse("(,2.0)").unwrap();
        assert!(range.matches("1.0"));
        assert!(range.matches("1.9.9"));
        assert!(!range.matches("2.0"));
        assert!(!range.matches("2.1"));
        assert!(range.has_restrictions());
        assert_eq!(
            range.recommended_version(),
            Some(Version::parse("0.0").unwrap())
        ); // lower is None, so returns None-based logic
    }

    #[test]
    fn test_range_latest() {
        let range = VersionRange::parse("latest").unwrap();
        assert!(range.is_latest);
        assert!(range.matches("1.0"));
        assert!(range.matches("99.99.99"));
        assert!(!range.matches(""));
        assert!(!range.has_restrictions());
        assert!(range.recommended_version().is_none());
    }

    #[test]
    fn test_range_display() {
        let range1 = VersionRange::parse("[1.0]").unwrap();
        assert_eq!(range1.to_string(), "[1.0]");

        let range2 = VersionRange::parse("[1.0,2.0)").unwrap();
        assert_eq!(range2.to_string(), "[1.0,2.0)");

        let range3 = VersionRange::parse("latest").unwrap();
        assert_eq!(range3.to_string(), "latest");
    }

    #[test]
    fn test_range_parse_errors() {
        assert!(VersionRange::parse("").is_err());
        assert!(VersionRange::parse("[1.0").is_err()); // missing closing bracket
        assert!(VersionRange::parse("1.0]").is_err()); // missing opening bracket
        assert!(VersionRange::parse("[,]").is_err()); // empty bounds
        assert!(VersionRange::parse("[abc,def]").is_err()); // invalid versions
    }

    #[test]
    fn test_range_upper_exclusive_lower_exclusive() {
        let range = VersionRange::parse("(1.0,2.0)").unwrap();
        assert!(!range.matches("1.0")); // 1.0 is excluded
        assert!(range.matches("1.1"));
        assert!(range.matches("1.9.9"));
        assert!(!range.matches("2.0")); // 2.0 is excluded
    }
}
