//! Tolerance type module
//!
//! Provides the ToleranceType enum for different levels of error tolerance.

/// The different levels of error tolerance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToleranceType {
    /// Tolerate no errors.
    None,

    /// Tolerate all errors.
    All,
}

impl ToleranceType {
    /// Get the tolerance type value as a lowercase string
    pub fn value(&self) -> &'static str {
        match self {
            ToleranceType::None => "none",
            ToleranceType::All => "all",
        }
    }

    /// Parse a string to ToleranceType
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" => Some(ToleranceType::None),
            "all" => Some(ToleranceType::All),
            _ => None,
        }
    }
}

impl std::fmt::Display for ToleranceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}
