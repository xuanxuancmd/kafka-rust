//! Kafka Connect Transforms
//!
//! This crate provides built-in data transformations for Kafka Connect.

pub mod transformation;
pub mod predicate;
pub mod simple;
pub mod complex;

pub use transformation::Transformation;
pub use predicate::Predicate;
