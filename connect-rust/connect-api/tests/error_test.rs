//! Tests for error module

use connect_api::error::{ConnectError, RetriableError};

#[test]
fn test_connect_error_display() {
    let error = ConnectError::Other("test error".to_string());
    assert_eq!(error.to_string(), "Error: test error");
}

#[test]
fn test_config_error_display() {
    let error = ConnectError::ConfigError("config error".to_string());
    assert_eq!(error.to_string(), "Config error: config error");
}

#[test]
fn test_connect_error_debug() {
    let error = ConnectError::Other("test error".to_string());
    let debug_str = format!("{:?}", error);
    assert!(debug_str.contains("test error"));
}

#[test]
fn test_retriable_error() {
    let retriable = ConnectError::TaskError("task error".to_string());
    assert!(retriable.retriable());

    let non_retriable = ConnectError::ConfigError("config error".to_string());
    assert!(!non_retriable.retriable());
}

#[test]
fn test_conversion_error_display() {
    let error = ConnectError::ConversionError("conversion error".to_string());
    assert_eq!(error.to_string(), "Conversion error: conversion error");
}

#[test]
fn test_connector_error_display() {
    let error = ConnectError::ConnectorError("connector error".to_string());
    assert_eq!(error.to_string(), "Connector error: connector error");
}

#[test]
fn test_io_error_retriable() {
    let error = ConnectError::IoError("io error".to_string());
    assert!(error.retriable());
}
