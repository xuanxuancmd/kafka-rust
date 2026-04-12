//! Test file for CompletableFuture
//!
//! This file contains unit tests for the CompletableFuture implementation.

use connect_runtime::completable_future::CompletableFuture;
use anyhow::anyhow;

#[tokio::test]
async fn test_completed_future() {
    let future = CompletableFuture::completed(42);
    let result = future.await.unwrap();
    assert_eq!(result, 42);
}

#[tokio::test]
async fn test_failed_future() {
    let future = CompletableFuture::<i32>::failed(anyhow!("test error"));
    let result = future.await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_manual_completion() {
    let future = CompletableFuture::<i32>::new();
    let handle = future.get_completion_handle();
    handle.complete_ok(42).await;
    let result = future.await.unwrap();
    assert_eq!(result, 42);
}

#[tokio::test]
async fn test_then_apply() {
    let future = CompletableFuture::completed(42);
    let result = future.then_apply(|x| x * 2).await.unwrap();
    assert_eq!(result, 84);
}

#[tokio::test]
async fn test_exceptionally() {
    let future = CompletableFuture::<i32>::failed(anyhow!("test error"));
    let result = future
        .exceptionally(|e| {
            assert_eq!(e.to_string(), "test error");
            0
        })
        .await
        .unwrap();
    assert_eq!(result, 0);
}

#[tokio::test]
async fn test_all_of() {
    let futures = vec![
        CompletableFuture::completed(1),
        CompletableFuture::completed(2),
        CompletableFuture::completed(3),
    ];
    let result = CompletableFuture::all_of(futures).await.unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_all_of_with_error() {
    let futures = vec![
        CompletableFuture::completed(1),
        CompletableFuture::<i32>::failed(anyhow!("error")),
        CompletableFuture::completed(3),
    ];
    let result = CompletableFuture::all_of(futures).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_chained_operations() {
    let future = CompletableFuture::completed(10);
    let result = future
        .then_apply(|x| x + 5)
        .then_apply(|x| x * 2)
        .await
        .unwrap();
    assert_eq!(result, 30);
}

#[tokio::test]
async fn test_completion_handle_clone() {
    let future = CompletableFuture::<i32>::new();
    let handle1 = future.get_completion_handle();
    let handle2 = handle1.clone();
    
    handle1.complete_ok(42).await;
    let result = future.await.unwrap();
    assert_eq!(result, 42);
}
