//! CompletableFuture
//!
//! 提供 CompletableFuture 结构体和 impl 块，支持 then_apply、all_of、exceptionally 等方法。
//! 使用 tokio::sync::oneshot 作为底层实现，提供类型安全的异步编程接口。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

/// CompletableFuture 是一个可以显式完成的 Future，提供与 Java CompletableFuture 相似的 API
///
/// 它支持链式调用、异常处理、组合多个 Future 等功能。
/// 底层使用 tokio::sync::oneshot 实现，确保线程安全和类型安全。
///
/// # 泛型参数
/// - `T`: Future 的成功返回值类型
///
/// # 约束
/// - `T: Send + 'static`: 确保类型可以在多线程环境中安全传递
///
/// # 示例
///
/// ```rust
/// use connect_runtime::completable_future::CompletableFuture;
///
/// #[tokio::main]
/// async fn main() {
///     // 创建并完成一个 Future
///     let future = CompletableFuture::completed(42);
///     let result = future.then_apply(|x| x * 2).await.unwrap();
///     assert_eq!(result, 84);
/// }
/// ```
pub struct CompletableFuture<T: Send + 'static> {
    /// 使用 oneshot::Receiver 来接收结果
    receiver: Option<oneshot::Receiver<Result<T, anyhow::Error>>>,
    /// 使用 Arc<Mutex> 包装 sender 以便在多个地方共享
    sender: Arc<Mutex<Option<oneshot::Sender<Result<T, anyhow::Error>>>>>,
}

impl<T: Send + 'static> CompletableFuture<T> {
    /// 创建一个新的未完成的 CompletableFuture
    ///
    /// 返回的 Future 需要通过 `complete` 方法显式完成。
    ///
    /// # 示例
    ///
    /// ```rust
    /// use connect_runtime::completable_future::CompletableFuture;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let future = CompletableFuture::<i32>::new();
    ///     // 在其他地方完成这个 Future
    ///     let handle = future.get_completion_handle();
    ///     handle.complete(Ok(42)).await;
    ///     let result = future.await.unwrap();
    ///     assert_eq!(result, 42);
    /// }
    /// ```
    pub fn new() -> Self {
        let (sender, receiver) = oneshot::channel();
        CompletableFuture {
            receiver: Some(receiver),
            sender: Arc::new(Mutex::new(Some(sender))),
        }
    }

    /// 创建一个已经成功完成的 CompletableFuture
    ///
    /// # 参数
    /// - `value`: 成功的返回值
    ///
    /// # 示例
    ///
    /// ```rust
    /// use connect_runtime::completable_future::CompletableFuture;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let future = CompletableFuture::completed(42);
    ///     let result = future.await.unwrap();
    ///     assert_eq!(result, 42);
    /// }
    /// ```
    pub fn completed(value: T) -> Self {
        let future = Self::new();
        let sender = future.sender.clone();
        tokio::spawn(async move {
            if let Some(s) = sender.lock().await.take() {
                let _ = s.send(Ok(value));
            }
        });
        future
    }

    /// 创建一个已经失败的 CompletableFuture
    ///
    /// # 参数
    /// - `error`: 错误信息
    ///
    /// # 示例
    ///
    /// ```rust
    /// use connect_runtime::completable_future::CompletableFuture;
    /// use anyhow::anyhow;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let future = CompletableFuture::<i32>::failed(anyhow!("something went wrong"));
    ///     let result = future.await;
    ///     assert!(result.is_err());
    /// }
    /// ```
    pub fn failed(error: anyhow::Error) -> Self {
        let future = Self::new();
        let sender = future.sender.clone();
        tokio::spawn(async move {
            if let Some(s) = sender.lock().await.take() {
                let _ = s.send(Err(error));
            }
        });
        future
    }

    /// 获取完成句柄，用于在异步上下文中完成 Future
    ///
    /// # 返回
    /// 返回一个 CompletionHandle，可以用来完成这个 Future
    ///
    /// # 示例
    ///
    /// ```rust
    /// use connect_runtime::completable_future::CompletableFuture;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let future = CompletableFuture::<i32>::new();
    ///     let handle = future.get_completion_handle();
    ///     handle.complete(Ok(42)).await;
    ///     let result = future.await.unwrap();
    ///     assert_eq!(result, 42);
    /// }
    /// ```
    pub fn get_completion_handle(&self) -> CompletionHandle<T> {
        CompletionHandle {
            sender: self.sender.clone(),
        }
    }

    /// 链式调用：当当前 Future 成功完成时，应用函数并返回新的 Future
    ///
    /// # 泛型参数
    /// - `U`: 新 Future 的返回值类型
    /// - `F`: 映射函数的类型
    ///
    /// # 参数
    /// - `f`: 映射函数，接收当前 Future 的成功值，返回新值
    ///
    /// # 返回
    /// 返回一个新的 CompletableFuture<U>
    ///
    /// # 示例
    ///
    /// ```rust
    /// use connect_runtime::completable_future::CompletableFuture;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let future = CompletableFuture::completed(42);
    ///     let result = future.then_apply(|x| x * 2).await.unwrap();
    ///     assert_eq!(result, 84);
    /// }
    /// ```
    pub fn then_apply<U, F>(self, f: F) -> CompletableFuture<U>
    where
        U: Send + 'static,
        F: FnOnce(T) -> U + Send + 'static,
    {
        let new_future = CompletableFuture::<U>::new();
        let new_sender = new_future.sender.clone();

        tokio::spawn(async move {
            let result = if let Some(receiver) = self.receiver {
                match receiver.await {
                    Ok(Ok(value)) => Ok(f(value)),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(anyhow::anyhow!("Future cancelled")),
                }
            } else {
                Err(anyhow::anyhow!("Future already consumed"))
            };

            if let Some(s) = new_sender.lock().await.take() {
                let _ = s.send(result);
            }
        });

        new_future
    }

    /// 异常处理：当当前 Future 失败时，应用恢复函数
    ///
    /// # 参数
    /// - `f`: 恢复函数，接收错误，返回新的成功值
    ///
    /// # 返回
    /// 返回一个新的 CompletableFuture<T>
    ///
    /// # 示例
    ///
    /// ```rust
    /// use connect_runtime::completable_future::CompletableFuture;
    /// use anyhow::anyhow;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let future = CompletableFuture::<i32>::failed(anyhow!("error"));
    ///     let result = future.exceptionally(|e| {
    ///         eprintln!("Error: {}", e);
    ///         0
    ///     }).await.unwrap();
    ///     assert_eq!(result, 0);
    /// }
    /// ```
    pub fn exceptionally<F>(self, f: F) -> CompletableFuture<T>
    where
        F: FnOnce(anyhow::Error) -> T + Send + 'static,
    {
        let new_future = CompletableFuture::<T>::new();
        let new_sender = new_future.sender.clone();

        tokio::spawn(async move {
            let result = if let Some(receiver) = self.receiver {
                match receiver.await {
                    Ok(Ok(value)) => Ok(value),
                    Ok(Err(e)) => Ok(f(e)),
                    Err(_) => Err(anyhow::anyhow!("Future cancelled")),
                }
            } else {
                Err(anyhow::anyhow!("Future already consumed"))
            };

            if let Some(s) = new_sender.lock().await.take() {
                let _ = s.send(result);
            }
        });

        new_future
    }

    /// 组合多个 Future，等待所有 Future 完成
    ///
    /// # 泛型参数
    /// - `I`: 迭代器类型
    ///
    /// # 参数
    /// - `futures`: Future 的迭代器
    ///
    /// # 返回
    /// 返回一个新的 CompletableFuture<Vec<T>>，包含所有 Future 的结果
    ///
    /// # 示例
    ///
    /// ```rust
    ///    use connect_runtime::completable_future::CompletableFuture;
    ///
    ///    #[tokio::main]
    ///    async fn main() {
    ///        let futures = vec![
    ///            CompletableFuture::completed(1),
    ///            CompletableFuture::completed(2),
    ///            CompletableFuture::completed(3),
    ///        ];
    ///        let result = CompletableFuture::all_of(futures).await.unwrap();
    ///        assert_eq!(result, vec![1, 2, 3]);
    ///    }
    /// ```
    pub fn all_of<I>(futures: I) -> CompletableFuture<Vec<T>>
    where
        I: IntoIterator<Item = CompletableFuture<T>>,
        I::IntoIter: Send + 'static,
    {
        let new_future = CompletableFuture::<Vec<T>>::new();
        let new_sender = new_future.sender.clone();

        tokio::spawn(async move {
            let mut results = Vec::new();
            let mut has_error = false;
            let mut error = None;

            for future in futures {
                if let Some(receiver) = future.receiver {
                    match receiver.await {
                        Ok(Ok(value)) => results.push(value),
                        Ok(Err(e)) => {
                            has_error = true;
                            error = Some(e);
                            break;
                        }
                        Err(_) => {
                            has_error = true;
                            error = Some(anyhow::anyhow!("Future cancelled"));
                            break;
                        }
                    }
                }
            }

            let result = if has_error {
                Err(error.unwrap())
            } else {
                Ok(results)
            };

            if let Some(s) = new_sender.lock().await.take() {
                let _ = s.send(result);
            }
        });

        new_future
    }
}

impl<T: Send + 'static> Future for CompletableFuture<T> {
    type Output = Result<T, anyhow::Error>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if let Some(ref mut receiver) = self.receiver {
            match Pin::new(receiver).poll(cx) {
                std::task::Poll::Ready(Ok(result)) => {
                    self.receiver = None;
                    std::task::Poll::Ready(result)
                }
                std::task::Poll::Ready(Err(_)) => {
                    self.receiver = None;
                    std::task::Poll::Ready(Err(anyhow::anyhow!("Future cancelled")))
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        } else {
            std::task::Poll::Ready(Err(anyhow::anyhow!("Future already consumed")))
        }
    }
}

/// CompletionHandle 用于完成 CompletableFuture
///
/// 这个句柄可以在不同的上下文中使用，完成对应的 Future。
pub struct CompletionHandle<T: Send + 'static> {
    sender: Arc<Mutex<Option<oneshot::Sender<Result<T, anyhow::Error>>>>>,
}

impl<T: Send + 'static> CompletionHandle<T> {
    /// 完成 Future，发送结果
    ///
    /// # 参数
    /// - `result`: 要发送的结果，可以是 Ok(value) 或 Err(error)
    ///
    /// # 示例
    ///
    /// ```rust
    /// use connect_runtime::completable_future::CompletableFuture;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let future = CompletableFuture::<i32>::new();
    ///     let handle = future.get_completion_handle();
    ///     handle.complete(Ok(42)).await;
    ///     let result = future.await.unwrap();
    ///     assert_eq!(result, 42);
    /// }
    /// ```
    pub async fn complete(&self, result: Result<T, anyhow::Error>) {
        if let Some(sender) = self.sender.lock().await.take() {
            let _ = sender.send(result);
        }
    }

    /// 完成 Future，发送成功值
    ///
    /// # 参数
    /// - `value`: 成功的返回值
    ///
    /// # 示例
    ///
    /// ```rust
    /// use connect_runtime::completable_future::CompletableFuture;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let future = CompletableFuture::<i32>::new();
    ///     let handle = future.get_completion_handle();
    ///     handle.complete_ok(42).await;
    ///     let result = future.await.unwrap();
    ///     assert_eq!(result, 42);
    /// }
    /// ```
    pub async fn complete_ok(&self, value: T) {
        self.complete(Ok(value)).await;
    }

    /// 完成 Future，发送错误
    ///
    /// # 参数
    /// - `error`: 错误信息
    ///
    /// # 示例
    ///
    /// ```rust
    /// use connect_runtime::completable_future::CompletableFuture;
    /// use anyhow::anyhow;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let future = CompletableFuture::<i32>::new();
    ///     let handle = future.get_completion_handle();
    ///     handle.complete_err(anyhow!("error")).await;
    ///     let result = future.await;
    ///     assert!(result.is_err());
    /// }
    /// ```
    pub async fn complete_err(&self, error: anyhow::Error) {
        self.complete(Err(error)).await;
    }
}

impl<T: Send + 'static> Clone for CompletionHandle<T> {
    fn clone(&self) -> Self {
        CompletionHandle {
            sender: self.sender.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
