use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::oneshot;

pub enum FutureError<E> {
    Raw(E),
    Inner(&'static str),
}

#[derive(Clone)]
pub struct CompletableFuture<T, E> {
    inner: Arc<Inner<T, E>>,
}

struct Inner<T, E> {
    sender: Mutex<Option<oneshot::Sender<Result<T, FutureError<E>>>>>,
    receiver: Mutex<Option<oneshot::Receiver<Result<T, FutureError<E>>>>>,
    completed: AtomicBool,
}

impl<T, E> CompletableFuture<T, E> {
    pub fn new() -> Self {
        let (tx, rx) = oneshot::channel();

        Self {
            inner: Arc::new(Inner {
                sender: Mutex::new(Some(tx)),
                receiver: Mutex::new(Some(rx)),
                completed: AtomicBool::new(false),
            }),
        }
    }

    pub fn complete(&self, value: T) -> Result<(), ()> {
        if self.inner.completed.swap(true, Ordering::SeqCst) {
            return Err(());
        }

        let mut sender_guard = match self.inner.sender.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(()),
        };

        if let Some(sender) = sender_guard.take() {
            sender.send(Ok(value)).map_err(|_| ())
        } else {
            Err(())
        }
    }

    pub fn complete_exceptionally(&self, error: E) -> Result<(), ()> {
        if self.inner.completed.swap(true, Ordering::SeqCst) {
            return Err(());
        }

        let mut sender_guard = match self.inner.sender.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(()),
        };

        if let Some(sender) = sender_guard.take() {
            sender.send(Err(FutureError::Raw(error))).map_err(|_| ())
        } else {
            Err(())
        }
    }

    pub fn is_done(&self) -> bool {
        self.inner.completed.load(Ordering::Relaxed)
    }
}

impl<T, E> fmt::Debug for CompletableFuture<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletableFuture")
            .field("is_done", &self.is_done())
            .finish()
    }
}


impl<T, E> Future for CompletableFuture<T, E> {
    type Output = Result<T, FutureError<E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut receiver_guard = match self.inner.receiver.lock() {
            Ok(guard) => guard,
            Err(_) => {
                // 锁中毒
                return Poll::Ready(Err(FutureError::Inner("Future lock poisoned")));
            }
        };

        if let Some(ref mut rx) = *receiver_guard {
            // 轮询接收端
            match Pin::new(rx).poll(cx) {
                Poll::Ready(Ok(result)) => {
                    // 清空 receiver
                    receiver_guard.take();
                    Poll::Ready(result)
                }
                Poll::Ready(Err(_)) => {
                    // channel 关闭
                    receiver_guard.take();
                    Poll::Ready(Err(FutureError::Inner("Future was cancelled or dropped")))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            // receiver 已经被取走，说明已经完成
            // 这种情况不应该发生，因为完成时我们已经清空了 receiver
            Poll::Ready(Err(FutureError::Inner("Future already consumed")))
        }
    }
}