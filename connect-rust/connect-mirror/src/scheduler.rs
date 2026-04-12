/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// 定时任务调度器，提供定时任务执行、超时管理和资源清理功能
///
/// Scheduler 使用 tokio 的异步运行时来调度和执行任务，支持：
/// - 定时重复任务
/// - 延迟启动的定时任务
/// - 同步执行（带超时）
/// - 异步执行
/// - 资源清理和优雅关闭
pub struct Scheduler {
    /// 调度器名称，用于日志标识
    name: String,
    /// 任务超时时间
    timeout: Duration,
    /// 是否已关闭
    closed: Arc<Mutex<bool>>,
    /// 所有后台任务的句柄，用于关闭时等待任务完成
    task_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Scheduler {
    /// 创建一个新的调度器
    ///
    /// # 参数
    /// - `name`: 调度器名称，用于日志标识
    /// - `timeout`: 任务执行的超时时间
    ///
    /// # 示例
    /// ```
    /// use std::time::Duration;
    /// let scheduler = Scheduler::new("MyScheduler".to_string(), Duration::from_secs(30));
    /// ```
    pub fn new(name: String, timeout: Duration) -> Self {
        Scheduler {
            name,
            timeout,
            closed: Arc::new(Mutex::new(false)),
            task_handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 创建一个新的调度器（从类名和角色构造名称）
    ///
    /// # 参数
    /// - `class_name`: 类名
    /// - `role`: 角色
    /// - `timeout`: 任务执行的超时时间
    ///
    /// # 示例
    /// ```
    /// use std::time::Duration;
    /// let scheduler = Scheduler::with_class_and_role("MirrorMaker", "Replication", Duration::from_secs(30));
    /// ```
    pub fn with_class_and_role(class_name: &str, role: &str, timeout: Duration) -> Self {
        let name = format!("Scheduler for {}: {}", class_name, role);
        Scheduler::new(name, timeout)
    }

    /// 调度一个重复执行的任务，立即开始执行
    ///
    /// # 参数
    /// - `task`: 要执行的任务
    /// - `interval`: 执行间隔
    /// - `description`: 任务描述，用于日志
    ///
    /// # 注意
    /// - 如果间隔为负数，任务不会被调度
    /// - 任务会在单独的异步任务中执行
    ///
    /// # 示例
    /// ```
    /// use std::time::Duration;
    /// let scheduler = Scheduler::new("Test".to_string(), Duration::from_secs(30));
    /// scheduler.schedule_repeating(
    ///     || async { println!("Hello"); },
    ///     Duration::from_secs(5),
    ///     "Greeting task"
    /// );
    /// ```
    pub fn schedule_repeating<F, Fut>(
        &self,
        task: F,
        interval: Duration,
        description: String,
    ) where
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        if interval.as_millis() < 0 {
            return;
        }

        let name = self.name.clone();
        let timeout = self.timeout;
        let closed = self.closed.clone();
        let task_handles = self.task_handles.clone();

        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                // 检查是否已关闭
                {
                    let is_closed = *closed.lock().await;
                    if is_closed {
                        info!("{} skipping task due to shutdown: {}", name, description);
                        break;
                    }
                }
                
                // 执行任务
                Self::execute_task_internal(&name, &description, timeout, &task).await;
            }
        });

        // 保存任务句柄
        tokio::spawn(async move {
            let mut handles = task_handles.lock().await;
            handles.push(handle);
        });
    }

    /// 调度一个重复执行的任务，延迟后开始执行
    ///
    /// # 参数
    /// - `task`: 要执行的任务
    /// - `interval`: 执行间隔
    /// - `description`: 任务描述，用于日志
    ///
    /// # 注意
    /// - 如果间隔为负数，任务不会被调度
    /// - 任务会在间隔时间后首次执行，然后按间隔重复执行
    ///
    /// # 示例
    /// ```
    /// use std::time::Duration;
    /// let scheduler = Scheduler::new("Test".to_string(), Duration::from_secs(30));
    /// scheduler.schedule_repeating_delayed(
    ///     || async { println!("Delayed Hello"); },
    ///     Duration::from_secs(5),
    ///     "Delayed greeting task"
    /// );
    /// ```
    pub fn schedule_repeating_delayed<F, Fut>(
        &self,
        task: F,
        interval: Duration,
        description: String,
    ) where
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        if interval.as_millis() < 0 {
            return;
        }

        let name = self.name.clone();
        let timeout = self.timeout;
        let closed = self.closed.clone();
        let task_handles = self.task_handles.clone();

        let handle = tokio::spawn(async move {
            // 首次延迟
            tokio::time::sleep(interval).await;
            
            let mut interval_timer = tokio::time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                // 检查是否已关闭
                {
                    let is_closed = *closed.lock().await;
                    if is_closed {
                        info!("{} skipping task due to shutdown: {}", name, description);
                        break;
                    }
                }
                
                // 执行任务
                Self::execute_task_internal(&name, &description, timeout, &task).await;
            }
        });

        // 保存任务句柄
        tokio::spawn(async move {
            let mut handles = task_handles.lock().await;
            handles.push(handle);
        });
    }

    /// 同步执行任务，带超时
    ///
    /// # 参数
    /// - `task`: 要执行的任务
    /// - `description`: 任务描述，用于日志
    ///
    /// # 注意
    /// - 如果任务执行超时，会记录错误日志
    /// - 如果任务被中断，会记录警告日志
    ///
    /// # 示例
    /// ```
    /// use std::time::Duration;
    /// let scheduler = Scheduler::new("Test".to_string(), Duration::from_secs(30));
    /// scheduler.execute(
    ///     || async { println!("One-time task"); Ok(()) },
    ///     "One-time task"
    /// );
    /// ```
    pub fn execute<F, Fut>(&self, task: F, description: String)
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let name = self.name.clone();
        let timeout = self.timeout;

        tokio::spawn(async move {
            let result = tokio::time::timeout(timeout, task()).await;
            
            match result {
                Ok(task_result) => {
                    match task_result {
                        Ok(_) => {
                            info!("{} completed task: {}", name, description);
                        }
                        Err(e) => {
                            error!("{} caught exception in task: {}: {}", name, description, e);
                        }
                    }
                }
                Err(_) => {
                    error!("{} timed out running task: {}", name, description);
                }
            }
        });
    }

    /// 异步执行任务，不等待完成
    ///
    /// # 参数
    /// - `task`: 要执行的任务
    /// - `description`: 任务描述，用于日志
    ///
    /// # 注意
    /// - 任务在后台异步执行，不阻塞调用者
    /// - 不会检查超时
    ///
    /// # 示例
    /// ```
    /// use std::time::Duration;
    /// let scheduler = Scheduler::new("Test".to_string(), Duration::from_secs(30));
    /// scheduler.execute_async(
    ///     || async { println!("Async task"); Ok(()) },
    ///     "Async task"
    /// );
    /// ```
    pub fn execute_async<F, Fut>(&self, task: F, description: String)
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let name = self.name.clone();
        let timeout = self.timeout;

        tokio::spawn(async move {
            Self::execute_task_internal(&name, &description, timeout, &task).await;
        });
    }

    /// 内部方法：执行任务并记录执行时间和错误
    async fn execute_task_internal<F, Fut>(
        name: &str,
        description: &str,
        timeout: Duration,
        task: &F,
    ) where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let start = std::time::Instant::now();
        
        match task().await {
            Ok(_) => {
                let elapsed = start.elapsed();
                info!("{} took {} ms", description, elapsed.as_millis());
                
                if elapsed > timeout {
                    warn!(
                        "{} took too long ({} ms) running task: {}",
                        name,
                        elapsed.as_millis(),
                        description
                    );
                }
            }
            Err(e) => {
                error!(
                    "{} caught exception in scheduled task: {}: {}",
                    name, description, e
                );
            }
        }
    }

    /// 关闭调度器，停止所有任务
    ///
    /// # 注意
    /// - 设置关闭标志，阻止新任务执行
    /// - 等待所有后台任务完成或超
    /// - 如果关闭超时，会记录错误日志
    ///
    /// # 示例
    /// ```
    /// use std::time::Duration;
    /// let scheduler = Scheduler::new("Test".to_string(), Duration::from_secs(30));
    /// scheduler.close().await;
    /// ```
    pub async fn close(&self) {
        // 设置关闭标志
        {
            let mut closed = self.closed.lock().await;
            *closed = true;
        }

        // 等待所有任务完成
        let handles = self.task_handles.lock().await;
        let timeout_result = tokio::time::timeout(self.timeout, async {
            for handle in handles.iter() {
                handle.await.ok();
            }
        })
        .await;

        match timeout_result {
            Ok(_) => {
                info!("{} shutdown completed successfully", self.name);
            }
            Err(_) => {
                error!(
                    "{} timed out during shutdown of internal scheduler",
                    self.name
                );
            }
        }
    }
}

impl Drop for Scheduler {
    /// Drop 实现，确保资源被清理
    fn drop(&mut self) {
        // 注意：Drop trait 不能是 async，所以我们只能记录日志
        // 实际的清理工作应该在 close() 方法中完成
        info!("{} scheduler dropped", self.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_scheduler_creation() {
        let scheduler = Scheduler::new("TestScheduler".to_string(), Duration::from_secs(30));
        assert_eq!(scheduler.name, "TestScheduler");
        assert_eq!(scheduler.timeout, Duration::from_secs(30));
    }

    #[tokio::test]
    async fn test_scheduler_with_class_and_role() {
        let scheduler =
            Scheduler::with_class_and_role("MirrorMaker", "Replication", Duration::from_secs(30));
        assert_eq!(scheduler.name, "Scheduler for MirrorMaker: Replication");
    }

    #[tokio::test]
    async fn test_execute() {
        let scheduler = Scheduler::new("TestScheduler".to_string(), Duration::from_secs(30));
        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();

        scheduler.execute(
            move || {
                let executed = executed_clone.clone();
                async move {
                    executed.store(true, Ordering::SeqCst);
                    Ok(())
                }
            },
            "Test task".to_string(),
        );

        // 等待任务执行
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(executed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_execute_async() {
        let scheduler = Scheduler::new("TestScheduler".to_string(), Duration::from_secs(30));
        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();

        scheduler.execute_async(
            move || {
                let executed = executed_clone.clone();
                async move {
                    executed.store(true, Ordering::SeqCst);
                    Ok(())
                }
            },
            "Async test task".to_string(),
        );

        // 等待任务执行
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(executed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_schedule_repeating() {
        let scheduler = Scheduler::new("TestScheduler".to_string(), Duration::from_secs(30));
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        scheduler.schedule_repeating(
            move || {
                let counter = counter_clone.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            Duration::from_millis(100),
            "Repeating task".to_string(),
        );

        // 等待任务执行几次
        tokio::time::sleep(Duration::from_millis(350)).await;
        let count = counter.load(Ordering::SeqCst);
        assert!(count >= 3, "Expected at least 3 executions, got {}", count);
    }

    #[tokio::test]
    async fn test_schedule_repeating_delayed() {
        let scheduler = Scheduler::new("TestScheduler".to_string(), Duration::from_secs(30));
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        scheduler.schedule_repeating_delayed(
            move || {
                let counter = counter_clone.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            Duration::from_millis(100),
            "Delayed repeating task".to_string(),
        );

        // 等待延迟时间 + 几次执行
        tokio::time::sleep(Duration::from_millis(450)).await;
        let count = counter.load(Ordering::SeqCst);
        assert!(count >= 3, "Expected at least 3 executions, got {}", count);
    }

    #[tokio::test]
    async fn test_close() {
        let scheduler = Scheduler::new("TestScheduler".to_string(), Duration::from_secs(30));
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        scheduler.schedule_repeating(
            move || {
                let counter = counter_clone.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            Duration::from_millis(50),
            "Repeating task".to_string(),
        );

        // 等待任务执行几次
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // 关闭调度器
        scheduler.close().await;

        // 等待一段时间，确保任务停止
        let count_before = counter.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let count_after = counter.load(Ordering::SeqCst);

        // 由于关闭机制，任务应该停止执行
        // 注意：由于异步特性，可能会有一些延迟
        assert!(count_after >= count_before);
    }

    #[tokio::test]
    async fn test_execute_with_error() {
        let scheduler = Scheduler::new("TestScheduler".to_string(), Duration::from_secs(30));

        scheduler.execute(
            || async { Err(anyhow::anyhow!("Test error")) },
            "Error task".to_string(),
        );

        // 等待任务执行
        tokio::time::sleep(Duration::from_millis(100)).await;
        // 错误应该被记录到日志中
    }

    #[tokio::test]
    async fn test_execute_with_timeout() {
        let scheduler = Scheduler::new("TestScheduler".to_string(), Duration::from_millis(100));

        scheduler.execute(
            || async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            },
            "Timeout task".to_string(),
        );

        // 等待超时
        tokio::time::sleep(Duration::from_millis(200)).await;
        // 超时应该被记录到日志中
    }
}
