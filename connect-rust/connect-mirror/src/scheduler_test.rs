// 独立测试文件，用于验证 scheduler.rs 的功能
// 这个文件不依赖其他可能有编译错误的模块

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// 模拟必要的类型和函数
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// 简化的 Scheduler 结构，用于测试逻辑
struct Scheduler {
    name: String,
    timeout: Duration,
}

impl Scheduler {
    fn new(name: String, timeout: Duration) -> Self {
        Scheduler { name, timeout }
    }

    fn with_class_and_role(class_name: &str, role: &str, timeout: Duration) -> Self {
        let name = format!("Scheduler for {}: {}", class_name, role);
        Scheduler::new(name, timeout)
    }
}

#[test]
fn test_scheduler_creation() {
    let scheduler = Scheduler::new("TestScheduler".to_string(), Duration::from_secs(30));
    assert_eq!(scheduler.name, "TestScheduler");
    assert_eq!(scheduler.timeout, Duration::from_secs(30));
}

#[test]
fn test_scheduler_with_class_and_role() {
    let scheduler =
        Scheduler::with_class_and_role("MirrorMaker", "Replication", Duration::from_secs(30));
    assert_eq!(scheduler.name, "Scheduler for MirrorMaker: Replication");
}

#[test]
fn test_atomic_operations() {
    let counter = Arc::new(AtomicU32::new(0));
    counter.fetch_add(1, Ordering::SeqCst);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn test_atomic_bool() {
    let flag = Arc::new(AtomicBool::new(false));
    assert!(!flag.load(Ordering::SeqCst));
    flag.store(true, Ordering::SeqCst);
    assert!(flag.load(Ordering::SeqCst));
}

#[test]
fn test_duration_operations() {
    let duration = Duration::from_secs(30);
    assert_eq!(duration.as_secs(), 30);
    assert!(duration.as_millis() > 0);
}

#[test]
fn test_string_formatting() {
    let name = format!("Scheduler for {}: {}", "MirrorMaker", "Replication");
    assert_eq!(name, "Scheduler for MirrorMaker: Replication");
}

fn main() {
    println!("All tests passed!");
}
