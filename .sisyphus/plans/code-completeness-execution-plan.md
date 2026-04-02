# Kafka Connect Rust 代码完整性修复执行计划

生成时间: 2026-04-01
目标: 确保代码完整性和正确性，相比kafka代码量只多不少

---

## 一、总体策略

### 1.1 执行原则

1. **并行优先**: 所有独立的修复任务可以并行执行
2. **小步迭代**: 每个任务完成后立即编译和测试
3. **质量优先**: 代码行数必须≥Java版本，功能必须完整
4. **测试驱动**: 每个修复必须包含对应的单元测试

### 1.2 依赖关系

```
connect-api (已完成)
├── connect-transforms (需要补充2个转换)
│   └── 依赖: connect-api
├── connect-runtime-core (需要补充3个存储实现)
│   ├── 依赖: connect-api, connect-transforms
│   └── connect-runtime-distributed (已完成)
└── connect-mirror (需要增强实现)
    ├── 依赖: connect-api, connect-runtime-core
    └── connect-mirror-client (已完成)
```

---

## 二、connect-transforms 模块修复计划

### 2.1 任务1: 实现RegexRouter转换

**优先级**: P0
**并行组**: Wave 1
**预计时间**: 30分钟

#### 2.1.1 实现要求

**文件**: `connect-transforms/src/regex_router.rs`

**功能**:
- 基于正则表达式修改topic名称
- 支持Key和Value两种变体
- 支持正则捕获组替换

**配置参数**:
```rust
pub struct RegexRouter<R: ConnectRecord<R>> {
    pattern: Regex,           // 正则表达式
    replacement: String,      // 替换字符串
    source: String,           // 源字段（可选）
    _phantom: PhantomData<R>,
}
```

**Public方法**:
- `new()` - 创建新实例
- `configure()` - 配置正则和替换字符串
- `apply()` - 应用正则替换到topic
- `config()` - 返回配置定义
- `close()` - 清理资源

**代码行数目标**: ≥200行（Java版本约250行）

#### 2.1.2 单元测试

**文件**: `connect-transforms/tests/regex_router_test.rs`

**测试用例**:
```rust
#[test]
fn test_regex_router_basic() {
    // 测试基本正则替换
}

#[test]
fn test_regex_router_capture_groups() {
    // 测试捕获组替换
}

#[test]
fn test_regex_router_invalid_regex() {
    // 测试无效正则表达式
}

#[test]
fn test_regex_router_key_variant() {
    // 测试Key变体
}

#[test]
fn test_regex_router_value_variant() {
    // 测试Value变体
}
```

**测试数量目标**: ≥10个测试用例

#### 2.1.3 验证步骤

1. 编译: `cargo check -p connect-transforms`
2. 单元测试: `cargo test -p connect-transforms regex_router_test`
3. 集成测试: `cargo test -p connect-transforms`
4. 代码行数检查: `wc -l connect-transforms/src/regex_router.rs ≥ 200`

---

### 2.2 任务2: 实现SetSchemaMetadata转换

**优先级**: P0
**并行组**: Wave 1
**预计时间**: 20分钟

#### 2.2.1 实现要求

**文件**: `connect-transforms/src/set_schema_metadata.rs`

**功能**:
- 修改schema的名称
- 修改schema的版本
- 支持Key和Value两种变体

**配置参数**:
```rust
pub struct SetSchemaMetadata<R: ConnectRecord<R>> {
    schema_name: Option<String>,  // 新的schema名称
    schema_version: Option<i32>, // 新的schema版本
    _phantom: PhantomData<R>,
}
```

**Public方法**:
- `new()` - 创建新实例
- `configure()` - 配置schema名称和版本
- `apply()` - 应用schema元数据修改
- `config()` - 返回配置定义
- `close()` - 清理资源

**代码行数目标**: ≥150行（Java版本约180行）

#### 2.2.2 单元测试

**文件**: `connect-transforms/tests/set_schema_metadata_test.rs`

**测试用例**:
```rust
#[test]
fn test_set_schema_name() {
    // 测试设置schema名称
}

#[test]
fn test_set_schema_version() {
    // 测试设置schema版本
}

#[test]
fn test_set_schema_both() {
    // 测试同时设置名称和版本
}

#[test]
fn test_set_schema_none() {
    // 测试不修改schema
}

#[test]
fn test_set_schema_key_variant() {
    // 测试Key变体
}

#[test]
fn test_set_schema_value_variant() {
    // 测试Value变体
}
```

**测试数量目标**: ≥8个测试用例

#### 2.2.3 验证步骤

1. 编译: `cargo check -p connect-transforms`
2. 单元测试: `cargo test -p connect-transforms set_schema_metadata_test`
3. 集成测试: `cargo test -p connect-transforms`
4. 代码行数检查: `wc -l connect-transforms/src/set_schema_metadata.rs ≥ 150`

---

## 三、connect-runtime-core 模块修复计划

### 3.1 任务3: 实现KafkaConfigBackingStore

**优先级**: P0
**并行组**: Wave 2
**预计时间**: 45分钟
**依赖**: connect-api完成

#### 3.1.1 实现要求

**文件**: `connect-runtime-core/src/kafka_config_store.rs`

**功能**:
- 使用Kafka topic存储配置数据
- 支持读写操作
- 支持版本控制
- 支持序列化和反序列化

**配置参数**:
```rust
pub struct KafkaConfigBackingStore {
    topic: String,                    // Kafka topic名称
    bootstrap_servers: String,          // Kafka集群地址
    producer: Arc<Mutex<KafkaProducer>>, // Kafka生产者
    consumer: Arc<Mutex<KafkaConsumer>>, // Kafka消费者
    timeout: Duration,                 // 操作超时
}
```

**Public方法**:
- `new()` - 创建新实例
- `start()` - 启动存储
- `stop()` - 停止存储
- `put()` - 存储配置
- `get()` - 读取配置
- `delete()` - 删除配置
- `list()` - 列出所有配置
- `close()` - 关闭连接

**代码行数目标**: ≥400行（Java版本约450行）

#### 3.1.2 单元测试

**文件**: `connect-runtime-core/tests/kafka_config_store_test.rs`

**测试用例**:
```rust
#[test]
fn test_kafka_config_store_put_and_get() {
    // 测试存储和读取配置
}

#[test]
fn test_kafka_config_store_delete() {
    // 测试删除配置
}

#[test]
fn test_kafka_config_store_list() {
    // 测试列出所有配置
}

#[test]
fn test_kafka_config_store_version_control() {
    // 测试版本控制
}

#[test]
fn test_kafka_config_store_serialization() {
    // 测试序列化和反序列化
}

#[test]
fn test_kafka_config_store_timeout() {
    // 测试超时处理
}

#[test]
fn test_kafka_config_store_concurrent_access() {
    // 测试并发访问
}
```

**测试数量目标**: ≥15个测试用例

#### 3.1.3 验证步骤

1. 编译: `cargo check -p connect-runtime-core`
2. 单元测试: `cargo test -p connect-runtime-core kafka_config_store_test`
3. 集成测试: `cargo test -p connect-runtime-core`
4. 代码行数检查: `wc -l connect-runtime-core/src/kafka_config_store.rs ≥ 400`

---

### 3.2 任务4: 实现KafkaOffsetBackingStore

**优先级**: P0
**并行组**: Wave 2
**预计时间**: 45分钟
**依赖**: connect-api完成

#### 3.2.1 实现要求

**文件**: `connect-runtime-core/src/kafka_offset_store.rs`

**功能**:
- 使用Kafka topic存储offset数据
- 支持分区级别的offset管理
- 支持批量提交
- 支持offset读取

**配置参数**:
```rust
pub struct KafkaOffsetBackingStore {
    topic: String,                    // Kafka topic名称
    bootstrap_servers: String,          // Kafka集群地址
    producer: Arc<Mutex<KafkaProducer>>, // Kafka生产者
    consumer: Arc<Mutex<KafkaConsumer>>, // Kafka消费者
    timeout: Duration,                 // 操作超时
}
```

**Public方法**:
- `new()` - 创建新实例
- `start()` - 启动存储
- `stop()` - 停止存储
- `put()` - 存储offset
- `get()` - 读取offset
- `put_all()` - 批量存储offset
- `get_all()` - 批量读取offset
- `close()` - 关闭连接

**代码行数目标**: ≥450行（Java版本约500行）

#### 3.2.2 单元测试

**文件**: `connect-runtime-core/tests/kafka_offset_store_test.rs`

**测试用例**:
```rust
#[test]
fn test_kafka_offset_store_put_and_get() {
    // 测试存储和读取offset
}

#[test]
fn test_kafka_offset_store_put_all_and_get_all() {
    // 测试批量存储和读取offset
}

#[test]
fn test_kafka_offset_store_partition_level() {
    // 测试分区级别的offset管理
}

#[test]
fn test_kafka_offset_store_serialization() {
    // 测试序列化和反序列化
}

#[test]
fn test_kafka_offset_store_concurrent_access() {
    // 测试并发访问
}

#[test]
fn test_kafka_offset_store_offset_update() {
    // 测试offset更新
}

#[test]
fn test_kafka_offset_store_empty_offset() {
    // 测试空offset处理
}
```

**测试数量目标**: ≥15个测试用例

#### 3.2.3 验证步骤

1. 编译: `cargo check -p connect-runtime-core`
2. 单元测试: `cargo test -p connect-runtime-core kafka_offset_store_test`
3. 集成测试: `cargo test -p connect-runtime-core`
4. 代码行数检查: `wc -l connect-runtime-core/src/kafka_offset_store.rs ≥ 450`

---

### 3.3 任务5: 实现KafkaStatusBackingStore

**优先级**: P0
**并行组**: Wave 2
**预计时间**: 40分钟
**依赖**: connect-api完成

#### 3.3.1 实现要求

**文件**: `connect-runtime-core/src/kafka_status_store.rs`

**功能**:
- 使用Kafka topic存储状态数据
- 支持连接器状态跟踪
- 支持任务状态跟踪
- 支持状态更新和查询

**配置参数**:
```rust
pub struct KafkaStatusBackingStore {
    topic: String,                    // Kafka topic名称
    bootstrap_servers: String,          // Kafka集群地址
    producer: Arc<Mutex<KafkaProducer>>, // Kafka生产者
    consumer: Arc<Mutex<KafkaConsumer>>, // Kafka消费者
    timeout: Duration,                 // 操作超时
}
```

**Public方法**:
- `new()` - 创建新实例
- `start()` - 启动存储
- `stop()` - 停止存储
- `put()` - 存储状态
- `get()` - 读取状态
- `delete()` - 删除状态
- `list()` - 列出所有状态
- `close()` - 关闭连接

**代码行数目标**: ≥400行（Java版本约450行）

#### 3.3.2 单元测试

**文件**: `connect-runtime-core/tests/kafka_status_store_test.rs`

**测试用例**:
```rust
#[test]
fn test_kafka_status_store_put_and_get() {
    // 测试存储和读取状态
}

#[test]
fn test_kafka_status_store_delete() {
    // 测试删除状态
}

#[test]
fn test_kafka_status_store_list() {
    // 测试列出所有状态
}

#[test]
fn test_kafka_status_store_connector_status() {
    // 测试连接器状态跟踪
}

#[test]
fn test_kafka_status_store_task_status() {
    // 测试任务状态跟踪
}

#[test]
fn test_kafka_status_store_serialization() {
    // 测试序列化和和反序列化
}

#[test]
fn test_kafka_status_store_concurrent_access() {
    // 测试并发访问
}
```

**测试数量目标**: ≥12个测试用例

#### 3.3.3 验证步骤

1. 编译: `cargo check -p connect-runtime-core`
2. 单元测试: `cargo test -p connect-runtime-core kafka_status_store_test`
3. 集成测试: `cargo test -p connect-runtime-core`
4. 代码行数检查: `wc -l connect-runtime-core/src/kafka_status_store.rs ≥ 400`

---

## 四、connect-mirror 模块增强计划

### 4.1 任务6: 增强MirrorSourceConnector实现

**优先级**: P0
**并行组**: Wave 3
**预计时间**: 60分钟
**依赖**: connect-runtime-core完成

#### 4.1.1 增强要求

**文件**: `connect-mirror/src/source_connector_impl.rs`

**需要补充的功能**:
1. **Exactly-once支持**
   - 实现事务边界定义
   - 实现offset alter操作
   - 实现幂等性保证

2. **高级复制功能**
   - 支持主题配置复制
   - 支持消费者组复制
   - 支持ACL复制

3. **错误处理**
   - 增强错误恢复机制
   - 实现重试逻辑
   - 实现错误报告

4. **性能优化**
   - 实现批量读取
   - 实现并行复制
   - 实现压缩传输

**代码行数目标**: ≥600行（当前271行，Java版本742行）

#### 4.1.2 单元测试

**文件**: `connect-mirror/tests/source_connector_test.rs`

**需要补充的测试用例**:
```rust
#[test]
fn test_mirror_source_connector_exactly_once() {
    // 测试Exactly-once支持
}

#[test]
fn test_mirror_source_connector_alter_offsets() {
    // 测试offset alter操作
}

#[test]
fn test_mirror_source_connector_topic_config_copy() {
    // 测试主题配置复制
}

#[test]
fn test_mirror_source_connector_consumer_group_copy() {
    // 测试消费者组复制
}

#[test]
fn test_mirror_source_connector_acl_copy() {
    // 测试ACL复制
}

#[test]
fn test_mirror_source_connector_error_recovery() {
    // 测试错误恢复
}

#[test]
fn test_mirror_source_connector_retry_logic() {
    // 测试重试逻辑
}

#[test]
fn test_mirror_source_connector_batch_read() {
    // 测试批量读取
}

#[test]
fn test_mirror_source_connector_parallel_copy() {
    // 测试并行复制
}
```

**测试数量目标**: ≥20个测试用例

#### 4.1.3 验证步骤

1. 编译: `cargo check -p connect-mirror`
2. 单元测试: `cargo test -p connect-mirror source_connector_test`
3. 集成测试: `cargo test -p connect-mirror`
4. 代码行数检查: `wc -l connect-mirror/src/source_connector_impl.rs ≥ 600`

---

### 4.2 任务7: 增强MirrorMaker2实现

**优先级**: P0
**并行组**: Wave 3
**预计时间**: 45分钟
**依赖**: connect-mirror-client完成

#### 4.2.1 增强要求

**文件**: `connect-mirror/src/maker_impl.rs`

**需要补充的功能**:
1. **集群管理**
   - 实现多集群拓扑支持
   - 实现集群健康检查
   - 实现集群故障转移

2. **复制策略**
   - 实现动态策略切换
   - 实现策略验证
   - 实现策略监控

3. **监控和指标**
   - 实现复制延迟监控
   - 实现数据一致性检查
   - 实现性能指标收集

4. **配置管理**
   - 实现热配置更新
   - 实现配置验证
   - 实现配置版本控制

**代码行数目标**: ≥500行（当前327行，Java版本约500行）

#### 4.2.2 单元测试

**文件**: `connect-mirror/tests/maker_test.rs`

**需要补充的测试用例**:
```rust
#[test]
fn test_mirror_maker_multi_cluster_topology() {
    // 测试多集群拓扑
}

#[test]
fn test_mirror_maker_cluster_health_check() {
    // 测试集群健康检查
}

#[test]
fn test_mirror_maker_cluster_failover() {
    // 测试集群故障转移
}

#[test]
fn test_mirror_maker_dynamic_policy_switch() {
    // 测试动态策略切换
}

#[test]
fn test_mirror_maker_policy_validation() {
    // 测试策略验证
}

#[test]
fn test_mirror_maker_replication_latency_monitoring() {
    // 测试复制延迟监控
}

#[test]
fn test_mirror_maker_data_consistency_check() {
    // 测试数据一致性检查
}

#[test]
fn test_mirror_maker_hot_config_update() {
    // 测试热配置更新
}

#[test]
fn test_mirror_maker_config_validation() {
    // 测试配置验证
}
```

**测试数量目标**: ≥18个测试用例

#### 4.2.3 验证步骤

1. 编译: `cargo check -p connect-mirror`
2. 单元测试: `cargo test -p connect-mirror maker_test`
3. 集成测试: `cargo test -p connect-mirror`
4. 代码行数检查: `wc -l connect-mirror/src/maker_impl.rs ≥ 500`

---

## 五、并行执行策略

### 5.1 Wave划分

```
Wave 1 (可并行执行):
├── 任务1: 实现RegexRouter转换
└── 任务2: 实现SetSchemaMetadata转换

Wave 2 (依赖Wave 1完成):
├── 任务3: 实现KafkaConfigBackingStore
├── 任务4: 实现KafkaOffsetBackingStore
└── 任务5: 实现KafkaStatusBackingStore

Wave 3 (依赖Wave 2完成):
├── 任务6: 增强MirrorSourceConnector实现
└── 任务7: 增强MirrorMaker2实现

Wave 4 (最终验证):
├── 全量编译检查
├── 全量测试运行
└── 代码完整性验证
```

### 5.2 并行执行命令

#### Wave 1
```bash
# 并行执行任务1和任务2
cargo run --bin task-1 &
cargo run --bin task-2 &
wait
```

#### Wave 2
```bash
# 并行执行任务3、任务4和任务5
cargo run --bin task-3 &
cargo run --bin task-4 &
cargo run --bin task-5 &
wait
```

#### Wave 3
```bash
# 并行执行任务6和任务7
cargo run --bin task-6 &
cargo run --bin task-7 &
wait
```

#### Wave 4
```bash
# 全量验证
cargo check --workspace
cargo test --workspace
```

---

## 六、验证和验收标准

### 6.1 编译验证

```bash
# 所有模块必须编译通过
cargo check --workspace

# 输出: Finished `dev` profile
```

### 6.2 测试验证

```bash
# 所有测试必须通过
cargo test --workspace

# 输出: test result: ok. N passed; 0 failed
```

### 6.3 代码行数验证

```bash
# connect-transforms: ≥1,251行 (当前1,051行)
wc -l connect-transforms/src/*.rs

# connect-runtime-core: ≥5,380行 (当前4,980行)
wc -l connect-runtime-core/src/*.rs

# connect-mirror: ≥2,084行 (当前2,084行)
wc -l connect-mirror/src/*.rs
```

### 6.4 功能完整性验证

| 模块 | Java功能数 | Rust功能数 | 覆盖率 |
|------|-----------|-----------|--------|
| connect-transforms | 17 | 17 | 100% |
| connect-runtime-core | 100+ | 100+ | 100% |
| connect-mirror | 34 | 34 | 100% |

### 6.5 AGENTS.md符合性验证

| 规约 | 符合度 |
|------|--------|
| 规约1: 1:1对应 | 100% |
| 规约2: 小步迭代 | 100% |
| 规约3: 无空实现 | 100% |
| 规约4: 二次检查 | 100% |
| 规约5: 阻塞问题处理 | 100% |

---

## 七、执行时间估算

| Wave | 任务数 | 预计时间 | 累计时间 |
|------|-------|---------|---------|
| Wave 1 | 2 | 30分钟 | 30分钟 |
| Wave 2 | 3 | 45分钟 | 75分钟 |
| Wave 3 | 2 | 60分钟 | 135分钟 |
| Wave 4 | 1 | 15分钟 | 150分钟 |

**总预计时间**: 2.5小时（并行执行）

---

## 八、执行检查清单

### 8.1 每个任务完成后

- [ ] 编译通过: `cargo check -p <module>`
- [ ] 单元测试通过: `cargo test -p <module> <test_file>`
- [ ] 代码行数达标: `wc -l <file> ≥ <target_lines>`
- [ ] 无TODO标记: `grep -r "TODO" <module>` (应为0)
- [ ] 无编译警告: `cargo check -p <module> 2>&1 | grep "warning"` (应为0)

### 8.2 每个Wave完成后

- [ ] 所有任务编译通过
- [ ] 所有任务测试通过
- [ ] 代码行数达标
- [ ] 无TODO标记
- [ ] 无编译警告

### 8.3 最终验收

- [ ] 全量编译通过: `cargo check --workspace`
- [ ] 全量测试通过: `cargo test --workspace`
- [ ] 代码行数达标（所有模块）
- [ ] 功能完整性100%
- [ ] AGENTS.md符合性100%
- [ ] 测试覆盖率≥80%

---

## 九、成功标准

### 9.1 代码完整性

1. ✅ 所有Java功能都有对应的Rust实现
2. ✅ 代码行数≥Java版本
3. ✅ 无TODO标记
4. ✅ 无空实现

### 9.2 代码正确性

1. ✅ 所有模块编译通过
2. ✅ 所有测试通过
3. ✅ 无编译警告
4. ✅ 无运行时错误

### 9.3 测试覆盖率

1. ✅ connect-transforms: ≥80%
2. ✅ connect-runtime-core: ≥80%
3. ✅ connect-mirror: ≥80%

### 9.4 AGENTS.md符合性

1. ✅ 规约1: 100%符合
2. ✅ 规约2: 100%符合
3. ✅ 规约3: 100%符合
4. ✅ 规约4: 100%符合
5. ✅ 规约5: 100%符合

---

## 十、回滚和恢复策略

### 10.1 任务失败处理

如果某个任务失败：

1. **保存失败信息**
   ```bash
   echo "Task X failed at $(date)" >> .sisyphus/failure-log.md
   ```

2. **回滚代码**
   ```bash
   git checkout -- <modified_files>
   ```

3. **记录问题**
   ```bash
   echo "Task X failed: <error_message>" >> .sisyphus/issues.md
   ```

4. **继续下一个任务**
   - 如果任务是独立的，可以继续执行其他任务
   - 如果任务有依赖，需要等待依赖任务完成

### 10.2 部分完成处理

如果只能完成部分任务：

1. **标记完成状态**
   ```bash
   echo "Wave X completed: [tasks]" >> .sisyphus/progress.md
   ```

2. **保存当前进度**
   ```bash
   git add -A
   git commit -m "Partial completion: Wave X"
   ```

3. **继续执行**
   - 根据依赖关系继续执行可执行的任务
   - 跳过失败的任务

---

## 十一、总结

本执行计划提供了完整的代码修复和验证策略，确保：

1. **代码完整性**: 相比kafka代码量只多不少
2. **代码正确性**: 经过编译和UT运行验证
3. **并行执行**: 最大化开发效率
4. **质量保证**: 每个任务都有完整的测试覆盖

按照此计划执行，可以确保Kafka Connect Rust实现的完整性和正确性。
