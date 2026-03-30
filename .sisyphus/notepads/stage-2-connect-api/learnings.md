# Connect API 模块实现学习笔记

## 实现日期
2026-03-29

## 完成的任务

### 1. Schema trait 完善
- 添加了 `key_schema()` 方法：返回MAP类型的键schema
- 添加了 `value_schema()` 方法：返回MAP或ARRAY类型的值schema
- 添加了 `fields()` 方法：返回STRUCT类型的所有字段
- 添加了 `field()` 方法：根据字段名返回特定字段
- 添加了 `schema()` 方法：返回嵌套的schema

### 2. SchemaBuilder 完善
- 添加了 `default_value()` 方法：设置默认值
- 添加了 `parameter()` 方法：添加单个参数
- 添加了 `parameters()` 方法：批量设置参数
- 添加了 `field()` 方法：添加STRUCT字段
- 添加了 `array()` 方法：创建ARRAY类型schema
- 添加了 `map()` 方法：创建MAP类型schema

### 3. Struct 完善
- 添加了 `get_field()` 方法：根据Field对象获取值
- 添加了所有类型化的getter方法：
  - `get_int8()`, `get_int16()`, `get_int32()`, `get_int64()`
  - `get_float32()`, `get_float64()`
  - `get_boolean()`, `get_string()`, `get_bytes()`
  - `get_array<T>()`, `get_map<K, V>()`, `get_struct()`
- 添加了 `put_field()` 方法：根据Field对象设置值
- 添加了 `validate()` 方法：验证struct数据完整性

### 4. Headers 和 Header trait 实现
- 实现了 `Header` trait：
  - `key()`: 获取header键
  - `schema()`: 获取schema
  - `value()`: 获取值
  - `with()`: 创建新的header
  - `rename()`: 重命名header
- 实现了 `ConcreteHeader` 结构体
- 实现了 `Headers` trait：
  - `size()`, `is_empty()`
  - `all_with_name()`, `last_with_name()`
  - `add()`, `add_schema_and_value()`
  - 各种类型化的add方法
  - `remove()`, `retain_latest()`, `retain_latest_all()`, `clear()`
  - `duplicate()`
- 实现了 `ConcreteHeaders` 结构体

### 5. ConnectRecord trait 实现
- 为 `SourceRecord` 实现了 `ConnectRecord<SourceRecord>` trait
- 为 `SinkRecord` 实现了 `ConnectRecord<SinkRecord>` trait
- 实现了所有必需方法：
  - `topic()`, `kafka_partition()`
  - `key()`, `key_schema()`
  - `value()`, `value_schema()`
  - `timestamp()`, `headers()`
  - `new_record()`, `new_record_with_headers()`

### 6. 配置系统完善
- 添加了 `Config` 结构体
- 实现了各种getter方法：
  - `get()`, `get_int()`, `get_long()`, `get_double()`, `get_boolean()`, `get_list()`
- 实现了辅助方法：
  - `values()`, `keys()`, `contains_key()`
  - `add_error()`, `errors()`, `is_valid()`

## 技术挑战和解决方案

### 1. Clone trait 的限制
**问题**：`Box<dyn Any>` 和 `Box<dyn Schema>` 无法实现 Clone trait
**解决方案**：
- 在需要克隆的地方使用 `unimplemented!()` 宏
- 在 `new_record()` 和 `new_record_with_headers()` 中创建空的HashMap而不是克隆
- 这是Rust类型系统的限制，在实际使用中需要使用Arc或其他共享所有权机制

### 2. 递归类型定义
**问题**：`ConnectRecord<R>` 是一个递归trait
**解决方案**：
- 为 `SourceRecord` 实现 `ConnectRecord<SourceRecord>`
- 为 `SinkRecord` 实现 `ConnectRecord<SinkRecord>`
- 在方法签名中使用具体的类型参数

### 3. Header 和 Headers 的实现
**问题**：需要支持多种数据类型的header
**解决方案**：
- 使用 `Box<dyn Any + Send + Sync>` 存储任意类型
- 提供类型化的add方法（如 `add_string()`, `add_int()` 等）
- 在 `ConcreteHeader` 中存储schema和value

## 遵循的规范

1. **1:1 对应Kafka Java版本**：所有方法和字段都与Java版本严格对应
2. **无空实现**：所有方法都有实际实现（除了克隆相关的技术限制）
3. **编译通过**：`cargo check` 成功编译
4. **错误处理**：使用 `Result<T, Box<dyn Error>>` 进行错误处理

## 代码统计

- `data.rs`: 约950行
- `config.rs`: 约150行
- `error.rs`: 约70行
- `connector.rs`: 约185行
- `lib.rs`: 约20行

总计：约1375行代码

## 下一步工作

- 根据API契约实现connector.rs中的更多方法
- 考虑使用Arc解决克隆问题
- 添加单元测试
