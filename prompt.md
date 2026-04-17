```
# 第 1 步：架构分析
opencode "@oracle 分析当前目录下kafka的connect模块的架构，制定connect模块迁移到rust的计划，划分迁移阶段。迁移计划体现“如何保证规格、细节不遗漏，且遵从AGENTS.md的设计与编码要求”，过程中可使用MCP deepwiki工具咨询kakfa的任何问题。"

# 第 2 步：依赖分析（后台任务）
opencode "ulw: @explore 分析所有模块依赖，生成依赖图，识别关键路径模块；@librarian 调研 rust 迁移最佳实践（尤其是原kafka实现过程中用到的java\scala语言特性和组件在rust中如何对等替代）；@oracle 根据依赖分析和调研的结果，刷新迁移计划。注意：1.分析过程中多种方案或其他疑问，应该向我提问。2.模块依赖kafka-client，为方便端到端测试，在严格遵守client的transforms契约下，你可以提供client的mock代码+预制数据。"

# 第 3 步：分模块迁移
opencode "ulw: @backend-engineer 遵从“迁移计划”，将当前目录下kafka的connect模块迁移到rust，将所有迁移和mock的代码都写到当前根目录的connect-rust下，使用 LSP 工具确保类型安全。迁移过程，应该小步迭代，保证每个模块和子任务迁移后都应该急时通过cargo build和cargo test。"

# 第 4 步：验证与测试
opencode "@explore 最终再一次，针对完整项目，使用 LSP 工具检查类型错误，通过cargo build命令保证编译通过，运行测试套件，生成迁移报告"
```


```
我正在完成kafka connect目录用rust语言重写到connect-rust目录下（除connect-file和connect-json外所有均迁移，含测试），当前已经完成了部分迁移，但完整性和正确性较差，且不满足下面的约束。请重新分析比对connect-rust/connect-api目录和connect/api目录下的详细差异，和1:1代码翻译方案，保证迁移的一致性、完整性和正确性。约束：1.一致性:迁移过程要严格保证目录、类名、函数名一致，如果已完成代码不符合要求请整改。 2. 优先分析connect/api中涉及的Java到rust的语言翻译难点，含对三方的依赖，分析结果提交我审阅。允许使用系统库、tokio*、serde*、futures*、async-trait、anyhow，禁止使用tempfile、thiserror、chrono等，所有新增外部依赖库一并提交我审阅。对于Java到rust的语言映射的封装工具类请查看common-trait/util/目录，应该尽量复用。比如：1）遇到java的CompletableFuture应该使用common-trait/util/completable_future.rs；2）涉及Java的继承体系，对于子类集中于同一目录且分类清晰，通过enum方式实现；否则继承应通过trait实现。3）反射、SPI等扩展机制，允许提供编译期宏实现编译时扩展机制即可。4）日志打印临时用原始的println方式即可。 3. 紧接着，分析connect/api对于connect目录外的其它kafka模块的依赖，涉及对于kafka-client依赖请在保证接口兼容的前提下在common-trait中生成接口声明，在kafka-client-mock内生成内存版的kafkaclient mock实现（当前已实现，分析是否缺失），这样方便后续端到端测试。对于kafka-client之外的结构体和工具类等应在common-trait下1:1生成。最终整理所有依赖和方案，提交我审阅。 4. 完整性度量方式：功能完整，迁移无遗漏，函数中不允许标注todo或者其他的空实现。“无遗漏” 比如通过对比connect/api模块中class数量与connect-rust/connect-api中struct+trait数量大致相同，函数个数大致相同，代码量只多不少，近似佐证。5. 正确性度量方式：确保connect-rust/connect-api模块代码编译通过，kafka对应模块的ut也应一并迁移，并保证运行通过，若kafka UT不足，也可以自行补充。 6. 满足上述要求后，请输出最终的中文报告，报告内容就是证明完成了上述的完整性和正确性，结果让我审阅。
```


```
我计划用rust语言重写kafka connect目录到connect-rust目录下（除connect/file、connect/json、connect/test-plugins外所有均迁移，含测试），请优先分析connect/api模块，完成1:1代码翻译方案，保证迁移代码的一致性、完整性、正确性。约束：1.迁移过程要严格保证目录、类名、函数名一致；2. 优先分析connect/api中涉及的Java到rust的语言翻译难点，含对三方的依赖，分析结果提交我审阅。允许使用系统库、tokio*、serde*、futures*、async-trait、anyhow、base64、regex、one_cel、chronol，禁止使用tempfile、thiserror等，所有新增外部依赖库一并提交我审阅。对于Java到rust的语言映射的封装工具类请查看common-trait/util/目录，应该尽量复用。比如：1）遇到java的CompletableFuture应该使用common-trait/util/completable_future.rs；2）涉及Java的继承体系，对于子类集中于同一目录且分类清晰，通过enum方式实现；否则继承应通过trait实现。3）反射、SPI等扩展机制，允许提供编译期宏实现编译时扩展机制即可。4）日志打印临时用原始的println方式即可。 3. 紧接着，分析connect/api对于connect目录外的其它kafka模块的依赖，涉及对于kafka-client依赖请在保证接口兼容的前提下在kafka-client-mock内生成内存版的kafkaclient mock实现（接口定义在common-trait中并以目录区分），这样方便后续端到端测试。对于kafka-client之外的结构体和工具类应在common-trait下1:1生成对应实现。最终整理所有依赖和方案，提交我审阅。 4. 完整性度量方式：功能完整，迁移无遗漏，函数中不允许标注todo或者其他的空实现。“无遗漏” 比如通过对比connect/api模块中class数量与connect-rust/connect-api中struct+trait数量大致相同，函数个数大致相同，代码量只多不少，近似佐证。5. 正确性度量方式：确保connect-rust/connect-api模块代码编译通过，kafka对应模块的ut也应一并迁移，并保证运行通过（ut迁移保证每个@Test测试功能点不遗漏，但是代码不需要逐行比对）。若kafka UT缺失或缺少核心功能覆盖，你应该补充UT保证功能正确。 6. 满足上述要求后，请输出最终的中文报告，报告内容就是证明完成了上述的完整性和正确性，结果让我审阅。
```
