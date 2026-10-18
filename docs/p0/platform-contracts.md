# 本地核心合同与验证记录

2026-10-18，依据设计 v0.2 和 P0 T03/T04。本轮已完成 T03 静态源码核对，提前实现不依赖集群的 YAML 与恢复判定。**这是 T04 的部分实现及 P1 的部分前置逻辑，不是完整 P0 或完整服务。**

## 1. 已实现的代码

| 入口 | 职责 | 明确边界 |
|---|---|---|
| [JobSpec](../../src/main/java/io/github/xuxiaotuan/magicflink/spec/JobSpec.java) | 不可变定义、参数、输入、固定检查类型、输出及类型化绑定结果 | 不是已授权的 ResolvedRunSpec |
| [JobSpecParser](../../src/main/java/io/github/xuxiaotuan/magicflink/spec/JobSpecParser.java) | UTF-8/YAML 严格解析、字段/类型约束、默认值合并、批次参数绑定 | 不解析/执行 SQL，不解析数据集版本或连接配置 |
| [StageIdentity](../../src/main/java/io/github/xuxiaotuan/magicflink/control/StageIdentity.java) | 逻辑阶段键、当前 attempt、执行器、CR UID/JobID、计划/配置/消费数据摘要 | 摘要是上层已冻结事实，本轮未实现规范化摘要算法 |
| [ReceiptGate](../../src/main/java/io/github/xuxiaotuan/magicflink/control/ReceiptGate.java) | 当前资格、取消、revision、证据完整性、幂等重复/冲突的纯判定 | 没有数据库事务或实际状态推进 |
| [SealRecovery](../../src/main/java/io/github/xuxiaotuan/magicflink/control/SealRecovery.java) | 从持久终态、证据、文件核验和封存观察选择下一步 | 不生成真实 CommitEvidence，不读写文件或数据库 |

采用单 Maven 工程，未引入 Web 框架、调度器、仓储抽象、内存数据库、插件体系或新进程。未来 Service/Runner 仍进入同一工程与统一测试扫描的 fat JAR，不接受用户 JAR。

## 2. YAML 与参数合同

`parse(byte[])` 接收完整的 UTF-8 原文；调用方 HTTP 层未来还须在读取 body 时限制大小，不能先无界读入。当前上限 256 KiB、32 个输入、64 条显式检查，嵌套深度额外收紧至 32。

解析器只读取 SnakeYAML 节点，不把 YAML 反序列化为用户指定 Java 类型。拒绝未知字段、重复键、多文档、锚点、别名、merge key、显式 null；此实现进一步拒绝所有显式 tag（含内置 tag），避免通过 tag 改变标量解释。布尔只接受 `true/false`，整数只接受十进制形式；拒绝 YAML 1.1 的 yes/no、八进制/十六进制等隐式简写。日期与 decimal 必须加引号。名称/检查 ID/输入别名、检查位置、必需字段、输入引用、非空无重复列、row-count 区间及有界 timeout 按 v0.2 校验。

平台实例化 `JobSpecParser(maxReadinessTimeout, decimalPrecision, decimalScale)`；配置不可由用户 YAML 覆盖。当前核心采用一个平台给定的 DECIMAL(p,s) 规则，保存在解析出的 JobSpec 中；运行绑定使用该定义冻结的规则，不能随服务默认设置漂移。未来数据集/SQL 类型核对必须在此基础上再验证，不在运行中更改规则或舍入值。本地测试使用 DECIMAL(38,18)，仅为测试策略，不作为生产默认发布。

`bind(spec, suppliedParameters)` 拒绝未声明参数、null、集合和错误类型。必需参数必须传值，可选参数必须在定义中有合法默认值。结果类型为 String、Boolean、BigInteger、BigDecimal、LocalDate；不把金额转换成 double。decimal 仅接受普通十进制字符串，无科学计数法，不截断、不舍入。日期严格校验日历。每个输入/输出 batch 为字面量或完整 `${name}`，不存在任意表达式求值。

SQL 保留原文，拒绝 `${...}` 插值，后续参数通过只读 `job_params` 表注入。**当前解析成功不代表 SQL 合法或可以执行**：单条 SELECT/WITH、禁止 DDL/DML、多语句和外部 Catalog、输出 Schema/DECIMAL/时间精度等仍须真实 Planner 和注册表验证。没有用正则假装完成 SQL 语义检查，也没有提供执行入口。

`preChecks`/`postChecks` 缺省才使用空列表。用户不能配置 row-compare；系统强制就绪规则、输出业务规则、目标完整比较仍需后续服务合并。本轮只验证检查定义，不执行检查查询或声称数据一致。

## 3. 阶段身份与回执接纳

逻辑键为 `(runId, stageType, inputAlias)`。COLLECT 必须有 inputAlias，其他阶段为空；不能用一个全局 attempt 覆盖多个输入。实现区分 PRECHECK、RESULT_CHECK、TARGET_CHECK 逻辑阶段，后续可映射至 Runner CHECK 执行模式。

StageIdentity 还包含 attemptId、执行器、资源身份及三个固定摘要。FLINK 必须包含 CR UID 和 JobID，INLINE 不得伪造这两个字段。外部资源同名重建但 UID 不同视为不同身份。

`Current` 是事务中读出的当前快照。ACTIVE 表示允许接纳此阶段；COMPLETED 必须来自该精确身份的持久阶段完成记录；INELIGIBLE 表示取消以外的失效/终止/不允许阶段。不能从回调里的 success 推断 COMPLETED。`evidenceComplete` 也必须由持久事实核验产生，不能相信请求中的布尔值。

| 条件（按优先顺序） | ReceiptGate 返回 | 调用方职责 |
|---|---|---|
| 当前 revision 与此前观察 revision 不同 | REREAD | 重新读取所有资格条件；其他输入正常完成可能导致冲突 |
| 身份不同、cancelRequested、INELIGIBLE | ARCHIVE_ONLY | 保留审计，不推进、不覆盖当前结果 |
| 已接纳回执同摘要 | ALREADY_ACCEPTED | 无副作用，不再创建下游 |
| 已接纳回执异摘要 | CONFLICT | 拒绝覆盖并报告证据冲突 |
| 尚未接纳且必需证据不完整 | WAIT_EVIDENCE | 等待完整事实 |
| 完整事实且阶段已持久完成、缺回执 | RECORD_ONLY | 仅补回执，不重复推进已完成阶段 |
| 完整事实且阶段 ACTIVE | ACCEPT | 在同一事务内再次校验并 CAS，接纳产物/完成阶段/创建唯一下一意图 |

**纯函数返回 ACCEPT 不是持久授权。** 函数执行后到数据库写入之间仍可能发生取消/换 attempt。后续调用方必须将读取、资格判断、产物引用、阶段完成和 CAS 放进同一个事务；本轮未用内存模拟事务而宣称竞争安全。取消后即便内容相同也只归档。

## 4. 文件封存恢复判定

Observation 必须绑定同一 StageIdentity 和对应证据内容。COMPLETE 包括合法文件集合、全部 writer（包括零输出）和输入分片完成，不是文件数非零；VALID 表示逐文件内容/大小/行数/Schema 已核对；MATCHING 表示内容摘要和身份一致，不只是对象存在。生成这些观察的 I/O 适配器尚未实现。

| 观察 | 下一步 |
|---|---|
| revision 冲突 | REREAD |
| 旧尝试、取消或失效阶段 | ARCHIVE_ONLY |
| 内容冲突/文件损坏，或相互矛盾的事实 | REJECT，保留现场，不覆盖修复 |
| 任一必需对象读取/文件核验结果 UNKNOWN | WAIT_UNKNOWN，不解释为不存在 |
| 观察到 SUCCEEDED，但尚未持久化成功事实 | PERSIST_SUCCESS，完成后重新观察 |
| 无持久成功事实，终态 UNKNOWN | WAIT_UNKNOWN；确认停止且证据等待期限已到才允许废弃 |
| 仍运行，或失败/取消但尚未满足停止及等待条件 | WAIT_STOP |
| 成功事实存在，完整提交证据缺失/不完整 | WAIT_EVIDENCE；确认停止且期限已到才 ABANDON_AND_RETRY |
| 完整证据与持久成功齐备，文件未核验 | VERIFY_FILES |
| 文件核验通过，Manifest 确认不存在 | BUILD_MANIFEST，采用固定身份/内容与原子不可变写入 |
| Manifest 一致，元数据未封存 | SEAL_METADATA，同事务完成资格检查、封存和阶段完成 |
| 元数据已封存且阶段完成，回执确认不存在 | REBUILD_RECEIPT，不重跑作业 |
| 已封存、阶段完成、回执一致 | DONE |

补充安全约束：

- CR 已不可见但原持久成功事实仍完整时，可以恢复封存，不要求重新出现 CR。
- 元数据 SEALED 与阶段 COMPLETED 必须一致；SEALED 缺持久成功、Manifest 或完整证据属于损坏，不重新拼装旧成功。
- 持久成功同时观察到 FAILED/CANCELLED/RUNNING、RUNNING 同时声称已停止、或未 SEALED 已有成功 receipt，均拒绝。
- ABANDON_AND_RETRY 只是一项建议；调用方仍须检查 RuntimeProfile 重试预算、再次确认停止，并在事务中切换 currentAttempt。它不删除文件、不启动任务，不能绕开保留期/配额。
- UNKNOWN 等待需要后续控制器有界重查并在超期时报告 NEEDS_ATTENTION；不能无限热循环，也不能因超时而覆盖对象。GET 错误不自动降级为 ABSENT。
- 本类只用于 COLLECT/COMPUTE 文件阶段；检查报告、目标写入、首次发布的恢复合同需要各自实现，不复用文件状态冒充发布事务。

## 5. 本轮验证证据

复用本机 Microsoft Java 17.0.14 与 Maven 3.9.16，实际命令：

```sh
JAVA_HOME=/Users/xujiawei/Library/Java/JavaVirtualMachines/ms-17.0.14/Contents/Home \
MAVEN_OPTS='-Xmx256m -XX:ActiveProcessorCount=2' \
/opt/homebrew/bin/mvn -o package
```

Surefire 固定一个测试进程，`-Xmx192m -XX:ActiveProcessorCount=2`。不启动 Flink MiniCluster、容器、Kubernetes 作业或 PostgreSQL。所有构建依赖使用既有本地缓存；本轮没有安装或部署。

2026-10-18 15:45:35（Asia/Shanghai）最终构建退出码 0：**64 tests，0 failures，0 errors，0 skipped，BUILD SUCCESS**。

| 测试 | 实际覆盖 |
|---|---|
| [JobSpecParserTest](../../src/test/java/io/github/xuxiaotuan/magicflink/spec/JobSpecParserTest.java)，46 个用例 | 设计完整示例、全部参数类型、日期/金额精确性、默认值、冻结精度、批次引用、全部检查类型与非法位置、字段/数量/大小/深度限制、UTF-8、重复键/null/tag/alias/merge 等拒绝 |
| [RecoveryTest](../../src/test/java/io/github/xuxiaotuan/magicflink/control/RecoveryTest.java)，18 个用例 | 逻辑阶段与资源/摘要隔离、取消、revision、重复/冲突回执、各封存故障窗口、UNKNOWN 与停止期限；其中一个用例遍历 25,600 组观察，检查封存和废弃的必要条件 |

先写测试再实现，最初缺类型导致编译失败，这只是脚手架 RED。随后审查中新增的三个语义用例取得真实断言失败：旧定义精度被服务新策略放宽、RUNNING+已停止错误进入重试、未封存+成功 receipt 错误进入封存。修复后通过，再增加完整设计示例与恢复组合检查并执行全量 package。另补充了文件恢复不可用于 INLINE/WRITE 的负例，确认先返回 BUILD_MANIFEST 导致断言失败，再加入执行器/阶段限制并通过。测试构造数据中的一次 YAML 缩进错误也已修正。

普通核心库产物位于 `target/magic-flink-0.1.0-SNAPSHOT.jar`；target 被忽略。已检查 JAR 含 34 个核心 class（含嵌套类型）、不含测试类；Markdown 本地链接无断链，`git diff --check` 通过，v0.2 设计摘要保持不变。该 JAR 没有 ServiceMain/RunnerMain，没有 shade，不是待上线发行制品。依赖版本、来源对照与扫描边界见[版本清单](version-lock.md)。

## 6. 尚未完成的验收

- T04 的 ConnectionProfile/DatasetDefinition/RuntimeProfile 完整合同、规范化摘要、原子对象写入、事务 CAS、首次发布唯一键初始化/历史去重及并发验证。
- T05 的双入口、真实 Flink/Planner/JDBC/Parquet/S3 依赖闭包、fat JAR、SBOM/扫描及固定镜像。
- T06/T07 的合法文件集合、失败 task 排除、零行、多文件、内容核验、真实存储故障和 F01–F13。
- P1 的服务受理、权限、规划、协调器及持久化；四源采集和业务 Join 仍按原阶段推进。

因此当前状态为“本地核心测试通过，运行/事务证据 evidence_incomplete”。T03 为 GO_TO_SPIKE，环境准入仍 NO_GO。后续可继续本地开发，不重复把环境检查当作所有代码工作的前置条件。
