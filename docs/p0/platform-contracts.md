# 本地核心合同与验证记录

2026-10-18，依据设计 v0.2 和 P0 T03/T04。已完成 T03 静态核对、YAML 与恢复判定；后续新增本地 SQL Planner、对象摘要，以及本批领域校验/交叉核验/本地存储。**这是 T04 的部分实现及 P1 的部分前置逻辑，不是完整 P0 或完整服务。**

## 1. 已实现的代码

| 入口 | 职责 | 明确边界 |
|---|---|---|
| [JobSpec](../../src/main/java/io/github/xuxiaotuan/magicflink/spec/JobSpec.java) | 不可变定义、参数、输入、固定检查类型、输出及类型化绑定结果 | 不是已授权的 ResolvedRunSpec |
| [JobSpecParser](../../src/main/java/io/github/xuxiaotuan/magicflink/spec/JobSpecParser.java) | UTF-8/YAML 严格解析、字段/类型约束、默认值合并、批次参数绑定 | 不解析/执行 SQL，不解析数据集版本或连接配置 |
| [StageIdentity](../../src/main/java/io/github/xuxiaotuan/magicflink/control/StageIdentity.java) | 逻辑阶段键、当前 attempt、执行器、CR UID/JobID、计划/配置/消费数据摘要 | 摘要仍需上层冻结事实；规范化算法见 protocol，不能拿摘要代替来源核验 |
| [ReceiptGate](../../src/main/java/io/github/xuxiaotuan/magicflink/control/ReceiptGate.java) | 当前资格、取消、revision、证据完整性、幂等重复/冲突的纯判定 | 没有数据库事务或实际状态推进 |
| [SqlPlannerValidator](../../src/main/java/io/github/xuxiaotuan/magicflink/spec/SqlPlannerValidator.java) | 同版 Flink 语法/名称/类型、AST 和关系计划能力、输出 Schema、运行 integer 范围 | 本地规划，不执行查询；表/参数 Schema 由平台提供 |
| [ObjectDigests](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/ObjectDigests.java) | Schema/定义/绑定/阶段计划/提交证据/清单/回执的规范化身份 | 最小共同摘要合同，不授予封存或发布资格 |
| [PlatformDefinitions](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/PlatformDefinitions.java) | P0 连接、源/目标、运行档位、登记规则和版本/权限/预算关系 | 不是实际数据库授权或全局资源预留 |
| [ResolvedRun](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/ResolvedRun.java) | 全部配置版本、参数/批次、主体、检查列和真实 Planner 的组合校验 | 不提供 HTTP 受理或持久注册表 |
| [FileStageProtocol](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/FileStageProtocol.java) | 计划/原生接纳集合/提交/分片/终态/清单关系 | 上游须实际生产可信事实，不代替原生适配或 SEALED 事务 |
| [DomainSnapshots](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/DomainSnapshots.java) | domain-v2 完整领域载荷与摘要 | 保留 canonical-v1，详细编码见 T04 合同 |
| [LocalImmutableObjectStore](../../src/main/java/io/github/xuxiaotuan/magicflink/connectors/LocalImmutableObjectStore.java) | 本地不可变元数据写入、核验读取和重开 | 专属 POSIX 目录；不是 S3、Parquet 或数据库适配器 |
| [SealRecovery](../../src/main/java/io/github/xuxiaotuan/magicflink/control/SealRecovery.java) | 从持久终态、证据、文件核验和封存观察选择下一步 | 不生成真实 CommitEvidence，不读写文件或数据库 |

采用单 Maven 工程，同版普通 Flink Planner 为 provided 依赖；未引入 Web 框架、调度器、仓储抽象、内存数据库、插件体系；测试仅额外启动受限测试 JVM 验证本地跨进程争写。未来 Service/Runner 仍进入同一工程与统一测试扫描的 fat JAR，不接受用户 JAR。

## 2. YAML 与参数合同

`parse(byte[])` 接收完整的 UTF-8 原文；调用方 HTTP 层未来还须在读取 body 时限制大小，不能先无界读入。当前上限 256 KiB、32 个输入、64 条显式检查，嵌套深度额外收紧至 32。

解析器只读取 SnakeYAML 节点，不把 YAML 反序列化为用户指定 Java 类型。拒绝未知字段、重复键、多文档、锚点、别名、merge key、显式 null；此实现进一步拒绝所有显式 tag（含内置 tag），避免通过 tag 改变标量解释。布尔只接受 `true/false`，整数只接受十进制形式；拒绝 YAML 1.1 的 yes/no、八进制/十六进制等隐式简写。日期与 decimal 必须加引号。名称/检查 ID/输入别名、检查位置、必需字段、输入引用、非空无重复列、row-count 区间及有界 timeout 按 v0.2 校验。

平台实例化 `JobSpecParser(maxReadinessTimeout, decimalPrecision, decimalScale)`；配置不可由用户 YAML 覆盖。当前核心采用一个平台给定的 DECIMAL(p,s) 规则，保存在解析出的 JobSpec 中；运行绑定使用该定义冻结的规则，不能随服务默认设置漂移。未来数据集/SQL 类型核对必须在此基础上再验证，不在运行中更改规则或舍入值。本地测试使用 DECIMAL(38,18)，仅为测试策略，不作为生产默认发布。

`bind(spec, suppliedParameters)` 拒绝未声明参数、null、集合和错误类型。必需参数必须传值，可选参数必须在定义中有合法默认值。结果类型为 String、Boolean、BigInteger、BigDecimal、LocalDate；不把金额转换成 double。decimal 仅接受普通十进制字符串，无科学计数法，不截断、不舍入。日期严格校验日历。每个输入/输出 batch 为字面量或完整 `${name}`，不存在任意表达式求值。

YAML 解析成功仍不代表 SQL 已验证；随后调用同版 Flink 的本地 Planner，执行 AST、关系计划与输出 Schema 的能力检查。当前已实现单条 SELECT/CTE、表引用隔离、Join 等值键、函数/类型和无损转换判定；LIMIT/OFFSET 因缺少稳定全序证明一律拒绝。详细范围、运行参数映射和错误合同见 [SQL 与摘要合同](sql-and-digest-contracts.md)。SQL 原文保留，不做插值，也不执行查询；注册表权限和真实运行仍未完成。

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

Observation 必须绑定同一 StageIdentity 和对应证据内容。COMPLETE 包括合法文件集合、全部 writer（包括零输出）和输入分片完成，不是文件数非零；VALID 表示逐文件内容/大小/行数/Schema 已核对；MATCHING 表示内容摘要和身份一致，不只是对象存在。本批已实现本地元数据 I/O 和计划/提交集合交叉核验；真实原生提交、文件内容扫描及数据库观察适配器仍待实现。

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

## 5. 首批核心验证证据（历史，64 项）

复用本机 Microsoft Java 17.0.14 与 Maven 3.9.16，实际命令：

```sh
JAVA_HOME=/Users/xujiawei/Library/Java/JavaVirtualMachines/ms-17.0.14/Contents/Home \
MAVEN_OPTS='-Xmx256m -XX:ActiveProcessorCount=2' \
/opt/homebrew/bin/mvn -o package
```

首批 Surefire 固定一个测试进程，`-Xmx192m -XX:ActiveProcessorCount=2`。不启动 Flink MiniCluster、容器、Kubernetes 作业或 PostgreSQL。所有构建依赖使用既有本地缓存；该批没有安装或部署；后续 Planner 构建变更见第 7 节。

2026-10-18 15:45:35（Asia/Shanghai）最终构建退出码 0：**64 tests，0 failures，0 errors，0 skipped，BUILD SUCCESS**。

| 测试 | 实际覆盖 |
|---|---|
| [JobSpecParserTest](../../src/test/java/io/github/xuxiaotuan/magicflink/spec/JobSpecParserTest.java)，46 个用例 | 设计完整示例、全部参数类型、日期/金额精确性、默认值、冻结精度、批次引用、全部检查类型与非法位置、字段/数量/大小/深度限制、UTF-8、重复键/null/tag/alias/merge 等拒绝 |
| [RecoveryTest](../../src/test/java/io/github/xuxiaotuan/magicflink/control/RecoveryTest.java)，18 个用例 | 逻辑阶段与资源/摘要隔离、取消、revision、重复/冲突回执、各封存故障窗口、UNKNOWN 与停止期限；其中一个用例遍历 25,600 组观察，检查封存和废弃的必要条件 |

先写测试再实现，最初缺类型导致编译失败，这只是脚手架 RED。随后审查中新增的三个语义用例取得真实断言失败：旧定义精度被服务新策略放宽、RUNNING+已停止错误进入重试、未封存+成功 receipt 错误进入封存。修复后通过，再增加完整设计示例与恢复组合检查并执行全量 package。另补充了文件恢复不可用于 INLINE/WRITE 的负例，确认先返回 BUILD_MANIFEST 导致断言失败，再加入执行器/阶段限制并通过。测试构造数据中的一次 YAML 缩进错误也已修正。

普通核心库产物位于 `target/magic-flink-0.1.0-SNAPSHOT.jar`；target 被忽略。已检查 JAR 含 34 个核心 class（含嵌套类型）、不含测试类；Markdown 本地链接无断链，`git diff --check` 通过，v0.2 设计摘要保持不变。该 JAR 没有 ServiceMain/RunnerMain，没有 shade，不是待上线发行制品。依赖版本、来源对照与扫描边界见[版本清单](version-lock.md)。

## 6. 尚未完成的验收

- T04 的真实配置能力核实、S3 条件写/读取语义、事务 CAS、首次发布唯一键初始化/历史去重及数据库并发验证。本批已补齐 P0 PostgreSQL 领域字段、交叉核验和本地原子不可变元数据存储，详见第 8 节。
- T05 的双入口、真实 Flink/Planner/JDBC/Parquet/S3 依赖闭包、fat JAR、SBOM/扫描及固定镜像。
- T06/T07 的合法文件集合、失败 task 排除、零行、多文件、内容核验、真实存储故障和 F01–F13。
- P1 的服务受理、权限、完整检查 Schema/运行能力受理、协调器及持久化；四源采集和业务 Join 仍按原阶段推进。

因此当前状态为“本地核心测试通过，运行/事务证据 evidence_incomplete”。T03 为 GO_TO_SPIKE，环境准入仍 NO_GO。后续可继续本地开发，不重复把环境检查当作所有代码工作的前置条件。


## 7. 第二批：本地 Planner 与摘要（历史，126 项）

新增实现和严格能力范围见 [SQL Planner 与对象摘要合同](sql-and-digest-contracts.md)。本轮补足真实 Flink 本地规划及 canonical-v1，共 126 个测试（YAML 46、SQL 37、摘要 25、恢复 18）；2026-10-18 16:14:21 最终 `mvn -o package` 退出码 0，126 tests / 0 failures / 0 errors / 0 skipped；普通应用 JAR 含 47 个应用 class，不含 Flink/Calcite 和测试类。

七类摘要覆盖 Schema、JobDefinition、ResolvedRunSpec、StagePlan、CommitEvidence、Manifest、StageReceipt。输入/输出配置版本、参数类型、逻辑阶段和外部身份被纳入身份；文件列表排序、重复身份拒绝、UTC 时间和恢复稳定性已测试。数据库 SEALED 状态与恢复进程当前时间不进入不可变内容。摘要不证明文件内容实际正确，也不代替事务 CAS。

测试复用现有工具；普通 Planner 依赖经 Maven 正常解析，后续支持离线构建。测试堆上限调整为 384 MiB、单 fork、2 个处理器，Maven 上限仍为 256 MiB；未启动集群/数据库、未安装/部署，未提交或推送。


## 8. 第三批：T04 领域与本地存储（当前，170 项）

详见 [T04 领域与本地存储合同](domain-and-local-storage-contracts.md)。新增 PlatformDefinitions、ResolvedRun、FileStageProtocol、DomainSnapshots、ObjectKeys、LocalImmutableObjectStore，补齐 P0 PostgreSQL 全部本批字段、明确版本/权限/预算、绑定值/检查 Schema/Planner 校验、完整预期 writer/split 与原生接纳提交集合核对。成功清单还须匹配持久终态与逐文件核验引用；COMPUTE 不能漏输入或读取其他运行目录。

完整领域载荷使用 magic-flink-domain-v2，原 canonical-v1 和黄金向量不变；新增规范内容解码、可信摘要与协议校验。实际本地存储覆盖同键幂等/冲突、发布前后异常恢复、错误不当不存在，以及跨实例/两个 JVM 争写。本地对象内容链经过持久化、重开与回读，不是内存 Map 模拟。

新增 44 项，加原有 126 项，共 170 项。构建命令沿用第 5 节，Surefire 单 fork/384 MiB/2 CPU，跨进程测试的两个子 JVM 各 64 MiB/1 CPU。最终 package 证据登记在详细合同中。未新增依赖或改变 POM，未安装、部署、暂存、提交或推送。

本批是本地合同和元数据 I/O 验收；真实 Source/FileSink/Parquet/S3、元数据库事务和目标发布仍 evidence_incomplete。登记权限、源端不可变声明及类型化事实不是运行证明；不会仅靠构造器生成 SEALED 资格或将 receipt 直接推进运行。
