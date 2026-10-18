# 本地 SQL Planner 校验与对象摘要合同

2026-10-18，依据 v0.2 第 4、5.5、5.6、7、8 节及 P0 T04/T05。此文记录本轮实际实现；T03 的 GO_TO_SPIKE 结论、环境运行准入与 F01–F13 状态不变。

## 1. SQL 校验入口与执行边界

[SqlPlannerValidator](../../src/main/java/io/github/xuxiaotuan/magicflink/spec/SqlPlannerValidator.java) 提供两个入口：

```java
Report validate(JobSpec definition, Map<String, SqlSchema> inputs, SqlSchema output);
Report validate(JobSpec.BoundJob run, Map<String, SqlSchema> inputs, SqlSchema output);
```

输入必须来自严格 YAML 解析及平台解析的固定 Schema；输入别名集合必须与定义完全相同。Schema 仅登记物理列与类型，不声明未经证明的 PRIMARY KEY。`SqlSchema` 保留列顺序、名称、精度、nullability 和 TIMESTAMP 家族。

校验顺序：

1. 核对 Flink 核心运行版本为 1.20.5、输入与输出 Schema 的类型能力。
2. 使用同版 Flink SQL Parser 工厂解析语法树：只接受一条 SELECT/非递归 CTE；限制表引用和原始表达式能力，防止危险表达式被后续常量简化抹去。
3. 每次创建独立 BATCH TableEnvironment。用零行、有明确物理 Schema 的 VALUES 临时视图表示有界输入；按声明构造 `job_params` 类型，不要求定义校验提供必填参数的运行值。统一 SQL local time zone 为 UTC。
4. 调用 Flink Parser 生成经过名称/类型解析的 PlannerQueryOperation。通过公开 getter 读取关系计划，遍历关系操作、聚合与表达式类型，验证 Join 两侧的等值键。
5. 按名称、顺序、nullability 和无损类型关系对比登记的输出 Schema，返回解析出的 Schema、引用输入集合和实际核对过的 Flink 版本。

全程不调用 execute、executeSql、collect 或数据源工厂；不启动 MiniCluster、JobManager、TaskManager 或外部连接。VALUES 仅是规划时的 Schema 载体，不是运行 fixture，也不用于推断实际数据为空、实际统计信息或唯一性。报告通过表示静态可规划，不能升级为执行或业务一致性通过。

## 2. 能力与拒绝条件

| 范围 | 当前行为 |
|---|---|
| 查询 | SELECT、非递归 CTE、过滤、投影、GROUP BY、确定性精确聚合；可使用有界标量子查询 |
| 表引用 | 只接受固定输入别名、`job_params` 和作用域内 CTE；拒绝数据库/Catalog 限定名、未知表、表函数、LATERAL 和动态 Source |
| Join | INNER/LEFT；关系计划必须能提取左右输入间至少一个等值键，允许额外过滤谓词；RIGHT/FULL/CROSS、逗号 Join、纯非等值或同侧自比较均拒绝 |
| 操作 | 拒绝 DDL/DML、INSERT、SET、ADD JAR、SHOW、多语句、UNION 和未列入能力表的关系操作；不对用户语句产生副作用 |
| 函数 | 固定 allowlist：精确算术/比较/逻辑、CASE/COALESCE、CAST、IN/LIKE、有限字符串函数、ABS/ROUND/FLOOR/CEIL、SUM/COUNT/MIN/MAX/AVG；同时核对解析后函数的确定性和 dynamic 标记 |
| 非确定性 | RAND、UUID、当前时间/日期、未知 UDF 等拒绝；常量 false 分支也不能隐藏违规表达式 |
| 类型 | 支持基础精确数值、字符串、二进制、日期时间；拒绝 FLOAT/DOUBLE/REAL、复杂嵌套和其余未支持类型；指数形式近似数字字面量也拒绝 |
| 金额/整数 | 在中间 CAST 和输出边界拒绝无法证明无损的缩窄；显式 ROUND 后仍需满足输出容量。整数可安全扩大，DECIMAL 的整数位和 scale 必须都容纳源类型；整数范围按完整类型范围判断，不靠测试值猜测 |
| 时间 | SQL local time zone 固定 UTC；输出 TIMESTAMP 与 TIMESTAMP_LTZ、精度按登记类型核对，不自动混同或截断；源端业务时区转换仍由采集契约完成 |
| 输出 | 列名与顺序精确匹配；nullable 结果不能自动映射 NOT NULL；DECIMAL 无损扩大允许，缩窄拒绝 |
| LIMIT/OFFSET | **当前一律拒绝，含 ORDER BY 后的 LIMIT。** v0.2 要求稳定全序；当前没有经过数据证明的唯一键传播机制，不能把普通 ORDER BY 或主键声明当作证明。属于保守能力限制，未宣称支持所有满足 v0.2 的合法 LIMIT |

`job_params` 的 integer 映射为 DECIMAL(38,0)，decimal 使用定义冻结的 DECIMAL(p,s)，其余为 STRING/BOOLEAN/DATE。运行入口额外拒绝超过 38 位的 integer；integer 默认值也校验这个限制。该检查与 YAML 类型绑定不同，受理运行必须依次调用绑定和运行规划，不能仅拿定义规划报告放行任意参数。

错误为 `SqlValidationException`，包含稳定 code 和 path=`sql`。例如 SQL_FORBIDDEN_OPERATION、SQL_UNKNOWN_INPUT、SQL_FORBIDDEN_JOIN、SQL_FORBIDDEN_FUNCTION、SQL_UNSUPPORTED_TYPE、SQL_LOSSY_CAST、SQL_OUTPUT_SCHEMA、SQL_PARAMETER_RANGE。通用 Planner 异常转换为 SQL_PLANNING_FAILED，不保留可能泄漏 SQL 字面量的原始异常信息。

剩余边界：数据集权限、配置版本解析、检查规则列/类型与币种的完整核对、真实数据溢出/NULL/唯一性、运行容量及文件 Source 贯通仍需后续受理和执行层实现。静态 DECIMAL 规划不证明实际 SUM 永不溢出；运行时必须让此类错误失败，不能静默截断。

## 3. 固定 Planner 依赖与接口来源

使用 `org.apache.flink:flink-table-planner_2.12:1.20.5`，Maven scope=`provided`。已有 loader 将 Planner/Calcite 隔离；本轮选普通 Planner 以直接检查关系计划，测试 classpath 不同时放 loader，应用普通 JAR 也不打包 Planner 类。

相关固定源码：

- [Flink Planner POM](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-table/flink-table-planner/pom.xml)：普通 Planner、API/runtime 归属与打包关系。
- [PlannerQueryOperation.getCalciteTree](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/operations/PlannerQueryOperation.java#L67)：读取已解析关系树。
- [ParserImpl.parse](https://github.com/apache/flink/blob/0980485040c6dbec3a1b32da05c4b655cf01ad2b/flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/delegation/ParserImpl.java#L91)：语句解析、校验和 Operation 转换。

此适配器耦合 Flink Internal/Calcite 接口，集中在 spec 中，不使用反射或私有字段。更换 Flink/Planner、loader 模式或优化行为时必须重跑能力和负例测试。未来同一应用的 Service/Runner classpath 都必须验证这一选择；本轮未证明固定镜像中的 SPI 或类加载兼容。

## 4. 规范化 JSON 与摘要规则

[CanonicalJson](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/CanonicalJson.java) 与 [ObjectDigests](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/ObjectDigests.java) 实现 `magic-flink-canonical-v1`。这是项目协议，不声明兼容 RFC 8785。

摘要输入固定为以下 envelope 的 UTF-8 字节，SHA-256 输出 64 位小写十六进制：

```json
{"content":{},"kind":"MANIFEST","protocol":"magic-flink-canonical-v1"}
```

示例只展示 envelope；实际 content 必须满足对应字段合同。不同 kind 做域隔离，同一字段集合不能跨对象类型混用。摘要字段本身不进入自身内容；引用其他对象时保留其摘要。

规则固定：

- Map 键按 Java String 的 UTF-16 顺序排序；保留大小写与 Unicode，不做 NFC 或路径猜测性归一化。字符串按 JSON 转义，拒绝非法代理项。
- Integer/Long/BigInteger 等整数精确编码成 JSON 整数；消费者不得先转为 double 再核验摘要。拒绝 Float、Double、NaN、Infinity、null、Set、任意 Java 对象。
- BigDecimal 编码为普通十进制**字符串**，保留 scale；日期为 ISO 字符串。参数额外保留 `{type,value}`，防止 string/date/decimal 的同文本值混同。
- Instant 固定为 UTC、9 位小数，例如 `2026-10-18T08:00:00.000000000Z`。编码器不读取当前时间；plannedAt/completedAt/retainUntil 必须来自原持久计划/事实。恢复观测时间与重试次数属于外部审计，不得添入 immutable content。
- 一般数组保留顺序，尤其 Schema 列、检查列表和复合键列。仅合同明确的集合排序：文件按 objectKey，writer 按 writerId，split 按 splitId；重复身份拒绝，不悄悄去重。文件额外拒绝重复 `(writerId,sequence)`。
- 首版内部文件键只接受规范的 `s3://小写bucket/受控路径`，路径字符限字母/数字/下划线/点/横线/斜杠；拒绝 `..` 归一差异、通配符、凭据、端口、query、fragment、百分号别名和其他 scheme。prefix 必须以 `/` 结尾，文件键不得以 `/` 结尾。来源 HTTP URL 不使用这套内部对象键规则。
- 深度上限 64，单份规范内容上限 1 MiB；极端 decimal 在展开普通字符串前检查规模。当前没有为生产大清单做分页/分块；超过上限明确拒绝，不能截断后计算摘要。
- Frozen 只保存已编码字节及摘要；输入集合后续修改不会影响它，返回的 byte[] 也是副本。

此版本字段/表示发生语义变化必须新建规范版本，不能在同一版本下静默换序列化策略。Schema 使用固定 Flink 类型的 asSerializableString，升级时还需核对该表示，必要时升级摘要协议。

## 5. 对象内容与来源合同

| kind / 入口 | 必需内容 | 约束与事实来源 |
|---|---|---|
| SCHEMA / schema(SqlSchema) | 有序 columns，每列 name/type（含精度和 nullability） | 来自平台注册的固定物理 Schema；不能把列当作无序集合 |
| JOB_DEFINITION / definition(JobSpec) | version、name、decimalRules、parameters、inputs、preChecks、sql、postChecks、output | 严格解析结果；YAML 注释、字段顺序不影响摘要，SQL 字符串本身保持原文 |
| RESOLVED_RUN_SPEC / resolved(...) | runId、jobId、jobVersion、releaseId、definitionDigest、类型化 parameters、固定 inputs/output | 输入/输出都冻结 dataset/version/schemaDigest/batch；引用名与定义精确对应，不能借此覆盖数据集；该函数不验证授权来源 |
| STAGE_PLAN / freeze | runId、stageType、inputAlias、stageAttemptId、releaseId、resolvedSpecDigest、consumedDataDigest、inputs、schemaDigest、parallelism、outputPrefix、plannedAt | 外部执行前持久化；COLLECT 有 inputAlias，其余为空；inputs 必须含固定读取/分片版本引用，不能恢复时从当前注册表补新版本 |
| COMMIT_EVIDENCE / freeze | 完整 stageIdentity、files、writers、splits、completedAt | files 含 objectKey/bytes/writerId/sequence；writers 含 writerId/fileCount/ended；splits 含 splitId/evidenceDigest。文件归属 writer 必须有结束记录且计数匹配，零文件也要求 writer 和 split 的显式结束证据 |
| MANIFEST / freeze | manifestId、stageIdentity、commitEvidenceDigest、schemaDigest、sourceVersions、files、totalRows、completedAt、retainUntil | 文件在提交身份基础上增加实际核验的 rows/sha256；行数汇总必须匹配、保留期不能早于完成时间；源版本与 InputBundle 等引用由生产方固定 |
| STAGE_RECEIPT / freeze | stageIdentity、status、artifactDigest、completedAt | 状态限 SUCCEEDED/FAILED/CANCELLED；只能由已确认事实生成/重建，不得用回执循环证明产物成功 |

stageIdentity 编码包含逻辑阶段键、stageAttemptId、executor、planDigest、resolvedSpecDigest、consumedDataDigest，FLINK 还包含 CR UID 与 Flink JobID，INLINE 不伪造外部资源。所有已命名摘要字段与文件 SHA-256 均要求 64 位小写十六进制。

`freeze(kind, fields)` 是受控生产方使用的底层内容封装：拒绝顶层缺字段/未知字段，核对上述文件/计数/摘要规则；它不代替 JobSpecParser、权限校验或完整领域对象验证。定义、绑定与 Schema 优先使用专门 helper。生产方还必须证明：writer/split 集合与 StagePlan 的预期集合完全相同、writerId 来自 T03 的真实身份、文件属于被接受的 attempt、各引用实际存在且内容正确。摘要 API 不会凭传入的 ended=true 或 sha256 就授予封存资格。

InputManifest/CandidateManifest 的完整源协议、投影/时区/令牌/业务控制总数等详细内容仍须由固定版本引用和对应生产对象提供。本轮冻结的是最小共同摘要载荷，未声称完整 DatasetDefinition/所有源协议对象均已实现。

## 6. 本轮测试、构建与剩余验收

沿用现有 Microsoft Java 17.0.14、Maven 3.9.16。新增普通 Planner 21,621,003 字节，经正常 Maven 构建解析至缓存，未执行 Maven install、安装系统软件或部署服务；后续编译/测试/打包使用 `mvn -o`。测试 JVM 上限由 192 MiB 调至 384 MiB，单 fork、2 个可用处理器，主 Maven 仍为 256 MiB；未启动 MiniCluster 或外部服务。

实际测试 classpath 含 63 个 JAR，其中 21 个非 shaded Flink JAR 全部为 1.20.5，不含 planner-loader。classpath 文件名与 SHA-256 清单记录到未跟踪的 `target/local-planner-classpath.tsv`。`dependency:tree` 的离线插件闭包缺 plexus-io/maven-dependency-analyzer，未为此安装/补下载工具；改用 Surefire 实际 classpath 核对。本轮不据此宣称最终发行依赖树、SBOM 或扫描已经完成。

测试覆盖：

- 原有 YAML 46 项和恢复判定 18 项。
- SQL 37 项：正例、非法操作/表/Join/函数、名称和类型错误、输出精度/nullability/时间家族、复用隔离、常量分支、DECIMAL/整数缩窄及运行 integer 范围。
- 摘要 25 项：独立黄金向量、键序/文件序无关、Schema 列序敏感、身份/源版本/文件内容变化、重复/计数/非法对象键拒绝、冻结字节、UTC/精确 decimal、深度与大小。

完整 Manifest 黄金向量由独立 Python 标准库 json/hashlib 计算，SHA-256 为 `902a5f4977fa98714b2ae2b0c321dbf77ccc047663c0d0a5cd0ca87522a4b266`，Java 测试断言固定值；不是生产函数算出再与自身比较。

先写测试，最初缺少类型/API 的 RED 只是编译失败；随后确实取得语义断言失败并修复：DECIMAL 缩窄被放行、整数缩窄被放行、false 分支中浮点 CAST/近似数字被简化后遗漏。常量分支的当前日期与 RAND 负例也通过。补足转换和原始 AST 校验后，全量本地测试通过。

本地通过仍不覆盖：真实文件/数据库连接、结果逐行正确性、目标写入回读、完整源字段与权限、SQL 检查执行、不可变对象条件写、元数据库事务 CAS、首次发布、fat JAR 双入口与镜像类加载。T01/T02、T05–T08 与 F01–F13 的原验收状态保留。


最终验证：2026-10-18 16:14:21（Asia/Shanghai），`mvn -o package` 退出码 0，126 tests / 0 failures / 0 errors / 0 skipped，BUILD SUCCESS。普通应用 JAR 含 47 个应用 class、不含 Flink/Calcite 或测试类；本轮 Markdown 本地链接检查与 `git diff --check` 通过，设计 v0.2 SHA-256 仍为 `fff41e1fbe958dbec7c1490c2a76633dc72d60ea9077957771c4c042f8818291`。编译存在 Calcite 可选 apiguardian 注解缺失警告，测试使用 SLF4J NOP 日志；这些警告不影响本次构建结果，最终镜像的注解/日志依赖仍由 T05 统一核验。
