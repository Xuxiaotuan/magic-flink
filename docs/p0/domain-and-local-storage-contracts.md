# T04 领域校验、计划交叉核验与本地不可变存储

2026-10-18，依据设计 v0.2 第 4、7、8、10 节及 P0 T04。本批交付不依赖集群的 P0 PostgreSQL 领域对象、冻结摘要、文件阶段交叉核验和实际本地元数据 I/O。未安装或部署；T04 的数据库协议、T05–T08 和 F01–F13 仍须各自验收。

## 1. 入口与字段

用户仍只提交既有 YAML。以下对象由平台构造，不增加用户 JDBC URL、凭据、任意 SQL 动作或 JAR 配置入口。平台引用必须包含不可变版本，拒绝 latest/current；本批只支持 P0 的 PostgreSQL，不把 HTTP/BQ/ClickHouse 提前标成已支持。

| 入口 | 已实现的字段与校验 |
|---|---|
| [PlatformDefinitions.ConnectionProfile](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/PlatformDefinitions.java) | 配置 ID/版本、规范物理连接 ID、PostgreSQL 地址、Vault/Kubernetes Secret 引用、TLS 声明、表级资源与权限、总连接数/探测预留/fetch size/查询超时；地址禁止明文凭据、query 和 fragment |
| JdbcDatasetDefinition | 固定连接版本、schema.table、物理列/精度/nullability、批次列与类型、时区、就绪标记表及版本/独立行数/完成时间字段、批次完成后不可变、投影/分片列/并发、有界且稳定可重读、允许空批、保留期、授权主体、登记必需规则 |
| OutputDatasetDefinition | 独立写入/发布连接版本、数据版本表、版本状态表、指针、历史与正式视图；业务 Schema、互不冲突的 batch/load/publication/revision 元数据列、业务键、全部有序比较字段、replace-batch、保留期/授权/登记规则 |
| RuntimeProfile | 固定 release、存储范围、JM/TM/slot/parallelism/CPU、运行/Application/输入/检查并发、inline 行数/字节/时限、阶段/运行/证据等待/轮询期限、重试次数/退避、核验缓冲及保留期 |
| Catalog | 版本引用闭合、只读输入、写入与发布分权且不同凭据、目标同库、实际资源清单匹配、Schema 字段与预算关系；同 endpoint 不可登记两个物理额度，同 physicalId 必须同地址且同额度 |
| [ResolvedRun](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/ResolvedRun.java) | run/job/version/主体、严格解析并绑定的定义、所有输入和输出的明确版本、平台快照、受理时间与固定运行期限；权限、引用/别名/参数集合、参数到批次绑定、批次物理类型/范围、SQL Planner 和检查列/精确数值类型交叉校验 |

Schema 只允许已支持的精确标量类型，拒绝浮点和复杂嵌套类型。批次列类型必须与登记的 parameter type 对应；分片列限整数或 DATE。业务列、元数据列和物理目标对象必须符合固定名称规则，不能夹带表达式。比较字段必须按 Schema 顺序覆盖所有业务列，不能用少数统计字段替代完整目标回读。

登记必需规则目前支持本数据集的 not-null、unique-key、row-count，必须有唯一 ID，列/范围合法且采集投影不能丢弃必需规则列；不支持的规则明确拒绝。规则冻结到平台注册快照，用户 YAML 不能覆盖。这里只校验并冻结规则，实际规则合并与执行器尚待接入；平台业务键非空/唯一及目标逐字段比较仍为后续强制检查。

资源验证拒绝非正额度、探测占满连接池、parallelism 超 slots、查询/inline 超阶段期限、轮询超过等待期限、保留期不足等。`connectionBudgets()` 只归并物理额度，不执行全局预留或扣还。DNS 别名等物理归属必须由平台核实，字符串校验不能发现隐藏别名。测试资源值是合成配置，不修改原环境预算，也不构成资源准入。

## 2. 新领域摘要与兼容边界

[DomainSnapshots](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/DomainSnapshots.java) 显式映射字段，不用反射/Java 序列化，也不读取当前时间。新载荷使用 **magic-flink-domain-v2**；原 magic-flink-canonical-v1 的七类最小摘要及黄金向量保留，未在同一协议下静默改字段。

| kind | 冻结内容 |
|---|---|
| PLATFORM_CATALOG | 所有配置版本、资源、权限、凭据引用、Schema、读取/就绪/登记规则、完整运行档位 |
| RESOLVED_RUN_SPEC | 定义摘要、类型化参数、批次、全部输入/输出版本、catalog/release、受理时间/运行截止时间 |
| JDBC_READ_FILTER | 固定表、批次列与类型化值、投影、分片列、绑定等值谓词；不是任意过滤 SQL |
| CONSUMED_INPUTS | 全部输入别名、版本、就绪/读取约束、控制总数，以及 COMPUTE 的清单生产身份/摘要/显式文件 |
| STAGE_PLAN | 阶段与 attempt、配置/定义/消费摘要、运行档位、Schema、预期 writer/split、系统生成目录、固定时间/保留期 |
| SPLIT_READ_PLAN | StagePlan 摘要与该逻辑分片；执行证据必须回指完全相同的读取约束 |
| COMMIT_EVIDENCE | 完整外部身份、合法文件、writer 结束、分片实际版本/行数/读取证据、原生接纳 attempt 和最终性摘要 |
| TERMINAL_OBSERVATION | 精确阶段/CR UID/Job ID、generation、终态、原完成和观察时间 |
| INPUT_MANIFEST / CANDIDATE_MANIFEST | 源快照、完整文件身份/大小/行数/SHA-256/Schema、总行数、提交证据与持久终态引用、固定时间/保留期 |
| STAGE_RECEIPT | 成功阶段身份、清单/提交/终态摘要、原完成时间；只是待事务接纳的回执内容 |

配置集合和文件等逻辑集合按稳定键排序，Schema、投影、复合键保留顺序。精确整数、decimal 字符串、UTC 时间、1 MiB 内容和深度上限延续 canonical-v1 的编码算法。凭据只有引用，不读取 Secret。

`CanonicalJson.decode` 严格校验 UTF-8、大小/深度和安全类型，拒绝重复键、非规范字节、null、浮点、YAML/tag/alias。`ObjectDigests.restoreCanonical(bytes, trustedDigest)` 再核对 SHA-256、固定 envelope 与协议/kind 白名单，返回不可变字节。**读取解码不自动恢复为已验证领域对象**：恢复调用方仍需按类型构造和交叉核验；可信 expectedDigest 必须来自已接纳记录，不能从待核验对象自取摘要形成循环证明。

## 3. StagePlan 与提交证据交叉核验

[FileStageProtocol](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/FileStageProtocol.java) 只覆盖 P0 的 FLINK 文件阶段 COLLECT/COMPUTE；不把 WRITE、发布或 INLINE 混成同一协议。

1. Plan 固定受理后的 run、release、完整 catalog/runtime、ResolvedRunSpec 与消费快照摘要。计划生成时间不能早于受理，阶段截止不超运行截止；恢复不重置计时或版本。
2. COLLECT 只包含自己的一个输入。Schema、批次、时区、投影和生成的过滤摘要必须匹配已授权定义；源令牌覆盖采集期限。整数/日期范围使用连续半开区间，拒绝空洞、重叠和重复空分片；nullable 分片列必须有一个显式 NULL 分片。
3. COMPUTE 必须包含 ResolvedRun 的全部输入。每个输入来自同 run/alias/定义摘要的 COLLECT 清单，保留期有效；分片一一对应清单的显式文件集合，文件还须属于生产清单的 attempt 数据前缀，拒绝目录通配符或跨运行文件。合法空清单也需要显式完成分片。
4. 预期 writer 集合由固定 sink 和 parallelism 生成，与输入数量及分片数量独立。核对计划、原生接纳集合和 writer 结束集合完全相同，包括零输出 writer；不根据文件数量猜测 writer 数量。
5. 计划、可信外部观察、Evidence 和 AcceptedCommits 的 stage/attempt/CR UID/Job ID/三个摘要必须一致。合法文件须属于精确数据前缀及原生接纳的 task attempt，文件身份集合与原生最终提交集合相同；重复 key/提交身份、缺失/额外 writer 或计数不匹配均拒绝。不能以最大 attempt 编号选胜者。
6. 完成的逻辑 split 集合必须与计划完全相同；逐项核对源版本、读取计划摘要及独立输入控制总数。文件 SHA-256 只证明内容身份，不能代替源端完整性证据。
7. Manifest 必须同时具有核验后的 CommitEvidence、匹配的持久 SUCCEEDED 终态引用及完整 VerifiedFile 集合；逐文件 Schema 和 COLLECT 总行数必须正确。COMPUTE 的 Join/聚合结果行数允许与输入不同，不用“输出等于输入行数”判断计算正确。
8. 已构造清单/回执仍需通过 ReceiptGate/SealRecovery 的当前资格判断和元数据库事务 CAS；取消或旧 attempt 只归档，不能直接推进或开放下游。

`AcceptedCommits` 必须由 T03 规定的成功原生提交通道生成；这里没有接入真实 FileSink/Source。`ManifestRef` 必须来自已接纳 SEALED 清单的实际读取，`StoredTerminal` 须先按可信摘要实际持久化并读取，`VerifiedFile` 须来自实际文件核验。类型构造器验证的是字段关系，无法证明调用方已完成 I/O、数据库封存或源端承诺。不能把合成测试中的这些事实升级为运行事实。

## 4. 本地不可变元数据存储

[LocalImmutableObjectStore](../../src/main/java/io/github/xuxiaotuan/magicflink/connectors/LocalImmutableObjectStore.java) 在专属本地 POSIX 目录中实现实际 I/O；S3 URI 只是受控逻辑对象键，映射为 `root/bucket/path`，不会访问 S3。接口只提供 `putIfAbsent(key, Frozen)` 和 `get(key, trustedDigest)`，没有覆盖、删除或列目录选文件接口。

写入协议：

1. 根和所有存储子目录必须是非符号链接的 0700 目录，不修改既有目录权限；对象键必须位于批准的 bucket/prefix 且规范化，禁止越界、query 和路径别名。
2. 在同一根的 `.pending` 中创建唯一 0600 文件，完整写入最多 1 MiB 内容并 `FileChannel.force(true)`，再改为 0400。
3. 以 `Files.createLink(target, temporary)` 将完整 inode 发布为固定目标名。不存在则创建，已存在则完整读取并逐字节比较：相同返回 ALREADY_EXISTS，不同报 ConflictException，绝不覆盖。
4. 对发布目录执行 force 后确认成功；最后只删除本次唯一临时名。发布后的对象不删除。目录/硬链接能力不足时失败，不退化为 HEAD→PUT 或可覆盖 rename。

本机测试验证了硬链接不覆盖、目录 force、跨实例/跨 JVM 竞争及完整对象回读。这使用 Java 的 [Files.createLink](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/file/Files.html#createLink(java.nio.file.Path,java.nio.file.Path)) 与 [FileChannel.force](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/channels/FileChannel.html#force(boolean))；API 和本地测试不构成任意文件系统或断电持久性承诺。

| 故障窗口 | 结果与恢复 |
|---|---|
| 发布前写入失败 | 无可见正式对象；仅清理本次临时名，按同内容重试 |
| link 成功后确认丢失/force 失败 | 返回 IOException，调用方按 UNKNOWN 处理；重试同键同内容或 GET 核验，不换 ID/内容 |
| 同键不同内容并发 | 恰有一个内容创建成功，其他冲突或同内容幂等，不覆盖胜者 |
| GET 正式路径确实不存在 | Optional.empty；仅 NoSuchFileException 表示当前不存在 |
| 权限、类型、读取或目录检查错误 | IOException；UNKNOWN，不解释为缺对象 |
| 摘要/规范内容/协议不符 | CorruptObjectException；拒绝，不修复覆盖 |

真实进程崩溃可能遗留 `.pending` 文件，后续读取忽略它们，不自动删除其他写入者文件。没有以 kill/断电测试证明所有硬件持久性窗口；没有网络超时仿真、NFS/S3 一致性或生产对象锁保证。根目录须由服务独占受控；静态 symlink 检查不防御拥有同一 UID 或 root 权限的进程在校验后恶意替换目录。

## 5. 本地测试与验收层级

新增测试在既有离线构建中运行，不安装依赖、不启动集群、容器或数据库。存储使用 JUnit 临时目录，跨进程测试仅创建两个受限的短生命周期测试 JVM，不操作既有项目进程。

| 测试 | 用例数 | 证据 |
|---|---:|---|
| PlatformDefinitionsTest | 7 | 固定版本、凭据引用、权限与共享额度、Schema/投影/批次、完整目标比较、状态表与必需规则、资源期限负例 |
| FileStageProtocolTest | 15 | 正常 COLLECT/COMPUTE、自洽但错误批次/漏输入、失败 attempt、外部身份/前缀、缺漏/替换分片、版本过期、NULL、零输出 writer、文件行数/Schema、持久终态要求、本地证据链重开 |
| LocalImmutableObjectStoreTest | 13 | 创建/幂等/重开、冲突不覆盖、发布前/后注入 IOException、损坏与读取错误、父目录/对象 symlink、范围限制、多线程及两个独立 JVM 争写 |
| CanonicalRestoreTest | 9 | 重复键/顺序/空白/非 JSON/浮点/null/tag/alias 拒绝、UTF-8、未知协议、错误摘要、不可变读取 |
| 原有 YAML/SQL/摘要/恢复 | 126 | 全量回归，旧 v1 黄金向量保持 |

四处新增负例取得真实语义 RED：缺持久成功终态仍可生成成功清单；清单文件与分片一致但来自其他运行目录；源投影遗漏登记必需规则列；URI.normalize 保留的根部 ../ 被误认为规范对象键。修正后全量 package 通过；缺 API 导致的初始编译失败不作为行为验证。

这批验收证明领域关系、实际本地不可变元数据 I/O 及本地竞争语义；不证明文件内容实际通过 Parquet 扫描、源数据一致、原生 committable 可闭合、数据库 SEALED/CAS、首次发布、资源扣还或 F01–F13 运行 PASS。P0 总体仍为 evidence_incomplete，环境准入状态不因本批代码改变。

最终验证：2026-10-18 17:04:17（Asia/Shanghai），使用已有 Java 17.0.14 与 Maven 3.9.16 执行 `mvn -o package`，退出码 0，**170 tests / 0 failures / 0 errors / 0 skipped，BUILD SUCCESS**。命令及堆配置沿用 [平台合同](platform-contracts.md)第 5、8 节；原始日志在忽略目录 `target/t04-final-package.log`。普通应用 JAR 含 97 个应用 class，不含 Flink/Calcite/测试类，尚不是 T05 fat JAR。Markdown 本地链接与 `git diff --check` 通过，设计 v0.2 摘要保持不变。本批没有安装、部署、暂存、提交或推送。
