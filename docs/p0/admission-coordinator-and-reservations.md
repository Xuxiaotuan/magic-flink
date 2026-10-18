# T04 首个 COLLECT 受理、协调器恢复与资源预留

2026-10-18，依据[设计 v0.2](../design/flink-yaml-batch-service-design.md)第 8.3、11.3、12、14 节，延续[尝试历史与恢复合同](attempt-history-and-recovery-contracts.md)。此前只能导入已有执行身份，再恢复下一阶段意图。本批增加没有外部执行身份的首批计划受理，以及使用持久事实推进一步工作的协调器。范围是本地协议代码，不安装、部署或连接既有服务。

## 1. 模块与持久对象

| 模块/表 | 职责 |
|---|---|
| [JdbcCoordinator](../../src/main/java/io/github/xuxiaotuan/magicflink/connectors/JdbcCoordinator.java) | 完整首批计划受理、租约、按受理顺序扫描、恢复工作列表、单步推进、预留与释放 |
| [CoordinatorContracts](../../src/main/java/io/github/xuxiaotuan/magicflink/control/CoordinatorContracts.java) | 平台 BudgetPolicy、Lease、Work、DispatchPermit、Reservation、Usage、Snapshot、Step |
| [FileStageProtocol.validatePlan](../../src/main/java/io/github/xuxiaotuan/magicflink/protocol/FileStageProtocol.java) | 复用原计划/配置/批次/Schema/读取策略核验，不以虚构执行身份触发校验 |
| mf_admission | 受理时间、完整首批计划引用、对象摘要、冻结资源需求与上限；摘要同时绑定 mf_run.registration_digest |
| mf_coordinator_lease | run、实例 owner、递增 epoch、到期时间；不代表外部执行器已被隔离 |
| mf_control_guard | 唯一配额事务锁和显式平台策略；没有策略则不派发 |
| mf_physical_connection | 物理连接 ID 与规范 endpoint 一一对应；跨运行、配置版本与逻辑别名复用 |
| mf_reservation | 复合尝试键、不可变申请量与摘要、释放标记和可靠停止证据摘要；历史不删除 |
| mf_run_slot | 首次派发取得活跃运行名额，阶段间保持占用，终结并可靠停止后显式归还 |

继续复用 mf_run、mf_attempt、mf_stage、停止证据和下一意图。不另建 MQ、调度服务、通用工作流框架或内存配额计数器。元数据 DDL 是尚未部署的建表合同；只有测试加载到专属临时 H2 文件，不能直接对旧表推测回填历史或配额。

## 2. 首批 COLLECT 如何受理

`admit(ResolvedRun, List<Plan>)` 是可信平台内部入口。业务用户仍只写 YAML；不能直接提交运行身份、资源量、凭据或任意计划。这一步要求所有声明输入均已有匹配的就绪事实和固定源版本快照，不执行就绪查询，也不将类型化 readinessDigest 当作真实来源证明。完整 WAITING_INPUTS API、探测调度与探测连接的实际扣还仍待接入。

受理顺序固定：

1. 核对输入别名集合完整且唯一，每份均为本运行自己的 COLLECT。复用完整计划验证，核对版本、批次、读取过滤、投影、Schema、runtime、截止时间和原配置摘要。
2. 从受控 RuntimeProfile 和 DatasetDefinition 推导资源量。每个 COLLECT 需要一个 Application 和该源 `read.maxConnections` 的最坏连接数；不接受调用方提供的资源量覆盖。
3. 在私有本地不可变存储写入 `runs/<run>/job.json`、`resolved.json`、`catalog.json` 和各尝试的 `plan.json`。完整作业定义保存 SQL 和参数声明，ResolvedRun 保存绑定值，Catalog 保存固定连接/数据集/运行档位。先有对象，再有数据库指针。
4. 取得 guard 锁，登记/核对物理 endpoint，单事务写运行、受理合同、初始租约行，以及每个输入的 ordinal=0 历史与当前指针。全部尝试为 FLINK/PLANNED，CR UID、Job ID、dispatchId 都为空。
5. 同内容重复受理不创建第二组尝试；不同内容不能覆盖原不可变对象或原登记。事务失败整组回滚。对象已写、数据库未提交时只留下未引用对象，不产生可派发运行，本批不自动回收这些对象。

受理时间在合同与数据库均使用项目规范的 UTC 纳秒格式，保证摘要比对和受理顺序一致。计划/运行已过期拒绝首次登记；重复已提交请求按原合同恢复，不另起计时。受理不扣 Application 或活跃运行额度；资源等待不产生外部作业。

## 3. 协调器如何重启接管

`pendingRuns(limit)` 以规范受理时间、runId 排序，最多返回 1000 条非终结运行。调用方逐条获取租约并调用有界 `step`；本批没有启动后台服务或线程。

`acquire` 在 run 锁下取得 1–60 秒的有限租约。未到期的另一 owner 无法接管；同 owner 重读恢复原租约。到期接管增加 epoch。`renew` 要求 owner、epoch、原 expiresAt 完全匹配且尚未过期；续租后的旧 Lease 不再有效。owner 应是实例启动时生成的唯一身份，不能让两个并发实例共用同一个 owner。

`recover` 在同一运行事务中重读受理、当前指针、历史定义、下一意图、停止证据和预留，并回读完整不可变对象。对象缺失、摘要/协议/角色错误、当前计划内容属于旧 attempt，均拒绝产生派发许可。数据库读取与对象 I/O 分开，I/O 后重新检查 revision/租约；状态变化要求重读，不把旧快照升级为许可。

| 恢复事实 | Work 与单步行为 |
|---|---|
| PLANNED、无旧派发 | DISPATCH；检查完整计划、期限和额度，事务登记成功后才返回一次 DispatchPermit |
| 已登记 dispatchId，尚未停止 | OBSERVE；沿用确定性 dispatchId 和资源名，恢复读取不返回第二张许可 |
| 取消或期限到达，停止未知 | STOP；交给可信适配器排空派发、确认执行终态与连接关闭，不自动释放额度 |
| 可靠停止且仍占阶段资源 | RELEASE_RESOURCES；先归还已确认可释放的历史/当前额度，再考虑新派发 |
| 成功终态但提交证据未封存 | RECOVER_EVIDENCE；保留原窗口、原 attempt，不凭进程成功生成清单 |
| 失败且已停止、尚未接纳完成 | RETRY_OR_ATTENTION；错误分类、预算和替换计划仍走既有安全重试协议，不自动造新 ID |
| 下一阶段意图已落库、尚未物化 | MATERIALIZE；返回原意图，需要计划构建器按原消费集合生成并写完整计划 |
| 已有发布授权、尚未补记成功 | RECONCILE_PUBLICATION；按原 publicationId 回读/幂等协议，不因取消撤销授权 |
| CANCEL_REQUESTED 且所有停止事实齐全 | FINISH_CANCELLATION；复用既有核验事务置 CANCELLED |
| 运行终结、阶段预留均已释放、所有尝试已停止 | RELEASE_RUN；显式归还活跃运行名额 |

`step` 至多提交一次状态变化。观察、停止、补证据、计划物化和目标核对返回工作类型，由后续受控适配器处理。本批没有实现检查→COMPUTE→WRITE→TARGET_CHECK 的完整阶段转换，也没有以空回调宣称工作已经执行。

## 4. 配额如何原子扣还

BudgetPolicy 由平台配置，必须给有限的活跃运行数、Application 数、INLINE 检查并发和物理连接额度。配置第一次持久化后只接受同内容幂等重放，在线调大/调小需独立受控迁移。策略不得抬高已冻结 RuntimeProfile 与连接档位的上限。

查询可用连接数为 `total - probes`，派发不借用专留的 probes 部分。预留按物理 ID 计额；规范 endpoint 不能在另一个 Catalog 中换一个 ID 获得第二份额度。DNS 别名/代理背后是否同一物理数据库仍需平台注册时核实，本地 URI 比对不能替代该事实。

`dispatch` 的锁顺序为 guard → run；所有配额改变遵循同一顺序。在一笔事务内核对 lease、revision、当前尝试、取消、phase、期限、冻结需求与全局用量；全部满足后写活跃运行名额、完整尝试预留、确定性派发 ID 和 SUBMITTING/RUNNING 状态。任一资源不足则整体 NOT_READY，没有部分占额、派发状态或外部调用。

不维护独立 used 计数器。guard 串行化预留变更，从有效预留记录汇总使用量；缺失已派发尝试的预留、缺失活跃运行名额、释放引用与停止事实不符均报损坏，不能将缺行当作释放。每运行同时采集不超过 2 个输入，并受冻结输入并发和全局/物理预算共同约束。

同一复合尝试键只预留一次。仅首次 APPLIED 返回 DispatchPermit；ALREADY_APPLIED、REREAD、异常和接管均没有重发许可。旧 `JdbcRunStore.beginDispatch` 对本入口受理的运行返回 NOT_READY，不能绕过预留；旧 Registration 仅保留可信导入/原有局部协议测试用途。

`release` 必须读取并核验该历史尝试的完整停止证据，且与派发 ID/身份一致。UNKNOWN、查询超时、租约到期、CR 暂不可见、仅完成清单均不能释放。释放保留原需求和停止引用，重复请求不重复归还。阶段额度归还不等于活跃运行名额归还；后者必须在 SUCCEEDED/CANCELLED 且全部尝试可靠停止、全部预留释放后单独确认。

COLLECT 资源需求已经接通完整计划。COMPUTE、文件侧 PRECHECK/RESULT_CHECK 有保守的 Application/INLINE 分类，但其完整计划构建、执行器和阶段推进尚未贯通。WRITE/TARGET_CHECK 的 loadId、写入/回读连接并发合同未接入，明确拒绝派发；不猜一个连接数放行。物理探测、校验 I/O 池、HTTP 速率/流预算仍待适配器接入。

## 5. 故障窗口与恢复

| 窗口 | 处理 |
|---|---|
| 对象写入失败/冲突 | 不登记可运行指针；原对象不覆盖；错误不解释为对象不存在 |
| 对象成功、数据库提交前失败 | 数据库无半条运行/尝试，保留未引用对象；原请求可重放 |
| 受理 commit 确认丢失 | 回读原 run/首批计划合同，恢复相同尝试，不重新选择源版本 |
| 预留或阶段派发更新失败 | 活跃运行名额、全部额度与派发登记整体回滚 |
| 派发登记 commit 确认丢失 | 原 dispatchId 保持占额；接管只观察，不能重发；无法确认未创建则保持未知 |
| 租约到期/服务重启 | owner/epoch 接管；外部执行和额度不变，旧 Lease 不能登记新派发或释放 |
| 取消与派发竞争 | 同 run revision 串行化；取消胜出无派发许可，派发胜出保留预留并等待可靠停止 |
| 已停止、释放 commit 确认丢失 | 回读同一尝试释放标记和停止引用；不得多归还额度 |
| 元数据或不可变对象缺失 | 报 MF004/IOException 等证据错误，保持已有占额，修复来源后才能推进 |

数据库租约只约束本地协调事务。DispatchPermit 是可信内部返回值，不是安全令牌；外部适配器还必须核对原资源名称/UID/注解、禁止失联旧派发者继续发送并归结在途请求。当前使用可注入 Clock 验证时间条件；真实多节点时钟、PostgreSQL 时间源/锁等待/故障与 fencing 仍须独立验收。绝不以这里的租约或 DRAINED DTO 宣称外部作业已停止。

## 6. 本地验收与后续关口

[JdbcCoordinatorTest](../../src/test/java/io/github/xuxiaotuan/magicflink/protocol/JdbcCoordinatorTest.java) 使用临时 H2 文件数据库和私有本地对象目录，覆盖完整首次受理、缺输入/错过滤拒绝、对象冲突、数据库回滚/提交丢确认、租约续期/接管、恢复观察、一次派发许可、取消竞争、全局/物理预算竞争、停止未知、释放幂等和活跃运行名额。

额外的失败反例验证：作业定义未保存、已派发预留缺失、其他运行的预留缺失制造容量、新尝试引用旧计划内容、配额抬高冻结上限、跨 Catalog 改名物理 endpoint、重试计划更换冻结资源档位。先取得真实断言失败再补对应检查。初始缺 API 的编译失败不作为行为证明；故障注入按具体写事务选定提交点，不能在前置只读恢复 commit 就提前注入，冒充派发已提交。

本批使用现有 Java 17.0.14/Maven 3.9.16 与缓存，POM/依赖不变。最终构建证据见下文。本地测试不能替代 PostgreSQL/S3/Flink/Operator 的故障恢复和业务数据验收。T04/P0 总体仍为 evidence_incomplete，F01–F13 仍 NOT_RUN；本轮不暂存、提交、推送、安装或部署。


### 最终构建记录

2026-10-18 19:23:15（Asia/Shanghai），使用既有 Java 17.0.14/Maven 3.9.16，沿用 `-Xmx256m -XX:ActiveProcessorCount=2` 和既有 Surefire 限制，执行 `mvn -o package`，退出码 0，**256 tests / 0 failures / 0 errors / 0 skipped，BUILD SUCCESS**。原有 227 项全量回归，新 [JdbcCoordinatorTest](../../src/test/java/io/github/xuxiaotuan/magicflink/protocol/JdbcCoordinatorTest.java) 29 项。完整日志保留于忽略目录 `target/t04-coordinator-final-package.log`。

最终 JAR 含 149 个应用 class，以及更新的 metadata.sql/publication.sql；已确认 Coordinator/DispatchPermit 类存在，不含 Flink/Calcite/H2 或测试类。这仍是本地核心普通 JAR，不是 T05 单 fat JAR 或双入口贯通。既有 Calcite 可选注解与 SLF4J NOP 警告未作为通过证据，发行版依赖与日志装配仍由 T05 验证。

本地 Markdown 链接、修改/新增文件空白检查及 `git diff --check` 通过。设计 v0.2 SHA-256 仍为 `fff41e1fbe958dbec7c1490c2a76633dc72d60ea9077957771c4c042f8818291`。未连接既有环境，未安装、部署、暂存、提交或推送。测试中的额度数值仅用于验证协议，不改变 E01 的资源准入 NO_GO，也不授权真实运行。旧 Registration 导入不自动形成配额账本，不能与受管理派发混用来代表全平台用量；真实服务启用前需完成历史运行与额度迁移核对。
