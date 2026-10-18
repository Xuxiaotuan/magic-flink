# T04 元数据库 CAS、首次发布与取消授权边界

2026-10-18，依据设计 v0.2 第 7.2、8.4、10.4、11.3 节。本批交付 JDBC 事务实现、表约束及不依赖集群的文件数据库测试，不安装或部署。PostgreSQL 仍是目标实现路线，H2 仅作测试引擎；本地通过不替代 F12 的真实 PostgreSQL 验收。

## 1. 代码与信任边界

| 入口 | 本批职责 |
|---|---|
| [TransactionContracts](../../src/main/java/io/github/xuxiaotuan/magicflink/control/TransactionContracts.java) | 规范业务键、固定发布请求/授权/回执、必需检查及运行登记；精确阶段身份的编码/恢复和内容摘要 |
| [JdbcRunStore](../../src/main/java/io/github/xuxiaotuan/magicflink/connectors/JdbcRunStore.java) | 元数据库事务：当前资格+revision、清单封存/阶段完成/下一意图、检查接纳、取消、发布授权及成功补记 |
| [JdbcPublicationStore](../../src/main/java/io/github/xuxiaotuan/magicflink/connectors/JdbcPublicationStore.java) | 目标事务：首次指针初始化、版本/摘要核对、revision CAS、唯一历史、同 ID 重送与一致快照观察 |
| [JdbcTransactions](../../src/main/java/io/github/xuxiaotuan/magicflink/connectors/JdbcTransactions.java) | 每次操作独占借用连接，READ_COMMITTED、显式 commit/rollback、语句超时、失败抛出；不自动重放事务 |
| [metadata.sql](../../src/main/resources/db/metadata.sql) / [publication.sql](../../src/main/resources/db/publication.sql) | 两库各自的最小 P0 协议表；应用没有启动时自动建表或迁移逻辑 |

两个 Store 使用分别配置的 DataSource，不共享 Connection，不开启跨库事务，不调用 Flink、对象存储、HTTP 或另一侧 Store。SQL 表名固定为 mf_ 前缀，参数使用 PreparedStatement；调用方必须使用批准的独立 Schema/账号并连接主库。当前没有新增生产 JDBC 驱动；驱动和 DataSource 超时/池配置由后续固定发行版提供。

Registration 是平台可信状态的登记入口。本批可用 COLLECTING 或 RECONCILING_TARGET 等指定阶段的合成 fixture 验证局部协议；这不是向用户开放的“跳过前序阶段”API，也未实现完整生命周期协调器。真实服务必须从已接纳的 ResolvedRun、阶段与检查证据产生登记/状态，不允许用户或回调直接构造它。

Manifest、CheckProof、AuthorizedPublication、PublicationObservation 都是内部类型，不能充当真实性或授权的密码凭证。调用方须先实际读取、核验相应不可变产物或目标历史；mf_stage 的身份摘要不能代替源端/文件核验。已有 [领域与存储合同](domain-and-local-storage-contracts.md) 的生产来源义务继续适用。

## 2. 元数据库的事务 CAS

每个修改先 `SELECT ... FOR UPDATE` 锁定 run，再比较调用方 observedRevision。不同则返回 REREAD，调用方重读后再核对资格；不会把另一个输入的正常完成误判为本输入失效。实际状态修改还使用 `UPDATE ... WHERE revision=? AND cancel_requested=FALSE`，必须恰好修改一行。所有 Store 入口都遵守先 run 后阶段/意图的顺序。

阶段身份包含逻辑键、attempt、执行器、CR UID/Job ID、plan/spec/consumed 三个摘要，完整规范内容及摘要同时保存；重开后可通过 currentStages 恢复，并校验内容、摘要与 SQL 逻辑键一致。身份损坏明确报错。

### 文件封存

sealFile 接收已完成交叉核验的 Manifest，并在同一事务内：

1. 比较 revision、当前精确身份、固定定义摘要、阶段资格和 cancelRequested。
2. 核对同阶段已有封存清单；同 ID/内容返回 ALREADY_APPLIED，冲突拒绝，不覆盖。
3. 尚未封存的文件必须仍在保留期内；过期返回 NOT_READY，不创建下游。
4. 写 SEALED 清单引用，完成该当前阶段。
5. 该阶段组仍有输入未完成时保持 COLLECTING；全部完成后才固定消费清单摘要、写唯一下一逻辑意图并推进到 PRECHECKING。COMPUTE 对应推进到 VALIDATING_RESULT。
6. 增加 revision，追加审计事件，一起 commit。

下一意图只记录唯一逻辑键与消费摘要，不是可直接发给 Operator 的完整 StagePlan。后续协调器仍须生成、持久化并验证下一计划，再派发外部副作用。清单先写对象存储再进入本事务；事务失败时其孤立对象不具备下游资格，不删除或改写它。

两个输入同时以旧 revision 完成：一个 APPLIED，另一个 REREAD；重读后第二个仍可完成。SEALED、阶段完成、下一意图或 revision 任一写入失败则全部回滚。已取消或旧身份的证据只追加审计，不接纳清单/检查，不改变当前版本。

### 检查与授权准备

RECONCILING_TARGET 登记时固定 canonical businessKey、loadId、candidateDigest、expectedRevision 和完整必需检查 ID→规则摘要。recordCheck 验证当前 TARGET_CHECK 身份、规则、候选和 load，追加不可覆盖的结果/证据引用；同身份/检查同内容幂等，异内容冲突。只在每项固定规则均 PASS 时完成目标检查阶段。FAIL、ERROR、SKIPPED 和缺项均不能授权。相同尝试不覆盖失败结果为 PASS；完整检查重试/更换尝试协调仍待后续接入。

## 3. 取消与持久授权的截止点

cancel 和 authorize 使用同一 run 行锁与 revision CAS。authorize 不调用目标；它只在本地事务中读取全部当前 PASS 证据，固定唯一 publicationId、完整请求摘要和 authorizedAt，将 phase 改为 PUBLISHING。新授权还必须在固定 run deadline 之前。

| 先提交者 | 后续行为 |
|---|---|
| 取消 | 原子设置 cancelRequested 与 CANCEL_REQUESTED，后续授权 ARCHIVE_ONLY；不存在发布意图；迟到证据只归档 |
| 发布授权 | 后续取消返回 CANCEL_TOO_LATE，只追加审计，不能设置取消标记或撤销原意图；即使目标调用尚未发生也一样 |
| revision 竞争失败 | REREAD，不擅自认定取消/授权成功，重新读取后处理 |

每个 run 最多一份授权意图，publicationId 全局唯一。同 run/同 ID 重送返回原意图资格；换 ID 冲突，跨 run 复用 ID 的唯一键错误在事务回滚后返回 CONFLICT。SQL/commit 错误均不表示“没有授权”；须读取原意图判定，不能另造 ID 重发。授权后即便 deadline 到期，也继续核对固定意图，不回退为取消或未发布。

本批只受理取消并阻止新副作用，不返回 CANCELLED。活动作业停止、在途 CR 派发归结、连接/Writer 关闭及配额释放没有实现；不能凭 CR 不存在或本地 future 结束宣布取消完成。

## 4. 首次发布、并发与历史幂等

businessKey 按 tenant、平台注册的规范物理目标和类型化批次编码；不包含 jobId/jobVersion。DECIMAL 批次去除无意义 scale 并归一零值，防止 1.0/1.00 绕过同一批次互斥；string/date/integer/decimal 保持类型区别。物理目标别名的归一仍由平台注册映射负责，不能把任意用户数据集名当作规范身份。

readRevision 只有成功查询且确认无指针行时返回 0；查询、权限、锁等待、连接或 commit 错误直接抛出，不降级为首次发布。

publish 仅允许 Service 发送从元数据库实际读出的 AuthorizedPublication，按以下目标事务执行：

1. 按 publicationId 查询已提交历史；完全相同请求返回旧回执，不重新切指针。相同 ID 不同内容冲突。
2. 插入 revision=0、activeLoadId/publicationId=NULL 的初始指针，`ON CONFLICT DO NOTHING`；随后锁定规范 businessKey 的指针行。
3. 获锁后再次查询 publicationId 历史，处理相同请求并发重送，避免将自己的成功误判为 revision 冲突。
4. 按 pointer→version 固定顺序锁定目标版本，核对 ACCEPTED、业务键、loadId 与候选摘要；比较固定 expectedRevision。
5. revision 相等才切换 activeLoadId、revision+1、publicationId，并插入唯一发布历史；初始化指针、切换和历史一起提交或回滚。冲突以异常退出事务，不遗留首次空指针。

两个不同 ID 首次竞争最多一个成功。相同 ID 并发重送返回同一不可变回执。若不同 businessKey 误用同一全局 ID，历史唯一键使败方全部回滚，再用新事务读取胜者历史作身份核对；不保留败方指针。

目标已经推进到下一版本时，旧 ID 重送仍返回旧成功历史，不覆盖新结果。observe 将本 ID 的历史和当前指针放进同一条 SQL 快照，另报 currentEffective；false 不抹去旧成功事实。没有历史只返回空观察，表示发布结果尚不能确认，不表示旧事务已停止，也不能与另一次 readRevision 拼成确定 SUPERSEDED。锁定事务中的确定 CAS 冲突才属于可靠冲突结论。

PostgreSQL 对 READ COMMITTED 和 ON CONFLICT 的语义见[官方事务隔离文档](https://www.postgresql.org/docs/17/transaction-iso.html)及 [INSERT 文档](https://www.postgresql.org/docs/17/sql-insert.html)。本批 SQL 采用这些原语；H2 仅支持兼容模式的一部分语义，仍须原版本 PostgreSQL 重跑并发与故障场景。

## 5. 两库未知结果与成功补记

commit 返回异常时，事务可能已经提交。JdbcTransactions 会尝试 rollback 并保留异常，rollback 不能证明此前 commit 没有成功；方法不会在内部无限重试或换身份。

| 窗口 | 恢复方式 |
|---|---|
| 元数据库授权事务未提交 | 读回确认无意图后重新判断资格；若取消已胜出则禁止授权 |
| 元数据库授权已提交但确认丢失 | 按 run 读回同一 publicationId/请求/authorizedAt；取消过晚，恢复发送或核对原请求 |
| 目标事务已提交但确认丢失 | 以原意图 observe/重送；唯一历史返回原回执，不产生第二次替换 |
| 目标成功、元数据库尚未补记 | 用同一目标快照观察，confirmPublication 核对原意图和回执，并同事务写成功结果、SUCCEEDED、revision 与审计 |
| 成功补记提交后确认丢失 | 重读 run/结果，同一回执幂等补记；不能重新发布或改历史 |
| 目标已有更晚发布 | 本次旧历史仍可补为 SUCCEEDED，审计标记历史成功；currentEffective 仅代表观察时刻 |

元数据库从未获得目标观察时保持 PUBLISHING，发布结果须继续按 UNKNOWN 处理；不会因异常置 CANCELLED 或擅自更改 expectedRevision。recordedAt 是应用在目标事务内写入的事实时间，不冒充数据库物理 commit timestamp/txid。网络连接/commit 超时由受控 DataSource 配置；每条语句有超时，不以客户端超时宣称服务器已停止。

## 6. 本轮可复现证据

复用已有 Java 17.0.14、Maven 3.9.16 和本地 H2 2.4.240 缓存，POM 仅增加 test scope。测试在 JUnit 临时目录中创建两个独立文件数据库，分别表示元数据与目标；不访问既有数据库/容器，不启动 TCP 数据库服务，不需要 Kubernetes。关闭连接后重开 DataSource 验证持久内容，而非用内存 Map 模拟事务。

| 测试 | 用例数 | 关键行为 |
|---|---:|---|
| JdbcRunStoreTest | 19 | 封存/下一意图原子回滚、两输入 CAS、取消迟到回执、全部规则与版本约束、三个非 PASS 状态、锁超时、取消/授权两种次序与独立连接竞争、授权前后提交失败、阶段身份重建、过期清单、全局 ID 冲突、两库丢确认恢复和历史成功补记 |
| JdbcPublicationStoreTest | 8 | 首次发布、两首次竞争、同 ID 并发、版本拒绝与历史写失败回滚、目标丢确认、不同内容 ID 冲突、查询错误、跨业务键同 ID、旧成功不回滚新结果 |
| TransactionContractsTest | 4 | 规范业务键、独立请求黄金向量、完整外部身份恢复、必需授权字段与回执 revision 约束 |
| 原有核心 | 170 | YAML、SQL Planner、摘要、文件协议、本地存储与恢复回归 |

测试实际执行 JDBC/SQL/锁/提交。故障注入只在测试 DataSource 包装器中：真实 commit 后抛出连接错误，或发送 commit 前拒绝；它保留真实数据库副作用，不替代业务判断。另通过真实数据库约束使清单下一意图、目标历史写入失败，验证所有前序更新回滚。

最初缺 API 的编译 RED 不作为行为证明。后续得到真实失败并修正：过期清单仍可封存并创建下游；跨 run 复用 publicationId 只抛唯一键错误而没有返回确定冲突。请求摘要的 Python json/hashlib 独立黄金值为 `5c94d49a230375f306b613f5f5875cf8ba9abfdb5cb430925f3db25637cb24f3`。

## 7. 剩余验收

本批是“JDBC 协议代码 + H2 文件数据库局部验证”。仍未完成 PostgreSQL 的行锁等待/死锁/约束、真实网络中断/进程崩溃和持久性实验；H2 PostgreSQL mode 不等于 PostgreSQL。F12 整项仍 NOT_RUN，T04/P0 整体仍 evidence_incomplete。

当前 mf_stage 保存各逻辑阶段的当前身份；完整历史尝试追加、更换尝试/停止确认、下一计划物化与派发、受理 API、规则合并/执行和全局配额扣还仍须后续协调器接入。目标测试只预置 ACCEPTED 元数据，不证明版本数据写入、数据库侧封存防迟到写、逐字段 row-compare 或正式视图已实现。这些保持 P1–P3 的原验收要求，不以本批局部测试降低标准。

SQL 文件不会自动执行；本轮仅由测试装载到专属临时 H2 文件。没有修改既有数据库，未安装、部署、暂存、提交或推送。

最终构建：2026-10-18 17:26:12（Asia/Shanghai），沿用既有 Java/Maven/堆限制执行 `mvn -o package`，退出码 0，**201 tests / 0 failures / 0 errors / 0 skipped，BUILD SUCCESS**；原始日志在忽略目录 `target/t04-transaction-package.log`。普通 JAR 含 118 个应用 class 和两份 SQL 资源，不含 Flink/Calcite/H2/测试类。Markdown 本地链接和 `git diff --check` 通过，v0.2 设计 SHA-256 仍为 `fff41e1fbe958dbec7c1490c2a76633dc72d60ea9077957771c4c042f8818291`。既有 Calcite 可选注解和 SLF4J NOP 警告仍在，不影响本次构建；发行版日志/依赖闭包仍须 T05 验证。


后续补充：本页为第四批历史。当前尝试历史/安全切换、停止确认、CANCELLED 资格和下一阶段意图的持久恢复已按 [第五批合同](attempt-history-and-recovery-contracts.md)实现并本地验证；真实外部执行器 fencing/停止、PostgreSQL 故障和完整 Service 仍未验收。Registration 新增必填 RetryPolicy/TimeLimits，元数据建表合同相应更新，未对既有数据库执行迁移。
