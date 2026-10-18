# 基于 Flink 的 YAML 批处理服务设计

文档版本：0.2　日期：2026-10-18　状态：设计评审稿

适用范围：银行内部自研批处理服务。用户只提交 YAML，平台统一构建、测试和扫描 fat JAR，通过 Flink Kubernetes Operator 运行有界任务。

实现与运行证据：`evidence_incomplete`。本文规定目标行为、数据契约和验收条件；尚未创建实现工程、构建制品或运行端到端测试。文中的模块、API、配置格式和错误码均为拟议协议。

## 1. 目标与核心决策

系统接收一份 YAML，完成异构数据源采集、多源 SQL Join、结果验收和目标写入。一次运行的成功必须同时满足：输入可追溯、计算结束、必需检查通过、目标回读一致、发布状态可确认。

采用“先固定输入，再计算，验收后发布”的执行路径。HTTP、JDBC、BigQuery 和 ClickHouse 数据先采集为内部存储上的不可变 Parquet 文件，后续计算与对账只读取已封存的输入版本。计算结果也先保存为不可变候选结果，通过业务验收后再写入目标隔离区，完成写入核对后发布。

### 1.1 已确认约束

| 编号 | 约束 | 设计落实 |
|---|---|---|
| R01 | 独立自研 | SeaTunnel、Flink CDC 仅作参考，不以其 fork 或运行时为底座 |
| R02 | 用户只写 YAML | 禁止用户上传 JAR、脚本、类名和运行时插件 |
| R03 | 平台统一 fat JAR | 所有平台能力随一个应用发布包交付；新能力必须重新测试和扫描 |
| R04 | 有界批处理 | 每个源必须有明确的结束条件和可识别的数据版本 |
| R05 | 四类 Source | JDBC、HTTP、BigQuery、ClickHouse 分别实现和验收 |
| R06 | 多源 SQL Join | 使用 Flink Table/SQL；不自研 SQL 优化器 |
| R07 | preaction 确认准备就绪 | 分为采集前就绪检查和采集后输入检查 |
| R08 | postaction 确认一致性 | 分为候选结果业务验收和写入后完整性核对 |
| R09 | Flink Operator 管理作业 | 分布式阶段使用 Application；轻量检查不强制启动 Flink |
| R10 | 当前只做设计 | 不创建产品代码，不部署，不提交或推送 Git |

### 1.2 第一版默认决策

- 一个常驻 `BatchService`，负责 API、元数据和状态推进；一个 `BatchRunner`，负责采集、计算、分布式检查和写出。二者由同一个应用 fat JAR 的不同入口启动。
- 内部存储默认采用行内获准的 S3 兼容对象存储，数据格式为 Parquet；HDFS 作为后续存储适配，不在第一版同时维护两套提交实现。
- 元数据库及首个正式输出适配器默认采用 PostgreSQL。此选择是研发基线，不代表已获行内准入；实际环境不接受时，在编码前替换并重新验证事务和发布契约。
- 一份 YAML 声明多个输入、一条结果 SQL、一个正式输出。复杂计算用 CTE 表达，第一版不开放任意节点工作流或多目标原子发布。
- 第一版默认失败重跑整个未完成阶段，不追求所有源的断点续读；已经封存的输入和候选结果可复用。
- `replace-batch` 通过目标内的版本指针切换实现，下游读取正式视图；不采用“先删除正式数据，再插入新数据”。
- 具体 Flink、JDK、Operator、驱动和客户端版本在阶段 P0 冻结为一个组合，后续运行不能自行选择未验证组合。

### 1.3 暂不支持

持续 CDC、流式 Join、用户 UDF 上传、Python/Shell、任意 HTTP 回调、任意数据库写操作作为 action、自动 Schema 演进、跨库分布式事务、多个正式输出，以及新的调度平台。定时触发由已有行内调度系统调用本服务 API。

## 2. 为什么先固定输入

直接从四类源读取并写入目标，部署组件较少，但把源端变化、读取恢复、SQL 重跑、目标去重和对账绑定在一次作业中。一次失败后的重新读取可能得到不同数据，无法解释差异发生在采集、计算还是写入。

本方案将三类结果分别固定：输入文件、候选结果文件、目标隔离版本。每个阶段都能根据上一个阶段的不可变产物独立恢复，成本是增加存储、网络流量和阶段启动开销。

| 选择 | 收益 | 代价及边界 |
|---|---|---|
| 统一输入文件 | 四类源转换后共用 SQL、检查和重跑逻辑 | 额外一次写入和读取；需管理数据保留和访问权限 |
| 固定候选结果 | 目标故障时不重新采集或计算 | 多一份结果存储；大结果需评估空间与写入时间 |
| 单一 fat JAR | 每个版本的代码能力和依赖固定 | 包体较大，一个连接器升级也需重新发布 |
| 分布式阶段使用独立 Application | 隔离计算资源与故障 | 保留启动成本；轻量检查在 Service 有界执行，不将所有逻辑阶段部署为集群 |
| 目标版本发布 | 失败尝试不影响下游当前结果 | 下游必须使用正式视图，不能直接扫描隔离表 |

落文件只固定采集结果，不能修复采集期间已经发生的不一致。严格模式仍要求源端不可变批次、可复用快照或稳定导出。如果源端只提供不断变化的列表且无版本保证，平台拒绝严格运行，不把“读完了”解释为“读到了同一批数据”。

## 3. 模块与部署边界

### 3.1 部署单元

| 单元 | 运行方式 | 持久依赖 | 不负责的内容 |
|---|---|---|---|
| BatchService | 常驻 Deployment，同一进程包含 API 和协调器 | 元数据库、配置注册表、内部存储、Kubernetes API | 不无界收集明细，不执行用户代码 |
| BatchRunner | 分布式阶段由 Operator 启动 Application | 本阶段获准的源、存储或目标 | 不创建下一阶段，不决定整个业务运行成功 |
| 检查执行器 | 轻量检查在 Service；大数据检查使用 Runner | 固定输入、候选结果或隔离目标 | 不因检查重试重新执行写入 |
| 发布适配器 | Service 驱动受限目标操作 | 目标数据库中的版本与发布记录 | 不执行用户提供的 DDL/DML |
| 外部设施 | 行内现有 Operator、镜像仓库、数据库、对象存储 | 各自运维体系 | 不纳入自研计算引擎范围 |

控制关系：调用方将 YAML 交给 BatchService；Service 保存版本与状态，创建阶段 FlinkDeployment；Operator 管理作业；Service 核对作业和阶段产物，决定是否进入下一阶段。

数据关系：四类源进入 InputManifest 指定的输入文件；Flink SQL 读取这些文件并产生 CandidateManifest；Writer 读取候选文件并产生隔离目标版本；对账通过后目标发布指针指向该版本。

Operator 的 Application 模式为应用提供独立集群。本方案仅为采用 Flink 执行的阶段尝试创建 Application；逻辑阶段不与部署单元一一对应，一个业务运行可以包含多个 Flink Job。[Operator 部署说明](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.16/docs/deployment/overview/)

### 3.2 逻辑职责与工程边界

第一版采用一个工程、一个应用发布包，先按下表划分包内职责，不为每个模型、适配器或阶段建立独立 Maven 模块、服务或插件框架。本轮不创建工程代码。

| 包内职责 | 包含内容 | 边界 |
|---|---|---|
| `spec` | YAML 解析、参数声明与绑定、配置注册表解析、公共协议模型 | 不依赖协调器；冻结运行配置与计划 |
| `control` | API、运行状态、幂等受理、Operator 操作、恢复、配额与发布协调 | 一个持久化协调循环；不另建工作流引擎或消息队列 |
| `runtime` | Flink 构图、文件封存、SQL、检查规则、阶段证据 | Service 与 Runner 共用规则语义；大数据执行交给 Flink |
| `connectors` | 四类 Source、内部存储和一个 PostgreSQL 目标适配 | 仅保留实际需要的内部接口；编译期注册，不做动态插件发现 |
| `app` | ServiceMain、RunnerMain、发行清单与打包 | 聚合一个 fat JAR，入口决定职责和权限 |

模型与协议不依赖控制服务；适配器不反向调度下一阶段。待出现可证明的依赖隔离或独立测试需求，再拆分构建模块。

RunnerMain 只接受平台生成的阶段计划，角色限制为 `COLLECT`、`COMPUTE`、`CHECK`、`WRITE`。发布由 Service 控制，防止计算任务自行开放结果。轻量检查共用规则实现，在 Service 的有界执行池运行，不产生 FlinkDeployment。

### 3.3 单一 JAR 与运行镜像

应用代码、连接器、客户端与检查实现进入一个普通 shaded JAR；Flink 核心与 Planner 等运行依赖归属固定镜像，不重复打入应用包。Service 入口使用该镜像提供的已验证 classpath，避免检查服务与 Runner 使用不同 SQL Planner。

构建时合并必需的 SPI 资源，检查驱动发现、重复类和依赖冲突。对易冲突依赖是否 relocation 由实际冲突测试决定，不预先进行广泛重定位。应用包不包含凭据和具体业务 YAML。

应用 JAR 单一不表示镜像内没有 Flink 自带 JAR。扫描范围覆盖应用包、全部运行依赖和基础镜像。测试、扫描、部署绑定同一镜像 digest，发布后不能以相同标签替换内容。

## 4. 平台对象与数据契约

### 4.1 配置对象

| 对象 | 核心字段 | 变更规则 |
|---|---|---|
| ConnectionProfile | 类型、获准地址、凭据引用、连接限制 | 平台维护；配置版本固定，凭据可按既定轮换策略更新 |
| DatasetDefinition | connectionRef、物理资源、Schema、批次参数类型、就绪策略、读取策略 | 不可变版本；用户只能引用授权数据集 |
| OutputDatasetDefinition | 目标、Schema、业务键、比较字段、允许写入模式、正式视图 | 平台预创建；第一版不自动迁移目标表 |
| RuntimeProfile | releaseId、资源、并发、时间限制、存储范围、重试策略 | 每次运行固定具体版本 |
| JobDefinition | YAML 原文、参数类型/默认值、SQL、规则集、规范化摘要、创建者 | 定义修改产生新版本；只换运行参数值不改定义版本 |
| ResolvedRunSpec | JobDefinition 版本、参数、所有引用版本、固定运行时 | 受理后不可变；不含明文凭据 |

DatasetDefinition 必须声明日期、数字和字符串的类型，以及批次完成后是否不可变、允许为空、预计保留期。不能仅登记一个连接地址就宣称数据集具备可重复读取能力。

### 4.2 标识

| 标识 | 语义 |
|---|---|
| `jobId` / `jobVersion` | 作业定义及不可变版本 |
| `runId` | 一次业务运行；重跑阶段不改变它 |
| `businessKey` | 发布互斥范围：租户、输出数据集和已绑定批次，不含 jobId，以防不同作业覆盖同一目标 |
| `stageAttemptId` | 某个阶段的一次独立尝试，包含阶段类型与序号 |
| `manifestId` | 一个已封存输入或候选结果的清单 |
| `loadId` | 一次目标隔离写入版本，与 WRITE 阶段尝试一一关联 |
| `publicationId` | 一次发布意图及其目标回执的幂等标识 |

Flink 内部 task attempt 与平台 stageAttemptId 不同。平台换阶段尝试不允许把多个失败尝试的输出文件拼在一起；作业内 task 重试则由本阶段 Sink 的提交协议选择合法文件。

### 4.3 InputManifest

每个输入生成一个清单；所有输入完成后再生成 InputBundle，记录本次计算使用的全部清单 ID 与摘要。

| 字段组 | 必须记录的内容 |
|---|---|
| 身份 | manifestId、runId、inputAlias、stageAttemptId、协议版本 |
| 源端版本 | 数据集配置版本、业务批次、快照/导出/不可变批次标识、就绪证据引用 |
| 读取约束 | 列投影、绑定后的过滤范围、分片计划摘要、时区和 Schema 摘要 |
| 文件 | 显式文件列表、位置、字节数、行数、SHA-256；不使用目录通配符选取数据 |
| 内容 | 固定 Schema、总行数、分片完成情况、必要控制总数 |
| 生命周期 | 开始/完成时间、源端令牌有效期、文件保留截止时间、sealed 标记 |

文件摘要证明文件内容未变，不证明源端记录没有漏读。采集完整性需独立核对分片覆盖、游标链、导出清单或上游控制总数。缺少证明时不能把采集行数本身当成上游完整行数。

InputBundle 保留各源独立版本以及业务批次对应关系。相同业务日期不是全局事务快照；允许的版本偏差由数据集协议规定，不由平台自动猜测。

### 4.4 CandidateManifest 与 StageReceipt

CandidateManifest 在输入清单基础上增加：InputBundle 摘要、JobSpec/SQL 摘要、releaseId、输出 Schema、候选文件列表、结果行数。它表示计算产物已封存，不表示业务验收已通过。

StageReceipt 是可重建的观察记录，包含逻辑阶段键、stageAttemptId、执行器、计划摘要、固定数据版本、产物摘要和完成状态；Flink 阶段额外记录 CR UID、Flink Job ID 与终态。它不是文件提交的唯一事实来源，也不能凭一条回调推进运行。

文件阶段的事实来源为持久化的成功终态证据、完整 CommitEvidence 和不可变 Manifest，封存协议见第 7 节；检查阶段为对应版本的完整检查报告；目标阶段为目标版本状态和发布历史。Service 从这些事实重建缺失 receipt，再按第 8.4 节检查当前尝试资格。receipt 已存在但摘要不同属于冲突，不能覆盖；原始证据不足时不能重建“成功”。

## 5. YAML v1 规范

### 5.1 文档级规则

采用 UTF-8 单文档 YAML，顶层字段固定为 `version`、`name`、`parameters`、`inputs`、`preChecks`、`sql`、`postChecks`、`output`。拒绝未知字段、重复键、多文档、自定义标签、锚点/别名和 merge key。第一版限制原文 256 KiB、最多 32 个输入、64 条显式检查，平台可收紧，不能由用户上调。

`version` 只能为字符串 `v1`。名称和输入别名使用小写字母起始、后续小写字母/数字/下划线；最长 63 字符，保留 `result`、`params`、`system`，避免与临时表冲突。检查 `id` 在一份配置内唯一。SQL 引用输入别名，不能引用任意外部 Catalog。

列表中的字段名不得重复，`columns` 和 `inputs` 不得为空。列名按已登记 Schema 精确匹配，v1 不做大小写猜测或自动改名。检查 id 沿用名称格式。时长只接受正整数加 `s`、`m` 或 `h`，例如 `30m`；零、负数、无限等待和超出 RuntimeProfile 上限的值均拒绝。省略字段时才使用默认值，显式 null 不等于省略。

### 5.2 顶层与嵌套字段

| 字段 | 类型与必需性 | 语义 |
|---|---|---|
| version | string，必需 | 协议版本，固定 v1 |
| name | string，必需 | 作业逻辑名称；不决定数据库表名 |
| parameters | declaration map，必需，可为空 | 参数声明：type、required 和可选 default；运行值由创建运行请求提供 |
| inputs | map，必需，1–32 项 | key 为 SQL 表别名，value 为 input 定义 |
| inputs.*.dataset | string，必需 | 授权数据集引用，受理时固定其具体版本 |
| inputs.*.batch | string，必需 | 固定批次值或单个参数引用 |
| preChecks | list，可选，默认空 | 用户附加前置检查；系统强制就绪检查不可关闭 |
| sql | string，必需 | 单条查询；允许 WITH，禁止多语句和写操作 |
| postChecks | list，可选，默认空 | 用户附加候选结果规则；输出数据集的必需规则不可取消 |
| output.dataset | string，必需 | 获授权的输出数据集 |
| output.batch | string，必需 | 输出批次，类型匹配输出数据集声明 |
| output.mode | enum，必需 | v1 仅 replace-batch |

每个参数声明只允许 `type`、`required`、`default`。type 必需且限于 string、boolean、integer、decimal、date；required 省略时为 true。required=true 禁止 default，运行时必须提供；required=false 必须提供符合类型的 default，省略运行值时采用默认值。v1 不引入 nullable 参数。参数名以字母起始，后接字母、数字或下划线，最多 63 字符。

运行值只接受对应标量：string/date/decimal 使用字符串，boolean 使用布尔值，integer 使用整数。日期采用带引号的 ISO 日期并校验有效性；金额禁止 YAML 浮点数。decimal 的精度和 scale 在 JobDefinition 冻结的规则集中固定，并与数据集和 SQL 类型核对。禁止 map/list、显式 null、未声明参数和冲突类型。参数可用于批次绑定或 `job_params`；每个被引用的参数必须声明，并满足所引用数据集的类型要求。

`batch` 仅允许字面量或完整的 `${parameterName}`，不接受字符串内任意表达式。SQL 内不替换 `${...}`。需要查询参数时，平台注册只读单行表 `job_params`，以类型化字段引用；该名称额外保留且不能作为输入别名。JDBC 条件用绑定参数，标识符来自数据集定义；HTTP 参数按结构编码，禁止用户控制完整 URL。

JobDefinition 保存参数声明与默认值，JobRun 接受声明范围内的具体值。仅更换业务日期等参数值时沿用 jobVersion，创建新 runId；SQL、参数类型/默认值或检查规则变化才创建新 JobVersion。受理运行时完成默认值合并、类型校验和引用版本解析，将全部实际值及摘要冻结在 ResolvedRunSpec。运行请求不能覆盖数据集、SQL、检查、镜像或资源配置。本文 v1 尚未发布，本次修订不涉及已上线协议迁移。

### 5.3 完整示例

以下数据集、字段和检查类型构成设计示例。实现时，平台注册表必须先登记相应数据集及 Schema。

```yaml
version: v1
name: daily_orders

parameters:
  businessDate:
    type: date
    required: true

inputs:
  orders:
    dataset: orders_jdbc
    batch: "${businessDate}"
  customers:
    dataset: customers_bq
    batch: "${businessDate}"
  branches:
    dataset: branches_http
    batch: "${businessDate}"
  risk:
    dataset: customer_risk_clickhouse
    batch: "${businessDate}"

preChecks:
  - id: all_inputs_ready
    type: batch-ready
    inputs: [orders, customers, branches, risk]
    timeout: 30m
  - id: customer_key_unique
    type: unique-key
    input: customers
    columns: [customer_id]
  - id: branch_key_unique
    type: unique-key
    input: branches
    columns: [branch_id]
  - id: risk_key_unique
    type: unique-key
    input: risk
    columns: [customer_id]

sql: |
  SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    c.customer_name,
    b.branch_name,
    r.risk_level
  FROM orders o
  LEFT JOIN customers c
    ON o.customer_id = c.customer_id
  LEFT JOIN branches b
    ON o.branch_id = b.branch_id
  LEFT JOIN risk r
    ON o.customer_id = r.customer_id

postChecks:
  - id: result_order_key_unique
    type: unique-key
    columns: [order_id]
  - id: complete_order_keys
    type: key-set-equal
    expectedInput: orders
    columns: [order_id]
  - id: total_amount_equal
    type: sum-equal
    expectedInput: orders
    column: amount
    tolerance: "0.00"

output:
  dataset: enriched_orders
  batch: "${businessDate}"
  mode: replace-batch
```

同一 jobVersion 可绑定不同日期。以下是创建运行 API 的请求体（用 YAML 展示），不是第二份作业定义：

```yaml
jobVersion: 1
parameters:
  businessDate: "2026-10-17"
```

示例 Schema：orders 包含 order_id、customer_id、branch_id、amount；customers 包含 customer_id、customer_name；branches 包含 branch_id、branch_name；risk 包含 customer_id、risk_level。各字段类型由数据集定义固定，amount 为同币种 DECIMAL。enriched_orders 的业务键为 order_id，比较字段为 SQL 输出的全部六列。

在三个维度键唯一、订单键唯一且所有 LEFT JOIN 均不滤掉订单的前提下，示例的键集合与金额守恒规则成立。示例允许维度缺失，不声称验证所有关联成功；若业务要求必须匹配，应增加引用完整性规则或对明确的匹配标识列做检查。不能用可能合法为空的描述字段替代匹配标识。

### 5.4 检查类型字段

| type | 使用位置 | 必需字段 | 可选字段与默认值 |
|---|---|---|---|
| batch-ready | preChecks，采集前 | id、inputs | timeout 默认 30m，不能超过平台上限；轮询间隔由平台固定 |
| unique-key | preChecks 或 postChecks | id、columns；pre 还需 input | 无；键列空值直接 FAIL |
| not-null | preChecks 或 postChecks | id、columns；pre 还需 input | 无 |
| row-count | preChecks 或 postChecks | id、min、max；pre 还需 input | 无；0 ≤ min ≤ max，闭区间 |
| key-set-equal | postChecks | id、expectedInput、columns | 无；v1 要求两侧列名与类型一致 |
| sum-equal | postChecks | id、expectedInput、column | tolerance 默认十进制字符串 "0.00"，必须非负 |
| row-compare | 平台自动生成的写入检查 | 候选版本、loadId、业务键、比较字段 | 不开放用户覆盖比较范围 |

每种检查只接受表内列出的字段及 `type`。pre 的 input 必须存在；post 的实际侧固定为本次候选结果，不能引用历史正式表。`key-set-equal` 按集合比较，必须与两侧键唯一规则共同使用；输出业务键唯一由平台强制检查，输入键唯一由规则和数据集契约补足。

`sum-equal` 只支持精确数值列；遇到 NULL 值判 FAIL，不静默忽略。零行集合的和在平台检查语义中定义为精确零，是否允许空批次另按数据集契约判定。多币种必须先按业务规则转换或分组，v1 的全列 sum 不替用户推断汇率。

v1 不开放 `sql-assertion`、任意聚合表达式和动作脚本。上述固定规则覆盖首版；新增规则随应用版本发布，未来扩展协议时保持旧字段含义不变。

### 5.5 SQL 与类型规则

- 支持单条 SELECT/CTE、INNER JOIN、LEFT JOIN、过滤、GROUP BY 和获准的精确数值聚合；Join 必须具有至少一个等值连接条件，可附加过滤谓词。v1 拒绝 RIGHT/FULL/CROSS JOIN、纯非等值 Join 和无连接条件 Join；后续支持范围随协议和发行能力清单扩展。
- 输入均为有界、固定 Schema、insert-only 文件表，输出必须兼容登记的输出 Schema。计算只能访问 InputBundle 中的表和 `job_params`。
- Flink Planner 负责语法、名称和类型解析；平台校验解析后的对象、函数、操作类型和计划能力。不能只依靠正则或关键字过滤。
- 禁止用户提交 CREATE/ALTER/DROP/INSERT、ADD JAR、函数注册、Catalog 切换和任意 SET。合法查询产生的内部 DDL/INSERT 由 Runner 生成。
- 禁止随机数、外部副作用函数、依赖当前时间的非确定性表达式，以及无稳定全序的 LIMIT。需要执行时间时使用平台固定的运行参数。
- 金额使用约定 DECIMAL 精度；禁止隐式降为浮点，溢出或无法无损转换时失败。舍入必须在 SQL 中显式表达并纳入业务规则。
- 业务日期按数据集时区解释，时间点规范化为 UTC 并保留 Schema 语义；TIMESTAMP 与 TIMESTAMP_LTZ 不混同处理。浮点 NaN、Infinity 和复杂嵌套类型 v1 默认拒绝；需要时在采集映射中显式转换。
- 主键声明不等于数据唯一。验证前不把未经证明的唯一性作为优化器假设。

Table/SQL 的 Source/Sink 有规划层和运行层接口。本方案只在采集侧保留异构差异，计算侧统一为内部文件表。[Flink 自定义 Source/Sink](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sourcessinks/)

### 5.6 校验顺序与错误返回

顺序固定为：字节与结构限制 → 严格字段解析 → 参数类型与引用 → 身份和数据集权限 → 配置版本解析 → Schema 与 SQL 规划 → 检查字段与输出模式 → 资源能力检查。此时不执行数据采集或写操作。定义校验使用声明的类型构造 `job_params`，不要求用户为必填参数提供本次运行值；创建运行时再校验具体值、批次范围和授权，冻结 ResolvedRunSpec 并完成绑定后的能力检查。定义可规划不表示任意运行参数都合法。

错误至少包含 code、path、message、可选 line/column。示例：`SPEC_UNKNOWN_FIELD` 指向 `output.jarURI`；`SPEC_UNKNOWN_INPUT` 指向错误别名；`SQL_FORBIDDEN_OPERATION` 指向不允许的语句；`SOURCE_NOT_REPLAYABLE` 指向不具备稳定版本的源；`OUTPUT_MODE_UNSUPPORTED` 指向目标能力不足。

静态校验通过只说明配置可被当前版本规划，不表示真实数据、网络或运行容量已经验证。

## 6. 四类 Source 的采集契约

### 6.1 统一能力与阶段输出

Source 在注册时声明 `bounded`、`stableInputVersion`、`parallelRead`、`replayable`、`projectionPushdown`、`filterPushdown`。前两项为严格运行必需；replayable 表示能否按相同源版本重采，不能仅表示网络请求可以再发一次。

读取计划包含固定批次、Schema、投影列、过滤范围、分片策略、连接并发上限、版本令牌和过期时间。每个分片产生数据文件和完成信息。记录解析错误默认导致阶段失败，不静默丢行；坏记录只保存必要且获准的诊断信息。

第一版为每个输入独立提交 COLLECT 作业，最多并发两个输入。一个输入失败不需要删除其他已封存输入；复用前核对它们仍满足相同业务批次协议。采集阶段只执行无副作用读取或获准的数据集导出过程，不能由 YAML 注入任意上游修改动作。

### 6.2 HTTP

DatasetDefinition 固定服务地址、资源路径、鉴权方式、允许请求参数、JSON 记录路径、字段映射和分页协议。用户只绑定业务批次。分页类型第一版支持稳定游标和不可变导出列表，默认单读取流。

采集前必须获得导出版本或稳定批次令牌。每次翻页带相同令牌；检测重复游标、游标循环、页版本变化和超过最大页数。空页是否结束由协议定义，不能根据任意空响应推断结束。429/临时错误遵守平台退避与源端限流；鉴权和 Schema 错误不盲目重试。

第一版不要求 HTTP 失败后从某一页继续写同一个产物。失败尝试整体废弃，在相同导出版本上重新采集；导出版本失效则进入 INPUT_VERSION_EXPIRED，不能改读最新版本继续同一个输入清单。

### 6.3 JDBC

平台选择具体数据库方言与驱动，DatasetDefinition 固定物理对象、批次列、只读查询模板、切分字段和 Schema。默认读取上游已封存的批次表或不可变分区；活跃业务表没有稳定快照协议时拒绝严格读取。

并行分片必须覆盖全范围且不重叠，明确上下界包含关系及 NULL 分片；切分列在该批次内不得变化。第一版优先数值/日期范围，不能以无稳定排序的 LIMIT/OFFSET 做可恢复切分。控制连接数、fetch size 和超时，避免并行度直接等同数据库连接数。

多个查询连接分别使用可重复读，并不证明它们共享一个快照。数据库特定的共享快照机制需独立适配、测试并登记能力，不能从通用 JDBC 接口推导出跨连接一致性。

### 6.4 BigQuery

采集策略为固定表快照的 Storage Read API；需要复杂上游查询时，由获准模板完成物化，记录查询 Job ID 和物化表版本，再开始读取。先确认上游作业结束，不能把查询受理当作数据生成完成。

ReadSession 的多个流均需要完成，记录快照时间、Schema 和流清单。会话过期后，仅在数据保留策略允许并能够重新读取同一版本时重建读取计划，否则失败。数据落入内部文件并封存后，后续计算不再依赖 ReadSession 存活。

Storage Read API 支持会话内快照读取和多个数据流，但仍需处理权限、会话期限、类型转换和完整流覆盖。[BigQuery Storage Read API](https://docs.cloud.google.com/bigquery/docs/reference/storage)

### 6.5 ClickHouse

固定数据库、表、集群路由和批次/版本条件，优先读取停止变更的批次。并行方案在“查询 Distributed 表”与“显式读取各分片”之间选一种，不能两种同时使用造成重复；副本选择不应重复计入同一分片。

如果读取 ReplacingMergeTree 逻辑结果，平台必须明确排序键、版本列、删除语义及是否需要 FINAL 等查询约束，并在相同口径下取得上游控制数。仅等待后台 merge 不能作为唯一性或完成条件。[ClickHouse ReplacingMergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree)

ClickHouse 在本版属于 Source 范围。把它扩展为正式 Sink 需要单独设计目标版本发布、复制拓扑与回读契约，不沿用 PostgreSQL 的事务承诺。

## 7. 不可变文件与提交协议

### 7.1 数据组织

内部路径由系统生成，用户不能指定。逻辑目录如下，花括号表示平台标识：

```text
runs/{runId}/
  config/resolved-run.json
  inputs/{inputAlias}/{stageAttemptId}/data/...
  inputs/{inputAlias}/{stageAttemptId}/commit-evidence.json
  inputs/{inputAlias}/{stageAttemptId}/manifest.json
  compute/{stageAttemptId}/data/...
  compute/{stageAttemptId}/commit-evidence.json
  compute/{stageAttemptId}/manifest.json
  checks/{checkAttemptId}/report.json
  checks/{checkAttemptId}/differences/...
  receipts/{stageAttemptId}.json
```

manifest 文件一经封存不得覆盖。计算读取显式文件列表，不通过列目录猜测哪些文件成功；文件写入进度、上传完成、元数据封存是不同状态。

### 7.2 封存事实与恢复协议

选择“持久提交证据 → 不可变清单 → 元数据封存”的路径。第一版不另建通用事务日志，也不承诺抢救每一次已经写入但证据丢失的产物。`CommitEvidence` 是文件提交适配器输出的一份终结索引，不是 Flink 原生保证已经提供的接口；P0 必须验证能从选定 Sink 的真实提交路径获得足够信息，不能用计划中的文件名代替实际成功提交集合。

CommitEvidence 必须包含 stageAttemptId、StagePlan 摘要、CR UID/Flink Job ID、实际成功提交的文件及其提交身份、全部逻辑分片完成证明、最终 writer 集合及批结束标记。它排除未提交、失败或被取代的 task attempt 文件。计划、分片完成记录、最终提交集合三者需交叉核对；零行也必须有显式完成证明，不能用空目录代表完成。证据由受控提交适配器在全部文件提交完成后以尝试固定键不可变写入；聚合索引尚未落盘时崩溃，默认不保证能从内存提交回调恢复。

正常封存顺序：

1. Service 持久化不可变 StagePlan、当前尝试和专属前缀，再提交作业。文件只允许该尝试写入，不被其他尝试复用。
2. Sink 完成批结束提交，适配器从真实提交结果生成完整 CommitEvidence。收到上传成功、task 完成通知或 Flink FINISHED，单独任何一项都不足以封存。
3. Service 持久保存与资源身份匹配的成功终态观察，在有界终结工作中核对 CommitEvidence 的完整性；按其显式文件列表验证文件内容，生成行数、大小和 SHA-256。文件核验使用 Service 的独立有界 I/O 池流式读取，限制并发、吞吐和缓冲内存，不占用 API 请求线程；其成本计入阶段预算，第一版不再增加一个独立核验 Application。
4. 按固定 manifestId 写入不可变 Manifest；同键已存在时校验规范化内容摘要，相同则复用，不同则报冲突。文件身份与内容决定清单，恢复时不以当前时间生成不同版本。
5. 元数据库事务同时校验当前尝试、阶段、计划和取消状态，将 Manifest 引用标记为 SEALED，记录阶段完成。下游仅读取已被该事务接纳的 SEALED 引用。随后生成或补建 StageReceipt。

| 故障窗口 | 恢复判据 | 处理 |
|---|---|---|
| 文件已提交，CommitEvidence 缺失/不完整 | 作业终止且达到证据等待期限后，仍无法证明完整合法文件集合 | 先确认旧作业终止，再废弃本次前缀；新尝试重跑该阶段，不扫描目录拼结果 |
| CommitEvidence 完整，Manifest 尚未写入 | 成功终态可验证，当前尝试仍有效，文件校验通过 | 从固定证据生成同一 Manifest；不重新执行采集/SQL |
| Manifest 已写，元数据未 SEALED | 清单身份、内容与提交证据一致，当前尝试仍有效 | 补做封存事务；孤立清单不能自行进入下游 |
| 元数据已 SEALED，receipt 缺失/通知丢失 | 持久终态、Manifest 和阶段记录完整 | 重建 receipt 并按状态推进，不再启动原作业 |
| 终态不可观察且没有持久成功观察 | 尚不能证明作业成功和提交最终性 | 保持 UNKNOWN，恢复观察；若证据确实不可恢复，确认停止后废弃并重跑 |
| 旧尝试或取消后的证据迟到 | 不再具备当前尝试资格 | 只归档证据，不封存为可用输入，不推进运行 |

封存前文件校验错误不允许放行；已经 SEALED 的文件损坏时停止使用并报告证据缺口，不能修补文件后沿用原摘要。资源清理必须等成功终态观察和产物证据已持久化；不能先删 CR 再依赖其状态重建回执。元数据库中成功封存与阶段完成采用同一事务，避免有产物却无阶段归属。

P0 的放行条件是：实测选定 Sink 能形成完整 CommitEvidence，且上述每个窗口具有可判定恢复路径。若合法提交集合不可可靠取得，则文件提交选型不通过；应回到设计评审调整实现选择，不以目录枚举猜测或未经验证的 exactly-once 声明替代。P1 再验证完整 Service 恢复链路。

Flink 1.20 文档区分 BATCH 恢复与 streaming checkpoint；不能仅依赖 checkpoint 回调实现批结束封存。[Flink 批处理模式](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/execution_mode/)

### 7.3 完整性与保留

清单中的总行数由已提交文件汇总，另与源端导出清单或控制数比较；两者不能来自同一个未经校验的计数器。空输入使用零文件/零行的合法清单，但仍需完整 Schema、上游空批次完成证据和 allowEmpty 许可。

文件保留时长由平台确定，运行受理时固定；未完成、检查中、发布中或被运行引用的清单不得回收。源端快照过期不影响已封存文件的计算重跑，但文件一旦删除，该运行不能宣称仍可重现。正式历史版本的保留与输入文件保留分别管理。

## 8. 执行流程与状态机

### 8.1 正常流程

| 阶段 | 运行主体 | 前置条件 | 完成证据 |
|---|---|---|---|
| VALIDATING | Service | 身份与 YAML 已受理 | ResolvedRunSpec、SQL 规划报告、权限结果 |
| WAITING_INPUTS | 轻量检查器 | 配置冻结 | 每个源的批次完成证据和版本令牌 |
| COLLECTING | 每输入一个 Runner | 相应源就绪且可读 | InputManifest SEALED |
| PRECHECKING | 检查执行器 | 全部输入 SEALED，InputBundle 固定 | 所有必需输入检查 PASS |
| COMPUTING | COMPUTE Runner | 输入检查通过 | CandidateManifest SEALED |
| VALIDATING_RESULT | 检查执行器 | 候选结果完整 | 所有必需业务规则 PASS |
| WRITING | WRITE Runner | 候选结果已验收 | loadId 写入完成且目标版本封存 |
| RECONCILING_TARGET | 检查执行器 | 目标封存且可查询 | 候选与目标键和字段核对 PASS |
| PUBLISHING | 发布适配器 | 目标验收通过、版本比较条件有效 | 目标端原子发布回执和正式视图回读 |
| SUCCEEDED | Service | 全部阶段证据匹配 | 完整 RunReport，发布版本明确 |

逻辑阶段表示必须满足的业务关口，不规定每个关口都启动集群。采集、SQL 和写入使用 Runner；检查根据固定 RuntimeProfile 选择 `INLINE` 或 `FLINK` 执行器。就绪轮询、元数据校验以及能够证明在行数/字节/时间上限内完成的检查使用 Service 有界执行池；完整扫描超过预算时使用一个 CHECK 作业执行同阶段规则。两种执行器共享类型、NULL、DECIMAL 和差异计数语义，均产生带版本身份的 CheckResult。

无法预先证明检查规模有界时直接选择 FLINK。INLINE 运行中超出上限时本次记 ERROR/LIMIT_EXCEEDED，禁止返回部分 PASS；同一固定版本可创建新的 FLINK 检查尝试，仍受阶段重试和 deadline 约束。stageAttempt 记录所选执行器，不在同一次尝试中偷偷切换执行方式。

因此四源任务不固定需要九个 Application；数据量大、三组检查都需分布式执行时仍可能达到该数量，必须计入时限。第一版保留独立输入采集与产物关口，不为追求少启动几个集群而引入通用合并 DAG。使用 Flink 的每次尝试只提交一个执行图，阶段间通过封存数据交接。

### 8.2 阶段尝试和业务状态

StageAttempt 记录 executor=INLINE/FLINK；状态为 PLANNED、SUBMITTING、RUNNING、VERIFYING、SUCCEEDED、FAILED、CANCELLED。INLINE 不经过外部 CR 提交，SUBMITTING 只适用于 FLINK。超时属于 FAILED 的 failureCode。对外观察未知时记录 `observationStatus=UNKNOWN` 并保留当前阶段，不能将 UNKNOWN 解释为失败后直接重提。

JobRun 的 phase 采用上表；另有 FAILED、CANCEL_REQUESTED、CANCELLED、NEEDS_ATTENTION、SUPERSEDED。SUPERSEDED 表示发布版本已被其他运行推进，本运行不能按原比较条件发布。

对外分别返回以下投影，由阶段记录、必需 CheckResult 和发布意图/目标回执推导；不维护三套可独立写入的状态机。若缓存投影，必须能从事实记录重建，不能成为恢复依据：

| 字段 | 含义与取值 |
|---|---|
| executionStatus | 采集/计算/写出执行情况：NOT_STARTED、RUNNING、SUCCEEDED、FAILED、CANCELLED |
| qualityStatus | 必需检查：NOT_CHECKED、RUNNING、PASS、FAIL、ERROR |
| publicationStatus | NOT_PUBLISHED、PUBLISHING、PUBLISHED、UNKNOWN、CONFLICT |

候选检查失败时，即便 COMPUTE 阶段成功，整个运行仍为 FAILED，qualityStatus=FAIL，publicationStatus=NOT_PUBLISHED。所有必需检查 PASS 且目标发布确认后才产生业务成功。后续新运行替换发布版本，不修改旧运行当时的成功事实；查询当前生效版本使用独立字段。

### 8.3 协调器恢复

协调器先持久化意图再产生副作用，并通过租约、状态 revision 和 CAS 约束并发。每个副作用使用稳定的幂等标识，例如阶段 CR 名称、manifestId、loadId 和 publicationId。

Kubernetes 资源名由 runId、阶段、输入别名和尝试序号生成短摘要，完整标识与计划摘要保存在注解中。同名资源存在时核对 namespace、UID、标签及计划摘要，一致则接管观察，不一致则报冲突。对象删除后同名重建也不能误认成原 UID。

租约只能避免正常控制器并发，不能阻止失联旧进程继续写目标。因此目标封存、写入权限和发布 CAS 仍须在目标端强制执行，不能以“已有数据库锁”替代外部系统约束。

### 8.4 当前尝试与迟到回执

逻辑阶段键为 `(runId, stageType, inputAlias)`；非输入阶段的 inputAlias 为空。每个逻辑阶段最多一个 currentAttemptId。不能只设置一个全运行级 currentAttemptId，否则两个并发输入会相互覆盖。

Service 接收观察、回调或恢复证据时，先保存不可变证据，再在元数据库同一事务中检查：

- 该 stageAttemptId 仍为此逻辑阶段的当前尝试，且执行器和外部资源身份匹配；
- StagePlan、ResolvedRunSpec、所消费 InputManifest/InputBundle/CandidateManifest/loadId 摘要与当前计划一致；
- JobRun 仍处于允许接纳该阶段结果的 phase，未终止，且 cancelRequested=false；
- 必需完成证据齐全，阶段尚未完成或是内容一致的幂等重复。

通过后才能更新阶段、接纳产物或检查结果、推进 phase，并以唯一逻辑键创建下一阶段意图。整个更新使用 run 行锁或 revision CAS；CAS 冲突后重新读取并核对条件，不能把另一个输入的正常完成误判为本输入失效。InputBundle 只汇总各输入当前有效且成功的清单。

已被替换、被取消、身份不匹配的回执只追加到审计记录，不改变当前状态，不触发下游，也不覆盖新尝试报告。重试先确认旧执行已经停止，再在事务中切换 currentAttemptId；超时或租约失效本身不是旧执行已停止的证明。若无法停止，保持 UNKNOWN/NEEDS_ATTENTION。

创建发布意图还需核对全部必需检查对应当前候选和 loadId 且 PASS，并与取消请求在同一 run 事务上串行化。发布授权边界及之后的取消语义见第 10.4、11.3 节。

## 9. 前后置检查与数据正确性

### 9.1 三类证据

1. 输入准备与质量：上游批次完成、采集覆盖完整、Schema 固定、键唯一与必要字段有效。
2. 计算业务正确性：SQL 结果满足业务守恒、关联覆盖和明确的数据规则。
3. 写入完整性：已经验收的候选结果与隔离目标逐键逐字段一致。

上游控制总数、业务恒等式和独立测试预期用于降低“生产 SQL 和校验 SQL 同时犯错”的风险。不能以对候选结果执行两遍同一查询作为独立正确性证明。

### 9.2 强制检查与用户规则

平台强制执行：输入清单完整性、输出 Schema、业务键非空且唯一、已封存版本身份核对、目标写入回读。数据集登记的业务必需规则也不可被用户 YAML 关闭。

用户 postChecks 作用于候选结果；目标回读由平台按 OutputDatasetDefinition 的全部业务字段自动生成 row-compare。后者使用 NULL-safe 等值比较，区分 NULL 与空字符串，DECIMAL 精确比较，时间点统一精度，二进制字段按字节比较。额外的平台审计字段不参与业务字段比较，但它们的 loadId、批次身份必须一致。

第一版目标回读采用精确比较，不用抽样或哈希摘要替代。大结果通过分区分布式 Join/反连接完成，不能把所有主键拉回 API 进程。哈希可以辅助分桶或定位，但相等哈希不单独作为全部字段一致的证据。

### 9.3 CheckResult 与重试

结果记录 checkId、规则版本、逻辑阶段键、stageAttemptId、计划摘要、InputBundle/CandidateManifest/loadId、PASS/FAIL/ERROR/SKIPPED、期望值、实际值、差异总数、证据引用及时间。重试追加结果，不覆盖旧证据。

PASS 表示已执行且符合规则；FAIL 表示已执行且有业务差异；ERROR 表示查询或检查运行失败；SKIPPED 表示未执行。只有 PASS 放行。FAIL 默认不自动重试；ERROR 按白名单错误重试相同数据版本，不能改规则或放宽阈值。修正规则或 SQL 时创建新 JobVersion 和新 runId；只修正运行参数值时沿用定义版本创建新运行。

差异报告默认只暴露数量、列名和受控样本引用。完整差异按数据权限保存；日志不输出凭据或未脱敏的业务明细。超过样本上限仍保留精确差异总数，不能将样本数冒充差异数量。

## 10. 首个目标适配器与发布协议

### 10.1 PostgreSQL 版本发布模型

以 PostgreSQL 作为第一条可验收路径。每个输出数据集预创建数据版本表、版本状态表、发布指针和正式视图，具体 Schema 由平台管理。

| 目标对象 | 语义 |
|---|---|
| 版本数据表 | 保存 batch、loadId、业务键和业务列；唯一约束包含 batch、loadId、业务键 |
| 版本状态表 | 每个 loadId 的 WRITING、SEALED、ACCEPTED、REJECTED 状态及候选摘要 |
| 发布指针 | 规范化 businessKey 唯一；当前 activeLoadId、单调 revision、publicationId；revision=0 时 activeLoadId 为空 |
| 发布历史 | publicationId 唯一、请求摘要、前后版本、revision、目标事务提交信息；与指针在同一事务写入 |
| 正式视图 | 只返回发布指针对应的数据；下游无权直接读取所有隔离版本 |

`replace-batch` 的含义为切换该批次的有效 loadId，不是物理删除旧数据。多个批次互不覆盖；旧版本按保留策略回收。下游单次语句获得其事务语义下的视图；多次查询需要由下游固定版本或使用相应事务，不能承诺跨任意查询时间的全局一致性。

### 10.2 同一 loadId 内的写入幂等

候选结果先通过业务键唯一检查。Writer 以 `(batch, loadId, businessKey)` 写入，重复键仅在所有业务字段完全一致时视为相同重试；相同键不同内容报 WRITE_CONFLICT，不采用最后写入者覆盖。

目标版本状态控制写权限。每个写入事务在目标端检查并锁定对应版本状态，使 SEALED 之后的迟到写入被拒绝；封存操作与在途写入正确串行。不能只由客户端先检查状态再无条件 INSERT。P2 需要验证这个数据库侧协议的并发和性能开销。

同一 Flink 作业内 task 重试可重复写相同记录，但不能隐藏候选结果中的业务重复。平台 WRITE 阶段失败后默认创建新 loadId，从同一候选文件完整重写；旧 loadId 不发布，不混合两次尝试的部分行。

### 10.3 封存与验收

WRITE 作业结束、最终写入完成后，适配器将版本转为 SEALED。数据库侧阻止后续修改，目标检查读取固定 loadId，与 CandidateManifest 逐键逐字段比较。通过后将版本标记为 ACCEPTED，失败则 REJECTED。

目标查询超时仅重试 RECONCILING_TARGET。只有真正写入失败或确认数据缺失并按策略重新加载时才创建新 WRITE 尝试；检查 ERROR 不是重写理由。

### 10.4 首次发布、并发与未知结果恢复

运行在 VALIDATING 结束前，从目标主库读取该 businessKey 的 revision，冻结为 expectedRevision。仅在查询成功且确认指针行不存在时取 0；超时、权限错误或不可用不能解释为“首次发布”。读取时不必创建指针行，不要求每天人工预建批次。businessKey 必须映射到平台注册的规范目标身份，禁止用两个注册别名绕过同一物理批次的互斥。

**服务端授权事务。** 全部目标检查 PASS 后，Service 在元数据库同一 run 事务内检查第 8.4 节条件和 cancelRequested=false，持久化唯一 publicationId、businessKey、loadId、候选摘要、expectedRevision、请求摘要及 `authorizedAt`，并将 phase 置为 PUBLISHING。该事务提交是发布授权点。提交之前禁止向目标发送发布请求；提交后恢复进程仅可执行或查询这一份固定意图，不新建第二个 publicationId。

**目标端发布事务。** PostgreSQL 适配器执行以下固定过程：

1. 校验授权意图身份与请求摘要。若 publicationId 的已提交历史存在且内容完全相同，返回原回执；同 ID 不同内容立即冲突。
2. 对 businessKey 执行唯一键约束下的“插入 revision=0、activeLoadId=NULL，冲突则保留现有行”，随后锁定该指针行。插入和后续发布必须在同一个目标事务内；不先在事务外建行，不使用无法锁住不存在行的 SELECT 作为唯一并发保护。
3. 获得指针锁后再次检查 publicationId 历史，处理相同请求并发重试。历史已存在则返回原回执，不能将自己的成功重试误报成 revision 冲突。校验 loadId 为 ACCEPTED、候选摘要和 businessKey 一致，并按固定锁顺序保护目标版本状态。
4. 比较当前 revision 与 expectedRevision。相等时更新 activeLoadId、revision+1、publicationId，同时插入发布历史；不相等则回滚并返回 PUBLISH_CONFLICT。指针、历史和首次建行全部同一事务提交或回滚。
5. Service 查询主库历史确认本 publicationId 的提交事实，核对所发布版本和正式视图当前指向，保存回执并结束运行。如果当前指针已被后续发布推进，保留本次曾成功的事实，并单独报告 currentEffective=false，不误报本次未发布。

两个首次发布都使用 expectedRevision=0：唯一约束和指针锁将其串行化，最多一个把 revision 从 0 推进到 1；另一个冲突。后续替换沿用同一 CAS 协议。空指针不暴露任何隔离数据。批次历史回收不删除或重置 revision；若需保留空状态则保留指针墓碑，防止旧 expectedRevision=0 再次有效。

**未知结果。** 元数据库与目标库不做跨库事务。响应丢失或网络中断时保持 publicationStatus=UNKNOWN，按原 publicationId 查询目标历史。已提交则补记回执；查无历史只证明此刻不可见，不证明旧事务已经停止。允许在权限和预算内重送完全相同的授权请求，目标事务的唯一约束、指针锁及锁后历史复查负责串行化，不因此生成新的意图或改 expectedRevision。查询或锁等待无法判定时维持 UNKNOWN/NEEDS_ATTENTION。

只有在目标端同一一致性快照中观察到“revision 已被推进且本 ID 无提交历史”，或在上述持锁事务内复查历史后返回确定冲突，才能判 SUPERSEDED；不能把两次不同时刻的查询拼成冲突证据。自身已提交则即便当前版本更新也仍按成功处理。不能自动提升 expectedRevision 后覆盖新结果。版本回退需要独立的新发布操作，不修改历史回执。

## 11. 失败恢复与重试策略

### 11.1 恢复矩阵

| 场景 | 可复用产物 | 动作 | 不允许的行为 |
|---|---|---|---|
| YAML/SQL/权限不合法 | 无 | 返回确定错误，不执行 | 调整配置后仍沿用原配置摘要 |
| 上游未就绪 | 已取得的有效就绪证据 | 在固定期限内轮询 | 把 HTTP 200 或非零行数当成所有源已完成 |
| 源端令牌失效 | 其他已封存输入 | 原版本可重建则重采，否则终止本运行 | 自动读取最新版本并混入旧 InputBundle |
| 某源采集中断 | 其他输入与其清单 | 新 COLLECT 尝试从相同源版本重采 | 追加到旧失败文件目录并一起计数 |
| 文件已提交但提交证据缺失 | 其他已封存产物 | 确认停止后废弃本次前缀，新尝试重跑 | 扫目录猜成功文件，或凭 FINISHED 封存 |
| 提交证据完整但清单/receipt 缺失 | 持久终态、CommitEvidence、已存在清单 | 按第 7.2 节补清单、封存事务或 receipt | 为补 receipt 再跑原 SQL，或改写同 ID 内容 |
| SQL 计算失败 | 全部输入清单 | 新 COMPUTE 尝试 | 重新访问源端而不记录输入变化 |
| 输入或候选检查 FAIL | 固定输入/候选及报告 | 终止并保留差异 | 自动放宽阈值或重试直到偶然通过 |
| 检查 ERROR | 被检查的固定版本 | 新检查尝试 | 重跑采集或写入作为默认恢复 |
| 写入失败 | 候选结果 | 新 loadId 完整重写 | 清空正式结果或拼接不同 loadId |
| 迟到写入 | 已封存 loadId | 目标端拒绝 | 封存后修改数据而沿用旧检查报告 |
| 服务或 Operator 不可达 | 数据库意图、已有资源 | 恢复观察，状态 UNKNOWN | 将暂时不可见视为可重复创建的证据 |
| 发布结果未知 | publicationId、目标历史 | 回读目标判定 | 直接再执行替换或删除操作 |
| 发布版本冲突 | 已验收的目标版本 | SUPERSEDED，保留证据 | 自动更新 expectedRevision 后再次覆盖 |
| 旧尝试成功回执迟到 | 历史证据 | 只归档；当前尝试、数据版本和取消标记联合判定 | 推进 phase、替换当前检查报告或触发发布 |
| 首次发布指针行不存在 | ACCEPTED 目标版本、固定 expectedRevision=0 | 同一目标事务初始化指针并 CAS 发布 | 人工预建作为前提，或把读异常当 revision=0 |
| 用户取消 | 已封存产物 | 按授权点前后分支处理，见第 11.3 节 | 已授权发布结果未知时提前报 CANCELLED |
| 存储损坏/清单缺失 | 可验证的其他产物 | 停止，报告证据缺口 | 只凭 Flink FINISHED 放行 |

### 11.2 默认重试预算

RuntimeProfile 固定重试参数。研发基线：源就绪等待 30 分钟、轮询 30 秒；只读瞬时请求含首次共最多 3 次调用、重试前退避 1/2 秒并受阶段截止时间限制；阶段自动重试最多 1 次，默认仅针对可重放、已确认终态的瞬时失败；另明确允许两种受限转换：因崩溃导致提交证据无法恢复时，按第 7.2 节废弃并重跑；INLINE 检查超限时，按第 8.1 节新建 FLINK 检查尝试。二者均消耗同一阶段重试预算，不能无限循环。必需检查 FAIL、鉴权、Schema、版本过期和发布冲突不自动重试。

Flink 内部任务重启与平台阶段重试分别计数；平台不能在旧作业仍可能运行时启动覆盖同一输出的尝试。阶段时间预算和请求预算统一受总 deadline 约束，避免嵌套重试无限放大。deadline 到期禁止开启新的计算/写入尝试，但已授权副作用仍需停止确认和结果核对；不能用超时把未知发布结果改成确定失败。

同一错误连续发生且没有状态变化时转为 NEEDS_ATTENTION，报告失败阶段、两次证据、可复用产物和下一项操作。已经确认提交的副作用不重放；遇到结果未知先查询，不能消耗“重试次数”盲目重做。

### 11.3 取消与发布授权的竞争

取消请求与发布授权使用同一 run 行的事务锁或 revision CAS，必须有确定先后顺序：

- **取消先提交，尚无已授权 publication_intent：** 原子设置 cancelRequested=true，禁止创建下游及发布意图；请求停止所有活动 Flink 作业和 INLINE 检查。旧回执即使报告成功也只归档。确认无活动执行、无可能继续写入的 Writer，且没有发布授权后，才置 CANCELLED。停止无法确认时保持 CANCEL_REQUESTED 并显示 observationStatus=UNKNOWN；超过处理预算则 NEEDS_ATTENTION，不假报停止。 对仍有 SUBMITTING 意图或在途创建请求的尝试，CR 暂不存在不是停止证据；必须确认原派发者不再发送、在途请求已归结，并核对对应资源已终止。无法取得这些确认时继续 UNKNOWN，防止先报取消完成、旧 Service 随后才创建作业。
- **发布授权先提交：** 取消返回 `CANCEL_TOO_LATE`，并记录取消请求的审计事件；不再将本运行置 CANCELLED，也不撤销授权后让旧进程继续持有一份有效意图。即使发布请求尚未发出，恢复协调器也按原意图完成执行/核对，最终报告 PUBLISHED、CONFLICT 或 UNKNOWN。此边界以持久授权为准，不依赖无法可靠观察的“网络请求是否已经发送”。

取消不会删除正式结果，已封存产物按保留策略留存。已授权发布的人工中止、回退属于后续独立受控操作，不在 v1 取消 API 中实现。RunReport 同时展示取消请求时间、授权时间和目标提交结果，避免把“取消请求已受理”当作撤回成功。

## 12. API 与元数据

### 12.1 最小 API

| 方法与路径 | 行为 |
|---|---|
| POST /v1/jobs/validate | 接受 YAML，返回结构、权限、SQL 规划与能力检查；无作业副作用 |
| POST /v1/jobs | 接受通过校验的 YAML，保存不可变版本及摘要 |
| POST /v1/jobs/{jobId}/runs | 指定 jobVersion 和已声明参数值；合并默认值后冻结 ResolvedRunSpec |
| GET /v1/runs/{runId} | 返回 phase、三个业务状态、当前阶段与可重试信息 |
| GET /v1/runs/{runId}/checks | 分阶段返回规则、结果和获授权证据引用 |
| GET /v1/runs/{runId}/report | 返回配置、输入、产物、目标与发布证据摘要 |
| POST /v1/runs/{runId}/cancel | 请求取消，接受请求不等于已经停止 |
| POST /v1/runs/{runId}/retry | 指定失败阶段；系统核对终态、输入存活和权限后创建新尝试 |

重试 API 不接受 SQL、参数值或检查规则变更。仅更换已声明参数值，沿用 jobVersion 创建新 runId；修改 SQL、参数声明/默认值或规则，创建新 JobVersion 和新 runId。已发布运行默认不允许重新执行 WRITE/PUBLISH。第一版不提供任意阶段跳过接口。

创建定义、运行与重试均使用幂等键，作用域为租户、调用身份和操作。相同键与相同规范化请求返回原结果；相同键不同请求返回冲突。HTTP 接受运行可返回 202，但不代表校验、计算或发布成功。

### 12.2 元数据表规划

| 表 | 关键内容与约束 |
|---|---|
| job_definition | jobId、version、原文、规范化摘要、创建人；版本不可变 |
| job_run | runId、jobVersion、ResolvedRunSpec、phase、cancelRequested、revision、deadline；三个对外状态为投影 |
| stage_attempt | 逻辑阶段键、尝试序号、executor、计划与数据版本摘要、资源 UID/Job ID、持久终态观察、提交证据引用、状态；每逻辑阶段最多一个当前尝试 |
| manifest_record | manifestId、类型、存储位置、摘要、SEALED 状态、保留期 |
| check_result | 规则 ID、尝试、数据版本、状态、差异、证据位置；追加记录 |
| publication_intent | publicationId、runId、businessKey、loadId、候选/请求摘要、expectedRevision、authorizedAt、目标回执；每运行最多一份授权意图 |
| resource_reservation | stageAttemptId、全局/连接额度、申请量与释放证据；重复受理不重复扣额 |
| run_event | 运行状态与操作审计，追加不可覆盖 |
| idempotency_record | 调用域、幂等键、请求摘要和结果引用 |

租约与状态更新使用数据库事务和 revision 比较。目标发布历史在目标库，服务端 publication_intent 在元数据库；两者通过幂等标识核对，不假定一个事务覆盖两库。

### 12.3 RunReport

报告至少包含 JobSpec 摘要、releaseId/JAR/镜像摘要、输入版本与文件清单摘要、阶段尝试及耗时、候选结果、必需检查、目标 loadId、发布回执、失败原因和未完成检查。报告明确分开：配置可规划、作业完成、规则通过、目标已发布。

没有规则覆盖的正确性属性标记为未验证，不从 PASS 推导全业务正确。外部权限、网络或获准制品不可用时记录 `external_blocked`；运行或数据证据不齐时记录 `evidence_incomplete`。

## 13. Operator 运行与权限隔离

每个 executor=FLINK 的 COLLECT/COMPUTE/CHECK/WRITE 尝试创建一个 FlinkDeployment；INLINE 检查不创建 CR。FlinkDeployment 固定 image digest、JAR 本地路径、RunnerMain、阶段计划位置和摘要。Service 不把用户 YAML 原样转成 CR，也不开放 jarURI、PodTemplate 或任意 Flink 配置透传。

阶段计划通过受控存储交付，Runner 启动后核对摘要、协议版本、releaseId 和允许角色。凭据仅按阶段注入：COMPUTE 只需输入和输出文件权限；COLLECT 需要相应源的只读权限；WRITE 只需候选文件读取和隔离写入权限；发布权限单独归 Service 使用。

Service 观察 CR UID、generation、Operator 对已提交配置的观察状态、Flink Job ID 与终态。Kubernetes Pod Running 不表示 Flink 作业执行完成；Flink FINISHED 不表示清单完整或目标已验收。CR 被错误删除时先核对持久终态、CommitEvidence/Manifest、历史 receipt 与目标事实，不能无条件重建并重放副作用。

资源清理在阶段证据持久化后进行；输入文件、候选结果、正式目标版本与审计报告不随 FlinkDeployment 删除。清理由专用保留策略驱动，第一版只生成可回收清单，不默认实现自动批量删除。

Flink、Operator、JDK 和连接器需作为一个发行组合验证。官方兼容表用于筛选，不能替代本项目打包及恢复测试。[Operator 兼容说明](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.16/docs/deployment/compatibility/)

## 14. 资源、可观察性与发布管理

### 14.1 默认运行约束

| 项目 | 研发初始策略 | 调整依据 |
|---|---|---|
| 输入并发 | 每运行最多同时采集 2 个输入，且受以下共享额度约束 | 源端连接预算、限流、读取压力 |
| 全局准入 | 最大活跃运行数、最大活跃 Application 数、Service 检查线程数必须在 P0 固定有限值 | 集群容量与 P5 压测；未配置不启用任务派发 |
| 连接共享预算 | 每 ConnectionProfile 固定最大连接/并行流及适用的请求速率 | 所有作业、检查、写入共用；注册别名不得绕过同一物理连接限额 |
| 作业资源 | 平台 standard-batch 档位，用户不可指定任意资源 | P0 环境规格和 P5 负载测试 |
| 并行度 | 每个阶段由平台档位限定 | 分片数、数据量、源端并发、Join 倾斜 |
| 文件格式 | 固定 Parquet Schema；压缩编码随 release 固定 | 类型保真与读写性能验证 |
| 文件大小 | 先以约 128 MiB 作为滚动目标，不作为必须达到的正确性条件 | 小文件数量、对象存储成本、吞吐 |
| 内存边界 | 小检查受总行数/字节数/并发限制；大表检查由 Flink 执行 | 内存、超限次数和回压指标 |
| 数据保留 | 平台按数据分类配置，受理时固定截止时间 | 行内要求、重跑窗口和容量；上线前必须有确定值 |

准入队列直接使用元数据库中的待运行/待派发记录，由现有协调循环按受理顺序扫描可满足预算的任务，不另建调度服务或 MQ。每次派发在一个元数据事务内预留全局额度和所需 ConnectionProfile 额度；不足则整体等待，不先占一部分额度再等待另一部分。RuntimeProfile 限定阶段最坏并发，源适配器必须在预留预算内创建连接/流；HTTP 请求速率按阶段分配额度，总和不超过连接级上限。就绪探测也受 Service 检查池和连接预算限制。

额度与 stageAttemptId 绑定，重复提交不重复预留。Service 租约到期、查询超时或 Pod 暂不可见均不自动释放；确认外部作业终止、连接关闭后释放。INLINE 的远端查询超时也需确认取消/结束，不能仅因本地 future 结束立即归还连接额度。UNKNOWN 暂占额度并告警，接受暂时降低吞吐，避免失联旧任务与新任务叠加突破源预算。阶段间及时归还计算与连接额度，业务运行等待上游就绪时不占 Application 槽位。

快照路径额外增加至少输入写入/读取、候选结果写入/读取及目标回读，容量估算必须包含并发运行、失败尝试和保留窗口。阶段启动时间也计入业务完成时间。没有规模数据时不承诺具体吞吐、成本或完成分钟数。

Join 倾斜、超大维表和笛卡尔放大首先通过静态限制、输入统计与运行监控暴露。v1 不自动重写用户 SQL，不未经验证强制广播，也不为了通过检查截断结果。必要时降低并行采集、增配计算档位或要求业务改写后发布新定义。

### 14.2 日志与指标

日志统一带 jobId、jobVersion、runId、stageAttemptId、releaseId、checkId/loadId/publicationId。结构化事件记录状态变更与原因，不记录连接密码、完整令牌或未脱敏行数据。

至少采集阶段耗时、源行数与字节数、分片完成数、HTTP 限流次数、JDBC 活动连接、Flink 重启与回压、文件数、候选行数、检查差异数、写入耗时、目标回读耗时、发布冲突数和 UNKNOWN 状态持续时间。

告警通过行内既有渠道接入。告警只报告状态与受控证据链接，不能把“请求已受理”显示为“处理已完成”。第一版不新建消息中心。

### 14.3 发行组合与升级

ReleaseManifest 固定应用版本、Git revision、JAR SHA-256、镜像 digest、依赖清单、Flink/JDK/Operator 兼容范围、YAML/Manifest 协议版本、支持的连接器和检查类型，以及测试和扫描报告引用。

新能力走应用代码发布，新业务作业走 YAML 版本。正在运行的任务绑定旧 releaseId，不热替换连接器。旧版本存在已知问题时由平台决定停止新受理或终止运行，不能静默用新 JAR 接续旧尝试。

应用服务升级需要兼容已有运行和元数据。无法兼容的版本并行部署或先排空旧运行；数据库迁移采用独立评审，不能把新 Runner 能加载旧配置视为恢复协议兼容证据。

## 15. 分阶段实现计划

本节是实施顺序与交付合同，不授权开始编码。每阶段先冻结接口和可复现验收输入，再实施并验证。阶段验收未通过时，不将相邻阶段的单元测试升级为整条业务链路完成。

### P0 运行基线与协议冻结

目标：确定能够实际构建和部署的唯一发行组合，避免在未经准入的依赖上展开实现。

| 工作包 | 预定模块/产物 | 验收条件 |
|---|---|---|
| P0-1 版本矩阵 | ReleaseManifest 规范、依赖归属与构建约束 | Flink、JDK、Operator、PostgreSQL、S3 客户端、Parquet、驱动版本全部固定；兼容资料及实际环境可核对 |
| P0-2 制品验证 | app 打包基线 | 一个应用 JAR 在固定镜像中分别启动 Service/Runner 角色；驱动与 SPI 可发现，无重复 Flink 类冲突 |
| P0-3 平台对象 | DatasetDefinition、OutputDatasetDefinition、RuntimeProfile 规范 | 注册两份 JDBC 输入、一个 PostgreSQL 输出和内部存储；权限及稳定批次协议明确 |
| P0-4 文件提交验证 | runtime/connectors 文件提交选型 | 证实真实提交路径能输出完整 CommitEvidence；验证提交后索引缺失则废弃重跑、证据完整则可生成清单；不能取得合法集合则阻止进入 P1 |
| P0-5 协议与资源基线 | 参数声明、当前尝试判定、首次发布事务、授权取消边界、RuntimeProfile | 冻结摘要与幂等字段；确定全局及连接额度、INLINE 检查上限；列明 A12/A26/A27/A33 选型实验结果 |

退出条件：环境、版本、稳定输入、目标正式视图方案和文件提交机制均有确定结论；通过第 16.2 节 P0 选型实验，P0 的局部实验不能冒充后续完整恢复验收。行内不允许 PostgreSQL 或对象存储时，替换该适配器设计再继续；不将外部准入障碍伪装为测试通过。

### P1 两个 JDBC 输入到候选结果

依赖 P0。目标是第一条数据垂直链路：两个固定 JDBC 批次 → Parquet 输入 → SQL Join → Parquet 候选结果。此阶段不写正式业务结果。

| 工作包 | 预定职责 | 最低验收 |
|---|---|---|
| P1-1 协议与规划 | spec | 定义与运行参数分离；同版本绑定多个日期；未知键、重复键、越权引用、类型错误、禁止 SQL 全部拒绝 |
| P1-2 输入与清单 | connectors/runtime | 分片无遗漏/重叠，NULL 切分可解释，空批次按协议处理；固定清单可重复读取 |
| P1-3 SQL 执行 | runtime | 两源 LEFT/INNER JOIN、CTE、精确聚合与固定测试预期逐行一致 |
| P1-4 控制链路 | control | 当前尝试及版本联合校验；创建响应丢失不重复创建；证据完整时补清单/receipt，证据缺失时停止旧任务并重跑 |
| P1-5 执行预算 | control/runtime | 先实现最小全局/连接额度预留释放、INLINE 有界执行与 FLINK 检查选择；P3 扩展并发故障验收 |

退出条件：通过第 16.2 节 P1 用例；真实 PostgreSQL 与真实 Operator 环境完成输入/候选清单封存和三类文件恢复窗口，重启服务后可继续观察。只运行本地 MiniCluster 时只能报告本地层级完成。

### P2 校验 写入与版本发布

依赖 P1。目标是首个可用业务闭环，范围仍是 JDBC 输入与 PostgreSQL 输出。

| 工作包 | 预定职责 | 最低验收 |
|---|---|---|
| P2-1 检查模型 | runtime | PASS/FAIL/ERROR/SKIPPED 分明；输入唯一性、结果键集、金额和空值规则有正反例 |
| P2-2 隔离写入 | connectors 中的 PostgreSQL 适配 | 相同键相同值重复写幂等，不同值冲突；正式视图看不到未发布 loadId |
| P2-3 目标封存 | 目标版本状态与 Writer 协议 | 封存与在途写入并发可串行；迟到 Writer 不能修改已验收版本 |
| P2-4 精确回读 | runtime 的 row-compare | 插入漏行、多行、单字段差异时精确报告且不发布 |
| P2-5 发布恢复 | publication_intent 与目标历史 | 首次指针不存在可发布；并发首次发布至多一个成功；授权后才调用目标；响应丢失和同 ID 并发重送可恢复，旧意图不覆盖新结果 |

退出条件：通过第 16.2 节 P2 用例；目标正式视图中可查询验收通过的新批次，旧批次保留；业务检查失败、写入失败和未知发布状态均不会被误报为成功。

### P3 恢复 取消与运行管理

依赖 P2。P1/P2 必需的幂等机制必须随功能实现，本阶段集中补齐故障组合和 API 行为，不把幂等延后到这里才开始设计。

| 工作包 | 预定职责 | 最低验收 |
|---|---|---|
| P3-1 阶段重试 | 预算、错误分类、新 stageAttemptId/loadId | 检查 ERROR 只重试检查，计算失败只复用封存输入，写入失败不重算 |
| P3-2 未知状态恢复 | 当前尝试、数据摘要、持久终态、CommitEvidence、目标历史 | 断网、Operator 重启和多份迟到回执不推进旧状态；证据可重建与不可重建两条路径均验证 |
| P3-3 取消 | cancelRequested、authorizedAt 与停止确认 | 取消先提交不得授权发布；授权先提交返回 CANCEL_TOO_LATE，包括目标请求尚未发送的窗口 |
| P3-4 元数据和报告 | 事件、幂等 API、派生状态、RunReport | 报告可重建；区分发布历史事实与当前生效版本；敏感字段不泄漏 |
| P3-5 准入和检查 | 共享额度、INLINE/FLINK 一致语义 | 多运行不突破共享预算，UNKNOWN 不提前释放；有界检查超限不返回部分 PASS |

退出条件：通过第 16.2 节 P3 明确列出的 JDBC/PostgreSQL 恢复与并发用例，并回归 P1/P2；保留资源状态、文件清单和目标查询证据。P4 的 HTTP/BigQuery/ClickHouse 验收不是本阶段退出前提。

### P4 完成 HTTP BigQuery ClickHouse 输入

依赖 P1 的 Source/Manifest 契约和 P2 的验收链路。三种适配器分开交付，不能以注册了 type 名称作为支持完成。

| 工作包 | 验收重点 |
|---|---|
| P4-1 HTTP | 稳定导出、游标循环、重复页、限流、版本过期、合法空批次；全部记录与独立导出基准一致 |
| P4-2 BigQuery | 真实 IAM/网络路径、物化完成、流覆盖、会话期限、DECIMAL/时间映射、固定快照重采 |
| P4-3 ClickHouse | 分片/副本边界、固定批次、去重口径、完整扫描、版本变化阻断 |
| P4-4 四源组合 | 文档示例对应的实际四源数据完成采集、Join、验收、写入和正式回读 |

退出条件：通过 A18–A20，四类 Source 各自与组合场景均有真实运行证据，并将已验证的文件封存、迟到回执、配额及取消协议在新增 Source 上回归。某一类受权限或网络阻塞时整体第一版范围未完成，标记 external_blocked，不以 mock 或其他源代替验收。

### P5 规模验证与交付

依赖 P3/P4。目标是给出可部署的资源档位、支持范围和可重复验证材料。

工作包括：典型与最大批次、维表大小、Join 放大与倾斜、并发运行、源端限流、文件数量、检查成本、失败重跑容量、目标发布竞争和保留策略验证。测试报告明确硬件、数据规模、输入分布、版本及冷/热启动条件。

交付物包含：固定 JAR 与镜像、ReleaseManifest、扫描结果、协议说明、数据集登记说明、Operator 部署配置、运行手册、故障恢复手册和验收报告。部署前扫描通过不代替数据验收，数据验收通过不代替行内发布流程。

退出条件：通过 A21，并回归 P1–P4 功能与故障集合；真实规模下满足经确认的业务时限与源端预算；四类 Source 和正式 Sink 在同一发行组合下通过功能与恢复验收。时限尚未确认时只能给出测量结果，不能宣称满足生产 SLA。

## 16. 可复现验收矩阵

### 16.1 固定功能数据集

研发验收使用一份可独立人工核算的数据集：100 笔订单，order_id=1…100，对应 amount=1.00…100.00；10 个客户、5 个机构、10 条客户风险记录，各维度键唯一且全部关联成功。预期候选结果为 100 行、订单键集合完整、金额 5050.00，所有描述字段按独立固定映射核对。

该数据只用于功能验收，不代表生产规模。另准备 0 行批次、重复维度键、空订单键、缺失维度、金额 NULL、超精度金额和多币种数据，分别验证允许/拒绝规则；不通过删除异常记录来使测试通过。

| 编号 | 场景与操作 | 必须观察到的结果 | 所属阶段 |
|---|---|---|---|
| A01 | 提交完整 YAML 与非法字段变体 | 合法可规划；重复键、任意 jarURI、未知输入和越权数据集拒绝 | P1 |
| A02 | 上游连接正常但批次未完成 | 继续等待且没有 COLLECT/COMPUTE 提交 | P1 |
| A03 | 采集分片边界、NULL、稳定排序测试 | 无漏行重行，清单与独立源控制数一致 | P1 |
| A04 | 同一 InputBundle 重算两次 | 结果行多重集合一致，不要求文件字节排列一致 | P1 |
| A05 | 增加重复客户键 | PRECHECKING FAIL，计算不执行 | P2 |
| A06 | 空输入合法/不合法两种登记策略 | 合法产生明确零行清单；不合法阻止后续阶段 | P1/P2 |
| A07 | 故障注入在目标封存前交换订单 1/2 的金额 1.00/2.00，候选文件保持原值 | 目标仍为 100 行/5050.00；row-compare 精确发现 2 个键的金额差异，阻止发布 | P2 |
| A08 | 同键同值与同键不同值重复写 | 前者幂等，后者 WRITE_CONFLICT | P2 |
| A09 | 写入一半中断后重跑 | 新 loadId，旧半成品不可见；正式结果无重复 | P2/P3 |
| A10 | 封存后旧 Writer 继续写 | 目标端拒绝，原验收版本未改变 | P2/P3 |
| A11 | CR 创建成功后丢失响应并重启服务 | 接管同 UID/摘要资源，不创建第二逻辑尝试 | P1/P3 |
| A12 | Flink task/JobManager 失败，并观察提交路径 | CommitEvidence/Manifest 只含合法提交集合；无法证明时废弃重跑，失败 task attempt 不混入 | P0/P1/P3 |
| A13 | 查询对账服务不可用 | qualityStatus=ERROR，只重试检查，不重新写入 | P3 |
| A14 | 目标事务提交后丢失响应 | 通过 publicationId 恢复确认，无第二次替换 | P2/P3 |
| A15 | 两运行使用同 expectedRevision 发布 | 至多一个成功，另一个冲突且不自动改 revision | P2/P3 |
| A16 | 取消与目标发布提交竞争，包括目标响应丢失 | 已授权则 CANCEL_TOO_LATE；结果未知保持 UNKNOWN，按历史恢复，绝不先报 CANCELLED | P3 |
| A17 | 篡改文件/清单或错误引用其他运行 | 摘要/身份校验失败，不计算或不发布 | P1/P3 |
| A18 | HTTP 翻页中批次变化/BQ 原版本过期 | 阻断或只恢复同一版本，不静默切换最新输入 | P4 |
| A19 | ClickHouse 分片与副本重复风险 | 与独立基准比对，无重复采集，查询口径明确 | P4 |
| A20 | 四源完整业务批次 | 输入、候选、目标和发布证据可串联 | P4 |
| A21 | 最大规模与并发运行 | 测得时间、资源、源压力和检查开销，满足正式确认的预算 | P5 |
| A22 | 目标新批次无指针行，以 expectedRevision=0 发布 | 同事务建行、切指针、写历史；正式视图可读，revision=1；中途故障全部回滚 | P2 |
| A23 | 两个不同 publicationId 首次并发发布同一 businessKey | 至多一个成功；另一个冲突；无孤立有效指针、无两个成功历史 | P2 |
| A24 | 取消先提交，旧 COLLECT/COMPUTE/CHECK 回执迟到；另测 CR 创建请求仍在途 | 旧证据只归档，不创建下游/发布意图；在途派发未归结时保持 UNKNOWN，不能凭 CR 不存在报 CANCELLED | P3 |
| A25 | 旧尝试已停止并被新尝试替换后，旧回执和旧 PASS 报告迟到 | 当前尝试及数据摘要不匹配，不能推进；两个正常并发输入完成不会相互误拒绝 | P3 |
| A26 | 完整 CommitEvidence 已持久化且成功终态可核实，Manifest 前/写后数据库封存前崩溃 | 两个窗口均补同一清单和 SEALED 引用；不重采、不重算；重复恢复内容一致 | P0/P1/P3 |
| A27 | 文件已提交但 CommitEvidence 缺失或缺一个终结标记 | 不按目录猜测封存；停止旧任务后用新前缀重跑；同源版本失效则报错不改读最新 | P0/P1/P3 |
| A28 | SEALED 与阶段成功已提交，receipt 缺失，Service 重启 | 从持久终态、清单和阶段事实补 receipt；最多创建一份下一阶段意图，不重跑原阶段 | P1/P3 |
| A29 | 在授权事务前、授权后但目标调用前分别插入取消，并使旧 Service 延迟恢复 | 前者不能授权/发送发布；后者返回 CANCEL_TOO_LATE，原意图仍可执行且不会出现 CANCELLED 后发布 | P3 |
| A30 | 同一 jobVersion 运行两天，测试默认值与非法参数 | 不增加定义版本；各自冻结参数；遗漏必填、错类型、未知值、修改 SQL/资源等请求拒绝 | P1 |
| A31 | 多运行共享同连接，耗尽额度，模拟失联作业和恢复 | 连接/流/请求预算不超限；待派发不部分占额度；UNKNOWN 不释放；重复观察不重复扣还 | P3/P5 |
| A32 | 相同固定数据分别用 INLINE/FLINK 检查，并触发 INLINE 超限 | PASS/FAIL/ERROR 口径和完整差异数一致；超限为 ERROR，新检查尝试可转 FLINK，不接受部分 PASS | P3 |
| A33 | 合法空批次及空候选输出，无数据文件 | 仍有完整提交/分片结束证明、Schema 与零行清单；空目录或缺标记不成功 | P0/P1 |

以上故障用例应在指定持久化边界暂停进程、断连或终止进程，并保留故障注入位置、恢复前后记录与目标查询证据；不能仅构造一条“成功回执”替代完整恢复链路。A14/A22/A23 还须覆盖相同 publicationId 的并发重复请求、目标提交后 Service 崩溃，以及后续发布已推进指针时查询旧成功历史。

### 16.2 阶段验收集合

| 阶段 | 必须完成的集合 | 证据边界 |
|---|---|---|
| P0 | A12/A26/A27/A33 的提交机制选型实验 | 可以使用最小验证程序；须取得真实 Sink/对象存储提交证据，不要求尚未实现的 Service/API 已完成；P1 必须重做完整链路 |
| P1 | A01–A04、A06 输入与空候选部分、A11、A12、A17 文件身份部分、A26–A28、A30、A33 | 真实 JDBC、内部存储、Operator 与 Service；示例四源的结构校验不代表四源连接可用 |
| P2 | 回归 P1；新增 A05–A10、A14、A15、A17 目标身份部分、A22、A23；完成 A06 空结果发布策略 | 真实 PostgreSQL 写入、封存、精确比较与事务发布；A09/A10/A14/A15 的组合故障在 P3 扩展 |
| P3 | 回归 P1/P2；新增 A13、A16、A24、A25、A29、A31、A32；组合复测 A09–A12、A14、A15、A17、A26–A28 | JDBC/PostgreSQL 范围恢复闭环；不依赖 P4 新增源，不宣称四源已完成 |
| P4 | A18–A20；新增源回归 A06、A12、A17、A24–A28、A31、A33 的适用部分 | 各源真实环境和四源组合；不适用项说明原因，不能将外部阻塞记为 PASS |
| P5 | A21、A31 规模场景，并回归 P1–P4 集合 | 冻结发行组合、实际规模与预算；有业务时限依据才报告 SLA 达标 |

每个用例保存适用版本、输入固定版本、预期、实测结果、资源/日志/文件/目标证据和未验证部分。表中阶段是验收归属，不代表可以把基本安全条件延迟实现；例如当前尝试校验在 P1 实现，发布授权在 P2 实现，P3 验证复杂竞争。

单元测试覆盖解析、状态转移、版本比较和检查规则；集成测试覆盖真实数据库事务、对象存储清单和连接器；集群测试覆盖 Operator 与 Flink 生命周期；业务 E2E 覆盖实际四源、目标和发布回读。四层证据分别记录。

## 17. 上线前输入与设计变更边界

以下信息不阻止本设计交付，但必须在对应阶段完成前落实，不能以模糊默认值带入生产。

| 信息 | 当前默认 | 落实时点 | 不满足时的处理 |
|---|---|---|---|
| 正式输出数据库与下游读法 | PostgreSQL + 正式视图 + 版本指针 | P0 | 若下游不能按视图/版本读取，重新设计发布适配，不能暴露隔离表 |
| 内部持久存储 | 获准 S3 兼容对象存储 | P0 | 替换为获准存储并验证不可变清单提交 |
| 四源批次与快照协议 | 不可变完成批次或稳定导出 | 对应适配器开发前 | 无稳定版本时拒绝严格模式，要求源端提供能力 |
| Flink/JDK/Operator/客户端版本 | 一个经验证的发行组合 | P0 | 不跨未经验证的版本复用运行保证 |
| 数据量、时限、并发及源压力 | 无生产数值承诺 | P5 前 | 先测量，再冻结资源档位与 SLA |
| 数据保留和差异报告访问 | 平台管理，无用户任意路径 | P0/P5 | 无批准保留策略时不启用生产采集和清理 |
| BigQuery 访问条件 | 获准网络与身份 | P4-2 前 | 标记 external_blocked，不以模拟测试替代 |

如果实施发现存储不能实现清单封存、目标不能保证发布 CAS、Source 不能重复读取原版本，或行内禁止当前默认依赖，应返回设计评审修改该边界。不得把“只能最终一致”改写成“已保证一致”，也不得悄悄降低验收标准。

## 18. 本次审查修订记录

| 审查问题 | v0.2 调整 | 对应验收 |
|---|---|---|
| 模块、状态与阶段部署过细 | 五组包内职责；三个对外状态为派生投影；轻量检查有界执行，分布式检查按需启用 | A31/A32 与发行基线 |
| 首次发布无指针行 | 读成功且缺行才解释为 revision=0；唯一键初始化与 CAS/历史同事务 | A22/A23 |
| 迟到成功回执越过取消/新尝试 | 逻辑阶段当前尝试、计划/数据版本、phase、取消标记同事务判定 | A24/A25 |
| 取消与旧进程发布竞争 | 持久发布授权为截止点，授权后取消明确返回过晚，继续核对固定意图 | A16/A29 |
| 文件提交到封存之间有恢复空白 | 明确 CommitEvidence；证据完整补建，缺失则停止并重跑；receipt 可重建 | A12/A26–A28/A33 |
| 每个批次生成定义版本 | 定义参数声明与运行参数值分离，冻结 ResolvedRunSpec | A30 |
| 仅有单运行并发限制 | 共享全局与 ConnectionProfile 预算，未知执行保留额度 | A31 |
| 验收数据及阶段依赖不准确 | 不同金额目标单侧交换；阶段退出引用明确用例集合 | A07、第 16.2 节 |

保留不可变输入、固定候选、目标隔离写入、精确回读与原子发布。这些机制分别应对输入漂移、计算重跑、失败半成品、写入差异和结果可见性，简化工程划分不能消除这些正确性关口。

## 19. 参考依据与证据范围

2026-10-18 核对以下官方资料，用于界定上游机制，不代表项目已实现或选定对应生产版本。

| 资料 | 支持的设计判断 |
|---|---|
| [Flink 批处理执行模式 1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/execution_mode/) | BATCH 与 streaming 恢复/提交不同，不能套用 checkpoint 假设 |
| [Flink 自定义 Source/Sink 1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sourcessinks/) | Table 规划与运行适配边界，固定类型和有界运行契约 |
| [Flink Kubernetes Operator 部署说明 1.16](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.16/docs/deployment/overview/) | Application/Session 模式及资源管理边界 |
| [Operator 兼容说明 1.16](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.16/docs/deployment/compatibility/) | 发行组合需要兼容筛选 |
| [BigQuery Storage Read API](https://docs.cloud.google.com/bigquery/docs/reference/storage) | 读取会话、快照与多流机制 |
| [ClickHouse ReplacingMergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree) | 后台去重不能替代即时唯一性和固定读取口径 |

本文的模块职责、YAML v1、状态机、Manifest、目标发布协议和实施阶段均为本项目设计决策。已完成的是设计整理及文档级核对；编译、扫描、集群运行、真实数据一致性和故障恢复均须按上述阶段取得证据后才能报告完成。
