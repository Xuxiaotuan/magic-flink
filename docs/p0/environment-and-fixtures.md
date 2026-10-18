# T01 环境盘点、实验边界与最小补齐方案

**检查日期：** 2026-10-18。**状态：** 只读盘点已完成；T01 部分完成，尚未验收，`evidence_incomplete`。

本文整理同一会话上一轮 T01/T02 的实际检查结果。检查基于项目提交 `76f7739`，包含本机版本查询、显式指定 OrbStack context 的 Kubernetes 查询、容器与镜像清单及有限范围的文件检查。本轮仅整理文档，未安装、启动、部署、修改环境或读取业务表。运行状态会变化，实施补齐前须重新核对。

关联文档：[P0 实施计划](../superpowers/plans/2026-10-18-p0-baseline-and-file-commit-validation.md)、[T02 候选版本与制品核验](version-lock.md)、[设计 v0.2](../design/flink-yaml-batch-service-design.md)。本文保留脱敏后的关键结果与复查入口；完整命令输出未作为仓库附件保存，不能替代后续实验的证据索引。

## 1. 已具备的条件与实际缺口

| 对象 | 实际检查结果 | 对 P0 的意义 |
|---|---|---|
| 主机 | macOS 26.6.2，arm64，8 个逻辑 CPU，24 GiB 内存；文件系统约 42 GiB 可用，使用率 91% | 可以继续静态研究；镜像、构建缓存和实验数据需控制空间，尚未证明实验容量充足 |
| 容器运行时 | Docker CLI/Engine 29.4.0，context 为 `orbstack`，服务端 linux/arm64；分配 8 CPU、约 11.7 GiB 内存 | 可复用运行时；分配量不等于空闲量，不代表两个 Application 并发实验已具备资源 |
| 本地 Kubernetes | 显式查询 `orbstack`，服务端 `v1.35.6+orb1`，节点 Ready、linux/arm64 | 集群存在；尚无本项目 namespace、配额、ServiceAccount 或权限验证 |
| kubectl / Helm | kubectl 1.33.9，Helm 4.0.1；只读查询成功 | kubectl 比服务端低两个 minor，需补兼容客户端；查询成功不等于版本受支持 |
| Apache Flink Operator | 旧 Helm release 声称 deployed，但对应 Deployment 为 NotFound；保留 Flink CRD | 不能计为运行环境就绪，且残留对象可能影响后续安装，详见第 2 节 |
| JDK / Maven / Flink | 已有 Java 17、Maven 3.9 系列、本地 Flink 1.20.5 目录 | 可减少后续工具安装，精确版本与制品身份缺口见 T02 |
| PostgreSQL | 本地已有声明为 17.11 的镜像；另一个项目的容器已停止；旧 Kubernetes PostgreSQL StatefulSet 副本数为 0 | 可考虑复用镜像层；尚无本项目可访问的实验库，未连接或核验数据库内容 |
| S3 兼容存储 | 检查范围内未发现运行中的 MinIO/RustFS 服务；已有 `mc` 客户端与 `minio/mc` 镜像 | 客户端不是存储服务；本项目 endpoint、bucket、权限与服务端版本均未取得 |
| 固定数据 | 未创建或查询本项目 fixture，未登记实际批次、权限和保留期 | 100 行 / 5050.00 目前是计划预期，不是已有数据的核验结果 |
| 扫描能力 | PATH 与已检查镜像中未找到 Trivy、Syft、Grype、Cosign 等相关工具；未取得扫描报告 | 不等于组织内没有扫描平台；优先确认既有获准平台，详见 T02 |

默认 Kubernetes context 指向另一个集群，本次没有查询该集群或切换默认 context。所有集群盘点均显式使用 `--context orbstack`。后续同样必须指定目标，不能依赖终端当前 context。

## 2. Operator 是环境补齐的首要阻塞点

实际观察到 `flink-lineage-test` namespace 中旧 Helm release `flink-operator`，revision 2，Chart/app 版本为 `1.17-SNAPSHOT`。其保存的 manifest 引用 `flink-operator-local:38a9f1974650`，但实际 `flink-kubernetes-operator` Deployment 不存在。Helm 的 deployed 状态只是 release 记录，不是控制器存活证据。

集群仍保留 `flinkdeployments`、`flinksessionjobs`、`flinkstatesnapshots`、`flinkbluegreendeployments` 的 CRD，storedVersion 为 `v1beta1`。本次未发现 FlinkDeployment 实例或匹配的 Flink webhook 配置。其他项目的 `zio-flink-operator-lab` 仍在运行；其监听范围、CRD 使用关系和与 Apache Operator 的互斥关系尚未查明。

因此，不能直接在新 namespace 安装候选 Operator 1.16.1。namespace 不能隔离集群级 CRD、ClusterRole 或重叠的控制器监听范围；storedVersion 相同也不能证明 schema 兼容。

**最小下一步是只读核对所有权和兼容性：** 对比现有 CRD 的 schema、served/storage 版本、转换配置与候选 Chart；核对 Helm 所有权、RBAC、webhook 资源名及各控制器监听范围。只有证明确实无需改动其他项目依赖的对象、不会重复接管资源，才可把复用 OrbStack 列为可实施方案。安装时如何处理 CRD 必须以该核对结果为依据，不能预先假定跳过 CRD 即安全。

若需要删除、降级或覆盖共享 CRD，或无法排除控制器冲突，则保持现状并标记 `external_blocked`。届时再评估独立实验集群的资源成本与授权，不自动创建第二套环境，也不清理旧 Helm release。

## 3. 缺失项的最小补齐顺序

下表均为待实施方案，不表示已执行。先做不改变运行状态的确认，再按后续授权补齐项目专属资源。

| 编号 / 责任角色 | 最小方案，优先复用 | 影响边界与完成证据 |
|---|---|---|
| E01 平台工程：确定集群边界 | 复用现有 OrbStack 前，完成第 2 节的 CRD/控制器核对；确认节点实际使用量、已有 requests/limits、磁盘余量与实验时段 | 不切 context、不升级集群、不改已有工作负载；明确兼容结论和资源预算后，才申请 `magic-flink-p0` namespace 与受限权限 |
| E02 平台工程：兼容客户端与 Operator | 保留现有 Helm；后续取得与 Kubernetes 1.35 兼容的 kubectl 精确版本，放项目专用工具目录并显式调用；Operator 1.16.1 仍按 T02 核验 | 不替换全局 kubectl；Operator 部署以 E01 通过为前提。验收包括真实 Deployment/Pod、限定监听范围、RBAC 和最小 Application 行为，不能只看 Helm 状态 |
| E03 数据库负责人：实验库 | 优先复用已有 PostgreSQL 17.11 镜像层；后续新建本项目实例与新数据卷，用不同 schema/角色隔离元数据、固定源、目标 | 不启动旧项目容器、不扩容旧 StatefulSet、不挂载旧 PVC。记录实际 `SELECT version()`、访问权限、TLS 适用约束和固定数据核验结果；主机无需先装 psql，可使用专属实例的客户端 |
| E04 存储负责人：专属对象范围 | 先确认是否有已获准、可访问的测试 S3 服务；若有，仅申请专属 bucket/prefix 与受限凭据引用。若没有，后续再选一个获准产品建立独立本地实例和新卷 | 目前没有已确认可复用 endpoint，不扫描或尝试其他项目凭据。记录服务端版本、权限、TLS、保留期；本地预实验不能代替银行目标存储验收，原子写入与 multipart 等能力仍由 T06 实测 |
| E05 平台/数据负责人：固定批次 | E03/E04 就绪后，仅创建第 4 节的合成数据与独立控制账本 | 确认版本身份、冻结权限和保留期后再读取；预期值必须与实际查询证据分列，不能把生成规则当作核验结果 |
| E06 平台工程：资源与清理范围 | 后续只给本项目资源加所有者标识、设配额和保留期；先完成单 Application 小数据预检，再进入计划规定的并行和竞争实验 | 不停止其他项目腾资源，不清理共享镜像/PVC。资源不足就暂停相关实验；不以降低 F01–F13 验收要求换取通过 |

kubectl 支持范围依据 [Kubernetes 官方版本偏差策略](https://kubernetes.io/releases/version-skew-policy/#kubectl)：与 API server 相差不超过一个 minor。此处建议优先选择 1.35 系列的获准补丁版本，实际版本与摘要取得后登记到 T02，当前不虚填。

## 4. 固定数据与访问边界：待创建、待核验

沿用 P0 计划，两个 JDBC 源使用同一独立 PostgreSQL 实验实例，不增加 HTTP/BQ/ClickHouse 实现范围：

- `orders_jdbc`：100 笔订单，`order_id` 为 1…100，金额为 1.00…100.00，采用精确十进制定点类型。
- `customers_jdbc`：10 个唯一客户；订单使用固定客户映射且全部匹配。映射明细在创建 fixture 时登记，当前未生成。
- Join 预期：100 行、金额总计 5050.00；控制数来自独立 fixture 账本，不从本轮采集计数器反推。
- 空输入与过滤后空输出使用独立批次，分别验证，不能混成同一种情况。
- 批次置为 ready 前核验内容和控制数；置为 ready 后，实验读取角色没有修改权限。保留版本、摘要、创建者、冻结时间及保留截止时间；这些字段当前尚无实际值。

建议资源命名为 namespace `magic-flink-p0`、容器 `magic-flink-p0-postgres`、数据卷 `magic-flink-p0-*`、对象前缀 `magic-flink/p0/{executionId}/`，统一标识 `app.kubernetes.io/part-of=magic-flink`。这些名称仅是待核准约定，未创建资源；不能凭名称前缀批量删除，清理需基于本次创建资源的明确清单。

仍需取得：实际资源所有者、凭据引用、网络/TLS 方式、源只读权限、元数据与目标角色、对象创建/读取/废弃权限、保留期，以及故障注入可操作的准确对象范围。公开仓库只保存脱敏配置说明，不保存凭据或内部端点。

## 5. 必须保护的已有环境

以下对象只作为保护清单，不作为待接管资源：

- `springboot-inventory-tutorial-postgres-1` 已停止的容器及其数据；只考虑复用已存在的镜像层。
- `paimon-gateway` 中副本数为 0 的 PostgreSQL StatefulSet 与 20 GiB PVC。
- `cube-ha-remediation`、`cube-operator-demo` 中原 MinIO 数据 PVC；PVC 为 Bound 不代表有可用 S3 服务，也不证明其数据可供本项目使用。
- 原 Flink namespace、Helm release、集群级 CRD，以及运行中的 `zio-flink-operator-lab`、Meteor、Spark Operator、cert-manager 等。
- 现有 Flink 安装目录、全局 Java/Maven 配置及其他项目的仓库认证信息。

共享主机和集群仍会共享 CPU、内存及磁盘，不能承诺仅靠 namespace 就“零影响”。本项目新增负载必须以余量检查、限额和受控实验时段为前提；无法证明满足边界时，暂停部署。

## 6. T01 验收状态与下次复查

| T01 要求 | 当前状态 | 尚缺证据 |
|---|---|---|
| 环境身份、架构、Operator 状态 | 部分完成 | 项目专属范围、所有者、CRD 兼容性及控制器监听关系 |
| PostgreSQL / S3 及受限访问 | `external_blocked`：尚无本项目可用环境 | 实际连接、权限、版本、TLS、保留与清理边界 |
| 固定数据及不可变性 | `NOT_RUN / evidence_incomplete` | 实际批次、控制账本、查询结果与写权限隔离 |
| 故障注入隔离 | 已给出约束，未验收 | 专属资源清单、权限验证及实验前复核 |

可复查入口为 `docker context show`、`docker version`、`docker ps -a`、镜像 inspect、`kubectl --context orbstack --request-timeout=8s` 下的节点/工作负载/CRD 只读查询，以及 `helm --kube-context orbstack list -A`。避免直接导出 Secrets、环境变量全文或包含凭据的连接配置。

T01 尚未通过；T05–T07 的运行前提不满足。仍可继续只读版本、源码与边界研究，但不得因此勾选实际环境和故障恢复验收。
