# T02 候选版本、制品检查与最小补齐方案

**检查日期：** 2026-10-18。**清单状态：** `NOT_FROZEN / evidence_incomplete`。T02 只读盘点已完成，制品核验、完整依赖解析和准入尚未完成，T02 未验收。

本文保留基于项目提交 `76f7739` 的本地版本、制品摘要与官方元数据检查，并补充基于 `1225e2a` 的 E01 Chart/CRD 静态核对。关联：[环境与固定数据](environment-and-fixtures.md)、[P0 计划](../superpowers/plans/2026-10-18-p0-baseline-and-file-commit-validation.md)。主体章节整理前轮证据；本轮核心构建新增证据见第 6 节；复用建议不是对原计划候选的正式替换，T08 才能形成通过验收的 ReleaseManifest。

## 1. 原候选与本机现状

| 组件 | 原计划候选 | 实际发现 | 最小处理建议与状态 |
|---|---|---|---|
| 作业 JDK | Temurin 17.0.20.1+1 或获准同代发行版 | 主机已有 Microsoft 17.0.14+7-LTS；PATH 的 java/javac 为 Corretto 8.0.442；默认 Maven 实际用 Azul 11.0.9.1 | 调查阶段可在子进程显式选择已有 Java 17；旧补丁的准入和安全状态未知，不直接冻结为发行版 |
| Maven | 3.9.11 | 默认 3.8.8；另有 3.9.10 和 Homebrew 3.9.16 | 建议评估复用 3.9.16，登记变更理由、构建插件兼容与准入后再替换候选；当前不修改原计划 |
| Flink 核心/Planner/文件连接器 | 1.20.5 | `~/software/flink/flink-1.20.5` 存在，manifest 版本相符；部分 JAR 摘要与 Central 不同 | 版本号已定位，发行来源未闭合；先核验来源，不覆盖现有安装 |
| S3 文件系统插件 | 与 Flink 同版 | `opt` 中存在 hadoop/presto 两种 1.20.5 JAR，未在 plugins 启用 | 不同时启用两套实现；后续按 T03 选定插件并仅装入本项目固定镜像，不能修改共享 Flink 目录 |
| Operator | 1.16.1，Chart/CRD/镜像对应同一候选 | 旧 Helm 为 1.17-SNAPSHOT，实际 Apache 控制器缺失；E01 已证明现有四份 CRD schema 与 1.16.1 一致，默认表示统一后 spec 一致 | 保留共享 CRD，独立 namespace 限定监听的方案有静态支持；镜像身份、准入及实际运行仍未核验，不升级或接管旧 release |
| Kubernetes / kubectl / Helm | 待登记精确环境 | 服务端 1.35.6+orb1；客户端 1.33.9；Helm 4.0.1 | 复用集群有条件；需专用兼容 kubectl。Operator 对该具体 Kubernetes 组合的运行证据未取得 |
| PostgreSQL | 17.11 | 已有 linux/arm64 的 `postgres:17-alpine` 镜像，元数据 PG_VERSION 为 17.11 | 可复用镜像层；需锁定 digest、来源与扫描，启动专属实例后再核验实际服务版本 |
| pgJDBC | 42.7.14 | 官方 POM 可访问；本地有 42.7.13 等缓存，未找到精确 42.7.14 | 保持候选；后续通过获准仓库取得并核验，不为省下载而无依据降版本 |
| Parquet / Hadoop | 由 Flink 指定版本解析 | 官方 POM 指向 Parquet 1.15.2、Hadoop 2.10.2；部分本地缓存存在 | 只是已发现的版本，不是完整运行闭包；提供范围和镜像归属仍需确认 |
| S3 服务端 / SDK | 待产品选择和依赖解析 | 无本项目可用服务端；插件内 SDK 闭包未完整核对 | 服务端版本、配置、SDK 和准入均未冻结；不能将 `mc` 客户端计为完成 |
| 扫描与签名证据 | 精确制品对应的获准结果 | 未取得本项目 SBOM、扫描或准入报告 | 先确认组织已有扫描通道，避免为盘点安装一套本地工具 |

既有子进程验证已成功：用 Microsoft JDK 17.0.14 启动现有 Maven 3.9.16，`mvn -version` 返回对应厂商与版本。这只证明该 Maven 能启动，未运行依赖解析、编译或打包。

其他项目的 `zio-flink-operator-lab:local` 镜像环境元数据声明 `jdk-17.0.20.1+1`，但未在镜像内执行 Java 核对，且该镜像没有 RepoDigest。它不能据此充当已核验的 Temurin 基础镜像或本项目发行制品。

Operator 1.16 的上游兼容信息见[官方兼容说明](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.16/docs/deployment/compatibility/)，支持 Flink 1.20、Helm 3/4 不等于当前共享 CRD 和 Kubernetes 组合已经实测通过。

E01 新增的静态证据详见[环境文档第 2 节](environment-and-fixtures.md#2-e01-专项结果保留共享-crd限定新-operator-范围)：旧 Apache 配置仅监听 `flink-lineage-test`；ZIO 对拟建 namespace 的 list 请求被 RBAC 拒绝；候选 Chart 在 `watchNamespaces=[magic-flink-p0]`、跳过 CRD 的离线渲染中，不生成集群级 RBAC，两个 webhook 也仅选择新 namespace。该结果不替代控制器与 Kubernetes 的运行兼容验证。

## 2. 制品身份检查：有版本号仍不够

本地 Flink 目录包含核心、Table/Planner、文件连接器与两个 S3 插件。以下 SHA-256 来自本地文件；文件名和 manifest 不能替代发行来源校验。

| 本地相对路径 | 本地 SHA-256 |
|---|---|
| `lib/flink-dist-1.20.5.jar` | `493092df4f5e37cdccda1789ac55feb6c4eabe819b6614d6220f2707bc53aa0b` |
| `lib/flink-connector-files-1.20.5.jar` | `5d3de95b1aed889c5829e70f14746ccd3c5eb97b11cc17948ab6fdce26c5fcf3` |
| `opt/flink-s3-fs-hadoop-1.20.5.jar` | `4775b5fe8efcb81a43b5192782a1a414a4e60a4186aa0a4ac2d9ef41ecad339e` |

与 Maven Central 同名制品的 SHA-1 对照如下。SHA-1 这里只用于记录此次差异，不作为发行准入的唯一校验机制。

| 制品 | 本地 SHA-1 | Central SHA-1 | 结果 |
|---|---|---|---|
| flink-connector-files 1.20.5 | `ac398c8b9e6cb437eae187dc0bb7ed55f7bd81c2` | `45803f94ca395b9c0e47611203a13842e31ff654` | 不同，字节数同为 563713 |
| flink-s3-fs-hadoop 1.20.5 | `ea68b9b52d2277c2ff4f867e62e0afee1993bc43` | `538f409a51532a8f9d790eca08ba4cd359419e63` | 不同，字节数同为 31260342 |

对照来源：[files 校验文件](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-files/1.20.5/flink-connector-files-1.20.5.jar.sha1)、[S3 Hadoop 校验文件](https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.5/flink-s3-fs-hadoop-1.20.5.jar.sha1)。本地 Maven 缓存的 Flink core 1.20.5 JAR/POM 与对应 Central SHA-1 相符，但这不能证明整个安装目录可信。

**判定：来源待核实，不判定为损坏或恶意。** 发行包中的 JAR 与独立发布到 Central 的 JAR 可能来自不同构建；应先找到当前目录对应的原始发行包、签名及校验文件，或可信构建记录，再针对相同来源进行比较。本次有限检查范围内未找到该证据。核实前，这两个 JAR 不得计为已核验发行输入；不移动、删除或覆盖它们。

已有 PostgreSQL 镜像记录如下：

- 本地 image ID：`sha256:25a89970a83255ae96484d709005bb35aa71f7455f941bc1634514f31c5d5c62`。
- RepoDigest：`postgres@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24`。
- 本地 inspect 显示 linux/arm64、PG_VERSION=17.11；尚未启动本项目实例。后续需明确仓库 digest 对应的索引/平台 manifest 关系，不能混同 image ID 和分发摘要，也不能仅用可变 tag 冻结。

## 3. 获取路径和依赖边界

初次 T02 只请求官方元数据或校验文件，下列路径返回 HTTP 200。后续 E01 为静态比较读取了 Operator Chart 压缩包并在临时目录离线渲染，临时文件已清除；未拉取镜像、安装依赖或部署 Chart：

| 元数据 | 检查结论与边界 |
|---|---|
| [Flink core 1.20.5 POM](https://repo.maven.apache.org/maven2/org/apache/flink/flink-core/1.20.5/flink-core-1.20.5.pom) | 可读取，不等于完整发行包已验签或获准 |
| [Flink Parquet 1.20.5 POM](https://repo.maven.apache.org/maven2/org/apache/flink/flink-parquet/1.20.5/flink-parquet-1.20.5.pom) | 可读取；观察到 parquet-hadoop 1.15.2，以及 provided 的 Flink Hadoop FS / Hadoop 依赖 |
| [pgJDBC 42.7.14 POM](https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.pom) | 可定位；精确本地 JAR、摘要和扫描仍缺 |
| [Operator 1.16.1 Chart 索引](https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.16.1/index.yaml) | E01 已读取对应 Chart、对照索引摘要并比较四份 CRD，完成离线渲染；镜像身份、签名与准入仍需取证 |
| Maven 3.9.11 官方 SHA-512 路径 | 可访问；未下载发行包或执行校验 |

E01 Chart 制品为 `flink-kubernetes-operator-1.16.1-helm.tgz`，67952 字节，SHA-256：`e3e107e7bf0a9e9762d699f81d782991a974d3193532e4953a4d4a8e3e9d8579`，与官方索引 digest 一致。四份 CRD 原始 spec 的差异仅为 conversion=None、listKind 与打印列 priority=0 的默认表示；OpenAPI schema 无差异。保留既有 CRD 不代表将其登记为本项目拥有，未来升级仍须重新核对共享边界。

已读取的 POM 中，`flink-parquet:1.20.5` 引用 `parquet-hadoop:1.15.2`，`flink-hadoop-fs:1.20.5` 与 Hadoop common/hdfs/mapreduce-client-core 2.10.2 为 provided。当前没有项目 POM，也没有 effective POM、完整 dependency tree、SBOM 或打包结果。因此，“缓存中有 parquet-hadoop”不代表 Parquet 能在最终运行 classpath 上使用。

继续沿用原计划的归属原则：核心、Planner 归固定镜像；文件系统与其 SDK 归选定固定插件；应用及 JDBC/格式适配归单一应用包，具体 provided 依赖的提供者必须逐项确认；构建与测试工具只进入构建/测试范围。日志、压缩库、Shade 合并规则、SPI 和 Service 是否需要独立 SDK 的结论均未完成，不在此补造版本。

Maven 配置存在环境串用风险：不同安装的全局 settings 包含旧 HTTP mirror 或其他项目仓库/认证配置。本次未解析全部生效 profile，也未调用这些私有仓库。补齐时应显式选定 Maven、项目专用 user/global settings 与获准仓库；不改 `~/.m2/settings.xml`、其他 Maven 安装配置或复制其他项目凭据。已有缓存仅在身份校验后复用；需要隔离时使用项目专属缓存，不默认复制整份 `.m2`。

## 4. 缺失项最小补齐与验收

下表列出剩余补齐方案；V03 已完成的 Chart/CRD 静态核对单独标明。安装、部署与构建仍待后续授权，可继续只读确认来源、兼容与准入渠道。

| 编号 / 责任角色 | 最小补齐方案 | 可判定的完成证据 |
|---|---|---|
| V01 平台研发：JDK/Maven 入口 | 优先复用现有 Java 17 与 Maven 3.9.16 做调查；只在命令子进程设置 JAVA_HOME/PATH。发行 JDK 需厂商、补丁准入；若现有版不能获准，再在项目专用目录取得获准版 | 实际 java/javac/Maven 三者路径、完整版本一致；若采用 3.9.16 或其他 JDK，记录候选变更和受影响验证，不改系统默认或 shell 配置 |
| V02 平台研发：Flink 来源 | 先追溯现有发行包的校验/签名或构建记录；无法证明时，后续另取可验证的 1.20.5 制品到项目专属目录 | 可复核的来源、签名/强摘要和逐项制品身份；现有共享安装保持原状 |
| V03 平台工程：Operator 与镜像 | Chart 摘要与共享 CRD 静态对比已完成；继续取得镜像、基础镜像、arm64 平台信息及准入，部署前复查最终清单 | 补齐镜像/依赖摘要和扫描；Operator JVM 与作业 JVM 分别登记。保留共享 CRD，不据静态一致性提前宣称运行兼容 |
| V04 平台研发：依赖闭包 | 在后续获准的最小构建阶段取得 pgJDBC 42.7.14 等缺失制品，解析完整传递依赖与插件内依赖，明确镜像/插件/应用/测试归属 | 无动态版本、SNAPSHOT、未登记运行时下载；输出精确清单与摘要，T05 再证明 classpath、SPI 和真实依赖运行有效 |
| V05 安全/制品管理：扫描渠道 | 先确认已有获准 CI 或扫描服务能否接收本项目固定制品；可用则复用，不优先安装全局工具 | 报告与 JAR/插件/镜像摘要绑定，包含规则/数据库时间、阻断项和处置结论；命令可用或扫描任务已提交都不算完成 |
| V06 平台/数据库/存储：最终组合 | 复用经核验的 PostgreSQL 镜像层；取得实际存储产品版本、专属配置摘要和客户端版本 | T02 候选输入可定位、可校验、可准入；T05–T07 同一组合实测后由 T08 冻结。没有运行证据不能提前生成已通过的 ReleaseManifest |

V01 的本地工具选择可以先于环境部署完成；V03 的共享资源静态边界已有证据，但部署仍依赖 E01 剩余预算与准备条件；V04 需后续允许最小构建，当前不创建 POM；V05 可先确认服务与规则，最终报告必须针对实际制品。P0 只维护一套候选路线，不为当前缺件并行引入 Flink 2.x 或第二套 Sink 实现。

## 5. 当前结论与后续门槛

已满足的是版本与现有资源可定位、部分官方元数据可获取、现有 Java 17 能启动 Maven 3.9.16，以及 E01 的 Chart 摘要、CRD 结构与作用范围静态核对。未满足的是完整发行来源闭环、依赖归属、Operator 镜像和运行兼容性、资源预算、实际存储身份及安全准入。

因此 T02 未通过，G1 版本冻结仍为 `evidence_incomplete`；运行所需的 Operator、专属数据库和对象存储尚未就绪，相关实验为 `external_blocked / NOT_RUN`。此标记表示环境前提缺失，不表示本轮“未安装”的范围约束是一项失败。F01–F13 均未执行，G2–G6 无新增通过证据。

后续可以针对 1.20.5 开展带条件的 T03 静态研究，但应记录其候选制品尚未冻结，正式接口结论须绑定最终采用的准确制品和源码。只有 T01/T02 的缺口补齐并满足原验收标准后，才能把后续运行任务标记为具备执行条件。


## 6. 2026-10-18 本地核心构建组合（不冻结运行发行版）

本次复用已有工具及 `~/.m2` 缓存，不安装 JDK/Maven，不修改默认 PATH、不下载构建依赖、不使用本机 Flink 分发 JAR。`mvn -o package` 成功只证明下列核心代码组合可本地构建，未取代上述 T02 运行制品和扫描关口。

| 用途 | 精确版本/坐标 | 本轮证据 |
|---|---|---|
| JDK | Microsoft 17.0.14，aarch64 | 子进程 JAVA_HOME 指定已有目录；实际 compile/test/package 成功 |
| Maven | 3.9.16，build 2bdd9fddda4b155ebf8000e807eb73fd829a51d5 | `/opt/homebrew/bin/mvn -version`，全程 offline |
| 唯一核心运行依赖 | org.yaml:snakeyaml:2.6 | 本地 JAR 与 Central SHA-1 相符；仅 compose 节点，不反序列化 Java 对象 |
| 测试 | org.junit.jupiter:junit-jupiter:5.11.4 | 64 个测试通过，测试依赖不进入普通应用 JAR |
| Maven 插件 | resources 3.3.1 / compiler 3.13.0 / surefire 3.5.3 / jar 3.4.2 | 已写入 POM，缓存闭包支持离线 package |

SnakeYAML JAR SHA-256：`c8f7a98e7394adda02f6317249710e4d1b4c7a25aa8c7eace0c2eea52eb8bf85`；本地与[官方 SHA-1](https://repo.maven.apache.org/maven2/org/yaml/snakeyaml/2.6/snakeyaml-2.6.jar.sha1)均为 `2bc14918a2f8d5414749ab12d0c590cd3198b8c1`。此校验是来源对照，不能代替签名、漏洞扫描和行内准入。JUnit/插件运行成功不代表整个缓存已验签。

T03 固定 Flink 源码 commit 和接口范围见[源码核对](file-commit-protocol.md)。当前 POM 未引入 Flink、JDBC、Parquet、S3 SDK；不代表最终发行依赖闭包已齐。当前 JAR 为开发中的核心库，不能部署或当作已扫描 fat JAR 提交。
