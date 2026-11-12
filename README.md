# lake-erasure

关联合成主体别名与活动记录，保留未能关联主体的独立明细；并在湖仓侧提供保留策略执行器：在不可变对象、历史快照和派生指标之间给出可以核验的删除闭包。全部使用本地合成数据，不提供法律判断，也不改动采集端的授权开关。

## 当前数据组织

- `identities`：`alias TEXT PRIMARY KEY, subject TEXT NOT NULL`
- `activity`：`event TEXT PRIMARY KEY, alias TEXT NOT NULL, day TEXT NOT NULL, category TEXT NOT NULL, amount TEXT NOT NULL`

`examples/source.json` 为本地合成输入（主体与活动均为虚构值），`examples/expected.json` 是当前查询的公开样例结果。`schema.json` 描述结构，运行时定义在 `warehouse/definition.py`。业务计算位于 `warehouse/domain.py`，事务装载位于 `warehouse/catalog.py`，内容寻址 JSONL 对象位于 `warehouse/store.py`。

输入批次以内容 SHA-256 固定；相同批次重投不重复写入，相同批次内容不同则报错。装载以完整输入包为事务，主键冲突不留下半批记录。关系数据保存在 SQLite，精确数量用十进制文本；导出对象按内容哈希命名，读取会重新校验，来源不在运行时修改。

## 湖仓保留策略执行器（lakehouse 包）

在 `warehouse` 当前表之外，`lakehouse` 包把明细物化为按月分区、按类别共享的不可变对象，维护贡献索引（`row_index` 行级、`agg_contrib` 主体级）、按月封存的分析快照和派生指标，并对已确认的 `subject_id` 执行删除闭包：

- **规划（`plan`/`preview`）**：固定策略（`examples/retention_rules.json`，按表给定留存窗口）、源快照序号与评估时钟。列出主体经过的明细分区、对象与汇总贡献，区分可重写对象、只能移入受限保留区的行（`examples/holds.json` 的证据保留项带独立编号与释放时刻）、必须重建的汇总。共享对象只重写不整块删除；金额等可逆指标按贡献移除并与重算结果互相校验，去重人数等不可逆指标从剩余明细重算。预览给出每个对象的行数与引用变化（旧对象 → 新对象），不只是文件名清单。超出留存窗口的分区排除在闭包之外，过期判断只采用传入时钟。
- **执行（`execute`）**：对象重写、受限区转存、汇总构建、引用切换、回收五个步骤持久化在 `plans`/`plan_steps`，中断后重入即恢复，已完成步骤幂等跳过。切换在单个事务内同时发布新明细与新汇总并写入删除屏障；切换时校验计划基线序号，过期则拒绝并要求重新规划。
- **读取（`query`/`read-object`/`audit-read`）**：普通查询总是看到同一事务发布的新明细与新汇总；指定旧快照的普通读者被删除屏障显式拒绝（`BarrierError`）；固定旧对象的读者收到明确的失效错误（`ObjectRetired`），不会悄悄拿到已删行。受限保留区与普通快照引用隔离，仅本地审计授权可读且每次访问写入 `audit_log`；同一行被多个保留项覆盖时，释放其中一个不会误清除（`release-holds` 只按传入时钟判断到期）。
- **压实与备份（`compact-*`/`backup`/`restore`/`reapply-barriers`）**：压实准备记录基线（当前快照序号与屏障计数），提交时校验，避免压实结果重新引用旧对象；备份恢复后在重放已完成删除的屏障日志（`barriers.jsonl`）之前，所有分析读取保持关闭（`RestoreIncomplete`）。
- **完成凭据（`credential`）**：只含计划指纹、计数与剩余保留理由，不复制主体内容。

湖仓目录布局：`objects/`（内容寻址明细与汇总对象）、`restricted/`（受限保留区对象）、`lake.sqlite3`（目录、快照、屏障、计划、审计）、`barriers.jsonl`（屏障日志，供恢复重放）。

## 环境与验证

Python 3.11 以上，仅使用标准库，无第三方安装。`runtime.txt` 记录验证解释器版本。时区相关开发使用系统 IANA 时区库。数据库和对象目录均由入口参数指定。源码和合成数据可以直接复制到 Linux 环境运行。

```bash
python3 -m compileall -q warehouse lakehouse tests scripts
python3 -m unittest discover -s tests -v
```

## 运行

基础导入、查询、导出往返（每次使用全新临时目录）：

```bash
python3 scripts/demo.py
```

保留策略执行器端到端演示：共享对象、不同释放日的重叠保留项、跨月快照、持有旧快照/旧对象的读者、切换中断恢复、压实基线冲突、备份恢复重放屏障，并逐项验证其他主体金额与人数不变、所有普通入口不再暴露该主体：

```bash
python3 scripts/retention_demo.py
```

分步调用可用（仓库模块位于项目根目录，需从根目录运行或设置 `PYTHONPATH`）：

```bash
python3 -m warehouse import --db /tmp/lake-erasure.sqlite3 --input examples/source.json --batch first
python3 -m warehouse inspect --db /tmp/lake-erasure.sqlite3
python3 -m warehouse export --db /tmp/lake-erasure.sqlite3 --objects /tmp/lake-erasure-objects

python3 -m lakehouse build --lake /tmp/lake --warehouse /tmp/lake-erasure.sqlite3 --clock 2026-11-12
python3 -m lakehouse seal --lake /tmp/lake --label 2026-11 --clock 2026-11-12
python3 -m lakehouse holds --lake /tmp/lake --file examples/holds.json
python3 -m lakehouse plan --lake /tmp/lake --subject synthetic-1 --clock 2026-11-12 --policy examples/retention_rules.json
python3 -m lakehouse preview --lake /tmp/lake --plan <plan_id>
python3 -m lakehouse execute --lake /tmp/lake --plan <plan_id>
python3 -m lakehouse query --lake /tmp/lake
python3 -m lakehouse credential --lake /tmp/lake --plan <plan_id>
```

`scripts/demo.py` 与 `scripts/retention_demo.py` 打印实际命令和完整输出，失败返回非零退出码，不回放预制日志。手工重复使用数据库路径时应留意已存在的业务主键。
