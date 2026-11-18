# Nebula 查询原理

> 核心结论：**读路径不经 Raft**；graphd 完成解析/计划/优化后按 VID 算 part、按 leader 扇出到 storaged，storaged 对 RocksDB 做 `get` / `prefix` / `range`。
>
> 对照：磁盘布局见 [02-on-disk-storage.md](./02-on-disk-storage.md)；官方架构细表见仓库 `doc/05-architecture.md` §3、§7、§9。

---

## 1. 总览

```text
Client
  │  nGQL (graph.thrift)
  ▼
graphd
  Parse → Validate → Plan → Optimize → Schedule/Execute
  │  StorageClient：VID → partId → leader host 扇出
  ▼
storaged（各 part 的 leader）
  GetNeighbors / GetProp / Lookup / Scan*
  │  读路径不经 Raft
  ▼
RocksDB（get / prefix / range）
```

| 组件 | 查询时做什么 |
|------|----------------|
| **graphd** | 无状态查询引擎：AST、计划、优化、RPC 扇出、结果拼装 |
| **metad** | 不跑查询本身；提供 schema / part 分布 / leader 缓存（经 MetaClient） |
| **storaged** | 本机 part 上扫 KV；默认要求 **leader + lease 有效** 才读 |

与写对照：写要 Raft WAL → 过半 → 提交进 RocksDB；**读只看已提交数据**。

---

## 2. graphd 流水线

入口：`GraphService::future_executeWithParameter` → `QueryEngine` → `QueryInstance`。

| 阶段 | 目录 / 关键类 | 产出 |
|------|----------------|------|
| **Parse** | `parser/`（Bison/Flex） | AST（`Sentence`） |
| **Validate** | `validator/`（`GoValidator`、`MatchValidator`、`LookupValidator`…） | 校验类型/权限，绑 schema |
| **Plan** | `planner/`（`GoPlanner`、`MatchPlanner`、`LookupPlanner`…） | `PlanNode` 树 |
| **Optimize** | `optimizer/`（谓词下推、`IndexScanRule`、属性裁剪…） | 改写后的最优计划 |
| **Schedule** | `scheduler/AsyncMsgNotifyBasedScheduler` | 按依赖异步跑 executor DAG |
| **Execute** | `executor/query/`（`GetNeighbors`、`Traverse`、`IndexScan`…） | 调 `StorageClient`，结果进 `ExecutionContext` |

单次查询生命周期由 `QueryInstance` 持有，结束后释放。

---

## 3. 常见语句 → Storage RPC

| 语句 | 典型计划思路 | Storage API |
|------|--------------|-------------|
| **GO** | `Expand` / `ExpandAll` / `GetNeighbors` | **`getNeighbors`** |
| **MATCH** | 起点：`IndexScan` / `ScanVertices` / 常量 VID；跳边：`Traverse`；补点属性：`AppendVertices` | 起点 **`lookupIndex`** 或 **`scanVertex`**；跳边 **`getNeighbors`**；属性 **`getProps`** |
| **FETCH PROP ON &lt;tag&gt;** | `GetVertices` | **`getProps`** |
| **FETCH PROP ON &lt;edge&gt;** | `GetEdges` | **`getProps`** |
| **LOOKUP ON** | `IndexScan`（优化器选索引） | **`lookupIndex`** |

方向约定（GO / MATCH）：

- **出边 / OUT**：正向 `edgeType`
- **入边 / IN / REVERSELY**：用 **负** `edgeType`（依赖写入时的反向边）
- **BOTH**：正负类型都扫

---

## 4. 分区路由与扇出

路由在 **graphd** 完成（`MetaClient::partId`），storaged 不再重算：

```text
partId = vid % numParts + 1     // INT64 / 8 字节 VID 直接当整数
         // 变长字符串 VID 先 MurmurHash2 再取模
```

`StorageClient` 流程：

1. 对每个起始 VID 算 `partId`
2. 查该 part 的 **leader**（本地缓存，来自 metad）
3. 按 host 聚合 → 每台 storaged 一个 thrift 请求并发发出
4. 若返回 `E_LEADER_CHANGED`，更新缓存并重试

| 场景 | 扇出范围 |
|------|----------|
| 已知一批起始 VID（GO / FETCH / MATCH 已定位点） | 只打这些 VID 所在 part 的 leader |
| LOOKUP / Scan 无种子 VID | **所有 part** 的 leader |
| 展开后再取 **dst 点属性** | 再按 dst VID 二次 `getProps` 扇出 |

边始终按 **src VID** 路由，故正向边与出点同 part（详见 [02 §7.2](./02-on-disk-storage.md)）。

---

## 5. storaged 读路径

### 5.1 三大读处理器

| Processor | 作用 | RocksDB 访问 |
|-----------|------|--------------|
| **GetNeighborsProcessor** | 一跳邻居（可带点属性） | 对 src：`edgePrefix(part, vid, edgeType)` **prefix**；点属性可选 `get(tagKey)` |
| **GetPropProcessor** | 按 VID / 边主键取属性 | **`get`** |
| **LookupProcessor** | 走二级索引 | 索引键 **prefix / range**，再可选 **`get`** 数据键 |

另有 `ScanVertex` / `ScanEdge`：按 part 全表扫，MATCH 找不到索引起点时的兜底。

### 5.2 GetNeighbors 本地算子树（示意）

```text
TagNode(s) ──┐
             ├─ HashJoin → Filter → Aggregate → GetNeighborsNode
SingleEdgeNode(s) ─┘
```

对每个 `(partId, vid)`：`plan.go` → `NebulaKeyUtils::edgePrefix` → `NebulaStore::prefix` → RocksDB 迭代器。

### 5.3 为什么一跳出边是「本地扫」

Edge key 形态：

```text
| item(partId+type) | srcId | edgeType | rank | dstId | …
```

从点 `101` 走出边：在 **part(101)** 上对 `edgePrefix(…, 101, serve)` 做一次 prefix，**不必跨 part**。

入边能同样本地扫，是因为插入时写了 **反向边**（src/dst 对调，`edgeType` 取负），落在原 dst 的 part 上。没有反向边就无法用「当前点 + 负类型」做本地入边扫描。

### 5.4 读要不要 Raft？

**不要。** `NebulaStore::get/prefix/range` 只做 leader/lease 检查后直接读 RocksDB。

```text
写：Client → Raft WAL → 过半 → commit → RocksDB
读：Client →（须是 leader）→ RocksDB
```

---

## 6. 访问模式成本（选型直觉）

| 模式 | 典型语句 | 成本形态 |
|------|----------|----------|
| **点查** | `FETCH` / 已知 VID | 每 tag 一次 `get`，O(1) |
| **一跳扩展** | `GO` / `MATCH` 跳边 | 单 part 上 O(度数) prefix |
| **索引定位** | `LOOKUP` / MATCH 属性起点 | 各 part 上索引 prefix/range，再可选回表 |

索引写入、键布局、REBUILD 与回表细节见 [05-index-principles.md](./05-index-principles.md)。
| **全表扫** | MATCH 无索引起点 | 各 part `scanVertex`，最贵 |

优化器关键动作：把过滤条件下推到索引（`IndexScanRule`）、裁剪不需要的属性列，减少 RPC 与解码量。

---

## 7. 端到端小例

```ngql
GO FROM 101 OVER serve YIELD serve.start_year, $$.team.name;
```

假设 `partition_num=10`，`part(101)=2`，`part(215)=6`：

```text
1. graphd 解析 GO → Expand/GetNeighbors 计划
2. part(101)=2 → 只向 part 2 的 leader 发 getNeighbors(101, serve)
3. storaged：prefix 扫 101 的 serve 出边 → 得到 dst=215，属性 start_year…
4. 若要 $$.team.name：再按 215 算 part=6，向 part 6 leader getProps(team)
5. graphd 拼结果返回
```

对应磁盘上：正向边在 `wal/2` / RocksDB 的 part2 键空间；点 `215` 的 team 在 part6（见 [02 示例](./02-on-disk-storage.md)）。

---

## 8. 和「写」的对照表

| | 读 | 写 |
|--|----|----|
| 权威路径 | RocksDB 已提交数据 | Raft WAL → 过半 → RocksDB |
| 路由 | VID → part → leader | 同左（边按 src） |
| 失败语义 | 可部分重试 / leader 变更 | 通常不容忍部分成功 |
| 索引 | Lookup 走 Index key | 有索引时与数据同批 `OP_BATCH_WRITE` |

---

## 9. 相关源码速查

| 主题 | 路径 |
|------|------|
| 查询入口 | `src/graph/service/GraphService.cpp`、`QueryInstance.cpp` |
| GO / MATCH 计划 | `src/graph/planner/ngql/GoPlanner.cpp`、`match/MatchPlanner.cpp` |
| 扇出 | `src/clients/storage/StorageClient.cpp`、`MetaClient::partId` |
| 邻居读 | `src/storage/query/GetNeighborsProcessor.cpp`、`exec/EdgeNode.h` |
| 索引读 | `src/storage/index/LookupProcessor.cpp` |
| KV 读 | `src/kvstore/NebulaStore.cpp`（`get` / `prefix` / `range`） |
| 架构细表 | `doc/05-architecture.md` §7（逐步行号） |
