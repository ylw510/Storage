# Nebula 索引：写入与查询原理

> 核心结论：**二级索引不是独立 B-Tree 服务**，而是与图数据同在按 space 的 RocksDB 里、**按 part 分片的 Index 类型 KV**；`CREATE INDEX` 只写 metad，存量要靠 `REBUILD`；写入时与数据同批 Raft `OP_BATCH_WRITE`；查询时 **扇出到所有 part** 做 prefix/range，必要时回表。
>
> 对照：[02-on-disk-storage.md](./02-on-disk-storage.md) §4.3 · [04-query-principles.md](./04-query-principles.md) · 仓库 `doc/05-architecture.md` §8 写路径 14b。

---

## 1. 总览

```text
CREATE TAG/EDGE INDEX
        │  只写 metad（IndexItem）
        ▼
  新 INSERT/UPDATE/DELETE
        │  同 part、同 OP_BATCH_WRITE：数据键 + 索引键
        ▼
  RocksDB（type=Index 的 key）
        ▲
LOOKUP / MATCH 属性起点
        │  graphd 扇出全部 part leader
        ▼
  LookupProcessor：prefix / range → 可选回表 get(数据键)
```

| 动作 | 落在哪 | 说明 |
|------|--------|------|
| `CREATE … INDEX` | **metad** | 登记 indexId、列定义；**不回填** storaged |
| `REBUILD … INDEX` | storaged 各 part | 扫已有 Tag/Edge，生成索引键 |
| DML（有索引后） | storaged | 删旧索引 + 写新索引 + 写数据，原子一批 |
| `LOOKUP` / 索引起点 | storaged 全 part | 无种子 VID，必须扫所有分片上的索引前缀 |

---

## 2. Tag Index vs Edge Index

| | Tag Index | Edge Index |
|--|-----------|------------|
| DDL | `CREATE TAG INDEX … ON tag(cols)` | `CREATE EDGE INDEX … ON edge(cols)` |
| Meta | `schema_id = tag_id` | `schema_id = edge_type` |
| 磁盘键 | `vertexIndexKeys` → 后缀 **VID** | `edgeIndexKeys` → 后缀 **src + rank + dst** |
| 维护时机 | 写/改/删该 Tag 属性时 | **仅正向边**（`edgeType > 0`）；反向边不建索引 |

常见约束（创建路径）：

- 单索引最多 **16** 列（复合索引）
- `STRING` 进索引会变成定长 `FIXED_STRING`（需 `type_length`）
- `GEOGRAPHY` 一般单列；内部可按 S2 cell 裂成多条索引键

---

## 3. Key 布局

### 3.0 KV 存储模型

storaged 不维护独立的 Tag 表或索引表，图数据以 RocksDB 的 **Key → Value** 存放（同一 space 使用 Default Column Family，见 [02-on-disk-storage.md](./02-on-disk-storage.md)）：

| | 含义 |
|--|------|
| **Key** | 一串字节，按 **字典序** 排序 |
| **Value** | 另一串字节 |

- `CREATE TAG` / `INSERT` 写的是 **Tag 数据键** 及其 **value**（属性行）。
- 建了索引并写入点后，会 **多写** 若干条 **索引键**（value 通常为空）。
- **已知 VID** → 用 Tag 那种 key 做 `get`，读 value。
- **按属性找 VID**（如 `age == 42`）→ 用索引 key 做 **prefix / range**，从 key 里解析 VID。

Tag 键与索引键落在 **同一个 RocksDB、同一套 memtable** 里；靠 key 前缀里的 part、类型（Tag / Index）等区分，而不是靠多张表或多个 Column Family。

### 3.1 公共头：`item`（4 字节）

key 最前 4 字节在源码里记为 `item`，类型为 `int32`，由 part 与键类型按位拼成：

```text
item = (partId << 8) | NebulaKeyType
```

- `kPartitionOffset = 8`：高位为 **partId**，最低一字节为 **键类型**（`kTag_ = 0x01`，`kIndex = 0x03`，见 `Types.h`）。
- 文档中常写作 `part2 | kTag_`，表示 partId=2、类型为 Tag 的 `item`；同一 part、同一类型的 key 在字典序上相邻。

Tag 数据键组装（`NebulaKeyUtils::tagKey`）：

```cpp
int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kTag_);
key.append(item, 4B).append(VID, vIdLen).append(tagId, 4B);
```

索引键的 `item` 同理，类型为 `kIndex`（`IndexKeyUtils::vertexIndexKeys`）。

### 3.2 Tag 行（数据键）

表示某个 **VID** 在指定 **Tag**（如 `player`）上的完整属性行。

```text
┌──────────────┬─────────────────┬─────────┐
│ item (4B)    │ VID (vIdLen)    │ tagId   │
│ part + kTag_ │ 定长，如 101    │ player  │
└──────────────┴─────────────────┴─────────┘
        Value：name、age 等列的 RowWriter 编码（业务数据在这里）
```

| 查询场景 | 用法 |
|----------|------|
| `FETCH PROP ON player "…"` | 拼出上表 key → `get` → 解码 value |

### 3.3 点索引（二级索引键）

```text
| item (4B) | indexId (4B) | 索引列编码值… | [nullableBitSet 2B?] | VID (vIdLen) |
```

其中 `item = (partId << 8) | NebulaKeyType::kIndex`（`0x03`）。

用于按索引列检索：在索引 `i_age` 下，将 **encode(age)** 映射到 **VID**；不重复存储完整属性行。

```text
┌──────────────┬──────────┬─────────────┬─────────┐
│ item (4B)    │ indexId  │ encode(列…) │ VID     │
│ part + Index │ i_age    │ 如 age=42   │ 101     │
└──────────────┴──────────┴─────────────┴─────────┘
        Value：通常为空 ""；有 TTL 时可为 indexVal
```

组装逻辑（`IndexKeyUtils::vertexIndexKeys`）：`item` + `indexId` + 编码后的索引列 + `VID`（定长填充）。

| 类型 | 说明 |
|------|------|
| **Tag 行** | Key 含 part、VID、tagId；属性在 value 中。 |
| **索引行** | Key 含 part、indexId、索引列编码、VID；value 多为空，查询时解析 key。 |

**同一 INSERT 后的磁盘条目（示例）**：VID=101，`age=42`，part 2：

```text
【主数据 — Tag 行】
KEY:   [part2|Tag] + VID(101) + tagId(player)
VALUE: 编码后的 name="Bob", age=42

【索引 — i_age】
KEY:   [part2|Index] + indexId(i_age) + encode(42) + VID(101)
VALUE: (空)
```

排序效果：所有 `[part2|Index|i_age|…]` 排成一段；`encode(42)` 相同的键相邻 → `LOOKUP … age == 42` 只扫该前缀。

| 查询目标 | 使用的 key |
|----------|------------|
| 已知 VID，读取 name / age | Tag 行 key，`get` 后解码 value |
| 按 age=42 解析 VID | 索引 key，前缀至 `indexId + encode(42)`，VID 在 key 尾部 |
| 索引仅含 age，还需 name | 由索引 key 得到 VID 后，对 Tag 行 key 回表 `get` |

### 3.4 边索引

```text
| item (4B) | indexId (4B) | 索引列编码值… | [nullableBitSet 2B?] | src | rank (8B) | dst |
```

### 3.5 编码与分片要点

- **索引 Value** 通常为空；有 TTL 时 value 里带 TTL 信息（`indexVal`）。
- 列按索引定义顺序拼接；**最左前缀**决定能否用等值 prefix，最后一列可做 range。
- 数值/时间等经特殊编码，使 **字典序 ≈ 数值序**（与 edge rank 思路类似）。
- 可空列：NULL 用类型宽度的 `0xFF` 填充，并在列值后、主键前挂 **2 字节** null bitset。
- 索引键落在 **与数据相同的 part**（点按 VID，边按 src VID），不是全局二级库。

---

## 4. 写入原理

### 4.1 CREATE 不等于立刻可查旧数据

```ngql
CREATE TAG INDEX i_age ON player(age);
-- 此时 metad 有 IndexItem；历史点没有索引键
REBUILD TAG INDEX i_age;   -- 后台 job：按 part 扫 player → 写索引键
```

重建期间并发 DML 常用 **OperationKey** 记「该改哪条索引」，rebuild 扫完再回放，避免和全量构建打架。

### 4.2 有索引时的 DML（原子批）

`AddVertices` / `AddEdges` 发现该 schema 有索引 → `doProcessWithIndex` → `BatchHolder`：

```text
对每个受影响的索引：
  1. 若有旧行：根据旧属性 collectIndexValues → remove 旧索引键
  2. 根据新属性 → put 新索引键（GEO 可能多 key）
  3. put / delete 数据键（Tag 或 Edge）
→ encodeBatchValue → Raft OP_BATCH_WRITE（与数据同日志提交）
```

| 操作 | 索引侧 |
|------|--------|
| INSERT | 写新索引键 |
| UPDATE | 删旧 + 写新（属性变了索引键就变） |
| DELETE | 删对应索引键 |
| INSERT EDGE | 只给 **正向边** 维护索引 |

这保证：**同一 part 内**「数据可见 ⇔ 索引可见」，不会出现「点写上了、索引还没提交」的半成功（相对该 Raft 提交而言）。

### 4.3 小例

```ngql
CREATE TAG INDEX i_age ON player(age);
INSERT VERTEX player(name, age) VALUES 101:("Tim Duncan", 42);
```

`part(101)=2` 时，同一 `OP_BATCH_WRITE` 写入两条 KV（布局见 §3.2、§3.3）：

| KV | 含义 |
|----|------|
| Tag key（part2, VID=101, tag=player） | 点属性行（value 存 name、age） |
| Index key（part2, indexId=i_age, encode(42), VID=101） | 可被 `LOOKUP … WHERE age == 42` 前缀扫描命中 |

---

## 5. 查询原理

### 5.1 LOOKUP 链路

```text
LOOKUP ON player WHERE player.age == 42 YIELD id(vertex), player.name
        │
        ▼
graphd：LookupValidator → IndexScan 计划
        Optimizer（IndexScanRule）选 index + column_hints
        │
        ▼
StorageClient::lookupIndex → getHostParts → **所有 part** 的 leader
        │
        ▼
LookupProcessor
  IndexVertexScanNode / IndexEdgeScanNode
    → Projection → [Dedup] → [Limit/TopN/Agg]
    → 残留谓词用 IndexSelectionNode
```

### 5.2 prefix vs range

| hints | RocksDB |
|-------|---------|
| 等值前缀（或空 hints 扫整索引） | `prefix(indexPrefix(part, indexId) + 编码列…)` |
| 最后一列范围 | `range(start, end)` |

每个 part 执行时 `resetPart(partId)`，只改 key 里的 part 前缀。

### 5.3 何时回表

索引键里已有：索引列 + VID（或边主键）。若 `YIELD` 还要 **未覆盖的列**（例如索引只有 `age`，还要 `name`）：

```text
命中索引键 → 取出 VID
  → get(tagKey(part, VID, tagId))  // 回表数据键
  → 解码 RowReader 取其余列
```

覆盖索引（返回列 ⊆ 索引列，且无字符串截断/地理等不确定编码）时可 **只解索引键**，免一次 `get`。

`STRING` 按定长截断存储、GEO 按 cell 裂键时，常被标成需要回表或残留过滤。

### 5.4 MATCH 中的索引起点

| 起点策略 | 作用 |
|----------|------|
| `PropIndexSeek` | 属性条件 → 建 `IndexScan` |
| `LabelIndexSeek` | 仅标签 / 选一个可用索引拿一批 VID |
| 无索引 | 退化为 `ScanVertices`（全 part 扫点） |

之后跳边仍走 `getNeighbors`（见 [04](./04-query-principles.md)），索引只负责 **找起点**。

---

## 6. 与无索引路径对比

| | 无索引 | 有索引 |
|--|--------|--------|
| 按属性找点 | 各 part `scanVertex` + 过滤 | 各 part 索引 prefix/range（通常更省） |
| 已知 VID | `getProps` / `getNeighbors` | 不经过二级索引 |
| 写入 | `OP_MULTI_PUT` 等 | 常升级为 **`OP_BATCH_WRITE`**（数据+索引） |
| 扇出 | 有 VID 则按 VID 定 part | LOOKUP **始终全 part** |

注意：索引再快，LOOKUP 也要问 **每一个 part**——选择性再高，RPC 扇出下界仍是 part 数；极高基数时要靠分区数、副本与并发，而不是「全局聚合索引」。

---

## 7. 实践注意

1. **建索引后记得 REBUILD**，否则只对新写入生效（严格说：旧数据查不到）。
2. 过滤条件尽量匹配 **最左前缀**；`WHERE b == 1` 对 `(a,b)` 复合索引帮助有限。
3. Edge 索引只服务正向边属性；按入边属性查要另想办法（或依赖应用侧建模）。
4. 索引过多会放大每次 INSERT/UPDATE/DELETE 的批大小与 WAL 量。
5. TTL：索引键也可带 TTL，扫描时过期条目会被跳过。

---

## 8. 相关源码速查

| 主题 | 路径 |
|------|------|
| Tag / Edge 数据键 | `src/common/utils/NebulaKeyUtils.{h,cpp}`（`tagKey`） |
| 索引键编码 | `src/common/utils/IndexKeyUtils.{h,cpp}`（`vertexIndexKeys`、`indexPrefix`） |
| 键类型枚举 | `src/common/utils/Types.h`（`NebulaKeyType`） |
| 创建（meta） | `src/meta/processors/index/Create{Tag,Edge}IndexProcessor.cpp` |
| 写入维护 | `src/storage/mutate/Add{Vertices,Edges}Processor.cpp`（`doProcessWithIndex`） |
| 更新 / 删除 | `src/storage/exec/UpdateNode.h`、`Delete{Vertices,Edges}Processor.cpp` |
| 重建 | `src/storage/admin/Rebuild{Tag,Edge}IndexTask.cpp` |
| 查询 | `src/storage/index/LookupProcessor.cpp`、`exec/Index{Scan,VertexScan,EdgeScan}Node.*` |
| 优化选索引 | `src/graph/optimizer/rule/IndexScanRule.cpp`、`util/OptimizerUtils.cpp` |
| MATCH 起点 | `src/graph/planner/match/{Prop,Label}IndexSeek.cpp` |
| 客户端扇出 | `src/clients/storage/StorageClient.cpp`（`lookupIndex`） |
