# Milvus 原理（三）：索引如何加速查询

> **系列索引**：[README.md](./README.md) · 上一篇：[查询](./02-query.md)
> **本篇边界**：同一批 `books` 上对比暴力、IVF、HNSW 和标量索引；Knowhere 如何接到 Segcore。不重复 WAL / tSafe。

---

## 1. 本篇要回答的问题

| 问题 | 本篇 |
|------|------|
| 没有向量索引时 search 算多少次距离？ | ✅ |
| IVF 怎样用「先比质心再比桶」省计算？ | ✅ |
| HNSW 怎样在图上走，而不是扫全表？ | ✅ |
| `category == "tech"` 如何用标量索引加速？ | ✅ |
| 索引文件存在哪，QueryNode 怎么用？ | ✅ |

---

## 2. 先算清：暴力到底贵在哪

查询 `q = [0.7283, 0.6853]`（`encode("Milvus HNSW vector search")`→PCA），L2，`topk=3`，filter `category=="tech"`，PK=4 已删。

**有效候选** 5 个 tech 向量（含已删的 4；删掉后 4 个）。2 维时 4 次 L2 微不足道。把表放大：

| 规模 | 无过滤暴力 | 含义 |
|------|------------|------|
| 8 行 × 2 维 | 8 次 L2 | 本教学例子 |
| 1e8 行 × 768 维 | ~1e8 次 768 维点积 | 单核秒～分钟级，无法在线 |

索引的交易：**不保证每次都找到绝对最近邻（ANNS）**，换 **少访问的向量个数**。FLAT 索引仍是暴力，只是走同一套 Knowhere 接口。

Milvus 向量执行引擎是 **Knowhere**（包 Faiss / hnswlib 等）。Sealed 有就绪索引时：

```text
SearchOnSealedIndex → VectorIndex::Query(dataset, search_info, bitset)
```

（`internal/core/src/query/SearchOnSealed.cpp`）

`bitset` 仍是第 2 篇那份：过滤 + MVCC + delete。**索引加速距离计算，不替代删除语义。**

Growing 段：`IndexingRecord` 与数据同步后走小索引，否则按 chunk 暴力（`SearchOnGrowing.cpp`）。

---

## 3. 示例数据在二维平面上

向量来自 MiniLM 再 PCA 到 2 维（[README](./README.md)）。大致分布：

```text
 y≈1.0  ┤  1*
 y≈0.74 ┤           6*
 y≈0.69 ┤              q●
 y≈0.14 ┤                    4*
 y≈0    ┼────────────────────────
 y≈-0.26┤                    2*
 y≈-0.25┤  5*
 y≈-0.38┤  3*
 y≈-0.91┤              8*
 y≈-1.0 ┤        7*
        ┼─── x≈-1 ──── 0 ──── +1 ───
```

fiction（3,5,7）偏左下；多数 tech 更靠右或靠上。IVF 仍可按「两大团」直觉理解。

精确 L2（未过滤）到 `q`：

| PK | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
|---:|--:|--:|--:|--:|--:|--:|--:|--:|
| L2 | 0.7264 | 0.9714 | 1.9666 | 0.6095 | 1.9369 | 1.4052 | 1.8502 | 1.6316 |

---

## 4. IVF_FLAT：先问「在哪个桶」

**IVF** = Inverted File。把空间划成 `nlist` 个桶（k-means 质心）。检索时只进离 q 最近的 `nprobe` 个桶，桶内再暴力（FLAT）或 PQ。

对本例取 `nlist=2`（教学；生产常见 1024～16384）：

| 桶 | 质心（类别均值，近似） | 成员 PK |
|----|------------------------|---------|
| C0 | `[0.35, 0.14]` | 1, 2, 4, 6, 8（tech） |
| C1 | `[-0.64, -0.54]` | 3, 5, 7（fiction） |

搜索参数 `nprobe=1`：

1. 算 `dist(q, C0)`、`dist(q, C1)` → C0 近得多（≈0.66 vs ≈1.84）。
2. **只扫描 C0 的 5 个向量**，不碰 C1 的 3 个。
3. 应用 bitset：PK=4 丢掉；再取 top-3 → **1, 2, 6**，与精确一致。

| | 距离计算次数（2 维） |
|--|---------------------|
| 暴力 8 行 | 8 |
| IVF nprobe=1 | 2 次质心 + 5 次桶内 = 7（例子太小，几乎不省） |
| 1e8 行、nlist=4096、桶均匀、nprobe=16 | 4096 次质心 + 约 16×(1e8/4096) ≈ **39 万** 次向量，而不是 1e8 |

`nprobe` 越大越准、越慢。`nprobe=nlist` 退化为全扫。

**IVF_PQ**：桶内向量换成 PQ 码本，内存从 `n×dim×4` 字节降到 `n×m` 字节级，距离用查表近似。本例 2 维不必 PQ；768 维十亿级常用。

建索引在 **sealed segment 落盘之后** 由 DataCoord 调度、DataNode/IndexNode 执行，产物写对象存储，QueryNode load 进 Knowhere。Growing 一般不建完整 IVF（行还在变），最多 interim。

---

## 5. HNSW：在「高速公路网」上贪心走

**HNSW** = Hierarchical Navigable Small World。多层图：

- 上层边少，跨度大（高速）
- 下层边多，局部密（地面道路）
- 每个点最多 `M` 条边；构建时 `efConstruction` 越大图越好、越慢

对本例画一层示意（真实 HNSW 有多层；同类别点更容易互相连）：

```text
上层（稀疏）:    1 -------- 3
                 |          |
下层（稠密）:  2-4-1-6       3-5-7
                 8/           （fiction）
               （tech 侧）
```

查询（`ef` = 动态候选集大小，必须 ≥ topk）：

1. 从上层入口开始，贪心走到该层局部最近 → 偏向 tech 侧（靠近 q）。
2. 下一层扩大邻居，维护大小为 `ef` 的候选堆。
3. 底层取出 top-3。bitset 为 0 的节点（PK=4）不进入结果。

**距离计算次数**大约随 `ef × log(n)` 增长，而不是 `n`。8 个点看不出 log；百万点时 HNSW 往往比 IVF_FLAT 低延迟、高召回，内存是图（每点约 `M` 条 int 边）+ 原向量（若不量化）。

搜索参数：`ef`（有的 API 叫 `efSearch`）。`ef=3` 刚够 topk，召回可能掉；`ef=64` 更稳、更慢。

Knowhere 对 HNSW 同样 `Query(dataset, config, bitset)`。Milvus 侧参数名 `M`、`efConstruction` 在 index params 里（见 `internal/util/indexparamcheck`）。

---

## 6. 过滤 + ANN：两种加速要叠在一起

第 2 篇的 bitset 和第 3 篇的 IVF/HNSW **同时生效**：

```text
        ┌─────────────┐
expr    │ INVERTED /  │──► 行 bitset（category=tech → 1,2,4,6,8）
        │ BITMAP / 扫 │
        └─────────────┘
                │
        ┌───────▼────────┐
delete  │ MVCC + delta   │──► PK=4 置 0
        └───────┬────────┘
                │
        ┌───────▼────────┐
ANN     │ IVF / HNSW     │──► 只在 bitset=1 的点里找近邻
        │ + Knowhere     │
        └────────────────┘
```

- **高选择率**（`year > 1900`，几乎全过）：bitset 几乎全 1，时间 ≈ 纯 ANN。
- **低选择率**（`id == 1`）：应靠 **PK Bloom / 主键索引** 剪 segment，而不是对全图 HNSW 再过滤。
- **中等选择率**（本例 `category=="tech"` ≈ 50%）：标量倒排先出 bitset，再 ANN（pre-filter）；或 iterative filter。

---

## 7. 标量索引：少扫 `category` / `year`

向量索引不管 `category == "tech"`。没有标量索引时，FilterBitsNode 扫描整列 VARCHAR。

| 类型 | 适合 | 本例 |
|------|------|------|
| **INVERTED** | 等值 / IN / 部分范围 | `category → {tech: offsets 0,1,2,3 on S0}` |
| **BITMAP** | 低基数 | `category` 只有 2 值，bitset 与运算极快 |
| **STL_SORT** | 数值范围 | `year > 2022` 二分 |
| **TRIE / NGRAM** | 字符串前缀 / 模糊 | `title like "Mil%"` |

S0 上 `INVERTED(category)`：

```text
"tech"    → [offset 0, 1, 2, 3]
"fiction" → []          # S0 全是 tech
```

表达式直接取 posting list，不必逐行 strcmp。再 AND 上 delete bitset。

标量索引文件格式见 `docs/design-docs/design_docs/20260209-scalar-index-unified-format.md`（V3 单文件 `MVSIDXV3`）。QueryNode load sealed 时挂到字段上，表达式叶子走 `chunk_scalar_index`。

---

## 8. 索引存在哪？何时能用？

```text
create_index(field_name="embedding", index_params={"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 64}})
load_collection("books")
```

1. DataCoord 给 **每个 sealed segment** 下发 BuildIndex（不是整表一个文件）。
2. Worker 读该段 `insert_log/.../104/*` 向量列，Knowhere `BuildAll`，写出 index blob。
3. 路径在 etcd/`SegmentIndex` 或 Storage V3 manifest 的 `_index/`。
4. QueryCoord 通知 QueryNode `LoadSegments`，Segcore `index_ready_bitset` 置位。
5. 之后 `vector_search` 走 `SearchOnSealedIndex`；未就绪则扫 raw 列（`ChunkedSegmentSealedImpl::vector_search` 的 if/else）。

**S0 与 S1 各有一份 HNSW**。Search 仍然按 shard 并行，只是每段内部从 O(n) 变成 O(log n) 量级。

Growing G0：可能只有 interim IVF_FLAT；G0 seal+flush+build 完成后，查询切到完整 HNSW。

---

## 9. 把三种路径对照同一查询

同一请求：`q=[0.7283,0.6853], topk=3, category=="tech"`，delete 4。

| 引擎 | S0 上做什么 | S1 上做什么 | 全局 Reduce |
|------|-------------|-------------|-------------|
| 无向量索引 | 扫 4 个向量，bitset 掉 4 | 扫 4 个，过滤后只留 1 | 1, 2, 6 |
| IVF nlist=2 nprobe=1 | 进 C0 桶，跳过 fiction（S0 无 fiction） | 质心仍选 C0，**本段 C0 只有 PK=1** | 同上 |
| HNSW | 从图入口走到 tech 侧，跳过节点 4 | 图在 fiction 侧，bitset 只留 1 | 同上 |

8 行时三者结果应一致。**数据到百万以上**，IVF/HNSW 的延迟差一个数量级，召回由 `nprobe` / `ef` 调。

---

## 10. 最小可跑示例（对照本系列）

```python
from pymilvus import MilvusClient, DataType

client = MilvusClient("http://localhost:19530")

schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
schema.add_field("id", DataType.INT64, is_primary=True)
schema.add_field("title", DataType.VARCHAR, max_length=64)
schema.add_field("category", DataType.VARCHAR, max_length=16)
schema.add_field("year", DataType.INT64)
schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=2)

index_params = client.prepare_index_params()
index_params.add_index(field_name="embedding", index_type="HNSW", metric_type="L2",
                       params={"M": 8, "efConstruction": 32})
index_params.add_index(field_name="category", index_type="INVERTED")

client.create_collection("books", schema=schema, index_params=index_params, num_shards=2)
client.insert("books", [
    {"id": 1, "title": "Milvus Internals", "category": "tech", "year": 2024, "embedding": [0.0724, 0.9974]},
    {"id": 2, "title": "Vector Search", "category": "tech", "year": 2023, "embedding": [0.9666, -0.2564]},
    {"id": 3, "title": "The Great Novel", "category": "fiction", "year": 2020, "embedding": [-0.9253, -0.3791]},
    {"id": 4, "title": "Database Systems", "category": "tech", "year": 2022, "embedding": [0.9908, 0.1352]},
    {"id": 5, "title": "Poetry of Spring", "category": "fiction", "year": 2021, "embedding": [-0.9686, -0.2485]},
    {"id": 6, "title": "HNSW Paper", "category": "tech", "year": 2016, "embedding": [-0.6759, 0.7370]},
    {"id": 7, "title": "Mystery Island", "category": "fiction", "year": 2019, "embedding": [-0.0369, -0.9993]},
    {"id": 8, "title": "Knowhere Guide", "category": "tech", "year": 2025, "embedding": [0.4056, -0.9141]},
])
client.flush("books")
client.delete("books", filter="id == 4")
client.load_collection("books")

res = client.search(
    collection_name="books",
    data=[[0.7283, 0.6853]],  # encode("Milvus HNSW vector search") → PCA
    limit=3,
    filter='category == "tech"',
    output_fields=["title", "year"],
    search_params={"metric_type": "L2", "params": {"ef": 16}},
)
print(res)
```

预期 top-3 PK：**1, 2, 6**（与 [README](./README.md) / `books_embeddings.json` 真值一致）。向量可用 `python encode_books.py` 重算。

---

## 11. 源码地图

| 主题 | 位置 |
|------|------|
| Knowhere 接口 | `docs/design-docs/design_docs/20211223-knowhere_design.md` |
| Sealed 走索引 / 走原列 | `ChunkedSegmentSealedImpl::vector_search` |
| `VectorIndex::Query` | `internal/core/src/query/SearchOnSealed.cpp`, `SearchOnIndex.cpp` |
| Growing 小索引 / 暴力 | `internal/core/src/query/SearchOnGrowing.cpp` |
| 索引类型名 | `internal/util/indexparamcheck/index_type.go` |
| 向量类型是否支持某 index | `internal/util/vecindexmgr/` |
| 标量索引文件 | `docs/design-docs/design_docs/20260209-scalar-index-unified-format.md` |

---

## 12. 三篇串起来

1. **存**：行被拆到 shard，在 growing 里变成列，flush 成每列一个 binlog；delete 写 delta。
2. **查**：Proxy 按 shard 问 Delegator；Segcore 用 bitset 挡住过滤失败和已删行，再算近邻；Proxy Reduce。
3. **索引**：bitset 圈定的集合上，IVF/HNSW 少算距离；倒排/BITMAP 少扫标量列。文件按 **segment** 建、按 segment load。
