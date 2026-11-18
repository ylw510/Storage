# Nebula 磁盘存储格式

> 核心结论：**图数据不是自研页文件/列存，而是编码成 KV → 经 Raft WAL 提交 → 落入 RocksDB**。

---

## 1. 总览

```text
nGQL INSERT / UPDATE / DELETE
        │
        ▼
  Tag / Edge / Index Key  +  RowWriterV2 Value
        │
        ▼
  Raft WAL（按 part 分段文件）  ──复制──→ followers
        │ commit（过半）
        ▼
  RocksDB（按 space 一个实例）  ← 长期存放图数据
```

有状态组件：


| 组件           | 磁盘上存什么                                       |
| ------------ | -------------------------------------------- |
| **storaged** | 图数据 KV（RocksDB）+ 每 part 的 Raft WAL           |
| **metad**    | 元数据 KV（同样 RocksDB + Raft，通常单 space / 单 part） |
| **graphd**   | 无图数据落盘（会话等走 meta）                            |


---

## 2. 磁盘目录布局

配置项 `--data_path`（可多盘）下，每个 GraphSpace 一个 RocksDB Engine：

```text
<data_path>/nebula/<spaceId>/
├── data/                 # RocksDB 根目录（SST / CURRENT / MANIFEST / LOG …）
│   ├── CURRENT
│   ├── MANIFEST-*
│   ├── *.sst
│   └── ...
├── wal/<partId>/         # 该 part 的 Raft FileBasedWal（默认与 data 同 space 根）
│   └── ...               # 分段 wal 文件
└── checkpoints/<name>/   # 可选：备份/checkpoint
```

要点：

- **一个 space → 一个 RocksDB**（`RocksEngine`：`{data_path}/nebula/{spaceId}/data`）。
- **一个 space 内多个 Part 共享同一 RocksDB**，靠 key 前缀里的 `partId` 区分。
- **每个 Part 有独立 Raft WAL**：`{walRoot}/wal/{partId}`（见 `NebulaStore::newPart`）。
- 多 `--data_path` 时，不同 part 可分布在不同盘上的不同 Engine。

写入时还有两层“日志”概念：


| 层                     | 作用                                    |
| --------------------- | ------------------------------------- |
| **Raft FileBasedWal** | 复制与崩溃恢复的权威日志；过半成功后才应用到 RocksDB        |
| **RocksDB WAL**       | Engine 内部；可用 `rocksdb_disable_wal` 关闭 |


---



## 3. 分区：VID → Part

路由在 **graphd** 侧完成（`MetaClient::partId`），`partId` 编进 key，storaged 不再重算。

```cpp
// clients/meta/MetaClient.cpp
PartitionID partId(int32_t numParts, const VertexID id) {
  uint64_t vid = 0;
  if (id.size() == 8) {
    memcpy(&vid, id.data(), 8);          // INT64 / FIXED_STRING(8) 直接当整数
  } else {
    vid = MurmurHash2(id.data());        // 变长字符串 VID
  }
  return vid % numParts + 1;             // part 从 1 开始
}
```

- **边按 src VID 路由**：正向边与出点同 part。
- 插入边时通常还会写 **反向边**（`edgeType` 取负），反向边的 “src” 是原 dst，因此可能落在 **另一个 part**。

---



## 4. Key 布局

`type` 与 `partId` 打包进 4 字节小端整数（`Types.h`）：

```text
item = (partId << kPartitionOffset) | NebulaKeyType
     = (partId << 8) | type          // kPartitionOffset = 8，kTypeMask = 0xFF
```


| NebulaKeyType | 值      |
| ------------- | ------ |
| Tag           | `0x01` |
| Edge          | `0x02` |
| Index         | `0x03` |
| System        | `0x04` |
| KeyValue      | `0x06` |
| Vertex        | `0x07` |




### 4.1 Tag（点属性）

```text
| item (4B) | vertexId (vIdLen, 右补 \0) | tagId (4B) |
```

- 一个点的多个 Tag → 多条独立 KV。
- 定长 VID + 右补 `\0` 是前缀扫描（按点取全部 tag）能正确工作的前提。



### 4.2 Edge

```text
| item (4B) | srcId (vIdLen) | edgeType (4B) | rank (8B) | dstId (vIdLen) | placeholder (1B) |
```

- `edgeType > 0`：正向边；`< 0`：反向边。
- `rank` 经 `encodeRank` 编码后写入（保证字典序与数值序一致）。
- `placeholder` 默认 `1`（边版本占位）。



### 4.3 Index

由 `IndexKeyUtils` 构造，大致：

```text
| item (type=Index) | indexId | 索引列编码值... | 主键信息(VID 或 src/rank/dst) |
```

有索引的写入会在同一条 Raft `OP_BATCH_WRITE` 里同时改数据键与索引键。

### 4.4 System

如 `systemCommitKey(partId)`：持久化该 part 的 Raft 提交位点，供重启恢复。

---



## 5. Value：RowWriterV2

属性不是 JSON，而是紧凑二进制行（`codec/RowWriterV2.h`）：

```text
<header 1B> <schema version 0~7B> <NULL flags> <定长字段区> <变长串尾部>
```

Header（v2）：`0000 1 vvv` —— 中两位为编码器版本，低三位为 schema 版本占用字节数。


| 类型                         | 占用                             |
| -------------------------- | ------------------------------ |
| BOOL / INT8                | 1                              |
| INT16                      | 2                              |
| INT32 / FLOAT / DATE       | 4                              |
| INT64 / DOUBLE / TIMESTAMP | 8                              |
| STRING / GEOGRAPHY         | 8（4B offset + 4B length，内容在尾部） |
| FIXED_STRING               | schema 声明长度                    |
| DATETIME                   | 15                             |


其它要点：

- 定长字段按序号 **O(1)** 定位。
- 可空列用 NULL 位图（每 8 个可空字段 1 字节）。
- `ALTER` **只追加 schema 版本**，旧行仍带旧版本号；读时 `RowReaderWrapper` 按版本选 provider，新列取默认值 / null。

---



## 6. 写入如何落到盘上

```text
storaged BaseProcessor
  → 编 Tag/Edge/Index KV
  → Part::asyncMultiPut / asyncAtomicOp
  → 编码成 Raft 日志（OP_MULTI_PUT / OP_BATCH_WRITE）
  → RaftPart：① 写 FileBasedWal  ② 复制到 quorum
  → 过半成功 → Part::commitLogs
  → 解码日志 → RocksDB WriteBatch → commitBatchWrite
  → 更新 systemCommitKey
```

读路径不经 Raft：直接对 RocksDB 做 `get` / `prefix` / `range`（邻居遍历常用 `edgePrefix`）。

---



## 7. 详细示例：从 nGQL 到磁盘字节

下面用官方常见的 basketball 风格数据，把 **目录、分区、Key 十六进制、Value 结构、WAL→RocksDB** 串起来。

### 7.1 Schema 与假设

```ngql
CREATE SPACE basketball(
  partition_num = 10,
  replica_factor = 3,
  vid_type = INT64
);

USE basketball;
CREATE TAG player(name string, age int);
CREATE TAG team(name string);
CREATE EDGE serve(start_year int, end_year int);
```

约定（仅示例，真实 ID 以 metad 分配为准）：


| 对象            | ID                            |
| ------------- | ----------------------------- |
| spaceId       | `1`                           |
| tag `player`  | TagID = `1`                   |
| tag `team`    | TagID = `2`                   |
| edge `serve`  | EdgeType = `101`（反向 = `-101`） |
| `--data_path` | `/data/nebula`                |
| 字节序           | 小端（x86）                       |
| `vIdLen`      | `8`                           |


写入：

```ngql
INSERT VERTEX player(name, age) VALUES 101:("Tim Duncan", 42);
INSERT VERTEX team(name) VALUES 215:("Spurs");
INSERT EDGE serve(start_year, end_year) VALUES 101 -> 215@0:(1997, 2016);
```



### 7.2 算 Part

```text
part(101) = 101 % 10 + 1 = 2
part(215) = 215 % 10 + 1 = 6
```


| 数据                         | 落在 part | 说明            |
| -------------------------- | ------- | ------------- |
| 点 `101` 的 player 属性        | **2**   |               |
| 点 `215` 的 team 属性          | **6**   |               |
| 正向边 `101 → 215`            | **2**   | 按 src=101     |
| 反向边 `215 → 101`（type=-101） | **6**   | 按“反向 src”=215 |

**为什么正向边落在 part 2、不是按 dst 分？** 边按 **src VID** 路由，不按 dst。  
`INSERT EDGE ... 101 -> 215` 的 src 是 `101`，故 `part(101)=2`，正向边与出点同 part。这样从点 `101` 扫出边时，点和正向边在同一 part，本地就能做邻居遍历。反向边以原 dst `215` 为“src”，才落到 `part(215)=6`。

三副本时，part 2 / part 6 各自有独立 Raft 组；**同一 space 的所有 part 仍共享** `/data/nebula/nebula/1/data` 这一套 RocksDB（key 内带 partId）。

### 7.3 磁盘上出现的目录

```text
/data/nebula/nebula/1/
├── data/                 # space 1 的 RocksDB（含 part2、part6 … 全部已提交 KV）
└── wal/
    ├── 2/                # part 2 的 Raft WAL（含 101 点 + 正向边的日志）
    └── 6/                # part 6 的 Raft WAL（含 215 点 + 反向边的日志）
```



### 7.4 Tag Key 逐字节

`item = (partId << 8) | 0x01`。

**点 101 / player / part 2**

```text
item = (2 << 8) | 0x01 = 0x00000201
VID  101 = 0x0000000000000065
TagID 1  = 0x00000001

Key (16 bytes):
  01 02 00 00 | 65 00 00 00 00 00 00 00 | 01 00 00 00
  └─ item ──┘   └────── VID 101 ──────┘   └─ tagId ─┘
```

**点 215 / team / part 6**

```text
item = (6 << 8) | 0x01 = 0x00000601
VID  215 = 0x00000000000000D7
TagID 2  = 0x00000002

Key (16 bytes):
  01 06 00 00 | D7 00 00 00 00 00 00 00 | 02 00 00 00
```



### 7.5 Edge Key 逐字节

`item = (partId << 8) | 0x02`。`rank` 经 `encodeRank`：先 `rank ^= (1<<63)`，再按 **大端** 写出（保证字典序 ≈ 数值序）。

```text
rank = 0
  → 0 ^ (1<<63) = 0x8000000000000000
  → 大端字节：80 00 00 00 00 00 00 00
placeholder = 0x01
```

**正向边 part 2：101 → 215，type=101**

```text
item     = 0x00000202
src=101  = 65 00 00 00 00 00 00 00
type=101 = 65 00 00 00                 # int32 LE，0x65 = 101
rank     = 80 00 00 00 00 00 00 00     # encodeRank(0)，大端
dst=215  = D7 00 00 00 00 00 00 00
ph       = 01

完整 Key（33 字节）：
  02 02 00 00
  65 00 00 00 00 00 00 00
  65 00 00 00
  80 00 00 00 00 00 00 00
  D7 00 00 00 00 00 00 00
  01
```

**反向边 part 6：215 → 101，type=-101**

```text
item      = 0x00000602
src=215   = D7 00 00 00 00 00 00 00
type=-101 = 9B FF FF FF                 # int32 LE
rank      = 80 00 00 00 00 00 00 00
dst=101   = 65 00 00 00 00 00 00 00
ph        = 01

完整 Key（33 字节）：
  02 06 00 00
  D7 00 00 00 00 00 00 00
  9B FF FF FF
  80 00 00 00 00 00 00 00
  65 00 00 00 00 00 00 00
  01
```

邻居遍历 `GO FROM 101 OVER serve` 时，storaged 对 part 2 做前缀扫描：

```text
edgePrefix(vIdLen=8, partId=2, src=101, type=101)
→ item + src + type
```

即可扫出所有从 101 出发、类型为 serve 的边。

### 7.6 Value（RowWriterV2）示意

以 `player(name="Tim Duncan", age=42)`、schema version = 0 为例（示意，非精确 dump）：

```text
Offset  内容
------  ------------------------------------------------
0       header：v2 + schema 版本长度（如 0x08 表示 v2 且 ver 占 0 字节，具体依赖实现）
…       NULL flags（若 name/age 可空）
…       name：8 字节 (offset, length) 指向尾部
…       age：4 字节 INT32 → 2A 00 00 00 (= 42)
…       尾部："Tim Duncan" 的 UTF-8 字节
```

`team(name="Spurs")`、`serve(1997, 2016)` 同理：定长 int 原地写，string 走尾部。

读时：

1. 用 key 从 RocksDB 取出 value 字节串；
2. `RowReaderWrapper` 读 header / schema version；
3. 向 metad 缓存的 schema 取对应版本 provider；
4. 按字段 O(1) 解出属性。



### 7.7 一次 INSERT 在 WAL / RocksDB 中的形态

以 `INSERT VERTEX player ... VALUES 101:(...)` 为例（无索引）：

```text
graphd
  → storaged(AddVertices)
  → 构造 KV：key=上节 Tag Key，value=RowWriterV2(...)
  → Part(space=1, part=2)::asyncMultiPut
  → Raft 日志 OP_MULTI_PUT{ (key, value) }
  → 追加到 /data/nebula/nebula/1/wal/2/
  → 复制到 part2 的其它副本
  → commitLogs → RocksDB WriteBatch Put(key, value)
  → 更新 systemCommitKey(part=2)
```

若该 Tag 上有索引 `CREATE TAG INDEX ... ON player(age)`，同一批会变成 `OP_BATCH_WRITE`：删旧索引键（若有）+ 写新索引键 + 写数据键，保证原子提交。

### 7.8 RocksDB 里最终可见的 KV 集合

space=1 的 `data/` 中（逻辑视图，SST 内仍是这些 key）：


| Key（逻辑）                                 | Value                       |
| --------------------------------------- | --------------------------- |
| Tag part=2, vid=101, tag=player         | RowV2(name, age)            |
| Tag part=6, vid=215, tag=team           | RowV2(name)                 |
| Edge part=2, 101→215, type=101, rank=0  | RowV2(start_year, end_year) |
| Edge part=6, 215→101, type=-101, rank=0 | 同上（或空属性，依实现）                |
| （可选）Index keys                          | 索引列编码                       |
| System commit keys per part             | 已提交 log id / term           |


---



## 8. 与 ClickHouse / 其它存储的对比


|           | Nebula                     | ClickHouse MergeTree（对照） |
| --------- | -------------------------- | ------------------------ |
| 物理文件      | RocksDB SST + Raft WAL     | Part 目录 + 列文件 + marks    |
| 逻辑单位      | Tag/Edge 一行 = 一条 KV        | 一行 = 多列文件中的同行偏移          |
| 分区        | VID hash/取模 → partId 进 key | 分区键 → 独立 part 目录         |
| 复制        | 按 part 的 Raft              | 按 shard 的复制表 / Keeper    |
| Schema 变更 | 追加版本，旧行不重写                 | mutation / 轻量 ALTER 等    |


---

