# Nebula 原理笔记

> 论点对照当前仓库源码（`/root/work/nebula`），聚焦存储、查询与集群运维路径。

## 目录

| 篇 | 问题 | 文件 |
|----|------|------|
| **1** | `BALANCE LEADER` 底层做了什么？单次 leader 转移的时序？ | [01-balance-leader.md](./01-balance-leader.md) |
| **2** | 图数据在磁盘上如何存储？Key/Value 格式与完整写入示例 | [02-on-disk-storage.md](./02-on-disk-storage.md) |
| **3** | 为什么不提供自增 VID / UUID？ | [03-why-no-auto-vid.md](./03-why-no-auto-vid.md) |
| **4** | 查询怎么跑？graphd 流水线、路由扇出、读路径与 GO/MATCH | [04-query-principles.md](./04-query-principles.md) |
| **5** | 二级索引如何写入与查询？CREATE/REBUILD、键布局、LOOKUP 回表 | [05-index-principles.md](./05-index-principles.md) |
