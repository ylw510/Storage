# ClickHouse 源码笔记

> 图文对照源码讲一条执行路径。论点以函数名、setting 和步骤名为准。文中 Mermaid 在 GitHub 上直接打开即可渲染。

## 文章目录

| 篇 | 文件 | 边界 |
|----|------|------|
| **1** | [分布式 JOIN：等值键如何被送进同一个 bucket](./01-distributed-shuffle-join.md) | `make_distributed_plan` 打开之后的 shuffle：规则规划器、Cascades、`ShuffleExchange` 切开成 `ShuffleSend` / `ShuffleReceive` |

### 推荐阅读

1. [第 1 篇](./01-distributed-shuffle-join.md) — 等值键如何进同一个 bucket，以及 `Shuffle HashJoin`、`ShuffleExchange`、`ShuffleSend` 这三层名字。
