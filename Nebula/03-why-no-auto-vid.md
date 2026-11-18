# 为什么 Nebula 不提供自增 VID / UUID

> 核心结论：**VID 必须由应用显式指定**；开源与企业版都不提供服务端自增 ID 或库内 UUID。这是分布式架构取舍。

---

## 为什么不做

### 1. VID 就是路由键

```text
partId = vid % numParts + 1
```

写入前就要知道 VID，才能打到正确的 storaged / Raft 组。若库端生成 ID，每次插入都要先问「发号器」，吞吐和延迟都差。

### 2. 自增依赖全局协调

多机多 part 下要保证全局唯一单调 ID，通常要中心化计数器或强一致分配服务——正好是 Nebula 想避开的热点。

### 3. 图语义里「身份」归应用

官方建议用业务唯一键当 VID（用户 ID、设备 ID 等）。同 VID = 同顶点；身份应由业务定义，而不是库偷偷发号。

### 4. 历史也证伪了「库内 UUID」

1.x 有过 `uuid()`，要落 KV 查冲突，性能差，后续版本拿掉了。文档写明：**不提供 auto increasing ID，也不提供 UUID**。

---

## 实际怎么做


| 做法               | 说明                 |
| ---------------- | ------------------ |
| 业务主键当 VID        | 最推荐，点查最快           |
| 应用侧 UUID / 雪花 ID | 写入前生成，再 `INSERT`   |
| `hash("业务键")`    | 仅适合规模不大、能接受碰撞风险的场景 |


```ngql
-- 业务唯一键直接当 VID（FIXED_STRING）
INSERT VERTEX player(name, age) VALUES "Tim Duncan":("Tim Duncan", 42);

-- 用 hash 算出 INT64 VID（仍要写在 VALUES 左边）
INSERT VERTEX player(name, age) VALUES hash("Tim Duncan"):("Tim Duncan", 42);
```

---



## 相关

- 磁盘上 VID 如何进 key、如何映射 part：见 [02-on-disk-storage.md](./02-on-disk-storage.md) §3、§4

