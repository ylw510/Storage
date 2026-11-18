# SeaweedFS 源码原理解读

基于 [seaweedfs/seaweedfs](https://github.com/seaweedfs/seaweedfs) 源码的分层笔记：从 S3 API → Filer → Master → Volume/Needle。

> 阅读目标：搞清一次写/读路径上 **谁生成什么、数据落在哪、元数据如何引用**。

---

## 文档索引

| 文档 | 说明 | 状态 |
|------|------|------|
| [01-overview.mdx](./01-overview.mdx) | 整体架构与组件职责 | 已写 |
| [02-needle-and-chunk.mdx](./02-needle-and-chunk.mdx) | Needle / Chunk / Fid 关系与读写示例 | 已写 |
| [03-assign-volume.mdx](./03-assign-volume.mdx) | AssignVolume：申请 fid 与可写 volume | 已写 |
| [04-multipart-upload.mdx](./04-multipart-upload.mdx) | S3 Multipart：`.uploads` / `.part` / Complete | 已写 |
| [05-multipart-dataloss-window.mdx](./05-multipart-dataloss-window.mdx) | Issue #10663：Complete 与 `s3.clean.uploads` 窗口 | 大纲 |

---

## 推荐阅读顺序

```text
01 整体架构
  → 02 Needle / Chunk / Fid（存储原语）
  → 03 AssignVolume（写前如何拿到位置）
  → 04 Multipart（S3 分片如何落到上述原语）
  → 05 Complete 数据丢失窗口（问题与修复方向）
```

---

## 源码锚点（仓库内路径）

| 主题 | 主要路径 |
|------|----------|
| Needle / FileId | `weed/storage/needle/` |
| Chunk 上传 | `weed/operation/upload_chunked.go` |
| AssignVolume | `weed/server/filer_grpc_server.go` → `master_grpc_server_assign.go` → `topology/` |
| S3 Put / putToFiler | `weed/s3api/s3api_object_handlers_put.go` |
| Multipart | `weed/s3api/s3api_object_handlers_multipart.go`, `filer_multipart.go` |
| ObjectTransaction | `weed/server/filer_grpc_server.go`, `weed/s3api/s3api_object_routed_write.go` |
