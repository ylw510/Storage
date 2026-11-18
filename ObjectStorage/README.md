# ObjectStorage

对象存储相关原理与实现笔记（Mintlify 文档站）。

## 本地预览

```bash
export PATH="/root/.npm-global/bin:$PATH"   # 若尚未加入 PATH
cd /root/Storage/ObjectStorage
mint dev
```

浏览器打开 http://localhost:3000（若被占用会自动改用 3001）

CLI：`mint` **4.2.860**，安装前缀 `/root/.npm-global`（已写入 `~/.bashrc` 的 PATH）

## 目录

| 路径 | 说明 |
|------|------|
| [index.mdx](./index.mdx) | 站点首页 |
| [docs.json](./docs.json) | Mintlify 导航与主题配置 |
| [reed-solomon-principles.mdx](./reed-solomon-principles.mdx) | Reed-Solomon 纠删码原理 |
| [seaweedfs/](./seaweedfs/) | SeaweedFS 源码原理解读 |
| [ceph/](./ceph/) | Ceph 原理笔记（Pool / PG / OSD / Object） |
| [snippets/](./snippets/) | MDX 可复用 React 组件 |
