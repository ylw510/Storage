# Text index `chinese` tokenizer 测试记录

环境：ClickHouse **26.11.1.1058**（`clickhousectl local use latest`）  
实例：`chinese-tokenizer-test`（HTTP 8123 / TCP 9000）  
日期：2026-11-02

连接：

```bash
clickhousectl local client -n chinese-tokenizer-test
# 或
clickhouse-client --host 127.0.0.1 --port 9000
```

说明：

- 分词器默认 **coarse_grained**；`fine_grained` 会额外产出重叠子词。
- 中文检索请用 `hasAllTokens` / `hasAnyTokens`，不要用 `hasToken`（后者只按 ASCII 分隔符切 needle）。
- 同一列不能建两个 text index。coarse / fine 分表验证。

---

## 测试表

```sql
DROP TABLE IF EXISTS tokenizer_eval;
DROP TABLE IF EXISTS tokenizer_eval_fine;

CREATE TABLE tokenizer_eval
(
    id UInt32,
    lang String,
    doc String,
    INDEX idx doc TYPE text(tokenizer = 'chinese') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE tokenizer_eval_fine
(
    id UInt32,
    lang String,
    doc String,
    INDEX idx doc TYPE text(tokenizer = chinese('fine_grained')) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO tokenizer_eval VALUES
    (1, 'zh',  '他来到了网易杭研大厦'),
    (2, 'zh',  '我来自北京邮电大学'),
    (3, 'zh',  '南京市长江大桥'),
    (4, 'en',  'ClickHouse is a fast analytical database'),
    (5, 'en',  'The text index uses inverted posting lists'),
    (6, 'en',  'Tokenizers split documents into searchable terms'),
    (7, 'mix', 'ClickHouse是一个列式数据库'),
    (8, 'mix', '使用5G网络下载iPhone'),
    (9, 'mix', 'email: foo@bar.com 你好世界');

INSERT INTO tokenizer_eval_fine SELECT * FROM tokenizer_eval;
```

`index_granularity = 1`：每行一个 granule，方便用 `EXPLAIN indexes = 1` 看裁剪比例（命中应为 `1/9`，未命中 `0/9`）。

---

## （1）中文分词

```sql
SELECT text,
       tokens(text, 'chinese') AS coarse,
       tokens(text, 'chinese', 'fine_grained') AS fine
FROM
(
    SELECT arrayJoin([
        '他来到了网易杭研大厦',
        '我来自北京邮电大学',
        '南京市长江大桥',
        '小明硕士毕业于中国科学院计算所'
    ]) AS text
);
```

| 文本 | coarse | fine |
|------|--------|------|
| 他来到了网易杭研大厦 | `['他','来到','了','网易','杭研','大厦']` | 额外拆出 `杭`、`研` |
| 我来自北京邮电大学 | `['我','来自','北京邮电大学']` | 额外拆出 `北京`、`北京邮电`、`邮电大学`、`大学` 等 |
| 南京市长江大桥 | `['南京市','长江大桥']` | 额外拆出 `南京`、`长江`、`大桥` 等 |
| 小明硕士毕业于中国科学院计算所 | `['小明','硕士','毕业','于','中国科学院','计算所']` | 额外拆出 `中国`、`科学院`、`计算` 等 |

结论：coarse 按词切分；fine 提高召回（子词可搜），索引更大。

---

## （2）英文分词

```sql
SELECT text,
       tokens(text, 'chinese') AS coarse,
       tokens(text, 'chinese', 'fine_grained') AS fine
FROM
(
    SELECT arrayJoin([
        'ClickHouse is a fast analytical database',
        'The text index uses inverted posting lists',
        'Tokenizers split documents into searchable terms'
    ]) AS text
);
```

| 文本 | coarse / fine（相同） |
|------|------------------------|
| ClickHouse is a fast analytical database | `['ClickHouse','is','a','fast','analytical','database']` |
| The text index uses inverted posting lists | `['The','text','index','uses','inverted','posting','lists']` |
| Tokenizers split documents into searchable terms | `['Tokenizers','split','documents','into','searchable','terms']` |

结论：英文按非字母数字切开，大小写保留；fine 与 coarse 一致。

---

## （3）中英混合分词

```sql
SELECT text,
       tokens(text, 'chinese') AS coarse,
       tokens(text, 'chinese', 'fine_grained') AS fine
FROM
(
    SELECT arrayJoin([
        'ClickHouse是一个列式数据库',
        '使用5G网络下载iPhone',
        'email: foo@bar.com 你好世界'
    ]) AS text
);
```

| 文本 | coarse | fine |
|------|--------|------|
| ClickHouse是一个列式数据库 | `['ClickHouse','是','一个','列式','数据库']` | 额外拆出 `数据`、`据库` |
| 使用5G网络下载iPhone | `['使用','5G','网络','下载','iPhone']` | 与 coarse 相同 |
| email: foo@bar.com 你好世界 | `['email','foo','bar','com','你好','世界']` | 与 coarse 相同 |

结论：中文按词切，英文/数字保留为独立 token；`@`、`:`、`.` 当作分隔符。

---

## （4）三种场景的索引命中

查询：

```sql
-- 结果命中
SELECT id, doc FROM tokenizer_eval WHERE hasAllTokens(doc, ['北京邮电大学']);
SELECT id, doc FROM tokenizer_eval WHERE hasAnyTokens(doc, ['大厦']);
SELECT id, doc FROM tokenizer_eval WHERE hasAllTokens(doc, ['ClickHouse', 'database']);
SELECT id, doc FROM tokenizer_eval WHERE hasAnyTokens(doc, ['inverted']);
SELECT id, doc FROM tokenizer_eval WHERE hasAllTokens(doc, ['数据库']);
SELECT id, doc FROM tokenizer_eval WHERE hasAllTokens(doc, ['iPhone']);
SELECT id, doc FROM tokenizer_eval WHERE hasAllTokens(doc, ['你好']);
SELECT id, doc FROM tokenizer_eval WHERE hasAllTokens(doc, ['大学']);  -- coarse 应为空
SELECT id, doc FROM tokenizer_eval_fine WHERE hasAllTokens(doc, ['大学']);  -- fine 命中 id 2

-- 强制走 skip index
SELECT id FROM tokenizer_eval WHERE hasAllTokens(doc, ['北京邮电大学']) SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tokenizer_eval WHERE hasAllTokens(doc, ['ClickHouse', 'database']) SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tokenizer_eval WHERE hasAllTokens(doc, ['数据库']) SETTINGS force_data_skipping_indices = 'idx';

-- granule 裁剪
EXPLAIN indexes = 1 SELECT id FROM tokenizer_eval WHERE hasAllTokens(doc, ['北京邮电大学']);
EXPLAIN indexes = 1 SELECT id FROM tokenizer_eval WHERE hasAllTokens(doc, ['ClickHouse', 'database']);
EXPLAIN indexes = 1 SELECT id FROM tokenizer_eval WHERE hasAllTokens(doc, ['数据库']);
EXPLAIN indexes = 1 SELECT id FROM tokenizer_eval WHERE hasAllTokens(doc, ['不存在的词']);
EXPLAIN indexes = 1 SELECT id FROM tokenizer_eval WHERE hasAllTokens(doc, ['nonexistent']);
EXPLAIN indexes = 1 SELECT id FROM tokenizer_eval WHERE hasAllTokens(doc, ['こんにちは']);
```

### 结果命中

| 场景 | 查询 | 命中 |
|------|------|------|
| 中文 | `hasAllTokens(doc, ['北京邮电大学'])` | id 2 `我来自北京邮电大学` |
| 中文 | `hasAnyTokens(doc, ['大厦'])` | id 1 `他来到了网易杭研大厦` |
| 英文 | `hasAllTokens(doc, ['ClickHouse', 'database'])` | id 4 |
| 英文 | `hasAnyTokens(doc, ['inverted'])` | id 5 |
| 混合 | `hasAllTokens(doc, ['数据库'])` | id 7 |
| 混合 | `hasAllTokens(doc, ['iPhone'])` | id 8 |
| 混合 | `hasAllTokens(doc, ['你好'])` | id 9 |
| coarse 子词 | `hasAllTokens(doc, ['大学'])` | 空（整词是 `北京邮电大学`） |
| fine 子词 | `hasAllTokens(doc, ['大学'])` on `tokenizer_eval_fine` | id 2 |

三种场景下 `force_data_skipping_indices = 'idx'` 均成功返回对应 id。

### EXPLAIN indexes

Skip index 名称均为 `idx`，Condition 为 `mode: All` 加上查询 token。

| 查询 | Skip Granules | 结论 |
|------|---------------|------|
| `['北京邮电大学']` | **1/9** | 中文命中并裁剪 |
| `['ClickHouse', 'database']` | **1/9** | 英文命中并裁剪 |
| `['数据库']` | **1/9** | 混合命中并裁剪 |
| `['不存在的词']` | **0/9** | 整 part 裁掉 |
| `['nonexistent']` | **0/9** | 整 part 裁掉 |
| `['こんにちは']` | **0/9** | 整 part 裁掉 |

命中查询的 EXPLAIN 同时出现 Prewhere 列 `__text_index_idx_hasAllTokens_*`，说明走了 text index 直读路径。

---

## 后续测试：1 亿行数据 vs 索引磁盘占用

环境仍为 ClickHouse **26.11.1.1058**，实例 `chinese-tokenizer-test`。  
表设置：`MergeTree ORDER BY id`，`index_granularity = 8192`，text index `GRANULARITY 1`。  
同一列不能建两个 text index，coarse / fine / splitByNonAlpha 分表测量。

### 数据

| 来源 | 说明 |
|------|------|
| [logpai/loghub](https://github.com/logpai/loghub) Spark | Zenodo `Spark.tar.gz`，过滤 `INFO/WARN/ERROR/DEBUG`，**27,410,260** 行真实 Spark 作业日志 |
| 中文 Wikipedia 摘要 | `wikipedia-cn-20230720-filtered`，**254,547** 条，截取前 40 字 |

生成 1 亿行（每行唯一 `req=`）：

```sql
concat(
    substringUTF8(spark_line, 1, 120),
    ' 详情=',
    substringUTF8(wiki_snippet, 1, 40),
    ' req=',
    toString(id)
)
```

样例：

```
17/06/09 18:34:15 INFO executor.Executor: Finished task 32.0 in stage 5646.0 (TID 225912). 2141 bytes result sent to dri 详情=刘美，字懋实，号蓉川，四川成都左护卫军籍成都府成都县人。 生平 正月二十四日生， req=86601516
```

无索引源表 `logs_src`：1 亿行，message 合计 **20.19 GiB**，part 上 **data_compressed = 6.44 GiB**，**data_uncompressed = 21.69 GiB**。

占用口径：`system.parts` 的 `data_*` 对数据列，`secondary_indices_*` / `system.data_skipping_indices` 对 text index（均为 **active parts**）。

### 结果

| 场景 | tokenizer | 数据压缩 | 数据未压缩 | **索引压缩** | 索引未压缩 | 表磁盘合计 | 索引/数据（压缩） |
|------|-----------|----------|------------|--------------|------------|------------|-------------------|
| 无索引 | — | 6.44 GiB | 21.69 GiB | 0 | 0 | 6.44 GiB | 0% |
| （1）splitByNonAlpha | `text(tokenizer = 'splitByNonAlpha')` | 6.45 GiB | 21.69 GiB | **3.56 GiB** | 4.59 GiB | 10.01 GiB | **55%** |
| （2a）Chinese coarse | `text(tokenizer = 'chinese')` | 6.44 GiB | 21.69 GiB | **5.78 GiB** | 6.49 GiB | 12.22 GiB | **90%** |
| （2b）Chinese fine | `text(tokenizer = chinese('fine_grained'))` | 6.44 GiB | 21.69 GiB | **6.05 GiB** | 6.74 GiB | 12.49 GiB | **94%** |

### 结论

1. **数据体积三种场景几乎一样**（同一份 1 亿行 message，压缩约 6.44 GiB）。差别全在索引。
2. **`splitByNonAlpha` 索引最省**：3.56 GiB，约为数据压缩体积的一半。它对中文几乎不按词切（连续汉字常成一个大 token），倒排字典更小。
3. **`chinese` coarse 索引明显更大**：5.78 GiB，约为数据的 90%。中文被切成词，token 数变多。
4. **`fine_grained` 再大一截，但增幅有限**：6.05 GiB（相对 coarse +0.27 GiB，约 +5%）。本语料主体是英文 Spark 日志，fine 与 coarse 在英文上相同；中文只有短摘要，重叠子词带来的增量不大。若换成以中文为主的语料，fine 与 coarse 的差距会更大。

---

## 后续测试：旧中英方案 vs text index（5000 万行）

旧中英搜索方案（布隆参数与生产一致）：

- **英文/按词**：text index `tokenizer = 'splitByNonAlpha'`
- **中文**：布隆过滤器 `ngrambf_v1(2, 60000, 5, 0)`（2-gram）

同一列同时挂一个 text index 和一个 ngrambf。数据取 `logs_src` 前 5000 万行（与 1 亿行同一生成方式）。

```sql
CREATE TABLE logs_text_split_ngram2bf
(
    id UInt64,
    message String,
    INDEX idx_split message TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX idx_ngram2 message TYPE ngrambf_v1(2, 60000, 5, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192;
```

| 项目 | 压缩 | 未压缩 |
|------|------|--------|
| 数据 | 3.22 GiB | 10.84 GiB |
| text `splitByNonAlpha` | **1.70 GiB** | 2.35 GiB |
| ngrambf_v1(2, 60000, 5, 0) | **340.57 MiB** | 349.33 MiB |
| 两个索引合计 | **2.03 GiB** | 2.69 GiB |
| 表磁盘合计 | 5.25 GiB | — |

text `splitByNonAlpha` 仍占索引的大头（1.70 GiB）。生产参数下 2-gram 布隆约 **341 MiB**，约为 text 索引的 20%、两索引合计的 16%。旧方案总索引 **2.03 GiB**，相对「只建 splitByNonAlpha text」多出约 0.33 GiB。布隆不存 posting，只能做 granule 级否证。

---

## 后续测试：纯英文 5000 万行（三种 tokenizer）

从 `logs_src` 去掉 `详情=...` 中文段，保留 Spark 英文日志 + `req=`，5000 万行。样例：

```
17/06/06 21:12:05 INFO executor.CoarseGrainedExecutorBackend: Registered signal handlers for [TERM, HUP, INT] req=0
```

无索引：数据压缩 **514 MiB** / 未压缩 **5.87 GiB**（英文可压，远小于中英混合）。

| tokenizer | 数据压缩 | **索引压缩** | 索引未压缩 | 表磁盘合计 | 索引/数据（压缩） |
|-----------|----------|--------------|------------|------------|-------------------|
| `splitByNonAlpha` | 514.42 MiB | **717.61 MiB** | 1.04 GiB | 1.20 GiB | 140% |
| `chinese` coarse | 514.44 MiB | **740.26 MiB** | 1.06 GiB | 1.23 GiB | 144% |
| `chinese` fine | 514.40 MiB | **736.16 MiB** | 1.06 GiB | 1.22 GiB | 143% |

结论：纯英文下三种 tokenizer 索引体积几乎一样（约 720–740 MiB）。`chinese` 对英文按非字母数字切开，fine 与 coarse 相同；相对 splitByNonAlpha 只多约 3%。索引比压缩后的数据还大，是因为英文日志压缩率很高（514 MiB），倒排本身压不了那么多。
