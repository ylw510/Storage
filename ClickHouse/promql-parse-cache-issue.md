# TimeSeries: add PromQL parse cache

---

## Background

ClickHouse parses PromQL on every request by constructing a `PrometheusQueryTree` from the query string. The parser is built on ANTLR (`PrometheusQueryParsingUtil-antlr.cpp`) and is invoked from multiple entry points:

- Prometheus HTTP API (`/api/v1/query`, `/api/v1/query_range`)
- `ParserPrometheusQuery` in the `promql` SQL dialect
- `prometheusQuery()` / `timeSeriesSelector()` table functions
- PromQL → SQL lowering in `PrometheusQueryToSQL/`

In typical dashboard workloads the **same query strings are re-evaluated every 15–60 seconds**. Each evaluation currently repeats the full lex/parse/build-tree work even when the query text is identical.

VictoriaMetrics already solves this with a dedicated parse cache (`app/vmselect/promql/parse_cache.go`, metrics type `promql/parse`): a sharded in-memory LRU keyed by the raw query string, storing the parsed `metricsql.Expr` and any parse error.

Write-path caches and selector probe memoization are being addressed in other work, but **PromQL parse caching is not covered yet**.

## Problem

Repeated parsing adds CPU overhead on hot query paths, especially for:

- Large dashboards with hundreds of panels re-querying on a fixed interval
- Recording rules and alert expressions evaluated frequently
- Complex queries with nested subqueries, aggregations, and long selector lists
- High-QPS `/api/v1/query_range` traffic where parse cost is paid per request even though the AST is unchanged

Parsing also happens more than once along a single request path (e.g. early syntax check in `ParserPrometheusQuery`, then again when building SQL from `PrometheusQueryTree`).

## Proposed solution

Introduce a **process-wide, cross-query PromQL parse cache** that memoizes the result of parsing a query string into `PrometheusQueryTree` (or an equivalent immutable AST wrapper).

### Cache key

- Primary key: exact PromQL query string (`std::string_view` / `String`)
- Do **not** include `promql_evaluation_time`, database, or table name in the key — those affect execution, not syntax/structure
- Parse errors should also be cached (same as VM) to avoid repeated failed parses from abusive clients

### Cache value

```cpp
struct PromQLParseCacheEntry
{
    std::shared_ptr<const PrometheusQueryTree> tree; // null on error
    std::exception_ptr parse_error;                  // set when parsing failed
};
```

Use `shared_ptr<const ...>` so callers can cheaply reuse the same immutable tree across concurrent queries.

### Architecture

Follow the VictoriaMetrics / fastcache pattern already used in VM:

- Fixed number of shards (e.g. 64 or 128), hash query string → shard
- Per-shard `std::mutex` + `HashMap<String, Entry>`
- Per-shard max entry count with simple eviction (delete ~10% of entries when over capacity)
- Global max entries ≈ 10k (configurable)

Suggested new files:

- `src/Parsers/Prometheus/PromQLParseCache.h`
- `src/Parsers/Prometheus/PromQLParseCache.cpp`

Integration points:

- `PrometheusQueryTree` constructor (or a new factory `PrometheusQueryTree::parseCached(std::string_view)`) — single choke point
- Optionally skip duplicate parse in `ParserPrometheusQuery::parseImpl` if the downstream path already uses the cache

### Settings

Add server-level settings (similar to other caches):

| Setting | Default | Description |
|---------|---------|-------------|
| `promql_parse_cache_max_entries` | `10000` | Max cached queries; `0` disables |
| `promql_parse_cache_shards` | `128` | Number of cache shards |

Expose via `system.metrics` / `ProfileEvents`:

- `PromQLParseCacheHits`
- `PromQLParseCacheMisses`
- `PromQLParseCacheEntries`
