# TimeSeries: add PromQL regexp and prefix caches for selector evaluation

---

## Background

PromQL instant and range selectors often use regex matchers (`=~`, `!~`) on label values and metric names, for example:

```promql
http_requests_total{job=~"prod-.*", instance!~"staging-.*"}
```

In ClickHouse TimeSeries, these matchers are lowered in `StorageTimeSeriesSelector::matcherToAST()` to SQL `match()` calls with Prometheus-style anchoring (`^…$`). Each query compiles and evaluates these regexes against the tags table (via text index / `match()` on tag columns).

VictoriaMetrics maintains **two separate caches** for this path:

1. **Compiled regexp cache** (`vendor/github.com/VictoriaMetrics/metricsql/regexp_cache.go`, metrics type `promql/regexp`) — memoizes compiled regex objects keyed by pattern string
2. **Regexp prefix/suffix cache** (`lib/storage/tag_filters.go`, `prefixesCache`) — memoizes the result of `regexutil.SimplifyPromRegex()`, splitting a PromQL regex into a literal prefix and remaining suffix to **prune index lookups** before full regex matching

ClickHouse has a general-purpose `OptimizedRegularExpression` utility, but TimeSeries selector evaluation does **not** maintain a PromQL-specific LRU cache across queries, nor does it extract and reuse literal prefixes from regex matchers for tags-table pruning.

Whole-metric probe memoization and CTE materialization for large ID sets are being addressed in other work, but **neither replaces regexp/prefix caching**.

## Problem

Dashboards and alerts frequently reuse the same regex patterns across panels and evaluation intervals. Without caching:

- Every query re-compiles the same anchored regex patterns for `match()`
- Regex-heavy selectors cannot cheaply narrow scans using literal prefixes (e.g. `job=~"prod-.*"` should allow prefix-based skipping before full regex evaluation)
- High-cardinality tags tables pay repeated compilation and matching cost on every 15–30 s refresh

This is especially visible when:

- Many panels share common label filters with regex matchers
- Metric names use regex (`{__name__=~"kube_.*"}`)
- Recording rules evaluate complex selectors at high frequency

## Proposed solution

Add two complementary, process-wide caches for PromQL selector regex handling.

### 1. Compiled regexp cache (`PromQLRegexpCache`)

**Purpose:** avoid re-compiling identical regex patterns.

**Key:** anchored pattern string as used in SQL (`^` + PromQL pattern + `$`, after Prometheus anchoring rules)

**Value:**

```cpp
struct PromQLRegexpCacheEntry
{
    OptimizedRegularExpression regex; // or RE2 / existing CH regex wrapper
    bool is_trivial;                  // e.g. empty pattern, exact match after simplification
    std::exception_ptr compile_error; // cache invalid patterns too
};
```

**Integration points:**

- `StorageTimeSeriesSelector::matcherToAST()` — when building `match()` for `RE` / `NRE` matchers
- Any other PromQL → SQL path that emits regex conditions on tags (e.g. `PrometheusQueryToSQL` selector lowering)
- Optional: share with `match()` function execution if the pattern string is identical

**Eviction:** LRU bounded by total pattern characters (VM uses ~1M chars) or entry count.

### 2. Regexp prefix/suffix cache (`PromQLRegexpPrefixCache`)

**Purpose:** extract literal prefixes from PromQL regex patterns to enable cheaper tags-table filtering.

**Key:** original PromQL regex pattern (before anchoring)

**Value:**

```cpp
struct PromQLRegexpPrefixEntry
{
    String prefix;  // longest guaranteed literal prefix
    String suffix;  // remaining pattern for full match() evaluation
    bool has_prefix; // false if no useful prefix extracted
};
```

**Usage in selector planning:**

For a matcher like `job=~"prod-.*"`:

1. Look up / compute `(prefix="prod-", suffix=".*")`
2. If `prefix` is non-empty, add an extra filter on the tags column:
   - equality or `startsWith` on dedicated tag columns, **or**
   - text-index-friendly condition before the expensive `match()`
3. Keep the full anchored `match()` in WHERE for correctness

For matchers with alternation (`a|b`), empty prefix, or other non-prefixable forms, fall back to full regex only (same as VM).

**Reference implementation:** VictoriaMetrics `simplifyRegexp()` in `lib/storage/tag_filters.go` + `regexutil.SimplifyPromRegex()`.

### Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `promql_regexp_cache_max_chars` | `1000000` | Max total pattern chars in regexp cache; `0` disables |
| `promql_regexp_prefix_cache_max_bytes` | auto (~5% memory) | Max size of prefix cache; `0` disables |

### Metrics

Expose in `system.metrics` / `ProfileEvents`:

- `PromQLRegexpCacheHits` / `PromQLRegexpCacheMisses` / `PromQLRegexpCacheEntries`
- `PromQLRegexpPrefixCacheHits` / `PromQLRegexpPrefixCacheMisses` / `PromQLRegexpPrefixCacheEntries`

## Design notes

### Thread safety

- Sharded LRU (similar to VM fastcache / `TimeSeriesActiveSeriesCache`)
- Immutable cache entries after insertion

### Correctness

- Prefix optimization is **planning-time only**; full `match()` must remain for exact semantics
- Prometheus regex anchoring rules must match existing behavior, including alternation anchoring
- `!~` matchers apply prefix narrowing only when semantically safe (same constraints as VM: prefix filter is a superset refinement, full negated match still required)

### Relationship to text index

TimeSeries tags tables already support text indexes (`TextIndexCache` at read time). Prefix extraction should generate conditions that the text index and primary key can use, not replace them.
