# Usage

Worked examples: getting sketches out of BigQuery, into ClickHouse, and
querying them. Assumes the UDF is installed ([deployment.md](deployment.md)).

## The shape of the problem

BigQuery has a table with an HLL sketch column:

```sql
-- BigQuery
SELECT dte, campaign_id, HLL_COUNT.INIT(user_id, 15) AS audience
FROM impressions
GROUP BY dte, campaign_id;
```

`audience` is `BYTES`. The goal is a ClickHouse column that answers the same
questions — distinct users per campaign, per week, across arbitrary groupings —
without moving `user_id` itself.

## 1. Export from BigQuery

Sketches are binary, so export them hex-encoded and let ClickHouse decode; this
survives CSV, JSON and shell pipelines without escaping problems.

```sql
-- BigQuery
SELECT
    dte,
    campaign_id,
    TO_HEX(audience) AS sketch_hex
FROM audience_daily
WHERE dte BETWEEN '2026-01-01' AND '2026-01-31';
```

Export precision matters: `HLL_COUNT.INIT(x, p)` defaults to `p = 15`, which is
what `uniqCombined64(15)` wants. If your pipeline used a different precision,
the UDF folds it down (and refuses to fold up — see
[architecture.md](architecture.md#precision-is-folded-never-invented)).

The precision is stored inside the sketch, so rather than digging through the
pipeline that wrote the column, convert one sketch and read it off:

```bash
cargo run --example convert_hex -- <sketch-hex> >/dev/null
# sketch precision 15, 1 of 32768 buckets set
```

## 2. Land the raw export

Keep the untouched hex in a staging table. Conversion is then a pure
transformation you can re-run without going back to BigQuery.

```sql
CREATE TABLE audience_daily_raw
(
    dte         Date,
    campaign_id UInt64,
    sketch_hex  String
)
ENGINE = MergeTree
ORDER BY (dte, campaign_id);
```

Load it however you normally would — `clickhouse-client` with `FORMAT CSV`, the
S3 table function, Airflow, whatever the pipeline is:

```bash
clickhouse-client --query \
  "INSERT INTO audience_daily_raw FORMAT CSVWithNames" < export.csv
```

```sql
-- or straight from object storage
INSERT INTO audience_daily_raw
SELECT dte, campaign_id, sketch_hex
FROM s3('https://…/audience_daily/*.parquet', 'Parquet');
```

## 3. Create the target table

The sketch column is an `AggregateFunction`, so the table is an
`AggregatingMergeTree` and the column type must name the exact function and
precision the UDF produces.

```sql
CREATE TABLE audience_daily
(
    dte         Date,
    campaign_id UInt64,
    audience    AggregateFunction(uniqCombined64(15), UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (dte, campaign_id);
```

### On a cluster

Create the local table on every node and a `Distributed` table over it. Both
are needed, and later schema changes must be applied to both.

```sql
CREATE TABLE audience_daily_local ON CLUSTER '{cluster}'
(
    dte         Date,
    campaign_id UInt64,
    audience    AggregateFunction(uniqCombined64(15), UInt64)
)
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{shard}/audience_daily_local', '{replica}')
ORDER BY (dte, campaign_id);

CREATE TABLE audience_daily ON CLUSTER '{cluster}' AS audience_daily_local
ENGINE = Distributed('{cluster}', currentDatabase(), audience_daily_local, cityHash64(campaign_id));
```

## 4. Convert

One statement, once per batch:

```sql
INSERT INTO audience_daily
SELECT
    dte,
    campaign_id,
    zetasketch_to_uniq_combined64_p15(unhex(sketch_hex)) AS audience
FROM audience_daily_raw
WHERE dte BETWEEN '2026-01-01' AND '2026-01-31';
```

`unhex` is what turns the exported text back into the sketch bytes; if your
pipeline delivers binary directly, pass the column as-is.

### Backfilling a large range

Convert a partition at a time so a failure costs one day, not a month, and so
memory stays flat:

```sql
-- repeat per day, or drive from a script / Airflow task
INSERT INTO audience_daily
SELECT dte, campaign_id, zetasketch_to_uniq_combined64_p15(unhex(sketch_hex))
FROM audience_daily_raw
WHERE dte = {day:Date};
```

Check as you go — every row should convert:

```sql
SELECT
    dte,
    count() AS rows,
    countIf(finalizeAggregation(audience) = 0) AS empty_states
FROM audience_daily
WHERE dte = {day:Date}
GROUP BY dte;
```

A nonzero `empty_states` that is not explained by genuinely empty source
sketches means rows failed to convert; see
[troubleshooting.md](troubleshooting.md#every-row-comes-back-as-0).

### Incremental loads

A materialized view keeps conversion automatic as new raw rows arrive:

```sql
CREATE MATERIALIZED VIEW audience_daily_mv TO audience_daily AS
SELECT
    dte,
    campaign_id,
    zetasketch_to_uniq_combined64_p15(unhex(sketch_hex)) AS audience
FROM audience_daily_raw;
```

The view fires per inserted block, so the UDF runs during ingest. Size
`pool_size` accordingly if ingest is bursty.

## 5. Query

Roll up across any grouping the order key allows. The sketch does the work:
these are exact set-union semantics on the approximate structure, so a user
seen on three days counts once.

```sql
-- distinct users per campaign over a month
SELECT
    campaign_id,
    uniqCombined64Merge(15)(audience) AS users
FROM audience_daily
WHERE dte >= '2026-01-01' AND dte < '2026-02-01'
GROUP BY campaign_id
ORDER BY users DESC;
```

```sql
-- weekly trend, deduplicated within each week
SELECT
    toStartOfWeek(dte) AS week,
    uniqCombined64Merge(15)(audience) AS users
FROM audience_daily
GROUP BY week
ORDER BY week;
```

```sql
-- one row's own count, no merging
SELECT dte, campaign_id, finalizeAggregation(audience) AS users
FROM audience_daily
WHERE dte = '2026-01-15';
```

```sql
-- total reach across everything, one number
SELECT uniqCombined64Merge(15)(audience) FROM audience_daily;
```

### Rolling daily into monthly

Pre-aggregate without losing the ability to merge further, using
`uniqCombined64MergeState`:

```sql
CREATE TABLE audience_monthly
(
    month       Date,
    campaign_id UInt64,
    audience    AggregateFunction(uniqCombined64(15), UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (month, campaign_id);

INSERT INTO audience_monthly
SELECT
    toStartOfMonth(dte) AS month,
    campaign_id,
    uniqCombined64MergeState(15)(audience) AS audience
FROM audience_daily
GROUP BY month, campaign_id;
```

## Gotchas

### The precision must match everywhere

`uniqCombined64(15)` in the column type, `15` in the UDF's `<return_type>`,
`--precision 15` on its command line, and `uniqCombined64Merge(15)` in queries.
ClickHouse rejects a mismatched *column* type at insert time, but a mismatch
between the binary's flag and the declared return type is silent.

### Do not mix sketch lineages

Do not merge converted BigQuery sketches with sketches ClickHouse built itself
over the same users:

```sql
-- WRONG: double counts, because the same user occupies unrelated buckets
SELECT uniqCombined64Merge(15)(audience)
FROM (
    SELECT audience FROM audience_daily            -- converted from BigQuery
    UNION ALL
    SELECT uniqCombined64State(15)(user_id) FROM native_impressions  -- built here
);
```

Each engine hashes values with its own function, so the two sketches are
independent samplings of the same population. Merging them is not a union; the
result overstates. Keep the two lineages in separate columns and compare their
finalized counts instead. [architecture.md](architecture.md#the-hash-functions-differ-and-that-is-fine)
explains why converted sketches merge safely with *each other*.

### Counts will differ slightly from BigQuery's

Same registers, different bias correction. Expect well under 1% — measured
drift is in [validation.md](validation.md). If you need BigQuery's exact
number, keep it alongside as a column; do not expect ClickHouse to reproduce it.

### Every converted state is 24 783 bytes

Even for one distinct value ([architecture.md](architecture.md#always-emit-the-large-container)).
A table with millions of low-cardinality sketches will be dominated by them;
consider whether those rows need a sketch at all, or aggregate to a coarser
grain before converting.
