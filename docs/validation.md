# Validation

How the conversion was checked, what the numbers are, and how to reproduce
them. The guiding rule: both ends of every check are bytes produced by the real
systems, never fixtures written by hand to match the implementation's own
assumptions.

## What is checked, and against what

| # | Check | Ground truth | Where |
|---|---|---|---|
| 1 | Encoder reproduces ClickHouse's own serialization byte for byte | a `uniqCombined64State(15)` blob from `clickhouse-server:24.8` | `tests/clickhouse_state_roundtrip.rs` |
| 2 | Decoder handles real BigQuery sketches across both representations | `HLL_COUNT.INIT` output from BigQuery | `tests/sketch_fixtures.rs` |
| 3 | End-to-end counts agree with BigQuery's own estimates | BigQuery `HLL_COUNT.EXTRACT` vs ClickHouse `finalizeAggregation` | manual, below |
| 4 | Real production sketches convert | a customer's BigQuery table, 100 rows | manual, below |
| 5 | UDF framing matches what ClickHouse sends | ClickHouse source, then a live server | `src/udf.rs`, `tests/udf_config.rs` |

Checks 1, 2 and 5 run in `cargo test` with no network and no ClickHouse. 3 and
4 need a server and credentials, so they are run deliberately.

## 1. Byte-exact encoder round trip

The strongest check, because it cannot pass by coincidence: take a state
ClickHouse itself wrote, parse it with an independent reader, re-encode it, and
compare.

```sql
SELECT hex(uniqCombined64State(15)(number)) FROM numbers(100000);
```

That blob is `tests/fixtures/clickhouse_state_numbers_100000.hex`. The test
parses out the registers, the rank histogram and the zeros counter, re-encodes
from the registers alone, and asserts the result equals the original 24 783
bytes. A second test asserts ClickHouse's own histogram and zeros counter are
exactly what the register array implies — confirming the reading of those two
trailing fields rather than assuming it.

**Result: identical.**

## 2. Decoding real BigQuery sketches

`tests/fixtures/bigquery_sketches.csv` holds real `HLL_COUNT.INIT(x, 15)`
output over synthetic inputs, chosen to cross the sparse → dense representation
boundary. Regenerate it with:

```bash
bq query --project_id=<project> --use_legacy_sql=false --format=csv --quiet '
SELECT "single" AS label, TO_HEX(HLL_COUNT.INIT(x,15)) AS sketch_hex,
       HLL_COUNT.EXTRACT(HLL_COUNT.INIT(x,15)) AS extracted_count
FROM UNNEST([42]) AS x
UNION ALL SELECT "small_sparse", TO_HEX(HLL_COUNT.INIT(x,15)), HLL_COUNT.EXTRACT(HLL_COUNT.INIT(x,15))
FROM UNNEST(GENERATE_ARRAY(1,25)) AS x
UNION ALL SELECT "medium_sparse", TO_HEX(HLL_COUNT.INIT(x,15)), HLL_COUNT.EXTRACT(HLL_COUNT.INIT(x,15))
FROM UNNEST(GENERATE_ARRAY(1,5000)) AS x
UNION ALL SELECT "dense_normal", TO_HEX(HLL_COUNT.INIT(x,15)), HLL_COUNT.EXTRACT(HLL_COUNT.INIT(x,15))
FROM UNNEST(GENERATE_ARRAY(1,50000)) AS x' > tests/fixtures/bigquery_sketches.csv
```

The inputs are synthetic integers, so the file contains no customer data and is
safe to commit. `single` and `small_sparse` exercise the sparse path,
`dense_normal` the dense one, `medium_sparse` sits near the changeover.

## 3. End-to-end agreement with BigQuery

Convert each fixture and ask ClickHouse what it thinks:

```bash
docker run -d --name ch-probe -p 28123:8123 \
  -e CLICKHOUSE_PASSWORD=probe_local_only clickhouse/clickhouse-server:24.8

tail -n +2 tests/fixtures/bigquery_sketches.csv | while IFS=, read -r label hex expected; do
  state=$(cargo run --quiet --example convert_hex -- "$hex" 2>/dev/null)
  got=$(curl -s -G -u default:probe_local_only \
    --data-urlencode "query=SELECT finalizeAggregation(unhex('$state')::AggregateFunction(uniqCombined64(15), UInt64))" \
    http://localhost:28123/)
  echo "$label: bigquery=$expected clickhouse=$got"
done
```

| sketch | distinct values | BigQuery | ClickHouse | drift |
|---|---|---|---|---|
| `single` | 1 | 1 | 1 | 0% |
| `small_sparse` | 25 | 25 | 25 | 0% |
| `medium_sparse` | 5 000 | 5 004 | 5 001 | 0.06% |
| `dense_normal` | 50 000 | 49 886 | 49 706 | 0.36% |

Identical results whether the conversion runs through the library directly or
through the deployed UDF in SQL.

### Why these are not zero

The registers are identical; the estimators are not. Both engines apply their
own bias correction on top of the same raw harmonic mean — ClickHouse via
`UniqCombinedBiasData` and a linear-counting threshold, BigQuery via
ZetaSketch's own tables. Small disagreement is expected and is a property of
HLL, not evidence of a conversion error. Check 1 is what pins the conversion
itself.

Exact agreement at 1 and 25 distinct values is also expected: at those
cardinalities both estimators fall back to linear counting over a nearly empty
register array, which is effectively exact.

## 4. Real production sketches

Run against a customer's BigQuery table — 100 rows of genuine
`HLL_COUNT`-built sketches:

| rows | conversion failures | p50 drift | p95 drift | max drift |
|---|---|---|---|---|
| 100 | 0 | 0% | 0% | 0% |

Every row converted, and each row's count matched BigQuery's exactly. Those
rows carry small per-row cardinalities (single digits to tens), where both
estimators are effectively exact — so this confirms the decoder handles real
sketches, while the synthetic table above is the more informative accuracy
signal at high cardinality.

**That data was never committed.** It was pulled read-only for the check and
kept out of the repository; `.gitignore` excludes `tests/fixtures/real_*.csv`
so it cannot be added by accident. Reproduce it against your own table with:

```sql
SELECT TO_HEX(<sketch_column>) AS sketch_hex,
       HLL_COUNT.EXTRACT(<sketch_column>) AS bq_extract
FROM <project.dataset.table>
WHERE <sketch_column> IS NOT NULL
LIMIT 1000
```

then compare each row's `finalizeAggregation(zetasketch_to_uniq_combined64_p15(unhex(sketch_hex)))`
against `bq_extract`.

## 5. UDF framing

`src/udf.rs`'s tests drive the exact byte sequence ClickHouse sends — text chunk
header, length-prefixed input rows, unprefixed output states — and asserts that
unconvertible rows become empty states, that diagnostics stay bounded no matter
how many rows fail, and that one process serves several chunks.

`tests/udf_config.rs` guards the shipped registration file against the four
ways it can be silently wrong: a `--` inside an XML comment, a `<clickhouse>`
wrapper, an absolute `<command>` path, and a precision that disagrees with the
declared return type.

Live verification against a server is in
[deployment.md](deployment.md#4-verify).

## Reproducing everything

```bash
cargo test                                  # checks 1, 2, 5
# checks 3 and 4 need a ClickHouse container and bq credentials, see above
```
