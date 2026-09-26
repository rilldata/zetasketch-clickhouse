# zetasketch-clickhouse

Convert Google ZetaSketch HLL++ sketches — the `BYTES` values BigQuery's
`HLL_COUNT.INIT` and `HLL_COUNT.MERGE_PARTIAL` produce — into ClickHouse
`uniqCombined64` aggregate-function states.

The point is to move approximate-distinct-count data out of BigQuery without
re-reading the underlying rows. A migrated sketch lands in an
`AggregateFunction(uniqCombined64(15), UInt64)` column and then behaves like
any other ClickHouse state: `uniqCombined64Merge(15)` rolls sketches up across
rows, partitions and time ranges, and `finalizeAggregation` reads one out.

Ships as two things: a Rust library, and a single static binary that
ClickHouse runs as an `executable_pool` UDF so the conversion happens inside
`INSERT … SELECT`.

```sql
INSERT INTO audience_daily
SELECT
    dte,
    campaign_id,
    zetasketch_to_uniq_combined64_p15(sketch_bytes) AS audience
FROM bigquery_export;

SELECT campaign_id, uniqCombined64Merge(15)(audience)
FROM audience_daily
WHERE dte >= today() - 30
GROUP BY campaign_id;
```

## How it works, in one paragraph

A HyperLogLog sketch is an array of per-bucket maximum ranks. ZetaSketch and
ClickHouse both store exactly that array, just with different framing — a
protobuf envelope with a sparse or dense payload on one side, a marker byte
plus bit-packed registers plus bookkeeping counters on the other. So this is a
transcode, not a recomputation: the registers are read out of one encoding and
written into the other, with no hashing and no access to the original values.
Bucket indices are opaque labels, so the fact that the two engines derive them
from different hash functions does not matter.
[docs/architecture.md](docs/architecture.md) covers this properly, including
what it does and does not preserve.

## Quickstart

```bash
cargo build --release          # needs protoc on PATH
cargo test                     # 52 tests, no network or ClickHouse required
```

Convert a single sketch without deploying anything:

```bash
cargo run --example convert_hex -- 087010011802200882070b1001180f20143203b28716
```

To install it into a ClickHouse server, see
[docs/deployment.md](docs/deployment.md). For the SQL patterns on the far side
— table layout, backfill, incremental loads — see [docs/usage.md](docs/usage.md).

## Library

```rust
use zetasketch_clickhouse::{decode_zetasketch, encode_clickhouse_state};

let registers = decode_zetasketch(sketch_bytes)?; // at whatever precision BigQuery used
let registers = registers.fold_to(15)?;           // down to uniqCombined64(15)
let state = encode_clickhouse_state(&registers)?; // insert into an AggregateFunction column
```

`decode_zetasketch` errors on anything that is not a v2 HLL++ sketch;
`fold_to` refuses to fold up; `encode_clickhouse_state` checks the register
array against its declared precision. The UDF turns all of those errors into
ClickHouse's empty state so one bad row cannot fail a query — see
[docs/architecture.md](docs/architecture.md#error-handling).

## Does it actually agree with BigQuery?

Yes, within HLL's own noise. Both sides of the validation use bytes produced
by the real systems, not hand-written fixtures.

| sketch (distinct values) | BigQuery `HLL_COUNT.EXTRACT` | ClickHouse after conversion | drift |
|---|---|---|---|
| 1 | 1 | 1 | 0% |
| 25 | 25 | 25 | 0% |
| 5 000 | 5 004 | 5 001 | 0.06% |
| 50 000 | 49 886 | 49 706 | 0.36% |

Plus 100 rows of real production sketches from a customer's BigQuery table:
every row converted, per-row drift 0% at p50, p95 and max.

Separately, the encoder is pinned to ClickHouse's own serializer: a real
`uniqCombined64State(15)` blob captured from `clickhouse-server:24.8` is parsed
and re-encoded byte-for-byte identically. [docs/validation.md](docs/validation.md)
has the method, the queries and the caveats.

## Documentation

| | |
|---|---|
| [architecture.md](docs/architecture.md) | Why a register transplant is valid, the conversion pipeline, module map, design decisions |
| [format-reference.md](docs/format-reference.md) | Both binary layouts, field by field, cited to upstream source lines |
| [udf-protocol.md](docs/udf-protocol.md) | How ClickHouse talks to an executable UDF, and the two asymmetries that bite |
| [deployment.md](docs/deployment.md) | Building for the server, installing, rolling out across a cluster, verifying |
| [usage.md](docs/usage.md) | Table design, backfill and incremental load patterns, roll-up queries |
| [validation.md](docs/validation.md) | How correctness was established and how to reproduce it |
| [troubleshooting.md](docs/troubleshooting.md) | Symptom-first guide, including the silent-empty-state failures |

## Scope and limitations

- **Sketch format**: `encoding_version = 2` only. Version 1 used a different
  sparse encoding; nothing BigQuery emits today uses it, and it is rejected
  with a clear error rather than misread.
- **Precision**: target precision 12–24, validated end to end only at 15.
  Sketches at a higher precision are folded down; folding up is impossible.
- **Container**: always emits ClickHouse's `LARGE` (dense) container, including
  for tiny sketches. Queries read it correctly at any cardinality; it is simply
  not byte-identical to what a live low-cardinality aggregation would produce,
  and it always occupies 24 783 bytes at precision 15.
- **Estimates are not bit-identical to BigQuery's.** Identical registers, but
  each engine applies its own bias correction, hence the sub-1% drift above.
- **Platforms**: built and validated for `aarch64-unknown-linux-musl` against
  ClickHouse 24.8. Other targets should work but have not been exercised.

## Credits

Ports binary formats from two Apache-2.0 projects, neither of which is
vendored here beyond `proto/hllplus-unique.proto`:
[google/zetasketch](https://github.com/google/zetasketch) and
[ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse). All layout
claims in the docs cite the upstream files they were derived from.
