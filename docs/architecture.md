# Architecture

## Why this is a transcode, not a recomputation

A HyperLogLog sketch is not a compressed set. It is an array of `2^p` small
integers, one per bucket, where entry `i` holds the largest *rank* — leading
zero count plus one — of any hashed value that landed in bucket `i`. Estimating
cardinality is a pure function of that array.

ZetaSketch and ClickHouse both maintain exactly this structure. They disagree
about how to write it down:

| | ZetaSketch (BigQuery) | ClickHouse `uniqCombined64` |
|---|---|---|
| envelope | protobuf `AggregatorStateProto` | marker byte + fixed layout |
| small cardinality | sparse list of `(bucket, rank)` pairs | flat array, then a hash set |
| large cardinality | one byte per bucket | 6 bits per bucket, bit-packed |
| bookkeeping | none stored | rank histogram + zeros counter |

So converting a sketch means reading the register array out of one encoding
and writing it into the other. No hashing happens, and the original values are
never needed — which is the entire point, because they are still in BigQuery.

### The hash functions differ, and that is fine

BigQuery derives a bucket index from the *high* bits of Google's hash;
ClickHouse derives it from the *low* bits of its own. If you were inserting
raw values into both systems you would get different arrays. But we are not
inserting anything — we are moving an array that has already been built.

Bucket index `i` is an opaque label: a slot that happens to hold the maximum
rank of one `1/2^p` slice of hash space. HLL's estimator only cares about the
*multiset* of ranks and how many buckets are empty, not which slice produced
which entry. Relabelling the slices consistently — which is what a direct
index-to-index copy does — leaves every quantity the estimator reads
unchanged. What it preserves precisely:

- the rank histogram, so the harmonic mean is identical;
- the zeros count, so linear counting behaves identically;
- merge semantics, because merging is per-bucket `max` and the relabelling is
  the same for every sketch this tool converts.

What it does not preserve: mixing converted sketches with sketches ClickHouse
built itself from the same underlying values. Those are two independent
samplings of the same population; merging them double-counts, because the same
value occupies unrelated buckets in each. Keep converted and natively-built
sketches in separate columns. See [usage.md](usage.md#do-not-mix-sketch-lineages).

## Pipeline

```
BigQuery BYTES
  │
  │  AggregatorStateProto (protobuf, field 112 = HLL++ payload)
  ▼
zetasketch::decode_zetasketch
  │    ├─ dense  `data`:        one byte per bucket, copied out
  │    └─ sparse `sparse_data`: varint differences → (bucket, rank), max per bucket
  ▼
Registers { precision, data }          ← the shared representation
  │
  │  Registers::fold_to(15)            ← only if BigQuery used a finer precision
  ▼
clickhouse::encode_clickhouse_state
  │    ├─ registers → 6-bit LSB-first bitstream
  │    ├─ rank histogram (u32 per rank)
  │    └─ zeros counter
  ▼
AggregateFunction(uniqCombined64(15), UInt64)
```

The byte-level detail of the first and last steps is in
[format-reference.md](format-reference.md).

## Module map

| module | responsibility |
|---|---|
| `registers` | `Registers`, the precision-tagged array both formats reduce to, plus precision folding and its rank-adjustment rule |
| `zetasketch` | protobuf envelope, sparse and dense representations, varint differences |
| `clickhouse` | the `LARGE` container: bit packing, rank histogram, zeros counter, empty state |
| `udf` | ClickHouse's executable-UDF framing, so a row is read, converted and written without buffering the whole chunk |
| `main` | argument parsing and one-time validation; everything else is in `udf` so it is testable without spawning a process |

`Registers` is the seam. Neither format module knows about the other, and both
integration test suites attach to it independently: one drives BigQuery bytes
in, the other checks ClickHouse bytes out.

## Design decisions

### Always emit the `LARGE` container

ClickHouse picks one of three containers by cardinality: `SMALL` (≤16 values,
stored exactly), `MEDIUM` (≤`2^(p-5)`, a hash set) and `LARGE` (the dense HLL).
`SMALL` and `MEDIUM` store *the values themselves*, which a sketch no longer
has — the hashes were thrown away when the sketch was built.

So every converted sketch is a `LARGE` state, even one holding a single
distinct value. ClickHouse reads it correctly at any cardinality, and merging
a converted `LARGE` with a native `SMALL` works, because merge promotes to the
widest container. The costs are that a converted state is always 24 783 bytes
at precision 15, and that a low-cardinality converted state is not byte-identical
to the one a live aggregation would produce. Only the size matters in practice;
budget for it when sizing columns with many low-cardinality sketches.

### Precision is folded, never invented

`fold_to` maps bucket `i` to `i >> (source_p - target_p)` and takes the maximum
rank per group, adjusting each rank for the index bits being dropped — if a
dropped bit is set, it alone determines the new rank; if all are zero, the
leading-zero run simply grew. This is the standard HLL++ downgrade and matches
`Encoding.downgradeRhoW` upstream.

Folding up is refused. There is no information in a coarse sketch to
reconstruct a finer one, and quietly padding would produce confidently wrong
counts. If BigQuery emits precision 15 and the column wants 15, folding is a
no-op clone.

### Error handling

The library is strict: every function returns `Result` and refuses anything it
cannot represent exactly — an unknown aggregator type, a v1 encoding, a
precision out of range, a register array whose length contradicts its
precision, a rank too large for its precision.

The UDF is lenient in exactly one way: a row that fails to convert becomes
[`EMPTY_STATE`](format-reference.md#the-empty-state), so it contributes nothing
to a roll-up, the same as a `NULL` would, and the query succeeds. A pooled UDF
process serves many queries; letting one malformed row abort the process would
take unrelated queries with it.

That leniency has an obvious hazard: a *systematically* unconvertible column —
wrong precision, v1 sketches — looks like a column of zeros rather than an
error. Two things push back on it:

1. `--precision` is validated at startup, so the most likely misconfiguration
   fails loudly before any row is read.
2. The first conversion failure and a final tally are written to stderr, capped
   at three lines per process. Per-row logging is deliberately not done:
   ClickHouse does not reliably drain a UDF's stderr, and a full pipe would
   wedge the process.

[troubleshooting.md](troubleshooting.md) starts from the zeros symptom.

### Why a separate binary rather than a ClickHouse patch

An executable UDF needs no ClickHouse fork, no rebuild, and no restart beyond
`SYSTEM RELOAD FUNCTIONS`. The cost is a process boundary per pooled worker and
RowBinary framing on both sides of it; at 24 783 bytes per row the framing is
noise next to the state itself. If this ever becomes hot enough to matter, the
same library can be linked into a native function without touching the format
code.
