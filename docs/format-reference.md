# Format reference

Both binary layouts, field by field. Every claim cites the upstream file it was
derived from, and each was then confirmed against bytes the real system
produced — see [validation.md](validation.md).

Line numbers refer to `google/zetasketch` and `ClickHouse/ClickHouse` at
`master` as of 2026-09-26. They drift; re-check before trusting them against a
much newer version.

---

## Part 1 — ZetaSketch, the BigQuery side

### The envelope

A sketch is a serialized proto2 `zetasketch.AggregatorStateProto`
(`proto/aggregator.proto:164-198`) whose HLL++ payload sits in extension field
112 (`proto/hllplus-unique.proto:111-114`):

| field | # | type | meaning |
|---|---|---|---|
| `type` | 1 | int32 | `AggregatorType`; must be `112` = `HYPERLOGLOG_PLUS_UNIQUE` |
| `num_values` | 2 | int64 | values added; not needed to reconstruct registers |
| `encoding_version` | 3 | int32 | **absent means 1**; only 2 is supported |
| `value_type` | 4 | int32 | type of the hashed values; informational here |
| `hyperloglogplus_unique_state` | 112 | message | the payload below |

The payload, `HyperLogLogPlusUniqueStateProto`
(`proto/hllplus-unique.proto:36-109`):

| field | # | type | meaning |
|---|---|---|---|
| `sparse_size` | 2 | int32 | number of entries in `sparse_data` |
| `precision_or_num_buckets` | 3 | int32 | **in v2, the precision `p` itself** (v1 stored `2^p`) |
| `sparse_precision_or_num_buckets` | 4 | int32 | likewise the sparse precision `sp` |
| `data` | 5 | bytes | dense representation |
| `sparse_data` | 6 | bytes | sparse representation |

Exactly one of `data` / `sparse_data` is normally populated. This crate merges
both if it ever sees both, rather than silently dropping one.

**Why we re-declare the proto.** `prost` does not support proto2 `extend`, so
`proto/wire.proto` declares the same fields at the same numbers as ordinary
fields. Extensions and plain fields are indistinguishable on the wire, so the
parse is identical; `proto/hllplus-unique.proto` is kept verbatim alongside it
as the reference. Wire tags are computed as `field_number << 3 | wire_type`,
exactly as upstream does in `State.java:147-161` and `:271-285`.

**Version 2 only.** Version 1 used a different sparse encoding — ZigZag
varints over the original HLL++ paper's scheme (`hllplus-unique.proto:97-101`).
Nothing BigQuery emits today uses it, and misreading it would produce plausible
but wrong counts, so it is rejected outright.

### NORMAL (dense) representation

`proto/hllplus-unique.proto:78-84`: when `data` is present it holds **exactly
`2^p` bytes, one per bucket** — *not* bit-packed. `data[i]` is that bucket's
rhoW, with `0` meaning "never set", confirmed by the "preserve 0 rhoW … this
represents any unset register" comment at `Encoding.java:100-107`.

This maps to `Registers` directly, with no transformation.

### SPARSE representation

`sparse_data` is a sorted list of non-negative `int32` values stored as
successive differences, each an unsigned base-128 varint — no ZigZag, because
the sequence is non-decreasing (`DifferenceDecoder.java:46-54`,
`VarInt.java:66-82`). Decoding is `value += varint` per entry.

Each value packs a bucket and a rank in one of two layouts, distinguished by a
flag bit (`Encoding.java:110-135`):

```
 flag=0:  0 │ padding             │ sparse index (sp bits)
 flag=1:  1 │ padding │ normal index (p bits) │ rhoW' (6 bits)

 flag bit position = max(sp, p + 6)          (Encoding.java:175)
```

Decoding to `(bucket, rhoW)` at normal precision (`Encoding.java:240-302`):

| flag | bucket | rhoW |
|---|---|---|
| 0 | `value >> (sp - p)` | leading zeros + 1 within the low `sp - p` bits of `value` |
| 1 | `(value ^ flag) >> 6` | `(value & 0x3F) + (sp - p)` |

The flag=1 form exists precisely because when the low `sp - p` index bits are
all zero the rank cannot be recovered from them, so it is carried explicitly.

Multiple entries can hit the same bucket; the maximum wins, matching upstream's
dedupe (`Encoding.java:380-431`) and what `SparseRepresentation.normalize()`
does when it upgrades to dense.

Bounds worth enforcing: `p ≤ 24` (`NormalRepresentation.MAXIMUM_PRECISION`) and
`p ≤ sp ≤ 25` (`SparseRepresentation.MAXIMUM_SPARSE_PRECISION`). They keep
`1 << max(sp, p + 6)` inside 32 bits — a sketch claiming `sp = 200` would
otherwise shift past the word width.

### Precision folding

Dropping `p - p'` index bits maps bucket `i` to `i >> (p - p')` and rewrites
each rank (`Encoding.java:485-508`, applied per bucket by
`NormalRepresentation.java:206-244`):

- rank `0` stays `0` — an unset bucket stays unset;
- if the dropped bits are all zero, the leading-zero run grew: `rank + (p - p')`;
- otherwise the highest set dropped bit determines it: `1 + leading_zeros(dropped bits)`.

Target bucket takes the maximum over its group. Upper bound stays consistent:
at `p = 24` the largest rank is 41, and `41 + 9 = 50`, exactly the largest rank
at `p = 15`.

---

## Part 2 — ClickHouse `uniqCombined64`, the target side

### Container selection

`uniqCombined64(K)` stores state in a `CombinedCardinalityEstimator`
(`src/AggregateFunctions/AggregateFunctionUniqCombined.h:117-126`) which picks
one of three containers and writes a marker byte to say which
(`src/Common/CombinedCardinalityEstimator.h:19-24, 252-263`):

| marker | container | holds | capacity |
|---|---|---|---|
| `1` | `SMALL` | the hash values themselves, flat array | ≤ 16 |
| `2` | `MEDIUM` | the hash values themselves, `HashSet` | ≤ `2^(K-5)` — 1024 at K=15 |
| `3` | `LARGE` | dense `HyperLogLogCounter` | unbounded |

`uniqCombined64` uses a 64-bit hash type, so the `+(sizeof(Key) == sizeof(UInt32))`
term in the medium threshold is 0.

Confirmed by probing `clickhouse-server:24.8`:

| rows aggregated | marker | total bytes |
|---|---|---|
| 0, 1, 10, 16 | `01` | 2, 10, 82, 130 |
| 17, 1000, 1024 | `02` | 138, 8003, 8195 |
| 1025, 100000 | `03` | **24783** |

This crate always writes `LARGE`: the other two store hash values, which a
sketch no longer has.

### The empty state

`SELECT hex(uniqCombined64State(15)(number)) FROM numbers(0)` returns `0100` —
the `SMALL` marker followed by a zero-length set. That two-byte constant is
what the UDF returns for a row it cannot convert.

### LARGE body

`HyperLogLogCounter::write` (`src/Common/HyperLogLogCounter.h:366-393`) writes
three fields, no header:

**1. Register array** — `CompactArray<…, rank_width = 6, bucket_count = 2^K>`.
The width is 6 because the hash type is `UInt64` (`RankWidth<UInt64>`,
`HyperLogLogCounter.h:232-238`). `CompactArray` (`src/Common/CompactArray.h:32-56`)
packs values **LSB-first and contiguously across byte boundaries** — bucket `i`
occupies bits `[6i, 6i+6)` of one little-endian bitstream:

```
 byte 0        byte 1        byte 2
 ┌────────┐    ┌────────┐    ┌────────┐
 │11111100│    │22222211│    │33333322│   bucket 0 = bits 0-5 of byte 0
 └────────┘    └────────┘    └────────┘   bucket 1 = bits 6-7 of byte 0 + bits 0-3 of byte 1
```

Size is `ceil(bucket_count * 6 / 8)`; at `K = 15` that is exactly 24 576 bytes
with no partial byte. `CompactArray` keeps one trailing padding byte in memory
for branchless access and does **not** serialize it (`CompactArray.h:52-63`).

**2. Rank histogram** — because `K ≥ 12`, `isBigRankStore` is true
(`HyperLogLogCounter.h:82-85`) and the default `DenominatorMode::StableIfBig`
selects the histogram denominator (`HyperLogLogCounter.h:154-194`): a
`UInt32[max_rank + 1]` counting how many buckets currently hold each rank, where
`max_rank = 64 - K + 1` (`HyperLogLogCounter.h:550`). At `K = 15` that is 51
entries, 204 bytes, native-endian (little-endian on any platform ClickHouse
ships for).

ClickHouse maintains it incrementally so estimation never rescans the registers.
To build it from a register array: start `histogram[0] = bucket_count`, then for
each nonzero rank `r` do `histogram[0] -= 1; histogram[r] += 1` — the batch form
of `Denominator::update(0, r)` (`HyperLogLogCounter.h:164-168`).

**3. Zeros counter** — the number of still-untouched buckets, in the narrowest
unsigned type that can hold `bucket_count` (`MinCounterType`,
`HyperLogLogCounter.h:60-75, 559`). At `K = 15`, `UInt16`, 2 bytes. It equals
`histogram[0]`.

### Total size

```
 1 + ceil(2^K · 6 / 8) + (64 - K + 2)·4 + zeros_width

 K = 15:  1 + 24576 + 204 + 2 = 24783 bytes
```

### Bucket indexing, and why the difference is harmless

ClickHouse takes its bucket index from the **low** `K` bits of its own hash
(`HyperLogLogCounter.h:291-292`); ZetaSketch takes it from the **high** `p` bits
of Google's (`Encoding.java:60-62`). Since this tool transplants an existing
array rather than hashing anything, index `i` is simply copied to index `i`.
The reasoning for why that is sound — and the one case where it is not — is in
[architecture.md](architecture.md#the-hash-functions-differ-and-that-is-fine).
