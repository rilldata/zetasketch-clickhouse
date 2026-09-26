//! Convert Google ZetaSketch HLL++ sketches into ClickHouse
//! `uniqCombined64(p)` aggregate-function states.
//!
//! BigQuery's `HLL_COUNT.INIT` / `HLL_COUNT.MERGE_PARTIAL` emit sketches as
//! `BYTES`: a serialized `zetasketch.AggregatorStateProto`. ClickHouse stores
//! `uniqCombined64` state as its own packed binary blob. Both are HyperLogLog++
//! register arrays underneath, so a sketch can be moved between the two systems
//! by *transplanting the registers* — no re-hashing and no access to the
//! original values required.
//!
//! ```no_run
//! use zetasketch_clickhouse::{decode_zetasketch, encode_clickhouse_state};
//!
//! # fn main() -> anyhow::Result<()> {
//! # let sketch_bytes: &[u8] = &[];
//! let registers = decode_zetasketch(sketch_bytes)?;   // whatever precision BigQuery used
//! let registers = registers.fold_to(15)?;             // ClickHouse uniqCombined64(15)
//! let state = encode_clickhouse_state(&registers)?;   // insert into an AggregateFunction column
//! # Ok(())
//! # }
//! ```
//!
//! # Module map
//!
//! * [`zetasketch`] — parse the BigQuery side (protobuf envelope, sparse and
//!   dense HLL++ representations).
//! * [`clickhouse`] — emit the ClickHouse side (`CombinedCardinalityEstimator`
//!   `LARGE` container: packed registers, rank histogram, zeros counter).
//! * [`registers`] — the precision-tagged register array both sides speak, and
//!   precision folding.
//! * [`udf`] — the RowBinary framing ClickHouse executable UDFs use, so the
//!   conversion can run as `executable_pool` UDF.
//!
//! Byte layouts, with line-level citations into `google/zetasketch` and
//! `ClickHouse/ClickHouse`, are documented in `docs/format-reference.md`.

pub mod clickhouse;
pub mod registers;
pub mod udf;
pub mod zetasketch;

pub use clickhouse::{encode_clickhouse_state, state_size, EMPTY_STATE};
pub use registers::Registers;
pub use zetasketch::decode_zetasketch;

/// Generated protobuf types for the sketch envelope. See `proto/wire.proto`.
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/zetasketch_clickhouse.wire.rs"));
}

/// Lowest target precision [`encode_clickhouse_state`] supports.
///
/// Below 12, ClickHouse's `HyperLogLogCounter` switches its denominator away
/// from the per-rank histogram layout this crate emits (`isBigRankStore`,
/// `src/Common/HyperLogLogCounter.h:82-85`), so the state bytes would have a
/// different shape entirely.
pub const MIN_TARGET_PRECISION: u8 = 12;

/// Highest target precision [`encode_clickhouse_state`] supports. Also the
/// highest normal precision ZetaSketch itself allows
/// (`NormalRepresentation.MAXIMUM_PRECISION`).
pub const MAX_TARGET_PRECISION: u8 = 24;

/// The precision this crate is validated against end-to-end, and the one
/// `uniqCombined64Merge(15)` roll-ups expect.
pub const DEFAULT_TARGET_PRECISION: u8 = 15;
