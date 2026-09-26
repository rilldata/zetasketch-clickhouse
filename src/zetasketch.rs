//! Parsing the BigQuery / ZetaSketch side.
//!
//! A sketch is a serialized `zetasketch.AggregatorStateProto` carrying a
//! `HyperLogLogPlusUniqueStateProto` in field 112. That payload holds the
//! HLL++ state in one of two representations — a dense byte-per-bucket array
//! (`data`) or a varint-difference-encoded sparse list (`sparse_data`) — which
//! this module normalizes into a [`Registers`] array.
//!
//! See `docs/format-reference.md` for the layout and citations.

use anyhow::{bail, ensure, Context, Result};
use prost::Message;

use crate::registers::{Registers, MAX_PRECISION};
use crate::wire;

/// `zetasketch.AggregatorType.HYPERLOGLOG_PLUS_UNIQUE` (`aggregator.proto:63`).
const HYPERLOGLOG_PLUS_UNIQUE_TYPE: i32 = 112;

/// The only `encoding_version` this crate understands. Version 1 used a
/// different, ZigZag-varint sparse encoding based on the original HLL++ paper
/// (`hllplus-unique.proto:97-101`); nothing BigQuery emits today uses it.
const SUPPORTED_ENCODING_VERSION: i32 = 2;

/// Highest sparse precision ZetaSketch allows
/// (`SparseRepresentation.MAXIMUM_SPARSE_PRECISION`). Enforcing it keeps the
/// `1 << max(sp, p + 6)` flag computation below inside `u32`.
const MAX_SPARSE_PRECISION: u8 = 25;

/// Bits of sparse rhoW' carried in a rho-encoded sparse value
/// (`Encoding.Sparse.RHOW_BITS`).
const RHO_BITS: u32 = 6;

/// Decodes a serialized ZetaSketch HLL++ sketch — the raw `BYTES` value
/// BigQuery returns for an `HLL_COUNT.INIT` / `HLL_COUNT.MERGE_PARTIAL`
/// column — into a dense register array at the sketch's own precision.
///
/// Use [`Registers::fold_to`] afterwards to reach the precision your
/// ClickHouse column expects.
///
/// # Errors
///
/// Returns an error for anything that is not a v2 HLL++ sketch: a different
/// aggregator type, `encoding_version` 1, a precision outside `[1, 24]`, a
/// malformed protobuf, or an internally inconsistent payload. Callers that
/// must not fail on bad rows (such as the UDF) should map the error to
/// [`crate::EMPTY_STATE`].
pub fn decode_zetasketch(bytes: &[u8]) -> Result<Registers> {
    let state = wire::AggregatorState::decode(bytes)
        .context("not a valid zetasketch.AggregatorStateProto")?;

    let agg_type = state
        .r#type
        .context("AggregatorStateProto is missing the required `type` field")?;
    ensure!(
        agg_type == HYPERLOGLOG_PLUS_UNIQUE_TYPE,
        "aggregator type {agg_type} is not HYPERLOGLOG_PLUS_UNIQUE ({HYPERLOGLOG_PLUS_UNIQUE_TYPE}); \
         this is not an HLL++ sketch"
    );

    // Absent means 1 (the proto2 default), which is the unsupported legacy format.
    let encoding_version = state.encoding_version.unwrap_or(1);
    ensure!(
        encoding_version == SUPPORTED_ENCODING_VERSION,
        "unsupported encoding_version {encoding_version}; only v{SUPPORTED_ENCODING_VERSION} sketches are supported"
    );

    let hll = state
        .hyperloglogplus_unique_state
        .context("AggregatorStateProto has no HyperLogLogPlusUniqueStateProto payload (field 112)")?;

    // In v2 this field is the precision itself, not the bucket count.
    let precision = hll
        .precision_or_num_buckets
        .context("HLL++ payload is missing precision_or_num_buckets")?;
    let precision = u8::try_from(precision)
        .ok()
        .filter(|p| (1..=MAX_PRECISION).contains(p))
        .with_context(|| format!("precision {precision} out of range [1, {MAX_PRECISION}]"))?;

    let mut registers = match hll.data.as_ref().filter(|d| !d.is_empty()) {
        Some(data) => Registers::new(precision, data.clone())
            .context("dense `data` length does not match the declared precision")?,
        None => Registers::empty(precision)?,
    };

    // Normally exactly one representation is populated, but merging the sparse
    // list into the dense array is what a representation upgrade would do
    // anyway, so handle both being present rather than silently dropping one.
    if let Some(sparse_bytes) = hll.sparse_data.as_ref().filter(|d| !d.is_empty()) {
        let sparse_precision = hll
            .sparse_precision_or_num_buckets
            .context("sketch has sparse_data but no sparse_precision_or_num_buckets")?;
        let sparse_precision = u8::try_from(sparse_precision)
            .ok()
            .filter(|sp| (precision..=MAX_SPARSE_PRECISION).contains(sp))
            .with_context(|| {
                format!(
                    "sparse precision {sparse_precision} out of range [{precision}, {MAX_SPARSE_PRECISION}]"
                )
            })?;

        merge_sparse_into(sparse_bytes, precision, sparse_precision, &mut registers.data)?;
    }

    Ok(registers)
}

/// Decodes the varint-difference-encoded sparse list and folds every entry into
/// `registers`, keeping the maximum rhoW per bucket.
///
/// The list is a sorted run of non-negative `u32` values stored as successive
/// differences (`DifferenceDecoder.java:46-54`), each of which encodes a
/// bucket and a rhoW in one of two layouts distinguished by a flag bit
/// (`Encoding.Sparse`, `Encoding.java:110-135` and `:240-302`).
fn merge_sparse_into(sparse_bytes: &[u8], p: u8, sp: u8, registers: &mut [u8]) -> Result<()> {
    debug_assert!(sp >= p && sp <= MAX_SPARSE_PRECISION && p <= MAX_PRECISION);

    let rho_mask: u32 = (1 << RHO_BITS) - 1;
    // `max(sp, p + 6) <= max(25, 30) = 30`, so this never overflows `u32`.
    let rho_encoded_flag: u32 = 1u32 << std::cmp::max(u32::from(sp), u32::from(p) + RHO_BITS);
    let index_shift = u32::from(sp - p);

    let mut value: u32 = 0;
    let mut pos = 0usize;
    while pos < sparse_bytes.len() {
        let (delta, next) = read_varint_u32(sparse_bytes, pos)?;
        pos = next;
        value = value
            .checked_add(delta)
            .context("sparse_data difference sequence overflowed u32")?;

        let (bucket, rho_w) = if value & rho_encoded_flag == 0 {
            // Flag clear: the value *is* the sparse index. The normal bucket is
            // its high `p` bits, and the rhoW comes from the low `sp - p` bits.
            (
                value >> index_shift,
                compute_rho_w(u64::from(value), index_shift),
            )
        } else {
            // Flag set: the low `sp - p` index bits were all zero, so the value
            // carries the normal index and an explicit sparse rhoW' instead.
            let stripped = value ^ rho_encoded_flag;
            (
                stripped >> RHO_BITS,
                (stripped & rho_mask) as u8 + (sp - p),
            )
        };

        let bucket = bucket as usize;
        if bucket >= registers.len() {
            bail!(
                "sparse value decodes to bucket {bucket}, past the {} buckets of a precision-{p} sketch",
                registers.len()
            );
        }
        if rho_w > registers[bucket] {
            registers[bucket] = rho_w;
        }
    }
    Ok(())
}

/// Number of leading zero bits plus one within the low `bits` bits of `value`;
/// `bits + 1` when they are all zero.
///
/// Ports `Encoding.computeRhoW` (`Encoding.java:466-475`), including its
/// mod-64 shift semantics for `bits == 0` (`wrapping_shl` masks identically to
/// Java's `long <<`).
fn compute_rho_w(value: u64, bits: u32) -> u8 {
    let w = value.wrapping_shl(64 - bits);
    if w == 0 {
        (bits + 1) as u8
    } else {
        (w.leading_zeros() + 1) as u8
    }
}

/// Reads one unsigned base-128 varint (`VarInt.java:66-82`). The v2 sparse
/// differences are always non-negative, so there is no ZigZag step.
fn read_varint_u32(buf: &[u8], mut pos: usize) -> Result<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0u32;
    loop {
        let b = *buf
            .get(pos)
            .context("sparse_data ended mid-varint")?;
        pos += 1;
        result |= u32::from(b & 0x7F) << shift;
        if b & 0x80 == 0 {
            return Ok((result, pos));
        }
        shift += 7;
        ensure!(shift < 32, "sparse_data varint is wider than 32 bits");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a sketch envelope the way BigQuery does, so decoding can be
    /// exercised without a network round trip.
    fn sketch(precision: i32, hll: wire::HllPlusUniqueState) -> Vec<u8> {
        wire::AggregatorState {
            r#type: Some(HYPERLOGLOG_PLUS_UNIQUE_TYPE),
            num_values: Some(0),
            encoding_version: Some(SUPPORTED_ENCODING_VERSION),
            value_type: None,
            hyperloglogplus_unique_state: Some(wire::HllPlusUniqueState {
                precision_or_num_buckets: Some(precision),
                ..hll
            }),
        }
        .encode_to_vec()
    }

    #[test]
    fn decodes_an_empty_sketch_to_all_zero_registers() {
        let regs = decode_zetasketch(&sketch(15, Default::default())).unwrap();
        assert_eq!(regs.precision, 15);
        assert_eq!(regs.bucket_count(), 32768);
        assert_eq!(regs.zero_count(), 32768);
    }

    #[test]
    fn decodes_the_dense_representation_verbatim() {
        let mut data = vec![0u8; 1 << 12];
        data[7] = 9;
        let bytes = sketch(
            12,
            wire::HllPlusUniqueState {
                data: Some(data.clone()),
                ..Default::default()
            },
        );

        let regs = decode_zetasketch(&bytes).unwrap();
        assert_eq!(regs.data, data);
    }

    #[test]
    fn decodes_a_rho_encoded_sparse_value() {
        // p=10, sp=14: flag sits at bit max(14, 16) = 16. A rho-encoded value
        // is flag | bucket << 6 | rhoW', and the normal rhoW adds sp - p = 4.
        let (p, sp) = (10u8, 14u8);
        let flag = 1u32 << 16;
        let encoded = flag | (3u32 << RHO_BITS) | 2;

        let bytes = sketch(
            i32::from(p),
            wire::HllPlusUniqueState {
                sparse_precision_or_num_buckets: Some(i32::from(sp)),
                sparse_data: Some(varint(encoded)),
                sparse_size: Some(1),
                ..Default::default()
            },
        );

        let regs = decode_zetasketch(&bytes).unwrap();
        assert_eq!(regs.data[3], 2 + (sp - p), "rhoW' plus the sp-p bits it stands for");
        assert_eq!(regs.zero_count(), regs.bucket_count() - 1);
    }

    #[test]
    fn decodes_an_index_only_sparse_value() {
        // Flag clear: the value is the sparse index itself. With p=10, sp=14 the
        // bucket is the high 10 bits and the rhoW comes from the low 4.
        let (p, sp) = (10u8, 14u8);
        let sparse_index = (5u32 << 4) | 0b0010; // bucket 5, two leading zeros in the low 4 bits

        let bytes = sketch(
            i32::from(p),
            wire::HllPlusUniqueState {
                sparse_precision_or_num_buckets: Some(i32::from(sp)),
                sparse_data: Some(varint(sparse_index)),
                sparse_size: Some(1),
                ..Default::default()
            },
        );

        let regs = decode_zetasketch(&bytes).unwrap();
        assert_eq!(regs.data[5], 3, "0b0010 in 4 bits -> two leading zeros + 1");
    }

    #[test]
    fn sparse_values_are_difference_encoded_and_deduped_by_max() {
        let (p, sp) = (10u8, 14u8);
        let flag = 1u32 << 16;
        let first = flag | (3u32 << RHO_BITS) | 2;
        let second = flag | (3u32 << RHO_BITS) | 5; // same bucket, larger rhoW

        let mut sparse = varint(first);
        sparse.extend(varint(second - first)); // stored as a difference

        let bytes = sketch(
            i32::from(p),
            wire::HllPlusUniqueState {
                sparse_precision_or_num_buckets: Some(i32::from(sp)),
                sparse_data: Some(sparse),
                sparse_size: Some(2),
                ..Default::default()
            },
        );

        let regs = decode_zetasketch(&bytes).unwrap();
        assert_eq!(regs.data[3], 5 + (sp - p), "the larger rhoW wins");
    }

    #[test]
    fn merges_sparse_data_into_dense_data_when_both_are_present() {
        let (p, sp) = (10u8, 14u8);
        let mut data = vec![0u8; 1 << 10];
        data[1] = 7;
        let flag = 1u32 << 16;

        let bytes = sketch(
            i32::from(p),
            wire::HllPlusUniqueState {
                data: Some(data),
                sparse_precision_or_num_buckets: Some(i32::from(sp)),
                sparse_data: Some(varint(flag | (3u32 << RHO_BITS) | 2)),
                sparse_size: Some(1),
                ..Default::default()
            },
        );

        let regs = decode_zetasketch(&bytes).unwrap();
        assert_eq!(regs.data[1], 7, "dense entries survive");
        assert_eq!(regs.data[3], 2 + (sp - p), "sparse entries are merged in");
    }

    #[test]
    fn rejects_the_legacy_v1_encoding() {
        let bytes = wire::AggregatorState {
            r#type: Some(HYPERLOGLOG_PLUS_UNIQUE_TYPE),
            num_values: Some(0),
            encoding_version: None, // absent means v1
            value_type: None,
            hyperloglogplus_unique_state: Some(wire::HllPlusUniqueState {
                precision_or_num_buckets: Some(15),
                ..Default::default()
            }),
        }
        .encode_to_vec();

        let err = decode_zetasketch(&bytes).unwrap_err().to_string();
        assert!(err.contains("encoding_version"), "{err}");
    }

    #[test]
    fn rejects_a_non_hll_aggregator() {
        let bytes = wire::AggregatorState {
            r#type: Some(100), // SUM
            num_values: Some(0),
            encoding_version: Some(2),
            value_type: None,
            hyperloglogplus_unique_state: None,
        }
        .encode_to_vec();

        assert!(decode_zetasketch(&bytes).is_err());
    }

    #[test]
    fn rejects_an_out_of_range_sparse_precision_instead_of_overflowing() {
        // sp = 200 would make `1 << max(sp, p + 6)` undefined behaviour in a
        // naive implementation; it must be a clean error.
        let bytes = sketch(
            15,
            wire::HllPlusUniqueState {
                sparse_precision_or_num_buckets: Some(200),
                sparse_data: Some(varint(1)),
                ..Default::default()
            },
        );

        let err = decode_zetasketch(&bytes).unwrap_err().to_string();
        assert!(err.contains("sparse precision"), "{err}");
    }

    #[test]
    fn rejects_a_sparse_precision_below_the_normal_precision() {
        let bytes = sketch(
            15,
            wire::HllPlusUniqueState {
                sparse_precision_or_num_buckets: Some(10),
                sparse_data: Some(varint(1)),
                ..Default::default()
            },
        );
        assert!(decode_zetasketch(&bytes).is_err());
    }

    #[test]
    fn rejects_a_dense_array_whose_length_contradicts_the_precision() {
        let bytes = sketch(
            15,
            wire::HllPlusUniqueState {
                data: Some(vec![0u8; 100]),
                ..Default::default()
            },
        );
        assert!(decode_zetasketch(&bytes).is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(decode_zetasketch(&[0xFF, 0xFF, 0xFF]).is_err());
        assert!(decode_zetasketch(&[]).is_err());
    }

    #[test]
    fn compute_rho_w_counts_leading_zeros_in_the_low_bits() {
        assert_eq!(compute_rho_w(0b0000, 4), 5, "all zero -> bits + 1");
        assert_eq!(compute_rho_w(0b1000, 4), 1);
        assert_eq!(compute_rho_w(0b0100, 4), 2);
        assert_eq!(compute_rho_w(0b0001, 4), 4);
    }

    #[test]
    fn read_varint_u32_round_trips_and_rejects_truncation() {
        for value in [0u32, 1, 127, 128, 300, 16_383, 16_384, u32::MAX] {
            let encoded = varint(value);
            assert_eq!(read_varint_u32(&encoded, 0).unwrap(), (value, encoded.len()));
        }
        assert!(read_varint_u32(&[0x80], 0).is_err(), "continuation bit with no successor");
        assert!(read_varint_u32(&[], 0).is_err());
    }

    fn varint(mut value: u32) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }
}
