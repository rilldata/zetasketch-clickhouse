//! Emitting the ClickHouse side.
//!
//! `uniqCombined64(p)` stores its state through a
//! `CombinedCardinalityEstimator`, which picks one of three containers by
//! cardinality and writes a marker byte to say which: `SMALL` (a flat array,
//! up to 16 values), `MEDIUM` (a hash set, up to `2^(p-5)` values) and `LARGE`
//! (a dense HyperLogLog counter). This module always emits `LARGE`, because
//! that is the only container that can carry an arbitrary register array.
//!
//! A `LARGE` body is three fields, in order:
//!
//! 1. the registers, 6 bits each, packed LSB-first as one little-endian bitstream;
//! 2. a rank histogram — `u32` per rank, `0..=64-p+1` — that ClickHouse keeps
//!    incrementally so it never has to rescan the registers to estimate;
//! 3. a counter of still-zero buckets, in the narrowest unsigned type that fits
//!    `2^p`.
//!
//! See `docs/format-reference.md` for the citations behind each of those.

use anyhow::{ensure, Result};

use crate::registers::Registers;
use crate::{MAX_TARGET_PRECISION, MIN_TARGET_PRECISION};

/// `details::ContainerType::LARGE` (`CombinedCardinalityEstimator.h:19-24`).
const CONTAINER_LARGE: u8 = 3;

/// `details::ContainerType::SMALL` with a zero-length `SmallSet`: the state
/// ClickHouse itself produces for an aggregation over no rows, confirmed with
/// `SELECT hex(uniqCombined64State(15)(number)) FROM numbers(0)` → `0100`.
///
/// The UDF returns this for rows it cannot convert, so a bad sketch contributes
/// nothing to a roll-up instead of failing the query.
pub const EMPTY_STATE: [u8; 2] = [0x01, 0x00];

/// Bits per register in ClickHouse's `CompactArray` rank store. `uniqCombined64`
/// hashes to `UInt64`, which selects a 6-bit rank width
/// (`details::RankWidth<UInt64>`, `HyperLogLogCounter.h:232-238`).
const RANK_WIDTH_BITS: usize = 6;

/// Serialized size of a `uniqCombined64(precision)` `LARGE` state, in bytes.
///
/// For the precision 15 this crate targets, that is 24783: a marker byte,
/// 24576 bytes of registers, a 204-byte rank histogram and a 2-byte zeros
/// counter.
pub fn state_size(precision: u8) -> Result<usize> {
    check_target_precision(precision)?;
    let bucket_count = 1usize << precision;
    Ok(1 + packed_register_bytes(bucket_count)
        + rank_histogram_len(precision) * 4
        + usize::from(zeros_counter_width(bucket_count as u64)))
}

/// Encodes a register array as a ClickHouse `uniqCombined64(precision)`
/// aggregate-function state, ready to be inserted into an
/// `AggregateFunction(uniqCombined64(precision), UInt64)` column or read by
/// `uniqCombined64Merge` / `finalizeAggregation`.
///
/// The output is always the `LARGE` container, whatever the cardinality — see
/// the note on small sketches in `docs/format-reference.md`.
///
/// # Errors
///
/// Fails when `precision` is outside `[12, 24]`, when the register array length
/// contradicts its precision, or when a register exceeds the largest rhoW that
/// precision can represent (which would mean the source sketch and its declared
/// precision disagree).
pub fn encode_clickhouse_state(regs: &Registers) -> Result<Vec<u8>> {
    check_target_precision(regs.precision)?;
    regs.check_invariant()?;

    let precision = regs.precision;
    let bucket_count = regs.bucket_count();
    let max_rank = regs.max_rank();

    // ClickHouse would read a too-large rank back as a different bucket's bits,
    // so refuse rather than emit a corrupt state.
    if let Some((bucket, &rank)) = regs
        .data
        .iter()
        .enumerate()
        .find(|(_, &r)| u32::from(r) > max_rank)
    {
        anyhow::bail!(
            "bucket {bucket} holds rhoW {rank}, above the maximum {max_rank} for precision {precision}"
        );
    }

    // The histogram ClickHouse maintains incrementally: every bucket starts at
    // rank 0 and moves to its own rank once something lands in it
    // (`details::Denominator::update`, `HyperLogLogCounter.h:164-168`).
    let mut rank_histogram = vec![0u32; rank_histogram_len(precision)];
    rank_histogram[0] = bucket_count as u32;
    for &rank in &regs.data {
        if rank != 0 {
            rank_histogram[0] -= 1;
            rank_histogram[usize::from(rank)] += 1;
        }
    }
    let zeros = u64::from(rank_histogram[0]);

    let mut out = Vec::with_capacity(state_size(precision)?);
    out.push(CONTAINER_LARGE);
    pack_registers_into(&regs.data, &mut out);
    for count in &rank_histogram {
        out.extend_from_slice(&count.to_le_bytes());
    }
    let width = usize::from(zeros_counter_width(bucket_count as u64));
    out.extend_from_slice(&zeros.to_le_bytes()[..width]);

    debug_assert_eq!(out.len(), state_size(precision)?);
    Ok(out)
}

fn check_target_precision(precision: u8) -> Result<()> {
    ensure!(
        (MIN_TARGET_PRECISION..=MAX_TARGET_PRECISION).contains(&precision),
        "precision {precision} is outside the supported range \
         [{MIN_TARGET_PRECISION}, {MAX_TARGET_PRECISION}] for ClickHouse states"
    );
    Ok(())
}

/// Ranks are `1..=64-p+1`, plus rank 0 for untouched buckets
/// (`max_rank`, `HyperLogLogCounter.h:550`).
fn rank_histogram_len(precision: u8) -> usize {
    (64 - usize::from(precision) + 1) + 1
}

/// `CompactArray::BITSET_SIZE` (`CompactArray.h:60`).
fn packed_register_bytes(bucket_count: usize) -> usize {
    (bucket_count * RANK_WIDTH_BITS).div_ceil(8)
}

/// Appends the registers as 6-bit fields packed LSB-first and running across
/// byte boundaries — bucket `i` occupies bits `[6i, 6i+6)` of one little-endian
/// bitstream, mirroring `CompactArray::set` (`CompactArray.h:41-50`).
fn pack_registers_into(values: &[u8], out: &mut Vec<u8>) {
    let byte_len = packed_register_bytes(values.len());
    let start = out.len();
    // One byte of slack so the final register can spill past `byte_len` when
    // the bit count is not a multiple of 8; ClickHouse keeps the same padding
    // byte and likewise does not serialize it (`CompactArray.h:52-63`).
    out.resize(start + byte_len + 1, 0);

    for (i, &value) in values.iter().enumerate() {
        let bit = i * RANK_WIDTH_BITS;
        let byte = start + bit / 8;
        let offset = bit % 8;
        let value = u16::from(value) & 0x3F;
        out[byte] |= (value << offset) as u8;
        if offset + RANK_WIDTH_BITS > 8 {
            out[byte + 1] |= (value >> (8 - offset)) as u8;
        }
    }

    out.truncate(start + byte_len);
}

/// `details::MinCounterType<bucket_count>` (`HyperLogLogCounter.h:60-75`): the
/// narrowest unsigned type that can hold a count up to `bucket_count`.
fn zeros_counter_width(bucket_count: u64) -> u8 {
    let wider = u8::from(bucket_count >= 1 << 8)
        + u8::from(bucket_count >= 1 << 16)
        + u8::from(bucket_count >= 1u64 << 32);
    1 << wider
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads registers back out of a packed state body, mirroring
    /// `CompactArray::get`, so packing can be checked against its own inverse.
    fn unpack(packed: &[u8], bucket_count: usize) -> Vec<u8> {
        let mut padded = packed.to_vec();
        padded.push(0);
        (0..bucket_count)
            .map(|i| {
                let bit = i * RANK_WIDTH_BITS;
                let word = u16::from(padded[bit / 8]) | (u16::from(padded[bit / 8 + 1]) << 8);
                ((word >> (bit % 8)) & 0x3F) as u8
            })
            .collect()
    }

    #[test]
    fn state_size_matches_the_observed_precision_15_layout() {
        assert_eq!(state_size(15).unwrap(), 24783);
        assert_eq!(1 + 24576 + 51 * 4 + 2, 24783);
    }

    #[test]
    fn state_size_rejects_unsupported_precisions() {
        assert!(state_size(11).is_err());
        assert!(state_size(25).is_err());
    }

    #[test]
    fn precision_15_registers_pack_without_a_partial_byte() {
        assert_eq!(packed_register_bytes(1 << 15), 24576);
        assert_eq!((1 << 15) * 6 % 8, 0);
    }

    #[test]
    fn packing_round_trips_through_its_inverse() {
        let values: Vec<u8> = (0..1000).map(|i| (i % 51) as u8).collect();
        let mut packed = Vec::new();
        pack_registers_into(&values, &mut packed);
        assert_eq!(packed.len(), packed_register_bytes(values.len()));
        assert_eq!(unpack(&packed, values.len()), values);
    }

    #[test]
    fn packing_appends_rather_than_overwriting() {
        let mut out = vec![0xAA];
        pack_registers_into(&[0x3F, 0x3F], &mut out);
        assert_eq!(out[0], 0xAA, "existing bytes are preserved");
    }

    #[test]
    fn zeros_counter_width_matches_min_counter_type() {
        assert_eq!(zeros_counter_width(255), 1);
        assert_eq!(zeros_counter_width(256), 2);
        assert_eq!(zeros_counter_width(1 << 15), 2); // precision 15
        assert_eq!(zeros_counter_width(65535), 2);
        assert_eq!(zeros_counter_width(1 << 16), 4); // precision 16
        assert_eq!(zeros_counter_width(1u64 << 32), 8);
    }

    #[test]
    fn empty_registers_encode_to_a_well_formed_state() {
        let state = encode_clickhouse_state(&Registers::empty(15).unwrap()).unwrap();
        assert_eq!(state.len(), 24783);
        assert_eq!(state[0], CONTAINER_LARGE);

        // Every bucket is still at rank 0, so the histogram and the zeros
        // counter both say "all of them".
        let histogram_at = 1 + 24576;
        assert_eq!(&state[histogram_at..histogram_at + 4], 32768u32.to_le_bytes());
        assert_eq!(&state[24781..], 32768u16.to_le_bytes());
    }

    #[test]
    fn the_rank_histogram_and_zeros_counter_describe_the_registers() {
        let mut regs = Registers::empty(15).unwrap();
        regs.data[0] = 1;
        regs.data[1] = 1;
        regs.data[2] = 7;

        let state = encode_clickhouse_state(&regs).unwrap();
        let histogram_at = 1 + 24576;
        let rank = |r: usize| {
            let at = histogram_at + r * 4;
            u32::from_le_bytes(state[at..at + 4].try_into().unwrap())
        };

        assert_eq!(rank(0), 32768 - 3);
        assert_eq!(rank(1), 2);
        assert_eq!(rank(7), 1);
        assert_eq!(
            u16::from_le_bytes(state[24781..].try_into().unwrap()),
            32768 - 3
        );
    }

    #[test]
    fn rejects_precisions_clickhouse_would_lay_out_differently() {
        assert!(encode_clickhouse_state(&Registers::empty(11).unwrap()).is_err());
    }

    #[test]
    fn rejects_a_rank_too_large_for_the_precision() {
        let mut regs = Registers::empty(15).unwrap();
        regs.data[0] = 51; // max_rank at precision 15 is 50
        assert!(encode_clickhouse_state(&regs).is_err());
    }

    #[test]
    fn rejects_registers_whose_length_contradicts_their_precision() {
        let bad = Registers {
            precision: 15,
            data: vec![0; 10],
        };
        assert!(encode_clickhouse_state(&bad).is_err());
    }
}
