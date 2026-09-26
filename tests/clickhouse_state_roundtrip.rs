//! Checks the encoder against bytes ClickHouse itself produced.
//!
//! `tests/fixtures/clickhouse_state_numbers_100000.hex` is the output of
//! `SELECT hex(uniqCombined64State(15)(number)) FROM numbers(100000)` on
//! `clickhouse/clickhouse-server:24.8`. Decoding it with an independent reader
//! and re-encoding must reproduce the original bytes exactly — if any field of
//! the layout were misplaced, the round trip would not close.

use zetasketch_clickhouse::{encode_clickhouse_state, state_size, Registers};

const PRECISION: u8 = 15;
const BUCKET_COUNT: usize = 1 << PRECISION;
const REGISTER_BYTES: usize = 24576;
const RANK_HISTOGRAM_BYTES: usize = 51 * 4;
const ZEROS_BYTES: usize = 2;

fn fixture() -> Vec<u8> {
    let hex = include_str!("fixtures/clickhouse_state_numbers_100000.hex");
    let hex = hex.trim();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

/// Independent reader for a precision-15 `LARGE` state, mirroring
/// `CompactArray::get`. Production code only ever writes these states, so this
/// inverse lives in the tests.
fn parse_large_state(state: &[u8]) -> (Registers, Vec<u32>, u16) {
    assert_eq!(state[0], 3, "expected the LARGE container marker");
    let body = &state[1..];
    assert_eq!(body.len(), REGISTER_BYTES + RANK_HISTOGRAM_BYTES + ZEROS_BYTES);

    let packed = &body[..REGISTER_BYTES];
    let data = (0..BUCKET_COUNT)
        .map(|i| {
            let bit = i * 6;
            let lo = u16::from(packed[bit / 8]);
            let hi = packed.get(bit / 8 + 1).copied().map_or(0, u16::from);
            (((lo | (hi << 8)) >> (bit % 8)) & 0x3F) as u8
        })
        .collect();

    let histogram = body[REGISTER_BYTES..REGISTER_BYTES + RANK_HISTOGRAM_BYTES]
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .collect();

    let zeros = u16::from_le_bytes(body[REGISTER_BYTES + RANK_HISTOGRAM_BYTES..].try_into().unwrap());

    (Registers::new(PRECISION, data).unwrap(), histogram, zeros)
}

#[test]
fn re_encoding_a_real_clickhouse_state_reproduces_it_byte_for_byte() {
    let original = fixture();
    let (registers, _, _) = parse_large_state(&original);

    assert_eq!(original.len(), state_size(PRECISION).unwrap());
    assert_eq!(
        encode_clickhouse_state(&registers).unwrap(),
        original,
        "encoder output must match ClickHouse's own serialization exactly"
    );
}

#[test]
fn clickhouses_own_rank_histogram_and_zeros_counter_agree_with_its_registers() {
    // Confirms our reading of those two trailing fields: they are derived from
    // the register array, not independent state.
    let (registers, histogram, zeros) = parse_large_state(&fixture());

    let mut expected = vec![0u32; histogram.len()];
    for &rank in &registers.data {
        expected[usize::from(rank)] += 1;
    }

    assert_eq!(histogram, expected);
    assert_eq!(usize::from(zeros), registers.zero_count());
    assert_eq!(histogram.iter().sum::<u32>(), BUCKET_COUNT as u32);
}
