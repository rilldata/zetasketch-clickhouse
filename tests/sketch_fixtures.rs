//! Checks the decoder against sketches BigQuery itself produced.
//!
//! `tests/fixtures/bigquery_sketches.csv` holds real `HLL_COUNT.INIT(x, 15)`
//! output over synthetic inputs chosen to span the sparse → dense
//! representation change, together with BigQuery's own `HLL_COUNT.EXTRACT`
//! estimate for each. See `docs/validation.md` for how it was generated and
//! for the cross-engine numbers.

use zetasketch_clickhouse::{decode_zetasketch, encode_clickhouse_state, state_size};

struct Fixture {
    label: String,
    sketch: Vec<u8>,
    bigquery_estimate: u64,
}

fn fixtures() -> Vec<Fixture> {
    include_str!("fixtures/bigquery_sketches.csv")
        .lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut columns = line.splitn(3, ',');
            let label = columns.next().unwrap().to_string();
            let hex = columns.next().unwrap();
            let bigquery_estimate = columns.next().unwrap().trim().parse().unwrap();
            let sketch = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            Fixture { label, sketch, bigquery_estimate }
        })
        .collect()
}

#[test]
fn the_fixture_set_still_covers_both_representations() {
    let labels: Vec<_> = fixtures().into_iter().map(|f| f.label).collect();
    for expected in ["single", "small_sparse", "medium_sparse", "dense_normal"] {
        assert!(labels.iter().any(|l| l == expected), "missing fixture {expected}");
    }
}

#[test]
fn every_bigquery_sketch_converts_to_a_well_formed_precision_15_state() {
    for fixture in fixtures() {
        let registers = decode_zetasketch(&fixture.sketch)
            .unwrap_or_else(|e| panic!("{}: decode failed: {e:#}", fixture.label));
        let registers = registers
            .fold_to(15)
            .unwrap_or_else(|e| panic!("{}: fold failed: {e:#}", fixture.label));
        let state = encode_clickhouse_state(&registers)
            .unwrap_or_else(|e| panic!("{}: encode failed: {e:#}", fixture.label));

        assert_eq!(state[0], 3, "{}: expected LARGE marker", fixture.label);
        assert_eq!(state.len(), state_size(15).unwrap(), "{}", fixture.label);
    }
}

#[test]
fn decoded_registers_are_consistent_with_bigquerys_own_estimate() {
    // Not an accuracy check — that needs ClickHouse's estimator, see
    // docs/validation.md. This only asserts the decode produced a register
    // array of the right shape: a sketch over n distinct values cannot have
    // touched more than n buckets, and must have touched at least one.
    for fixture in fixtures() {
        let registers = decode_zetasketch(&fixture.sketch).unwrap();
        let touched = registers.bucket_count() - registers.zero_count();

        assert!(touched > 0, "{}: no buckets set", fixture.label);
        assert!(
            touched as u64 <= fixture.bigquery_estimate,
            "{}: {touched} buckets set for an estimate of {}",
            fixture.label,
            fixture.bigquery_estimate
        );
    }
}

#[test]
fn folding_to_a_coarser_precision_never_loses_a_touched_bucket() {
    for fixture in fixtures() {
        let registers = decode_zetasketch(&fixture.sketch).unwrap();
        let folded = registers.fold_to(12).unwrap();

        let touched_before = registers.bucket_count() - registers.zero_count();
        let touched_after = folded.bucket_count() - folded.zero_count();

        assert!(
            touched_after <= touched_before,
            "{}: folding cannot create buckets",
            fixture.label
        );
        assert!(
            touched_after >= touched_before.min(folded.bucket_count()) / 8,
            "{}: folding lost an implausible number of buckets ({touched_before} -> {touched_after})",
            fixture.label
        );
    }
}
