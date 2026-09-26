//! Converts one hex-encoded sketch on the command line and prints the
//! hex-encoded ClickHouse state, for poking at a single row without wiring up
//! the UDF:
//!
//! ```text
//! cargo run --example convert_hex -- <sketch-hex> [precision]
//! ```
//!
//! Pipe the output into ClickHouse to check it:
//!
//! ```text
//! SELECT finalizeAggregation(unhex('<state-hex>')::AggregateFunction(uniqCombined64(15), UInt64))
//! ```

use anyhow::{bail, Context, Result};
use zetasketch_clickhouse::{
    decode_zetasketch, encode_clickhouse_state, DEFAULT_TARGET_PRECISION,
};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);

    let Some(hex) = args.next() else {
        bail!("usage: convert_hex <sketch-hex> [precision]");
    };
    let precision = match args.next() {
        Some(p) => p.parse().context("precision must be a number")?,
        None => DEFAULT_TARGET_PRECISION,
    };

    if hex.len() % 2 != 0 {
        bail!("sketch hex has an odd number of characters");
    }
    let sketch: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).context("sketch hex is not valid hex"))
        .collect::<Result<_>>()?;

    let registers = decode_zetasketch(&sketch)?;
    eprintln!(
        "sketch precision {}, {} of {} buckets set",
        registers.precision,
        registers.bucket_count() - registers.zero_count(),
        registers.bucket_count()
    );

    let state = encode_clickhouse_state(&registers.fold_to(precision)?)?;
    println!("{}", state.iter().map(|b| format!("{b:02X}")).collect::<String>());
    Ok(())
}
