//! ClickHouse executable UDF: reads ZetaSketch sketches on stdin, writes
//! `uniqCombined64` states on stdout. See `docs/deployment.md`.

use std::io::{self, BufWriter, Write};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use zetasketch_clickhouse::{
    udf, DEFAULT_TARGET_PRECISION, MAX_TARGET_PRECISION, MIN_TARGET_PRECISION,
};

#[derive(Parser)]
#[command(
    name = "zetasketch-clickhouse",
    about = "Convert ZetaSketch HLL++ sketches (BigQuery HLL_COUNT) into ClickHouse uniqCombined64 states",
    long_about = "Reads ClickHouse executable-UDF input on stdin and writes \
                  AggregateFunction(uniqCombined64(P), UInt64) states on stdout.\n\n\
                  Register it with clickhouse/zetasketch_clickhouse_function.xml; the \
                  --precision given here must match the precision in that file's \
                  <return_type>.",
    version
)]
struct Args {
    /// Target uniqCombined64 precision. Must match the UDF's declared return type.
    #[arg(long, default_value_t = DEFAULT_TARGET_PRECISION)]
    precision: u8,

    /// Wire format spoken on stdin/stdout.
    #[arg(long, value_enum, default_value_t = Format::RowBinary)]
    format: Format,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum Format {
    /// ClickHouse RowBinary with `send_chunk_header=true`.
    RowBinary,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Checked once at startup rather than per row: a precision the encoder
    // cannot use would otherwise turn every row into an empty state, and a
    // column of zeros is a miserable thing to debug.
    if !(MIN_TARGET_PRECISION..=MAX_TARGET_PRECISION).contains(&args.precision) {
        anyhow::bail!(
            "--precision {} is outside the supported range [{MIN_TARGET_PRECISION}, {MAX_TARGET_PRECISION}]",
            args.precision
        );
    }

    let stdin = io::stdin();
    let mut input = stdin.lock();
    // Stdout's own lock is line-buffered, which would flush on every 0x0A byte
    // in what is binary output; BufWriter flushes on chunk boundaries instead.
    let mut output = BufWriter::with_capacity(1 << 16, io::stdout().lock());
    let mut diagnostics = io::stderr().lock();

    match args.format {
        Format::RowBinary => udf::run(&mut input, &mut output, args.precision, &mut diagnostics)?,
    }

    output.flush().context("flushing stdout")
}
