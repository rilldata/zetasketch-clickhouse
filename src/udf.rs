//! The ClickHouse executable-UDF wire protocol.
//!
//! With `<format>RowBinary</format>` and `<send_chunk_header>true</send_chunk_header>`,
//! ClickHouse talks to the process like this:
//!
//! ```text
//! stdin   "<row count>\n"  then that many RowBinary String values
//!                          (each: varint byte length, then the bytes)
//! stdout  that many RowBinary AggregateFunction values
//!                          (each: raw state bytes, no length prefix at all)
//! ```
//!
//! The asymmetry is real and is the single easiest thing to get wrong here:
//! the argument column is a `String`, which RowBinary length-prefixes, while
//! the return column is an `AggregateFunction`, which RowBinary hands straight
//! to the aggregate function's own `deserialize`. Adding a length prefix to the
//! output does not raise an error — ClickHouse reads the first byte of the
//! prefix as a container-type marker, fails to match any known container, and
//! silently yields an empty state. See `docs/udf-protocol.md`.

use std::io::{ErrorKind, Read, Write};

use anyhow::{bail, ensure, Context, Result};

use crate::{decode_zetasketch, encode_clickhouse_state, EMPTY_STATE};

/// Refuse absurd RowBinary string lengths rather than trying to allocate them.
/// Sketches are kilobytes; anything near this means the stream has desynced.
const MAX_ROW_BYTES: u64 = 64 * 1024 * 1024;

/// Converts one sketch, or returns [`EMPTY_STATE`] if it cannot be converted.
///
/// A UDF process serves many queries over its lifetime, so a single unusable
/// row must never take the process — or the query — down with it. Rows that
/// fail here contribute nothing to a roll-up, exactly like a `NULL` would.
pub fn convert_one(sketch_bytes: &[u8], target_precision: u8) -> Result<Vec<u8>, ConversionError> {
    decode_zetasketch(sketch_bytes)
        .and_then(|regs| regs.fold_to(target_precision))
        .and_then(|regs| encode_clickhouse_state(&regs))
        .map_err(|source| ConversionError {
            source,
            input_len: sketch_bytes.len(),
        })
}

/// Why one row could not be converted, with enough context to tell a malformed
/// value apart from a systematically wrong configuration.
#[derive(Debug)]
pub struct ConversionError {
    pub source: anyhow::Error,
    pub input_len: usize,
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#} (input was {} bytes)", self.source, self.input_len)
    }
}

/// Reads chunks from `input` until end of stream, writing one converted state
/// per input row to `output`.
///
/// `diagnostics` receives at most two lines for the whole run: the first
/// conversion failure and a final tally. That is deliberate — ClickHouse does
/// not necessarily drain a UDF's stderr, so a per-row log could fill the pipe
/// and wedge the process, while total silence makes a systematically
/// unconvertible column (the wrong precision, v1 sketches) look like a column
/// of zeros with no explanation.
pub fn run(
    input: &mut impl Read,
    output: &mut impl Write,
    target_precision: u8,
    diagnostics: &mut impl Write,
) -> Result<()> {
    let mut failures: u64 = 0;
    let mut rows: u64 = 0;

    loop {
        let Some(row_count) = read_chunk_header(input)? else {
            break; // end of stream between chunks
        };

        for _ in 0..row_count {
            let sketch = read_row_binary_string(input)?;
            rows += 1;

            let state = match convert_one(&sketch, target_precision) {
                Ok(state) => state,
                Err(error) => {
                    failures += 1;
                    if failures == 1 {
                        let _ = writeln!(diagnostics, "zetasketch-clickhouse: row {rows}: {error}");
                        let _ = writeln!(
                            diagnostics,
                            "zetasketch-clickhouse: returning the empty state for this and any \
                             further unconvertible rows; only the first is reported"
                        );
                    }
                    EMPTY_STATE.to_vec()
                }
            };

            if let Err(e) = output.write_all(&state) {
                if e.kind() == ErrorKind::BrokenPipe {
                    return Ok(()); // ClickHouse tore the pool down; nothing to report
                }
                return Err(e).context("writing converted state");
            }
        }

        if let Err(e) = output.flush() {
            if e.kind() == ErrorKind::BrokenPipe {
                return Ok(());
            }
            return Err(e).context("flushing converted chunk");
        }
    }

    if failures > 0 {
        let _ = writeln!(
            diagnostics,
            "zetasketch-clickhouse: {failures} of {rows} rows could not be converted"
        );
    }
    Ok(())
}

/// Reads a `send_chunk_header` row count: ASCII decimal digits terminated by a
/// newline, not a fixed-width integer (`SendingChunkHeaderTransform::transform`
/// in `src/Processors/Sources/ShellCommandSource.cpp` writes
/// `writeText(chunk.getNumRows(), buffer); writeChar('\n', buffer);`).
///
/// Returns `None` at a clean end of stream; a header cut in half is an error.
pub fn read_chunk_header(input: &mut impl Read) -> Result<Option<u64>> {
    let mut digits = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        if input.read(&mut byte).context("reading chunk header")? == 0 {
            if digits.is_empty() {
                return Ok(None);
            }
            bail!("stream ended inside a chunk header (no trailing newline)");
        }
        if byte[0] == b'\n' {
            break;
        }
        digits.push(byte[0]);
    }

    let text = std::str::from_utf8(&digits).context("chunk header is not UTF-8")?;
    let count = text
        .parse::<u64>()
        .with_context(|| format!("chunk header {text:?} is not a row count"))?;
    Ok(Some(count))
}

/// Reads one RowBinary `String`: a varint byte length followed by the bytes.
pub fn read_row_binary_string(input: &mut impl Read) -> Result<Vec<u8>> {
    let len = read_varint_u64(input)?;
    ensure!(
        len <= MAX_ROW_BYTES,
        "RowBinary string claims {len} bytes, above the {MAX_ROW_BYTES}-byte sanity limit; \
         the input stream has probably desynced"
    );

    let mut buf = vec![0u8; len as usize];
    input
        .read_exact(&mut buf)
        .context("stream ended inside a RowBinary string")?;
    Ok(buf)
}

fn read_varint_u64(input: &mut impl Read) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        input
            .read_exact(&mut byte)
            .context("stream ended inside a varint")?;
        result |= u64::from(byte[0] & 0x7F) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        ensure!(shift < 64, "varint is wider than 64 bits");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_size;

    fn write_row_binary_string(out: &mut Vec<u8>, bytes: &[u8]) {
        let mut len = bytes.len() as u64;
        loop {
            let byte = (len & 0x7F) as u8;
            len >>= 7;
            if len == 0 {
                out.push(byte);
                break;
            }
            out.push(byte | 0x80);
        }
        out.extend_from_slice(bytes);
    }

    #[test]
    fn chunk_header_is_decimal_text() {
        assert_eq!(read_chunk_header(&mut &b"12345\n"[..]).unwrap(), Some(12345));
        assert_eq!(read_chunk_header(&mut &b"0\n"[..]).unwrap(), Some(0));
    }

    #[test]
    fn chunk_header_reports_end_of_stream_and_truncation_differently() {
        assert_eq!(read_chunk_header(&mut &b""[..]).unwrap(), None);
        assert!(read_chunk_header(&mut &b"12"[..]).is_err());
        assert!(read_chunk_header(&mut &b"nope\n"[..]).is_err());
    }

    #[test]
    fn row_binary_strings_are_length_prefixed() {
        let mut encoded = Vec::new();
        write_row_binary_string(&mut encoded, b"hello");
        assert_eq!(read_row_binary_string(&mut &encoded[..]).unwrap(), b"hello");
    }

    #[test]
    fn an_absurd_length_is_rejected_instead_of_allocated() {
        // varint for u64::MAX, which a desynced stream can easily produce
        let huge = [0xFF; 10];
        assert!(read_row_binary_string(&mut &huge[..]).is_err());
    }

    #[test]
    fn unconvertible_rows_become_empty_states_without_failing_the_run() {
        let mut input = Vec::new();
        input.extend_from_slice(b"2\n");
        write_row_binary_string(&mut input, &[]);
        write_row_binary_string(&mut input, &[0xDE, 0xAD]);

        let mut output = Vec::new();
        let mut diagnostics = Vec::new();
        run(&mut &input[..], &mut output, 15, &mut diagnostics).unwrap();

        // Output is the two states back to back: no chunk header, no length
        // prefixes. Anything else would desync ClickHouse's reader.
        let mut expected = Vec::new();
        expected.extend_from_slice(&EMPTY_STATE);
        expected.extend_from_slice(&EMPTY_STATE);
        assert_eq!(output, expected);
    }

    #[test]
    fn diagnostics_are_bounded_no_matter_how_many_rows_fail() {
        let rows = 500;
        let mut input = format!("{rows}\n").into_bytes();
        for _ in 0..rows {
            write_row_binary_string(&mut input, &[0xDE, 0xAD]);
        }

        let mut output = Vec::new();
        let mut diagnostics = Vec::new();
        run(&mut &input[..], &mut output, 15, &mut diagnostics).unwrap();

        let text = String::from_utf8(diagnostics).unwrap();
        assert_eq!(text.lines().count(), 3, "first failure (2 lines) plus one tally");
        assert!(text.contains("500 of 500 rows could not be converted"), "{text}");
        assert_eq!(output.len(), rows * EMPTY_STATE.len());
    }

    #[test]
    fn a_real_sketch_converts_to_a_full_state_and_reports_nothing() {
        // The smallest fixture: BigQuery HLL_COUNT.INIT over a single value.
        let sketch = hex("087010011802200882070b1001180f20143203b28716");

        let mut input = Vec::new();
        input.extend_from_slice(b"1\n");
        write_row_binary_string(&mut input, &sketch);

        let mut output = Vec::new();
        let mut diagnostics = Vec::new();
        run(&mut &input[..], &mut output, 15, &mut diagnostics).unwrap();

        assert_eq!(output.len(), state_size(15).unwrap());
        assert_eq!(output[0], 3, "LARGE container marker");
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn several_chunks_are_served_from_one_process() {
        let sketch = hex("087010011802200882070b1001180f20143203b28716");
        let mut input = Vec::new();
        for _ in 0..3 {
            input.extend_from_slice(b"1\n");
            write_row_binary_string(&mut input, &sketch);
        }

        let mut output = Vec::new();
        run(&mut &input[..], &mut output, 15, &mut Vec::new()).unwrap();
        assert_eq!(output.len(), 3 * state_size(15).unwrap());
    }

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}
