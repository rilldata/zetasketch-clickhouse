# The executable-UDF protocol

What ClickHouse actually sends to and expects from an `executable_pool` UDF
process, and the three places where the documentation leaves you to find out
the hard way. Everything here was read out of ClickHouse's source and then
confirmed against a running 24.8 server.

## The exchange

With `<format>RowBinary</format>` and `<send_chunk_header>true</send_chunk_header>`,
for each block of rows:

```
ClickHouse ──▶ stdin    "3\n"                       row count, ASCII, newline-terminated
                        <varint len><bytes>         row 1, RowBinary String
                        <varint len><bytes>         row 2
                        <varint len><bytes>         row 3

ClickHouse ◀── stdout   <state bytes>               row 1, RowBinary AggregateFunction
                        <state bytes>               row 2
                        <state bytes>               row 3
```

A pooled process stays alive across queries and receives many such blocks, so
it must loop until stdin reaches EOF rather than handling one block and exiting.

## Gotcha 1: the chunk header is text, not a binary integer

"Send row count before the chunk" reads like a length prefix. It is not:

```cpp
// src/Processors/Sources/ShellCommandSource.cpp, SendingChunkHeaderTransform
void transform(Chunk & chunk) override
{
    writeText(chunk.getNumRows(), buffer);
    writeChar('\n', buffer);
}
```

So three rows arrive as the two bytes `31 0A`, not as eight little-endian
bytes. Reading it as a `UInt64` consumes the start of the first row's data and
desyncs the stream immediately — which surfaces as the process dying on a
nonsense allocation, not as a protocol error.

This also applies in one direction only. `SendingChunkHeaderTransform` is
attached to the *input* pipe (`ShellCommandSource.cpp:959-964`); nothing adds a
header to what ClickHouse reads back. The output is bare rows.

## Gotcha 2: the output has no length prefixes at all

The input argument is a `String`, which RowBinary length-prefixes. The return
value is an `AggregateFunction`, which RowBinary does **not**:

```cpp
// src/DataTypes/Serializations/SerializationAggregateFunction.cpp:63-83
void SerializationAggregateFunction::deserializeBinary(IColumn & column, ReadBuffer & istr, …) const
{
    …
    function->deserialize(place, istr, version, &arena);   // reads straight from the stream
}
```

Nothing tells `deserialize` how many bytes to take. It works because a
`uniqCombined64` state is self-terminating: the marker byte says which
container follows, which fixes the length.

Writing a length prefix anyway does not raise an error, which is what makes
this expensive to debug. ClickHouse reads the prefix's first byte as the
container marker; for a 24 783-byte state that byte is `0xCF`, which matches no
container. `CombinedCardinalityEstimator::read` has no `else` branch for an
unrecognized marker (`src/Common/CombinedCardinalityEstimator.h:232-250`), so it
silently leaves the state at its default — empty. Every row comes back `0` with
no diagnostic anywhere.

The tests in `src/udf.rs` pin both sides of this asymmetry.

## Gotcha 3: where ClickHouse looks for the pieces

### The registration file

The server scans for executable-function definitions with
`user_defined_executable_functions_config`, which defaults to `*_function.*ml`
and is resolved as a glob **against the config directory root**. Two consequences:

- The file must be named to match — `zetasketch_clickhouse_function.xml`, not
  `udf.xml`.
- It must sit in `/etc/clickhouse-server/`, not `config.d/`. Files in
  `config.d/` are merged into the main config, which is a different mechanism;
  the loader never sees them as function definitions. The server will happily
  log `Merging configuration file …` for such a file and still not register
  anything.

Because the file is parsed standalone, its root element is `<functions>`, not
`<clickhouse><functions>`. With the wrapper, the loader fails with
`Not found: functions.name`.

### The binary

`<command>` is resolved relative to `user_scripts_path` (default
`/var/lib/clickhouse/user_scripts/`). An absolute path is rejected outright:

```
Executable file /opt/udf/zetasketch-clickhouse does not exist
inside user scripts folder /var/lib/clickhouse/user_scripts/
```

So `<command>zetasketch-clickhouse --precision 15 --format row-binary</command>`,
with the binary installed into that directory.

### XML comments cannot contain `--`

Not ClickHouse-specific, but it bites here because the natural thing to write
in a comment is the command line, and the flags contain double hyphens. The
SAX parser rejects the *entire file*, and the only symptom is the function
missing. `tests/udf_config.rs` checks the shipped file for this, along with the
root element, the binary path form and the precision agreeing with the declared
return type.

## Process lifecycle

`lifetime = 0` keeps pooled processes alive indefinitely, so one process
handles many blocks from many queries. Practical consequences for the
implementation:

- **Loop until EOF.** Exiting after one block makes ClickHouse respawn per
  block, which is slow but works — so this bug hides in light testing.
- **A single bad row must not be fatal.** Killing the process over one
  malformed value takes out the other queries sharing the pool. Bad rows are
  converted into the empty state instead.
- **Stderr is not reliably drained.** With the default `stderr_reaction`,
  ClickHouse does not necessarily read the pipe, so unbounded logging can fill
  it and block the process mid-write. Diagnostics are capped at three lines per
  process.
- **`BrokenPipe` on write is normal.** It means the pool was torn down; the
  process exits cleanly rather than reporting an error nobody is listening for.
- **A restart replaces the binary.** After overwriting it, `SYSTEM RELOAD
  FUNCTIONS` recycles pooled processes; in-flight queries finish against the old
  one.

## Reproducing the wire format by hand

Useful when changing the framing. This is exactly what ClickHouse would send
for one row:

```bash
python3 - <<'EOF' > /tmp/one_row.bin
import sys
sketch = bytes.fromhex("087010011802200882070b1001180f20143203b28716")
out = sys.stdout.buffer
out.write(b"1\n")                      # chunk header: one row, as text
out.write(bytes([len(sketch)]))        # varint length (short enough to be one byte)
out.write(sketch)
EOF

./zetasketch-clickhouse --precision 15 --format row-binary < /tmp/one_row.bin | wc -c
# 24783 — one state, no framing around it
```
