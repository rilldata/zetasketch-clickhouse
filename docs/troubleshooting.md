# Troubleshooting

Symptom first. Most failures here are silent by nature — a broken conversion
produces empty sketches rather than an error — so start from what the data
looks like, not from the logs.

## `Function with name 'zetasketch_to_uniq_combined64_p15' does not exist`

The registration file was not loaded. In order of likelihood:

1. **Wrong directory.** It must be in the config root
   (`/etc/clickhouse-server/`), not `config.d/`. A file in `config.d/` is
   merged into the server config, which is a different mechanism — you will see
   `Merging configuration file …` in the log and no function.
2. **Wrong filename.** It must match `user_defined_executable_functions_config`,
   default `*_function.*ml`. `udf.xml` does not match;
   `zetasketch_clickhouse_function.xml` does.
3. **Malformed XML.** Check the log for the loader's own errors:
   ```bash
   grep ExternalUserDefinedExecutableFunctionsLoader \
     /var/log/clickhouse-server/clickhouse-server.log | tail
   ```
   - `SAXParseException: Invalid token` — almost always a `--` inside an XML
     comment, which is illegal and rejects the whole file. Run
     `cargo test --test udf_config` against your edited copy.
   - `Not found: functions.name` — the file is wrapped in `<clickhouse>`. These
     files are parsed standalone; the root element is `<functions>`.
4. **Not reloaded.** Run `SYSTEM RELOAD FUNCTIONS`. Over HTTP it must be a
   POST; a GET is read-only and fails with
   `Cannot execute query in readonly mode`.
5. **Only some nodes.** If it works sometimes, the function is missing on
   certain shards or replicas. Check every node at once:
   ```sql
   SELECT hostName(), count() FROM clusterAllReplicas('<cluster>', system.functions)
   WHERE name = 'zetasketch_to_uniq_combined64_p15' GROUP BY hostName();
   ```

## `Executable file … does not exist inside user scripts folder`

`<command>` is resolved against `user_scripts_path`
(default `/var/lib/clickhouse/user_scripts/`), and an absolute path is refused.
Use the bare name in `<command>` and install the binary into that directory,
owned by and executable for the `clickhouse` user.

## `Child process was exited with return code 101`

101 is a Rust panic. Given the binary's own tests pass, the usual cause is that
the process is being fed something other than the expected framing — check that
`<send_chunk_header>true</send_chunk_header>` and `<format>RowBinary</format>`
are both set, since the reader expects a text row count before each chunk.

Reproduce outside ClickHouse to see the actual message:

```bash
printf '1\n' > /tmp/probe.bin
python3 -c "
import sys
s = bytes.fromhex('087010011802200882070b1001180f20143203b28716')
sys.stdout.buffer.write(bytes([len(s)])+s)" >> /tmp/probe.bin

/var/lib/clickhouse/user_scripts/zetasketch-clickhouse \
  --precision 15 --format row-binary < /tmp/probe.bin | wc -c
# expect 24783
```

## `GLIBC_2.xx not found` when the process starts

The binary was built against a newer glibc than the server image has
(ClickHouse's official images are Ubuntu 20.04, glibc 2.31). Rebuild as a
static musl binary — see [deployment.md](deployment.md#1-build-a-binary-for-the-server).
Confirm with `file`: it should say `statically linked`.

## Every row comes back as `0`

The single most likely failure, because several distinct causes all funnel into
it: the UDF returns ClickHouse's empty state for any row it cannot convert, and
an empty state finalizes to 0.

Work through it in this order:

**1. Is it the conversion, or the data?** Check a row the tool is known to
handle:

```sql
SELECT finalizeAggregation(
  zetasketch_to_uniq_combined64_p15(unhex('087010011802200882070b1001180f20143203b28716'))
);
-- 1 means the pipeline is healthy and the problem is your input
-- 0 means the conversion path itself is broken
```

**2. If the known-good fixture also returns 0**, the output framing is being
misread. This is the signature of a length prefix on the output side: ClickHouse
reads the prefix's first byte as a container marker, matches nothing, and leaves
the state empty without erroring. Confirm the binary is the one you think it is
(`md5sum` it on the node) and that it produces exactly 24 783 bytes for the
fixture using the probe command above.

**3. If the fixture works but your data does not**, the rows are being rejected
by the decoder. The process reports the first failure on stderr; whether you
see it depends on `stderr_reaction`, so the reliable route is to test a sample
row directly:

```bash
cargo run --example convert_hex -- <your-sketch-hex>
```

Likely messages:

| message | meaning |
|---|---|
| `unsupported encoding_version 1` | legacy sketch format; not supported — re-export with a current BigQuery pipeline |
| `aggregator type … is not HYPERLOGLOG_PLUS_UNIQUE` | the column is not an HLL sketch, or the bytes are something else entirely |
| `not a valid zetasketch.AggregatorStateProto` | the bytes are truncated or mangled — a common cause is the hex never being `unhex`-ed, or being double-encoded in export |
| `cannot fold precision 12 up to 15` | the sketches were built at a lower precision than the column expects; rebuild them at 15 or use a `_p12` function |
| `precision … out of range` | the sketch declares something implausible; the bytes are probably corrupt |

**4. Check the precision actually in use.** A `--precision` the encoder rejects
fails at startup, but one that merely disagrees with the declared return type
does not — and it produces exactly this symptom.

ClickHouse does not expose an executable UDF's definition through SQL
(`system.functions.create_query` is empty for them; `origin` only tells you it
is `ExecutableUserDefined`), so read it off the node:

```bash
grep -E 'return_type|command' /etc/clickhouse-server/zetasketch_clickhouse_function.xml
```

The precision inside `uniqCombined64(…)` and the one after `--precision` must
match each other, and both must match the target column's type. On a repo
checkout, `cargo test --test udf_config` asserts the first of those.

## Counts differ slightly from BigQuery

Expected, and not a bug: identical registers, different bias correction in each
engine. Measured drift is well under 1% — see
[validation.md](validation.md#why-these-are-not-zero). If a row is off by
orders of magnitude rather than fractions of a percent, that is a real problem;
compare the register fill (`convert_hex` prints buckets set) against the
expected cardinality.

## Merged counts are far too high

Almost always mixed sketch lineages: converted BigQuery sketches merged with
sketches ClickHouse built itself over the same population. They are independent
samplings, so merging overstates. See
[usage.md](usage.md#do-not-mix-sketch-lineages).

## Inserts are slow, or memory climbs during a backfill

Each converted state is 24 783 bytes regardless of cardinality, so a wide
backfill moves a lot of bytes. Before raising `pool_size`:

- convert a partition at a time rather than the whole range in one statement;
- check whether low-cardinality rows need a sketch at all, or could be
  aggregated to a coarser grain first;
- confirm the bottleneck really is the UDF — `system.query_log`'s
  `ProfileEvents` will show where the time goes.

`pool_size` beyond the node's core count does not help; each process is a
separate OS process with its own buffers.

## The function disappeared after a restart

The XML is in place but the binary is not — for example it was written to a
container filesystem that is not persisted, while the config came from a
ConfigMap. On Kubernetes, install the binary into the data volume with an init
container so it survives pod replacement
([deployment.md](deployment.md#kubernetes-clickhouse-operator)).
