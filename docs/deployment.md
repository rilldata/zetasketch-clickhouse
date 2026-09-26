# Deployment

Installing the UDF on a ClickHouse cluster: what to build, where the files go,
how to roll it out across nodes, and how to verify and back it out.

## What has to be true on every node

An executable UDF runs as a child process of the ClickHouse server that
evaluates it. In a cluster, any node that can be asked to evaluate the function
must have both pieces, or queries that happen to land there fail with
`Function with name 'zetasketch_to_uniq_combined64_p15' does not exist`:

| file | location | notes |
|---|---|---|
| the binary | `<user_scripts_path>/zetasketch-clickhouse` | default `/var/lib/clickhouse/user_scripts/`, must be executable by the `clickhouse` user |
| the registration | `<config_dir>/zetasketch_clickhouse_function.xml` | default `/etc/clickhouse-server/`, **not** `config.d/` |

"Any node that can be asked" means **every shard and every replica** when the
conversion runs against a `Distributed` table, since the function is evaluated
on whichever node holds the data. Skipping read-only replicas is a common way
to get intermittent failures weeks later.

Both constraints are non-obvious and explained in
[udf-protocol.md](udf-protocol.md#where-clickhouse-looks-for-the-pieces).

## 1. Build a binary for the server

The binary runs inside the server's environment, which is usually not your
workstation. Build it statically against musl so it does not depend on the
server's glibc version — ClickHouse's official images are still Ubuntu 20.04
(glibc 2.31), and a binary built on a newer toolchain will fail at exec with
`GLIBC_2.34 not found`.

Pick the target that matches the **server**, not your laptop:

```bash
rustup target add aarch64-unknown-linux-musl     # arm64 nodes
rustup target add x86_64-unknown-linux-musl      # amd64 nodes

cargo build --release --bin zetasketch-clickhouse \
  --target aarch64-unknown-linux-musl
```

On macOS, cross-compiling to musl needs a Linux toolchain; the simplest route
is a builder container of the right architecture:

```bash
docker run --rm --platform linux/arm64 \
  -v "$PWD":/work -w /work rust:1-bookworm bash -c '
    apt-get update -qq && apt-get install -y -qq protobuf-compiler musl-tools
    rustup target add aarch64-unknown-linux-musl
    cargo build --release --bin zetasketch-clickhouse --target aarch64-unknown-linux-musl'
```

Confirm what you built before shipping it:

```bash
file target/aarch64-unknown-linux-musl/release/zetasketch-clickhouse
# ELF 64-bit LSB executable, ARM aarch64, statically linked
```

## 2. Install on a node

```bash
install -o clickhouse -g clickhouse -m 0755 \
  zetasketch-clickhouse /var/lib/clickhouse/user_scripts/zetasketch-clickhouse

install -o clickhouse -g clickhouse -m 0644 \
  clickhouse/zetasketch_clickhouse_function.xml \
  /etc/clickhouse-server/zetasketch_clickhouse_function.xml

clickhouse-client --query "SYSTEM RELOAD FUNCTIONS"
```

`SYSTEM RELOAD FUNCTIONS` is enough; no restart is required. It is a write
operation, so over the HTTP interface it must be a POST — a GET is treated as
read-only and rejected.

If you change `--precision`, change `<return_type>` in the same edit. A
mismatch is not an error, it is a column of zeros. `cargo test --test udf_config`
checks the shipped file for this, but only the shipped file.

## 3. Roll it out across the cluster

### Bare metal or VMs

Config management is the only sane route; the manual form is:

```bash
for host in $(cat nodes.txt); do
  scp zetasketch-clickhouse "$host":/tmp/
  scp clickhouse/zetasketch_clickhouse_function.xml "$host":/tmp/
  ssh "$host" '
    sudo install -o clickhouse -g clickhouse -m 0755 \
      /tmp/zetasketch-clickhouse /var/lib/clickhouse/user_scripts/
    sudo install -o clickhouse -g clickhouse -m 0644 \
      /tmp/zetasketch_clickhouse_function.xml /etc/clickhouse-server/
    clickhouse-client --query "SYSTEM RELOAD FUNCTIONS"
    clickhouse-client --query "
      SELECT hostName(), count() FROM system.functions
      WHERE name = '\''zetasketch_to_uniq_combined64_p15'\''"'
done
```

No restart, no coordination, no downtime: nodes without the function yet simply
cannot evaluate it, and nothing else changes. Install everywhere *before* the
first query that uses it.

### Kubernetes (clickhouse-operator)

The XML belongs in the operator's config files section; the binary needs a
volume, because a ConfigMap cannot carry an executable bit.

```yaml
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
spec:
  configuration:
    files:
      # Lands in /etc/clickhouse-server/ — note the name, it must match
      # the *_function.*ml glob, and must not be placed under config.d/.
      zetasketch_clickhouse_function.xml: |
        <functions>
          <function>
            <type>executable_pool</type>
            <name>zetasketch_to_uniq_combined64_p15</name>
            <return_type>AggregateFunction(uniqCombined64(15), UInt64)</return_type>
            <argument><type>String</type><name>sketch_bytes</name></argument>
            <format>RowBinary</format>
            <command>zetasketch-clickhouse --precision 15 --format row-binary</command>
            <pool_size>8</pool_size>
            <lifetime>0</lifetime>
            <send_chunk_header>true</send_chunk_header>
            <max_command_execution_time>30</max_command_execution_time>
          </function>
        </functions>
  templates:
    podTemplates:
      - name: clickhouse
        spec:
          initContainers:
            - name: install-udf
              # An image that contains the binary at /dist/zetasketch-clickhouse
              image: <your-registry>/zetasketch-clickhouse:0.1.0
              command:
                - sh
                - -c
                - |
                  install -m 0755 /dist/zetasketch-clickhouse \
                    /var/lib/clickhouse/user_scripts/zetasketch-clickhouse
              volumeMounts:
                - name: data-volumeclaim-template
                  mountPath: /var/lib/clickhouse
          containers:
            - name: clickhouse
              # unchanged
```

The init container writes into the same persistent volume the server uses, so
the binary survives restarts and is in place before the server starts. Rolling
the StatefulSet applies it pod by pod; the operator restarts pods anyway, so no
`SYSTEM RELOAD FUNCTIONS` is needed on that path.

A minimal image for the init container:

```dockerfile
FROM scratch
COPY target/aarch64-unknown-linux-musl/release/zetasketch-clickhouse /dist/zetasketch-clickhouse
```

Build it for the architecture your nodes run, and keep the tag pinned to a
version rather than `latest` so a pod reschedule cannot silently change the
converter under a half-finished backfill.

## 4. Verify

Per node, cheapest first:

```sql
-- registered at all?
SELECT name FROM system.functions WHERE name = 'zetasketch_to_uniq_combined64_p15';

-- the process actually starts and speaks the protocol
SELECT finalizeAggregation(
  zetasketch_to_uniq_combined64_p15(unhex('087010011802200882070b1001180f20143203b28716'))
);
-- expect exactly 1: that fixture is HLL_COUNT.INIT over a single value

-- a bad row must be absorbed, not fatal
SELECT finalizeAggregation(zetasketch_to_uniq_combined64_p15(unhex('deadbeef')));
-- expect 0
```

Across a cluster, ask every node at once:

```sql
SELECT
    hostName(),
    finalizeAggregation(
      zetasketch_to_uniq_combined64_p15(unhex('087010011802200882070b1001180f20143203b28716'))
    ) AS probe
FROM clusterAllReplicas('<your_cluster>', system.one);
```

Every row must read 1. A node returning 0 has the function but a broken
conversion path; a node erroring has not been installed. Both are covered in
[troubleshooting.md](troubleshooting.md).

## 5. Tuning

| setting | meaning | guidance |
|---|---|---|
| `pool_size` | concurrent UDF processes per node | start at 8; raise toward the core count if conversion is the bottleneck during a backfill, lower it if memory is tight — each process is ~1 MB plus buffers |
| `lifetime` | `0` keeps pooled processes alive indefinitely | keep `0`; process startup per chunk would dominate |
| `max_command_execution_time` | seconds before a chunk is abandoned | 30 is generous; conversion is ~microseconds per row, so hitting this means the process is wedged, not slow |
| `command_read_timeout` / `command_write_timeout` | per-IO timeouts, milliseconds | defaults are fine; raise only if you see timeouts under heavy load |

Throughput is dominated by the 24 783-byte state per row, not by the conversion
itself. If a backfill is slow, look at insert batching and network before
raising `pool_size`.

## 6. Upgrading and rolling back

The function's name encodes its precision, so a precision change is a *new*
function rather than a redefinition — deploy `..._p16` alongside `..._p15`,
migrate, then remove the old one. Existing stored states are unaffected either
way; they are read by `uniqCombined64Merge(15)`, not by this UDF.

To change the binary in place, overwrite it and run `SYSTEM RELOAD FUNCTIONS`.
Pooled processes are recycled, so in-flight queries finish against the old
binary and new ones start against the new. Roll back by reinstating the
previous binary the same way.

To remove the function entirely: delete the XML, run `SYSTEM RELOAD FUNCTIONS`,
then delete the binary. Columns already populated keep working — they hold
ordinary `uniqCombined64` states and have no dependency on this tool.
