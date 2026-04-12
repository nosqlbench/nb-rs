# 60: CLI Structure

The nb-rs CLI provides workload execution, GK benchmarking,
diagnostic tools, and shell completions.

---

## Command Tree

```
nbrs
├── run           Execute a workload
│   adapter=<name> workload=<file.yaml> cycles=N concurrency=N
│   tags=<filter> rate=N format=<type>
│   op="<inline statement>"
│
├── bench         GK kernel micro-benchmark
│   <expr>        GK expression to benchmark
│   cycles=N concurrency=N --explain
│   <file.gk>    Benchmark a GK module file
│
├── web           Start/stop web UI
│   --daemon --stop --restart
│   bind=<addr> port=<port>
│
├── describe      Inspect workload/GK structure
│   workload <file.yaml>
│   gk stdlib
│
└── <file.yaml>   Bare file invocation → auto-detect run
```

### Bare File Invocation

```
nbrs myworkload.yaml tags=phase:rampup cycles=1000
```

Equivalent to `nbrs run workload=myworkload.yaml ...`. The CLI
detects `.yaml`/`.yml` extensions and routes to `run`.

---

## Parameter Passing

All parameters use `key=value` syntax:

```
cassnbrs run adapter=cql hosts=127.0.0.1 workload=cql_vector.yaml \
  tags=phase:search cycles=100 concurrency=100
```

No `--key value` form for workload/adapter params. Flags like
`--explain` and `--daemon` use standard flag syntax.

### Known Parameters

The runner validates all parameters at startup. Unrecognized
parameters produce a hard error:

```
error: unrecognized parameter(s): 'trhreads'. Check for typos.
```

Known parameter categories:
- **Activity**: `cycles`, `concurrency`, `rate`, `stanzarate`,
  `stanza_concurrency`, `sc`, `errors`, `seq`, `tags`
- **Workload**: `workload`, `op`, `format`, `filename`
- **Adapter selection**: `adapter`, `driver`
- **CQL**: `hosts`, `host`, `port`, `keyspace`, `consistency`,
  `username`, `password`, `request_timeout_ms`
- **HTTP**: `base_url`, `timeout`

Workload params (from `params:` section) are also accepted on
CLI and override YAML defaults.

---

## Completions

CLI completions use `veks-completion` as the single source of
truth. The command tree is defined once and generates completions
for bash, zsh, fish, and PowerShell.

```rust
fn cli_tree() -> Tree {
    Tree::new("nbrs")
        .command("run", Node::leaf_with_flags(
            &["adapter=", "workload=", "cycles=", "concurrency=", ...],
            &["--dry-run"],
        ))
        .command("bench", Node::leaf_with_flags(
            &["cycles=", "concurrency=", "--cycles", "--concurrency"],
            &["--explain"],
        ))
}
```

### Dynamic Completion

Workload params are discoverable: when the user has specified
`workload=file.yaml`, the completion engine parses the YAML
`params:` section and offers those param names as completions.

---

## --explain Mode

On the `bench` command, `--explain` dumps the GK compiler event
stream showing each compilation step:

```
$ nbrs bench --explain "mod(hash(cycle), 1000)" cycles=5
[parsed]    cycle → graph input #0
[parsed]    hash  → Hash64 node
[wired]     hash.input[0] ← input:cycle
[parsed]    mod   → ModU64(1000)
[wired]     mod.input[0] ← hash.output[0]
[fusion]    mod(hash(x), K) → hash_range(x, K)
[output]    hash_range → selected as program output
[compiled]  1 node (fused), 1 output, 0 constants folded

cycle=0: 723
cycle=1: 456
cycle=2: 891
cycle=3: 234
cycle=4: 567
```

---

## Input Summary

When running `bench`, the CLI prints a summary of all inputs:

```
1 input: cycle (u64)
3 bindings:
  user_id: u64 (cycle-time)
  dim: u64 (init-time, folded to 25)
  query: str (cycle-time)
```
