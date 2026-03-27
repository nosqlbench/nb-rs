# nbrs

A Rust-based performance testing engine. Part of the [nosqlbench](https://github.com/nosqlbench/nosqlbench) project.

## First Light

Create a workload file `hello.yaml`:

```yaml
bindings:
  id: Hash(); Mod(1000000)
  seq: Identity()

ops:
  write:
    ratio: 3
    stmt: "INSERT INTO users (id, name) VALUES ({id}, 'user_{seq}');"
  read:
    ratio: 1
    stmt: "SELECT * FROM users WHERE id={id};"
```

Run it:

```
$ nbrs run workload=hello.yaml cycles=8

nbrs: 2 ops selected, 8 cycles, 1 threads, driver=stdout
nbrs: stanza length=4, sequencer=Bucket
INSERT INTO users (id, name) VALUES (527897, 'user_0');
SELECT * FROM users WHERE id=460078;
INSERT INTO users (id, name) VALUES (564547, 'user_2');
INSERT INTO users (id, name) VALUES (960189, 'user_3');
SELECT * FROM users WHERE id=862456;
INSERT INTO users (id, name) VALUES (96332, 'user_5');
INSERT INTO users (id, name) VALUES (670075, 'user_6');
INSERT INTO users (id, name) VALUES (81455, 'user_7');
nbrs: done
```

Every `{id}` is a deterministic pseudo-random value derived from the cycle number. Same cycle, same output. The 3:1 write:read ratio is visible in the sequence.

## Build

```
cargo build --release
```

## Usage

```
nbrs run workload=<file.yaml> [parameters...]
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `workload=` | required | YAML workload file |
| `cycles=` | 1 | Cycles to execute (supports K, M, B) |
| `threads=` | 1 | Concurrency level |
| `rate=` | unlimited | Ops per second |
| `tags=` | all | Tag filter (e.g., `block:main`) |
| `format=` | stmt | Output: stmt, json, csv, assignments |
| `seq=` | bucket | Sequencer: bucket, interval, concat |

See [Getting Started](docs/guide/getting_started.md) for the full guide.

## Architecture

```
nb-variates     Generation kernel: deterministic data from coordinates
nb-metrics      HDR histograms, frame-based capture, reporters
nb-workload     YAML workload parsing and normalization
nb-rate         Async token bucket rate limiter
nb-errorhandler Modular composable error handling
nb-activity     Async execution engine
nb-rs           CLI binary (nbrs)
```

## License

Apache-2.0
