# Simulator design

## Purpose

The simulator exercises the real `congestion-limiter` library under controlled synthetic load
and writes gnuplot-ready output. The core questions it is meant to answer:

- **Topology**: where should limiters be placed — client-side, server-side, or both?
  Which placement is most effective under overload? Which algorithms suit each position?
- **Algorithm comparison**: how do AIMD, Vegas, and Gradient behave under the same load?
- **Algorithmic fairness**: if N independent clients all run the same algorithm against the
  same server, do they converge to equal shares, or can one starve the others? Analogous to
  TCP's convergence property.
- **Weighted fairness**: do `PartitionedLimiter` partitions share capacity proportionally to
  their weights?
- **Load adaptation**: how quickly do limits adjust to step changes in load?

## Architecture

Event-driven loop, modelled on `../pool-sim`. The engine owns a min-heap event queue,
a tokio paused clock (for determinism), an RNG, and a metrics collector.
No concurrent tasks in v1 — events are processed one at a time.

**v1 constraint:** `try_acquire()` only — requests that cannot immediately acquire a permit
are rejected. Queuing and LIFO/FIFO comparison are deferred to v2.

## Events

- `Arrive { client_id }` — a new request from client `client_id`
- `Complete { ... }` — request processing is done; release tokens and record outcome
- `End` — simulation duration elapsed

## Load patterns

- `Constant { rps }` — Poisson arrivals at a fixed rate
- `Step { phases }` — sequence of `(duration, rps)` phases; inter-arrival times are
  exponentially distributed within each phase

## Clients and servers

Each `Client` has an id, a `LoadPattern`, and an optional `Arc<Limiter<...>>`.
The `Server` has a latency distribution (Erlang), a failure rate, and an optional limiter.
Scenarios wire these together; the topology (client limiter only, server limiter only, both)
is an explicit choice per scenario, making it easy to compare placement strategies.

## Metrics

Per-request records:
```
time_s  client_id  latency_s  outcome
```
`outcome`: `success` | `client_rejected` | `server_rejected` | `overload`

Limiter snapshots (taken at every event for every node with a limiter):
```
time_s  node  limit  in_flight  available
```
`node`: `client_0`, `client_1`, ..., `server`

## Output

Two space-separated `.dat` files written to `output/<scenario>/`:
- `requests.dat`
- `snapshots.dat`

A `plot.gnu` script (committed to `simulator/`) renders four panels:
1. Throughput over time (rolling 1 s window)
2. Rejection rate over time
3. Concurrency limit over time (per node)
4. In-flight over time (per node)

## CLI

```
cargo run --bin simulator -- <scenario> [--seed N] [--output-dir path]
```

Scenarios are named Rust functions in `main.rs`. Output defaults to `output/<scenario>/`.

## What this does not cover (v2+)

- Queuing: `acquire_timeout()` + per-request tokio tasks, for LIFO/FIFO comparison
- Config file / CLI arg scenario configuration
- Multi-scenario comparison charts in one run
- `waiting()` metric (queue depth) — deferred until queuing is simulated
