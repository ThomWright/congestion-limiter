# CLAUDE.md

## Commands

```bash
# Run a single test
cargo test --lib path::to::module::test_name -- --nocapture

# Lint
cargo clippy --all-features -- --deny warnings
```

## Introduction

`congestion-limiter` is a dynamic concurrency limiter for Rust async code, inspired by TCP congestion control. It adjusts concurrency limits based on observed latency and failure rate to provide automatic backpressure.

A summary of the public API: acquire a `Token` via `Limiter::acquire()`, do work, call `token.set_outcome(Success | Overload)`. On release, the limit algorithm receives a `Sample` and computes a new limit, which propagates to the semaphore via `set_limit()`.

## Code conventions

- Always add `reason = "..."` to `#[allow(...)]` attributes
- Doc comments are encouraged, but should focus on describing behaviour and intent in plain English, not restating what the code does
- Tests frequently use `#[tokio::test(start_paused = true)]` with `tokio::time::advance()` + `tokio::task::yield_now()` for deterministic timing
