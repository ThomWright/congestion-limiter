# Formal Specifications

Quint models of the semaphore design, used to verify correctness properties.

- `single_pool.qnt` — Baseline single-pool semaphore model. Establishes the fundamental semaphore contract.
- `multi_pool.qnt` — Multi-pool semaphore with per-pool state, cross-pool borrowing, and home-pool return routing. Includes a safety refinement check proving that projected aggregate state satisfies the single-pool invariants.

## Running the simulator

Quick smoke test with random traces:

```sh
quint run --main=single_pool_test spec/single_pool.qnt --max-samples=1000 --max-steps=50
quint run --main=multi_pool_test spec/multi_pool.qnt --max-samples=1000 --max-steps=50
```

## Exhaustive verification with Apalache

Apalache is an SMT-based model checker that works directly with Quint. Requires Java 17+.

```sh
quint verify --main=single_pool_verify spec/single_pool.qnt
quint verify --main=multi_pool_verify spec/multi_pool.qnt
```

Note: Apalache uses bounded model checking, which can be slow for specs with large branching factors (e.g. `set_limit` with a wide nondeterministic range). For small state spaces, TLC (below) is significantly faster.

## Exhaustive verification with TLC

TLC does explicit-state BFS model checking — much faster than Apalache for small state spaces. Requires compiling Quint to TLA+ first.

### 1. Compile Quint to TLA+

```sh
quint compile --target tlaplus spec/single_pool.qnt > spec/single_pool.tla
quint compile --target tlaplus spec/multi_pool.qnt > spec/multi_pool.tla
```

### 2. Run TLC

```sh
cd spec
java -jar /path/to/tla2tools.jar single_pool.tla
java -jar /path/to/tla2tools.jar multi_pool_tlc.tla
```

`multi_pool_tlc.tla` is a thin wrapper that provides the `WEIGHTS` sequence constant, which can't be defined inline in TLC's `.cfg` format.

The compiled `.tla` files are gitignored. The hand-written TLC support files (`Apalache.tla`, `Variants.tla`, `multi_pool_tlc.tla`) and `.cfg` files are versioned.
