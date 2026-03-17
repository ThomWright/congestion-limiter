# Queue ordering strategies

## Problem

Under sustained overload, a FIFO queue accumulates requests that have already timed out on
the caller's side. These stale entries consume queue capacity and, when eventually served,
produce wasted work — the permit is granted but the caller is long gone. The result is that
latency increases for everyone and throughput falls despite capacity being available.

## Options

### FIFO (current)

Requests are served in arrival order. Simple and fair in the sense that no request jumps the
queue. Works well under light load. Under heavy load, old requests pile up and the effective
goodput drops as more and more served permits go to callers that have already timed out.

### LIFO

Requests are served in reverse arrival order. Under overload, this maximises the chance that
the served request is still alive on the caller's side. Old requests are implicitly shed —
they time out naturally while newer requests get through. Appropriate for latency-sensitive
workloads where a stale response has no value.

### Adaptive (FIFO → LIFO under pressure)

The queue switches from FIFO to LIFO when entries become stale, using entry **age** as the
signal rather than queue depth. Queue depth is a proxy for age but is noisy (a deep queue
in a high-throughput system is not the same as a deep queue in a slow one). Age directly
answers: "is this request still worth serving?"

A configurable `max_age: Duration` per pool sets the threshold. When the oldest entry in a
pool exceeds `max_age`, the pool switches to LIFO until the queue drains back below the
threshold.

## Per-pool configuration

Ordering should be configurable per pool, not globally. Pools represent quality-of-service
tiers, and the right strategy differs:

- **High-QoS pools → LIFO or adaptive.** High-QoS callers have tight timeout budgets. Under
  pressure, serving the freshest requests maximises the fraction that succeed before timing
  out. Shedding old requests is the right trade-off.
- **Low-QoS pools → FIFO.** Low-QoS workloads (e.g. batch jobs) tolerate longer waits. FIFO
  provides predictable ordering and avoids starvation of requests that have been waiting a
  long time.

## Cross-pool priority interaction

The cross-pool priority calculation (`weight × time_in_queue`) is computed from the **front**
of each pool's queue, regardless of that pool's ordering mode. This means:

- *Which pool wins the next permit* is still determined by weighted fairness.
- *Which entry within the winning pool is served* is determined by the pool's ordering mode.

This separation keeps cross-pool fairness intact without any changes to `dequeue_highest_priority`.

For LIFO pools, the front entry (used for priority scoring) will be older than the back entry
(the one actually served). This is acceptable: the pool still receives its fair share of
permits relative to other pools; the ordering choice is internal to the pool.

## Implementation sketch

```rust
enum QueueOrder {
    Fifo,
    Lifo,
    Adaptive { max_age: Duration },
}
```

In `Pool`: add `order: QueueOrder`.

In `dequeue_highest_priority`: after selecting the winning pool, dispatch on `pool.order`
to pop from front (FIFO) or back (LIFO/adaptive when stale).

For adaptive, check `pool.queue.front().map(|e| e.id.added.elapsed() > max_age)` to decide
which end to pop from.

## Open questions

- Should `max_age` default to the caller's `acquire_timeout` duration? That would make
  adaptive self-tuning, but requires plumbing the timeout into the pool.
- Is LIFO per-pool enough, or do we want LIFO as a tiebreak within `dequeue_highest_priority`
  (i.e. when two pools have equal priority, prefer the one with the newer entry)?
- The simulator will be useful for validating these choices — it can compare goodput and
  p99 latency across strategies under step-load and gradual-ramp scenarios.
