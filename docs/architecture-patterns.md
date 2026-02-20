# Architecture patterns

This document covers practical considerations for where and how to use concurrency limiters in a distributed system.

## Server-side vs client-side limiting

Limiters can be used on the server side (protecting a service from incoming overload) or on the client side (preventing a client from overwhelming a downstream service). Both serve different purposes and work well together.

**Server-side limiting** protects the server itself. The limiter sits in front of request processing and rejects excess requests before they consume resources. Delay-based algorithms tend to work well here because the server measures its own processing latency — a direct, low-noise signal of internal load. This lets it act proactively, shedding load as queues grow but *before* requests start failing. (By the time a server observes loss signals, it's already failing its clients — too late to prevent the damage.) The server also sees all incoming traffic, giving it a complete picture of load rather than one client's partial view. And it has domain knowledge about its own workload — it knows which operations are expensive and which are cheap, and can apply different limiters accordingly (see [per-operation limiting](#per-operation-limiting)).

**Client-side limiting** prevents a client from overwhelming downstream services. Each client independently tracks the health of its downstream calls and backs off when it detects problems. Loss-based algorithms tend to be a better fit here: the client can't see the server's internal state, and the latency it measures includes network hops, load balancer overhead, and other sources of noise unrelated to server load. Failure signals — rejected requests (HTTP 429), timeouts — are clearer and less ambiguous. In a load-balanced setup, requests may also hit different server instances with different loads, adding further variance to latency.

When multiple clients share a downstream service, their limiters compete for capacity. This makes the *fairness* properties of the algorithm important. Loss-based algorithms like AIMD are fair — they converge to equal shares of capacity. However, loss-based algorithms can "muscle out" delay-based ones, since they keep pushing until they see failures while delay-based algorithms back off earlier. If mixing algorithm types across clients, be aware of this asymmetry.

Client-side limiters typically assume that a load balancer distributes requests across multiple server instances. The limiter doesn't need to know about individual servers — it treats the downstream service as a single entity.

## Push-based vs pull-based systems

The discussion above assumes a push-based model: clients send requests and servers receive them. The server can't control the arrival rate — it can only reject or accept what arrives. Backpressure has to be *communicated back* to the sender, via rejections, timeouts, or explicit signals like HTTP 429.

In a pull-based system — a message queue consumer, for example — the consumer fetches work when it's ready. Backpressure is inherent: if the consumer is busy, it simply doesn't pull the next message. The queue absorbs the difference, and no explicit signal is needed.

Concurrency limiting still has a role in pull-based systems, in two ways:

**Controlling the pull rate.** A consumer can use the concurrency limit to decide when to pull: if no tokens are available, don't fetch the next message. The limiter's rejection acts as a local backpressure signal, and the queue is the buffer. This is also where `RejectionDelay` is relevant — if the message broker has eager redelivery (e.g. RabbitMQ nacking a message causes immediate retry), the delay helps avoid a tight retry loop.

**Limiting downstream calls.** Even in a pull-based system, the consumer typically makes push-based calls to downstream services (databases, APIs, other services) as part of processing each message. Client-side limiting on those outbound calls applies exactly as described above.

## Partitioning and QoS

Not all traffic is equally important. A service might need to guarantee that user-facing requests get priority over batch jobs, even under load. Partitioning provides a form of quality of service (QoS): by reserving capacity for different traffic classes, you can ensure that high-priority work isn't starved by lower-priority work during contention.

For example, a service might allocate 75% of its capacity to user-facing requests and 25% to batch processing. A partitioned limiter divides a single overall concurrency limit into weighted partitions. Each partition has a guaranteed minimum share, but can borrow unused capacity from other partitions — so batch jobs can still use spare capacity when user traffic is light.

Partitioning requires a way to identify which class a request belongs to — typically via request headers or metadata. This identification must be trusted, since a client claiming to be "user-facing" when it's actually a batch job would undermine the allocation.

Static partitioning (fixed classes with known weights) is straightforward. Dynamic partitioning — for example, by customer ID with an unbounded set of values — is harder, and would probably require keeping the cardinality low by using a naturally limited identifier. For example, instead of customer IDs, put each customer into a "tier" (e.g. free, standard, premium) and partition by tier.

## Per-operation limiting

Consider a service with three operations that have very different latency characteristics:

1. Read resource by ID — fast (1-5ms)
2. Write new resource — medium (10-50ms)
3. Full text search — slow (50-500ms)

A single limiter for the whole service is simple and protects against overall overload. If all operations have similar latency, it works well. But with a wide range of latencies, a delay-based limiter may behave unpredictably — the "normal" latency that it uses as a baseline is a blend of very different operations.

Using separate limiters per operation gives more predictable behaviour, since each limiter sees a consistent latency profile. The challenge is that the separate limiters need to fairly share the service's total capacity — you wouldn't want the fast-read limiter to consume all available concurrency, leaving nothing for writes.

The congestion control algorithms are generally designed to be fair, so separate limiters competing for shared resources should converge to reasonable allocations. Alternatively, a partitioned limiter can provide explicit capacity guarantees for each operation type.

## Considerations for limiter placement

In a multi-service system, where should limiters go? There's a parallel here with the end-to-end principle for retries: doing something at every layer can cause multiplicative effects, while doing it only at the edges may miss important signals.

A reasonable starting point:

- **Every server protects itself** with a server-side limiter, shedding excess load before it causes internal overload.
- **Backpressure propagates upward** through rejection signals (HTTP 429, timeouts) from overloaded services.
- **Client-side limiting at the edges** — the topmost services (closest to the end user or traffic source) use client-side limiters to avoid sending traffic that will be rejected downstream.

This avoids the complexity of client-side limiters at every hop in a call chain, while still ensuring that every service can protect itself and that backpressure reaches the system's edges.

The right approach will depend on the specifics of your system — the depth of the call chain, how traffic is distributed, and where the bottlenecks tend to occur.
