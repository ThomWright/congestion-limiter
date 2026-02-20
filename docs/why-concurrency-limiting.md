# Why concurrency limiting

## The overload problem

Services have finite resources: CPU cores, memory, database connections, network bandwidth. Under normal load, requests are processed quickly and queues stay short. But when the arrival rate exceeds what the service can handle, queues grow, latency increases, and requests start failing. If left unchecked, a service can reach a state where it spends more time managing its overloaded queues than doing useful work — throughput actually *decreases* as load increases.

The goal is to maximise *goodput*: the rate of successfully completed work. In an overloaded system, goodput can be far lower than throughput, because much of the work in progress will ultimately fail (via timeouts, for example). To maintain high goodput, a system needs to shed excess load — rejecting work it can't handle so it can focus on the work it can.

For this to be effective in a distributed system, load shedding needs to propagate. If a downstream service is overloaded and rejecting requests, upstream services need to detect this and stop sending traffic they know will fail. This propagation of "slow down" signals is called *backpressure*.

## Resources, queueing, and Little's Law

All systems have hard limits on the amount of concurrency they can support. The available concurrency is constrained by resources such as CPU, memory, disk and network bandwidth, thread pools and connection pools. For most of these (memory being one exception), when they become saturated, queues start to build up.

Little's Law relates three properties of a system in steady state (i.e. one where queues are not growing due to overload):

```text
L = λW
```

where:

- `L` = the average number of jobs in the system (concurrency)
- `λ` = the average arrival rate
- `W` = the average time a job spends in the system (latency)

For example, for requests to a server: `concurrency = requests per second × average latency`.

A single CPU core has a natural concurrency limit of 1 before jobs start queueing and latency increases. If jobs take 10ms on average, then one core can handle about 100 requests per second. Four cores could handle roughly 400 — but in practice, with shared resources, contention and I/O, the real limit is harder to predict.

The key insight is that concurrency, rate and latency are fundamentally linked. This has important implications for how we choose to limit traffic.

## Rate limiting vs concurrency limiting

Rate limiting — controlling the number of requests per second — is the most common form of traffic control. But rate is one-dimensional: it doesn't consider how long each request takes. Concurrency limiting — controlling how many requests are *in flight* at once — captures both rate and latency.

Think of it like a nightclub. Rate limiting is controlling how many people enter per minute. Concurrency limiting is controlling how many people are inside at once. If people start staying longer (drinks are slow, the queue for the toilets is long), a rate limit still lets the same number of people in per minute, and the club gets dangerously overcrowded. A concurrency limit naturally adapts: if people stay longer, fewer new people are admitted.

Here's a concrete example. Suppose load testing reveals that a service starts to struggle above about 100 requests per second. The expected average latency is 100ms, so the expected maximum concurrency is 10. Normal arrival rate is 80 RPS.

**With a rate limit of 100 RPS**: Imagine an unexpected situation causes latency to increase 10x, to 1 second — perhaps some expensive requests are being made, or capacity has been reduced. 80 RPS is still well within the rate limit, so all requests are admitted. But applying Little's Law: `L = 80 × 1 = 80` concurrent requests. The service is overwhelmed, and the rate limit did nothing to prevent it.

**With a concurrency limit of 10**: The same scenario plays out — latency spikes to 1 second. Now, with 80 arriving per second but only 10 allowed in flight, roughly 7 out of 8 requests are rejected. The service is protected. As load decreases and latency recovers, more requests are admitted again.

Concurrency limits respond to changes in both rate (more requests) and latency (more expensive requests, reduced capacity). Rate limits only respond to changes in rate.

## Static vs dynamic limits

Even if you accept that concurrency limiting is the right approach, a question remains: what should the limit be?

A static limit requires you to know the answer up front. This is hard enough for a single service with predictable workloads, and it's harder still in a distributed system where downstream capacity can change — deployments, autoscaling, partial outages, shifting traffic patterns. A limit that was right yesterday might be too low today (wasting capacity) or too high (not protecting the system).

A dynamic limit sidesteps this problem. Instead of choosing a number, you let the system discover it. By observing how the system behaves — is latency increasing? Are requests failing? — the limit can be automatically adjusted upward when there's headroom and downward when there's congestion. This is the approach taken by TCP congestion control, which has decades of research behind it.

## Why not just circuit breakers?

Circuit breakers are a common pattern for handling downstream failures: when a service detects that a downstream dependency is failing, it "trips" the circuit breaker and stops sending traffic entirely for a cooling-off period.

This works well for complete outages, but many overload situations are partial — a service is slow or dropping some requests, but could still handle *some* traffic. A circuit breaker's all-or-nothing response turns a partial outage into a complete one for that dependency:

```text
        availability = 0%         overloaded
          v      v                  v
Client -> API -> internal system -> database
                               ^
                      circuit breaker trips
```

Concurrency limiting provides a more proportional response. Instead of cutting off traffic entirely, it reduces traffic to a level the downstream system can handle. During a partial outage, overall availability can be much higher — the service continues to process requests up to the limit, rather than rejecting everything.
