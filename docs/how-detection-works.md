# How detection works

For a dynamic concurrency limit to work, it needs a way to detect when a system is approaching or exceeding its capacity. This library takes inspiration from TCP congestion control, which has been solving essentially the same problem for decades: how fast can I send data without overwhelming the network?

## Observing symptoms, not causes

The underlying causes of overload are resource bottlenecks: CPU saturation, memory pressure, exhausted connection pools, network bandwidth limits. In a complex distributed system, it's hard to predict which resource will be the bottleneck, or what effect that bottleneck will have on overall system behaviour.

Instead of trying to monitor specific resources, this library observes *symptoms* of overload: increasing latency and job failures. This is the same philosophy as [alerting on symptoms, not causes](https://docs.google.com/document/d/199PqyG3UsyXlwieHaqbGiWVa8eMWi8zzAn0YfcApr8Q/edit) — you don't need to know *why* the system is struggling, only that it *is*.

These symptoms fall into two categories, named after their TCP equivalents:

- **Delay** — increased latency (analogous to increased round-trip time in TCP)
- **Loss** — job failures caused by overload (analogous to packet loss in TCP)

## Delay-based detection

Delay-based algorithms monitor job latency to detect congestion. The idea is straightforward: when a system approaches its capacity, queues start to form, and latency increases. Rising latency is an early warning sign — it indicates that the system is congested, even if requests are still completing successfully.

This makes delay-based detection *proactive*. It can reduce the concurrency limit before the system is actually overloaded, preventing failures rather than reacting to them.

The trade-off is sensitivity to noise. Not all latency increases are caused by congestion — garbage collection pauses, network jitter, or a single slow request can temporarily inflate latency measurements. This can cause the limit to decrease unnecessarily. Windowing (aggregating samples over a time period before updating the limit) helps smooth out short-lived spikes.

The [Gradient](../src/limit/gradient.rs) algorithm compares short-term and long-term average latency to detect trends. [Vegas](../src/limit/vegas.rs) takes a different approach, tracking the minimum observed latency as a baseline and estimating how many jobs are queueing based on the difference between actual and expected latency.

## Loss-based detection

Loss-based algorithms react to job failures that are caused by overload. When a downstream system is overwhelmed, it may start rejecting requests or timing out. These failures are the signal that the concurrency limit needs to decrease.

This makes loss-based detection *reactive* — it responds after overload has already occurred, rather than anticipating it. The limit decreases in response to failures, and increases again when failures stop.

The strength of this approach is robustness: a failure is a clear signal, less ambiguous than a latency change. But it requires a reliable way to distinguish load-based failures from other kinds of failures. If the algorithm reduces concurrency in response to errors that aren't caused by overload (e.g. a bug, a bad request, or a network partition), it can make availability worse by unnecessarily restricting traffic.

These failure signals — which also serve as backpressure signals to upstream systems — can be either:

- **Explicit** — the downstream system tells you it's overloaded (e.g. HTTP 429 Too Many Requests, HTTP 503 Service Unavailable, gRPC `RESOURCE_EXHAUSTED`)
- **Implicit** — you infer overload from symptoms (e.g. request timeouts, connection refusals)

Explicit signals are more reliable but require the downstream system to implement them. Implicit signals work without downstream cooperation but are more ambiguous — a timeout could be caused by overload, or by something unrelated like a firewall silently dropping packets. In practice, a combination often works best: explicit signals provide a clear backpressure mechanism, while implicit signals like rising latency provide earlier, more gradual detection.

The [AIMD](../src/limit/aimd.rs) algorithm uses loss-based detection with additive increase and multiplicative decrease. [Vegas](../src/limit/vegas.rs) combines both loss and delay detection — it uses delay to make fine-grained adjustments and loss as a stronger signal for more aggressive reduction.

<!-- TODO: explore push-based vs pull-based systems and how backpressure differs between them (e.g. HTTP request/response vs message queue consumption) -->
