# Roadmap

- [ ] Limit algorithms
  - [x] Loss-based
    - [x] AIMD
  - [x] Delay-based
    - [x] Gradient
      - [x] Time-based short window (e.g. min. 1s, min. 10 samples)
            _This way the limit won't be updated on every sample_
        - Just move the short window to the Windowed wrapper!
    - [x] Vegas
      - [ ] Probe min. latency
      - [ ] Support fast start
  - [x] Windowed wrapper
    - [x] Percentile sampler
    - [x] Average sampler
    - [ ] Configure window duration decision policy?
- [ ] Tests
  - [x] Vegas
- [ ] Simulator:
  - [ ] Topology
    - [ ] `Source` and `Sink` interfaces?
    - [ ] `LoadSource -> Option<ClientLimiter> -> Option<ServerLimiter> -> Server`?
    - [ ] `Server -> *Servers`?
  - [ ] LoadSource - cycle through behaviours, e.g. 100 RPS for 10 seconds, 0 RPS for 2 seconds
  - [ ] Results
    - [ ] Each node keep track of own metrics?
    - [ ] Graphs
  - [ ] Test fairness
- [ ] Limiter
  - [x] Rejection delay
    - Option to add delay before rejecting jobs. Intended to slow down clients, e.g. RabbitMQ retries.
  - [ ] Fractional limits – e.g. 0.5 allows 1 job every other RTT
  - [ ] Static partitioning
  - [ ] Dynamic partitioning?
    - How possible would it be to partition somewhat dynamically? E.g. on customer IDs?
  - [ ] LIFO for jobs waiting for a token
    - Optimise for latency
- [ ] Documentation
  - [ ] README
    - [ ] Examples
  - [ ] Rustdoc `#![warn(missing_docs)]`
    - [ ] Examples
  - [ ] Move most docs into Rust doc format to view in e.g. crates.io
