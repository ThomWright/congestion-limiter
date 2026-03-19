use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use rand::{prelude::Distribution, rngs::SmallRng};
use statrs::distribution::Erlang;

/// A database with a fixed worker pool, modelled as an M/M/c queue.
///
/// Latency increases as the number of in-flight requests approaches the worker count.
/// Beyond that point, requests queue and each additional batch of `c` requests adds
/// another full service round to the wait time.
pub struct Database {
    /// Number of parallel workers (connection pool size, `c` in M/M/c).
    pub workers: usize,
    /// Service time distribution per worker.
    pub base_latency: Erlang,
    in_flight: AtomicUsize,
}

impl Database {
    pub fn new(workers: usize, base_latency: Erlang) -> Self {
        Self {
            workers,
            base_latency,
            in_flight: AtomicUsize::new(0),
        }
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Register a new request and sample its total latency (queue wait + service time).
    ///
    /// With `c` workers, the request at position `n+1` (1-indexed) enters service in
    /// round `⌈(n+1)/c⌉`. Each round takes one full service time, so total latency is
    /// `service_time × ⌈(n+1)/c⌉`.
    pub fn start_request(&self, rng: &mut SmallRng) -> Duration {
        let n = self.in_flight.fetch_add(1, Ordering::AcqRel);
        let service_time = Duration::from_secs_f64(self.base_latency.sample(rng));
        let rounds = ((n + 1) as f64 / self.workers as f64).ceil() as u32;
        service_time * rounds
    }

    /// Mark a request as complete, freeing one in-flight slot.
    pub fn finish_request(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}
