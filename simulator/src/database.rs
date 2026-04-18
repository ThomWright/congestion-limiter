use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use rand::{prelude::Distribution, rngs::SmallRng};
use statrs::distribution::Erlang;

/// A database with a worker pool modelled as an M/M/c queue.
///
/// Latency increases as the number of in-flight requests approaches the worker count. Beyond that
/// point, requests queue and each additional batch of `c` requests adds another full service round
/// to the wait time.
///
/// Worker count can be changed mid-simulation via `set_workers` to simulate capacity changes (node
/// failure, scale-down, query degradation, etc.).
pub struct Database {
    /// Number of parallel workers (connection pool size, `c` in M/M/c).
    workers: AtomicUsize,
    /// Service time distribution per worker.
    pub base_latency: Erlang,
    in_flight: AtomicUsize,
    /// Scheduled worker-count changes: `(offset_from_start, new_worker_count)`.
    capacity_timeline: Vec<(Duration, usize)>,
}

impl Database {
    pub fn new(workers: usize, base_latency: Erlang) -> Self {
        Self {
            workers: AtomicUsize::new(workers),
            base_latency,
            in_flight: AtomicUsize::new(0),
            capacity_timeline: vec![],
        }
    }

    /// Attach a capacity timeline to this database.
    pub fn with_capacity_timeline(mut self, timeline: Vec<(Duration, usize)>) -> Self {
        self.capacity_timeline = timeline;
        self
    }

    pub fn workers(&self) -> usize {
        self.workers.load(Ordering::Acquire)
    }

    pub fn set_workers(&self, n: usize) {
        self.workers.store(n, Ordering::Release);
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
        let workers = self.workers.load(Ordering::Acquire).max(1);
        let rounds = ((n + 1) as f64 / workers as f64).ceil() as u32;
        service_time * rounds
    }

    /// Mark a request as complete, freeing one in-flight slot.
    pub fn finish_request(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use statrs::distribution::Erlang;

    #[test]
    fn set_workers_changes_latency() {
        let db = Database::new(4, Erlang::new(2, 100.0).unwrap());
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0);

        // With 4 workers, first 4 requests are round 1 (no queuing).
        db.start_request(&mut rng);
        db.start_request(&mut rng);
        db.start_request(&mut rng);
        let latency_round1 = db.start_request(&mut rng); // position 4, round 1

        // Drop to 1 worker; next request is position 5, round ceil(5/1)=5.
        db.set_workers(1);
        let latency_round5 = db.start_request(&mut rng);

        assert!(
            latency_round5 > latency_round1,
            "latency should increase when worker count drops"
        );
    }
}
