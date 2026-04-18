use std::{sync::Arc, time::Duration};

use rand::rngs::SmallRng;

use congestion_limiter::limiter::{Limiter, LimiterState, Token};

use crate::{algo::LimitAlgo, load::LoadPattern};

/// A client process that generates requests according to a load pattern.
pub struct Client {
    pub id: usize,
    pub load_pattern: LoadPattern,
    pub limiter: Option<Arc<Limiter<LimitAlgo>>>,
    /// When this client starts generating requests, measured from simulation start.
    pub active_from: Duration,
    /// When this client stops generating requests, measured from simulation start.
    /// `None` means active until the simulation ends.
    pub active_until: Option<Duration>,
}

impl Client {
    pub fn new(id: usize, load_pattern: LoadPattern, limiter: Option<Arc<Limiter<LimitAlgo>>>) -> Self {
        Self {
            id,
            load_pattern,
            limiter,
            active_from: Duration::ZERO,
            active_until: None,
        }
    }

    /// Set the time (from simulation start) at which this client begins generating requests.
    pub fn active_from(mut self, t: Duration) -> Self {
        self.active_from = t;
        self
    }

    /// Set the time (from simulation start) at which this client stops generating requests.
    pub fn active_until(mut self, t: Duration) -> Self {
        self.active_until = Some(t);
        self
    }

    /// Try to immediately acquire a concurrency token.
    ///
    /// Returns `None` if the limiter is full.
    pub fn try_acquire(&self) -> Option<Token> {
        self.limiter.as_ref().and_then(|l| l.try_acquire())
    }

    /// Returns the current state of the client's limiter, if it has one.
    pub fn limiter_state(&self) -> Option<LimiterState> {
        self.limiter.as_ref().map(|l| l.state())
    }

    /// Sample the next inter-arrival offset from `elapsed` into the simulation.
    pub fn next_arrival_offset(&self, elapsed: Duration, rng: &mut SmallRng) -> Duration {
        self.load_pattern.next_interarrival(elapsed, rng)
    }
}
