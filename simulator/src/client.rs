use std::{sync::Arc, time::Duration};

use rand::rngs::SmallRng;

use congestion_limiter::limiter::{Limiter, LimiterState, Token};

use crate::{algo::LimitAlgo, load::LoadPattern};

/// A client process that generates requests according to a load pattern.
pub struct Client {
    pub id: usize,
    pub load_pattern: LoadPattern,
    pub limiter: Option<Arc<Limiter<LimitAlgo>>>,
}

impl Client {
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
