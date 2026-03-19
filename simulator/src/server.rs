use std::{sync::Arc, time::Duration};

use rand::{prelude::Distribution, rngs::SmallRng, Rng};
use statrs::distribution::Erlang;

use congestion_limiter::limiter::{Limiter, LimiterState, Outcome, Token};

use crate::algo::LimitAlgo;

/// A time-varying failure rate for the server.
pub enum FailureRate {
    /// Constant failure probability in `[0, 1)`.
    Constant(f64),

    /// A sequence of `(duration, failure_rate)` phases.
    #[allow(dead_code, reason = "available for step-failure scenarios, not yet used")]
    Step(Vec<(Duration, f64)>),
}

impl FailureRate {
    /// Returns the failure probability at `elapsed` time into the simulation.
    pub fn at(&self, elapsed: Duration) -> f64 {
        match self {
            FailureRate::Constant(r) => *r,
            FailureRate::Step(phases) => {
                let mut remaining = elapsed;
                let last = phases.last().map(|(_, r)| *r).unwrap_or(0.0);
                for (duration, rate) in phases {
                    if remaining < *duration {
                        return *rate;
                    }
                    remaining -= *duration;
                }
                last
            }
        }
    }
}

/// A server that processes requests with a configurable latency and failure rate.
pub struct Server {
    pub latency: Erlang,
    pub failure_rate: FailureRate,
    pub limiter: Option<Arc<Limiter<LimitAlgo>>>,
}

impl Server {
    /// Try to accept a request.
    ///
    /// Returns `None` if the server's limiter rejected the request.
    /// Returns `Some((token, latency))` if accepted; `token` is `None` when there is no
    /// server-side limiter.
    pub fn try_accept(&self, rng: &mut SmallRng) -> Option<(Option<Token>, Duration)> {
        let latency = Duration::from_secs_f64(self.latency.sample(rng));
        match &self.limiter {
            Some(limiter) => limiter.try_acquire().map(|t| (Some(t), latency)),
            None => Some((None, latency)),
        }
    }

    /// Sample an outcome for a completed request given how far into the simulation we are.
    pub fn sample_outcome(&self, elapsed: Duration, rng: &mut SmallRng) -> Outcome {
        if rng.gen_range(0.0..1.0_f64) < self.failure_rate.at(elapsed) {
            Outcome::Overload
        } else {
            Outcome::Success
        }
    }

    /// Returns the current state of the server's limiter, if it has one.
    pub fn limiter_state(&self) -> Option<LimiterState> {
        self.limiter.as_ref().map(|l| l.state())
    }
}
