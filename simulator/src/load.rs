use std::time::Duration;

use rand::{prelude::Distribution, rngs::SmallRng};
use statrs::distribution::Exp;

/// A load pattern controlling how fast a client generates requests.
pub enum LoadPattern {
    /// Constant Poisson arrivals at `rps` requests per second.
    Constant { dist: Exp },

    /// A sequence of `(duration, rps)` phases.
    ///
    /// Arrivals within each phase are Poisson at that phase's rate.
    Step { phases: Vec<(Duration, Exp)> },

    /// Linearly ramps from `start_rps` to `end_rps` over `ramp_duration`, then holds at `end_rps`.
    Ramp {
        start_rps: f64,
        end_rps: f64,
        ramp_duration: Duration,
    },
}

impl LoadPattern {
    /// Constant Poisson arrivals at `rps` requests per second.
    pub fn constant(rps: f64) -> Self {
        LoadPattern::Constant {
            dist: Exp::new(rps).expect("rps must be positive"),
        }
    }

    /// A sequence of `(duration, rps)` phases.
    pub fn step(phases: Vec<(Duration, f64)>) -> Self {
        LoadPattern::Step {
            phases: phases
                .into_iter()
                .map(|(d, rps)| (d, Exp::new(rps).expect("rps must be positive")))
                .collect(),
        }
    }

    /// Linearly ramps from `start_rps` to `end_rps` over `ramp_duration`, then holds at `end_rps`.
    pub fn ramp(start_rps: f64, end_rps: f64, ramp_duration: Duration) -> Self {
        LoadPattern::Ramp { start_rps, end_rps, ramp_duration }
    }

    /// Sample the next inter-arrival duration given how far into the simulation we are.
    pub fn next_interarrival(&self, elapsed: Duration, rng: &mut SmallRng) -> Duration {
        match self {
            LoadPattern::Constant { dist } => Duration::from_secs_f64(dist.sample(rng)),
            LoadPattern::Step { phases } => {
                let mut remaining = elapsed;
                let last = &phases.last().expect("phases must not be empty").1;
                let mut active = last;
                for (duration, dist) in phases {
                    if remaining < *duration {
                        active = dist;
                        break;
                    }
                    remaining -= *duration;
                }
                Duration::from_secs_f64(active.sample(rng))
            }
            LoadPattern::Ramp { start_rps, end_rps, ramp_duration } => {
                let clamped = elapsed.min(*ramp_duration);
                let progress = clamped.as_secs_f64() / ramp_duration.as_secs_f64();
                let rps = start_rps + (end_rps - start_rps) * progress;
                // Avoid division by zero if ramp starts at 0 rps.
                let rps = rps.max(1e-6);
                let dist = Exp::new(rps).expect("rps must be positive");
                Duration::from_secs_f64(dist.sample(rng))
            }
        }
    }
}
