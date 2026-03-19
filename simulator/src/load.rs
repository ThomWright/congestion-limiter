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

    /// Sample the next inter-arrival duration given how far into the simulation we are.
    pub fn next_interarrival(&self, elapsed: Duration, rng: &mut SmallRng) -> Duration {
        let dist = match self {
            LoadPattern::Constant { dist } => dist,
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
                active
            }
        };

        Duration::from_secs_f64(dist.sample(rng))
    }
}
