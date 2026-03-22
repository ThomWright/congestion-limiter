use std::time::Duration;

use rand::{prelude::Distribution, rngs::SmallRng};
use statrs::distribution::Exp;

/// A load pattern controlling how fast a client generates requests.
pub enum LoadPattern {
    /// Constant Poisson arrivals at `rps` requests per second.
    Constant { dist: Exp },

    /// A sequence of `(duration, rps)` constant phases.
    ///
    /// Arrivals within each phase are Poisson at that phase's rate.
    Step { phases: Vec<(Duration, Exp)> },

    /// A sequence of `(duration, target_rps)` waypoints.
    ///
    /// Within each segment the rate ramps linearly from the previous waypoint's target
    /// to this one's. The first segment starts from 1 rps. A zero-duration waypoint
    /// followed by a non-zero one produces an instantaneous step change.
    Segments { waypoints: Vec<(Duration, f64)> },
}

impl LoadPattern {
    /// Constant Poisson arrivals at `rps` requests per second.
    pub fn constant(rps: f64) -> Self {
        LoadPattern::Constant {
            dist: Exp::new(rps).expect("rps must be positive"),
        }
    }

    /// A sequence of `(duration, rps)` constant phases.
    pub fn step(phases: Vec<(Duration, f64)>) -> Self {
        LoadPattern::Step {
            phases: phases
                .into_iter()
                .map(|(d, rps)| (d, Exp::new(rps).expect("rps must be positive")))
                .collect(),
        }
    }

    /// A sequence of `(duration, target_rps)` waypoints.
    ///
    /// Each segment ramps from the previous waypoint's target to this one's.
    /// The first segment ramps from 1 rps. Equal consecutive targets hold the rate
    /// constant. A zero-duration waypoint immediately sets the rate for the next segment.
    pub fn segments(waypoints: Vec<(Duration, f64)>) -> Self {
        LoadPattern::Segments { waypoints }
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
            LoadPattern::Segments { waypoints } => {
                let mut remaining = elapsed;
                let mut prev_rps = 1.0_f64;
                // Default: hold at the last waypoint's target after all segments expire.
                let mut rps = waypoints.last().map(|&(_, r)| r).unwrap_or(1.0);
                for &(duration, target_rps) in waypoints {
                    if remaining < duration {
                        let progress = remaining.as_secs_f64() / duration.as_secs_f64();
                        rps = prev_rps + (target_rps - prev_rps) * progress;
                        break;
                    }
                    remaining -= duration;
                    prev_rps = target_rps;
                    rps = target_rps;
                }
                let dist = Exp::new(rps.max(1e-6)).expect("rps must be positive");
                Duration::from_secs_f64(dist.sample(rng))
            }
        }
    }
}
