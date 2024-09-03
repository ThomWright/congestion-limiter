use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{limits::Sample, moving_avg};

use super::{defaults::MIN_SAMPLE_LATENCY, LimitAlgorithm};

/// Delay-based congestion avoidance.
///
/// Additive-increase, multiplicative decrease based on change in average latency.
///
/// Considers the difference in average latency between a short time window and a longer window.
/// Changes in these values is considered an indicator of a change in load on the system.
///
/// Wrap with a [`crate::windowed::Windowed`] to control the short time window, otherwise the latest
/// sample is used.
///
/// Inspired by TCP congestion control algorithms using delay gradients.
///
/// - [Revisiting TCP Congestion Control Using Delay Gradients](https://hal.science/hal-01597987/)
#[derive(Debug)]
pub struct Gradient {
    min_limit: usize,
    max_limit: usize,

    limit: AtomicUsize,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    long_window_latency: moving_avg::ExpSmoothed,
    limit: f64,
}

impl Gradient {
    const DEFAULT_MIN_LIMIT: usize = 1;
    const DEFAULT_MAX_LIMIT: usize = 1000;

    const DEFAULT_INCREASE: f64 = 4.;
    const DEFAULT_INCREASE_MIN_UTILISATION: f64 = 0.8;
    const DEFAULT_INCREASE_MIN_GRADIENT: f64 = 0.9;

    const DEFAULT_LONG_WINDOW_SAMPLES: u16 = 500;

    const DEFAULT_TOLERANCE: f64 = 2.;
    const DEFAULT_SMOOTHING: f64 = 0.2;

    pub fn new_with_initial_limit(initial_limit: usize) -> Self {
        assert!(initial_limit > 0);

        Self {
            min_limit: Self::DEFAULT_MIN_LIMIT,
            max_limit: Self::DEFAULT_MAX_LIMIT,

            limit: AtomicUsize::new(initial_limit),
            inner: Mutex::new(Inner {
                long_window_latency: moving_avg::ExpSmoothed::new_with_window_size(
                    Self::DEFAULT_LONG_WINDOW_SAMPLES,
                ),
                limit: initial_limit as f64,
            }),
        }
    }

    pub fn with_max_limit(self, max: usize) -> Self {
        assert!(max > 0);
        Self {
            max_limit: max,
            ..self
        }
    }
}

#[async_trait]
impl LimitAlgorithm for Gradient {
    fn limit(&self) -> usize {
        self.limit.load(Ordering::Acquire)
    }

    async fn update(&self, sample: Sample) -> usize {
        // FIXME: Improve or justify safety of numerical conversions
        if sample.latency < MIN_SAMPLE_LATENCY {
            return self.limit.load(Ordering::Acquire);
        }

        let mut inner = self.inner.lock().await;

        // Update long window
        let long = inner.long_window_latency.sample(sample.latency);

        let ratio = long.as_secs_f64() / sample.latency.as_secs_f64();

        // Speed up return to baseline after long period of increased load.
        if ratio > 2.0 {
            inner.long_window_latency.set(long.mul_f64(0.95));
        }

        let old_limit = inner.limit;

        // Only apply downwards gradient (when latency has increased).
        // Limit to >= 0.5 to prevent aggressive load shedding.
        // Tolerate a given amount of latency difference.
        let gradient = (Self::DEFAULT_TOLERANCE * ratio).clamp(0.5, 1.0);

        let utilisation = sample.in_flight as f64 / old_limit;

        // Only apply an increase if we're using enough to justify it
        // and we're not trying to reduce the limit by much.
        let increase = if utilisation > Self::DEFAULT_INCREASE_MIN_UTILISATION
            && gradient > Self::DEFAULT_INCREASE_MIN_GRADIENT
        {
            Self::DEFAULT_INCREASE
        } else {
            0.0
        };

        // Apply gradient, and allow an additive increase.
        let mut new_limit = old_limit * gradient + increase;
        new_limit =
            old_limit * (1.0 - Self::DEFAULT_SMOOTHING) + new_limit * Self::DEFAULT_SMOOTHING;

        new_limit = (new_limit).clamp(self.min_limit as f64, self.max_limit as f64);

        inner.limit = new_limit;
        let rounded_limit = new_limit as usize;
        self.limit.store(rounded_limit, Ordering::Release);

        rounded_limit
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{Limiter, Outcome};

    use super::*;

    #[tokio::test]
    async fn it_works() {
        static INIT_LIMIT: usize = 10;
        let gradient = Gradient::new_with_initial_limit(INIT_LIMIT);

        let limiter = Limiter::new(gradient);

        /*
         * Concurrency = 10
         * Steady latency
         */
        let mut tokens = Vec::with_capacity(10);
        for _ in 0..10 {
            let token = limiter.try_acquire().await.unwrap();
            tokens.push(token);
        }
        for mut token in tokens {
            token.set_latency(Duration::from_millis(25));
            limiter.release(token, Some(Outcome::Success)).await;
        }
        let higher_limit = limiter.limit();
        assert!(
            higher_limit > INIT_LIMIT,
            "steady latency + high concurrency: increase limit"
        );

        /*
         * Concurrency = 10
         * 10x previous latency
         */
        let mut tokens = Vec::with_capacity(10);
        for _ in 0..10 {
            let mut token = limiter.try_acquire().await.unwrap();
            token.set_latency(Duration::from_millis(250));
            tokens.push(token);
        }
        for token in tokens {
            limiter.release(token, Some(Outcome::Success)).await;
        }
        assert!(
            limiter.limit() < higher_limit,
            "increased latency: decrease limit"
        );
    }
}
