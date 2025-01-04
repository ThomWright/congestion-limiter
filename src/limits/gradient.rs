use std::{
    ops::RangeInclusive,
    sync::atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use conv::ConvAsUtil;
use tokio::sync::Mutex;

use crate::{
    limits::{defaults, Sample},
    moving_avg,
};

use super::{defaults::MIN_SAMPLE_LATENCY, LimitAlgorithm};

/// Delay-based congestion avoidance.
///
/// Additive-increase, multiplicative decrease based on change in average latency.
///
/// Considers the difference in average latency between a short time window and a longer window.
/// Changes in these values is considered an indicator of a change in load on the system.
///
/// Wrap with a [`crate::limits::windowed::Windowed`] to control the short time window, otherwise the latest
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
    const DEFAULT_INCREASE: f64 = 4.;
    const DEFAULT_INCREASE_MIN_UTILISATION: f64 = 0.8;
    const DEFAULT_INCREASE_MIN_GRADIENT: f64 = 0.9;

    const DEFAULT_LONG_WINDOW_SAMPLES: u16 = 500;

    const DEFAULT_TOLERANCE: f64 = 2.;
    const DEFAULT_SMOOTHING: f64 = 0.2;

    #[allow(missing_docs)]
    pub fn new_with_initial_limit(initial_limit: usize) -> Self {
        Self::new(
            initial_limit,
            defaults::DEFAULT_MIN_LIMIT..=defaults::DEFAULT_MAX_LIMIT,
        )
    }

    #[allow(missing_docs)]
    pub fn new(initial_limit: usize, limit_range: RangeInclusive<usize>) -> Self {
        assert!(*limit_range.start() >= 1, "Limits must be at least 1");
        assert!(
            initial_limit >= *limit_range.start(),
            "Initial limit less than minimum"
        );
        assert!(
            initial_limit <= *limit_range.end(),
            "Initial limit more than maximum"
        );

        Self {
            min_limit: *limit_range.start(),
            max_limit: *limit_range.end(),

            limit: AtomicUsize::new(initial_limit),
            inner: Mutex::new(Inner {
                long_window_latency: moving_avg::ExpSmoothed::new_with_window_size(
                    Self::DEFAULT_LONG_WINDOW_SAMPLES,
                ),
                limit: initial_limit as f64,
            }),
        }
    }

    #[allow(missing_docs)]
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

        let rounded_limit = new_limit
            .approx()
            .expect("should be clamped within usize bounds");
        self.limit.store(rounded_limit, Ordering::Release);

        rounded_limit
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::limiter::{Limiter, Outcome};

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
            token.set_outcome(Outcome::Success).await;
        }
        let higher_limit = limiter.state().limit();
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
            token.set_outcome(Outcome::Success).await;
        }
        assert!(
            limiter.state().limit() < higher_limit,
            "increased latency: decrease limit"
        );
    }
}
