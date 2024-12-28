use std::{
    fmt::Debug,
    ops::RangeInclusive,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{limiter::Outcome, limits::defaults};

use super::{aimd::multiplicative_decrease, defaults::MIN_SAMPLE_LATENCY, LimitAlgorithm, Sample};

/// Loss- and delay-based congestion avoidance.
///
/// Additive increase, additive decrease. Multiplicative decrease when overload detected.
///
/// Estimates queuing delay by comparing the current latency with the minimum observed latency to
/// estimate the number of jobs being queued.
///
/// For greater stability consider wrapping with a percentile window sampler. This calculates a
/// percentile (e.g. P90) over a period of time and provides that as a sample. Vegas then compares
/// recent P90 latency with the minimum observed P90. Used this way, Vegas can handle heterogeneous
/// workloads, as long as the percentile latency is fairly stable.
///
/// Can fairly distribute concurrency between independent clients as long as there is enough server
/// capacity to handle the requests. That is: as long as the server isn't overloaded and failing to
/// handle requests as a result.
///
/// Inspired by TCP Vegas.
///
/// - [TCP Vegas: End to End Congestion Avoidance on a Global
///   Internet](https://www.cs.princeton.edu/courses/archive/fall06/cos561/papers/vegas.pdf)
/// - [Understanding TCP Vegas: Theory and
///   Practice](https://www.cs.princeton.edu/research/techreps/TR-628-00)
/// - [A TCP Vegas Implementation for Linux](http://neal.nu/uw/linux-vegas/)
/// - [Linux kernel
///   implementation](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/net/ipv4/tcp_vegas.c)
pub struct Vegas {
    min_limit: usize,
    max_limit: usize,

    /// Lower queueing threshold, as a function of the current limit.
    alpha: Box<dyn (Fn(usize) -> f64) + Send + Sync>,
    /// Upper queueing threshold, as a function of the current limit.
    beta: Box<dyn (Fn(usize) -> f64) + Send + Sync>,

    limit: AtomicUsize,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// The minimum observed latency, used as a baseline.
    ///
    /// This is the latency we would expect to see if there is no congestion.
    base_latency: Duration,
}

impl Vegas {
    const DEFAULT_ALPHA_MULTIPLIER: f64 = 3_f64;
    const DEFAULT_BETA_MULTIPLIER: f64 = 6_f64;

    /// Used when we see overload occurring.
    const DEFAULT_DECREASE_FACTOR: f64 = 0.9;

    /// Utilisation needs to be above this to increase the limit.
    const DEFAULT_INCREASE_MIN_UTILISATION: f64 = 0.8;

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
            limit: AtomicUsize::new(initial_limit),
            min_limit: *limit_range.start(),
            max_limit: *limit_range.end(),

            alpha: Box::new(|limit| {
                Self::DEFAULT_ALPHA_MULTIPLIER * (limit as f64).log10().max(1_f64)
            }),
            beta: Box::new(|limit| {
                Self::DEFAULT_BETA_MULTIPLIER * (limit as f64).log10().max(1_f64)
            }),

            inner: Mutex::new(Inner {
                base_latency: Duration::MAX,
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
impl LimitAlgorithm for Vegas {
    fn limit(&self) -> usize {
        self.limit.load(Ordering::Acquire)
    }

    /// Vegas algorithm.
    ///
    /// Generally applied over a window size of one or two RTTs.
    ///
    /// Little's law: `L = λW = concurrency = rate * latency` (averages).
    ///
    /// The algorithm in terms of rates:
    ///
    /// ```text
    /// BASE_D = estimated base latency with no queueing
    /// D(w)   = observed average latency per job over window w
    /// L(w)   = concurrency limit for window w
    /// F(w)   = average jobs in flight during window w
    ///
    /// L(w) / BASE_D = E    = expected rate (no queueing)
    /// F(w) / D(w)   = A(w) = actual rate during window w
    ///
    /// E - A(w) = DIFF [>= 0]
    ///
    /// alpha = low rate threshold: too little queueing
    /// beta  = high rate threshold: too much queueing
    ///
    /// L(w+1) = L(w) + 1 if DIFF < alpha
    ///               - 1 if DIFF > beta
    /// ```
    ///
    /// Or, using queue size instead of rate:
    ///
    /// ```text
    /// D(w) - BASE_D = ΔD(w) = extra average latency in window w caused by queueing
    /// A(w) * ΔD(w)  = Q(w)  = estimated average queue size in window w
    ///
    /// alpha = low queueing threshold
    /// beta  = high queueing threshold
    ///
    /// L(w+1) = L(w) + 1 if Q(w) < alpha
    ///               - 1 if Q(w) > beta
    /// ```
    async fn update(&self, sample: Sample) -> usize {
        if sample.latency < MIN_SAMPLE_LATENCY {
            return self.limit.load(Ordering::Acquire);
        }

        let mut inner = self.inner.lock().await;

        if sample.latency < inner.base_latency {
            // Record a baseline "no load" latency and keep the limit.
            inner.base_latency = sample.latency;
            // return self.limit.load(Ordering::Acquire);
        }

        let update_limit = |limit: usize| {
            // TODO: periodically reset baseline latency measurement.

            let actual_rate = sample.in_flight as f64 / sample.latency.as_secs_f64();

            let extra_latency = sample.latency.as_secs_f64() - inner.base_latency.as_secs_f64();

            let estimated_queued_jobs = actual_rate * extra_latency;

            let utilisation = sample.in_flight as f64 / limit as f64;

            let increment = limit.ilog10().max(1) as usize;

            let limit = if sample.outcome == Outcome::Overload {
                // Limit too big – overload
                multiplicative_decrease(limit, Self::DEFAULT_DECREASE_FACTOR)
            } else if estimated_queued_jobs > (self.beta)(limit) {
                // Limit too big – too much queueing
                limit - increment
            } else if estimated_queued_jobs < (self.alpha)(limit)
                && utilisation >= Self::DEFAULT_INCREASE_MIN_UTILISATION
            {
                // Limit too small – low queueing + high utilisation

                // TODO: support some kind of fast start, e.g. increase by beta when almost no queueing
                limit + increment
            } else {
                // Perfect porridge
                limit
            };

            Some(limit.clamp(self.min_limit, self.max_limit))
        };

        self.limit
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, update_limit)
            .expect("we always return Some(limit)");

        self.limit.load(Ordering::SeqCst)
    }
}

impl Debug for Vegas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vegas")
            .field("limit", &self.limit)
            .field("min_limit", &self.min_limit)
            .field("max_limit", &self.max_limit)
            .field("alpha(1)", &(self.alpha)(1))
            .field("beta(1)", &(self.beta)(1))
            .field("inner", &self.inner)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc, time::Duration};

    use itertools::Itertools;

    use crate::limiter::{Limiter, Outcome};

    use super::*;

    #[tokio::test]
    async fn it_works() {
        static INIT_LIMIT: usize = 10;
        let vegas = Vegas::new_with_initial_limit(INIT_LIMIT);

        let limiter = Arc::new(Limiter::new(vegas));

        /*
         * Warm up
         *
         * Concurrency = 5
         * Steady latency
         */
        let mut tokens = Vec::with_capacity(5);
        for _ in 0..5 {
            let token = limiter.try_acquire().await.unwrap();
            tokens.push(token);
        }
        for mut token in tokens {
            token.set_latency(Duration::from_millis(25));
            token.release(Some(Outcome::Success)).await;
        }

        /*
         * Concurrency = 9
         * Steady latency
         */
        let mut tokens = Vec::with_capacity(9);
        for _ in 0..9 {
            let token = limiter.try_acquire().await.unwrap();
            tokens.push(token);
        }
        for mut token in tokens {
            token.set_latency(Duration::from_millis(25));
            token.release(Some(Outcome::Success)).await;
        }
        let higher_limit = limiter.state().limit();
        assert!(
            higher_limit > INIT_LIMIT,
            "Steady latency + high concurrency => increase limit"
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
            token.release(Some(Outcome::Success)).await;
        }
        assert!(
            limiter.state().limit() < higher_limit,
            "Increased latency => decrease limit"
        );
    }

    #[tokio::test]
    async fn windowed() {
        use crate::aggregation::Percentile;
        use crate::limits::Windowed;

        static INIT_LIMIT: usize = 10;
        let vegas = Windowed::new(
            Vegas::new_with_initial_limit(INIT_LIMIT),
            Percentile::default(),
        )
        .with_min_samples(3)
        .with_min_window(Duration::ZERO)
        .with_max_window(Duration::ZERO);

        let limiter = Arc::new(Limiter::new(vegas));

        let mut next_tokens = VecDeque::with_capacity(9);

        /*
         * Warm up
         *
         * Steady latency, keeping concurrency high
         */
        for _ in 0..9 {
            let token = limiter.try_acquire().await.unwrap();
            next_tokens.push_back(token);
        }

        let release_tokens = next_tokens.drain(0..).collect_vec();
        for mut token in release_tokens {
            token.set_latency(Duration::from_millis(25));
            token.release(Some(Outcome::Success)).await;

            let token = limiter.try_acquire().await.unwrap();
            next_tokens.push_back(token);
        }

        /*
         * Steady latency
         */
        let release_tokens = next_tokens.drain(0..).collect_vec();
        for mut token in release_tokens {
            token.set_latency(Duration::from_millis(25));
            token.release(Some(Outcome::Success)).await;

            let token = limiter.try_acquire().await.unwrap();
            next_tokens.push_back(token);
        }

        let higher_limit = limiter.state().limit();
        assert!(
            higher_limit > INIT_LIMIT,
            "Steady latency + high concurrency => increase limit. Limit: {}",
            higher_limit
        );

        /*
         * 40x previous latency
         */
        let release_tokens = next_tokens.drain(0..).collect_vec();
        for mut token in release_tokens {
            token.set_latency(Duration::from_millis(1000));
            token.release(Some(Outcome::Success)).await;

            let token = limiter.try_acquire().await.unwrap();
            next_tokens.push_back(token);
        }

        let lower_limit = limiter.state().limit();
        assert!(
            lower_limit < higher_limit,
            "Increased latency => decrease limit. Limit: {}",
            lower_limit
        );
    }
}
