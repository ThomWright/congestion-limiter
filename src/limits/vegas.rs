use std::{
    fmt::Debug,
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use rand::Rng;

use crate::{limiter::Outcome, limits::defaults};

use super::{aimd::multiplicative_decrease, defaults::MIN_SAMPLE_LATENCY, LimitAlgorithm, Sample};

/// Loss- and delay-based congestion avoidance.
///
/// Additive increase, additive decrease. Multiplicative decrease when overload detected.
///
/// Estimates queuing delay by comparing the current latency with the minimum observed latency to
/// estimate the number of jobs being queued.
///
/// # When to use Vegas
///
/// Vegas uses the minimum observed latency as a proxy for "no-load" latency. This works well when
/// the no-load latency is stable — as in TCP, where the base RTT is essentially deterministic
/// (propagation delay over a fixed path).
///
/// Vegas is generally only suitable for services with a tight, stable latency distribution: a
/// single type of operation with low variance, such as cache reads or simple key-value lookups. In
/// these cases, the minimum observed latency is a reasonable proxy for the true no-load cost.
///
/// It works poorly for services with wide latency distributions — for example, those handling mixed
/// operation types or where individual operation cost varies significantly. With a wide
/// distribution, the minimum observed latency is just the lucky tail, not a meaningful floor.
/// Normal completions then look like they carry queuing delay, causing spurious limit decreases.
/// Over time, the baseline drifts far below the true no-load latency, and the concurrency limit
/// converges well below the server's actual capacity.
///
/// For typical backend services with variable latency, prefer [`Gradient`](super::Gradient), which
/// compares recent latency against a longer-term moving average and does not rely on a minimum
/// baseline.
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

    /// Controls how often the baseline latency is probed.
    ///
    /// A probe fires every `probe_jitter * probe_multiplier * limit` samples. On a probe, the
    /// baseline is reset to the current sample's latency and the limit is left unchanged. This
    /// prevents the all-time minimum from drifting arbitrarily low over long runs.
    probe_multiplier: usize,

    limit: AtomicUsize,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// The minimum observed latency, used as a baseline.
    ///
    /// This is the latency we would expect to see if there is no congestion.
    base_latency: Duration,

    /// Number of samples received since the last probe.
    samples_since_probe: usize,

    /// Jitter applied to the probe interval to desynchronise probes across independent clients.
    ///
    /// Random value in `[0.5, 1.0)`, resampled after each probe.
    probe_jitter: f64,
}

impl Vegas {
    const DEFAULT_ALPHA_MULTIPLIER: f64 = 3_f64;
    const DEFAULT_BETA_MULTIPLIER: f64 = 6_f64;

    /// Used when we see overload occurring.
    const DEFAULT_DECREASE_FACTOR: f64 = 0.9;

    /// Below this utilisation the limit is left unchanged in either direction.
    ///
    /// At low utilisation there is no useful signal: queueing estimates are noisy and there is no
    /// reason to adjust the limit. Only overload (explicit failure) still triggers a decrease.
    const DEFAULT_MIN_UTILISATION: f64 = 0.5;

    /// Default probe multiplier. A probe fires every `~probe_multiplier * limit` samples.
    const DEFAULT_PROBE_MULTIPLIER: usize = 30;

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

            probe_multiplier: Self::DEFAULT_PROBE_MULTIPLIER,

            inner: Mutex::new(Inner {
                base_latency: Duration::MAX,
                samples_since_probe: 0,
                probe_jitter: rand::thread_rng().gen_range(0.5..1.0),
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

        let mut inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let current_limit = self.limit.load(Ordering::Acquire);
        inner.samples_since_probe += 1;
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "jitter is 0 <= 1.0, so this won't lose precision, truncate, or change sign"
        )]
        let probe_interval =
            (inner.probe_jitter * self.probe_multiplier as f64 * current_limit as f64).round()
                as usize;
        if inner.samples_since_probe >= probe_interval {
            // Probe: reset the baseline to the current sample and leave the limit unchanged.
            // This prevents the all-time minimum from drifting arbitrarily low over long runs.
            inner.base_latency = sample.latency;
            inner.samples_since_probe = 0;
            inner.probe_jitter = rand::thread_rng().gen_range(0.5..1.0);
            return current_limit;
        }

        if sample.latency < inner.base_latency {
            // Record a baseline "no load" latency and keep the limit.
            inner.base_latency = sample.latency;
            return self.limit.load(Ordering::Acquire);
        }

        let update_limit = |limit: usize| {
            let actual_rate = sample.in_flight as f64 / sample.latency.as_secs_f64();

            let extra_latency = sample.latency.as_secs_f64() - inner.base_latency.as_secs_f64();

            let estimated_queued_jobs = actual_rate * extra_latency;

            let utilisation = sample.in_flight as f64 / limit as f64;

            let increment = limit.ilog10().max(1) as usize;

            let limit = if sample.outcome == Outcome::Overload {
                // Limit too big – overload
                multiplicative_decrease(limit, Self::DEFAULT_DECREASE_FACTOR)
            } else if utilisation < Self::DEFAULT_MIN_UTILISATION {
                // Low utilisation – signal is too noisy to act on; leave the limit alone
                limit
            } else if estimated_queued_jobs > (self.beta)(limit) {
                // Limit too big – too much queueing
                limit - increment
            } else if estimated_queued_jobs < (self.alpha)(limit) {
                // Limit too small – low queueing
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
    use std::{collections::VecDeque, time::Duration};

    use itertools::Itertools;

    use crate::limiter::{Limiter, Outcome};

    use super::*;

    #[tokio::test]
    async fn it_works() {
        static INIT_LIMIT: usize = 10;
        let vegas = Vegas::new_with_initial_limit(INIT_LIMIT);

        let limiter = Limiter::builder().limit_algo(vegas).build();

        /*
         * Warm up
         *
         * Concurrency = 5
         * Steady latency
         */
        let mut tokens = Vec::with_capacity(5);
        for _ in 0..5 {
            let token = limiter.try_acquire().unwrap();
            tokens.push(token);
        }
        for mut token in tokens {
            token.set_latency(Duration::from_millis(25));
            token.set_outcome(Outcome::Success).await;
        }

        /*
         * Concurrency = 9
         * Steady latency
         */
        let mut tokens = Vec::with_capacity(9);
        for _ in 0..9 {
            let token = limiter.try_acquire().unwrap();
            tokens.push(token);
        }
        for mut token in tokens {
            token.set_latency(Duration::from_millis(25));
            token.set_outcome(Outcome::Success).await;
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
            let mut token = limiter.try_acquire().unwrap();
            token.set_latency(Duration::from_millis(250));
            tokens.push(token);
        }
        for token in tokens {
            token.set_outcome(Outcome::Success).await;
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

        let limiter = Limiter::builder().limit_algo(vegas).build();

        let mut next_tokens = VecDeque::with_capacity(9);

        /*
         * Warm up
         *
         * Steady latency, keeping concurrency high
         */
        for _ in 0..9 {
            let token = limiter.try_acquire().unwrap();
            next_tokens.push_back(token);
        }

        let release_tokens = next_tokens.drain(0..).collect_vec();
        for mut token in release_tokens {
            token.set_latency(Duration::from_millis(25));
            token.set_outcome(Outcome::Success).await;

            let token = limiter.try_acquire().unwrap();
            next_tokens.push_back(token);
        }

        /*
         * Steady latency
         */
        let release_tokens = next_tokens.drain(0..).collect_vec();
        for mut token in release_tokens {
            token.set_latency(Duration::from_millis(25));
            token.set_outcome(Outcome::Success).await;

            let token = limiter.try_acquire().unwrap();
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
            token.set_outcome(Outcome::Success).await;

            let token = limiter.try_acquire().unwrap();
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
