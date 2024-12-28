use std::{fmt::Debug, ops::RangeInclusive, time::Duration};

use async_trait::async_trait;
use tokio::{sync::Mutex, time::Instant};

use crate::aggregation::Aggregator;

use super::{defaults::MIN_SAMPLE_LATENCY, LimitAlgorithm, Sample};

/// A wrapper around a [LimitAlgorithm] which aggregates samples within a window, periodically
/// updating the limit.
///
/// The window duration is dynamic, based on latencies seen in the previous window.
///
/// Various [aggregators](crate::aggregation) are available to aggregate samples.
#[derive(Debug)]
pub struct Windowed<L, S> {
    window_bounds: RangeInclusive<Duration>,
    min_samples: usize,

    /// Samples below this threshold will be discarded and not contribute to the current window.
    ///
    /// Useful for discarding samples which are not representative of the system we're trying to
    /// observe. For example, if an error occurs locally on the client machine, it doesn't tell us
    /// anything about the state of the server we're trying to communicate with.
    min_latency_threshold: Duration,

    inner: L,

    window: Mutex<Window<S>>,
}

#[derive(Debug)]
struct Window<S> {
    start: Instant,
    duration: Duration,

    aggregator: S,
    /// The minimum latency observed in the current window.
    ///
    /// Used to determine the next window duration.
    min_latency: Duration,
}

impl<L: LimitAlgorithm, S: Aggregator> Windowed<L, S> {
    const DEFAULT_MIN_SAMPLES: usize = 10;

    #[allow(missing_docs)]
    pub fn new(inner: L, sampler: S) -> Self {
        let min_window = Duration::from_micros(1);
        Self {
            window_bounds: RangeInclusive::new(min_window, Duration::from_secs(1)),
            min_samples: Self::DEFAULT_MIN_SAMPLES,
            min_latency_threshold: MIN_SAMPLE_LATENCY,

            inner,

            window: Mutex::new(Window {
                duration: min_window,
                start: Instant::now(),

                aggregator: sampler,
                min_latency: Duration::MAX,
            }),
        }
    }

    /// At least this many samples need to be aggregated before updating the limit.
    pub fn with_min_samples(mut self, samples: usize) -> Self {
        assert!(samples > 0, "at least one sample required per window");
        self.min_samples = samples;
        self
    }

    /// Minimum time to wait before attempting to update the limit.
    pub fn with_min_window(mut self, min: Duration) -> Self {
        self.window_bounds = min..=*self.window_bounds.end();
        self
    }

    /// Maximum time to wait before attempting to update the limit.
    ///
    /// Will wait for longer if not enough samples have been aggregated. See
    /// [with_min_samples()](Self::with_min_samples()).
    pub fn with_max_window(mut self, max: Duration) -> Self {
        self.window_bounds = *self.window_bounds.start()..=max;
        self
    }
}

#[async_trait]
impl<L, S> LimitAlgorithm for Windowed<L, S>
where
    L: LimitAlgorithm + Send + Sync,
    S: Aggregator + Send + Sync + Debug,
{
    fn limit(&self) -> usize {
        self.inner.limit()
    }

    async fn update(&self, sample: Sample) -> usize {
        if sample.latency < self.min_latency_threshold {
            return self.inner.limit();
        }

        let mut window = self.window.lock().await;

        window.min_latency = window.min_latency.min(sample.latency);

        let agg_sample = window.aggregator.sample(sample);

        if window.aggregator.sample_size() >= self.min_samples
            && window.start.elapsed() >= window.duration
        {
            window.reset(&self.window_bounds);

            self.inner.update(agg_sample).await
        } else {
            self.inner.limit()
        }
    }
}

impl<S> Window<S>
where
    S: Aggregator,
{
    fn reset(&mut self, bounds: &RangeInclusive<Duration>) {
        self.min_latency = Duration::MAX;
        self.aggregator.reset();

        self.start = Instant::now();

        // Use a window duration of 2 * RTT (RTT ~= min latency).
        self.duration = self.min_latency.clamp(*bounds.start(), *bounds.end()) * 2;
    }
}

#[cfg(test)]
mod tests {
    use crate::{aggregation::Average, limiter::Outcome, limits::Vegas};

    use super::*;

    #[tokio::test]
    async fn it_works() {
        let samples = 2;

        // Just test with a min sample size for now
        let windowed_vegas = Windowed::new(Vegas::new_with_initial_limit(10), Average::default())
            .with_min_samples(samples)
            .with_min_window(Duration::ZERO)
            .with_max_window(Duration::ZERO);

        let mut limit = 0;

        for _ in 0..samples {
            limit = windowed_vegas
                .update(Sample {
                    in_flight: 1,
                    latency: Duration::from_millis(10),
                    outcome: Outcome::Success,
                })
                .await;
        }
        assert_eq!(limit, 10, "first window shouldn't change limit for Vegas");

        for _ in 0..samples {
            limit = windowed_vegas
                .update(Sample {
                    in_flight: 1,
                    latency: Duration::from_millis(100),
                    outcome: Outcome::Overload,
                })
                .await;
        }
        assert!(limit < 10, "limit should be reduced");
    }
}
