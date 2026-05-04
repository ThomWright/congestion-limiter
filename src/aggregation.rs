//! [Sample] aggregators.

use std::{collections::BTreeMap, fmt::Debug, time::Duration};

use conv::ConvUtil;

use crate::{limiter::Outcome, limits::Sample};

/// Aggregates multiple samples into one.
///
/// Additional samples can be added to update the aggregated sample. As such, the sample window can
/// be expanded, but only contracted again by resetting.
pub trait Aggregator {
    /// Add a sample to the aggregation.
    ///
    /// Returns the current aggregated sample.
    fn sample(&mut self, sample: Sample) -> Sample;

    #[allow(missing_docs)]
    fn sample_size(&self) -> usize;

    #[allow(missing_docs)]
    fn reset(&mut self);
}

/// Average latency and concurrency (in flight).
#[derive(Debug)]
pub struct Average {
    latency_sum: Duration,
    in_flight_sum: u128,
    overload: Outcome,
    samples: usize,
}

/// A latency percentile, with sample-matched concurrency (in flight).
pub struct Percentile {
    percentile: f64,
    overload: Outcome,
    num_samples: usize,
    samples: BTreeMap<Duration, Vec<Sample>>,
}

impl Aggregator for Average {
    fn sample(&mut self, sample: Sample) -> Sample {
        self.latency_sum += sample.latency;
        self.in_flight_sum += sample.in_flight as u128;
        self.overload = self.overload.overloaded_or(sample.outcome);
        self.samples += 1;
        Sample {
            in_flight: (self.in_flight_sum / self.samples as u128) as usize,
            #[allow(
                clippy::cast_precision_loss,
                reason = "sample count is window-bounded, well within f64 precision"
            )]
            latency: self.latency_sum.div_f64(self.samples as f64),
            outcome: self.overload,
        }
    }

    fn sample_size(&self) -> usize {
        self.samples
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

impl Default for Average {
    fn default() -> Self {
        Self {
            latency_sum: Duration::ZERO,
            in_flight_sum: 0,
            overload: Outcome::Success,
            samples: 0,
        }
    }
}

impl Percentile {
    #[allow(missing_docs)]
    /// # Panics
    ///
    /// Panics if `percentile` is not in the range `(0, 1)` exclusive.
    #[must_use]
    pub fn new(percentile: f64) -> Self {
        assert!(
            percentile > 0. && percentile < 1.,
            "percentiles must be between 0 and 1 exclusive"
        );
        Self {
            percentile,
            ..Default::default()
        }
    }

    fn percentile_sample(&self) -> Option<&Sample> {
        let index = self.percentile_index();

        index.and_then(|index| {
            self.samples
                .iter()
                .flat_map(|(_, sample)| sample)
                .nth(index)
        })
    }

    fn percentile_index(&self) -> Option<usize> {
        if self.num_samples == 0 {
            return None;
        }

        #[allow(
            clippy::cast_precision_loss,
            reason = "sample count is window-bounded, well within f64 precision"
        )]
        let float_index = self.num_samples as f64 * self.percentile;

        Some(
            float_index
                .ceil()
                .approx_as::<usize>()
                .expect("percentile should be < 1")
                - 1,
        )
    }
}

impl Aggregator for Percentile {
    fn sample(&mut self, sample: Sample) -> Sample {
        self.overload = self.overload.overloaded_or(sample.outcome);
        self.samples.entry(sample.latency).or_default().push(sample);
        self.num_samples += 1;

        let perc_sample = self
            .percentile_sample()
            .expect("Sample should exist at expected index");

        Sample {
            // TODO: what is best to do with the concurrency (in flight)?
            //
            // - max?
            // - percentile?
            // - match the sample of the latency percentile? <- Doing this one for now
            in_flight: perc_sample.in_flight,
            latency: perc_sample.latency,
            outcome: self.overload,
        }
    }

    fn sample_size(&self) -> usize {
        self.num_samples
    }

    fn reset(&mut self) {
        *self = Self {
            percentile: self.percentile,
            ..Default::default()
        };
    }
}

impl Default for Percentile {
    fn default() -> Self {
        Self {
            percentile: 0.5,
            samples: BTreeMap::new(),
            num_samples: 0,
            overload: Outcome::Success,
        }
    }
}

impl Debug for Percentile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Percentile")
            .field("percentile", &self.percentile)
            .field("overload", &self.overload)
            .field("num_samples", &self.num_samples)
            .field("samples", &self.samples)
            .field("(aggregated sample)", &self.percentile_sample())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn average() {
        let mut aggregator = Average::default();

        aggregator.sample(Sample {
            in_flight: 1,
            latency: Duration::from_millis(1),
            outcome: Outcome::Success,
        });

        aggregator.sample(Sample {
            in_flight: 5,
            latency: Duration::from_millis(3),
            outcome: Outcome::Overload,
        });

        let sample = aggregator.sample(Sample {
            in_flight: 3,
            latency: Duration::from_millis(5),
            outcome: Outcome::Success,
        });

        assert_eq!(
            sample,
            Sample {
                in_flight: 3,
                latency: Duration::from_millis(3),
                outcome: Outcome::Overload,
            }
        );
    }

    #[tokio::test]
    async fn average_reset() {
        let mut aggregator = Average::default();

        aggregator.sample(Sample {
            in_flight: 1,
            latency: Duration::from_millis(1),
            outcome: Outcome::Success,
        });

        aggregator.reset();

        let sample = aggregator.sample(Sample {
            in_flight: 3,
            latency: Duration::from_millis(5),
            outcome: Outcome::Success,
        });

        assert_eq!(
            sample,
            Sample {
                in_flight: 3,
                latency: Duration::from_millis(5),
                outcome: Outcome::Success,
            },
            "should be equal to new sample after reset"
        );
    }

    #[tokio::test]
    async fn percentile_p01() {
        let mut aggregator = Percentile::new(0.01);

        aggregator.sample(Sample {
            in_flight: 5,
            latency: Duration::from_millis(3),
            outcome: Outcome::Overload,
        });

        aggregator.sample(Sample {
            in_flight: 1,
            latency: Duration::from_millis(1),
            outcome: Outcome::Success,
        });

        let sample = aggregator.sample(Sample {
            in_flight: 3,
            latency: Duration::from_millis(5),
            outcome: Outcome::Success,
        });

        assert_eq!(
            sample,
            Sample {
                in_flight: 1,
                latency: Duration::from_millis(1),
                outcome: Outcome::Overload,
            }
        );
    }

    #[tokio::test]
    async fn percentile_p99() {
        let mut aggregator = Percentile::new(0.99);

        aggregator.sample(Sample {
            in_flight: 5,
            latency: Duration::from_millis(3),
            outcome: Outcome::Overload,
        });

        aggregator.sample(Sample {
            in_flight: 1,
            latency: Duration::from_millis(1),
            outcome: Outcome::Success,
        });

        let sample = aggregator.sample(Sample {
            in_flight: 3,
            latency: Duration::from_millis(5),
            outcome: Outcome::Success,
        });

        assert_eq!(
            sample,
            Sample {
                in_flight: 3,
                latency: Duration::from_millis(5),
                outcome: Outcome::Overload,
            }
        );
    }

    #[tokio::test]
    #[allow(clippy::float_cmp)]
    async fn percentile_reset() {
        let mut aggregator = Percentile::new(0.99);

        aggregator.sample(Sample {
            in_flight: 1,
            latency: Duration::from_millis(1),
            outcome: Outcome::Success,
        });

        aggregator.reset();

        let sample = aggregator.sample(Sample {
            in_flight: 3,
            latency: Duration::from_millis(5),
            outcome: Outcome::Success,
        });

        assert_eq!(
            sample,
            Sample {
                in_flight: 3,
                latency: Duration::from_millis(5),
                outcome: Outcome::Success,
            },
            "should be equal to new sample after reset"
        );

        assert_eq!(
            aggregator.percentile, 0.99,
            "percentile shouldn't change after reset"
        );
    }
}
