//! Moving averages.

use std::{collections::VecDeque, time::Duration};

use conv::ConvUtil;

/// An [exponential moving average](https://en.wikipedia.org/wiki/Exponential_smoothing).
#[derive(Debug)]
pub struct ExpSmoothed {
    /// Smoothing factor `α`. Weighting for the previous duration in the window.
    ///
    /// 0 < `a` < 1
    smoothing_factor: f64,

    value: Duration,

    // For initial warmup period
    initial_sum: Duration,
    initial_count: u16,
}

impl ExpSmoothed {
    /// > Exponential smoothing puts substantial weight on past observations, so the initial value
    /// > of demand will have an unreasonably large effect on early forecasts. This problem can be
    /// > overcome by allowing the process to evolve for a reasonable number of periods (10 or more)
    /// > and using the average of the demand during those periods as the initial forecast.
    /// >
    /// > [Source](https://en.wikipedia.org/wiki/Exponential_smoothing#Choosing_the_initial_smoothed_value)
    const INITIAL_WARMUP_SAMPLES: u16 = 10;

    pub fn new_with_window_size(k: u16) -> Self {
        Self {
            smoothing_factor: Self::smoothing_for_window(k),
            value: Duration::ZERO,
            initial_sum: Duration::ZERO,
            initial_count: 0,
        }
    }

    pub fn sample(&mut self, sample: Duration) -> Duration {
        if self.initial_count < Self::INITIAL_WARMUP_SAMPLES {
            self.initial_sum += sample;
            self.initial_count += 1;

            self.value = self.initial_sum / self.initial_count.into();
        } else if sample >= self.value {
            self.value = self.value + (sample - self.value).mul_f64(self.smoothing_factor);
        } else {
            self.value = self.value - (self.value - sample).mul_f64(self.smoothing_factor);
        }
        self.value
    }

    pub fn set(&mut self, value: Duration) {
        self.value = value;
    }

    fn smoothing_for_window(k: u16) -> f64 {
        assert!(k > 0, "window size must be > 0");
        assert!(k < u16::MAX, "window size mustn't overflow");

        2.0 / (f64::from(k + 1))
    }
}

/// A [simple moving average](https://en.wikipedia.org/wiki/Moving_average#Simple_moving_average).
#[derive(Debug)]
pub struct Simple {
    window_size: u16,

    values: VecDeque<Duration>,
    avg: Duration,
}

impl Simple {
    #[expect(dead_code)]
    pub fn new_with_window_size(window_size: u16) -> Self {
        assert!(window_size > 0, "window size must be > 0");
        Self {
            window_size,

            values: VecDeque::with_capacity(window_size.into()),
            avg: Duration::ZERO,
        }
    }

    #[expect(dead_code)]
    pub fn sample(&mut self, sample: Duration) -> Duration {
        let count = self
            .values
            .len()
            .approx_as::<u32>()
            .expect("length is constrained to u16");

        if count >= self.window_size.into() {
            let prev = self.values.pop_front().expect("should be non-empty");
            self.avg += (sample - prev) / count;
        } else {
            self.avg = (sample + (count * self.avg)) / (count + 1);
        };

        self.values.push_back(sample);

        self.avg
    }
}
