//! Algorithms for controlling concurrency limits.

mod aimd;
mod defaults;
mod fixed;
mod gradient;
mod vegas;
mod windowed;

use async_trait::async_trait;
use std::{fmt::Debug, time::Duration};

use crate::limiter::Outcome;

pub use aimd::Aimd;
pub use fixed::Fixed;
pub use gradient::Gradient;
pub use vegas::Vegas;
pub use windowed::Windowed;

/// An algorithm for controlling a concurrency limit.
#[async_trait]
pub trait LimitAlgorithm: Debug + Sync {
    /// The current limit.
    fn limit(&self) -> usize;

    /// Update the concurrency limit in response to a new job completion.
    async fn update(&self, sample: Sample) -> usize;
}

/// The result of a job (or jobs), including the [Outcome] (loss) and latency (delay).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sample {
    pub(crate) latency: Duration,
    /// Jobs in flight when the sample was taken.
    pub(crate) in_flight: usize,
    pub(crate) outcome: Outcome,
}

#[cfg(test)]
pub(crate) mod mock {
    use std::sync::{
        atomic::{self, AtomicUsize},
        Arc, Mutex,
    };

    use super::{LimitAlgorithm, Sample};

    #[derive(Debug, Default)]
    pub struct MockLimitAlgorithm {
        limit: AtomicUsize,
        samples: Arc<Mutex<Vec<Sample>>>,
    }

    impl MockLimitAlgorithm {
        pub fn set_limit(self: &Arc<Self>, limit: usize) {
            self.limit.store(limit, atomic::Ordering::Release);
        }

        pub async fn samples(self: &Arc<Self>) -> Vec<Sample> {
            self.samples.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl LimitAlgorithm for Arc<MockLimitAlgorithm> {
        fn limit(&self) -> usize {
            self.limit.load(atomic::Ordering::Acquire)
        }

        async fn update(&self, sample: Sample) -> usize {
            self.samples.lock().unwrap().push(sample);
            self.limit.load(atomic::Ordering::Acquire)
        }
    }
}
