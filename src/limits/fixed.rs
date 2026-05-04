use async_trait::async_trait;

use super::{LimitAlgorithm, Sample};

/// A simple, fixed concurrency limit.
#[derive(Debug)]
pub struct Fixed(usize);
impl Fixed {
    #[allow(missing_docs)]
    /// # Panics
    ///
    /// Panics if `limit` is 0.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        assert!(limit > 0);

        Self(limit)
    }
}

#[async_trait]
impl LimitAlgorithm for Fixed {
    fn limit(&self) -> usize {
        self.0
    }

    async fn update(&self, _reading: Sample) -> usize {
        self.0
    }
}
