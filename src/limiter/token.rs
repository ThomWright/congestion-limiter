use std::{sync::Arc, time::Duration};

use tokio::{sync::OwnedSemaphorePermit, time::Instant};

use super::{CapacityUnit, Outcome, Releaser};

/// A concurrency token, required to run a job.
///
/// Release the token back to the [Limiter](crate::limiter::Limiter) after the job is complete.
#[must_use = "Call `limiter.set_outcome()` with this token once done."]
#[derive(Debug)]
pub struct Token {
    permit: Option<OwnedSemaphorePermit>,
    releaser: Arc<dyn Releaser + Sync + Send>,

    start: Instant,
    #[cfg(test)]
    latency: Duration,
}

impl Token {
    pub(crate) fn new(
        permit: OwnedSemaphorePermit,
        releaser: Arc<impl Releaser + Sync + Send + 'static>,
    ) -> Self {
        Self {
            permit: Some(permit),
            releaser,
            start: Instant::now(),
            #[cfg(test)]
            latency: Duration::ZERO,
        }
    }

    /// Set the outcome for this job.
    ///
    /// Will recalculate capacity and release this token back to the pool.
    pub async fn set_outcome(self, outcome: Outcome) -> CapacityUnit {
        self.releaser.update_limit(outcome, self.latency()).await
    }

    #[cfg(test)]
    pub(crate) fn set_latency(&mut self, latency: Duration) {
        use std::ops::Sub;

        use tokio::time::Instant;

        self.start = Instant::now().sub(latency);
        self.latency = latency;
    }

    #[cfg(test)]
    pub(crate) fn latency(&self) -> Duration {
        self.latency
    }

    #[cfg(not(test))]
    pub(crate) fn latency(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for Token {
    fn drop(&mut self) {
        self.releaser
            .release(self.permit.take().expect("permit should exist until drop"))
    }
}
