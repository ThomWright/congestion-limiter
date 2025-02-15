//! Limiters, including various wrappers.

use std::{
    fmt::Debug,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use tokio::sync::OwnedSemaphorePermit;

use crate::limits::LimitAlgorithm;
use total::TotalLimiter;

pub use partitioning::PartitionedLimiter;
pub use rejection_delay::RejectionDelay;
pub use token::Token;

mod partitioning;
mod rejection_delay;
mod token;
mod total;

type CapacityUnit = usize;
type CapacityUnitNeg = isize;
type AtomicCapacityUnit = AtomicUsize;

/// Limits the number of concurrent jobs.
///
/// Concurrency is limited through the use of [Token]s. Acquire a token to run a job, and release the
/// token once the job is finished.
///
/// The limit will be automatically adjusted based on observed latency (delay) and/or failures
/// caused by overload (loss).
///
/// Cheaply cloneable.
#[derive(Debug)]
pub struct Limiter<T> {
    total_limiter: TotalLimiter<T>,
}

/// A snapshot of the state of the [Limiter].
///
/// Not guaranteed to be consistent under high concurrency.
#[derive(Debug, Clone, Copy)]
pub struct LimiterState {
    limit: CapacityUnit,
    available: CapacityUnit,
    in_flight: CapacityUnit,
}

/// Whether a job succeeded or failed as a result of congestion/overload.
///
/// Errors not considered to be caused by overload should be ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The job succeeded, or failed in a way unrelated to overload.
    Success,
    /// The job failed because of overload, e.g. it timed out or an explicit backpressure signal
    /// was observed.
    Overload,
}

#[async_trait::async_trait]
pub(crate) trait Releaser: Debug {
    /// The [Outcome] of the job, and the time taken to perform it, may be used
    /// to update the concurrency limit.
    ///
    /// Set the outcome to `None` to ignore the job.
    ///
    /// Returns the new limit.
    /// TODO: do we need to return the new limit?
    async fn update_limit(&self, outcome: Outcome, latency: Duration) -> CapacityUnit;

    fn release(&self, permit: OwnedSemaphorePermit);
}

impl<T> Limiter<T>
where
    T: LimitAlgorithm + Send + 'static,
{
    /// Create a limiter with a given limit control algorithm.
    pub fn new(limit_algo: T) -> Arc<Self> {
        Arc::new(Self {
            total_limiter: TotalLimiter::new(limit_algo),
        })
    }

    /// In some cases [Token]s are acquired asynchronously when updating the limit.
    #[cfg(test)]
    pub fn new_with_release_notifier(limit_algo: T, n: Arc<tokio::sync::Notify>) -> Arc<Self> {
        let mut limiter = TotalLimiter::new(limit_algo);
        limiter.with_release_notifier(n);
        Arc::new(Self {
            total_limiter: limiter,
        })
    }

    /// The current state of the limiter.
    pub fn state(&self) -> LimiterState {
        LimiterState {
            limit: self.total_limiter.limit(),
            available: self.total_limiter.available(),
            in_flight: self.total_limiter.in_flight(),
        }
    }

    /// Try to immediately acquire a concurrency [Token].
    ///
    /// Returns `None` if there are none available.
    pub fn try_acquire(self: &Arc<Self>) -> Option<Token> {
        self.total_limiter
            .try_acquire()
            .map(|permit| self.mint_token(permit))
    }

    /// Try to acquire a concurrency [Token], waiting for `duration` if there are none available.
    ///
    /// Returns `None` if there are none available after `duration`.
    pub async fn acquire_timeout(self: &Arc<Self>, duration: Duration) -> Option<Token> {
        self.total_limiter
            .acquire_timeout(duration)
            .await
            .map(|permit| self.mint_token(permit))
    }

    fn mint_token(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> Token {
        Token::new(permit, Arc::clone(self))
    }
}

#[async_trait::async_trait]
impl<T: LimitAlgorithm + Debug + Sync> Releaser for Limiter<T> {
    async fn update_limit(&self, outcome: Outcome, latency: Duration) -> CapacityUnit {
        self.total_limiter.update_limit(outcome, latency).await
    }

    fn release(&self, permit: OwnedSemaphorePermit) {
        self.total_limiter.release(permit);
    }
}

impl LimiterState {
    /// The current concurrency limit.
    pub fn limit(&self) -> CapacityUnit {
        self.limit
    }
    /// The amount of concurrency available to use.
    pub fn available(&self) -> CapacityUnit {
        self.available
    }
    /// The number of jobs in flight.
    pub fn in_flight(&self) -> CapacityUnit {
        self.in_flight
    }
}

impl Outcome {
    pub(crate) fn overloaded_or(self, other: Outcome) -> Outcome {
        use Outcome::*;
        match (self, other) {
            (Success, Overload) => Overload,
            _ => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crate::{
        limiter::{Limiter, Outcome},
        limits::{mock::MockLimitAlgorithm, Fixed, Sample},
    };

    #[tokio::test]
    async fn explicitly_releasing_token() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(10);

        let limiter = Limiter::new(Arc::clone(&mock_algo));

        let mut token = limiter.try_acquire().unwrap();

        assert_eq!(limiter.state().in_flight, 1);

        token.set_latency(Duration::from_secs(1));
        token.set_outcome(Outcome::Success).await;

        assert_eq!(limiter.state().in_flight, 0);
        assert_eq!(limiter.state().limit, 10);

        assert_eq!(
            mock_algo.samples().await,
            vec![Sample {
                in_flight: 1,
                latency: Duration::from_secs(1),
                outcome: Outcome::Success
            }]
        );
    }

    #[tokio::test]
    async fn dropping_token() {
        let limiter = Limiter::new(Fixed::new(10));

        let token = limiter.try_acquire().unwrap();

        assert_eq!(limiter.state().in_flight, 1);

        drop(token);

        assert_eq!(limiter.state().in_flight, 0);
        assert_eq!(limiter.state().limit, 10);
    }
}
