use std::{
    cmp,
    fmt::Debug,
    sync::{atomic, Arc},
    time::Duration,
};

use conv::ValueFrom;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError},
    time::timeout,
};

use crate::limits::{LimitAlgorithm, Sample};

use super::{AtomicCapacityUnit, CapacityUnit, Outcome};

/// A limiter for total capacity (as opposed to partitions on top).
#[derive(Debug)]
pub(crate) struct TotalLimiter<T> {
    limit_algo: T,
    semaphore: Arc<Semaphore>,
    limit: AtomicCapacityUnit,

    /// Best-effort consistency
    in_flight: AtomicCapacityUnit,

    #[cfg(test)]
    notifier: Option<Arc<tokio::sync::Notify>>,
}

impl<T> TotalLimiter<T>
where
    T: LimitAlgorithm,
{
    pub(crate) fn new(limit_algo: T) -> Self {
        let initial_permits = limit_algo.limit();
        assert!(initial_permits >= 1);

        TotalLimiter {
            limit_algo,
            semaphore: Arc::new(Semaphore::new(initial_permits)),
            limit: AtomicCapacityUnit::new(initial_permits),
            in_flight: AtomicCapacityUnit::new(0),

            #[cfg(test)]
            notifier: None,
        }
    }

    /// In some cases [Token]s are acquired asynchronously when updating the limit.
    #[cfg(test)]
    pub fn with_release_notifier(&mut self, n: Arc<tokio::sync::Notify>) {
        self.notifier.replace(n);
    }

    fn new_sample(&self, latency: Duration, outcome: Outcome) -> Sample {
        Sample {
            latency,
            in_flight: self.in_flight(),
            outcome,
        }
    }

    pub(crate) fn available(&self) -> CapacityUnit {
        self.semaphore.available_permits()
    }

    pub(crate) fn limit(&self) -> CapacityUnit {
        self.limit.load(atomic::Ordering::Acquire)
    }

    pub(crate) fn in_flight(&self) -> CapacityUnit {
        self.in_flight.load(atomic::Ordering::Acquire)
    }

    fn inc_in_flight(&self) -> CapacityUnit {
        self.in_flight.fetch_add(1, atomic::Ordering::SeqCst) + 1
    }

    fn dec_in_flight(&self) -> CapacityUnit {
        self.in_flight.fetch_sub(1, atomic::Ordering::SeqCst) - 1
    }

    pub(crate) async fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        match Arc::clone(&self.semaphore).try_acquire_owned() {
            Ok(permit) => {
                self.inc_in_flight();
                Some(permit)
            }
            Err(TryAcquireError::NoPermits) => None,

            Err(TryAcquireError::Closed) => {
                panic!("we own the semaphore, we shouldn't have closed it")
            }
        }
    }

    pub(crate) async fn acquire_timeout(&self, duration: Duration) -> Option<OwnedSemaphorePermit> {
        match timeout(duration, Arc::clone(&self.semaphore).acquire_owned()).await {
            Ok(Ok(permit)) => {
                self.inc_in_flight();
                Some(permit)
            }
            Err(_) => None,

            Ok(Err(_)) => {
                panic!("we own the semaphore, we shouldn't have closed it")
            }
        }
    }

    pub(crate) async fn update_limit(&self, outcome: Outcome, latency: Duration) -> CapacityUnit {
        let sample = self.new_sample(latency, outcome);

        let new_limit = self.limit_algo.update(sample).await;

        let old_limit = self.limit.swap(new_limit, atomic::Ordering::SeqCst);

        match new_limit.cmp(&old_limit) {
            cmp::Ordering::Greater => {
                self.semaphore.add_permits(new_limit - old_limit);

                #[cfg(test)]
                if let Some(n) = &self.notifier {
                    n.notify_one();
                }
            }
            cmp::Ordering::Less => {
                let semaphore = self.semaphore.clone();
                #[cfg(test)]
                let notifier = self.notifier.clone();

                tokio::spawn(async move {
                    // If there aren't enough permits available then this will wait until enough
                    // become available. This could take a while, so we do this in the background.
                    let permits = semaphore
                        .acquire_many(
                            u32::value_from(old_limit - new_limit)
                                .expect("change in limit shouldn't be > u32::MAX"),
                        )
                        .await
                        .expect("we own the semaphore, we shouldn't have closed it");

                    // Acquiring some permits and throwing them away reduces the available limit.
                    permits.forget();

                    #[cfg(test)]
                    if let Some(n) = notifier {
                        n.notify_one();
                    }
                });
            }
            _ =>
            {
                #[cfg(test)]
                if let Some(n) = &self.notifier {
                    n.notify_one();
                }
            }
        }

        new_limit
    }

    pub(crate) fn release(&self, permit: OwnedSemaphorePermit) {
        self.dec_in_flight();
        drop(permit);
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crate::{
        limiter::Outcome,
        limits::{mock::MockLimitAlgorithm, LimitAlgorithm, Sample},
    };

    use super::TotalLimiter;

    // TODO: Should we use a wrapper for the permit which releases on drop?
    #[tokio::test]
    async fn explicitly_releasing_permit_should_update_in_flight() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(10);

        let limiter = TotalLimiter::new(Arc::clone(&mock_algo));

        let permit = limiter.try_acquire().await.expect("enough capacity");

        assert_eq!(limiter.limit(), 10, "limit");
        assert_eq!(limiter.in_flight(), 1, "in flight");
        assert_eq!(limiter.available(), 9, "available");

        limiter.release(permit);

        assert_eq!(limiter.limit(), 10, "limit");
        assert_eq!(limiter.in_flight(), 0, "in flight");
        assert_eq!(limiter.available(), 10, "available");
    }

    #[tokio::test]
    async fn updates_samples() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(10);

        let limiter = TotalLimiter::new(Arc::clone(&mock_algo));

        let permit = limiter.try_acquire().await.expect("enough capacity");

        mock_algo.set_limit(20);

        limiter
            .update_limit(Outcome::Success, Duration::from_secs(1))
            .await;
        limiter.release(permit);

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
    async fn increasing_limit() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(10);

        let limiter = TotalLimiter::new(Arc::clone(&mock_algo));

        let permit = limiter.try_acquire().await.expect("enough capacity");

        mock_algo.set_limit(20);

        limiter
            .update_limit(Outcome::Success, Duration::from_secs(1))
            .await;
        limiter.release(permit);

        assert_eq!(limiter.limit(), 20, "limit");
        assert_eq!(limiter.in_flight(), 0, "in flight");
        assert_eq!(limiter.available(), 20, "available");

        assert_eq!(mock_algo.limit(), 20);
    }

    #[tokio::test]
    async fn decreasing_limit() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(10);

        let limiter = TotalLimiter::new(Arc::clone(&mock_algo));

        let permit = limiter.try_acquire().await.expect("enough capacity");

        mock_algo.set_limit(5);

        limiter
            .update_limit(Outcome::Success, Duration::from_secs(1))
            .await;
        limiter.release(permit);

        // Give an opportunity to decrease the available permits in the background
        tokio::task::yield_now().await;

        assert_eq!(limiter.limit(), 5, "limit");
        assert_eq!(limiter.in_flight(), 0, "in flight");
        assert_eq!(limiter.available(), 5, "available");
    }

    #[tokio::test]
    async fn decreasing_limit_below_available() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(10);

        let limiter = TotalLimiter::new(Arc::clone(&mock_algo));

        let permit1 = limiter.try_acquire().await.expect("enough capacity");
        let permit2 = limiter.try_acquire().await.expect("enough capacity");
        let permit3 = limiter.try_acquire().await.expect("enough capacity");

        mock_algo.set_limit(1);

        limiter
            .update_limit(Outcome::Success, Duration::from_secs(1))
            .await;
        limiter.release(permit1);

        // Give an opportunity to decrease the available permits in the background
        tokio::task::yield_now().await;

        assert_eq!(limiter.limit(), 1, "limit");
        assert_eq!(limiter.in_flight(), 2, "in flight");
        assert_eq!(limiter.available(), 0, "available");

        limiter
            .update_limit(Outcome::Success, Duration::from_secs(1))
            .await;
        limiter.release(permit2);

        // Give an opportunity to decrease the available permits in the background
        tokio::task::yield_now().await;

        assert_eq!(limiter.limit(), 1, "limit");
        assert_eq!(limiter.in_flight(), 1, "in flight");
        assert_eq!(limiter.available(), 0, "available");

        limiter
            .update_limit(Outcome::Success, Duration::from_secs(1))
            .await;
        limiter.release(permit3);

        // Give an opportunity to decrease the available permits in the background
        tokio::task::yield_now().await;

        assert_eq!(limiter.limit(), 1, "limit");
        assert_eq!(limiter.in_flight(), 0, "in flight");
        assert_eq!(limiter.available(), 1, "available");
    }
}
