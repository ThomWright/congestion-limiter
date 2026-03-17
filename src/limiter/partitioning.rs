use std::{
    fmt::Debug,
    sync::{atomic, Arc},
    time::Duration,
};

use crate::semaphore::Permit;
use bon::bon;

use crate::{
    limiter::{Outcome, Token},
    limits::LimitAlgorithm,
};

use super::{AtomicCapacityUnit, CapacityUnit, LimiterState, Releaser, TotalLimiter};

type StateIndex = usize;

/// A partitioned [Limiter](crate::limiter::Limiter), using some fraction of the concurrency limit.
///
/// Capacity can be borrowed from other partitions if available.
#[derive(Debug)]
pub struct PartitionedLimiter<T> {
    state_index: StateIndex,
    scheduler: Arc<Scheduler<T>>,
}

/// Divides up capacity between multiple weighted partitions.
#[derive(Debug)]
pub(crate) struct Scheduler<T> {
    total_limiter: TotalLimiter<T>,
    partition_states: Vec<PartitionState>,
}

#[derive(Debug)]
struct PartitionState {
    /// Normalised, sum of fractions = 1.
    fraction: f64,
    in_flight: AtomicCapacityUnit,
}

#[bon]
impl<T> PartitionedLimiter<T>
where
    T: LimitAlgorithm + Send + 'static,
{
    /// Create a set of limiters, one per partition, with the given weights.
    ///
    /// The provided weights will be normalised. E.g. weights of 2, 2 and 4 will result in
    /// partitions of 25%, 25% and 50% of the total limit, respectively.
    ///
    /// `weights` must not be empty.
    ///
    /// `max_queue_size` bounds how many callers can wait for a token at once;
    /// excess callers receive an immediate rejection. Use [`usize::MAX`] to allow unbounded
    /// waiting.
    #[builder]
    pub fn new(
        limit_algo: T,
        weights: Vec<f64>,
        #[builder(default = usize::MAX)] max_queue_size: usize,
    ) -> Vec<Arc<Self>> {
        // TODO: explicitly allow or disallow borrowing?

        let num_partitions = weights.len();

        let scheduler = Scheduler::new(limit_algo, max_queue_size, weights);

        let mut partitions = Vec::with_capacity(num_partitions);
        for i in 0..num_partitions {
            partitions.push(Arc::new(Self {
                state_index: i,
                scheduler: scheduler.clone(),
            }));
        }

        partitions
    }

    /// The current state of the limiter.
    pub fn state(&self) -> LimiterState {
        self.scheduler.state(self.state_index)
    }

    /// Try to immediately acquire a concurrency [Token].
    ///
    /// Returns `None` if there are none available.
    pub fn try_acquire(self: &Arc<Self>) -> Option<Token> {
        self.scheduler
            .try_acquire(self.state_index)
            .map(|permit| self.mint_token(permit))
    }

    /// Try to acquire a concurrency [Token], waiting for `duration` if there are none available.
    ///
    /// Returns `None` if there are none available after `duration`.
    pub async fn acquire_timeout(self: &Arc<Self>, duration: Duration) -> Option<Token> {
        self.scheduler
            .acquire_timeout(duration, self.state_index)
            .await
            .map(|permit| self.mint_token(permit))
    }

    fn mint_token(self: &Arc<Self>, permit: Permit) -> Token {
        Token::new(permit, Arc::clone(self))
    }
}

impl<T> Scheduler<T>
where
    T: LimitAlgorithm + Send + 'static,
{
    /// Create a scheduler with the given weights.
    ///
    /// The provided weights will be normalised. E.g. weights of 2, 2 and 4 will result in
    /// partitions of 25%, 25% and 50% of the total limit, respectively.
    ///
    /// `weights` must not be empty.
    pub(crate) fn new(limit_algo: T, max_queue_size: usize, weights: Vec<f64>) -> Arc<Self> {
        assert!(!weights.is_empty(), "Must provide at least one weight");

        let total: f64 = weights.iter().sum();
        let fractions: Vec<f64> = weights.iter().map(|w| w / total).collect();

        let total_limiter =
            TotalLimiter::new_weighted(limit_algo, max_queue_size, fractions.clone());

        let partition_states = fractions
            .into_iter()
            .map(|fraction| PartitionState {
                fraction,
                in_flight: AtomicCapacityUnit::new(0),
            })
            .collect();

        Arc::new(Scheduler {
            total_limiter,
            partition_states,
        })
    }

    fn state(&self, index: StateIndex) -> LimiterState {
        let partition_state = self
            .partition_states
            .get(index)
            .expect("partition state index should not be out of bounds");
        partition_state.state(self.total_limiter.limit())
    }

    fn try_acquire(self: &Arc<Self>, index: StateIndex) -> Option<Permit> {
        match self.total_limiter.try_acquire_from_pool(index) {
            Some(permit) => {
                self.partition_states[index].inc_in_flight();
                Some(permit)
            }
            None => None,
        }
    }

    async fn acquire_timeout(&self, duration: Duration, index: StateIndex) -> Option<Permit> {
        match self
            .total_limiter
            .acquire_timeout_from_queue(duration, index)
            .await
        {
            Some(permit) => {
                self.partition_states[index].inc_in_flight();
                Some(permit)
            }
            None => None,
        }
    }

    async fn update_limit(
        &self,
        outcome: Outcome,
        latency: Duration,
        _index: StateIndex,
    ) -> CapacityUnit {
        self.total_limiter.update_limit(outcome, latency).await
    }
}

#[async_trait::async_trait]
impl<T: LimitAlgorithm + Send + 'static> Releaser for PartitionedLimiter<T> {
    async fn update_limit(&self, outcome: Outcome, latency: Duration) -> CapacityUnit {
        self.scheduler
            .update_limit(outcome, latency, self.state_index)
            .await
    }

    fn release(&self, permit: Permit) {
        self.scheduler.partition_states[self.state_index].dec_in_flight();
        self.scheduler.total_limiter.release(permit);
    }
}

impl PartitionState {
    fn state(&self, total_limit: CapacityUnit) -> LimiterState {
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "fraction is in [0,1] so the product is non-negative and bounded by total_limit"
        )]
        let limit = (total_limit as f64 * self.fraction).ceil() as CapacityUnit;
        let in_flight = self.in_flight();
        LimiterState {
            limit,
            in_flight,
            available: limit.saturating_sub(in_flight),
        }
    }

    fn inc_in_flight(&self) -> CapacityUnit {
        self.in_flight.fetch_add(1, atomic::Ordering::SeqCst) + 1
    }

    fn dec_in_flight(&self) -> CapacityUnit {
        self.in_flight.fetch_sub(1, atomic::Ordering::SeqCst) - 1
    }

    fn in_flight(&self) -> CapacityUnit {
        self.in_flight.load(atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use crate::{
        limiter::{Outcome, PartitionedLimiter, Token},
        limits::mock::MockLimitAlgorithm,
    };

    #[tokio::test]
    async fn it_works() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(10);

        let weights = vec![0.5, 0.5];
        let partitions = PartitionedLimiter::builder()
            .limit_algo(Arc::clone(&mock_algo))
            .weights(weights)
            .build();

        // Initial conditions
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].state().limit(), 5);
        assert_eq!(partitions[1].state().limit(), 5);

        // Take a token from each partition
        let token0 = partitions[0].try_acquire().unwrap();
        let token1 = partitions[1].try_acquire().unwrap();

        assert_eq!(partitions[0].state().available(), 4);
        assert_eq!(partitions[1].state().available(), 4);
        assert_eq!(partitions[0].state().in_flight(), 1);
        assert_eq!(partitions[1].state().in_flight(), 1);

        // Set an outcome for one of the tokens
        token0.set_outcome(Outcome::Success).await;
        // And just drop the other one
        drop(token1);

        assert_eq!(partitions[0].state().available(), 5);
        assert_eq!(partitions[1].state().available(), 5);
        assert_eq!(partitions[0].state().in_flight(), 0);
        assert_eq!(partitions[1].state().in_flight(), 0);

        // Dropping a token won't contribute a sample to the limiter algorithm,
        // only setting an outcome will.
        assert_eq!(mock_algo.samples().await.len(), 1);
    }

    #[tokio::test]
    async fn borrowing() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(4);

        let weights = vec![0.5, 0.5];
        let partitions = PartitionedLimiter::builder()
            .limit_algo(Arc::clone(&mock_algo))
            .weights(weights)
            .build();

        // Initial conditions
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].state().limit(), 2);
        assert_eq!(partitions[1].state().limit(), 2);

        // Saturate the first partition
        let _token0_0 = partitions[0].try_acquire().unwrap();
        let _token0_1 = partitions[0].try_acquire().unwrap();

        // Try to borrow some capacity from the other partition
        let token0_2 = partitions[0].try_acquire();

        assert!(
            token0_2.is_some(),
            "should be able to borrow _some_ capacity from other partition"
        );

        // Check system state
        assert_eq!(partitions[0].state().limit(), 2);
        assert_eq!(partitions[1].state().limit(), 2);
        assert_eq!(partitions[0].state().available(), 0);
        assert_eq!(partitions[1].state().available(), 2);
        assert_eq!(partitions[0].state().in_flight(), 3);
        assert_eq!(partitions[1].state().in_flight(), 0);

        // Attempt to borrow more than we're allowed
        let token0_3 = partitions[0].try_acquire();
        assert!(
            token0_3.is_none(),
            "should not be able to borrow _all_ capacity from other partition"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn queueing() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(4);

        let weights = vec![0.5, 0.5];
        let partitions = PartitionedLimiter::builder()
            .limit_algo(Arc::clone(&mock_algo))
            .weights(weights)
            .build();

        // Initial conditions
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].state().limit(), 2);
        assert_eq!(partitions[1].state().limit(), 2);

        // Saturate both partitions
        let token0_0 = partitions[0].try_acquire().unwrap();
        let _token0_1 = partitions[0].try_acquire().unwrap();
        let _token1_0 = partitions[1].try_acquire().unwrap();
        let _token1_1 = partitions[1].try_acquire().unwrap();

        // Wait to acquire a token from the first partition
        let t: Arc<Mutex<Option<Token>>> = Arc::new(Mutex::new(Option::None));
        let queueing_task = tokio::spawn({
            let t = Arc::clone(&t);
            async move {
                let token = partitions[0].acquire_timeout(Duration::MAX).await;
                t.lock().unwrap().replace(token.unwrap());
            }
        });

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(
            t.lock().unwrap().is_none(),
            "should not have acquired token yet"
        );

        // Drop a token from the first partition
        drop(token0_0);

        // Wait for acquiring to complete
        queueing_task.await.expect("task should not panic");

        assert!(t.lock().unwrap().is_some(), "should acquire the token");
    }

    #[tokio::test]
    async fn in_flight_is_decremented_after_release() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(10);

        let partitions = PartitionedLimiter::builder()
            .limit_algo(Arc::clone(&mock_algo))
            .weights(vec![1.0])
            .build();

        let t1 = partitions[0].try_acquire().unwrap();
        t1.set_outcome(Outcome::Success).await;

        let t2 = partitions[0].try_acquire().unwrap();
        t2.set_outcome(Outcome::Success).await;

        let samples = mock_algo.samples().await;
        assert_eq!(samples[0].in_flight, 1, "first sample");
        assert_eq!(samples[1].in_flight, 1, "second sample: should not grow");
    }

    /// A waiter from a higher-weight partition is served before a waiter from a lower-weight
    /// partition, even if the lower-weight waiter has been waiting longer.
    #[tokio::test(start_paused = true)]
    async fn priority_ordering() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        // distribute(20, [1.0, 10.0]) → [2, 18]. buffer(18) = 2.
        mock_algo.set_limit(20);

        let partitions = PartitionedLimiter::builder()
            .limit_algo(Arc::clone(&mock_algo))
            .weights(vec![1.0, 10.0])
            .build();

        // Exhaust all permits from partition 1.
        let mut held = Vec::new();
        while let Some(t) = partitions[1].try_acquire() {
            held.push(t);
        }
        // Also take partition 0's permits.
        while let Some(t) = partitions[0].try_acquire() {
            held.push(t);
        }

        let order: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

        // Queue a waiter for the low-weight partition 0.
        let w0 = tokio::spawn({
            let p = Arc::clone(&partitions[0]);
            let order = Arc::clone(&order);
            async move {
                let _t = p.acquire_timeout(Duration::MAX).await.unwrap();
                order.lock().unwrap().push(0u32);
            }
        });
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_millis(5)).await;

        // Queue a waiter for the high-weight partition 1.
        let w1 = tokio::spawn({
            let p = Arc::clone(&partitions[1]);
            let order = Arc::clone(&order);
            async move {
                let _t = p.acquire_timeout(Duration::MAX).await.unwrap();
                order.lock().unwrap().push(1u32);
            }
        });
        tokio::task::yield_now().await;

        //   w0: weight=1/11, waited=10ms  → priority ≈ 0.00091
        //   w1: weight=10/11, waited=5ms  → priority ≈ 0.0045  ← wins
        tokio::time::advance(Duration::from_millis(5)).await;

        // Drop all held permits. Returns go to home pools.
        // Pool 1's waiter (w1) is served first via case 2 (home pool has waiter).
        // Pool 0's waiter (w0) is served later when a pool-0 permit returns.
        drop(held);
        tokio::task::yield_now().await;

        w1.await.unwrap();
        w0.await.unwrap();

        assert_eq!(*order.lock().unwrap(), vec![1, 0]);
    }

    // TODO: test fairness
}
