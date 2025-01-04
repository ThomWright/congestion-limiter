use std::{
    collections::LinkedList,
    fmt::Debug,
    sync::{atomic, Arc},
    time::Duration,
};

use conv::{ConvAsUtil, ConvUtil};
use tokio::{
    sync::{oneshot, OwnedSemaphorePermit, RwLock},
    time::timeout,
};

use crate::{
    limiter::{Outcome, Token},
    limits::LimitAlgorithm,
};

use super::{AtomicCapacityUnit, CapacityUnit, LimiterState, Releaser, TotalLimiter};

type StateIndex = usize;

/// A partitioned [Limiter], using some fraction of the concurrency limit.
#[derive(Debug)]
pub struct PartitionedLimiter<T> {
    state_index: usize,
    scheduler: Arc<Scheduler<T>>,
}

/// Divides up capacity between multiple weighted partitions.
#[derive(Debug)]
pub(crate) struct Scheduler<T> {
    total_limiter: TotalLimiter<T>,

    waiters: RwLock<LinkedList<(StateIndex, oneshot::Sender<OwnedSemaphorePermit>)>>,

    partition_states: Vec<PartitionState>,
}

#[derive(Debug)]
struct PartitionState {
    /// Normalised, sum of fractions = 1.
    fraction: f64,
    in_flight: AtomicCapacityUnit,
    // job_queue: Mutex<VecDeque<Arc<Notify>>>,
}

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
    pub fn new(limit_algo: T, weights: Vec<f64>) -> Vec<Arc<Self>> {
        let num_partitions = weights.len();

        let total_limiter = TotalLimiter::new(limit_algo);
        let scheduler = Scheduler::new(total_limiter, weights);

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
    pub async fn try_acquire(self: &Arc<Self>) -> Option<Token> {
        self.scheduler
            .try_acquire(self.state_index)
            .await
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

    fn mint_token(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> Token {
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
    pub(crate) fn new(total_limiter: TotalLimiter<T>, weights: Vec<f64>) -> Arc<Self> {
        assert!(!weights.is_empty(), "Must provide at least one weight");

        let total: f64 = weights.iter().sum();

        let mut partition_states = Vec::with_capacity(weights.len());

        for weight in weights {
            let fraction = weight / total;

            partition_states.push(PartitionState {
                fraction,
                in_flight: AtomicCapacityUnit::new(0),
                // job_queue: Mutex::new(VecDeque::default()),
            });
        }

        Arc::new(Scheduler {
            total_limiter,
            partition_states,
            waiters: RwLock::default(),
        })
    }

    fn state(&self, index: usize) -> LimiterState {
        let partition_state = self
            .partition_states
            .get(index)
            .expect("partition state index should not be out of bounds");
        partition_state.state(self.total_limiter.limit())
    }

    async fn try_acquire(self: &Arc<Self>, index: usize) -> Option<OwnedSemaphorePermit> {
        let state = &self.partition_states[index];

        let total_limit = self.total_limiter.limit();
        if state.in_flight() < state.limit(total_limit) || self.spare_capacity(total_limit) > 0 {
            match self.total_limiter.try_acquire().await {
                Some(permit) => {
                    self.partition_states[index].inc_in_flight();
                    Some(permit)
                }
                None => None,
            }
        } else {
            None
        }
    }

    async fn acquire_timeout(
        &self,
        duration: Duration,
        index: usize,
    ) -> Option<OwnedSemaphorePermit> {
        let state = &self.partition_states[index];
        match timeout(duration, async {
            let total_limit = self.total_limiter.limit();
            if state.in_flight() < state.limit(total_limit) || self.spare_capacity(total_limit) > 0
            {
                self.total_limiter.try_acquire().await
            } else {
                let (snd, rx) = oneshot::channel();
                let mut waiters = self.waiters.write().await;
                waiters.push_back((index, snd));
                match rx.await {
                    Ok(token) => Some(token),
                    Err(_) => None,
                }
            }
        })
        .await
        {
            Ok(Some(permit)) => {
                self.partition_states[index].inc_in_flight();
                Some(permit)
            }
            Err(_) => None,
            Ok(None) => None,
        }
    }

    async fn update_limit(
        &self,
        outcome: Outcome,
        latency: Duration,
        _index: usize,
    ) -> CapacityUnit {
        self.total_limiter.update_limit(outcome, latency).await
    }

    /// When a permit becomes available, give it to the next job in the queue with the higher
    /// priority.
    ///
    /// The underlying semaphore is simply a FIFO queue. Instead, what we want is to give out tokens
    /// to jobs in partitions which are under-subscribed in favour of partitions which are
    /// oversubscribed.
    fn reuse_permit(self: &Arc<Self>, permit: OwnedSemaphorePermit, index: usize) {
        self.partition_states[index].dec_in_flight();

        let scheduler = Arc::clone(self);
        tokio::spawn(async move {
            // TODO: A better strategy for choosing which waiter to wake, based on priority.
            // For now this is just a FIFO queue, so it's kind of pointless!
            let waiter = scheduler.waiters.write().await.pop_front();
            match waiter {
                Some((_index, waiter)) => {
                    match waiter.send(permit) {
                        Ok(()) => {}
                        Err(_) => {
                            // Nothing to do, the token will be dropped
                        }
                    };
                }
                None => drop(permit),
            }
        });
    }

    /// Total spare capacity which can be used by any partition.
    fn spare_capacity(&self, total_limit: CapacityUnit) -> CapacityUnit {
        self.partition_states.iter().fold(0, |total, partition| {
            total + partition.spare_capacity(total_limit)
        })
    }
}

#[async_trait::async_trait]
impl<T: LimitAlgorithm + Send + 'static> Releaser for PartitionedLimiter<T> {
    async fn update_limit(&self, outcome: Outcome, latency: Duration) -> CapacityUnit {
        self.scheduler
            .update_limit(outcome, latency, self.state_index)
            .await
    }

    fn release(&self, permit: OwnedSemaphorePermit) {
        self.scheduler.reuse_permit(permit, self.state_index);
    }
}

impl PartitionState {
    const BUFFER_FRACTION: f64 = 0.1;

    fn state(&self, total_limit: CapacityUnit) -> LimiterState {
        let limit = self.limit(total_limit);
        let in_flight = self.in_flight();
        LimiterState {
            limit,
            in_flight,
            available: limit.saturating_sub(in_flight),
        }
    }

    fn limit(&self, total_limit: CapacityUnit) -> CapacityUnit {
        fractional_limit(total_limit, self.fraction)
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

    /// Spare capacity which can be used by other partitions.
    fn spare_capacity(&self, total_limit: CapacityUnit) -> CapacityUnit {
        let partition_limit = self.limit(total_limit);
        let buffer = (partition_limit as f64 * Self::BUFFER_FRACTION)
            .ceil()
            .approx_as::<CapacityUnit>()
            .expect("should be < usize::MAX");
        (partition_limit - self.in_flight()).saturating_sub(buffer)
    }
}

fn fractional_limit(limit: CapacityUnit, fraction: f64) -> CapacityUnit {
    let limit_f64 = limit as f64 * fraction;

    limit_f64
        .ceil()
        .approx()
        .expect("should be clamped within usize bounds")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        limiter::{Outcome, PartitionedLimiter},
        limits::mock::MockLimitAlgorithm,
    };

    #[tokio::test]
    async fn it_works() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(10);

        let partitions = PartitionedLimiter::new(Arc::clone(&mock_algo), vec![0.5, 0.5]);

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].state().limit(), 5);
        assert_eq!(partitions[1].state().limit(), 5);

        let token0 = partitions[0].try_acquire().await.unwrap();
        let token1 = partitions[1].try_acquire().await.unwrap();

        assert_eq!(partitions[0].state().available(), 4);
        assert_eq!(partitions[1].state().available(), 4);
        assert_eq!(partitions[0].state().in_flight(), 1);
        assert_eq!(partitions[1].state().in_flight(), 1);

        token0.set_outcome(Outcome::Success).await;
        drop(token1);

        assert_eq!(partitions[0].state().available(), 5);
        assert_eq!(partitions[1].state().available(), 5);
        assert_eq!(partitions[0].state().in_flight(), 0);
        assert_eq!(partitions[1].state().in_flight(), 0);

        // Dropping a token won't contribute a sample to the limiter algorithm,
        // only setting an outcome will.
        assert_eq!(mock_algo.samples().await.len(), 1);
    }
}
