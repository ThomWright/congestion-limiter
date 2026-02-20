use std::{
    collections::VecDeque,
    fmt::Debug,
    sync::{atomic, Arc},
    time::Duration,
};

use conv::{ConvAsUtil, ConvUtil};
use tokio::{
    sync::{oneshot, Mutex, OwnedSemaphorePermit},
    time::{timeout, Instant},
};

use crate::{
    limiter::{Outcome, Token},
    limits::LimitAlgorithm,
};

use super::{
    AtomicCapacityUnit, CapacityUnit, CapacityUnitNeg, LimiterState, Releaser, TotalLimiter,
};

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
    job_queue: Mutex<VecDeque<(Instant, oneshot::Sender<OwnedSemaphorePermit>)>>,
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
        // TODO: explicitly allow or disallow borrowing?

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
                job_queue: Mutex::new(VecDeque::default()),
            });
        }

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

    fn try_acquire(self: &Arc<Self>, index: StateIndex) -> Option<OwnedSemaphorePermit> {
        let partition = &self.partition_states[index];

        let total_limit = self.total_limiter.limit();
        if partition.has_capacity_available(total_limit) || self.has_spare_capacity(total_limit) {
            match self.total_limiter.try_acquire() {
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
        index: StateIndex,
    ) -> Option<OwnedSemaphorePermit> {
        let partition = self
            .partition_states
            .get(index)
            .expect("partition index should not be out of bounds");

        match timeout(duration, async {
            // First we try without the lock, then if we don't succeed we lock the queue and try
            // again. This is OK because there should never be a state where a job is in the
            // queue whilst there are available permits.
            let mut lock = None;
            let mut locked_queue = loop {
                let total_limit = self.total_limiter.limit();

                // Cases:
                // 1. Partition: available, total: available -> acquire
                // 2. Partition: available, total: full -> queue
                //    (another partition has borrowed)
                // 3. Partition: full, total: spare -> acquire
                // 4. Partition: full, total: no spare -> queue

                // First, check if we might have capacity.
                if partition.has_capacity_available(total_limit)
                    || self.has_spare_capacity(total_limit)
                {
                    // Either this partition has capacity, or there's some spare capacity we can
                    // borrow.

                    // Let's see if the there's a permit available (this might change by
                    // time we try to acquire it).
                    let permit = self.total_limiter.try_acquire();

                    if permit.is_some() {
                        return permit;
                    }
                }

                // If not, then we might have to queue this job.
                break match lock {
                    None => {
                        // Any jobs in this partition will lock the queue before returning their
                        // permit, so locking the queue here prevents any new permits being added
                        // underneath us.

                        // We might miss available spare capacity *from other partitions*, but
                        // that's OK.

                        // So lock the queue and try again.
                        lock = Some(partition.job_queue.lock().await);
                        continue;
                    }

                    // We locked the queue and there is still no available capacity
                    Some(lock) => lock,
                };
            };

            // No available capacity. We'll have to wait.

            let (tx, rx) = oneshot::channel();
            locked_queue.push_back((Instant::now(), tx));
            drop(locked_queue);

            // Wait for a permit to be recycled.
            let permit = rx.await;

            permit.ok()
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
        _index: StateIndex,
    ) -> CapacityUnit {
        self.total_limiter.update_limit(outcome, latency).await
    }

    /// When a permit becomes available, give it to the next job in the queue with the highest
    /// priority.
    ///
    /// The underlying semaphore is simply a FIFO queue. Instead, what we want is a kind of priority
    /// queue, where the priority is based on the time a job has been waiting and the weight of the
    /// partition.
    fn recycle_permit(self: &Arc<Self>, permit: OwnedSemaphorePermit, index: StateIndex) {
        self.partition_states[index].dec_in_flight();

        /// Like a normal FIFO queue, items which arrive first have higher priority.
        ///
        /// However, this priority is weighted per partition. Partitions with higher weights have
        /// higher priority.
        fn priority(weight: f64, time_in_q: Duration) -> f64 {
            weight * time_in_q.as_secs_f64()
        }

        let scheduler = Arc::clone(self);
        tokio::spawn(async move {
            let mut highest_priority = 0.0;
            let mut unlocked_queue = None;

            // Iterate through all partitions to find the job with the highest priority.
            // The item at the front of each queue has the highest priority for that partition.
            // O(n) where n is the number of partitions.
            for p in scheduler.partition_states.iter() {
                let q = p.job_queue.lock().await;
                if let Some((instant, _tx)) = q.front() {
                    let job_priority = priority(p.fraction, instant.elapsed());
                    if job_priority > highest_priority {
                        highest_priority = job_priority;
                        unlocked_queue = Some(q);
                    }
                }
            }

            if let Some(mut queue) = unlocked_queue {
                let (_, tx) = queue.pop_front().unwrap();
                let _ = tx.send(permit);
            } else {
                drop(permit);
            }
        });
    }

    /// Total spare capacity which can be used by any partition.
    fn spare_capacity(&self, total_limit: CapacityUnit) -> CapacityUnitNeg {
        self.partition_states.iter().fold(0, |total, partition| {
            total + partition.spare_capacity(total_limit)
        })
    }

    fn has_spare_capacity(&self, total_limit: CapacityUnit) -> bool {
        self.spare_capacity(total_limit) > 0
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
        self.scheduler.recycle_permit(permit, self.state_index);
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
    ///
    /// Can be negative when:
    /// 1. capacity is being borrowed from another partition.
    /// 2. the total limit has been reduced.
    fn spare_capacity(&self, total_limit: CapacityUnit) -> CapacityUnitNeg {
        let partition_limit = self.limit(total_limit);
        let buffer = (partition_limit as f64 * Self::BUFFER_FRACTION)
            .ceil()
            .approx_as::<CapacityUnitNeg>()
            .expect("should be < usize::MAX");

        let spare = partition_limit
            .approx_as::<CapacityUnitNeg>()
            .expect("should be < isize::MAX")
            - self
                .in_flight()
                .approx_as::<CapacityUnitNeg>()
                .expect("should be < isize::MAX");

        // Keep a buffer just for this partition, to grow into if we need it.
        if spare > 0 {
            (spare - buffer).max(0)
        } else {
            spare
        }
    }

    fn has_capacity_available(&self, total_limit: CapacityUnit) -> bool {
        self.in_flight() < self.limit(total_limit)
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
    use std::{sync::Arc, time::Duration};

    use tokio::sync::Mutex;

    use crate::{
        limiter::{Outcome, PartitionedLimiter, Token},
        limits::mock::MockLimitAlgorithm,
    };

    #[tokio::test]
    async fn it_works() {
        let mock_algo = Arc::new(MockLimitAlgorithm::default());
        mock_algo.set_limit(10);

        let weights = vec![0.5, 0.5];
        let partitions = PartitionedLimiter::new(Arc::clone(&mock_algo), weights);

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
        let partitions = PartitionedLimiter::new(Arc::clone(&mock_algo), weights);

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
        let partitions = PartitionedLimiter::new(Arc::clone(&mock_algo), weights);

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
                t.lock().await.replace(token.unwrap());
            }
        });

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(
            t.lock().await.is_none(),
            "should not have acquired token yet"
        );

        // Drop a token from the first partition
        drop(token0_0);

        // Wait for acquiring to complete
        queueing_task.await.expect("task should not panic");

        assert!(t.lock().await.is_some(), "should acquire the token");
    }

    // TODO: test fairness
    // TODO: test prioritisation
}
