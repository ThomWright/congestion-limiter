//! A semaphore implementation.
//!
//! It aims to solve two problems with tokio's semaphore:
//!
//! 1. No limit on queue size. This can lead to an unbounded number of jobs in the system.
//! 2. No control over queue prioritisation strategy. It is always FIFO.
//!
//! This won't be as optimised as tokio's semaphore, but it should be good enough for our use case.

use std::{
    cmp::Ordering,
    collections::VecDeque,
    future::Future,
    sync::{
        atomic::{self, AtomicBool},
        Arc, Mutex,
    },
};

use bon::bon;
use tokio::{sync::oneshot, time::Instant};

#[derive(Debug)]
struct Pool {
    weight: f64,
    queue: VecDeque<QueueEntry>,
    limit: usize,
    available: usize,
    in_flight: usize,
    priority_reduction: usize,
}

#[derive(Debug)]
struct SemaphoreState {
    pools: Vec<Pool>,
}

#[derive(Debug)]
pub struct Semaphore {
    state: Mutex<SemaphoreState>,
    max_queue_size: usize,
    closed: AtomicBool,
}

#[derive(Debug)]
struct QueueEntry {
    id: EntryId,
    sender: oneshot::Sender<Result<Permit, AcquireError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EntryId {
    added: Instant,
    /// Discriminates between different entries added at the same time.
    ///
    /// Assumes no more than `usize` entries are added at the same time.
    discriminator: usize,
}

#[derive(Debug)]
pub struct Permit {
    semaphore: Option<Arc<Semaphore>>,
    home_pool: usize,
    user_pool: usize,
}

struct Acquire {
    id: EntryId,
    queue_idx: usize,
    receiver: oneshot::Receiver<Result<Permit, AcquireError>>,
    semaphore: Arc<Semaphore>,
}

#[derive(Debug, thiserror::Error)]
pub enum AcquireError {
    #[error("No permits available")]
    NoPermits,
    #[error("Acquire queue full")]
    QueueFull,
    #[error("Acquire timed out")]
    Timeout,
    #[error("Semaphore closed")]
    Closed,
}

#[bon]
impl Semaphore {
    #[builder]
    pub fn new(
        /// Initial number of permits in the pool.
        initial_permits: usize,
        /// Maximum queue size per pool.
        max_queue_size: usize,
        /// Weights for each pool. Defaults to `[1.0]`.
        #[builder(default = vec![1.0])]
        weights: Vec<f64>,
    ) -> Self {
        assert!(!weights.is_empty(), "weights must not be empty");

        let dist = distribute(initial_permits, &weights);
        let cap_per_queue = max_queue_size.min(64);
        Self {
            state: Mutex::new(SemaphoreState {
                pools: weights
                    .into_iter()
                    .enumerate()
                    .map(|(i, w)| Pool {
                        weight: w,
                        queue: VecDeque::with_capacity(cap_per_queue),
                        limit: dist[i],
                        available: dist[i],
                        in_flight: 0,
                        priority_reduction: 0,
                    })
                    .collect(),
            }),
            max_queue_size,
            closed: AtomicBool::new(false),
        }
    }

    pub(crate) fn try_acquire(self: Arc<Self>, pool_idx: usize) -> Result<Permit, AcquireError> {
        if self.closed.load(atomic::Ordering::Acquire) {
            return Err(AcquireError::Closed);
        }

        let mut state = self.state.lock().expect("lock should not be poisoned");
        match Self::try_acquire_inner(&mut state, pool_idx) {
            Some((home, user)) => {
                drop(state);
                Ok(Permit {
                    semaphore: Some(Arc::clone(&self)),
                    home_pool: home,
                    user_pool: user,
                })
            }
            None => Err(AcquireError::NoPermits),
        }
    }

    /// Try to acquire a permit from `pool_idx`'s own pool, then by borrowing
    /// from a donor with spare capacity above its buffer threshold.
    ///
    /// Returns `Some((home_pool, user_pool))` on success.
    fn try_acquire_inner(state: &mut SemaphoreState, pool_idx: usize) -> Option<(usize, usize)> {
        // Own pool
        if state.pools[pool_idx].available > 0 {
            state.pools[pool_idx].available -= 1;
            state.pools[pool_idx].in_flight += 1;
            return Some((pool_idx, pool_idx));
        }

        // Borrow from a donor with spare above buffer
        let donor = (0..state.pools.len())
            .find(|&i| i != pool_idx && state.pools[i].available > buffer(state.pools[i].limit))?;

        state.pools[donor].available -= 1;
        state.pools[pool_idx].in_flight += 1;
        Some((donor, pool_idx))
    }

    pub(crate) async fn acquire_timeout(
        self: Arc<Self>,
        timeout: std::time::Duration,
        queue_idx: usize,
    ) -> Result<Permit, AcquireError> {
        let (receiver, id) = {
            if self.closed.load(atomic::Ordering::Acquire) {
                return Err(AcquireError::Closed);
            }

            let mut state = self.state.lock().expect("lock should not be poisoned");

            // Fast path: try own pool or borrow
            if let Some((home, user)) = Self::try_acquire_inner(&mut state, queue_idx) {
                drop(state);
                return Ok(Permit {
                    semaphore: Some(Arc::clone(&self)),
                    home_pool: home,
                    user_pool: user,
                });
            }

            if state.pools[queue_idx].queue.len() >= self.max_queue_size {
                return Err(AcquireError::QueueFull);
            }

            let (sender, receiver) = oneshot::channel();
            let sub = &mut state.pools[queue_idx];
            let id = match sub.queue.back() {
                Some(entry) => entry.id.next(),
                _ => EntryId::default(),
            };
            sub.queue.push_back(QueueEntry { id, sender });
            drop(state);

            (receiver, id)
        };

        let acquire = Acquire::new(Arc::clone(&self), receiver, id, queue_idx);
        match tokio::time::timeout(timeout, acquire).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(e)) => Err(e),
            Err(_elapsed) => Err(AcquireError::Timeout),
        }
    }

    /// Dequeue the highest-priority entry across all pools.
    ///
    /// Priority = `weight * time_in_queue`. Scans the front of each pool's queue (O(p)) and pops
    /// from the winner. FIFO tiebreak: older entry (lower `EntryId`) wins.
    ///
    /// Returns `(pool_index, entry)`.
    fn dequeue_highest_priority(pools: &mut [Pool]) -> Option<(usize, QueueEntry)> {
        let best_idx = pools
            .iter()
            .enumerate()
            .filter_map(|(qi, sub)| sub.queue.front().map(|e| (qi, sub.weight, e)))
            .max_by(|(_, wa, a), (_, wb, b)| {
                let pa = wa * a.id.added.elapsed().as_secs_f64();
                let pb = wb * b.id.added.elapsed().as_secs_f64();
                pa.partial_cmp(&pb)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| b.id.cmp(&a.id)) // FIFO tiebreak: lower id = older = wins
            })
            .map(|(qi, _, _)| qi)?;

        pools[best_idx].queue.pop_front().map(|e| (best_idx, e))
    }

    /// Return a permit to its home pool. Four outcomes, checked in order:
    ///
    /// 1. Home pool has `priority_reduction > 0` → consume (permit destroyed).
    /// 2. Home pool's queue has a waiter → wake it (permit stays in-flight).
    /// 3. Home pool has spare capacity and another pool has a queued waiter →
    ///    lend to that waiter (permit stays in-flight, re-homed).
    /// 4. Otherwise → return to home pool's available.
    fn return_permit(self: Arc<Self>, home: usize, user: usize) {
        let mut state = self.state.lock().expect("lock should not be poisoned");

        // Always decrement user pool's in_flight.
        state.pools[user].in_flight -= 1;

        // Case 1: consume to satisfy a pending reduction.
        if state.pools[home].priority_reduction > 0 {
            state.pools[home].priority_reduction -= 1;
            return;
        }

        // Case 2: wake home pool's front waiter.
        if !state.pools[home].queue.is_empty() {
            let entry = state.pools[home].queue.pop_front().unwrap();
            state.pools[home].in_flight += 1;
            drop(state);
            let _ = entry.sender.send(Ok(Permit {
                semaphore: Some(Arc::clone(&self)),
                home_pool: home,
                user_pool: home,
            }));
            return;
        }

        // Case 3: home pool has spare — lend to another pool's waiter.
        // Home's queue is empty (case 2 was false), so dequeue_highest_priority
        // will only pick from other pools.
        if state.pools[home].available >= buffer(state.pools[home].limit) {
            if let Some((waiter_pool, entry)) = Self::dequeue_highest_priority(&mut state.pools) {
                state.pools[waiter_pool].in_flight += 1;
                drop(state);
                let _ = entry.sender.send(Ok(Permit {
                    semaphore: Some(Arc::clone(&self)),
                    home_pool: home,
                    user_pool: waiter_pool,
                }));
                return;
            }
        }

        // Case 4: return to home pool's available.
        state.pools[home].available += 1;
    }

    pub fn available_permits(&self) -> usize {
        self.state
            .lock()
            .expect("lock should not be poisoned")
            .pools
            .iter()
            .map(|p| p.available)
            .sum()
    }

    /// Set the total permit limit, redistributing across pools proportionally.
    ///
    /// For each pool, given its new target limit:
    /// - If increasing: cancel priority reductions, wake waiters, add to available.
    /// - If decreasing: consume available immediately, remainder → priority_reduction.
    pub(crate) fn set_limit(self: Arc<Self>, new_total: usize) {
        let mut state = self.state.lock().expect("lock should not be poisoned");
        let weights: Vec<f64> = state.pools.iter().map(|p| p.weight).collect();
        let new_dist = distribute(new_total, &weights);

        // Collect waiters to wake outside the lock: (pool_idx, entry).
        let mut to_wake: Vec<(usize, QueueEntry)> = Vec::new();

        for (i, pool) in state.pools.iter_mut().enumerate() {
            let new_limit = new_dist[i];
            let delta = new_limit.cast_signed() - pool.limit.cast_signed();
            pool.limit = new_limit;

            if delta > 0 {
                let delta = delta.cast_unsigned();
                // Cancel pending reductions first.
                let cancel = delta.min(pool.priority_reduction);
                pool.priority_reduction -= cancel;
                let after_cancel = delta - cancel;

                // Wake waiters from this pool's queue.
                let wake = after_cancel.min(pool.queue.len());
                for _ in 0..wake {
                    let entry = pool.queue.pop_front().unwrap();
                    pool.in_flight += 1;
                    to_wake.push((i, entry));
                }

                // Remainder goes to available.
                pool.available += after_cancel - wake;
            } else if delta < 0 {
                let abs_delta = (-delta).cast_unsigned();
                // Consume available immediately.
                let consume = abs_delta.min(pool.available);
                pool.available -= consume;
                // Remainder becomes priority_reduction.
                pool.priority_reduction += abs_delta - consume;
            }
        }

        drop(state);

        // Send permits to woken waiters outside the lock.
        for (pool_idx, entry) in to_wake {
            let _ = entry.sender.send(Ok(Permit {
                semaphore: Some(Arc::clone(&self)),
                home_pool: pool_idx,
                user_pool: pool_idx,
            }));
        }
    }

    fn cancel(&self, id: EntryId, queue_idx: usize) {
        let mut state = self.state.lock().expect("lock should not be poisoned");
        let entries = &mut state.pools[queue_idx].queue;
        if let Ok(index) = entries.binary_search_by_key(&id, |entry| entry.id) {
            entries.remove(index);
        }
    }

    fn close(&self) {
        self.closed.store(true, atomic::Ordering::Release);

        let mut state = self.state.lock().expect("lock should not be poisoned");
        for sub in state.pools.iter_mut() {
            for entry in sub.queue.drain(..) {
                let _ = entry.sender.send(Err(AcquireError::Closed));
            }
        }
    }

    #[cfg(test)]
    fn priority_reduction(&self) -> usize {
        self.state
            .lock()
            .expect("lock should not be poisoned")
            .pools
            .iter()
            .map(|p| p.priority_reduction)
            .sum()
    }
}

/// Buffer threshold for a pool: 10% of the limit (rounded up).
///
/// A pool must keep at least this many permits available before lending to others.
pub(crate) fn buffer(limit: usize) -> usize {
    limit.div_ceil(10)
}

/// Distribute `total` across `weights.len()` buckets proportional to each weight,
/// using the largest remainder method so the sum is exactly `total`.
pub(crate) fn distribute(total: usize, weights: &[f64]) -> Vec<usize> {
    assert!(!weights.is_empty());
    let sum: f64 = weights.iter().sum();
    assert!(sum > 0.0, "weights must sum to a positive value");

    let exact: Vec<f64> = weights.iter().map(|w| (w / sum) * total as f64).collect();
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "weights are non-negative so floor() is always non-negative and fits in usize"
    )]
    let floors: Vec<usize> = exact.iter().map(|v| v.floor() as usize).collect();
    let mut remainders: Vec<(usize, f64)> = exact
        .iter()
        .zip(floors.iter())
        .enumerate()
        .map(|(i, (e, f))| (i, e - *f as f64))
        .collect();

    let floor_sum: usize = floors.iter().sum();
    let mut result = floors;
    let deficit = total - floor_sum;

    // Distribute remaining units to the buckets with the largest remainders.
    remainders.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    for &(i, _) in remainders.iter().take(deficit) {
        result[i] += 1;
    }

    result
}

impl Drop for Semaphore {
    fn drop(&mut self) {
        self.close();
    }
}

impl Acquire {
    fn new(
        semaphore: Arc<Semaphore>,
        receiver: oneshot::Receiver<Result<Permit, AcquireError>>,
        id: EntryId,
        queue_idx: usize,
    ) -> Self {
        Self {
            id,
            queue_idx,
            receiver,
            semaphore,
        }
    }
}

impl Future for Acquire {
    type Output = Result<Permit, AcquireError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.as_mut().receiver)
            .poll(cx)
            .map(|r| match r {
                Ok(Ok(permit)) => Ok(permit),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(AcquireError::Closed),
            })
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        self.semaphore.cancel(self.id, self.queue_idx);
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(semaphore) = self.semaphore.take() {
            semaphore.return_permit(self.home_pool, self.user_pool);
        }
    }
}

impl EntryId {
    fn next(&self) -> Self {
        if self.added == Instant::now() {
            Self {
                added: self.added,
                discriminator: self.discriminator + 1,
            }
        } else {
            Self::default()
        }
    }
}

impl Default for EntryId {
    fn default() -> Self {
        Self {
            added: Instant::now(),
            discriminator: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, time::Duration};

    use assert_matches::assert_matches;

    use super::*;

    fn queue_len(semaphore: &Semaphore) -> usize {
        semaphore
            .state
            .lock()
            .unwrap()
            .pools
            .iter()
            .map(|q| q.queue.len())
            .sum()
    }

    #[tokio::test]
    async fn try_acquire() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(1)
                .build(),
        );
        {
            let p1 = semaphore.clone().try_acquire(0);
            assert!(p1.is_ok());

            let p2 = semaphore.clone().try_acquire(0);
            assert!(p2.is_err());
        }
        let p = semaphore.clone().try_acquire(0);
        assert!(p.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_timeout() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(1)
                .build(),
        );
        let p1 = semaphore.clone().try_acquire(0).unwrap();
        let acquired = Arc::new(Mutex::new(false));
        let acquire_task = tokio::spawn({
            let acquired = acquired.clone();
            let semaphore = semaphore.clone();
            async move {
                let p2 = semaphore.acquire_timeout(Duration::from_secs(2), 0).await;
                let mut a = acquired.lock().unwrap();
                *a = true;
                p2
            }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!*acquired.lock().unwrap());
        drop(p1);
        acquire_task.await.unwrap().expect("should acquire");
        assert!(*acquired.lock().unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_timeout_times_out() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(1)
                .build(),
        );
        // Acquire and hold the only permit
        let _p1 = semaphore.clone().try_acquire(0).unwrap();
        let acquire_task = tokio::spawn({
            let semaphore = semaphore.clone();
            async move {
                // Try to acquire another permit
                semaphore.acquire_timeout(Duration::from_secs(2), 0).await
            }
        });
        tokio::time::sleep(Duration::from_secs(3)).await;

        let acquire_result = acquire_task.await.unwrap();
        assert!(acquire_result.is_err());
        assert_matches!(acquire_result, Err(AcquireError::Timeout));
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_timeout_removes_from_queue_when_dropped() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(2)
                .build(),
        );

        // Acquire and hold the only permit
        let _p1 = semaphore.clone().try_acquire(0).unwrap();

        // Queue up two acquires
        let acquire_task1 = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::from_secs(1), 0).await }
        });
        let acquire_task2 = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });

        // Wait for the first acquire to time out
        let res = acquire_task1.await.unwrap();
        assert_matches!(res, Err(AcquireError::Timeout));
        assert_eq!(queue_len(&semaphore), 1);

        // Abort the second acquire
        acquire_task2.abort();
        tokio::task::yield_now().await;
        assert_eq!(queue_len(&semaphore), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_timeout_errors_when_semaphore_closed() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(1)
                .build(),
        );

        let _p1 = semaphore.clone().try_acquire(0).unwrap();
        let acquire_task = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::from_secs(1), 0).await }
        });

        tokio::time::sleep(Duration::from_millis(500)).await;
        semaphore.close();

        let res = acquire_task.await.unwrap();
        assert_matches!(res, Err(AcquireError::Closed));
    }

    #[tokio::test]
    async fn acquire_errs_when_closed() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(1)
                .build(),
        );
        semaphore.close();

        let res = semaphore.clone().try_acquire(0);
        assert_matches!(res, Err(AcquireError::Closed));

        let res = semaphore.acquire_timeout(Duration::from_secs(1), 0).await;
        assert_matches!(res, Err(AcquireError::Closed));
    }

    /// Regression test: when a waiter acquires a permit via the queue, the permit count must
    /// remain consistent (i.e. the permit is not double-returned).
    #[tokio::test(start_paused = true)]
    async fn queued_acquire_does_not_double_return_permit() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(1)
                .build(),
        );

        let p1 = semaphore.clone().try_acquire(0).unwrap();

        // Queue up a waiter
        let waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await.unwrap() }
        });

        // Release p1; the waiter should receive the permit
        drop(p1);
        let p2 = waiter.await.unwrap();

        // Only 1 permit should be in circulation: p2
        assert_eq!(semaphore.available_permits(), 0);

        // After dropping p2 we should be back to exactly 1 available permit
        drop(p2);
        assert_eq!(semaphore.available_permits(), 1);
    }

    /// Regression test: queue full error is returned at exactly the configured limit.
    #[tokio::test]
    async fn queue_full_error() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(1)
                .build(),
        );
        let _p1 = semaphore.clone().try_acquire(0).unwrap();

        // First waiter fills the queue
        let _waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });
        // Yield so the waiter actually enters the queue
        tokio::task::yield_now().await;

        // Second waiter should be rejected
        let res = semaphore.clone().acquire_timeout(Duration::ZERO, 0).await;
        assert_matches!(res, Err(AcquireError::QueueFull));
    }

    /// `set_limit` increasing wakes exactly as many queued waiters as new permits added.
    #[tokio::test(start_paused = true)]
    async fn set_limit_increase_wakes_waiters() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(3)
                .build(),
        );

        // Fill up all permits
        let _p1 = semaphore.clone().try_acquire(0).unwrap();

        // Queue up 2 waiters
        let waiter1 = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });
        let waiter2 = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(queue_len(&semaphore), 2);

        // Increase limit from 1 to 3: both waiters should be woken.
        semaphore.clone().set_limit(3);
        tokio::task::yield_now().await;

        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(queue_len(&semaphore), 0);

        assert!(waiter1.await.unwrap().is_ok());
        assert!(waiter2.await.unwrap().is_ok());
    }

    /// `set_limit` increasing with more permits than waiters puts the surplus into available.
    #[tokio::test(start_paused = true)]
    async fn set_limit_increase_surplus_goes_to_available() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(2)
                .max_queue_size(1)
                .build(),
        );

        // Hold both permits
        let _p1 = semaphore.clone().try_acquire(0).unwrap();
        let _p2 = semaphore.clone().try_acquire(0).unwrap();

        // Queue up 1 waiter
        let waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });
        tokio::task::yield_now().await;

        // Increase limit from 2 to 5: 1 goes to the waiter, 2 remain available.
        semaphore.clone().set_limit(5);
        tokio::task::yield_now().await;

        assert_eq!(semaphore.available_permits(), 2);
        assert!(waiter.await.unwrap().is_ok());
    }

    /// Returning an in-flight permit satisfies `priority_reduction`, shrinking the pool.
    #[tokio::test]
    async fn priority_reduction_consumed_on_return() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(3)
                .max_queue_size(0)
                .build(),
        );

        let p1 = semaphore.clone().try_acquire(0).unwrap();
        let p2 = semaphore.clone().try_acquire(0).unwrap();
        // 1 available, 2 in-flight

        // Decrease limit from 3 to 0: consume 1 available immediately, 2 pending
        semaphore.clone().set_limit(0);
        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(semaphore.priority_reduction(), 2);

        drop(p1); // satisfies 1 pending reduction
        assert_eq!(semaphore.priority_reduction(), 1);
        assert_eq!(semaphore.available_permits(), 0);

        drop(p2); // satisfies the last pending reduction
        assert_eq!(semaphore.priority_reduction(), 0);
        assert_eq!(semaphore.available_permits(), 0); // pool is now size 0
    }

    /// Waiters are served in FIFO order.
    #[tokio::test(start_paused = true)]
    async fn fifo_ordering() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(3)
                .build(),
        );

        let p1 = semaphore.clone().try_acquire(0).unwrap();

        let order: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

        let make_waiter = |n: u32| {
            let semaphore = semaphore.clone();
            let order = order.clone();
            tokio::spawn(async move {
                let _p = semaphore.acquire_timeout(Duration::MAX, 0).await.unwrap();
                order.lock().unwrap().push(n);
                // _p dropped here: wakes the next waiter in the queue
            })
        };

        let w1 = make_waiter(1);
        let w2 = make_waiter(2);
        let w3 = make_waiter(3);

        // Let all waiters enter the queue
        tokio::task::yield_now().await;
        assert_eq!(queue_len(&semaphore), 3);

        // Releasing p1 wakes w1; each task drops its permit on exit, waking the next.
        drop(p1);
        w1.await.unwrap();
        w2.await.unwrap();
        w3.await.unwrap();

        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }

    /// `set_limit` decrease immediately consumes available permits.
    #[tokio::test]
    async fn set_limit_decrease_consumes_available() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(5)
                .max_queue_size(0)
                .build(),
        );

        semaphore.clone().set_limit(2);

        assert_eq!(semaphore.available_permits(), 2);
        assert_eq!(
            semaphore.priority_reduction(),
            0,
            "no pending reduction when permits were available"
        );
    }

    /// `set_limit` decrease records the remainder in `priority_reduction` when not enough permits
    /// are immediately available.
    #[tokio::test]
    async fn set_limit_decrease_sets_priority_reduction() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(3)
                .max_queue_size(0)
                .build(),
        );

        let _p1 = semaphore.clone().try_acquire(0).unwrap();
        let _p2 = semaphore.clone().try_acquire(0).unwrap();
        // 1 available, 2 in-flight

        // Decrease from 3 to 0: consume 1 available, 2 pending
        semaphore.clone().set_limit(0);

        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(semaphore.priority_reduction(), 2);
    }

    /// Returning a permit satisfies `priority_reduction` before waking a queued waiter.
    #[tokio::test(start_paused = true)]
    async fn return_permit_satisfies_priority_reduction_before_queue() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(1)
                .build(),
        );

        let p1 = semaphore.clone().try_acquire(0).unwrap();

        // A waiter joins the queue
        let waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(queue_len(&semaphore), 1);

        // Record a priority reduction via set_limit decrease
        semaphore.clone().set_limit(0);

        // Return the permit: the reduction should consume it, not the waiter
        drop(p1);
        tokio::task::yield_now().await;

        assert_eq!(
            semaphore.priority_reduction(),
            0,
            "reduction should be satisfied"
        );
        assert_eq!(queue_len(&semaphore), 1, "waiter should still be in queue");

        // Clean up: the waiter will never get a permit, abort it
        waiter.abort();
        tokio::task::yield_now().await;
    }

    /// `set_limit` increase cancels pending reductions before waking waiters or adding to available.
    #[tokio::test(start_paused = true)]
    async fn set_limit_increase_cancels_pending_reductions() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(4)
                .max_queue_size(2)
                .build(),
        );

        // Hold all 4 permits
        let _p1 = semaphore.clone().try_acquire(0).unwrap();
        let _p2 = semaphore.clone().try_acquire(0).unwrap();
        let _p3 = semaphore.clone().try_acquire(0).unwrap();
        let _p4 = semaphore.clone().try_acquire(0).unwrap();

        // Decrease limit from 4 to 1: 3 pending reductions
        semaphore.clone().set_limit(1);
        assert_eq!(semaphore.priority_reduction(), 3);

        // Queue up a waiter
        let waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });
        tokio::task::yield_now().await;

        // Increase limit from 1 to 3: should cancel 2 reductions, not wake the waiter
        semaphore.clone().set_limit(3);

        assert_eq!(
            semaphore.priority_reduction(),
            1,
            "2 of 3 reductions should be cancelled"
        );
        assert_eq!(semaphore.available_permits(), 0, "no permits added to pool");
        assert_eq!(
            queue_len(&semaphore),
            1,
            "waiter should not have been woken"
        );

        waiter.abort();
        tokio::task::yield_now().await;
    }

    /// With two pools of different weights, the higher-weight waiter is served first even
    /// when the lower-weight waiter has been in the queue longer.
    #[tokio::test(start_paused = true)]
    async fn priority_ordering() {
        // distribute(20, [1.0, 10.0]) → [2, 18]. buffer(18) = 2.
        // We hold 19, leaving pool 1 with available=1 (above buffer after return).
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(20)
                .max_queue_size(2)
                .weights(vec![1.0, 10.0])
                .build(),
        );

        // Hold 19 permits: 2 from pool 0, 17 from pool 1.
        let mut held = Vec::new();
        for _ in 0..2 {
            held.push(semaphore.clone().try_acquire(0).unwrap());
        }
        for _ in 0..17 {
            held.push(semaphore.clone().try_acquire(1).unwrap());
        }
        // Pool 0: available=0. Pool 1: available=1.
        // Take the last available permit (pool 1) — this is the one we'll return.
        let p1 = semaphore.clone().try_acquire(1).unwrap();

        let order: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

        // Queue a waiter in pool 0 (weight 1.0).
        let w0 = tokio::spawn({
            let semaphore = semaphore.clone();
            let order = order.clone();
            async move {
                let _p = semaphore.acquire_timeout(Duration::MAX, 0).await.unwrap();
                order.lock().unwrap().push(0u32);
            }
        });
        tokio::task::yield_now().await;

        // Advance so w0 has been waiting 5ms.
        tokio::time::advance(Duration::from_millis(5)).await;

        // Queue a waiter in pool 1 (weight 10.0).
        let w1 = tokio::spawn({
            let semaphore = semaphore.clone();
            let order = order.clone();
            async move {
                let _p = semaphore.acquire_timeout(Duration::MAX, 1).await.unwrap();
                order.lock().unwrap().push(1u32);
            }
        });
        tokio::task::yield_now().await;

        // Advance another 5ms. Now:
        //   pool 0: weight=1.0,  waited=10ms → priority = 0.010
        //   pool 1: weight=10.0, waited=5ms  → priority = 0.050  ← wins
        tokio::time::advance(Duration::from_millis(5)).await;

        // Drop p1 (home_pool=1). Case 2: pool 1 has a waiter (w1) → wake w1.
        drop(p1);
        w1.await.unwrap();

        // w1's permit is dropped. home_pool=1, pool 1 queue empty (case 2 false).
        // Case 3: pool 1 available(0) >= buffer(18)=2 → false. Falls to case 4.
        // w0 won't be served unless we free more permits.
        // Drop one of the held permits (home_pool=1) to give pool 1 spare.
        drop(held.pop()); // returns to pool 1
        drop(held.pop()); // returns to pool 1, now available=2 >= buffer=2

        // Next return should trigger case 3 and lend to w0.
        drop(held.pop());
        w0.await.unwrap();

        assert_eq!(*order.lock().unwrap(), vec![1, 0]);
    }

    /// Timing out in a non-default sub-queue removes the entry from the correct sub-queue and
    /// leaves other sub-queues untouched.
    #[tokio::test(start_paused = true)]
    async fn cancel_targets_correct_sub_queue() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(2)
                .weights(vec![1.0, 1.0])
                .build(),
        );

        let _p1 = semaphore.clone().try_acquire(0).unwrap();

        // Queue a waiter in sub-queue 0 — should survive.
        let w0 = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });

        // Queue a waiter in sub-queue 1 with a short timeout — should be cancelled.
        let w1 = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::from_secs(1), 1).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(queue_len(&semaphore), 2);

        tokio::time::advance(Duration::from_secs(2)).await;
        assert_matches!(w1.await.unwrap(), Err(AcquireError::Timeout));

        // w1's entry should be gone; w0's entry in sub-queue 0 is untouched.
        assert_eq!(queue_len(&semaphore), 1);

        w0.abort();
        tokio::task::yield_now().await;
        assert_eq!(queue_len(&semaphore), 0);
    }

    #[test]
    fn distribute_even() {
        assert_eq!(distribute(10, &[1.0, 1.0]), vec![5, 5]);
    }

    #[test]
    fn distribute_weighted() {
        // 1:3 ratio of 10 → exact 2.5, 7.5 → floors 2, 7 → tied remainders,
        // stable sort gives the extra unit to idx 0 → [3, 7].
        assert_eq!(distribute(10, &[1.0, 3.0]), vec![3, 7]);
    }

    #[test]
    fn distribute_single() {
        assert_eq!(distribute(5, &[1.0]), vec![5]);
    }

    #[test]
    fn distribute_zero_total() {
        assert_eq!(distribute(0, &[1.0, 2.0, 3.0]), vec![0, 0, 0]);
    }

    #[test]
    fn distribute_sum_is_exact() {
        let result = distribute(7, &[1.0, 1.0, 1.0]);
        assert_eq!(result.iter().sum::<usize>(), 7);
        // Each gets at least 2, two get 3
        assert!(result.iter().all(|&v| v == 2 || v == 3));
    }
}
