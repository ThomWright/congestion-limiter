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
    // Per-pool fields, unused until Phase 2.
    #[allow(dead_code)]
    limit: usize,
    #[allow(dead_code)]
    available: usize,
    #[allow(dead_code)]
    in_flight: usize,
    #[allow(dead_code)]
    priority_reduction: usize,
}

#[derive(Debug)]
struct SemaphoreState {
    available: usize,
    priority_reduction: usize,
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
        /// Maximum queue size for each sub-queue. The total max queue size is `max_queue_size * weights.len()`.
        max_queue_size: usize,
        /// Weights for each sub-queue. Defaults to `[1.0]`.
        #[builder(default = vec![1.0])]
        weights: Vec<f64>,
    ) -> Self {
        assert!(!weights.is_empty(), "weights must not be empty");

        // Avoid unbounded memory usage for very large max_queue_size.
        let cap_per_queue = max_queue_size.min(64);
        let total_max_queue_size = max_queue_size.saturating_mul(weights.len());
        Self {
            state: Mutex::new(SemaphoreState {
                available: initial_permits,
                priority_reduction: 0,
                pools: weights
                    .into_iter()
                    .map(|w| Pool {
                        weight: w,
                        queue: VecDeque::with_capacity(cap_per_queue),
                        limit: 0,
                        available: 0,
                        in_flight: 0,
                        priority_reduction: 0,
                    })
                    .collect(),
            }),
            max_queue_size: total_max_queue_size,
            closed: AtomicBool::new(false),
        }
    }

    pub fn try_acquire(self: Arc<Self>) -> Result<Permit, AcquireError> {
        if self.closed.load(atomic::Ordering::Acquire) {
            return Err(AcquireError::Closed);
        }

        let mut state = self.state.lock().expect("lock should not be poisoned");
        if state.available == 0 {
            return Err(AcquireError::NoPermits);
        }
        state.available -= 1;
        drop(state);

        Ok(Permit {
            semaphore: Some(Arc::clone(&self)),
            home_pool: 0,
            user_pool: 0,
        })
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

            if state.available > 0 {
                state.available -= 1;
                return Ok(Permit {
                    semaphore: Some(Arc::clone(&self)),
                    home_pool: 0,
                    user_pool: 0,
                });
            }

            let total_len: usize = state.pools.iter().map(|q| q.queue.len()).sum();
            if total_len >= self.max_queue_size {
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

    /// Dequeue the highest-priority entry across all sub-queues.
    ///
    /// Priority = `weight * time_in_queue`. Scans the front of each sub-queue (O(p)) and pops
    /// from the winner. FIFO tiebreak: older entry (lower `EntryId`) wins.
    fn dequeue_highest_priority(pools: &mut [Pool]) -> Option<QueueEntry> {
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

        pools[best_idx].queue.pop_front()
    }

    fn return_permit(self: Arc<Self>) {
        let mut state = self.state.lock().expect("lock should not be poisoned");

        if state.priority_reduction > 0 {
            // Consume the permit to satisfy a pending reduction.
            state.priority_reduction -= 1;
            return;
        }

        if let Some(entry) = Self::dequeue_highest_priority(&mut state.pools) {
            drop(state);
            let _ = entry.sender.send(Ok(Permit {
                semaphore: Some(Arc::clone(&self)),
                home_pool: 0,
                user_pool: 0,
            }));
        } else {
            state.available += 1;
        }
    }

    pub fn available_permits(&self) -> usize {
        self.state.lock().expect("lock should not be poisoned").available
    }

    /// Increase the number of permits by `n`.
    ///
    /// Cancels pending reductions first, then wakes queued waiters, then adds to the available
    /// pool.
    pub fn add_permits(self: Arc<Self>, n: usize) {
        let mut state = self.state.lock().expect("lock should not be poisoned");
        let mut to_add = n;

        // Cancel pending reductions before giving permits to waiters or the pool.
        let cancel = to_add.min(state.priority_reduction);
        state.priority_reduction -= cancel;
        to_add -= cancel;

        if to_add == 0 {
            return;
        }

        // Wake up to to_add queued waiters.
        let total_len: usize = state.pools.iter().map(|q| q.queue.len()).sum();
        let to_wake = to_add.min(total_len);
        let mut woken = Vec::with_capacity(to_wake);
        for _ in 0..to_wake {
            let entry = Self::dequeue_highest_priority(&mut state.pools)
                .expect("total_len > 0 implies at least one sub-queue has an entry");
            woken.push(entry);
        }

        // Add any remaining to available.
        let remaining = to_add - to_wake;
        state.available += remaining;
        drop(state);

        // Send permits to woken waiters outside the lock.
        for entry in woken {
            let _ = entry.sender.send(Ok(Permit {
                semaphore: Some(Arc::clone(&self)),
                home_pool: 0,
                user_pool: 0,
            }));
        }
    }

    /// Decrease the number of permits by `n`.
    ///
    /// Consumes available permits immediately; any remainder is recorded in
    /// `priority_reduction` and consumed as in-flight permits are returned,
    /// taking priority over queued waiters.
    pub fn decrease_permits(&self, n: usize) {
        let mut state = self.state.lock().expect("lock should not be poisoned");
        let consume = n.min(state.available);
        state.available -= consume;
        state.priority_reduction += n - consume;
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

    pub(crate) fn pool_count(&self) -> usize {
        self.state
            .lock()
            .expect("lock should not be poisoned")
            .pools
            .len()
    }

    #[cfg(test)]
    fn priority_reduction(&self) -> usize {
        self.state.lock().expect("lock should not be poisoned").priority_reduction
    }
}

/// Distribute `total` across `weights.len()` buckets proportional to each weight,
/// using the largest remainder method so the sum is exactly `total`.
pub(crate) fn distribute(total: usize, weights: &[f64]) -> Vec<usize> {
    assert!(!weights.is_empty());
    let sum: f64 = weights.iter().sum();
    assert!(sum > 0.0, "weights must sum to a positive value");

    let exact: Vec<f64> = weights.iter().map(|w| (w / sum) * total as f64).collect();
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

impl Permit {
    pub fn home_pool(&self) -> usize {
        self.home_pool
    }

    pub fn user_pool(&self) -> usize {
        self.user_pool
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(semaphore) = self.semaphore.take() {
            semaphore.return_permit();
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
            let p1 = semaphore.clone().try_acquire();
            assert!(p1.is_ok());

            let p2 = semaphore.clone().try_acquire();
            assert!(p2.is_err());
        }
        let p = semaphore.try_acquire();
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
        let p1 = semaphore.clone().try_acquire().unwrap();
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
        let _p1 = semaphore.clone().try_acquire().unwrap();
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
        let _p1 = semaphore.clone().try_acquire().unwrap();

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

        let _p1 = semaphore.clone().try_acquire().unwrap();
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

        let res = semaphore.clone().try_acquire();
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

        let p1 = semaphore.clone().try_acquire().unwrap();

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
        let _p1 = semaphore.clone().try_acquire().unwrap();

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

    /// `add_permits` wakes exactly as many queued waiters as new permits added.
    #[tokio::test(start_paused = true)]
    async fn add_permits_wakes_waiters() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(3)
                .build(),
        );

        // Fill up all permits
        let _p1 = semaphore.clone().try_acquire().unwrap();

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

        // Add 2 permits: both waiters should be woken, nothing left in available.
        semaphore.clone().add_permits(2);
        tokio::task::yield_now().await;

        // Check while tasks still hold their permits (before awaiting them, which drops the
        // permits and returns them to the pool).
        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(queue_len(&semaphore), 0);

        assert!(waiter1.await.unwrap().is_ok());
        assert!(waiter2.await.unwrap().is_ok());
    }

    /// `add_permits` with more permits than waiters puts the surplus into available_permits.
    #[tokio::test(start_paused = true)]
    async fn add_permits_surplus_goes_to_available() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(2)
                .max_queue_size(1)
                .build(),
        );

        // Hold both permits
        let _p1 = semaphore.clone().try_acquire().unwrap();
        let _p2 = semaphore.clone().try_acquire().unwrap();

        // Queue up 1 waiter
        let waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });
        tokio::task::yield_now().await;

        // Add 3 permits: 1 goes to the waiter, 2 remain available.
        semaphore.clone().add_permits(3);
        tokio::task::yield_now().await;

        // Check while waiter still holds its permit.
        assert_eq!(semaphore.available_permits(), 2);
        assert!(waiter.await.unwrap().is_ok());
    }

    /// Returning an in-flight permit satisfies `priority_reduction`, shrinking the pool.
    #[tokio::test]
    async fn decrease_permits_consumed_as_in_flight_return() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(3)
                .max_queue_size(0)
                .build(),
        );

        let p1 = semaphore.clone().try_acquire().unwrap();
        let p2 = semaphore.clone().try_acquire().unwrap();
        // 1 available, 2 in-flight

        semaphore.decrease_permits(3); // consume 1 available immediately, 2 pending
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

        let p1 = semaphore.clone().try_acquire().unwrap();

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

    /// `decrease_permits` immediately consumes available permits.
    #[tokio::test]
    async fn decrease_permits_consumes_available_immediately() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(5)
                .max_queue_size(0)
                .build(),
        );

        semaphore.decrease_permits(3);

        assert_eq!(semaphore.available_permits(), 2);
        assert_eq!(
            semaphore.priority_reduction(),
            0,
            "no pending reduction when permits were available"
        );
    }

    /// `decrease_permits` records the remainder in `priority_reduction` when not enough permits
    /// are immediately available.
    #[tokio::test]
    async fn decrease_permits_sets_priority_reduction_for_remainder() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(3)
                .max_queue_size(0)
                .build(),
        );

        let _p1 = semaphore.clone().try_acquire().unwrap();
        let _p2 = semaphore.clone().try_acquire().unwrap();
        // 1 available, 2 in-flight

        semaphore.decrease_permits(4); // want to take 4, only 1 available

        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(semaphore.priority_reduction(), 3);
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

        let p1 = semaphore.clone().try_acquire().unwrap();

        // A waiter joins the queue
        let waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(queue_len(&semaphore), 1);

        // Record a priority reduction via decrease_permits
        semaphore.decrease_permits(1);

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

    /// `add_permits` cancels pending reductions rather than giving permits to waiters or the pool.
    #[tokio::test(start_paused = true)]
    async fn add_permits_cancels_pending_reductions() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(4)
                .max_queue_size(2)
                .build(),
        );

        // Hold all 4 permits
        let _p1 = semaphore.clone().try_acquire().unwrap();
        let _p2 = semaphore.clone().try_acquire().unwrap();
        let _p3 = semaphore.clone().try_acquire().unwrap();
        let _p4 = semaphore.clone().try_acquire().unwrap();

        // Record 3 pending reductions (e.g. from a limit decrease)
        semaphore.decrease_permits(3);
        assert_eq!(semaphore.priority_reduction(), 3);

        // Queue up a waiter
        let waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });
        tokio::task::yield_now().await;

        // Add 2 permits: should cancel 2 reductions, not wake the waiter
        semaphore.clone().add_permits(2);

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

    /// With two sub-queues of different weights, the higher-weight waiter is served first even
    /// when the lower-weight waiter has been in the queue longer.
    #[tokio::test(start_paused = true)]
    async fn priority_ordering() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .initial_permits(1)
                .max_queue_size(2)
                .weights(vec![1.0, 10.0])
                .build(),
        );

        let p1 = semaphore.clone().try_acquire().unwrap();

        let order: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

        // Queue a waiter in sub-queue 0 (weight 1.0).
        let w0 = tokio::spawn({
            let semaphore = semaphore.clone();
            let order = order.clone();
            async move {
                let _p = semaphore.acquire_timeout(Duration::MAX, 0).await.unwrap();
                order.lock().unwrap().push(0u32);
                // _p dropped here, waking the next waiter
            }
        });
        tokio::task::yield_now().await;

        // Advance so w0 has been waiting 5ms.
        tokio::time::advance(Duration::from_millis(5)).await;

        // Queue a waiter in sub-queue 1 (weight 10.0).
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
        //   sub-queue 0: weight=1.0,  waited=10ms → priority = 0.010
        //   sub-queue 1: weight=10.0, waited=5ms  → priority = 0.050  ← wins
        tokio::time::advance(Duration::from_millis(5)).await;

        drop(p1);
        w1.await.unwrap();
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

        let _p1 = semaphore.clone().try_acquire().unwrap();

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
