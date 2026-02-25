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
        atomic::{self, AtomicBool, AtomicUsize},
        Arc, Mutex,
    },
};

use bon::bon;
use tokio::{sync::oneshot, time::Instant};

#[derive(Debug)]
struct WeightedSubQueue {
    weight: f64,
    entries: VecDeque<QueueEntry>,
}

#[derive(Debug)]
pub struct Semaphore {
    available_permits: AtomicUsize,
    queues: Mutex<Vec<WeightedSubQueue>>,
    total_max_queue_size: usize,
    /// Number of returning permits to silently consume rather than return to the pool or wake a
    /// waiter. Checked before the queue in [`Semaphore::return_permit`], so these take priority
    /// over queued waiters.
    priority_reduction: AtomicUsize,
    // The tokio implementation has a nice optimisation where the closed state is a bitmask over
    // `available_permits` (a few bits are reserved for flags), so it can load the complete state
    // atomically.
    // TODO: would this be useful for us?
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
        Self {
            available_permits: AtomicUsize::new(initial_permits),
            total_max_queue_size: max_queue_size * weights.len(),
            queues: Mutex::new(
                weights
                    .into_iter()
                    .map(|w| WeightedSubQueue {
                        weight: w,
                        entries: VecDeque::with_capacity(cap_per_queue),
                    })
                    .collect(),
            ),
            priority_reduction: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }
    }

    pub fn try_acquire(self: Arc<Self>) -> Result<Permit, AcquireError> {
        let mut available_permits = self.available_permits.load(atomic::Ordering::Acquire);
        loop {
            if self.closed.load(atomic::Ordering::Acquire) {
                return Err(AcquireError::Closed);
            }
            if available_permits == 0 {
                return Err(AcquireError::NoPermits);
            }
            match self.available_permits.compare_exchange(
                available_permits,
                available_permits - 1,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Permit {
                        semaphore: Some(Arc::clone(&self)),
                    });
                }
                Err(permits) => {
                    available_permits = permits;
                }
            }
        }
    }

    pub(crate) async fn acquire_timeout(
        self: Arc<Self>,
        timeout: std::time::Duration,
        queue_idx: usize,
    ) -> Result<Permit, AcquireError> {
        let mut available_permits = self.available_permits.load(atomic::Ordering::Acquire);
        loop {
            if self.closed.load(atomic::Ordering::Acquire) {
                return Err(AcquireError::Closed);
            }
            if available_permits == 0 {
                let (receiver, id) = {
                    let mut queues = self.queues.lock().expect("lock should not be poisoned");

                    // Re-check under the lock: a concurrent return_permit may have incremented
                    // available_permits after our load but before we locked the queues. If so,
                    // grab the permit directly without queuing.
                    let mut current = self.available_permits.load(atomic::Ordering::Acquire);
                    loop {
                        if current == 0 {
                            break;
                        }
                        match self.available_permits.compare_exchange(
                            current,
                            current - 1,
                            atomic::Ordering::AcqRel,
                            atomic::Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                return Ok(Permit {
                                    semaphore: Some(Arc::clone(&self)),
                                });
                            }
                            Err(v) => current = v,
                        }
                    }

                    let total_len: usize = queues.iter().map(|q| q.entries.len()).sum();
                    if total_len >= self.total_max_queue_size {
                        return Err(AcquireError::QueueFull);
                    }
                    let (sender, receiver) = oneshot::channel();

                    let sub = &mut queues[queue_idx];
                    let id = match sub.entries.back() {
                        Some(entry) => entry.id.next(),
                        _ => EntryId::default(),
                    };
                    sub.entries.push_back(QueueEntry { id, sender });
                    drop(queues);

                    (receiver, id)
                };

                let acquire = Acquire::new(Arc::clone(&self), receiver, id, queue_idx);
                match tokio::time::timeout(timeout, acquire).await {
                    Ok(Ok(permit)) => {
                        return Ok(permit);
                    }
                    Ok(Err(e)) => {
                        return Err(e);
                    }
                    Err(_elapsed) => {
                        return Err(AcquireError::Timeout);
                    }
                }
            }
            match self.available_permits.compare_exchange(
                available_permits,
                available_permits - 1,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Permit {
                        semaphore: Some(Arc::clone(&self)),
                    });
                }
                Err(permits) => {
                    available_permits = permits;
                }
            }
        }
    }

    /// Dequeue the highest-priority entry across all sub-queues.
    ///
    /// Priority = `weight * time_in_queue`. Scans the front of each sub-queue (O(p)) and pops
    /// from the winner. FIFO tiebreak: older entry (lower `EntryId`) wins.
    fn dequeue_highest_priority(queues: &mut [WeightedSubQueue]) -> Option<QueueEntry> {
        let best_idx = queues
            .iter()
            .enumerate()
            .filter_map(|(qi, sub)| sub.entries.front().map(|e| (qi, sub.weight, e)))
            .max_by(|(_, wa, a), (_, wb, b)| {
                let pa = wa * a.id.added.elapsed().as_secs_f64();
                let pb = wb * b.id.added.elapsed().as_secs_f64();
                pa.partial_cmp(&pb)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| b.id.cmp(&a.id)) // FIFO tiebreak: lower id = older = wins
            })
            .map(|(qi, _, _)| qi)?;
        queues[best_idx].entries.pop_front()
    }

    fn return_permit(self: Arc<Self>) {
        // Priority: satisfy pending reductions before waking queued waiters.
        let mut pending = self.priority_reduction.load(atomic::Ordering::Acquire);
        while pending > 0 {
            match self.priority_reduction.compare_exchange(
                pending,
                pending - 1,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            ) {
                Ok(_) => return, // permit consumed; not returned to pool or queue
                Err(v) => pending = v,
            }
        }

        let mut queues = self.queues.lock().expect("lock should not be poisoned");
        if let Some(entry) = Self::dequeue_highest_priority(&mut queues) {
            drop(queues);

            let _ = entry.sender.send(Ok(Permit {
                semaphore: Some(Arc::clone(&self)),
            }));
        } else {
            self.available_permits
                .fetch_add(1, atomic::Ordering::Release);
        }
    }

    pub fn available_permits(&self) -> usize {
        self.available_permits.load(atomic::Ordering::Acquire)
    }

    /// Increase the number of permits by `n`.
    ///
    /// Cancels pending reductions first, then wakes queued waiters, then adds to the available
    /// pool.
    pub fn add_permits(self: Arc<Self>, n: usize) {
        let mut to_add = n;

        // Cancel pending reductions before giving permits to waiters or the pool.
        let mut pending = self.priority_reduction.load(atomic::Ordering::Acquire);
        while pending > 0 && to_add > 0 {
            let to_cancel = to_add.min(pending);
            match self.priority_reduction.compare_exchange(
                pending,
                pending - to_cancel,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            ) {
                Ok(_) => {
                    to_add -= to_cancel;
                    pending -= to_cancel;
                }
                Err(v) => pending = v,
            }
        }

        if to_add == 0 {
            return;
        }

        // Wake up to to_add queued waiters, then add any remaining to available_permits.
        let mut queues = self.queues.lock().expect("lock should not be poisoned");
        let total_len: usize = queues.iter().map(|q| q.entries.len()).sum();
        let to_wake = to_add.min(total_len);
        for _ in 0..to_wake {
            let entry = Self::dequeue_highest_priority(&mut queues)
                .expect("total_len > 0 implies at least one sub-queue has an entry");
            let _ = entry.sender.send(Ok(Permit {
                semaphore: Some(Arc::clone(&self)),
            }));
        }
        let remaining = to_add - to_wake;
        if remaining > 0 {
            self.available_permits
                .fetch_add(remaining, atomic::Ordering::Release);
        }
    }

    /// Decrease the number of permits by `n`.
    ///
    /// Consumes available permits immediately; any remainder is recorded in [`priority_reduction`]
    /// and consumed as in-flight permits are returned, taking priority over queued waiters.
    pub fn decrease_permits(&self, n: usize) {
        let mut remaining = n;

        // Immediately consume as many available permits as possible.
        let mut available = self.available_permits.load(atomic::Ordering::Acquire);
        while remaining > 0 && available > 0 {
            let to_take = remaining.min(available);
            match self.available_permits.compare_exchange(
                available,
                available - to_take,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            ) {
                Ok(_) => {
                    remaining -= to_take;
                    available -= to_take;
                }
                Err(v) => available = v,
            }
        }

        // Schedule the rest to be consumed as in-flight permits are returned.
        if remaining > 0 {
            self.priority_reduction
                .fetch_add(remaining, atomic::Ordering::Release);
        }
    }

    fn cancel(&self, id: EntryId, queue_idx: usize) {
        let mut queues = self.queues.lock().expect("lock should not be poisoned");
        let entries = &mut queues[queue_idx].entries;
        if let Ok(index) = entries.binary_search_by_key(&id, |entry| entry.id) {
            entries.remove(index);
        }
    }

    fn close(&self) {
        self.closed.store(true, atomic::Ordering::Release);

        let mut queues = self.queues.lock().expect("lock should not be poisoned");
        for sub in queues.iter_mut() {
            for entry in sub.entries.drain(..) {
                let _ = entry.sender.send(Err(AcquireError::Closed));
            }
        }
    }
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
            .queues
            .lock()
            .unwrap()
            .iter()
            .map(|q| q.entries.len())
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
        assert_eq!(
            semaphore.priority_reduction.load(atomic::Ordering::Acquire),
            2
        );

        drop(p1); // satisfies 1 pending reduction
        assert_eq!(
            semaphore.priority_reduction.load(atomic::Ordering::Acquire),
            1
        );
        assert_eq!(semaphore.available_permits(), 0);

        drop(p2); // satisfies the last pending reduction
        assert_eq!(
            semaphore.priority_reduction.load(atomic::Ordering::Acquire),
            0
        );
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
            semaphore.priority_reduction.load(atomic::Ordering::Acquire),
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
        assert_eq!(
            semaphore.priority_reduction.load(atomic::Ordering::Acquire),
            3
        );
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

        // Record a priority reduction
        semaphore
            .priority_reduction
            .store(1, atomic::Ordering::Release);

        // Return the permit: the reduction should consume it, not the waiter
        drop(p1);
        tokio::task::yield_now().await;

        assert_eq!(
            semaphore.priority_reduction.load(atomic::Ordering::Acquire),
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
        assert_eq!(
            semaphore.priority_reduction.load(atomic::Ordering::Acquire),
            3
        );

        // Queue up a waiter
        let waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX, 0).await }
        });
        tokio::task::yield_now().await;

        // Add 2 permits: should cancel 2 reductions, not wake the waiter
        semaphore.clone().add_permits(2);

        assert_eq!(
            semaphore.priority_reduction.load(atomic::Ordering::Acquire),
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
}
