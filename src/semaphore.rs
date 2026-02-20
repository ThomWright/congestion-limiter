//! A semaphore implementation.
//!
//! It aims to solve two problems with tokio's semaphore:
//!
//! 1. No limit on queue size. This can lead to an unbounded number of jobs in the system.
//! 2. No control over queue prioritisation strategy. It is always FIFO.
//!
//! This won't be as optimised as tokio's semaphore, but it should be good enough for our use case.

use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        atomic::{self, AtomicBool, AtomicUsize},
        Arc, Mutex,
    },
};

use bon::{bon, Builder};
use tokio::{sync::oneshot, time::Instant};

// Design options:
// 1. Atomics + Mutex<VecDeque>
// 2. Channels to a task with an event loop (simple usize + VecDeque)
//    Would need to put limits on the channel length...

// TODO:
// - Basic impl
// - Permit drop
// - Closed state
// - forgetting a permit
// - queue size limit
// - acquire many
// - high priority acquire (to reduce number of permits when system is under load)
// - queue prioritisation strategy

#[derive(Debug)]
pub struct Semaphore {
    available_permits: AtomicUsize,
    queue: Mutex<VecDeque<QueueEntry>>,
    max_queue_size: usize,
    // The tokio implementation has a nice optimisation where the closed state is a bitmask over
    // `available_permits` (a few bits are reserved for flags), so it can load the complete state
    // atomically.
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
    pub fn new(max_permits: usize, max_queue_size: usize) -> Self {
        Self {
            available_permits: AtomicUsize::new(max_permits),
            queue: Mutex::new(VecDeque::with_capacity(max_queue_size)),
            max_queue_size,
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

    pub async fn acquire_timeout(
        self: Arc<Self>,
        timeout: std::time::Duration,
    ) -> Result<Permit, AcquireError> {
        let mut available_permits = self.available_permits.load(atomic::Ordering::Acquire);
        loop {
            if self.closed.load(atomic::Ordering::Acquire) {
                return Err(AcquireError::Closed);
            }
            if available_permits == 0 {
                let (receiver, id) = {
                    let mut queue = self.queue.lock().expect("lock should not be poisoned");

                    // Re-check under the lock: a concurrent return_permit may have incremented
                    // available_permits after our load but before we locked the queue. If so,
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

                    if queue.len() >= self.max_queue_size {
                        return Err(AcquireError::QueueFull);
                    }
                    let (sender, receiver) = oneshot::channel();

                    let id = match queue.back() {
                        Some(entry) => entry.id.next(),
                        _ => EntryId::default(),
                    };
                    queue.push_back(QueueEntry { id, sender });
                    drop(queue);

                    (receiver, id)
                };

                let acquire = Acquire::new(Arc::clone(&self), receiver, id);
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

    fn return_permit(self: Arc<Self>) {
        let mut queue = self.queue.lock().expect("lock should not be poisoned");
        if let Some(entry) = queue.pop_front() {
            drop(queue);

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

    pub fn add_permits(self: Arc<Self>, n: usize) {
        // Wake up to n queued waiters, then add any remaining to available_permits.
        let mut queue = self.queue.lock().expect("lock should not be poisoned");
        let to_wake = n.min(queue.len());
        for _ in 0..to_wake {
            let entry = queue.pop_front().expect("just checked len");
            let _ = entry.sender.send(Ok(Permit {
                semaphore: Some(Arc::clone(&self)),
            }));
        }
        let remaining = n - to_wake;
        if remaining > 0 {
            self.available_permits
                .fetch_add(remaining, atomic::Ordering::Release);
        }
    }

    pub async fn acquire_many(self: Arc<Self>, n: u32) -> Result<Vec<Permit>, AcquireError> {
        let mut permits = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let permit = Arc::clone(&self)
                .acquire_timeout(std::time::Duration::MAX)
                .await?;
            permits.push(permit);
        }
        Ok(permits)
    }

    fn cancel(&self, id: EntryId) {
        let mut queue = self.queue.lock().expect("lock should not be poisoned");
        if let Ok(index) = queue.binary_search_by_key(&id, |entry| entry.id) {
            queue.remove(index);
            drop(queue);
        }
    }

    fn close(&self) {
        self.closed.store(true, atomic::Ordering::Release);

        let mut queue = self.queue.lock().expect("lock should not be poisoned");
        for entry in queue.drain(..) {
            let _ = entry.sender.send(Err(AcquireError::Closed));
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
    ) -> Self {
        Self {
            id,
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
        self.semaphore.cancel(self.id);
    }
}

impl Permit {
    /// Forget the permit without returning it to the semaphore.
    ///
    /// This permanently removes a permit from the pool, reducing the effective limit by 1.
    pub fn forget(mut self) {
        self.semaphore = None;
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

    #[tokio::test]
    async fn try_acquire() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .max_permits(1)
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
                .max_permits(1)
                .max_queue_size(1)
                .build(),
        );
        let p1 = semaphore.clone().try_acquire().unwrap();
        let acquired = Arc::new(Mutex::new(false));
        let acquire_task = tokio::spawn({
            let acquired = acquired.clone();
            let semaphore = semaphore.clone();
            async move {
                let p2 = semaphore.acquire_timeout(Duration::from_secs(2)).await;
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
                .max_permits(1)
                .max_queue_size(1)
                .build(),
        );
        // Acquire and hold the only permit
        let _p1 = semaphore.clone().try_acquire().unwrap();
        let acquire_task = tokio::spawn({
            let semaphore = semaphore.clone();
            async move {
                // Try to acquire another permit
                semaphore.acquire_timeout(Duration::from_secs(2)).await
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
                .max_permits(1)
                .max_queue_size(2)
                .build(),
        );

        // Acquire and hold the only permit
        let _p1 = semaphore.clone().try_acquire().unwrap();

        // Queue up two acquires
        let acquire_task1 = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::from_secs(1)).await }
        });
        let acquire_task2 = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX).await }
        });

        // Wait for the first acquire to time out
        let res = acquire_task1.await.unwrap();
        assert_matches!(res, Err(AcquireError::Timeout));
        assert_eq!(semaphore.queue.lock().unwrap().len(), 1);

        // Abort the second acquire
        acquire_task2.abort();
        tokio::task::yield_now().await;
        assert_eq!(semaphore.queue.lock().unwrap().len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_timeout_errors_when_semaphore_closed() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .max_permits(1)
                .max_queue_size(1)
                .build(),
        );

        let _p1 = semaphore.clone().try_acquire().unwrap();
        let acquire_task = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::from_secs(1)).await }
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
                .max_permits(1)
                .max_queue_size(1)
                .build(),
        );
        semaphore.close();

        let res = semaphore.clone().try_acquire();
        assert_matches!(res, Err(AcquireError::Closed));

        let res = semaphore.acquire_timeout(Duration::from_secs(1)).await;
        assert_matches!(res, Err(AcquireError::Closed));
    }

    /// Regression test: when a waiter acquires a permit via the queue, the permit count must
    /// remain consistent (i.e. the permit is not double-returned).
    #[tokio::test(start_paused = true)]
    async fn queued_acquire_does_not_double_return_permit() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .max_permits(1)
                .max_queue_size(1)
                .build(),
        );

        let p1 = semaphore.clone().try_acquire().unwrap();

        // Queue up a waiter
        let waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX).await.unwrap() }
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
                .max_permits(1)
                .max_queue_size(1)
                .build(),
        );
        let _p1 = semaphore.clone().try_acquire().unwrap();

        // First waiter fills the queue
        let _waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX).await }
        });
        // Yield so the waiter actually enters the queue
        tokio::task::yield_now().await;

        // Second waiter should be rejected
        let res = semaphore.clone().acquire_timeout(Duration::ZERO).await;
        assert_matches!(res, Err(AcquireError::QueueFull));
    }

    /// `add_permits` wakes exactly as many queued waiters as new permits added.
    #[tokio::test(start_paused = true)]
    async fn add_permits_wakes_waiters() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .max_permits(1)
                .max_queue_size(3)
                .build(),
        );

        // Fill up all permits
        let _p1 = semaphore.clone().try_acquire().unwrap();

        // Queue up 2 waiters
        let waiter1 = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX).await }
        });
        let waiter2 = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(semaphore.queue.lock().unwrap().len(), 2);

        // Add 2 permits: both waiters should be woken, nothing left in available.
        semaphore.clone().add_permits(2);
        tokio::task::yield_now().await;

        // Check while tasks still hold their permits (before awaiting them, which drops the
        // permits and returns them to the pool).
        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(semaphore.queue.lock().unwrap().len(), 0);

        assert!(waiter1.await.unwrap().is_ok());
        assert!(waiter2.await.unwrap().is_ok());
    }

    /// `add_permits` with more permits than waiters puts the surplus into available_permits.
    #[tokio::test(start_paused = true)]
    async fn add_permits_surplus_goes_to_available() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .max_permits(2)
                .max_queue_size(1)
                .build(),
        );

        // Hold both permits
        let _p1 = semaphore.clone().try_acquire().unwrap();
        let _p2 = semaphore.clone().try_acquire().unwrap();

        // Queue up 1 waiter
        let waiter = tokio::spawn({
            let semaphore = semaphore.clone();
            async move { semaphore.acquire_timeout(Duration::MAX).await }
        });
        tokio::task::yield_now().await;

        // Add 3 permits: 1 goes to the waiter, 2 remain available.
        semaphore.clone().add_permits(3);
        tokio::task::yield_now().await;

        // Check while waiter still holds its permit.
        assert_eq!(semaphore.available_permits(), 2);
        assert!(waiter.await.unwrap().is_ok());
    }

    /// Forgetting a permit permanently removes it from the pool.
    #[tokio::test]
    async fn forget_reduces_pool_size() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .max_permits(3)
                .max_queue_size(0)
                .build(),
        );

        assert_eq!(semaphore.available_permits(), 3);

        let p = semaphore.clone().try_acquire().unwrap();
        p.forget();

        assert_eq!(semaphore.available_permits(), 2);

        // Confirm the remaining permits still work normally
        let p2 = semaphore.clone().try_acquire().unwrap();
        drop(p2);
        assert_eq!(semaphore.available_permits(), 2);
    }

    /// `acquire_many` acquires exactly n permits.
    #[tokio::test]
    async fn acquire_many_acquires_n_permits() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .max_permits(5)
                .max_queue_size(0)
                .build(),
        );

        let permits = semaphore.clone().acquire_many(3).await.unwrap();
        assert_eq!(permits.len(), 3);
        assert_eq!(semaphore.available_permits(), 2);

        drop(permits);
        assert_eq!(semaphore.available_permits(), 5);
    }

    /// `acquire_many` blocks until all permits are available.
    #[tokio::test(start_paused = true)]
    async fn acquire_many_waits_for_permits() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .max_permits(2)
                .max_queue_size(3)
                .build(),
        );

        // Hold both permits
        let p1 = semaphore.clone().try_acquire().unwrap();
        let p2 = semaphore.clone().try_acquire().unwrap();

        let done = Arc::new(Mutex::new(false));
        let task = tokio::spawn({
            let semaphore = semaphore.clone();
            let done = done.clone();
            async move {
                let permits = semaphore.acquire_many(2).await.unwrap();
                *done.lock().unwrap() = true;
                permits
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!*done.lock().unwrap(), "should still be waiting");

        drop(p1);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!*done.lock().unwrap(), "still waiting for second permit");

        drop(p2);
        let permits = task.await.unwrap();
        assert!(*done.lock().unwrap());
        assert_eq!(permits.len(), 2);
    }

    /// Waiters are served in FIFO order.
    #[tokio::test(start_paused = true)]
    async fn fifo_ordering() {
        let semaphore = Arc::new(
            Semaphore::builder()
                .max_permits(1)
                .max_queue_size(3)
                .build(),
        );

        let p1 = semaphore.clone().try_acquire().unwrap();

        let order: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

        let make_waiter = |n: u32| {
            let semaphore = semaphore.clone();
            let order = order.clone();
            tokio::spawn(async move {
                let _p = semaphore.acquire_timeout(Duration::MAX).await.unwrap();
                order.lock().unwrap().push(n);
                // _p dropped here: wakes the next waiter in the queue
            })
        };

        let w1 = make_waiter(1);
        let w2 = make_waiter(2);
        let w3 = make_waiter(3);

        // Let all waiters enter the queue
        tokio::task::yield_now().await;
        assert_eq!(semaphore.queue.lock().unwrap().len(), 3);

        // Releasing p1 wakes w1; each task drops its permit on exit, waking the next.
        drop(p1);
        w1.await.unwrap();
        w2.await.unwrap();
        w3.await.unwrap();

        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }
}
