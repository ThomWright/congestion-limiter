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

use bon::Builder;
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

#[derive(Debug, Builder)]
pub struct Options {
    max_permits: usize,
    max_queue_size: usize,
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

impl Semaphore {
    pub fn new(options: Options) -> Self {
        Self {
            available_permits: AtomicUsize::new(options.max_permits),
            queue: Mutex::new(VecDeque::with_capacity(options.max_queue_size)),
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
                    if queue.len() == queue.capacity() {
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
                    Ok(Ok(_)) => {
                        return Ok(Permit {
                            semaphore: Some(Arc::clone(&self)),
                        });
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
            .map(|r| {
                match r {
                    Ok(Ok(permit)) => Ok(permit),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(AcquireError::Closed),
                }
            })
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        self.semaphore.cancel(self.id);
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.semaphore
            .take()
            .expect("semaphore should exist until dropped")
            .return_permit();
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
        let semaphore = Arc::new(Semaphore::new(
            Options::builder().max_permits(1).max_queue_size(1).build(),
        ));
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
        let semaphore = Arc::new(Semaphore::new(
            Options::builder().max_permits(1).max_queue_size(1).build(),
        ));
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
        let semaphore = Arc::new(Semaphore::new(
            Options::builder().max_permits(1).max_queue_size(1).build(),
        ));
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
        let semaphore = Arc::new(Semaphore::new(
            Options::builder().max_permits(1).max_queue_size(2).build(),
        ));

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
        let semaphore = Arc::new(Semaphore::new(
            Options::builder().max_permits(1).max_queue_size(1).build(),
        ));

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
        let semaphore = Arc::new(Semaphore::new(
            Options::builder().max_permits(1).max_queue_size(1).build(),
        ));
        semaphore.close();

        let res = semaphore.clone().try_acquire();
        assert_matches!(res, Err(AcquireError::Closed));

        let res = semaphore.acquire_timeout(Duration::from_secs(1)).await;
        assert_matches!(res, Err(AcquireError::Closed));
    }
}
