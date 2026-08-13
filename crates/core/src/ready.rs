//! Callback queues (R-022, R-054).
//!
//! Two queues feed the dispatcher:
//!
//! * [`ReadyQueue`] — loop-thread-only FIFO of pending callbacks
//!   (`call_soon`, completion callbacks). Drained in batches of
//!   `DISPATCH_BATCH` between I/O polls (R-054).
//! * [`CrossThreadQueue`] — lock-free MPSC handoff used by
//!   `call_soon_threadsafe` (R-022). Producers are arbitrary threads; the
//!   single consumer is the loop thread, which drains it into the
//!   `ReadyQueue` at the top of each tick after a backend wakeup.
//!
//! `crossbeam-queue`'s `SegQueue` provides the lock-free MPSC (it is MPMC,
//! used here single-consumer; R-022 allows crossbeam explicitly).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_queue::SegQueue;

/// R-054: callbacks dispatched per drain between I/O polls.
pub const DISPATCH_BATCH: usize = 128;

pub struct ReadyQueue<T> {
    queue: VecDeque<T>,
}

impl<T> Default for ReadyQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ReadyQueue<T> {
    pub fn new() -> Self {
        ReadyQueue { queue: VecDeque::with_capacity(DISPATCH_BATCH * 2) }
    }

    #[inline]
    pub fn push(&mut self, item: T) {
        self.queue.push_back(item);
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    /// Return an item to the FRONT (undo of `pop` — exceptional unwind
    /// paths that must preserve FIFO order).
    #[inline]
    pub fn push_front(&mut self, item: T) {
        self.queue.push_front(item);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

/// Lock-free MPSC handoff for cross-thread `call_soon_threadsafe` (R-022).
///
/// `push` returns `true` when the caller must also wake the backend (the
/// flag was not already set) — this collapses N producer wakeups into one
/// kernel post per quiescent period.
pub struct CrossThreadQueue<T> {
    queue: SegQueue<T>,
    wake_pending: AtomicBool,
}

impl<T> Default for CrossThreadQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CrossThreadQueue<T> {
    pub fn new() -> Self {
        CrossThreadQueue { queue: SegQueue::new(), wake_pending: AtomicBool::new(false) }
    }

    /// Enqueue from any thread. Returns `true` if the producer should post a
    /// backend wakeup (first producer since the last drain).
    #[must_use]
    pub fn push(&self, item: T) -> bool {
        self.queue.push(item);
        !self.wake_pending.swap(true, Ordering::AcqRel)
    }

    /// Drain into `ready` on the loop thread. Bounded by the queue length
    /// observed at entry so producers cannot starve the tick.
    pub fn drain_into(&self, ready: &mut ReadyQueue<T>) -> usize {
        // Clear the flag before draining: a producer that pushes after this
        // point sets it again and posts a fresh wakeup, so nothing is lost.
        self.wake_pending.store(false, Ordering::Release);
        let mut n = 0;
        let bound = self.queue.len();
        while n < bound {
            match self.queue.pop() {
                Some(item) => {
                    ready.push(item);
                    n += 1;
                }
                None => break,
            }
        }
        n
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

/// Convenience alias used by the reactor: shared handle for producers.
pub type SharedCrossThreadQueue<T> = Arc<CrossThreadQueue<T>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn ready_queue_is_fifo() {
        let mut q = ReadyQueue::new();
        for i in 0..10 {
            q.push(i);
        }
        let drained: Vec<_> = std::iter::from_fn(|| q.pop()).collect();
        assert_eq!(drained, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn first_push_requests_wakeup_only_once() {
        let q = CrossThreadQueue::new();
        assert!(q.push(1));
        assert!(!q.push(2));
        assert!(!q.push(3));
        let mut ready = ReadyQueue::new();
        assert_eq!(q.drain_into(&mut ready), 3);
        // After a drain the next producer must wake again.
        assert!(q.push(4));
    }

    #[test]
    fn concurrent_producers_lose_nothing() {
        let q = Arc::new(CrossThreadQueue::new());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let q = q.clone();
                thread::spawn(move || {
                    for i in 0..1000 {
                        let _ = q.push(t * 1000 + i);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let mut ready = ReadyQueue::new();
        let mut total = 0;
        while !q.is_empty() {
            total += q.drain_into(&mut ready);
        }
        assert_eq!(total, 8000);
        let mut seen: Vec<_> = std::iter::from_fn(|| ready.pop()).collect();
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 8000);
    }
}
