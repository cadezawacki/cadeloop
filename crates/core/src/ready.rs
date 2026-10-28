//! Callback queues (R-022, R-054).
//!
//! Two queues feed the dispatcher:
//!
//! * [`ReadyQueue`] — loop-thread-only FIFO of pending callbacks
//!   (`call_soon`, completion callbacks). Each tick dispatches exactly the
//!   callbacks queued when it started, as asyncio's `_run_once` does
//!   (R-054).
//! * [`CrossThreadQueue`] — lock-free MPSC handoff (R-022) for callbacks
//!   and timers scheduled by another thread while the loop thread is
//!   parked in its poll (the only time the loop state is out of reach;
//!   see the binding layer's gil_boundary). Producers are arbitrary
//!   threads; the single consumer is the loop thread, which drains it into
//!   the `ReadyQueue` and the timer heap after a backend wakeup.
//!
//! `crossbeam-queue`'s `SegQueue` provides the lock-free MPSC (it is MPMC,
//! used here single-consumer; R-022 allows crossbeam explicitly).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_queue::SegQueue;

use crate::time::Ticks;

/// Initial capacity of the ready queue and of the binding layer's dispatch
/// batch: room for a typical tick without regrowth. Not a limit -- a tick
/// dispatches every callback that was ready when it began (R-054).
pub const INITIAL_BATCH_CAPACITY: usize = 256;

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
        ReadyQueue { queue: VecDeque::with_capacity(INITIAL_BATCH_CAPACITY) }
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

    /// Queued items, front to back (GC traversal support).
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.queue.iter()
    }
}

/// Lock-free MPSC handoff for cross-thread `call_soon_threadsafe` (R-022).
///
/// `push` returns `true` when the caller must also wake the backend (the
/// flag was not already set) — this collapses N producer wakeups into one
/// kernel post per quiescent period.
pub struct CrossThreadQueue<T> {
    queue: SegQueue<T>,
    /// `(deadline, token)` timers (`call_later`/`call_at` from another
    /// thread). Rare, so a second queue rather than a wider item type on
    /// the callback path.
    timers: SegQueue<(Ticks, T)>,
    wake_pending: AtomicBool,
}

impl<T> Default for CrossThreadQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CrossThreadQueue<T> {
    pub fn new() -> Self {
        CrossThreadQueue {
            queue: SegQueue::new(),
            timers: SegQueue::new(),
            wake_pending: AtomicBool::new(false),
        }
    }

    /// Enqueue from any thread. Returns `true` if the producer should post a
    /// backend wakeup (first producer since the last drain).
    #[must_use]
    pub fn push(&self, item: T) -> bool {
        self.queue.push(item);
        !self.wake_pending.swap(true, Ordering::AcqRel)
    }

    /// [`CrossThreadQueue::push`] for a timer due at `deadline`.
    #[must_use]
    pub fn push_timer(&self, deadline: Ticks, item: T) -> bool {
        self.timers.push((deadline, item));
        !self.wake_pending.swap(true, Ordering::AcqRel)
    }

    /// Drain on the loop thread: callbacks into `ready`, timers into
    /// `schedule`. Bounded by the queue lengths observed at entry so
    /// producers cannot starve the tick. Returns the number of items.
    pub fn drain_into(&self, ready: &mut ReadyQueue<T>, mut schedule: impl FnMut(Ticks, T)) -> usize {
        // Nothing pushed since the last drain: the flag is the cheap test
        // (one relaxed load), and it is exact -- every push sets it AFTER
        // enqueueing, and only a drain clears it.
        if !self.wake_pending.load(Ordering::Relaxed) {
            return 0;
        }
        // Clear the flag before draining: a producer that pushes after this
        // point sets it again and posts a fresh wakeup, so nothing is lost.
        //
        // A swap, not a store. A producer whose `swap(true)` found the flag
        // already set posts no wakeup and relies on THIS drain to see its
        // item. A plain store does not synchronize-with that producer's
        // RMW, so on a weakly ordered CPU the item could still be invisible
        // to the pops below -- and with the flag now clear and no wakeup
        // posted, it would sit there until something unrelated woke the
        // loop. An RMW reads from the producer's swap (release sequence),
        // which makes its push happen-before our pops.
        self.wake_pending.swap(false, Ordering::AcqRel);
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
        if !self.timers.is_empty() {
            for _ in 0..self.timers.len() {
                match self.timers.pop() {
                    Some((deadline, item)) => {
                        schedule(deadline, item);
                        n += 1;
                    }
                    None => break,
                }
            }
        }
        n
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty() && self.timers.is_empty()
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
        assert_eq!(q.drain_into(&mut ready, |_, _| unreachable!()), 3);
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
            total += q.drain_into(&mut ready, |_, _| unreachable!());
        }
        assert_eq!(total, 8000);
        let mut seen: Vec<_> = std::iter::from_fn(|| ready.pop()).collect();
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 8000);
    }

    #[test]
    fn timers_drain_to_the_scheduler_and_wake_once() {
        let q = CrossThreadQueue::new();
        assert!(q.push_timer(50, 'a'));
        assert!(!q.push('z'));
        assert!(!q.push_timer(10, 'b'));
        let mut ready = ReadyQueue::new();
        let mut timers = Vec::new();
        assert_eq!(q.drain_into(&mut ready, |d, t| timers.push((d, t))), 3);
        assert_eq!(timers, vec![(50, 'a'), (10, 'b')]);
        assert_eq!(ready.len(), 1);
        assert!(q.is_empty());
        assert!(q.push_timer(1, 'c'), "a drain re-arms the wakeup");
    }
}
