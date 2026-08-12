//! Native timer heap (R-053).
//!
//! A 4-ary min-heap keyed on `u64` ticks (nanoseconds since loop epoch).
//! No Python objects live inside the heap: each entry carries an opaque
//! token `T` (the binding layer maps tokens to callback slab slots).
//!
//! Cancellation marks a tombstone via a shared flag; the entry stays in the
//! heap until it surfaces or until compaction removes it. Compaction runs
//! when tombstones exceed 50% of live entries (R-053).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::time::Ticks;

/// Shared cancellation flag for one scheduled timer.
///
/// `TimerHandle.cancel()` on the Python side flips this; the heap treats the
/// entry as a tombstone from then on. Atomic because asyncio does not forbid
/// cross-thread `cancel()` even though it only documents loop-thread use.
#[derive(Debug, Default)]
pub struct TimerToken {
    cancelled: AtomicBool,
}

impl TimerToken {
    pub fn new() -> Arc<Self> {
        Arc::new(TimerToken { cancelled: AtomicBool::new(false) })
    }

    #[inline]
    pub fn cancel(&self) -> bool {
        !self.cancelled.swap(true, Ordering::AcqRel)
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

struct Entry<T> {
    deadline: Ticks,
    /// Tie-breaker preserving FIFO order among equal deadlines, as asyncio
    /// semantics require for `call_at` with identical times.
    seq: u64,
    token: Arc<TimerToken>,
    payload: T,
}

const ARITY: usize = 4;

pub struct TimerHeap<T> {
    entries: Vec<Entry<T>>,
    seq: u64,
    /// Number of entries whose token is known-cancelled. Maintained lazily:
    /// incremented by `note_cancelled`, recomputed during compaction.
    tombstones: usize,
    /// Payloads of removed tombstones. The heap NEVER drops a payload
    /// itself: payloads may be Python references whose decref can run
    /// arbitrary code (`__del__`/GC), which the binding layer must only do
    /// outside its state critical section. Collected via `take_graveyard`.
    graveyard: Vec<T>,
}

impl<T> Default for TimerHeap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> TimerHeap<T> {
    pub fn new() -> Self {
        TimerHeap { entries: Vec::new(), seq: 0, tombstones: 0, graveyard: Vec::new() }
    }

    /// Take ownership of all tombstoned payloads removed so far, so the
    /// caller can drop them at a safe point (see `graveyard` field docs).
    pub fn take_graveyard(&mut self) -> Vec<T> {
        std::mem::take(&mut self.graveyard)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Schedule `payload` at `deadline`. Returns the shared cancellation
    /// token to hand to the Python `TimerHandle`.
    pub fn schedule(&mut self, deadline: Ticks, payload: T) -> Arc<TimerToken> {
        let token = TimerToken::new();
        self.schedule_with_token(deadline, payload, token.clone());
        token
    }

    pub fn schedule_with_token(&mut self, deadline: Ticks, payload: T, token: Arc<TimerToken>) {
        let seq = self.seq;
        self.seq += 1;
        self.entries.push(Entry { deadline, seq, token, payload });
        self.sift_up(self.entries.len() - 1);
    }

    /// Record that a handle was cancelled so the tombstone ratio can be
    /// tracked without scanning. Triggers compaction when tombstones exceed
    /// 50% of the heap (R-053).
    pub fn note_cancelled(&mut self) {
        self.tombstones += 1;
        if self.tombstones * 2 > self.entries.len() {
            self.compact();
        }
    }

    /// Deadline of the earliest live (non-tombstone) entry.
    /// Pops tombstones from the top as a side effect.
    pub fn next_deadline(&mut self) -> Option<Ticks> {
        self.drop_cancelled_top();
        self.entries.first().map(|e| e.deadline)
    }

    /// Pop the earliest live entry if its deadline is <= `now`.
    pub fn pop_expired(&mut self, now: Ticks) -> Option<T> {
        loop {
            self.drop_cancelled_top();
            let first = self.entries.first()?;
            if first.deadline > now {
                return None;
            }
            let entry = self.pop_top();
            if !entry.token.is_cancelled() {
                return Some(entry.payload);
            }
            // Raced with a cancel between drop_cancelled_top and pop; retry.
            self.graveyard.push(entry.payload);
            self.saturating_dec_tombstones();
        }
    }

    fn drop_cancelled_top(&mut self) {
        while let Some(first) = self.entries.first() {
            if !first.token.is_cancelled() {
                return;
            }
            let entry = self.pop_top();
            self.graveyard.push(entry.payload);
            self.saturating_dec_tombstones();
        }
    }

    fn saturating_dec_tombstones(&mut self) {
        self.tombstones = self.tombstones.saturating_sub(1);
    }

    /// Rebuild the heap without tombstoned entries.
    fn compact(&mut self) {
        let old = std::mem::take(&mut self.entries);
        self.entries = Vec::with_capacity(old.len());
        for e in old {
            if e.token.is_cancelled() {
                self.graveyard.push(e.payload);
            } else {
                self.entries.push(e);
            }
        }
        self.tombstones = 0;
        // Floyd heap construction: sift down from the last parent.
        if self.entries.len() > 1 {
            let last_parent = (self.entries.len() - 2) / ARITY;
            for i in (0..=last_parent).rev() {
                self.sift_down(i);
            }
        }
    }

    fn pop_top(&mut self) -> Entry<T> {
        let last = self.entries.len() - 1;
        self.entries.swap(0, last);
        let entry = self.entries.pop().expect("pop_top on empty heap");
        if !self.entries.is_empty() {
            self.sift_down(0);
        }
        entry
    }

    #[inline]
    fn less(&self, a: usize, b: usize) -> bool {
        let (ea, eb) = (&self.entries[a], &self.entries[b]);
        (ea.deadline, ea.seq) < (eb.deadline, eb.seq)
    }

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let parent = (i - 1) / ARITY;
            if self.less(i, parent) {
                self.entries.swap(i, parent);
                i = parent;
            } else {
                break;
            }
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        let len = self.entries.len();
        loop {
            let first_child = i * ARITY + 1;
            if first_child >= len {
                break;
            }
            let mut min = i;
            let last_child = (first_child + ARITY).min(len);
            for c in first_child..last_child {
                if self.less(c, min) {
                    min = c;
                }
            }
            if min == i {
                break;
            }
            self.entries.swap(i, min);
            i = min;
        }
    }

    #[cfg(test)]
    fn is_valid_heap(&self) -> bool {
        (1..self.entries.len()).all(|i| !self.less(i, (i - 1) / ARITY))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn pops_in_deadline_order() {
        let mut h = TimerHeap::new();
        for &d in &[50u64, 10, 30, 20, 40] {
            h.schedule(d, d);
        }
        let mut out = Vec::new();
        while let Some(v) = h.pop_expired(u64::MAX) {
            out.push(v);
        }
        assert_eq!(out, vec![10, 20, 30, 40, 50]);
    }

    #[test]
    fn fifo_for_equal_deadlines() {
        let mut h = TimerHeap::new();
        for i in 0..10u64 {
            h.schedule(7, i);
        }
        let mut out = Vec::new();
        while let Some(v) = h.pop_expired(u64::MAX) {
            out.push(v);
        }
        assert_eq!(out, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn respects_now_boundary() {
        let mut h = TimerHeap::new();
        h.schedule(10, "early");
        h.schedule(20, "late");
        assert_eq!(h.pop_expired(15), Some("early"));
        assert_eq!(h.pop_expired(15), None);
        assert_eq!(h.next_deadline(), Some(20));
    }

    #[test]
    fn cancelled_entries_are_skipped() {
        let mut h = TimerHeap::new();
        let t1 = h.schedule(10, 1);
        h.schedule(20, 2);
        t1.cancel();
        h.note_cancelled();
        assert_eq!(h.pop_expired(u64::MAX), Some(2));
        assert_eq!(h.pop_expired(u64::MAX), None);
    }

    #[test]
    fn graveyard_receives_every_discarded_payload() {
        let mut h = TimerHeap::new();
        let tokens: Vec<_> = (0..20u64).map(|i| h.schedule(i, i)).collect();
        for t in tokens.iter().take(15) {
            t.cancel();
            h.note_cancelled(); // trips compaction past 50%
        }
        let mut live = Vec::new();
        while let Some(v) = h.pop_expired(u64::MAX) {
            live.push(v);
        }
        assert_eq!(live, (15..20).collect::<Vec<_>>());
        let mut dead = h.take_graveyard();
        dead.sort();
        assert_eq!(dead, (0..15).collect::<Vec<_>>(), "no payload may be silently dropped");
        assert!(h.take_graveyard().is_empty());
    }

    #[test]
    fn compaction_removes_tombstones() {
        let mut h = TimerHeap::new();
        let tokens: Vec<_> = (0..100u64).map(|i| h.schedule(i, i)).collect();
        // Cancel 60% — compaction must trip at >50%. It fires at the 51st
        // cancel (51 tombstones of 100), dropping the heap to 49 live
        // entries; the remaining 9 cancels re-accumulate as tombstones
        // (9/49 < 50%), so the post-cancel length is at most 49.
        for t in tokens.iter().take(60) {
            t.cancel();
            h.note_cancelled();
        }
        assert!(h.len() <= 49, "tombstones not compacted: len={}", h.len());
        assert_eq!(h.take_graveyard().len(), 51, "compaction payloads routed to graveyard");
        let mut out = Vec::new();
        while let Some(v) = h.pop_expired(u64::MAX) {
            out.push(v);
        }
        assert_eq!(out, (60..100).collect::<Vec<_>>());
    }

    proptest! {
        /// Property (R-121): for any interleaving of schedules and cancels,
        /// pops surface exactly the non-cancelled payloads in
        /// (deadline, insertion) order, and the heap invariant holds.
        #[test]
        fn heap_order_and_cancel_correct(
            ops in proptest::collection::vec((0u64..1000, proptest::bool::weighted(0.3)), 0..300)
        ) {
            let mut h = TimerHeap::new();
            let mut expected = Vec::new();
            for (i, &(deadline, cancel)) in ops.iter().enumerate() {
                let token = h.schedule(deadline, (deadline, i));
                if cancel {
                    token.cancel();
                    h.note_cancelled();
                } else {
                    expected.push((deadline, i));
                }
                prop_assert!(h.is_valid_heap());
            }
            expected.sort();
            let mut got = Vec::new();
            while let Some(v) = h.pop_expired(u64::MAX) {
                got.push(v);
            }
            prop_assert_eq!(got, expected);
        }
    }
}
