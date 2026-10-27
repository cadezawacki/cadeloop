//! Native timer heap (R-053).
//!
//! A 4-ary min-heap keyed on `u64` ticks (nanoseconds since loop epoch).
//! No Python objects are touched by the heap itself: each entry carries an
//! opaque payload `T` (the binding layer's timer handle) that answers
//! [`TimerPayload::is_cancelled`].
//!
//! Cancellation is read straight off the payload rather than through a
//! separately allocated, reference-counted token. The heap used to hold an
//! `Arc<AtomicBool>` per entry, which cost every `call_later` a heap
//! allocation plus four atomic read-modify-writes (two `Arc` clones and two
//! drops) -- on the single hottest timer path, for a flag the handle object
//! already carries.
//!
//! Cancelled entries stay in the heap as tombstones until they surface or
//! until compaction removes them. Compaction runs when tombstones exceed
//! 50% of the heap (R-053). The binding layer reports cancellations through
//! [`TimerHeap::note_cancelled`]; it only does so for a payload that is
//! still scheduled (see [`TimerPayload::mark_unscheduled`]), so cancelling
//! a timer that already fired costs the heap nothing.

use crate::time::Ticks;

/// What the heap needs to know about a scheduled payload.
pub trait TimerPayload {
    /// True once the timer was cancelled; the heap treats the entry as a
    /// tombstone from then on.
    fn is_cancelled(&self) -> bool;

    /// The payload just left the heap -- popped because it expired, or
    /// discarded as a tombstone. Lets the binding layer track asyncio's
    /// `TimerHandle._scheduled` so a later `cancel()` knows there is no
    /// tombstone to account for.
    #[inline]
    fn mark_unscheduled(&self) {}
}

struct Entry<T> {
    deadline: Ticks,
    /// Tie-breaker preserving FIFO order among equal deadlines, as asyncio
    /// semantics require for `call_at` with identical times.
    seq: u64,
    payload: T,
}

impl<T> Entry<T> {
    #[inline(always)]
    fn key(&self) -> (Ticks, u64) {
        (self.deadline, self.seq)
    }
}

const ARITY: usize = 4;

pub struct TimerHeap<T> {
    entries: Vec<Entry<T>>,
    seq: u64,
    /// Number of entries believed cancelled. Maintained lazily: incremented
    /// by `note_cancelled`, decremented as tombstones surface, recomputed
    /// (to zero) by compaction.
    tombstones: usize,
    /// Payloads of removed tombstones. The heap NEVER drops a payload
    /// itself: payloads may be Python references whose decref can run
    /// arbitrary code (`__del__`/GC), which the binding layer must only do
    /// outside its state critical section. Collected via `take_graveyard`.
    graveyard: Vec<T>,
}

impl<T: TimerPayload> Default for TimerHeap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: TimerPayload> TimerHeap<T> {
    pub fn new() -> Self {
        TimerHeap { entries: Vec::new(), seq: 0, tombstones: 0, graveyard: Vec::new() }
    }

    /// Take ownership of all tombstoned payloads removed so far, so the
    /// caller can drop them at a safe point (see `graveyard` field docs).
    pub fn take_graveyard(&mut self) -> Vec<T> {
        std::mem::take(&mut self.graveyard)
    }

    #[inline]
    pub fn has_graveyard(&self) -> bool {
        !self.graveyard.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Schedule `payload` at `deadline`.
    pub fn schedule(&mut self, deadline: Ticks, payload: T) {
        let seq = self.seq;
        self.seq += 1;
        self.entries.push(Entry { deadline, seq, payload });
        self.sift_up(self.entries.len() - 1);
    }

    /// Record `n` cancellations of still-scheduled payloads so the tombstone
    /// ratio can be tracked without scanning. Triggers compaction when
    /// tombstones exceed 50% of the heap (R-053).
    pub fn note_cancelled(&mut self, n: usize) {
        self.tombstones = self.tombstones.saturating_add(n);
        if self.tombstones * 2 > self.entries.len() {
            self.compact();
        }
    }

    /// Deadline of the earliest live (non-tombstone) entry.
    /// Pops tombstones from the top as a side effect.
    #[inline]
    pub fn next_deadline(&mut self) -> Option<Ticks> {
        self.drop_cancelled_top();
        self.entries.first().map(|e| e.deadline)
    }

    /// Every scheduled payload, in no particular order (GC traversal
    /// support; tombstones included).
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.entries.iter().map(|e| &e.payload)
    }

    /// Deadline at the top without discarding tombstones (may be a
    /// cancelled entry's: callers use it only as a cheap lower bound).
    #[inline]
    pub fn peek_deadline(&self) -> Option<Ticks> {
        self.entries.first().map(|e| e.deadline)
    }

    /// Pop the earliest live entry if its deadline is <= `now`.
    pub fn pop_expired(&mut self, now: Ticks) -> Option<T> {
        loop {
            let first = self.entries.first()?;
            if first.deadline > now {
                // A cancelled entry above `now` is left for later: it is
                // removed when it surfaces or by compaction.
                return None;
            }
            let entry = self.pop_top();
            entry.payload.mark_unscheduled();
            if !entry.payload.is_cancelled() {
                return Some(entry.payload);
            }
            self.graveyard.push(entry.payload);
            self.saturating_dec_tombstones();
        }
    }

    fn drop_cancelled_top(&mut self) {
        while let Some(first) = self.entries.first() {
            if !first.payload.is_cancelled() {
                return;
            }
            let entry = self.pop_top();
            entry.payload.mark_unscheduled();
            self.graveyard.push(entry.payload);
            self.saturating_dec_tombstones();
        }
    }

    #[inline]
    fn saturating_dec_tombstones(&mut self) {
        self.tombstones = self.tombstones.saturating_sub(1);
    }

    /// Remove every entry, in no particular order (loop close). Payloads
    /// are handed back; the caller drops them outside its critical section.
    pub fn drain_all(&mut self) -> Vec<T> {
        let mut out: Vec<T> = Vec::with_capacity(self.entries.len() + self.graveyard.len());
        for e in self.entries.drain(..) {
            e.payload.mark_unscheduled();
            out.push(e.payload);
        }
        out.append(&mut self.graveyard);
        self.tombstones = 0;
        out
    }

    /// Rebuild the heap without tombstoned entries.
    fn compact(&mut self) {
        let mut live = 0usize;
        // In-place partition: no second allocation, and live entries keep
        // their relative order (the heap is rebuilt below regardless).
        for i in 0..self.entries.len() {
            if self.entries[i].payload.is_cancelled() {
                continue;
            }
            self.entries.swap(live, i);
            live += 1;
        }
        for e in self.entries.drain(live..) {
            e.payload.mark_unscheduled();
            self.graveyard.push(e.payload);
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
        let entry = self.entries.swap_remove(0);
        if self.entries.len() > 1 {
            self.sift_down(0);
        }
        entry
    }

    #[inline(always)]
    fn less(&self, a: usize, b: usize) -> bool {
        self.entries[a].key() < self.entries[b].key()
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

/// A plain payload for tests and Python-free users: a value plus a shared
/// cancellation flag.
#[cfg(test)]
pub mod testing {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug)]
    pub struct Tok<V> {
        pub value: V,
        pub cancelled: Arc<AtomicBool>,
        pub scheduled: Arc<AtomicBool>,
    }

    impl<V> Tok<V> {
        pub fn new(value: V) -> Self {
            Tok {
                value,
                cancelled: Arc::new(AtomicBool::new(false)),
                scheduled: Arc::new(AtomicBool::new(true)),
            }
        }

        /// Returns whether the token was still scheduled (i.e. whether the
        /// heap must be told about a new tombstone).
        pub fn cancel(&self) -> bool {
            self.cancelled.store(true, Ordering::Relaxed);
            self.scheduled.load(Ordering::Relaxed)
        }
    }

    impl<V> super::TimerPayload for Tok<V> {
        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Relaxed)
        }

        fn mark_unscheduled(&self) {
            self.scheduled.store(false, Ordering::Relaxed);
        }
    }

    impl<V: PartialEq> PartialEq for Tok<V> {
        fn eq(&self, other: &Self) -> bool {
            self.value == other.value
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::Tok;
    use super::*;
    use proptest::prelude::*;

    fn vals<V: Clone>(v: Vec<Tok<V>>) -> Vec<V> {
        v.into_iter().map(|t| t.value).collect()
    }

    fn drain<V: Clone>(h: &mut TimerHeap<Tok<V>>) -> Vec<V> {
        let mut out = Vec::new();
        while let Some(v) = h.pop_expired(u64::MAX) {
            out.push(v.value);
        }
        out
    }

    #[test]
    fn pops_in_deadline_order() {
        let mut h = TimerHeap::new();
        for &d in &[50u64, 10, 30, 20, 40] {
            h.schedule(d, Tok::new(d));
        }
        assert_eq!(drain(&mut h), vec![10, 20, 30, 40, 50]);
    }

    #[test]
    fn fifo_for_equal_deadlines() {
        let mut h = TimerHeap::new();
        for i in 0..10u64 {
            h.schedule(7, Tok::new(i));
        }
        assert_eq!(drain(&mut h), (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn respects_now_boundary() {
        let mut h = TimerHeap::new();
        h.schedule(10, Tok::new("early"));
        h.schedule(20, Tok::new("late"));
        assert_eq!(h.pop_expired(15).map(|t| t.value), Some("early"));
        assert!(h.pop_expired(15).is_none());
        assert_eq!(h.next_deadline(), Some(20));
    }

    #[test]
    fn cancelled_entries_are_skipped() {
        let mut h = TimerHeap::new();
        let t1 = Tok::new(1);
        h.schedule(10, t1.clone());
        h.schedule(20, Tok::new(2));
        assert!(t1.cancel());
        h.note_cancelled(1);
        assert_eq!(drain(&mut h), vec![2]);
    }

    #[test]
    fn popping_marks_unscheduled() {
        let mut h = TimerHeap::new();
        let t = Tok::new(1);
        h.schedule(10, t.clone());
        assert!(t.scheduled.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(h.pop_expired(10).map(|t| t.value), Some(1));
        // A cancel after firing must not be reported as a new tombstone.
        assert!(!t.cancel(), "fired timer still claims to be scheduled");
    }

    #[test]
    fn graveyard_receives_every_discarded_payload() {
        let mut h = TimerHeap::new();
        let tokens: Vec<_> = (0..20u64).map(Tok::new).collect();
        for (i, t) in tokens.iter().enumerate() {
            h.schedule(i as u64, t.clone());
        }
        for t in tokens.iter().take(15) {
            if t.cancel() {
                h.note_cancelled(1); // trips compaction past 50%
            }
        }
        assert_eq!(drain(&mut h), (15..20).collect::<Vec<_>>());
        let mut dead = vals(h.take_graveyard());
        dead.sort();
        assert_eq!(dead, (0..15).collect::<Vec<_>>(), "no payload may be silently dropped");
        assert!(h.take_graveyard().is_empty());
    }

    #[test]
    fn compaction_removes_tombstones() {
        let mut h = TimerHeap::new();
        let tokens: Vec<_> = (0..100u64).map(Tok::new).collect();
        for (i, t) in tokens.iter().enumerate() {
            h.schedule(i as u64, t.clone());
        }
        // Cancel 60% — compaction must trip at >50%. It fires at the 51st
        // cancel (51 tombstones of 100), dropping the heap to 49 live
        // entries; the remaining 9 cancels re-accumulate as tombstones
        // (9/49 < 50%), so the post-cancel length is at most 49.
        for t in tokens.iter().take(60) {
            if t.cancel() {
                h.note_cancelled(1);
            }
        }
        assert!(h.len() <= 49, "tombstones not compacted: len={}", h.len());
        assert_eq!(h.take_graveyard().len(), 51, "compaction payloads routed to graveyard");
        assert_eq!(drain(&mut h), (60..100).collect::<Vec<_>>());
    }

    #[test]
    fn drain_all_returns_everything() {
        let mut h = TimerHeap::new();
        for i in 0..10u64 {
            h.schedule(i, Tok::new(i));
        }
        let t = Tok::new(99);
        h.schedule(5, t.clone());
        t.cancel();
        let _ = h.next_deadline(); // nothing surfaced yet (top is live)
        let mut all = vals(h.drain_all());
        all.sort();
        assert_eq!(all.len(), 11);
        assert!(h.is_empty());
        assert!(!t.scheduled.load(std::sync::atomic::Ordering::Relaxed));
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
                let tok = Tok::new((deadline, i));
                h.schedule(deadline, tok.clone());
                if cancel {
                    if tok.cancel() {
                        h.note_cancelled(1);
                    }
                } else {
                    expected.push((deadline, i));
                }
                prop_assert!(h.is_valid_heap());
            }
            expected.sort();
            prop_assert_eq!(drain(&mut h), expected);
        }

        /// Interleaved pops and cancels: tombstone accounting never
        /// discards a live timer, whatever order things happen in.
        #[test]
        fn interleaved_pop_and_cancel(
            ops in proptest::collection::vec((0u64..200, 0u8..3), 0..400)
        ) {
            let mut h = TimerHeap::new();
            let mut live: Vec<Tok<u64>> = Vec::new();
            let mut now = 0u64;
            let mut fired: Vec<u64> = Vec::new();
            let mut cancelled: std::collections::HashSet<u64> = Default::default();
            for (i, &(d, action)) in ops.iter().enumerate() {
                match action {
                    0 => {
                        let t = Tok::new(i as u64);
                        h.schedule(now + d, t.clone());
                        live.push(t);
                    }
                    1 if !live.is_empty() => {
                        // `live` only ever holds still-scheduled tokens, so
                        // this cancel must win: the timer may never fire.
                        let t = live.remove(d as usize % live.len());
                        prop_assert!(t.cancel(), "live token not scheduled");
                        h.note_cancelled(1);
                        cancelled.insert(t.value);
                    }
                    _ => {
                        now += d;
                        while let Some(t) = h.pop_expired(now) {
                            fired.push(t.value);
                        }
                        live.retain(|t| t.scheduled.load(std::sync::atomic::Ordering::Relaxed));
                    }
                }
                prop_assert!(h.is_valid_heap());
            }
            for v in &fired {
                prop_assert!(!cancelled.contains(v), "cancelled timer {} fired", v);
            }
            while let Some(t) = h.pop_expired(u64::MAX) {
                prop_assert!(!cancelled.contains(&t.value));
                fired.push(t.value);
            }
            let total: usize = fired.len() + cancelled.len();
            let scheduled = ops.iter().filter(|(_, a)| *a == 0).count();
            prop_assert_eq!(total, scheduled);
        }
    }
}
