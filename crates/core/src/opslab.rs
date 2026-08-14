//! Overlapped-operation slab and lifecycle state machine (R-037).
//!
//! Every in-flight kernel operation owns one pinned slab slot holding its
//! `OVERLAPPED` (on Windows) plus bookkeeping. The spec calls the
//! OVERLAPPED/buffer use-after-free under cancellation the #1 UAF risk
//! (§16); the mitigations required by R-037 are implemented here:
//!
//! * slots never move: the slab grows by appending fixed-size chunks, so a
//!   pointer to a slot's `OVERLAPPED` stays valid until the slot is freed;
//! * a `{Free, Posted, Completed, Cancelled}` state machine with
//!   `debug_assert`ed transitions guards every lifecycle edge;
//! * a slot can only return to the freelist from `Completed` — a cancelled
//!   op still waits for its completion to be reaped
//!   (`ERROR_OPERATION_ABORTED` or success-raced) before the slot is
//!   reusable, and `CancelIoEx` returning `ERROR_NOT_FOUND` must NOT free
//!   the slot early (the completion is still in flight to the port);
//! * generation counters make stale `op_id`s detectable: an `OpId` embeds
//!   the generation observed at allocation and refuses to resolve after the
//!   slot is recycled.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpState {
    /// Slot is on the freelist.
    Free,
    /// Operation submitted to the kernel; OVERLAPPED memory is pinned.
    Posted,
    /// Completion reaped; payload may be consumed, slot may be freed.
    Completed,
    /// `CancelIoEx` issued while `Posted`; completion not yet reaped.
    /// The only legal exit is `Completed` (the kernel always delivers a
    /// completion for a posted op, aborted or not).
    Cancelled,
}

/// Kind of I/O operation, used for dispatch and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Accept,
    Recv,
    Send,
    Connect,
    Disconnect,
    /// Cross-thread wakeup posted via PostQueuedCompletionStatus.
    Wakeup,
    RecvFrom,
    SendTo,
    /// Overlapped ReadFile/WriteFile on a named-pipe HANDLE (R-051
    /// Windows: subprocess stdio pipes ride the same IOCP port as socket
    /// ops, distinguished only by which Win32 result-fetch call applies).
    PipeRead,
    PipeWrite,
}

/// Stable identifier for an in-flight op: slot index + generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpId {
    pub index: u32,
    pub generation: u32,
}

impl fmt::Display for OpId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "op{}#{}", self.index, self.generation)
    }
}

pub struct OpSlot<D> {
    state: OpState,
    generation: u32,
    pub kind: OpKind,
    /// Backend-specific payload (OVERLAPPED, WSABUFs, buffer slot refs...).
    /// Boxed indirection is NOT used: `D` lives inline in the chunk and
    /// never moves (chunks are never reallocated).
    pub data: D,
}

const CHUNK: usize = 256;

/// Pinned slab: a `Vec` of boxed fixed-size chunks. Growing appends a chunk;
/// existing slots never move (R-037 pinning requirement).
///
/// `in_flight` counts ops the kernel may still touch (Posted | Cancelled);
/// a reaped-but-unreleased (Completed) slot is no longer in flight.
pub struct OpSlab<D> {
    chunks: Vec<Box<[OpSlot<D>; CHUNK]>>,
    freelist: Vec<u32>,
    in_flight: usize,
    make_default: fn() -> D,
}

impl<D> OpSlab<D> {
    pub fn new(make_default: fn() -> D) -> Self {
        OpSlab { chunks: Vec::new(), freelist: Vec::new(), in_flight: 0, make_default }
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    pub fn capacity(&self) -> usize {
        self.chunks.len() * CHUNK
    }

    fn grow(&mut self) {
        let make = self.make_default;
        let base = self.capacity() as u32;
        let chunk: Vec<OpSlot<D>> = (0..CHUNK)
            .map(|_| OpSlot { state: OpState::Free, generation: 0, kind: OpKind::Wakeup, data: make() })
            .collect();
        let chunk: Box<[OpSlot<D>; CHUNK]> =
            chunk.into_boxed_slice().try_into().unwrap_or_else(|_| unreachable!());
        self.chunks.push(chunk);
        // Push in reverse so low indices are handed out first.
        for i in (0..CHUNK as u32).rev() {
            self.freelist.push(base + i);
        }
    }

    #[inline]
    fn slot(&self, index: u32) -> &OpSlot<D> {
        &self.chunks[index as usize / CHUNK][index as usize % CHUNK]
    }

    #[inline]
    fn slot_mut(&mut self, index: u32) -> &mut OpSlot<D> {
        &mut self.chunks[index as usize / CHUNK][index as usize % CHUNK]
    }

    /// Allocate a slot and transition Free -> Posted.
    pub fn post(&mut self, kind: OpKind) -> OpId {
        if self.freelist.is_empty() {
            self.grow();
        }
        let index = self.freelist.pop().expect("freelist refilled by grow");
        self.in_flight += 1;
        let slot = self.slot_mut(index);
        debug_assert_eq!(slot.state, OpState::Free, "post() on non-free slot");
        slot.state = OpState::Posted;
        slot.kind = kind;
        OpId { index, generation: slot.generation }
    }

    /// Access payload of a live (Posted/Cancelled/Completed) op.
    /// Returns None for stale ids (slot recycled) — callers MUST treat that
    /// as "completion already processed", never as an error to retry.
    pub fn get(&self, id: OpId) -> Option<&OpSlot<D>> {
        let slot = self.slot(id.index);
        (slot.generation == id.generation && slot.state != OpState::Free).then_some(slot)
    }

    pub fn get_mut(&mut self, id: OpId) -> Option<&mut OpSlot<D>> {
        let slot = self.slot_mut(id.index);
        (slot.generation == id.generation && slot.state != OpState::Free).then_some(slot)
    }

    /// Posted -> Cancelled (CancelIoEx issued). Idempotent for already
    /// cancelled ops; returns whether the caller should actually invoke
    /// CancelIoEx (only on the first Posted -> Cancelled edge).
    pub fn mark_cancelled(&mut self, id: OpId) -> bool {
        let Some(slot) = self.get_mut(id) else { return false };
        match slot.state {
            OpState::Posted => {
                slot.state = OpState::Cancelled;
                true
            }
            OpState::Cancelled => false,
            OpState::Completed => false,
            OpState::Free => unreachable!("get() filters Free"),
        }
    }

    /// {Posted, Cancelled} -> Completed, when the completion is reaped from
    /// the kernel. Returns the op kind, plus whether the op had been
    /// cancelled (so the dispatcher can suppress user callbacks).
    pub fn complete(&mut self, id: OpId) -> Option<(OpKind, bool)> {
        let slot = self.slot_mut(id.index);
        if slot.generation != id.generation {
            // Stale completion for a recycled slot: kernel bug or double
            // reap. Debug-fatal, ignore in release.
            debug_assert!(false, "stale completion for {id}");
            return None;
        }
        let was_cancelled = match slot.state {
            OpState::Posted => false,
            OpState::Cancelled => true,
            s @ (OpState::Free | OpState::Completed) => {
                debug_assert!(false, "complete() on {s:?} slot {id}");
                return None;
            }
        };
        slot.state = OpState::Completed;
        let kind = slot.kind;
        self.in_flight -= 1;
        Some((kind, was_cancelled))
    }

    /// Completed -> Free. The ONLY transition that recycles a slot.
    pub fn release(&mut self, id: OpId) {
        let slot = self.slot_mut(id.index);
        debug_assert_eq!(slot.generation, id.generation, "release() with stale id {id}");
        debug_assert_eq!(slot.state, OpState::Completed, "release() on non-completed slot {id}");
        if slot.generation == id.generation && slot.state == OpState::Completed {
            slot.state = OpState::Free;
            slot.generation = slot.generation.wrapping_add(1);
            self.freelist.push(id.index);
        }
    }

    pub fn state(&self, id: OpId) -> Option<OpState> {
        let slot = self.slot(id.index);
        (slot.generation == id.generation).then_some(slot.state)
    }

    /// Iterate ids of all in-flight (Posted or Cancelled) ops — used by
    /// `loop.close()` to CancelIoEx-and-drain (R-122).
    pub fn in_flight_ids(&self) -> Vec<OpId> {
        let mut out = Vec::with_capacity(self.in_flight);
        for (ci, chunk) in self.chunks.iter().enumerate() {
            for (si, slot) in chunk.iter().enumerate() {
                if matches!(slot.state, OpState::Posted | OpState::Cancelled) {
                    out.push(OpId { index: (ci * CHUNK + si) as u32, generation: slot.generation });
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn new_slab() -> OpSlab<u64> {
        OpSlab::new(|| 0)
    }

    #[test]
    fn post_complete_release_roundtrip() {
        let mut s = new_slab();
        let id = s.post(OpKind::Recv);
        assert_eq!(s.state(id), Some(OpState::Posted));
        assert_eq!(s.complete(id), Some((OpKind::Recv, false)));
        s.release(id);
        assert_eq!(s.in_flight(), 0);
        // Stale id no longer resolves.
        assert!(s.get(id).is_none());
        assert_eq!(s.state(id), None);
    }

    #[test]
    fn cancel_still_requires_completion_reap() {
        let mut s = new_slab();
        let id = s.post(OpKind::Send);
        assert!(s.mark_cancelled(id), "first cancel issues CancelIoEx");
        assert!(!s.mark_cancelled(id), "second cancel is a no-op");
        // Slot is NOT free yet — completion must still be reaped.
        assert_eq!(s.state(id), Some(OpState::Cancelled));
        assert_eq!(s.in_flight(), 1);
        assert_eq!(s.complete(id), Some((OpKind::Send, true)));
        assert_eq!(s.in_flight(), 0, "reaped op is no longer in flight");
        s.release(id);
        assert_eq!(s.state(id), None);
    }

    #[test]
    fn cancel_after_completion_is_noop() {
        // ERROR_NOT_FOUND path: op completed before CancelIoEx landed.
        let mut s = new_slab();
        let id = s.post(OpKind::Recv);
        assert_eq!(s.complete(id), Some((OpKind::Recv, false)));
        assert!(!s.mark_cancelled(id));
        s.release(id);
    }

    #[test]
    fn slots_are_pinned_across_growth() {
        let mut s = new_slab();
        let first = s.post(OpKind::Recv);
        let addr_before = std::ptr::from_ref(&s.get(first).unwrap().data) as usize;
        // Force multiple chunk growths.
        let ids: Vec<_> = (0..CHUNK * 3).map(|_| s.post(OpKind::Send)).collect();
        let addr_after = std::ptr::from_ref(&s.get(first).unwrap().data) as usize;
        assert_eq!(addr_before, addr_after, "slot moved during growth (R-037 violation)");
        for id in ids {
            s.complete(id).unwrap();
            s.release(id);
        }
    }

    #[test]
    fn in_flight_ids_reports_posted_and_cancelled() {
        let mut s = new_slab();
        let a = s.post(OpKind::Recv);
        let b = s.post(OpKind::Send);
        let c = s.post(OpKind::Accept);
        s.mark_cancelled(b);
        s.complete(c).unwrap();
        s.release(c);
        let ids = s.in_flight_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&a) && ids.contains(&b));
    }

    /// Property test for the OVERLAPPED lifecycle (R-121/R-037): a random
    /// interleaving of post/cancel/complete/release actions never violates
    /// the state machine, never double-frees, and `in_flight` stays exact.
    #[derive(Debug, Clone)]
    enum Action {
        Post,
        Cancel(usize),
        Complete(usize),
        Release(usize),
    }

    fn action_strategy() -> impl Strategy<Value = Action> {
        prop_oneof![
            3 => Just(Action::Post),
            2 => (0usize..64).prop_map(Action::Cancel),
            2 => (0usize..64).prop_map(Action::Complete),
            2 => (0usize..64).prop_map(Action::Release),
        ]
    }

    proptest! {
        #[test]
        fn lifecycle_state_machine_holds(actions in proptest::collection::vec(action_strategy(), 0..500)) {
            let mut s = new_slab();
            let mut live: Vec<(OpId, OpState)> = Vec::new();
            for action in actions {
                match action {
                    Action::Post => {
                        let id = s.post(OpKind::Recv);
                        live.push((id, OpState::Posted));
                    }
                    Action::Cancel(i) => {
                        let idx = i % live.len().max(1);
                        if let Some(entry) = live.get_mut(idx) {
                            let should_cancelio = s.mark_cancelled(entry.0);
                            prop_assert_eq!(should_cancelio, entry.1 == OpState::Posted);
                            if entry.1 == OpState::Posted {
                                entry.1 = OpState::Cancelled;
                            }
                        }
                    }
                    Action::Complete(i) => {
                        let idx = i % live.len().max(1);
                        if let Some(entry) = live.get_mut(idx) {
                            if matches!(entry.1, OpState::Posted | OpState::Cancelled) {
                                let (_, was_cancelled) = s.complete(entry.0).unwrap();
                                prop_assert_eq!(was_cancelled, entry.1 == OpState::Cancelled);
                                entry.1 = OpState::Completed;
                            }
                        }
                    }
                    Action::Release(i) => {
                        if !live.is_empty() {
                            let idx = i % live.len();
                            if live[idx].1 == OpState::Completed {
                                let (id, _) = live.remove(idx);
                                s.release(id);
                                prop_assert!(s.get(id).is_none(), "stale id resolved after release");
                            }
                        }
                    }
                }
                let expected_in_flight =
                    live.iter().filter(|(_, st)| matches!(st, OpState::Posted | OpState::Cancelled)).count();
                prop_assert_eq!(s.in_flight(), expected_in_flight);
                for (id, st) in &live {
                    prop_assert_eq!(s.state(*id), Some(*st));
                }
            }
        }
    }
}
