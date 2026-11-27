//! The native access log (R-140), binding half.
//!
//! `cadeloop_core::accesslog` formats finished requests and writes them
//! from a thread that never takes the GIL; see there for why. This module
//! adds what Python code gets: the *tap*, a callable handed a list of
//! `(client, method, target, status, duration_ms)` tuples -- the same
//! five values the logging path's sink receives -- once per tick that
//! finished any request, not once per request. The tap works with or
//! without a file.

use cadeloop_core::accesslog::LineLog;
pub(crate) use cadeloop_core::accesslog::{client_text, Format};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

/// Records waiting for the tap, past which new ones are dropped. The tap
/// runs a tick after the first record of a batch, so this is only reached
/// if the tap itself cannot be called.
const TAP_LIMIT: usize = 1 << 16;

/// One finished request, waiting for the tap.
pub(crate) struct TapRecord {
    peer: Option<Py<PyAny>>,
    method: &'static str,
    target: Vec<u8>,
    status: u16,
    dur_ms: f64,
}

pub(crate) struct AccessLog {
    lines: LineLog,
    /// The application's tap, and the loop's own method that calls it.
    tap: Option<(Py<PyAny>, Py<PyAny>)>,
    tap_batch: Vec<TapRecord>,
    tap_scheduled: bool,
    /// Totals, for `stats()`.
    pub(crate) total_records: u64,
    tap_dropped: u64,
}

impl AccessLog {
    /// `path`: a file to append to, `-` for standard output, or None for a
    /// tap alone. `tap` is the application's callable with the loop's
    /// flush method.
    pub(crate) fn open(
        path: Option<&str>,
        format: Format,
        tap: Option<(Py<PyAny>, Py<PyAny>)>,
    ) -> std::io::Result<AccessLog> {
        Ok(AccessLog {
            lines: LineLog::open(path, format)?,
            tap,
            tap_batch: Vec::new(),
            tap_scheduled: false,
            total_records: 0,
            tap_dropped: 0,
        })
    }

    /// Records lost in all: by a writer that fell behind, or a tap that
    /// could not be called.
    pub(crate) fn total_dropped(&self) -> u64 {
        self.lines.total_dropped + self.tap_dropped
    }

    /// Record one finished request. Runs in the state cell: no Python
    /// beyond taking a reference to `peer`.
    ///
    /// Returns the loop's flush method when this record opens a tap batch
    /// -- the caller schedules it, outside the cell.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record(
        &mut self,
        py: Python<'_>,
        client: &str,
        peer: Option<&Py<PyAny>>,
        method: &'static str,
        target: &[u8],
        status: u16,
        dur_ms: f64,
    ) -> Option<Py<PyAny>> {
        self.total_records += 1;
        self.lines.record(client, method, target, status, dur_ms);
        let (_, flush) = self.tap.as_ref()?;
        if self.tap_batch.len() >= TAP_LIMIT {
            self.tap_dropped += 1;
            return None;
        }
        self.tap_batch.push(TapRecord {
            peer: peer.map(|p| p.clone_ref(py)),
            method,
            target: target.to_vec(),
            status,
            dur_ms,
        });
        if self.tap_scheduled {
            return None;
        }
        self.tap_scheduled = true;
        Some(flush.clone_ref(py))
    }

    /// For the loop's `__traverse__`: the tap is paired with the loop's own
    /// bound method, which is a cycle the collector has to be shown.
    pub(crate) fn visit_refs(&self, visit: &pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        if let Some((tap, flush)) = &self.tap {
            visit.call(tap)?;
            visit.call(flush)?;
        }
        Ok(())
    }

    /// Anything to hand to the writer? One comparison, for the tick.
    #[inline]
    pub(crate) fn has_lines(&self) -> bool {
        self.lines.has_lines()
    }

    /// Hand this tick's records to the writer. Runs in the state cell;
    /// never blocks.
    pub(crate) fn hand_off(&mut self) {
        self.lines.hand_off();
    }

    /// The tap and the batch it is owed, taken out for the call (which is
    /// made outside the cell). The batch's Python references go with it.
    pub(crate) fn take_tap_batch(&mut self, py: Python<'_>) -> Option<(Py<PyAny>, Vec<TapRecord>)> {
        self.tap_scheduled = false;
        let (tap, _) = self.tap.as_ref()?;
        if self.tap_batch.is_empty() {
            return None;
        }
        Some((tap.clone_ref(py), std::mem::take(&mut self.tap_batch)))
    }

    /// Stop the writer once it has written everything handed to it, or
    /// after a short wait if it is stuck. Blocking: call it outside the
    /// state cell with the GIL released. Returns false if the writer did
    /// not finish.
    pub(crate) fn finish(&mut self) -> bool {
        self.lines.finish()
    }
}

/// Build the tap's argument: a list of
/// `(client, method, target, status, duration_ms)`.
pub(crate) fn tap_argument<'py>(py: Python<'py>, batch: Vec<TapRecord>) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for r in batch {
        let peer = r.peer.unwrap_or_else(|| py.None());
        list.append((peer, r.method, PyBytes::new(py, &r.target), r.status, r.dur_ms))?;
    }
    Ok(list)
}
