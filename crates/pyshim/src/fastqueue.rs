//! Native asyncio-compatible FIFO queue (`cadeloop.Queue`).
//!
//! Why: profiling `queue_pingpong` shows the loop's share of the bench is
//! ~1% -- the other 99% is `asyncio.Queue`'s pure-Python put/get
//! coroutines and their frames. No loop, however fast, can recover that
//! from underneath the stdlib class. This queue moves the hot paths into
//! Rust: `put_nowait`/`get_nowait` are single native calls, and awaited
//! `put()`/`get()` return native awaitables whose fast path never builds
//! a coroutine frame at all (a `StopIteration(value)` straight from
//! `__next__`).
//!
//! Semantics are stdlib-faithful, ported from CPython 3.11's
//! `asyncio/queues.py`:
//!   * parked waiters are loop futures resolved with None; the waker
//!     re-checks the queue on wake (never a direct item handoff, which
//!     would lose the item if the woken task is cancelled between the
//!     wake and its next step);
//!   * cancellation recovery matches `Queue.get`/`Queue.put`'s `except:`
//!     blocks byte for byte -- cancel the waiter, drop it from the deque,
//!     and if the condition it was waiting for now holds and it was NOT
//!     cancelled-before-resolution, wake the next waiter so the wake is
//!     not lost;
//!   * `join()`/`task_done()` follow the unfinished-count contract, with
//!     `join()` returning an already-done awaitable when the count is 0;
//!   * `QueueFull`/`QueueEmpty` are asyncio's own exception classes.
//!
//! Thread affinity: loop-thread only, like every asyncio queue
//! (`unsendable` enforces it). RefCell borrows are never held across a
//! Python call -- waker and parker both pop/push first, call second --
//! so re-entrancy through GC or callbacks cannot trip a double borrow.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

use pyo3::exceptions::PyValueError;
use pyo3::gc::{PyTraverseError, PyVisit};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

static QUEUE_FULL: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static QUEUE_EMPTY: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn asyncio_exc<'py>(
    py: Python<'py>,
    cell: &'static PyOnceLock<Py<PyAny>>,
    name: &str,
) -> PyResult<&'py Py<PyAny>> {
    cell.get_or_try_init(py, || Ok(py.import("asyncio")?.getattr(name)?.unbind()))
}

fn raise_full(py: Python<'_>) -> PyErr {
    match asyncio_exc(py, &QUEUE_FULL, "QueueFull") {
        Ok(cls) => match cls.bind(py).call0() {
            Ok(exc) => PyErr::from_value(exc),
            Err(e) => e,
        },
        Err(e) => e,
    }
}

fn raise_empty(py: Python<'_>) -> PyErr {
    match asyncio_exc(py, &QUEUE_EMPTY, "QueueEmpty") {
        Ok(cls) => match cls.bind(py).call0() {
            Ok(exc) => PyErr::from_value(exc),
            Err(e) => e,
        },
        Err(e) => e,
    }
}

/// Resolve the first still-pending waiter with None. Pops before calling
/// into Python (set_result can run arbitrary code re-entering the queue).
fn wake_next(py: Python<'_>, waiters: &RefCell<VecDeque<Py<PyAny>>>) {
    loop {
        let fut = waiters.borrow_mut().pop_front();
        let Some(fut) = fut else { return };
        let done: bool =
            fut.bind(py).call_method0(intern!(py, "done")).and_then(|v| v.extract()).unwrap_or(true);
        if !done {
            let _ = fut.bind(py).call_method1(intern!(py, "set_result"), (py.None(),));
            return;
        }
    }
}

#[pyclass(name = "FastQueue", module = "cadeloop._core", unsendable)]
pub struct FastQueue {
    maxsize: i64,
    items: RefCell<VecDeque<Py<PyAny>>>,
    getters: RefCell<VecDeque<Py<PyAny>>>,
    putters: RefCell<VecDeque<Py<PyAny>>>,
    unfinished: Cell<i64>,
    join_waiters: RefCell<Vec<Py<PyAny>>>,
}

impl FastQueue {
    fn is_full(&self) -> bool {
        self.maxsize > 0 && self.items.borrow().len() as i64 >= self.maxsize
    }

    /// The shared put fast path: append + accounting + getter wake.
    fn put_now(&self, py: Python<'_>, item: Py<PyAny>) {
        self.items.borrow_mut().push_back(item);
        self.unfinished.set(self.unfinished.get() + 1);
        wake_next(py, &self.getters);
    }

    /// The shared get fast path: pop + putter wake.
    fn get_now(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        let item = self.items.borrow_mut().pop_front();
        if item.is_some() {
            wake_next(py, &self.putters);
        }
        item
    }
}

#[pymethods]
impl FastQueue {
    #[new]
    #[pyo3(signature = (maxsize=0))]
    fn new(maxsize: i64) -> Self {
        FastQueue {
            maxsize,
            items: RefCell::new(VecDeque::new()),
            getters: RefCell::new(VecDeque::new()),
            putters: RefCell::new(VecDeque::new()),
            unfinished: Cell::new(0),
            join_waiters: RefCell::new(Vec::new()),
        }
    }

    #[getter]
    fn maxsize(&self) -> i64 {
        self.maxsize
    }

    fn qsize(&self) -> usize {
        self.items.borrow().len()
    }

    fn empty(&self) -> bool {
        self.items.borrow().is_empty()
    }

    fn full(&self) -> bool {
        self.is_full()
    }

    fn put_nowait(&self, py: Python<'_>, item: Py<PyAny>) -> PyResult<()> {
        if self.is_full() {
            return Err(raise_full(py));
        }
        self.put_now(py, item);
        Ok(())
    }

    fn get_nowait(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.get_now(py) {
            Some(v) => Ok(v),
            None => Err(raise_empty(py)),
        }
    }

    /// One-call put fast path for the Python `async def put` wrapper:
    /// False when full (no exception -- the wrapper parks), True after
    /// append + accounting + getter wake.
    fn try_put(&self, py: Python<'_>, item: Py<PyAny>) -> bool {
        if self.is_full() {
            return false;
        }
        self.put_now(py, item);
        true
    }

    /// One-call get fast path: pops (waking a putter) or hands back the
    /// caller's sentinel -- an identity check instead of an exception.
    fn get_or(&self, py: Python<'_>, default: Py<PyAny>) -> Py<PyAny> {
        match self.get_now(py) {
            Some(v) => v,
            None => default,
        }
    }

    /// Park a waiter future created by the wrapper's slow path.
    fn park_getter(&self, fut: Py<PyAny>) {
        self.getters.borrow_mut().push_back(fut);
    }

    fn park_putter(&self, fut: Py<PyAny>) {
        self.putters.borrow_mut().push_back(fut);
    }

    /// CPython 3.11 queues.py `except:` recovery for a parked get: cancel
    /// the waiter, drop it from the deque, and pass its wake along if the
    /// queue is non-empty and the future was resolved before the
    /// cancellation landed.
    fn getter_recovery(&self, py: Python<'_>, fut: Py<PyAny>) {
        let fut = fut.bind(py);
        let _ = fut.call_method0(intern!(py, "cancel"));
        {
            let mut w = self.getters.borrow_mut();
            if let Some(pos) = w.iter().position(|f| f.bind(py).is(fut)) {
                w.remove(pos);
            }
        }
        let cancelled: bool =
            fut.call_method0(intern!(py, "cancelled")).and_then(|v| v.extract()).unwrap_or(true);
        if !self.items.borrow().is_empty() && !cancelled {
            wake_next(py, &self.getters);
        }
    }

    /// The put-side twin, keyed on free capacity instead of items.
    fn putter_recovery(&self, py: Python<'_>, fut: Py<PyAny>) {
        let fut = fut.bind(py);
        let _ = fut.call_method0(intern!(py, "cancel"));
        {
            let mut w = self.putters.borrow_mut();
            if let Some(pos) = w.iter().position(|f| f.bind(py).is(fut)) {
                w.remove(pos);
            }
        }
        let cancelled: bool =
            fut.call_method0(intern!(py, "cancelled")).and_then(|v| v.extract()).unwrap_or(true);
        if !self.is_full() && !cancelled {
            wake_next(py, &self.putters);
        }
    }

    fn task_done(&self, py: Python<'_>) -> PyResult<()> {
        let n = self.unfinished.get();
        if n <= 0 {
            return Err(PyValueError::new_err("task_done() called too many times"));
        }
        self.unfinished.set(n - 1);
        if n == 1 {
            loop {
                let fut = self.join_waiters.borrow_mut().pop();
                let Some(fut) = fut else { break };
                let done: bool =
                    fut.bind(py).call_method0(intern!(py, "done")).and_then(|v| v.extract()).unwrap_or(true);
                if !done {
                    let _ = fut.bind(py).call_method1(intern!(py, "set_result"), (py.None(),));
                }
            }
        }
        Ok(())
    }

    /// Await until every put item has had task_done() called for it.
    /// Already-satisfied joins return a completed awaitable; otherwise the
    /// loop future itself is returned (directly awaitable, resolved when
    /// the count reaches zero -- the same observable behavior as the
    /// stdlib's Event-based wait).
    fn join(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.unfinished.get() == 0 {
            return crate::http::value_awaitable(py, py.None());
        }
        let asyncio = py.import("asyncio")?;
        let pyloop = asyncio.call_method0(intern!(py, "get_running_loop"))?;
        let fut = pyloop.call_method0(intern!(py, "create_future"))?;
        self.join_waiters.borrow_mut().push(fut.clone().unbind());
        Ok(fut.unbind())
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for v in self.items.borrow().iter() {
            visit.call(v)?;
        }
        for v in self.getters.borrow().iter() {
            visit.call(v)?;
        }
        for v in self.putters.borrow().iter() {
            visit.call(v)?;
        }
        for v in self.join_waiters.borrow().iter() {
            visit.call(v)?;
        }
        Ok(())
    }

    fn __clear__(&self) {
        self.items.borrow_mut().clear();
        self.getters.borrow_mut().clear();
        self.putters.borrow_mut().clear();
        self.join_waiters.borrow_mut().clear();
    }
}
