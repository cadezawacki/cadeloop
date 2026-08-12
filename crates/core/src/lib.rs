//! CadeLoop core: L0 kernel I/O backends and the L1 reactor.
//!
//! This crate is intentionally free of any Python/PyO3 dependency (R-010:
//! GIL assumptions live behind the `gil_boundary` module in `cadeloop-pyshim`).
//! Everything here is generic over an opaque callback token `T` supplied by
//! the binding layer, which keeps the scheduling and I/O state machines
//! testable on any host platform even though the shipping backends (IOCP,
//! RIO) are Windows-only.

pub mod backend;
pub mod buffers;
pub mod opslab;
pub mod ready;
pub mod reactor;
pub mod timer;
pub mod time;

pub use reactor::{Reactor, ReactorConfig};
