//! Makes PyO3's interpreter-version cfgs (`Py_3_12`, `Py_3_13`, ...)
//! available to this crate.
//!
//! asyncio changed between 3.11, 3.12 and 3.13 in ways a caller can see:
//! what `writelines()` after `write_eof()` says, whether an empty datagram
//! is sent, how a `Handle` prints. A wheel is built for one interpreter
//! version, so each difference is decided here, at compile time: the 3.11
//! build contains none of the newer versions' code, and nothing checks a
//! version while the loop runs.

fn main() {
    pyo3_build_config::use_pyo3_cfgs();
}
