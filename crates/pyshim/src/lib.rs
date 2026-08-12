//! `cadeloop._core` — the PyO3 extension module (L2 bindings).

mod coreloop;
mod gil_boundary;
mod handles;

use pyo3::prelude::*;

// R-070: mimalloc as the extension's global allocator. Python's own object
// allocator is untouched.
#[cfg(feature = "mimalloc-allocator")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<coreloop::CoreLoop>()?;
    m.add_class::<handles::Handle>()?;
    m.add_class::<handles::TimerHandle>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
