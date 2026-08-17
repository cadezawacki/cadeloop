# Prebuilt wheels

**The prebuilt wheels that used to live here were removed.**

They were built before the pyo3 0.24.2 → 0.29.2 upgrade
(RUSTSEC-2026-0176, RUSTSEC-2026-0177) and before several correctness
fixes in `config.py` / `loop.py` / `server.py`, so installing them
delivered stale behaviour *and* the known-vulnerable native dependency
while looking like the reviewed source. Committed binaries drift from the
tree silently — the tree is the source of truth, so they are gone rather
than refreshed by hand.

## Build one instead

```bash
pip install maturin
maturin build --release --out dist
pip install --no-index --find-links dist cadeloop
```

Windows wheels (`cp311-win_amd64`, R-110) must be built on Windows; CI
builds them on every push and uploads them as run artifacts (see the
`build-windows` job in `.github/workflows/ci.yml`).

## Or download a released one

Tagged releases carry prebuilt wheels as assets — a PGO Windows wheel, a
PGO + `x86-64-v3` Windows wheel, and a Linux wheel. Grab one from
<https://github.com/cadezawacki/cadeloop/releases>, or install by URL:

```bash
pip install https://github.com/cadezawacki/cadeloop/releases/download/<tag>/<wheel>
```

Take the baseline wheel (build tag `2`) unless every target CPU is
Haswell/Zen 1 or newer; the `1v3` wheel faults on anything older. See
"Release wheels" in `docs/ops.md`. Nothing is on PyPI, so
`pip install cadeloop` still does not work.
