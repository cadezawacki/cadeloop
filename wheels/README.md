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

## Or just install it

```bash
pip install cadeloop
```

Tagged releases publish to PyPI and attach the same files to the GitHub
Release, so <https://github.com/cadezawacki/cadeloop/releases> works as a
direct-URL alternative:

```bash
pip install https://github.com/cadezawacki/cadeloop/releases/download/<tag>/<wheel>
```

One wheel is Release-only: the `x86-64-v3` Windows build (build tag
`1v3`) is deliberately kept off PyPI because it faults on pre-Haswell
CPUs. `pip install cadeloop` gets the portable PGO wheel (build tag `2`);
the v3 one has to be named by URL. See "Release wheels" in `docs/ops.md`.
