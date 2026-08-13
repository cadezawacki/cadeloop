# vendor/llhttp

llhttp (MIT, R-014) is vendored at a pinned release for the M2 HTTP engine
(R-080: strict mode, driven from Rust).

- Pinned release: **v9.2.1** (generated C release artifact
  `llhttp-release-v9.2.1.tar.gz`, upstream tag `release/v9.2.1`).
- Vendored from the generated-release branch tag `release/v9.2.1`
  (nodejs/llhttp commit recorded below); files: `llhttp.c`, `llhttp.h`,
  `api.c`, `http.c`, `LICENSE-MIT`.
- Re-vendor with `python vendor/llhttp/fetch.py` or
  `git clone --branch release/v9.2.1 https://github.com/nodejs/llhttp`.
- The build wires it in via a `cc` build script in `crates/core` behind the
  `http` feature (added in M2); nothing links it before then.

Vendored files are committed once fetched so builds are hermetic (R-112:
no system deps beyond the OS). Do not edit vendored sources; bump the pin
in `fetch.py` instead.
