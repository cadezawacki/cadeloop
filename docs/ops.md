# Ops notes

## Logging (R-140)
Logger name: `"cadeloop"` (stdlib `logging`). Default: errors + lifecycle
only. Access log: opt-in, bounded queue + background writer thread,
drop-on-overflow counted and reported when the writer catches up — never
blocks the loop. (Before this was implemented the sink called
`logger.info()` inline on the loop thread, so a handler doing blocking
I/O stalled every connection the worker held.)

## Introspection (R-103/R-141)
`loop.stats()` returns counters: `ticks, polls, completions,
callbacks_dispatched, timers_fired, xthread_items, spin_hits, ready_len,
timers_len, backend` (M1 adds `syscalls_saved_inline, buffers_in_use`;
M2 adds `tasks_eager_completed`). `Config(stats_endpoint=<port>)` serves
the same dict as JSON, bound to 127.0.0.1 only, with a `worker` key
naming whose counters they are. One worker binds it: every worker on the
same port would hand each scrape whichever process the kernel picked,
which makes a counter series meaningless.

## Debug mode (R-142)
`PYTHONASYNCIODEBUG=1` (or `-X dev`) enables slow-callback warnings
(>100ms, logged with the handle repr) and native op-state assertions
(debug builds).

## Multi-worker RSS alignment (R-091, M3)
Workers pin to physical cores (`cfg.pin=True`). For NIC alignment, size RSS
queues to workers and pin them to the same cores, e.g.:

```powershell
Set-NetAdapterRss -Name "Ethernet" -BaseProcessorNumber 0 `
    -MaxProcessors <workers> -NumberOfReceiveQueues <workers>
```

cadeloop does not configure NICs programmatically (spec §8).

## Latency modes (R-060)
`latency_mode`: `throughput` (spin 0µs) / `balanced` (20µs, default) /
`spin` (200µs). Spinning trades CPU for tail latency; use `spin` only on
dedicated cores.

The mode also picks the write policy. By default an HTTP response is
*corked* (R-035): it is queued and flushed at the tick's flush phase, so
several connections' responses leave in one `writev`/`WSASend`. That is
the throughput choice, and it is the right default — but it couples a
response's latency to how much *other* work the same tick batched in
front of it. `immediate_flush` (`--immediate-flush` / `--no-immediate-flush`)
puts each response on the wire the moment it is wire-ready instead. It
defaults to on under `latency_mode="spin"` and off otherwise; set it
explicitly to override either way.

Two caveats worth stating plainly:

- It costs syscalls. A streaming response that emits N chunks in one tick
  becomes up to N sends instead of one. A single-message response is
  unaffected — `http.response.start` only stashes the head, so head and
  body still leave together.
- We have not measured a latency win from it. On a 4-core VM with a
  single-process load generator the run-to-run p99 spread was larger than
  any difference between the modes, in both directions. The knob exists
  because the trade-off is real and deployment-specific; measure it on
  your own hardware before turning it on. `stats()["sends_posted"]`, read
  against `bytes_sent`, shows what corking is buying you.


## Cutting a release

**The version lives in exactly one place: `[workspace.package] version`
in the root `Cargo.toml`.** Bump that line, merge it to `main`, and the
rest happens on its own. Nothing else needs editing, and nothing else
should be edited — every other copy of the version is derived:

| where | how it gets the version |
| --- | --- |
| `crates/core`, `crates/pyshim` | `version.workspace = true` |
| `cadeloop._core.__version__` | `env!("CARGO_PKG_VERSION")` at compile time |
| `cadeloop.__version__` | re-exported from `_core` |
| `pyproject.toml` | `dynamic = ["version"]` — maturin reads the crate |
| wheel / sdist filenames | maturin, from the same crate metadata |
| the `vX.Y.Z` git tag | `tag-release.yml`, from `Cargo.toml` |
| `Cargo.lock` | cargo — CI runs `cargo metadata --locked` so it cannot drift |

What happens on merge to `main`:

1. **`tag-release.yml`** reads the workspace version, and if no `vX.Y.Z`
   tag exists for it, creates and pushes one. If the tag already exists
   the run is a no-op, so the workflow is safe on every push and safe to
   re-run by hand.
2. It then **dispatches `release.yml` at that tag**. This is explicit on
   purpose: a tag pushed with the default `GITHUB_TOKEN` does *not* fire
   `on: push: tags` — GitHub suppresses that to stop workflows recursing.
   `workflow_dispatch` is a documented exception to that rule, and
   dispatching at a tag ref leaves `github.ref` as `refs/tags/vX.Y.Z`, so
   the run publishes exactly as a human-pushed tag would.
3. **`release.yml`** verifies, builds, creates the GitHub Release, and
   uploads to PyPI, as described below.

Pushing a tag by hand still works and still triggers `release.yml` — but
the tag now has to agree with `Cargo.toml` or the run fails in seconds
(see `verify-version`). Prefer the bump.

### The failure this replaced

`v0.0.2` was tagged at a commit whose `Cargo.toml` still read `0.0.1`.
Nothing checked, so the full PGO matrix ran, produced
`cadeloop-0.0.1-*.whl`, attached those to the *v0.0.2* Release, and the
disagreement finally surfaced in `pypi-publish` — the last job of the
run:

```
tag v0.0.2 implies version 0.0.2, but built cadeloop-0.0.1-2-cp311-cp311-win_amd64.whl
```

Two changes make that unrepresentable. The tag is now *derived* from
`Cargo.toml` rather than typed alongside it, and `verify-version` runs
first and gates every other job, so a mismatched tag costs fifteen
seconds instead of a full matrix plus a wrong Release.

## Release wheels (R-110/R-111)

`release.yml` builds three artifacts on tag push (or dispatch):

- `wheel-win-pgo-baseline` — the production cp311-win_amd64 wheel,
  PGO-optimized: an instrumented build runs the repo's own scheduling +
  native-HTTP workload, profiles merge via llvm-profdata, and the final
  build compiles with `-Cprofile-use`.
- `wheel-win-pgo-v3` — same, plus `-C target-cpu=x86-64-v3` (AVX2/BMI2/
  FMA baseline). It refuses nothing at runtime — on a pre-v3 CPU it dies
  with SIGILL — so only deploy it to fleets known to be Haswell/Zen1 or
  newer.
- `wheel-linux` — the Linux wheel.

### Where the wheels land

A **tag push** (`v*`) publishes to two places:

1. **The GitHub Release** for that tag gets every artifact — both Windows
   wheels, the Linux wheel, and the sdist.
2. **PyPI** gets everything *except* the x86-64-v3 wheel, via Trusted
   Publishing (OIDC, no stored API token).

```bash
pip install cadeloop                                    # PyPI
pip install https://github.com/cadezawacki/cadeloop/releases/download/<tag>/<wheel>
```

Publishing keys off the **ref**, not the trigger. A **workflow_dispatch
against a branch** has no tag to hang a Release off, publishes to
neither destination, and leaves its wheels as run artifacts only — that
is the way to spot-check a build without shipping it. A dispatch
**against a `v*` tag** publishes normally, which is how `tag-release.yml`
drives the automated path.

### Things that will bite you on a release

**PyPI uploads are irreversible.** A filename, once uploaded, can never
be reused for that project — deleting the release does not free it. The
`pypi-publish` job therefore runs last, gated on `github-release`
succeeding, and asserts that every file's version matches the tag before
uploading. That assertion should now be unreachable — `verify-version`
rejects a mismatched tag before anything is built — but it stays, because
it is the last guard on the side of the line that cannot be undone.

**The v3 wheel is withheld from PyPI on purpose.** pip's build-tag
ordering already prefers the baseline, but "prefers" is the wrong
guarantee for an index everyone installs from without reading. It stays a
Release asset, reachable only by explicit URL.

**The manylinux container has no `python3` on PATH.** Every CPython in
the image lives under `/opt/python/<tag>/bin`, and the runner's
`setup-python` is not mounted into the container, so a bare
`maturin build` dies with *"Couldn't find any python interpreters from
'python3'"*. The job passes
`--interpreter /opt/python/cp311-cp311/bin/python` explicitly, which also
pins the build to the single version `requires-python` allows rather than
whatever `--find-interpreter` turns up in a future image. A follow-up step
asserts the result is exactly one `cp311` + `manylinux2014`/`manylinux_2_17`
wheel — a wheel tagged `linux_x86_64` or `manylinux_2_39` is not a crash,
it just installs nowhere, and without the check only a user running
`pip install cadeloop` would find out.

**The Linux wheel is built in a manylinux2014 container**
(`PyO3/maturin-action`), not on the runner. A plain `maturin build` on
`ubuntu-latest` links against glibc 2.39 and is tagged `manylinux_2_39`,
which pip refuses to install on Debian 12, RHEL 9, or Ubuntu 22.04. That
was tolerable when a human picked the artifact by hand; it is not
tolerable as the wheel PyPI serves to everyone. The sdist step runs
*before* the container step so `dist/` belongs to the runner user rather
than to root.

**Trusted Publishing must match on every field.** PyPI's publisher
config names the owner, repository, workflow filename (`release.yml`) and
environment name (`pypi`). All four are claims in the OIDC token and all
four are checked, so the `pypi-publish` job declares
`environment: name: pypi` to match. Change one side and the upload fails
with a generic "not a trusted publisher" error that does not say which
field disagreed.

The environment also gives you a place to hang a required reviewer: add
one under Settings → Environments → pypi and every publish pauses for
manual approval.

### Why the Windows wheels carry a build tag

Both Windows legs produce the same PEP 427 filename — same project,
version, `cp311`, `win_amd64` — so they would collide in one Release and
silently overwrite each other. The workflow retags them:
`cadeloop-<ver>-2-cp311-...` (baseline) and `cadeloop-<ver>-1v3-cp311-...`
(v3). Only the build tag differs; project name and version are untouched,
so nothing disagrees with the wheel's own metadata.

The numbering is deliberate. pip orders build tags as `(int, str)` with
"no tag" ranking lowest, so `2` beats `1v3` — aim pip at a directory
holding both and it picks the portable wheel, and reaching the v3 wheel
takes naming it explicitly. Had the baseline kept its untagged name, any
tag at all on the v3 wheel would have outranked it and made the
SIGILL-on-old-hardware build the default. Keep both tags if you touch
this, and keep baseline's number higher.
