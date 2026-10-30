# Ops notes

## Logging (R-140)
Logger name: `"cadeloop"` (stdlib `logging`). Default: errors + lifecycle
only. Access log: opt-in, bounded buffer + background writer thread that
emits in batches, drop-on-overflow counted and reported when the writer
catches up — never blocks the loop. (Before this was implemented the sink
called `logger.info()` inline on the loop thread, so a handler doing
blocking I/O stalled every connection the worker held.) While the
`cadeloop.access` logger is not enabled for INFO, nothing is recorded per
request at all; the level is re-read once a second.

## Introspection (R-103/R-141)
`loop.stats()` returns counters: `ticks, polls, completions,
callbacks_dispatched, timers_fired, xthread_items, spins, spin_hits, ready_len,
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
Pinning is opt-in (`cfg.pin=True`, `--pin`): each worker's event-loop
thread is pinned to one CPU of the process's affinity mask, and the threads
it starts (executors, the access-log writer) are not. On a loopback
benchmark it measured no gain, and on shared or virtualised CPUs a pinned
worker cannot move off a busy core; on dedicated hardware, align it with the
NIC. For NIC alignment, size RSS queues to workers and pin them to the same
cores, e.g.:

```powershell
Set-NetAdapterRss -Name "Ethernet" -BaseProcessorNumber 0 `
    -MaxProcessors <workers> -NumberOfReceiveQueues <workers>
```

cadeloop does not configure NICs programmatically (spec §8).

## Latency modes (R-060)
`latency_mode`: `throughput` (spin 0µs) / `balanced` (20µs, default) /
`spin` (200µs). Spinning trades CPU for tail latency; use `spin` only on
dedicated cores.

### Adaptive spinning
Outside `spin` mode the loop spins only while spins are catching work
(`adaptive_spin`, `--adaptive-spin` / `--no-adaptive-spin`;
`crates/core/src/spin_gate.rs`). A spin that catches work earns three
credits, one that catches nothing spends one, and the loop spins while it
has credit (at most twelve): it keeps spinning while more than one spin in
four catches work and stops below that. While stopped, one park in eight
spins as a probe, and a probe that catches work starts it again. The cost
is a few integer operations per park; the window itself is measured with
the timestamps the spin already takes.

A credit rather than a run of misses: the first version stopped after four
misses in a row. On back-to-back ping-pong whose spins caught 97% of
messages, a bunch of four misses stopped it anyway, and stopped, its spins
caught only about 60% -- often enough to miss four in a row again. One of
three runs stayed there with more than half its parks not spinning; with
the credit, five of five ran like the fixed spin.

A catch is work that arrives during the spin. Work already waiting at its
first look -- typically the completion of a send the loop just made --
would have ended a park as fast, and counting it kept an echo server
spinning before every park although every spin that actually waited caught
nothing.

`stats()["spins"]` counts the parks that spun and `spin_hits` the spins
that caught work, so `spin_hits / spins` is the hit rate the gate acts on.
`spin` mode turns the gate off: a deployment that picked it has dedicated
the core, and wants the spin before every park.

Measured on a 4-vCPU Linux VM (epoll backend): an asyncio echo server on
`Loop(spin_us=...)`, one Python client sending 64-byte messages over
loopback (`TCP_NODELAY`) and waiting for each echo, server CPU from
`/proc/<pid>/task/*/schedstat`, medians of three 2-second runs. Rates are
messages per second; CPU is server CPU per message.

| traffic | no spin | 20 µs before every park | 20 µs adaptive |
|---|---|---|---|
| back to back | 26,955/s, p50 34.1 µs, 19.1 µs CPU | 42,572/s, p50 20.9 µs, 22.9 µs CPU | 40,673/s, p50 21.5 µs, 23.8 µs CPU |
| 50 µs gaps | p50 36.5 µs, 22.0 µs CPU | p50 36.9 µs, 41.3 µs CPU | p50 35.6 µs, 25.6 µs CPU |
| 200 µs gaps | p50 61.9 µs, 32.5 µs CPU | p50 37.7 µs, 45.9 µs CPU | p50 57.6 µs, 33.7 µs CPU |
| 1 ms gaps | p50 89.8 µs, 47.2 µs CPU | p50 92.9 µs, 70.4 µs CPU | p50 89.0 µs, 52.9 µs CPU |
| 5 ms gaps | p50 143.3 µs, 75.8 µs CPU | p50 146.4 µs, 98.2 µs CPU | p50 135.8 µs, 79.8 µs CPU |
| bursts of 20, 1 ms apart | 10,797/s, p50 34.3 µs, 21.7 µs CPU | 12,517/s, p50 22.9 µs, 26.9 µs CPU | 12,278/s, p50 23.3 µs, 28.3 µs CPU |
| bursts of 200, 5 ms apart | 15,097/s, p50 35.6 µs, 21.0 µs CPU | 18,881/s, p50 23.5 µs, 26.3 µs CPU | 19,485/s, p50 22.4 µs, 25.0 µs CPU |

- Back to back, the gate stays open: 2.00 spins and 0.97–0.99 caught
  messages per message in five of five runs, as with the fixed spin. Eight
  interleaved runs each measured 39,811/s fixed against 40,050/s adaptive,
  so the table's 4.5% is run-to-run noise.
- At 50 µs, 200 µs and 1 ms gaps none of the fixed spin's spins caught
  anything (`spin_hits` 0); the gate cut them from 2.00 to 0.26 per
  message.
- At 200 µs gaps the fixed spin's p50 was still lower, in two separate
  sessions, although it caught nothing there. The gain comes from
  something other than caught work -- plausibly how fast this VM wakes a
  core that slept slightly less, but that was not isolated -- and the gate
  cannot see it. Where that latency is worth the CPU,
  `--no-adaptive-spin` keeps it.
- Absolute numbers moved a lot between two sessions on this VM type (the
  no-spin back-to-back rate by 66%); compare within a row.

### Write policy
The mode also picks the write policy. By default an HTTP response is
*corked* (R-035): it is queued and flushed at the tick's flush phase, so
the responses and stream chunks one tick produced for a connection leave in
one `writev`/`WSASend`. That is
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
- On Linux loopback it measured as a net loss. One worker on a 4-vCPU VM,
  64 keep-alive connections from `bench/loadgen` (built for Linux, 3
  threads), medians of 3 runs:

  | app | corked | immediate flush |
  |---|---|---|
  | hello world, closed loop | 66,207 req/s, 14.7 µs CPU/req | 57,020 req/s, 17.0 µs CPU/req |
  | ~14 µs of Python per request, closed loop | 34,514 req/s, 28.7 µs CPU/req | 20,375 req/s, 48.7 µs CPU/req |
  | ~14 µs of Python per request, 15,000 req/s open loop | p50 348 µs, p99 2.5 ms | p50 1.8 ms, p99 191 ms |

  Every variant posted one send per request. Flushing only *finished*
  responses that had nothing pipelined behind them -- the case where
  corking cannot save a send -- measured no better: within 3% of immediate
  flush's closed-loop throughput, and a 168 ms p99 at 15,000 req/s. With
  the ~14 µs app it dispatched tens of requests per tick, as corking did,
  so its extra CPU is neither syscalls nor smaller batches; where it goes
  was not isolated. That is why no mode flushes early on its own. Windows
  is unmeasured: the knob exists because the trade-off is
  deployment-specific, so measure it on your own hardware before turning
  it on. `stats()["sends_posted"]`, read against `bytes_sent`, shows what
  corking is buying you.


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
| `Cargo.lock` | `tag-release.yml` regenerates and commits it as part of cutting the tag |

What happens on merge to `main`:

1. **`tag-release.yml`** reads the workspace version, and if no `vX.Y.Z`
   tag exists for it, regenerates `Cargo.lock` (`cargo update
   --workspace` — only the two path members move, dependency pins are
   untouched), commits that if it changed, and pushes a tag. The lock
   sync is best-effort and cannot fail the release; see below. If the
   tag already exists the run is a no-op, so the workflow is safe on
   every push and safe to re-run by hand.
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

### Why nothing asserts Cargo.lock is already in step

A first attempt at the above had `tag-release.yml` and `verify-version`
both run `cargo metadata --locked`, on the reasoning that the lock ships
inside the sdist and should not contradict the wheel. That blocked the
0.0.3 release outright:

```
error: cannot update the lock file ... because --locked was passed to prevent this
```

A version bump here is a one-line edit, often made in the GitHub web UI,
and the lock is the one file that edit cannot produce. Asserting on
derived data made the release depend on a human remembering to
regenerate it — precisely the coupling the single source of truth is
supposed to remove.

Regenerating it instead, but with `--offline`, then blocked 0.0.3 a
second time:

```
error: no matching package named `crossbeam-queue` found
location searched: crates.io index
```

`--offline` had been added to justify "no index access". It holds on a
developer's machine, whose cargo cache is already populated, and not on
a runner, whose cache is cold — resolving the graph at all needs the
index, even though only the two path members change.

Both failures were the same mistake wearing different clothes: a
cosmetic detail given the power to stop a release. So the sync is now
**best-effort and cannot fail the run**. If `cargo update` fails, if the
commit fails, or if the push is refused (branch protection, or a commit
landing mid-run), the workflow warns, leaves the tree as it found it,
and tags anyway. A stale lock only restates a version cargo recomputes
by itself, and the sdist installs cleanly either way — that is worth a
warning, never a blocked release.

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

**An sdist must contain the license file its metadata names.** PyPI
implements Metadata 2.4, and rejects a distribution whose `PKG-INFO`
declares a `License-File` the archive does not hold. maturin writes
`License-File: LICENSE` from `pyproject.toml` either way, but the sdist's
file list comes from `cargo package --list` against
`crates/pyshim/Cargo.toml`, which cannot see a `LICENSE` at the repo
root — so the metadata promised a file the tarball lacked. Wheels are
unaffected: maturin copies the license into `.dist-info/licenses/`
itself, which is why 0.0.4 uploaded both wheels and then failed:

```
400 License-File LICENSE does not exist in distribution file
cadeloop-0.0.4.tar.gz at cadeloop-0.0.4/LICENSE
```

That is the worst place to learn it — the version ends up half
published, and the wheel filenames are spent. `[tool.maturin] include`
puts the file in the sdist, and `wheel-linux` now asserts at build time
that every declared license file is really there. **`twine check` does
not catch this**; it passed the exact tarball PyPI refused.

Note that a rejected upload does *not* burn the filename — only a
successful one does. But a half-published version cannot be completed by
re-running the tag either, since the re-run builds from the tree at that
tag, which still has the bug. Bump to the next version instead.

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

**`gh-action-pypi-publish` cannot be SHA-pinned.** Every other
third-party action in this repository is pinned to a commit SHA, on the
reasoning in `ci.yml`: a moving tag means what CI executes was decided
by whoever last pushed there. This action is the exception, because it
is a *Docker* action — it resolves to
`ghcr.io/pypa/gh-action-pypi-publish:<ref>`, using the ref after the `@`
as the image tag. Released images are published only under version tags
(`v1`, `v1.14`, `v1.14.2` share one digest; no commit-SHA tag carries
it), so a SHA pin does not tighten it, it breaks it:

```
docker: Error response from daemon: manifest unknown
```

That is how the v0.0.3 publish failed — after the wheels were built and
the Release was already up. It is pinned to the patch tag `v1.14.2`,
which is the tightest ref that resolves; `v1` and `v1.14` re-point on
every release.

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
