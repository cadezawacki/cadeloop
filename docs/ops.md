# Ops notes

## Logging (R-140)
Logger name: `"cadeloop"` (stdlib `logging`). Default: errors + lifecycle
only. Access log (M2): opt-in, buffered ring + background writer thread,
drop-on-overflow counted — never blocks the loop.

## Introspection (R-103/R-141)
`loop.stats()` returns counters: `ticks, polls, completions,
callbacks_dispatched, timers_fired, xthread_items, spin_hits, ready_len,
timers_len, backend` (M1 adds `syscalls_saved_inline, buffers_in_use`;
M2 adds `tasks_eager_completed`). `Config(stats_endpoint=<port>)` (M2)
serves the same dict as JSON on localhost.

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

**Nothing is published yet.** These are GitHub Actions *run artifacts*:
the workflow does not create a GitHub Release, upload release assets, or
push to PyPI, so there is no `pip install <url>` or `pip install cadeloop`
route today. Download them from the workflow run, or build from source
with `maturin build --release`. Publication will be turned on
deliberately; until then, treat any install instruction that names a
release URL as wrong.
