# Windows benchmark data, October 2026

The data behind the Windows tables and charts in the top-level README.
Machine, software and method: [docs/benchmarks.md](../../../docs/benchmarks.md#windows).

| file | contents |
|---|---|
| `summary.json` | Aggregated rows (median rep, merged-histogram percentiles, server CPU and memory) for every server and workload, idle-connection memory, timer results — what `bench/windows/report.py` turns into the tables and charts |
| `runs.jsonl.gz` | Every run as recorded: load generator config and result (full latency histogram, per-second series for overload), server process-tree samples twice a second (CPU, private bytes, working set, handles, threads, context switches), TCP counter deltas |
| `ab-final-vs-1.0.1.json` | cadeloop 1.0.1 against the final build, same session, three rotated reps |
| `ab-cand3-vs-1.0.1.json` | the same for the build before the receive-size tail rule (`/sleep`, churn, pipelined and TLS rows come from it) |
| `timers.json` | `bench/windows/latency.py`: sleep overshoot and cross-thread wakeup latency |
| `sched.json`, `sched-1.0.1.json` | `bench/harness/harness.py --suite sched` for the final build and contenders, and for 1.0.1 |
| `builds.txt` | sha256 of each `_core.pyd` measured |
| `followup-runs.jsonl.gz`, `ab-followup.json` | The follow-up A/B (the pass's final build against the build that stops duplicating the socket behind `get_extra_info("socket")` and trims the WebSocket receive path), every run and the aggregated rows; each run carries `build` (`pass` or `follow-up`) |

Rows come from four sessions on one machine with the same configuration:
the contender comparison (every third-party server, and cadeloop 1.0.1),
two A/B sessions (1.0.1 against two candidate builds) and a re-measure of
uvicorn on cadeloop with the final build. cadeloop 1.0.1 ran in every
session as a control; its plaintext throughput was 73.7 K, 74.3 K, 74.8 K
and 73.4 K req/s (median of three each), so the sessions are comparable.
The scheduling runs and the timer runs used the final build.

The follow-up A/B is a fifth session on the same machine: cadeloop's own
server on both builds in five rotated reps (WebSocket workloads and three
HTTP guards), then uvicorn on cadeloop, one build per server process,
alternated three times each. The tables and charts above do not use it;
[docs/benchmarks-windows.md](../../../docs/benchmarks-windows.md#follow-up-ab-socket-handle-and-websocket-receive-path)
does.

Recreate the tables and charts:

```powershell
python -c "import gzip,shutil; shutil.copyfileobj(gzip.open('runs.jsonl.gz'), open('runs.jsonl','wb'))"
python ..\..\windows\report.py . --summary summary.json --markdown tables.md --charts out --timers timers.json --sched sched.json
```
