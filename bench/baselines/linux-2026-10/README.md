# Linux benchmark data, October 2026

The data behind the Linux tables and charts in the top-level README.
Machine, software and method: [docs/benchmarks.md](../../../docs/benchmarks.md#linux-environment).
Every number as text: [docs/benchmarks-linux.md](../../../docs/benchmarks-linux.md).

| file | contents |
|---|---|
| `summary.json` | Aggregated rows (median rep, merged-histogram percentiles, server CPU and memory) for every server and workload, idle-connection memory, and timer results, as `bench/suite/report.py` writes it |
| `runs.jsonl.gz` | Every run as recorded: the load generator's config and result (full latency histogram, per-second series for the overload run), the server's process tree sampled twice a second (CPU, unique memory, resident memory, file descriptors, threads, context switches), and TCP counter deltas |
| `meta.json` | The session: command lines, Python, platform, the staged cadeloop build (`IDENTITY.txt`) and every contender's version |
| `timers.json` | `bench/suite/latency.py`: sleep overshoot and cross-thread wake-up latency per loop |
| `sched.json` | `bench/harness/harness.py --suite sched`: the scheduling microbenchmarks |
| `readme-tables.json` | The README's Linux tables, as `report.py --tables` writes them; `bench/harness/tables.py` draws them |

All of it comes from one session on one VM, run back to back: the HTTP,
WebSocket, TLS and echo suites, then idle connections and overload, then
timers and scheduling. The build measured is cadeloop revision `808d72d`.

Recreate the tables and charts:

```bash
mkdir -p main && python -c "import gzip,shutil; shutil.copyfileobj(gzip.open('runs.jsonl.gz'), open('main/runs.jsonl','wb'))"
cp meta.json main/
python ../../suite/report.py main --platform linux --summary summary.json --markdown tables.md \
    --tables readme-tables.json --charts out --timers timers.json --sched sched.json
python ../../harness/tables.py readme-tables.json --outdir out
```
