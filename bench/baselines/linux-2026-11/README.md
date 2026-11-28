# Linux benchmark data, November 2026

The data behind the Linux tables and charts in the top-level README.
Machine, software and method: [docs/benchmarks.md](../../../docs/benchmarks.md#linux-environment).
Every number as text: [docs/benchmarks-linux.md](../../../docs/benchmarks-linux.md).
It replaces [the October data](../linux-2026-10/), which was three runs per
workload on a slower VM; numbers from the two sessions do not compare.

| file | contents |
|---|---|
| `summary.json` | Aggregated rows for every server and workload: the median run with every run's rate, the confidence interval of that median and its level, the intervals of the per-run latency percentiles, server CPU and memory, the load generator's CPU as a share of its allowance, the hypervisor's steal time; idle-connection memory; timer results; `resolution` (the same build measured twice, workload by workload) and `spread` (run-to-run variation over all cells), as `bench/suite/report.py` writes it |
| `runs.jsonl.gz` | Every run as recorded: the load generator's config and result (full latency histogram, per-second series for the overload run), the server's process tree sampled twice a second (CPU, unique memory, resident memory, file descriptors, threads, context switches), TCP counter deltas, and the host's steal time over the run |
| `meta.json` | The session: every command line in the order it ran, Python, platform, the staged cadeloop build (`IDENTITY.txt`) and every contender's version |
| `timers.json` | `bench/suite/latency.py --reps 9`: sleep overshoot and cross-thread wake-up latency per loop, each loop in nine fresh processes |
| `sched.json` | `bench/harness/harness.py --suite sched --measured 9`: the scheduling microbenchmarks, every run kept |
| `readme-tables.json` | The README's Linux tables, as `report.py --tables` writes them; `bench/harness/tables.py` draws them |

All of it comes from one session on one VM. The HTTP, FastAPI and
access-log suites and the WebSocket, TLS and echo suites took turns, three
runs of every workload at a time, until each had nine; then idle connections
and overload, then timers and scheduling. cadeloop ran under two names,
`control` and `candidate`, throughout: one build, measured twice, so that
the difference between the two says how small a difference between two
servers the session can show. The handshake-per-request TLS workload ran
under two certificates (`tls-churn-c16`: ECDSA P-256; `tls-churn-rsa-c16`:
RSA 2048), the second added to the session part way through (its nine runs
are spread over the last two thirds). The build measured is cadeloop
revision `2e2edee`.

Recreate the tables and charts:

```bash
mkdir -p main && python -c "import gzip,shutil; shutil.copyfileobj(gzip.open('runs.jsonl.gz'), open('main/runs.jsonl','wb'))"
cp meta.json main/
python ../../suite/report.py main --platform linux --summary summary.json --markdown tables.md \
    --tables readme-tables.json --charts out --timers timers.json --sched sched.json
python ../../harness/tables.py readme-tables.json --outdir out
```
