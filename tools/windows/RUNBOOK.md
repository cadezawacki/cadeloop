# Windows validation runbook

One session on a Windows machine closes every currently-open Windows
gate: IOCP behavioral verification (M1), RIO behavioral validation (M3),
the first Windows benchmark numbers vs winloop (R-001/R-002/R-003
loopback previews), the cp311-win_amd64 wheel (R-110), and the soak
(R-113). Everything is orchestrated by `tools\windows\validate.ps1`,
which never stops on a red step and collects all artifacts into one zip.

## Requirements

- Windows 10/11 or Server 2019+ (RIO needs Win8+/Server 2012+ — any
  modern machine qualifies), x64, ≥4 cores recommended.
- Internet access for installers + pip.
- ~10 GB free disk, ~60–100 minutes.

## One-time setup (skip anything already installed)

1. **Git**: https://git-scm.com/download/win (defaults are fine).
2. **Rust**: https://rustup.rs → run `rustup-init.exe`, accept defaults
   (stable-x86_64-pc-windows-msvc). If it prompts about missing Visual
   Studio components, let it install them.
3. **Visual Studio Build Tools 2022** (if rustup didn't already):
   https://visualstudio.microsoft.com/visual-cpp-build-tools/ → check
   "Desktop development with C++" (MSVC v143 + Windows 11 SDK).
4. **Python 3.11.x, 64-bit** — exactly 3.11, not 3.12/3.13:
   https://www.python.org/downloads/release/python-31115/
   ("Windows installer (64-bit)"). CHECK "Add python.exe to PATH".
5. Open a **new** "x64 Native Tools Command Prompt for VS 2022" (or a
   fresh PowerShell after installs), verify:
   ```
   python --version   -> Python 3.11.x
   rustc -V
   git --version
   ```

## Run

```powershell
git clone https://github.com/cadezawacki/cadeloop
cd cadeloop
git checkout claude/new-session-bq3hp6
powershell -ExecutionPolicy Bypass -File tools\windows\validate.ps1
```

Leave it alone until it prints `DELIVERABLE: ...\cadeloop-windows-results.zip`.
Red steps are fine — they are findings, keep going. If the SCRIPT itself
dies early (rather than a step failing), grab whatever exists in
`windows-results\` plus the console text and send that.

## What it does

| step | what | gate |
|---|---|---|
| 00-01 | machine fingerprint, pip deps (incl. winloop) | context |
| 02-03 | `cargo test` + clippy on real Windows | M1 |
| 04 | build extension; construct IOCP AND RIO loops | M1/M3 |
| 05 | full pytest suite on IOCP | M1 |
| 06 | full pytest suite on RIO (`CADELOOP_BACKEND=rio`) | M3 |
| 07 | targeted backend smoke (echo, 10 MiB, 120-conn CQ growth, native HTTP, abrupt-close storm, mixed outbound, 3 s soak) x both backends | M1/M3 |
| 08 | CPython asyncio conformance suite | R-120 |
| 09-15 | benchmarks: sched/echo-rtt/echo-64/http for {cadeloop-IOCP, cadeloop-RIO, asyncio, winloop, uvicorn stacks, hypercorn} | R-001/002/003 preview |
| 16 | maturin wheel + clean-venv install + serve smoke | R-110 |
| 17 | 120 s scheduling soak (RSS growth < 5%) | R-113 |
| 18 | workers=2 spawn-model smoke: shared listener, PID-stamped responses | R-090 |
| 19 | bare-callable app + workers>1 raises ValueError (no fork) | R-090 note |

## Deliver back

1. **`cadeloop-windows-results.zip`** (repo root) — this is the main
   deliverable; it contains every step log, all bench JSONs, the smoke
   JSON, SUMMARY.txt, and the built wheel.
2. The last ~30 lines of console output (the SUMMARY table).
3. If any step said FAIL: nothing else to do — the logs in the zip carry
   the tracebacks; do NOT retry or debug on the machine.

Optional extras if the machine has spare time (each appended to the zip
folder before re-zipping, or sent separately):

- Longer soak: `python tests\stress\soak_timers.py --seconds 900 > windows-results\19-soak-long.log 2>&1`
- A second full bench pass at a quiet time of day (re-run steps 09–15 by
  re-invoking validate.ps1 — it overwrites logs, so copy the first
  windows-results folder aside first).
