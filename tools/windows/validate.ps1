# cadeloop Windows validation — one-shot orchestrator.
#
# Runs the complete M1/M3 Windows validation matrix (Rust tests, Python
# suites on IOCP and RIO, targeted backend smoke, CPython conformance,
# benchmarks vs asyncio/winloop, wheel build, soak) and collects every
# artifact into windows-results\ + a single zip to send back.
#
# Every step is continue-on-error: a red step is a RESULT, not a reason
# to stop. Expected total runtime: 60-100 minutes.
#
# Usage (from the repo root, in a "x64 Native Tools"-capable PowerShell):
#   powershell -ExecutionPolicy Bypass -File tools\windows\validate.ps1

$ErrorActionPreference = "Continue"
$repo = (Get-Location).Path
$R = Join-Path $repo "windows-results"
New-Item -ItemType Directory -Force -Path $R | Out-Null
$summary = @()

function Step($name, [scriptblock]$body) {
    $log = Join-Path $R "$name.log"
    $t0 = Get-Date
    Write-Host "==== [$name] ====" -ForegroundColor Cyan
    Set-Location $repo          # steps never inherit a failed step's cwd
    cmd /c exit 0               # reset stale $LASTEXITCODE
    try {
        & $body 2>&1 | Tee-Object -FilePath $log
        $code = $LASTEXITCODE
    } catch {
        $_ | Out-File -Append $log
        $code = -1
    }
    $secs = [int]((Get-Date) - $t0).TotalSeconds
    $status = if ($code -eq 0 -or $null -eq $code) { "OK" } else { "FAIL($code)" }
    Write-Host "==== [$name] $status in ${secs}s ====" -ForegroundColor Yellow
    $script:summary += "{0,-28} {1,-10} {2,6}s" -f $name, $status, $secs
}

# ---- 00: environment fingerprint -------------------------------------
Step "00-env" {
    cmd /c ver
    Get-CimInstance Win32_OperatingSystem | Select-Object Caption, Version, BuildNumber, OSArchitecture | Format-List
    Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors, MaxClockSpeed | Format-List
    Get-CimInstance Win32_ComputerSystem | Select-Object TotalPhysicalMemory | Format-List
    python --version
    rustc -V
    cargo -V
    git rev-parse HEAD
    git status --short
}

# ---- 01: python deps --------------------------------------------------
Step "01-pip-deps" {
    python -m pip install --upgrade pip
    python -m pip install pytest pytest-timeout uvicorn aiohttp trustme starlette fastapi hypercorn winloop maturin
    python -m pip list
}

# ---- 02: rust unit tests (real Windows: exercises IOCP paths) ---------
Step "02-cargo-test" { cargo test --workspace }

# ---- 03: clippy on the windows target ---------------------------------
Step "03-clippy" { cargo clippy --workspace --all-targets }

# ---- 04: build the extension (dev mode) -------------------------------
Step "04-build-ext" {
    cargo build --release -p cadeloop-pyshim
    Copy-Item target\release\_core.dll python\cadeloop\_core.pyd -Force
    $env:PYTHONPATH = "$repo\python"
    python -c "import cadeloop; lp = cadeloop.new_event_loop(); print('backend:', lp.stats()['backend']); lp.close()"
    python -c "from cadeloop.loop import Loop; lp = Loop(backend='rio'); print('rio backend:', lp.stats()['backend']); lp.close()"
}

$env:PYTHONPATH = "$repo\python"

# ---- 05: full python suite on IOCP (the default) ----------------------
Step "05-pytest-iocp" {
    Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
    python -m pytest tests\unit tests\conformance -q --timeout 120
}

# ---- 06: full python suite on RIO ------------------------------------
Step "06-pytest-rio" {
    $env:CADELOOP_BACKEND = "rio"
    python -m pytest tests\unit tests\conformance -q --timeout 120
    Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
}

# ---- 07: targeted backend smoke (both backends) -----------------------
Step "07-backend-smoke" {
    python tools\windows\rio_smoke.py iocp rio --out (Join-Path $R "backend-smoke.json")
}

# ---- 08: CPython asyncio conformance suite ----------------------------
Step "08-cpython-conformance" {
    python tests\conformance\run_cpython_suite.py
}

# ---- 09: benchmarks ---------------------------------------------------
# Contenders per suite: cadeloop (IOCP), cadeloop-on-RIO, stdlib asyncio,
# winloop (the spec's Windows reference). uvloop/rloop/rsloop have no
# Windows support.
Step "09-bench-sched" {
    Set-Location bench
    python harness\harness.py --suite sched --loops cadeloop,asyncio,winloop --out (Join-Path $R "win-sched.json")
    Set-Location $repo
}

Step "10-bench-echo-rtt-iocp" {
    Set-Location bench
    Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
    python harness\harness.py --suite echo --loops cadeloop,asyncio,winloop --conns 1 --msgs 5000 --out (Join-Path $R "win-echo-rtt-iocp.json")
    Set-Location $repo
}

Step "11-bench-echo-rtt-rio" {
    Set-Location bench
    $env:CADELOOP_BACKEND = "rio"
    python harness\harness.py --suite echo --loops cadeloop --conns 1 --msgs 5000 --out (Join-Path $R "win-echo-rtt-rio.json")
    Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
    Set-Location $repo
}

Step "12-bench-echo-64-iocp" {
    Set-Location bench
    python harness\harness.py --suite echo --loops cadeloop,asyncio,winloop --conns 64 --msgs 2000 --out (Join-Path $R "win-echo-64-iocp.json")
    Set-Location $repo
}

Step "13-bench-echo-64-rio" {
    Set-Location bench
    $env:CADELOOP_BACKEND = "rio"
    python harness\harness.py --suite echo --loops cadeloop --conns 64 --msgs 2000 --out (Join-Path $R "win-echo-64-rio.json")
    Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
    Set-Location $repo
}

Step "14-bench-http" {
    Set-Location bench
    python harness\harness.py --suite http --contenders cadeloop-native,cadeloop-native-rio,uvicorn+cadeloop,uvicorn+asyncio,uvicorn+winloop,hypercorn --conns 64 --seconds 3 --out (Join-Path $R "win-http.json")
    Set-Location $repo
}

Step "15-bench-http-uvicorn-on-rio" {
    Set-Location bench
    $env:CADELOOP_BACKEND = "rio"
    python harness\harness.py --suite http --contenders uvicorn+cadeloop --conns 64 --seconds 3 --out (Join-Path $R "win-http-uvicorn-rio.json")
    Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
    Set-Location $repo
}

# ---- 16: wheel build + clean-venv install test (R-110) ----------------
Step "16-wheel" {
    python -m maturin build --release
    $wheel = Get-ChildItem target\wheels\*.whl | Select-Object -First 1
    Copy-Item $wheel.FullName $R -Force
    python -m venv (Join-Path $R "wheelvenv")
    & (Join-Path $R "wheelvenv\Scripts\python.exe") -m pip install $wheel.FullName
    # Import + serve smoke WITHOUT the repo on the path:
    Push-Location $env:TEMP
    & (Join-Path $R "wheelvenv\Scripts\python.exe") -c @"
import cadeloop, asyncio
lp = cadeloop.new_event_loop()
print('wheel import OK; backend:', lp.stats()['backend'])
async def app(scope, receive, send):
    if scope['type'] != 'http': return
    await receive()
    await send({'type': 'http.response.start', 'status': 200, 'headers': []})
    await send({'type': 'http.response.body', 'body': b'wheel-ok'})
lid, bound, _ = lp._core.http_listen('127.0.0.1', 0, app, lp)
import threading, urllib.request
threading.Timer(0.3, lambda: None).start()
def hit():
    import time; time.sleep(0.5)
    r = urllib.request.urlopen(f'http://127.0.0.1:{bound[1]}/', timeout=5)
    print('wheel serve OK:', r.read())
    lp.call_soon_threadsafe(lp.stop)
threading.Thread(target=hit).start()
lp.run_forever()
lp.close()
"@
    Pop-Location
}

# ---- 17: scheduling soak (RSS growth gate, R-113) ---------------------
Step "17-soak" {
    python tests\stress\soak_timers.py --seconds 120
}

# ---- 18: multiworker degradation notice (no fork on Windows) ----------
Step "18-workers-degrade" {
    python -c @"
import logging, threading, urllib.request, time, sys
sys.path.insert(0, r'$repo\python')
logging.basicConfig(level=logging.INFO)
import cadeloop
async def app(scope, receive, send):
    if scope['type'] != 'http': return
    await receive()
    await send({'type': 'http.response.start', 'status': 200, 'headers': []})
    await send({'type': 'http.response.body', 'body': b'ok'})
def run():
    cadeloop.serve(app, '127.0.0.1', 8971, workers=4)  # must WARN + run 1 worker
t = threading.Thread(target=run, daemon=True)
t.start()
time.sleep(1.5)
print(urllib.request.urlopen('http://127.0.0.1:8971/', timeout=5).read())
"@
}

# ---- summary + zip ----------------------------------------------------
$summaryPath = Join-Path $R "SUMMARY.txt"
$summary | Out-File $summaryPath
Get-Content $summaryPath | Write-Host
Remove-Item (Join-Path $R "wheelvenv") -Recurse -Force -ErrorAction SilentlyContinue
$zip = Join-Path $repo "cadeloop-windows-results.zip"
Remove-Item $zip -ErrorAction SilentlyContinue
Compress-Archive -Path "$R\*" -DestinationPath $zip
Write-Host "`nDELIVERABLE: $zip" -ForegroundColor Green
