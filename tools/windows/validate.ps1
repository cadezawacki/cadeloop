# cadeloop Windows validation -- one-shot orchestrator (v4).
#
# v4: RIO availability gate -- one 2s construct check (rio_check.py)
# decides whether the RIO-dependent steps (06, rio smoke, 11, 13, 15,
# the rio HTTP contender) run or get SKIP(rio) summary rows, so a
# machine where RIO cannot initialize loses zero time to repeated
# failures. Bench harness timeouts tightened (12s per run, 10s server
# start). v3: pins Python to the repo .venv (hard-stop unless 3.11),
# clears windows-results\ at start, rio_probe diagnosis step, and the
# process-level watchdog around all long steps (nothing can stall).
#
# Usage (repo root): powershell -ExecutionPolicy Bypass -File tools\windows\validate.ps1

$ErrorActionPreference = "Continue"
$repo = (Get-Location).Path
$R = Join-Path $repo "windows-results"
New-Item -ItemType Directory -Force -Path $R | Out-Null
Remove-Item "$R\*" -Recurse -Force -ErrorAction SilentlyContinue
$summary = @()
$WD = "tools\windows\run_with_timeout.py"

# ---- python pinning: the repo venv or bust ----------------------------
$PY = Join-Path $repo ".venv\Scripts\python.exe"
if (-not (Test-Path $PY)) {
    Write-Host "creating .venv with Python 3.11..." -ForegroundColor Cyan
    if (Get-Command py -ErrorAction SilentlyContinue) { py -3.11 -m venv "$repo\.venv" }
    else { python -m venv "$repo\.venv" }
}
$ver = & $PY -c "import sys; print('%d.%d' % sys.version_info[:2])"
if ($ver -ne "3.11") {
    Write-Host "FATAL: .venv python reports $ver -- need 3.11 (delete .venv and install Python 3.11)" -ForegroundColor Red
    exit 1
}
Write-Host "using $PY (Python $ver)" -ForegroundColor Green

function Step($name, [scriptblock]$body) {
    $log = Join-Path $R "$name.log"
    $t0 = Get-Date
    Write-Host "==== [$name] ====" -ForegroundColor Cyan
    Set-Location $repo
    cmd /c exit 0
    try {
        & $body 2>&1 | ForEach-Object {
            $line = "$_"
            $line | Out-File -Append -Encoding utf8 $log
            Write-Host $line
        }
        $code = $LASTEXITCODE
    } catch {
        "$_" | Out-File -Append -Encoding utf8 $log
        $code = -1
    }
    $secs = [int]((Get-Date) - $t0).TotalSeconds
    $status = if ($code -eq 0 -or $null -eq $code) { "OK" } else { "FAIL($code)" }
    Write-Host "==== [$name] $status in ${secs}s ====" -ForegroundColor Yellow
    $script:summary += "{0,-28} {1,-10} {2,6}s" -f $name, $status, $secs
}

Step "00-env" {
    cmd /c ver
    Get-CimInstance Win32_OperatingSystem | Select-Object Caption, Version, BuildNumber, OSArchitecture | Format-List
    Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors, MaxClockSpeed | Format-List
    & $PY --version
    & $PY -c "import sys; print('exe:', sys.executable)"
    rustc -V
    cargo -V
    git rev-parse HEAD
    git status --short
}

Step "01-pip-deps" {
    & $PY -m pip install --upgrade pip
    & $PY -m pip install pytest pytest-timeout uvicorn aiohttp trustme starlette fastapi hypercorn winloop rsloop maturin
    & $PY -m pip list
}

Step "02-cargo-test" { & $PY $WD 1200 -- cargo test --workspace }

Step "03-clippy" { & $PY $WD 900 -- cargo clippy --workspace --all-targets }

Step "04-build-ext" {
    cargo build --release -p cadeloop-pyshim
    Copy-Item target\release\_core.dll python\cadeloop\_core.pyd -Force
    $env:PYTHONPATH = "$repo\python"
    & $PY tools\windows\rio_check.py
    & $PY -c "import cadeloop; lp = cadeloop.new_event_loop(); print('backend:', lp.stats()['backend']); lp.close()"
}

Step "04b-rio-probe" {
    & $PY $WD 600 -- cargo run --release --example rio_probe -p cadeloop-core
}

$env:PYTHONPATH = "$repo\python"

# ---- RIO availability gate --------------------------------------------
# One 2-second construct check decides whether the RIO-dependent steps
# run at all. On machines where RIO cannot initialize (see
# 04b-rio-probe.log) they are SKIPPED instantly instead of each failing
# slowly with the same construction error.
& $PY tools\windows\rio_check.py
$rioOK = ($LASTEXITCODE -eq 0)
if ($rioOK) {
    Write-Host "RIO gate: available -- running full RIO steps" -ForegroundColor Green
} else {
    Write-Host "RIO gate: unavailable on this machine -- RIO steps SKIPPED (diagnosis: 04b-rio-probe.log)" -ForegroundColor Yellow
}

function SkipStep($name) {
    Write-Host "==== [$name] SKIPPED (rio unavailable) ====" -ForegroundColor Yellow
    $script:summary += "{0,-28} {1,-10} {2,6}s" -f $name, "SKIP(rio)", 0
}

function PytestSweep($label) {
    $fail = 0
    Get-ChildItem tests\unit\test_*.py | ForEach-Object {
        Write-Host "--- $label $($_.Name) ---"
        & $PY $WD 300 -- $PY -m pytest $_.FullName -v -rA --timeout 120 --timeout-method=thread
        if ($LASTEXITCODE -ne 0) { $fail = 1 }
    }
    Write-Host "--- $label tests\conformance ---"
    & $PY $WD 600 -- $PY -m pytest tests\conformance -v -rA --timeout 120 --timeout-method=thread
    if ($LASTEXITCODE -ne 0) { $fail = 1 }
    cmd /c exit $fail
}

Step "05-pytest-iocp" {
    Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
    PytestSweep "iocp"
}

if ($rioOK) {
    Step "06-pytest-rio" {
        $env:CADELOOP_BACKEND = "rio"
        PytestSweep "rio"
        $code = $LASTEXITCODE
        Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
        cmd /c exit $code
    }
    Step "07-backend-smoke" {
        & $PY $WD 900 -- $PY tools\windows\rio_smoke.py iocp rio --out (Join-Path $R "backend-smoke.json")
    }
} else {
    SkipStep "06-pytest-rio"
    Step "07-backend-smoke" {
        & $PY $WD 900 -- $PY tools\windows\rio_smoke.py iocp --out (Join-Path $R "backend-smoke.json")
    }
}

Step "08-cpython-conformance" {
    & $PY $WD 1200 -- $PY tests\conformance\run_cpython_suite.py
}

Step "09-bench-sched" {
    Set-Location bench
    & $PY ..\$WD 1800 -- $PY harness\harness.py --suite sched --loops cadeloop,asyncio,winloop,rsloop --out (Join-Path $R "win-sched.json")
    Set-Location $repo
}

Step "10-bench-echo-rtt-iocp" {
    Set-Location bench
    Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
    & $PY ..\$WD 1200 -- $PY harness\harness.py --suite echo --loops cadeloop,asyncio,winloop,rsloop --conns 1 --msgs 5000 --out (Join-Path $R "win-echo-rtt-iocp.json")
    Set-Location $repo
}

if ($rioOK) {
    Step "11-bench-echo-rtt-rio" {
        Set-Location bench
        $env:CADELOOP_BACKEND = "rio"
        & $PY ..\$WD 900 -- $PY harness\harness.py --suite echo --loops cadeloop --conns 1 --msgs 5000 --out (Join-Path $R "win-echo-rtt-rio.json")
        Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
        Set-Location $repo
    }
} else {
    SkipStep "11-bench-echo-rtt-rio"
}

Step "12-bench-echo-64-iocp" {
    Set-Location bench
    & $PY ..\$WD 1200 -- $PY harness\harness.py --suite echo --loops cadeloop,asyncio,winloop,rsloop --conns 64 --msgs 2000 --out (Join-Path $R "win-echo-64-iocp.json")
    Set-Location $repo
}

if ($rioOK) {
    Step "13-bench-echo-64-rio" {
        Set-Location bench
        $env:CADELOOP_BACKEND = "rio"
        & $PY ..\$WD 900 -- $PY harness\harness.py --suite echo --loops cadeloop --conns 64 --msgs 2000 --out (Join-Path $R "win-echo-64-rio.json")
        Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
        Set-Location $repo
    }
} else {
    SkipStep "13-bench-echo-64-rio"
}

$httpContenders = "cadeloop-native,uvicorn+cadeloop,uvicorn+asyncio,uvicorn+winloop,uvicorn+rsloop,hypercorn"
if ($rioOK) { $httpContenders = "cadeloop-native,cadeloop-native-rio,uvicorn+cadeloop,uvicorn+asyncio,uvicorn+winloop,uvicorn+rsloop,hypercorn" }
Step "14-bench-http" {
    Set-Location bench
    & $PY ..\$WD 1800 -- $PY harness\harness.py --suite http --contenders $httpContenders --conns 64 --seconds 3 --out (Join-Path $R "win-http.json")
    Set-Location $repo
}

if ($rioOK) {
    Step "15-bench-http-uvicorn-on-rio" {
        Set-Location bench
        $env:CADELOOP_BACKEND = "rio"
        & $PY ..\$WD 900 -- $PY harness\harness.py --suite http --contenders uvicorn+cadeloop --conns 64 --seconds 3 --out (Join-Path $R "win-http-uvicorn-rio.json")
        Remove-Item Env:\CADELOOP_BACKEND -ErrorAction SilentlyContinue
        Set-Location $repo
    }
} else {
    SkipStep "15-bench-http-uvicorn-on-rio"
}

Step "16-wheel" {
    & $PY $WD 1200 -- $PY -m maturin build --release
    $wheel = Get-ChildItem target\wheels\*.whl | Select-Object -First 1
    Copy-Item $wheel.FullName $R -Force
    & $PY -m venv (Join-Path $R "wheelvenv")
    & (Join-Path $R "wheelvenv\Scripts\python.exe") -m pip install $wheel.FullName
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

Step "17-soak" {
    & $PY $WD 300 -- $PY tests\stress\soak_timers.py --seconds 120
}

Step "18-workers-degrade" {
    & $PY $WD 120 -- $PY -c @"
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

$summaryPath = Join-Path $R "SUMMARY.txt"
$summary | Out-File -Encoding utf8 $summaryPath
Get-Content $summaryPath | Write-Host
Remove-Item (Join-Path $R "wheelvenv") -Recurse -Force -ErrorAction SilentlyContinue
$zip = Join-Path $repo "cadeloop-windows-results.zip"
Remove-Item $zip -ErrorAction SilentlyContinue
Compress-Archive -Path "$R\*" -DestinationPath $zip
Write-Host "`nDELIVERABLE: $zip" -ForegroundColor Green
