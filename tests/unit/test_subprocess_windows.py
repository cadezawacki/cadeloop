"""R-051 subprocess + pipes (M5-Windows): IOCP named-pipe transports
(cadeloop._winpipes) driving the stdlib 3.11 subprocess machinery via
connect_read_pipe/connect_write_pipe/subprocess_exec/subprocess_shell.

Mirrors test_subprocess.py's POSIX cases exactly so both platforms are
held to the same behavioral contract. Skipped everywhere but win32 —
this project's only Windows CI/hardware validation is the manual
validate.ps1 run (tools/windows/RUNBOOK.md); this file is what runs
there once wired into the suite list.
"""

import asyncio
import sys

import pytest

import cadeloop

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="IOCP named-pipe path (win32 only)")


@pytest.fixture()
def loop():
    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


def test_subprocess_exec_communicate(loop):
    async def main():
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import sys; data = sys.stdin.read(); print('got:' + data.strip())",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        out, _err = await asyncio.wait_for(proc.communicate(b"hello"), 15)
        assert out.strip() == b"got:hello"
        assert proc.returncode == 0

    loop.run_until_complete(main())


def test_subprocess_shell_and_returncode(loop):
    async def main():
        proc = await asyncio.create_subprocess_shell(
            "echo shell-ok && exit 3",
            stdout=asyncio.subprocess.PIPE,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), 15)
        assert b"shell-ok" in out
        assert proc.returncode == 3

    loop.run_until_complete(main())


def test_subprocess_parallel(loop):
    async def one(i):
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", f"print({i} * 7)", stdout=asyncio.subprocess.PIPE
        )
        out, _ = await proc.communicate()
        return int(out)

    async def main():
        results = await asyncio.wait_for(asyncio.gather(*(one(i) for i in range(5))), 20)
        assert results == [i * 7 for i in range(5)]

    loop.run_until_complete(main())


def test_subprocess_stderr_separate(loop):
    async def main():
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import sys; print('out-line'); print('err-line', file=sys.stderr)",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), 15)
        assert b"out-line" in out
        assert b"err-line" in err

    loop.run_until_complete(main())


def test_pipe_transports_direct(loop):
    """connect_read_pipe / connect_write_pipe over an overlapped named
    pipe pair (asyncio.windows_utils.pipe — the same helper
    _winpipes.SubprocessTransport uses for subprocess stdio)."""

    async def main():
        from asyncio import windows_utils

        # duplex=True: WritePipeTransport's peer-closed probe (stdlib
        # parity — see _winpipes.WritePipeTransport's docstring) issues a
        # phantom ReadFile on the write handle to detect the reader
        # going away. That requires GENERIC_READ on the write handle too
        # — exactly why windows_utils.Popen always opens stdin pipes
        # duplex=True, not just GENERIC_WRITE.
        h_read, h_write = windows_utils.pipe(duplex=True)
        r_handle = windows_utils.PipeHandle(h_read)
        w_handle = windows_utils.PipeHandle(h_write)
        rx = asyncio.get_event_loop().create_future()

        class Sink(asyncio.Protocol):
            def __init__(self):
                self.buf = b""

            def data_received(self, data):
                self.buf += data

            def connection_lost(self, exc):
                if not rx.done():
                    rx.set_result(self.buf)

        r_tr, _p = await loop.connect_read_pipe(Sink, r_handle)
        w_tr, _wp = await loop.connect_write_pipe(asyncio.BaseProtocol, w_handle)
        w_tr.write(b"through the pipe")
        w_tr.close()
        data = await asyncio.wait_for(rx, 10)
        assert data == b"through the pipe"
        r_tr.close()

    loop.run_until_complete(main())


def test_subprocess_kill_and_wait(loop):
    async def main():
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", "import time; time.sleep(30)",
        )
        proc.kill()
        rc = await asyncio.wait_for(proc.wait(), 15)
        assert rc != 0

    loop.run_until_complete(main())
