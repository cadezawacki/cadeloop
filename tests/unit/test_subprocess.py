"""R-051 subprocess + pipes (M5, POSIX): the stdlib 3.11 subprocess
machinery running on cadeloop via connect_read_pipe/connect_write_pipe
and the private readiness aliases."""

import asyncio
import os
import sys

import cadeloop
import pytest

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="POSIX subprocess path (Windows: M5, IOCP pipes)"
)


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


def test_subprocess_exec_accepts_path_like(loop):
    """CPython's subprocess_exec passes arguments straight to Popen,
    which accepts os.PathLike; cadeloop's stricter str/bytes check
    rejected a pathlib.Path program that works on the standard loop.
    Reported on PR #1."""
    import pathlib

    async def main():
        proc = await asyncio.create_subprocess_exec(
            pathlib.Path(sys.executable),
            "-c",
            "print('ok')",
            stdout=asyncio.subprocess.PIPE,
        )
        out, _err = await asyncio.wait_for(proc.communicate(), 15)
        assert out.strip() == b"ok"
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


def test_pipe_transports_direct(loop):
    # connect_read_pipe / connect_write_pipe over an os.pipe pair.
    async def main():
        r_fd, w_fd = os.pipe()
        rx = asyncio.get_event_loop().create_future()

        class Sink(asyncio.Protocol):
            def __init__(self):
                self.buf = b""

            def data_received(self, data):
                self.buf += data

            def connection_lost(self, exc):
                if not rx.done():
                    rx.set_result(self.buf)

        r_tr, _p = await loop.connect_read_pipe(Sink, os.fdopen(r_fd, "rb", 0))
        w_tr, _wp = await loop.connect_write_pipe(
            asyncio.BaseProtocol, os.fdopen(w_fd, "wb", 0)
        )
        w_tr.write(b"through the pipe")
        w_tr.close()
        data = await asyncio.wait_for(rx, 10)
        assert data == b"through the pipe"
        r_tr.close()

    loop.run_until_complete(main())
