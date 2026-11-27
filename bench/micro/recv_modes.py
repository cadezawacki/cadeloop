"""What would receiving straight into the bytes object save?

cadeloop's stream transports receive into a pooled buffer and copy what
arrived into the `bytes` the protocol is given. Receiving directly into
that object would save the copy -- 12% of a 64 KiB echo server's CPU is
that memmove -- but the copy that remains, the kernel's, then lands in a
freshly allocated object instead of a buffer that is still in cache, and
a read shorter than the object means shrinking it afterwards.

This measures only that trade, with nothing of cadeloop in it: one
receiver on a loopback TCP pair, a sender in another process, and the two
ways of producing the same `bytes`:

    direct      data = sock.recv(65536)
    warm+copy   n = sock.recv_into(buffer); data = bytes(view[:n])

    python bench/micro/recv_modes.py <sender chunk> <chunks kept alive>

Results (Linux 6.18, CPython 3.11, loopback) are in ADR-40: between 6%
faster and 7% slower depending on how full the reads are, which is why
the transport's receive path was left as it is.
"""

import os
import socket
import statistics
import subprocess
import sys
import time

READS = 60_000
BUFFER = 65536


def send_forever(size, port):
    sock = socket.create_connection(("127.0.0.1", port))
    blob = os.urandom(size)
    try:
        while True:
            sock.sendall(blob)
    except OSError:
        pass


def measure(read, hold):
    """Microseconds per 64 KiB received, and the average read size."""
    kept = []
    total = 0
    started = time.perf_counter()
    for _ in range(READS):
        data = read()
        total += len(data)
        kept.append(data)
        if len(kept) > hold:
            kept.pop(0)
    elapsed = time.perf_counter() - started
    return elapsed / total * BUFFER * 1e6, total / READS


def main():
    size, hold = int(sys.argv[1]), int(sys.argv[2])
    if len(sys.argv) > 3:
        send_forever(size, int(sys.argv[3]))
        return
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = str(listener.getsockname()[1])
    sender = subprocess.Popen([sys.executable, __file__, str(size), str(hold), port])
    conn, _ = listener.accept()
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
    warm = bytearray(BUFFER)
    view = memoryview(warm)

    def direct():
        return conn.recv(BUFFER)

    def warm_then_copy():
        n = conn.recv_into(view)
        return bytes(view[:n])

    time.sleep(0.3)
    results = {"direct": [], "warm+copy": []}
    for _ in range(5):
        results["direct"].append(measure(direct, hold))
        results["warm+copy"].append(measure(warm_then_copy, hold))
    sender.kill()
    direct_us = statistics.median(r[0] for r in results["direct"])
    warm_us = statistics.median(r[0] for r in results["warm+copy"])
    read_size = statistics.median(r[1] for r in results["direct"])
    print(
        f"sender chunk {size} B, {hold} chunks kept alive, average read {read_size:.0f} B: "
        f"per 64 KiB received, warm+copy {warm_us:.2f} us, direct {direct_us:.2f} us "
        f"({(direct_us / warm_us - 1) * 100:+.1f}%)"
    )


if __name__ == "__main__":
    main()
