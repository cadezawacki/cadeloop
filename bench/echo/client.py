#!/usr/bin/env python3
"""Threaded blocking-socket echo load generator.

Loop-independent by construction (no asyncio in the client), so every
server contender is measured by the same yardstick. Prints one JSON line:
{"msgs_per_sec", "p50_us", "p99_us", "conns", "size", "total_msgs"}.
"""

import argparse
import json
import socket
import threading
import time


def worker(port, size, msgs, barrier, out, idx):
    payload = bytes(idx % 256 for _ in range(size))
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    lat = []
    barrier.wait()
    recv_into = s.recv_into
    sendall = s.sendall
    buf = bytearray(size)
    view = memoryview(buf)
    for _ in range(msgs):
        t0 = time.perf_counter_ns()
        sendall(payload)
        got = 0
        while got < size:
            n = recv_into(view[got:])
            if not n:
                raise RuntimeError("peer closed")
            got += n
        lat.append(time.perf_counter_ns() - t0)
    s.close()
    out[idx] = lat


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--conns", type=int, default=64)
    parser.add_argument("--size", type=int, default=1024)
    parser.add_argument("--msgs", type=int, default=1000)
    args = parser.parse_args()

    barrier = threading.Barrier(args.conns + 1)
    out = [None] * args.conns
    threads = [
        threading.Thread(target=worker, args=(args.port, args.size, args.msgs, barrier, out, i))
        for i in range(args.conns)
    ]
    for t in threads:
        t.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0

    lats = sorted(x for lat in out for x in lat)
    total = len(lats)
    print(
        json.dumps(
            {
                "msgs_per_sec": total / elapsed,
                "p50_us": lats[total // 2] / 1000,
                "p99_us": lats[int(total * 0.99)] / 1000,
                "conns": args.conns,
                "size": args.size,
                "total_msgs": total,
            }
        )
    )


if __name__ == "__main__":
    main()
