#!/usr/bin/env python3
"""Threaded keep-alive HTTP/1.1 load generator (loop-independent).

Each thread owns one persistent connection and issues sequential GETs for
--seconds. Prints one JSON line: {"rps", "p50_us", "p99_us", "conns",
"total_requests", "errors"}.
"""

import argparse
import json
import socket
import threading
import time

REQUEST = b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n"


def read_response(sock, buf):
    """Read one complete HTTP/1.1 response; returns leftover bytes."""
    while True:
        header_end = buf.find(b"\r\n\r\n")
        if header_end != -1:
            break
        chunk = sock.recv(65536)
        if not chunk:
            raise ConnectionError("closed mid-response")
        buf += chunk
    headers = buf[:header_end].lower()
    cl_at = headers.find(b"content-length:")
    if cl_at == -1:
        raise ConnectionError("no content-length (chunked not supported by this client)")
    cl_end = headers.find(b"\r\n", cl_at)
    length = int(headers[cl_at + 15 : cl_end if cl_end != -1 else None].strip())
    body_have = len(buf) - header_end - 4
    while body_have < length:
        chunk = sock.recv(65536)
        if not chunk:
            raise ConnectionError("closed mid-body")
        buf += chunk
        body_have += len(chunk)
    total = header_end + 4 + length
    return buf[total:]


def worker(port, seconds, barrier, out, idx):
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    lat = []
    errors = 0
    barrier.wait()
    deadline = time.perf_counter() + seconds
    leftover = b""
    while time.perf_counter() < deadline:
        t0 = time.perf_counter_ns()
        try:
            s.sendall(REQUEST)
            leftover = read_response(s, leftover)
        except ConnectionError:
            errors += 1
            s.close()
            s = socket.create_connection(("127.0.0.1", port))
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            leftover = b""
            continue
        lat.append(time.perf_counter_ns() - t0)
    s.close()
    out[idx] = (lat, errors)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--conns", type=int, default=64)
    parser.add_argument("--seconds", type=float, default=3.0)
    args = parser.parse_args()

    barrier = threading.Barrier(args.conns + 1)
    out = [None] * args.conns
    threads = [
        threading.Thread(target=worker, args=(args.port, args.seconds, barrier, out, i))
        for i in range(args.conns)
    ]
    for t in threads:
        t.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0

    lats = sorted(x for (lat, _err) in out for x in lat)
    errors = sum(err for (_lat, err) in out)
    total = len(lats)
    print(
        json.dumps(
            {
                "rps": total / elapsed,
                "p50_us": lats[total // 2] / 1000 if total else 0,
                "p99_us": lats[int(total * 0.99)] / 1000 if total else 0,
                "conns": args.conns,
                "total_requests": total,
                "errors": errors,
            }
        )
    )


if __name__ == "__main__":
    main()
