# Linux benchmark results: full tables

Every server and workload from the Linux run summarised in the
[README](../README.md#linux-epoll), with all columns. Method and machine:
[benchmarks.md](benchmarks.md#linux-environment); raw data:
[bench/baselines/linux-2026-10/](../bench/baselines/linux-2026-10/).

Columns: requests or messages per second (median of three runs, with the
slowest and fastest run); latency percentiles from all three runs' merged
histograms; server CPU (user plus kernel time of the server's process tree)
per successful operation; peak unique memory (USS), open file descriptors
("handles") and threads of the server's process tree during the run; failed
operations. "cadeloop" in the HTTP, WebSocket and TLS tables is
`cadeloop.serve()`; "uvicorn[standard]" is uvicorn with httptools and
websockets on uvloop; the other uvicorn rows use h11 and wsproto.

In the idle-connection table, granian's 5,000 row is not comparable: with its
default settings it accepted 648 of the connections and stopped there, 4,096
more waited unaccepted in the kernel's queue, and 256 timed out while
connecting, so its process held far fewer than the others.

## HTTP

**plaintext-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 117.3 K (116.7 K–120.8 K) | 479 µs | 1.28 ms | 2.33 ms | 8.4 µs | 25 MiB | 72 | 1 | 0 |
| granian | 48.7 K (43.0 K–49.0 K) | 1.31 ms | 2.92 ms | 5.37 ms | 31.5 µs | 25 MiB | 99 | 6 | 0 |
| uvicorn[standard] | 24.5 K (23.9 K–25.4 K) | 2.49 ms | 4.92 ms | 7.73 ms | 40.3 µs | 24 MiB | 78 | 5 | 0 |
| uvicorn+cadeloop | 9.6 K (9.2 K–9.7 K) | 6.42 ms | 10.49 ms | 15.99 ms | 103.2 µs | 31 MiB | 73 | 1 | 0 |
| uvicorn+uvloop | 6.4 K (6.2 K–6.9 K) | 9.31 ms | 15.20 ms | 22.81 ms | 155.1 µs | 23 MiB | 78 | 5 | 0 |
| uvicorn+asyncio | 5.3 K (5.2 K–5.3 K) | 11.67 ms | 18.09 ms | 26.74 ms | 187.1 µs | 21 MiB | 71 | 1 | 0 |
| hypercorn+asyncio | 3.6 K (3.4 K–3.8 K) | 16.52 ms | 26.48 ms | 33.29 ms | 278.2 µs | 21 MiB | 71 | 1 | 0 |

**plaintext-c1**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 28.1 K (25.2 K–28.2 K) | 32 µs | 85 µs | 266 µs | 34.2 µs | 25 MiB | 9 | 1 | 0 |
| granian | 8.0 K (6.4 K–8.2 K) | 120 µs | 373 µs | 1.04 ms | 102.4 µs | 25 MiB | 36 | 6 | 0 |
| uvicorn[standard] | 12.8 K (12.0 K–13.3 K) | 71 µs | 184 µs | 565 µs | 66.2 µs | 24 MiB | 15 | 5 | 0 |
| uvicorn+cadeloop | 5.6 K (4.9 K–6.0 K) | 162 µs | 504 µs | 1.20 ms | 160.1 µs | 31 MiB | 10 | 1 | 0 |
| uvicorn+uvloop | 6.3 K (5.5 K–6.6 K) | 141 µs | 442 µs | 1.01 ms | 154.8 µs | 23 MiB | 15 | 5 | 0 |
| uvicorn+asyncio | 5.3 K (4.9 K–5.7 K) | 160 µs | 541 µs | 1.23 ms | 179.8 µs | 21 MiB | 8 | 1 | 0 |
| hypercorn+asyncio | 3.3 K (3.1 K–3.4 K) | 270 µs | 664 µs | 1.88 ms | 289.3 µs | 21 MiB | 72 | 1 | 0 |

**pipelined16-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 320.4 K (310.4 K–323.1 K) | 3.08 ms | 6.68 ms | 12.85 ms | 3.1 µs | 25 MiB | 72 | 1 | 0 |
| granian | 50.5 K (49.5 K–52.0 K) | 10.75 ms | 24.90 ms | 44.04 ms | 31.5 µs | 25 MiB | 99 | 6 | 0 |
| uvicorn[standard] | 21.7 K (20.1 K–22.1 K) | 27.53 ms | 60.82 ms | 71.30 ms | 46.2 µs | 27 MiB | 78 | 5 | 0 |
| uvicorn+cadeloop | 9.3 K (8.0 K–9.5 K) | 63.44 ms | 144.70 ms | 169.87 ms | 106.3 µs | 30 MiB | 73 | 1 | 0 |
| uvicorn+uvloop | 6.6 K (6.3 K–7.2 K) | 82.84 ms | 182.45 ms | 207.62 ms | 150.2 µs | 23 MiB | 78 | 5 | 0 |
| uvicorn+asyncio | 5.8 K (5.7 K–5.8 K) | 98.57 ms | 197.13 ms | 224.40 ms | 172.2 µs | 21 MiB | 71 | 1 | 0 |
| hypercorn+asyncio | 4.4 K (3.9 K–4.5 K) | 127.93 ms | 272.63 ms | 318.77 ms | 226.4 µs | 22 MiB | 72 | 1 | 0 |

**json-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 49.9 K (41.1 K–51.2 K) | 1.25 ms | 3.11 ms | 6.42 ms | 19.7 µs | 24 MiB | 72 | 1 | 0 |
| granian | 27.4 K (26.4 K–28.0 K) | 2.29 ms | 4.26 ms | 9.83 ms | 54.5 µs | 26 MiB | 99 | 6 | 0 |
| uvicorn[standard] | 13.7 K (13.3 K–15.9 K) | 4.10 ms | 7.93 ms | 16.65 ms | 71.9 µs | 27 MiB | 78 | 5 | 0 |
| uvicorn+cadeloop | 7.9 K (7.3 K–8.5 K) | 7.60 ms | 14.16 ms | 25.17 ms | 124.0 µs | 30 MiB | 73 | 1 | 0 |
| uvicorn+uvloop | 5.9 K (5.9 K–6.1 K) | 10.35 ms | 15.73 ms | 22.28 ms | 167.9 µs | 23 MiB | 78 | 5 | 0 |
| uvicorn+asyncio | 5.4 K (5.3 K–5.6 K) | 11.01 ms | 18.61 ms | 30.93 ms | 177.8 µs | 21 MiB | 71 | 1 | 0 |
| hypercorn+asyncio | 3.4 K (3.0 K–3.6 K) | 18.09 ms | 28.84 ms | 56.10 ms | 294.8 µs | 23 MiB | 135 | 1 | 0 |

**large64k-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 30.9 K (27.3 K–31.1 K) | 2.00 ms | 4.13 ms | 7.73 ms | 32.0 µs | 29 MiB | 72 | 1 | 0 |
| granian | 28.7 K (28.0 K–31.6 K) | 2.03 ms | 5.37 ms | 9.70 ms | 57.1 µs | 29 MiB | 99 | 6 | 0 |
| uvicorn[standard] | 18.4 K (17.3 K–19.1 K) | 3.31 ms | 6.55 ms | 14.42 ms | 53.5 µs | 27 MiB | 78 | 5 | 0 |
| uvicorn+cadeloop | 6.9 K (6.6 K–7.3 K) | 8.65 ms | 14.55 ms | 25.69 ms | 142.9 µs | 30 MiB | 73 | 1 | 0 |
| uvicorn+uvloop | 6.1 K (5.8 K–6.3 K) | 10.09 ms | 15.73 ms | 24.12 ms | 161.9 µs | 23 MiB | 78 | 5 | 0 |
| uvicorn+asyncio | 5.4 K (4.8 K–5.6 K) | 11.27 ms | 20.45 ms | 24.38 ms | 181.7 µs | 21 MiB | 71 | 1 | 0 |
| hypercorn+asyncio | 3.5 K (3.4 K–3.5 K) | 17.56 ms | 28.31 ms | 37.22 ms | 283.5 µs | 23 MiB | 135 | 1 | 0 |

**stream1m-c16**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 2.1 K (2.0 K–2.2 K) | 7.34 ms | 11.67 ms | 14.55 ms | 481.9 µs | 28 MiB | 24 | 1 | 0 |
| granian | 1.8 K (1.7 K–1.9 K) | 8.78 ms | 14.68 ms | 46.14 ms | 996.1 µs | 29 MiB | 51 | 6 | 0 |
| uvicorn[standard] | 1.1 K (1.1 K–1.2 K) | 13.50 ms | 24.90 ms | 36.70 ms | 878.2 µs | 27 MiB | 30 | 5 | 0 |
| uvicorn+cadeloop | 1.0 K (992–1.1 K) | 14.81 ms | 21.76 ms | 41.42 ms | 962.2 µs | 30 MiB | 25 | 1 | 0 |
| uvicorn+uvloop | 620 (571–631) | 25.69 ms | 39.32 ms | 52.43 ms | 1596.6 µs | 23 MiB | 30 | 5 | 0 |
| uvicorn+asyncio | 591 (587–619) | 26.21 ms | 38.27 ms | 42.99 ms | 1670.9 µs | 21 MiB | 23 | 1 | 0 |
| hypercorn+asyncio | 395 (383–416) | 38.80 ms | 55.57 ms | 58.72 ms | 2491.0 µs | 23 MiB | 87 | 1 | 0 |

**upload16k-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 61.7 K (60.6 K–65.3 K) | 983 µs | 2.13 ms | 3.90 ms | 15.9 µs | 27 MiB | 72 | 1 | 0 |
| granian | 16.6 K (15.8 K–16.7 K) | 3.80 ms | 6.75 ms | 24.64 ms | 84.4 µs | 37 MiB | 99 | 6 | 0 |
| uvicorn[standard] | 15.4 K (15.2 K–16.2 K) | 3.80 ms | 7.80 ms | 20.71 ms | 63.5 µs | 27 MiB | 78 | 5 | 0 |
| uvicorn+cadeloop | 7.1 K (6.7 K–7.2 K) | 8.65 ms | 14.55 ms | 36.70 ms | 136.5 µs | 33 MiB | 73 | 1 | 0 |
| uvicorn+uvloop | 5.3 K (5.2 K–5.4 K) | 11.67 ms | 17.56 ms | 35.13 ms | 188.4 µs | 24 MiB | 78 | 5 | 0 |
| uvicorn+asyncio | 4.3 K (4.1 K–4.9 K) | 13.37 ms | 22.28 ms | 28.57 ms | 231.6 µs | 22 MiB | 71 | 1 | 0 |
| hypercorn+asyncio | 3.3 K (3.2 K–3.7 K) | 18.09 ms | 29.36 ms | 42.99 ms | 301.2 µs | 25 MiB | 87 | 1 | 0 |

**sleep1ms-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 29.9 K (28.8 K–31.7 K) | 2.10 ms | 3.51 ms | 6.29 ms | 15.9 µs | 26 MiB | 72 | 1 | 0 |
| granian | 28.9 K (28.2 K–30.6 K) | 2.05 ms | 4.78 ms | 20.19 ms | 40.9 µs | 37 MiB | 99 | 6 | 0 |
| uvicorn[standard] | 17.9 K (16.5 K–18.5 K) | 3.64 ms | 6.88 ms | 13.50 ms | 49.8 µs | 27 MiB | 78 | 5 | 0 |
| uvicorn+cadeloop | 9.5 K (9.1 K–9.7 K) | 6.55 ms | 11.14 ms | 26.21 ms | 102.9 µs | 33 MiB | 73 | 1 | 0 |
| uvicorn+uvloop | 6.3 K (5.8 K–6.4 K) | 9.83 ms | 15.99 ms | 28.05 ms | 154.5 µs | 23 MiB | 78 | 5 | 0 |
| uvicorn+asyncio | 5.2 K (5.1 K–5.6 K) | 11.01 ms | 19.66 ms | 35.65 ms | 186.0 µs | 21 MiB | 71 | 1 | 0 |
| hypercorn+asyncio | 3.8 K (3.6 K–4.1 K) | 15.73 ms | 26.74 ms | 41.42 ms | 259.4 µs | 25 MiB | 135 | 1 | 0 |

**churn-c16**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 16.9 K (16.0 K–20.4 K) | 365 µs | 1.93 ms | 4.13 ms | 42.9 µs | 25 MiB | 25 | 1 | 0 |
| granian | 12.8 K (12.7 K–13.6 K) | 754 µs | 2.95 ms | 5.83 ms | 72.0 µs | 33 MiB | 51 | 6 | 0 |
| uvicorn[standard] | 10.5 K (10.4 K–10.6 K) | 1.29 ms | 3.34 ms | 7.67 ms | 91.4 µs | 27 MiB | 22 | 5 | 0 |
| uvicorn+cadeloop | 5.2 K (5.0 K–5.4 K) | 2.72 ms | 5.51 ms | 19.14 ms | 186.2 µs | 33 MiB | 25 | 1 | 0 |
| uvicorn+uvloop | 4.1 K (3.5 K–4.3 K) | 3.51 ms | 7.54 ms | 12.85 ms | 238.8 µs | 23 MiB | 17 | 5 | 0 |
| uvicorn+asyncio | 4.2 K (3.7 K–4.2 K) | 3.54 ms | 8.06 ms | 13.50 ms | 237.0 µs | 21 MiB | 23 | 1 | 0 |
| hypercorn+asyncio | 2.2 K (2.2 K–2.3 K) | 6.62 ms | 14.68 ms | 25.95 ms | 450.5 µs | 26 MiB | 87 | 1 | 0 |

## WebSocket

**ws-64b-c1**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 27.7 K (26.4 K–29.0 K) | 31 µs | 91 µs | 299 µs | 34.4 µs | 23 MiB | 9 | 1 | 0 |
| granian | 6.8 K (6.8 K–7.4 K) | 128 µs | 328 µs | 1.04 ms | 149.2 µs | 23 MiB | 36 | 6 | 0 |
| uvicorn[standard] | 12.1 K (9.2 K–13.1 K) | 74 µs | 291 µs | 827 µs | 72.6 µs | 24 MiB | 15 | 5 | 0 |
| uvicorn+uvloop | 10.3 K (9.5 K–10.5 K) | 86 µs | 260 µs | 762 µs | 74.3 µs | 23 MiB | 15 | 5 | 0 |
| uvicorn+asyncio | 5.8 K (5.7 K–6.6 K) | 147 µs | 463 µs | 1.10 ms | 138.6 µs | 20 MiB | 8 | 1 | 0 |
| hypercorn+asyncio | 6.5 K (5.6 K–6.6 K) | 143 µs | 467 µs | 1.21 ms | 127.9 µs | 20 MiB | 8 | 1 | 0 |

**ws-64b-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 114.3 K (113.3 K–124.2 K) | 504 µs | 1.28 ms | 2.98 ms | 8.6 µs | 25 MiB | 72 | 1 | 0 |
| granian | 50.6 K (48.0 K–51.5 K) | 1.20 ms | 2.95 ms | 5.51 ms | 33.8 µs | 33 MiB | 99 | 6 | 0 |
| uvicorn[standard] | 19.2 K (17.6 K–19.3 K) | 3.24 ms | 5.70 ms | 10.88 ms | 51.4 µs | 26 MiB | 78 | 5 | 0 |
| uvicorn+uvloop | 22.6 K (20.9 K–23.9 K) | 2.69 ms | 5.31 ms | 11.01 ms | 43.1 µs | 24 MiB | 78 | 5 | 0 |
| uvicorn+asyncio | 10.3 K (10.0 K–10.5 K) | 5.83 ms | 10.88 ms | 27.79 ms | 94.3 µs | 21 MiB | 71 | 1 | 0 |
| hypercorn+asyncio | 11.6 K (11.2 K–11.7 K) | 5.24 ms | 8.78 ms | 12.19 ms | 84.9 µs | 22 MiB | 71 | 1 | 0 |

**ws-16k-c16**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 42.1 K (37.6 K–48.3 K) | 332 µs | 967 µs | 2.65 ms | 22.7 µs | 25 MiB | 24 | 1 | 0 |
| granian | 27.5 K (25.9 K–28.6 K) | 541 µs | 1.47 ms | 3.87 ms | 58.0 µs | 34 MiB | 51 | 6 | 0 |
| uvicorn[standard] | 16.4 K (14.4 K–16.9 K) | 934 µs | 1.87 ms | 3.44 ms | 60.6 µs | 27 MiB | 30 | 5 | 0 |
| uvicorn+uvloop | 12.8 K (11.9 K–13.6 K) | 1.11 ms | 2.88 ms | 4.59 ms | 76.7 µs | 24 MiB | 30 | 5 | 0 |
| uvicorn+asyncio | 7.6 K (7.2 K–7.7 K) | 1.98 ms | 3.74 ms | 9.18 ms | 130.4 µs | 22 MiB | 23 | 1 | 0 |
| hypercorn+asyncio | 7.4 K (7.0 K–7.5 K) | 2.05 ms | 3.70 ms | 6.49 ms | 133.4 µs | 23 MiB | 23 | 1 | 0 |

**ws-stream-1k-w32-c16**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 380.5 K (312.4 K–406.7 K) | 1.20 ms | 4.59 ms | 9.18 ms | 2.6 µs | 27 MiB | 24 | 1 | 0 |
| granian | 35.4 K (33.6 K–36.1 K) | 14.16 ms | 22.02 ms | 60.82 ms | 39.6 µs | 28 MiB | 51 | 6 | 0 |
| uvicorn[standard] | 39.7 K (38.6 K–41.4 K) | 12.19 ms | 21.76 ms | 36.18 ms | 23.9 µs | 27 MiB | 30 | 5 | 0 |
| uvicorn+uvloop | 28.8 K (25.9 K–29.9 K) | 16.52 ms | 39.32 ms | 47.19 ms | 33.2 µs | 24 MiB | 30 | 5 | 0 |
| uvicorn+asyncio | 29.4 K (28.1 K–29.7 K) | 16.38 ms | 33.03 ms | 40.89 ms | 32.7 µs | 22 MiB | 23 | 1 | 0 |
| hypercorn+asyncio | 23.0 K (19.9 K–23.9 K) | 21.50 ms | 43.52 ms | 56.10 ms | 42.9 µs | 23 MiB | 23 | 1 | 0 |

## TLS

**tls-plaintext-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 65.0 K (63.4 K–67.6 K) | 950 µs | 2.13 ms | 3.87 ms | 15.2 µs | 24 MiB | 72 | 1 | 0 |
| granian | 45.4 K (41.0 K–46.9 K) | 1.36 ms | 3.24 ms | 7.08 ms | 34.1 µs | 23 MiB | 99 | 6 | 0 |
| uvicorn[standard] | 15.4 K (15.4 K–15.4 K) | 3.96 ms | 7.86 ms | 15.60 ms | 63.6 µs | 24 MiB | 78 | 5 | 0 |
| uvicorn+uvloop | 5.9 K (5.7 K–5.9 K) | 10.49 ms | 16.52 ms | 21.76 ms | 168.3 µs | 22 MiB | 78 | 5 | 0 |
| uvicorn+asyncio | 5.2 K (4.8 K–5.4 K) | 11.93 ms | 19.92 ms | 22.02 ms | 190.0 µs | 37 MiB | 71 | 1 | 0 |

**tls-churn-c16**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 1.1 K (1.1 K–1.1 K) | 4.52 ms | 9.04 ms | 12.98 ms | 885.9 µs | 24 MiB | 29 | 1 | 0 |
| granian | 1.1 K (1.0 K–1.1 K) | 7.93 ms | 18.35 ms | 24.64 ms | 948.3 µs | 23 MiB | 51 | 6 | 0 |
| uvicorn[standard] | 763 (750–860) | 3.44 ms | 5.96 ms | 10.09 ms | 1285.4 µs | 24 MiB | 22 | 5 | 0 |
| uvicorn+uvloop | 690 (645–726) | 3.15 ms | 6.16 ms | 10.22 ms | 1420.9 µs | 22 MiB | 21 | 5 | 0 |
| uvicorn+asyncio | 753 (673–774) | 5.83 ms | 14.42 ms | 18.61 ms | 1306.6 µs | 37 MiB | 31 | 1 | 0 |

## TCP echo (raw asyncio.Protocol)

**echo-1k-c1**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 33.7 K (29.8 K–35.9 K) | 26 µs | 76 µs | 274 µs | 28.3 µs | 19 MiB | 9 | 1 | 0 |
| uvloop | 24.2 K (23.2 K–24.7 K) | 39 µs | 85 µs | 250 µs | 20.2 µs | 14 MiB | 15 | 5 | 0 |
| rloop | 23.6 K (23.1 K–24.9 K) | 39 µs | 88 µs | 266 µs | 19.6 µs | 12 MiB | 10 | 1 | 0 |
| rsloop | 24.6 K (23.4 K–24.6 K) | 38 µs | 85 µs | 274 µs | 20.1 µs | 15 MiB | 13 | 5 | 0 |
| asyncio | 20.4 K (20.2 K–20.6 K) | 45 µs | 109 µs | 344 µs | 28.7 µs | 11 MiB | 8 | 1 | 0 |

**echo-1k-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 169.8 K (146.3 K–176.8 K) | 348 µs | 975 µs | 2.98 ms | 5.8 µs | 21 MiB | 72 | 1 | 0 |
| uvloop | 149.4 K (143.5 K–154.8 K) | 389 µs | 1.05 ms | 2.23 ms | 6.6 µs | 14 MiB | 78 | 5 | 0 |
| rloop | 170.6 K (156.2 K–171.4 K) | 356 µs | 827 µs | 2.46 ms | 5.7 µs | 12 MiB | 74 | 1 | 0 |
| rsloop | 150.0 K (148.0 K–160.8 K) | 377 µs | 975 µs | 2.82 ms | 6.4 µs | 16 MiB | 76 | 5 | 0 |
| asyncio | 115.2 K (112.3 K–115.5 K) | 487 µs | 1.46 ms | 3.67 ms | 8.4 µs | 11 MiB | 71 | 1 | 0 |

**echo-1k-w16-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 352.1 K (345.2 K–359.4 K) | 1.36 ms | 14.94 ms | 25.43 ms | 2.8 µs | 23 MiB | 72 | 1 | 0 |
| uvloop | 366.4 K (349.7 K–373.5 K) | 100 µs | 18.09 ms | 31.20 ms | 2.6 µs | 14 MiB | 78 | 5 | 0 |
| rloop | 366.3 K (358.6 K–378.6 K) | 76 µs | 18.87 ms | 36.18 ms | 2.6 µs | 12 MiB | 74 | 1 | 0 |
| rsloop | 381.1 K (376.6 K–384.5 K) | 1.15 ms | 13.63 ms | 21.76 ms | 2.5 µs | 19 MiB | 76 | 5 | 0 |
| asyncio | 379.1 K (348.7 K–381.0 K) | 1.10 ms | 14.68 ms | 22.81 ms | 2.6 µs | 11 MiB | 71 | 1 | 0 |

**echo-64k-c8**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 31.9 K (26.4 K–33.6 K) | 244 µs | 803 µs | 2.08 ms | 30.8 µs | 24 MiB | 16 | 1 | 0 |
| uvloop | 39.1 K (39.0 K–39.3 K) | 180 µs | 483 µs | 1.20 ms | 25.1 µs | 14 MiB | 22 | 5 | 0 |
| rloop | 42.0 K (41.9 K–45.7 K) | 176 µs | 401 µs | 1.31 ms | 23.3 µs | 12 MiB | 82 | 1 | 0 |
| rsloop | 36.1 K (34.6 K–37.2 K) | 215 µs | 406 µs | 1.08 ms | 27.2 µs | 18 MiB | 20 | 5 | 0 |
| asyncio | 37.7 K (34.7 K–37.8 K) | 190 µs | 541 µs | 1.46 ms | 26.1 µs | 11 MiB | 15 | 1 | 0 |

## Idle keep-alive connections

| server | connections | private / conn | working set / conn | handles / conn | private after close |
|---|---|---|---|---|---|
| cadeloop | 1000 | 12.5 KiB | 12.5 KiB | 1.00 | 28 MiB |
| granian | 1000 | 21.4 KiB | 21.2 KiB | 1.00 | 42 MiB |
| uvicorn[standard] | 1000 | 7.5 KiB | 7.5 KiB | 1.00 | 31 MiB |
| uvicorn+cadeloop | 1000 | 18.3 KiB | 18.3 KiB | 1.00 | 42 MiB |
| uvicorn+uvloop | 1000 | 6.8 KiB | 6.8 KiB | 1.00 | 29 MiB |
| uvicorn+asyncio | 1000 | 7.0 KiB | 7.0 KiB | 1.00 | 27 MiB |
| hypercorn+asyncio | 1000 | 14.2 KiB | 14.2 KiB | 1.00 | 34 MiB |
| cadeloop | 5000 | 7.2 KiB | 7.2 KiB | 1.00 | 33 MiB |
| granian | 4744 | 1.6 KiB | 1.6 KiB | 0.14 | 48 MiB |
| uvicorn[standard] | 5000 | 6.2 KiB | 6.2 KiB | 1.00 | 48 MiB |
| uvicorn+cadeloop | 5000 | 11.7 KiB | 11.7 KiB | 1.00 | 70 MiB |
| uvicorn+uvloop | 5000 | 5.6 KiB | 5.6 KiB | 1.00 | 45 MiB |
| uvicorn+asyncio | 5000 | 5.5 KiB | 5.5 KiB | 1.00 | 53 MiB |
| hypercorn+asyncio | 5000 | 11.2 KiB | 11.2 KiB | 0.94 | 88 MiB |


## Timers and wake-ups

How late `asyncio.sleep(d)` returned on an idle loop, median / p99 over 200 sleeps (40 at 10 ms and 50 ms); `busy` is a 1 ms sleep while another task keeps the loop running short callbacks; `cross-thread` is a `call_soon_threadsafe()` from another thread into a sleeping loop.

| loop | sleep(0.5 ms) | sleep(1 ms) | sleep(5 ms) | sleep(10 ms) | sleep(50 ms) | busy, 1 ms | cross-thread |
|---|---|---|---|---|---|---|---|
| cadeloop | +0.64 / 1.32 ms | +0.13 / 0.37 ms | +0.29 / 0.49 ms | +0.32 / 1.89 ms | +0.43 / 1.19 ms | +0.01 / 0.14 ms | 93 / 642 µs |
| uvloop | +-0.50 / -0.45 ms | +0.16 / 0.98 ms | +0.20 / 1.57 ms | +0.25 / 0.46 ms | +0.31 / 0.50 ms | +-0.00 / 0.08 ms | 95 / 617 µs |
| rloop | +0.61 / 0.72 ms | +0.12 / 0.32 ms | +0.17 / 33.26 ms | +0.18 / 0.28 ms | +0.27 / 1.38 ms | +0.01 / 0.03 ms | 78 / 1034 µs |
| rsloop | +0.06 / 0.73 ms | +0.07 / 0.27 ms | +0.13 / 0.36 ms | +0.14 / 0.31 ms | +0.22 / 1.24 ms | +0.01 / 0.13 ms | 94 / 320 µs |
| asyncio | +0.65 / 0.89 ms | +0.16 / 0.28 ms | +0.22 / 0.65 ms | +0.24 / 0.46 ms | +0.37 / 4.62 ms | +0.01 / 0.71 ms | 123 / 599 µs |

## Scheduling microbenchmarks

Millions of operations per second: the median of 15 runs (three passes of 3 warmup and 5 measured runs, a fresh process per run), with the slowest and fastest run.

| benchmark | cadeloop | uvloop | rloop | rsloop | asyncio |
|---|---|---|---|---|---|
| `task_fib` | 0.096 (0.050–0.107) | 0.081 (0.062–0.093) | 0.088 (0.069–0.095) | 0.090 (0.061–0.097) | 0.056 (0.046–0.064) |
| `call_soon_chain` | 3.412 (2.189–5.433) | 1.345 (0.845–1.605) | 3.456 (2.053–3.748) | 1.939 (1.291–2.331) | 0.499 (0.372–0.560) |
| `call_soon_burst` | 4.117 (1.451–5.480) | 1.102 (0.936–1.267) | 2.536 (1.727–2.911) | 1.010 (0.637–1.475) | 0.710 (0.562–0.827) |
| `timer_schedule_cancel` | 2.693 (0.616–3.781) | 0.427 (0.368–0.497) | 1.683 (1.254–1.965) | 1.127 (0.681–1.241) | 0.492 (0.394–0.512) |
| `timer_fire` | 2.219 (0.620–2.490) | 1.136 (0.789–1.302) | 1.344 (0.892–1.531) | 0.988 (0.754–1.194) | 0.258 (0.195–0.307) |
| `sleep0_chain` | 1.653 (1.105–1.791) | 0.846 (0.616–0.926) | 1.496 (0.791–1.687) | 0.961 (0.612–1.203) | 0.368 (0.265–0.389) |
| `task_spawn` | 0.262 (0.167–0.303) | 0.252 (0.165–0.272) | 0.255 (0.165–0.276) | 0.254 (0.175–0.278) | 0.180 (0.129–0.204) |
| `threadsafe_throughput` | 3.528 (1.002–4.301) | 1.734 (1.368–1.964) | 3.674 (2.470–4.339) | 3.439 (2.573–3.689) | 0.060 (0.053–0.067) |
| `future_chain` | 1.202 (0.748–1.312) | 0.514 (0.362–0.564) | 0.869 (0.569–1.014) | 0.610 (0.397–0.691) | 0.210 (0.144–0.226) |
| `gather_fanin` | 0.269 (0.177–0.303) | 0.231 (0.137–0.264) | 0.243 (0.174–0.274) | 0.242 (0.200–0.271) | 0.153 (0.092–0.181) |
| `queue_pingpong` | 1.134 (0.697–1.276) | 1.051 (0.830–1.215) | 1.150 (0.811–1.233) | 1.129 (0.643–1.178) | 1.001 (0.598–1.135) |
| `queue_pingpong_native` | 2.764 (1.708–3.096) | 2.710 (1.493–2.872) | 2.887 (1.773–3.107) | 2.358 (1.593–2.722) | 2.041 (1.584–2.395) |
