# Windows benchmark results — full tables

Every server and workload from the Windows run summarised in the
[README](../README.md#windows-11-iocp), with all columns. Method:
[benchmarks.md](benchmarks.md#windows); data:
[bench/baselines/windows-2026-10/](../bench/baselines/windows-2026-10/).

Columns: requests or messages per second (median of three runs, with the
slowest and fastest run); latency percentiles from all three runs' merged
histograms; server CPU (user + kernel of the server's process tree) per
successful operation; peak private memory, handles and threads of the
server's process tree during the run; failed operations. "cadeloop" is the
current build, "cadeloop 1.0.1" the previous release.

## HTTP

**plaintext-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 78.0 K (77.3 K–78.1 K) | 819 µs | 991 µs | 1.75 ms | 12.8 µs | 33 MiB | 465 | 5 | 0 |
| cadeloop 1.0.1 | 73.7 K (73.5 K–74.1 K) | 860 µs | 1.06 ms | 1.57 ms | 13.5 µs | 35 MiB | 463 | 5 | 0 |
| uvicorn+cadeloop | 18.4 K (18.0 K–18.5 K) | 3.38 ms | 5.05 ms | 5.77 ms | 54.1 µs | 37 MiB | 550 | 5 | 0 |
| uvicorn+winloop | 18.0 K (17.8 K–18.0 K) | 3.44 ms | 6.95 ms | 7.73 ms | 55.3 µs | 61 MiB | 473 | 12 | 0 |
| uvicorn+asyncio | 10.8 K (10.8 K–10.9 K) | 5.83 ms | 7.01 ms | 10.09 ms | 92.2 µs | 29 MiB | 418 | 5 | 0 |
| hypercorn+asyncio | 7.6 K (7.4 K–7.6 K) | 8.39 ms | 9.96 ms | 13.11 ms | 131.5 µs | 28 MiB | 400 | 5 | 0 |

**plaintext-c1**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 47.2 K (46.5 K–48.4 K) | 20 µs | 51 µs | 112 µs | 20.6 µs | 33 MiB | 403 | 5 | 0 |
| cadeloop 1.0.1 | 45.5 K (45.0 K–46.0 K) | 20 µs | 52 µs | 115 µs | 21.6 µs | 35 MiB | 400 | 5 | 0 |
| uvicorn+cadeloop | 13.1 K (13.0 K–13.2 K) | 70 µs | 195 µs | 373 µs | 75.2 µs | 37 MiB | 424 | 5 | 0 |
| uvicorn+winloop | 10.9 K (10.1 K–11.0 K) | 81 µs | 217 µs | 385 µs | 82.7 µs | 61 MiB | 410 | 12 | 0 |
| uvicorn+asyncio | 7.7 K (7.5 K–8.0 K) | 116 µs | 270 µs | 524 µs | 119.9 µs | 29 MiB | 355 | 5 | 0 |
| hypercorn+asyncio | 5.8 K (5.8 K–5.9 K) | 158 µs | 365 µs | 639 µs | 167.9 µs | 29 MiB | 401 | 5 | 0 |

**pipelined16-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 553.2 K (550.0 K–554.1 K) | 1.82 ms | 2.98 ms | 3.21 ms | 1.8 µs | 33 MiB | 466 | 5 | 0 |
| cadeloop 1.0.1 | 544.9 K (540.6 K–546.7 K) | 1.84 ms | 3.05 ms | 3.44 ms | 1.8 µs | 35 MiB | 463 | 5 | 0 |
| uvicorn+cadeloop | 17.5 K (17.5 K–17.8 K) | 32.51 ms | 59.77 ms | 65.01 ms | 56.8 µs | 37 MiB | 550 | 5 | 0 |
| uvicorn+winloop | 17.6 K (17.5 K–17.7 K) | 31.72 ms | 59.77 ms | 65.01 ms | 56.3 µs | 61 MiB | 473 | 12 | 0 |
| uvicorn+asyncio | 13.2 K (13.2 K–13.2 K) | 46.66 ms | 78.64 ms | 89.13 ms | 75.7 µs | 29 MiB | 418 | 5 | 0 |
| hypercorn+asyncio | 7.7 K (7.6 K–7.7 K) | 77.59 ms | 136.31 ms | 146.80 ms | 130.4 µs | 30 MiB | 401 | 5 | 0 |

**json-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 57.8 K (57.2 K–58.0 K) | 1.10 ms | 1.54 ms | 2.00 ms | 17.2 µs | 33 MiB | 465 | 5 | 0 |
| cadeloop 1.0.1 | 55.1 K (55.0 K–55.9 K) | 1.15 ms | 1.62 ms | 2.02 ms | 17.9 µs | 35 MiB | 463 | 5 | 0 |
| uvicorn+cadeloop | 16.0 K (15.8 K–16.1 K) | 3.87 ms | 5.83 ms | 6.42 ms | 62.2 µs | 37 MiB | 550 | 5 | 0 |
| uvicorn+winloop | 15.9 K (15.8 K–16.0 K) | 3.90 ms | 7.80 ms | 8.52 ms | 62.8 µs | 61 MiB | 473 | 12 | 0 |
| uvicorn+asyncio | 10.0 K (9.9 K–10.0 K) | 6.36 ms | 7.73 ms | 10.35 ms | 100.0 µs | 32 MiB | 418 | 5 | 0 |
| hypercorn+asyncio | 7.0 K (6.8 K–7.0 K) | 9.18 ms | 11.67 ms | 15.20 ms | 143.6 µs | 35 MiB | 464 | 5 | 0 |

**large64k-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 41.2 K (40.4 K–41.3 K) | 1.52 ms | 2.13 ms | 3.08 ms | 24.1 µs | 39 MiB | 465 | 5 | 0 |
| cadeloop 1.0.1 | 38.6 K (38.1 K–38.6 K) | 1.62 ms | 2.42 ms | 3.34 ms | 25.8 µs | 41 MiB | 463 | 5 | 0 |
| uvicorn+cadeloop | 14.5 K (14.2 K–14.6 K) | 4.39 ms | 5.70 ms | 6.82 ms | 68.8 µs | 37 MiB | 550 | 5 | 0 |
| uvicorn+winloop | 14.9 K (14.2 K–14.9 K) | 4.26 ms | 5.77 ms | 7.60 ms | 67.0 µs | 61 MiB | 473 | 12 | 0 |
| uvicorn+asyncio | 8.9 K (8.8 K–8.9 K) | 7.08 ms | 9.04 ms | 12.85 ms | 112.0 µs | 40 MiB | 418 | 5 | 0 |
| hypercorn+asyncio | 6.4 K (6.2 K–6.4 K) | 9.96 ms | 13.37 ms | 17.30 ms | 155.7 µs | 42 MiB | 464 | 5 | 0 |

**stream1m-c16**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 3.3 K (3.2 K–3.3 K) | 4.85 ms | 6.03 ms | 7.73 ms | 306.0 µs | 39 MiB | 417 | 5 | 0 |
| cadeloop 1.0.1 | 3.1 K (3.1 K–3.1 K) | 5.05 ms | 6.62 ms | 8.78 ms | 322.6 µs | 41 MiB | 415 | 5 | 0 |
| uvicorn+cadeloop | 2.0 K (2.0 K–2.0 K) | 7.47 ms | 13.63 ms | 15.07 ms | 497.5 µs | 37 MiB | 454 | 5 | 0 |
| uvicorn+winloop | 2.0 K (2.0 K–2.0 K) | 7.14 ms | 15.34 ms | 17.04 ms | 500.3 µs | 61 MiB | 425 | 12 | 0 |
| uvicorn+asyncio | 1.1 K (1.1 K–1.1 K) | 14.42 ms | 17.30 ms | 20.19 ms | 900.2 µs | 39 MiB | 370 | 5 | 0 |
| hypercorn+asyncio | 858 (848–869) | 18.61 ms | 21.50 ms | 26.21 ms | 1162.4 µs | 42 MiB | 416 | 5 | 0 |

**upload16k-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 61.1 K (60.9 K–61.2 K) | 1.03 ms | 1.44 ms | 1.70 ms | 16.2 µs | 41 MiB | 465 | 5 | 0 |
| cadeloop 1.0.1 | 60.9 K (60.9 K–61.7 K) | 1.03 ms | 1.44 ms | 1.93 ms | 16.2 µs | 37 MiB | 463 | 5 | 0 |
| uvicorn+cadeloop | 13.0 K (12.8 K–13.1 K) | 4.78 ms | 7.34 ms | 8.13 ms | 76.1 µs | 45 MiB | 550 | 5 | 0 |
| uvicorn+winloop | 12.8 K (12.6 K–13.0 K) | 4.85 ms | 9.70 ms | 10.62 ms | 77.8 µs | 62 MiB | 473 | 12 | 0 |
| uvicorn+asyncio | 9.1 K (9.1 K–9.2 K) | 6.95 ms | 8.06 ms | 11.14 ms | 109.4 µs | 39 MiB | 418 | 5 | 0 |
| hypercorn+asyncio | 6.2 K (6.1 K–6.4 K) | 10.09 ms | 12.45 ms | 16.12 ms | 158.0 µs | 47 MiB | 416 | 5 | 0 |

**sleep1ms-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 31.6 K (31.3 K–31.8 K) | 2.06 ms | 2.69 ms | 3.31 ms | 15.3 µs | 42 MiB | 466 | 5 | 0 |
| cadeloop 1.0.1 | 4.5 K (4.5 K–4.6 K) | 15.60 ms | 17.56 ms | 18.35 ms | 20.8 µs | 36 MiB | 463 | 5 | 0 |
| uvicorn+cadeloop | 17.4 K (17.1 K–17.4 K) | 3.57 ms | 5.31 ms | 6.23 ms | 56.9 µs | 44 MiB | 550 | 5 | 0 |
| uvicorn+winloop | 10.9 K (8.4 K–11.6 K) | 3.67 ms | 19.14 ms | 21.23 ms | 59.1 µs | 61 MiB | 473 | 12 | 0 |
| uvicorn+asyncio | 10.1 K (10.0 K–10.1 K) | 6.29 ms | 7.80 ms | 9.96 ms | 99.0 µs | 38 MiB | 418 | 5 | 0 |
| hypercorn+asyncio | 7.0 K (6.9 K–7.1 K) | 9.04 ms | 11.14 ms | 14.02 ms | 141.7 µs | 49 MiB | 464 | 5 | 0 |

**churn-c16**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 24.1 K (23.8 K–24.2 K) | 336 µs | 745 µs | 1.05 ms | 40.6 µs | 39 MiB | 417 | 5 | 0 |
| cadeloop 1.0.1 | 23.8 K (23.6 K–23.8 K) | 336 µs | 770 µs | 1.08 ms | 40.6 µs | 36 MiB | 414 | 5 | 0 |
| uvicorn+cadeloop | 8.6 K (8.4 K–8.6 K) | 1.62 ms | 2.49 ms | 3.18 ms | 115.2 µs | 44 MiB | 454 | 5 | 3 |
| uvicorn+winloop | 9.2 K (9.2 K–9.2 K) | 1.49 ms | 2.26 ms | 3.44 ms | 107.4 µs | 61 MiB | 424 | 12 | 0 |
| uvicorn+asyncio | 4.7 K (4.6 K–4.7 K) | 3.18 ms | 4.72 ms | 8.52 ms | 213.5 µs | 38 MiB | 360 | 5 | 0 |
| hypercorn+asyncio | 2.9 K (2.9 K–3.0 K) | 5.11 ms | 8.00 ms | 11.27 ms | 339.5 µs | 50 MiB | 411 | 5 | 0 |


## WebSocket

**ws-64b-c1**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 48.3 K (48.2 K–49.0 K) | 19 µs | 45 µs | 105 µs | 20.3 µs | 31 MiB | 403 | 5 | 0 |
| cadeloop 1.0.1 | 45.2 K (44.3 K–46.0 K) | 21 µs | 53 µs | 112 µs | 21.8 µs | 31 MiB | 401 | 5 | 0 |
| uvicorn+winloop | 22.8 K (16.4 K–23.5 K) | 40 µs | 133 µs | 229 µs | 33.3 µs | 60 MiB | 410 | 12 | 0 |
| uvicorn+asyncio | 17.7 K (17.6 K–18.0 K) | 51 µs | 137 µs | 246 µs | 52.3 µs | 25 MiB | 354 | 5 | 0 |
| hypercorn+asyncio | 15.5 K (15.2 K–15.7 K) | 56 µs | 160 µs | 256 µs | 60.3 µs | 23 MiB | 338 | 5 | 0 |

**ws-64b-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 77.8 K (77.7 K–78.0 K) | 819 µs | 999 µs | 1.57 ms | 12.8 µs | 33 MiB | 466 | 5 | 0 |
| cadeloop 1.0.1 | 74.2 K (73.5 K–74.7 K) | 852 µs | 1.05 ms | 1.46 ms | 13.5 µs | 35 MiB | 464 | 5 | 0 |
| uvicorn+winloop | 43.3 K (30.4 K–43.6 K) | 1.44 ms | 3.93 ms | 6.03 ms | 23.0 µs | 62 MiB | 473 | 12 | 0 |
| uvicorn+asyncio | 31.3 K (21.5 K–31.6 K) | 2.03 ms | 4.52 ms | 7.73 ms | 32.0 µs | 29 MiB | 417 | 5 | 0 |
| hypercorn+asyncio | 26.3 K (25.7 K–26.5 K) | 2.39 ms | 3.18 ms | 4.59 ms | 38.1 µs | 30 MiB | 401 | 5 | 0 |

**ws-16k-c16**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 47.6 K (47.5 K–47.8 K) | 328 µs | 463 µs | 688 µs | 21.0 µs | 38 MiB | 418 | 5 | 0 |
| cadeloop 1.0.1 | 47.9 K (47.8 K–48.1 K) | 328 µs | 475 µs | 762 µs | 20.7 µs | 36 MiB | 416 | 5 | 0 |
| uvicorn+winloop | 21.0 K (15.2 K–21.1 K) | 688 µs | 2.52 ms | 3.83 ms | 47.0 µs | 62 MiB | 425 | 12 | 0 |
| uvicorn+asyncio | 18.5 K (12.1 K–18.5 K) | 860 µs | 3.24 ms | 3.64 ms | 54.1 µs | 31 MiB | 369 | 5 | 0 |
| hypercorn+asyncio | 15.4 K (15.3 K–15.4 K) | 1.01 ms | 1.46 ms | 2.29 ms | 64.9 µs | 30 MiB | 353 | 5 | 0 |

**ws-stream-1k-w32-c16**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 406.6 K (406.5 K–407.5 K) | 1.08 ms | 1.67 ms | 2.02 ms | 2.5 µs | 41 MiB | 418 | 5 | 0 |
| cadeloop 1.0.1 | 396.1 K (387.9 K–397.3 K) | 1.06 ms | 1.74 ms | 2.08 ms | 2.5 µs | 40 MiB | 416 | 5 | 0 |
| uvicorn+winloop | 92.1 K (56.4 K–93.0 K) | 5.11 ms | 17.56 ms | 26.21 ms | 10.9 µs | 63 MiB | 425 | 12 | 0 |
| uvicorn+asyncio | 66.4 K (48.9 K–67.9 K) | 7.73 ms | 19.14 ms | 26.74 ms | 15.0 µs | 32 MiB | 369 | 5 | 0 |
| hypercorn+asyncio | 56.7 K (55.2 K–56.9 K) | 9.70 ms | 12.06 ms | 13.76 ms | 17.6 µs | 31 MiB | 353 | 5 | 0 |


## TLS

**tls-plaintext-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 62.6 K (61.2 K–63.3 K) | 1.01 ms | 1.39 ms | 2.08 ms | 16.0 µs | 34 MiB | 465 | 5 | 0 |
| cadeloop 1.0.1 | 60.1 K (60.1 K–60.4 K) | 1.04 ms | 1.47 ms | 2.08 ms | 16.5 µs | 37 MiB | 463 | 5 | 0 |
| uvicorn+winloop | 15.7 K (15.5 K–16.0 K) | 3.93 ms | 7.93 ms | 8.91 ms | 63.6 µs | 79 MiB | 473 | 12 | 0 |
| uvicorn+asyncio | 9.2 K (9.1 K–9.4 K) | 6.82 ms | 8.32 ms | 10.88 ms | 108.4 µs | 47 MiB | 418 | 5 | 0 |

**tls-churn-c16**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 3.6 K (3.5 K–3.7 K) | 2.02 ms | 2.85 ms | 3.24 ms | 272.8 µs | 35 MiB | 417 | 5 | 0 |
| cadeloop 1.0.1 | 3.7 K (3.6 K–3.7 K) | 1.98 ms | 2.82 ms | 3.51 ms | 270.8 µs | 36 MiB | 415 | 5 | 0 |
| uvicorn+winloop | 2.3 K (2.3 K–2.3 K) | 3.05 ms | 4.59 ms | 5.83 ms | 428.2 µs | 69 MiB | 428 | 12 | 0 |
| uvicorn+asyncio | 1.5 K (1.4 K–1.5 K) | 2.20 ms | 3.05 ms | 4.85 ms | 666.9 µs | 30 MiB | 362 | 5 | 0 |


## TCP echo (raw asyncio.Protocol)

**echo-1k-c1**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 51.8 K (51.7 K–52.8 K) | 18 µs | 41 µs | 95 µs | 18.8 µs | 31 MiB | 399 | 5 | 0 |
| cadeloop 1.0.1 | 47.5 K (46.3 K–47.5 K) | 19 µs | 53 µs | 108 µs | 20.5 µs | 31 MiB | 397 | 5 | 0 |
| winloop | 40.4 K (40.3 K–41.4 K) | 23 µs | 54 µs | 104 µs | 13.6 µs | 54 MiB | 403 | 12 | 0 |
| rsloop | 40.4 K (40.3 K–40.8 K) | 23 µs | 48 µs | 123 µs | 29.9 µs | 21 MiB | 338 | 27 | 0 |
| asyncio | 31.5 K (31.1 K–31.7 K) | 29 µs | 79 µs | 141 µs | 23.9 µs | 18 MiB | 331 | 5 | 0 |

**echo-1k-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 84.4 K (83.3 K–84.4 K) | 762 µs | 909 µs | 1.28 ms | 11.8 µs | 32 MiB | 462 | 5 | 0 |
| cadeloop 1.0.1 | 78.7 K (78.6 K–79.6 K) | 803 µs | 1.01 ms | 1.57 ms | 12.5 µs | 34 MiB | 460 | 5 | 0 |
| winloop | 80.1 K (79.2 K–81.2 K) | 778 µs | 1.56 ms | 1.84 ms | 12.4 µs | 54 MiB | 466 | 12 | 0 |
| rsloop | 87.7 K (84.9 K–88.3 K) | 729 µs | 958 µs | 1.44 ms | 16.8 µs | 22 MiB | 466 | 26 | 0 |
| asyncio | 53.6 K (53.6 K–54.2 K) | 1.18 ms | 1.54 ms | 3.57 ms | 18.6 µs | 22 MiB | 394 | 5 | 0 |

**echo-1k-w16-c64**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 345.7 K (345.2 K–348.4 K) | 2.88 ms | 3.44 ms | 4.52 ms | 2.9 µs | 36 MiB | 462 | 5 | 0 |
| cadeloop 1.0.1 | 336.1 K (320.5 K–344.5 K) | 2.92 ms | 3.77 ms | 4.26 ms | 2.9 µs | 36 MiB | 460 | 5 | 0 |
| winloop | 374.7 K (367.8 K–376.2 K) | 2.49 ms | 5.18 ms | 5.77 ms | 2.7 µs | 55 MiB | 466 | 12 | 0 |
| rsloop | 456.9 K (452.5 K–457.3 K) | 2.03 ms | 2.49 ms | 3.34 ms | 4.4 µs | 25 MiB | 530 | 26 | 0 |
| asyncio | 286.9 K (286.3 K–287.4 K) | 3.44 ms | 4.46 ms | 6.42 ms | 3.5 µs | 27 MiB | 394 | 5 | 0 |

**echo-64k-c8**

| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors |
|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 29.8 K (29.0 K–30.7 K) | 242 µs | 614 µs | 762 µs | 33.5 µs | 38 MiB | 406 | 5 | 0 |
| cadeloop 1.0.1 | 32.6 K (32.0 K–34.0 K) | 215 µs | 549 µs | 745 µs | 30.6 µs | 35 MiB | 404 | 5 | 0 |
| winloop | 26.8 K (25.3 K–31.4 K) | 270 µs | 745 µs | 1.03 ms | 37.1 µs | 54 MiB | 410 | 12 | 0 |
| rsloop | 43.7 K (43.6 K–43.8 K) | 174 µs | 324 µs | 647 µs | 33.0 µs | 24 MiB | 482 | 26 | 0 |
| asyncio | 31.4 K (31.2 K–31.8 K) | 233 µs | 459 µs | 770 µs | 31.5 µs | 28 MiB | 338 | 5 | 0 |


## Idle keep-alive connections

| server | connections | private / conn | working set / conn | handles / conn | private after close |
|---|---|---|---|---|---|
| cadeloop | 1000 | 16.7 KiB | 8.0 KiB | 1.00 | 36 MiB |
| cadeloop 1.0.1 | 1000 | 75.7 KiB | 68.1 KiB | 1.00 | 99 MiB |
| uvicorn+cadeloop | 1000 | 21.2 KiB | 12.9 KiB | 2.00 | 51 MiB |
| uvicorn+winloop | 1000 | 7.8 KiB | 7.5 KiB | 1.00 | 67 MiB |
| uvicorn+asyncio | 1000 | 70.4 KiB | 71.4 KiB | 1.00 | 93 MiB |
| hypercorn+asyncio | 1000 | 78.9 KiB | 78.0 KiB | 1.00 | 101 MiB |
| cadeloop | 5000 | 6.6 KiB | 7.2 KiB | 1.00 | 42 MiB |
| cadeloop 1.0.1 | 5000 | 54.6 KiB | 54.5 KiB | 1.00 | 355 MiB |
| uvicorn+cadeloop | 5000 | 11.2 KiB | 11.2 KiB | 2.00 | 67 MiB |
| uvicorn+winloop | 5000 | 6.4 KiB | 6.6 KiB | 1.00 | 79 MiB |
| uvicorn+asyncio | 5000 | 57.9 KiB | 57.6 KiB | 1.00 | 376 MiB |
| hypercorn+asyncio | 5000 | 63.0 KiB | 62.8 KiB | 1.00 | 408 MiB |
