# Linux benchmark results: full tables

Every server and workload from the Linux run summarized in the
[README](../README.md#linux-epoll), with all columns. Method and machine:
[benchmarks.md](benchmarks.md#linux-environment); raw data:
[bench/baselines/linux-2026-11/](../bench/baselines/linux-2026-11/).

Columns: requests or messages per second as the median of nine runs, and the
96% confidence interval of that median (the second lowest and the second
highest run; see [how sure the numbers are](benchmarks.md#how-sure-the-numbers-are));
latency percentiles from all nine runs' merged histograms; server CPU (user
plus kernel time of the server's process tree) per successful operation;
peak unique memory (USS), open file descriptors ("handles") and threads of
the server's process tree during the run; failed operations; and a note
where the load generator was at its own CPU limit, which makes that figure a
lower bound. "cadeloop" in the HTTP, FastAPI, WebSocket and TLS tables is
`cadeloop.serve()`; "uvicorn[standard]" is uvicorn with httptools and
websockets on uvloop; the other uvicorn rows use h11 and wsproto.

Three things to know when reading them:

- The handshake-per-request TLS workload appears twice. `tls-churn-c16` ran
  under an ECDSA P-256 certificate, `tls-churn-rsa-c16` under an RSA 2048
  one, and the servers compare differently under the two: an RSA signature
  is most of a request's CPU on every server, an ECDSA one is not. The
  October run did not record its certificate; the 0.9 ms of CPU per request
  it measured on this workload is what an RSA key costs.
- In the idle-connection table, granian's 5,000 row is not comparable: with
  its default settings it accepted 648 of the connections and stopped there,
  4,096 more waited unaccepted in the kernel's queue, and 256 timed out while
  connecting, so its process held far fewer than the others.
- The idle-connection figures, like the overload run in the README, are
  single measurements, not medians of nine.

## How far to trust these numbers

Across 202 cells, one run differs from the next by 4.3% (coefficient of variation; 6.5% at the 90th percentile of cells). The interval of a median is ±4.1% of it at the median cell and ±6.5% at the 90th percentile.

The same cadeloop build was measured twice, under two names, in all 28 workloads. The two medians differ by 1.4% at the median workload, 3.7% at the 90th percentile and 5.5% at most; their intervals fail to overlap in 0 of 28. A difference between two servers smaller than that is not one these runs can show.

| suite | workload | candidate | control | difference | intervals overlap |
|---|---|---|---|---|---|
| app | fastapi-file1m-c16 | 1.2 K | 1.2 K | +1.1% | yes |
| app | fastapi-file64k-c64 | 2.9 K | 3.0 K | -1.9% | yes |
| app | fastapi-json-c64 | 12.9 K | 12.9 K | -0.3% | yes |
| app | fastapi-sync-c64 | 2.3 K | 2.3 K | -0.1% | yes |
| echo | echo-1k-c1 | 34.9 K | 35.2 K | -0.8% | yes |
| echo | echo-1k-c64 | 196.7 K | 195.7 K | +0.5% | yes |
| echo | echo-1k-w16-c64 | 493.2 K | 498.2 K | -1.0% | yes |
| echo | echo-64k-c8 | 40.3 K | 39.8 K | +1.1% | yes |
| http | churn-c16 | 20.2 K | 20.0 K | +0.8% | yes |
| http | json-c64 | 62.6 K | 61.2 K | +2.2% | yes |
| http | large64k-c64 | 46.8 K | 45.8 K | +2.1% | yes |
| http | pipelined16-c64 | 402.3 K | 384.8 K | +4.4% | yes |
| http | plaintext-c1 | 27.8 K | 28.3 K | -1.8% | yes |
| http | plaintext-c64 | 142.0 K | 139.9 K | +1.4% | yes |
| http | sleep1ms-c64 | 31.3 K | 31.1 K | +0.4% | yes |
| http | stream1m-c16 | 2.8 K | 2.6 K | +5.5% | yes |
| http | upload16k-c64 | 69.5 K | 68.5 K | +1.5% | yes |
| log | log-plaintext-c64 | 127.7 K | 128.7 K | -0.8% | yes |
| tls | tls-churn-c16 | 2.6 K | 2.5 K | +2.7% | yes |
| tls | tls-churn-rsa-c16 | 1.3 K | 1.4 K | -3.7% | yes |
| tls | tls-plaintext-c64 | 78.9 K | 79.4 K | -0.6% | yes |
| tlsecho | tlsecho-16k-c16 | 36.8 K | 36.2 K | +1.8% | yes |
| tlsecho | tlsecho-1k-c64 | 67.6 K | 67.4 K | +0.3% | yes |
| ws | ws-16k-c16 | 71.6 K | 72.6 K | -1.4% | yes |
| ws | ws-256k-c8 | 5.3 K | 5.3 K | -1.6% | yes |
| ws | ws-64b-c1 | 29.2 K | 29.6 K | -1.5% | yes |
| ws | ws-64b-c64 | 145.6 K | 148.4 K | -1.9% | yes |
| ws | ws-stream-1k-w32-c16 | 456.4 K | 455.8 K | +0.1% | yes |

## HTTP

**plaintext-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 142.0 K | 133.1 K–147.0 K | 9 | 430 µs | 999 µs | 1.97 ms | 6.9 µs | 25 MiB | 72 | 1 | 0 |  |
| uvicorn[standard] | 31.4 K | 29.4 K–33.1 K | 9 | 1.95 ms | 4.13 ms | 7.47 ms | 31.5 µs | 25 MiB | 78 | 5 | 0 |  |
| uvicorn+uvloop | 7.9 K | 7.3 K–8.2 K | 9 | 7.80 ms | 14.29 ms | 22.02 ms | 124.9 µs | 23 MiB | 78 | 5 | 0 |  |
| uvicorn+asyncio | 7.6 K | 7.5 K–7.8 K | 9 | 8.13 ms | 12.06 ms | 19.92 ms | 129.9 µs | 21 MiB | 71 | 1 | 0 |  |
| uvicorn+cadeloop | 11.8 K | 11.5 K–11.9 K | 9 | 5.24 ms | 8.26 ms | 11.40 ms | 84.5 µs | 31 MiB | 73 | 1 | 0 |  |
| granian | 48.8 K | 43.9 K–50.2 K | 9 | 1.29 ms | 2.82 ms | 4.98 ms | 31.3 µs | 21 MiB | 99 | 6 | 0 |  |
| hypercorn+asyncio | 4.7 K | 4.6 K–4.8 K | 9 | 13.24 ms | 22.02 ms | 31.72 ms | 211.4 µs | 21 MiB | 71 | 1 | 0 |  |

**plaintext-c1**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 27.8 K | 26.7 K–29.1 K | 9 | 31 µs | 78 µs | 248 µs | 34.2 µs | 25 MiB | 9 | 1 | 0 |  |
| uvicorn[standard] | 15.0 K | 13.8 K–15.0 K | 9 | 63 µs | 137 µs | 434 µs | 54.3 µs | 25 MiB | 15 | 5 | 0 |  |
| uvicorn+uvloop | 7.9 K | 7.2 K–8.0 K | 9 | 115 µs | 291 µs | 770 µs | 124.3 µs | 23 MiB | 15 | 5 | 0 |  |
| uvicorn+asyncio | 7.2 K | 6.9 K–7.6 K | 9 | 125 µs | 262 µs | 713 µs | 136.2 µs | 21 MiB | 8 | 1 | 0 |  |
| uvicorn+cadeloop | 6.9 K | 6.3 K–7.2 K | 9 | 137 µs | 328 µs | 786 µs | 135.7 µs | 30 MiB | 10 | 1 | 0 |  |
| granian | 8.0 K | 7.3 K–8.1 K | 9 | 120 µs | 248 µs | 729 µs | 97.3 µs | 20 MiB | 36 | 6 | 0 |  |
| hypercorn+asyncio | 4.1 K | 4.1 K–4.2 K | 9 | 219 µs | 512 µs | 1.23 ms | 235.1 µs | 21 MiB | 72 | 1 | 0 |  |

**pipelined16-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 402.3 K | 380.1 K–405.8 K | 9 | 2.49 ms | 4.65 ms | 9.04 ms | 2.5 µs | 25 MiB | 72 | 1 | 0 |  |
| uvicorn[standard] | 24.8 K | 24.4 K–25.3 K | 9 | 23.33 ms | 51.38 ms | 63.96 ms | 39.9 µs | 27 MiB | 78 | 5 | 0 |  |
| uvicorn+uvloop | 8.2 K | 7.9 K–8.6 K | 9 | 68.16 ms | 144.70 ms | 184.55 ms | 121.0 µs | 23 MiB | 78 | 5 | 0 |  |
| uvicorn+asyncio | 7.2 K | 7.0 K–7.4 K | 9 | 80.74 ms | 171.97 ms | 201.33 ms | 137.8 µs | 21 MiB | 71 | 1 | 0 |  |
| uvicorn+cadeloop | 11.8 K | 11.1 K–12.0 K | 9 | 49.81 ms | 102.76 ms | 131.07 ms | 84.9 µs | 30 MiB | 73 | 1 | 0 |  |
| granian | 53.0 K | 48.8 K–54.3 K | 9 | 10.49 ms | 23.86 ms | 34.08 ms | 29.2 µs | 20 MiB | 99 | 6 | 0 |  |
| hypercorn+asyncio | 5.3 K | 5.0 K–5.5 K | 9 | 106.95 ms | 220.20 ms | 257.95 ms | 187.3 µs | 22 MiB | 72 | 1 | 0 |  |

**json-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 62.6 K | 60.4 K–63.2 K | 9 | 999 µs | 2.06 ms | 3.67 ms | 15.9 µs | 25 MiB | 72 | 1 | 0 |  |
| uvicorn[standard] | 18.0 K | 17.2 K–18.7 K | 9 | 3.41 ms | 5.90 ms | 10.88 ms | 55.0 µs | 27 MiB | 78 | 5 | 0 |  |
| uvicorn+uvloop | 7.1 K | 7.0 K–7.4 K | 9 | 8.78 ms | 13.11 ms | 20.45 ms | 138.8 µs | 23 MiB | 78 | 5 | 0 |  |
| uvicorn+asyncio | 6.7 K | 6.7 K–6.9 K | 9 | 9.18 ms | 13.89 ms | 21.23 ms | 146.2 µs | 21 MiB | 71 | 1 | 0 |  |
| uvicorn+cadeloop | 10.0 K | 9.7 K–10.1 K | 9 | 6.23 ms | 10.09 ms | 16.25 ms | 99.5 µs | 30 MiB | 73 | 1 | 0 |  |
| granian | 30.6 K | 28.8 K–31.5 K | 9 | 2.06 ms | 3.87 ms | 6.68 ms | 48.8 µs | 21 MiB | 99 | 6 | 0 |  |
| hypercorn+asyncio | 4.2 K | 4.2 K–4.3 K | 9 | 14.29 ms | 24.38 ms | 42.47 ms | 232.1 µs | 23 MiB | 135 | 1 | 0 |  |

**large64k-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 46.8 K | 42.7 K–47.3 K | 9 | 1.29 ms | 2.98 ms | 5.70 ms | 21.2 µs | 24 MiB | 72 | 1 | 0 |  |
| uvicorn[standard] | 21.6 K | 20.2 K–23.6 K | 9 | 2.79 ms | 5.44 ms | 7.54 ms | 45.8 µs | 27 MiB | 78 | 5 | 0 |  |
| uvicorn+uvloop | 7.2 K | 7.0 K–7.6 K | 9 | 8.52 ms | 14.29 ms | 19.66 ms | 137.2 µs | 23 MiB | 78 | 5 | 0 |  |
| uvicorn+asyncio | 6.8 K | 6.5 K–7.0 K | 9 | 9.04 ms | 15.34 ms | 20.19 ms | 146.9 µs | 21 MiB | 71 | 1 | 0 |  |
| uvicorn+cadeloop | 8.3 K | 8.0 K–8.6 K | 9 | 7.47 ms | 10.75 ms | 17.83 ms | 118.8 µs | 30 MiB | 73 | 1 | 0 |  |
| granian | 32.4 K | 31.4 K–34.3 K | 9 | 1.88 ms | 4.10 ms | 6.88 ms | 51.8 µs | 25 MiB | 99 | 6 | 0 |  |
| hypercorn+asyncio | 4.3 K | 4.0 K–4.4 K | 9 | 14.29 ms | 24.12 ms | 32.24 ms | 231.9 µs | 23 MiB | 135 | 1 | 0 |  |

**stream1m-c16**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 2.8 K | 2.5 K–2.9 K | 9 | 5.64 ms | 9.57 ms | 14.29 ms | 359.7 µs | 24 MiB | 24 | 1 | 0 |  |
| uvicorn[standard] | 1.3 K | 1.3 K–1.4 K | 9 | 11.80 ms | 20.45 ms | 26.21 ms | 738.3 µs | 27 MiB | 30 | 5 | 0 |  |
| uvicorn+uvloop | 677 | 664–706 | 9 | 23.07 ms | 35.65 ms | 50.33 ms | 1471.7 µs | 23 MiB | 30 | 5 | 0 |  |
| uvicorn+asyncio | 666 | 657–681 | 9 | 23.86 ms | 36.18 ms | 46.66 ms | 1489.8 µs | 21 MiB | 23 | 1 | 0 |  |
| uvicorn+cadeloop | 1.2 K | 1.1 K–1.2 K | 9 | 13.11 ms | 18.87 ms | 24.90 ms | 826.9 µs | 30 MiB | 25 | 1 | 0 |  |
| granian | 1.9 K | 1.8 K–2.1 K | 9 | 7.73 ms | 14.94 ms | 22.28 ms | 950.2 µs | 24 MiB | 51 | 6 | 0 |  |
| hypercorn+asyncio | 471 | 425–484 | 9 | 33.29 ms | 48.23 ms | 63.44 ms | 2105.6 µs | 23 MiB | 87 | 1 | 0 |  |

**upload16k-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 69.5 K | 68.6 K–72.1 K | 9 | 901 µs | 1.74 ms | 3.21 ms | 14.2 µs | 28 MiB | 72 | 1 | 0 |  |
| uvicorn[standard] | 22.1 K | 20.5 K–23.1 K | 9 | 2.79 ms | 5.31 ms | 8.52 ms | 44.7 µs | 27 MiB | 78 | 5 | 0 |  |
| uvicorn+uvloop | 6.8 K | 6.3 K–6.9 K | 9 | 9.31 ms | 17.83 ms | 24.12 ms | 145.9 µs | 24 MiB | 78 | 5 | 0 |  |
| uvicorn+asyncio | 6.2 K | 6.0 K–6.3 K | 9 | 9.96 ms | 15.07 ms | 23.86 ms | 160.4 µs | 22 MiB | 71 | 1 | 0 |  |
| uvicorn+cadeloop | 8.7 K | 8.5 K–8.9 K | 9 | 7.21 ms | 11.27 ms | 15.60 ms | 114.0 µs | 33 MiB | 73 | 1 | 0 |  |
| granian | 18.1 K | 17.5 K–19.3 K | 9 | 3.41 ms | 5.96 ms | 9.83 ms | 79.6 µs | 30 MiB | 99 | 6 | 0 |  |
| hypercorn+asyncio | 4.0 K | 3.8 K–4.0 K | 9 | 15.73 ms | 24.90 ms | 31.72 ms | 249.0 µs | 25 MiB | 87 | 1 | 0 |  |

**sleep1ms-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 31.3 K | 31.0 K–31.6 K | 9 | 2.03 ms | 2.72 ms | 4.46 ms | 14.4 µs | 28 MiB | 72 | 1 | 0 |  |
| uvicorn[standard] | 19.2 K | 18.2 K–19.5 K | 9 | 3.41 ms | 5.77 ms | 9.83 ms | 40.2 µs | 27 MiB | 78 | 5 | 0 |  |
| uvicorn+uvloop | 7.7 K | 7.5 K–7.9 K | 9 | 8.00 ms | 12.58 ms | 27.79 ms | 128.5 µs | 24 MiB | 78 | 5 | 0 |  |
| uvicorn+asyncio | 7.0 K | 6.7 K–7.3 K | 9 | 8.78 ms | 15.47 ms | 28.05 ms | 139.0 µs | 22 MiB | 71 | 1 | 0 |  |
| uvicorn+cadeloop | 11.1 K | 10.5 K–11.5 K | 9 | 5.51 ms | 9.83 ms | 14.68 ms | 89.1 µs | 33 MiB | 73 | 1 | 0 |  |
| granian | 29.8 K | 29.3 K–30.1 K | 9 | 2.05 ms | 4.13 ms | 9.57 ms | 40.2 µs | 33 MiB | 99 | 6 | 0 |  |
| hypercorn+asyncio | 4.5 K | 4.3 K–4.6 K | 9 | 13.76 ms | 22.54 ms | 32.77 ms | 221.4 µs | 25 MiB | 135 | 1 | 0 |  |

**churn-c16**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 20.2 K | 18.9 K–21.6 K | 9 | 303 µs | 1.54 ms | 3.01 ms | 34.8 µs | 27 MiB | 25 | 1 | 0 | load generator at 91% of its CPU: a lower bound |
| uvicorn[standard] | 13.8 K | 12.9 K–15.2 K | 9 | 958 µs | 2.69 ms | 5.18 ms | 69.7 µs | 27 MiB | 20 | 5 | 0 |  |
| uvicorn+uvloop | 5.0 K | 4.8 K–5.5 K | 9 | 2.72 ms | 5.90 ms | 9.96 ms | 196.3 µs | 24 MiB | 17 | 5 | 0 |  |
| uvicorn+asyncio | 5.1 K | 5.1 K–5.3 K | 9 | 2.85 ms | 5.44 ms | 10.75 ms | 191.5 µs | 22 MiB | 23 | 1 | 0 |  |
| uvicorn+cadeloop | 6.5 K | 6.1 K–6.7 K | 9 | 2.23 ms | 4.39 ms | 7.27 ms | 150.8 µs | 33 MiB | 25 | 1 | 0 |  |
| granian | 14.9 K | 14.3 K–15.5 K | 9 | 664 µs | 2.39 ms | 4.65 ms | 64.6 µs | 30 MiB | 51 | 6 | 0 |  |
| hypercorn+asyncio | 2.8 K | 2.7 K–2.9 K | 9 | 5.18 ms | 11.80 ms | 20.45 ms | 357.6 µs | 26 MiB | 87 | 1 | 0 |  |

## A FastAPI app

**fastapi-json-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 12.9 K | 12.7 K–13.3 K | 9 | 4.78 ms | 8.06 ms | 12.71 ms | 76.9 µs | 46 MiB | 72 | 1 | 0 |  |
| uvicorn[standard] | 8.0 K | 7.9 K–8.4 K | 9 | 7.54 ms | 12.45 ms | 19.14 ms | 123.3 µs | 42 MiB | 78 | 5 | 0 |  |
| uvicorn+uvloop | 4.9 K | 4.9 K–5.0 K | 9 | 12.45 ms | 20.71 ms | 29.10 ms | 200.7 µs | 42 MiB | 78 | 5 | 0 |  |
| uvicorn+asyncio | 4.7 K | 4.6 K–4.8 K | 9 | 13.11 ms | 21.50 ms | 28.84 ms | 211.3 µs | 40 MiB | 71 | 1 | 0 |  |
| uvicorn+cadeloop | 5.7 K | 5.4 K–5.8 K | 9 | 11.01 ms | 17.30 ms | 27.26 ms | 173.5 µs | 50 MiB | 73 | 1 | 0 |  |
| granian | 8.3 K | 7.9 K–8.7 K | 9 | 7.41 ms | 13.37 ms | 17.83 ms | 152.9 µs | 51 MiB | 99 | 6 | 0 |  |
| hypercorn+asyncio | 3.2 K | 2.7 K–3.3 K | 9 | 19.40 ms | 38.80 ms | 47.19 ms | 309.9 µs | 41 MiB | 71 | 1 | 0 |  |

**fastapi-sync-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 2.3 K | 2.1 K–2.4 K | 9 | 26.48 ms | 48.76 ms | 81.79 ms | 415.1 µs | 52 MiB | 72 | 41 | 0 |  |
| uvicorn[standard] | 1.9 K | 1.8 K–2.0 K | 9 | 31.72 ms | 66.58 ms | 78.64 ms | 509.0 µs | 46 MiB | 78 | 45 | 0 |  |
| uvicorn+uvloop | 1.6 K | 1.5 K–1.6 K | 9 | 37.75 ms | 73.40 ms | 93.32 ms | 606.1 µs | 46 MiB | 78 | 45 | 0 |  |
| uvicorn+asyncio | 1.4 K | 1.3 K–1.5 K | 9 | 44.04 ms | 78.64 ms | 100.66 ms | 738.8 µs | 43 MiB | 71 | 41 | 0 |  |
| uvicorn+cadeloop | 1.6 K | 1.6 K–1.8 K | 9 | 36.18 ms | 77.59 ms | 98.57 ms | 590.8 µs | 55 MiB | 73 | 41 | 0 |  |
| granian | 2.2 K | 2.1 K–2.2 K | 9 | 27.79 ms | 64.49 ms | 76.55 ms | 490.7 µs | 55 MiB | 99 | 46 | 0 |  |
| hypercorn+asyncio | 1.3 K | 1.3 K–1.3 K | 9 | 47.71 ms | 89.13 ms | 105.91 ms | 799.5 µs | 45 MiB | 135 | 41 | 0 |  |

**fastapi-file64k-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 2.9 K | 2.7 K–3.2 K | 9 | 20.97 ms | 33.55 ms | 46.14 ms | 352.1 µs | 54 MiB | 108 | 41 | 0 |  |
| uvicorn[standard] | 756 | 707–791 | 9 | 82.84 ms | 118.49 ms | 134.22 ms | 1435.8 µs | 55 MiB | 131 | 45 | 0 |  |
| uvicorn+uvloop | 656 | 635–687 | 9 | 94.37 ms | 134.22 ms | 153.09 ms | 1656.0 µs | 55 MiB | 131 | 45 | 0 |  |
| uvicorn+asyncio | 564 | 511–577 | 9 | 111.15 ms | 155.19 ms | 180.36 ms | 1946.8 µs | 54 MiB | 131 | 41 | 0 |  |
| uvicorn+cadeloop | 697 | 641–724 | 9 | 89.13 ms | 138.41 ms | 159.38 ms | 1552.4 µs | 64 MiB | 124 | 41 | 0 |  |
| granian | 3.5 K | 3.2 K–3.5 K | 9 | 17.56 ms | 46.14 ms | 61.34 ms | 413.6 µs | 65 MiB | 102 | 78 | 0 |  |
| hypercorn+asyncio | 521 | 502–544 | 9 | 119.54 ms | 178.26 ms | 209.72 ms | 2107.5 µs | 56 MiB | 189 | 41 | 0 |  |

**fastapi-file1m-c16**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 1.2 K | 1.1 K–1.2 K | 9 | 13.37 ms | 23.59 ms | 29.10 ms | 1060.5 µs | 85 MiB | 40 | 45 | 0 |  |
| uvicorn[standard] | 258 | 241–266 | 9 | 61.87 ms | 81.79 ms | 92.27 ms | 4210.3 µs | 56 MiB | 46 | 45 | 0 |  |
| uvicorn+uvloop | 229 | 223–241 | 9 | 68.16 ms | 91.23 ms | 101.71 ms | 4724.6 µs | 55 MiB | 46 | 45 | 0 |  |
| uvicorn+asyncio | 196 | 186–201 | 9 | 80.74 ms | 103.81 ms | 112.20 ms | 5901.6 µs | 55 MiB | 39 | 41 | 0 |  |
| uvicorn+cadeloop | 242 | 222–250 | 9 | 66.06 ms | 92.27 ms | 101.71 ms | 4468.1 µs | 65 MiB | 41 | 41 | 0 |  |
| granian | 1.5 K | 1.5 K–1.5 K | 9 | 10.49 ms | 20.45 ms | 31.46 ms | 1190.2 µs | 68 MiB | 64 | 78 | 0 |  |
| hypercorn+asyncio | 176 | 171–188 | 9 | 87.03 ms | 116.39 ms | 125.83 ms | 6458.6 µs | 57 MiB | 39 | 41 | 0 |  |

## Plaintext with the access log on

**log-plaintext-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 127.7 K | 122.8 K–133.6 K | 9 | 455 µs | 1.08 ms | 1.80 ms | 8.1 µs | 25 MiB | 73 | 2 | 0 |  |
| cadeloop, logging module | 25.9 K | 24.9 K–26.2 K | 9 | 2.33 ms | 11.80 ms | 19.40 ms | 42.1 µs | 26 MiB | 72 | 2 | 0 |  |
| uvicorn[standard] | 13.3 K | 12.9 K–13.9 K | 9 | 4.59 ms | 7.14 ms | 10.09 ms | 75.2 µs | 25 MiB | 78 | 5 | 0 |  |
| uvicorn+uvloop | 6.5 K | 6.4 K–6.6 K | 9 | 9.57 ms | 14.16 ms | 20.71 ms | 152.4 µs | 23 MiB | 78 | 5 | 0 |  |
| uvicorn+asyncio | 6.3 K | 5.9 K–6.4 K | 9 | 9.96 ms | 15.20 ms | 20.71 ms | 158.7 µs | 21 MiB | 71 | 1 | 0 |  |
| uvicorn+cadeloop | 8.2 K | 8.1 K–8.4 K | 9 | 7.47 ms | 11.67 ms | 20.45 ms | 121.7 µs | 31 MiB | 73 | 1 | 0 |  |
| granian | 14.3 K | 14.1 K–14.7 K | 9 | 4.39 ms | 7.47 ms | 12.71 ms | 103.7 µs | 21 MiB | 99 | 6 | 0 |  |
| hypercorn+asyncio | 2.8 K | 2.7 K–3.0 K | 9 | 21.50 ms | 37.22 ms | 65.01 ms | 350.9 µs | 21 MiB | 71 | 1 | 0 |  |

## WebSocket

**ws-64b-c1**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 29.2 K | 27.6 K–30.4 K | 9 | 31 µs | 75 µs | 225 µs | 32.6 µs | 23 MiB | 9 | 1 | 0 |  |
| uvicorn[standard] | 14.2 K | 13.5 K–14.7 K | 9 | 66 µs | 145 µs | 430 µs | 57.9 µs | 24 MiB | 15 | 5 | 0 |  |
| uvicorn+uvloop | 12.0 K | 11.7 K–12.6 K | 9 | 77 µs | 150 µs | 459 µs | 62.1 µs | 23 MiB | 15 | 5 | 0 |  |
| uvicorn+asyncio | 9.9 K | 9.3 K–10.1 K | 9 | 96 µs | 197 µs | 541 µs | 80.3 µs | 21 MiB | 8 | 1 | 0 |  |
| granian | 7.2 K | 6.8 K–7.4 K | 9 | 128 µs | 262 µs | 999 µs | 138.4 µs | 19 MiB | 36 | 6 | 0 |  |
| hypercorn+asyncio | 9.1 K | 8.6 K–9.5 K | 9 | 103 µs | 199 µs | 582 µs | 87.1 µs | 20 MiB | 8 | 1 | 0 |  |

**ws-64b-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 145.6 K | 141.1 K–151.9 K | 9 | 406 µs | 942 µs | 1.84 ms | 6.8 µs | 25 MiB | 72 | 1 | 0 |  |
| uvicorn[standard] | 22.8 K | 22.1 K–23.9 K | 9 | 2.72 ms | 4.26 ms | 6.95 ms | 43.6 µs | 26 MiB | 78 | 5 | 0 |  |
| uvicorn+uvloop | 29.2 K | 28.0 K–30.2 K | 9 | 2.10 ms | 3.74 ms | 5.96 ms | 33.7 µs | 24 MiB | 78 | 5 | 0 |  |
| uvicorn+asyncio | 18.2 K | 17.1 K–18.3 K | 9 | 3.51 ms | 5.51 ms | 8.19 ms | 54.7 µs | 22 MiB | 71 | 1 | 0 |  |
| granian | 55.6 K | 51.4 K–59.7 K | 9 | 1.08 ms | 2.49 ms | 4.65 ms | 31.9 µs | 29 MiB | 99 | 6 | 0 |  |
| hypercorn+asyncio | 18.1 K | 16.1 K–18.3 K | 9 | 3.51 ms | 5.70 ms | 7.80 ms | 54.8 µs | 22 MiB | 71 | 1 | 0 |  |

**ws-16k-c16**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 71.6 K | 65.9 K–76.3 K | 9 | 215 µs | 500 µs | 1.15 ms | 13.7 µs | 26 MiB | 24 | 1 | 0 |  |
| uvicorn[standard] | 19.0 K | 18.5 K–19.3 K | 9 | 827 µs | 1.38 ms | 2.79 ms | 52.2 µs | 27 MiB | 30 | 5 | 0 |  |
| uvicorn+uvloop | 16.8 K | 15.6 K–17.6 K | 9 | 918 µs | 2.13 ms | 3.08 ms | 58.9 µs | 24 MiB | 30 | 5 | 0 |  |
| uvicorn+asyncio | 12.3 K | 11.6 K–13.5 K | 9 | 1.20 ms | 2.46 ms | 4.13 ms | 80.6 µs | 22 MiB | 23 | 1 | 0 |  |
| granian | 32.4 K | 29.3 K–33.5 K | 9 | 471 µs | 1.16 ms | 3.05 ms | 51.5 µs | 29 MiB | 51 | 6 | 0 |  |
| hypercorn+asyncio | 11.2 K | 10.5 K–11.6 K | 9 | 1.34 ms | 2.62 ms | 4.65 ms | 88.3 µs | 23 MiB | 23 | 1 | 0 |  |

**ws-256k-c8**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 5.3 K | 4.9 K–6.0 K | 9 | 1.44 ms | 2.52 ms | 5.24 ms | 188.2 µs | 40 MiB | 16 | 1 | 0 |  |
| uvicorn[standard] | 4.0 K | 3.8 K–4.1 K | 9 | 1.97 ms | 2.98 ms | 5.11 ms | 243.8 µs | 33 MiB | 22 | 5 | 0 |  |
| uvicorn+uvloop | 1.8 K | 1.7 K–1.8 K | 9 | 3.93 ms | 10.09 ms | 12.45 ms | 565.9 µs | 29 MiB | 22 | 5 | 0 |  |
| uvicorn+asyncio | 1.6 K | 1.5 K–1.6 K | 9 | 4.78 ms | 7.73 ms | 11.67 ms | 614.6 µs | 28 MiB | 15 | 1 | 0 |  |
| granian | 5.9 K | 5.4 K–6.2 K | 9 | 1.39 ms | 2.82 ms | 5.37 ms | 276.8 µs | 32 MiB | 43 | 6 | 0 |  |
| hypercorn+asyncio | 1.4 K | 1.3 K–1.4 K | 9 | 5.83 ms | 8.91 ms | 10.75 ms | 735.3 µs | 29 MiB | 15 | 1 | 0 |  |

**ws-stream-1k-w32-c16**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 456.4 K | 448.2 K–476.5 K | 9 | 999 µs | 3.18 ms | 6.88 ms | 2.2 µs | 36 MiB | 24 | 1 | 0 |  |
| uvicorn[standard] | 51.5 K | 49.5 K–54.7 K | 9 | 9.57 ms | 16.52 ms | 23.86 ms | 18.7 µs | 33 MiB | 30 | 5 | 0 |  |
| uvicorn+uvloop | 38.8 K | 34.9 K–42.1 K | 9 | 12.06 ms | 28.31 ms | 36.18 ms | 25.2 µs | 29 MiB | 30 | 5 | 0 |  |
| uvicorn+asyncio | 37.4 K | 34.5 K–38.9 K | 9 | 12.85 ms | 28.05 ms | 37.75 ms | 26.2 µs | 28 MiB | 23 | 1 | 0 |  |
| granian | 41.1 K | 38.5 K–42.8 K | 9 | 12.19 ms | 19.14 ms | 25.95 ms | 35.2 µs | 30 MiB | 51 | 6 | 0 |  |
| hypercorn+asyncio | 28.3 K | 27.7 K–29.6 K | 9 | 17.04 ms | 30.15 ms | 39.85 ms | 34.6 µs | 29 MiB | 23 | 1 | 0 |  |

## TLS

**tls-plaintext-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 78.9 K | 75.5 K–86.5 K | 9 | 778 µs | 1.65 ms | 3.01 ms | 12.5 µs | 25 MiB | 72 | 1 | 0 |  |
| uvicorn[standard] | 20.8 K | 19.5 K–22.0 K | 9 | 2.95 ms | 5.44 ms | 8.91 ms | 47.6 µs | 24 MiB | 78 | 5 | 0 |  |
| uvicorn+uvloop | 7.2 K | 6.9 K–7.3 K | 9 | 8.78 ms | 13.37 ms | 19.66 ms | 138.7 µs | 23 MiB | 78 | 5 | 0 |  |
| uvicorn+asyncio | 6.3 K | 6.1 K–6.6 K | 9 | 9.83 ms | 14.42 ms | 24.90 ms | 157.6 µs | 37 MiB | 71 | 1 | 0 |  |
| granian | 51.2 K | 48.2 K–51.5 K | 9 | 1.23 ms | 2.69 ms | 4.65 ms | 30.6 µs | 19 MiB | 99 | 6 | 0 |  |

**tls-churn-c16**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 2.6 K | 2.4 K–2.7 K | 9 | 1.85 ms | 4.33 ms | 7.93 ms | 385.2 µs | 25 MiB | 29 | 1 | 0 |  |
| uvicorn[standard] | 1.5 K | 1.4 K–1.6 K | 9 | 1.84 ms | 4.39 ms | 8.26 ms | 655.6 µs | 25 MiB | 26 | 5 | 0 |  |
| uvicorn+uvloop | 1.2 K | 1.2 K–1.3 K | 9 | 2.06 ms | 4.39 ms | 7.08 ms | 798.9 µs | 24 MiB | 22 | 5 | 0 |  |
| uvicorn+asyncio | 1.3 K | 1.3 K–1.4 K | 9 | 3.80 ms | 8.52 ms | 12.19 ms | 750.9 µs | 37 MiB | 30 | 1 | 0 |  |
| granian | 4.3 K | 4.1 K–4.5 K | 9 | 1.41 ms | 4.39 ms | 6.42 ms | 218.6 µs | 19 MiB | 51 | 6 | 0 |  |

**tls-churn-rsa-c16**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 1.3 K | 1.3 K–1.4 K | 9 | 3.70 ms | 7.08 ms | 11.53 ms | 736.3 µs | 21 MiB | 29 | 1 | 0 |  |
| uvicorn[standard] | 973 | 948–997 | 9 | 2.82 ms | 5.05 ms | 8.13 ms | 1011.2 µs | 23 MiB | 22 | 5 | 0 |  |
| uvicorn+uvloop | 836 | 793–870 | 9 | 2.75 ms | 4.78 ms | 7.60 ms | 1190.8 µs | 21 MiB | 21 | 5 | 0 |  |
| uvicorn+asyncio | 841 | 802–896 | 9 | 4.78 ms | 11.53 ms | 15.20 ms | 1182.3 µs | 27 MiB | 30 | 1 | 0 |  |
| granian | 1.2 K | 1.1 K–1.2 K | 9 | 6.95 ms | 15.99 ms | 19.66 ms | 844.0 µs | 16 MiB | 51 | 6 | 0 |  |

## TCP echo (raw asyncio.Protocol)

**echo-1k-c1**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 34.9 K | 33.9 K–35.6 K | 9 | 25 µs | 63 µs | 160 µs | 27.5 µs | 19 MiB | 9 | 1 | 0 |  |
| uvloop | 23.9 K | 23.5 K–24.7 K | 9 | 39 µs | 82 µs | 229 µs | 19.4 µs | 14 MiB | 15 | 5 | 0 |  |
| rloop | 24.1 K | 23.5 K–24.9 K | 9 | 39 µs | 81 µs | 213 µs | 18.5 µs | 12 MiB | 10 | 1 | 0 |  |
| rsloop | 24.1 K | 23.7 K–24.7 K | 9 | 39 µs | 81 µs | 252 µs | 19.3 µs | 15 MiB | 13 | 5 | 0 |  |
| asyncio | 21.3 K | 21.0 K–22.0 K | 9 | 43 µs | 90 µs | 266 µs | 25.9 µs | 11 MiB | 8 | 1 | 0 |  |

**echo-1k-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 196.7 K | 191.1 K–207.2 K | 9 | 299 µs | 762 µs | 1.82 ms | 5.0 µs | 22 MiB | 72 | 1 | 0 |  |
| uvloop | 192.9 K | 189.1 K–198.3 K | 9 | 307 µs | 786 µs | 2.00 ms | 5.1 µs | 14 MiB | 78 | 5 | 0 |  |
| rloop | 197.2 K | 194.6 K–203.8 K | 9 | 299 µs | 721 µs | 1.90 ms | 5.0 µs | 12 MiB | 74 | 1 | 0 |  |
| rsloop | 191.2 K | 184.7 K–194.2 K | 9 | 315 µs | 705 µs | 1.90 ms | 5.1 µs | 15 MiB | 76 | 5 | 0 |  |
| asyncio | 154.4 K | 142.0 K–160.3 K | 9 | 385 µs | 958 µs | 2.75 ms | 6.4 µs | 11 MiB | 71 | 1 | 0 |  |

**echo-1k-w16-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 493.2 K | 478.3 K–510.1 K | 9 | 1.43 ms | 9.04 ms | 14.55 ms | 2.0 µs | 24 MiB | 72 | 1 | 0 | load generator at 98% of its CPU: a lower bound |
| uvloop | 503.1 K | 491.0 K–516.7 K | 9 | 1.01 ms | 10.49 ms | 17.83 ms | 1.9 µs | 14 MiB | 78 | 5 | 0 | load generator at 99% of its CPU: a lower bound |
| rloop | 506.7 K | 489.0 K–536.4 K | 9 | 680 µs | 11.01 ms | 18.61 ms | 1.9 µs | 12 MiB | 74 | 1 | 0 | load generator at 99% of its CPU: a lower bound |
| rsloop | 528.8 K | 489.0 K–537.9 K | 9 | 1.43 ms | 8.91 ms | 14.68 ms | 1.8 µs | 18 MiB | 76 | 5 | 0 | load generator at 98% of its CPU: a lower bound |
| asyncio | 565.7 K | 543.6 K–576.6 K | 9 | 1.34 ms | 7.80 ms | 12.98 ms | 1.7 µs | 11 MiB | 71 | 1 | 0 | load generator at 99% of its CPU: a lower bound |

**echo-64k-c8**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 40.3 K | 39.6 K–41.7 K | 9 | 197 µs | 401 µs | 1.10 ms | 24.3 µs | 25 MiB | 16 | 1 | 0 |  |
| uvloop | 44.6 K | 44.1 K–45.7 K | 9 | 162 µs | 377 µs | 893 µs | 22.2 µs | 14 MiB | 22 | 5 | 0 |  |
| rloop | 47.5 K | 46.7 K–48.3 K | 9 | 162 µs | 324 µs | 836 µs | 20.8 µs | 12 MiB | 82 | 1 | 0 |  |
| rsloop | 39.3 K | 38.2 K–40.1 K | 9 | 199 µs | 373 µs | 836 µs | 25.1 µs | 18 MiB | 20 | 5 | 0 |  |
| asyncio | 43.8 K | 42.0 K–44.4 K | 9 | 170 µs | 406 µs | 1.05 ms | 22.5 µs | 11 MiB | 15 | 1 | 0 |  |

## TLS echo (raw asyncio.Protocol behind create_server(ssl=...))

**tlsecho-1k-c64**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 67.6 K | 60.5 K–69.7 K | 9 | 918 µs | 1.88 ms | 3.15 ms | 14.7 µs | 41 MiB | 72 | 1 | 0 |  |
| cadeloop, native TLS | 109.3 K | 104.5 K–113.1 K | 9 | 573 µs | 1.31 ms | 2.88 ms | 9.1 µs | 25 MiB | 72 | 1 | 0 |  |
| uvloop | 80.9 K | 64.3 K–84.6 K | 9 | 696 µs | 1.98 ms | 3.93 ms | 12.2 µs | 17 MiB | 78 | 5 | 0 |  |
| rsloop | 69.8 K | 67.3 K–72.5 K | 9 | 786 µs | 3.54 ms | 6.36 ms | 31.7 µs | 23 MiB | 141 | 195 | 0 |  |
| asyncio | 31.9 K | 30.2 K–32.7 K | 9 | 2.03 ms | 3.38 ms | 6.88 ms | 31.2 µs | 30 MiB | 71 | 1 | 0 |  |

**tlsecho-16k-c16**

| server | per s (median) | 96% CI | runs | p50 | p99 | p99.9 | server CPU / op | peak private | handles | threads | errors | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cadeloop | 36.8 K | 35.1 K–38.0 K | 9 | 422 µs | 778 µs | 1.49 ms | 27.0 µs | 42 MiB | 24 | 1 | 0 |  |
| cadeloop, native TLS | 49.0 K | 46.5 K–49.8 K | 9 | 315 µs | 696 µs | 1.64 ms | 20.2 µs | 26 MiB | 24 | 1 | 0 |  |
| uvloop | 48.3 K | 42.1 K–54.5 K | 9 | 279 µs | 950 µs | 2.20 ms | 20.4 µs | 18 MiB | 30 | 5 | 0 |  |
| rsloop | 40.6 K | 39.1 K–42.3 K | 9 | 319 µs | 1.62 ms | 4.85 ms | 49.0 µs | 25 MiB | 45 | 99 | 0 |  |
| asyncio | 29.4 K | 26.7 K–29.9 K | 9 | 524 µs | 1.41 ms | 2.13 ms | 33.6 µs | 31 MiB | 23 | 1 | 0 |  |

## Idle keep-alive connections

| server | connections | private / conn | working set / conn | handles / conn | private after close |
|---|---|---|---|---|---|
| cadeloop | 1000 | 10.4 KiB | 10.4 KiB | 1.00 | 30 MiB |
| uvicorn[standard] | 1000 | 7.5 KiB | 7.5 KiB | 1.00 | 31 MiB |
| uvicorn+uvloop | 1000 | 6.8 KiB | 6.8 KiB | 1.00 | 29 MiB |
| uvicorn+asyncio | 1000 | 7.2 KiB | 7.2 KiB | 1.00 | 27 MiB |
| uvicorn+cadeloop | 1000 | 18.3 KiB | 18.3 KiB | 1.00 | 41 MiB |
| granian | 1000 | 21.2 KiB | 21.2 KiB | 1.00 | 38 MiB |
| hypercorn+asyncio | 1000 | 14.3 KiB | 14.3 KiB | 1.00 | 34 MiB |
| cadeloop | 5000 | 6.5 KiB | 6.5 KiB | 1.00 | 34 MiB |
| uvicorn[standard] | 5000 | 6.2 KiB | 6.2 KiB | 1.00 | 48 MiB |
| uvicorn+uvloop | 5000 | 5.6 KiB | 5.6 KiB | 1.00 | 48 MiB |
| uvicorn+asyncio | 5000 | 5.5 KiB | 5.5 KiB | 1.00 | 54 MiB |
| uvicorn+cadeloop | 5000 | 12.4 KiB | 12.4 KiB | 1.00 | 72 MiB |
| granian | 4744 | 1.4 KiB | 1.4 KiB | 0.14 | 39 MiB |
| hypercorn+asyncio | 5000 | 11.2 KiB | 11.2 KiB | 1.00 | 89 MiB |

## Timers and wake-ups

How late `asyncio.sleep(d)` returned on an idle loop, median / p99 over 200 sleeps (40 at 10 ms and 50 ms): the median over 9 fresh processes per loop, the loops taking turns, with the 96% confidence interval of that median in brackets. A negative figure is a sleep that returned early. `busy` is a 1 ms sleep while another task keeps the loop running short callbacks; `cross-thread` is a `call_soon_threadsafe()` from another thread into a sleeping loop.

| loop | sleep(0.5 ms) | sleep(1 ms) | sleep(5 ms) | sleep(10 ms) | sleep(50 ms) | busy, 1 ms | cross-thread |
|---|---|---|---|---|---|---|---|
| cadeloop | +0.66 [+0.62, +0.67] / 0.82 ms | +0.16 [+0.15, +0.17] / 0.28 ms | +0.33 [+0.31, +0.33] / 0.52 ms | +0.33 [+0.30, +0.37] / 0.51 ms | +0.48 [+0.45, +0.48] / 0.72 ms | +0.00 [+0.00, +0.01] / 0.04 ms | 105 [100, 118] / 232 µs |
| uvloop | -0.50 [-0.50, -0.50] / -0.48 ms | +0.16 [+0.14, +0.19] / 0.33 ms | +0.21 [+0.20, +0.21] / 0.40 ms | +0.24 [+0.23, +0.25] / 0.41 ms | +0.33 [+0.33, +0.35] / 0.47 ms | +0.00 [+0.00, +0.00] / 0.08 ms | 119 [112, 127] / 240 µs |
| rloop | +0.65 [+0.62, +0.67] / 0.80 ms | +0.15 [+0.12, +0.17] / 0.32 ms | +0.20 [+0.18, +0.21] / 0.33 ms | +0.21 [+0.21, +0.23] / 0.37 ms | +0.31 [+0.29, +0.32] / 0.45 ms | +0.01 [+0.00, +0.01] / 0.07 ms | 100 [95, 110] / 226 µs |
| rsloop | +0.08 [+0.06, +0.09] / 0.23 ms | +0.11 [+0.07, +0.12] / 0.27 ms | +0.15 [+0.14, +0.15] / 0.28 ms | +0.19 [+0.18, +0.21] / 0.28 ms | +0.27 [+0.26, +0.28] / 0.42 ms | +0.01 [+0.01, +0.02] / 0.07 ms | 108 [95, 116] / 239 µs |
| asyncio | +0.68 [+0.67, +0.70] / 0.84 ms | +0.18 [+0.16, +0.21] / 0.37 ms | +0.23 [+0.22, +0.23] / 0.39 ms | +0.26 [+0.25, +0.27] / 0.39 ms | +0.38 [+0.36, +0.39] / 0.52 ms | +0.01 [+0.01, +0.01] / 0.10 ms | 144 [132, 149] / 281 µs |

## Scheduling microbenchmarks

Millions of operations per second: the median of 9 runs (a fresh process per run, the loops taking turns), with its 96% confidence interval.

| benchmark | cadeloop | uvloop | rloop | rsloop | asyncio |
|---|---|---|---|---|---|
| `task_fib` | 0.093 (0.088–0.097) | 0.079 (0.073–0.090) | 0.085 (0.084–0.087) | 0.090 (0.086–0.095) | 0.063 (0.058–0.067) |
| `call_soon_chain` | 6.999 (4.646–7.644) | 1.986 (1.853–2.155) | 4.620 (4.218–4.864) | 2.874 (2.633–2.987) | 0.593 (0.546–0.638) |
| `call_soon_burst` | 5.957 (4.325–6.842) | 1.116 (1.070–1.226) | 3.009 (2.826–3.094) | 1.120 (1.014–1.227) | 0.802 (0.720–0.860) |
| `timer_schedule_cancel` | 4.221 (3.432–4.426) | 0.500 (0.468–0.525) | 2.138 (2.060–2.297) | 1.098 (0.981–1.181) | 0.547 (0.515–0.575) |
| `timer_fire` | 2.591 (2.255–2.812) | 1.282 (1.200–1.391) | 1.646 (1.260–1.726) | 1.370 (1.146–1.434) | 0.338 (0.288–0.350) |
| `sleep0_chain` | 2.043 (1.489–2.196) | 1.102 (0.940–1.206) | 1.990 (1.918–2.052) | 1.431 (1.005–1.491) | 0.432 (0.419–0.461) |
| `task_spawn` | 0.310 (0.289–0.340) | 0.311 (0.278–0.321) | 0.285 (0.244–0.327) | 0.305 (0.289–0.338) | 0.225 (0.223–0.242) |
| `threadsafe_throughput` | 4.014 (3.763–4.968) | 2.034 (1.686–2.295) | 4.743 (3.881–5.055) | 3.589 (3.501–3.813) | 0.076 (0.065–0.081) |
| `future_chain` | 1.602 (1.550–1.663) | 0.703 (0.665–0.816) | 1.250 (1.172–1.334) | 0.888 (0.811–0.952) | 0.242 (0.235–0.267) |
| `gather_fanin` | 0.336 (0.293–0.349) | 0.312 (0.287–0.316) | 0.296 (0.253–0.307) | 0.287 (0.272–0.293) | 0.195 (0.164–0.210) |
| `queue_pingpong` | 1.412 (1.121–1.523) | 1.355 (1.009–1.404) | 1.374 (1.348–1.463) | 1.306 (1.023–1.382) | 1.270 (1.212–1.352) |
| `queue_pingpong_native` | 3.452 (2.846–3.511) | 3.245 (3.018–3.370) | 3.375 (2.564–3.566) | 3.100 (2.569–3.278) | 2.681 (2.317–2.898) |
