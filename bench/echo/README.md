# bench/echo — TCP echo benchmark (M1)

The R-003/R-131 echo acceptance benchmark: 1 KiB messages, 64 concurrent
connections, cadeloop vs {stdlib Proactor, stdlib Selector, winloop}.

Lands with the M1 IOCP transports. Components:

- `client/` — custom Rust echo load generator (R-131): fixed connection
  count, pipelined 1 KiB frames, records per-message RTTs (median/p99/p999)
  and aggregate throughput; pinned to cores disjoint from the server
  (R-130).
- `server_cadeloop.py`, `server_stdlib.py`, `server_winloop.py` — minimal
  `loop.create_server` echo servers per contender.

Authoritative numbers are two-machine runs (R-131); loopback runs are
reported separately with the `SIO_LOOPBACK_FAST_PATH` disclosure.
