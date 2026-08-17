# Risk register (spec §16) — mitigations in this tree

| Risk | Mitigation (implemented / planned) |
|---|---|
| OVERLAPPED/buffer UAF under cancellation | Implemented: pinned op slab, `{Free,Posted,Completed,Cancelled}` machine with debug-asserted transitions, recycle only after completion reap, generation-checked ids (`opslab.rs`); buffer slots refcounted with debug poison `0xDD` (`buffers.rs`); both proptested (R-121). |
| `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS` with LSPs | Implemented: `XP1_IFS_HANDLES` check per socket in `register_socket`; skip-modes not applied for non-IFS providers (R-031). |
| RIO scaling cliffs (RQ sizing, CQ overflow) | Planned M3: CQ overflow = fatal-log + stop posting recvs (backpressure), never silent loss; `rio_cq_size`/`rio_rq_*` configurable (design in `rio.rs` header). |
| GIL re-entry cost dominating small messages | Implemented: GIL released once per poll, re-acquired once per batch (R-021); 128-callback dispatch batches (R-054); vectorcall dispatch. Measure gil-held% via ETW in M1 perf work (R-133). |
| add_reader emulation semantic drift | Planned M1/M4: zero-byte probe emulation documented as level-triggered approximation; asyncpg-pattern conformance tests (R-057, R-124). |
| Eager-task semantics breaking Task-identity assumptions | `cfg.eager_tasks=True` default with off switch already in Config (§16); interop suite to run both ways (M2). |
| Loopback-only benchmark self-deception | Two-machine numbers authoritative (R-131); the Linux scheduling report in README is explicitly labeled non-acceptance; `SIO_LOOPBACK_FAST_PATH` disclosed wherever loopback numbers appear. |
| 3.11 C-API fragility (Task/frame introspection) | Native code sticks to public C-API (`PyObject_VectorcallMethod`, `PyContext_CopyCurrent`, `PyIter_Send` planned); CI pins the 3.11 matrix. |
| Deadlock: lock held across GIL-released poll | Implemented (found during M0 design): no lock — thread-affinity state cell that raises instead of blocking (`gil_boundary.rs`). |
| `__del__`/GC re-entrancy inside state critical section | Implemented: graveyard protocol — no Python decref inside the cell; discarded tokens dropped outside (`timer.rs`/`reactor.rs`/`coreloop.rs`/`net.rs`). |
| Cancel-vs-completion races dropping or corrupting received data | Found by the M1 10 MB-transfer test: cancelled recv ops whose completion was already queued. Fixed twice over: pause_reading cancels nothing (stops re-posting only), and kernel ops hold their own buffer-slot refcount released at completion reap (`net.rs`, R-073). |
| Transport machinery taxing pure-scheduling ticks | Found by the benchmark-regression check: +430ns/tick. Fixed: zero-timeout polls skip epoll_wait and keep the GIL; one clock read per tick; net phases skipped with no events. Guarded by the sched baseline in CI. |
| panic=abort in a Python process | Release builds abort on Rust panic (R-111). Hot paths are panic-free by construction (no unwrap on user-reachable paths); debug builds keep unwinding + `debug_assert`s. |
| Eager AppTask diverging from real Task semantics under anyio | Found by the M2 Starlette suite: anyio task groups weakref `asyncio.current_task()` and crashed on None. Implemented: AppTask registers via `_asyncio._enter_task`/`_leave_task` per step, is weakref-able, forwards `cancel()` to the awaited future, and exposes done/get_loop/get_name/uncancel/cancelling (ADR-19). Residual gaps (cancel-count bookkeeping, task introspection APIs) remain behind `eager_tasks=False`. |
| Lifespan hang on apps that ignore the protocol | Found by the bench app: returning from the lifespan scope without `startup.complete` left serve() waiting forever. Fixed: uvicorn-style `auto` — return-or-raise before completion disables lifespan and serving proceeds (`server.py`). |
