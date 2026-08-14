"""``cadeloop.Config`` (R-102): every spec tunable, validated eagerly.

Unknown kwargs raise ``TypeError`` (dataclass-generated ``__init__``).
``Config.from_env(prefix="CADELOOP_")`` reads typed overrides from the
environment.
"""

from __future__ import annotations

import dataclasses
import math
import os
import typing

__all__ = ["Config", "LATENCY_PRESETS"]

# R-060: latency_mode presets -> spin window (µs).
LATENCY_PRESETS = {"throughput": 0, "balanced": 20, "spin": 200}

# R-020. "epoll" is the Linux dev backend the CLI offers on
# non-Windows platforms; rejecting it here made that flag always
# raise before binding (Codex review, PR #1).
_BACKENDS = ("auto", "iocp", "rio", "epoll")
_GC_MODES = ("default", "freeze", "disable")  # R-075


@dataclasses.dataclass
class Config:
    # --- loop / reactor -------------------------------------------------
    backend: str = "auto"  # R-020
    latency_mode: str = "balanced"  # R-060
    spin_us: int | None = None  # R-060 (None -> derived from latency_mode)
    # R-060/R-035: put each HTTP response on the wire the moment it is
    # wire-ready instead of corking it until the tick's flush phase.
    # Corking is the throughput choice and stays the default; immediate
    # flush costs syscalls and buys tail latency, because a response that
    # was ready first otherwise waits behind however many *other*
    # connections' app dispatch the same tick batched ahead of it.
    # None -> derived from latency_mode (on for "spin").
    immediate_flush: bool | None = None
    # --- kernel I/O -----------------------------------------------------
    accept_pool: int = 64  # R-032
    rio_cq_size: int = 65536  # R-041
    rio_rq_recv: int = 32  # R-042
    rio_rq_send: int = 32  # R-042
    loopback_fast_path: bool = True  # R-038 (benchmark-only relevance)
    tfo: bool = False  # R-038 TCP Fast Open on listeners
    # --- DNS (R-055) ------------------------------------------------------
    # serve()'s own default (on): a short server-side cache is a
    # reasonable tradeoff for a request-serving process. Loop() built
    # directly (bypassing Config) defaults to off, matching the
    # AbstractEventLoop contract (real asyncio.getaddrinfo never caches).
    dns_cache: bool = True
    dns_cache_ttl: float = 5.0  # documented: RFC TTLs ignored
    # --- tasks / GC -------------------------------------------------------
    eager_tasks: bool = True  # R-056 / §16 (switchable interop escape hatch)
    gc_mode: str = "freeze"  # R-075 server default
    warmup: int = 1000  # R-075 requests before gc.freeze()
    # --- HTTP engine (R-080) ----------------------------------------------
    max_header_bytes: int = 64 * 1024
    max_headers: int = 100
    max_url: int = 8 * 1024
    request_line_timeout: float = 5.0
    keepalive_idle: float = 75.0
    # Finite by default: the engine buffers a whole request body before
    # dispatch, so `None` (unlimited) lets an unauthenticated client turn
    # one request into unbounded memory. Raise it, or set None explicitly,
    # for large-upload workloads; over the limit the client gets a 413.
    max_body: int | None = 16 * 1024 * 1024
    reuse_scope: bool = False  # R-083 (correctness default)
    # --- transports ---------------------------------------------------------
    write_high_water: int = 64 * 1024  # R-122 backpressure defaults
    write_low_water: int = 16 * 1024
    # --- multi-process (§8) --------------------------------------------------
    workers: int = 0  # 0 -> physical cores (R-090)
    pin: bool = True  # R-091
    grace: float = 10.0  # R-092 graceful drain seconds
    # --- misc ----------------------------------------------------------------
    access_log: bool = False  # R-140
    stats_endpoint: int | None = None  # R-141 localhost port or None

    def __post_init__(self):
        if self.backend not in _BACKENDS:
            raise ValueError(
                f"backend must be one of {_BACKENDS}, got {self.backend!r}"
            )
        if self.backend == "rio" and not os.environ.get("CADELOOP_ALLOW_EXPERIMENTAL_RIO"):
            raise ValueError(
                "backend='rio' is experimental and unvalidated on real Windows "
                "hardware — every machine tested so far has hit either an "
                "OS-level RIO initialization failure or a data-path stall (see "
                "docs/roadmap.md's M3 entry). 'auto' already avoids it and stays "
                "on the hardware-validated IOCP backend. To opt in anyway (e.g. "
                "for RIO diagnosis with tools/windows/rio_smoke.py or "
                "rio_bisect.py), set CADELOOP_ALLOW_EXPERIMENTAL_RIO=1, or "
                "construct cadeloop.Loop(backend='rio') directly."
            )
        if self.latency_mode not in LATENCY_PRESETS:
            raise ValueError(
                f"latency_mode must be one of {tuple(LATENCY_PRESETS)}, "
                f"got {self.latency_mode!r}"
            )
        if self.gc_mode not in _GC_MODES:
            raise ValueError(
                f"gc_mode must be one of {_GC_MODES}, got {self.gc_mode!r}"
            )
        if self.spin_us is None:
            self.spin_us = LATENCY_PRESETS[self.latency_mode]
        if self.immediate_flush is None:
            self.immediate_flush = self.latency_mode == "spin"
        for field, minimum in (
            ("accept_pool", 1),
            ("rio_cq_size", 1),
            ("rio_rq_recv", 1),
            ("rio_rq_send", 1),
            ("max_header_bytes", 1),
            ("max_headers", 1),
            ("max_url", 1),
            ("warmup", 0),
            ("workers", 0),
            ("spin_us", 0),
        ):
            if getattr(self, field) < minimum:
                raise ValueError(f"{field} must be >= {minimum}")
        for field in ("request_line_timeout", "keepalive_idle", "grace", "dns_cache_ttl"):
            value = getattr(self, field)
            # NaN fails every comparison, so a bare `< 0` check waves it
            # through -- and a NaN request timeout converts to zero
            # downstream, silently disabling the slow-request guard it was
            # meant to configure. An infinite grace is the same shape:
            # a shutdown that waits without a usable bound. These arrive
            # from environment variables and CLI flags as readily as from
            # code, so they are worth rejecting where they are read.
            if not math.isfinite(value):
                raise ValueError(f"{field} must be a finite number, got {value!r}")
            if value < 0:
                raise ValueError(f"{field} must be >= 0")
        if self.max_body is not None and self.max_body < 0:
            raise ValueError("max_body must be None or >= 0")
        if self.write_low_water > self.write_high_water:
            raise ValueError("write_low_water must be <= write_high_water")

    @classmethod
    def from_env(cls, prefix: str = "CADELOOP_") -> "Config":
        """Build a Config from ``{prefix}{FIELD}`` environment variables."""
        kwargs = {}
        hints = typing.get_type_hints(cls)
        for f in dataclasses.fields(cls):
            raw = os.environ.get(prefix + f.name.upper())
            if raw is None:
                continue
            kwargs[f.name] = _parse(raw, hints[f.name], f.name)
        return cls(**kwargs)


def _parse(raw: str, hint, name: str):
    optional = typing.get_origin(hint) in (typing.Union, __import__("types").UnionType)
    if optional:
        args = [a for a in typing.get_args(hint) if a is not type(None)]
        if raw.lower() in ("none", ""):
            return None
        hint = args[0]
    if hint is bool:
        low = raw.strip().lower()
        if low in ("1", "true", "yes", "on"):
            return True
        if low in ("0", "false", "no", "off"):
            return False
        raise ValueError(f"invalid boolean for {name}: {raw!r}")
    if hint is int:
        return int(raw)
    if hint is float:
        return float(raw)
    return raw
