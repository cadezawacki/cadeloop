"""``cadeloop.Config`` (R-102): every spec tunable, validated eagerly.

Unknown kwargs raise ``TypeError`` (dataclass-generated ``__init__``), as
does a value of the wrong type; an out-of-range value raises
``ValueError``.
``Config.from_env(prefix="CADELOOP_")`` reads typed overrides from the
environment.
"""

from __future__ import annotations

import dataclasses
import math
import numbers
import operator
import os
import sys
import typing

__all__ = ["Config", "LATENCY_PRESETS"]

# R-060: latency_mode presets -> spin window (µs).
LATENCY_PRESETS = {"throughput": 0, "balanced": 20, "spin": 200}

# R-020. "epoll" is the Linux dev backend the CLI offers on
# non-Windows platforms; rejecting it here made that flag always
# raise before binding (Codex review, PR #1).
_BACKENDS = ("auto", "iocp", "rio", "epoll")
_GC_MODES = ("default", "freeze", "disable")  # R-075

_BOOL_FIELDS = ("loopback_fast_path", "tfo", "dns_cache", "eager_tasks", "pin", "access_log")
_USIZE_MAX = sys.maxsize * 2 + 1
_U32_MAX = 2**32 - 1
# field: (minimum, maximum). The maxima are the native integer types these
# end up in: past them the failure used to surface as an OverflowError
# from deep inside startup -- after the application's lifespan startup
# had already run -- instead of here.
_INT_FIELDS = {
    "accept_pool": (1, _USIZE_MAX),
    "rio_cq_size": (1, _U32_MAX),
    "rio_rq_recv": (1, _U32_MAX),
    "rio_rq_send": (1, _U32_MAX),
    "max_header_bytes": (1, _USIZE_MAX),
    "max_headers": (1, _USIZE_MAX),
    "max_url": (1, _USIZE_MAX),
    "warmup": (0, None),
    "workers": (0, None),
    "spin_us": (0, 2**64 - 1),
    "write_high_water": (0, _USIZE_MAX),
    "write_low_water": (0, _USIZE_MAX),
}
_OPTIONAL_INT_FIELDS = {"max_body": (0, _USIZE_MAX), "stats_endpoint": (1, 65535)}
_FLOAT_FIELDS = ("request_line_timeout", "keepalive_idle", "grace", "dns_cache_ttl")


def _checked_int(name, value):
    # operator.index takes any real integer (numpy's included) and refuses
    # floats; bool is an int subclass, but `workers=True` is a mistake.
    if isinstance(value, bool):
        raise TypeError(f"{name} must be an int, not bool")
    try:
        return operator.index(value)
    except TypeError:
        raise TypeError(f"{name} must be an int, got {type(value).__name__}") from None


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
    # R-075: with gc_mode="freeze", requests served before a second
    # gc.freeze() takes in what the first requests built lazily (route and
    # template caches, compiled statements); 0 freezes at startup only.
    warmup: int = 1000
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
    # --- transports ---------------------------------------------------------
    write_high_water: int = 64 * 1024  # R-122 backpressure defaults
    write_low_water: int = 16 * 1024
    # --- multi-process (§8) --------------------------------------------------
    workers: int = 0  # 0 -> one per CPU the process may run on (R-090)
    # R-091: pin each worker's loop thread to one CPU. Off by default:
    # measured on loopback it bought nothing (73.5k vs 76.3k requests per
    # worker CPU-second, pinned vs not), and pinning is a deployment
    # decision -- RSS-aligned NIC queues on dedicated hardware -- that
    # shared or virtualised CPUs mostly punish (a pinned worker cannot move
    # off a busy core). uvicorn and gunicorn do not pin either.
    pin: bool = False
    grace: float = 10.0  # R-092 graceful drain seconds
    # --- misc ----------------------------------------------------------------
    access_log: bool = False  # R-140
    # R-141: serve stats() as JSON on 127.0.0.1:<port>. Bound by one
    # worker only (see server._start_stats_endpoint) -- the payload names
    # which. None disables it.
    stats_endpoint: int | None = None

    def __post_init__(self):
        for field in _BOOL_FIELDS:
            if not isinstance(getattr(self, field), bool):
                # A string here was stored as given and read as truthy:
                # access_log="false" turned the access log ON.
                raise TypeError(
                    f"{field} must be a bool, got {type(getattr(self, field)).__name__}"
                )
        if self.immediate_flush is not None and not isinstance(self.immediate_flush, bool):
            raise TypeError(
                f"immediate_flush must be a bool or None, got {type(self.immediate_flush).__name__}"
            )
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
        for field, (minimum, maximum) in _INT_FIELDS.items():
            value = _checked_int(field, getattr(self, field))
            if value < minimum:
                raise ValueError(f"{field} must be >= {minimum}")
            if maximum is not None and value > maximum:
                raise ValueError(f"{field} must be <= {maximum}")
            setattr(self, field, value)
        for field, (minimum, maximum) in _OPTIONAL_INT_FIELDS.items():
            value = getattr(self, field)
            if value is None:
                continue
            value = _checked_int(field, value)
            if not minimum <= value <= maximum:
                raise ValueError(f"{field} must be None or in {minimum}..{maximum}")
            setattr(self, field, value)
        for field in _FLOAT_FIELDS:
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, numbers.Real):
                raise TypeError(f"{field} must be a number, got {type(value).__name__}")
            value = float(value)
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
            setattr(self, field, value)
        # (Non-negative watermarks are checked above, BEFORE this ordering:
        # `write_low_water=-1, write_high_water=0` is ordered, and the
        # negative value used to reach CoreLoop's usize argument.)
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
        if raw.strip().lower() in ("none", ""):
            return None
        hint = args[0]
    if hint is bool:
        low = raw.strip().lower()
        if low in ("1", "true", "yes", "on"):
            return True
        if low in ("0", "false", "no", "off"):
            return False
        raise ValueError(f"invalid boolean for {name}: {raw!r}")
    try:
        if hint is int:
            return int(raw)
        if hint is float:
            return float(raw)
    except ValueError:
        raise ValueError(f"invalid {hint.__name__} for {name}: {raw!r}") from None
    return raw
