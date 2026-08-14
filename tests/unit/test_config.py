"""Config surface (R-102)."""

import pytest

from cadeloop.config import LATENCY_PRESETS, Config


def test_defaults_match_spec():
    c = Config()
    assert c.backend == "auto"  # R-020
    assert c.latency_mode == "balanced" and c.spin_us == 20  # R-060
    assert c.accept_pool == 64  # R-032
    assert c.rio_cq_size == 65536  # R-041
    assert c.rio_rq_recv == 32 and c.rio_rq_send == 32  # R-042
    assert c.loopback_fast_path is True and c.tfo is False  # R-038
    assert c.dns_cache is True  # R-055
    assert c.eager_tasks is True  # R-056
    assert c.gc_mode == "freeze" and c.warmup == 1000  # R-075
    assert c.max_header_bytes == 64 * 1024  # R-080
    assert c.max_headers == 100
    assert c.max_url == 8 * 1024
    assert c.request_line_timeout == 5.0
    assert c.keepalive_idle == 75.0
    # Finite by default (see Config.max_body): the engine buffers a whole
    # body before dispatch, so an unlimited default was an unauthenticated
    # memory-exhaustion vector. None stays available as an explicit opt-in.
    assert c.max_body == 16 * 1024 * 1024
    assert c.reuse_scope is False  # R-083
    assert c.write_high_water == 64 * 1024  # R-122
    assert c.write_low_water == 16 * 1024
    assert c.pin is True and c.grace == 10.0  # R-091/R-092
    assert c.access_log is False  # R-140


def test_latency_presets():
    assert LATENCY_PRESETS == {"throughput": 0, "balanced": 20, "spin": 200}
    assert Config(latency_mode="throughput").spin_us == 0
    assert Config(latency_mode="spin").spin_us == 200
    assert Config(latency_mode="balanced", spin_us=77).spin_us == 77


def test_unknown_kwarg_raises_typeerror():
    with pytest.raises(TypeError):
        Config(does_not_exist=1)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"backend": "kqueue"},
        {"latency_mode": "warp"},
        {"gc_mode": "sometimes"},
        {"accept_pool": 0},
        {"workers": -1},
        {"grace": -1.0},
        {"max_body": -5},
        {"write_low_water": 100_000, "write_high_water": 50_000},
    ],
)
def test_validation_errors(kwargs):
    with pytest.raises(ValueError):
        Config(**kwargs)


def test_epoll_backend_is_accepted():
    """The CLI offers --backend epoll on non-Windows platforms, so Config
    must accept it: rejecting it made the advertised flag always raise
    before binding (Codex review, PR #1). This previously asserted the
    opposite, encoding the bug."""
    assert Config(backend="epoll").backend == "epoll"


def test_from_env(monkeypatch):
    monkeypatch.setenv("CADELOOP_WORKERS", "4")
    monkeypatch.setenv("CADELOOP_TFO", "true")
    monkeypatch.setenv("CADELOOP_DNS_CACHE", "off")
    monkeypatch.setenv("CADELOOP_GRACE", "2.5")
    monkeypatch.setenv("CADELOOP_MAX_BODY", "1048576")
    monkeypatch.setenv("CADELOOP_BACKEND", "iocp")
    c = Config.from_env()
    assert c.workers == 4
    assert c.tfo is True
    assert c.dns_cache is False
    assert c.grace == 2.5
    assert c.max_body == 1048576
    assert c.backend == "iocp"


def test_from_env_custom_prefix(monkeypatch):
    monkeypatch.setenv("APP_WORKERS", "2")
    monkeypatch.setenv("CADELOOP_WORKERS", "9")
    assert Config.from_env(prefix="APP_").workers == 2


def test_from_env_bad_bool(monkeypatch):
    monkeypatch.setenv("CADELOOP_PIN", "maybe")
    with pytest.raises(ValueError):
        Config.from_env()
