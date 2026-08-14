"""CLI (R-101) and serve() validation. End-to-end serving is covered in
test_http_engine.py; here we check config validation and flag mapping."""

import contextlib
import io
import sys

import pytest

import cadeloop
import cadeloop.__main__ as cli
from cadeloop.server import load_app


async def dummy_app(scope, receive, send):  # pragma: no cover - never run
    pass


def test_serve_validates_config_before_binding():
    with pytest.raises(ValueError, match="latency_mode"):
        cadeloop.serve(dummy_app, latency_mode="bogus")
    with pytest.raises(TypeError):
        cadeloop.serve(dummy_app, nonexistent_option=1)
    with pytest.raises(TypeError, match="callable"):
        cadeloop.serve(12345)
    with pytest.raises(ValueError, match="module:attribute"):
        cadeloop.serve("not-an-app-spec")  # string apps resolve via load_app


def test_serve_bare_callable_workers_no_fork_raises(monkeypatch):
    """Spawned workers re-import the app, so workers>1 with a bare
    callable (not an import string) on a platform without os.fork
    previously just logged a warning and silently ran 1 worker — an easy
    mistake (cadeloop.serve(app, workers=4) with a live app object) that
    looked like "scaling isn't working" rather than a clear config error.
    Simulates the no-fork (Windows) branch on any platform by removing
    os.fork for the duration of the test."""
    import cadeloop.server as server_module

    monkeypatch.delattr(server_module.os, "fork", raising=False)
    with pytest.raises(ValueError, match="import string"):
        cadeloop.serve(dummy_app, workers=4)


def test_load_app_errors():
    with pytest.raises(ValueError, match="module:attribute"):
        load_app("no-colon")
    with pytest.raises(ModuleNotFoundError):
        load_app("definitely_missing_module:app")
    with pytest.raises(AttributeError, match="no attribute"):
        load_app("os:not_a_real_attr")


def test_load_app_resolves():
    assert load_app("os.path:join") is __import__("os.path").path.join


def test_cli_maps_flags_to_serve(monkeypatch):
    calls = {}

    def fake_serve(app, host, port, **kw):
        calls.update(kw, app=app, host=host, port=port)

    monkeypatch.setattr(cli, "serve", fake_serve)
    assert cli.main(["os.path:join", "--workers", "3", "--port", "9001", "--no-pin"]) == 0
    # The SPEC is forwarded, not a resolved callable: serve() needs the
    # string for the fork-free worker model, and passing a callable made
    # `--workers > 1` fail as though a bare callable had been supplied.
    # Reported by Codex review on PR #1.
    assert calls["app"] == "os.path:join"
    assert calls["port"] == 9001
    assert calls["workers"] == 3
    assert calls["pin"] is False


def test_cli_still_validates_the_app_spec_eagerly(monkeypatch):
    """Forwarding the string must not defer a typo until after binding."""
    monkeypatch.setattr(cli, "serve", lambda *a, **k: None)
    with pytest.raises(AttributeError, match="no attribute"):
        cli.main(["os:not_a_real_attr"])


def test_cli_rejects_bad_backend():
    # Swallow argparse's usage/error output: it goes to real stderr, and a
    # "python -m cadeloop: error: invalid choice" line in a green sweep log
    # reads like a failure (this test PASSES by rejecting the name).
    with pytest.raises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
        cli.main(["os.path:join", "--backend", "not-a-backend"])


def test_cli_accepts_platform_backend(monkeypatch):
    # The platform's own explicit backend name must be a valid choice
    # (epoll on Linux, iocp on Windows) — regression: epoll was missing
    # from the CLI entirely.
    calls = {}
    monkeypatch.setattr(cli, "serve", lambda app, host, port, **kw: calls.update(kw))
    name = "iocp" if sys.platform == "win32" else "epoll"
    assert cli.main(["os.path:join", "--backend", name]) == 0
    assert calls["backend"] == name


@pytest.mark.skipif(sys.platform == "win32", reason="child watchers are POSIX-only")
def test_installed_policy_keeps_the_posix_child_watcher():
    """asyncio.create_subprocess_* goes through the policy's child
    watcher, which lives in the Unix policy rather than in
    BaseDefaultEventLoopPolicy. Deriving from the base class produced an
    installed policy whose loops could not spawn subprocesses at all.
    Reported by Codex review on PR #1."""
    import asyncio

    import cadeloop

    previous = asyncio.get_event_loop_policy()
    try:
        cadeloop.install()
        policy = asyncio.get_event_loop_policy()
        assert isinstance(policy, cadeloop.EventLoopPolicy)
        # The method exists AND returns a functional watcher.
        with contextlib.suppress(DeprecationWarning):
            watcher = policy.get_child_watcher()
        assert watcher is not None
        assert hasattr(watcher, "add_child_handler")
    finally:
        asyncio.set_event_loop_policy(previous)


def test_stats_endpoint_serves_json_on_loopback():
    """R-141 was documented in docs/ops.md from M2 but never
    implemented: setting the option configured nothing and the endpoint
    was silently absent."""
    import asyncio
    import json

    from cadeloop.server import _start_stats_endpoint

    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    try:
        lid, port = _start_stats_endpoint(lp, 0, None)

        async def fetch():
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(b"GET /stats HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
            await w.drain()
            data = await asyncio.wait_for(r.read(), 5.0)
            w.close()
            return data

        resp = lp.run_until_complete(fetch())
        lp._core.listener_close(lid)
    finally:
        asyncio.set_event_loop(None)
        lp.close()

    head, _, body = resp.partition(b"\r\n\r\n")
    assert head.startswith(b"HTTP/1.1 200"), head[:60]
    assert b"application/json" in head.lower(), head
    payload = json.loads(body)
    assert "ticks" in payload and "backend" in payload, payload
    assert payload["worker"] == 0


def test_stats_endpoint_port_is_validated():
    for bad in (0, -1, 70000):
        with pytest.raises(ValueError, match="stats_endpoint"):
            cadeloop.Config(stats_endpoint=bad)
    assert cadeloop.Config(stats_endpoint=None).stats_endpoint is None
    assert cadeloop.Config(stats_endpoint=9001).stats_endpoint == 9001


def test_cli_can_turn_off_the_body_cap(monkeypatch):
    """`max_body=None` means "no cap", and with a plain `type=int` the CLI
    could not say it -- `--max-body none` failed argparse outright, so a
    CLI user had no way to turn the 16 MiB default off."""
    seen = {}

    def fake_serve(app, host, port, **kw):
        seen.update(kw)

    monkeypatch.setattr(cli, "serve", fake_serve)
    monkeypatch.setattr(cli, "load_app", lambda spec: dummy_app)

    cli.main(["mod:app", "--max-body", "none"])
    assert "max_body" in seen and seen["max_body"] is None, seen

    seen.clear()
    cli.main(["mod:app", "--max-body", "1024"])
    assert seen["max_body"] == 1024

    seen.clear()
    cli.main(["mod:app"])
    assert "max_body" not in seen, "an absent flag must not override the default"


def test_cli_rejects_a_non_numeric_body_cap(monkeypatch):
    monkeypatch.setattr(cli, "load_app", lambda spec: dummy_app)
    with pytest.raises(SystemExit):
        with contextlib.redirect_stderr(io.StringIO()):
            cli.main(["mod:app", "--max-body", "banana"])
