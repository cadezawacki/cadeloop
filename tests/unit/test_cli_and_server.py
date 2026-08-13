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
    assert calls["app"] is load_app("os.path:join")
    assert calls["port"] == 9001
    assert calls["workers"] == 3
    assert calls["pin"] is False


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
