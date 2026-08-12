"""CLI (R-101) and serve() gating."""

import pytest

import cadeloop
from cadeloop.__main__ import main
from cadeloop.server import load_app


async def dummy_app(scope, receive, send):  # pragma: no cover - never run
    pass


def test_serve_signature_and_m2_gate():
    with pytest.raises(NotImplementedError, match="milestone"):
        cadeloop.serve(dummy_app, "0.0.0.0", 9000, workers=2, backend="iocp")


def test_serve_validates_config_before_gate():
    with pytest.raises(ValueError, match="latency_mode"):
        cadeloop.serve(dummy_app, latency_mode="bogus")
    with pytest.raises(TypeError):
        cadeloop.serve(dummy_app, nonexistent_option=1)
    with pytest.raises(TypeError, match="callable"):
        cadeloop.serve("not-an-app")


def test_load_app_errors():
    with pytest.raises(ValueError, match="module:attribute"):
        load_app("no-colon")
    with pytest.raises(ModuleNotFoundError):
        load_app("definitely_missing_module:app")
    with pytest.raises(AttributeError, match="no attribute"):
        load_app("os:not_a_real_attr")


def test_load_app_resolves():
    assert load_app("os.path:join") is __import__("os.path").path.join


def test_cli_maps_flags_to_config(capsys):
    # Reaches the M2 gate with the validated config echoed in the message.
    with pytest.raises(NotImplementedError, match="workers=3"):
        main(["os.path:join", "--workers", "3", "--port", "9001", "--no-pin"])


def test_cli_rejects_bad_backend():
    with pytest.raises(SystemExit):
        main(["os.path:join", "--backend", "epoll"])
