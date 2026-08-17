"""Drop-in interop (R-124 flavor, Linux dev backend): real frameworks on an
unmodified cadeloop loop — uvicorn serving HTTP/1.1 ASGI, aiohttp server +
client. Skipped where the libraries aren't installed."""

import asyncio
import json

import cadeloop
import pytest

uvicorn = pytest.importorskip("uvicorn")
aiohttp = pytest.importorskip("aiohttp")

from aiohttp import web  # noqa: E402


@pytest.fixture()
def loop():
    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


async def _asgi_app(scope, receive, send):
    assert scope["type"] == "http"
    body = json.dumps({"path": scope["path"], "loop": "cadeloop"}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body})


def test_uvicorn_http11_on_cadeloop(loop):
    """uvicorn (h11) serving over cadeloop transports; aiohttp client over
    cadeloop create_connection. Full HTTP/1.1 keep-alive drop-in proof."""

    async def main():
        config = uvicorn.Config(
            _asgi_app,
            host="127.0.0.1",
            port=0,
            log_level="error",
            loop="asyncio",  # uvicorn uses the running (cadeloop) loop
            lifespan="off",
            http="h11",
        )
        server = uvicorn.Server(config)
        serve_task = loop.create_task(server.serve())
        for _ in range(200):
            if server.started:
                break
            await asyncio.sleep(0.01)
        assert server.started, "uvicorn failed to start on cadeloop"
        port = server.servers[0].sockets[0].getsockname()[1]

        async with aiohttp.ClientSession() as session:
            for i in range(20):  # keep-alive reuse across requests
                async with session.get(f"http://127.0.0.1:{port}/req{i}") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data == {"path": f"/req{i}", "loop": "cadeloop"}

        server.should_exit = True
        await serve_task
        assert isinstance(asyncio.get_running_loop(), cadeloop.Loop)

    loop.run_until_complete(main())


def test_aiohttp_server_and_client(loop):
    async def main():
        async def handler(request):
            body = await request.text()
            return web.Response(text=f"echo:{body}", headers={"x-served-by": "cadeloop"})

        app = web.Application()
        app.router.add_post("/echo", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]

        async with aiohttp.ClientSession() as session:
            payload = "x" * 50_000  # multi-chunk body
            async with session.post(f"http://127.0.0.1:{port}/echo", data=payload) as resp:
                assert resp.status == 200
                assert resp.headers["x-served-by"] == "cadeloop"
                assert await resp.text() == f"echo:{payload}"

        await runner.cleanup()

    loop.run_until_complete(main())


def test_streams_api_headline(loop):
    """The asyncio.open_connection/start_server pairing used by most
    tutorials — must work verbatim."""

    async def main():
        async def on_client(reader, writer):
            data = await reader.readline()
            writer.write(data.upper())
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(on_client, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(b"hello streams\n")
        assert await reader.readline() == b"HELLO STREAMS\n"
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())
