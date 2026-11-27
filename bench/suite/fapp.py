"""The framework app: FastAPI, as an application would use it.

`app.py` is a bare ASGI callable, which measures the servers and almost
nothing else. This one puts a framework between the server and the
handler -- routing, dependency resolution, pydantic validation and
serialization, Starlette's responses -- so the numbers say what a server
is worth to a FastAPI service. Every contender serves this same module.

  /          13-byte plaintext (the readiness probe)
  /model     a JSON document through a pydantic response model, `async def`
  /sync      the same document from a plain `def` handler, which FastAPI
             runs on a worker thread (what a handler calling a blocking
             database driver looks like to the server)
  /file64k   a 64 KiB file as a FileResponse
  /file1m    a 1 MiB file as a FileResponse
"""

import os
import tempfile

from fastapi import FastAPI
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel

app = FastAPI()

ITEMS = [{"id": i, "name": f"item-{i}", "ok": i % 2 == 0} for i in range(10)]


class Item(BaseModel):
    id: int
    name: str
    ok: bool


class Doc(BaseModel):
    message: str
    items: list[Item]


def _file(name: str, size: int) -> str:
    # One copy for every server process of every run; written once.
    directory = os.path.join(tempfile.gettempdir(), "cadeloop-bench-files")
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, name)
    if not os.path.exists(path) or os.path.getsize(path) != size:
        scratch = f"{path}.{os.getpid()}"
        with open(scratch, "wb") as f:
            f.write(b"z" * size)
        os.replace(scratch, path)
    return path


FILE_64K = _file("64k.bin", 65536)
FILE_1M = _file("1m.bin", 1 << 20)


@app.get("/", response_class=PlainTextResponse)
async def root():
    return "Hello, World!"


@app.get("/model", response_model=Doc)
async def model():
    return {"message": "Hello, World!", "items": ITEMS}


@app.get("/sync", response_model=Doc)
def sync():
    return {"message": "Hello, World!", "items": ITEMS}


@app.get("/file64k")
async def file64k():
    return FileResponse(FILE_64K, media_type="application/octet-stream")


@app.get("/file1m")
async def file1m():
    return FileResponse(FILE_1M, media_type="application/octet-stream")
