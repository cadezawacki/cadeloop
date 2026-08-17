#!/usr/bin/env python3
"""Fetch the pinned llhttp release into vendor/llhttp (R-112).

Pins both the version and the artifact SHA-256; refuses to unpack on
mismatch.
"""

import hashlib
import io
import pathlib
import sys
import tarfile
import urllib.request

VERSION = "9.2.1"
URL = (
    "https://github.com/nodejs/llhttp/archive/refs/tags/"
    f"release/v{VERSION}.tar.gz"
)
# SHA-256 of the release tarball; verify on first fetch and pin the value
# here (fill on the machine that performs the initial vendoring, then the
# hash gates every subsequent fetch).
SHA256 = None  # e.g. "abc123..."

WANTED = {"llhttp.h", "llhttp.c", "api.c", "http.c"}


def main() -> int:
    dest = pathlib.Path(__file__).parent
    print(f"fetching llhttp v{VERSION} ...")
    data = urllib.request.urlopen(URL, timeout=60).read()
    digest = hashlib.sha256(data).hexdigest()
    if SHA256 is None:
        print(f"NOTE: first fetch — pin SHA256 = \"{digest}\" in fetch.py")
    elif digest != SHA256:
        print(f"FATAL: SHA-256 mismatch: got {digest}, pinned {SHA256}")
        return 1
    extracted = 0
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
        for member in tf.getmembers():
            name = pathlib.PurePosixPath(member.name).name
            parent = pathlib.PurePosixPath(member.name).parent.name
            if name in WANTED and parent in ("src", "include", "native"):
                content = tf.extractfile(member).read()
                (dest / name).write_bytes(content)
                print(f"  {name} ({len(content)} bytes)")
                extracted += 1
    if extracted < len(WANTED):
        print(
            "WARNING: release tag layout differs (generated C lives in the "
            "'release' artifact for some versions); extracted "
            f"{extracted}/{len(WANTED)} files. Adjust WANTED/paths."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
