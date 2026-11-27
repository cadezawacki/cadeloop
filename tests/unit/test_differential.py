"""Generated differential tests: one scripted scenario, two event loops.

test_asyncio_parity.py holds the hand-written cases -- each one a
difference somebody found. This module goes looking for the next one. A
seed expands into a scenario: how the connection is made, what kind of
protocol sits on it, and a short script of transport calls and peer
actions (writes of every size, half-closes, resets, pauses, calls made
after close, arguments of the wrong type). The scenario runs on cadeloop
and on asyncio's selector loop, and the two traces -- every protocol
callback and the outcome of every call, in order -- have to be equal.

What makes the traces comparable at all:

* The peer is a plain non-blocking socket the test drives itself, not a
  second transport, so only one implementation is under test and the peer
  does exactly what the script says (including an RST, which a transport
  cannot be asked for precisely).
* Between steps the scenario runs to quiescence: no callback, no byte in
  either direction, no change in the write buffer for a few consecutive
  turns. The peer only reads during those turns, so what a call sees --
  "the peer is not reading", "the buffer is empty" -- is the script's
  doing, not the scheduler's.
* What TCP does not promise is left out of the trace: how a stream is cut
  into `data_received` calls (lengths and a checksum are compared, per run
  of consecutive deliveries), the exact size of a non-empty write buffer,
  and which of the connection errors a reset surfaces as.

A failure prints the seed and the script; `CADELOOP_DIFF_SEED=<n>` reruns
one, `CADELOOP_DIFF_SEEDS=<count>` sets how many the suite generates.
"""

import asyncio
import os
import random
import socket
import ssl
import struct
import sys
import zlib

import cadeloop
import pytest

SEEDS = int(os.environ.get("CADELOOP_DIFF_SEEDS", "100"))
ONLY = os.environ.get("CADELOOP_DIFF_SEED")

# Loopback TCP on Windows takes a send of any size in one call, where
# asyncio's selector loop then sees an empty buffer and a completion-based
# backend does not: the write-buffer half of every trace would differ for
# reasons that are the platform's. Until these scenarios have been run on
# Windows hardware they are opt-in there (CI runs them as a step that is
# allowed to fail, to collect that evidence).
pytestmark = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("CADELOOP_DIFF_WINDOWS"),
    reason="generated differential scenarios are opt-in on Windows (CADELOOP_DIFF_WINDOWS=1)",
)

# Explicit buffer sizes switch the kernel's autotuning off, so "a big
# write does not fit" holds on every run. They stay above the loopback
# segment size (64 KiB): a window smaller than one segment makes TCP wait
# on its timers, and a transfer then moves in 40-200 ms steps.
SOCKBUF = 128 * 1024
SMALL = (1, 7, 100, 1000, 4096)
BIG = (2 << 20, 3 << 20, 4 << 20)
# Write-buffer limits a script may set: either far below what a big write
# leaves unsent or far above it, never where the two loops' first send
# could land on different sides.
HIGH_WATER = (None, 0, 1, 1024, 65536, 64 << 20)
LOW_WATER = (None, None, 0, 1, 512, 65536, 128 << 20)


def _payload(rng, n):
    """`n` bytes that differ from run to run of the generator but not
    between the two loops (the scenario carries them)."""
    seed = rng.randrange(1 << 30)
    block = struct.pack("<I", seed) * 256
    return (block * (n // len(block) + 1))[:n]


# --------------------------------------------------------------------------
# the scenario
# --------------------------------------------------------------------------


def generate(seed, *, tls=False):
    rng = random.Random(seed)
    sc = {
        "seed": seed,
        "tls": tls,
        # How the transport comes to be: dialed, accepted by a listening
        # server, or wrapped around a socket the caller connected.
        "mode": rng.choice(["connect", "accept", "adopt_client", "adopt_server"]),
        "buffered": rng.random() < 0.3,
        "bufsize": rng.choice([1, 100, 4096, 65536, 262144]),
        "eof_returns": rng.choice([None, None, False, True]),
        "ops": [],
    }
    transport_ops = [
        ("write_small", 10),
        ("write_big", 4),
        ("write_empty", 1),
        ("writelines", 2),
        ("write_bad_type", 1),
        ("write_eof", 3),
        ("can_write_eof", 1),
        ("close", 3),
        ("abort", 2),
        ("pause_reading", 3),
        ("resume_reading", 3),
        ("is_reading", 2),
        ("is_closing", 2),
        ("buffer_size", 3),
        ("get_limits", 1),
        ("set_limits", 2),
        ("set_protocol", 1),
        ("extra_info", 1),
    ]
    peer_ops = [
        ("peer_send_small", 10),
        ("peer_send_big", 3),
        ("peer_stall", 3),
        ("peer_unstall", 3),
        ("peer_shutdown_wr", 3),
        ("peer_close", 2),
        ("peer_reset", 2),
    ]
    table = transport_ops + peer_ops
    names = [n for n, _ in table]
    weights = [w for _, w in table]
    for _ in range(rng.randint(3, 12)):
        name = rng.choices(names, weights)[0]
        if tls and name in ("write_eof", "peer_shutdown_wr"):
            # TLS has no half-close: write_eof raises (covered by
            # can_write_eof) and a peer's close_notify is its last word.
            name = "can_write_eof" if name == "write_eof" else "peer_close"
        op = {"op": name}
        if name in ("write_small", "peer_send_small"):
            op["data"] = _payload(rng, rng.choice(SMALL))
        elif name in ("write_big", "peer_send_big"):
            op["data"] = _payload(rng, rng.choice(BIG))
        elif name == "writelines":
            op["data"] = [_payload(rng, rng.choice(SMALL)) for _ in range(rng.randint(0, 4))]
        elif name == "write_bad_type":
            op["data"] = rng.choice(["text", 7, None, [b"x"]])
        elif name == "set_limits":
            op["high"] = rng.choice(HIGH_WATER)
            op["low"] = rng.choice(LOW_WATER)
        elif name == "extra_info":
            op["name"] = rng.choice(["peername", "sockname", "socket", "sslcontext", "no-such-key"])
        sc["ops"].append(op)
    return sc


def describe(sc):
    lines = [
        f"seed {sc['seed']}  tls={sc['tls']}  mode={sc['mode']}  buffered={sc['buffered']}"
        f"  bufsize={sc['bufsize']}  eof_received returns {sc['eof_returns']!r}"
    ]
    for i, op in enumerate(sc["ops"]):
        extra = {k: (f"<{len(v)} bytes>" if isinstance(v, bytes) else v) for k, v in op.items() if k != "op"}
        if isinstance(extra.get("data"), list):
            extra["data"] = [f"<{len(x)} bytes>" for x in extra["data"]]
        lines.append(f"  {i:2d}. {op['op']} {extra if extra else ''}")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# the peer: a socket the test drives by hand
# --------------------------------------------------------------------------


class Peer:
    """The far end. `pump()` moves whatever can move right now and says
    whether anything did; it is only called while the scenario settles."""

    def __init__(self, sock):
        sock.setblocking(False)
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock = sock
        self.stalled = False
        self.pending = bytearray()  # asked to send, not yet taken by the kernel
        self.sent = 0
        self.received = 0
        self.crc = 0
        self.inbound = "open"  # open | eof | reset
        self.closed = False

    # plaintext hooks; TlsPeer overrides them
    def _recv(self, n):
        return self.sock.recv(n)

    def _send(self, data):
        return self.sock.send(data)

    def send(self, data):
        if not self.closed:
            self.pending += data

    def backlog(self):
        """Bytes this end still means to send."""
        return len(self.pending)

    def pump(self):
        moved = False
        if self.closed:
            return False
        while self.pending:
            try:
                n = self._send(self.pending[: 256 * 1024])
            except (BlockingIOError, ssl.SSLWantWriteError, ssl.SSLWantReadError):
                break
            except OSError:
                self.pending.clear()
                break
            if not n:
                break
            del self.pending[:n]
            self.sent += n
            moved = True
        while not self.stalled and self.inbound == "open":
            try:
                data = self._recv(256 * 1024)
            except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
                break
            except ConnectionError:
                self.inbound = "reset"
                moved = True
                break
            except OSError:
                self.inbound = "reset"
                moved = True
                break
            if not data:
                self.inbound = "eof"
                moved = True
                break
            self.received += len(data)
            self.crc = zlib.crc32(data, self.crc)
            moved = True
        return moved

    def shutdown_wr(self):
        if not self.closed:
            try:
                self.sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def close(self):
        if not self.closed:
            self.closed = True
            self.sock.close()

    def reset(self):
        if not self.closed:
            self.closed = True
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            self.sock.close()


class TlsPeer(Peer):
    """The same far end behind TLS, sans-IO: an SSLObject over two
    MemoryBIOs, moved to and from the socket by `pump()`. The handshake is
    just the first thing a pump does."""

    def __init__(self, sock, ctx, *, server_side):
        super().__init__(sock)
        self.inbio = ssl.MemoryBIO()
        self.outbio = ssl.MemoryBIO()
        hostname = None if server_side else "localhost"
        self.obj = ctx.wrap_bio(self.inbio, self.outbio, server_side=server_side, server_hostname=hostname)
        self.handshaken = False
        self.wire_out = bytearray()
        self.wire_eof = False
        self.tls_error = None

    def _flush_wire(self):
        self.wire_out += self.outbio.read()
        moved = False
        while self.wire_out:
            try:
                n = self.sock.send(self.wire_out[: 256 * 1024])
            except BlockingIOError:
                break
            except OSError:
                self.wire_out.clear()
                break
            del self.wire_out[:n]
            moved = True
        return moved

    def _fill_wire(self):
        moved = False
        while not self.wire_eof:
            try:
                data = self.sock.recv(256 * 1024)
            except BlockingIOError:
                break
            except OSError:
                self.wire_eof = True
                self.inbound = "reset"
                moved = True
                break
            if not data:
                self.wire_eof = True
                self.inbio.write_eof()
                moved = True
                break
            self.inbio.write(data)
            moved = True
        return moved

    def backlog(self):
        return len(self.pending) + len(self.wire_out)

    def _recv(self, n):
        try:
            return self.obj.read(n)
        except ssl.SSLZeroReturnError:
            return b""
        except ssl.SSLWantReadError:
            if self.wire_eof:
                return b""  # the stream ended without a close_notify
            raise
        except ssl.SSLError as e:
            self.tls_error = type(e).__name__
            return b""

    def _send(self, data):
        return self.obj.write(bytes(data))

    def pump(self):
        if self.closed:
            return False
        moved = False if self.stalled else self._fill_wire()
        if not self.handshaken:
            try:
                self.obj.do_handshake()
                self.handshaken = True
                moved = True
            except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
                pass
            except (ssl.SSLError, OSError) as e:
                self.tls_error = type(e).__name__
                self.inbound = "reset"
                self.handshaken = True
        if self.handshaken and self.tls_error is None:
            moved |= super().pump()
        moved |= self._flush_wire()
        return moved

    def close(self):
        # A polite TLS close: close_notify, then the socket.
        if not self.closed:
            try:
                self.obj.unwrap()
            except (ssl.SSLError, OSError):
                pass
            self._flush_wire()
            self.closed = True
            self.sock.close()


# --------------------------------------------------------------------------
# the protocol under the transport: records what it is told
# --------------------------------------------------------------------------


def _exc_kind(exc):
    if exc is None:
        return None
    if isinstance(exc, ConnectionError):
        return "ConnectionError"  # which one a reset surfaces as is timing
    return type(exc).__name__


class _RecorderBase:
    def __init__(self, trace, sc, tag=""):
        self.trace = trace
        self.sc = sc
        self.tag = tag
        self.transport = None

    def _event(self, *ev):
        self.trace.append((self.tag + ev[0],) + ev[1:])

    def _data(self, data):
        last = self.trace[-1] if self.trace else None
        if last is not None and last[0] == self.tag + "data":
            self.trace[-1] = (last[0], last[1] + len(data), zlib.crc32(data, last[2]))
        else:
            self.trace.append((self.tag + "data", len(data), zlib.crc32(data)))

    def connection_made(self, transport):
        self.transport = transport
        self._event("connection_made")

    def eof_received(self):
        self._event("eof_received")
        return self.sc["eof_returns"]

    def pause_writing(self):
        self._event("pause_writing")

    def resume_writing(self):
        self._event("resume_writing")

    def connection_lost(self, exc):
        self._event("connection_lost", _exc_kind(exc))


class Recorder(_RecorderBase, asyncio.Protocol):
    def data_received(self, data):
        self._data(data)


class BufferedRecorder(_RecorderBase, asyncio.BufferedProtocol):
    def __init__(self, trace, sc, tag=""):
        super().__init__(trace, sc, tag)
        self.buf = bytearray(sc["bufsize"])

    def get_buffer(self, sizehint):
        # A view, not the bytearray itself: over TLS, asyncio's sslproto
        # fills the buffer through `buf[offset:]`, which copies a
        # bytearray, and the protocol is then handed stale bytes for
        # everything past the first record (CPython 3.11-3.13). cadeloop's
        # native TLS does not have that bug -- test_native_tls.py holds it
        # to that -- so the reference has to be kept off it here.
        return memoryview(self.buf)

    def buffer_updated(self, nbytes):
        self._data(bytes(memoryview(self.buf)[:nbytes]))


def _recorder(trace, sc, tag=""):
    return (BufferedRecorder if sc["buffered"] else Recorder)(trace, sc, tag)


# --------------------------------------------------------------------------
# running a scenario
# --------------------------------------------------------------------------


def _tcp():
    """A TCP socket that says so. asyncio turns Nagle off only for sockets
    whose `proto` is IPPROTO_TCP -- one made without naming the protocol
    has proto 0 and keeps it on, and so does every socket accepted from
    it -- while cadeloop turns it off for every TCP stream. With Nagle on,
    a small write waits for the previous one's delayed ACK and arrives
    40 ms after the script has moved on."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
    _tune(sock)
    return sock


def _tune(sock):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKBUF)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKBUF)


def _pair():
    """A connected loopback TCP pair with fixed buffer sizes."""
    lst = _tcp()
    lst.bind(("127.0.0.1", 0))
    lst.listen(1)
    a = _tcp()
    a.connect(lst.getsockname())
    b, _ = lst.accept()
    lst.close()
    return a, b


async def _settle(peer, proto, trace):
    """Run until nothing moves: no callback, no byte either way, no change
    in the write buffer, for a few consecutive turns of the loop.

    While something is still owed -- the peer has bytes to send to a
    transport that is reading, or the transport has bytes buffered for a
    peer that is reading -- "a few turns" is not enough: TCP may be
    sitting on a timer. Then only a long silence ends the wait."""
    quiet = 0
    last = None
    for _ in range(30000):
        await asyncio.sleep(0.001)
        moved = peer.pump()
        transport = proto.transport
        try:
            buffered = transport.get_write_buffer_size()
            # Under TLS the application's transport only counts plaintext
            # it has not encrypted yet; the ciphertext waits in the
            # transport below it (sslproto's and the port's alike).
            below = getattr(getattr(transport, "_ssl_protocol", None), "_transport", None)
            if below is not None:
                buffered += below.get_write_buffer_size()
        except Exception:
            buffered = -1
        state = (len(trace), trace[-1] if trace else None, buffered, peer.backlog(), peer.received)
        if moved or state != last:
            quiet = 0
            last = state
            continue
        quiet += 1
        owed = False
        if not peer.closed:
            if peer.backlog():
                try:
                    owed = bool(transport.is_reading())
                except Exception:
                    owed = False
            if buffered > 0 and not peer.stalled:
                owed = True
        if quiet >= (300 if owed else 4):
            return
    raise AssertionError("the scenario never settled")


def _call(trace, name, fn, normalize=lambda v: v):
    """Record a call's outcome. The entry is opened before the call and
    closed after it, so a callback the call makes synchronously shows up
    between the two -- that ordering is part of what is compared."""
    trace.append(("call", name))
    try:
        result = normalize(fn())
    except Exception as e:
        trace.append(("raised", type(e).__name__))
    else:
        trace.append(("returned", result))


def _delivered(trace):
    """Bytes the protocol has been given so far."""
    return sum(ev[1] for ev in trace if ev[0].endswith("data"))


async def run_scenario(loop, sc, tls_ctxs=None):
    trace = []
    proto = _recorder(trace, sc)
    server = None
    server_ctx, client_ctx = tls_ctxs if tls_ctxs else (None, None)
    mode = sc["mode"]

    # -- bring the connection up -------------------------------------------
    if mode == "accept":
        lst = _tcp()
        lst.bind(("127.0.0.1", 0))
        server = await loop.create_server(lambda: proto, sock=lst, ssl=server_ctx)
        raw = _tcp()
        raw.setblocking(False)
        try:
            raw.connect(lst.getsockname())
        except BlockingIOError:
            pass
        peer = TlsPeer(raw, client_ctx, server_side=False) if sc["tls"] else Peer(raw)
        transport_side_is_server = True
        pending_connect = None
    elif mode == "connect":
        lst = _tcp()
        lst.bind(("127.0.0.1", 0))
        lst.listen(1)
        lst.setblocking(False)
        kw = {"ssl": client_ctx, "server_hostname": "localhost"} if sc["tls"] else {}
        pending_connect = asyncio.ensure_future(
            loop.create_connection(lambda: proto, "127.0.0.1", lst.getsockname()[1], **kw)
        )
        raw = None
        for _ in range(2000):
            await asyncio.sleep(0.001)
            try:
                raw, _ = lst.accept()
                break
            except BlockingIOError:
                continue
        lst.close()
        assert raw is not None, "the connection never arrived"
        peer = TlsPeer(raw, server_ctx, server_side=True) if sc["tls"] else Peer(raw)
        transport_side_is_server = False
    else:
        a, b = _pair()
        transport_side_is_server = mode == "adopt_server"
        if transport_side_is_server:
            adopting = loop.connect_accepted_socket(lambda: proto, sock=b, ssl=server_ctx)
            pending_connect = asyncio.ensure_future(adopting)
            peer = TlsPeer(a, client_ctx, server_side=False) if sc["tls"] else Peer(a)
        else:
            kw = {"ssl": client_ctx, "server_hostname": "localhost"} if sc["tls"] else {}
            pending_connect = asyncio.ensure_future(loop.create_connection(lambda: proto, sock=a, **kw))
            peer = TlsPeer(b, server_ctx, server_side=True) if sc["tls"] else Peer(b)

    for _ in range(4000):
        await asyncio.sleep(0.001)
        peer.pump()
        if proto.transport is not None and (pending_connect is None or pending_connect.done()):
            if not sc["tls"] or peer.handshaken:
                break
    else:
        raise AssertionError("the connection never came up")
    if pending_connect is not None:
        pending_connect.result()
    transport = proto.transport
    if mode in ("connect", "accept"):
        # The loop made this socket; give it the pair's buffer sizes.
        _tune(transport.get_extra_info("socket"))
    await _settle(peer, proto, trace)

    # -- the script ----------------------------------------------------------
    extra_protos = 0
    for op in sc["ops"]:
        name = op["op"]
        t = proto.transport
        if name in ("write_small", "write_big", "write_bad_type"):
            _call(trace, name, lambda: t.write(op["data"]))
        elif name == "write_empty":
            _call(trace, name, lambda: t.write(b""))
        elif name == "writelines":
            _call(trace, name, lambda: t.writelines(op["data"]))
        elif name == "write_eof":
            _call(trace, name, t.write_eof)
        elif name == "can_write_eof":
            _call(trace, name, t.can_write_eof)
        elif name == "close":
            _call(trace, name, t.close)
        elif name == "abort":
            # Whether unsent data was dropped decides what the peer can
            # have seen, so it is part of the record.
            _call(trace, "abort", lambda: (t.get_write_buffer_size() > 0, t.abort())[0])
        elif name == "pause_reading":
            _call(trace, name, t.pause_reading)
        elif name == "resume_reading":
            _call(trace, name, t.resume_reading)
        elif name == "is_reading":
            _call(trace, name, t.is_reading)
        elif name == "is_closing":
            _call(trace, name, t.is_closing)
        elif name == "buffer_size":
            _call(trace, name, t.get_write_buffer_size, lambda n: n > 0)
        elif name == "get_limits":
            _call(trace, name, t.get_write_buffer_limits)
        elif name == "set_limits":
            _call(trace, name, lambda: t.set_write_buffer_limits(high=op["high"], low=op["low"]))
            _call(trace, "get_limits", t.get_write_buffer_limits)
        elif name == "set_protocol":
            extra_protos += 1
            new = _recorder(trace, sc, tag=f"p{extra_protos}:")
            new.transport = t
            _call(trace, name, lambda: t.set_protocol(new))
            if not any(ev[0].endswith("connection_lost") for ev in trace):
                # Asked only of a live connection. A protocol handed to a
                # transport whose connection is lost has nothing left to
                # receive; asyncio stores it all the same, cadeloop does
                # not (holding it would tie protocol and transport into a
                # cycle for no caller's benefit).
                _call(trace, "get_protocol_is_new", lambda: t.get_protocol() is new)
            proto = new
        elif name == "extra_info":
            key = op["name"]
            _call(trace, f"extra_info:{key}", lambda: type(t.get_extra_info(key, "dflt")).__name__)
        elif name in ("peer_send_small", "peer_send_big"):
            peer.send(op["data"])
        elif name == "peer_stall":
            peer.stalled = True
        elif name == "peer_unstall":
            peer.stalled = False
        elif name == "peer_shutdown_wr":
            # Recorded before the step, True or False, in both traces: a
            # half-close with bytes the kernel had not yet taken throws
            # those bytes away (see `_reconcile`).
            trace.append(("peer_shut_with_unsent_data", peer.backlog() > 0))
            peer.shutdown_wr()
        elif name in ("peer_close", "peer_reset"):
            unread = peer.sent > _delivered(trace)
            peer.close() if name == "peer_close" else peer.reset()
            trace.append(("step", name))
            trace.append(("peer_left_unread_data", unread))
            await _settle(peer, proto, trace)
            continue
        else:
            raise AssertionError(name)
        trace.append(("step", name))
        await _settle(peer, proto, trace)

    # -- wind down -------------------------------------------------------------
    peer.stalled = False
    await _settle(peer, proto, trace)
    trace.append(("peer", peer.inbound, peer.received, peer.crc, peer.sent > _delivered(trace)))
    _call(trace, "final:is_closing", proto.transport.is_closing)
    try:
        proto.transport.abort()
    except AttributeError:
        pass  # asyncio, on a transport it has already let go of (see _reconcile)
    await _settle(peer, proto, trace)
    peer.close()
    if server is not None:
        server.close()
        await server.wait_closed()
    await asyncio.sleep(0.005)
    return trace


def _first_error_loss(trace):
    for i, ev in enumerate(trace):
        if ev[0].endswith("connection_lost") and ev[1] is not None:
            return i
    return None


def _steps_before(trace, index):
    return [ev[1] for ev in trace[:index] if ev[0] == "step"]


def _after_the_peer_left(trace, cut_short=()):
    """The trace without what a departed peer makes a matter of timing
    (see `_reconcile`). `cut_short`: which of the peer's half-closes, in
    order, left unsent data behind in either trace."""
    out = []
    dead = unread = False
    shuts = 0
    for ev in trace:
        if ev[0] == "step" and ev[1] in ("peer_close", "peer_reset"):
            dead = True
        elif ev == ("peer_left_unread_data", True):
            unread = True
        elif ev[0] == "peer_shut_with_unsent_data":
            unread = unread or (shuts < len(cut_short) and cut_short[shuts])
            shuts += 1
            continue  # the two traces may disagree on it; what follows is what matters
        if dead and ev[0].endswith(("pause_writing", "resume_writing")):
            continue
        if unread and ev[0].endswith("data"):
            continue
        out.append(ev)
    return out


def _paused_set_protocol(trace):
    """Where the first set_protocol() made on a paused transport is, or
    None (see `_reconcile`)."""
    paused = False
    for i, ev in enumerate(trace):
        if ev[0].endswith("pause_writing"):
            paused = True
        elif ev[0].endswith("resume_writing"):
            paused = False
        elif ev == ("call", "set_protocol") and paused:
            return i
    return None


def _without_the_stuck_buffers_pauses(trace):
    """A 3.12 asyncio trace without the pause_writing/resume_writing its
    dead transport's stuck buffer causes (see `_reconcile`)."""
    out = []
    lost = stuck = False
    for ev in trace:
        if ev[0].endswith("connection_lost"):
            lost = True
        elif lost and ev == ("call", "writelines"):
            stuck = True
        if stuck and ev[0].endswith(("pause_writing", "resume_writing")):
            continue
        out.append(ev)
    return out


def _without_flow_control_after(trace, index):
    return [ev for i, ev in enumerate(trace)
            if i <= index or not ev[0].endswith(("pause_writing", "resume_writing"))]


def _reconcile(ours, theirs):
    """Remove from both traces what no event loop can be held to.

    * A reset is noticed when the loop next looks at the socket. epoll
      reports an error condition whether or not anyone asked, so cadeloop
      learns of an RST at once; the selector loop learns of it at its next
      read or write, which for a transport that is paused, or that stopped
      reading after EOF, may be several steps later or never. Once a peer
      has closed or reset, the two traces are compared up to the first
      connection_lost(error) in either and no further -- unless both put
      it in the same place, in which case everything is compared.
    * A write to a peer that has closed races the reset coming back: the
      loop that gets one send in before it sees a full buffer and pauses
      the protocol, the loop that gets two sees the error first and never
      pauses. From the peer's close on, pause_writing and resume_writing
      are left out of both traces, and so is a write_eof() that fails in
      one loop only (asyncio's raises whatever shutdown() says about the
      dead socket).
    * A peer that left while the transport had not read everything it
      sent: how much of the rest still arrives depends on what the kernel
      had accepted by then. The deliveries after that point are left out.
      The same for a peer that half-closes with bytes of its own still
      unsent (a paused transport, a full socket): the shutdown throws the
      rest away, and how much had already crossed depends on how much room
      the receiving side's reads had made -- cadeloop reads ahead of a
      pause and the selector loop does not, which moved the cut by 164
      bytes in one scenario and 64 KiB in another.
    * set_protocol() on a transport whose protocol is paused. asyncio
      keeps the paused flag on the transport, so the new protocol is later
      sent a resume_writing() it never had a pause_writing() for, and gets
      no pause_writing() when its own write finds the buffer still over
      the mark. cadeloop starts a new protocol unpaused and keeps the two
      calls in pairs per protocol (start_tls needs that: sslproto's
      resume_writing asserts it was paused). From such a set_protocol()
      on, pause_writing and resume_writing are left out of both traces;
      test_set_protocol_does_not_carry_a_pause_over holds cadeloop to its
      side of it.
    * After unsent data has been thrown away, or the connection has
      failed, how much the peer received is the kernel's business: the
      peer's record keeps its end state and drops the byte count.
    * A transport that closes with unread data in its socket resets the
      connection; one that had already pulled that data out of the kernel
      closes it cleanly. cadeloop reads ahead and the selector loop does
      not, so when the peer had sent more than the protocol was given,
      "eof" and "reset" at the peer's end are one outcome.
    * asyncio forgets its loop when a transport is lost, and a transport
      lost by close() draining its buffer never marks itself lost: calls
      made on it afterwards (abort(), write()) die of an AttributeError on
      None. That is the stdlib's accident, not a contract; whatever
      cadeloop answers there stands.
    * The same kind of accident, in 3.12 only: its selector transport sets
      `_write_ready = None` when the connection is lost and its new
      writelines() calls it without checking, so writelines() on a lost
      transport dies of "TypeError: 'NoneType' object is not callable" --
      after it has appended the data to the dead transport's buffer, which
      then reports that size for good. (An early 3.12 clears the method
      in close() instead, so there a transport lost to an error gets the
      stuck buffer without the TypeError.) 3.11 and 3.13 drop the data
      silently, and so does cadeloop on all three. The stuck buffer has
      two more echoes: abort() finds "unsent data" to throw away on a
      connection that was lost long before, and a later
      set_write_buffer_limits() below its size calls pause_writing() on
      whatever protocol the dead transport holds. Both are left out.
    """
    marks = [[ev[1] for ev in trace if ev[0] == "peer_shut_with_unsent_data"] for trace in (ours, theirs)]
    cut_short = [x or y for x, y in zip(*marks, strict=False)]
    ours, theirs = _after_the_peer_left(ours, cut_short), _after_the_peer_left(theirs, cut_short)
    swapped = [i for i in (_paused_set_protocol(ours), _paused_set_protocol(theirs)) if i is not None]
    if swapped:
        ours = _without_flow_control_after(ours, min(swapped))
        theirs = _without_flow_control_after(theirs, min(swapped))
    a, b = _first_error_loss(ours), _first_error_loss(theirs)
    if a != b:
        cut = min(i for i in (a, b) if i is not None)
        left = ("peer_close", "peer_reset")
        if any(n in left for n in _steps_before(ours, cut + 1) + _steps_before(theirs, cut + 1)):
            ours, theirs = ours[:cut], theirs[:cut]

    def lossy(trace):
        return any(
            (ev[0] == "returned" and i and trace[i - 1] == ("call", "abort") and ev[1] is True)
            or (ev[0].endswith("connection_lost") and ev[1] is not None)
            for i, ev in enumerate(trace)
        )

    drop_counts = lossy(ours) or lossy(theirs)

    def peer_record(ev):
        if ev[0] != "peer":
            return ev
        _, state, received, crc, unread = ev
        if unread and state in ("eof", "reset"):
            state = "closed"
        return ("peer", state) if drop_counts else ("peer", state, received, crc)

    ours = [peer_record(ev) for ev in ours]
    theirs = [peer_record(ev) for ev in theirs]

    if sys.version_info[:2] == (3, 12):
        theirs = _without_the_stuck_buffers_pauses(theirs)
    lost = gone = stuck_buffer = False
    for i, ev in enumerate(theirs):
        if ev[0].endswith("connection_lost"):
            lost = True
        elif ev[0] == "step" and ev[1] in ("peer_close", "peer_reset"):
            gone = True
        elif i < len(ours) and ours[i][0] in ("returned", "raised"):
            stdlib_accident = lost and ev == ("raised", "AttributeError")
            lost_writelines_312 = (
                sys.version_info[:2] == (3, 12) and lost and theirs[i - 1] == ("call", "writelines")
            )
            writelines_312 = lost_writelines_312 and ev == ("raised", "TypeError")
            dead_socket = gone and ev == ("raised", "OSError") and theirs[i - 1] == ("call", "write_eof")
            stuck_buffer = stuck_buffer or lost_writelines_312
            stuck_size = (
                stuck_buffer and ev[0] == "returned" and theirs[i - 1][1] in ("buffer_size", "abort")
            )
            if stdlib_accident or writelines_312 or stuck_size or dead_socket:
                theirs[i] = ours[i]
    return ours, theirs


def _run_on(kind, sc, tls_ctxs=None):
    if kind == "cadeloop":
        loop = cadeloop.Loop(native_tls=False)
    elif kind == "cadeloop-native-tls":
        loop = cadeloop.Loop(native_tls=True)
    else:
        loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    errors = []
    loop.set_exception_handler(lambda _loop, ctx: errors.append(str(ctx.get("message"))))
    try:
        return loop.run_until_complete(asyncio.wait_for(run_scenario(loop, sc, tls_ctxs), 120))
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def _diff(a, b):
    out = []
    for i in range(max(len(a), len(b))):
        x = a[i] if i < len(a) else "<nothing>"
        y = b[i] if i < len(b) else "<nothing>"
        mark = "  " if x == y else "!="
        out.append(f"{mark} {i:3d}  cadeloop {x!r:60}  asyncio {y!r}")
    return "\n".join(out)


def _seeds():
    if ONLY:
        return [int(ONLY)]
    return list(range(SEEDS))


@pytest.mark.parametrize("seed", _seeds())
def test_plain_tcp_scenario_matches_asyncio(seed):
    sc = generate(seed)
    ours, theirs = _reconcile(_run_on("cadeloop", sc), _run_on("asyncio", sc))
    assert ours == theirs, f"\n{describe(sc)}\n{_diff(ours, theirs)}"


@pytest.fixture(scope="module")
def tls_contexts():
    trustme = pytest.importorskip("trustme")
    ca = trustme.CA()
    cert = ca.issue_cert("localhost", "127.0.0.1")
    server_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    cert.configure_cert(server_ctx)
    client_ctx = ssl.create_default_context()
    ca.configure_trust(client_ctx)
    return server_ctx, client_ctx


@pytest.mark.parametrize("seed", _seeds()[: max(1, len(_seeds()) // 2)])
def test_tls_scenario_matches_asyncio(seed, tls_contexts):
    """The same scripts over TLS, the peer speaking it through an
    SSLObject it pumps by hand -- so it can also leave without a
    close_notify, or reset mid-record."""
    sc = generate(seed, tls=True)
    ours, theirs = _reconcile(_run_on("cadeloop", sc, tls_contexts), _run_on("asyncio", sc, tls_contexts))
    assert ours == theirs, f"\n{describe(sc)}\n{_diff(ours, theirs)}"


@pytest.mark.parametrize("seed", _seeds()[: max(1, len(_seeds()) // 2)])
def test_native_tls_scenario_matches_asyncio(seed, tls_contexts):
    """The TLS scripts again, with cadeloop's own TLS protocol (the Rust
    port of sslproto, `Loop(native_tls=True)`) on cadeloop's side and the
    standard library's still on asyncio's: the port is held to the
    original, callback for callback."""
    sc = generate(seed, tls=True)
    ours = _run_on("cadeloop-native-tls", sc, tls_contexts)
    ours, theirs = _reconcile(ours, _run_on("asyncio", sc, tls_contexts))
    assert ours == theirs, f"\n{describe(sc)}\n{_diff(ours, theirs)}"
