//! RFC 6455 WebSocket wire codec (R-087): handshake accept-key, frame
//! parsing/encoding, and message assembly. Platform-free and
//! Python-free — the engine (pyshim) drives it; everything here is
//! unit-tested on any host.
//!
//! Server role only: outbound frames are unmasked, inbound frames MUST
//! be masked (1002 otherwise). permessage-deflate is deliberately NOT
//! negotiated (declining an extension is protocol-legal; clients fall
//! back to uncompressed) — a later refinement can add it.

// ---------------------------------------------------------------------
// handshake (SHA-1 + base64 are tiny, fully-specified transforms; the
// SHA-1 here is a protocol constant derivation, not a security use)
// ---------------------------------------------------------------------

const WS_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];
    let ml = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&ml.to_be_bytes());
    // The padding above makes the length an exact multiple of 64, so the
    // remainder `as_chunks` returns is always empty.
    for chunk in msg.as_chunks::<64>().0 {
        let mut w = [0u32; 80];
        for (i, word) in chunk.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(*word);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let tmp = a.rotate_left(5).wrapping_add(f).wrapping_add(e).wrapping_add(k).wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = tmp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
    let mut out = [0u8; 20];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { T[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { T[n as usize & 63] as char } else { '=' });
    }
    out
}

/// Sec-WebSocket-Accept for a client's Sec-WebSocket-Key (RFC 6455 §4.2.2).
pub fn accept_key(client_key: &[u8]) -> String {
    let mut buf = Vec::with_capacity(client_key.len() + WS_GUID.len());
    buf.extend_from_slice(client_key);
    buf.extend_from_slice(WS_GUID);
    base64(&sha1(&buf))
}

// ---------------------------------------------------------------------
// frames
// ---------------------------------------------------------------------

pub const OP_CONT: u8 = 0x0;
pub const OP_TEXT: u8 = 0x1;
pub const OP_BINARY: u8 = 0x2;
pub const OP_CLOSE: u8 = 0x8;
pub const OP_PING: u8 = 0x9;
pub const OP_PONG: u8 = 0xA;

/// Encode one server frame (FIN set, unmasked).
pub fn frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 10);
    push_frame_header(&mut out, opcode, payload.len());
    out.extend_from_slice(payload);
    out
}

/// The header alone of a server frame (FIN set, unmasked) carrying `n`
/// payload bytes: for a payload that is sent from where it already is,
/// right behind this, instead of being copied into the frame.
pub fn frame_header(opcode: u8, n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(10);
    push_frame_header(&mut out, opcode, n);
    out
}

fn push_frame_header(out: &mut Vec<u8>, opcode: u8, n: usize) {
    push_header(out, 0x80 | (opcode & 0x0F), n);
}

/// One server frame (FIN set, unmasked) whose payload is `deflated`: a
/// whole message compressed by [`Deflater::message`] (RSV1 set).
pub fn frame_deflated(opcode: u8, deflated: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(deflated.len() + 10);
    push_header(&mut out, 0x80 | RSV1 | (opcode & 0x0F), deflated.len());
    out.extend_from_slice(deflated);
    out
}

fn push_header(out: &mut Vec<u8>, first: u8, n: usize) {
    out.push(first);
    if n < 126 {
        out.push(n as u8);
    } else if n <= 0xFFFF {
        out.push(126);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(127);
        out.extend_from_slice(&(n as u64).to_be_bytes());
    }
}

/// Encode a close frame with a status code and (truncated) reason.
///
/// A close payload is capped at 125 bytes, leaving 123 for the reason.
/// Truncation lands on a character boundary, not a byte one: slicing mid
/// sequence emits invalid UTF-8, and RFC 6455 requires the reason to be
/// valid UTF-8, so a compliant peer would answer our close with a
/// protocol error (Codex review, PR #1).
pub fn close_frame(code: u16, reason: &str) -> Vec<u8> {
    let reason = truncate_on_char_boundary(reason, 123);
    let mut payload = Vec::with_capacity(2 + reason.len());
    payload.extend_from_slice(&code.to_be_bytes());
    payload.extend_from_slice(reason.as_bytes());
    frame(OP_CLOSE, &payload)
}

/// Largest prefix of `s` that fits in `max` bytes without splitting a
/// character.
fn truncate_on_char_boundary(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

// ---------------------------------------------------------------------------
// permessage-deflate (RFC 7692)
// ---------------------------------------------------------------------------
//
// Opt-in, and negotiated one way only: no context takeover in either
// direction. With takeover, each side keeps its compressor's window from
// message to message -- better ratios on small similar messages, at the
// price of a compressor and a decompressor held for every connection
// (about a quarter of a megabyte, idle or not). Without it every message
// stands alone, so one compressor and one decompressor serve every
// connection of a worker and an idle WebSocket costs what it did before.

/// RSV1 in a frame's first byte: "this message is compressed".
const RSV1: u8 = 0x40;

/// The empty stored block a sync flush ends a DEFLATE stream with. The
/// extension leaves these four bytes off the wire and the receiver puts
/// them back.
const DEFLATE_TAIL: [u8; 4] = [0x00, 0x00, 0xff, 0xff];

/// Choose among a client's `Sec-WebSocket-Extensions` offers (one header
/// value per item; each may hold several comma-separated offers). Returns
/// the value of the response header for the first `permessage-deflate`
/// offer this implementation can honour, or None to negotiate nothing --
/// which is always a legal answer.
///
/// Honoured: any offer whose parameters are `client_max_window_bits`
/// (with or without a value), `client_no_context_takeover`,
/// `server_no_context_takeover`, and `server_max_window_bits=15`. An offer
/// that asks the server for a smaller window is passed over (the
/// compressor here has one window size), as is one with a parameter this
/// does not know, twice the same parameter, or a malformed value.
pub fn negotiate_deflate<'a>(header_values: impl Iterator<Item = &'a [u8]>) -> Option<String> {
    for value in header_values {
        'offer: for offer in value.split(|&b| b == b',') {
            let mut parts = offer.split(|&b| b == b';').map(trim_ows);
            if !parts.next().is_some_and(|name| name.eq_ignore_ascii_case(b"permessage-deflate")) {
                continue;
            }
            let mut seen = [false; 4];
            let mut echo_server_bits = false;
            for param in parts {
                let (name, arg) = match param.iter().position(|&b| b == b'=') {
                    Some(i) => (trim_ows(&param[..i]), Some(unquote(trim_ows(&param[i + 1..])))),
                    None => (param, None),
                };
                let which = if name.eq_ignore_ascii_case(b"server_no_context_takeover") {
                    0
                } else if name.eq_ignore_ascii_case(b"client_no_context_takeover") {
                    1
                } else if name.eq_ignore_ascii_case(b"client_max_window_bits") {
                    2
                } else if name.eq_ignore_ascii_case(b"server_max_window_bits") {
                    3
                } else {
                    continue 'offer;
                };
                if std::mem::replace(&mut seen[which], true) {
                    continue 'offer;
                }
                let ok = match (which, arg) {
                    (0 | 1, None) => true,
                    (2, None) => true,
                    (2, Some(bits)) => window_bits(bits).is_some(),
                    (3, Some(bits)) => {
                        echo_server_bits = true;
                        window_bits(bits) == Some(15)
                    }
                    _ => false,
                };
                if !ok {
                    continue 'offer;
                }
            }
            let mut response =
                String::from("permessage-deflate; server_no_context_takeover; client_no_context_takeover");
            if echo_server_bits {
                response.push_str("; server_max_window_bits=15");
            }
            return Some(response);
        }
    }
    None
}

fn trim_ows(mut b: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = b {
        b = rest;
    }
    while let [rest @ .., b' ' | b'\t'] = b {
        b = rest;
    }
    b
}

fn unquote(b: &[u8]) -> &[u8] {
    match b {
        [b'"', inner @ .., b'"'] => inner,
        _ => b,
    }
}

/// An RFC 7692 window size, 8 to 15, written without a leading zero.
fn window_bits(b: &[u8]) -> Option<u8> {
    if b.is_empty() || b.len() > 2 || b[0] == b'0' || !b.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let n = b.iter().fold(0u8, |n, d| n * 10 + (d - b'0'));
    (8..=15).contains(&n).then_some(n)
}

/// Compresses outbound messages. One per worker: it is reset before
/// every message (no context takeover).
pub struct Deflater(Box<miniz_oxide::deflate::core::CompressorOxide>);

impl Default for Deflater {
    fn default() -> Self {
        Deflater::new()
    }
}

impl Deflater {
    pub fn new() -> Self {
        // Level 1, raw DEFLATE. On JSON the fast level gives up little
        // ratio (4.0x against 4.3x at 1 KiB, 9.5x against 11x at 10 KiB,
        // by zlib's levels 1 and 6) for about half the time, and the time
        // is spent on the loop's thread.
        let flags = miniz_oxide::deflate::core::create_comp_flags_from_zip_params(1, -15, 0);
        Deflater(Box::new(miniz_oxide::deflate::core::CompressorOxide::new(flags)))
    }

    /// `payload` as the payload of a compressed message: a sync-flushed
    /// DEFLATE stream with its last four bytes removed.
    pub fn message(&mut self, payload: &[u8]) -> Vec<u8> {
        use miniz_oxide::{MZError, MZFlush};
        self.0.reset();
        // Room for the worst case (stored blocks) in one call.
        let mut out = vec![0u8; payload.len() + payload.len() / 8 + 64];
        let mut written = 0;
        let mut input = payload;
        loop {
            let res =
                miniz_oxide::deflate::stream::deflate(&mut self.0, input, &mut out[written..], MZFlush::Sync);
            written += res.bytes_written;
            input = &input[res.bytes_consumed..];
            match res.status {
                // Everything consumed and the flush fit: done.
                Ok(_) if input.is_empty() && written < out.len() => break,
                Ok(_) | Err(MZError::Buf) if written == out.len() => out.resize(out.len() * 2, 0),
                _ => break,
            }
        }
        out.truncate(written);
        if out.ends_with(&DEFLATE_TAIL) {
            out.truncate(out.len() - DEFLATE_TAIL.len());
        }
        out
    }
}

/// Decompresses inbound messages. One per worker, reset before every
/// message (no context takeover).
pub struct Inflater(Box<miniz_oxide::inflate::stream::InflateState>);

impl Default for Inflater {
    fn default() -> Self {
        Inflater::new()
    }
}

impl Inflater {
    pub fn new() -> Self {
        Inflater(miniz_oxide::inflate::stream::InflateState::new_boxed(miniz_oxide::DataFormat::Raw))
    }

    /// The message a compressed `payload` holds, or how to fail the
    /// connection: 1009 when it would inflate past `max` bytes -- checked
    /// as it grows, so a few kilobytes cannot be made to allocate
    /// gigabytes -- and 1007 when it is not valid DEFLATE.
    pub fn message(&mut self, payload: &[u8], max: usize) -> Result<Vec<u8>, (u16, &'static str)> {
        use miniz_oxide::{MZError, MZFlush, MZStatus};
        self.0.reset(miniz_oxide::DataFormat::Raw);
        let mut out: Vec<u8> = Vec::new();
        let mut written = 0;
        for mut input in [payload, &DEFLATE_TAIL[..]] {
            loop {
                if written == out.len() {
                    if written > max {
                        return Err((1009, "message too big"));
                    }
                    // Grow by what is likely needed, never past one byte
                    // over the limit (which is how "too big" is noticed).
                    let step = (input.len() * 4).clamp(1024, 64 * 1024);
                    out.resize((written + step).min(max + 1), 0);
                }
                let res = miniz_oxide::inflate::stream::inflate(
                    &mut self.0,
                    input,
                    &mut out[written..],
                    MZFlush::None,
                );
                written += res.bytes_written;
                input = &input[res.bytes_consumed..];
                match res.status {
                    Ok(MZStatus::StreamEnd) => {
                        out.truncate(written);
                        return if written > max { Err((1009, "message too big")) } else { Ok(out) };
                    }
                    Ok(_) | Err(MZError::Buf) => {
                        if input.is_empty() && written < out.len() {
                            break; // this part is consumed and there was room to say more
                        }
                        if res.bytes_written == 0 && res.bytes_consumed == 0 && written < out.len() {
                            return Err((1007, "invalid compressed data"));
                        }
                    }
                    Err(_) => return Err((1007, "invalid compressed data")),
                }
            }
        }
        out.truncate(written);
        if written > max {
            return Err((1009, "message too big"));
        }
        Ok(out)
    }
}

/// One inbound event, message-assembled.
#[derive(Debug, PartialEq)]
pub enum WsEvent {
    Text(String),
    Binary(Vec<u8>),
    /// A whole data message that arrived compressed (permessage-deflate;
    /// only after [`WsRx::allow_deflate`]). The caller inflates it with
    /// its [`Inflater`]; `text` says which kind of message it is.
    Deflated {
        text: bool,
        payload: Vec<u8>,
    },
    Ping(Vec<u8>),
    Pong,
    /// Peer close frame: (code, reason).
    Close(u16, String),
    /// Protocol violation: fail the connection with this close code.
    Fail(u16, &'static str),
}

/// Inbound frame accumulator + message assembler. Feed raw socket bytes;
/// drain protocol events. Limits: `max_message` bounds the ASSEMBLED
/// message (1009 beyond it).
pub struct WsRx {
    /// The unparsed tail of earlier pushes: a frame not yet complete.
    buf: Vec<u8>,
    /// Bytes the last `parse_one` consumed.
    consumed: usize,
    frag_op: Option<u8>,
    frag: Vec<u8>,
    /// The message being assembled from fragments is compressed.
    frag_deflated: bool,
    /// permessage-deflate was negotiated: RSV1 on the first frame of a
    /// data message marks it compressed.
    deflate: bool,
    pub max_message: usize,
    /// The peer's Close frame, or a protocol failure, ended the stream:
    /// nothing after it is parsed.
    ended: bool,
}

impl WsRx {
    pub fn new(max_message: usize) -> Self {
        WsRx {
            buf: Vec::new(),
            consumed: 0,
            frag_op: None,
            frag: Vec::new(),
            frag_deflated: false,
            deflate: false,
            max_message,
            ended: false,
        }
    }

    /// permessage-deflate was negotiated for this connection.
    pub fn allow_deflate(&mut self) {
        self.deflate = true;
    }

    /// Stop parsing: the caller has failed the connection for a reason
    /// the parser could not see (a compressed message that would not
    /// inflate).
    pub fn end(&mut self) {
        self.ended = true;
        self.buf = Vec::new();
    }

    pub fn push(&mut self, data: &[u8], out: &mut Vec<WsEvent>) {
        if self.ended {
            return;
        }
        if self.buf.is_empty() {
            // Nothing carried over -- the usual case, one or more whole
            // frames per read: parse straight out of `data` and keep only
            // an incomplete tail. Copying every read into `buf` first cost
            // a copy per read, and held `buf` at the largest read's size
            // for the connection's life.
            let used = self.parse_all(data, out);
            if !self.ended && used < data.len() {
                self.buf.extend_from_slice(&data[used..]);
            }
            return;
        }
        let mut buf = std::mem::take(&mut self.buf);
        buf.extend_from_slice(data);
        let used = self.parse_all(&buf, out);
        if !self.ended {
            // Drop what this push parsed, ONCE: `parse_all` walks a cursor
            // instead of draining per frame -- draining per frame was
            // O(remaining) each, i.e. O(n^2) for a buffer full of small
            // frames. A single 64 KiB recv of ~10,900 empty frames cost
            // ~115 ms of in-cell parse CPU (vs ~0.36 ms fed one frame at a
            // time), stalling the whole worker; the cursor makes it linear.
            buf.drain(..used);
            self.buf = buf;
        }
    }

    /// Parse every complete frame at the front of `b` into `out`; returns
    /// how many bytes that consumed. A Close, or a protocol failure, ends
    /// the stream: nothing after it is parsed, and `ended` is set.
    fn parse_all(&mut self, b: &[u8], out: &mut Vec<WsEvent>) -> usize {
        let mut pos = 0;
        loop {
            match self.parse_one(&b[pos..]) {
                ParseOne::NeedMore => return pos,
                ParseOne::Event(ev) => {
                    pos += self.consumed;
                    // A Close ends the stream as a failure does. A peer may
                    // send nothing after its Close (RFC 6455 5.5.1), and the
                    // application has been told websocket.disconnect, the
                    // last message ASGI ever delivers: frames behind it --
                    // in the same read, or still arriving while our echo
                    // drains -- were queued as messages after the
                    // disconnect, and charged to the inbox, all the same.
                    let last = matches!(ev, WsEvent::Fail(..) | WsEvent::Close(..));
                    out.push(ev);
                    if last {
                        self.ended = true;
                        // Nothing more will be parsed; drop the buffer.
                        self.buf = Vec::new();
                        return pos;
                    }
                }
                ParseOne::Continue => pos += self.consumed,
            }
        }
    }

    /// One frame from the front of `b`. A frame parsed into an event or a
    /// fragment sets `consumed` to its length; a failure does not need it
    /// (the stream ends there).
    fn parse_one(&mut self, b: &[u8]) -> ParseOne {
        if b.len() < 2 {
            return ParseOne::NeedMore;
        }
        let fin = b[0] & 0x80 != 0;
        let opcode = b[0] & 0x0F;
        let rsv = b[0] & 0x70;
        // RSV1 means "compressed", and only where the extension puts it:
        // on the first frame of a data message, once negotiated. Anywhere
        // else -- a continuation, a control frame, RSV2 or RSV3, or no
        // extension at all -- a reserved bit is a protocol error.
        let deflated = rsv == RSV1 && self.deflate && matches!(opcode, OP_TEXT | OP_BINARY);
        if rsv != 0 && !deflated {
            return ParseOne::Event(WsEvent::Fail(1002, "reserved bits set"));
        }
        let masked = b[1] & 0x80 != 0;
        if !masked {
            return ParseOne::Event(WsEvent::Fail(1002, "client frame not masked"));
        }
        let mut off = 2usize;
        let mut len = (b[1] & 0x7F) as u64;
        if len == 126 {
            if b.len() < off + 2 {
                return ParseOne::NeedMore;
            }
            len = u64::from(u16::from_be_bytes([b[off], b[off + 1]]));
            off += 2;
            if len < 126 {
                return ParseOne::Event(WsEvent::Fail(1002, "non-minimal payload length"));
            }
        } else if len == 127 {
            if b.len() < off + 8 {
                return ParseOne::NeedMore;
            }
            // RFC 6455 section 5.2 limits this field to 63 bits.  Check
            // before converting to usize so a malicious length cannot wrap
            // on a 32-bit target either.
            if b[off] & 0x80 != 0 {
                return ParseOne::Event(WsEvent::Fail(1002, "invalid payload length"));
            }
            len = u64::from_be_bytes(b[off..off + 8].try_into().unwrap());
            off += 8;
            if len <= u16::MAX as u64 {
                return ParseOne::Event(WsEvent::Fail(1002, "non-minimal payload length"));
            }
        }
        if opcode >= OP_CLOSE && (len > 125 || !fin) {
            return ParseOne::Event(WsEvent::Fail(1002, "malformed control frame"));
        }
        if len > self.max_message as u64 {
            return ParseOne::Event(WsEvent::Fail(1009, "message too big"));
        }
        if b.len() < off + 4 {
            return ParseOne::NeedMore;
        }
        let mask: [u8; 4] = b[off..off + 4].try_into().unwrap();
        off += 4;
        let len = len as usize;
        if b.len() < off + len {
            return ParseOne::NeedMore;
        }
        let mut payload: Vec<u8> = b[off..off + len].to_vec();
        unmask(&mut payload, mask);
        self.consumed = off + len;

        match opcode {
            OP_PING => ParseOne::Event(WsEvent::Ping(payload)),
            OP_PONG => ParseOne::Event(WsEvent::Pong),
            OP_CLOSE => {
                if payload.len() == 1 {
                    return ParseOne::Event(WsEvent::Fail(1002, "malformed close payload"));
                }
                let (code, reason) = if payload.len() >= 2 {
                    let code = u16::from_be_bytes([payload[0], payload[1]]);
                    if !valid_close_code(code) {
                        return ParseOne::Event(WsEvent::Fail(1002, "invalid close code"));
                    }
                    match String::from_utf8(payload[2..].to_vec()) {
                        Ok(r) => (code, r),
                        Err(_) => return ParseOne::Event(WsEvent::Fail(1007, "close reason not utf-8")),
                    }
                } else {
                    (1005, String::new())
                };
                ParseOne::Event(WsEvent::Close(code, reason))
            }
            OP_TEXT | OP_BINARY => {
                if self.frag_op.is_some() {
                    return ParseOne::Event(WsEvent::Fail(1002, "new message inside fragmented message"));
                }
                if fin {
                    self.finish_message(opcode, payload, deflated)
                } else {
                    self.frag_op = Some(opcode);
                    self.frag = payload;
                    self.frag_deflated = deflated;
                    ParseOne::Continue
                }
            }
            OP_CONT => {
                let Some(op) = self.frag_op else {
                    return ParseOne::Event(WsEvent::Fail(1002, "continuation without a message"));
                };
                if self.frag.len() + payload.len() > self.max_message {
                    return ParseOne::Event(WsEvent::Fail(1009, "message too big"));
                }
                self.frag.extend_from_slice(&payload);
                if fin {
                    self.frag_op = None;
                    let msg = std::mem::take(&mut self.frag);
                    self.finish_message(op, msg, self.frag_deflated)
                } else {
                    ParseOne::Continue
                }
            }
            _ => ParseOne::Event(WsEvent::Fail(1002, "unknown opcode")),
        }
    }

    fn finish_message(&mut self, opcode: u8, payload: Vec<u8>, deflated: bool) -> ParseOne {
        if deflated {
            return ParseOne::Event(WsEvent::Deflated { text: opcode == OP_TEXT, payload });
        }
        if opcode == OP_TEXT {
            match String::from_utf8(payload) {
                Ok(s) => ParseOne::Event(WsEvent::Text(s)),
                Err(_) => ParseOne::Event(WsEvent::Fail(1007, "text message not utf-8")),
            }
        } else {
            ParseOne::Event(WsEvent::Binary(payload))
        }
    }
}

/// XOR `data` with the 4-byte masking key, repeated (RFC 6455 5.3).
///
/// Eight bytes at a time, as whole words: a shape the compiler turns into
/// vector XORs. The byte-at-a-time loop this replaces -- `*byte ^=
/// mask[i & 3]` -- stayed scalar, one table lookup per byte, and was a
/// third of the server's CPU on an echo of 64 KiB messages.
fn unmask(data: &mut [u8], mask: [u8; 4]) {
    let word = u64::from_ne_bytes([mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3]]);
    let (words, tail) = data.as_chunks_mut::<8>();
    for w in words {
        *w = (u64::from_ne_bytes(*w) ^ word).to_ne_bytes();
    }
    // Each word started on a multiple of 8, so the tail starts on the key's
    // first byte too.
    for (i, byte) in tail.iter_mut().enumerate() {
        *byte ^= mask[i & 3];
    }
}

pub fn valid_close_code(code: u16) -> bool {
    matches!(code, 1000..=1014 | 3000..=4999) && !matches!(code, 1004..=1006 | 1015)
}

enum ParseOne {
    NeedMore,
    Continue,
    Event(WsEvent),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Client-side frame builder (masked) for tests.
    fn client_frame(opcode: u8, payload: &[u8], fin: bool) -> Vec<u8> {
        let mask = [0x11u8, 0x22, 0x33, 0x44];
        let n = payload.len();
        let mut out = vec![if fin { 0x80 } else { 0x00 } | opcode];
        if n < 126 {
            out.push(0x80 | n as u8);
        } else if n <= 0xFFFF {
            out.push(0x80 | 126);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        } else {
            out.push(0x80 | 127);
            out.extend_from_slice(&(n as u64).to_be_bytes());
        }
        out.extend_from_slice(&mask);
        out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i & 3]));
        out
    }

    #[test]
    fn unmask_matches_the_rfc_definition_at_every_length() {
        // RFC 6455 5.3: transformed-octet-i = original-octet-i XOR
        // masking-key-octet-(i MOD 4). Every length through several words
        // and their tails, and a few large ones.
        let mask = [0x37u8, 0xfa, 0x21, 0x3d];
        for n in (0..=67).chain([255, 256, 257, 4096, 65535, 65536, 65537]) {
            let plain: Vec<u8> = (0..n).map(|i| (i * 31 + 7) as u8).collect();
            let mut data = plain.clone();
            unmask(&mut data, mask);
            for (i, (&got, &was)) in data.iter().zip(&plain).enumerate() {
                assert_eq!(got, was ^ mask[i % 4], "byte {i} of {n}");
            }
            unmask(&mut data, mask);
            assert_eq!(data, plain, "masking twice restores {n} bytes");
        }
        // The RFC's own example (5.7): a masked "Hello".
        let mut hello = [0x7fu8, 0x9f, 0x4d, 0x51, 0x58];
        unmask(&mut hello, mask);
        assert_eq!(&hello, b"Hello");
    }

    #[test]
    fn frame_header_is_the_frame_less_its_payload() {
        for n in [0usize, 1, 125, 126, 127, 0xFFFF, 0x10000, 300_000] {
            let payload = vec![0xA5u8; n];
            for opcode in [OP_TEXT, OP_BINARY] {
                let whole = frame(opcode, &payload);
                let header = frame_header(opcode, n);
                assert_eq!(whole.len(), header.len() + n);
                assert_eq!(&whole[..header.len()], header.as_slice(), "{n}-byte payload");
                assert_eq!(&whole[header.len()..], payload.as_slice());
            }
        }
    }

    #[test]
    fn accept_key_rfc_vector() {
        // The vector from RFC 6455 §1.3.
        assert_eq!(accept_key(b"dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn text_roundtrip_and_fragmentation() {
        let mut rx = WsRx::new(1 << 20);
        let mut out = Vec::new();
        rx.push(&client_frame(OP_TEXT, b"hello", true), &mut out);
        assert_eq!(out, vec![WsEvent::Text("hello".into())]);
        out.clear();
        rx.push(&client_frame(OP_TEXT, b"frag", false), &mut out);
        rx.push(&client_frame(OP_CONT, b"mented", true), &mut out);
        assert_eq!(out, vec![WsEvent::Text("fragmented".into())]);
    }

    #[test]
    fn many_frames_in_one_push_are_all_parsed_and_compacted() {
        // One buffer packed with many complete frames: every one must be
        // delivered, in order, and the cursor must leave `buf` empty
        // afterwards (the compaction that keeps this linear rather than
        // O(n^2) -- see `push`).
        let mut rx = WsRx::new(1 << 20);
        let mut buf = Vec::new();
        let n = 5000;
        for i in 0..n {
            buf.extend_from_slice(&client_frame(OP_BINARY, &[(i & 0xff) as u8], true));
        }
        let mut out = Vec::new();
        rx.push(&buf, &mut out);
        assert_eq!(out.len(), n);
        for (i, ev) in out.iter().enumerate() {
            assert_eq!(*ev, WsEvent::Binary(vec![(i & 0xff) as u8]));
        }
        // Fully consumed: no bytes retained.
        assert!(rx.buf.is_empty());
    }

    #[test]
    fn any_split_of_a_stream_yields_the_same_events() {
        // Whole frames, a fragmented message with a ping between its
        // fragments, and a larger frame: parsed straight from one push,
        // from a carried tail after a split at every byte boundary, and
        // byte by byte -- every route must agree.
        let mut stream = Vec::new();
        stream.extend(client_frame(OP_TEXT, b"hello", true));
        stream.extend(client_frame(OP_BINARY, &[1, 2, 3], false));
        stream.extend(client_frame(OP_PING, b"p", true));
        stream.extend(client_frame(OP_CONT, &[4, 5], true));
        stream.extend(client_frame(OP_BINARY, &[7u8; 300], true));
        let mut rx = WsRx::new(1 << 20);
        let mut whole = Vec::new();
        rx.push(&stream, &mut whole);
        assert_eq!(
            whole,
            vec![
                WsEvent::Text("hello".into()),
                WsEvent::Ping(b"p".to_vec()),
                WsEvent::Binary(vec![1, 2, 3, 4, 5]),
                WsEvent::Binary(vec![7u8; 300]),
            ]
        );
        for cut in 0..=stream.len() {
            let mut rx = WsRx::new(1 << 20);
            let mut out = Vec::new();
            rx.push(&stream[..cut], &mut out);
            rx.push(&stream[cut..], &mut out);
            assert_eq!(out, whole, "split at byte {cut}");
            assert!(rx.buf.is_empty());
        }
        let mut rx = WsRx::new(1 << 20);
        let mut out = Vec::new();
        for b in &stream {
            rx.push(std::slice::from_ref(b), &mut out);
        }
        assert_eq!(out, whole);
    }

    #[test]
    fn cursor_survives_a_frame_split_across_pushes() {
        // A frame that straddles a push boundary, interleaved with whole
        // frames, must reassemble correctly with the cursor in play.
        let mut rx = WsRx::new(1 << 20);
        let whole = client_frame(OP_TEXT, b"a", true);
        let split = client_frame(OP_BINARY, b"split-payload", true);
        let (head, tail) = split.split_at(3);
        let mut out = Vec::new();
        rx.push(&whole, &mut out);
        rx.push(head, &mut out); // incomplete: retained
        rx.push(tail, &mut out);
        rx.push(&whole, &mut out);
        assert_eq!(
            out,
            vec![
                WsEvent::Text("a".into()),
                WsEvent::Binary(b"split-payload".to_vec()),
                WsEvent::Text("a".into()),
            ]
        );
        assert!(rx.buf.is_empty());
    }

    #[test]
    fn partial_delivery_and_large_len() {
        let mut rx = WsRx::new(1 << 20);
        let mut out = Vec::new();
        let payload = vec![7u8; 70_000]; // 64-bit length header
        let f = client_frame(OP_BINARY, &payload, true);
        for chunk in f.chunks(1013) {
            rx.push(chunk, &mut out);
        }
        assert_eq!(out, vec![WsEvent::Binary(payload)]);
    }

    #[test]
    fn control_frames_and_close() {
        let mut rx = WsRx::new(1024);
        let mut out = Vec::new();
        rx.push(&client_frame(OP_PING, b"p", true), &mut out);
        let mut close_payload = 1000u16.to_be_bytes().to_vec();
        close_payload.extend_from_slice(b"bye");
        rx.push(&client_frame(OP_CLOSE, &close_payload, true), &mut out);
        assert_eq!(out, vec![WsEvent::Ping(b"p".to_vec()), WsEvent::Close(1000, "bye".into())]);
    }

    #[test]
    fn nothing_after_the_peers_close_is_parsed() {
        let mut close_payload = 1000u16.to_be_bytes().to_vec();
        close_payload.extend_from_slice(b"bye");
        // Data, a Close, then more data in the same read...
        let mut rx = WsRx::new(1024);
        let mut out = Vec::new();
        let mut read = client_frame(OP_TEXT, b"before", true);
        read.extend(client_frame(OP_CLOSE, &close_payload, true));
        read.extend(client_frame(OP_TEXT, b"after", true));
        read.extend(client_frame(OP_PING, b"p", true));
        rx.push(&read, &mut out);
        assert_eq!(out, vec![WsEvent::Text("before".into()), WsEvent::Close(1000, "bye".into())]);
        // ...and in later reads.
        out.clear();
        rx.push(&client_frame(OP_BINARY, b"later", true), &mut out);
        assert!(out.is_empty(), "frames after the Close were delivered: {out:?}");
    }

    #[test]
    fn protocol_violations() {
        // Unmasked client frame -> 1002.
        let mut rx = WsRx::new(1024);
        let mut out = Vec::new();
        rx.push(&frame(OP_TEXT, b"nope"), &mut out); // server-style unmasked
        assert_eq!(out, vec![WsEvent::Fail(1002, "client frame not masked")]);
        // Oversized -> 1009.
        let mut rx = WsRx::new(8);
        let mut out = Vec::new();
        rx.push(&client_frame(OP_BINARY, b"123456789", true), &mut out);
        assert_eq!(out, vec![WsEvent::Fail(1009, "message too big")]);
        // Invalid utf-8 text -> 1007.
        let mut rx = WsRx::new(1024);
        let mut out = Vec::new();
        rx.push(&client_frame(OP_TEXT, &[0xFF, 0xFE], true), &mut out);
        assert_eq!(out, vec![WsEvent::Fail(1007, "text message not utf-8")]);
    }

    #[test]
    fn close_reason_truncates_on_char_boundaries() {
        // 122 ASCII bytes + a 3-byte character: byte-slicing at 123 would
        // emit the first third of the character and invalidate the frame.
        let reason = format!("{}\u{20ac}", "a".repeat(122));
        let f = close_frame(1000, &reason);
        let payload = &f[2..]; // 2-byte header for a <126-byte payload
        assert!(payload.len() <= 125);
        std::str::from_utf8(&payload[2..]).expect("close reason must stay valid UTF-8");
        assert_eq!(&payload[2..], "a".repeat(122).as_bytes());

        // A reason that fits is untouched, multibyte and all.
        let short = "ok \u{20ac}";
        let f = close_frame(1000, short);
        assert_eq!(&f[4..], short.as_bytes());
    }

    #[test]
    fn rejects_malformed_lengths_and_close_payloads() {
        fn parse(frame: &[u8]) -> Vec<WsEvent> {
            let mut rx = WsRx::new(1 << 20);
            let mut out = Vec::new();
            rx.push(frame, &mut out);
            out
        }

        // Length 1 encoded with the 16-bit form is forbidden by section 5.2.
        let non_minimal = [0x81, 0xFE, 0, 1];
        assert_eq!(parse(&non_minimal), vec![WsEvent::Fail(1002, "non-minimal payload length")]);

        // The most significant bit of a 64-bit payload length must be zero.
        let oversized = [0x82, 0xFF, 0x80, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(parse(&oversized), vec![WsEvent::Fail(1002, "invalid payload length")]);

        // A close body cannot contain only half of its two-byte status code.
        assert_eq!(
            parse(&client_frame(OP_CLOSE, &[0x03], true)),
            vec![WsEvent::Fail(1002, "malformed close payload")]
        );

        for code in [999u16, 1005, 1015, 2000, 5000] {
            assert_eq!(
                parse(&client_frame(OP_CLOSE, &code.to_be_bytes(), true)),
                vec![WsEvent::Fail(1002, "invalid close code")]
            );
        }
    }

    // ---- permessage-deflate ------------------------------------------------

    /// A client frame with RSV1 set.
    fn deflated_client_frame(opcode: u8, payload: &[u8], fin: bool) -> Vec<u8> {
        let mut f = client_frame(opcode, payload, fin);
        f[0] |= 0x40;
        f
    }

    #[test]
    fn deflate_round_trips_every_size() {
        let mut d = Deflater::new();
        let mut i = Inflater::new();
        for n in [0usize, 1, 5, 125, 126, 1000, 65535, 65536, 300_000, 2_000_000] {
            // Compressible, incompressible, and one long run.
            let sample = b"{\"id\": 12345, \"name\": \"item\"}, ";
            let text: Vec<u8> = (0..n).map(|k| sample[k % sample.len()]).collect();
            let noise: Vec<u8> = (0..n).map(|k| (k.wrapping_mul(2654435761) >> 13) as u8).collect();
            let run = vec![7u8; n];
            for payload in [&text, &noise, &run] {
                let z = d.message(payload);
                assert!(!z.ends_with(&[0x00, 0x00, 0xff, 0xff]) || payload.is_empty(), "tail left on at {n}");
                assert_eq!(&i.message(&z, n.max(1)).unwrap(), payload, "size {n}");
            }
            if n >= 1000 {
                assert!(d.message(&text).len() < n / 3, "text of {n} bytes barely compressed");
            }
        }
    }

    #[test]
    fn deflate_matches_the_rfcs_examples() {
        // RFC 7692 7.2.3.1: "Hello", one compressed message.
        let hello = [0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
        let mut i = Inflater::new();
        assert_eq!(i.message(&hello, 100).unwrap(), b"Hello");
        // 7.2.3.4: the same with BFINAL set and an empty block behind it.
        let bfinal = [0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00];
        assert_eq!(i.message(&bfinal, 100).unwrap(), b"Hello");
        // 7.2.3.5: two DEFLATE blocks in one message.
        let two = [0xf2, 0x48, 0x05, 0x00, 0x00, 0x00, 0xff, 0xff, 0xca, 0xc9, 0xc9, 0x07, 0x00];
        assert_eq!(i.message(&two, 100).unwrap(), b"Hello");
        // 7.2.3.3: a block with no compression.
        let stored = [0x00, 0x05, 0x00, 0xfa, 0xff, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x00];
        assert_eq!(i.message(&stored, 100).unwrap(), b"Hello");
    }

    #[test]
    fn inflate_stops_at_the_limit_and_rejects_garbage() {
        let mut d = Deflater::new();
        let mut i = Inflater::new();
        // A megabyte of zeros is about a kilobyte compressed: the limit is
        // on what comes out.
        let bomb = d.message(&vec![0u8; 1 << 20]);
        assert!(bomb.len() < 8192, "{}", bomb.len());
        assert_eq!(i.message(&bomb, 1 << 20).unwrap().len(), 1 << 20);
        assert_eq!(i.message(&bomb, (1 << 20) - 1), Err((1009, "message too big")));
        assert_eq!(i.message(&bomb, 1000), Err((1009, "message too big")));
        // And the decompressor is usable again afterwards.
        assert_eq!(i.message(&d.message(b"after"), 100).unwrap(), b"after");
        // Not DEFLATE at all (a reserved block type).
        assert_eq!(i.message(&[0x07, 0x00, 0x00], 100), Err((1007, "invalid compressed data")));
        assert_eq!(i.message(&d.message(b"still fine"), 100).unwrap(), b"still fine");
    }

    #[test]
    fn rsv1_is_a_protocol_error_until_negotiated() {
        let mut rx = WsRx::new(1 << 20);
        let mut out = Vec::new();
        rx.push(&deflated_client_frame(OP_TEXT, b"x", true), &mut out);
        assert_eq!(out, vec![WsEvent::Fail(1002, "reserved bits set")]);
    }

    #[test]
    fn rsv1_marks_whole_messages_and_nothing_else() {
        let mut d = Deflater::new();
        let z = d.message(b"compressed text");
        let mut rx = WsRx::new(1 << 20);
        rx.allow_deflate();
        let mut out = Vec::new();
        // One frame; then fragmented, RSV1 on the first frame only; then
        // an uncompressed message on the same connection.
        rx.push(&deflated_client_frame(OP_TEXT, &z, true), &mut out);
        rx.push(&deflated_client_frame(OP_BINARY, &z[..3], false), &mut out);
        rx.push(&client_frame(OP_PING, b"p", true), &mut out); // control frames interleave
        rx.push(&client_frame(OP_CONT, &z[3..], true), &mut out);
        rx.push(&client_frame(OP_TEXT, b"plain", true), &mut out);
        assert_eq!(
            out,
            vec![
                WsEvent::Deflated { text: true, payload: z.clone() },
                WsEvent::Ping(b"p".to_vec()),
                WsEvent::Deflated { text: false, payload: z.clone() },
                WsEvent::Text("plain".into()),
            ]
        );
        // RSV1 on a continuation, on a control frame, or RSV2: errors.
        for bad in [
            {
                let mut v = client_frame(OP_TEXT, b"a", false);
                v.extend(deflated_client_frame(OP_CONT, b"b", true));
                v
            },
            deflated_client_frame(OP_PING, b"", true),
            {
                let mut v = client_frame(OP_TEXT, b"a", true);
                v[0] |= 0x20;
                v
            },
        ] {
            let mut rx = WsRx::new(1 << 20);
            rx.allow_deflate();
            let mut out = Vec::new();
            rx.push(&bad, &mut out);
            assert_eq!(out.last(), Some(&WsEvent::Fail(1002, "reserved bits set")), "{bad:?}");
        }
    }

    #[test]
    fn a_compressed_server_frame_carries_rsv1() {
        let f = frame_deflated(OP_TEXT, &[1, 2, 3]);
        assert_eq!(f, vec![0x80 | 0x40 | OP_TEXT, 3, 1, 2, 3]);
        let big = frame_deflated(OP_BINARY, &vec![9u8; 70_000]);
        assert_eq!(&big[..2], &[0x80 | 0x40 | OP_BINARY, 127]);
        assert_eq!(big.len(), 70_000 + 10);
    }

    #[test]
    fn negotiation() {
        fn offer(v: &str) -> Option<String> {
            negotiate_deflate(std::iter::once(v.as_bytes()))
        }
        let base = "permessage-deflate; server_no_context_takeover; client_no_context_takeover";
        // What browsers and the websockets library send.
        assert_eq!(offer("permessage-deflate; client_max_window_bits").as_deref(), Some(base));
        assert_eq!(offer("permessage-deflate").as_deref(), Some(base));
        assert_eq!(offer("PerMessage-Deflate ;  client_max_window_bits=12 ").as_deref(), Some(base));
        assert_eq!(
            offer("permessage-deflate; server_no_context_takeover; client_no_context_takeover").as_deref(),
            Some(base)
        );
        // The full window may be asked for by name, and is echoed.
        let echoed = format!("{base}; server_max_window_bits=15");
        assert_eq!(offer("permessage-deflate; server_max_window_bits=15"), Some(echoed.clone()));
        assert_eq!(offer("permessage-deflate; server_max_window_bits=\"15\""), Some(echoed));
        // A smaller server window cannot be honoured: that offer is
        // passed over, and a later one that can be is taken.
        assert_eq!(offer("permessage-deflate; server_max_window_bits=10"), None);
        assert_eq!(
            offer(
                "permessage-deflate; server_max_window_bits=10, permessage-deflate; client_max_window_bits"
            )
            .as_deref(),
            Some(base)
        );
        // Unknown extensions and parameters, repeats, bad values.
        assert_eq!(offer("x-webkit-deflate-frame"), None);
        assert_eq!(offer("permessage-deflate; mystery"), None);
        assert_eq!(offer("permessage-deflate; client_max_window_bits; client_max_window_bits"), None);
        assert_eq!(offer("permessage-deflate; client_max_window_bits=7"), None);
        assert_eq!(offer("permessage-deflate; client_max_window_bits=08"), None);
        assert_eq!(offer("permessage-deflate; server_no_context_takeover=1"), None);
        assert_eq!(offer("permessage-deflate; server_max_window_bits"), None);
        assert_eq!(offer(""), None);
        // Offers may be spread over several header lines.
        let lines = [&b"foo"[..], &b"permessage-deflate"[..]];
        assert_eq!(negotiate_deflate(lines.into_iter()).as_deref(), Some(base));
    }

    /// Properties, over generated input: what the fixed cases above pin
    /// down at chosen points, these hold everywhere the generator reaches.
    mod props {
        use super::*;
        use proptest::collection::vec;
        use proptest::prelude::*;

        const BASE: &str = "permessage-deflate; server_no_context_takeover; client_no_context_takeover";

        /// A frame with every header bit in the caller's hands.
        fn raw_frame(first: u8, masked: bool, mask: [u8; 4], payload: &[u8]) -> Vec<u8> {
            let n = payload.len();
            let m = if masked { 0x80 } else { 0 };
            let mut out = vec![first];
            if n < 126 {
                out.push(m | n as u8);
            } else if n <= 0xFFFF {
                out.push(m | 126);
                out.extend_from_slice(&(n as u16).to_be_bytes());
            } else {
                out.push(m | 127);
                out.extend_from_slice(&(n as u64).to_be_bytes());
            }
            if masked {
                out.extend_from_slice(&mask);
                out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i & 3]));
            } else {
                out.extend_from_slice(payload);
            }
            out
        }

        fn payload() -> impl Strategy<Value = Vec<u8>> {
            prop_oneof![
                4 => "[ -~]{0,40}".prop_map(String::into_bytes),
                2 => "\\PC{0,40}".prop_map(String::into_bytes),
                3 => vec(any::<u8>(), 0..300),
                // Either side of where the length field goes from 16 to 64 bits.
                1 => vec(any::<u8>(), 65_000..66_000),
            ]
        }

        /// One data message in one to four frames, sometimes with a ping
        /// between the fragments (which a peer may send). Frame by frame,
        /// so the test knows where each starts.
        fn message() -> impl Strategy<Value = Vec<Vec<u8>>> {
            (
                any::<bool>(),
                prop::bool::weighted(0.25),
                prop::bool::weighted(0.8),
                payload(),
                vec(any::<prop::sample::Index>(), 0..3),
                any::<[u8; 4]>(),
                prop::bool::weighted(0.3),
            )
                .prop_map(|(text, rsv1, valid, p, cuts, mask, ping)| {
                    // Most text messages are text; the rest are whatever bytes came.
                    let p =
                        if text && valid { String::from_utf8_lossy(&p).into_owned().into_bytes() } else { p };
                    let mut ends: Vec<usize> = cuts.iter().map(|c| c.index(p.len() + 1)).collect();
                    ends.sort_unstable();
                    ends.push(p.len());
                    let last = ends.len() - 1;
                    let mut out = Vec::new();
                    let mut start = 0;
                    for (i, end) in ends.into_iter().enumerate() {
                        let opcode = match (i, text) {
                            (0, true) => OP_TEXT,
                            (0, false) => OP_BINARY,
                            _ => OP_CONT,
                        };
                        let bits =
                            if i == last { 0x80 } else { 0 } | if i == 0 && rsv1 { RSV1 } else { 0 } | opcode;
                        out.push(raw_frame(bits, true, mask, &p[start..end]));
                        if ping && i != last {
                            out.push(raw_frame(0x80 | OP_PING, true, mask, b"still here"));
                        }
                        start = end;
                    }
                    out
                })
        }

        /// A piece of a client's stream: mostly whole messages, now and
        /// then a control frame, a close, a frame with arbitrary header
        /// bits, or bytes that are no frame at all.
        fn piece() -> impl Strategy<Value = Vec<Vec<u8>>> {
            let control = (
                prop_oneof![Just(OP_PING), Just(OP_PONG)],
                prop::bool::weighted(0.95),
                vec(any::<u8>(), 0..130),
                any::<[u8; 4]>(),
            )
                .prop_map(|(op, fin, p, mask)| raw_frame(if fin { 0x80 } else { 0 } | op, true, mask, &p));
            let close = (
                proptest::option::of(prop_oneof![
                    Just(1000u16),
                    Just(1001),
                    Just(1005),
                    Just(3000),
                    Just(4999),
                    any::<u16>()
                ]),
                payload(),
                any::<[u8; 4]>(),
            )
                .prop_map(|(code, reason, mask)| {
                    let mut p = Vec::new();
                    if let Some(code) = code {
                        p.extend_from_slice(&code.to_be_bytes());
                        p.extend_from_slice(&reason[..reason.len().min(60)]);
                    }
                    raw_frame(0x80 | OP_CLOSE, true, mask, &p)
                });
            let odd = (any::<u8>(), any::<bool>(), vec(any::<u8>(), 0..200), any::<[u8; 4]>())
                .prop_map(|(first, masked, p, mask)| raw_frame(first, masked, mask, &p));
            prop_oneof![
                12 => message(),
                3 => control.prop_map(|frame| vec![frame]),
                1 => close.prop_map(|frame| vec![frame]),
                1 => odd.prop_map(|frame| vec![frame]),
                1 => vec(any::<u8>(), 0..24).prop_map(|bytes| vec![bytes]),
            ]
        }

        fn events(wire: &[u8], cuts: &[usize], max: usize, deflate: bool) -> Vec<WsEvent> {
            let mut rx = WsRx::new(max);
            if deflate {
                rx.allow_deflate();
            }
            let mut out = Vec::new();
            let mut at = 0;
            for &cut in cuts {
                rx.push(&wire[at..cut], &mut out);
                at = cut;
            }
            rx.push(&wire[at..], &mut out);
            out
        }

        fn sorted_cuts(cuts: &[prop::sample::Index], len: usize) -> Vec<usize> {
            let mut cuts: Vec<usize> = cuts.iter().map(|c| c.index(len + 1)).collect();
            cuts.sort_unstable();
            cuts
        }

        /// Bytes that compress in every way a message might: not at all,
        /// by repetition, as text, and a thousandfold.
        fn body() -> impl Strategy<Value = Vec<u8>> {
            prop_oneof![
                3 => vec(any::<u8>(), 0..2000),
                3 => (vec(any::<u8>(), 1..40), 1usize..400).prop_map(|(unit, n)| unit.repeat(n)),
                3 => "[a-z\" :,{}0-9]{0,3000}".prop_map(String::into_bytes),
                1 => (any::<u8>(), 0usize..200_000).prop_map(|(b, n)| vec![b; n]),
            ]
        }

        proptest! {
            /// What a stream means does not depend on how the network cut
            /// it up: one read, any reads, a read ending at every byte of
            /// every frame's header, or a byte at a time give the same
            /// events. (A read boundary inside a length field or a mask is
            /// where a streaming parser goes wrong.)
            #[test]
            fn a_stream_reads_the_same_however_it_is_cut(
                pieces in vec(piece(), 0..8),
                cuts in vec(any::<prop::sample::Index>(), 0..12),
                max in prop_oneof![3 => Just(1usize << 20), 1 => 0usize..400],
                deflate in any::<bool>(),
            ) {
                let frames: Vec<Vec<u8>> = pieces.into_iter().flatten().collect();
                let wire = frames.concat();
                let whole = events(&wire, &[], max, deflate);
                let cut = events(&wire, &sorted_cuts(&cuts, wire.len()), max, deflate);
                prop_assert_eq!(&whole, &cut);
                // The longest header is 14 bytes: 2, an 8-byte length, the mask.
                let mut in_headers = Vec::new();
                let mut start = 0;
                for frame in &frames {
                    in_headers.extend((start..start + frame.len().min(16)).filter(|&at| at < wire.len()));
                    start += frame.len();
                }
                prop_assert_eq!(&whole, &events(&wire, &in_headers, max, deflate));
                if wire.len() <= 4096 {
                    let each: Vec<usize> = (1..wire.len()).collect();
                    prop_assert_eq!(&whole, &events(&wire, &each, max, deflate));
                }
                // The stream ends at a close or a failure, and only there.
                for (i, ev) in whole.iter().enumerate() {
                    let ends = matches!(ev, WsEvent::Fail(..) | WsEvent::Close(..));
                    prop_assert!(!ends || i == whole.len() - 1, "event after the end: {:?}", whole);
                    match ev {
                        WsEvent::Text(s) => prop_assert!(s.len() <= max),
                        WsEvent::Binary(b) | WsEvent::Ping(b) => prop_assert!(b.len() <= max),
                        WsEvent::Deflated { payload, .. } => {
                            prop_assert!(deflate, "a compressed message nobody negotiated");
                            prop_assert!(payload.len() <= max);
                        }
                        WsEvent::Close(code, _) => prop_assert!(*code == 1005 || valid_close_code(*code)),
                        WsEvent::Fail(code, _) => prop_assert!(matches!(code, 1002 | 1007 | 1009)),
                        WsEvent::Pong => {}
                    }
                }
            }

            /// Whatever is compressed comes back, the size limit is exact,
            /// and one compressor and one decompressor serve message after
            /// message (nothing carries over between them).
            #[test]
            fn what_is_deflated_inflates_to_itself(first in body(), second in body()) {
                let mut deflater = Deflater::new();
                let mut inflater = Inflater::new();
                for message in [&first, &second, &first] {
                    let packed = deflater.message(message);
                    let unpacked = inflater.message(&packed, message.len());
                    prop_assert_eq!(unpacked.as_ref(), Ok(message));
                    if !message.is_empty() {
                        prop_assert_eq!(
                            inflater.message(&packed, message.len() - 1),
                            Err((1009, "message too big"))
                        );
                    }
                }
            }

            /// A compressed payload is the peer's to write. Nothing it can
            /// hold makes the decompressor panic, spin, or hand back more
            /// than the limit -- and the decompressor is fit for the next
            /// message afterwards.
            #[test]
            fn inflating_anything_is_bounded(
                data in vec(any::<u8>(), 0..2000),
                max in 0usize..5000,
            ) {
                let mut inflater = Inflater::new();
                match inflater.message(&data, max) {
                    Ok(out) => prop_assert!(out.len() <= max),
                    Err((code, _)) => prop_assert!(code == 1007 || code == 1009),
                }
                let packed = Deflater::new().message(b"the next message");
                prop_assert_eq!(inflater.message(&packed, 16), Ok(b"the next message".to_vec()));
            }

            /// The same for a real message that was damaged on the way:
            /// bytes changed, and cut short.
            #[test]
            fn inflating_a_damaged_message_is_bounded(
                message in body(),
                damage in vec((any::<prop::sample::Index>(), any::<u8>()), 0..4),
                keep in any::<prop::sample::Index>(),
                max in prop_oneof![Just(1usize << 20), 0usize..3000],
            ) {
                let mut packed = Deflater::new().message(&message);
                if !packed.is_empty() {
                    for (at, byte) in &damage {
                        let at = at.index(packed.len());
                        packed[at] = *byte;
                    }
                }
                packed.truncate(keep.index(packed.len() + 1).max(packed.len() / 2));
                let mut inflater = Inflater::new();
                match inflater.message(&packed, max) {
                    Ok(out) => prop_assert!(out.len() <= max),
                    Err((code, _)) => prop_assert!(code == 1007 || code == 1009),
                }
            }

            /// The extension header is the client's to write, too: any
            /// bytes are answered with nothing or with one of the two
            /// responses there are, and a window size is only echoed when
            /// the client named one.
            #[test]
            fn negotiation_answers_anything_safely(
                values in vec(prop_oneof![
                    vec(any::<u8>(), 0..60),
                    vec(prop::sample::select(vec![
                        "permessage-deflate", "PERMESSAGE-DEFLATE", ";", ",", "=", " ", "\t", "\"",
                        "client_max_window_bits", "server_max_window_bits", "client_no_context_takeover",
                        "server_no_context_takeover", "15", "8", "7", "16", "015", "x", "",
                    ]), 0..14).prop_map(|words| words.concat().into_bytes()),
                ], 0..4),
            ) {
                let answer = negotiate_deflate(values.iter().map(Vec::as_slice));
                if let Some(answer) = answer {
                    let echoed = format!("{BASE}; server_max_window_bits=15");
                    prop_assert!(answer == BASE || answer == echoed, "{}", answer);
                    let named = values.iter().any(|v| {
                        v.to_ascii_lowercase().windows(22).any(|w| w == b"server_max_window_bits")
                    });
                    prop_assert!(answer == BASE || named);
                }
            }

            /// Every offer the documentation says is honoured, is: the
            /// legal parameters in any order, any letter case, any legal
            /// spacing, with or without quotes around a value.
            #[test]
            fn every_legal_offer_is_taken(
                order in Just(vec![0usize, 1, 2, 3]).prop_shuffle(),
                take in vec(any::<bool>(), 4),
                client_bits in proptest::option::of(8u8..=15),
                quoted in any::<bool>(),
                upper in any::<bool>(),
                pad in "[ \t]{0,3}",
                before in prop::sample::select(vec!["", "x-webkit-deflate-frame, ", "permessage-deflate; mystery=1,"]),
            ) {
                let mut offer = format!("{before}{pad}permessage-deflate{pad}");
                let mut wants_echo = false;
                for which in order {
                    if !take[which] {
                        continue;
                    }
                    let param = match which {
                        0 => "server_no_context_takeover".to_string(),
                        1 => "client_no_context_takeover".to_string(),
                        2 => match client_bits {
                            Some(bits) if quoted => format!("client_max_window_bits=\"{bits}\""),
                            Some(bits) => format!("client_max_window_bits{pad}={pad}{bits}"),
                            None => "client_max_window_bits".to_string(),
                        },
                        _ => {
                            wants_echo = true;
                            "server_max_window_bits=15".to_string()
                        }
                    };
                    offer.push_str(&format!(";{pad}{param}{pad}"));
                }
                let offer = if upper { offer.to_ascii_uppercase() } else { offer };
                let answer = negotiate_deflate(std::iter::once(offer.as_bytes()));
                let expected = if wants_echo { format!("{BASE}; server_max_window_bits=15") } else { BASE.to_string() };
                prop_assert_eq!(answer, Some(expected), "offer {:?}", offer);
            }
        }
    }
}
