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
    let n = payload.len();
    let mut out = Vec::with_capacity(n + 10);
    out.push(0x80 | (opcode & 0x0F));
    if n < 126 {
        out.push(n as u8);
    } else if n <= 0xFFFF {
        out.push(126);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(127);
        out.extend_from_slice(&(n as u64).to_be_bytes());
    }
    out.extend_from_slice(payload);
    out
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

/// One inbound event, message-assembled.
#[derive(Debug, PartialEq)]
pub enum WsEvent {
    Text(String),
    Binary(Vec<u8>),
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
    buf: Vec<u8>,
    /// Bytes of `buf` already parsed into events. Compacted away once per
    /// `push`, not per frame (see `push`).
    pos: usize,
    frag_op: Option<u8>,
    frag: Vec<u8>,
    pub max_message: usize,
    /// The peer's Close frame, or a protocol failure, ended the stream:
    /// nothing after it is parsed.
    ended: bool,
}

impl WsRx {
    pub fn new(max_message: usize) -> Self {
        WsRx { buf: Vec::new(), pos: 0, frag_op: None, frag: Vec::new(), max_message, ended: false }
    }

    pub fn push(&mut self, data: &[u8], out: &mut Vec<WsEvent>) {
        if self.ended {
            return;
        }
        self.buf.extend_from_slice(data);
        loop {
            match self.parse_one() {
                ParseOne::NeedMore => break,
                ParseOne::Event(ev) => {
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
                        self.pos = 0;
                        return;
                    }
                }
                ParseOne::Continue => {}
            }
        }
        // Compact ONCE per push: drop the prefix consumed this call so
        // `buf` holds only the unparsed tail. `parse_one` advances a cursor
        // instead of draining per frame -- draining per frame was
        // O(remaining) each, i.e. O(n^2) for a buffer full of small frames.
        // A single 64 KiB recv of ~10,900 empty frames cost ~115 ms of
        // in-cell parse CPU (vs ~0.36 ms fed one frame at a time), stalling
        // the whole worker; the cursor makes it linear.
        if self.pos != 0 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
    }

    fn parse_one(&mut self) -> ParseOne {
        let b = &self.buf[self.pos..];
        if b.len() < 2 {
            return ParseOne::NeedMore;
        }
        let fin = b[0] & 0x80 != 0;
        if b[0] & 0x70 != 0 {
            // RSV bits without a negotiated extension (we negotiate none).
            return ParseOne::Event(WsEvent::Fail(1002, "reserved bits set"));
        }
        let opcode = b[0] & 0x0F;
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
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[i & 3];
        }
        self.pos += off + len;

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
                    self.finish_message(opcode, payload)
                } else {
                    self.frag_op = Some(opcode);
                    self.frag = payload;
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
                    self.finish_message(op, msg)
                } else {
                    ParseOne::Continue
                }
            }
            _ => ParseOne::Event(WsEvent::Fail(1002, "unknown opcode")),
        }
    }

    fn finish_message(&mut self, opcode: u8, payload: Vec<u8>) -> ParseOne {
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
        // Fully consumed: no bytes retained, cursor reset.
        assert!(rx.buf.is_empty());
        assert_eq!(rx.pos, 0);
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
}
