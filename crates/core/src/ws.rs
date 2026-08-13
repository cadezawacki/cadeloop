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
    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
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
            let tmp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
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
pub fn close_frame(code: u16, reason: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(2 + reason.len().min(123));
    payload.extend_from_slice(&code.to_be_bytes());
    payload.extend_from_slice(&reason.as_bytes()[..reason.len().min(123)]);
    frame(OP_CLOSE, &payload)
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
    frag_op: Option<u8>,
    frag: Vec<u8>,
    pub max_message: usize,
    failed: bool,
}

impl WsRx {
    pub fn new(max_message: usize) -> Self {
        WsRx { buf: Vec::new(), frag_op: None, frag: Vec::new(), max_message, failed: false }
    }

    pub fn push(&mut self, data: &[u8], out: &mut Vec<WsEvent>) {
        if self.failed {
            return;
        }
        self.buf.extend_from_slice(data);
        loop {
            match self.parse_one() {
                ParseOne::NeedMore => return,
                ParseOne::Event(ev) => {
                    let fail = matches!(ev, WsEvent::Fail(..));
                    out.push(ev);
                    if fail {
                        self.failed = true;
                        return;
                    }
                }
                ParseOne::Continue => {}
            }
        }
    }

    fn parse_one(&mut self) -> ParseOne {
        let b = &self.buf;
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
        } else if len == 127 {
            if b.len() < off + 8 {
                return ParseOne::NeedMore;
            }
            len = u64::from_be_bytes(b[off..off + 8].try_into().unwrap());
            off += 8;
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
        self.buf.drain(..off + len);

        match opcode {
            OP_PING => ParseOne::Event(WsEvent::Ping(payload)),
            OP_PONG => ParseOne::Event(WsEvent::Pong),
            OP_CLOSE => {
                let (code, reason) = if payload.len() >= 2 {
                    let code = u16::from_be_bytes([payload[0], payload[1]]);
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
}
