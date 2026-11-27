//! HTTP/1.1 request parsing over vendored llhttp (R-080).
//!
//! Pure Rust + C — zero Python objects until dispatch (the pyshim layer
//! builds the ASGI scope from [`Request`] parts). Strict mode (all lenient
//! flags off — llhttp's default). Limits per R-080 are enforced inside the
//! callbacks; violations surface as [`ParseError`] with the right 4xx
//! status.
//!
//! Chunked transfer decoding is llhttp's job: `on_body` receives decoded
//! bytes. Keep-alive/pipelining (R-085): one `execute` call parses every
//! complete message in the buffer; completed requests queue in order.

use std::collections::VecDeque;
use std::ffi::c_void;
use std::os::raw::{c_char, c_int};

// ---- llhttp FFI (layout mirrors vendor/llhttp/llhttp.h exactly) --------

#[repr(C)]
pub struct Llhttp {
    _index: i32,
    _span_pos0: *mut c_void,
    _span_cb0: *mut c_void,
    error: i32,
    reason: *const c_char,
    error_pos: *const c_char,
    data: *mut c_void,
    _current: *mut c_void,
    content_length: u64,
    type_: u8,
    method: u8,
    http_major: u8,
    http_minor: u8,
    header_state: u8,
    lenient_flags: u16,
    upgrade: u8,
    finish: u8,
    flags: u16,
    status_code: u16,
    initial_message_completed: u8,
    settings: *mut c_void,
}

type Cb = Option<unsafe extern "C" fn(*mut Llhttp) -> c_int>;
type DataCb = Option<unsafe extern "C" fn(*mut Llhttp, *const c_char, usize) -> c_int>;

#[repr(C)]
#[derive(Default)]
pub struct Settings {
    on_message_begin: Cb,
    on_url: DataCb,
    on_status: DataCb,
    on_method: DataCb,
    on_version: DataCb,
    on_header_field: DataCb,
    on_header_value: DataCb,
    on_chunk_extension_name: DataCb,
    on_chunk_extension_value: DataCb,
    on_headers_complete: Cb,
    on_body: DataCb,
    on_message_complete: Cb,
    on_url_complete: Cb,
    on_status_complete: Cb,
    on_method_complete: Cb,
    on_version_complete: Cb,
    on_header_field_complete: Cb,
    on_header_value_complete: Cb,
    on_chunk_extension_name_complete: Cb,
    on_chunk_extension_value_complete: Cb,
    on_chunk_header: Cb,
    on_chunk_complete: Cb,
    on_reset: Cb,
}

extern "C" {
    fn llhttp_init(parser: *mut Llhttp, kind: c_int, settings: *const Settings);
    fn llhttp_execute(parser: *mut Llhttp, data: *const c_char, len: usize) -> c_int;
    fn llhttp_should_keep_alive(parser: *const Llhttp) -> c_int;
    fn llhttp_method_name(method: c_int) -> *const c_char;
    fn llhttp_errno_name(err: c_int) -> *const c_char;
}

const HTTP_REQUEST: c_int = 1;
const HPE_OK: c_int = 0;
const HPE_PAUSED_UPGRADE: c_int = 22;
/// Our limit violations (callbacks return -1 == HPE_USER... actually
/// llhttp maps nonzero callback returns to HPE_USER/HPE_CB_*).
const _HPE_USER: c_int = 24;

// ---- limits (R-080 defaults; configurable via serve cfg) ---------------

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_header_bytes: usize,
    pub max_headers: usize,
    pub max_url: usize,
    pub max_body: Option<usize>,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_header_bytes: 64 * 1024,
            max_headers: 100,
            max_url: 8 * 1024,
            // Finite BY DEFAULT (R-086). The engine buffers a whole body
            // before dispatching the request, so an unlimited default let
            // any unauthenticated client turn one request into unbounded
            // resident memory. 16 MiB covers ordinary JSON, form and
            // multipart payloads while bounding the worst case; a larger
            // limit (or None for unlimited) is an explicit opt-in, and
            // exceeding it answers 413 rather than closing silently.
            max_body: Some(16 * 1024 * 1024),
        }
    }
}

/// One fully parsed request, ready for ASGI scope construction.
#[derive(Debug)]
pub struct Request {
    pub method: &'static str,
    pub http_minor: u8,
    pub keep_alive: bool,
    /// llhttp saw `Connection: upgrade` + `Upgrade:` on this request
    /// (R-087: the engine decides whether to take the WebSocket path).
    pub upgrade: bool,
    /// Raw request-target bytes (R-081 raw_path; percent-decoding is the
    /// scope builder's job).
    pub url: Vec<u8>,
    /// Names already lower-cased (R-081).
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

impl Request {
    /// Bytes this parsed request retains, for the pipeline queue budget.
    /// Approximate by design: the point is bounding memory, not exact
    /// accounting.
    pub fn queued_size(&self) -> usize {
        self.url.len()
            + self.body.len()
            + self.headers.iter().map(|(n, v)| n.len() + v.len() + 4).sum::<usize>()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseError {
    /// Suggested response status (400 or 431/413).
    pub status: u16,
    pub reason: &'static str,
}

struct Acc {
    limits: Limits,
    url: Vec<u8>,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    /// How many headers the connection's previous request had: what the
    /// list is sized for up front. A client repeats its header set, and a
    /// list grown from nothing was reallocated (and its entries moved)
    /// three or four times on the way to a browser's eighteen.
    last_headers: usize,
    field: Vec<u8>,
    value: Vec<u8>,
    in_value: bool,
    header_bytes: usize,
    body: Vec<u8>,
    completed: VecDeque<Request>,
    error: Option<ParseError>,
    /// A request head (request line + headers) is mid-receipt — drives
    /// the R-080 request-line/header timeout, which anchors at head
    /// start so drip-fed bytes (slowloris) cannot extend it. Body
    /// receipt is NOT head time (a slow upload falls under keep-alive
    /// idle policy instead).
    in_head: bool,
    /// The head just parsed carried `Expect: 100-continue` and the caller
    /// still owes the client an interim response (RFC 7231 5.1.1).
    owes_continue: bool,
    /// A request that expects `100 Continue` is still waiting for its
    /// body: an interim response sent now can still matter. Once the
    /// message completes it cannot (RFC 7231 5.1.1 lets a server omit it
    /// then), and one written after the final response would be read as
    /// the NEXT request's.
    expecting_body: bool,
}

impl Acc {
    fn fail(&mut self, status: u16, reason: &'static str) -> c_int {
        self.error = Some(ParseError { status, reason });
        -1
    }

    /// `Content-Length` as declared by the head, if it parses. llhttp has
    /// already rejected conflicting or malformed ones by this point.
    fn declared_content_length(&self) -> Option<u64> {
        let (_, v) = self.headers.iter().find(|(n, _)| n.as_slice() == b"content-length")?;
        std::str::from_utf8(v).ok()?.trim().parse::<u64>().ok()
    }

    /// `Expect: 100-continue`. The field is a comma-separated list of
    /// expectations and the token is case-insensitive (RFC 7231 5.1.1);
    /// any other expectation is one we do not understand, and the spec's
    /// answer to that is 417 -- which we do not send, because refusing an
    /// otherwise valid request over an unknown expectation is worse for
    /// interoperability than ignoring it, and every mainstream server
    /// ignores it too.
    fn expects_continue(&self) -> bool {
        self.headers.iter().filter(|(n, _)| n.as_slice() == b"expect").any(|(_, v)| {
            v.split(|&b| b == b',').any(|tok| tok.trim_ascii().eq_ignore_ascii_case(b"100-continue"))
        })
    }

    fn commit_header(&mut self) {
        if !self.in_value {
            return;
        }
        self.in_value = false;
        let mut field = std::mem::take(&mut self.field);
        let value = std::mem::take(&mut self.value);
        // llhttp drives these same callbacks for the TRAILER section of a
        // chunked request, and `on_headers_complete` -- with it the
        // one-Host check -- has already run by then. Committing a trailer
        // as an ordinary header therefore let a client smuggle a second
        // `Host:` (verified reaching scope["headers"] as `host=attacker`
        // past a legitimate `Host: legit`), or an `Authorization:`, into
        // the very fields routing and auth are decided from. ASGI has no
        // request-trailer mechanism to deliver them through, so they are
        // dropped rather than filtered: there is no set of names for
        // which merging them here is correct.
        if !self.in_head {
            return;
        }
        field.make_ascii_lowercase();
        self.headers.push((field, value));
    }
}

unsafe fn acc<'a>(p: *mut Llhttp) -> &'a mut Acc {
    unsafe { &mut *((*p).data.cast::<Acc>()) }
}

unsafe extern "C" fn on_url(p: *mut Llhttp, at: *const c_char, len: usize) -> c_int {
    let a = unsafe { acc(p) };
    if a.url.len() + len > a.limits.max_url {
        return a.fail(414, "request target too long");
    }
    a.url.extend_from_slice(unsafe { std::slice::from_raw_parts(at.cast::<u8>(), len) });
    0
}

unsafe extern "C" fn on_header_field(p: *mut Llhttp, at: *const c_char, len: usize) -> c_int {
    let a = unsafe { acc(p) };
    a.commit_header();
    a.header_bytes += len;
    if a.header_bytes > a.limits.max_header_bytes {
        return a.fail(431, "headers too large");
    }
    // A limit on what is kept: a trailer field is read, measured against
    // the byte limit above, and dropped (see `commit_header`), so it is not
    // one more header. Counting it here refused a request whose head was
    // exactly full for carrying a trailer, and no other.
    if a.in_head && a.headers.len() >= a.limits.max_headers {
        return a.fail(431, "too many headers");
    }
    push_piece(&mut a.field, unsafe { std::slice::from_raw_parts(at.cast::<u8>(), len) });
    0
}

/// Append a piece of a header's name or value. llhttp nearly always
/// delivers each whole, so the first piece is given exactly its size --
/// one allocation of the right length, instead of the generic growth path
/// starting from nothing.
#[inline]
fn push_piece(buf: &mut Vec<u8>, piece: &[u8]) {
    if buf.capacity() == 0 {
        *buf = Vec::with_capacity(piece.len());
    }
    buf.extend_from_slice(piece);
}

unsafe extern "C" fn on_header_value(p: *mut Llhttp, at: *const c_char, len: usize) -> c_int {
    let a = unsafe { acc(p) };
    a.in_value = true;
    a.header_bytes += len;
    if a.header_bytes > a.limits.max_header_bytes {
        return a.fail(431, "headers too large");
    }
    push_piece(&mut a.value, unsafe { std::slice::from_raw_parts(at.cast::<u8>(), len) });
    0
}

unsafe extern "C" fn on_message_begin(p: *mut Llhttp) -> c_int {
    let a = unsafe { acc(p) };
    a.in_head = true;
    if a.headers.capacity() == 0 {
        a.headers.reserve_exact(a.last_headers.clamp(8, 64));
    }
    0
}

unsafe extern "C" fn on_headers_complete(p: *mut Llhttp) -> c_int {
    let minor = unsafe { (*p).http_minor };
    let a = unsafe { acc(p) };
    a.commit_header();
    a.in_head = false;
    // RFC 7230 5.4: an HTTP/1.1 request MUST carry exactly one Host.
    // llhttp validates syntax only, so without this a request with none
    // (or two disagreeing ones) reaches the application and the authority
    // it routes on can differ from what an intermediary saw.
    if minor >= 1 {
        let hosts = a.headers.iter().filter(|(n, _)| n.as_slice() == b"host").count();
        if hosts != 1 {
            return a.fail(400, "HTTP/1.1 requires exactly one Host header");
        }
    }
    // RFC 7230 3.3.2 / 7231 5.1.1. A declared length over the cap is
    // known here, before a byte of the body has been read: refusing now
    // costs one response instead of buffering an upload we have already
    // decided to reject -- and for an `Expect: 100-continue` client it is
    // the whole point of the expectation, since the client has not sent
    // the body yet and never will.
    if let Some(cap) = a.limits.max_body {
        if let Some(declared) = a.declared_content_length() {
            if declared > cap as u64 {
                return a.fail(413, "body too large");
            }
        }
    }
    // Otherwise the expectation is honored: the caller writes the
    // interim response, because it owns the wire.
    if minor >= 1 && a.expects_continue() {
        a.owes_continue = true;
        a.expecting_body = true;
    }
    0
}

unsafe extern "C" fn on_body(p: *mut Llhttp, at: *const c_char, len: usize) -> c_int {
    let a = unsafe { acc(p) };
    if let Some(cap) = a.limits.max_body {
        if a.body.len() + len > cap {
            return a.fail(413, "body too large");
        }
    }
    a.body.extend_from_slice(unsafe { std::slice::from_raw_parts(at.cast::<u8>(), len) });
    0
}

unsafe extern "C" fn on_message_complete(p: *mut Llhttp) -> c_int {
    let keep_alive = unsafe { llhttp_should_keep_alive(p) } != 0;
    let upgrade = unsafe { (*p).upgrade } != 0;
    let (method, minor) = unsafe { ((*p).method, (*p).http_minor) };
    let a = unsafe { acc(p) };
    let method_name = method_str(method);
    // The body arrived with (or before) any interim response could matter.
    a.owes_continue = false;
    a.expecting_body = false;
    a.last_headers = a.headers.len();
    a.completed.push_back(Request {
        method: method_name,
        upgrade,
        http_minor: minor,
        keep_alive,
        url: std::mem::take(&mut a.url),
        headers: std::mem::take(&mut a.headers),
        body: std::mem::take(&mut a.body),
    });
    a.header_bytes = 0;
    // The trailer section's LAST field/value pair is still sitting in the
    // accumulator: nothing calls commit_header() after it, because the
    // message ended instead of another header starting. Leaving it there
    // let the next PIPELINED request commit it as its own first header --
    // `Authorization: Bearer stolen` from one request arriving as request
    // two's opening field, verified on a keep-alive connection. Round 10
    // stopped trailers being committed to their OWN request; this is the
    // same injection crossing a request boundary instead.
    a.field.clear();
    a.value.clear();
    a.in_value = false;
    0
}

fn method_str(m: u8) -> &'static str {
    unsafe {
        let ptr = llhttp_method_name(m as c_int);
        std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(ptr).to_bytes())
    }
}

fn errno_str(e: c_int) -> &'static str {
    unsafe {
        let ptr = llhttp_errno_name(e);
        std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(ptr).to_bytes())
    }
}

const SETTINGS: Settings = Settings {
    on_message_begin: Some(on_message_begin),
    on_url: Some(on_url),
    on_status: None,
    on_method: None,
    on_version: None,
    on_header_field: Some(on_header_field),
    on_header_value: Some(on_header_value),
    on_chunk_extension_name: None,
    on_chunk_extension_value: None,
    on_headers_complete: Some(on_headers_complete),
    on_body: Some(on_body),
    on_message_complete: Some(on_message_complete),
    on_url_complete: None,
    on_status_complete: None,
    on_method_complete: None,
    on_version_complete: None,
    on_header_field_complete: None,
    on_header_value_complete: None,
    on_chunk_extension_name_complete: None,
    on_chunk_extension_value_complete: None,
    on_chunk_header: None,
    on_chunk_complete: None,
    on_reset: None,
};

/// Per-connection HTTP/1.1 request parser.
pub struct HttpParser {
    raw: Box<Llhttp>,
    acc: Box<Acc>,
    /// The head completed an upgrade and llhttp paused (HPE_PAUSED_UPGRADE).
    /// Every byte after that offset belongs to the upgraded protocol, not
    /// HTTP, so no further `feed` may touch llhttp again.
    upgraded: bool,
}

// SAFETY: thread-affine by loop contract; raw pointers reference the
// heap-pinned boxes moved together with the parser.
unsafe impl Send for HttpParser {}

impl HttpParser {
    pub fn new(limits: Limits) -> Self {
        let mut raw: Box<Llhttp> = Box::new(unsafe { std::mem::zeroed() });
        let mut acc = Box::new(Acc {
            limits,
            url: Vec::new(),
            headers: Vec::new(),
            last_headers: 0,
            field: Vec::new(),
            value: Vec::new(),
            in_value: false,
            header_bytes: 0,
            body: Vec::new(),
            completed: VecDeque::new(),
            error: None,
            in_head: false,
            owes_continue: false,
            expecting_body: false,
        });
        unsafe {
            llhttp_init(&mut *raw, HTTP_REQUEST, &SETTINGS);
            raw.data = (&mut *acc as *mut Acc).cast();
        }
        HttpParser { raw, acc, upgraded: false }
    }

    /// Feed bytes; completed requests become available via `next_request`.
    /// Feed bytes. `Ok(Some(offset))` means the parser paused after an
    /// upgrade head (HPE_PAUSED_UPGRADE): bytes from `offset` onward are
    /// NOT HTTP — they belong to the upgraded protocol's stream (R-087).
    pub fn feed(&mut self, data: &[u8]) -> Result<Option<usize>, ParseError> {
        // Already upgraded: every byte is the upgraded protocol's. Calling
        // llhttp again would only return the lingering HPE_PAUSED_UPGRADE
        // WITHOUT refreshing `error_pos` (it short-circuits on the stored
        // error), so the old code then read `error_pos` -- a pointer into a
        // PREVIOUS feed's buffer -- and `offset_from`'d it against THIS
        // buffer: undefined behavior, and in practice a garbage split that
        // dropped or truncated the first client frame to arrive in a later
        // recv. Nothing resumes an upgraded parser (we never call
        // llhttp_resume_after_upgrade), so skip it entirely.
        if self.upgraded {
            return Ok(Some(0));
        }
        let rc = unsafe { llhttp_execute(&mut *self.raw, data.as_ptr().cast(), data.len()) };
        if rc == HPE_PAUSED_UPGRADE {
            // First (and only) valid read of `error_pos`: llhttp set it to a
            // position inside THIS buffer during this execute.
            let consumed = unsafe { (self.raw.error_pos).offset_from(data.as_ptr().cast()) };
            let consumed = consumed.clamp(0, data.len() as isize) as usize;
            self.upgraded = true;
            return Ok(Some(consumed));
        }
        if rc != HPE_OK {
            if let Some(err) = self.acc.error {
                return Err(err);
            }
            let _ = errno_str(rc); // (available for logging)
            return Err(ParseError { status: 400, reason: "malformed request" });
        }
        Ok(None)
    }

    /// The HTTP version of the request being (or last) parsed, if the
    /// parser has seen one at all. Errors before the version is parsed
    /// (malformed start line, an over-long URL) resolve to None; a
    /// pipelined error early in the next request reports the previous
    /// request's version, which the same client just spoke.
    pub fn http_version(&self) -> Option<(u8, u8)> {
        match (self.raw.http_major, self.raw.http_minor) {
            (0, 0) => None,
            v => Some(v),
        }
    }

    pub fn next_request(&mut self) -> Option<Request> {
        self.acc.completed.pop_front()
    }

    pub fn has_request(&self) -> bool {
        !self.acc.completed.is_empty()
    }

    /// A request head is currently mid-receipt (R-080 timeout phase).
    pub fn in_head(&self) -> bool {
        self.acc.in_head
    }

    /// True once, per head that carried `Expect: 100-continue`.
    ///
    /// The caller owns the write queue, so it is the caller that writes
    /// the interim response; taking the flag here makes it impossible to
    /// send twice for one request. Without this the client waits out its
    /// own continue timeout before sending the body -- a fixed stall on
    /// every upload for clients that use the expectation, and an
    /// indefinite one for the stricter clients that never give up.
    pub fn take_owes_continue(&mut self) -> bool {
        std::mem::take(&mut self.acc.owes_continue)
    }

    /// A request expecting `100 Continue` is still waiting for its body
    /// (see `Acc::expecting_body`).
    pub fn expecting_body(&self) -> bool {
        self.acc.expecting_body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(input: &[u8]) -> Request {
        let mut p = HttpParser::new(Limits::default());
        p.feed(input).unwrap();
        p.next_request().expect("complete request")
    }

    #[test]
    fn parses_simple_get() {
        let r = parse_one(b"GET /path?x=1 HTTP/1.1\r\nHost: h\r\nUser-Agent: t\r\n\r\n");
        assert_eq!(r.method, "GET");
        assert_eq!(r.http_minor, 1);
        assert!(r.keep_alive);
        assert_eq!(r.url, b"/path?x=1");
        assert_eq!(
            r.headers,
            vec![(b"host".to_vec(), b"h".to_vec()), (b"user-agent".to_vec(), b"t".to_vec())]
        );
        assert!(r.body.is_empty());
    }

    #[test]
    fn parses_post_body_and_pipelined_next() {
        let mut p = HttpParser::new(Limits::default());
        p.feed(b"POST /a HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhelloGET /b HTTP/1.1\r\nHost: h\r\n\r\n")
            .unwrap();
        let a = p.next_request().unwrap();
        assert_eq!((a.method, a.body.as_slice()), ("POST", b"hello".as_slice()));
        let b = p.next_request().unwrap();
        assert_eq!((b.method, b.url.as_slice()), ("GET", b"/b".as_slice()));
        assert!(p.next_request().is_none());
    }

    #[test]
    fn parses_chunked_body_decoded() {
        let r = parse_one(
            b"POST /c HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n7\r\n world!\r\n0\r\n\r\n"
                [..].as_ref(),
        );
        assert_eq!(r.body, b"hello world!"[..].to_vec());
    }

    #[test]
    fn split_feeds_accumulate() {
        let mut p = HttpParser::new(Limits::default());
        for chunk in [b"GE".as_ref(), b"T / HT", b"TP/1.1\r\nHos", b"t: h\r\n", b"\r\n"] {
            p.feed(chunk).unwrap();
        }
        let r = p.next_request().unwrap();
        assert_eq!(r.method, "GET");
    }

    #[test]
    fn http10_defaults_to_close() {
        let r = parse_one(b"GET / HTTP/1.0\r\nHost: h\r\n\r\n");
        assert_eq!(r.http_minor, 0);
        assert!(!r.keep_alive);
    }

    #[test]
    fn malformed_is_a_400() {
        let mut p = HttpParser::new(Limits::default());
        let err = p.feed(b"NOT A REQUEST\r\n\r\n").unwrap_err();
        assert_eq!(err.status, 400);
    }

    #[test]
    fn url_limit_is_414() {
        let mut p = HttpParser::new(Limits { max_url: 16, ..Default::default() });
        let err = p.feed(b"GET /aaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.1\r\n\r\n").unwrap_err();
        assert_eq!(err.status, 414);
    }

    #[test]
    fn header_count_limit_is_431() {
        let mut p = HttpParser::new(Limits { max_headers: 2, ..Default::default() });
        let err = p.feed(b"GET / HTTP/1.1\r\nHost: h\r\nA: 1\r\nB: 2\r\n\r\n").unwrap_err();
        assert_eq!(err.status, 431);
    }

    #[test]
    fn body_limit_is_413() {
        let mut p = HttpParser::new(Limits { max_body: Some(4), ..Default::default() });
        let err = p.feed(b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 10\r\n\r\n0123456789").unwrap_err();
        assert_eq!(err.status, 413);
    }

    #[test]
    fn a_trailer_is_not_one_more_header() {
        let limits = Limits { max_headers: 2, ..Limits::default() };
        // A head exactly as full as allowed, and a trailer section behind
        // the body: the trailers are dropped, so there is nothing over.
        let mut p = HttpParser::new(limits);
        p.feed(
            b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n\
              1\r\nx\r\n0\r\nX-Sum: 1\r\nX-More: 2\r\n\r\n",
        )
        .unwrap();
        let r = p.next_request().unwrap();
        assert_eq!(r.headers.len(), 2);
        assert_eq!(r.body, b"x");
        // A third header in the head is still one too many.
        let mut p = HttpParser::new(limits);
        let err = p.feed(b"GET / HTTP/1.1\r\nHost: h\r\nA: 1\r\nB: 2\r\n\r\n").unwrap_err();
        assert_eq!((err.status, err.reason), (431, "too many headers"));
        // And trailers still count toward the bytes a request's fields may take.
        let mut p = HttpParser::new(Limits { max_header_bytes: 40, ..limits });
        let err = p
            .feed(
                b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n\
                  0\r\nX-Long: 0123456789012345678901234567890123456789\r\n\r\n",
            )
            .unwrap_err();
        assert_eq!((err.status, err.reason), (431, "headers too large"));
    }

    /// Properties, over generated input. The parser is fed by the network,
    /// which cuts a request wherever it likes, and by clients, which write
    /// whatever they like: what holds for the fixed cases above has to
    /// hold for all of it.
    mod props {
        use super::*;
        use proptest::collection::vec;
        use proptest::prelude::*;

        /// A request as a client would write it.
        #[derive(Debug, Clone)]
        struct Written {
            method: &'static str,
            target: String,
            minor: u8,
            /// The application's own headers, as written (any letter case).
            headers: Vec<(String, String)>,
            connection: Option<&'static str>,
            body: Option<Vec<u8>>,
            /// Chunked transfer coding: where the body is cut into chunks.
            /// None sends it under a Content-Length.
            chunks: Option<Vec<prop::sample::Index>>,
            /// A trailer section after a chunked body. Never delivered.
            trailers: Vec<(String, String)>,
            /// `Name:value` and `Name: value` are the same header.
            space: bool,
        }

        impl Written {
            fn chunked(&self) -> bool {
                self.body.is_some() && self.chunks.is_some() && self.minor == 1
            }

            fn keep_alive(&self) -> bool {
                match (self.minor, self.connection) {
                    (1, Some(c)) => !c.eq_ignore_ascii_case("close"),
                    (1, None) => true,
                    (_, c) => c.is_some_and(|c| c.eq_ignore_ascii_case("keep-alive")),
                }
            }

            /// Every header of the head, in the order it goes on the wire.
            fn head(&self) -> Vec<(String, String)> {
                let mut all = Vec::new();
                if self.minor == 1 {
                    all.push(("Host".to_string(), "example.test".to_string()));
                }
                all.extend(self.headers.iter().cloned());
                if let Some(c) = self.connection {
                    all.push(("Connection".to_string(), c.to_string()));
                }
                if let Some(body) = &self.body {
                    if self.chunked() {
                        all.push(("Transfer-Encoding".to_string(), "chunked".to_string()));
                    } else {
                        all.push(("Content-Length".to_string(), body.len().to_string()));
                    }
                }
                all
            }

            fn wire(&self) -> Vec<u8> {
                let sep = if self.space { ": " } else { ":" };
                let mut out =
                    format!("{} {} HTTP/1.{}\r\n", self.method, self.target, self.minor).into_bytes();
                for (name, value) in self.head() {
                    out.extend_from_slice(format!("{name}{sep}{value}\r\n").as_bytes());
                }
                out.extend_from_slice(b"\r\n");
                let Some(body) = &self.body else { return out };
                if !self.chunked() {
                    out.extend_from_slice(body);
                    return out;
                }
                let mut ends: Vec<usize> =
                    self.chunks.iter().flatten().map(|c| c.index(body.len() + 1)).collect();
                ends.push(body.len());
                ends.sort_unstable();
                let mut start = 0;
                for (i, end) in ends.into_iter().enumerate() {
                    if end == start {
                        continue; // an empty chunk would end the body
                    }
                    // Sizes are hexadecimal in either case, and a chunk may carry an extension.
                    let size =
                        if i % 2 == 0 { format!("{:x}", end - start) } else { format!("{:X}", end - start) };
                    let ext = if i % 3 == 2 { ";note=1" } else { "" };
                    out.extend_from_slice(format!("{size}{ext}\r\n").as_bytes());
                    out.extend_from_slice(&body[start..end]);
                    out.extend_from_slice(b"\r\n");
                    start = end;
                }
                out.extend_from_slice(b"0\r\n");
                for (name, value) in &self.trailers {
                    out.extend_from_slice(format!("{name}{sep}{value}\r\n").as_bytes());
                }
                out.extend_from_slice(b"\r\n");
                out
            }

            /// What the application must be shown for it.
            fn parsed(&self) -> Seen {
                Seen {
                    method: self.method,
                    minor: self.minor,
                    keep_alive: self.keep_alive(),
                    upgrade: false,
                    url: self.target.clone().into_bytes(),
                    headers: self
                        .head()
                        .into_iter()
                        .map(|(n, v)| (n.to_ascii_lowercase().into_bytes(), v.into_bytes()))
                        .collect(),
                    body: self.body.clone().unwrap_or_default(),
                }
            }
        }

        #[derive(Debug, PartialEq, Clone)]
        struct Seen {
            method: &'static str,
            minor: u8,
            keep_alive: bool,
            upgrade: bool,
            url: Vec<u8>,
            headers: Vec<(Vec<u8>, Vec<u8>)>,
            body: Vec<u8>,
        }

        /// How a connection's HTTP ended, as far as it was fed.
        #[derive(Debug, PartialEq, Clone, Copy)]
        enum End {
            Open,
            /// Refused, with this status. (The reason text is not part of
            /// the verdict: a request over two limits at once is refused
            /// for whichever the parser comes to first, and inside one
            /// header that depends on where a read ended.)
            Refused(u16),
            /// The offset, in the whole stream, where the upgraded
            /// protocol's bytes start.
            Upgraded(usize),
        }

        /// Feed `wire` in the pieces `cuts` make of it, as the server
        /// does: requests are taken after every read, and nothing is fed
        /// after a refusal or an upgrade.
        fn run(wire: &[u8], cuts: &[usize], limits: Limits) -> (Vec<Seen>, End) {
            let mut parser = HttpParser::new(limits);
            let mut seen = Vec::new();
            let mut end = End::Open;
            let mut at = 0;
            for &stop in cuts.iter().chain([&wire.len()]) {
                let fed = parser.feed(&wire[at..stop]);
                while let Some(r) = parser.next_request() {
                    seen.push(Seen {
                        method: r.method,
                        minor: r.http_minor,
                        keep_alive: r.keep_alive,
                        upgrade: r.upgrade,
                        url: r.url,
                        headers: r.headers,
                        body: r.body,
                    });
                }
                match fed {
                    Ok(None) => {}
                    Ok(Some(offset)) => {
                        end = End::Upgraded(at + offset);
                        break;
                    }
                    Err(e) => {
                        end = End::Refused(e.status);
                        break;
                    }
                }
                at = stop;
            }
            (seen, end)
        }

        fn sorted_cuts(cuts: &[prop::sample::Index], len: usize) -> Vec<usize> {
            let mut cuts: Vec<usize> = cuts.iter().map(|c| c.index(len + 1)).collect();
            cuts.sort_unstable();
            cuts
        }

        fn header() -> impl Strategy<Value = (String, String)> {
            // Names no parser gives a meaning to; values with no space at
            // either end (which is not part of a value), empty included.
            (
                "[Xx]-[A-Za-z0-9-]{0,12}",
                prop_oneof![1 => Just(String::new()), 6 => "[!-~]([ -~]{0,30}[!-~])?"],
            )
        }

        fn written() -> impl Strategy<Value = Written> {
            (
                prop::sample::select(vec!["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]),
                "/[A-Za-z0-9._~%/-]{0,40}(\\?[A-Za-z0-9=&%+-]{0,20})?",
                prop_oneof![5 => Just(1u8), 1 => Just(0u8)],
                vec(header(), 0..6),
                proptest::option::weighted(
                    0.3,
                    prop::sample::select(vec!["close", "Close", "keep-alive", "Keep-Alive"]),
                ),
                proptest::option::of(
                    prop_oneof![4 => vec(any::<u8>(), 0..200), 1 => vec(any::<u8>(), 4000..5000)],
                ),
                proptest::option::of(vec(any::<prop::sample::Index>(), 0..4)),
                vec(header(), 0..3),
                any::<bool>(),
            )
                .prop_map(
                    |(method, target, minor, headers, connection, body, chunks, trailers, space)| Written {
                        method,
                        target,
                        minor,
                        headers,
                        connection,
                        body,
                        chunks,
                        trailers,
                        space,
                    },
                )
        }

        /// Requests one after another on one connection. Only the last may
        /// ask for the connection to close: nothing may follow one that
        /// does.
        fn pipeline() -> impl Strategy<Value = Vec<Written>> {
            vec(written(), 1..5).prop_map(|mut all| {
                let last = all.len() - 1;
                for w in &mut all[..last] {
                    if !w.keep_alive() {
                        w.connection = Some("keep-alive");
                    }
                }
                all
            })
        }

        /// What the limits make of one request: the refusal, if any, in
        /// the order a parser reading front to back comes to it.
        fn refusal(w: &Written, limits: Limits) -> Option<u16> {
            if w.target.len() > limits.max_url {
                return Some(414);
            }
            let head = w.head();
            let mut bytes: usize = head.iter().map(|(n, v)| n.len() + v.len()).sum();
            if head.len() > limits.max_headers || bytes > limits.max_header_bytes {
                return Some(431);
            }
            let body = w.body.as_ref()?;
            if limits.max_body.is_some_and(|cap| body.len() > cap) {
                return Some(413);
            }
            if w.chunked() {
                // Trailers are read, measured, and dropped: they count
                // toward the bytes a request's fields may take, and are not
                // headers.
                bytes += w.trailers.iter().map(|(n, v)| n.len() + v.len()).sum::<usize>();
                if bytes > limits.max_header_bytes {
                    return Some(431);
                }
            }
            None
        }

        proptest! {
            /// Every request written is the request delivered -- method,
            /// target, headers in order, body, and whether the connection
            /// stays open -- wherever the reads fall: one read, any reads,
            /// or a byte at a time. Trailers never arrive, and never leak
            /// into the request behind them.
            #[test]
            fn a_pipeline_parses_to_what_was_written_however_it_is_cut(
                requests in pipeline(),
                cuts in vec(any::<prop::sample::Index>(), 0..10),
            ) {
                let wire: Vec<u8> = requests.iter().flat_map(Written::wire).collect();
                let expected: Vec<Seen> = requests.iter().map(Written::parsed).collect();
                let (seen, end) = run(&wire, &[], Limits::default());
                prop_assert_eq!(end, End::Open);
                prop_assert_eq!(&seen, &expected);
                prop_assert_eq!(&run(&wire, &sorted_cuts(&cuts, wire.len()), Limits::default()).0, &expected);
                if wire.len() <= 20_000 {
                    let each: Vec<usize> = (1..wire.len()).collect();
                    prop_assert_eq!(run(&wire, &each, Limits::default()), (expected, End::Open));
                }
            }

            /// An upgrade ends HTTP at the end of its head, to the byte:
            /// what follows is the new protocol's, and the offset where it
            /// starts is the same however the stream was cut (a frame the
            /// client sent on the heels of its handshake is not lost, and
            /// not parsed as a request).
            #[test]
            fn an_upgrade_hands_over_at_the_same_byte_however_it_is_cut(
                before in vec(written(), 0..3),
                target in "/[a-z0-9/]{0,20}",
                headers in vec(header(), 0..4),
                after in vec(any::<u8>(), 0..200),
                cuts in vec(any::<prop::sample::Index>(), 0..10),
            ) {
                let mut wire = Vec::new();
                let mut expected = Vec::new();
                for mut w in before {
                    if !w.keep_alive() {
                        w.connection = Some("keep-alive");
                    }
                    wire.extend(w.wire());
                    expected.push(w.parsed());
                }
                let mut head = vec![
                    ("host".to_string(), "example.test".to_string()),
                    ("upgrade".to_string(), "websocket".to_string()),
                    ("connection".to_string(), "Upgrade".to_string()),
                ];
                head.extend(headers.into_iter().map(|(n, v)| (n.to_ascii_lowercase(), v)));
                wire.extend_from_slice(format!("GET {target} HTTP/1.1\r\n").as_bytes());
                for (name, value) in &head {
                    wire.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
                }
                wire.extend_from_slice(b"\r\n");
                let handover = wire.len();
                wire.extend_from_slice(&after);
                expected.push(Seen {
                    method: "GET",
                    minor: 1,
                    keep_alive: true,
                    upgrade: true,
                    url: target.into_bytes(),
                    headers: head.into_iter().map(|(n, v)| (n.into_bytes(), v.into_bytes())).collect(),
                    body: Vec::new(),
                });
                let whole = run(&wire, &[], Limits::default());
                prop_assert_eq!(&whole, &(expected, End::Upgraded(handover)));
                prop_assert_eq!(&whole, &run(&wire, &sorted_cuts(&cuts, wire.len()), Limits::default()));
                let each: Vec<usize> = (1..wire.len()).collect();
                prop_assert_eq!(&whole, &run(&wire, &each, Limits::default()));
            }

            /// The limits refuse exactly what exceeds them, with the status
            /// that says which, and nothing that fits -- again wherever the
            /// reads fall (a limit counted per read instead of per request
            /// would pass every fixed test and fail here).
            #[test]
            fn limits_refuse_exactly_what_exceeds_them(
                w in written(),
                max_url in 1usize..64,
                max_headers in 0usize..10,
                max_header_bytes in 8usize..300,
                max_body in proptest::option::of(0usize..300),
                cuts in vec(any::<prop::sample::Index>(), 0..10),
            ) {
                let limits = Limits { max_url, max_headers, max_header_bytes, max_body };
                let wire = w.wire();
                let expected = match refusal(&w, limits) {
                    Some(status) => (Vec::new(), End::Refused(status)),
                    None => (vec![w.parsed()], End::Open),
                };
                prop_assert_eq!(&run(&wire, &[], limits), &expected);
                prop_assert_eq!(&run(&wire, &sorted_cuts(&cuts, wire.len()), limits), &expected);
                let each: Vec<usize> = (1..wire.len()).collect();
                prop_assert_eq!(&run(&wire, &each, limits), &expected);
            }

            /// Damaged and hostile input: a real pipeline with bytes
            /// changed, or no request at all. Nothing panics, nothing
            /// delivered is over a limit, and the verdict -- the requests
            /// let through, and whether the connection then stays open, is
            /// refused, or is handed over at which byte -- does not depend
            /// on where the reads fell. (The status of a refusal can: a
            /// request both malformed and too long is 400 or 414 by which
            /// the parser meets first, and it is told about a long target
            /// when the read holding it ends.)
            #[test]
            fn damaged_input_gets_one_verdict_however_it_is_cut(
                requests in vec(written(), 0..4),
                damage in vec((any::<prop::sample::Index>(), any::<u8>()), 0..4),
                junk in prop_oneof![3 => Just(Vec::new()), 1 => vec(any::<u8>(), 0..120)],
                cuts in vec(any::<prop::sample::Index>(), 0..10),
                small in any::<bool>(),
            ) {
                let limits = if small {
                    Limits { max_url: 24, max_headers: 4, max_header_bytes: 96, max_body: Some(64) }
                } else {
                    Limits::default()
                };
                let mut wire: Vec<u8> = requests.iter().flat_map(Written::wire).collect();
                if !wire.is_empty() {
                    for (at, byte) in &damage {
                        let at = at.index(wire.len());
                        wire[at] = *byte;
                    }
                }
                wire.extend_from_slice(&junk);
                let verdict = |cuts: &[usize]| {
                    let (seen, end) = run(&wire, cuts, limits);
                    (seen, if let End::Refused(_) = end { End::Refused(0) } else { end })
                };
                let whole = verdict(&[]);
                prop_assert_eq!(&whole, &verdict(&sorted_cuts(&cuts, wire.len())));
                if wire.len() <= 20_000 {
                    let each: Vec<usize> = (1..wire.len()).collect();
                    prop_assert_eq!(&whole, &verdict(&each));
                }
                for r in &whole.0 {
                    prop_assert!(r.url.len() <= limits.max_url);
                    prop_assert!(r.headers.len() <= limits.max_headers);
                    prop_assert!(r.headers.iter().map(|(n, v)| n.len() + v.len()).sum::<usize>() <= limits.max_header_bytes);
                    prop_assert!(limits.max_body.is_none_or(|cap| r.body.len() <= cap));
                    prop_assert!(r.headers.iter().all(|(n, _)| !n.iter().any(u8::is_ascii_uppercase)));
                    if r.minor >= 1 {
                        prop_assert_eq!(r.headers.iter().filter(|(n, _)| n == b"host").count(), 1);
                    }
                }
            }
        }
    }
}
