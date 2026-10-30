// loadgen: a native load generator for cadeloop's Windows benchmarks.
//
// One binary, stdlib only, so it builds wherever Go does and measures the
// server rather than an interpreter: the Python clients in bench/http and
// bench/echo are GIL-bound (docs/benchmarks.md, "Trap 1").
//
// Modes:
//
//	http   keep-alive HTTP/1.1 (closed loop, pipelined, or open loop at a
//	       fixed/phased rate), or one connection per request (-churn)
//	echo   TCP echo: write -size bytes, read them back, compare
//	ws     WebSocket echo: masked frames out, frames back, compared
//	hold   open -c connections (optionally one request each) and hold them
//	       idle for -d; prints "HOLDING n" once all are up
//
// Every response is validated (status, framing, length, and -expect-body
// contents); anything else is an error, never a success. Latency in open
// loop is measured from each request's INTENDED send time, so a stalled
// server shows up as queueing delay instead of being silently skipped
// (coordinated omission). Results: one JSON line prefixed "RESULT ".
package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Mode        string  `json:"mode"`
	Host        string  `json:"host"`
	Port        int     `json:"port"`
	Path        string  `json:"path"`
	Method      string  `json:"method"`
	Conns       int     `json:"conns"`
	DurationS   float64 `json:"duration_s"`
	WarmupS     float64 `json:"warmup_s"`
	TimeoutS    float64 `json:"timeout_s"`
	Rate        float64 `json:"rate"`
	Phases      string  `json:"phases,omitempty"`
	Pipeline    int     `json:"pipeline"`
	Body        int     `json:"req_body"`
	ExpectCode  int     `json:"expect_status"`
	ExpectLen   int     `json:"expect_len"`
	ExpectBody  string  `json:"expect_body,omitempty"`
	Churn       bool    `json:"churn"`
	TLS         bool    `json:"tls"`
	Size        int     `json:"size"`
	Window      int     `json:"window"`
	Text        bool    `json:"text"`
	Procs       int     `json:"procs"`
	Series      bool    `json:"series"`
	HoldRequest bool    `json:"hold_request"`
}

var cfg Config

// Run clock. Everything is relative to loadStart; [measureFrom, measureTo)
// is the measured window (warmup excluded).
var (
	loadStart   int64 // ns on the QPC timeline (clock.go)
	measureFrom int64
	measureTo   int64
	stopping    atomic.Bool
	phases      []phase
	series      []*secondBucket
)

type phase struct {
	rate  float64
	until time.Duration // cumulative end offset from loadStart
}

type secondBucket struct {
	h    AtomicHist
	errs uint64
}

type Stats struct {
	hist       Hist
	ok         uint64
	bytesIn    uint64
	bytesOut   uint64
	bodyBytes  uint64
	late       uint64
	reconnects uint64
	// Keep-alive connections the server ended with a valid
	// "Connection: close" response (hypercorn does after 1000 requests):
	// not errors, just a reconnect.
	serverCloses uint64
	incomplete   uint64
	errs         map[string]uint64
}

func newStats() *Stats { return &Stats{errs: map[string]uint64{}} }

func inWindow(t int64) bool { return t >= measureFrom && t < measureTo }

func (s *Stats) record(sent, done int64, in, out, body int) {
	if !inWindow(sent) || !inWindow(done) || done < sent {
		return
	}
	ns := uint64(done - sent)
	s.hist.Record(ns)
	s.ok++
	s.bytesIn += uint64(in)
	s.bytesOut += uint64(out)
	s.bodyBytes += uint64(body)
	if series != nil {
		if i := int((done - loadStart) / 1e9); i >= 0 && i < len(series) {
			series[i].h.Record(ns)
		}
	}
}

var verbose bool
var verboseLeft atomic.Int64

func (s *Stats) failErr(err error) {
	if verbose && verboseLeft.Add(-1) >= 0 {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
	s.fail(classify(err))
}

func (s *Stats) fail(kind string) {
	t := now()
	if series != nil {
		if i := int((t - loadStart) / 1e9); i >= 0 && i < len(series) {
			atomic.AddUint64(&series[i].errs, 1)
		}
	}
	if inWindow(t) {
		s.errs[kind]++
	}
}

func classify(err error) string {
	if err == nil {
		return "none"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "eof"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "forcibly closed"), strings.Contains(msg, "reset"):
		return "reset"
	case strings.Contains(msg, "refused"):
		return "refused"
	case strings.Contains(msg, "aborted"):
		return "aborted"
	}
	var pe *protoErr
	if errors.As(err, &pe) {
		return pe.kind
	}
	return "other"
}

type protoErr struct{ kind, detail string }

func (p *protoErr) Error() string { return p.kind + ": " + p.detail }

func perr(kind, f string, a ...any) error { return &protoErr{kind, fmt.Sprintf(f, a...)} }

func timeout() time.Duration { return time.Duration(cfg.TimeoutS * float64(time.Second)) }

func dial() (net.Conn, error) {
	d := net.Dialer{Timeout: timeout()}
	c, err := d.Dial("tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	if cfg.TLS {
		tc := tls.Client(c, &tls.Config{InsecureSkipVerify: true, ServerName: cfg.Host, NextProtos: []string{"http/1.1"}})
		tc.SetDeadline(time.Now().Add(timeout()))
		if err := tc.Handshake(); err != nil {
			c.Close()
			return nil, err
		}
		tc.SetDeadline(time.Time{})
		return tc, nil
	}
	return c, nil
}

// abortClose closes with linger 0 (RST): churn tests then leave no
// TIME_WAIT on either side to exhaust the client's ephemeral ports.
func abortClose(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetLinger(0)
	} else if tl, ok := c.(*tls.Conn); ok {
		if tc, ok := tl.NetConn().(*net.TCPConn); ok {
			tc.SetLinger(0)
		}
	}
	c.Close()
}

// ---- pacing -----------------------------------------------------------

func rateAt(off time.Duration) float64 {
	if len(phases) == 0 {
		return cfg.Rate
	}
	for _, p := range phases {
		if off < p.until {
			return p.rate
		}
	}
	return phases[len(phases)-1].rate
}

// nextIntended advances one connection's schedule by its share of the
// offered rate at that point in time.
func nextIntended(t int64) int64 {
	r := rateAt(time.Duration(t-loadStart)) / float64(cfg.Conns)
	if r <= 0 {
		return t + int64(time.Hour)
	}
	return t + int64(1e9/r)
}

// ---- HTTP -------------------------------------------------------------

var reqBytes []byte

func buildRequest() {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s %s HTTP/1.1\r\nHost: %s:%d\r\n", cfg.Method, cfg.Path, cfg.Host, cfg.Port)
	if cfg.Body > 0 {
		fmt.Fprintf(&b, "Content-Type: application/octet-stream\r\nContent-Length: %d\r\n", cfg.Body)
	}
	if cfg.Churn {
		b.WriteString("Connection: close\r\n")
	}
	b.WriteString("\r\n")
	if cfg.Body > 0 {
		body := make([]byte, cfg.Body)
		for i := range body {
			body[i] = byte('a' + i%26)
		}
		b.Write(body)
	}
	reqBytes = b.Bytes()
}

type respInfo struct {
	status  int
	bodyLen int
	total   int
	close   bool
}

// countingReader counts raw bytes taken off the wire.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	return line, nil
}

func readResponse(br *bufio.Reader, cr *countingReader, scratch []byte) (respInfo, error) {
	var ri respInfo
	before := cr.n - br.Buffered()
	line, err := readLine(br)
	if err != nil {
		return ri, err
	}
	if len(line) < 12 || !bytes.HasPrefix(line, []byte("HTTP/1.")) {
		return ri, perr("proto", "bad status line %q", line)
	}
	ri.status, err = strconv.Atoi(string(line[9:12]))
	if err != nil {
		return ri, perr("proto", "bad status %q", line)
	}
	cl := -1
	chunked := false
	for {
		h, err := readLine(br)
		if err != nil {
			return ri, err
		}
		if len(h) <= 2 {
			break
		}
		colon := bytes.IndexByte(h, ':')
		if colon < 0 {
			return ri, perr("proto", "bad header %q", h)
		}
		name := h[:colon]
		val := bytes.TrimSpace(h[colon+1:])
		switch {
		case asciiEq(name, "content-length"):
			if cl >= 0 {
				return ri, perr("proto", "duplicate content-length")
			}
			cl, err = strconv.Atoi(string(val))
			if err != nil || cl < 0 {
				return ri, perr("proto", "bad content-length %q", val)
			}
		case asciiEq(name, "transfer-encoding"):
			chunked = bytes.Contains(bytes.ToLower(val), []byte("chunked"))
		case asciiEq(name, "connection"):
			ri.close = bytes.Contains(bytes.ToLower(val), []byte("close"))
		}
	}
	noBody := ri.status/100 == 1 || ri.status == 204 || ri.status == 304 || cfg.Method == "HEAD"
	checkBody := cfg.ExpectBody != ""
	var got []byte
	switch {
	case noBody:
	case chunked:
		if cl >= 0 {
			return ri, perr("proto", "content-length with chunked")
		}
		for {
			sl, err := readLine(br)
			if err != nil {
				return ri, err
			}
			s := bytes.TrimSpace(sl)
			if i := bytes.IndexByte(s, ';'); i >= 0 {
				s = s[:i]
			}
			n, err := strconv.ParseInt(string(s), 16, 64)
			if err != nil || n < 0 {
				return ri, perr("proto", "bad chunk size %q", sl)
			}
			if n == 0 {
				for { // trailers
					t, err := readLine(br)
					if err != nil {
						return ri, err
					}
					if len(t) <= 2 {
						break
					}
				}
				break
			}
			if checkBody && len(got)+int(n) <= 1<<16 {
				start := len(got)
				got = append(got, make([]byte, n)...)
				if _, err := io.ReadFull(br, got[start:]); err != nil {
					return ri, err
				}
			} else if _, err := br.Discard(int(n)); err != nil {
				return ri, err
			}
			ri.bodyLen += int(n)
			if _, err := br.Discard(2); err != nil {
				return ri, err
			}
		}
	case cl >= 0:
		if checkBody && cl <= len(scratch) {
			if _, err := io.ReadFull(br, scratch[:cl]); err != nil {
				return ri, err
			}
			got = scratch[:cl]
		} else if _, err := br.Discard(cl); err != nil {
			return ri, err
		}
		ri.bodyLen = cl
	default: // close-delimited
		n, err := io.Copy(io.Discard, br)
		if err != nil {
			return ri, err
		}
		ri.bodyLen = int(n)
		ri.close = true
	}
	ri.total = cr.n - br.Buffered() - before
	if cfg.ExpectCode != 0 && ri.status != cfg.ExpectCode {
		return ri, perr("status", "status %d", ri.status)
	}
	if cfg.ExpectLen >= 0 && ri.bodyLen != cfg.ExpectLen {
		return ri, perr("badlen", "body %d bytes, want %d", ri.bodyLen, cfg.ExpectLen)
	}
	if checkBody && string(got) != cfg.ExpectBody {
		return ri, perr("badbody", "body %q", truncate(got))
	}
	return ri, nil
}

func truncate(b []byte) string {
	if len(b) > 40 {
		return string(b[:40]) + "..."
	}
	return string(b)
}

func asciiEq(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		c := b[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

type httpConn struct {
	c  net.Conn
	cr *countingReader
	br *bufio.Reader
}

func openHTTP() (*httpConn, error) {
	c, err := dial()
	if err != nil {
		return nil, err
	}
	cr := &countingReader{r: c}
	return &httpConn{c: c, cr: cr, br: bufio.NewReaderSize(cr, 64<<10)}, nil
}

func httpWorker(id int, st *Stats, wg *sync.WaitGroup) {
	defer wg.Done()
	scratch := make([]byte, 64<<10)
	var hc *httpConn
	depth := cfg.Pipeline
	if depth < 1 {
		depth = 1
	}
	batch := bytes.Repeat(reqBytes, depth)
	openLoop := cfg.Rate > 0 || len(phases) > 0
	// Spread connections' schedules across one interval.
	intended := loadStart
	if openLoop {
		r := rateAt(0) / float64(cfg.Conns)
		if r > 0 {
			intended = loadStart + int64(1e9/r*float64(id)/float64(cfg.Conns))
		}
	}
	for !stopping.Load() {
		var sent int64
		if openLoop {
			if now() >= measureTo {
				break
			}
			sleepUntil(intended)
			sent = intended
			if lag := now() - intended; lag > 1e6 && inWindow(intended) {
				st.late++
			}
			intended = nextIntended(intended)
		}
		if hc == nil {
			var err error
			hc, err = openHTTP()
			if err != nil {
				st.fail("connect:" + classify(err))
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}
		if !openLoop {
			sent = now()
		}
		hc.c.SetDeadline(time.Now().Add(timeout()))
		if _, err := hc.c.Write(batch); err != nil {
			st.failErr(err)
			abortClose(hc.c)
			hc = nil
			st.reconnects++
			continue
		}
		var failed error
		closed := false
		for i := 0; i < depth; i++ {
			ri, err := readResponse(hc.br, hc.cr, scratch)
			if err != nil {
				failed = err
				break
			}
			st.record(sent, now(), ri.total, len(reqBytes), ri.bodyLen)
			if ri.close && !cfg.Churn {
				closed = true
				if i < depth-1 {
					// Pipelined requests behind a close go unanswered.
					failed = perr("closed", "server closed with %d pipelined requests unanswered", depth-1-i)
				}
				break
			}
		}
		if failed != nil {
			st.failErr(failed)
			abortClose(hc.c)
			hc = nil
			st.reconnects++
			continue
		}
		if closed {
			st.serverCloses++
			hc.c.Close()
			hc = nil
			continue
		}
		if cfg.Churn {
			abortClose(hc.c)
			hc = nil
		}
	}
	if hc != nil {
		hc.c.Close()
	}
}

// ---- TCP echo / WebSocket -----------------------------------------------

func payload(n int, text bool) []byte {
	p := make([]byte, n)
	if text {
		for i := range p {
			p[i] = byte('a' + i%26)
		}
	} else {
		rand.Read(p)
	}
	return p
}

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func wsHandshake(c net.Conn, br *bufio.Reader) error {
	k := make([]byte, 16)
	rand.Read(k)
	key := base64.StdEncoding.EncodeToString(k)
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", cfg.Path, cfg.Host, cfg.Port, key)
	c.SetDeadline(time.Now().Add(timeout()))
	if _, err := io.WriteString(c, req); err != nil {
		return err
	}
	line, err := readLine(br)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(line, []byte("HTTP/1.1 101")) {
		return perr("status", "handshake %q", line)
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	want := base64.StdEncoding.EncodeToString(sum[:])
	ok := false
	for {
		h, err := readLine(br)
		if err != nil {
			return err
		}
		if len(h) <= 2 {
			break
		}
		if i := bytes.IndexByte(h, ':'); i > 0 && asciiEq(h[:i], "sec-websocket-accept") {
			ok = string(bytes.TrimSpace(h[i+1:])) == want
		}
	}
	if !ok {
		return perr("proto", "bad Sec-WebSocket-Accept")
	}
	c.SetDeadline(time.Time{})
	return nil
}

func wsFrame(opcode byte, p []byte) []byte {
	var hdr []byte
	hdr = append(hdr, 0x80|opcode)
	n := len(p)
	switch {
	case n < 126:
		hdr = append(hdr, 0x80|byte(n))
	case n <= 0xffff:
		hdr = append(hdr, 0x80|126, byte(n>>8), byte(n))
	default:
		hdr = append(hdr, 0x80|127)
		hdr = binary.BigEndian.AppendUint64(hdr, uint64(n))
	}
	var mask [4]byte
	rand.Read(mask[:])
	hdr = append(hdr, mask[:]...)
	out := append(hdr, p...)
	body := out[len(hdr):]
	for i := range body {
		body[i] ^= mask[i&3]
	}
	return out
}

// wsRead reads one data message (answering pings), returning its payload
// in buf. Fragmented messages are reassembled.
func wsRead(c net.Conn, br *bufio.Reader, buf []byte) ([]byte, int, error) {
	out := buf[:0]
	raw := 0
	for {
		var h [2]byte
		if _, err := io.ReadFull(br, h[:]); err != nil {
			return nil, raw, err
		}
		raw += 2
		fin := h[0]&0x80 != 0
		op := h[0] & 0x0f
		if h[1]&0x80 != 0 {
			return nil, raw, perr("proto", "masked server frame")
		}
		n := uint64(h[1] & 0x7f)
		switch n {
		case 126:
			var e [2]byte
			if _, err := io.ReadFull(br, e[:]); err != nil {
				return nil, raw, err
			}
			n = uint64(binary.BigEndian.Uint16(e[:]))
			raw += 2
		case 127:
			var e [8]byte
			if _, err := io.ReadFull(br, e[:]); err != nil {
				return nil, raw, err
			}
			n = binary.BigEndian.Uint64(e[:])
			raw += 8
		}
		if n > 64<<20 {
			return nil, raw, perr("proto", "frame of %d bytes", n)
		}
		start := len(out)
		if cap(out)-start < int(n) {
			nb := make([]byte, start, start+int(n))
			copy(nb, out)
			out = nb
		}
		out = out[:start+int(n)]
		if _, err := io.ReadFull(br, out[start:]); err != nil {
			return nil, raw, err
		}
		raw += int(n)
		switch op {
		case 0x8:
			return nil, raw, perr("closed", "server sent close")
		case 0x9: // ping: answer, drop from the message
			c.Write(wsFrame(0xA, append([]byte(nil), out[start:]...)))
			out = out[:start]
			continue
		case 0xA:
			out = out[:start]
			continue
		}
		if fin {
			return out, raw, nil
		}
	}
}

type streamConn struct {
	c  net.Conn
	br *bufio.Reader
}

func openStream() (*streamConn, error) {
	c, err := dial()
	if err != nil {
		return nil, err
	}
	sc := &streamConn{c: c, br: bufio.NewReaderSize(c, 64<<10)}
	if cfg.Mode == "ws" {
		if err := wsHandshake(c, sc.br); err != nil {
			c.Close()
			return nil, err
		}
	}
	return sc, nil
}

// readMsg reads one echoed message and checks it against want.
func readMsg(sc *streamConn, want, buf []byte) (int, error) {
	if cfg.Mode == "ws" {
		got, raw, err := wsRead(sc.c, sc.br, buf)
		if err != nil {
			return raw, err
		}
		if !bytes.Equal(got, want) {
			return raw, perr("badbody", "echo mismatch (%d bytes, want %d)", len(got), len(want))
		}
		return raw, nil
	}
	if _, err := io.ReadFull(sc.br, buf[:len(want)]); err != nil {
		return 0, err
	}
	if !bytes.Equal(buf[:len(want)], want) {
		return len(want), perr("badbody", "echo mismatch")
	}
	return len(want), nil
}

func streamWorker(id int, st *Stats, wg *sync.WaitGroup) {
	defer wg.Done()
	msg := payload(cfg.Size, cfg.Text || cfg.Mode != "ws")
	buf := make([]byte, cfg.Size+64)
	var frame []byte
	if cfg.Mode == "ws" {
		op := byte(0x2)
		if cfg.Text {
			op = 0x1
		}
		frame = wsFrame(op, append([]byte(nil), msg...))
	} else {
		frame = msg
	}
	var sc *streamConn
	for !stopping.Load() {
		if sc == nil {
			var err error
			sc, err = openStream()
			if err != nil {
				st.fail("connect:" + classify(err))
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}
		if cfg.Window <= 1 {
			sent := now()
			sc.c.SetDeadline(time.Now().Add(timeout()))
			if _, err := sc.c.Write(frame); err != nil {
				st.failErr(err)
				abortClose(sc.c)
				sc = nil
				st.reconnects++
				continue
			}
			raw, err := readMsg(sc, msg, buf)
			if err != nil {
				st.failErr(err)
				abortClose(sc.c)
				sc = nil
				st.reconnects++
				continue
			}
			st.record(sent, now(), raw, len(frame), len(msg))
			continue
		}
		// Windowed: a writer keeps up to Window messages outstanding; this
		// goroutine reads and timestamps them in order.
		if err := windowed(sc, st, frame, msg, buf); err != nil {
			st.failErr(err)
			st.reconnects++
		}
		abortClose(sc.c)
		sc = nil
	}
	if sc != nil {
		if cfg.Mode == "ws" {
			sc.c.SetDeadline(time.Now().Add(time.Second))
			sc.c.Write(wsFrame(0x8, []byte{0x03, 0xe8}))
		}
		sc.c.Close()
	}
}

func windowed(sc *streamConn, st *Stats, frame, msg, buf []byte) error {
	slots := make(chan struct{}, cfg.Window)
	sends := make(chan int64, cfg.Window)
	done := make(chan struct{})
	werr := make(chan error, 1)
	go func() {
		for {
			select {
			case slots <- struct{}{}:
			case <-done:
				return
			}
			if stopping.Load() {
				close(sends)
				return
			}
			sends <- now()
			sc.c.SetWriteDeadline(time.Now().Add(timeout()))
			if _, err := sc.c.Write(frame); err != nil {
				werr <- err
				close(sends)
				return
			}
		}
	}()
	defer close(done)
	for sent := range sends {
		sc.c.SetReadDeadline(time.Now().Add(timeout()))
		raw, err := readMsg(sc, msg, buf)
		if err != nil {
			return err
		}
		st.record(sent, now(), raw, len(frame), len(msg))
		<-slots
	}
	select {
	case err := <-werr:
		return err
	default:
		return nil
	}
}

// ---- hold ---------------------------------------------------------------

func hold(st *Stats) {
	conns := make([]net.Conn, cfg.Conns)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64)
	var up atomic.Int64
	for i := 0; i < cfg.Conns; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			c, err := dial()
			if err != nil {
				mu.Lock()
				st.errs["connect:"+classify(err)]++
				mu.Unlock()
				return
			}
			if cfg.HoldRequest {
				cr := &countingReader{r: c}
				br := bufio.NewReaderSize(cr, 4096)
				c.SetDeadline(time.Now().Add(timeout()))
				if _, err = c.Write(reqBytes); err == nil {
					_, err = readResponse(br, cr, make([]byte, 4096))
				}
				c.SetDeadline(time.Time{})
				if err != nil {
					mu.Lock()
					st.errs[classify(err)]++
					mu.Unlock()
					c.Close()
					return
				}
			}
			conns[i] = c
			up.Add(1)
		}(i)
	}
	wg.Wait()
	fmt.Printf("HOLDING %d\n", up.Load())
	os.Stdout.Sync()
	time.Sleep(time.Duration(cfg.DurationS * float64(time.Second)))
	// Every connection is still open from our side: count the ones the
	// server dropped while they were idle (a read returns at once).
	alive := 0
	for _, c := range conns {
		if c == nil {
			continue
		}
		c.SetReadDeadline(time.Now().Add(time.Millisecond))
		var b [1]byte
		_, err := c.Read(b[:])
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			alive++
		}
		abortClose(c)
	}
	st.ok = uint64(alive)
}

// ---- main -----------------------------------------------------------------

func parsePhases(s string) ([]phase, time.Duration, error) {
	var out []phase
	var total time.Duration
	for _, part := range strings.Split(s, ",") {
		rd := strings.SplitN(strings.TrimSpace(part), ":", 2)
		if len(rd) != 2 {
			return nil, 0, fmt.Errorf("phase %q: want rate:duration", part)
		}
		r, err := strconv.ParseFloat(rd[0], 64)
		if err != nil {
			return nil, 0, err
		}
		d, err := time.ParseDuration(rd[1])
		if err != nil {
			return nil, 0, err
		}
		total += d
		out = append(out, phase{rate: r, until: total})
	}
	return out, total, nil
}

func main() {
	flag.StringVar(&cfg.Mode, "mode", "http", "http | echo | ws | hold")
	flag.StringVar(&cfg.Host, "host", "127.0.0.1", "server host")
	flag.IntVar(&cfg.Port, "port", 8000, "server port")
	flag.StringVar(&cfg.Path, "path", "/", "request path")
	flag.StringVar(&cfg.Method, "method", "", "HTTP method (default GET, or POST with -body)")
	flag.IntVar(&cfg.Conns, "c", 64, "connections")
	flag.Float64Var(&cfg.DurationS, "d", 10, "measured seconds")
	flag.Float64Var(&cfg.WarmupS, "warmup", 2, "warmup seconds (not measured)")
	flag.Float64Var(&cfg.TimeoutS, "timeout", 5, "per-request timeout seconds")
	flag.Float64Var(&cfg.Rate, "rate", 0, "open-loop total requests/s (0 = closed loop)")
	flag.StringVar(&cfg.Phases, "phases", "", "open-loop rate phases rate:dur,... (overrides -rate/-d/-warmup)")
	flag.IntVar(&cfg.Pipeline, "pipeline", 1, "HTTP requests written per round trip")
	flag.IntVar(&cfg.Body, "body", 0, "request body bytes")
	flag.IntVar(&cfg.ExpectCode, "expect-status", 200, "required status (0 = any)")
	flag.IntVar(&cfg.ExpectLen, "expect-len", -1, "required response body length (-1 = any)")
	flag.StringVar(&cfg.ExpectBody, "expect-body", "", "required response body")
	flag.BoolVar(&cfg.Churn, "churn", false, "one connection per request")
	flag.BoolVar(&cfg.TLS, "tls", false, "TLS (certificate not verified)")
	flag.IntVar(&cfg.Size, "size", 1024, "echo/ws message bytes")
	flag.IntVar(&cfg.Window, "window", 1, "echo/ws messages in flight per connection")
	flag.BoolVar(&cfg.Text, "text", false, "ws text frames")
	flag.IntVar(&cfg.Procs, "procs", 0, "GOMAXPROCS (0 = default)")
	flag.BoolVar(&cfg.Series, "series", false, "emit a per-second time series")
	flag.BoolVar(&cfg.HoldRequest, "hold-request", false, "hold: one request per connection first")
	flag.BoolVar(&verbose, "v", false, "print the first errors")
	flag.Parse()
	verboseLeft.Store(5)
	if cfg.Procs > 0 {
		runtime.GOMAXPROCS(cfg.Procs)
	}
	cfg.Procs = runtime.GOMAXPROCS(0)
	if cfg.Method == "" {
		cfg.Method = "GET"
		if cfg.Body > 0 {
			cfg.Method = "POST"
		}
	}
	buildRequest()

	total := time.Duration((cfg.WarmupS + cfg.DurationS) * float64(time.Second))
	if cfg.Phases != "" {
		var err error
		phases, total, err = parsePhases(cfg.Phases)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		cfg.WarmupS = 0
		cfg.DurationS = total.Seconds()
		cfg.Series = true
	}
	if cfg.Series {
		series = make([]*secondBucket, int(total/time.Second)+2)
		for i := range series {
			series[i] = &secondBucket{}
		}
	}

	st := newStats()
	loadStart = now()
	measureFrom = loadStart + int64(cfg.WarmupS*1e9)
	measureTo = loadStart + int64(total)

	var u0, k0 float64
	var workers []*Stats
	if cfg.Mode == "hold" {
		measureFrom, measureTo = loadStart, loadStart+int64(24*time.Hour)
		u0, k0 = cpuTimes()
		hold(st)
		workers = append(workers, st)
	} else {
		var wg sync.WaitGroup
		for i := 0; i < cfg.Conns; i++ {
			ws := newStats()
			workers = append(workers, ws)
			wg.Add(1)
			switch cfg.Mode {
			case "http":
				go httpWorker(i, ws, &wg)
			case "echo", "ws":
				go streamWorker(i, ws, &wg)
			default:
				fmt.Fprintln(os.Stderr, "unknown -mode", cfg.Mode)
				os.Exit(2)
			}
		}
		time.Sleep(time.Duration(measureFrom - now()))
		u0, k0 = cpuTimes()
		time.Sleep(time.Duration(measureTo - now()))
		stopping.Store(true)
		u1, k1 := cpuTimes()
		// Workers finish the request they are in (bounded by the timeout);
		// completions after measureTo are not counted.
		finished := make(chan struct{})
		go func() { wg.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(timeout() + 2*time.Second):
			for _, w := range workers {
				w.incomplete++
			}
		}
		u0, k0 = u1-u0, k1-k0
	}
	if cfg.Mode == "hold" {
		u1, k1 := cpuTimes()
		u0, k0 = u1-u0, k1-k0
	}

	agg := newStats()
	for _, w := range workers {
		agg.hist.Merge(&w.hist)
		agg.ok += w.ok
		agg.bytesIn += w.bytesIn
		agg.bytesOut += w.bytesOut
		agg.bodyBytes += w.bodyBytes
		agg.late += w.late
		agg.reconnects += w.reconnects
		agg.serverCloses += w.serverCloses
		agg.incomplete += w.incomplete
		for k, v := range w.errs {
			agg.errs[k] += v
		}
	}
	measured := cfg.DurationS
	us := func(ns uint64) float64 { return float64(ns) / 1e3 }
	var nerr uint64
	for _, v := range agg.errs {
		nerr += v
	}
	res := map[string]any{
		"config":        cfg,
		"measured_s":    measured,
		"ok":            agg.ok,
		"errors":        agg.errs,
		"error_total":   nerr,
		"late_sends":    agg.late,
		"reconnects":    agg.reconnects,
		"server_closes": agg.serverCloses,
		"incomplete":    agg.incomplete,
		"rate":          float64(agg.ok) / measured,
		"bytes_in":      agg.bytesIn,
		"bytes_out":     agg.bytesOut,
		"body_bytes":    agg.bodyBytes,
		"in_mbps":       float64(agg.bytesIn) * 8 / 1e6 / measured,
		"latency_us": map[string]float64{
			"p50":  us(agg.hist.Quantile(0.50)),
			"p90":  us(agg.hist.Quantile(0.90)),
			"p99":  us(agg.hist.Quantile(0.99)),
			"p999": us(agg.hist.Quantile(0.999)),
			"max":  us(agg.hist.max),
			"min":  us(agg.hist.min),
			"mean": agg.hist.Mean() / 1e3,
		},
		"hist_ns":      agg.hist.Sparse(),
		"client_cpu_s": map[string]float64{"user": u0, "kernel": k0},
	}
	if series != nil {
		var ts []map[string]any
		for i, b := range series {
			h := b.h.Snapshot()
			ts = append(ts, map[string]any{
				"t": i, "ok": h.n, "errors": atomic.LoadUint64(&b.errs),
				"p50_us": us(h.Quantile(0.5)), "p99_us": us(h.Quantile(0.99)), "max_us": us(h.max),
			})
		}
		res["series"] = ts
	}
	out, _ := json.Marshal(res)
	fmt.Printf("RESULT %s\n", out)
}
