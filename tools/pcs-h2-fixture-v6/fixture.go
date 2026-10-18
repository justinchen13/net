// Copyright 2026 The PCS Authors.
// SPDX-License-Identifier: BSD-3-Clause
// Private synthetic laboratory fixture. No production credentials or destinations.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

func integer(name string, fallback, low, high int) int {
	s := os.Getenv(name)
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < low || n > high {
		panic("invalid " + name)
	}
	return n
}

// Explicit enumeration: both arms run this same binary, with no implicit default.
func parseUploadDelay(value string) (time.Duration, error) {
	switch value {
	case "0":
		return 0, nil
	case "5":
		return 5 * time.Second, nil
	default:
		return 0, fmt.Errorf("UPLOAD_DELAY_SECONDS must be exactly 0 or 5")
	}
}

func pattern(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i % 251)
	}
	return p
}
func emit(v any) {
	if err := json.NewEncoder(os.Stdout).Encode(v); err != nil {
		panic(err)
	}
}
func percentile(v []float64, p float64) any {
	if len(v) == 0 {
		return nil
	}
	w := append([]float64(nil), v...)
	sort.Float64s(w)
	return w[max(0, int(math.Ceil(float64(len(w))*p))-1)]
}

// Count only frame headers decrypted by the real TLS client. Retain no payload.
type frameEvent struct {
	Stream         uint32  `json:"stream_id"`
	Type           byte    `json:"type"`
	Flags          byte    `json:"flags"`
	Length         int     `json:"length"`
	HeaderOffset   float64 `json:"header_offset_seconds"`
	CompleteOffset float64 `json:"complete_offset_seconds"`
}

type frameConn struct {
	net.Conn
	mu                                                     sync.Mutex
	header                                                 [9]byte
	headerN, remaining                                     int
	dataFrames, dataBytes, maxData, wuStream, wuConnection int64
	tx                                                     txObserver
	epoch                                                  time.Time
	frames                                                 []frameEvent
	framesTruncated                                        bool
	frameStarted                                           time.Time
}

func (c *frameConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.mu.Lock()
	defer c.mu.Unlock()
	for b := p[:n]; len(b) > 0; {
		if c.remaining > 0 {
			k := min(c.remaining, len(b))
			c.remaining -= k
			b = b[k:]
			if c.remaining == 0 {
				c.completedFrame()
			}
			continue
		}
		k := copy(c.header[c.headerN:], b)
		c.headerN += k
		b = b[k:]
		if c.headerN < 9 {
			continue
		}
		c.remaining = int(c.header[0])<<16 | int(c.header[1])<<8 | int(c.header[2])
		c.frameStarted = time.Now()
		if c.header[3] == 0 {
			c.dataFrames++
			c.dataBytes += int64(c.remaining)
			c.maxData = max(c.maxData, int64(c.remaining))
		}
		if c.header[3] == 8 && c.remaining == 4 {
			id := uint32(c.header[5]&0x7f)<<24 | uint32(c.header[6])<<16 | uint32(c.header[7])<<8 | uint32(c.header[8])
			if id == 0 {
				c.wuConnection++
			} else {
				c.wuStream++
			}
		}
		if c.remaining == 0 {
			c.completedFrame()
		}
		c.headerN = 0
	}
	return n, err
}
func (c *frameConn) resetCounts() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dataFrames = 0
	c.dataBytes = 0
	c.maxData = 0
	c.wuStream = 0
	c.wuConnection = 0
}

type lane struct {
	id     int
	body   io.ReadCloser
	writer *io.PipeWriter
	reader *bufio.Reader
	cancel context.CancelFunc
}

func (l *lane) Close() { l.cancel(); l.writer.Close(); l.body.Close() }

type transfer struct {
	DueOffsetSeconds           float64 `json:"due_offset_seconds"`
	TimerWakeOffsetSeconds     float64 `json:"timer_wake_offset_seconds"`
	ScheduledToCompleteSeconds float64 `json:"scheduled_to_complete_seconds"`
	EndStreamVerified          bool    `json:"end_stream_verified"`
	LaneID                     int     `json:"lane_id"`
	Kind                       string  `json:"kind"`
	Bytes                      int64   `json:"client_received_bytes"`
	UploadBytes                int64   `json:"upload_origin_verified_bytes"`
	Seconds                    float64 `json:"seconds"`
	StartOffsetSeconds         float64 `json:"start_offset_seconds"`
	EndOffsetSeconds           float64 `json:"end_offset_seconds"`
	Verified                   bool    `json:"verified"`
	Error                      string  `json:"error,omitempty"`
}

func request(l *lane, path string, body []byte, expected [32]byte, expectedN int, kind string, epoch time.Time) transfer {
	start := time.Now()
	r := transfer{LaneID: l.id, Kind: kind, StartOffsetSeconds: start.Sub(epoch).Seconds()}
	var reader io.Reader
	method := "GET"
	if body != nil {
		reader = bytes.NewReader(body)
		method = "POST"
	}
	req, err := http.NewRequest(method, "http://127.0.0.1:18080"+path, reader)
	if err == nil {
		err = req.Write(l.writer)
	}
	var resp *http.Response
	if err == nil {
		resp, err = http.ReadResponse(l.reader, req)
	}
	if err == nil {
		h := sha256.New()
		r.Bytes, err = io.Copy(h, io.LimitReader(resp.Body, int64(expectedN)+1))
		resp.Body.Close()
		if err == nil && (resp.StatusCode != 200 || r.Bytes != int64(expectedN) || !bytes.Equal(h.Sum(nil), expected[:])) {
			err = fmt.Errorf("invalid status, length, or checksum")
		}
		if err == nil && body != nil {
			n, e := strconv.ParseInt(resp.Header.Get("X-Lab-Verified-Upload-Bytes"), 10, 64)
			if e != nil || n != int64(len(body)) {
				err = fmt.Errorf("origin upload checksum/length failed")
			} else {
				r.UploadBytes = n
			}
		}
	}
	r.Seconds = time.Since(start).Seconds()
	r.EndOffsetSeconds = time.Since(epoch).Seconds()
	if kind != "prime" && kind != "warmup" && err == nil {
		l.writer.Close()
		_, endErr := l.reader.ReadByte()
		r.EndStreamVerified = endErr == io.EOF
		if !r.EndStreamVerified {
			err = fmt.Errorf("expected tunnel END_STREAM/EOF, got %v", endErr)
		}
	}
	r.Verified = err == nil
	if err != nil {
		r.Error = err.Error()
	}
	return r
}

// Outgoing SETTINGS/WINDOW_UPDATE parser retains only protocol control bytes.
// It consumes the real Transport preface, never modifies the client algorithm.
type txObserver struct {
	Preface                          int
	Header                           [9]byte
	HeaderN, Remaining, Length       int
	Payload                          []byte
	InitialSettings                  map[uint16]uint32
	ConnectionWindowAdded            uint64
	InitialConnectionWindowIncrement uint32
	InitialConnectionWindowObserved  bool
	ParseError                       bool
}

func (t *txObserver) consume(b []byte) {
	const preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	for len(b) > 0 {
		if t.Preface < len(preface) {
			if b[0] != preface[t.Preface] {
				t.ParseError = true
			}
			t.Preface++
			b = b[1:]
			continue
		}
		if t.Remaining > 0 {
			n := min(t.Remaining, len(b))
			if t.Header[3] == 4 || t.Header[3] == 8 {
				if len(t.Payload)+n <= 256 {
					t.Payload = append(t.Payload, b[:n]...)
				} else {
					t.ParseError = true
				}
			}
			t.Remaining -= n
			b = b[n:]
			if t.Remaining == 0 {
				t.complete()
			}
			continue
		}
		n := copy(t.Header[t.HeaderN:], b)
		t.HeaderN += n
		b = b[n:]
		if t.HeaderN < 9 {
			continue
		}
		t.Length = int(t.Header[0])<<16 | int(t.Header[1])<<8 | int(t.Header[2])
		t.Remaining = t.Length
		t.HeaderN = 0
		t.Payload = t.Payload[:0]
		if t.Remaining == 0 {
			t.complete()
		}
	}
}
func (t *txObserver) complete() {
	stream := uint32(t.Header[5]&127)<<24 | uint32(t.Header[6])<<16 | uint32(t.Header[7])<<8 | uint32(t.Header[8])
	if t.Header[3] == 4 && t.Header[4]&1 == 0 {
		if t.Length%6 != 0 || len(t.Payload) != t.Length {
			t.ParseError = true
			return
		}
		if t.InitialSettings == nil {
			t.InitialSettings = map[uint16]uint32{}
			for b := t.Payload; len(b) > 0; b = b[6:] {
				id := uint16(b[0])<<8 | uint16(b[1])
				v := uint32(b[2])<<24 | uint32(b[3])<<16 | uint32(b[4])<<8 | uint32(b[5])
				t.InitialSettings[id] = v
			}
		}
	}
	if t.Header[3] == 8 && stream == 0 {
		if len(t.Payload) != 4 {
			t.ParseError = true
			return
		}
		increment := uint32(t.Payload[0]&127)<<24 | uint32(t.Payload[1])<<16 | uint32(t.Payload[2])<<8 | uint32(t.Payload[3])
		if !t.InitialConnectionWindowObserved {
			t.InitialConnectionWindowIncrement = increment
			t.InitialConnectionWindowObserved = true
		}
		t.ConnectionWindowAdded += uint64(increment)
	}
}
func (c *frameConn) Write(p []byte) (int, error) {
	n, e := c.Conn.Write(p)
	c.mu.Lock()
	c.tx.consume(p[:n])
	c.mu.Unlock()
	return n, e
}
func (c *frameConn) completedFrame() {
	if c.epoch.IsZero() {
		return
	}
	if len(c.frames) >= 8192 {
		c.framesTruncated = true
		return
	}
	c.frames = append(c.frames, frameEvent{Stream: uint32(c.header[5]&127)<<24 | uint32(c.header[6])<<16 | uint32(c.header[7])<<8 | uint32(c.header[8]), Type: c.header[3], Flags: c.header[4], Length: int(c.header[0])<<16 | int(c.header[1])<<8 | int(c.header[2]), HeaderOffset: c.frameStarted.Sub(c.epoch).Seconds(), CompleteOffset: time.Since(c.epoch).Seconds()})
}
func (c *frameConn) measured(epoch time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch = epoch
	c.frames = nil
	c.framesTruncated = false
	c.dataFrames = 0
	c.dataBytes = 0
	c.maxData = 0
	c.wuStream = 0
	c.wuConnection = 0
}
func (c *frameConn) metadata() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]any{"data_frames": c.dataFrames, "data_frame_bytes": c.dataBytes, "max_data_frame_bytes": c.maxData, "stream_window_updates_received": c.wuStream, "connection_window_updates_received": c.wuConnection, "transmitted_initial_settings": c.tx.InitialSettings, "transmitted_connection_window_added": c.tx.ConnectionWindowAdded, "initial_connection_window_increment_observed": c.tx.InitialConnectionWindowObserved, "initial_connection_window_increment": c.tx.InitialConnectionWindowIncrement, "initial_connection_receive_credit": uint64(65535) + uint64(c.tx.InitialConnectionWindowIncrement), "transmit_control_parse_error": c.tx.ParseError, "received_frame_headers": append([]frameEvent(nil), c.frames...), "frame_trace_truncated": c.framesTruncated}
}

func originMain(bulkBytes, smallBytes, uploadBytes int) {
	bulk, small, upload, warm, prime := pattern(bulkBytes), pattern(smallBytes), pattern(uploadBytes), pattern(262144), pattern(1)
	uploadHash := sha256.Sum256(upload)
	server := &http.Server{ReadHeaderTimeout: 10 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p []byte
		switch r.URL.Path {
		case "/prime":
			p = prime
		case "/warmup":
			p = warm
		case "/bulk":
			p = bulk
		case "/small":
			p = small
		case "/upload":
			h := sha256.New()
			n, e := io.Copy(h, io.LimitReader(r.Body, int64(uploadBytes)+1))
			if e != nil || n != int64(uploadBytes) || !bytes.Equal(h.Sum(nil), uploadHash[:]) {
				http.Error(w, "upload mismatch", 400)
				return
			}
			w.Header().Set("X-Lab-Verified-Upload-Bytes", strconv.Itoa(uploadBytes))
			p = small
		default:
			http.NotFound(w, r)
			return
		}
		if r.URL.Path != "/prime" && r.URL.Path != "/warmup" {
			w.Header().Set("Connection", "close")
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(p)))
		_, _ = w.Write(p)
	})}
	ln, e := net.Listen("tcp4", "127.0.0.1:18080")
	if e != nil {
		panic(e)
	}
	emit(map[string]any{"event": "origin_ready", "utc": time.Now().UTC().Format(time.RFC3339Nano)})
	if e = server.Serve(ln); e != nil && e != http.ErrServerClosed {
		panic(e)
	}
}

func main() {
	bulkN := integer("BULK_COUNT", 3, 1, 4)
	bulkBytes := integer("BULK_BYTES", 524288, 1, 8388608)
	smallN := integer("SMALL_COUNT", 40, 40, 40)
	smallBytes := integer("SMALL_BYTES", 1099, 1099, 1099)
	smallConcurrency := integer("SMALL_CONCURRENCY", 4, 4, 4)
	smallDelay := integer("SMALL_DELAY_MS", 1000, 0, 5000)
	smallSpacing := integer("SMALL_SPACING_MS", 250, 1, 1000)
	uploadN := 1
	uploadBytes := 65536
	uploadDelay, delayErr := parseUploadDelay(os.Getenv("UPLOAD_DELAY_SECONDS"))
	if delayErr != nil {
		panic(delayErr)
	}
	if os.Getenv("FIXTURE_ROLE") == "origin" {
		originMain(bulkBytes, smallBytes, uploadBytes)
		return
	}
	if os.Getenv("FIXTURE_ROLE") != "client" {
		panic("explicit fixture role required")
	}
	if os.Getenv("PROXY_ADDRESS") != "192.0.2.1:8443" {
		panic("exact private endpoint required")
	}
	raw, e := os.ReadFile(os.Getenv("LAB_AUTH_FILE"))
	if e != nil {
		panic(e)
	}
	var credentials struct {
		User     string `json:"user"`
		Password string `json:"password"`
	}
	if json.Unmarshal(raw, &credentials) != nil || credentials.User == "" || credentials.Password == "" {
		panic("invalid synthetic credentials")
	}
	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(credentials.User+":"+credentials.Password))
	cert, e := os.ReadFile(os.Getenv("LAB_CA_FILE"))
	if e != nil {
		panic(e)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert) {
		panic("bad private CA")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	dialer := tls.Dialer{Config: &tls.Config{RootCAs: roots, ServerName: "proxy.lab.invalid", NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS12}}
	c, e := dialer.DialContext(ctx, "tcp4", os.Getenv("PROXY_ADDRESS"))
	if e != nil {
		panic(e)
	}
	if c.(*tls.Conn).ConnectionState().NegotiatedProtocol != "h2" {
		panic("ALPN mismatch")
	}
	fc := &frameConn{Conn: c}
	cc, e := (&http2.Transport{MaxReadFrameSize: 16777215, StrictMaxConcurrentStreams: true}).NewClientConn(fc)
	if e != nil {
		panic(e)
	}
	defer cc.Close()
	go func() { <-ctx.Done(); cc.Close() }()
	setupStart := time.Now()
	lanes := make([]*lane, bulkN+smallN+uploadN)
	errs := make(chan error, len(lanes))
	var prep sync.WaitGroup
	sem := make(chan struct{}, 16)
	primeHash := sha256.Sum256(pattern(1))
	for i := range lanes {
		prep.Add(1)
		go func(i int) {
			defer prep.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			lc, lcancel := context.WithCancel(ctx)
			pr, pw := io.Pipe()
			req := (&http.Request{Method: "CONNECT", URL: &url.URL{Scheme: "https", Host: "127.0.0.1:18080"}, Host: "127.0.0.1:18080", Header: http.Header{"Proxy-Authorization": []string{auth}, "X-Lab-Lane": []string{strconv.Itoa(i)}}, Body: pr}).WithContext(lc)
			resp, err := cc.RoundTrip(req)
			if err != nil {
				lcancel()
				pw.Close()
				errs <- err
				return
			}
			if resp.StatusCode != 200 {
				lcancel()
				pw.Close()
				resp.Body.Close()
				errs <- fmt.Errorf("CONNECT rejected")
				return
			}
			l := &lane{id: i, body: resp.Body, writer: pw, reader: bufio.NewReader(resp.Body), cancel: lcancel}
			lanes[i] = l
			v := request(l, "/prime", nil, primeHash, 1, "prime", setupStart)
			if !v.Verified {
				errs <- fmt.Errorf("prime failed")
			}
		}(i)
	}
	prep.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			panic(err)
		}
	}
	defer func() {
		for _, l := range lanes {
			if l != nil {
				l.Close()
			}
		}
	}()
	emit(map[string]any{"event": "idle_probe_begin", "utc": time.Now().UTC().Format(time.RFC3339Nano), "outer_connections": 1, "preconnected_tunnels": len(lanes), "settings": fc.metadata()})
	pingEpoch := time.Now()
	pings := make([]map[string]any, 0, 20)
	for i := 0; i < 20; i++ {
		due := pingEpoch.Add(time.Duration(i) * 250 * time.Millisecond)
		if d := time.Until(due); d > 0 {
			time.Sleep(d)
		}
		start := time.Now()
		pc, pcancel := context.WithTimeout(ctx, 2*time.Second)
		err := cc.Ping(pc)
		pcancel()
		pings = append(pings, map[string]any{"index": i, "started_utc": start.UTC().Format(time.RFC3339Nano), "elapsed_ms": time.Since(start).Seconds() * 1000, "ok": err == nil})
		if err != nil {
			emit(map[string]any{"event": "idle_probe_failed", "pings": pings})
			panic("idle PING failed")
		}
	}
	emit(map[string]any{"event": "idle_probe_result", "pings": pings, "utc": time.Now().UTC().Format(time.RFC3339Nano)})
	warm := request(lanes[0], "/warmup", nil, sha256.Sum256(pattern(262144)), 262144, "warmup", setupStart)
	if !warm.Verified {
		panic("warmup failed")
	}
	time.Sleep(time.Second)
	emit(map[string]any{"event": "ready", "utc": time.Now().UTC().Format(time.RFC3339Nano), "outer_connections": 1, "preconnected_and_primed_tunnels": len(lanes), "warmup_lane_id": 0, "warmup_bytes": 262144, "warmup": warm, "tls_chain_and_name_verified": true, "alpn": "h2", "settings": fc.metadata(), "setup_seconds": time.Since(setupStart).Seconds()})
	scan := bufio.NewScanner(os.Stdin)
	if !scan.Scan() || strings.TrimSpace(scan.Text()) != "go" {
		panic("GO barrier required")
	}
	epoch := time.Now()
	fc.measured(epoch)
	emit(map[string]any{"event": "go", "utc": epoch.UTC().Format(time.RFC3339Nano)})
	deadline := time.AfterFunc(60*time.Second, func() { cancel(); cc.Close() })
	defer deadline.Stop()
	out := make(chan transfer, len(lanes))
	var work sync.WaitGroup
	bulkHash, smallHash := sha256.Sum256(pattern(bulkBytes)), sha256.Sum256(pattern(smallBytes))
	upload := pattern(uploadBytes)
	for i := 0; i < bulkN; i++ {
		work.Add(1)
		go func(i int) {
			defer work.Done()
			v := request(lanes[i], "/bulk", nil, bulkHash, bulkBytes, "bulk", epoch)
			v.ScheduledToCompleteSeconds = v.EndOffsetSeconds
			out <- v
		}(i)
	}
	smallSem := make(chan struct{}, smallConcurrency)
	for i := 0; i < smallN; i++ {
		work.Add(1)
		go func(i int) {
			defer work.Done()
			due := time.Duration(smallDelay+i*smallSpacing) * time.Millisecond
			if d := time.Until(epoch.Add(due)); d > 0 {
				time.Sleep(d)
			}
			woke := time.Since(epoch).Seconds()
			smallSem <- struct{}{}
			defer func() { <-smallSem }()
			v := request(lanes[bulkN+i], "/small", nil, smallHash, smallBytes, "small", epoch)
			v.DueOffsetSeconds = due.Seconds()
			v.TimerWakeOffsetSeconds = woke
			v.ScheduledToCompleteSeconds = v.EndOffsetSeconds - due.Seconds()
			out <- v
		}(i)
	}
	work.Add(1)
	go func() {
		defer work.Done()
		if d := time.Until(epoch.Add(uploadDelay)); d > 0 {
			time.Sleep(d)
		}
		woke := time.Since(epoch).Seconds()
		v := request(lanes[bulkN+smallN], "/upload", upload, smallHash, smallBytes, "upload", epoch)
		v.DueOffsetSeconds = uploadDelay.Seconds()
		v.TimerWakeOffsetSeconds = woke
		v.ScheduledToCompleteSeconds = v.EndOffsetSeconds - uploadDelay.Seconds()
		out <- v
	}()
	work.Wait()
	if !deadline.Stop() {
		panic("workload deadline fired")
	}
	close(out)
	rows := []transfer{}
	var smallTimes, scheduledTimes []float64
	var received, uploaded, bulkReceived int64
	bulkEnd := 0.0
	failures := 0
	for v := range out {
		rows = append(rows, v)
		received += v.Bytes
		uploaded += v.UploadBytes
		if !v.Verified {
			failures++
		}
		if v.Kind == "bulk" {
			bulkReceived += v.Bytes
			bulkEnd = max(bulkEnd, v.EndOffsetSeconds)
		}
		if v.Kind == "small" && v.Verified {
			smallTimes = append(smallTimes, v.Seconds)
			scheduledTimes = append(scheduledTimes, v.ScheduledToCompleteSeconds)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].LaneID < rows[j].LaneID })
	emit(map[string]any{"event": "result", "started_utc": epoch.UTC().Format(time.RFC3339Nano), "completed_utc": time.Now().UTC().Format(time.RFC3339Nano), "seconds": time.Since(epoch).Seconds(), "outer_connections": 1, "upload_delay_seconds": uploadDelay.Seconds(), "preconnected_tunnels": len(lanes), "small_success_count": len(smallTimes), "small_p50_seconds": percentile(smallTimes, .5), "small_p95_seconds": percentile(smallTimes, .95), "small_scheduled_p95_seconds": percentile(scheduledTimes, .95), "client_received_bytes": received, "verified_upload_origin_bytes": uploaded, "bulk_received_bytes": bulkReceived, "bulk_completion_seconds": bulkEnd, "bulk_goodput_MB_per_second": float64(bulkReceived) / 1e6 / bulkEnd, "sha256_all_pass": failures == 0, "failures": failures, "transfers": rows, "frames": fc.metadata(), "measurement_scope": "Actual request Write through complete body SHA; tunnel EOF checked separately before result. Scheduled-to-complete includes client semaphore wait. Real time; no artificial gate or feedback injection."})
	if failures != 0 {
		os.Exit(1)
	}
}
