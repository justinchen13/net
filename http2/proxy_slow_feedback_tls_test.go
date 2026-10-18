// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2/hpack"
)

// This is a deterministic causal ordering check, not a latency benchmark.
// A seed DATA/flush must actually complete through certificate-verified TLS
// before the server's normal completion path can enable the pressure window.
// An atomic fake clock advances only while that real socket Write is gated;
// no test calls proxyWriteCompleted/applyProxyWriteFeedback directly. A second
// gate then reproduces DATA arriving after bulk selection. Clock duration is
// synthetic, while the I/O, server completion, received frames and SHA are real.
func TestProxySlowWriteFeedbackLateDATARealTLS(t *testing.T) {
	for _, tc := range []struct {
		name        string
		seedBytes   int
		elapsed     time.Duration
		wantQuantum int
	}{
		{"successful_DATA_at_threshold", 32768, 100 * time.Millisecond, 4096},
		{"successful_flush_at_threshold", 512, 100 * time.Millisecond, 4096},
		{"successful_DATA_below_threshold", 32768, 100*time.Millisecond - time.Nanosecond, 65536},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate, seedGate := newLateDataGate(), newLateDataGate()
			defer gate.unblock()
			defer seedGate.unblock()
			seedStart := make(chan struct{})
			notices := make(chan feedbackTLSNotice, 1)
			var clockNS atomic.Int64
			var seedReturned atomic.Bool
			epoch := time.Unix(1700000000, 0)
			clock := func() time.Time { return epoch.Add(time.Duration(clockNS.Load())) }
			bulkStart, smallStart, othersFinish := make(chan struct{}), make(chan struct{}), make(chan struct{})
			payloads := map[uint32][]byte{
				1: bytes.Repeat([]byte{0x6b}, 65536),
				3: bytes.Repeat([]byte{0x73}, 578),
				5: bytes.Repeat([]byte{0x70}, tc.seedBytes), 7: []byte("stream-seven"),
				9: []byte("stream-nine"), 11: []byte("stream-eleven"),
			}
			ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var id uint32
				if _, err := fmt.Sscanf(r.URL.Path, "/%d", &id); err != nil || payloads[id] == nil {
					http.Error(w, "bad fixture path", 400)
					return
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(payloads[id])))
				w.WriteHeader(200)
				if err := http.NewResponseController(w).Flush(); err != nil {
					return
				}
				start := othersFinish
				if id == 1 {
					start = bulkStart
				}
				if id == 3 {
					start = smallStart
				}
				if id == 5 {
					start = seedStart
				}
				select {
				case <-start:
				case <-r.Context().Done():
					return
				}
				if _, err := w.Write(payloads[id]); err != nil {
					return
				}
				_ = http.NewResponseController(w).Flush()
				if id == 5 {
					select {
					case <-othersFinish:
					case <-r.Context().Done():
					}
				}
			}))
			ts.Listener = &feedbackTLSListener{Listener: ts.Listener, seed: seedGate, later: gate, seedReturned: &seedReturned}
			ts.EnableHTTP2 = true
			if err := ConfigureServer(ts.Config, &Server{NewWriteScheduler: func() WriteScheduler {
				return &feedbackTLSScheduler{
					proxySlowWriteScheduler: newProxySlowWriteScheduler(clock),
					seed:                    seedGate, later: gate, notices: notices, seedReturned: &seedReturned,
				}
			}}); err != nil {
				t.Fatal(err)
			}
			ts.StartTLS()
			defer ts.Close()
			roots := x509.NewCertPool()
			roots.AddCert(ts.Certificate())
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", ts.Listener.Addr().String(), &tls.Config{
				RootCAs: roots, NextProtos: []string{NextProtoTLS}, MinVersion: tls.VersionTLS13,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			defer gate.unblock() // Release gates before client close / server shutdown.
			defer seedGate.unblock()
			if conn.ConnectionState().NegotiatedProtocol != NextProtoTLS || len(conn.ConnectionState().VerifiedChains) == 0 {
				t.Fatal("verified TLS and h2 required")
			}
			if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(conn, ClientPreface); err != nil {
				t.Fatal(err)
			}
			fw := NewFramer(conn, nil)
			if err := fw.WriteSettings(Setting{ID: SettingInitialWindowSize, Val: 1 << 20}, Setting{ID: SettingMaxFrameSize, Val: 16777215}); err != nil {
				t.Fatal(err)
			}
			if err := fw.WriteWindowUpdate(0, 1<<20); err != nil {
				t.Fatal(err)
			}
			var block bytes.Buffer
			enc := hpack.NewEncoder(&block)
			for _, id := range []uint32{1, 3, 5, 7, 9, 11} {
				block.Reset()
				for _, h := range []hpack.HeaderField{
					{Name: ":method", Value: "GET"}, {Name: ":scheme", Value: "https"},
					{Name: ":authority", Value: ts.Listener.Addr().String()}, {Name: ":path", Value: fmt.Sprintf("/%d", id)},
				} {
					if err := enc.WriteField(h); err != nil {
						t.Fatal(err)
					}
				}
				if err := fw.WriteHeaders(HeadersFrameParam{StreamID: id, BlockFragment: block.Bytes(), EndHeaders: true, EndStream: true}); err != nil {
					t.Fatal(err)
				}
			}
			headersReady, smallReceived := make(chan struct{}), make(chan struct{})
			result := make(chan lateDataReadResult, 1)
			go func() {
				out := lateDataReadResult{data: make(map[uint32][]byte)}
				defer func() { result <- out }()
				fr := NewFramer(nil, conn)
				fr.SetMaxReadFrameSize(16777215)
				headers, ended := make(map[uint32]bool), make(map[uint32]bool)
				for len(ended) != len(payloads) {
					f, err := fr.ReadFrame()
					if err != nil {
						out.err = err
						return
					}
					switch f := f.(type) {
					case *HeadersFrame:
						if payloads[f.StreamID] == nil {
							out.err = fmt.Errorf("unexpected headers stream %d", f.StreamID)
							return
						}
						if !headers[f.StreamID] {
							headers[f.StreamID] = true
							if len(headers) == len(payloads) {
								close(headersReady)
							}
						}
						if f.StreamEnded() {
							ended[f.StreamID] = true
						}
					case *DataFrame:
						if payloads[f.StreamID] == nil {
							out.err = fmt.Errorf("unexpected stream %d", f.StreamID)
							return
						}
						out.data[f.StreamID] = append(out.data[f.StreamID], f.Data()...)
						if f.StreamID == 1 && !out.smallComplete {
							out.bulkBeforeSmall += len(f.Data())
						}
						if f.StreamID == 3 && len(out.data[3]) == len(payloads[3]) && !out.smallComplete {
							out.smallComplete = true
							close(smallReceived)
						}
						if f.StreamEnded() {
							ended[f.StreamID] = true
						}
					case *RSTStreamFrame:
						out.err = fmt.Errorf("unexpected reset %d: %v", f.StreamID, f.ErrCode)
						return
					case *GoAwayFrame:
						out.err = fmt.Errorf("unexpected GOAWAY %v", f.ErrCode)
						return
					}
				}
				for id := range payloads {
					if !ended[id] {
						out.err = fmt.Errorf("missing END_STREAM for %d", id)
						return
					}
				}
			}()
			wait := func(ch <-chan struct{}, what string) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
					t.Fatalf("timeout waiting for %s", what)
				}
			}
			wait(headersReady, "six response headers on one TLS connection")
			if err := fw.WriteSettingsAck(); err != nil {
				t.Fatal(err)
			}
			close(seedStart)
			wait(seedGate.entered, "seed actual TLS socket Write entered")
			select {
			case <-notices:
				t.Fatal("feedback preceded actual Write completion")
			default:
			}
			clockNS.Add(int64(tc.elapsed))
			seedGate.unblock()
			var notice feedbackTLSNotice
			select {
			case notice = <-notices:
			case <-time.After(5 * time.Second):
				t.Fatal("server completion feedback not observed")
			}
			wantKind := "data"
			if tc.seedBytes == 512 {
				wantKind = "flush"
			}
			wantPressure := tc.wantQuantum == proxySlowWriteQuantum
			if !notice.rawWriteReturned || notice.elapsed != tc.elapsed || notice.kind != wantKind || notice.pressure != wantPressure {
				t.Fatalf("invalid actual-completion feedback: %+v", notice)
			}
			if int(seedGate.selectedBytes.Load()) != tc.seedBytes {
				t.Fatal("cold seed was already fragmented")
			}
			close(bulkStart)
			wait(gate.entered, "selected DATA blocked in real TLS socket write")
			close(smallStart)
			wait(gate.queued, "late competing DATA enqueued")
			select {
			case <-smallReceived:
				t.Fatal("competitor bypassed the in-progress frame")
			default:
			}
			gate.unblock()
			wait(smallReceived, "complete competing payload received")
			close(othersFinish)
			var out lateDataReadResult
			select {
			case out = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("responses did not all end")
			}
			if out.err != nil {
				t.Fatal(out.err)
			}
			for id, want := range payloads {
				if len(out.data[id]) != len(want) || sha256.Sum256(out.data[id]) != sha256.Sum256(want) {
					t.Fatalf("stream %d payload mismatch", id)
				}
			}
			wantBefore := tc.wantQuantum
			if int(gate.selectedBytes.Load()) != wantBefore || out.bulkBeforeSmall != wantBefore {
				t.Fatalf("selected=%d bulk before complete competitor=%d, want %d", gate.selectedBytes.Load(), out.bulkBeforeSmall, wantBefore)
			}
			t.Logf("verified TLS; real successful %s completion triggered pressure=%v; six open streams; late 578B complete after %d bulk bytes; all SHA/END_STREAM verified; fake clock only, no latency claim", notice.kind, notice.pressure, out.bulkBeforeSmall)
		})
	}
}

type feedbackTLSNotice struct {
	elapsed                    time.Duration
	kind                       string
	pressure, rawWriteReturned bool
}

type feedbackTLSScheduler struct {
	*proxySlowWriteScheduler
	seed, later  *lateDataGate
	notices      chan<- feedbackTLSNotice
	seedReturned *atomic.Bool
	reported     bool // serve goroutine only
}

func (s *feedbackTLSScheduler) Pop() (FrameWriteRequest, bool) {
	wr, ok := s.proxySlowWriteScheduler.Pop()
	if ok && wr.DataSize() > 0 {
		var gate *lateDataGate
		if wr.StreamID() == 5 {
			gate = s.seed
		}
		if wr.StreamID() == 1 {
			gate = s.later
		}
		if gate != nil {
			gate.selected.Do(func() { gate.selectedBytes.Store(int32(wr.DataSize())); gate.armed.Store(true) })
		}
	}
	return wr, ok
}
func (s *feedbackTLSScheduler) Push(wr FrameWriteRequest) {
	s.proxySlowWriteScheduler.Push(wr)
	if wr.StreamID() == 3 && wr.DataSize() > 0 {
		s.later.queuedOnce.Do(func() { close(s.later.queued) })
	}
}
func (s *feedbackTLSScheduler) proxyWriteCompleted(wr FrameWriteRequest, elapsed time.Duration, completed time.Time) {
	// This wrapper is called only by the server completion path. It delegates
	// policy before reporting a copied notice; no test mutates policy state.
	s.proxySlowWriteScheduler.proxyWriteCompleted(wr, elapsed, completed)
	if s.reported || elapsed <= 0 {
		return
	}
	kind := ""
	switch wr.write.(type) {
	case *writeData:
		if wr.StreamID() == 5 {
			kind = "data"
		}
	case flushFrameWriter:
		kind = "flush"
	}
	if kind == "" {
		return
	}
	s.reported = true
	s.notices <- feedbackTLSNotice{elapsed: elapsed, kind: kind, pressure: !s.slowUntil.IsZero(), rawWriteReturned: s.seedReturned.Load()}
}

type feedbackTLSListener struct {
	net.Listener
	seed, later  *lateDataGate
	seedReturned *atomic.Bool
}

func (l *feedbackTLSListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &feedbackTLSConn{Conn: c, seed: l.seed, later: l.later, seedReturned: l.seedReturned}, nil
}

type feedbackTLSConn struct {
	net.Conn
	seed, later  *lateDataGate
	seedReturned *atomic.Bool
}

func (c *feedbackTLSConn) Write(p []byte) (int, error) {
	if c.seed.armed.CompareAndSwap(true, false) {
		close(c.seed.entered)
		<-c.seed.release
		n, err := c.Conn.Write(p)
		if err == nil && n == len(p) {
			c.seedReturned.Store(true)
		}
		return n, err
	}
	if c.later.armed.CompareAndSwap(true, false) {
		close(c.later.entered)
		<-c.later.release
	}
	return c.Conn.Write(p)
}
