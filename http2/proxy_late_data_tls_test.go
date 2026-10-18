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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2/hpack"
)

// This is a causal ordering check, not a throughput/latency benchmark. The
// competing DATA arrives AFTER selection of the first bulk DATA. A gated real
// TLS socket holds that selected frame until the competitor is in the scheduler.
// Five other streams remain open, but only two offer DATA during the gate.
// No sleep determines the ordering and no production transport hook is added.
func TestProxyDataArrivingAfterSelectionTLS(t *testing.T) {
	for _, quantum := range []int32{0, 16384} {
		t.Run(fmt.Sprintf("quantum_%d", quantum), func(t *testing.T) {
			gate := newLateDataGate()
			defer gate.unblock()
			bulkStart, smallStart, othersFinish := make(chan struct{}), make(chan struct{}), make(chan struct{})
			payloads := map[uint32][]byte{
				1: bytes.Repeat([]byte{0x6b}, 65536),
				3: bytes.Repeat([]byte{0x73}, 578),
				5: []byte("stream-five"), 7: []byte("stream-seven"),
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
				select {
				case <-start:
				case <-r.Context().Done():
					return
				}
				if _, err := w.Write(payloads[id]); err != nil {
					return
				}
				_ = http.NewResponseController(w).Flush()
			}))
			ts.Listener = &lateDataListener{Listener: ts.Listener, gate: gate}
			ts.EnableHTTP2 = true
			if err := ConfigureServer(ts.Config, &Server{NewWriteScheduler: func() WriteScheduler {
				ws, err := NewProxyWriteSchedulerWithDataQuantum(quantum)
				if err != nil {
					panic(err)
				}
				return &lateDataScheduler{WriteScheduler: ws, gate: gate}
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
			defer gate.unblock() // Release a failed gate before waiting for server shutdown.
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
			wantBefore := 65536
			if quantum != 0 {
				wantBefore = int(quantum)
			}
			if int(gate.selectedBytes.Load()) != wantBefore || out.bulkBeforeSmall != wantBefore {
				t.Fatalf("selected=%d bulk before complete competitor=%d, want %d", gate.selectedBytes.Load(), out.bulkBeforeSmall, wantBefore)
			}
			t.Logf("verified TLS; six open streams; late 578B payload complete after %d bulk bytes; all stream SHA256 and END_STREAM verified; no latency claim", out.bulkBeforeSmall)
		})
	}
}

type lateDataGate struct {
	armed                             atomic.Bool
	selectedBytes                     atomic.Int32
	selected, queuedOnce, releaseOnce sync.Once
	entered, queued, release          chan struct{}
}

func newLateDataGate() *lateDataGate {
	return &lateDataGate{entered: make(chan struct{}), queued: make(chan struct{}), release: make(chan struct{})}
}
func (g *lateDataGate) unblock() { g.releaseOnce.Do(func() { close(g.release) }) }

type lateDataScheduler struct {
	WriteScheduler
	gate *lateDataGate
}

func (s *lateDataScheduler) Pop() (FrameWriteRequest, bool) {
	w, ok := s.WriteScheduler.Pop()
	if ok && w.StreamID() == 1 && w.DataSize() > 0 {
		s.gate.selected.Do(func() { s.gate.selectedBytes.Store(int32(w.DataSize())); s.gate.armed.Store(true) })
	}
	return w, ok
}
func (s *lateDataScheduler) Push(w FrameWriteRequest) {
	s.WriteScheduler.Push(w)
	if w.StreamID() == 3 && w.DataSize() > 0 {
		s.gate.queuedOnce.Do(func() { close(s.gate.queued) })
	}
}

type lateDataListener struct {
	net.Listener
	gate *lateDataGate
}

func (l *lateDataListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	return &lateDataConn{Conn: c, gate: l.gate}, nil
}

type lateDataConn struct {
	net.Conn
	gate *lateDataGate
}

func (c *lateDataConn) Write(p []byte) (int, error) {
	if c.gate.armed.CompareAndSwap(true, false) {
		close(c.gate.entered)
		<-c.gate.release
	}
	return c.Conn.Write(p)
}

type lateDataReadResult struct {
	data            map[uint32][]byte
	bulkBeforeSmall int
	smallComplete   bool
	err             error
}
