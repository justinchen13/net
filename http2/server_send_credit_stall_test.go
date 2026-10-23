// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build go1.25 || goexperiment.synctest

package http2

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

const creditTestTimeout = 3 * time.Second
const creditTestBodySize = 32768

// This peer exchanges actual HTTP/2 frames through ServeConn with synthetic
// net.Conn/time, not through direct scheduler Pop calls. CONNECT is half-closed
// in the upload direction so handler completion need not reset that direction.
func creditTestRequest(st *serverTester, id uint32, authority string) {
	st.writeHeaders(HeadersFrameParam{StreamID: id, EndHeaders: true, EndStream: true,
		BlockFragment: st.encodeHeaderRaw(":method", "CONNECT", ":authority", authority)})
}

func creditTestOnServe(st *serverTester, f func(*serverConn)) {
	st.sc.sendServeMsg(f)
	st.sync()
}

func creditTestDrain(st *serverTester, expected map[uint32][]byte) {
	st.t.Helper()
	got := make(map[uint32][]byte)
	done := make(map[uint32]bool)
	for len(done) < len(expected) {
		f := st.readFrame()
		if f == nil {
			st.t.Fatal("missing DATA/END_STREAM")
		}
		switch f := f.(type) {
		case *HeadersFrame:
			if _, ok := expected[f.StreamID]; !ok {
				st.t.Fatal("unexpected response stream")
			}
			if !f.HeadersEnded() {
				st.t.Fatal("unexpected continued fixture headers")
			}
			st.decodeHeader(f.HeaderBlockFragment()) // preserve HPACK dynamic state for later streams
		case *DataFrame:
			if _, ok := expected[f.StreamID]; !ok {
				st.t.Fatal("unexpected DATA stream")
			}
			got[f.StreamID] = append(got[f.StreamID], f.Data()...)
			if f.StreamEnded() {
				done[f.StreamID] = true
			}
		default:
			st.t.Fatalf("unexpected frame while draining: %T", f)
		}
	}
	for id, want := range expected {
		if !bytes.Equal(got[id], want) {
			st.t.Fatalf("stream %d body mismatch: got %d want %d", id, len(got[id]), len(want))
		}
	}
}

func newCreditTestStall(t testing.TB, enabled bool, options ...func(*Server)) (*serverTester, <-chan error) {
	t.Helper()
	done := make(chan error, 4)
	st := newServerTester(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Host == "fill.test" {
			_, err := w.Write(bytes.Repeat([]byte{'f'}, initialWindowSize))
			if err != nil {
				t.Errorf("filler Write: %v", err)
			}
			return
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			done <- err
			return
		}
		_, err := w.Write(bytes.Repeat([]byte{'d'}, creditTestBodySize))
		if err == nil {
			err = http.NewResponseController(w).Flush()
		}
		done <- err
	}, func(s *Server) {
		s.NewWriteScheduler = NewProxyWriteScheduler
		s.ReadIdleTimeout = 2 * time.Second
		s.PingTimeout = time.Second
		s.WriteByteTimeout = 10 * time.Second
		s.PingTimeoutAfterWrite = true
		if enabled {
			s.SendCreditStallTimeout = creditTestTimeout
		}
		for _, f := range options {
			f(s)
		}
	})
	st.greet()
	st.writeSettings(Setting{SettingInitialWindowSize, 1 << 20})
	st.wantSettingsAck()
	creditTestRequest(st, 1, "fill.test")
	creditTestDrain(st, map[uint32][]byte{1: bytes.Repeat([]byte{'f'}, initialWindowSize)})
	creditTestRequest(st, 3, "old-a.test")
	st.wantHeaders(wantHeader{streamID: 3, endStream: false})
	creditTestRequest(st, 5, "old-b.test")
	st.wantHeaders(wantHeader{streamID: 5, endStream: false})
	st.sync()
	creditTestOnServe(st, func(sc *serverConn) {
		if sc.flow.n != 0 {
			t.Fatalf("filler did not consume connection credit: %d", sc.flow.n)
		}
		for _, id := range []uint32{3, 5} {
			if sc.streams[id].flow.n <= 0 {
				t.Fatal("stream credit must remain positive")
			}
		}
		if enabled && !sc.sendCreditDataEligible() {
			t.Fatal("two queued DATA streams not established")
		}
	})
	return st, done
}

// Continue client control activity throughout the stall. Server PING ACK writes
// do not consume DATA credit or count as DATA progress. This also suppresses
// the ordinary read-idle health probe without touching its implementation.
func creditTestWaitActive(st *serverTester) {
	for i := byte(0); i < 6; i++ {
		payload := [8]byte{0xc1, i}
		st.writePing(false, payload)
		ack := readFrame[*PingFrame](st.t, st)
		if !ack.IsAck() || ack.Data != payload {
			st.t.Fatal("client PING not acknowledged")
		}
		st.advance(500 * time.Millisecond)
	}
}

func TestProxySendCreditStallNativeDrain(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "off"
		if enabled {
			name = "on"
		}
		t.Run(name, func(t *testing.T) {
			synctestTest(t, func(t testing.TB) {
				st, done := newCreditTestStall(t, enabled)
				creditTestWaitActive(st)
				if !enabled {
					st.wantIdle()
					creditTestOnServe(st, func(sc *serverConn) {
						if sc.sendCreditStall != nil {
							t.Fatal("default-off allocated policy state/timer")
						}
					})
				} else {
					probe := readFrame[*PingFrame](t, st)
					if probe.IsAck() {
						t.Fatal("expected independent probe")
					}
					st.sync() // successful Flush and scheduling must precede the ACK in this case
					creditTestOnServe(st, func(sc *serverConn) {
						p := sc.sendCreditStall.probe
						if p == nil || !p.written || p.acknowledged || sc.writingFrame {
							t.Fatal("probe self-invalidated or did not finish Flush")
						}
						if sc.pingSent {
							t.Fatal("policy borrowed normal health state")
						}
					})
					st.writePing(true, probe.Data)
					st.wantGoAway(5, ErrCodeNo) // actual peer observes the wire frame
					st.wantIdle()
					// Explicitly create a fresh outer: proves service is possible,
					// not that every production client automatically repools.
					fresh := newServerTester(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("fresh")) })
					fresh.greet()
					fresh.bodylessReq1()
					creditTestDrain(fresh, map[uint32][]byte{1: []byte("fresh")})
				}
				select {
				case err := <-done:
					t.Fatalf("old CONNECT completed before WU: %v", err)
				default:
				}
				creditTestOnServe(st, func(sc *serverConn) {
					if sc.streams[3] == nil || sc.streams[5] == nil || sc.flow.n != 0 {
						t.Fatal("old streams were canceled or credit fabricated")
					}
				})
				st.writeWindowUpdate(0, 2*creditTestBodySize)
				creditTestDrain(st, map[uint32][]byte{3: bytes.Repeat([]byte{'d'}, creditTestBodySize), 5: bytes.Repeat([]byte{'d'}, creditTestBodySize)})
				for i := 0; i < 2; i++ {
					if err := <-done; err != nil {
						t.Fatalf("old stream did not resume: %v", err)
					}
				}
			})
		})
	}
}

func TestProxySendCreditStallNoACKDoesNotClose(t *testing.T) {
	synctestTest(t, func(t testing.TB) {
		st, _ := newCreditTestStall(t, true, func(s *Server) { s.ReadIdleTimeout = time.Hour })
		creditTestWaitActive(st)
		probe := readFrame[*PingFrame](t, st)
		st.sync()
		st.advance(5 * time.Second)
		st.wantIdle() // no GOAWAY, no repeat probe, and no policy-created hard close
		if st.cc.(*synctestNetConn).IsClosedByPeer() {
			t.Fatal("policy timeout closed old streams")
		}
		st.writePing(true, probe.Data) // ACK after the independent deadline is stale
		st.wantIdle()
		creditTestOnServe(st, func(sc *serverConn) {
			if sc.inGoAway || sc.pingSent || sc.sendCreditStall.probe != nil || !sc.sendCreditStall.tried {
				t.Fatal("stale ACK revived timed-out probe")
			}
			if sc.streams[3] == nil || sc.streams[5] == nil {
				t.Fatal("old streams lost")
			}
		})
	})
}

func TestProxySendCreditStallWUInvalidatesProbe(t *testing.T) {
	synctestTest(t, func(t testing.TB) {
		st, _ := newCreditTestStall(t, true, func(s *Server) { s.ReadIdleTimeout = time.Hour })
		creditTestWaitActive(st)
		probe := readFrame[*PingFrame](t, st)
		// Even a stream-only WU invalidates the old stall generation.
		st.writeWindowUpdate(3, 1)
		st.writePing(true, probe.Data)
		st.wantIdle()
		creditTestOnServe(st, func(sc *serverConn) {
			if sc.inGoAway || sc.sendCreditStall.probe != nil {
				t.Fatal("pre-WU probe survived generation reset")
			}
		})
	})
}

func creditTestState() *serverConn {
	sc := &serverConn{srv: &Server{SendCreditStallTimeout: time.Second},
		baseCtx: context.Background(), streams: make(map[uint32]*stream), serveG: newGoroutineLock(),
		sendCreditStall: &sendCreditStallState{since: time.Now().Add(-2 * time.Second), generation: 7, tried: true}, pingTimeout: time.Second}
	for _, id := range []uint32{3, 5} {
		st := &stream{id: id, sc: sc, state: stateOpen, sendCreditPending: 10}
		st.flow.n = 100
		st.flow.setConnFlow(&sc.flow)
		sc.streams[id] = st
	}
	return sc
}

func TestProxySendCreditStallEligibility(t *testing.T) {
	cases := []struct {
		name   string
		change func(*serverConn)
		want   bool
	}{
		{"two_conn_blocked", func(*serverConn) {}, true},
		{"idle", func(sc *serverConn) {
			for _, st := range sc.streams {
				st.sendCreditPending = 0
			}
		}, false},
		{"single", func(sc *serverConn) { delete(sc.streams, 5) }, false},
		{"only_stream_blocked", func(sc *serverConn) {
			sc.flow.n = 100
			for _, st := range sc.streams {
				st.flow.n = 0
			}
		}, false},
		{"one_stream_has_own_zero", func(sc *serverConn) { sc.streams[5].flow.n = 0 }, false},
		{"control_only", func(sc *serverConn) {
			for _, st := range sc.streams {
				st.sendCreditPending = 0
				st.observationFrames = 1
			}
		}, false},
		{"cancel_queued", func(sc *serverConn) { sc.streams[5].resetQueued = true }, false},
		{"not_authenticated", func(sc *serverConn) { sc.srv.SendCreditStallEnabled = func(context.Context) bool { return false } }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := creditTestState()
			tc.change(sc)
			if got := sc.sendCreditDataEligible(); got != tc.want {
				t.Fatalf("eligible=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestProxySendCreditProbeACKAndFlushOrder(t *testing.T) {
	sc := creditTestState()
	defer sc.sendCreditStall.stopTimer()
	p := &sendCreditProbe{generation: 7, nonce: [8]byte{7}, writing: true}
	sc.sendCreditStall.probe = p
	sc.writingFrame = true
	sc.updateSendCreditStall()
	if sc.sendCreditStall.probe != p {
		t.Fatal("own in-flight PING invalidated itself")
	}
	if sc.acknowledgeSendCreditProbe([8]byte{8}) {
		t.Fatal("mismatched ACK accepted")
	}
	if !sc.acknowledgeSendCreditProbe(p.nonce) || !p.acknowledged || sc.inGoAway {
		t.Fatal("ACK-before-Flush semantics wrong")
	}
	// If qualification disappears before Flush completes, the matching ACK
	// must not be enough to drain. No connection or scheduler is needed here.
	sc.flow.n = 1
	sc.updateSendCreditStall()
	sc.writingFrame = false
	sc.noteSendCreditFrameWritten(FrameWriteRequest{write: &writeSendCreditPing{generation: 7, nonce: p.nonce}}, nil)
	if sc.inGoAway || sc.sendCreditStall.probe != nil {
		t.Fatal("late completion revived invalid generation")
	}
}

func TestProxySendCreditProbeActiveFrameAndDATAReset(t *testing.T) {
	sc := creditTestState()
	s := sc.sendCreditStall
	defer s.stopTimer()
	originalSince := s.since
	sc.noteSendCreditFrameStart(FrameWriteRequest{write: &writePing{}})
	if s.since != originalSince {
		t.Fatal("control write counted as DATA progress")
	}
	sc.writingFrame = true
	sc.updateSendCreditStall()
	if s.probe != nil || s.tick != nil {
		t.Fatal("external active writer permitted probe")
	}
	sc.writingFrame = false
	s.probe = &sendCreditProbe{generation: s.generation, nonce: [8]byte{1}}
	sc.noteSendCreditFrameStart(FrameWriteRequest{write: flushFrameWriter{}})
	if s.probe != nil || !s.since.IsZero() {
		t.Fatal("unrelated active frame retained probe generation")
	}
	st := sc.streams[3]
	st.sendCreditPending = 10
	sc.noteSendCreditFrameStart(FrameWriteRequest{stream: st, write: &writeData{streamID: 3, p: make([]byte, 4)}})
	if st.sendCreditPending != 6 || !s.since.IsZero() {
		t.Fatal("partial DATA pending count or reset wrong")
	}
}

func TestProxySendCreditProbeLateACKAndError(t *testing.T) {
	sc := creditTestState()
	s := sc.sendCreditStall
	defer s.stopTimer()
	s.probe = &sendCreditProbe{generation: 7, nonce: [8]byte{1}, written: true}
	s.due = time.Now().Add(-time.Second)
	if !sc.acknowledgeSendCreditProbe([8]byte{1}) || s.probe != nil || sc.inGoAway || !s.tried {
		t.Fatal("late ACK raced expired timer into drain")
	}
	s.probe = &sendCreditProbe{generation: 7, nonce: [8]byte{2}, acknowledged: true}
	sc.noteSendCreditFrameWritten(FrameWriteRequest{write: &writeSendCreditPing{generation: 7, nonce: [8]byte{2}}}, errors.New("flush failed"))
	if sc.inGoAway || s.probe != nil {
		t.Fatal("failed Flush authorized drain")
	}
}

// Regression: replyToWriter clears wr.write when done is nonnil. The real
// wroteFrame path must inspect DATA before replying, not classify the cleared
// request as a zero-byte control frame.
func TestProxySendCreditDATACompletionWithDoneResets(t *testing.T) {
	sc := creditTestState()
	sc.writeSched = newRoundRobinWriteScheduler()
	sc.writingFrame = true
	sc.needsFrameFlush = false
	st := sc.streams[3]
	st.sendCreditPending = 6 // the selected prefix was already removed at start
	generation := sc.sendCreditStall.generation
	done := make(chan error, 1)
	wr := FrameWriteRequest{stream: st, write: &writeData{streamID: 3, p: make([]byte, 4)}, done: done}
	sc.wroteFrame(frameWriteResult{wr: wr})
	if sc.sendCreditStall.generation != generation+1 || !sc.sendCreditStall.since.IsZero() {
		t.Fatal("positive DATA completion lost after replyToWriter cleared frame")
	}
	if st.sendCreditPending != 6 {
		t.Fatal("completion double-debited pending DATA")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("handler completion was not delivered")
	}
}
