// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build go1.25 || goexperiment.synctest

package http2

import (
	"testing"
	"time"
)

// This is a ServeConn test with a bounded in-memory connection, not a claim
// about TCP backpressure. A real ordinary PING ACK Flush is held by the test
// connection while synthetic time crosses T. No policy probe has been tried,
// so the tried guard cannot mask a missing active-writer check.
func TestProxySendCreditControlBusyAcrossTimeout(t *testing.T) {
	synctestTest(t, func(t testing.TB) {
		st, _ := newCreditTestStall(t, true, func(s *Server) { s.ReadIdleTimeout = time.Hour })
		var since time.Time
		var generation uint64
		creditTestOnServe(st, func(sc *serverConn) {
			s := sc.sendCreditStall
			if sc.writingFrame || s.probe != nil || !sc.sendCreditDataEligible() {
				t.Fatal("fixture is not an idle writer with two credit-blocked DATA streams")
			}
			s.tried = false // explicitly exclude the guard that masked the old negative
			since, generation = s.since, s.generation
		})

		client := st.cc.(*synctestNetConn)
		client.SetReadBufferSize(0)
		controlNonce := [8]byte{0x63, 0x13}
		st.writePing(false, controlNonce)
		creditTestOnServe(st, func(sc *serverConn) {
			if !sc.writingFrame || !sc.writingFrameAsync {
				t.Fatal("ordinary control Flush did not become active")
			}
		})
		st.advance(creditTestTimeout + time.Second)
		creditTestOnServe(st, func(sc *serverConn) {
			s := sc.sendCreditStall
			if !sc.writingFrame || s.tried || s.probe != nil || s.tick != nil || sc.inGoAway {
				t.Fatal("active ordinary control writer allowed a policy attempt")
			}
			if s.since != since || s.generation != generation || time.Since(s.since) < creditTestTimeout {
				t.Fatal("control-only activity reset the continuous DATA-stall interval")
			}
		})
		st.wantIdle()

		// Releasing the controlled Flush permits a probe immediately: ordinary
		// control activity did not restart T. No additional time is advanced.
		client.SetReadBufferSize(65536)
		st.sync()
		ack := readFrame[*PingFrame](t, st)
		if !ack.IsAck() || ack.Data != controlNonce {
			t.Fatal("ordinary control response was not preserved")
		}
		probe := readFrame[*PingFrame](t, st)
		if probe.IsAck() {
			t.Fatal("expected independent policy probe after writer became idle")
		}
		creditTestOnServe(st, func(sc *serverConn) {
			s := sc.sendCreditStall
			if !s.tried || s.probe == nil || s.probe.nonce != probe.Data || !s.probe.written || sc.writingFrame {
				t.Fatal("writer release did not permit a successfully flushed probe")
			}
			if s.since != since || s.generation != generation || sc.inGoAway || sc.pingSent {
				t.Fatal("probe changed the stall interval or borrowed normal PING state")
			}
		})
		st.wantIdle() // receiving the probe alone is insufficient for GOAWAY
	})
}

// The wire PING and matching peer ACK are real ServeConn exchanges. To make
// ACK-before-wroteFrame deterministic, the serve-owned callback writes and
// flushes the policy PING synchronously, then withholds only its completion
// notification. We inject that successful notification through wroteFrame
// after observing the ACK. This tests the completion-order state machine,
// not the probability of this scheduling order on a real TCP connection.
func TestProxySendCreditFreshACKBeforeCompletionDrains(t *testing.T) {
	synctestTest(t, func(t testing.TB) {
		st, _ := newCreditTestStall(t, true, func(s *Server) { s.ReadIdleTimeout = time.Hour })
		var wr FrameWriteRequest
		var generation uint64
		creditTestOnServe(st, func(sc *serverConn) {
			s := sc.sendCreditStall
			if sc.writingFrame || s.probe != nil || !sc.sendCreditDataEligible() {
				t.Fatal("fixture is not ready for the controlled completion order")
			}
			s.stopTimer()
			s.since = time.Now().Add(-creditTestTimeout)
			s.tried = true
			generation = s.generation
			s.probe = &sendCreditProbe{generation: generation, nonce: [8]byte{0x61, 0x13, 0x02}}
			wr = FrameWriteRequest{write: &writeSendCreditPing{generation: generation, nonce: s.probe.nonce}}
			sc.noteSendCreditFrameStart(wr)
			sc.writingFrame = true
			sc.writingFrameAsync = true
			sc.needsFrameFlush = true
			if err := wr.write.writeFrame(sc); err != nil {
				t.Fatalf("controlled PING Write/Flush: %v", err)
			}
			// No writeFrameAsync goroutine exists for this injected write;
			// its single completion will be delivered explicitly below.
		})
		probe := readFrame[*PingFrame](t, st)
		if probe.IsAck() {
			t.Fatal("expected policy PING on the wire")
		}
		st.writePing(true, probe.Data)
		creditTestOnServe(st, func(sc *serverConn) {
			p := sc.sendCreditStall.probe
			if p == nil || p.generation != generation || !p.acknowledged || p.written || !p.writing {
				t.Fatal("fresh ACK was not held pending the completion report")
			}
			if sc.inGoAway || !sc.writingFrame || sc.pingSent || !sc.sendCreditDataEligible() {
				t.Fatal("ACK alone drained the connection or changed qualification")
			}
		})
		st.wantIdle() // no GOAWAY before the successful completion notification

		creditTestOnServe(st, func(sc *serverConn) {
			sc.wroteFrame(frameWriteResult{wr: wr})
			if !sc.inGoAway || sc.goAwayCode != ErrCodeNo || sc.sendCreditStall.probe != nil {
				t.Fatal("successful completion after fresh ACK did not authorize graceful drain")
			}
			if sc.streams[3] == nil || sc.streams[5] == nil || sc.flow.n != 0 {
				t.Fatal("graceful drain removed old streams or invented connection credit")
			}
		})
		st.wantGoAway(5, ErrCodeNo) // actual peer receives the graceful wire frame
		st.wantIdle()
	})
}
