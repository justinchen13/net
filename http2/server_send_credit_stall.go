// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"crypto/rand"
	"time"
)

// All fields are owned by the serve goroutine. This policy does not change
// credit accounting, the normal health PING, or the lifetime of old streams.
type sendCreditStallState struct {
	timer      *time.Timer
	tick       <-chan time.Time
	due        time.Time
	since      time.Time
	generation uint64
	tried      bool
	probe      *sendCreditProbe
}

type sendCreditProbe struct {
	generation   uint64
	nonce        [8]byte
	written      bool
	acknowledged bool
	writing      bool
}

// This distinct frame type cannot complete or arm the normal health PING.
// Its successful completion includes Flush, not merely Framer buffering.
type writeSendCreditPing struct {
	generation uint64
	nonce      [8]byte
}

func (p *writeSendCreditPing) writeFrame(ctx writeContext) error {
	if err := ctx.Framer().WritePing(false, p.nonce); err != nil {
		return err
	}
	return ctx.Flush()
}
func (*writeSendCreditPing) staysWithinBuffer(int) bool { return false }

func (s *sendCreditStallState) stopTimer() {
	if s.timer != nil {
		if !s.timer.Stop() {
			select {
			case <-s.timer.C:
			default:
			}
		}
	}
	s.tick = nil
	s.due = time.Time{}
}
func (s *sendCreditStallState) arm(at time.Time) {
	s.stopTimer()
	delay := time.Until(at)
	if delay < 0 {
		delay = 0
	}
	if s.timer == nil {
		s.timer = time.NewTimer(delay)
	} else {
		s.timer.Reset(delay)
	}
	s.tick = s.timer.C
	s.due = at
}
func (s *sendCreditStallState) reset() {
	s.stopTimer()
	s.generation++
	s.since = time.Time{}
	s.tried = false
	s.probe = nil
}

// No observation callback or diagnostic sampling is required for eligibility.
// A kernel send queue may still contain data even when writingFrame is false.
func (sc *serverConn) sendCreditDataEligible() bool {
	if sc.inGoAway || sc.flow.n > 0 {
		return false
	}
	if f := sc.srv.SendCreditStallEnabled; f != nil && !f(sc.baseCtx) {
		return false
	}
	count := 0
	for _, st := range sc.streams {
		if st.sendCreditPending > 0 && st.flow.n > 0 && !st.resetQueued &&
			(st.state == stateOpen || st.state == stateHalfClosedRemote) {
			count++
			if count == 2 {
				return true
			}
		}
	}
	return false
}

// Called after each serve-loop turn and from the policy timer. The current
// generation must remain eligible; unrelated control writes are not DATA
// progress. An active frame prevents qualification, except this probe's own
// in-flight Flush, whose completion is separately required before draining.
func (sc *serverConn) updateSendCreditStall() {
	sc.serveG.check()
	s := sc.sendCreditStall
	if s == nil {
		return
	}
	if !sc.sendCreditDataEligible() {
		if !s.since.IsZero() || s.probe != nil {
			s.reset()
		}
		return
	}
	if s.since.IsZero() {
		s.since = time.Now()
	}
	if sc.writingFrame {
		if s.probe != nil {
			if !s.probe.writing {
				s.reset()
			}
		} else {
			// No new probe while a frame is active. Control writes do not
			// pretend to be DATA progress; their completion reevaluates us.
			s.stopTimer()
		}
		return
	}
	if s.probe != nil {
		return
	}
	if s.tried {
		return
	}
	at := s.since.Add(sc.srv.SendCreditStallTimeout)
	if time.Now().Before(at) {
		if s.tick == nil {
			s.arm(at)
		}
		return
	}
	s.stopTimer()
	s.tried = true // including entropy failure: at most one attempt per generation
	p := &sendCreditProbe{generation: s.generation}
	if _, err := rand.Read(p.nonce[:]); err != nil {
		return
	}
	// Do not consume an outstanding normal health PING's matching ACK.
	if sc.pingSent && p.nonce == sc.sentPingData {
		p.nonce[0] ^= 1
	}
	s.probe = p
	sc.observe("send_credit_probe_queued")
	sc.writeFrame(FrameWriteRequest{write: &writeSendCreditPing{generation: p.generation, nonce: p.nonce}})
}

func (sc *serverConn) sendCreditTimerFired() {
	sc.serveG.check()
	s := sc.sendCreditStall
	if s == nil || s.tick == nil {
		return
	}
	// A stopped/reset timer may have a stale delivery. Never act just because
	// a tick was received: validate the current deadline and generation state.
	if time.Now().Before(s.due) {
		s.arm(s.due)
		return
	}
	s.stopTimer()
	if s.probe != nil {
		// The independent ACK budget expired. Keep tried=true for this stall;
		// no GOAWAY, no hard close, and no mutation of normal health timers.
		s.probe = nil
		sc.observe("send_credit_probe_timeout")
		return
	}
	sc.updateSendCreditStall()
}

func (sc *serverConn) noteSendCreditFrameStart(wr FrameWriteRequest) {
	s := sc.sendCreditStall
	if s == nil {
		return
	}
	if n := wr.DataSize(); n > 0 {
		wr.stream.sendCreditPending -= int64(n)
		s.reset() // selecting positive DATA is real credit/progress, not control activity
		return
	}
	if s.probe == nil {
		return
	}
	if p, ok := wr.write.(*writeSendCreditPing); ok &&
		p.generation == s.probe.generation && p.nonce == s.probe.nonce {
		s.probe.writing = true
		return
	}
	// A different frame became active between our decision and fresh ACK.
	// Its completion cannot retroactively validate the old generation.
	s.reset()
}

func (sc *serverConn) noteSendCreditFrameWritten(wr FrameWriteRequest, err error) {
	s := sc.sendCreditStall
	if s == nil {
		return
	}
	if wr.DataSize() > 0 {
		s.reset() // successful DATA completion also ends the no-progress interval
		return
	}
	p, ok := wr.write.(*writeSendCreditPing)
	if !ok {
		return
	}
	current := s.probe
	if current == nil || current.generation != p.generation || current.nonce != p.nonce {
		return
	}
	if err != nil {
		s.reset()
		return
	} // existing wroteFrame handles the connection error
	current.writing = false
	current.written = true
	sc.observe("send_credit_probe_written")
	if current.acknowledged {
		sc.finishSendCreditProbe()
	} else {
		s.arm(time.Now().Add(sc.pingTimeout)) // configured ACK budget, starts after actual Flush
	}
}

func (sc *serverConn) acknowledgeSendCreditProbe(nonce [8]byte) bool {
	s := sc.sendCreditStall
	if s == nil || s.probe == nil || s.probe.nonce != nonce {
		return false
	}
	if s.probe.written && !s.due.IsZero() && !time.Now().Before(s.due) {
		// select may pick a late ACK before the already-due timer delivery.
		s.stopTimer()
		s.probe = nil
		sc.observe("send_credit_probe_timeout")
		return true
	}
	if !s.probe.acknowledged {
		sc.observe("send_credit_probe_ack")
	}
	s.probe.acknowledged = true
	if s.probe.written {
		sc.finishSendCreditProbe()
	}
	return true
}

func (sc *serverConn) finishSendCreditProbe() {
	s := sc.sendCreditStall
	p := s.probe
	if p == nil || !p.written || !p.acknowledged {
		return
	}
	eligible := p.generation == s.generation && !sc.writingFrame &&
		!s.since.IsZero() && time.Since(s.since) >= sc.srv.SendCreditStallTimeout && sc.sendCreditDataEligible()
	s.stopTimer()
	s.probe = nil
	if eligible {
		if !sc.reserveSendCreditDrainLease() {
			sc.observe("send_credit_drain_limit")
			return
		}
		sc.observe("send_credit_drain_queued")
		// Serve-owned path: never self-send to serveMsgCh. NO_ERROR retires
		// this connection for future streams; old streams may remain forever.
		sc.startGracefulShutdownInternal()
	}
}
