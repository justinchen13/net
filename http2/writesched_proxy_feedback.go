// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import "time"

const (
	proxySlowWriteThreshold = 100 * time.Millisecond
	proxySlowWriteWindow    = 2 * time.Second
	proxySlowWriteQuantum   = 4096
)

// proxyWriteFeedbackScheduler is deliberately private and opt-in. Policy state
// is owned by the serve goroutine. The clock function is fixed at construction
// and may also be called by the single frame writer; it must be concurrency safe.
type proxyWriteFeedbackScheduler interface {
	proxyWriteClock() func() time.Time
	proxyWriteCompleted(FrameWriteRequest, time.Duration, time.Time)
}

// NewProxyWriteSchedulerWithSlowWriteFeedback opts into an experimental policy.
// A successful DATA or flush write taking at least 100 ms enables a 4096-byte
// DATA turn for the following 2 seconds, only while at least two streams are
// open. Another qualifying write refreshes that window. No timer is started.
//
// This is a write-wall-time pressure signal (buffer/TLS/socket work and waits),
// not a client ACK, delivery measurement, or diagnosis of network congestion.
// Two open streams need not be two active transfers. An already selected frame
// cannot be preempted. Control priority, both round-robin cursors, stream FIFO,
// flow control and writer completion retain the existing proxy policy.
//
// Existing constructors do not enable this policy or query its clock.
func NewProxyWriteSchedulerWithSlowWriteFeedback() WriteScheduler {
	return newProxySlowWriteScheduler(time.Now)
}

func newProxySlowWriteScheduler(now func() time.Time) *proxySlowWriteScheduler {
	return &proxySlowWriteScheduler{
		roundRobinWriteScheduler: NewProxyWriteScheduler().(*roundRobinWriteScheduler),
		now:                      now,
	}
}

type proxySlowWriteScheduler struct {
	*roundRobinWriteScheduler
	now       func() time.Time // immutable; time.Now in production
	slowUntil time.Time        // serve goroutine only
}

func (ws *proxySlowWriteScheduler) proxyWriteClock() func() time.Time { return ws.now }

func (ws *proxySlowWriteScheduler) proxyWriteCompleted(wr FrameWriteRequest, elapsed time.Duration, completed time.Time) {
	if completed.IsZero() || elapsed < proxySlowWriteThreshold {
		return
	}
	switch wr.write.(type) {
	case *writeData:
		// The stream can have closed while an asynchronous write was in flight.
		// Do not revive a closed queue or derive feedback from its late result.
		if ws.streams[wr.StreamID()] == nil {
			return
		}
	case flushFrameWriter:
		if len(ws.streams) == 0 {
			return
		}
	default:
		return
	}
	ws.slowUntil = completed.Add(proxySlowWriteWindow)
}

func (ws *proxySlowWriteScheduler) Pop() (FrameWriteRequest, bool) {
	ws.dataQuantum = 0
	if len(ws.streams) >= 2 && !ws.slowUntil.IsZero() {
		if ws.now().Before(ws.slowUntil) {
			ws.dataQuantum = proxySlowWriteQuantum
		} else {
			ws.slowUntil = time.Time{}
		}
	}
	return ws.roundRobinWriteScheduler.Pop()
}

func (ws *proxySlowWriteScheduler) observationSchedulerName() string {
	return "proxy_slow_write_feedback"
}

// begin/end run around the actual frame writer call, including synchronous
// buffered writes. The async completion timestamp travels with its result so
// time waiting in wroteFrameCh or the serve loop is excluded from the signal.
// No new time.Now call is made for a scheduler that has not opted in.
func (sc *serverConn) beginProxyWrite(wr FrameWriteRequest) time.Time {
	if sc.writeFeedback != nil {
		switch wr.write.(type) {
		case *writeData, flushFrameWriter:
			return sc.writeFeedbackNow()
		}
	}
	return time.Time{}
}

func (sc *serverConn) endProxyWrite(res *frameWriteResult, start time.Time) {
	if !start.IsZero() {
		res.feedbackCompletedAt = sc.writeFeedbackNow()
		res.feedbackWriteDuration = res.feedbackCompletedAt.Sub(start)
	}
}

func (sc *serverConn) applyProxyWriteFeedback(res frameWriteResult) {
	if res.wr.stream != nil && res.wr.stream.state == stateClosed {
		return
	}
	if sc.writeFeedback != nil && res.err == nil && !res.feedbackCompletedAt.IsZero() {
		sc.writeFeedback.proxyWriteCompleted(res.wr, res.feedbackWriteDuration, res.feedbackCompletedAt)
	}
}
