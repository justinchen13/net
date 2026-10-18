// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func slowFeedbackFixture() (*proxySlowWriteScheduler, *serverConn, *stream, *time.Time) {
	now := time.Unix(1700000000, 0)
	ws := newProxySlowWriteScheduler(func() time.Time { return now })
	sc := &serverConn{maxFrameSize: 16777215, writeFeedback: ws, writeFeedbackNow: ws.now}
	sc.flow.add(1 << 28)
	st := quantumStream(ws, sc, 1, 1<<28)
	return ws, sc, st, &now
}

func slowFeedbackComplete(sc *serverConn, wr FrameWriteRequest, elapsed time.Duration, completed time.Time, err error) {
	sc.applyProxyWriteFeedback(frameWriteResult{wr: wr, err: err, feedbackWriteDuration: elapsed, feedbackCompletedAt: completed})
}

func TestProxySlowWriteThresholdAndExpiry(t *testing.T) {
	for _, elapsed := range []time.Duration{proxySlowWriteThreshold - time.Nanosecond, proxySlowWriteThreshold} {
		ws, sc, st, now := slowFeedbackFixture()
		quantumStream(ws, sc, 3, 1<<20) // Open, intentionally not active DATA.
		wr := fifoDiagnosticData(st, 65536)
		slowFeedbackComplete(sc, wr, elapsed, *now, nil)
		ws.Push(wr)
		got, ok := ws.Pop()
		want := 65536
		if elapsed == proxySlowWriteThreshold {
			want = proxySlowWriteQuantum
		}
		if !ok || got.DataSize() != want {
			t.Fatalf("elapsed=%v size=%d want=%d", elapsed, got.DataSize(), want)
		}
	}
	for _, offset := range []time.Duration{proxySlowWriteWindow - time.Nanosecond, proxySlowWriteWindow, proxySlowWriteWindow + time.Nanosecond} {
		ws, sc, st, now := slowFeedbackFixture()
		quantumStream(ws, sc, 3, 1<<20)
		wr := fifoDiagnosticData(st, 65536)
		slowFeedbackComplete(sc, wr, proxySlowWriteThreshold, *now, nil)
		*now = now.Add(offset)
		ws.Push(wr)
		got, ok := ws.Pop()
		want := 65536
		if offset < proxySlowWriteWindow {
			want = proxySlowWriteQuantum
		}
		if !ok || got.DataSize() != want {
			t.Fatalf("offset=%v size=%d want=%d", offset, got.DataSize(), want)
		}
		if offset >= proxySlowWriteWindow && !ws.slowUntil.IsZero() {
			t.Fatal("expired deadline retained after Pop")
		}
	}
}

func TestProxySlowWriteSingleStreamRefreshErrorAndClosed(t *testing.T) {
	ws, sc, st, now := slowFeedbackFixture()
	wr := fifoDiagnosticData(st, 65536)
	slowFeedbackComplete(sc, wr, proxySlowWriteThreshold, *now, nil)
	deadline := ws.slowUntil
	ws.Push(wr)
	got, ok := ws.Pop()
	if !ok || got.DataSize() != 65536 {
		t.Fatal("single stream fragmented")
	}
	*now = now.Add(time.Second)
	quantumStream(ws, sc, 3, 1<<20)
	ws.Push(fifoDiagnosticData(st, 65536))
	got, ok = ws.Pop()
	if !ok || got.DataSize() != proxySlowWriteQuantum || ws.slowUntil != deadline {
		t.Fatal("second stream did not inherit still-live window")
	}
	slowFeedbackComplete(sc, wr, time.Second, *now, errors.New("write failed"))
	if ws.slowUntil != deadline {
		t.Fatal("failed DATA refreshed window")
	}
	flush := FrameWriteRequest{write: flushFrameWriter{}}
	slowFeedbackComplete(sc, flush, proxySlowWriteThreshold, *now, nil)
	if !ws.slowUntil.Equal(now.Add(proxySlowWriteWindow)) {
		t.Fatal("slow flush failed to refresh")
	}
	*now = now.Add(time.Second)
	slowFeedbackComplete(sc, flush, proxySlowWriteThreshold, *now, nil)
	deadline = ws.slowUntil
	if !deadline.Equal(now.Add(proxySlowWriteWindow)) {
		t.Fatal("successive slow flush failed to refresh")
	}
	slowFeedbackComplete(sc, flush, time.Second, now.Add(time.Second), errors.New("flush failed"))
	if ws.slowUntil != deadline {
		t.Fatal("failed flush refreshed window")
	}
	ws.CloseStream(1)
	slowFeedbackComplete(sc, wr, time.Second, now.Add(time.Second), nil)
	if ws.slowUntil != deadline {
		t.Fatal("closed DATA queue refreshed window")
	}
	ws.CloseStream(3)
	slowFeedbackComplete(sc, flush, time.Second, now.Add(time.Second), nil)
	if ws.slowUntil != deadline {
		t.Fatal("flush with no streams refreshed window")
	}
}

func TestProxySlowWriteActualCompletionExcludesHandoff(t *testing.T) {
	ws, sc, st, now := slowFeedbackFixture()
	wr := fifoDiagnosticData(st, 8192)
	start := sc.beginProxyWrite(wr)
	*now = now.Add(proxySlowWriteThreshold - time.Nanosecond)
	res := frameWriteResult{wr: wr}
	sc.endProxyWrite(&res, start)
	*now = now.Add(time.Second) // Serve handoff delay must not turn this into a slow write.
	sc.applyProxyWriteFeedback(res)
	if !ws.slowUntil.IsZero() {
		t.Fatal("serve handoff counted as write time")
	}
	start = sc.beginProxyWrite(wr)
	*now = now.Add(proxySlowWriteThreshold)
	res = frameWriteResult{wr: wr}
	sc.endProxyWrite(&res, start)
	completed := *now
	*now = now.Add(3 * time.Second)
	sc.applyProxyWriteFeedback(res)
	if !ws.slowUntil.Equal(completed.Add(proxySlowWriteWindow)) {
		t.Fatal("window anchored to dispatch instead of completion")
	}
	quantumStream(ws, sc, 3, 1<<20)
	ws.Push(wr)
	got, ok := ws.Pop()
	if !ok || got.DataSize() != 8192 {
		t.Fatal("expired feedback from delayed dispatch was revived")
	}
}

func TestProxySlowWriteDefaultOffAndControlClock(t *testing.T) {
	choices := []WriteScheduler{NewProxyWriteScheduler(), newRoundRobinWriteScheduler()}
	for _, q := range []int32{0, 16384} {
		choices = append(choices, quantumScheduler(t, q))
	}
	for _, ws := range choices {
		if _, ok := ws.(proxyWriteFeedbackScheduler); ok {
			t.Fatal("existing constructor opted in")
		}
		sc := &serverConn{writeSched: ws, writeFeedbackNow: func() time.Time { panic("default clock queried") }}
		for _, wr := range []FrameWriteRequest{{write: &writeData{}}, {write: flushFrameWriter{}}} {
			start := sc.beginProxyWrite(wr)
			res := frameWriteResult{wr: wr}
			sc.endProxyWrite(&res, start)
			sc.applyProxyWriteFeedback(res)
			if !res.feedbackCompletedAt.IsZero() {
				t.Fatal("default path gained timing")
			}
		}
	}
	if ws, err := NewProxyWriteSchedulerWithDataQuantum(4096); err == nil || ws != nil {
		t.Fatal("fixed constructor changed")
	}
	ws, sc, st, now := slowFeedbackFixture()
	for _, wr := range []FrameWriteRequest{{write: writeWindowUpdate{n: 1}}, {stream: st, write: &writeResHeaders{streamID: 1}}} {
		sc.writeFeedbackNow = func() time.Time { panic("non-DATA/flush clock queried") }
		if !sc.beginProxyWrite(wr).IsZero() {
			t.Fatal("control timing enabled")
		}
		slowFeedbackComplete(sc, wr, time.Second, *now, nil)
		if !ws.slowUntil.IsZero() {
			t.Fatal("control frame triggered pressure")
		}
	}
	if !(&serverConn{writeSched: ws}).writeSchedIgnoresRFC7540() {
		t.Fatal("proxy priority semantics changed")
	}
}

type feedbackCallCounter struct{ calls int }

func (c *feedbackCallCounter) proxyWriteClock() func() time.Time { return time.Now }
func (c *feedbackCallCounter) proxyWriteCompleted(FrameWriteRequest, time.Duration, time.Time) {
	c.calls++
}

func TestProxySlowWriteNoFailedOrClosedCallback(t *testing.T) {
	counter := &feedbackCallCounter{}
	sc := &serverConn{writeFeedback: counter}
	st := &stream{id: 1, state: stateOpen}
	wr := FrameWriteRequest{stream: st, write: &writeData{streamID: 1, p: []byte{1}}}
	at := time.Unix(1700000000, 0)
	slowFeedbackComplete(sc, wr, time.Second, at, errors.New("failed"))
	st.state = stateClosed
	slowFeedbackComplete(sc, wr, time.Second, at, nil)
	if counter.calls != 0 {
		t.Fatal("failed or closed write invoked callback")
	}
	st.state = stateOpen
	slowFeedbackComplete(sc, wr, time.Second, at, nil)
	if counter.calls != 1 {
		t.Fatal("successful open write callback lost")
	}
}

func activeSlowFeedbackScheduler() (*proxySlowWriteScheduler, *serverConn) {
	now := time.Unix(1700000000, 0)
	ws := newProxySlowWriteScheduler(func() time.Time { return now })
	sc := &serverConn{maxFrameSize: 16777215, writeFeedback: ws, writeFeedbackNow: ws.now}
	sc.flow.add(1 << 28)
	// An open empty peer is intentional: eligibility does not imply activity.
	quantumStream(ws, sc, 999, 1<<28)
	slowFeedbackComplete(sc, FrameWriteRequest{write: flushFrameWriter{}}, proxySlowWriteThreshold, now, nil)
	return ws, sc
}

func TestProxySlowWritePartialFIFOAndCompletion(t *testing.T) {
	ws, sc := activeSlowFeedbackScheduler()
	sc.flow.n = 1 << 20
	st := quantumStream(ws, sc, 1, 1<<20)
	payload := make([]byte, 65539)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	done, ended := make(chan error, 1), make(chan error, 1)
	ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: 1, p: payload}, done: done})
	ws.Push(FrameWriteRequest{stream: st, write: writeWindowUpdate{streamID: 1, n: 4096}})
	ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: 1, endStream: true}, done: ended})
	var received []byte
	wu, eof := false, false
	for {
		wr, ok := ws.Pop()
		if !ok {
			break
		}
		switch w := wr.write.(type) {
		case *writeData:
			if len(w.p) > proxySlowWriteQuantum {
				t.Fatal("DATA turn exceeds quantum")
			}
			if len(w.p) == 0 {
				if !wu || !w.endStream || wr.done != ended {
					t.Fatal("empty END_STREAM reordered")
				}
				eof = true
			} else {
				if wu || eof || w.endStream {
					t.Fatal("DATA reordered or ended early")
				}
				received = append(received, w.p...)
				if len(received) < len(payload) && wr.done != nil {
					t.Fatal("partial write completed handler")
				}
				if len(received) == len(payload) && wr.done != done {
					t.Fatal("final completion lost")
				}
			}
		case writeWindowUpdate:
			if !bytes.Equal(received, payload) || wu || w.n != 4096 {
				t.Fatal("following WU bypassed DATA")
			}
			wu = true
		default:
			t.Fatal("unexpected frame")
		}
		wr.replyToWriter(nil)
	}
	if !eof || !wu || !bytes.Equal(received, payload) {
		t.Fatal("incomplete payload/FIFO")
	}
	for _, ch := range []chan error{done, ended} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("missing completion")
		}
		select {
		case <-ch:
			t.Fatal("duplicate completion")
		default:
		}
	}
	if sc.flow.n != (1<<20)-int32(len(payload)) || st.flow.n != sc.flow.n {
		t.Fatal("flow accounting changed")
	}
}

func TestProxySlowWriteFlowPeerLimitAndClose(t *testing.T) {
	ws, sc := activeSlowFeedbackScheduler()
	sc.maxFrameSize = 2048
	sc.flow.n = 7000
	st := quantumStream(ws, sc, 1, 9000)
	done := make(chan error, 1)
	ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: 1, p: make([]byte, 65536), endStream: true}, done: done})
	pop := func(want int) {
		t.Helper()
		wr, ok := ws.Pop()
		if !ok || wr.DataSize() != want || wr.done != nil || wr.write.(*writeData).endStream {
			t.Fatalf("partial got=%d available=%v want=%d", wr.DataSize(), ok, want)
		}
	}
	pop(2048)
	pop(2048)
	pop(2048)
	pop(856)
	if _, ok := ws.Pop(); ok {
		t.Fatal("connection credit bypassed")
	}
	sc.flow.add(20000)
	pop(2000)
	if _, ok := ws.Pop(); ok {
		t.Fatal("stream credit bypassed")
	}
	st.flow.add(20000)
	pop(2048)
	if sc.maxFrameSize != 2048 {
		t.Fatal("peer frame limit changed")
	}
	ws.CloseStream(1)
	if _, ok := ws.Pop(); ok {
		t.Fatal("closed stream remainder retained")
	}
	// CloseStream itself drops queued frames; server close-notification, rather
	// than scheduler completion, releases the handler. Do not invent completion.
	select {
	case <-done:
		t.Fatal("closed remainder spuriously succeeded")
	default:
	}
	fresh := quantumStream(ws, sc, 3, 100)
	ws.Push(FrameWriteRequest{stream: fresh, write: &writeData{streamID: 3, p: []byte("fresh")}})
	wr, ok := ws.Pop()
	if !ok || wr.StreamID() != 3 || !bytes.Equal(wr.write.(*writeData).p, []byte("fresh")) {
		t.Fatal("recycled queue retained old frames")
	}
	ws.CloseStream(3)
	checkFairnessRing(t, ws.roundRobinWriteScheduler)
}

func TestProxySlowWritePreservesFairnessAndGlobalControl(t *testing.T) {
	ws, sc := activeSlowFeedbackScheduler()
	for i := uint32(0); i < 10; i++ {
		st := quantumStream(ws, sc, 2*i+1, 1<<28)
		if i < 9 {
			ws.Push(fifoDiagnosticWU(st))
		} else {
			ws.Push(fifoDiagnosticData(st, 65536))
		}
	}
	// The global-control priority is intentionally unchanged.
	ws.Push(FrameWriteRequest{write: writeWindowUpdate{n: 4096}})
	if wr, ok := ws.Pop(); !ok || wr.StreamID() != 0 {
		t.Fatal("global priority changed")
	}
	last, progress := 0, 0
	for step := 1; step <= 900; step++ {
		wr, ok := ws.Pop()
		if !ok {
			t.Fatal("ready DATA starved")
		}
		if wr.StreamID() == 19 {
			if step-last > 99 || wr.DataSize() != proxySlowWriteQuantum {
				t.Fatal("ordinary cursor/quantum progress violated")
			}
			last, progress = step, progress+1
			// Refill only after a full original request. Avoid extra queued DATA.
			if progress%16 == 0 {
				ws.Push(fifoDiagnosticData(wr.stream, 65536))
			}
		} else {
			ws.Push(fifoDiagnosticWU(wr.stream))
		}
	}
	if progress == 0 || 900-last >= 99 {
		t.Fatal("DATA starved at tail")
	}
	checkFairnessRing(t, ws.roundRobinWriteScheduler)
}
