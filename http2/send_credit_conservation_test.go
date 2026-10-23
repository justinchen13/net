// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"testing"
)

// Debit is a selection ledger, not an ACK ledger. Removing a queued remainder
// must not charge it or refund DATA already selected for the wire.
func TestProxySendCreditConservationSplitAndCancel(t *testing.T) {
	sc := &serverConn{maxFrameSize: 16384}
	sc.flow.add(initialWindowSize)
	ws := NewProxyWriteScheduler()
	st := &stream{id: 1, sc: sc}
	st.flow.add(1 << 20)
	st.flow.setConnFlow(&sc.flow)
	ws.OpenStream(1, OpenStreamOptions{})
	payload := bytes.Repeat([]byte{0x63}, 100000)
	ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: 1, p: payload}})
	var wire bytes.Buffer
	fr := NewFramer(&wire, nil)
	consumed := 0
	for {
		wr, ok := ws.Pop()
		if !ok {
			break
		}
		wd := wr.write.(*writeData)
		if len(wd.p) == 0 {
			t.Fatal("unexpected empty selection")
		}
		if err := fr.WriteData(1, false, wd.p); err != nil {
			t.Fatal(err)
		}
		consumed += len(wd.p)
		if got, want := sc.flow.n, int32(initialWindowSize-consumed); got != want {
			t.Fatalf("credit=%d want=%d", got, want)
		}
	}
	if consumed != initialWindowSize || sc.flow.n != 0 {
		t.Fatalf("selected=%d credit=%d", consumed, sc.flow.n)
	}
	ws.CloseStream(1) // RST/cancel drops the unconsumed tail only.
	if sc.flow.n != 0 {
		t.Fatal("cancellation changed consumed connection credit")
	}
	if _, ok := ws.Pop(); ok {
		t.Fatal("canceled tail still queued")
	}
	if !sc.flow.add(1234) || sc.flow.n != 1234 {
		t.Fatal("accepted connection update lost")
	}
	// Read the actual encoded frames and verify exactly the initial grant.
	rd := NewFramer(nil, &wire)
	received := 0
	for wire.Len() != 0 {
		f, err := rd.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		received += len(f.(*DataFrame).Data())
	}
	if received != consumed {
		t.Fatalf("wire=%d selected=%d", received, consumed)
	}
}

func TestProxySendCreditControlAndStreamUpdates(t *testing.T) {
	sc := &serverConn{maxFrameSize: 16384}
	sc.flow.add(9)
	st := &stream{id: 1, sc: sc}
	st.flow.add(5)
	st.flow.setConnFlow(&sc.flow)
	wr := FrameWriteRequest{stream: st, write: &writeData{streamID: 1, p: make([]byte, 10)}}
	_, rest, n := wr.Consume(16384)
	if n != 2 || sc.flow.n != 4 || st.flow.n != 0 {
		t.Fatal("partial debit wrong")
	}
	if !st.flow.add(7) || sc.flow.n != 4 {
		t.Fatal("stream update changed connection credit")
	}
	_, tail, n := rest.Consume(16384)
	if n != 2 || sc.flow.n != 0 || st.flow.n != 3 {
		t.Fatal("connection cap not preserved")
	}
	if _, _, n := tail.Consume(16384); n != 0 {
		t.Fatal("DATA exceeded connection credit")
	}
	ctl := FrameWriteRequest{write: &writePing{}}
	if _, _, n := ctl.Consume(16384); n != 1 || sc.flow.n != 0 {
		t.Fatal("control consumed DATA credit")
	}
	empty := FrameWriteRequest{stream: st, write: &writeData{streamID: 1}}
	if _, _, n := empty.Consume(16384); n != 1 || sc.flow.n != 0 {
		t.Fatal("empty DATA consumed credit")
	}
}
