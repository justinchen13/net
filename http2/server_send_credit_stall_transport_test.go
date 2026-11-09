// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build go1.25 || goexperiment.synctest

package http2

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"testing/synctest"
)

// Exercise the real Transport.RoundTrip connection pool, using the existing
// synthetic network fixture. The server side supplies NO_ERROR GOAWAY; this
// test does not trigger the send-credit policy or claim compatibility with a
// different client implementation. No new ClientConn is constructed by the
// test: the second RoundTrip must make the Transport create it automatically.
func TestProxySendCreditTransportRepoolKeepsAcceptedStream(t *testing.T) {
	synctestTest(t, func(t testing.TB) {
		tt := newTestTransport(t)
		oldBody := []byte("accepted-before-goaway-and-finished-after-new-connection")
		newBody := []byte("new-request-completed-on-the-new-connection")
		const prefixLen = 7

		oldReq, err := http.NewRequest("GET", "https://dummy.tld/old", nil)
		if err != nil {
			t.Fatal(err)
		}
		oldRT := tt.roundTrip(oldReq)
		oldConn := tt.getConn()
		// RoundTrip has already queued HEADERS after the connection preface.
		// greet assumes no pending request and would mistake these HEADERS
		// for the client's later SETTINGS ACK.
		oldConn.wantFrameType(FrameSettings)
		oldConn.wantFrameType(FrameWindowUpdate)
		oldConn.wantHeaders(wantHeader{streamID: 1, endStream: true})
		oldConn.writeSettings()
		oldConn.writeSettingsAck()
		oldConn.wantFrameType(FrameSettings) // SETTINGS ACK
		oldConn.writeHeaders(HeadersFrameParam{
			StreamID: 1, EndHeaders: true,
			BlockFragment: oldConn.makeHeaderBlockFragment(":status", "200"),
		})
		oldConn.writeData(1, false, oldBody[:prefixLen])
		oldRT.wantStatus(200)
		oldResp := oldRT.response()
		prefix := make([]byte, prefixLen)
		if _, err := io.ReadFull(oldResp.Body, prefix); err != nil {
			t.Fatalf("read accepted stream prefix: %v", err)
		}
		if !bytes.Equal(prefix, oldBody[:prefixLen]) {
			t.Fatal("accepted stream prefix differs")
		}

		// Stream 1 was accepted, so GOAWAY must preserve it while making the
		// connection unavailable to a later request to the same authority.
		oldConn.writeGoAway(1, ErrCodeNo, nil)
		if state := oldConn.cc.State(); !state.Closing || state.Closed || state.StreamsActive != 1 {
			t.Fatalf("GOAWAY did not retain the accepted stream: %+v", state)
		}
		if oldConn.isClosed() {
			t.Fatal("GOAWAY closed the still-active old connection")
		}
		oldConn.wantIdle()

		newReq, err := http.NewRequest("GET", "https://dummy.tld/new", nil)
		if err != nil {
			t.Fatal(err)
		}
		newRT := tt.roundTrip(newReq)
		newConn := tt.getConn() // Produced by Transport, not explicitly dialed here.
		if newConn.cc == oldConn.cc || newConn.netconn == oldConn.netconn {
			t.Fatal("new request reused the retired connection")
		}
		oldConn.wantIdle() // No new HEADERS or cancellation on the old connection.
		newConn.wantFrameType(FrameSettings)
		newConn.wantFrameType(FrameWindowUpdate)
		newConn.wantHeaders(wantHeader{streamID: 1, endStream: true})
		newConn.writeSettings()
		newConn.writeSettingsAck()
		newConn.wantFrameType(FrameSettings)
		newConn.writeHeaders(HeadersFrameParam{
			StreamID: 1, EndHeaders: true,
			BlockFragment: newConn.makeHeaderBlockFragment(":status", "200"),
		})
		newConn.writeData(1, true, newBody)
		newRT.wantStatus(200)
		newRT.wantBody(newBody) // Includes EOF and exact bytes, not only headers.

		if state := oldConn.cc.State(); state.Closed || state.StreamsActive != 1 || oldConn.isClosed() {
			t.Fatalf("finishing the new response disturbed the old stream: %+v", state)
		}
		oldConn.writeData(1, true, oldBody[prefixLen:])
		rest, err := io.ReadAll(oldResp.Body)
		if err != nil {
			t.Fatalf("finish accepted stream after GOAWAY: %v", err)
		}
		if !bytes.Equal(append(prefix, rest...), oldBody) {
			t.Fatal("accepted stream body did not complete exactly")
		}
		synctest.Wait()
		if !oldConn.isClosed() {
			t.Fatal("retired connection remained open after its final stream ended")
		}
		if tt.hasConn() {
			t.Fatal("unexpected additional connection or replay")
		}
	})
}
