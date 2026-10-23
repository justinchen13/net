// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
    "bytes"
    "errors"
    "io"
    "testing"

    "golang.org/x/net/http2/hpack"
)

type goAwayFlushContext struct {
    fr *Framer
    flushErr error
    flushCalls int
}
func (c *goAwayFlushContext) Framer() *Framer { return c.fr }
func (c *goAwayFlushContext) Flush() error { c.flushCalls++; return c.flushErr }
func (*goAwayFlushContext) CloseConn() error { return nil }
func (*goAwayFlushContext) HeaderEncoder() (*hpack.Encoder, *bytes.Buffer) { panic("unused") }
type goAwayFailWriter struct { err error }
func (w goAwayFailWriter) Write([]byte) (int, error) { return 0, w.err }

func TestProxyGoAwayFlushErrorPropagated(t *testing.T) {
    want := errors.New("flush failed")
    c := &goAwayFlushContext{fr: NewFramer(io.Discard, nil), flushErr: want}
    if got := (&writeGoAway{maxStreamID: 3, code: ErrCodeNo}).writeFrame(c); got != want {
        t.Fatalf("successful frame buffering hid flush error: got %v, want %v", got, want)
    }
    if c.flushCalls != 1 { t.Fatalf("flush calls=%d", c.flushCalls) }
}

func TestProxyGoAwayFrameErrorPreserved(t *testing.T) {
    want := errors.New("frame failed")
    c := &goAwayFlushContext{fr: NewFramer(goAwayFailWriter{want}, nil), flushErr: errors.New("flush")}
    if got := (&writeGoAway{code: ErrCodeNo}).writeFrame(c); got != want { t.Fatalf("got %v", got) }
    if c.flushCalls != 0 { t.Fatal("flushed after failed frame write") }
}

func TestProxyGoAwaySuccessfulWireFrame(t *testing.T) {
    var buf bytes.Buffer
    c := &goAwayFlushContext{fr: NewFramer(&buf, nil)}
    if err := (&writeGoAway{maxStreamID: 7, code: ErrCodeNo}).writeFrame(c); err != nil { t.Fatal(err) }
    f, err := NewFramer(nil, &buf).ReadFrame()
    if err != nil { t.Fatal(err) }
    g, ok := f.(*GoAwayFrame)
    if !ok || g.LastStreamID != 7 || g.ErrCode != ErrCodeNo || c.flushCalls != 1 { t.Fatalf("GOAWAY not preserved: %v", f) }
}
