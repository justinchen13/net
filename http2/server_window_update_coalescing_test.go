package http2

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

// These tests also compile on the frozen parent ca43. If the opt-in field is
// absent, enabled leaves that old implementation unchanged; the enabled lower
// Write count test then fails on its actual two writes, not on a missing symbol.
func setBodyReadCoalescingForTest(s *Server, enabled bool) {
	f := reflect.ValueOf(s).Elem().FieldByName("CoalesceBodyReadWindowUpdates")
	if f.IsValid() {
		f.SetBool(enabled)
	}
}

type windowUpdateTestAddr string

func (a windowUpdateTestAddr) Network() string { return "memory" }
func (a windowUpdateTestAddr) String() string  { return string(a) }

// Record actual net.Conn.Write boundaries below the HTTP/2 buffered writer.
// This is a pure in-memory transport: it makes no TLS/packet/latency claim.
type windowUpdateTestConn struct {
	mu       sync.Mutex
	writes   [][]byte
	accepted []byte
	closed   bool
	writeErr error
	short    bool
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (c *windowUpdateTestConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	first := len(c.writes) == 0
	c.writes = append(c.writes, bytes.Clone(p))
	c.mu.Unlock()
	if first && c.release != nil {
		close(c.entered)
		<-c.release
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.short {
		c.accepted = append(c.accepted, p[:1]...)
		return 1, nil
	}
	c.accepted = append(c.accepted, p...)
	return len(p), nil
}

func (c *windowUpdateTestConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *windowUpdateTestConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}
func (c *windowUpdateTestConn) LocalAddr() net.Addr              { return windowUpdateTestAddr("local") }
func (c *windowUpdateTestConn) RemoteAddr() net.Addr             { return windowUpdateTestAddr("peer") }
func (c *windowUpdateTestConn) SetDeadline(time.Time) error      { return nil }
func (c *windowUpdateTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *windowUpdateTestConn) SetWriteDeadline(time.Time) error { return nil }
func (c *windowUpdateTestConn) unblock() {
	if c.release != nil {
		c.once.Do(func() { close(c.release) })
	}
}

type windowUpdateHarness struct {
	sc *serverConn
	st *stream
	c  *windowUpdateTestConn
}

func newWindowUpdateHarness(t *testing.T, enabled bool) *windowUpdateHarness {
	t.Helper()
	c := &windowUpdateTestConn{}
	srv := &Server{}
	setBodyReadCoalescingForTest(srv, enabled)
	sc := &serverConn{
		srv: srv, conn: c, baseCtx: context.Background(),
		bw: newBufferedWriter(c, 0), serveG: newGoroutineLock(),
		wroteFrameCh: make(chan frameWriteResult, 1),
		writeSched:   NewProxyWriteScheduler(), streams: make(map[uint32]*stream),
		maxClientStreamID: 1, maxFrameSize: 16384,
	}
	sc.framer = NewFramer(sc.bw, nil)
	sc.flow.add(65535)
	sc.inflow.init(65535)
	st := &stream{sc: sc, id: 1, state: stateOpen, ctx: context.Background()}
	st.inflow.init(65535)
	st.flow.setConnFlow(&sc.flow)
	st.flow.add(65535)
	sc.streams[1] = st
	sc.writeSched.OpenStream(1, OpenStreamOptions{})
	h := &windowUpdateHarness{sc, st, c}
	t.Cleanup(func() {
		c.unblock() // A failed assertion must not leave a blocked async writer.
		h.drain(t)
	})
	return h
}

func (h *windowUpdateHarness) drain(t *testing.T) []error {
	t.Helper()
	var result []error
	for i := 0; h.sc.writingFrameAsync; i++ {
		if i >= 16 {
			t.Fatal("unbounded frame write completion loop")
		}
		select {
		case res := <-h.sc.wroteFrameCh:
			if res.err != nil {
				result = append(result, res.err)
			}
			h.sc.wroteFrame(res)
		case <-time.After(5 * time.Second):
			t.Fatal("asynchronous in-memory writer did not finish")
		}
	}
	return result
}

func (h *windowUpdateHarness) frames(t *testing.T) []Frame {
	t.Helper()
	f := NewFramer(nil, bytes.NewReader(h.c.accepted))
	var frames []Frame
	for {
		frame, err := f.ReadFrame()
		if err == io.EOF {
			return frames
		}
		if err != nil {
			t.Fatalf("decode accepted wire bytes: %v", err)
		}
		if data, ok := frame.(*DataFrame); ok {
			// A Framer invalidates/reuses its last frame on the next read.
			copy := *data
			copy.data = bytes.Clone(data.Data())
			frame = &copy
		}
		frames = append(frames, frame)
	}
}

func TestBodyReadCoalescingDisabledDefault(t *testing.T) {
	f := reflect.ValueOf(&Server{}).Elem().FieldByName("CoalesceBodyReadWindowUpdates")
	if !f.IsValid() || f.Kind() != reflect.Bool || f.Bool() {
		t.Fatal("candidate must expose an explicit default-false bool")
	}
}

func assertWindowUpdate(t *testing.T, f Frame, streamID, increment uint32) {
	t.Helper()
	wu, ok := f.(*WindowUpdateFrame)
	if !ok || wu.StreamID != streamID || wu.Increment != increment {
		t.Fatalf("frame = %#v, want WINDOW_UPDATE(%d,%d)", f, streamID, increment)
	}
}

func TestBodyReadCoalescingLowerWrites(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "legacy"
		if enabled {
			name = "coalesced"
		}
		t.Run(name, func(t *testing.T) {
			h := newWindowUpdateHarness(t, enabled)
			h.sc.noteBodyRead(h.st, 4096)
			if errs := h.drain(t); len(errs) != 0 {
				t.Fatal(errs)
			}
			want := []int{13, 13}
			if enabled {
				want = []int{26}
			}
			var sizes []int
			for _, p := range h.c.writes {
				sizes = append(sizes, len(p))
			}
			if !reflect.DeepEqual(sizes, want) {
				t.Fatalf("actual lower Write sizes = %v, want %v", sizes, want)
			}
			frames := h.frames(t)
			if len(frames) != 2 {
				t.Fatalf("%d frames, want 2", len(frames))
			}
			assertWindowUpdate(t, frames[0], 0, 4096)
			assertWindowUpdate(t, frames[1], 1, 4096)
			if h.sc.inflow.avail != 65535+4096 || h.st.inflow.avail != 65535+4096 || h.sc.queuedControlFrames != 0 {
				t.Fatal("window or control accounting changed")
			}
		})
	}
}

func TestBodyReadCoalescingThresholdsAndStreamStates(t *testing.T) {
	for _, tc := range []struct {
		name         string
		conn, stream inflow
		n            int
		state        streamState
	}{
		{"below_4KiB", inflow{avail: 65535}, inflow{avail: 65535}, 4095, stateOpen},
		{"exact_4KiB", inflow{avail: 65535}, inflow{avail: 65535}, 4096, stateOpen},
		{"urgent_below_4KiB", inflow{avail: 100}, inflow{avail: 100}, 100, stateOpen},
		{"zero_available", inflow{}, inflow{}, 1, stateOpen},
		{"connection_only", inflow{}, inflow{avail: 65535}, 1, stateOpen},
		{"stream_only", inflow{avail: 65535}, inflow{}, 1, stateOpen},
		{"pending_reaches_low_window", inflow{avail: 100, unsent: 99}, inflow{avail: 100, unsent: 99}, 1, stateOpen},
		{"zero_new_bytes_flush_pending", inflow{unsent: 1}, inflow{unsent: 1}, 0, stateOpen},
		{"closed_remote", inflow{avail: 65535}, inflow{avail: 65535}, 4096, stateHalfClosedRemote},
		{"closed", inflow{avail: 65535}, inflow{avail: 65535}, 4096, stateClosed},
		{"closed_local_still_receives", inflow{avail: 65535}, inflow{avail: 65535}, 4096, stateHalfClosedLocal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wire [][]byte
			for _, enabled := range []bool{false, true} {
				h := newWindowUpdateHarness(t, enabled)
				h.sc.inflow, h.st.inflow, h.st.state = tc.conn, tc.stream, tc.state
				conn, stream := tc.conn, tc.stream
				connIncrement := conn.add(tc.n)
				var streamIncrement int32
				if tc.state != stateHalfClosedRemote && tc.state != stateClosed {
					streamIncrement = stream.add(tc.n)
				}
				h.sc.noteBodyRead(h.st, tc.n)
				if errs := h.drain(t); len(errs) != 0 {
					t.Fatal(errs)
				}
				if h.sc.inflow != conn || h.st.inflow != stream {
					t.Fatalf("inflow mismatch: conn=%+v stream=%+v", h.sc.inflow, h.st.inflow)
				}
				frames := h.frames(t)
				for _, item := range []struct{ id, n uint32 }{{0, uint32(connIncrement)}, {1, uint32(streamIncrement)}} {
					if item.n != 0 {
						if len(frames) == 0 {
							t.Fatal("urgent or threshold update was deferred")
						}
						assertWindowUpdate(t, frames[0], item.id, item.n)
						frames = frames[1:]
					}
				}
				if len(frames) != 0 {
					t.Fatal("extra update")
				}
				wire = append(wire, bytes.Clone(h.c.accepted))
			}
			if !bytes.Equal(wire[0], wire[1]) {
				t.Fatal("opt-in changed encoded frames")
			}
		})
	}
}

func TestBodyReadCoalescingExistingScheduleAndFIFO(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		h := newWindowUpdateHarness(t, enabled)
		// Model a nested schedule invocation. noteBodyRead must neither clear nor
		// borrow this existing guard; it queues and the outer scheduler resumes.
		h.sc.inFrameScheduleLoop = true
		h.sc.writeFrame(FrameWriteRequest{write: &writeData{streamID: 1, p: []byte("x")}, stream: h.st})
		h.sc.writeFrame(FrameWriteRequest{write: &writePing{data: [8]byte{1}}})
		h.sc.noteBodyRead(h.st, 4096)
		if !h.sc.inFrameScheduleLoop || h.sc.writingFrame {
			t.Fatal("changed nested scheduling state")
		}
		h.sc.inFrameScheduleLoop = false
		h.st.flow.n = 0 // DATA at this stream head must keep the stream WU behind it.
		h.sc.scheduleFrameWrite()
		h.drain(t)
		frames := h.frames(t)
		if len(frames) != 2 {
			t.Fatalf("blocked stream changed frame count: %d", len(frames))
		}
		if _, ok := frames[0].(*PingFrame); !ok {
			t.Fatal("global control FIFO changed")
		}
		assertWindowUpdate(t, frames[1], 0, 4096)
		h.st.flow.add(1)
		h.sc.scheduleFrameWrite()
		h.drain(t)
		frames = h.frames(t)
		if len(frames) != 4 {
			t.Fatalf("flow grant: %d frames", len(frames))
		}
		if data, ok := frames[2].(*DataFrame); !ok || string(data.Data()) != "x" {
			t.Fatal("stream FIFO/data integrity changed")
		}
		assertWindowUpdate(t, frames[3], 1, 4096)
		if h.sc.flow.n != 65534 || h.st.flow.n != 0 || h.sc.queuedControlFrames != 0 {
			t.Fatal("DATA/control accounting changed")
		}
	}
}

func TestBodyReadCoalescingBusyWriter(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		h := newWindowUpdateHarness(t, enabled)
		h.c.entered, h.c.release = make(chan struct{}), make(chan struct{})
		h.sc.writeFrame(FrameWriteRequest{write: &writePing{data: [8]byte{1}}})
		select {
		case <-h.c.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("writer did not reach gate")
		}
		h.sc.noteBodyRead(h.st, 4096) // Must return while the first Write is blocked.
		if !h.sc.writingFrame || !h.sc.writingFrameAsync {
			t.Fatal("lost outstanding async writer")
		}
		h.c.unblock()
		h.drain(t)
		frames := h.frames(t)
		if len(frames) != 3 || len(h.c.writes) != 2 {
			t.Fatal("busy writer introduced an extra flush or lost a frame")
		}
		assertWindowUpdate(t, frames[1], 0, 4096)
		assertWindowUpdate(t, frames[2], 1, 4096)
	}
}

func TestBodyReadCoalescingWriteErrors(t *testing.T) {
	injected := errors.New("injected transport write failure")
	for _, enabled := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			h := newWindowUpdateHarness(t, enabled)
			h.c.short = short
			want := io.ErrShortWrite
			if !short {
				h.c.writeErr = injected
				want = injected
			}
			h.sc.noteBodyRead(h.st, 4096)
			errs := h.drain(t)
			if len(errs) == 0 || !errors.Is(errs[0], want) || !h.c.closed {
				t.Fatalf("write failure not preserved: errors=%v closed=%v", errs, h.c.closed)
			}
			if h.sc.inflow.avail != 65535+4096 || h.st.inflow.avail != 65535+4096 || h.sc.inFrameScheduleLoop || h.sc.writingFrame {
				t.Fatal("error left invalid accounting or scheduling state")
			}
			wantAccepted := 0
			if short {
				wantAccepted = 1
			}
			if len(h.c.accepted) != wantAccepted {
				t.Fatal("unexpected data accepted after failed transport write")
			}
		}
	}
}

func TestBodyReadCoalescingOtherWindowUpdateCallersUnchanged(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		h := newWindowUpdateHarness(t, enabled)
		// The direct helper path used by padding/discard/initial window updates
		// keeps immediate scheduling, even with the noteBodyRead-only opt-in.
		h.sc.sendWindowUpdate32(nil, 4096)
		h.sc.sendWindowUpdate32(h.st, 4096)
		h.drain(t)
		if len(h.c.writes) != 2 {
			t.Fatal("changed a caller outside noteBodyRead")
		}
	}
}

func TestBodyReadCoalescingStreamClosedWhileQueued(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		h := newWindowUpdateHarness(t, enabled)
		h.sc.writingFrame = true
		h.sc.noteBodyRead(h.st, 4096)
		// Equivalent scheduler removal to closeStream, before a writer is free.
		h.st.state = stateClosed
		h.sc.writeSched.CloseStream(h.st.id)
		delete(h.sc.streams, h.st.id)
		h.sc.writingFrame = false
		h.sc.scheduleFrameWrite()
		h.drain(t)
		frames := h.frames(t)
		if len(frames) != 1 {
			t.Fatalf("queued stream close emitted %d frames, want only connection update", len(frames))
		}
		assertWindowUpdate(t, frames[0], 0, 4096)
	}
}
