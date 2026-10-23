// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build go1.25 || goexperiment.synctest

package http2

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2/hpack"
)

func creditDrainCount(q *sendCreditDrainQuota) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inUse
}

type creditDrainEvents struct {
	mu     sync.Mutex
	counts map[string]int
}

func (e *creditDrainEvents) record(_ context.Context, o ServerObservation) {
	if !strings.HasPrefix(o.Event, "send_credit_") {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.counts == nil {
		e.counts = make(map[string]int)
	}
	e.counts[o.Event]++
}

func (e *creditDrainEvents) want(t testing.TB, want map[string]int) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	for event, n := range want {
		if e.counts[event] != n {
			t.Errorf("event %s=%d, want %d", event, e.counts[event], n)
		}
	}
	for event, n := range e.counts {
		if want[event] != n {
			t.Errorf("unexpected event %s=%d", event, n)
		}
	}
}

// synctestNetConn.Close signals the peer's read/write halves, but does not
// interrupt this endpoint's own blocked Read or Write. These tests initiate
// server-side closure (including wroteFrame's I/O-error path), so the server
// endpoint must also honor net.Conn.Close's local-unblock contract. Otherwise
// its frame reader remains blocked and ServeConn correctly has not yet exited.
// This adapter is local to this fixture; it does not change runtime behavior.
type creditDrainTestConn struct{ *synctestNetConn }

func (c *creditDrainTestConn) Close() error {
	err := c.synctestNetConn.Close()
	c.loc.setReadError(net.ErrClosed)
	c.rem.setWriteError(net.ErrClosed)
	return err
}

// This native-frame fixture shares the exact *Server across connections. It
// uses the same serveConn implementation and in-memory connection as
// newServerTester, without allocating a new Server for every connection.
func newCreditDrainTester(t testing.TB, shared *Server, h1 *http.Server) (*serverTester, <-chan struct{}) {
	t.Helper()
	cli, srv := synctestNetPipe()
	cli.SetReadDeadline(time.Now())
	cli.autoWait = true
	st := &serverTester{t: t, cc: cli, h1server: h1, h2server: shared}
	st.hpackEnc = hpack.NewEncoder(&st.headerBuf)
	st.testConnFramer = testConnFramer{t: t, fr: NewFramer(cli, cli), dec: hpack.NewDecoder(initialHeaderTableSize, nil)}
	returned := make(chan struct{})
	connc := make(chan *serverConn)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Host == "fill.test" {
			_, _ = w.Write(bytes.Repeat([]byte{'f'}, initialWindowSize))
			return
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			return
		}
		if _, err := w.Write(bytes.Repeat([]byte{'d'}, creditTestBodySize)); err != nil {
			return // cancellation/connection closure is deliberate in these tests
		}
		_ = http.NewResponseController(w).Flush()
	})
	go func() {
		shared.serveConn(&creditDrainTestConn{srv}, &ServeConnOpts{Handler: handler, BaseConfig: h1}, func(sc *serverConn) { connc <- sc })
		close(returned) // after the outer defer has released any lease
	}()
	st.sc = <-connc
	t.Cleanup(func() { st.Close() })
	st.sync()
	st.greet()
	st.writeSettings(Setting{SettingInitialWindowSize, 1 << 20})
	st.wantSettingsAck()
	creditTestRequest(st, 1, "fill.test")
	creditTestDrain(st, map[uint32][]byte{1: bytes.Repeat([]byte{'f'}, initialWindowSize)})
	creditTestRequest(st, 3, "old-a.test")
	st.wantHeaders(wantHeader{streamID: 3})
	creditTestRequest(st, 5, "old-b.test")
	st.wantHeaders(wantHeader{streamID: 5})
	creditTestOnServe(st, func(sc *serverConn) {
		if !sc.sendCreditDataEligible() || sc.sendCreditDrainQuota == nil {
			t.Fatal("native fixture did not establish eligible DATA and a shared quota")
		}
	})
	return st, returned
}

func creditDrainWantReturned(t testing.TB, returned <-chan struct{}) {
	t.Helper()
	select {
	case <-returned:
	default:
		t.Fatal("ServeConn has not returned")
	}
}

func TestProxySendCreditDrainNativeLeaseLifetime(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name := "direct"
		if configured {
			name = "configured"
		}
		t.Run(name, func(t *testing.T) {
			synctestTest(t, func(t testing.TB) {
				var events creditDrainEvents
				shared := &Server{SendCreditStallTimeout: creditTestTimeout, MaxConcurrentSendCreditStallDrains: 1,
					ReadIdleTimeout: time.Hour, PingTimeout: time.Second, WriteByteTimeout: 10 * time.Second,
					NewWriteScheduler: NewProxyWriteScheduler, Observe: events.record}
				h1 := &http.Server{}
				if configured {
					if err := ConfigureServer(h1, shared); err != nil {
						t.Fatal(err)
					}
				}
				a, aReturned := newCreditDrainTester(t, shared, h1)
				b, bReturned := newCreditDrainTester(t, shared, h1)
				q := a.sc.sendCreditDrainQuota
				if q != b.sc.sendCreditDrainQuota || creditDrainCount(q) != 0 {
					t.Fatal("connections did not share one initially empty quota")
				}
				a.advance(creditTestTimeout)
				pa := readFrame[*PingFrame](t, a)
				pb := readFrame[*PingFrame](t, b)
				a.writePing(true, pa.Data)
				a.wantGoAway(5, ErrCodeNo)
				if creditDrainCount(q) != 1 {
					t.Fatal("GOAWAY completion released the retirement lease")
				}
				b.writePing(true, pb.Data)
				b.wantIdle()
				events.want(t, map[string]int{"send_credit_probe_queued": 2, "send_credit_probe_written": 2,
					"send_credit_probe_ack": 2, "send_credit_drain_queued": 1,
					"send_credit_goaway_written": 1, "send_credit_drain_limit": 1})
				creditTestOnServe(b, func(sc *serverConn) {
					if sc.inGoAway || sc.sendCreditDrainLease || !sc.sendCreditStall.tried || sc.sendCreditStall.probe != nil {
						t.Fatal("full quota did not abandon this generation without retirement")
					}
					if sc.streams[3] == nil || sc.streams[5] == nil {
						t.Fatal("full quota canceled existing streams")
					}
				})
				if b.cc.(*synctestNetConn).IsClosedByPeer() {
					t.Fatal("full quota closed the nonretired connection")
				}
				b.closeConn()
				b.sync()
				creditDrainWantReturned(t, bReturned)
				if creditDrainCount(q) != 1 {
					t.Fatal("a failed reservation released another connection's lease")
				}

				// Reset, stream/connection WU, and last-stream completion are not exit.
				a.writeRSTStream(3, ErrCodeCancel)
				a.writeWindowUpdate(5, 1)
				creditTestOnServe(a, func(sc *serverConn) {
					if sc.streams[3] != nil || sc.streams[5] == nil || !sc.sendCreditDrainLease || creditDrainCount(q) != 1 {
						t.Fatal("cancel or WU released an active retired connection's lease")
					}
				})
				a.writeWindowUpdate(0, creditTestBodySize)
				creditTestDrain(a, map[uint32][]byte{5: bytes.Repeat([]byte{'d'}, creditTestBodySize)})
				a.sync()
				if creditDrainCount(q) != 1 {
					t.Fatal("last stream completion released before ServeConn exit")
				}
				select {
				case <-aReturned:
					t.Fatal("fixture missed the interval before graceful exit")
				default:
				}
				a.advance(goAwayTimeout)
				a.wantClosed()
				creditDrainWantReturned(t, aReturned)
				if creditDrainCount(q) != 0 {
					t.Fatal("actual graceful ServeConn exit leaked a lease")
				}

				c, cReturned := newCreditDrainTester(t, shared, h1)
				c.advance(creditTestTimeout)
				pc := readFrame[*PingFrame](t, c)
				c.writePing(true, pc.Data)
				c.wantGoAway(5, ErrCodeNo)
				if creditDrainCount(q) != 1 {
					t.Fatal("next connection could not reuse the released slot")
				}
				c.closeConn()
				c.sync()
				creditDrainWantReturned(t, cReturned)
				if creditDrainCount(q) != 0 {
					t.Fatal("connection error exit leaked the reused slot")
				}
			})
		})
	}
}

func TestProxySendCreditDrainObservationGatesAndTimeout(t *testing.T) {
	for _, name := range []string{"enabled", "observer_hidden", "policy_off"} {
		t.Run(name, func(t *testing.T) {
			synctestTest(t, func(t testing.TB) {
				var events creditDrainEvents
				enabled := name != "policy_off"
				st, _ := newCreditTestStall(t, enabled, func(s *Server) {
					s.ReadIdleTimeout = time.Hour
					s.Observe = events.record
					s.ObserveEnabled = func(context.Context) bool { return name != "observer_hidden" }
				})
				st.advance(creditTestTimeout)
				if enabled {
					p := readFrame[*PingFrame](t, st)
					if p.IsAck() {
						t.Fatal("expected policy probe")
					}
					if creditDrainCount(st.sc.sendCreditDrainQuota) != 0 {
						t.Fatal("probe reserved before its ACK")
					}
					st.advance(2 * time.Second)
				}
				st.wantIdle()
				if name == "enabled" {
					events.want(t, map[string]int{"send_credit_probe_queued": 1,
						"send_credit_probe_written": 1, "send_credit_probe_timeout": 1})
				} else {
					events.want(t, nil)
				}
			})
		})
	}
}

func TestProxySendCreditDrainGoAwayWriteErrorReleases(t *testing.T) {
	synctestTest(t, func(t testing.TB) {
		s := &Server{SendCreditStallTimeout: creditTestTimeout, MaxConcurrentSendCreditStallDrains: 1,
			ReadIdleTimeout: time.Hour, PingTimeout: time.Second, WriteByteTimeout: time.Second,
			NewWriteScheduler: NewProxyWriteScheduler}
		st, returned := newCreditDrainTester(t, s, &http.Server{})
		st.advance(creditTestTimeout)
		p := readFrame[*PingFrame](t, st)
		// Probe Flush succeeded; fail only subsequent server writes. This is
		// a controlled in-memory I/O error, not a TCP-loss simulation.
		st.cc.(*synctestNetConn).loc.setWriteError(errors.New("test GOAWAY write failure"))
		st.writePing(true, p.Data)
		st.sync()
		creditDrainWantReturned(t, returned)
		if !st.sc.inGoAway || st.sc.sendCreditDrainLease || creditDrainCount(st.sc.sendCreditDrainQuota) != 0 {
			t.Fatal("GOAWAY I/O failure did not return its lease on ServeConn exit")
		}
	})
}

func TestProxySendCreditDrainInitializationAndCopies(t *testing.T) {
	for _, cap := range []int{0, -1, -100} {
		s := &Server{SendCreditStallTimeout: time.Second, MaxConcurrentSendCreditStallDrains: cap}
		q := s.sendCreditDrainQuotaForConn()
		if q.limit != defaultMaxSendCreditStallDrains {
			t.Fatalf("cap %d bypassed finite default: %d", cap, q.limit)
		}
	}
	off := &Server{}
	if off.sendCreditDrainQuotaForConn() != nil || off.sendCreditDrains != nil {
		t.Fatal("disabled policy allocated a quota")
	}
	before := Server{SendCreditStallTimeout: time.Second, MaxConcurrentSendCreditStallDrains: 1}
	independent := before
	q := before.sendCreditDrainQuotaForConn()
	if independent.sendCreditDrainQuotaForConn() == q {
		t.Fatal("uninitialized Server copies unexpectedly share quota")
	}
	after := before // nonconcurrent initialized copy shares the fixed quota
	if after.sendCreditDrainQuotaForConn() != q {
		t.Fatal("initialized Server copy lost shared quota")
	}
	if err := ConfigureServer(&http.Server{}, &before); err != nil {
		t.Fatal(err)
	}
	if before.sendCreditDrainQuotaForConn() != q || q.limit != 1 {
		t.Fatal("ConfigureServer reset the quota")
	}
}

func TestProxySendCreditDrainConcurrentInitialization(t *testing.T) {
	const workers = 32
	s := &Server{SendCreditStallTimeout: time.Second, MaxConcurrentSendCreditStallDrains: 3}
	out := make(chan *sendCreditDrainQuota, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); out <- s.sendCreditDrainQuotaForConn() }()
	}
	wg.Wait()
	close(out)
	var first *sendCreditDrainQuota
	for q := range out {
		if first == nil {
			first = q
		}
		if q != first || q.limit != 3 {
			t.Fatal("concurrent initialization split a Server quota")
		}
	}
	other := &Server{SendCreditStallTimeout: time.Second, MaxConcurrentSendCreditStallDrains: 3}
	if other.sendCreditDrainQuotaForConn() == first {
		t.Fatal("distinct Servers share one quota")
	}
}

func TestProxySendCreditDrainConcurrentReservations(t *testing.T) {
	const workers, cap = 32, 3
	q := &sendCreditDrainQuota{limit: cap}
	ready := make(chan bool, workers)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sc := &serverConn{sendCreditDrainQuota: q}
			ready <- sc.reserveSendCreditDrainLease()
			<-release
			sc.releaseSendCreditDrainLease()
			sc.releaseSendCreditDrainLease() // idempotent owner cleanup
		}()
	}
	acquired := 0
	for i := 0; i < workers; i++ {
		if <-ready {
			acquired++
		}
	}
	if acquired != cap || creditDrainCount(q) != cap {
		t.Errorf("reservations=%d, in use=%d, want %d", acquired, creditDrainCount(q), cap)
	}
	close(release)
	wg.Wait()
	if creditDrainCount(q) != 0 {
		t.Fatal("concurrent cleanup leaked or double-released leases")
	}
}
