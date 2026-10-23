// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import "sync"

const defaultMaxSendCreditStallDrains = 16

// The short global lock only initializes an enabled Server's pointer. It
// retains no Server references and is not used by frame handling or leases.
// Keeping all locks behind a pointer preserves Server's copyable layout.
var sendCreditDrainInitMu sync.Mutex

type sendCreditDrainQuota struct {
	mu    sync.Mutex
	limit int
	inUse int
}

func (s *Server) sendCreditDrainQuotaForConn() *sendCreditDrainQuota {
	if s.SendCreditStallTimeout <= 0 {
		return nil
	}
	sendCreditDrainInitMu.Lock()
	defer sendCreditDrainInitMu.Unlock()
	if s.sendCreditDrains == nil {
		limit := s.MaxConcurrentSendCreditStallDrains
		if limit <= 0 {
			limit = defaultMaxSendCreditStallDrains
		}
		s.sendCreditDrains = &sendCreditDrainQuota{limit: limit}
	}
	return s.sendCreditDrains
}

func (q *sendCreditDrainQuota) tryAcquire() bool {
	if q == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.inUse >= q.limit {
		return false
	}
	q.inUse++
	return true
}

func (sc *serverConn) reserveSendCreditDrainLease() bool {
	// Called only after fresh ACK, successful Flush, current eligibility, and
	// an idle writer are established. A full quota abandons this generation;
	// the caller already cleared its probe and retains tried=true.
	if sc.sendCreditDrainLease || !sc.sendCreditDrainQuota.tryAcquire() {
		return false
	}
	sc.sendCreditDrainLease = true
	return true
}

func (sc *serverConn) releaseSendCreditDrainLease() {
	// Only the outer ServeConn defer calls this in production. A connection
	// which did not acquire a lease cannot release another connection's slot.
	if !sc.sendCreditDrainLease {
		return
	}
	sc.sendCreditDrainLease = false
	q := sc.sendCreditDrainQuota
	q.mu.Lock()
	q.inUse--
	q.mu.Unlock()
}
