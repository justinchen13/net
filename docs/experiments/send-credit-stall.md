# Experimental connection send-credit stall policy (not run or deployed)

Base: `ca43f763b381b813d5fb2c1344a332a0441f91ab`. The shared GOAWAY
correctness change propagates a failed Flush. Both policy-off and policy-on
comparisons include it. No runtime test, benchmark or production claim has been
made for this branch.

## Outbound connection-credit audit

The connection outflow starts at 65535. Valid connection WINDOW_UPDATE adds its
increment through `processWindowUpdate` / `outflow.add`. Stream SETTINGS and
stream WINDOW_UPDATE affect stream credit, not the connection balance.
`FrameWriteRequest.Consume` is the only DATA debit: it takes the selected positive
DATA bytes from both stream and connection budgets, bounded by effective credit,
maximum frame size and the requested selection cap. Non-DATA and zero DATA do
not debit. A split consumes only the returned prefix; the queued remainder has
not yet consumed credit.

The scheduler `Pop` calls Consume on the serve goroutine and immediately returns
the selected frame to `startFrameWrite` in that same scheduler turn. A received
RST cannot interleave between Pop and startFrameWrite on that goroutine. Once a
write is in flight, its DATA credit has been consumed; a write failure closes the
connection. Closing a stream removes its queued, unconsumed remainder. It does
not refund already consumed outbound DATA. `closeStream`'s buffered-body return
is **inbound** connection credit and is not an outbound refund.

Thus the source invariant is initial 65535 + accepted connection WU increments
minus selected DATA bytes. Selected bytes can be buffered or in an active write;
this invariant is not a TCP ACK ledger. No continuing-connection debit-without-
selected-DATA defect was found by this static audit. Unit source will separately
exercise partial DATA and RST/drop of the unconsumed remainder. These conclusions
do not reconstruct the real connection's historical credit ledger: the existing
WU counter has no per-frame increment or stream/connection distinction.

## Policy boundary

The unfinished proposal is an opt-in mitigation for future requests after a fresh PING response,
not a diagnosis of why a client withheld flow credit. Existing streams remain
on the old connection; no requests are replayed, moved, or canceled by the
policy. A NO_ERROR GOAWAY can leave a stalled old connection alive indefinitely.
`writingFrame == false` is only an application writer condition, not evidence
that the kernel send queue is empty. Deployment requires separate integration,
resource and performance qualification.

## Round stopped before policy implementation

Only the shared `writeGoAway.writeFrame` Flush-error fix, three GOAWAY test
functions, two outbound credit-conservation test functions, and this static
audit are present. The stall detector, enable callback, policy timer/nonce,
PING/ACK state machine, graceful-drain integration, and real-peer policy tests
were **not implemented**. No Go tools, tests, builds, network experiments,
or deployment were run for this worktree. New Go test files are still
pre-format source. All tests remain NOT_RUN. Root is preserving this exact
unvalidated draft on an experimental branch at round closure; that commit
and publication are not test or release acceptance.

A future active policy probe must have an independent nonce/timer and abandon
only the policy on timeout. It must not borrow the ordinary health PING's
timeout to introduce a new hard close of existing CONNECT streams. Ordinary
health PING behavior must remain unchanged.

The observed real wait is an H2 flow-credit boundary; why the peer withheld
credit is unknown. Static source accounting did not establish a debit bug, and
the GOAWAY Flush fix does not resolve that observed stall or establish a speed
benefit. No policy or production release is qualified by this draft.
