# Experimental connection send-credit stall policy

Status at R13 closure: implemented and functionally tested; default off,
not integrated into production Caddy, not performance-qualified or deployed.
Base: `ca43f763b381b813d5fb2c1344a332a0441f91ab`.

## Policy

`Server.SendCreditStallTimeout <= 0` allocates no watchdog state/timer and does
not scan streams. A positive timeout enables an independent serve-owned probe.
Eligibility requires zero connection send credit and at least two queued
positive-DATA streams with positive own-stream credit. T measures continuous
eligibility without DATA selection/completion progress. Ordinary control writes
do not reset T; the writer must be idle to start a probe and decide retirement.
This does not imply that TCP was idle throughout T. The optional enable callback
is an operator-supplied connection-context selector; it does not authenticate
requests by itself.

The probe owns its random nonce, generation and timer. It never borrows normal
health PING state or timeout scheduling. Fresh matching ACK and successful Flush
completion must both be observed while eligibility remains before the internal
NO_ERROR GOAWAY path runs. ACK may arrive before the completion notification.
No ACK abandons that generation's one probe without a new hard-close deadline.
An actual PING Write/Flush I/O error still follows the existing connection-error
close path. WINDOW_UPDATE, DATA progress, cancellation or lost eligibility
invalidate the generation; another active frame during a probe conservatively
invalidates it too. The probe's own successful Flush is exempt.

Existing CONNECTs are not migrated, replayed or canceled by graceful retirement.
They resume only if the peer provides real credit. GOAWAY does not rescue an
already stalled old tunnel or prove why the peer withheld credit.

## Executed validation

- The shared GOAWAY Flush fix: old production writer reproduces two expected
  failures and one positive; candidate passes all three. Two split/cancel and
  control/stream-credit tests also pass.
- Initial candidate targets: 13 top-level / 23 including subtests pass.
- Full ordinary regression on the exact same runtime: HTTP2 450 top-level /
  625 including subtests pass, one skip; HPACK 35 pass, one skip.
- Two additional order tests pass ordinary execution. They hold a real control
  Flush in a bounded synthetic connection across T with tried=false; and use
  actual wire PING/ACK with a deliberately delayed wroteFrame notification to
  test ACK-before-completion. These test state ordering, not physical TCP timing.
- Final focused HTTP2 race regression: 42 top-level / 120 including subtests
  pass. Unchanged HPACK full race: 35 pass, one skip. No race failures observed.

ServeConn tests use actual H2/HPACK frames over synthetic net.Conn/time. The peer
reads GOAWAY; old bodies remain blocked until legitimate WU and then match exact
expected bytes. A fresh outer is explicitly opened by the test. Automatic
Shadowrocket repooling is not demonstrated. Normal health PING tests remain in
the focused race set. All Go runs use pinned Go1.25.6 images, offline checked
modules, serialized .4CPU/512MiB/no extra swap containers with cleanup verified.

## Release gaps

Retired old connections may live indefinitely; there is no global outstanding
retirement lease cap yet. Enabled eligibility scans stop at two matches but
worst-case O(streams) cost remains; 512-stream throughput/CPU/tail acceptance is
not run. Actual Caddy TLS/auth/ACL/padding integration, automatic client repooling,
full release security/cost/performance gates and post-release observation remain
open. No production speed improvement or historical root-cause repair is claimed.
R05/R10 failed performance candidates remain HOLD.

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
selected-DATA defect was found by this static audit. Passing unit tests exercise partial DATA and RST/drop of the unconsumed remainder. These conclusions
do not reconstruct the real connection's historical credit ledger: the existing
WU counter has no per-frame increment or stream/connection distinction.


## Historical R12 boundary

At R12 close only the Flush draft and five unformatted tests existed; all tests
were NOT_RUN and the policy was not implemented. R13 implemented and tested the
prototype as listed above. Two review defects were corrected before execution:
a redundant synthetic Flush could invalidate its own probe, and completion hooks
must run before replyToWriter releases/nils a DATA writer. The original real
58.19s peer-credit withholding cause remains unknown. Central evidence and round
history are maintained in PCS/caddy/OPERATIONS.md, not as vendored source there.
