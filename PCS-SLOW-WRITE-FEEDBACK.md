# Opt-in slow-write DATA quantum candidate

Status: nine targeted tests passed in normal and race modes on 2026-10-18
(08:45:20–08:46:52 and 08:47:11–08:48:22 UTC). The exact tested runtime
commit is `7036bdb`; [results and complete test logs](docs/results/slow-write-feedback-20261018/summary.json)
include the earlier disk-guard interruption. Full integration, performance
acceptance and production deployment remain pending. Earlier fixed-quantum
experiments (88 rounds, HOLD) remain unchanged.

Base: `19484587`. The only runtime changes are `http2/server.go` and the new
`http2/writesched_proxy_feedback.go`. Existing fixed 0/16384 constructors retain
their accepted inputs and behavior. An application must explicitly return
`http2.NewProxyWriteSchedulerWithSlowWriteFeedback()` from `Server.NewWriteScheduler`;
there is no implicit environment switch or Caddy configuration change here.

Policy constants are frozen: successful DATA or flush writer call duration
**>=100 ms**, pressure window **[completion, completion+2 s)**, DATA quantum
**4096 bytes** while **at least two scheduler streams are open**. A subsequent
successful qualifying write refreshes the window. One open stream keeps the
normal peer/flow-limited turn; opening another during the window makes it
eligible. A failed write or a late result for an already-closed DATA stream does
not invoke the policy callback. A flush with no remaining open streams cannot
refresh pressure. Expiry is checked on selection; there is no background timer.

The existing proxy scheduler still owns control priority, the independent normal
and preferred round-robin cursors, the eight-head preference limit, per-stream
FIFO, frame/connection/stream limits, END_STREAM and final handler completion.
The candidate cannot preempt a selected frame, unqueue kernel bytes, change TLS
records, accelerate client ACKs, or prove two streams are two active transfers.

## Timing and ownership

Policy state and scheduler mutation belong to the serve goroutine. The writer
uses an immutable concurrency-safe clock and returns duration/completion time in
its existing result message. Async timing surrounds `framer.endWrite`; synchronous
timing surrounds the buffered frame writer call; flush timing surrounds `Flush`.
DATA header construction before the async writer begins is outside this duration.
Waiting to launch the writer and waiting for the serve goroutine to process its
result are excluded. Scheduling stalls during the actual timed call can remain.
This is buffer/TLS/socket write wall time, **not delivery time or a network cause**.

Default schedulers do not opt into the private interface and perform no new clock
reads. The candidate does add fixed result metadata (time.Time+Duration, 32 bytes
on this amd64 ABI), immutable connection fields and conditional checks to the
shared path. Therefore default-off cost must be measured; it is not claimed zero.
The opt-in wrapper has bounded per-connection state and creates no per-packet log,
background goroutine, timer or policy allocation per completion.

## Targeted validation and remaining acceptance

Eight deterministic unit tests cover threshold/expiry nanosecond boundaries,
single-to-multiple streams, successful/failed repeated flush refresh, closed
queues, actual-completion versus serve handoff, default-off/no-control timing,
no failed/closed callback, FIFO/flow/peer cap/END_STREAM/done/close recycling and
both-cursor fairness. The existing fixed-quantum regression tests remain intact.

`TestProxySlowWriteFeedbackLateDATARealTLS` has three cases: successful DATA at
100 ms, successful flush at 100 ms, and successful DATA at 100 ms minus 1 ns.
Its first gate blocks a real verified TLS socket Write; an atomic fake clock is
advanced while that write is blocked. The test never directly calls feedback.
The server's real successful completion invokes the callback, and the test
requires the raw write to have returned. The first seed stays unfragmented.
A second gate then admits a 578-byte DATA competitor only after bulk selection.
Expected complete client receive ordering is 4096 bulk bytes before the competitor
for the two qualifying cases versus 65536 bytes for the below-threshold case.
All six streams require full SHA256 and END_STREAM; failure cleanup releases both
gates before connection/server shutdown. Clock duration is injected logic time,
**not a measured 100 ms transport stall or evidence of a latency improvement**.

Before performance conclusions, use real `time.Now` in a separately frozen,
calibrated private-netns experiment. Preserve actual RTT, effective DATA frame
size, flow-credit qualification, raw-write pressure, verified TLS, offload and
qdisc state, same-lane complete client timing, and quota evidence. Small response
P95 must fall by both at least 30% and 100 ms; throughput loss cannot exceed 5%,
CPU per effective byte cannot rise over 20%, and non-target small-response P95
cannot regress over 20%. Resource-invalid rounds cannot establish these gates.
The exact workload/order, repetitions and resource gates must be frozen before
that experiment; no performance run is authorized by this document.
