# Body-read WINDOW_UPDATE coalescing candidate

Status: static candidate only; not compiled, tested, published, or deployed.
Base: `ca43f763b381b813d5fb2c1344a332a0441f91ab`. This is independent of the
slow-write-feedback / DATA-quantum candidate.

The preceding four-arm experiment changed only when a synthetic 64 KiB upload
started. Delaying it by five seconds removed the early paired WINDOW_UPDATE and
35-byte TCP-payload burst, and reduced the early sampled BBR model peak in both
pairs. Network RTT/drop qualification still failed. This supports investigating
the coupling; it does not prove a Linux rate_sample mechanism or a speed benefit.

## Narrow intervention

`Server.CoalesceBodyReadWindowUpdates` is an explicit default-false opt-in.
When enabled, `noteBodyRead` queues the connection update and, if permitted by the
stream state, the stream update before scheduling once in the same serve event.
It does not wait for a timer, a future frame, or another handler notification.

The existing `writeFrame` enqueue body is extracted intact to `queueFrame`.
The ordinary wrapper still enqueues and schedules immediately. The shared
`queueWindowUpdate` performs exactly the existing `inflow.add` operation once
per eligible window. A zero return still does not schedule. `noteBodyRead`
does not borrow or modify `inFrameScheduleLoop` or `writingFrame`, and adds no
connection state, goroutine, clock, lock, atomic, or per-event allocation.

The 4 KiB accumulation and low-available-window emergency conditions are unchanged.
Connection credit is still returned for closed/half-closed-remote streams, with
no stream update in those states. Half-closed-local streams remain eligible.
Control priority, stream FIFO, DATA flow accounting, completion/error handling,
TLS, authentication and timeouts remain in their existing paths.

Other callers of `sendWindowUpdate` and `sendWindowUpdate32` retain immediate
scheduling. In particular, padding/zero-padding low-window refunds in processData,
initial connection capacity, discarded DATA, and stream-close refunds are not
batched by this flag. They may still produce short control writes; the candidate
does not promise to eliminate all small TCP packets.

The expected two-into-one lower Write is specific to an initially empty writable
scheduler/buffer. Existing queued frames, a full buffer, or an asynchronous writer
can change the batching outcome. A coalesced Write is not a guaranteed TLS-record
or packet boundary. Failure may change which lower Write attempt contains a frame;
the contract preserves reported transport failure, connection closure and window
accounting, not the deliberately changed lower Write segmentation.

## Deterministic regression design

`http2/server_window_update_coalescing_test.go` uses an in-memory `net.Conn` under
the real HTTP/2 framer, buffer and scheduler, with channel gates and actual
`wroteFrameCh` completion handling. It never opens a network socket. It checks:

- Empty queue: OFF gives two 13-byte lower Writes; ON gives one 26-byte Write;
  both decode to the same ordered two WINDOW_UPDATE frames and increments.
- 4095/4096-byte threshold, emergency low/zero window, pre-existing unsent credit,
  zero newly consumed bytes, only-one-update and closed stream states.
- An existing scheduling guard remains owned by its caller; global control order,
  stream FIFO and DATA flow blocking remain intact.
- A real blocked async lower Write permits the body-read notification to queue
  and return; release drains without losing or duplicating frames.
- Transport errors and short-write errors propagate and close the connection;
  window accounting and scheduling state remain valid.
- Other update callers and a stream closed while its update is queued retain
  their original behavior.

The test setter uses reflection solely so this same file also compiles on the
frozen old base. With the option absent it leaves the base unchanged. Running only
`TestBodyReadCoalescingLowerWrites/coalesced` on that old base should fail because
the observed lower Write sizes are `[13,13]`, not because a new symbol is missing.
This negative result must be actually executed and retained; it is not claimed yet.

## Acceptance and cost boundaries

The new flag check is only in `noteBodyRead`; default paths also acquire the
small helper-call structure introduced by the enqueue extraction. No claim that
OFF is cost-free is made. Old production versus new OFF must be checked separately
from new OFF versus ON, with the original CPU/throughput safeguards.

Before any network experiment: exact-base negative regression, candidate targeted
normal/race tests, existing HTTP2/HPACK regressions, and a full Caddy CLI build with
the option wired explicitly are required. Use separately reviewed remote CI with
exact source/toolchain/action/artifact binding, not a new local compilation here.

Only after those checks can the existing trusted-TLS fixture compare unchanged
GO-time upload between old and new server ABBA arms. Keep network, load, capture,
resource and cleanup settings fixed. Header/WU counts and BBR-model changes are
mechanism observations; original network disqualification remains visible.
Any performance claim still requires the predeclared end-to-end small completion
P95 improvement of at least 30% and 100 ms plus unchanged CPU/throughput safeguards.
No queue enlargement or weaker qualification gate is authorized by this candidate.
