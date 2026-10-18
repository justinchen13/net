# Late competing DATA over verified TLS

A production HTTP/2 observation showed a 64KiB DATA frame still being written while another tunnel had a 578B/852B fragment queued. The competing fragments arrived about0.3–0.4ms after the large frame had already been selected. They belonged to a longer tunnel, not proven independent short requests. TCP was making slow progress in recovery; HTTP/2 credit remained available.

The new deterministic regression uses one certificate-verified TLS1.3/h2 connection with six response streams open. Only two streams offer DATA during the controlled gate. It holds the selected first bulk DATA at its next underlying TLS socket write, then lets a578B fragment enqueue on another stream before releasing the writer. Channels establish ordering; no sleep chooses a race. It verifies every payload by length/SHA256 and every expected END_STREAM.

Execution2026-10-18T07:49:13.126374–07:50:40.290271Z passed, using Go1.25.6 with0.35CPU,512MiB,no additional swap and no external network. The Go test itself took0.111s; most wall time was compilation. Immutable builder, source and file hashes, actual resource constraints, complete log, container exit0/noOOM and verified owned cleanup are retained in [results](results/late-data-20261018/result.json).

- Existing q0 policy: the competing payload was received after65536bulk bytes.
- Existing q16KiB policy: the competing payload was received after16384bulk bytes.

This proves selection/receive ordering and late-arrival applicability. It does not quantify end-to-end latency or eliminate TCP head-of-line blocking, and is not a new performance acceptance. The previous88-trial performance HOLD and numerical gates are unchanged; the fixed-quantum candidate is not deployed. No production code was changed in this branch. A future feedback-based candidate must separately preserve fairness/control priority/FIFO/flow accounting and pass frozen latency, delivered-throughput, CPU and memory gates.
