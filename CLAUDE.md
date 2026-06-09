# Architecture notes

## Capacity Headroom: effective fleet utilization

`queryCapacityTimeseries` in `src/key-manager.ts` returns
`effectiveFleetUtilization` per (bucket, window), used directly by the
dashboard via `headroom = 1 - effectiveFleetUtilization`.

Two corrections over raw sample averaging:

- **Forward-fill.** A key with persisted utilization (`lastSeenAt < bucketEnd`
  and `resetAt > bucketStart`) but no sample in the bucket still counts at
  that utilization. Without this, a key in cooldown stops emitting samples
  and silently disappears, inflating apparent headroom.

- **Cross-window fold.** On the 5h line, a key whose 7d utilization is at
  the cap (`>= 1.0`, sampled OR forward-filled) counts as 100% util
  regardless of its 5h state — a weekly-blocked key can serve no traffic.
  The 7d line is unaffected by 5h state.

Keys not on the live roster never contribute, even if they have rows in
`capacity_window_timeseries`. Fleet members with no sample and no persisted
state count as 0% (unknown = full headroom).

`sanitizeCapacityWindows` drops `entry.capacity.windows` entries whose
`resetAt` has passed, and `normalizePrimaryCapacityWindow` rewrites
`status="rejected"` to `allowed`/`allowed_warning` — so persisted
`status="rejected"` is never observable. The cross-fold relies on
`utilization >= 1.0`, not status.

## First-chunk timeout: per-request

`proxyRequest` computes `requestFirstChunkTimeoutMs` once at entry from the
incoming `anthropic-beta` header:

- Header contains `context-1m` → `config.firstChunkTimeoutMsContext1m`
  (env `FIRST_CHUNK_TIMEOUT_MS_CONTEXT_1M`, default **120 000 ms**).
- Otherwise → `config.firstChunkTimeoutMs`
  (env `FIRST_CHUNK_TIMEOUT_MS`, default **45 000 ms**).

The chosen value is used for the abandon deadline, the failure-result
message, and all retry-diagnostic log entries within the same request, so
operators see the threshold the request was actually held against.

Reason for the split: 1M-context requests have materially higher
time-to-first-SSE-byte on Anthropic's side (KV-cache assembly). With a
single 16 s threshold, 1M traffic abandons ~7% of the time on cold
caches while normal traffic abandons <1%.

## Upstream stream reaper: lifecycle-independent

A streamed upstream's teardown used to be driven entirely by the downstream
consumer: every cleanup path lived inside the response `ReadableStream`'s
`pull()` (the in-pull idle check) or `cancel()`. Both only fire while the
client keeps reading. When an upstream delivered its first SSE chunk then went
silent and the client stopped pulling (gone, or a half-open peer Bun never
detects), `pull()` was never called again, the idle timeout never armed, and
the `ESTABLISHED` socket to Anthropic was held indefinitely — observed open
15–72 h, accumulating and consuming per-key Anthropic concurrency.

The fix makes `activeStreams` the authoritative registry and reaps off a
wall-clock timer instead of consumer demand:

- **`reapStaleActiveStreams(now)`** runs from a `setInterval` in `startServer`
  (`STREAM_REAPER_INTERVAL_MS`, 30 s; `.unref()`d; cleared on shutdown). It
  abandons any flowing stream whose silence (`now - lastChunkAt`) exceeds its
  `idleTimeoutMs` (`config.streamIdleTimeoutMs`, env `STREAM_IDLE_TIMEOUT_MS`,
  default **120 000 ms**). It is also the only time-driven heartbeat for the
  "Active stream snapshot" — which is otherwise emitted only on chunk arrival,
  so a silent leaking fleet logged nothing.

- **One idempotent `reap(reason)`** per entry (built in `attachUpstreamReaper`
  once the upstream reader exists): `abortController.abort` (the real lever
  that closes the Anthropic socket) → fire-and-forget `reader.cancel` (never
  awaited — it stalls on exactly these dead streams) → `releaseLock` →
  `observer.abandon`. Idempotent via registry membership. Every teardown path
  funnels through it: the in-pull idle timeout, a read error, the downstream
  `cancel()`, and the reaper. Only the natural `done` path calls
  `observer.finish()`. Distinct reasons (`reaped_idle`, `stream_idle_timeout`,
  `downstream_cancelled`, `stream_read_failed_after_first_chunk`) plus a
  `totalStreamsReaped` counter in the snapshot quantify what the old
  consumer-driven path was missing.

A stream waiting for its first chunk is owned by the first-chunk timeout, not
the reaper: until the reader is attached its `idleTimeoutMs` is `Infinity` and
its `reap` is null, so the sweep skips it.

Two deliberate non-choices: there is **no** max-lifetime cap — silence, not
age, is the kill criterion, so a legitimately long 1M-context generation that
keeps emitting bytes is never reaped. And there is **no** `req.signal`
disconnect handler — in Bun, `req.signal` and the response stream's `cancel()`
share one disconnect detector and fire together, so it would be redundant with
the (now-fixed) `cancel()` and still miss the half-open case the reaper covers.
