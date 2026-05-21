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
