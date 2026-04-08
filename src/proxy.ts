import type { KeyManager } from "./key-manager.ts";
import {
  type CapacityObservation,
  type ProxyConfig,
  type ProxyResult,
  type ProxyTokenEntry,
  unixMs,
} from "./types.ts";
import { log } from "./logger.ts";
import { emitWithKeys } from "./events.ts";
import type { SchemaTracker } from "./schema-tracker.ts";

const RATE_LIMIT_STATUS = 429 as const;
const ACTIVE_STREAM_SNAPSHOT_INTERVAL_MS = 2_000;
const RECENT_STREAM_ACTIVITY_WINDOW_MS = 1_000;

type ActiveStreamState = {
  readonly traceId: string;
  readonly label: string;
  readonly user: string | undefined;
  readonly path: string;
  readonly openedAt: number;
  firstChunkAt: number | null;
  lastChunkAt: number | null;
  chunkCount: number;
  eventCount: number;
  bytesReceived: number;
};

const activeStreams = new Map<string, ActiveStreamState>();
let lastActiveStreamSnapshotAt = 0;

/**
 * Headers we strip from the outgoing request — they get replaced with our key
 * or are hop-by-hop headers that shouldn't be forwarded.
 */
const STRIPPED_REQUEST_HEADERS: ReadonlySet<string> = new Set([
  "x-api-key",
  "authorization",
  "host",
  "connection",
  "keep-alive",
  "transfer-encoding",
]);

/**
 * Headers we strip from the upstream response before forwarding to the client.
 * content-encoding is stripped because Bun's fetch auto-decompresses responses,
 * so the body is already decompressed when we forward it.
 */
const STRIPPED_RESPONSE_HEADERS: ReadonlySet<string> = new Set([
  "connection",
  "keep-alive",
  "transfer-encoding",
  "content-encoding",
  "content-length",
]);

/**
 * Proxy an incoming request through the key rotation pool.
 *
 * Tries keys in order until one succeeds or all are exhausted.
 * On 429 it marks the key as rate-limited and immediately retries the next.
 * Non-429 errors are returned as-is — those aren't transient rate limits.
 */
export async function proxyRequest(
  req: Request,
  keyManager: KeyManager,
  config: ProxyConfig,
  schemaTracker: SchemaTracker,
  proxyUser?: ProxyTokenEntry | null,
): Promise<ProxyResult> {
  if (keyManager.totalCount() === 0) {
    return { kind: "no_keys" };
  }

  if (proxyUser) keyManager.recordTokenRequest(proxyUser);

  const url = new URL(req.url);
  const traceId = req.headers.get("x-request-id") ?? crypto.randomUUID();
  const requestContentLength = req.headers.get("content-length");

  const triedKeys = new Set<string>();
  let attempts = 0;

  while (attempts < config.maxRetriesPerRequest) {
    const entry = keyManager.getNextAvailableKey();

    if (entry === null) {
      if (proxyUser) keyManager.recordTokenError(proxyUser);
      return allExhaustedResult(keyManager);
    }

    if (triedKeys.has(entry.key)) {
      if (proxyUser) keyManager.recordTokenError(proxyUser);
      return allExhaustedResult(keyManager);
    }

    triedKeys.add(entry.key);
    attempts++;
    keyManager.recordRequest(entry);

    const upstreamUrl = `${config.upstream}${url.pathname}${url.search}`;
    const headers = buildUpstreamHeaders(req.headers, entry.key);
    const requestBodyState = snapshotRequestBodyState(req);

    const allHeaders: Record<string, string> = {};
    for (const [k, v] of headers.entries()) {
      allHeaders[k] = k === "authorization" ? v.slice(0, 30) + "..." : v;
    }
    log("info", "Proxying request", {
      label: entry.label,
      user: proxyUser?.label,
      method: req.method,
      path: url.pathname,
      attempt: attempts,
      traceId,
      upstreamUrl,
      requestContentLength,
      requestBodyState,
      headers: allHeaders,
    });
    if (attempts > 1 || requestBodyState.bodyUsed || requestBodyState.bodyLocked) {
      log("info", "Retry diagnostics before upstream fetch", {
        label: entry.label,
        user: proxyUser?.label,
        method: req.method,
        path: url.pathname,
        attempt: attempts,
        traceId,
        requestContentLength,
        requestBodyState,
      });
    }
    emitWithKeys({
      type: "request", ts: new Date().toISOString(), label: entry.label,
      user: proxyUser?.label,
      method: req.method, path: url.pathname, attempt: attempts,
    }, keyManager.listKeys());

    let upstream: Response;
    const fetchStartedAt = Date.now();
    try {
      upstream = await fetchUpstream(upstreamUrl, req.method, headers, req.body);
    } catch (err) {
      keyManager.recordError(entry);
      if (proxyUser) keyManager.recordTokenError(proxyUser);
      log("error", "Upstream fetch failed", {
        label: entry.label,
        user: proxyUser?.label,
        method: req.method,
        path: url.pathname,
        attempt: attempts,
        traceId,
        durationMs: Date.now() - fetchStartedAt,
        requestContentLength,
        requestBodyState: snapshotRequestBodyState(req),
        error: String(err),
      });
      emitWithKeys({
        type: "error", ts: new Date().toISOString(), label: entry.label,
        user: proxyUser?.label, error: String(err),
      }, keyManager.listKeys());
      return {
        kind: "error",
        status: 502,
        body: `Upstream connection failed: ${String(err)}`,
        usedKey: entry,
      };
    }

    const capacityObservation = extractCapacityObservation(
      upstream.headers,
      upstream.status,
      fetchStartedAt,
    );
    keyManager.recordCapacityObservation(entry, capacityObservation);

    log("info", "Upstream responded", {
      label: entry.label,
      user: proxyUser?.label,
      status: upstream.status,
      method: req.method,
      path: url.pathname,
      attempt: attempts,
      traceId,
      durationMs: Date.now() - fetchStartedAt,
    });
    emitWithKeys({
      type: "response", ts: new Date().toISOString(), label: entry.label,
      user: proxyUser?.label, status: upstream.status,
    }, keyManager.listKeys());

    const headerChanges = schemaTracker.recordHeaders(upstream.headers);
    if (headerChanges.length > 0) {
      emitWithKeys({
        type: "schema_change", ts: new Date().toISOString(),
        changes: headerChanges,
      }, keyManager.listKeys());
    }

    if (upstream.status === RATE_LIMIT_STATUS) {
      const retryAfter = parseRetryAfter(upstream.headers);
      keyManager.recordRateLimit(entry, retryAfter);
      await upstream.text();
      const retryBodyState = snapshotRequestBodyState(req);

      log("info", "Rate limited, trying next key", {
        label: entry.label,
        user: proxyUser?.label,
        method: req.method,
        path: url.pathname,
        attempt: attempts,
        traceId,
        durationMs: Date.now() - fetchStartedAt,
        retryAfter,
        availableKeys: keyManager.availableCount(),
        requestContentLength,
        requestBodyState: retryBodyState,
      });
      emitWithKeys({
        type: "rate_limit", ts: new Date().toISOString(), label: entry.label,
        user: proxyUser?.label, retryAfter, availableKeys: keyManager.availableCount(),
      }, keyManager.listKeys());
      continue;
    }

    if (upstream.status >= 400) {
      keyManager.recordError(entry);
      if (proxyUser) keyManager.recordTokenError(proxyUser);
      const body = await upstream.text();
      log("warn", "Upstream error", {
        label: entry.label,
        user: proxyUser?.label,
        status: upstream.status,
        body: body.slice(0, 500),
      });
      emitWithKeys({
        type: "error", ts: new Date().toISOString(), label: entry.label,
        user: proxyUser?.label, status: upstream.status,
      }, keyManager.listKeys());
      return { kind: "error", status: upstream.status, body, usedKey: entry };
    }

    const responseHeaders = new Headers();
    for (const [key, value] of upstream.headers.entries()) {
      if (!STRIPPED_RESPONSE_HEADERS.has(key.toLowerCase())) {
        responseHeaders.set(key, value);
      }
    }

    const isStreaming = (upstream.headers.get("content-type") ?? "").includes("text/event-stream");
    let body: ReadableStream<Uint8Array> | string | null;

    if (isStreaming && upstream.body !== null) {
      body = createTokenTrackingStream(
        upstream.body,
        entry,
        keyManager,
        proxyUser,
        schemaTracker,
        url.pathname,
        traceId,
      );
    } else if (upstream.body !== null) {
      const text = await upstream.text();
      extractTokensFromJson(text, entry, keyManager, proxyUser);
      const bodyChanges = schemaTracker.recordResponseJson(url.pathname, text);
      if (bodyChanges.length > 0) {
        emitWithKeys({
          type: "schema_change", ts: new Date().toISOString(),
          changes: bodyChanges,
        }, keyManager.listKeys());
      }
      body = text;
    } else {
      keyManager.recordSuccess(entry, 0, 0);
      if (proxyUser) keyManager.recordTokenSuccess(proxyUser, 0, 0);
      body = null;
    }

    const proxiedResponse = new Response(body, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: responseHeaders,
    });

    return { kind: "success", response: proxiedResponse, usedKey: entry };
  }

  if (proxyUser) keyManager.recordTokenError(proxyUser);
  return allExhaustedResult(keyManager);
}

// ── Helpers ───────────────────────────────────────────────────────

function allExhaustedResult(keyManager: KeyManager): ProxyResult {
  if (keyManager.totalCount() === 0) return { kind: "no_keys" };
  return { kind: "all_exhausted", earliestAvailableAt: keyManager.getEarliestAvailableAt() };
}

function snapshotRequestBodyState(req: Request): {
  readonly hasBody: boolean;
  readonly bodyUsed: boolean;
  readonly bodyLocked: boolean;
} {
  return {
    hasBody: req.body !== null,
    bodyUsed: req.bodyUsed,
    bodyLocked: req.body?.locked ?? false,
  };
}

function countRecentlyActiveStreams(now: number): number {
  let count = 0;
  for (const stream of activeStreams.values()) {
    if (stream.lastChunkAt !== null && now - stream.lastChunkAt <= RECENT_STREAM_ACTIVITY_WINDOW_MS) {
      count++;
    }
  }
  return count;
}

function maybeLogActiveStreamSnapshot(now: number): void {
  if (activeStreams.size <= 1) return;
  if (now - lastActiveStreamSnapshotAt < ACTIVE_STREAM_SNAPSHOT_INTERVAL_MS) return;
  lastActiveStreamSnapshotAt = now;

  const streams = [...activeStreams.values()]
    .sort((a, b) => a.openedAt - b.openedAt)
    .map((stream) => ({
      traceId: stream.traceId,
      label: stream.label,
      user: stream.user,
      path: stream.path,
      ageMs: now - stream.openedAt,
      firstChunkDelayMs: stream.firstChunkAt === null ? null : stream.firstChunkAt - stream.openedAt,
      sinceLastChunkMs: stream.lastChunkAt === null ? null : now - stream.lastChunkAt,
      waitingForFirstChunk: stream.firstChunkAt === null,
      chunkCount: stream.chunkCount,
      eventCount: stream.eventCount,
      bytesReceived: stream.bytesReceived,
    }));

  log("info", "Active stream snapshot", {
    activeStreams: activeStreams.size,
    recentlyActiveStreams: countRecentlyActiveStreams(now),
    recentWindowMs: RECENT_STREAM_ACTIVITY_WINDOW_MS,
    streams,
  });
}

function registerActiveStream(
  traceId: string,
  label: string,
  user: string | undefined,
  path: string,
): void {
  const now = Date.now();
  activeStreams.set(traceId, {
    traceId,
    label,
    user,
    path,
    openedAt: now,
    firstChunkAt: null,
    lastChunkAt: null,
    chunkCount: 0,
    eventCount: 0,
    bytesReceived: 0,
  });

  log("info", "Stream opened", {
    traceId,
    label,
    user,
    path,
    activeStreams: activeStreams.size,
  });
  maybeLogActiveStreamSnapshot(now);
}

function recordActiveStreamChunk(traceId: string, chunkBytes: number): void {
  const stream = activeStreams.get(traceId);
  if (!stream) return;

  const now = Date.now();
  stream.chunkCount++;
  stream.bytesReceived += chunkBytes;
  stream.lastChunkAt = now;

  if (stream.firstChunkAt === null) {
    stream.firstChunkAt = now;
    const recentlyActiveStreams = countRecentlyActiveStreams(now);
    log("info", "Stream first chunk", {
      traceId,
      label: stream.label,
      user: stream.user,
      path: stream.path,
      firstChunkDelayMs: now - stream.openedAt,
      activeStreams: activeStreams.size,
      otherRecentlyActiveStreams: Math.max(0, recentlyActiveStreams - 1),
    });
  }

  maybeLogActiveStreamSnapshot(now);
}

function recordActiveStreamEvent(traceId: string): void {
  const stream = activeStreams.get(traceId);
  if (!stream) return;
  stream.eventCount++;
}

function closeActiveStream(
  traceId: string,
  inputTokens: number,
  outputTokens: number,
  cacheReadTokens: number,
  cacheCreationTokens: number,
): void {
  const stream = activeStreams.get(traceId);
  if (!stream) return;

  const now = Date.now();
  activeStreams.delete(traceId);
  log("info", "Stream closed", {
    traceId,
    label: stream.label,
    user: stream.user,
    path: stream.path,
    durationMs: now - stream.openedAt,
    firstChunkDelayMs: stream.firstChunkAt === null ? null : stream.firstChunkAt - stream.openedAt,
    sinceLastChunkMs: stream.lastChunkAt === null ? null : now - stream.lastChunkAt,
    chunkCount: stream.chunkCount,
    eventCount: stream.eventCount,
    bytesReceived: stream.bytesReceived,
    input: inputTokens,
    output: outputTokens,
    cacheRead: cacheReadTokens,
    cacheCreation: cacheCreationTokens,
    activeStreamsRemaining: activeStreams.size,
  });
  maybeLogActiveStreamSnapshot(now);
}

function fetchUpstream(
  url: string,
  method: string,
  headers: Headers,
  body: ReadableStream<Uint8Array> | null,
): Promise<Response> {
  if (method === "GET" || method === "HEAD" || method === "OPTIONS") {
    return fetch(url, { method, headers });
  }
  return fetch(url, { method, headers, body, duplex: "half" } satisfies BunFetchRequestInit);
}

function buildUpstreamHeaders(incoming: Headers, apiKey: string): Headers {
  const headers = new Headers();
  for (const [key, value] of incoming.entries()) {
    if (!STRIPPED_REQUEST_HEADERS.has(key.toLowerCase())) {
      headers.set(key, value);
    }
  }

  if (apiKey.startsWith("sk-ant-oat")) {
    headers.set("authorization", `Bearer ${apiKey}`);
    const beta = headers.get("anthropic-beta") ?? "";
    if (!beta.includes("oauth-2025-04-20")) {
      headers.set("anthropic-beta", beta ? `${beta},oauth-2025-04-20` : "oauth-2025-04-20");
    }
  } else {
    headers.set("x-api-key", apiKey);
  }

  headers.set("anthropic-version", incoming.get("anthropic-version") ?? "2023-06-01");
  return headers;
}

type QuotaStatus = "allowed" | "allowed_warning" | "rejected";

type EarlyWarningThreshold = {
  readonly utilization: number;
  readonly timePct: number;
};

type EarlyWarningWindowConfig = {
  readonly windowName: string;
  readonly claimAbbrev: string;
  readonly windowDurationMs: number;
  readonly thresholds: readonly EarlyWarningThreshold[];
};

type CapacityWindowDraft = {
  readonly windowName: string;
  status?: QuotaStatus | null;
  utilization?: number | null;
  resetAt?: ReturnType<typeof unixMs> | null;
  surpassedThreshold?: number | null;
};

const EARLY_WARNING_WINDOWS: readonly EarlyWarningWindowConfig[] = [
  {
    windowName: "unified-5h",
    claimAbbrev: "5h",
    windowDurationMs: 5 * 60 * 60 * 1000,
    thresholds: [{ utilization: 0.9, timePct: 0.72 }],
  },
  {
    windowName: "unified-7d",
    claimAbbrev: "7d",
    windowDurationMs: 7 * 24 * 60 * 60 * 1000,
    thresholds: [
      { utilization: 0.75, timePct: 0.6 },
      { utilization: 0.5, timePct: 0.35 },
      { utilization: 0.25, timePct: 0.15 },
    ],
  },
] as const;

function parseRetryAfter(headers: Headers): number {
  const retryAfterMs = parseFinite(headers.get("retry-after-ms"));
  if (retryAfterMs !== null && retryAfterMs > 0) {
    return Math.max(1, Math.ceil(retryAfterMs / 1000));
  }

  const header = headers.get("retry-after");
  if (header !== null) {
    const secs = parseFinite(header);
    if (secs !== null && secs > 0) {
      return Math.max(1, Math.ceil(secs));
    }

    const dateMs = Date.parse(header);
    if (Number.isFinite(dateMs)) {
      return Math.max(1, Math.ceil((dateMs - Date.now()) / 1000));
    }
  }

  return 60;
}

function normalizeQuotaStatus(value: string | null): QuotaStatus | null {
  if (value === "allowed" || value === "allowed_warning" || value === "rejected") {
    return value;
  }
  return null;
}

function computeTimeProgress(resetAt: ReturnType<typeof unixMs>, windowDurationMs: number): number {
  const resetAtMs = Number(resetAt);
  const windowStart = resetAtMs - windowDurationMs;
  const elapsed = Date.now() - windowStart;
  return Math.max(0, Math.min(1, elapsed / windowDurationMs));
}

function shouldWarnForWindow(
  utilization: number | null | undefined,
  resetAt: ReturnType<typeof unixMs> | null | undefined,
  config: EarlyWarningWindowConfig,
): boolean {
  if (utilization === null || utilization === undefined || resetAt === null || resetAt === undefined) {
    return false;
  }

  const timeProgress = computeTimeProgress(resetAt, config.windowDurationMs);
  return config.thresholds.some(
    (threshold) => utilization >= threshold.utilization && timeProgress <= threshold.timePct,
  );
}

function setWindowField<T extends keyof CapacityWindowDraft>(
  draft: CapacityWindowDraft,
  field: T,
  value: CapacityWindowDraft[T] | null,
): void {
  if (value !== null) {
    draft[field] = value;
  }
}

function extractSupportedCapacityWindows(headers: Headers): CapacityWindowDraft[] {
  const windows = new Map<string, CapacityWindowDraft>();
  let earlyWarningObserved = false;

  const unifiedStatusHeader = normalizeQuotaStatus(headers.get("anthropic-ratelimit-unified-status"));
  const unifiedResetAt = parseEpochMs(headers.get("anthropic-ratelimit-unified-reset"));
  const unifiedUtilization = parseFinite(headers.get("anthropic-ratelimit-unified-utilization"));
  const unifiedSurpassedThreshold = parseFinite(headers.get("anthropic-ratelimit-unified-surpassed-threshold"));

  for (const config of EARLY_WARNING_WINDOWS) {
    const utilization = parseFinite(headers.get(`anthropic-ratelimit-unified-${config.claimAbbrev}-utilization`));
    const resetAt = parseEpochMs(headers.get(`anthropic-ratelimit-unified-${config.claimAbbrev}-reset`));
    const surpassedThreshold = parseFinite(headers.get(
      `anthropic-ratelimit-unified-${config.claimAbbrev}-surpassed-threshold`,
    ));

    if (utilization === null && resetAt === null && surpassedThreshold === null) continue;

    const status: QuotaStatus = surpassedThreshold !== null || shouldWarnForWindow(utilization, resetAt, config)
      ? "allowed_warning"
      : "allowed";
    if (status === "allowed_warning") earlyWarningObserved = true;

    const draft: CapacityWindowDraft = { windowName: config.windowName, status };
    setWindowField(draft, "utilization", utilization);
    setWindowField(draft, "resetAt", resetAt);
    setWindowField(draft, "surpassedThreshold", surpassedThreshold);
    windows.set(config.windowName, draft);
  }

  if (
    unifiedStatusHeader !== null
    || unifiedResetAt !== null
    || unifiedUtilization !== null
    || unifiedSurpassedThreshold !== null
  ) {
    let status = unifiedStatusHeader;
    if (status === "allowed" || status === "allowed_warning") {
      status = earlyWarningObserved || unifiedSurpassedThreshold !== null ? "allowed_warning" : "allowed";
    }

    const draft: CapacityWindowDraft = { windowName: "unified" };
    setWindowField(draft, "status", status);
    setWindowField(draft, "utilization", unifiedUtilization);
    setWindowField(draft, "resetAt", unifiedResetAt);
    setWindowField(draft, "surpassedThreshold", unifiedSurpassedThreshold);
    windows.set("unified", draft);
  }

  return [...windows.values()];
}

function extractCapacityObservation(
  headers: Headers,
  status: number,
  fetchStartedAt: number,
): CapacityObservation {
  const observedSignals = new Set<string>();
  let requestId: string | undefined;
  let organizationId: string | undefined;
  let representativeClaim: string | undefined;
  let retryAfterSecs: number | undefined;
  let shouldRetry: boolean | undefined;
  let fallbackAvailable: boolean | undefined;
  let fallbackPercentage: number | undefined;
  let overageStatus: string | undefined;
  let overageDisabledReason: string | undefined;
  let latencyMs: number | undefined;

  const requestIdHeader = headers.get("request-id") ?? headers.get("x-request-id");
  if (requestIdHeader !== null) {
    requestId = requestIdHeader;
    observedSignals.add("request_id");
  }

  const organizationHeader = headers.get("anthropic-organization-id");
  if (organizationHeader !== null) {
    organizationId = organizationHeader;
    observedSignals.add("organization");
  }

  const representativeClaimHeader = headers.get("anthropic-ratelimit-unified-representative-claim");
  if (representativeClaimHeader !== null) {
    representativeClaim = representativeClaimHeader;
    observedSignals.add("representative_claim");
  }

  if (headers.has("retry-after") || headers.has("retry-after-ms")) {
    observedSignals.add("retry_after");
  }
  if (headers.has("retry-after") || headers.has("retry-after-ms") || status === RATE_LIMIT_STATUS) {
    retryAfterSecs = parseRetryAfter(headers);
  }

  const shouldRetryHeader = headers.get("x-should-retry");
  if (shouldRetryHeader === "true") {
    shouldRetry = true;
    observedSignals.add("should_retry");
  } else if (shouldRetryHeader === "false") {
    shouldRetry = false;
    observedSignals.add("should_retry");
  }

  const fallbackHeader = headers.get("anthropic-ratelimit-unified-fallback");
  if (fallbackHeader === "available") {
    fallbackAvailable = true;
    observedSignals.add("fallback");
  } else if (fallbackHeader === "unavailable") {
    fallbackAvailable = false;
    observedSignals.add("fallback");
  }

  const fallbackPercentageHeader = parseFinite(headers.get("anthropic-ratelimit-unified-fallback-percentage"));
  if (fallbackPercentageHeader !== null) {
    fallbackPercentage = fallbackPercentageHeader;
    observedSignals.add("fallback");
  }

  const overageStatusHeader = headers.get("anthropic-ratelimit-unified-overage-status");
  if (overageStatusHeader !== null) {
    overageStatus = overageStatusHeader;
    observedSignals.add("overage");
  }

  const overageDisabledReasonHeader = headers.get("anthropic-ratelimit-unified-overage-disabled-reason");
  if (overageDisabledReasonHeader !== null) {
    overageDisabledReason = overageDisabledReasonHeader;
    observedSignals.add("overage");
  }

  const headerLatency = parseLatencyMs(headers);
  if (headerLatency !== null) latencyMs = headerLatency;
  else latencyMs = Math.max(0, Date.now() - fetchStartedAt);

  const windows = extractSupportedCapacityWindows(headers);
  if (windows.length > 0) observedSignals.add("windows");

  return {
    seenAt: unixMs(Date.now()),
    httpStatus: status,
    ...(requestId !== undefined ? { requestId } : {}),
    ...(organizationId !== undefined ? { organizationId } : {}),
    ...(representativeClaim !== undefined ? { representativeClaim } : {}),
    ...(retryAfterSecs !== undefined ? { retryAfterSecs } : {}),
    ...(shouldRetry !== undefined ? { shouldRetry } : {}),
    ...(fallbackAvailable !== undefined ? { fallbackAvailable } : {}),
    ...(fallbackPercentage !== undefined ? { fallbackPercentage } : {}),
    ...(overageStatus !== undefined ? { overageStatus } : {}),
    ...(overageDisabledReason !== undefined ? { overageDisabledReason } : {}),
    ...(latencyMs !== undefined ? { latencyMs } : {}),
    ...(observedSignals.size > 0 ? { observedSignals: [...observedSignals].sort() } : {}),
    ...(windows.length > 0
      ? {
          windows: windows.map((window) => ({
            ...window,
            lastSeenAt: unixMs(Date.now()),
          })),
        }
      : {}),
  };
}

function parseFinite(value: string | null): number | null {
  if (value === null) return null;
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseEpochMs(value: string | null): ReturnType<typeof unixMs> | null {
  const parsed = parseFinite(value);
  if (parsed === null || parsed <= 0) return null;
  if (parsed >= 1e11) return unixMs(Math.round(parsed));
  return unixMs(Math.round(parsed * 1000));
}

function parseLatencyMs(headers: Headers): number | null {
  const envoy = parseFinite(headers.get("x-envoy-upstream-service-time"));
  if (envoy !== null) return Math.round(envoy);

  const serverTiming = headers.get("server-timing");
  if (serverTiming === null) return null;

  const explicitOrigin = /x-originResponse;dur=([\d.]+)/i.exec(serverTiming);
  if (explicitOrigin) {
    const parsed = parseFinite(explicitOrigin[1] ?? null);
    if (parsed !== null) return Math.round(parsed);
  }

  const generic = /dur=([\d.]+)/i.exec(serverTiming);
  if (!generic) return null;
  const parsed = parseFinite(generic[1] ?? null);
  return parsed === null ? null : Math.round(parsed);
}

// ── Token tracking ────────────────────────────────────────────────

interface AnthropicUsage {
  readonly input_tokens?: number;
  readonly output_tokens?: number;
  readonly cache_creation_input_tokens?: number;
  readonly cache_read_input_tokens?: number;
}


interface AnthropicResponse {
  readonly usage?: AnthropicUsage;
}

interface AnthropicStreamDelta {
  readonly type?: string;
  readonly usage?: AnthropicUsage;
  readonly message?: { readonly usage?: AnthropicUsage };
}

function extractTokensFromJson(
  text: string,
  entry: import("./types.ts").ApiKeyEntry,
  keyManager: KeyManager,
  proxyUser?: ProxyTokenEntry | null,
): void {
  try {
    const parsed = JSON.parse(text) as AnthropicResponse;
    const input = parsed.usage?.input_tokens ?? 0;
    const output = parsed.usage?.output_tokens ?? 0;
    const cacheRead = parsed.usage?.cache_read_input_tokens ?? 0;
    const cacheCreation = parsed.usage?.cache_creation_input_tokens ?? 0;
    keyManager.recordSuccess(entry, input, output, cacheRead, cacheCreation);
    if (proxyUser) keyManager.recordTokenSuccess(proxyUser, input, output, cacheRead, cacheCreation);
    if (input > 0 || output > 0 || cacheRead > 0) {
      log("info", "Token usage", { label: entry.label, user: proxyUser?.label, input, output, cacheRead, cacheCreation });
      emitWithKeys({
        type: "tokens", ts: new Date().toISOString(), label: entry.label,
        user: proxyUser?.label, input, output,
      }, keyManager.listKeys());
    }
  } catch {
    keyManager.recordSuccess(entry, 0, 0);
    if (proxyUser) keyManager.recordTokenSuccess(proxyUser, 0, 0);
  }
}

/**
 * Wraps a streaming response body to intercept SSE events and extract token
 * usage from message_start and message_delta events. Data passes through
 * unmodified — we only observe.
 */
function createTokenTrackingStream(
  source: ReadableStream<Uint8Array>,
  entry: import("./types.ts").ApiKeyEntry,
  keyManager: KeyManager,
  proxyUser: ProxyTokenEntry | null | undefined,
  schemaTracker: SchemaTracker,
  endpoint: string,
  traceId: string,
): ReadableStream<Uint8Array> {
  const decoder = new TextDecoder();
  let buffer = "";
  let inputTokens = 0;
  let outputTokens = 0;
  let cacheReadTokens = 0;
  let cacheCreationTokens = 0;
  registerActiveStream(traceId, entry.label, proxyUser?.label, endpoint);

  return source.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      controller.enqueue(chunk);
      recordActiveStreamChunk(traceId, chunk.byteLength);

      buffer += decoder.decode(chunk, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const json = line.slice(6).trim();
        if (json === "[DONE]") continue;

        try {
          const event = JSON.parse(json) as AnthropicStreamDelta;
          recordActiveStreamEvent(traceId);
          if (event.type === "message_start" && event.message?.usage) {
            inputTokens += event.message.usage.input_tokens ?? 0;
            cacheReadTokens += event.message.usage.cache_read_input_tokens ?? 0;
            cacheCreationTokens += event.message.usage.cache_creation_input_tokens ?? 0;
          }
          if (event.type === "message_delta" && event.usage) {
            outputTokens += event.usage.output_tokens ?? 0;
          }

          const eventChanges = schemaTracker.recordStreamEvent(endpoint, event.type ?? "unknown", event);
          if (eventChanges.length > 0) {
            emitWithKeys({
              type: "schema_change", ts: new Date().toISOString(),
              changes: eventChanges,
            }, keyManager.listKeys());
          }
        } catch {
          // Not valid JSON — skip
        }
      }
    },

    flush() {
      closeActiveStream(traceId, inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens);
      keyManager.recordSuccess(entry, inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens);
      if (proxyUser) keyManager.recordTokenSuccess(proxyUser, inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens);
      if (inputTokens > 0 || outputTokens > 0 || cacheReadTokens > 0) {
        log("info", "Token usage (stream)", {
          label: entry.label,
          user: proxyUser?.label,
          input: inputTokens,
          output: outputTokens,
          cacheRead: cacheReadTokens,
          cacheCreation: cacheCreationTokens,
        });
        emitWithKeys({
          type: "tokens", ts: new Date().toISOString(), label: entry.label,
          user: proxyUser?.label, input: inputTokens, output: outputTokens,
          cacheRead: cacheReadTokens, cacheCreation: cacheCreationTokens,
        }, keyManager.listKeys());
      }
    },
  }));
}
