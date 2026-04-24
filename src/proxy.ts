import type { KeyManager } from "./key-manager.ts";
import {
  type ApiKeyEntry,
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
const SLOW_STREAM_SILENCE_LOG_MS = 5_000;
const SLOW_FIRST_CHUNK_LOG_MS = 5_000;
const STREAM_START_HISTORY_WINDOW_MS = 15 * 60 * 1_000;
const MAX_ACTIVE_REQUEST_PEERS_LOGGED = 5;

type ActiveRequestPhase =
  | "fetching_upstream"
  | "waiting_for_first_chunk"
  | "streaming";

type ActiveRequestState = {
  readonly traceId: string;
  readonly label: string;
  readonly user: string | undefined;
  readonly path: string;
  readonly sessionId: string | null;
  readonly conversationKey: string | null;
  attempt: number;
  readonly startedAt: number;
  phase: ActiveRequestPhase;
  upstreamRespondedAt: number | null;
  firstChunkAt: number | null;
};

type ActiveRequestPhaseCounts = {
  activeRequests: number;
  fetchingUpstreamRequests: number;
  waitingForFirstChunkRequests: number;
  streamingRequests: number;
};

type ActiveRequestPeerSnapshot = {
  readonly traceId: string;
  readonly label: string;
  readonly sessionId: string | null;
  readonly conversationKey: string | null;
  readonly attempt: number;
  readonly phase: ActiveRequestPhase;
  readonly ageMs: number;
  readonly sinceUpstreamResponseMs: number | null;
  readonly sinceFirstChunkMs: number | null;
};

type ActiveRequestContext = ActiveRequestPhaseCounts & {
  sameLabelActiveRequests: number;
  sameLabelFetchingUpstreamRequests: number;
  sameLabelWaitingForFirstChunkRequests: number;
  sameLabelStreamingRequests: number;
  sameSessionActiveRequests: number;
  sameSessionFetchingUpstreamRequests: number;
  sameSessionWaitingForFirstChunkRequests: number;
  sameSessionStreamingRequests: number;
  sameConversationActiveRequests: number;
  sameConversationFetchingUpstreamRequests: number;
  sameConversationWaitingForFirstChunkRequests: number;
  sameConversationStreamingRequests: number;
  sameLabelPeers: ActiveRequestPeerSnapshot[];
};

type StreamStartHistoryEntry = {
  readonly traceId: string;
  readonly label: string;
  readonly sessionId: string | null;
  readonly conversationKey: string | null;
  readonly attempt: number;
  readonly at: number;
  readonly outcome: "first_chunk" | "first_chunk_timeout";
};

type StreamStartHistorySummary = {
  recentStreamStartWindowMs: number;
  totalFirstChunks15m: number;
  totalFirstChunkTimeouts15m: number;
  sameLabelFirstChunks15m: number;
  sameLabelFirstChunkTimeouts15m: number;
  sameSessionFirstChunks15m: number;
  sameSessionFirstChunkTimeouts15m: number;
  sameConversationFirstChunks15m: number;
  sameConversationFirstChunkTimeouts15m: number;
};

type ActiveStreamState = {
  readonly traceId: string;
  readonly label: string;
  readonly user: string | undefined;
  readonly path: string;
  readonly openedAt: number;
  firstChunkAt: number | null;
  lastChunkAt: number | null;
  maxSilenceGapMs: number;
  chunkCount: number;
  eventCount: number;
  bytesReceived: number;
};

type BufferedRequestBody = Uint8Array | null;

function getStreamReader(source: ReadableStream<Uint8Array>) {
  return source.getReader();
}

type UpstreamReader = ReturnType<typeof getStreamReader>;

type RoutingSelection = {
  readonly entry: ApiKeyEntry | null;
  readonly routingDecision:
    | "first_chunk_retry_same_key"
    | "global_sticky_fallback"
    | "conversation_affinity_hit"
    | "conversation_new_assignment"
    | "conversation_affinity_remapped"
    | "conversation_affinity_cooldown_passthrough";
  readonly affinityHit: boolean;
  readonly remapped: boolean;
  readonly priorityTier: number | null;
  readonly candidateCount: number;
  readonly conversationCountForSelectedKey: number | null;
  readonly pool: "primary" | "secondary" | "tertiary" | null;
  readonly worstHeadroom: number | null;
  readonly cooldownRemainingMs: number | null;
};

type StreamStartFailureReason =
  | "first_chunk_timeout"
  | "stream_ended_before_first_chunk"
  | "stream_read_failed_before_first_chunk";

type StreamStartFailure = {
  readonly reason: StreamStartFailureReason;
  readonly attempt: number;
  readonly usedKey: import("./types.ts").ApiKeyEntry;
  readonly error?: string;
};

type FirstStreamChunkResult =
  | {
      readonly kind: "chunk";
      readonly firstChunk: Uint8Array;
      readonly reader: UpstreamReader;
    }
  | {
      readonly kind: "retry";
      readonly reason: StreamStartFailureReason;
      readonly error?: string;
    };

type StreamObserver = {
  readonly observeChunk: (chunk: Uint8Array) => void;
  readonly finish: () => void;
  readonly abandon: (reason: string) => void;
};

type NextChunkResult =
  | {
      readonly kind: "chunk";
      readonly chunk: Uint8Array;
    }
  | {
      readonly kind: "done";
    }
  | {
      readonly kind: "error";
      readonly error: unknown;
    }
  | {
      readonly kind: "timeout";
    };

const activeStreams = new Map<string, ActiveStreamState>();
const activeRequests = new Map<string, ActiveRequestState>();
const recentStreamStartHistory: StreamStartHistoryEntry[] = [];
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
  const sessionId = normalizeConversationSessionId(req.headers.get("x-claude-code-session-id"));
  const conversationKey = buildConversationKey(proxyUser, sessionId);
  const requestContentLength = req.headers.get("content-length");
  let requestBody: BufferedRequestBody;
  try {
    requestBody = await bufferRequestBody(req);
  } catch (err) {
    const entry = keyManager.getNextAvailableKey();
    if (entry === null) {
      if (proxyUser) keyManager.recordTokenError(proxyUser);
      return allExhaustedResult(keyManager);
    }
    if (proxyUser) keyManager.recordTokenError(proxyUser);
    return {
      kind: "error",
      status: 400,
      body: JSON.stringify({
        error: {
          type: "proxy_error",
          message: `Failed to read request body: ${String(err)}`,
        },
      }),
      usedKey: entry,
    };
  }
  const requestBodyState = snapshotRequestBodyState(req, requestBody);

  let attempts = 0;
  let firstChunkRetries = 0;
  let sawRateLimit = false;
  let lastStreamStartFailure: StreamStartFailure | null = null;
  let preferredRetryKey: ApiKeyEntry | null = null;

  while (attempts < config.maxRetriesPerRequest) {
    const selection: RoutingSelection = preferredRetryKey !== null && isKeyAvailableNow(preferredRetryKey)
      ? {
          entry: preferredRetryKey,
          routingDecision: "first_chunk_retry_same_key",
          affinityHit: conversationKey !== null,
          remapped: false,
          priorityTier: preferredRetryKey.priority,
          candidateCount: 1,
          conversationCountForSelectedKey: null,
          pool: null,
          worstHeadroom: null,
          cooldownRemainingMs: null,
        }
      : keyManager.getKeyForConversation(conversationKey, sessionId);
    const entry: ApiKeyEntry | null = selection.entry;
    preferredRetryKey = null;

    if (entry === null) {
      // Pinned key briefly cooling — hand the client a 429 with retry-after
      // matching the remaining cooldown, so it retries on the same key once
      // it's back. Preserves prompt cache on huge affinity-pinned contexts.
      if (selection.routingDecision === "conversation_affinity_cooldown_passthrough") {
        return affinityCooldownPassthroughResult(selection.cooldownRemainingMs ?? 0);
      }
      if (lastStreamStartFailure !== null && !sawRateLimit) {
        if (proxyUser) keyManager.recordTokenError(proxyUser);
        return firstChunkFailureResult(lastStreamStartFailure, config.firstChunkTimeoutMs);
      }
      if (proxyUser) keyManager.recordTokenError(proxyUser);
      return allExhaustedResult(keyManager);
    }
    attempts++;
    keyManager.recordRequest(entry);

    const upstreamUrl = `${config.upstream}${url.pathname}${url.search}`;
    const headers = buildUpstreamHeaders(req.headers, entry.key);

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
      sessionId,
      conversationKey,
      routingDecision: selection.routingDecision,
      affinityHit: selection.affinityHit,
      remappedConversation: selection.remapped,
      priorityTier: selection.priorityTier,
      candidateCount: selection.candidateCount,
      conversationCountForSelectedKey: selection.conversationCountForSelectedKey,
      pool: selection.pool,
      worstHeadroom: selection.worstHeadroom,
      upstreamUrl,
      requestContentLength,
      requestBodyState,
      headers: allHeaders,
    });
    registerActiveRequest(
      traceId,
      entry.label,
      proxyUser?.label,
      url.pathname,
      sessionId,
      conversationKey,
      attempts,
    );
    if (attempts > 1) {
      log("info", "Retry diagnostics before upstream fetch", {
        label: entry.label,
        user: proxyUser?.label,
        method: req.method,
        path: url.pathname,
        attempt: attempts,
        traceId,
        sessionId,
        conversationKey,
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
    const abortController = new AbortController();
    try {
      upstream = await fetchUpstream(upstreamUrl, req.method, headers, requestBody, abortController.signal);
    } catch (err) {
      clearActiveRequest(traceId);
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
        requestBodyState,
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
      sawRateLimit = true;
      const retryAfter = parseRetryAfter(upstream.headers);
      keyManager.recordRateLimit(entry, retryAfter);
      await upstream.text();
      clearActiveRequest(traceId);

      log("info", "Rate limited, trying next key", {
        label: entry.label,
        user: proxyUser?.label,
        method: req.method,
        path: url.pathname,
        attempt: attempts,
        traceId,
        sessionId,
        durationMs: Date.now() - fetchStartedAt,
        retryAfter,
        availableKeys: keyManager.availableCount(),
        requestContentLength,
        requestBodyState,
      });
      emitWithKeys({
        type: "rate_limit", ts: new Date().toISOString(), label: entry.label,
        user: proxyUser?.label, retryAfter, availableKeys: keyManager.availableCount(),
      }, keyManager.listKeys());
      lastStreamStartFailure = null;
      preferredRetryKey = null;
      continue;
    }

    if (upstream.status >= 400) {
      clearActiveRequest(traceId);
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
      const observer = createTokenTrackingObserver(
        entry,
        keyManager,
        proxyUser,
        schemaTracker,
        url.pathname,
        traceId,
      );
      markActiveRequestWaitingForFirstChunk(traceId);
      const waitContext = buildActiveRequestContext(traceId, Date.now());
      if (
        waitContext !== null
        && (
          waitContext.sameLabelActiveRequests > 0
          || waitContext.sameSessionActiveRequests > 0
          || waitContext.sameConversationActiveRequests > 0
        )
      ) {
        log("info", "Waiting for first stream chunk with peer activity", {
          label: entry.label,
          user: proxyUser?.label,
          method: req.method,
          path: url.pathname,
          attempt: attempts,
          traceId,
          sessionId,
          conversationKey,
          activeRequestContext: waitContext,
        });
      }
      const firstChunk = await waitForFirstStreamChunk(
        upstream.body,
        config.firstChunkTimeoutMs,
        abortController,
      );
      if (firstChunk.kind === "retry") {
        const timeoutObservedAt = Date.now();
        const timeoutContext = buildActiveRequestContext(traceId, timeoutObservedAt);
        const currentRequest = activeRequests.get(traceId);
        const currentWaitingForFirstChunkMs = currentRequest?.upstreamRespondedAt == null
          ? null
          : timeoutObservedAt - currentRequest.upstreamRespondedAt;
        const recentHistory = summarizeRecentStreamStartHistory(
          entry.label,
          sessionId,
          conversationKey,
          timeoutObservedAt,
        );
        observer.abandon(firstChunk.reason);
        keyManager.recordError(entry);
        firstChunkRetries++;
        preferredRetryKey = entry;
        lastStreamStartFailure = {
          reason: firstChunk.reason,
          attempt: attempts,
          usedKey: entry,
          ...(firstChunk.error !== undefined ? { error: firstChunk.error } : {}),
        };

        log("warn", "No first stream chunk yet, retrying request", {
          label: entry.label,
          user: proxyUser?.label,
          method: req.method,
          path: url.pathname,
          attempt: attempts,
          traceId,
          sessionId,
          conversationKey,
          durationMs: Date.now() - fetchStartedAt,
          firstChunkTimeoutMs: config.firstChunkTimeoutMs,
          maxFirstChunkRetries: config.maxFirstChunkRetries,
          firstChunkRetries,
          retryStrategy: "sticky_same_key_unless_unavailable",
          reason: firstChunk.reason,
          currentWaitingForFirstChunkMs,
          ...(timeoutContext !== null ? { activeRequestContext: timeoutContext } : {}),
          recentStreamStartHistory: recentHistory,
          ...(firstChunk.error !== undefined ? { error: firstChunk.error } : {}),
        });
        emitWithKeys({
          type: "error",
          ts: new Date().toISOString(),
          label: entry.label,
          user: proxyUser?.label,
          path: url.pathname,
          attempt: attempts,
          traceId,
          error: firstChunk.reason,
          firstChunkTimeoutMs: config.firstChunkTimeoutMs,
          firstChunkRetries,
        }, keyManager.listKeys());

        if (firstChunkRetries > config.maxFirstChunkRetries) {
          if (proxyUser) keyManager.recordTokenError(proxyUser);
          return firstChunkFailureResult(lastStreamStartFailure, config.firstChunkTimeoutMs);
        }
        continue;
      }

      lastStreamStartFailure = null;
      observer.observeChunk(firstChunk.firstChunk);
      body = createTrackedStreamFromReader(
        firstChunk.reader,
        firstChunk.firstChunk,
        observer,
        abortController,
        config.streamIdleTimeoutMs,
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
      clearActiveRequest(traceId);
      body = text;
    } else {
      clearActiveRequest(traceId);
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

  if (lastStreamStartFailure !== null && !sawRateLimit) {
    if (proxyUser) keyManager.recordTokenError(proxyUser);
    return firstChunkFailureResult(lastStreamStartFailure, config.firstChunkTimeoutMs);
  }

  if (proxyUser) keyManager.recordTokenError(proxyUser);
  return allExhaustedResult(keyManager);
}

// ── Helpers ───────────────────────────────────────────────────────

export function resetProxyDebugStateForTests(): void {
  activeStreams.clear();
  activeRequests.clear();
  recentStreamStartHistory.length = 0;
  lastActiveStreamSnapshotAt = 0;
}

function allExhaustedResult(keyManager: KeyManager): ProxyResult {
  if (keyManager.totalCount() === 0) return { kind: "no_keys" };
  return { kind: "all_exhausted", earliestAvailableAt: keyManager.getEarliestAvailableAt() };
}

function affinityCooldownPassthroughResult(cooldownRemainingMs: number): ProxyResult {
  return {
    kind: "affinity_cooldown_passthrough",
    retryAfterSecs: Math.max(1, Math.ceil(cooldownRemainingMs / 1000)),
  };
}

function isKeyAvailableNow(entry: ApiKeyEntry): boolean {
  const currentTime = unixMs(Date.now());
  const currentDay = new Date().getDay();
  return entry.availableAt <= currentTime && entry.allowedDays.includes(currentDay);
}

function normalizeConversationSessionId(raw: string | null): string | null {
  const normalized = raw?.trim() ?? "";
  return normalized === "" ? null : normalized;
}

function buildConversationKey(
  proxyUser: ProxyTokenEntry | null | undefined,
  sessionId: string | null,
): string | null {
  if (sessionId === null) return null;
  const actor = proxyUser?.label ?? "anon";
  return `${actor}:${sessionId}`;
}

function firstChunkFailureResult(
  failure: StreamStartFailure,
  firstChunkTimeoutMs: number,
): ProxyResult {
  return {
    kind: "error",
    status: 504,
    body: JSON.stringify({
      error: {
        type: "proxy_error",
        message: describeStreamStartFailure(failure, firstChunkTimeoutMs),
      },
    }),
    usedKey: failure.usedKey,
  };
}

function describeStreamStartFailure(
  failure: StreamStartFailure,
  firstChunkTimeoutMs: number,
): string {
  const attemptCount = failure.attempt;
  switch (failure.reason) {
    case "first_chunk_timeout":
      return `Upstream stream produced no first chunk within ${firstChunkTimeoutMs}ms after ${attemptCount} attempt(s).`;
    case "stream_ended_before_first_chunk":
      return `Upstream stream ended before the first chunk after ${attemptCount} attempt(s).`;
    case "stream_read_failed_before_first_chunk":
      return failure.error
        ? `Upstream stream failed before the first chunk after ${attemptCount} attempt(s): ${failure.error}`
        : `Upstream stream failed before the first chunk after ${attemptCount} attempt(s).`;
  }
}

async function bufferRequestBody(req: Request): Promise<BufferedRequestBody> {
  if (req.body === null) return null;
  const body = await req.arrayBuffer();
  return new Uint8Array(body);
}

function normalizeChunk(chunk: Uint8Array | ArrayBufferView): Uint8Array {
  return chunk instanceof Uint8Array
    ? chunk
    : new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
}

function snapshotRequestBodyState(req: Request, bufferedBody?: BufferedRequestBody): {
  readonly hasBody: boolean;
  readonly bodyUsed: boolean;
  readonly bodyLocked: boolean;
  readonly bufferedBytes: number;
} {
  return {
    hasBody: req.body !== null,
    bodyUsed: req.bodyUsed,
    bodyLocked: req.body?.locked ?? false,
    bufferedBytes: bufferedBody?.byteLength ?? 0,
  };
}

function registerActiveRequest(
  traceId: string,
  label: string,
  user: string | undefined,
  path: string,
  sessionId: string | null,
  conversationKey: string | null,
  attempt: number,
): void {
  activeRequests.set(traceId, {
    traceId,
    label,
    user,
    path,
    sessionId,
    conversationKey,
    attempt,
    startedAt: Date.now(),
    phase: "fetching_upstream",
    upstreamRespondedAt: null,
    firstChunkAt: null,
  });
}

function markActiveRequestWaitingForFirstChunk(traceId: string): void {
  const request = activeRequests.get(traceId);
  if (!request) return;
  request.phase = "waiting_for_first_chunk";
  request.upstreamRespondedAt = Date.now();
  request.firstChunkAt = null;
}

function markActiveRequestStreaming(traceId: string, now: number): void {
  const request = activeRequests.get(traceId);
  if (!request) return;
  request.phase = "streaming";
  if (request.upstreamRespondedAt === null) request.upstreamRespondedAt = now;
  if (request.firstChunkAt === null) request.firstChunkAt = now;
}

function clearActiveRequest(traceId: string): void {
  activeRequests.delete(traceId);
}

function buildActiveRequestContext(traceId: string, now: number): ActiveRequestContext | null {
  const current = activeRequests.get(traceId);
  if (!current) return null;

  const peers = [...activeRequests.values()].filter((request) => request.traceId !== traceId);
  const sameLabelPeers = peers.filter((request) => request.label === current.label);
  const sameSessionPeers = current.sessionId === null
    ? []
    : peers.filter((request) => request.sessionId === current.sessionId);
  const sameConversationPeers = current.conversationKey === null
    ? []
    : peers.filter((request) => request.conversationKey === current.conversationKey);

  const overall = countActiveRequestPhases(peers);
  const sameLabel = countActiveRequestPhases(sameLabelPeers);
  const sameSession = countActiveRequestPhases(sameSessionPeers);
  const sameConversation = countActiveRequestPhases(sameConversationPeers);

  return {
    ...overall,
    sameLabelActiveRequests: sameLabel.activeRequests,
    sameLabelFetchingUpstreamRequests: sameLabel.fetchingUpstreamRequests,
    sameLabelWaitingForFirstChunkRequests: sameLabel.waitingForFirstChunkRequests,
    sameLabelStreamingRequests: sameLabel.streamingRequests,
    sameSessionActiveRequests: sameSession.activeRequests,
    sameSessionFetchingUpstreamRequests: sameSession.fetchingUpstreamRequests,
    sameSessionWaitingForFirstChunkRequests: sameSession.waitingForFirstChunkRequests,
    sameSessionStreamingRequests: sameSession.streamingRequests,
    sameConversationActiveRequests: sameConversation.activeRequests,
    sameConversationFetchingUpstreamRequests: sameConversation.fetchingUpstreamRequests,
    sameConversationWaitingForFirstChunkRequests: sameConversation.waitingForFirstChunkRequests,
    sameConversationStreamingRequests: sameConversation.streamingRequests,
    sameLabelPeers: sameLabelPeers
      .sort((a, b) => a.startedAt - b.startedAt)
      .slice(0, MAX_ACTIVE_REQUEST_PEERS_LOGGED)
      .map((request) => snapshotActiveRequestPeer(request, now)),
  };
}

function pruneRecentStreamStartHistory(now: number): void {
  while (
    recentStreamStartHistory.length > 0
    && now - recentStreamStartHistory[0]!.at > STREAM_START_HISTORY_WINDOW_MS
  ) {
    recentStreamStartHistory.shift();
  }
}

function recordStreamStartHistory(
  traceId: string,
  label: string,
  sessionId: string | null,
  conversationKey: string | null,
  attempt: number,
  outcome: StreamStartHistoryEntry["outcome"],
  at: number,
): void {
  pruneRecentStreamStartHistory(at);
  recentStreamStartHistory.push({
    traceId,
    label,
    sessionId,
    conversationKey,
    attempt,
    at,
    outcome,
  });
}

function summarizeRecentStreamStartHistory(
  label: string,
  sessionId: string | null,
  conversationKey: string | null,
  now: number,
): StreamStartHistorySummary {
  pruneRecentStreamStartHistory(now);

  let totalFirstChunks15m = 0;
  let totalFirstChunkTimeouts15m = 0;
  let sameLabelFirstChunks15m = 0;
  let sameLabelFirstChunkTimeouts15m = 0;
  let sameSessionFirstChunks15m = 0;
  let sameSessionFirstChunkTimeouts15m = 0;
  let sameConversationFirstChunks15m = 0;
  let sameConversationFirstChunkTimeouts15m = 0;

  for (const entry of recentStreamStartHistory) {
    const isFirstChunk = entry.outcome === "first_chunk";
    if (isFirstChunk) totalFirstChunks15m++;
    else totalFirstChunkTimeouts15m++;

    if (entry.label === label) {
      if (isFirstChunk) sameLabelFirstChunks15m++;
      else sameLabelFirstChunkTimeouts15m++;
    }
    if (sessionId !== null && entry.sessionId === sessionId) {
      if (isFirstChunk) sameSessionFirstChunks15m++;
      else sameSessionFirstChunkTimeouts15m++;
    }
    if (conversationKey !== null && entry.conversationKey === conversationKey) {
      if (isFirstChunk) sameConversationFirstChunks15m++;
      else sameConversationFirstChunkTimeouts15m++;
    }
  }

  return {
    recentStreamStartWindowMs: STREAM_START_HISTORY_WINDOW_MS,
    totalFirstChunks15m,
    totalFirstChunkTimeouts15m,
    sameLabelFirstChunks15m,
    sameLabelFirstChunkTimeouts15m,
    sameSessionFirstChunks15m,
    sameSessionFirstChunkTimeouts15m,
    sameConversationFirstChunks15m,
    sameConversationFirstChunkTimeouts15m,
  };
}

function countActiveRequestPhases(requests: readonly ActiveRequestState[]): ActiveRequestPhaseCounts {
  const counts: ActiveRequestPhaseCounts = {
    activeRequests: requests.length,
    fetchingUpstreamRequests: 0,
    waitingForFirstChunkRequests: 0,
    streamingRequests: 0,
  };

  for (const request of requests) {
    if (request.phase === "fetching_upstream") counts.fetchingUpstreamRequests++;
    else if (request.phase === "waiting_for_first_chunk") counts.waitingForFirstChunkRequests++;
    else if (request.phase === "streaming") counts.streamingRequests++;
  }

  return counts;
}

function snapshotActiveRequestPeer(request: ActiveRequestState, now: number): ActiveRequestPeerSnapshot {
  return {
    traceId: request.traceId,
    label: request.label,
    sessionId: request.sessionId,
    conversationKey: request.conversationKey,
    attempt: request.attempt,
    phase: request.phase,
    ageMs: now - request.startedAt,
    sinceUpstreamResponseMs: request.upstreamRespondedAt === null ? null : now - request.upstreamRespondedAt,
    sinceFirstChunkMs: request.firstChunkAt === null ? null : now - request.firstChunkAt,
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
      maxSilenceGapMs: getStreamMaxSilenceGapMs(stream, now),
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
    maxSilenceGapMs: 0,
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
  const request = activeRequests.get(traceId);

  const now = Date.now();
  const silenceGapMs = stream.lastChunkAt === null ? null : now - stream.lastChunkAt;
  if (silenceGapMs !== null) {
    stream.maxSilenceGapMs = Math.max(stream.maxSilenceGapMs, silenceGapMs);
    if (silenceGapMs >= SLOW_STREAM_SILENCE_LOG_MS) {
      log("info", "Stream silence gap", {
        traceId,
        label: stream.label,
        user: stream.user,
        path: stream.path,
        silenceGapMs,
        chunkCountBeforeGap: stream.chunkCount,
        eventCountBeforeGap: stream.eventCount,
        bytesReceivedBeforeGap: stream.bytesReceived,
      });
    }
  }
  stream.chunkCount++;
  stream.bytesReceived += chunkBytes;
  stream.lastChunkAt = now;

  if (stream.firstChunkAt === null) {
    stream.firstChunkAt = now;
    markActiveRequestStreaming(traceId, now);
    recordStreamStartHistory(
      traceId,
      stream.label,
      request?.sessionId ?? null,
      request?.conversationKey ?? null,
      request?.attempt ?? 1,
      "first_chunk",
      now,
    );
    const firstChunkDelayMs = now - stream.openedAt;
    const recentlyActiveStreams = countRecentlyActiveStreams(now);
    const requestContext = buildActiveRequestContext(traceId, now);
    log("info", "Stream first chunk", {
      traceId,
      label: stream.label,
      user: stream.user,
      path: stream.path,
      firstChunkDelayMs,
      activeStreams: activeStreams.size,
      otherRecentlyActiveStreams: Math.max(0, recentlyActiveStreams - 1),
      ...(requestContext !== null ? {
        sameLabelActiveRequests: requestContext.sameLabelActiveRequests,
        sameLabelWaitingForFirstChunkRequests: requestContext.sameLabelWaitingForFirstChunkRequests,
        sameLabelStreamingRequests: requestContext.sameLabelStreamingRequests,
        sameConversationActiveRequests: requestContext.sameConversationActiveRequests,
        sameConversationWaitingForFirstChunkRequests: requestContext.sameConversationWaitingForFirstChunkRequests,
        sameConversationStreamingRequests: requestContext.sameConversationStreamingRequests,
      } : {}),
    });
    if (firstChunkDelayMs >= SLOW_FIRST_CHUNK_LOG_MS) {
      log("warn", "Slow first stream chunk", {
        traceId,
        label: stream.label,
        user: stream.user,
        path: stream.path,
        attempt: request?.attempt ?? 1,
        sessionId: request?.sessionId ?? null,
        conversationKey: request?.conversationKey ?? null,
        firstChunkDelayMs,
        activeStreams: activeStreams.size,
        otherRecentlyActiveStreams: Math.max(0, recentlyActiveStreams - 1),
        ...(requestContext !== null ? { activeRequestContext: requestContext } : {}),
        recentStreamStartHistory: summarizeRecentStreamStartHistory(
          stream.label,
          request?.sessionId ?? null,
          request?.conversationKey ?? null,
          now,
        ),
      });
    }
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
    maxSilenceGapMs: getStreamMaxSilenceGapMs(stream, now),
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

function abandonActiveStream(
  traceId: string,
  reason: string,
  inputTokens: number,
  outputTokens: number,
  cacheReadTokens: number,
  cacheCreationTokens: number,
): void {
  const stream = activeStreams.get(traceId);
  if (!stream) return;
  const request = activeRequests.get(traceId);

  const now = Date.now();
  if (reason === "first_chunk_timeout") {
    recordStreamStartHistory(
      traceId,
      stream.label,
      request?.sessionId ?? null,
      request?.conversationKey ?? null,
      request?.attempt ?? 1,
      "first_chunk_timeout",
      now,
    );
  }
  activeStreams.delete(traceId);
  log("warn", "Stream abandoned", {
    traceId,
    label: stream.label,
    user: stream.user,
    path: stream.path,
    reason,
    durationMs: now - stream.openedAt,
    firstChunkDelayMs: stream.firstChunkAt === null ? null : stream.firstChunkAt - stream.openedAt,
    sinceLastChunkMs: stream.lastChunkAt === null ? null : now - stream.lastChunkAt,
    maxSilenceGapMs: getStreamMaxSilenceGapMs(stream, now),
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

function getStreamMaxSilenceGapMs(stream: ActiveStreamState, now: number): number | null {
  if (stream.firstChunkAt === null || stream.lastChunkAt === null) return null;
  return Math.max(stream.maxSilenceGapMs, now - stream.lastChunkAt);
}

async function readNextChunkWithTimeout(
  reader: UpstreamReader,
  timeoutMs: number,
): Promise<NextChunkResult> {
  if (timeoutMs <= 0) {
    try {
      const result = await reader.read();
      if (result.done || result.value === undefined) return { kind: "done" };
      return { kind: "chunk", chunk: normalizeChunk(result.value) };
    } catch (error) {
      return { kind: "error", error };
    }
  }

  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  try {
    const readPromise = reader.read().then(
      (result) => ({ kind: "read" as const, result }),
      (error) => ({ kind: "error" as const, error }),
    );
    const timeoutPromise = new Promise<{ kind: "timeout" }>((resolve) => {
      timeoutId = setTimeout(() => resolve({ kind: "timeout" }), timeoutMs);
    });
    const outcome = await Promise.race([readPromise, timeoutPromise]);

    if (outcome.kind === "timeout") return { kind: "timeout" };
    if (outcome.kind === "error") return { kind: "error", error: outcome.error };
    if (outcome.result.done || outcome.result.value === undefined) return { kind: "done" };
    return { kind: "chunk", chunk: normalizeChunk(outcome.result.value) };
  } finally {
    if (timeoutId !== null) clearTimeout(timeoutId);
  }
}

function fetchUpstream(
  url: string,
  method: string,
  headers: Headers,
  body: BufferedRequestBody,
  signal?: AbortSignal,
): Promise<Response> {
  const normalizedSignal = signal ?? null;
  if (method === "GET" || method === "HEAD" || method === "OPTIONS") {
    return fetch(url, {
      method,
      headers,
      signal: normalizedSignal,
      keepalive: false,
    });
  }
  return fetch(
    url,
    {
      method,
      headers,
      body,
      signal: normalizedSignal,
      keepalive: false,
      duplex: "half",
    } satisfies BunFetchRequestInit,
  );
}

async function waitForFirstStreamChunk(
  source: ReadableStream<Uint8Array>,
  timeoutMs: number,
  abortController: AbortController,
): Promise<FirstStreamChunkResult> {
  const reader = getStreamReader(source);
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  try {
    const readPromise = reader.read().then(
      (result) => ({ kind: "read" as const, result }),
      (error) => ({ kind: "error" as const, error: String(error) }),
    );
    const timeoutPromise = new Promise<{ kind: "timeout" }>((resolve) => {
      timeoutId = setTimeout(() => resolve({ kind: "timeout" }), timeoutMs);
    });
    const outcome = await Promise.race([readPromise, timeoutPromise]);

    if (outcome.kind === "timeout") {
      abortController.abort("first_chunk_timeout");
      try { await reader.cancel("first_chunk_timeout"); } catch {}
      try { reader.releaseLock(); } catch {}
      return { kind: "retry", reason: "first_chunk_timeout" };
    }

    if (outcome.kind === "error") {
      try { await reader.cancel("stream_read_failed_before_first_chunk"); } catch {}
      try { reader.releaseLock(); } catch {}
      return {
        kind: "retry",
        reason: "stream_read_failed_before_first_chunk",
        error: outcome.error,
      };
    }

    if (outcome.result.done || outcome.result.value === undefined) {
      try { await reader.cancel("stream_ended_before_first_chunk"); } catch {}
      try { reader.releaseLock(); } catch {}
      return { kind: "retry", reason: "stream_ended_before_first_chunk" };
    }

    return {
      kind: "chunk",
      firstChunk: normalizeChunk(outcome.result.value),
      reader,
    };
  } finally {
    if (timeoutId !== null) clearTimeout(timeoutId);
  }
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
  } else {
    headers.set("x-api-key", apiKey);
  }

  return headers;
}

type QuotaStatus = "allowed" | "allowed_warning" | "rejected";

type SupportedCapacityWindowConfig = {
  readonly windowName: string;
  readonly claimAbbrev: string;
  readonly advisoryWarningUtilization: number;
};

type CapacityWindowDraft = {
  readonly windowName: string;
  status?: QuotaStatus | null;
  utilization?: number | null;
  resetAt?: ReturnType<typeof unixMs> | null;
  surpassedThreshold?: number | null;
};

const SUPPORTED_CAPACITY_WINDOWS: readonly SupportedCapacityWindowConfig[] = [
  {
    windowName: "unified-5h",
    claimAbbrev: "5h",
    advisoryWarningUtilization: 0.9,
  },
  {
    windowName: "unified-7d",
    claimAbbrev: "7d",
    advisoryWarningUtilization: 0.75,
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

function hasReachedWindowThreshold(
  utilization: number | null | undefined,
  surpassedThreshold: number | null | undefined,
): boolean {
  return utilization !== null
    && utilization !== undefined
    && surpassedThreshold !== null
    && surpassedThreshold !== undefined
    && utilization >= surpassedThreshold;
}

function shouldWarnForWindowUtilization(
  utilization: number | null | undefined,
  config: SupportedCapacityWindowConfig,
): boolean {
  return utilization !== null
    && utilization !== undefined
    && utilization >= config.advisoryWarningUtilization;
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

  for (const config of SUPPORTED_CAPACITY_WINDOWS) {
    const utilization = parseFinite(headers.get(`anthropic-ratelimit-unified-${config.claimAbbrev}-utilization`));
    const resetAt = parseEpochMs(headers.get(`anthropic-ratelimit-unified-${config.claimAbbrev}-reset`));
    const surpassedThreshold = parseFinite(headers.get(
      `anthropic-ratelimit-unified-${config.claimAbbrev}-surpassed-threshold`,
    ));

    if (utilization === null && resetAt === null && surpassedThreshold === null) continue;

    const status: QuotaStatus = hasReachedWindowThreshold(utilization, surpassedThreshold)
      || shouldWarnForWindowUtilization(utilization, config)
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
      status = earlyWarningObserved || hasReachedWindowThreshold(unifiedUtilization, unifiedSurpassedThreshold)
        ? "allowed_warning"
        : status;
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

function createTokenTrackingObserver(
  entry: import("./types.ts").ApiKeyEntry,
  keyManager: KeyManager,
  proxyUser: ProxyTokenEntry | null | undefined,
  schemaTracker: SchemaTracker,
  endpoint: string,
  traceId: string,
): StreamObserver {
  const decoder = new TextDecoder();
  let buffer = "";
  let inputTokens = 0;
  let outputTokens = 0;
  let cacheReadTokens = 0;
  let cacheCreationTokens = 0;
  let finalized = false;
  registerActiveStream(traceId, entry.label, proxyUser?.label, endpoint);

  function observeChunk(chunk: Uint8Array): void {
    if (finalized) return;
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
  }

  function finish(): void {
    if (finalized) return;
    finalized = true;
    buffer = "";
    closeActiveStream(traceId, inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens);
    clearActiveRequest(traceId);
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
  }

  function abandon(reason: string): void {
    if (finalized) return;
    finalized = true;
    buffer = "";
    abandonActiveStream(traceId, reason, inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens);
    clearActiveRequest(traceId);
  }

  return { observeChunk, finish, abandon };
}

function createTrackedStreamFromReader(
  reader: UpstreamReader,
  firstChunk: Uint8Array,
  observer: StreamObserver,
  abortController: AbortController,
  streamIdleTimeoutMs: number,
): ReadableStream<Uint8Array> {
  let sentFirstChunk = false;
  let closed = false;

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (closed) return;

      if (!sentFirstChunk) {
        sentFirstChunk = true;
        controller.enqueue(firstChunk);
        return;
      }

      const nextChunk = await readNextChunkWithTimeout(reader, streamIdleTimeoutMs);
      if (nextChunk.kind === "timeout") {
        closed = true;
        abortController.abort("stream_idle_timeout");
        try { await reader.cancel("stream_idle_timeout"); } catch {}
        try { reader.releaseLock(); } catch {}
        observer.abandon("stream_idle_timeout");
        controller.error(new Error(`Upstream stream idle timeout after ${streamIdleTimeoutMs}ms`));
        return;
      }
      if (nextChunk.kind === "error") {
        closed = true;
        observer.abandon("stream_read_failed_after_first_chunk");
        controller.error(nextChunk.error);
        return;
      }
      if (nextChunk.kind === "done") {
        closed = true;
        observer.finish();
        try { reader.releaseLock(); } catch {}
        controller.close();
        return;
      }

      const chunk = nextChunk.chunk;
      observer.observeChunk(chunk);
      controller.enqueue(chunk);
    },

    async cancel(reason) {
      if (closed) return;
      closed = true;
      try { await reader.cancel(reason); } catch {}
      try { reader.releaseLock(); } catch {}
      observer.abandon("downstream_cancelled");
    },
  });
}
