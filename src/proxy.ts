import type { KeyManager } from "./key-manager.ts";
import type { ProxyConfig, ProxyResult, ProxyTokenEntry } from "./types.ts";
import { log } from "./logger.ts";
import { emitWithKeys } from "./events.ts";

const RATE_LIMIT_STATUS = 429 as const;

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
  proxyUser?: ProxyTokenEntry | null,
): Promise<ProxyResult> {
  if (keyManager.totalCount() === 0) {
    return { kind: "no_keys" };
  }

  if (proxyUser) keyManager.recordTokenRequest(proxyUser);

  const url = new URL(req.url);

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

    log("info", "Proxying request", {
      label: entry.label,
      user: proxyUser?.label,
      method: req.method,
      path: url.pathname,
      attempt: attempts,
      authType: "x-api-key",
    });
    emitWithKeys({
      type: "request", ts: new Date().toISOString(), label: entry.label,
      user: proxyUser?.label,
      method: req.method, path: url.pathname, attempt: attempts,
    }, keyManager.listKeys());

    let upstream: Response;
    try {
      upstream = await fetchUpstream(upstreamUrl, req.method, headers, req.body);
    } catch (err) {
      keyManager.recordError(entry);
      if (proxyUser) keyManager.recordTokenError(proxyUser);
      log("error", "Upstream fetch failed", {
        label: entry.label,
        user: proxyUser?.label,
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

    log("info", "Upstream responded", {
      label: entry.label,
      user: proxyUser?.label,
      status: upstream.status,
    });
    emitWithKeys({
      type: "response", ts: new Date().toISOString(), label: entry.label,
      user: proxyUser?.label, status: upstream.status,
    }, keyManager.listKeys());

    if (upstream.status === RATE_LIMIT_STATUS) {
      const retryAfter = parseRetryAfter(upstream.headers.get("retry-after"));
      keyManager.recordRateLimit(entry, retryAfter);
      await upstream.text();

      log("info", "Rate limited, trying next key", {
        label: entry.label,
        user: proxyUser?.label,
        retryAfter,
        availableKeys: keyManager.availableCount(),
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
      body = createTokenTrackingStream(upstream.body, entry, keyManager, proxyUser);
    } else if (upstream.body !== null) {
      const text = await upstream.text();
      extractTokensFromJson(text, entry, keyManager, proxyUser);
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
  const earliest = keyManager.getEarliestAvailableKey();
  if (earliest === null) return { kind: "no_keys" };
  return { kind: "all_exhausted", earliestAvailableAt: earliest.availableAt };
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

  headers.set("x-api-key", apiKey);
  headers.set("anthropic-version", incoming.get("anthropic-version") ?? "2023-06-01");
  return headers;
}

function parseRetryAfter(header: string | null): number {
  if (header === null) return 60;
  const secs = parseFloat(header);
  return Number.isFinite(secs) && secs > 0 ? secs : 60;
}

// ── Token tracking ────────────────────────────────────────────────

interface AnthropicUsage {
  readonly input_tokens?: number;
  readonly output_tokens?: number;
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
    keyManager.recordSuccess(entry, input, output);
    if (proxyUser) keyManager.recordTokenSuccess(proxyUser, input, output);
    if (input > 0 || output > 0) {
      log("info", "Token usage", { label: entry.label, user: proxyUser?.label, input, output });
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
  proxyUser?: ProxyTokenEntry | null,
): ReadableStream<Uint8Array> {
  const decoder = new TextDecoder();
  let buffer = "";
  let inputTokens = 0;
  let outputTokens = 0;

  return source.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      controller.enqueue(chunk);

      buffer += decoder.decode(chunk, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const json = line.slice(6).trim();
        if (json === "[DONE]") continue;

        try {
          const event = JSON.parse(json) as AnthropicStreamDelta;
          if (event.type === "message_start" && event.message?.usage) {
            inputTokens += event.message.usage.input_tokens ?? 0;
          }
          if (event.type === "message_delta" && event.usage) {
            outputTokens += event.usage.output_tokens ?? 0;
          }
        } catch {
          // Not valid JSON — skip
        }
      }
    },

    flush() {
      keyManager.recordSuccess(entry, inputTokens, outputTokens);
      if (proxyUser) keyManager.recordTokenSuccess(proxyUser, inputTokens, outputTokens);
      if (inputTokens > 0 || outputTokens > 0) {
        log("info", "Token usage (stream)", {
          label: entry.label,
          user: proxyUser?.label,
          input: inputTokens,
          output: outputTokens,
        });
        emitWithKeys({
          type: "tokens", ts: new Date().toISOString(), label: entry.label,
          user: proxyUser?.label, input: inputTokens, output: outputTokens,
        }, keyManager.listKeys());
      }
    },
  }));
}

