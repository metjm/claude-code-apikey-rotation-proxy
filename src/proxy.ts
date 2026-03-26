import type { KeyManager } from "./key-manager.ts";
import type { ProxyConfig, ProxyResult } from "./types.ts";
import { log } from "./logger.ts";

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
 */
const STRIPPED_RESPONSE_HEADERS: ReadonlySet<string> = new Set([
  "connection",
  "keep-alive",
  "transfer-encoding",
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
): Promise<ProxyResult> {
  if (keyManager.totalCount() === 0) {
    return { kind: "no_keys" };
  }

  const triedKeys = new Set<string>();
  let attempts = 0;

  while (attempts < config.maxRetriesPerRequest) {
    const entry = keyManager.getNextAvailableKey();

    if (entry === null) {
      return allExhaustedResult(keyManager);
    }

    if (triedKeys.has(entry.key)) {
      return allExhaustedResult(keyManager);
    }

    triedKeys.add(entry.key);
    attempts++;
    keyManager.recordRequest(entry);

    const url = new URL(req.url);
    const upstreamUrl = `${config.upstream}${url.pathname}${url.search}`;
    const headers = buildUpstreamHeaders(req.headers, entry.key);

    log("debug", "Proxying request", {
      label: entry.label,
      method: req.method,
      path: url.pathname,
      attempt: attempts,
    });

    let upstream: Response;
    try {
      upstream = await fetchUpstream(upstreamUrl, req.method, headers, req.body);
    } catch (err) {
      keyManager.recordError(entry);
      log("error", "Upstream fetch failed", {
        label: entry.label,
        error: String(err),
      });
      return {
        kind: "error",
        status: 502,
        body: `Upstream connection failed: ${String(err)}`,
        usedKey: entry,
      };
    }

    if (upstream.status === RATE_LIMIT_STATUS) {
      const retryAfter = parseRetryAfter(upstream.headers.get("retry-after"));
      keyManager.recordRateLimit(entry, retryAfter);
      await upstream.text();

      log("info", "Rate limited, trying next key", {
        label: entry.label,
        retryAfter,
        availableKeys: keyManager.availableCount(),
      });
      continue;
    }

    if (upstream.status >= 400) {
      keyManager.recordError(entry);
      const body = await upstream.text();
      return { kind: "error", status: upstream.status, body, usedKey: entry };
    }

    const responseHeaders = new Headers();
    for (const [key, value] of upstream.headers.entries()) {
      if (!STRIPPED_RESPONSE_HEADERS.has(key.toLowerCase())) {
        responseHeaders.set(key, value);
      }
    }

    const proxiedResponse = new Response(upstream.body, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: responseHeaders,
    });

    keyManager.recordSuccess(entry, 0, 0);
    return { kind: "success", response: proxiedResponse, usedKey: entry };
  }

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
