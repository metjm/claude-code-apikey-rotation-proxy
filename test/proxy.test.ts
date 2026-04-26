import { describe, test, expect, afterEach, beforeEach, spyOn } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Server } from "bun";

import { KeyManager } from "../src/key-manager.ts";
import { proxyRequest, resetProxyDebugStateForTests } from "../src/proxy.ts";
import { subscribe, type ProxyEvent } from "../src/events.ts";
import type { ProxyConfig, ProxyTokenEntry } from "../src/types.ts";
import { SchemaTracker } from "../src/schema-tracker.ts";
import { setLogLevel } from "../src/logger.ts";

// ── Helpers ────────────────────────────────────────────────────────

/** Valid fake key that satisfies the sk-ant- prefix requirement. */
const FAKE_KEY_A = "sk-ant-api03-test-key-AAAAAAAAAAAAAAAA";
const FAKE_KEY_B = "sk-ant-api03-test-key-BBBBBBBBBBBBBBBB";
const FAKE_KEY_C = "sk-ant-api03-test-key-CCCCCCCCCCCCCCCC";
const FAKE_OAUTH = "sk-ant-oat-test-oauth-AAAAAAAAAAAAAAAA";

interface TestSetup {
  km: KeyManager;
  st: SchemaTracker;
  tmpDir: string;
  cleanup: () => void;
}

function createTestSchemaTracker(tmpDir: string): SchemaTracker {
  const dbPath = join(tmpDir, "test-state.db");
  return new SchemaTracker(dbPath);
}

function createTestSetup(opts?: { perConversationPinning?: boolean }): TestSetup {
  const tmpDir = mkdtempSync(join(tmpdir(), "proxy-test-"));
  // KeyManager mode mirrors makeConfig's default — see comment there.
  const km = new KeyManager(tmpDir, {
    perConversationPinning: opts?.perConversationPinning ?? true,
  });
  const st = createTestSchemaTracker(tmpDir);
  return {
    km,
    st,
    tmpDir,
    cleanup: () => {
      try { st.close(); } catch {}
      try { km.close(); } catch {}
      rmSync(tmpDir, { recursive: true, force: true });
    },
  };
}

interface MockUpstream {
  url: string;
  server: Server;
  stop: () => void;
}

function startMockUpstream(
  handler: (req: Request) => Response | Promise<Response>,
): MockUpstream {
  const server = Bun.serve({ port: 0, fetch: handler });
  return {
    url: `http://localhost:${server.port}`,
    server,
    stop: () => server.stop(true),
  };
}

function makeConfig(upstream: string, overrides?: Partial<ProxyConfig>): ProxyConfig {
  return {
    port: 0,
    upstream,
    adminToken: null,
    dataDir: "/tmp",
    maxRetriesPerRequest: 10,
    firstChunkTimeoutMs: 16_000,
    streamIdleTimeoutMs: 120_000,
    maxFirstChunkRetries: 2,
    webhookUrl: null,
    // Default test config exercises per-conversation pinning (the toggle-on
    // case) since most existing tests assert hash-based routing of sub-agents.
    // Tests for session-only default product behavior override this.
    perConversationPinning: true,
    ...overrides,
  };
}

/** Build a Request aimed at the proxy (the host doesn't matter for proxyRequest). */
function makeRequest(
  path: string,
  opts?: RequestInit & { baseUrl?: string },
): Request {
  const base = opts?.baseUrl ?? "http://proxy.local";
  return new Request(`${base}${path}`, opts);
}

function futureEpochSeconds(offsetSecs: number): string {
  return String(Math.floor(Date.now() / 1000) + offsetSecs);
}

/**
 * Collect all emitted events during a callback.
 * Returns [result, collectedEvents].
 */
async function collectEvents<T>(
  fn: () => Promise<T>,
): Promise<[T, ProxyEvent[]]> {
  const events: ProxyEvent[] = [];
  const unsub = subscribe((e) => events.push(e));
  try {
    const result = await fn();
    return [result, events];
  } finally {
    unsub();
  }
}

// ── Lifecycle ──────────────────────────────────────────────────────

let setups: TestSetup[] = [];
let upstreams: MockUpstream[] = [];

afterEach(() => {
  for (const u of upstreams) u.stop();
  upstreams = [];
  for (const s of setups) s.cleanup();
  setups = [];
  resetProxyDebugStateForTests();
});

/** Convenience: create setup + track for cleanup. */
function setup(opts?: { perConversationPinning?: boolean }): TestSetup {
  const s = createTestSetup(opts);
  setups.push(s);
  return s;
}

/** Convenience: create upstream + track for cleanup. */
function upstream(
  handler: (req: Request) => Response | Promise<Response>,
): MockUpstream {
  const u = startMockUpstream(handler);
  upstreams.push(u);
  return u;
}

// ────────────────────────────────────────────────────────────────────
// TESTS
// ────────────────────────────────────────────────────────────────────

describe("Basic Proxying", () => {
  test("returns no_keys when no keys registered", async () => {
    const { km, st } = setup();
    const mock = upstream(() => new Response("should not reach"));
    const config = makeConfig(mock.url);

    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    expect(result.kind).toBe("no_keys");
  });

  test("successfully proxies GET request", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream((req) => {
      expect(req.method).toBe("GET");
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/models", { method: "GET" }), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      expect(result.response.status).toBe(200);
      const body = await result.response.json();
      expect(body).toEqual({ ok: true });
    }
  });

  test("successfully proxies POST request with body", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    let receivedBody: unknown = null;
    const mock = upstream(async (req) => {
      expect(req.method).toBe("POST");
      receivedBody = await req.json();
      return new Response(JSON.stringify({ id: "msg_123", usage: { input_tokens: 10, output_tokens: 5 } }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const config = makeConfig(mock.url);
    const payload = { model: "claude-sonnet-4-20250514", messages: [{ role: "user", content: "Hello" }] };
    const result = await proxyRequest(
      makeRequest("/v1/messages", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      }),
      km,
      config,
      st,
    );

    expect(result.kind).toBe("success");
    expect(receivedBody).toEqual(payload);
  });

  test("preserves query string in upstream URL", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    let receivedUrl = "";
    const mock = upstream((req) => {
      receivedUrl = req.url;
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages?beta=true&version=2"), km, config, st);

    const parsed = new URL(receivedUrl);
    expect(parsed.pathname).toBe("/v1/messages");
    expect(parsed.searchParams.get("beta")).toBe("true");
    expect(parsed.searchParams.get("version")).toBe("2");
  });

  test("returns upstream response status and body", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(JSON.stringify({ result: "done" }), {
        status: 201,
        statusText: "Created",
        headers: { "content-type": "application/json" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      expect(result.response.status).toBe(201);
      const body = await result.response.json();
      expect(body).toEqual({ result: "done" });
    }
  });
});

// ────────────────────────────────────────────────────────────────────

describe("Header Handling", () => {
  test("disables upstream keep-alive for every fetch", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("ok", { status: 200 }),
    );

    try {
      const result = await proxyRequest(makeRequest("/v1/messages"), km, makeConfig("https://api.anthropic.com"), st);

      expect(result.kind).toBe("success");
      expect(fetchSpy).toHaveBeenCalledTimes(1);

      const [, init] = fetchSpy.mock.calls[0]!;
      expect(init).toBeDefined();
      expect((init as RequestInit).keepalive).toBe(false);
      expect((init as RequestInit).headers).toBeInstanceOf(Headers);
      expect(((init as RequestInit).headers as Headers).get("connection")).not.toBe("close");
    } finally {
      fetchSpy.mockRestore();
    }
  });

  test("strips x-api-key from outgoing request", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    let receivedHeaders: Headers | null = null;
    const mock = upstream((req) => {
      receivedHeaders = req.headers;
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(
      makeRequest("/v1/messages", {
        headers: { "x-api-key": "client-supplied-key" },
      }),
      km,
      config,
      st,
    );

    // The x-api-key should be the proxy's key, not the client-supplied one
    expect(receivedHeaders!.get("x-api-key")).toBe(FAKE_KEY_A);
    expect(receivedHeaders!.get("x-api-key")).not.toBe("client-supplied-key");
  });

  test("strips authorization from outgoing request", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    let receivedHeaders: Headers | null = null;
    const mock = upstream((req) => {
      receivedHeaders = req.headers;
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(
      makeRequest("/v1/messages", {
        headers: { authorization: "Bearer client-token" },
      }),
      km,
      config,
      st,
    );

    // For a non-OAuth key, there should be no authorization header
    expect(receivedHeaders!.get("authorization")).toBeNull();
  });

  test("strips host, connection, keep-alive, transfer-encoding from client headers", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    let receivedHeaders: Headers | null = null;
    const mock = upstream((req) => {
      receivedHeaders = req.headers;
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(
      makeRequest("/v1/messages", {
        headers: {
          host: "evil.com",
          connection: "keep-alive",
          "keep-alive": "timeout=5",
          "transfer-encoding": "chunked",
          "x-marker": "test-value",
        },
      }),
      km,
      config,
      st,
    );

    // The proxy strips client-supplied hop-by-hop headers from the built
    // headers object. fetch() itself may re-add "host" and "connection" at
    // the transport layer (these are not the original client values).
    // Verify the client's evil.com host was NOT forwarded:
    expect(receivedHeaders!.get("host")).not.toBe("evil.com");
    // keep-alive custom header should be stripped:
    expect(receivedHeaders!.get("keep-alive")).toBeNull();
    // Non-stripped headers should still be forwarded:
    expect(receivedHeaders!.get("x-marker")).toBe("test-value");
  });

  test("adds x-api-key for regular API keys", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    let receivedHeaders: Headers | null = null;
    const mock = upstream((req) => {
      receivedHeaders = req.headers;
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(receivedHeaders!.get("x-api-key")).toBe(FAKE_KEY_A);
    expect(receivedHeaders!.get("authorization")).toBeNull();
  });

  test("adds Authorization: Bearer for OAuth tokens (sk-ant-oat-*)", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_OAUTH, "oauth-key");

    let receivedHeaders: Headers | null = null;
    const mock = upstream((req) => {
      receivedHeaders = req.headers;
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(receivedHeaders!.get("authorization")).toBe(`Bearer ${FAKE_OAUTH}`);
    expect(receivedHeaders!.get("x-api-key")).toBeNull();
  });

  test("forwards client-provided anthropic-version without adding a default", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    let receivedVersion: string | null = null;
    const mock = upstream((req) => {
      receivedVersion = req.headers.get("anthropic-version");
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    expect(receivedVersion).toBeNull();

    await proxyRequest(
      makeRequest("/v1/messages", {
        headers: { "anthropic-version": "2024-01-01" },
      }),
      km,
      config,
      st,
    );
    expect(receivedVersion).toBe("2024-01-01");
  });

  test("preserves custom client headers (e.g. content-type, user-agent)", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    let receivedHeaders: Headers | null = null;
    const mock = upstream((req) => {
      receivedHeaders = req.headers;
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(
      makeRequest("/v1/messages", {
        headers: {
          "content-type": "application/json",
          "user-agent": "my-test-client/1.0",
          "x-custom-header": "preserved",
        },
      }),
      km,
      config,
      st,
    );

    expect(receivedHeaders!.get("content-type")).toBe("application/json");
    expect(receivedHeaders!.get("user-agent")).toBe("my-test-client/1.0");
    expect(receivedHeaders!.get("x-custom-header")).toBe("preserved");
  });

  test("strips content-encoding, content-length, transfer-encoding from response", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    // Use "identity" content-encoding (no-op) so fetch() does not try to
    // decompress the body. The proxy code strips the header regardless of
    // its value. We also include content-length and connection to verify
    // the full STRIPPED_RESPONSE_HEADERS set.
    const mock = upstream(() =>
      new Response("ok", {
        headers: {
          "content-encoding": "identity",
          "content-length": "2",
          "x-request-id": "abc123",
          "connection": "keep-alive",
          "keep-alive": "timeout=5",
        },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      expect(result.response.headers.get("content-encoding")).toBeNull();
      expect(result.response.headers.get("content-length")).toBeNull();
      expect(result.response.headers.get("connection")).toBeNull();
      expect(result.response.headers.get("keep-alive")).toBeNull();
      // Non-stripped headers are preserved
      expect(result.response.headers.get("x-request-id")).toBe("abc123");
    }
  });
});

// ────────────────────────────────────────────────────────────────────

describe("Rate Limit Handling (429)", () => {
  test("detects 429 and rotates to next key", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    let callCount = 0;
    const mock = upstream((req) => {
      callCount++;
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "30" },
        });
      }
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    expect(callCount).toBe(2);
  });

  test("parses Retry-After header", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "120" },
        });
      }
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    // key-a should be unavailable for ~120s
    const keys = km.listKeys();
    const keyA = keys.find((k) => k.label === "key-a")!;
    // availableAt should be roughly now + 120s
    const expectedMin = Date.now() + 119_000;
    const expectedMax = Date.now() + 121_000;
    expect(keyA.availableAt).toBeGreaterThanOrEqual(expectedMin);
    expect(keyA.availableAt).toBeLessThanOrEqual(expectedMax);
  });

  test("prefers retry-after-ms when present on 429 responses", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: {
            "retry-after": "120",
            "retry-after-ms": "1500",
          },
        });
      }
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const keyA = km.listKeys().find((k) => k.label === "key-a")!;
    const expectedMin = Date.now() + 1_000;
    const expectedMax = Date.now() + 3_000;
    expect(keyA.availableAt).toBeGreaterThanOrEqual(expectedMin);
    expect(keyA.availableAt).toBeLessThanOrEqual(expectedMax);
  });

  test("defaults retry to 60s when header missing", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        // No retry-after header
        return new Response("rate limited", { status: 429 });
      }
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const keys = km.listKeys();
    const keyA = keys.find((k) => k.label === "key-a")!;
    const expectedMin = Date.now() + 59_000;
    const expectedMax = Date.now() + 61_000;
    expect(keyA.availableAt).toBeGreaterThanOrEqual(expectedMin);
    expect(keyA.availableAt).toBeLessThanOrEqual(expectedMax);
  });

  test("marks key unavailable for retry period", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "300" },
        });
      }
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const keys = km.listKeys();
    const keyA = keys.find((k) => k.label === "key-a")!;
    expect(keyA.isAvailable).toBe(false);

    const keyB = keys.find((k) => k.label === "key-b")!;
    expect(keyB.isAvailable).toBe(true);
  });

  test("returns all_exhausted when all keys rate-limited", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const mock = upstream(() =>
      new Response("rate limited", {
        status: 429,
        headers: { "retry-after": "60" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("all_exhausted");
    if (result.kind === "all_exhausted") {
      expect(result.earliestAvailableAt).toBeGreaterThan(Date.now());
    }
  });

  test("tracks rateLimitHits stat", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "30" },
        });
      }
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const keys = km.listKeys();
    const keyA = keys.find((k) => k.label === "key-a")!;
    expect(keyA.stats.rateLimitHits).toBe(1);
  });

  test("emits rate_limit event with user label", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");
    const proxyUser = km.addToken("test-token-12345678", "alice");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "45" },
        });
      }
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser),
    );

    const rlEvents = events.filter((e) => e.type === "rate_limit");
    expect(rlEvents.length).toBe(1);
    expect(rlEvents[0]!.user).toBe("alice");
    expect(rlEvents[0]!.retryAfter).toBe(45);
  });
});

// ────────────────────────────────────────────────────────────────────

describe("Error Handling", () => {
  test("returns 502 on upstream connection failure", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    // Point at a port that is not listening
    const config = makeConfig("http://localhost:1");
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("error");
    if (result.kind === "error") {
      expect(result.status).toBe(502);
      expect(result.body).toContain("Upstream connection failed");
    }
  });

  test("returns upstream error status for 4xx/5xx (not 429)", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const errorBody = JSON.stringify({ error: { type: "invalid_request_error", message: "bad model" } });
    const mock = upstream(() =>
      new Response(errorBody, {
        status: 400,
        headers: { "content-type": "application/json" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("error");
    if (result.kind === "error") {
      expect(result.status).toBe(400);
      expect(JSON.parse(result.body)).toEqual({
        error: { type: "invalid_request_error", message: "bad model" },
      });
    }
  });

  test("returns upstream 500 error as-is", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response("internal server error", { status: 500 }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("error");
    if (result.kind === "error") {
      expect(result.status).toBe(500);
    }
  });

  test("does not retry on non-429 errors", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    let callCount = 0;
    const mock = upstream(() => {
      callCount++;
      return new Response("forbidden", { status: 403 });
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    // Should only hit once -- no retry on 403
    expect(callCount).toBe(1);
  });

  test("records error stats on key and token", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "alice");

    const mock = upstream(() =>
      new Response("forbidden", { status: 403 }),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser);

    const keys = km.listKeys();
    expect(keys[0]!.stats.errors).toBe(1);

    const tokens = km.listTokens();
    expect(tokens[0]!.stats.errors).toBe(1);
  });
});

// ────────────────────────────────────────────────────────────────────

describe("Token Tracking - Non-Streaming", () => {
  test("extracts usage.input_tokens from JSON response", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(
        JSON.stringify({
          id: "msg_123",
          type: "message",
          usage: { input_tokens: 150, output_tokens: 42 },
        }),
        { headers: { "content-type": "application/json" } },
      ),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const keys = km.listKeys();
    expect(keys[0]!.stats.totalTokensIn).toBe(150);
  });

  test("extracts usage.output_tokens from JSON response", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(
        JSON.stringify({
          id: "msg_123",
          usage: { input_tokens: 100, output_tokens: 250 },
        }),
        { headers: { "content-type": "application/json" } },
      ),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const keys = km.listKeys();
    expect(keys[0]!.stats.totalTokensOut).toBe(250);
  });

  test("defaults to 0 when usage missing", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(JSON.stringify({ id: "msg_123" }), {
        headers: { "content-type": "application/json" },
      }),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const keys = km.listKeys();
    expect(keys[0]!.stats.totalTokensIn).toBe(0);
    expect(keys[0]!.stats.totalTokensOut).toBe(0);
    expect(keys[0]!.stats.successfulRequests).toBe(1);
  });

  test("records token stats on key", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(
        JSON.stringify({ usage: { input_tokens: 50, output_tokens: 75 } }),
        { headers: { "content-type": "application/json" } },
      ),
    );

    const config = makeConfig(mock.url);
    // Two requests to accumulate
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const keys = km.listKeys();
    expect(keys[0]!.stats.totalTokensIn).toBe(100);
    expect(keys[0]!.stats.totalTokensOut).toBe(150);
    expect(keys[0]!.stats.successfulRequests).toBe(2);
  });

  test("records token stats on proxy user", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "alice");

    const mock = upstream(() =>
      new Response(
        JSON.stringify({ usage: { input_tokens: 200, output_tokens: 300 } }),
        { headers: { "content-type": "application/json" } },
      ),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser);

    const tokens = km.listTokens();
    const alice = tokens.find((t) => t.label === "alice")!;
    expect(alice.stats.totalTokensIn).toBe(200);
    expect(alice.stats.totalTokensOut).toBe(300);
    expect(alice.stats.successfulRequests).toBe(1);
  });

  test("emits tokens event", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(
        JSON.stringify({ usage: { input_tokens: 10, output_tokens: 20 } }),
        { headers: { "content-type": "application/json" } },
      ),
    );

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st),
    );

    const tokenEvents = events.filter((e) => e.type === "tokens");
    expect(tokenEvents.length).toBe(1);
    expect(tokenEvents[0]!.input).toBe(10);
    expect(tokenEvents[0]!.output).toBe(20);
  });

  test("tokens event carries sessionId and conversationHash from request (non-streaming)", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(
        JSON.stringify({ usage: { input_tokens: 11, output_tokens: 22 } }),
        { headers: { "content-type": "application/json" } },
      ),
    );

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(
        makeRequest("/v1/messages", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-claude-code-session-id": "sess-abc",
          },
          body: JSON.stringify({
            messages: [{ role: "user", content: "first message body" }],
          }),
        }),
        km,
        config,
        st,
      ),
    );

    const tokenEvents = events.filter((e) => e.type === "tokens");
    expect(tokenEvents.length).toBe(1);
    expect(tokenEvents[0]!.sessionId).toBe("sess-abc");
    // Hash is 16 hex chars derived from JSON.stringify(messages[0]).
    expect(tokenEvents[0]!.conversationHash).toMatch(/^[0-9a-f]{16}$/);
  });

  test("tokens event has null sessionId and conversationHash when no header or body", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(
        JSON.stringify({ usage: { input_tokens: 1, output_tokens: 2 } }),
        { headers: { "content-type": "application/json" } },
      ),
    );

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st),
    );

    const tokenEvents = events.filter((e) => e.type === "tokens");
    expect(tokenEvents.length).toBe(1);
    expect(tokenEvents[0]!.sessionId).toBe(null);
    expect(tokenEvents[0]!.conversationHash).toBe(null);
  });
});

// ────────────────────────────────────────────────────────────────────

describe("Token Tracking - Streaming (SSE)", () => {
  function makeSSEBody(events: string[]): ReadableStream<Uint8Array> {
    const encoder = new TextEncoder();
    return new ReadableStream({
      start(controller) {
        for (const e of events) {
          controller.enqueue(encoder.encode(e));
        }
        controller.close();
      },
    });
  }

  function parseConsoleEntries(...spies: Array<ReturnType<typeof spyOn>>): Array<Record<string, unknown>> {
    return spies.flatMap((spy) =>
      spy.mock.calls.map(([line]) => JSON.parse(line as string) as Record<string, unknown>)
    );
  }

  test("logs stream lifecycle with trace IDs and chunk timing", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    setLogLevel("debug");
    const logSpy = spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = spyOn(console, "warn").mockImplementation(() => {});

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":10}}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":5}}\n\n',
      "data: [DONE]\n\n",
    ];

    const mock = upstream(() =>
      new Response(makeSSEBody(sseData), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    try {
      const config = makeConfig(mock.url);
      const result = await proxyRequest(
        makeRequest("/v1/messages", { headers: { "x-request-id": "trace-stream-1" } }),
        km,
        config,
        st,
      );

      expect(result.kind).toBe("success");
      if (result.kind === "success") {
        await result.response.text();
      }

      const entries = parseConsoleEntries(logSpy, warnSpy);
      const opened = entries.find((entry) => entry.msg === "Stream opened");
      const firstChunk = entries.find((entry) => entry.msg === "Stream first chunk");
      const closed = entries.find((entry) => entry.msg === "Stream closed");

      expect(opened).toBeDefined();
      expect(opened?.traceId).toBe("trace-stream-1");
      expect(opened?.activeStreams).toBe(1);

      expect(firstChunk).toBeDefined();
      expect(firstChunk?.traceId).toBe("trace-stream-1");
      expect(firstChunk?.firstChunkDelayMs).toEqual(expect.any(Number));
      expect(firstChunk?.otherRecentlyActiveStreams).toBe(0);

      expect(closed).toBeDefined();
      expect(closed?.traceId).toBe("trace-stream-1");
      expect(closed?.maxSilenceGapMs).toEqual(expect.any(Number));
      expect(closed?.chunkCount).toBeGreaterThan(0);
      expect(closed?.eventCount).toBe(2);
      expect(closed?.activeStreamsRemaining).toBe(0);
    } finally {
      logSpy.mockRestore();
      warnSpy.mockRestore();
      setLogLevel("info");
    }
  });

  test("logs slow first chunks with recent history context", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    setLogLevel("debug");
    const logSpy = spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = spyOn(console, "warn").mockImplementation(() => {});

    let now = 1_000_000;
    const dateNowSpy = spyOn(Date, "now").mockImplementation(() => now);
    const encoder = new TextEncoder();
    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(new Response(new ReadableStream({
      start(controller) {
        setTimeout(() => {
          now += 6_000;
          controller.enqueue(encoder.encode('data: {"type":"message_start","message":{"usage":{"input_tokens":1}}}\n\n'));
          controller.close();
        }, 0);
      },
    }), {
      status: 200,
      headers: { "content-type": "text/event-stream" },
    }));

    try {
      const result = await proxyRequest(
        makeRequest("/v1/messages", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-request-id": "trace-slow-first-chunk",
            "x-claude-code-session-id": "session-slow",
          },
          body: JSON.stringify({ stream: true, messages: [] }),
        }),
        km,
        makeConfig("http://mocked-upstream.local"),
        st,
      );

      expect(result.kind).toBe("success");
      await result.response.text();

      const entries = parseConsoleEntries(logSpy, warnSpy);
      const slowEntry = entries.find((entry) =>
        entry.msg === "Slow first stream chunk"
        && entry.traceId === "trace-slow-first-chunk"
      );

      expect(slowEntry).toBeDefined();
      expect(slowEntry).toMatchObject({
        label: "key-a",
        sessionId: "session-slow",
        firstChunkDelayMs: 6000,
      });
      expect(slowEntry?.recentStreamStartHistory).toMatchObject({
        recentStreamStartWindowMs: 15 * 60 * 1000,
        totalFirstChunks15m: 1,
        totalFirstChunkTimeouts15m: 0,
        sameLabelFirstChunks15m: 1,
        sameLabelFirstChunkTimeouts15m: 0,
        sameSessionFirstChunks15m: 1,
        sameSessionFirstChunkTimeouts15m: 0,
      });
    } finally {
      fetchSpy.mockRestore();
      dateNowSpy.mockRestore();
      logSpy.mockRestore();
      warnSpy.mockRestore();
      setLogLevel("info");
    }
  });

  test("retries the same key when the first SSE chunk stalls", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const payload = {
      model: "claude-sonnet-4-20250514",
      stream: true,
      messages: [{ role: "user", content: "retry stalled stream" }],
    };
    const seenKeys: string[] = [];
    const seenBodies: string[] = [];

    const stalledSse = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":10}}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":1}}\n\n',
      "data: [DONE]\n\n",
    ];
    const fastSse = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":20}}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":2}}\n\n',
      "data: [DONE]\n\n",
    ];

    let keyAAttempts = 0;
    let simulatedCompetingRequest = false;
    const fetchSpy = spyOn(globalThis, "fetch").mockImplementation(async (_input, init) => {
      const headers = new Headers(init?.headers);
      const body = init?.body;
      const bodyText = body instanceof Uint8Array ? new TextDecoder().decode(body) : "";
      seenKeys.push(headers.get("x-api-key") ?? "");
      seenBodies.push(bodyText);

      if (headers.get("x-api-key") === FAKE_KEY_A) {
        keyAAttempts++;
        if (keyAAttempts === 1) {
          let cancelled = false;
          setTimeout(() => {
            if (simulatedCompetingRequest) return;
            simulatedCompetingRequest = true;
            const competingKey = km.listKeys()[1];
            if (competingKey) km.recordRequest(competingKey);
          }, 5);
          return new Response(new ReadableStream({
            start(controller) {
              setTimeout(() => {
                if (cancelled) return;
                const encoder = new TextEncoder();
                for (const chunk of stalledSse) {
                  controller.enqueue(encoder.encode(chunk));
                }
                controller.close();
              }, 50);
            },
            cancel() {
              cancelled = true;
            },
          }), {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          });
        }

        return new Response(makeSSEBody(fastSse), {
          status: 200,
          headers: { "content-type": "text/event-stream" },
        });
      }

      return new Response(makeSSEBody(fastSse), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      });
    });

    try {
      const config = makeConfig("http://mocked-upstream.local", {
        firstChunkTimeoutMs: 20,
        maxFirstChunkRetries: 1,
      });
      const result = await proxyRequest(
        makeRequest("/v1/messages", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(payload),
        }),
        km,
        config,
        st,
      );

      expect(result.kind).toBe("success");
      if (result.kind === "success") {
        const body = await result.response.text();
        expect(body).toContain('"output_tokens":2');
      }

      expect(seenKeys).toEqual([FAKE_KEY_A, FAKE_KEY_A]);
      expect(seenBodies).toEqual([JSON.stringify(payload), JSON.stringify(payload)]);

      const [keyA, keyB] = km.listKeys();
      expect(keyA!.stats.errors).toBe(1);
      expect(keyA!.stats.successfulRequests).toBe(1);
      expect(keyB!.stats.successfulRequests).toBe(0);
      expect(keyB!.stats.errors).toBe(0);
    } finally {
      fetchSpy.mockRestore();
    }
  });

  test("logs same-session active request context when first chunk timeouts happen under concurrency", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    setLogLevel("debug");
    const logSpy = spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = spyOn(console, "warn").mockImplementation(() => {});

    let firstFetchStarted!: () => void;
    const firstFetchStartedPromise = new Promise<void>((resolve) => {
      firstFetchStarted = resolve;
    });
    let secondFetchStarted!: () => void;
    const secondFetchStartedPromise = new Promise<void>((resolve) => {
      secondFetchStarted = resolve;
    });
    let fetchCalls = 0;
    const encoder = new TextEncoder();
    const fetchSpy = spyOn(globalThis, "fetch").mockImplementation(async () => {
      fetchCalls++;
      if (fetchCalls === 1) firstFetchStarted();
      if (fetchCalls === 2) secondFetchStarted();

      let cancelled = false;
      return new Response(new ReadableStream({
        start(controller) {
          setTimeout(() => {
            if (cancelled) return;
            controller.enqueue(encoder.encode('data: {"type":"message_start","message":{"usage":{"input_tokens":1}}}\n\n'));
            controller.close();
          }, 200);
        },
        cancel() {
          cancelled = true;
        },
      }), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      });
    });

    try {
      const config = makeConfig("http://mocked-upstream.local", {
        firstChunkTimeoutMs: 50,
        maxFirstChunkRetries: 0,
      });
      const payload = JSON.stringify({ stream: true, messages: [] });

      const requestA = proxyRequest(
        makeRequest("/v1/messages", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-request-id": "trace-timeout-a",
            "x-claude-code-session-id": "session-1",
          },
          body: payload,
        }),
        km,
        config,
        st,
      );
      await firstFetchStartedPromise;
      const requestB = proxyRequest(
        makeRequest("/v1/messages", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-request-id": "trace-timeout-b",
            "x-claude-code-session-id": "session-1",
          },
          body: payload,
        }),
        km,
        config,
        st,
      );
      await secondFetchStartedPromise;
      await new Promise((resolve) => setTimeout(resolve, 0));

      const [resultA, resultB] = await Promise.all([requestA, requestB]);
      expect(resultA.kind).toBe("error");
      expect(resultB.kind).toBe("error");

      const entries = parseConsoleEntries(logSpy, warnSpy);
      const retryEntry = entries.find((entry) =>
        entry.msg === "No first stream chunk yet, retrying request"
        && entry.traceId === "trace-timeout-a"
      );

      expect(retryEntry).toBeDefined();
      const context = retryEntry?.activeRequestContext as Record<string, unknown> | undefined;
      expect(context).toBeDefined();
      expect(context?.sameLabelActiveRequests).toBe(1);
      expect(context?.sameSessionActiveRequests).toBe(1);
      expect(context?.sameConversationActiveRequests).toBe(1);
      expect(context?.sameLabelWaitingForFirstChunkRequests).toBe(1);
      expect(context?.sameSessionWaitingForFirstChunkRequests).toBe(1);
      expect(context?.sameConversationWaitingForFirstChunkRequests).toBe(1);

      const sameLabelPeers = context?.sameLabelPeers as Array<Record<string, unknown>> | undefined;
      expect(sameLabelPeers).toBeDefined();
      expect(sameLabelPeers).toHaveLength(1);
      expect(sameLabelPeers?.[0]).toMatchObject({
        traceId: "trace-timeout-b",
        sessionId: "session-1",
        phase: "waiting_for_first_chunk",
      });

      expect(retryEntry?.recentStreamStartHistory).toMatchObject({
        recentStreamStartWindowMs: 15 * 60 * 1000,
        totalFirstChunks15m: 0,
        totalFirstChunkTimeouts15m: 0,
        sameLabelFirstChunks15m: 0,
        sameLabelFirstChunkTimeouts15m: 0,
        sameSessionFirstChunks15m: 0,
        sameSessionFirstChunkTimeouts15m: 0,
        sameConversationFirstChunks15m: 0,
        sameConversationFirstChunkTimeouts15m: 0,
      });
    } finally {
      fetchSpy.mockRestore();
      logSpy.mockRestore();
      warnSpy.mockRestore();
      setLogLevel("info");
    }
  });

  test("logs long stream silence gaps and tracks the maximum gap", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    setLogLevel("debug");
    const logSpy = spyOn(console, "log").mockImplementation(() => {});

    const originalNow = Date.now;
    let fakeNow = 1_000_000;
    Date.now = () => fakeNow;

    const encoder = new TextEncoder();
    let releaseSecondChunk!: () => void;
    const secondChunkReady = new Promise<void>((resolve) => {
      releaseSecondChunk = resolve;
    });
    let pullCount = 0;
    const fetchSpy = spyOn(globalThis, "fetch").mockImplementation(async () =>
      new Response(new ReadableStream<Uint8Array>({
        async pull(controller) {
          pullCount++;
          if (pullCount === 1) {
            controller.enqueue(encoder.encode('data: {"type":"message_start","message":{"usage":{"input_tokens":10}}}\n\n'));
            return;
          }
          if (pullCount === 2) {
            await secondChunkReady;
            fakeNow += 6_123;
            controller.enqueue(encoder.encode('data: {"type":"message_delta","usage":{"output_tokens":5}}\n\n'));
            controller.close();
          }
        },
      }), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    try {
      const config = makeConfig("http://mocked-upstream.local");
      const result = await proxyRequest(
        makeRequest("/v1/messages", { headers: { "x-request-id": "trace-gap-1" } }),
        km,
        config,
        st,
      );

      expect(result.kind).toBe("success");
      if (result.kind === "success") {
        const bodyPromise = result.response.text();
        releaseSecondChunk();
        await bodyPromise;
      }

      const entries = parseConsoleEntries(logSpy);
      const gap = entries.find((entry) => entry.msg === "Stream silence gap");
      const closed = entries.find((entry) => entry.msg === "Stream closed");

      expect(gap).toBeDefined();
      expect(gap?.traceId).toBe("trace-gap-1");
      expect(gap?.silenceGapMs).toBe(6_123);
      expect(gap?.chunkCountBeforeGap).toBe(1);
      expect(gap?.eventCountBeforeGap).toBe(1);

      expect(closed).toBeDefined();
      expect(closed?.traceId).toBe("trace-gap-1");
      expect(closed?.maxSilenceGapMs).toBe(6_123);
    } finally {
      Date.now = originalNow;
      fetchSpy.mockRestore();
      logSpy.mockRestore();
      setLogLevel("info");
    }
  });

  test("abandons a streaming response after 2 minutes of upstream silence", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    setLogLevel("debug");
    const logSpy = spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = spyOn(console, "warn").mockImplementation(() => {});

    let cancelled = false;
    const encoder = new TextEncoder();
    const fetchSpy = spyOn(globalThis, "fetch").mockImplementation(async () =>
      new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode('data: {"type":"message_start","message":{"usage":{"input_tokens":10}}}\n\n'));
        },
        cancel() {
          cancelled = true;
        },
      }), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    try {
      const config = makeConfig("http://mocked-upstream.local", {
        streamIdleTimeoutMs: 20,
      });
      const result = await proxyRequest(
        makeRequest("/v1/messages", { headers: { "x-request-id": "trace-idle-timeout-1" } }),
        km,
        config,
        st,
      );

      expect(result.kind).toBe("success");
      if (result.kind === "success") {
        const body = result.response.body;
        expect(body).not.toBeNull();
        const reader = body!.getReader();
        const first = await reader.read();
        expect(first.done).toBe(false);
        await expect(reader.read()).rejects.toThrow("Upstream stream idle timeout after 20ms");
      }

      expect(cancelled).toBe(true);

      const entries = parseConsoleEntries(logSpy, warnSpy);
      const abandoned = entries.find((entry) => entry.msg === "Stream abandoned");

      expect(abandoned).toBeDefined();
      expect(abandoned?.traceId).toBe("trace-idle-timeout-1");
      expect(abandoned?.reason).toBe("stream_idle_timeout");
      expect(Number(abandoned?.sinceLastChunkMs)).toBeGreaterThanOrEqual(20);

      const keys = km.listKeys();
      expect(keys[0]!.stats.successfulRequests).toBe(0);
    } finally {
      fetchSpy.mockRestore();
      logSpy.mockRestore();
      warnSpy.mockRestore();
      setLogLevel("info");
    }
  });

  test("returns 504 when the same key keeps stalling past the retry budget", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":10}}}\n\n',
      "data: [DONE]\n\n",
    ];

    const seenKeys: string[] = [];
    const fetchSpy = spyOn(globalThis, "fetch").mockImplementation(async (_input, init) => {
      const headers = new Headers(init?.headers);
      seenKeys.push(headers.get("x-api-key") ?? "");

      return new Response((() => {
        let cancelled = false;
        return new ReadableStream({
          start(controller) {
          setTimeout(() => {
            if (cancelled) return;
            const encoder = new TextEncoder();
            for (const chunk of sseData) {
              controller.enqueue(encoder.encode(chunk));
            }
            controller.close();
          }, 50);
        },
        cancel() {
          cancelled = true;
        },
      });
      })(), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      });
    });

    try {
      const config = makeConfig("http://mocked-upstream.local", {
        firstChunkTimeoutMs: 20,
        maxFirstChunkRetries: 1,
      });
      const result = await proxyRequest(
        makeRequest("/v1/messages", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ stream: true, messages: [] }),
        }),
        km,
        config,
        st,
      );

      expect(result.kind).toBe("error");
      if (result.kind === "error") {
        expect(result.status).toBe(504);
        expect(JSON.parse(result.body)).toEqual({
          error: {
            type: "proxy_error",
            message: "Upstream stream produced no first chunk within 20ms after 2 attempt(s).",
          },
        });
      }

      expect(seenKeys).toEqual([FAKE_KEY_A, FAKE_KEY_A]);

      const [keyA, keyB] = km.listKeys();
      expect(keyA!.stats.errors).toBe(2);
      expect(keyB!.stats.errors).toBe(0);
    } finally {
      fetchSpy.mockRestore();
    }
  });

  test("detects text/event-stream content-type", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":10}}}\n\n',
      'data: {"type":"content_block_delta","delta":{"text":"Hi"}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":5}}\n\n',
      "data: [DONE]\n\n",
    ];

    const mock = upstream(() =>
      new Response(makeSSEBody(sseData), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      // Must consume the stream to trigger flush
      await result.response.text();

      const keys = km.listKeys();
      expect(keys[0]!.stats.totalTokensIn).toBe(10);
      expect(keys[0]!.stats.totalTokensOut).toBe(5);
    }
  });

  test("passes stream data through unchanged to client", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const sseChunks = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":5}}}\n\n',
      'data: {"type":"content_block_delta","delta":{"text":"Hello world"}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":3}}\n\n',
    ];

    const mock = upstream(() =>
      new Response(makeSSEBody(sseChunks), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      const body = await result.response.text();
      // All original data should be present
      expect(body).toContain("message_start");
      expect(body).toContain("content_block_delta");
      expect(body).toContain("Hello world");
      expect(body).toContain("message_delta");
    }
  });

  test("extracts input tokens from message_start event", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":500}}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":0}}\n\n',
    ];

    const mock = upstream(() =>
      new Response(makeSSEBody(sseData), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      await result.response.text();
      const keys = km.listKeys();
      expect(keys[0]!.stats.totalTokensIn).toBe(500);
    }
  });

  test("extracts output tokens from message_delta event", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":0}}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":999}}\n\n',
    ];

    const mock = upstream(() =>
      new Response(makeSSEBody(sseData), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      await result.response.text();
      const keys = km.listKeys();
      expect(keys[0]!.stats.totalTokensOut).toBe(999);
    }
  });

  test("accumulates tokens across multiple events", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":100}}}\n\n',
      'data: {"type":"content_block_delta","delta":{"text":"chunk1"}}\n\n',
      'data: {"type":"content_block_delta","delta":{"text":"chunk2"}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":50}}\n\n',
    ];

    const mock = upstream(() =>
      new Response(makeSSEBody(sseData), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      await result.response.text();
      const keys = km.listKeys();
      expect(keys[0]!.stats.totalTokensIn).toBe(100);
      expect(keys[0]!.stats.totalTokensOut).toBe(50);
      expect(keys[0]!.stats.successfulRequests).toBe(1);
    }
  });

  test("records stats on flush (stream end)", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "alice");

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":77}}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":33}}\n\n',
    ];

    const mock = upstream(() =>
      new Response(makeSSEBody(sseData), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      // Before consuming: stats should NOT yet be recorded (flush hasn't run)
      const keysBefore = km.listKeys();
      // successfulRequests is only recorded on flush
      expect(keysBefore[0]!.stats.successfulRequests).toBe(0);

      // Now consume the stream to trigger flush
      await result.response.text();

      const keysAfter = km.listKeys();
      expect(keysAfter[0]!.stats.successfulRequests).toBe(1);
      expect(keysAfter[0]!.stats.totalTokensIn).toBe(77);
      expect(keysAfter[0]!.stats.totalTokensOut).toBe(33);

      const tokens = km.listTokens();
      const alice = tokens.find((t) => t.label === "alice")!;
      expect(alice.stats.successfulRequests).toBe(1);
      expect(alice.stats.totalTokensIn).toBe(77);
      expect(alice.stats.totalTokensOut).toBe(33);
    }
  });

  test("handles invalid JSON in stream events gracefully", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":10}}}\n\n',
      "data: {this is not valid json}\n\n",
      "data: BROKEN\n\n",
      'data: {"type":"message_delta","usage":{"output_tokens":5}}\n\n',
    ];

    const mock = upstream(() =>
      new Response(makeSSEBody(sseData), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      // Should not throw, should still extract valid tokens
      const body = await result.response.text();
      expect(body).toContain("BROKEN");

      const keys = km.listKeys();
      expect(keys[0]!.stats.totalTokensIn).toBe(10);
      expect(keys[0]!.stats.totalTokensOut).toBe(5);
    }
  });
});

// ────────────────────────────────────────────────────────────────────

describe("Key Rotation", () => {
  test("tries multiple keys on successive 429s", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");
    km.addKey(FAKE_KEY_C, "key-c");

    const keysUsed: string[] = [];
    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key")!;
      keysUsed.push(key);
      if (key === FAKE_KEY_A || key === FAKE_KEY_B) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "60" },
        });
      }
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    expect(keysUsed.length).toBe(3);
    // All three keys should have been tried
    expect(new Set(keysUsed).size).toBe(3);
  });

  test("stops when a key succeeds", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");
    km.addKey(FAKE_KEY_C, "key-c");

    let callCount = 0;
    const mock = upstream((req) => {
      callCount++;
      const key = req.headers.get("x-api-key")!;
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "60" },
        });
      }
      // key-b succeeds
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    // Should stop at 2 (key-a 429, key-b success), not try key-c
    expect(callCount).toBe(2);
  });

  test("stops at max retries", async () => {
    const { km, st } = setup();
    // Add many keys but set low max retries
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");
    km.addKey(FAKE_KEY_C, "key-c");

    let callCount = 0;
    const mock = upstream(() => {
      callCount++;
      return new Response("rate limited", {
        status: 429,
        headers: { "retry-after": "60" },
      });
    });

    const config = makeConfig(mock.url, { maxRetriesPerRequest: 2 });
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    // Should stop after 2 attempts
    expect(callCount).toBe(2);
    expect(result.kind).toBe("all_exhausted");
  });

  test("does not retry same key twice", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    let callCount = 0;
    const mock = upstream(() => {
      callCount++;
      return new Response("rate limited", {
        status: 429,
        headers: { "retry-after": "60" },
      });
    });

    const config = makeConfig(mock.url, { maxRetriesPerRequest: 10 });
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    // With only 1 key, it should be tried exactly once, then all_exhausted
    expect(callCount).toBe(1);
    expect(result.kind).toBe("all_exhausted");
  });

  test("returns all_exhausted after all keys tried", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const mock = upstream(() =>
      new Response("rate limited", {
        status: 429,
        headers: { "retry-after": "60" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("all_exhausted");
    if (result.kind === "all_exhausted") {
      // earliestAvailableAt should be in the future
      expect(result.earliestAvailableAt).toBeGreaterThan(Date.now());
    }
  });
});

// ────────────────────────────────────────────────────────────────────

describe("Proxy User Attribution", () => {
  test("records token request once per top-level request (not per retry)", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");
    const proxyUser = km.addToken("test-token-12345678", "alice");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "30" },
        });
      }
      return new Response(
        JSON.stringify({ usage: { input_tokens: 10, output_tokens: 5 } }),
        { headers: { "content-type": "application/json" } },
      );
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser);

    const tokens = km.listTokens();
    const alice = tokens.find((t) => t.label === "alice")!;
    // recordTokenRequest is called once at the top, not per retry
    expect(alice.stats.totalRequests).toBe(1);
  });

  test("records token success with correct token counts", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "bob");

    const mock = upstream(() =>
      new Response(
        JSON.stringify({ usage: { input_tokens: 500, output_tokens: 1000 } }),
        { headers: { "content-type": "application/json" } },
      ),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser);

    const tokens = km.listTokens();
    const bob = tokens.find((t) => t.label === "bob")!;
    expect(bob.stats.successfulRequests).toBe(1);
    expect(bob.stats.totalTokensIn).toBe(500);
    expect(bob.stats.totalTokensOut).toBe(1000);
  });

  test("records token error on failure", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "charlie");

    const mock = upstream(() =>
      new Response("forbidden", { status: 403 }),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser);

    const tokens = km.listTokens();
    const charlie = tokens.find((t) => t.label === "charlie")!;
    expect(charlie.stats.errors).toBe(1);
    expect(charlie.stats.totalRequests).toBe(1);
  });

  test("records token error on all-keys-exhausted", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "diana");

    const mock = upstream(() =>
      new Response("rate limited", {
        status: 429,
        headers: { "retry-after": "60" },
      }),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser);

    const tokens = km.listTokens();
    const diana = tokens.find((t) => t.label === "diana")!;
    expect(diana.stats.errors).toBe(1);
  });

  test("records token error on upstream connection failure", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "eve");

    const config = makeConfig("http://localhost:1");
    await proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser);

    const tokens = km.listTokens();
    const eve = tokens.find((t) => t.label === "eve")!;
    expect(eve.stats.errors).toBe(1);
  });

  test("includes user label in all emitted events", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "frank");

    const mock = upstream(() =>
      new Response(
        JSON.stringify({ usage: { input_tokens: 10, output_tokens: 5 } }),
        { headers: { "content-type": "application/json" } },
      ),
    );

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser),
    );

    // All events should have user set
    for (const event of events) {
      // "keys" and "schema_change" type events may not have user, so skip those
      if (event.type !== "keys" && event.type !== "schema_change") {
        expect(event.user).toBe("frank");
      }
    }
  });
});

// ────────────────────────────────────────────────────────────────────

describe("Event Emission", () => {
  test("emits request event with method, path, attempt, user", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "alice");

    const mock = upstream(() => new Response("ok"));

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(
        makeRequest("/v1/messages", { method: "POST" }),
        km,
        config,
        st,
        proxyUser,
      ),
    );

    const reqEvents = events.filter((e) => e.type === "request");
    expect(reqEvents.length).toBe(1);
    expect(reqEvents[0]!.method).toBe("POST");
    expect(reqEvents[0]!.path).toBe("/v1/messages");
    expect(reqEvents[0]!.attempt).toBe(1);
    expect(reqEvents[0]!.user).toBe("alice");
    expect(reqEvents[0]!.ts).toBeDefined();
  });

  test("emits response event with status and user", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "bob");

    const mock = upstream(() =>
      new Response("ok", { status: 200 }),
    );

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser),
    );

    const resEvents = events.filter((e) => e.type === "response");
    expect(resEvents.length).toBe(1);
    expect(resEvents[0]!.status).toBe(200);
    expect(resEvents[0]!.user).toBe("bob");
  });

  test("emits error event with details and user", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "charlie");

    // Use an unreachable port to trigger a connection error
    const config = makeConfig("http://localhost:1");
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser),
    );

    const errEvents = events.filter((e) => e.type === "error");
    expect(errEvents.length).toBe(1);
    expect(errEvents[0]!.user).toBe("charlie");
    expect(errEvents[0]!.error).toBeDefined();
  });

  test("emits error event for non-429 upstream errors", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "diana");

    const mock = upstream(() =>
      new Response("bad request", { status: 400 }),
    );

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser),
    );

    const errEvents = events.filter((e) => e.type === "error");
    expect(errEvents.length).toBe(1);
    expect(errEvents[0]!.status).toBe(400);
    expect(errEvents[0]!.user).toBe("diana");
  });

  test("emits rate_limit event with retryAfter and user", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");
    const proxyUser = km.addToken("test-token-12345678", "eve");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "90" },
        });
      }
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser),
    );

    const rlEvents = events.filter((e) => e.type === "rate_limit");
    expect(rlEvents.length).toBe(1);
    expect(rlEvents[0]!.retryAfter).toBe(90);
    expect(rlEvents[0]!.user).toBe("eve");
    expect(rlEvents[0]!.availableKeys).toBeDefined();
  });

  test("emits tokens event with input/output counts and user", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "frank");

    const mock = upstream(() =>
      new Response(
        JSON.stringify({ usage: { input_tokens: 42, output_tokens: 88 } }),
        { headers: { "content-type": "application/json" } },
      ),
    );

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser),
    );

    const tokenEvents = events.filter((e) => e.type === "tokens");
    expect(tokenEvents.length).toBe(1);
    expect(tokenEvents[0]!.input).toBe(42);
    expect(tokenEvents[0]!.output).toBe(88);
    expect(tokenEvents[0]!.user).toBe("frank");
    expect(tokenEvents[0]!.label).toBe("key-a");
  });

  test("emits request events for each retry attempt", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");
    km.addKey(FAKE_KEY_C, "key-c");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A || key === FAKE_KEY_B) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "30" },
        });
      }
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st),
    );

    const reqEvents = events.filter((e) => e.type === "request");
    expect(reqEvents.length).toBe(3);
    expect(reqEvents[0]!.attempt).toBe(1);
    expect(reqEvents[1]!.attempt).toBe(2);
    expect(reqEvents[2]!.attempt).toBe(3);
  });

  test("emits response event for each upstream response including 429s", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "30" },
        });
      }
      return new Response("ok", { status: 200 });
    });

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st),
    );

    const resEvents = events.filter((e) => e.type === "response");
    expect(resEvents.length).toBe(2);
    expect(resEvents[0]!.status).toBe(429);
    expect(resEvents[1]!.status).toBe(200);
  });

  test("does not emit tokens event when usage is zero", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(JSON.stringify({ id: "msg_123" }), {
        headers: { "content-type": "application/json" },
      }),
    );

    const config = makeConfig(mock.url);
    const [, events] = await collectEvents(() =>
      proxyRequest(makeRequest("/v1/messages"), km, config, st),
    );

    const tokenEvents = events.filter((e) => e.type === "tokens");
    expect(tokenEvents.length).toBe(0);
  });
});

// ────────────────────────────────────────────────────────────────────

describe("Edge Cases", () => {
  test("handles null upstream body", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(null, { status: 204 }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      expect(result.response.status).toBe(204);
      const keys = km.listKeys();
      expect(keys[0]!.stats.successfulRequests).toBe(1);
      expect(keys[0]!.stats.totalTokensIn).toBe(0);
      expect(keys[0]!.stats.totalTokensOut).toBe(0);
    }
  });

  test("handles non-JSON response body gracefully", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response("plain text response", {
        status: 200,
        headers: { "content-type": "text/plain" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      const body = await result.response.text();
      expect(body).toBe("plain text response");
      // Should default to 0 tokens (JSON parse fails gracefully)
      const keys = km.listKeys();
      expect(keys[0]!.stats.totalTokensIn).toBe(0);
      expect(keys[0]!.stats.totalTokensOut).toBe(0);
      expect(keys[0]!.stats.successfulRequests).toBe(1);
    }
  });

  test("handles SSE stream with [DONE] marker", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":25}}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":15}}\n\n',
      "data: [DONE]\n\n",
    ];

    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      start(controller) {
        for (const chunk of sseData) {
          controller.enqueue(encoder.encode(chunk));
        }
        controller.close();
      },
    });

    const mock = upstream(() =>
      new Response(stream, {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      await result.response.text();
      const keys = km.listKeys();
      expect(keys[0]!.stats.totalTokensIn).toBe(25);
      expect(keys[0]!.stats.totalTokensOut).toBe(15);
    }
  });

  test("maxRetriesPerRequest of 1 means only one attempt", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    let callCount = 0;
    const mock = upstream(() => {
      callCount++;
      return new Response("rate limited", {
        status: 429,
        headers: { "retry-after": "10" },
      });
    });

    const config = makeConfig(mock.url, { maxRetriesPerRequest: 1 });
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(callCount).toBe(1);
    expect(result.kind).toBe("all_exhausted");
  });

  test("success result includes usedKey reference", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() => new Response("ok"));

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      expect(result.usedKey.key).toBe(FAKE_KEY_A);
      expect(result.usedKey.label).toBe("key-a");
    }
  });

  test("error result includes usedKey reference", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response("bad", { status: 400 }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    expect(result.kind).toBe("error");
    if (result.kind === "error") {
      expect(result.usedKey.key).toBe(FAKE_KEY_A);
      expect(result.usedKey.label).toBe("key-a");
    }
  });

  test("streaming tokens event emitted with user label on flush", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    const proxyUser = km.addToken("test-token-12345678", "alice");

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":50}}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":25}}\n\n',
    ];
    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      start(controller) {
        for (const chunk of sseData) {
          controller.enqueue(encoder.encode(chunk));
        }
        controller.close();
      },
    });

    const mock = upstream(() =>
      new Response(stream, {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);

    // We need to collect events including those during stream consumption
    const events: ProxyEvent[] = [];
    const unsub = subscribe((e) => events.push(e));

    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st, proxyUser);
    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      await result.response.text();
    }

    unsub();

    const tokenEvents = events.filter((e) => e.type === "tokens");
    expect(tokenEvents.length).toBe(1);
    expect(tokenEvents[0]!.input).toBe(50);
    expect(tokenEvents[0]!.output).toBe(25);
    expect(tokenEvents[0]!.user).toBe("alice");
  });

  test("streaming tokens event carries sessionId and conversationHash so the dashboard can route the throughput chart", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const sseData = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":5}}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":7}}\n\n',
    ];
    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      start(controller) {
        for (const chunk of sseData) {
          controller.enqueue(encoder.encode(chunk));
        }
        controller.close();
      },
    });

    const mock = upstream(() =>
      new Response(stream, {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);
    const events: ProxyEvent[] = [];
    const unsub = subscribe((e) => events.push(e));

    const result = await proxyRequest(
      makeRequest("/v1/messages", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-claude-code-session-id": "sess-stream-1",
        },
        body: JSON.stringify({
          stream: true,
          messages: [{ role: "user", content: "stream test" }],
        }),
      }),
      km,
      config,
      st,
    );
    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      await result.response.text();
    }
    unsub();

    const tokenEvents = events.filter((e) => e.type === "tokens");
    expect(tokenEvents.length).toBeGreaterThan(0);
    const firstHash = tokenEvents[0]!.conversationHash;
    expect(firstHash).toMatch(/^[0-9a-f]{16}$/);
    for (const ev of tokenEvents) {
      expect(ev.sessionId).toBe("sess-stream-1");
      // Every delta must carry the same conversationHash so the dashboard
      // routes them all to one chart.
      expect(ev.conversationHash).toBe(firstHash);
    }
    // Deltas across all emitted token events must equal the cumulative
    // counts the upstream advertised — i.e. exactly one bar's worth of
    // input + output reaches the dashboard, no double counting.
    const totalInput = tokenEvents.reduce((s, ev) => s + ((ev.input as number) || 0), 0);
    const totalOutput = tokenEvents.reduce((s, ev) => s + ((ev.output as number) || 0), 0);
    expect(totalInput).toBe(5);
    expect(totalOutput).toBe(7);
  });

  test("properly increments totalRequests on key for each attempt", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_A) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "60" },
        });
      }
      return new Response("ok");
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const keys = km.listKeys();
    const keyA = keys.find((k) => k.label === "key-a")!;
    const keyB = keys.find((k) => k.label === "key-b")!;

    expect(keyA.stats.totalRequests).toBe(1);
    expect(keyA.stats.rateLimitHits).toBe(1);
    expect(keyB.stats.totalRequests).toBe(1);
    expect(keyB.stats.successfulRequests).toBe(1);
  });
});

// ────────────────────────────────────────────────────────────────────

describe("Schema Tracking Integration", () => {
  test("headers are tracked on each proxied response", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: {
          "content-type": "application/json",
          "x-request-id": "req-123",
        },
      }),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const headers = st.listHeaders();
    const headerNames = headers.map((h) => h.name);
    expect(headerNames).toContain("content-type");
    expect(headerNames).toContain("x-request-id");
  });

  test("non-streaming response body fields are tracked", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const responseBody = {
      id: "msg_abc123",
      type: "message",
      role: "assistant",
      content: [{ type: "text", text: "Hello" }],
      model: "claude-sonnet-4-20250514",
      stop_reason: "end_turn",
      usage: { input_tokens: 25, output_tokens: 10 },
    };

    const mock = upstream(() =>
      new Response(JSON.stringify(responseBody), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    expect(result.kind).toBe("success");

    const fields = st.listFields();
    const paths = fields.map((f) => f.path);
    expect(paths).toContain("id");
    expect(paths).toContain("type");
    expect(paths).toContain("model");
    expect(paths).toContain("stop_reason");
    expect(paths).toContain("usage.input_tokens");
    expect(paths).toContain("usage.output_tokens");
    expect(paths).toContain("content[].type");
    expect(paths).toContain("content[].text");

    // Verify endpoint is set correctly
    const idField = fields.find((f) => f.path === "id");
    expect(idField!.endpoint).toBe("/v1/messages");
    expect(idField!.context).toBe("response");
  });

  test("streaming SSE response body fields are tracked", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const encoder = new TextEncoder();
    function makeSSE(events: string[]): ReadableStream<Uint8Array> {
      return new ReadableStream({
        start(controller) {
          for (const e of events) controller.enqueue(encoder.encode(e));
          controller.close();
        },
      });
    }

    const sseData = [
      'data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":10}}}\n\n',
      'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":5}}\n\n',
      "data: [DONE]\n\n",
    ];

    const mock = upstream(() =>
      new Response(makeSSE(sseData), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    expect(result.kind).toBe("success");
    if (result.kind === "success") {
      // Must consume the stream to trigger schema tracking
      await result.response.text();
    }

    const fields = st.listFields();
    const contexts = [...new Set(fields.map((f) => f.context))];
    // Should have fields from message_start, content_block_delta, message_delta
    expect(contexts).toContain("message_start");
    expect(contexts).toContain("content_block_delta");
    expect(contexts).toContain("message_delta");

    // Check specific fields from message_start
    const messageStartFields = fields.filter((f) => f.context === "message_start");
    const msPaths = messageStartFields.map((f) => f.path);
    expect(msPaths).toContain("type");
    expect(msPaths).toContain("message.id");
    expect(msPaths).toContain("message.usage.input_tokens");
  });

  test("headers are tracked on error responses (4xx/5xx)", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(JSON.stringify({ type: "error", error: { type: "overloaded_error" } }), {
        status: 529,
        headers: {
          "content-type": "application/json",
          "x-error-id": "err-456",
        },
      }),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const headers = st.listHeaders();
    const headerNames = headers.map((h) => h.name);
    expect(headerNames).toContain("x-error-id");
    expect(headerNames).toContain("content-type");
  });

  test("headers are tracked on 429 rate-limit responses", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    let callCount = 0;
    const mock = upstream(() => {
      callCount++;
      if (callCount === 1) {
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "30", "x-ratelimit-header": "seen" },
        });
      }
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const headers = st.listHeaders();
    const headerNames = headers.map((h) => h.name);
    // Headers from the 429 response should be tracked
    expect(headerNames).toContain("x-ratelimit-header");
    expect(headerNames).toContain("retry-after");
  });

  test("body fields are NOT tracked on error responses", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(JSON.stringify({ type: "error", error: { type: "overloaded_error", message: "Server overloaded" } }), {
        status: 529,
        headers: { "content-type": "application/json" },
      }),
    );

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    // Body fields from error responses should NOT be tracked
    const fields = st.listFields();
    expect(fields).toHaveLength(0);
  });
});

describe("Capacity observation integration", () => {
  test("successful-response capacity signals stay analytics-only and do not cool the key", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(JSON.stringify({ ok: true, usage: { input_tokens: 2, output_tokens: 1 } }), {
        status: 200,
        headers: {
          "content-type": "application/json",
          "request-id": "req_success_analytics_1",
          "anthropic-ratelimit-unified-status": "rejected",
          "anthropic-ratelimit-unified-reset": futureEpochSeconds(4 * 60 * 60),
          "anthropic-ratelimit-unified-overage-status": "rejected",
          "anthropic-ratelimit-unified-overage-disabled-reason": "out_of_credits",
        },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    expect(result.kind).toBe("success");

    const key = km.listKeys()[0]!;
    expect(key.isAvailable).toBe(true);
    expect(key.availableAt).toBeLessThanOrEqual(Date.now());
    expect(key.capacityHealth).not.toBe("cooling_down");
    expect(key.capacity.lastRequestId).toBe("req_success_analytics_1");
    expect(key.capacity.overageStatus).toBe("rejected");
    expect(key.capacity.overageDisabledReason).toBe("out_of_credits");
    expect(key.capacity.retryAfterSecs).toBeNull();
    expect(key.capacity.responseCount).toBe(1);
    expect(key.capacity.normalizedHeaderCount).toBe(1);
    expect(key.capacity.windows.find((w) => w.windowName === "unified")!.status).toBe("rejected");
  });

  test("normalizes supported Claude Code analytics headers and derives warning windows from threshold signals", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(JSON.stringify({ ok: true, usage: { input_tokens: 2, output_tokens: 1 } }), {
        status: 200,
        headers: {
          "content-type": "application/json",
          "request-id": "req_capacity_1",
          "anthropic-organization-id": "org-abc",
          "anthropic-ratelimit-unified-representative-claim": "seven_day",
          "anthropic-ratelimit-unified-status": "allowed",
          "anthropic-ratelimit-unified-reset": futureEpochSeconds(4 * 60 * 60),
          // Raw per-window status headers are not part of the supported Claude Code semantics.
          "anthropic-ratelimit-unified-7d-status": "rejected",
          "anthropic-ratelimit-unified-7d-utilization": "0.92",
          "anthropic-ratelimit-unified-7d-reset": futureEpochSeconds(3 * 24 * 60 * 60),
          "anthropic-ratelimit-unified-7d-surpassed-threshold": "0.75",
          "anthropic-ratelimit-unified-fallback": "available",
          "x-should-retry": "false",
          "x-envoy-upstream-service-time": "1234",
        },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    expect(result.kind).toBe("success");

    const key = km.listKeys()[0]!;
    expect(key.capacity.organizationId).toBe("org-abc");
    expect(key.capacity.lastRequestId).toBe("req_capacity_1");
    expect(key.capacity.representativeClaim).toBe("seven_day");
    expect(key.capacity.fallbackAvailable).toBe(true);
    expect(key.capacity.shouldRetry).toBe(false);
    expect(key.capacity.latencyMs).toBe(1234);
    expect(key.capacity.responseCount).toBe(1);
    expect(key.capacity.normalizedHeaderCount).toBe(1);
    expect(key.capacity.signalCoverage.find((signal) => signal.signalName === "request_id")!.seenCount).toBe(1);
    expect(key.capacity.signalCoverage.find((signal) => signal.signalName === "windows")!.seenCount).toBe(1);
    expect(key.isAvailable).toBe(true);
    expect(key.capacity.windows.find((w) => w.windowName === "unified")!.status).toBe("allowed_warning");
    expect(key.capacity.windows.find((w) => w.windowName === "unified-7d")!.utilization).toBe(0.92);
    expect(key.capacity.windows.find((w) => w.windowName === "unified-7d")!.surpassedThreshold).toBe(0.75);
    expect(key.capacity.windows.find((w) => w.windowName === "unified-7d")!.status).toBe("allowed_warning");
    expect(key.capacity.windows.find((w) => w.windowName === "unified-overage")).toBeUndefined();
    expect(key.capacityHealth).toBe("warning");
  });

  test("threshold headers stay informational until utilization actually reaches them", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(JSON.stringify({ ok: true, usage: { input_tokens: 2, output_tokens: 1 } }), {
        status: 200,
        headers: {
          "content-type": "application/json",
          "request-id": "req_capacity_threshold_only",
          "anthropic-organization-id": "org-threshold",
          "anthropic-ratelimit-unified-representative-claim": "five_hour",
          "anthropic-ratelimit-unified-status": "allowed",
          "anthropic-ratelimit-unified-reset": futureEpochSeconds(26 * 60_000),
          "anthropic-ratelimit-unified-5h-utilization": "0.14",
          "anthropic-ratelimit-unified-5h-reset": futureEpochSeconds(26 * 60_000),
          "anthropic-ratelimit-unified-5h-surpassed-threshold": "0.9",
          "anthropic-ratelimit-unified-7d-utilization": "0.25",
          "anthropic-ratelimit-unified-7d-reset": futureEpochSeconds(6 * 24 * 60 * 60_000 + 7 * 60 * 60_000),
          "anthropic-ratelimit-unified-7d-surpassed-threshold": "0.75",
        },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    expect(result.kind).toBe("success");

    const key = km.listKeys()[0]!;
    expect(key.capacity.windows.find((w) => w.windowName === "unified")!.status).toBe("allowed");
    expect(key.capacity.windows.find((w) => w.windowName === "unified-5h")!.status).toBe("allowed");
    expect(key.capacity.windows.find((w) => w.windowName === "unified-5h")!.surpassedThreshold).toBe(0.9);
    expect(key.capacity.windows.find((w) => w.windowName === "unified-7d")!.status).toBe("allowed");
    expect(key.capacity.windows.find((w) => w.windowName === "unified-7d")!.utilization).toBe(0.25);
    expect(key.capacity.windows.find((w) => w.windowName === "unified-7d")!.surpassedThreshold).toBe(0.75);
    expect(key.capacityHealth).toBe("healthy");
  });

  test("partial capacity headers merge while ignoring raw per-window status headers", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    let callCount = 0;
    const mock = upstream(() => {
      callCount++;
      if (callCount === 1) {
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: {
            "content-type": "application/json",
            "anthropic-organization-id": "org-first",
            "anthropic-ratelimit-unified-7d-status": "rejected",
            "anthropic-ratelimit-unified-7d-utilization": "0.1",
            "anthropic-ratelimit-unified-7d-reset": futureEpochSeconds(6 * 24 * 60 * 60),
          },
        });
      }
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: {
          "content-type": "application/json",
          "request-id": "req-second",
          "anthropic-ratelimit-unified-status": "allowed",
          "anthropic-ratelimit-unified-reset": futureEpochSeconds(60 * 60),
          "anthropic-ratelimit-unified-5h-status": "rejected",
          "anthropic-ratelimit-unified-5h-utilization": "0.32",
          "anthropic-ratelimit-unified-5h-reset": futureEpochSeconds(4 * 60 * 60),
        },
      });
    });

    const config = makeConfig(mock.url);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    await proxyRequest(makeRequest("/v1/messages"), km, config, st);

    const key = km.listKeys()[0]!;
    expect(key.capacity.organizationId).toBe("org-first");
    expect(key.capacity.lastRequestId).toBe("req-second");
    expect(key.capacity.responseCount).toBe(2);
    expect(key.capacity.normalizedHeaderCount).toBe(2);
    expect(key.isAvailable).toBe(true);
    expect(key.capacity.windows.map((w) => w.windowName).sort()).toEqual(["unified", "unified-5h", "unified-7d"]);
    expect(key.capacity.windows.find((w) => w.windowName === "unified")!.status).toBe("allowed");
    expect(key.capacity.windows.find((w) => w.windowName === "unified-5h")!.status).not.toBe("rejected");
    expect(key.capacity.windows.find((w) => w.windowName === "unified-7d")!.status).not.toBe("rejected");
    expect(key.capacityHealth).toBe("healthy");
  });

  test("responses without normalized headers still update response counts without pretending headers were seen", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() =>
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    expect(result.kind).toBe("success");

    const key = km.listKeys()[0]!;
    expect(key.capacity.responseCount).toBe(1);
    expect(key.capacity.normalizedHeaderCount).toBe(0);
    expect(key.capacity.lastResponseAt).not.toBeNull();
    expect(key.capacity.lastHeaderAt).toBeNull();
    expect(key.capacity.signalCoverage).toEqual([]);
  });

  test("429 responses update capacity state before rotating away", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    let callCount = 0;
    const mock = upstream(() => {
      callCount++;
      if (callCount === 1) {
        return new Response("rate limited", {
          status: 429,
          headers: {
            "retry-after": "30",
            "request-id": "req_capacity_429",
            "anthropic-ratelimit-unified-status": "rejected",
            "anthropic-ratelimit-unified-reset": futureEpochSeconds(30),
            "anthropic-ratelimit-unified-5h-status": "rejected",
            "anthropic-ratelimit-unified-5h-utilization": "0.95",
            "anthropic-ratelimit-unified-5h-reset": futureEpochSeconds(30),
            "anthropic-ratelimit-unified-5h-surpassed-threshold": "0.9",
            "anthropic-ratelimit-unified-overage-status": "rejected",
            "anthropic-ratelimit-unified-overage-disabled-reason": "out_of_credits",
          },
        });
      }
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const config = makeConfig(mock.url);
    const result = await proxyRequest(makeRequest("/v1/messages"), km, config, st);
    expect(result.kind).toBe("success");

    const keyA = km.listKeys().find((k) => k.label === "key-a")!;
    expect(keyA.capacity.retryAfterSecs).toBe(30);
    expect(keyA.capacity.lastRequestId).toBe("req_capacity_429");
    expect(keyA.capacity.overageStatus).toBe("rejected");
    expect(keyA.capacity.overageDisabledReason).toBe("out_of_credits");
    expect(keyA.capacity.responseCount).toBe(1);
    expect(keyA.capacity.normalizedHeaderCount).toBe(1);
    expect(keyA.capacity.windows.find((w) => w.windowName === "unified")!.status).toBe("rejected");
    expect(keyA.capacity.windows.find((w) => w.windowName === "unified-5h")!.status).toBe("allowed_warning");
    expect(keyA.capacity.windows.find((w) => w.windowName === "unified-5h")!.surpassedThreshold).toBe(0.9);
    expect(keyA.capacity.windows.find((w) => w.windowName === "unified-overage")).toBeUndefined();
    expect(keyA.isAvailable).toBe(false);
    expect(keyA.capacityHealth).toBe("cooling_down");
  });
});

describe("Day-restricted keys", () => {
  test("Claude Code sessions stay sticky and the bucket-of-3 rotation distributes new sessions across keys", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const seen: Array<{ session: string | null; key: string | null }> = [];
    const mock = upstream((req) => {
      seen.push({
        session: req.headers.get("x-claude-code-session-id"),
        key: req.headers.get("x-api-key"),
      });
      return new Response(JSON.stringify({
        usage: { input_tokens: 1, output_tokens: 1 },
      }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const config = makeConfig(mock.url);

    const turnsBySession = new Map<string, Array<{ role: string; content: string }>>();
    async function send(session: string, content: string): Promise<void> {
      const turns = turnsBySession.get(session) ?? [];
      turns.push({ role: "user", content });
      turnsBySession.set(session, turns);
      const res = await proxyRequest(makeRequest("/v1/messages", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-claude-code-session-id": session,
        },
        body: JSON.stringify({ messages: turns }),
      }), km, config, st);
      expect(res.kind).toBe("success");
      turns.push({ role: "assistant", content: "ok" });
    }

    // First three new sessions land on key-a (bucket fills 3 before rolling),
    // the fourth new session rolls to key-b.
    await send("session-a", "a1");
    await send("session-b", "b1");
    await send("session-c", "c1");
    await send("session-d", "d1");
    // Each session is sticky on follow-ups regardless of routing rules.
    await send("session-a", "a2");
    await send("session-d", "d2");

    expect(seen).toEqual([
      { session: "session-a", key: FAKE_KEY_A },
      { session: "session-b", key: FAKE_KEY_A },
      { session: "session-c", key: FAKE_KEY_A },
      { session: "session-d", key: FAKE_KEY_B },
      { session: "session-a", key: FAKE_KEY_A },
      { session: "session-d", key: FAKE_KEY_B },
    ]);
  });

  test("sub-agents under one session-id route to different keys when they have different messages[0]", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const seen: Array<{ key: string | null; firstContent: string }> = [];
    const mock = upstream(async (req) => {
      const body = await req.json() as { messages: Array<{ content: string }> };
      seen.push({
        key: req.headers.get("x-api-key"),
        firstContent: body.messages[0].content,
      });
      return new Response(JSON.stringify({ usage: { input_tokens: 1, output_tokens: 1 } }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const config = makeConfig(mock.url);
    const session = "shared-session";
    async function send(firstContent: string): Promise<void> {
      const res = await proxyRequest(makeRequest("/v1/messages", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-claude-code-session-id": session,
        },
        body: JSON.stringify({ messages: [{ role: "user", content: firstContent }] }),
      }), km, config, st);
      expect(res.kind).toBe("success");
    }

    // Parent and sub-agents share session-id but have different first messages.
    // The bucket-of-3 rotation fills key-a with the first three new conversations,
    // then rolls to key-b for the fourth — proving the conversations are routed
    // independently rather than all pinning to the parent's key.
    await send("parent first prompt");
    await send("sub-agent A first prompt");
    await send("sub-agent B first prompt");
    await send("sub-agent C first prompt");
    // Each conversation is sticky on its own key for follow-ups.
    await send("parent first prompt");
    await send("sub-agent C first prompt");

    const keyByContent = new Map<string, Set<string>>();
    for (const s of seen) {
      const ks = keyByContent.get(s.firstContent) ?? new Set();
      ks.add(s.key ?? "");
      keyByContent.set(s.firstContent, ks);
    }
    // Each logical conversation pinned to exactly one key.
    for (const [, ks] of keyByContent) expect(ks.size).toBe(1);
    // The conversations did not all collapse onto a single key.
    const allKeys = new Set(seen.map((s) => s.key));
    expect(allKeys.size).toBeGreaterThan(1);
  });

  test("a session remaps after 429 and stays on the new key", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const seen: string[] = [];
    let sessionAAttempts = 0;
    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      seen.push(key ?? "");
      if (key === FAKE_KEY_A) {
        sessionAAttempts++;
        if (sessionAAttempts === 1) {
          // Long cooldown (> 5-min threshold) triggers the full remap path.
          // Short cooldowns would make the proxy wait server-side instead.
          return new Response("rate limited", {
            status: 429,
            headers: { "retry-after": "1800" },
          });
        }
      }

      return new Response(JSON.stringify({
        usage: {
          input_tokens: 1,
          output_tokens: 1,
        },
      }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const config = makeConfig(mock.url);

    const turn1: Array<{ role: string; content: string }> = [
      { role: "user", content: "first" },
    ];
    const first = await proxyRequest(makeRequest("/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-claude-code-session-id": "session-a",
      },
      body: JSON.stringify({ messages: turn1 }),
    }), km, config, st);
    expect(first.kind).toBe("success");

    const turn2 = [
      ...turn1,
      { role: "assistant", content: "ok" },
      { role: "user", content: "follow-up" },
    ];
    const second = await proxyRequest(makeRequest("/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-claude-code-session-id": "session-a",
      },
      body: JSON.stringify({ messages: turn2 }),
    }), km, config, st);
    expect(second.kind).toBe("success");

    expect(seen).toEqual([FAKE_KEY_A, FAKE_KEY_B, FAKE_KEY_B]);
  });

  test("returns all_exhausted with future earliestAvailableAt when all keys are day-restricted", async () => {
    const { km, st, cleanup } = createTestSetup();
    const mock = startMockUpstream(() => new Response("OK"));
    try {
      km.addKey(FAKE_KEY_A, "restricted");

      // Restrict to a day that is NOT today
      const today = new Date().getDay();
      const notToday = (today + 1) % 7;
      km.updateKeyAllowedDays(FAKE_KEY_A, [notToday]);

      const config = makeConfig(mock.url);
      const req = new Request(`${mock.url}/v1/messages`, { method: "POST", body: "{}" });
      const result = await proxyRequest(req, km, config, st);

      expect(result.kind).toBe("all_exhausted");
      if (result.kind === "all_exhausted") {
        // earliestAvailableAt should be midnight (in the future)
        expect(result.earliestAvailableAt).toBeGreaterThan(Date.now());
        // And at most ~24 hours from now
        expect(result.earliestAvailableAt - Date.now()).toBeLessThanOrEqual(24 * 60 * 60 * 1000);
      }
    } finally {
      mock.stop();
      cleanup();
    }
  });
});

describe("Affinity cooldown server-side wait", () => {
  test("short cooldown on pinned key → proxy waits server-side and retries on same key", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    let firstKeyLabel: string | null = null;
    let fakeKeyBCalled = false;
    let calls = 0;
    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_B) fakeKeyBCalled = true;
      calls++;
      if (calls === 1) {
        firstKeyLabel = key;
        // Short cooldown — 1 second so the test runs fast but still exercises the wait.
        return new Response("rate limited", {
          status: 429,
          headers: { "retry-after": "1" },
        });
      }
      // Any subsequent upstream call gets a success.
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const config = makeConfig(mock.url);
    const req = makeRequest("/v1/messages", {
      method: "POST",
      headers: { "x-claude-code-session-id": "sess-wait" },
      body: JSON.stringify({ model: "m", messages: [] }),
    });

    const started = Date.now();
    const result = await proxyRequest(req, km, config, st);
    const elapsedMs = Date.now() - started;

    expect(result.kind).toBe("success");
    expect(elapsedMs).toBeGreaterThanOrEqual(900); // waited ≥ ~1s
    expect(fakeKeyBCalled).toBe(false);
    // The retry landed on the same key as the original 429, not the other one.
    if (result.kind === "success") {
      expect(result.usedKey.key).toBe(firstKeyLabel);
    }
    // Upstream saw two attempts — initial 429, then the post-wait retry.
    expect(calls).toBe(2);
  });

  test("long cooldown (>5 min) on pinned key → remaps to other key as before", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    let fakeKeyBCalled = false;
    const mock = upstream((req) => {
      const key = req.headers.get("x-api-key");
      if (key === FAKE_KEY_B) {
        fakeKeyBCalled = true;
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      // Pinned key returns 429 with a 30-minute cooldown — past the 5-min threshold.
      return new Response("rate limited", {
        status: 429,
        headers: { "retry-after": "1800" },
      });
    });

    const config = makeConfig(mock.url);
    const req = makeRequest("/v1/messages", {
      method: "POST",
      headers: { "x-claude-code-session-id": "sess-long" },
      body: JSON.stringify({ model: "m", messages: [] }),
    });
    const result = await proxyRequest(req, km, config, st);

    expect(result.kind).toBe("success");
    expect(fakeKeyBCalled).toBe(true);
  });
});

describe("Synthetic claude-cli helper sessions", () => {
  // claude-cli stamps an all-zero-prefix UUID for one-shot helper calls
  // (e.g. generate_session_title → Haiku). They have no prompt cache to
  // preserve and shouldn't pin to a key, show in the dashboard sessions
  // table, or count toward round-robin assignment.
  const SYNTHETIC_SESSION = "00000000-0000-4000-8000-a51e390d86a8";

  test("synthetic session bypasses conversation pinning and dashboard sessions", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const mock = upstream(() => new Response(
      JSON.stringify({ usage: { input_tokens: 1, output_tokens: 1 } }),
      { status: 200, headers: { "content-type": "application/json" } },
    ));
    const config = makeConfig(mock.url);

    async function send(session: string, content: string): Promise<void> {
      const res = await proxyRequest(makeRequest("/v1/messages", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-claude-code-session-id": session,
        },
        body: JSON.stringify({ messages: [{ role: "user", content }] }),
      }), km, config, st);
      expect(res.kind).toBe("success");
    }

    await send(SYNTHETIC_SESSION, "title gen 1");
    await send(SYNTHETIC_SESSION, "title gen 2");
    await send(SYNTHETIC_SESSION, "title gen 3");

    // Routed via the no-conversation fallback, never through affinity logic.
    const decisions = km.getRecentRoutingDecisions();
    expect(decisions.length).toBe(3);
    for (const d of decisions) {
      expect(d.routingDecision).toBe("global_sticky_fallback");
      expect(d.conversationKey).toBe(null);
      expect(d.sessionId).toBe(null);
    }

    // No affinity entries created → recentSessions stays empty across all keys.
    for (const k of km.listKeys()) {
      expect(k.recentSessions).toEqual([]);
    }

    // A real session afterwards is unaffected — pins fresh, gets counted.
    await send("real-session", "real first turn");
    const real = km.getRecentRoutingDecisions()[0]!;
    expect(real.routingDecision).toBe("conversation_new_assignment");
    expect(real.conversationKey).not.toBe(null);
    const totalRecent = km.listKeys().reduce((n, k) => n + k.recentSessions.length, 0);
    expect(totalRecent).toBe(1);
  });

  test("any all-zero-prefix UUID is treated as synthetic", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() => new Response(
      JSON.stringify({ usage: { input_tokens: 1, output_tokens: 1 } }),
      { status: 200, headers: { "content-type": "application/json" } },
    ));
    const config = makeConfig(mock.url);

    const res = await proxyRequest(makeRequest("/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-claude-code-session-id": "00000000-0000-4000-8000-deadbeefcafe",
      },
      body: JSON.stringify({ messages: [{ role: "user", content: "x" }] }),
    }), km, config, st);
    expect(res.kind).toBe("success");

    expect(km.getRecentRoutingDecisions()[0]!.routingDecision).toBe("global_sticky_fallback");
    expect(km.listKeys()[0]!.recentSessions).toEqual([]);
  });
});

describe("Non-/v1/messages session traffic", () => {
  // count_tokens (and any other /v1/messages sibling endpoint) shares a
  // session-id with the real call but produces no first-message hash, so
  // it pins by 2-part conversationKey for routing co-location and stays
  // out of the dashboard sessions table.
  test("count_tokens pins by sessionId but is hidden from recentSessions", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    let lastKeySeen: string | null = null;
    const mock = upstream((req) => {
      lastKeySeen = req.headers.get("x-api-key");
      return new Response(JSON.stringify({ input_tokens: 42 }), {
        status: 200, headers: { "content-type": "application/json" },
      });
    });
    const config = makeConfig(mock.url);

    async function send(path: string, session: string, content: string): Promise<void> {
      const res = await proxyRequest(makeRequest(path, {
        method: "POST",
        headers: { "content-type": "application/json", "x-claude-code-session-id": session },
        body: JSON.stringify({ messages: [{ role: "user", content }] }),
      }), km, config, st);
      expect(res.kind).toBe("success");
    }

    // First count_tokens call lands on some key and pins a 2-part conversationKey.
    await send("/v1/messages/count_tokens", "session-x", "probe a");
    const firstKey = lastKeySeen;
    expect(firstKey).not.toBeNull();
    const firstDecision = km.getRecentRoutingDecisions()[0]!;
    expect(firstDecision.conversationKey).toBe("anon:session-x");
    expect(firstDecision.routingDecision).toBe("conversation_new_assignment");

    // A second count_tokens with the same session hits the same pinned key
    // — affinity preserved even though there's no first-message hash.
    await send("/v1/messages/count_tokens", "session-x", "probe b");
    expect(lastKeySeen).toBe(firstKey);
    expect(km.getRecentRoutingDecisions()[0]!.routingDecision).toBe("conversation_affinity_hit");

    // ...but the dashboard sessions table doesn't show it (no hash → not a
    // real conversation turn).
    for (const k of km.listKeys()) {
      expect(k.recentSessions).toEqual([]);
    }
  });

  // Session-only pinning is the product default. Sub-agents within one
  // session share a key (no hash-based split). Every session with affinity
  // shows on the dashboard regardless of which path it used.
  test("session-only mode (default): all calls in a session pin to one key, dashboard shows the session", async () => {
    const { km, st } = setup({ perConversationPinning: false });
    km.addKey(FAKE_KEY_A, "key-a");
    km.addKey(FAKE_KEY_B, "key-b");

    const seenKeys: string[] = [];
    const mock = upstream((req) => {
      seenKeys.push(req.headers.get("x-api-key") ?? "");
      return new Response(JSON.stringify({ usage: { input_tokens: 1, output_tokens: 1 } }), {
        status: 200, headers: { "content-type": "application/json" },
      });
    });
    const config = makeConfig(mock.url, { perConversationPinning: false });

    async function send(path: string, content: string): Promise<void> {
      const res = await proxyRequest(makeRequest(path, {
        method: "POST",
        headers: { "content-type": "application/json", "x-claude-code-session-id": "session-z" },
        body: JSON.stringify({ messages: [{ role: "user", content }] }),
      }), km, config, st);
      expect(res.kind).toBe("success");
    }

    // Three different first messages — under per-conv mode they'd split
    // across keys. Under session-only mode they all stick to the first.
    await send("/v1/messages", "parent prompt");
    await send("/v1/messages", "sub-agent A");
    await send("/v1/messages", "sub-agent B");
    await send("/v1/messages/count_tokens", "probe");

    expect(new Set(seenKeys).size).toBe(1);

    // Dashboard shows the session as one row (one 2-part affinity entry).
    const recent = km.listKeys().flatMap((k) => k.recentSessions);
    expect(recent.length).toBe(1);
    expect(recent[0]!.sessionId).toBe("session-z");
    expect(recent[0]!.conversations.length).toBe(1);
    expect(recent[0]!.conversations[0]!.hash).toBeNull();
  });

  test("real /v1/messages turn on the same session does show on the dashboard", async () => {
    const { km, st } = setup();
    km.addKey(FAKE_KEY_A, "key-a");

    const mock = upstream(() => new Response(
      JSON.stringify({ usage: { input_tokens: 1, output_tokens: 1 } }),
      { status: 200, headers: { "content-type": "application/json" } },
    ));
    const config = makeConfig(mock.url);

    async function send(path: string, content: string): Promise<void> {
      const res = await proxyRequest(makeRequest(path, {
        method: "POST",
        headers: { "content-type": "application/json", "x-claude-code-session-id": "session-y" },
        body: JSON.stringify({ messages: [{ role: "user", content }] }),
      }), km, config, st);
      expect(res.kind).toBe("success");
    }

    await send("/v1/messages/count_tokens", "probe");
    await send("/v1/messages", "real");

    const recent = km.listKeys()[0]!.recentSessions;
    // Only the real turn shows up — the count_tokens probe is filtered out.
    expect(recent.length).toBe(1);
    expect(recent[0]!.conversations.length).toBe(1);
    expect(recent[0]!.conversations[0]!.hash).not.toBeNull();
  });
});
