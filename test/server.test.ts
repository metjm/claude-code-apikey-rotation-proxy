import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { KeyManager } from "../src/key-manager.ts";
import { handleAdminRoute } from "../src/admin.ts";
import { proxyRequest } from "../src/proxy.ts";
import type { ProxyConfig, ProxyTokenEntry } from "../src/types.ts";
import { SchemaTracker } from "../src/schema-tracker.ts";

// ── Test helpers ──────────────────────────────────────────────────

/** Fake API key that passes the sk-ant- prefix validation */
const FAKE_KEY_1 = "sk-ant-api03-aaaaaaaaaa-bbbbbbbbbbbbbb-cccccccccccc";
const FAKE_KEY_2 = "sk-ant-api03-dddddddddd-eeeeeeeeeeeeee-ffffffffffff";
const FAKE_KEY_3 = "sk-ant-api03-gggggggggg-hhhhhhhhhhhhhh-iiiiiiiiiiii";

/** A fake OAuth token (sk-ant-oat prefix) */
const FAKE_OAUTH_KEY = "sk-ant-oat-dddddddddd-eeeeeeeeeeeeee-ffffffffffff";

/** Fake proxy tokens (must be >= 8 chars) */
const PROXY_TOKEN_ALICE = "alice-proxy-token-12345";
const PROXY_TOKEN_BOB = "bob-proxy-token-67890";

/** Admin credentials */
const ADMIN_TOKEN = "test-admin-secret-token";

interface MockUpstream {
  url: string;
  port: number;
  stop: () => void;
}

interface ProxyInstance {
  url: string;
  port: number;
  km: KeyManager;
  config: ProxyConfig;
  stop: () => void;
}

/**
 * Start a mock Anthropic upstream. The wrapper drains the request body before
 * delegating to the handler. This is essential because Bun.serve may return
 * HTTP 400 if the request body is not consumed before the response is sent.
 *
 * For retried proxy requests, the body stream may be locked/consumed already
 * (since the proxy reuses `req.body` on retries with duplex: "half"). We use
 * a short timeout so we don't hang forever waiting on a dead stream.
 */
function startMockUpstream(
  handler: (req: Request) => Response | Promise<Response>,
): MockUpstream {
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      // Drain request body with a timeout to handle retried/locked streams.
      try {
        await Promise.race([
          req.arrayBuffer(),
          new Promise((resolve) => setTimeout(resolve, 100)),
        ]);
      } catch {}
      return handler(req);
    },
  });
  return {
    url: `http://localhost:${server.port}`,
    port: server.port,
    stop: () => server.stop(true),
  };
}

/**
 * Like startMockUpstream but the handler receives (req, bodyText) so it can
 * inspect the request body. The body is read before calling the handler.
 * NOTE: Do not use this for tests where the proxy may retry (429 rotation),
 * because retried requests may have an empty body stream.
 */
function startMockUpstreamWithBody(
  handler: (req: Request, body: string) => Response | Promise<Response>,
): MockUpstream {
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      let bodyText = "";
      try {
        if (req.method !== "GET" && req.method !== "HEAD") {
          bodyText = await req.text();
        }
      } catch {
        // Body may be empty/locked on retried requests
      }
      return handler(req, bodyText);
    },
  });
  return {
    url: `http://localhost:${server.port}`,
    port: server.port,
    stop: () => server.stop(true),
  };
}

function startProxy(opts: {
  dataDir: string;
  upstream: string;
  adminToken?: string | null;
}): ProxyInstance {
  const km = new KeyManager(opts.dataDir);
  const st = new SchemaTracker(km.dbPath);
  const config: ProxyConfig = {
    port: 0,
    upstream: opts.upstream,
    adminToken: opts.adminToken ?? null,
    dataDir: opts.dataDir,
    maxRetriesPerRequest: 10,
    firstChunkTimeoutMs: 16_000,
    maxFirstChunkRetries: 2,
    webhookUrl: null,
  };

  const server = Bun.serve({
    port: 0,
    async fetch(req: Request): Promise<Response> {
      const url = new URL(req.url);

      // Serve dashboard
      if (url.pathname === "/dashboard" || url.pathname === "/dashboard/") {
        return new Response("<html><body>dashboard</body></html>", {
          headers: { "content-type": "text/html" },
        });
      }
      if (url.pathname === "/dashboard/chart.umd.min.js.map") {
        return new Response(null, { status: 204 });
      }
      if (url.pathname === "/favicon.ico") {
        return new Response(null, { status: 204 });
      }

      // Admin routes
      const adminResponse = await handleAdminRoute(req, km, config, st);
      if (adminResponse !== null) return adminResponse;

      // Auth gate
      let proxyUser: ProxyTokenEntry | null = null;
      if (km.hasTokens()) {
        const xApiKey = req.headers.get("x-api-key");
        const auth = req.headers.get("authorization");
        const incoming =
          xApiKey ?? (auth?.startsWith("Bearer ") ? auth.slice(7) : null);
        if (!incoming) {
          return new Response(
            JSON.stringify({
              error: {
                type: "proxy_error",
                message:
                  "Proxy authentication required. Set your API key to a valid proxy token.",
              },
            }),
            { status: 401, headers: { "content-type": "application/json" } },
          );
        }
        proxyUser = km.validateToken(incoming);
        if (!proxyUser) {
          return new Response(
            JSON.stringify({
              error: {
                type: "proxy_error",
                message: "Invalid proxy token.",
              },
            }),
            { status: 401, headers: { "content-type": "application/json" } },
          );
        }
      }

      const result = await proxyRequest(req, km, config, st, proxyUser);

      switch (result.kind) {
        case "success":
          return result.response;
        case "no_keys":
          return new Response(
            JSON.stringify({
              error: {
                type: "proxy_error",
                message: proxyUser
                  ? "No API keys configured. Add keys via the admin API."
                  : "Service not available.",
              },
            }),
            { status: 503, headers: { "content-type": "application/json" } },
          );
        case "all_exhausted": {
          const waitSecs = Math.ceil(
            Math.max(0, result.earliestAvailableAt - Date.now()) / 1000,
          );
          return new Response(
            JSON.stringify({
              error: {
                type: "proxy_error",
                message: proxyUser
                  ? `All API keys are rate-limited. Retry in ${waitSecs}s.`
                  : "Too many requests.",
              },
            }),
            {
              status: 429,
              headers: {
                "content-type": "application/json",
                "retry-after": String(waitSecs),
              },
            },
          );
        }
        case "error":
          return new Response(result.body, {
            status: result.status,
            headers: { "content-type": "application/json" },
          });
        case "rate_limited":
          return new Response(
            JSON.stringify({
              error: { type: "proxy_error", message: "Rate limited" },
            }),
            {
              status: 429,
              headers: {
                "content-type": "application/json",
                "retry-after": String(result.retryAfterSecs),
              },
            },
          );
      }
    },
  });

  return {
    url: `http://localhost:${server.port}`,
    port: server.port,
    km,
    config,
    stop: () => { server.stop(true); st.close(); km.close(); },
  };
}

function makeTempDir(): string {
  return mkdtempSync(join(tmpdir(), "proxy-test-"));
}

function cleanupTempDir(dir: string): void {
  try {
    rmSync(dir, { recursive: true, force: true });
  } catch {
    // ignore
  }
}

function futureEpochSeconds(offsetMs: number): string {
  return String(Math.floor((Date.now() + offsetMs) / 1000));
}

/** Simple JSON mock upstream that returns a success response */
function jsonOkHandler(): (req: Request) => Response {
  return () =>
    new Response(JSON.stringify({ ok: true }), {
      headers: { "content-type": "application/json" },
    });
}

// ── Dashboard ────────────────────────────────────────────────────

describe("Dashboard", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;

  beforeAll(() => {
    dataDir = makeTempDir();
    upstream = startMockUpstream(jsonOkHandler());
    proxy = startProxy({ dataDir, upstream: upstream.url });
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("GET /dashboard serves HTML", async () => {
    const res = await fetch(`${proxy.url}/dashboard`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/html");
    const body = await res.text();
    expect(body).toContain("dashboard");
  });

  test("GET /dashboard/ (trailing slash) serves HTML", async () => {
    const res = await fetch(`${proxy.url}/dashboard/`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/html");
    const body = await res.text();
    expect(body).toContain("dashboard");
  });

  test("GET /admin/bootstrap reports auth disabled when no admin token is configured", async () => {
    const res = await fetch(`${proxy.url}/admin/bootstrap`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({ authRequired: false });
  });

  test("dashboard inline script parses without syntax errors", () => {
    const html = readFileSync(new URL("../public/dashboard.html", import.meta.url), "utf8");
    const scriptStart = html.lastIndexOf("<script>");
    const scriptEnd = html.lastIndexOf("</script>");

    expect(scriptStart).toBeGreaterThanOrEqual(0);
    expect(scriptEnd).toBeGreaterThan(scriptStart);

    const script = html.slice(scriptStart + "<script>".length, scriptEnd);

    expect(() => new Function(script)).not.toThrow();
  });

  test("GET /favicon.ico returns 204", async () => {
    const res = await fetch(`${proxy.url}/favicon.ico`);
    expect(res.status).toBe(204);
  });

  test("GET /dashboard/chart.umd.min.js.map returns 204", async () => {
    const res = await fetch(`${proxy.url}/dashboard/chart.umd.min.js.map`);
    expect(res.status).toBe(204);
  });
});

// ── Admin Auth End-to-End ────────────────────────────────────────

describe("Admin Auth End-to-End", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;

  beforeAll(() => {
    dataDir = makeTempDir();
    upstream = startMockUpstream(jsonOkHandler());
    proxy = startProxy({
      dataDir,
      upstream: upstream.url,
      adminToken: ADMIN_TOKEN,
    });
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("GET /admin/health works without auth", async () => {
    const res = await fetch(`${proxy.url}/admin/health`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.status).toBeDefined();
    expect(body.keys).toBeDefined();
  });

  test("GET /admin/bootstrap reports auth required without auth", async () => {
    const res = await fetch(`${proxy.url}/admin/bootstrap`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({ authRequired: true });
  });

  test("GET /admin/events works without auth", async () => {
    const controller = new AbortController();
    const res = await fetch(`${proxy.url}/admin/events`, {
      signal: controller.signal,
    });
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/event-stream");
    controller.abort();
  });

  test("GET /admin/keys without auth returns 401", async () => {
    const res = await fetch(`${proxy.url}/admin/keys`);
    expect(res.status).toBe(401);
    const body = await res.json();
    expect(body.error).toBe("Unauthorized");
  });

  test("POST /admin/keys without auth returns 401", async () => {
    const res = await fetch(`${proxy.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_1 }),
    });
    expect(res.status).toBe(401);
  });

  test("GET /admin/stats without auth returns 401", async () => {
    const res = await fetch(`${proxy.url}/admin/stats`);
    expect(res.status).toBe(401);
  });

  test("GET /admin/tokens without auth returns 401", async () => {
    const res = await fetch(`${proxy.url}/admin/tokens`);
    expect(res.status).toBe(401);
  });

  test("POST /admin/tokens without auth returns 401", async () => {
    const res = await fetch(`${proxy.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: PROXY_TOKEN_ALICE }),
    });
    expect(res.status).toBe(401);
  });

  test("wrong token returns 401", async () => {
    const res = await fetch(`${proxy.url}/admin/keys`, {
      headers: { authorization: "Bearer wrong-token" },
    });
    expect(res.status).toBe(401);
    const body = await res.json();
    expect(body.error).toBe("Unauthorized");
  });

  test("correct token allows access to /admin/keys", async () => {
    const res = await fetch(`${proxy.url}/admin/keys`, {
      headers: { authorization: `Bearer ${ADMIN_TOKEN}` },
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.keys).toBeArray();
  });

  test("correct token allows access to /admin/stats", async () => {
    const res = await fetch(`${proxy.url}/admin/stats`, {
      headers: { authorization: `Bearer ${ADMIN_TOKEN}` },
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.keyCount).toBeDefined();
    expect(body.totals).toBeDefined();
  });

  test("correct token allows access to /admin/tokens", async () => {
    const res = await fetch(`${proxy.url}/admin/tokens`, {
      headers: { authorization: `Bearer ${ADMIN_TOKEN}` },
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.tokens).toBeArray();
  });

  test("unknown admin path returns 404", async () => {
    const res = await fetch(`${proxy.url}/admin/nonexistent`, {
      headers: { authorization: `Bearer ${ADMIN_TOKEN}` },
    });
    expect(res.status).toBe(404);
    const body = await res.json();
    expect(body.error).toBe("Not found");
  });

  test("wrong HTTP method returns 405", async () => {
    const res = await fetch(`${proxy.url}/admin/keys`, {
      method: "DELETE",
      headers: { authorization: `Bearer ${ADMIN_TOKEN}` },
    });
    expect(res.status).toBe(405);
    const body = await res.json();
    expect(body.error).toBe("Method not allowed");
  });
});

// ── Admin Auth Disabled ──────────────────────────────────────────

describe("Admin Auth Disabled (no ADMIN_TOKEN)", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;

  beforeAll(() => {
    dataDir = makeTempDir();
    upstream = startMockUpstream(jsonOkHandler());
    proxy = startProxy({
      dataDir,
      upstream: upstream.url,
      adminToken: null,
    });
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("admin endpoints accessible without auth when no ADMIN_TOKEN", async () => {
    const keysRes = await fetch(`${proxy.url}/admin/keys`);
    expect(keysRes.status).toBe(200);

    const statsRes = await fetch(`${proxy.url}/admin/stats`);
    expect(statsRes.status).toBe(200);

    const tokensRes = await fetch(`${proxy.url}/admin/tokens`);
    expect(tokensRes.status).toBe(200);
  });
});

// ── Key Management Workflow ──────────────────────────────────────

describe("Key Management Workflow", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;

  beforeAll(() => {
    dataDir = makeTempDir();
    upstream = startMockUpstream(jsonOkHandler());
    proxy = startProxy({ dataDir, upstream: upstream.url });
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("full key lifecycle: add, list, remove", async () => {
    // Initially no keys
    const listEmpty = await fetch(`${proxy.url}/admin/keys`);
    const emptyBody = await listEmpty.json();
    expect(emptyBody.keys).toEqual([]);

    // Add key 1 with label
    const add1 = await fetch(`${proxy.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_1, label: "production-1" }),
    });
    expect(add1.status).toBe(201);
    const add1Body = await add1.json();
    expect(add1Body.added.label).toBe("production-1");
    expect(add1Body.added.maskedKey).toContain("...");

    // Add key 2 without label (auto-generated)
    const add2 = await fetch(`${proxy.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_2 }),
    });
    expect(add2.status).toBe(201);
    const add2Body = await add2.json();
    expect(add2Body.added.label).toBe("key-2");

    // List should show 2 keys
    const list2 = await fetch(`${proxy.url}/admin/keys`);
    const list2Body = await list2.json();
    expect(list2Body.keys).toHaveLength(2);
    expect(list2Body.keys[0].label).toBe("production-1");
    expect(list2Body.keys[0].maskedKey).toContain("...");
    expect(list2Body.keys[0].stats).toBeDefined();
    expect(list2Body.keys[0].stats.totalRequests).toBe(0);
    expect(list2Body.keys[0].isAvailable).toBe(true);
    expect(list2Body.keys[1].label).toBe("key-2");

    // Remove key 1
    const removeRes = await fetch(`${proxy.url}/admin/keys/remove`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_1 }),
    });
    expect(removeRes.status).toBe(200);
    const removeBody = await removeRes.json();
    expect(removeBody.removed).toBe(true);

    // List should show 1 key
    const list1 = await fetch(`${proxy.url}/admin/keys`);
    const list1Body = await list1.json();
    expect(list1Body.keys).toHaveLength(1);
    expect(list1Body.keys[0].label).toBe("key-2");
  });

  test("adding duplicate key returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_2 }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain("already registered");
  });

  test("adding key with empty key field returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: "" }),
    });
    expect(res.status).toBe(400);
  });

  test("adding key with invalid JSON returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "not json",
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain("Invalid JSON body");
  });

  test("adding key with invalid prefix returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: "invalid-key-format" }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain("sk-ant-");
  });

  test("removing nonexistent key returns 404", async () => {
    const res = await fetch(`${proxy.url}/admin/keys/remove`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: "sk-ant-nonexistent-key" }),
    });
    expect(res.status).toBe(404);
    const body = await res.json();
    expect(body.error).toBe("Key not found");
  });

  test("removing key with invalid body returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/keys/remove`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "not json",
    });
    expect(res.status).toBe(400);
  });

  test("health reflects key count", async () => {
    const res = await fetch(`${proxy.url}/admin/health`);
    const body = await res.json();
    expect(body.keys.total).toBe(1); // FAKE_KEY_2 remains from above
    expect(body.keys.available).toBe(1);
    expect(body.status).toBe("ok");
  });

  test("health reports no_keys when empty", async () => {
    // Remove last key
    await fetch(`${proxy.url}/admin/keys/remove`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_2 }),
    });

    const res = await fetch(`${proxy.url}/admin/health`);
    const body = await res.json();
    expect(body.keys.total).toBe(0);
    expect(body.keys.available).toBe(0);
    expect(body.status).toBe("no_keys");
  });

  test("stats aggregate correctly", async () => {
    // Add two keys
    await fetch(`${proxy.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_1, label: "s1" }),
    });
    await fetch(`${proxy.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_3, label: "s2" }),
    });

    const res = await fetch(`${proxy.url}/admin/stats`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.keyCount).toBe(2);
    expect(body.availableKeys).toBe(2);
    expect(body.totals).toBeDefined();
    expect(body.totals.totalRequests).toBe(0);
    expect(body.totals.successfulRequests).toBe(0);
    expect(body.totals.rateLimitHits).toBe(0);
    expect(body.totals.errors).toBe(0);
    expect(body.totals.totalTokensIn).toBe(0);
    expect(body.totals.totalTokensOut).toBe(0);
    expect(body.keys).toHaveLength(2);
  });
});

// ── Token Management Workflow ────────────────────────────────────

describe("Token Management Workflow", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;

  beforeAll(() => {
    dataDir = makeTempDir();
    upstream = startMockUpstream(jsonOkHandler());
    proxy = startProxy({ dataDir, upstream: upstream.url });
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("full token lifecycle: add, list, remove", async () => {
    // Initially no tokens
    const listEmpty = await fetch(`${proxy.url}/admin/tokens`);
    const emptyBody = await listEmpty.json();
    expect(emptyBody.tokens).toEqual([]);

    // Add token with label
    const add1 = await fetch(`${proxy.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: PROXY_TOKEN_ALICE, label: "alice" }),
    });
    expect(add1.status).toBe(201);
    const add1Body = await add1.json();
    expect(add1Body.added.label).toBe("alice");

    // Add token without label (auto-generated)
    const add2 = await fetch(`${proxy.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: PROXY_TOKEN_BOB }),
    });
    expect(add2.status).toBe(201);
    const add2Body = await add2.json();
    expect(add2Body.added.label).toBe("user-2");

    // List should show 2 tokens
    const list2 = await fetch(`${proxy.url}/admin/tokens`);
    const list2Body = await list2.json();
    expect(list2Body.tokens).toHaveLength(2);
    expect(list2Body.tokens[0].label).toBe("alice");
    expect(list2Body.tokens[0].maskedToken).toContain("...");
    expect(list2Body.tokens[0].stats).toBeDefined();
    expect(list2Body.tokens[0].stats.totalRequests).toBe(0);
    expect(list2Body.tokens[1].label).toBe("user-2");

    // Remove token
    const removeRes = await fetch(`${proxy.url}/admin/tokens/remove`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: PROXY_TOKEN_BOB }),
    });
    expect(removeRes.status).toBe(200);
    const removeBody = await removeRes.json();
    expect(removeBody.removed).toBe(true);

    // List should show 1 token
    const list1 = await fetch(`${proxy.url}/admin/tokens`);
    const list1Body = await list1.json();
    expect(list1Body.tokens).toHaveLength(1);
    expect(list1Body.tokens[0].label).toBe("alice");
  });

  test("adding duplicate token returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: PROXY_TOKEN_ALICE }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain("already registered");
  });

  test("adding token with empty field returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: "" }),
    });
    expect(res.status).toBe(400);
  });

  test("adding token too short returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: "short" }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain("at least 8 characters");
  });

  test("adding token with invalid JSON returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "not json",
    });
    expect(res.status).toBe(400);
  });

  test("removing nonexistent token returns 404", async () => {
    const res = await fetch(`${proxy.url}/admin/tokens/remove`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: "nonexistent-token-12345" }),
    });
    expect(res.status).toBe(404);
    const body = await res.json();
    expect(body.error).toBe("Token not found");
  });

  test("removing token with invalid body returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/tokens/remove`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "not json",
    });
    expect(res.status).toBe(400);
  });

  test("adding token with non-string label returns 400", async () => {
    const res = await fetch(`${proxy.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: "valid-token-length-ok", label: 123 }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain("label");
  });
});

// ── Proxy Auth Gate ──────────────────────────────────────────────

describe("Proxy Auth Gate", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;

  beforeAll(() => {
    dataDir = makeTempDir();
    upstream = startMockUpstream(jsonOkHandler());
    proxy = startProxy({ dataDir, upstream: upstream.url });
    // Add an API key so proxying can work
    proxy.km.addKey(FAKE_KEY_1, "gate-key");
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("no proxy tokens = open proxy (requests pass through)", async () => {
    // No tokens registered, so anyone can proxy
    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ok).toBe(true);
  });

  test("with proxy tokens: no auth header returns 401", async () => {
    // Add a proxy token to enable auth gate
    proxy.km.addToken(PROXY_TOKEN_ALICE, "alice");

    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    expect(res.status).toBe(401);
    const body = await res.json();
    expect(body.error.type).toBe("proxy_error");
    expect(body.error.message).toContain("Proxy authentication required");
  });

  test("with proxy tokens: wrong token returns 401", async () => {
    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": "wrong-token-definitely",
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    expect(res.status).toBe(401);
    const body = await res.json();
    expect(body.error.type).toBe("proxy_error");
    expect(body.error.message).toContain("Invalid proxy token");
  });

  test("with proxy tokens: valid x-api-key header succeeds", async () => {
    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": PROXY_TOKEN_ALICE,
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ok).toBe(true);
  });

  test("with proxy tokens: valid Authorization: Bearer header succeeds", async () => {
    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${PROXY_TOKEN_ALICE}`,
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ok).toBe(true);
  });

  test("after removing all tokens: proxy is open again", async () => {
    proxy.km.removeToken(PROXY_TOKEN_ALICE);
    expect(proxy.km.hasTokens()).toBe(false);

    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ok).toBe(true);
  });
});

// ── Full Proxy Flow ──────────────────────────────────────────────

describe("Full Proxy Flow", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;

  /** Captured from the last upstream request */
  let capturedHeaders: Record<string, string> = {};
  let capturedPath = "";
  let capturedMethod = "";

  beforeAll(() => {
    dataDir = makeTempDir();
    upstream = startMockUpstreamWithBody((req, _body) => {
      const url = new URL(req.url);
      capturedPath = url.pathname + url.search;
      capturedMethod = req.method;
      capturedHeaders = {};
      for (const [key, value] of req.headers.entries()) {
        capturedHeaders[key] = value;
      }

      if (url.pathname === "/v1/messages") {
        return new Response(
          JSON.stringify({
            id: "msg_abc123",
            type: "message",
            role: "assistant",
            content: [{ type: "text", text: "Hello!" }],
            model: "claude-3-haiku-20240307",
            usage: { input_tokens: 42, output_tokens: 17 },
          }),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
              "x-request-id": "req_upstream_123",
            },
          },
        );
      }

      return new Response(
        JSON.stringify({ error: "not found" }),
        { status: 404, headers: { "content-type": "application/json" } },
      );
    });
    proxy = startProxy({ dataDir, upstream: upstream.url });
    proxy.km.addKey(FAKE_KEY_1, "flow-key");
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("request is proxied to upstream with correct headers", async () => {
    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 100,
        messages: [{ role: "user", content: "Hi" }],
      }),
    });

    expect(res.status).toBe(200);
    await res.json();

    // Verify upstream received the request
    expect(capturedPath).toBe("/v1/messages");
    expect(capturedMethod).toBe("POST");

    // Verify the upstream request has the API key
    expect(capturedHeaders["x-api-key"]).toBe(FAKE_KEY_1);
    expect(capturedHeaders["anthropic-version"]).toBe("2023-06-01");
  });

  test("upstream response is returned to client with correct headers", async () => {
    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 100,
        messages: [{ role: "user", content: "Hi" }],
      }),
    });

    expect(res.status).toBe(200);
    expect(res.headers.get("x-request-id")).toBe("req_upstream_123");

    const body = await res.json();
    expect(body.id).toBe("msg_abc123");
    expect(body.content[0].text).toBe("Hello!");
    expect(body.usage.input_tokens).toBe(42);
    expect(body.usage.output_tokens).toBe(17);
  });

  test("stats updated after request", async () => {
    const statsBefore = await fetch(`${proxy.url}/admin/stats`);
    const before = await statsBefore.json();
    const reqsBefore = before.totals.totalRequests;
    const successBefore = before.totals.successfulRequests;

    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 100,
        messages: [{ role: "user", content: "Hi" }],
      }),
    });
    await res.json();

    const statsAfter = await fetch(`${proxy.url}/admin/stats`);
    const after = await statsAfter.json();
    expect(after.totals.totalRequests).toBe(reqsBefore + 1);
    expect(after.totals.successfulRequests).toBe(successBefore + 1);
  });

  test("token usage tracked (mock upstream returns usage JSON)", async () => {
    // Start fresh to isolate counts
    const freshDir = makeTempDir();
    const freshUpstream = startMockUpstream(
      () =>
        new Response(
          JSON.stringify({
            usage: { input_tokens: 100, output_tokens: 50 },
          }),
          { headers: { "content-type": "application/json" } },
        ),
    );
    const freshProxy = startProxy({
      dataDir: freshDir,
      upstream: freshUpstream.url,
    });
    freshProxy.km.addKey(FAKE_KEY_1, "token-track-key");

    const res = await fetch(`${freshProxy.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    await res.json();

    const stats = await fetch(`${freshProxy.url}/admin/stats`);
    const body = await stats.json();
    expect(body.totals.totalTokensIn).toBe(100);
    expect(body.totals.totalTokensOut).toBe(50);
    expect(body.keys[0].stats.totalTokensIn).toBe(100);
    expect(body.keys[0].stats.totalTokensOut).toBe(50);

    freshProxy.stop();
    freshUpstream.stop();
    cleanupTempDir(freshDir);
  });

  test("OAuth token (sk-ant-oat) uses Bearer auth upstream", async () => {
    const oauthDir = makeTempDir();
    let oauthCapturedHeaders: Record<string, string> = {};
    const oauthUpstream = startMockUpstreamWithBody((req) => {
      oauthCapturedHeaders = {};
      for (const [key, value] of req.headers.entries()) {
        oauthCapturedHeaders[key] = value;
      }
      return new Response(JSON.stringify({ ok: true }), {
        headers: { "content-type": "application/json" },
      });
    });
    const oauthProxy = startProxy({
      dataDir: oauthDir,
      upstream: oauthUpstream.url,
    });
    oauthProxy.km.addKey(FAKE_OAUTH_KEY, "oauth-key");

    const res = await fetch(`${oauthProxy.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    await res.json();

    expect(oauthCapturedHeaders["authorization"]).toBe(
      `Bearer ${FAKE_OAUTH_KEY}`,
    );
    // Should not have x-api-key for OAuth tokens
    expect(oauthCapturedHeaders["x-api-key"]).toBeUndefined();

    oauthProxy.stop();
    oauthUpstream.stop();
    cleanupTempDir(oauthDir);
  });

  test("query string is preserved through proxy", async () => {
    const res = await fetch(
      `${proxy.url}/v1/messages?beta=true&version=2`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: "claude-3-haiku-20240307",
          max_tokens: 1,
          messages: [],
        }),
      },
    );
    await res.text();

    expect(capturedPath).toContain("?beta=true&version=2");
  });
});

// ── Capacity Telemetry End-to-End ────────────────────────────────

describe("Capacity Telemetry End-to-End", () => {
  test("successful responses treat per-window rejected headers as analytics and keep the key routable", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(() =>
      new Response(
        JSON.stringify({
          usage: { input_tokens: 10, output_tokens: 4 },
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
            "request-id": "req-cap-e2e-1",
            "anthropic-organization-id": "org-cap-e2e",
            "anthropic-ratelimit-unified-representative-claim": "seven_day",
            "anthropic-ratelimit-unified-status": "allowed",
            "anthropic-ratelimit-unified-reset": futureEpochSeconds(45 * 60_000),
            "anthropic-ratelimit-unified-7d-status": "rejected",
            "anthropic-ratelimit-unified-7d-utilization": "0.93",
            "anthropic-ratelimit-unified-7d-reset": futureEpochSeconds(7 * 24 * 60 * 60_000),
            "anthropic-ratelimit-unified-overage-status": "rejected",
            "anthropic-ratelimit-unified-overage-disabled-reason": "out_of_credits",
            "anthropic-ratelimit-unified-fallback": "available",
            "anthropic-ratelimit-unified-fallback-percentage": "0.5",
            "x-should-retry": "false",
            "x-envoy-upstream-service-time": "1876",
          },
        },
      ),
    );
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "cap-e2e-key");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 8,
        messages: [{ role: "user", content: "capacity" }],
      }),
    });
    expect(res.status).toBe(200);
    await res.json();

    const stats = await fetch(`${p.url}/admin/stats`);
    const body = await stats.json();
    const key = body.keys.find((k: { label: string }) => k.label === "cap-e2e-key");

    expect(key).toBeDefined();
    const unified = key!.capacity.windows.find(
      (w: { windowName: string }) => w.windowName === "unified",
    );
    const sevenDay = key!.capacity.windows.find(
      (w: { windowName: string }) => w.windowName === "unified-7d",
    );

    expect(key.isAvailable).toBe(true);
    expect(key.capacity.organizationId).toBe("org-cap-e2e");
    expect(key.capacity.lastRequestId).toBe("req-cap-e2e-1");
    expect(key.capacity.representativeClaim).toBe("seven_day");
    expect(key.capacity.responseCount).toBe(1);
    expect(key.capacity.normalizedHeaderCount).toBe(1);
    expect(key.capacity.shouldRetry).toBe(false);
    expect(key.capacity.fallbackAvailable).toBe(true);
    expect(key.capacity.fallbackPercentage).toBe(0.5);
    expect(key.capacity.overageStatus).toBe("rejected");
    expect(key.capacity.overageDisabledReason).toBe("out_of_credits");
    expect(key.capacity.latencyMs).toBe(1876);
    expect(key.capacityHealth).toBe("warning");
    expect(key.capacity.signalCoverage.find((s: { signalName: string }) => s.signalName === "windows")).toBeDefined();
    expect(unified).toBeDefined();
    expect(unified.status).toBe("allowed_warning");
    expect(sevenDay).toBeDefined();
    expect(sevenDay.status).toBe("allowed_warning");
    expect(sevenDay.utilization).toBe(0.93);

    expect(body.capacitySummary.healthyKeys).toBe(0);
    expect(body.capacitySummary.warningKeys).toBe(1);
    expect(body.capacitySummary.rejectedKeys).toBe(0);
    expect(body.capacitySummary.fallbackAvailableKeys).toBe(1);
    expect(body.capacitySummary.overageRejectedKeys).toBe(1);
    expect(body.capacitySummary.distinctOrganizations).toBe(1);
    expect(body.capacitySummary.windows.find((w: { windowName: string }) => w.windowName === "unified-7d")!.maxUtilization).toBe(0.93);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("responses without normalized headers increase response counts without fabricating signal state", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(() =>
      new Response(
        JSON.stringify({ ok: true }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
            "x-custom-response": "present",
          },
        },
      ),
    );
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "cap-sparse-key");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 4,
        messages: [{ role: "user", content: "sparse" }],
      }),
    });
    expect(res.status).toBe(200);
    await res.json();

    const stats = await fetch(`${p.url}/admin/stats`);
    const body = await stats.json();
    const key = body.keys.find((k: { label: string }) => k.label === "cap-sparse-key");

    expect(key).toBeDefined();
    expect(key.capacity.responseCount).toBe(1);
    expect(key.capacity.normalizedHeaderCount).toBe(0);
    expect(key.capacity.lastResponseAt).not.toBeNull();
    expect(key.capacity.lastHeaderAt).toBeNull();
    expect(key.capacity.signalCoverage).toEqual([]);
    expect(key.capacity.windows).toEqual([]);
    expect(key.capacityHealth).toBe("unknown");
    expect(body.capacitySummary.windows).toEqual([]);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("/admin/capacity/timeseries rolls up utilization windows without trusting undocumented per-window statuses", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(() =>
      new Response(
        JSON.stringify({ ok: true }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
            "anthropic-ratelimit-unified-status": "allowed_warning",
            "anthropic-ratelimit-unified-reset": futureEpochSeconds(60 * 60_000),
            "anthropic-ratelimit-unified-5h-status": "allowed",
            "anthropic-ratelimit-unified-5h-utilization": "0.41",
            "anthropic-ratelimit-unified-5h-reset": futureEpochSeconds(5 * 60 * 60_000),
            "anthropic-ratelimit-unified-7d-status": "rejected",
            "anthropic-ratelimit-unified-7d-utilization": "0.88",
            "anthropic-ratelimit-unified-7d-reset": futureEpochSeconds(7 * 24 * 60 * 60_000),
          },
        },
      ),
    );
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "cap-ts-key");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 4,
        messages: [{ role: "user", content: "timeseries" }],
      }),
    });
    expect(res.status).toBe(200);
    await res.json();

    // Flush debounced capacity timeseries writes before re-reading through HTTP.
    p.stop();
    const reloaded = startProxy({ dataDir: dir, upstream: up.url });

    const tsRes = await fetch(`${reloaded.url}/admin/capacity/timeseries?hours=24&resolution=hour&key=cap-ts-key`);
    expect(tsRes.status).toBe(200);
    const tsBody = await tsRes.json();

    expect(tsBody.buckets.length).toBe(3);
    const unified = tsBody.buckets.find((b: { windowName: string }) => b.windowName === "unified");
    const fiveHour = tsBody.buckets.find((b: { windowName: string }) => b.windowName === "unified-5h");
    const sevenDay = tsBody.buckets.find((b: { windowName: string }) => b.windowName === "unified-7d");
    expect(unified).toBeDefined();
    expect(fiveHour).toBeDefined();
    expect(sevenDay).toBeDefined();
    expect(unified.warning).toBe(1);
    expect(fiveHour.allowed).toBe(1);
    expect(fiveHour.warning).toBe(0);
    expect(fiveHour.rejected).toBe(0);
    expect(fiveHour.maxUtilization).toBe(0.41);
    expect(sevenDay.allowed).toBe(0);
    expect(sevenDay.warning).toBe(1);
    expect(sevenDay.rejected).toBe(0);
    expect(sevenDay.maxUtilization).toBe(0.88);

    reloaded.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("initial SSE snapshot exposes routing state separately from analytics-only overage rejection", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(() =>
      new Response(
        JSON.stringify({ ok: true }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
            "anthropic-organization-id": "org-sse-cap",
            "anthropic-ratelimit-unified-status": "allowed",
            "anthropic-ratelimit-unified-reset": futureEpochSeconds(30 * 60_000),
            "anthropic-ratelimit-unified-overage-status": "rejected",
            "anthropic-ratelimit-unified-overage-disabled-reason": "org_level_disabled",
            "anthropic-ratelimit-unified-7d-utilization": "0.77",
            "anthropic-ratelimit-unified-7d-reset": futureEpochSeconds(7 * 24 * 60 * 60_000),
          },
        },
      ),
    );
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "cap-sse-key");

    const proxyRes = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 4,
        messages: [{ role: "user", content: "sse" }],
      }),
    });
    expect(proxyRes.status).toBe(200);
    await proxyRes.json();

    const controller = new AbortController();
    const sseRes = await fetch(`${p.url}/admin/events`, { signal: controller.signal });
    expect(sseRes.status).toBe(200);

    const reader = sseRes.body!.getReader();
    const decoder = new TextDecoder();
    let accumulated = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      accumulated += decoder.decode(value, { stream: true });
      if (accumulated.includes("\n\n")) break;
    }

    controller.abort();
    try { reader.cancel(); } catch {}

    const firstLine = accumulated
      .split("\n")
      .find((line) => line.startsWith("data: "));
    expect(firstLine).toBeDefined();
    const event = JSON.parse(firstLine!.slice(6));
    const key = event.keys.find((k: { label: string }) => k.label === "cap-sse-key");

    expect(event.type).toBe("keys");
    expect(key).toBeDefined();
    expect(event.capacitySummary.healthyKeys).toBe(0);
    expect(event.capacitySummary.warningKeys).toBe(1);
    expect(event.capacitySummary.rejectedKeys).toBe(0);
    expect(event.capacitySummary.overageRejectedKeys).toBe(1);
    expect(event.capacitySummary.windows.find((w: { windowName: string }) => w.windowName === "unified")).toBeDefined();
    expect(key.capacity.organizationId).toBe("org-sse-cap");
    expect(key.isAvailable).toBe(true);
    expect(key.capacityHealth).toBe("warning");
    expect(key.capacity.overageStatus).toBe("rejected");
    expect(key.capacity.overageDisabledReason).toBe("org_level_disabled");
    expect(key.capacity.responseCount).toBe(1);
    expect(key.capacity.normalizedHeaderCount).toBe(1);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });
});

// ── Rate Limit Rotation End-to-End ───────────────────────────────

describe("Rate Limit Rotation End-to-End", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;
  let upstreamCallCount: number;

  beforeAll(() => {
    dataDir = makeTempDir();
    upstreamCallCount = 0;

    upstream = startMockUpstream((req) => {
      upstreamCallCount++;
      const apiKey = req.headers.get("x-api-key");

      // First key gets rate limited, second key succeeds
      if (apiKey === FAKE_KEY_1) {
        return new Response(
          JSON.stringify({
            error: { type: "rate_limit_error", message: "Rate limited" },
          }),
          {
            status: 429,
            headers: {
              "content-type": "application/json",
              "retry-after": "30",
              "anthropic-ratelimit-unified-status": "rejected",
              "anthropic-ratelimit-unified-reset": futureEpochSeconds(30 * 60_000),
              "anthropic-ratelimit-unified-overage-status": "rejected",
              "anthropic-ratelimit-unified-overage-disabled-reason": "out_of_credits",
            },
          },
        );
      }

      return new Response(
        JSON.stringify({
          id: "msg_rotated",
          type: "message",
          content: [{ type: "text", text: "Success after rotation!" }],
          usage: { input_tokens: 10, output_tokens: 5 },
        }),
        {
          status: 200,
          headers: { "content-type": "application/json" },
        },
      );
    });

    proxy = startProxy({ dataDir, upstream: upstream.url });
    proxy.km.addKey(FAKE_KEY_1, "limited-key");
    proxy.km.addKey(FAKE_KEY_2, "good-key");
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("client gets successful response after key rotation on 429", async () => {
    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 100,
        messages: [{ role: "user", content: "test" }],
      }),
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.id).toBe("msg_rotated");
    expect(body.content[0].text).toBe("Success after rotation!");

    // Upstream should have been called at least twice (first 429, then 200)
    expect(upstreamCallCount).toBeGreaterThanOrEqual(2);
  });

  test("stats show rate limit on first key, success on second", async () => {
    const stats = await fetch(`${proxy.url}/admin/stats`);
    const body = await stats.json();

    const limitedKey = body.keys.find(
      (k: { label: string }) => k.label === "limited-key",
    );
    const goodKey = body.keys.find(
      (k: { label: string }) => k.label === "good-key",
    );

    expect(limitedKey).toBeDefined();
    expect(limitedKey.stats.rateLimitHits).toBeGreaterThanOrEqual(1);
    expect(limitedKey.isAvailable).toBe(false); // rate limited
    expect(limitedKey.capacity.retryAfterSecs).toBe(30);
    expect(limitedKey.capacity.overageStatus).toBe("rejected");
    expect(limitedKey.capacity.overageDisabledReason).toBe("out_of_credits");
    expect(limitedKey.capacity.normalizedHeaderCount).toBeGreaterThanOrEqual(1);
    expect(limitedKey.capacity.windows.find((w: { windowName: string }) => w.windowName === "unified")!.status).toBe("rejected");
    expect(limitedKey.capacity.windows.find((w: { windowName: string }) => w.windowName === "unified-overage")).toBeUndefined();
    expect(limitedKey.capacityHealth).toBe("cooling_down");
    expect(body.capacitySummary.coolingDownKeys).toBeGreaterThanOrEqual(1);
    expect(body.capacitySummary.rejectedKeys).toBe(0);

    expect(goodKey).toBeDefined();
    expect(goodKey.stats.successfulRequests).toBeGreaterThanOrEqual(1);

    // Totals should reflect both
    expect(body.totals.rateLimitHits).toBeGreaterThanOrEqual(1);
    expect(body.totals.successfulRequests).toBeGreaterThanOrEqual(1);
  });

  test("POST /admin/keys/reset-cooldowns puts cooled keys back into rotation", async () => {
    let stats = await fetch(`${proxy.url}/admin/stats`);
    let body = await stats.json();
    let limitedKey = body.keys.find(
      (k: { label: string }) => k.label === "limited-key",
    );
    expect(limitedKey).toBeDefined();

    if (limitedKey.isAvailable) {
      const prime = await fetch(`${proxy.url}/v1/messages`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: "claude-3-haiku-20240307",
          max_tokens: 100,
          messages: [{ role: "user", content: "test" }],
        }),
      });
      expect(prime.status).toBe(200);
      stats = await fetch(`${proxy.url}/admin/stats`);
      body = await stats.json();
      limitedKey = body.keys.find(
        (k: { label: string }) => k.label === "limited-key",
      );
      expect(limitedKey).toBeDefined();
    }

    expect(limitedKey.isAvailable).toBe(false);

    const reset = await fetch(`${proxy.url}/admin/keys/reset-cooldowns`, {
      method: "POST",
    });
    expect(reset.status).toBe(200);
    const resetBody = await reset.json();
    expect(resetBody.reset).toBeGreaterThanOrEqual(1);
    expect(resetBody.availableKeys).toBe(2);

    stats = await fetch(`${proxy.url}/admin/stats`);
    body = await stats.json();
    limitedKey = body.keys.find(
      (k: { label: string }) => k.label === "limited-key",
    );
    expect(limitedKey).toBeDefined();
    expect(limitedKey.isAvailable).toBe(true);
    expect(limitedKey.availableAt).toBe(0);
  });

  test("all keys rate-limited returns 429 with retry-after", async () => {
    const dir2 = makeTempDir();
    const allLimited = startMockUpstream(
      () =>
        new Response(
          JSON.stringify({ error: { type: "rate_limit_error" } }),
          {
            status: 429,
            headers: {
              "content-type": "application/json",
              "retry-after": "60",
            },
          },
        ),
    );
    const proxy2 = startProxy({ dataDir: dir2, upstream: allLimited.url });
    proxy2.km.addKey(FAKE_KEY_1, "limited-1");
    proxy2.km.addKey(FAKE_KEY_2, "limited-2");

    const res = await fetch(`${proxy2.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });

    expect(res.status).toBe(429);
    const retryAfter = res.headers.get("retry-after");
    expect(retryAfter).toBeDefined();
    expect(parseInt(retryAfter!, 10)).toBeGreaterThan(0);

    const body = await res.json();
    expect(body.error.type).toBe("proxy_error");
    expect(body.error.message).toBe("Too many requests.");

    proxy2.stop();
    allLimited.stop();
    cleanupTempDir(dir2);
  });
});

// ── Streaming Response End-to-End ────────────────────────────────

describe("Streaming Response End-to-End", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;

  beforeAll(() => {
    dataDir = makeTempDir();

    // The streaming upstream must handle the request body before responding.
    // We use startMockUpstreamWithBody to ensure the body is consumed.
    upstream = startMockUpstreamWithBody(() => {
      const encoder = new TextEncoder();
      const stream = new ReadableStream({
        start(controller) {
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({
                type: "message_start",
                message: {
                  id: "msg_stream_1",
                  type: "message",
                  role: "assistant",
                  content: [],
                  model: "claude-3-haiku-20240307",
                  usage: { input_tokens: 25, output_tokens: 0 },
                },
              })}\n\n`,
            ),
          );
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({
                type: "content_block_start",
                index: 0,
                content_block: { type: "text", text: "" },
              })}\n\n`,
            ),
          );
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({
                type: "content_block_delta",
                index: 0,
                delta: { type: "text_delta", text: "Hello streaming!" },
              })}\n\n`,
            ),
          );
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({
                type: "content_block_stop",
                index: 0,
              })}\n\n`,
            ),
          );
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({
                type: "message_delta",
                usage: { output_tokens: 12 },
                delta: { stop_reason: "end_turn", stop_sequence: null },
              })}\n\n`,
            ),
          );
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({ type: "message_stop" })}\n\n`,
            ),
          );

          controller.close();
        },
      });

      return new Response(stream, {
        status: 200,
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
        },
      });
    });

    proxy = startProxy({ dataDir, upstream: upstream.url });
    proxy.km.addKey(FAKE_KEY_1, "stream-key");
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("client receives complete SSE stream", async () => {
    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 100,
        stream: true,
        messages: [{ role: "user", content: "Hi" }],
      }),
    });

    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/event-stream");

    const text = await res.text();

    // Verify all SSE events are present
    expect(text).toContain("message_start");
    expect(text).toContain("content_block_start");
    expect(text).toContain("content_block_delta");
    expect(text).toContain("Hello streaming!");
    expect(text).toContain("content_block_stop");
    expect(text).toContain("message_delta");
    expect(text).toContain("message_stop");

    // Verify SSE format (data: prefix)
    const lines = text.split("\n").filter((l) => l.startsWith("data: "));
    expect(lines.length).toBeGreaterThanOrEqual(6);
  });

  test("token stats recorded after stream ends", async () => {
    // Make a request and read the full stream
    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 100,
        stream: true,
        messages: [{ role: "user", content: "count tokens" }],
      }),
    });
    // Read the full response to trigger flush
    await res.text();

    const stats = await fetch(`${proxy.url}/admin/stats`);
    const body = await stats.json();
    const key = body.keys.find(
      (k: { label: string }) => k.label === "stream-key",
    );

    expect(key).toBeDefined();
    expect(key.stats.totalTokensIn).toBeGreaterThanOrEqual(25);
    expect(key.stats.totalTokensOut).toBeGreaterThanOrEqual(12);
    expect(key.stats.successfulRequests).toBeGreaterThanOrEqual(1);
  });
});

// ── Error Responses ──────────────────────────────────────────────

describe("Error Responses", () => {
  test("503 when no keys configured", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(jsonOkHandler());
    const p = startProxy({ dataDir: dir, upstream: up.url });

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });

    expect(res.status).toBe(503);
    const body = await res.json();
    expect(body.error.type).toBe("proxy_error");
    expect(body.error.message).toBe("Service not available.");

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("502 when upstream unreachable", async () => {
    const dir = makeTempDir();
    // Point to a port that nothing is listening on
    const p = startProxy({
      dataDir: dir,
      upstream: "http://127.0.0.1:1",
    });
    p.km.addKey(FAKE_KEY_1, "unreachable-key");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });

    expect(res.status).toBe(502);
    await res.text();

    p.stop();
    cleanupTempDir(dir);
  });

  test("upstream 400 error forwarded to client", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(
      () =>
        new Response(
          JSON.stringify({
            error: {
              type: "invalid_request_error",
              message: "Bad request",
            },
          }),
          {
            status: 400,
            headers: { "content-type": "application/json" },
          },
        ),
    );
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "err-key");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ bad: "request" }),
    });

    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error.type).toBe("invalid_request_error");
    expect(body.error.message).toBe("Bad request");

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("upstream 500 error forwarded to client", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(
      () =>
        new Response(
          JSON.stringify({
            error: {
              type: "api_error",
              message: "Internal server error",
            },
          }),
          {
            status: 500,
            headers: { "content-type": "application/json" },
          },
        ),
    );
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "err-key-500");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });

    expect(res.status).toBe(500);
    const body = await res.json();
    expect(body.error.type).toBe("api_error");

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("upstream 400 increments error count on key stats", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(
      () =>
        new Response(
          JSON.stringify({ error: { type: "invalid_request_error" } }),
          { status: 400, headers: { "content-type": "application/json" } },
        ),
    );
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "err-stat-key");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    await res.text();

    const stats = await fetch(`${p.url}/admin/stats`);
    const body = await stats.json();
    expect(body.totals.errors).toBe(1);
    expect(body.totals.successfulRequests).toBe(0);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });
});

// ── SSE Event Stream ─────────────────────────────────────────────

describe("SSE Event Stream", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;

  beforeAll(() => {
    dataDir = makeTempDir();
    upstream = startMockUpstream(
      () =>
        new Response(
          JSON.stringify({
            id: "msg_sse_test",
            usage: { input_tokens: 15, output_tokens: 8 },
          }),
          { headers: { "content-type": "application/json" } },
        ),
    );
    proxy = startProxy({ dataDir, upstream: upstream.url });
    proxy.km.addKey(FAKE_KEY_1, "sse-key");
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("initial keys snapshot sent on connect", async () => {
    const controller = new AbortController();
    const res = await fetch(`${proxy.url}/admin/events`, {
      signal: controller.signal,
    });
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/event-stream");
    expect(res.headers.get("cache-control")).toBe("no-cache");

    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let accumulated = "";

    // Read until we get the initial snapshot
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      accumulated += decoder.decode(value, { stream: true });
      if (accumulated.includes("\n\n")) break;
    }

    controller.abort();
    try { reader.cancel(); } catch {}

    // Parse the first event
    const lines = accumulated
      .split("\n")
      .filter((l) => l.startsWith("data: "));
    expect(lines.length).toBeGreaterThanOrEqual(1);
    const firstEvent = JSON.parse(lines[0]!.slice(6));
    expect(firstEvent.type).toBe("keys");
    expect(firstEvent.ts).toBeDefined();
    expect(firstEvent.keys).toBeArray();
    expect(firstEvent.tokens).toBeArray();
  });

  test("receives request/response/tokens events after proxy request", async () => {
    const controller = new AbortController();
    const res = await fetch(`${proxy.url}/admin/events`, {
      signal: controller.signal,
    });

    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let accumulated = "";

    // Read initial snapshot
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      accumulated += decoder.decode(value, { stream: true });
      if (accumulated.includes("\n\n")) break;
    }

    // Now make a proxy request to generate events
    const proxyReqDone = fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    }).then((r) => r.text());

    // Collect events
    accumulated = "";
    const collectStart = Date.now();
    while (Date.now() - collectStart < 3000) {
      const readPromise = reader.read();
      const timeoutPromise = new Promise<{ value: undefined; done: true }>(
        (resolve) =>
          setTimeout(
            () => resolve({ value: undefined, done: true }),
            1000,
          ),
      );
      const { value, done } = await Promise.race([
        readPromise,
        timeoutPromise,
      ]);
      if (done || !value) break;
      accumulated += decoder.decode(value, { stream: true });
      // Check if we have enough events (request + response + tokens = 3)
      const dataLines = accumulated
        .split("\n")
        .filter((l) => l.startsWith("data: "));
      if (dataLines.length >= 3) break;
    }

    controller.abort();
    try { reader.cancel(); } catch {}
    await proxyReqDone;

    // Parse collected events
    const dataLines = accumulated
      .split("\n")
      .filter((l) => l.startsWith("data: "));
    const events = dataLines.map((l) => JSON.parse(l.slice(6)));
    const eventTypes = events.map(
      (e: { type: string }) => e.type,
    );

    expect(eventTypes).toContain("request");
    expect(eventTypes).toContain("response");

    // Verify request event structure
    const reqEvent = events.find(
      (e: { type: string }) => e.type === "request",
    );
    expect(reqEvent.label).toBe("sse-key");
    expect(reqEvent.method).toBe("POST");
    expect(reqEvent.path).toBe("/v1/messages");
    expect(reqEvent.ts).toBeDefined();
    // Events emitted with emitWithKeys include keys array
    expect(reqEvent.keys).toBeArray();

    // Verify response event
    const respEvent = events.find(
      (e: { type: string }) => e.type === "response",
    );
    expect(respEvent.label).toBe("sse-key");
    expect(respEvent.status).toBe(200);
    expect(respEvent.keys).toBeArray();
  });

  test("events include user label when proxy token is used", async () => {
    proxy.km.addToken(PROXY_TOKEN_ALICE, "alice");

    const controller = new AbortController();
    const res = await fetch(`${proxy.url}/admin/events`, {
      signal: controller.signal,
    });

    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let accumulated = "";

    // Read initial snapshot
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      accumulated += decoder.decode(value, { stream: true });
      if (accumulated.includes("\n\n")) break;
    }

    // Make authenticated request
    const proxyReqDone = fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": PROXY_TOKEN_ALICE,
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    }).then((r) => r.text());

    accumulated = "";
    const collectStart = Date.now();
    while (Date.now() - collectStart < 3000) {
      const readPromise = reader.read();
      const timeoutPromise = new Promise<{ value: undefined; done: true }>(
        (resolve) =>
          setTimeout(
            () => resolve({ value: undefined, done: true }),
            1000,
          ),
      );
      const { value, done } = await Promise.race([
        readPromise,
        timeoutPromise,
      ]);
      if (done || !value) break;
      accumulated += decoder.decode(value, { stream: true });
      const dataLines = accumulated
        .split("\n")
        .filter((l) => l.startsWith("data: "));
      if (dataLines.length >= 2) break;
    }

    controller.abort();
    try { reader.cancel(); } catch {}
    await proxyReqDone;

    const dataLines = accumulated
      .split("\n")
      .filter((l) => l.startsWith("data: "));
    const events = dataLines.map((l) => JSON.parse(l.slice(6)));

    const reqEvent = events.find(
      (e: { type: string }) => e.type === "request",
    );
    expect(reqEvent).toBeDefined();
    expect(reqEvent.user).toBe("alice");

    // Cleanup: remove alice token for other tests
    proxy.km.removeToken(PROXY_TOKEN_ALICE);
  });

  test("initial snapshot includes tokens list", async () => {
    proxy.km.addToken(PROXY_TOKEN_BOB, "bob");

    const controller = new AbortController();
    const res = await fetch(`${proxy.url}/admin/events`, {
      signal: controller.signal,
    });

    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let accumulated = "";

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      accumulated += decoder.decode(value, { stream: true });
      if (accumulated.includes("\n\n")) break;
    }

    controller.abort();
    try { reader.cancel(); } catch {}

    const lines = accumulated
      .split("\n")
      .filter((l) => l.startsWith("data: "));
    const firstEvent = JSON.parse(lines[0]!.slice(6));
    expect(firstEvent.type).toBe("keys");
    expect(firstEvent.tokens).toBeArray();
    expect(firstEvent.tokens.length).toBeGreaterThanOrEqual(1);
    const bobToken = firstEvent.tokens.find(
      (t: { label: string }) => t.label === "bob",
    );
    expect(bobToken).toBeDefined();

    proxy.km.removeToken(PROXY_TOKEN_BOB);
  });
});

// ── Per-User Stats ───────────────────────────────────────────────

describe("Per-User Stats", () => {
  let dataDir: string;
  let upstream: MockUpstream;
  let proxy: ProxyInstance;

  beforeAll(() => {
    dataDir = makeTempDir();
    upstream = startMockUpstream(
      () =>
        new Response(
          JSON.stringify({
            id: "msg_user_stats",
            usage: { input_tokens: 30, output_tokens: 20 },
          }),
          { headers: { "content-type": "application/json" } },
        ),
    );
    proxy = startProxy({ dataDir, upstream: upstream.url });
    proxy.km.addKey(FAKE_KEY_1, "user-stat-key");
  });

  afterAll(() => {
    proxy.stop();
    upstream.stop();
    cleanupTempDir(dataDir);
  });

  test("alice's stats are tracked per token", async () => {
    // Add alice token
    const addRes = await fetch(`${proxy.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: PROXY_TOKEN_ALICE, label: "alice" }),
    });
    expect(addRes.status).toBe(201);

    // Make 3 requests as alice
    for (let i = 0; i < 3; i++) {
      const res = await fetch(`${proxy.url}/v1/messages`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": PROXY_TOKEN_ALICE,
        },
        body: JSON.stringify({
          model: "claude-3-haiku-20240307",
          max_tokens: 100,
          messages: [{ role: "user", content: `request ${i}` }],
        }),
      });
      expect(res.status).toBe(200);
      await res.json();
    }

    // Check alice's stats
    const tokensRes = await fetch(`${proxy.url}/admin/tokens`);
    const tokensBody = await tokensRes.json();
    const alice = tokensBody.tokens.find(
      (t: { label: string }) => t.label === "alice",
    );

    expect(alice).toBeDefined();
    expect(alice.stats.totalRequests).toBe(3);
    expect(alice.stats.successfulRequests).toBe(3);
    expect(alice.stats.errors).toBe(0);
    expect(alice.stats.totalTokensIn).toBe(90); // 30 * 3
    expect(alice.stats.totalTokensOut).toBe(60); // 20 * 3
    expect(alice.stats.lastUsedAt).not.toBeNull();
  });

  test("bob has separate stats from alice", async () => {
    // Add bob token
    await fetch(`${proxy.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: PROXY_TOKEN_BOB, label: "bob" }),
    });

    // Make 1 request as bob
    const res = await fetch(`${proxy.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": PROXY_TOKEN_BOB,
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 100,
        messages: [{ role: "user", content: "hi from bob" }],
      }),
    });
    expect(res.status).toBe(200);
    await res.json();

    // Check bob's stats
    const tokensRes = await fetch(`${proxy.url}/admin/tokens`);
    const tokensBody = await tokensRes.json();
    const bob = tokensBody.tokens.find(
      (t: { label: string }) => t.label === "bob",
    );
    const alice = tokensBody.tokens.find(
      (t: { label: string }) => t.label === "alice",
    );

    expect(bob).toBeDefined();
    expect(bob.stats.totalRequests).toBe(1);
    expect(bob.stats.successfulRequests).toBe(1);
    expect(bob.stats.totalTokensIn).toBe(30);
    expect(bob.stats.totalTokensOut).toBe(20);

    // Alice unchanged
    expect(alice.stats.totalRequests).toBe(3);
  });

  test("token errors tracked per user on upstream failure", async () => {
    const dir2 = makeTempDir();
    const failUpstream = startMockUpstream(
      () =>
        new Response(
          JSON.stringify({
            error: { type: "api_error", message: "fail" },
          }),
          { status: 500, headers: { "content-type": "application/json" } },
        ),
    );
    const p2 = startProxy({ dataDir: dir2, upstream: failUpstream.url });
    p2.km.addKey(FAKE_KEY_1, "err-key");
    p2.km.addToken(PROXY_TOKEN_ALICE, "alice");

    const res = await fetch(`${p2.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": PROXY_TOKEN_ALICE,
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    expect(res.status).toBe(500);
    await res.text();

    const tokensRes = await fetch(`${p2.url}/admin/tokens`);
    const body = await tokensRes.json();
    const alice = body.tokens.find(
      (t: { label: string }) => t.label === "alice",
    );
    expect(alice.stats.totalRequests).toBe(1);
    expect(alice.stats.errors).toBe(1);
    expect(alice.stats.successfulRequests).toBe(0);

    p2.stop();
    failUpstream.stop();
    cleanupTempDir(dir2);
  });
});

// ── Stripped Headers ─────────────────────────────────────────────

describe("Header Handling", () => {
  test("hop-by-hop and auth headers stripped from upstream request", async () => {
    const dir = makeTempDir();
    let capturedHeaders: Record<string, string> = {};
    const up = startMockUpstreamWithBody((req) => {
      capturedHeaders = {};
      for (const [key, value] of req.headers.entries()) {
        capturedHeaders[key] = value;
      }
      return new Response(JSON.stringify({ ok: true }), {
        headers: { "content-type": "application/json" },
      });
    });
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "header-key");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": "should-be-stripped",
        authorization: "Bearer should-be-stripped",
        "x-custom-header": "should-survive",
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    await res.json();

    // The proxy should replace x-api-key with the real key
    expect(capturedHeaders["x-api-key"]).toBe(FAKE_KEY_1);
    // Custom headers should pass through
    expect(capturedHeaders["x-custom-header"]).toBe("should-survive");
    // anthropic-version should be set
    expect(capturedHeaders["anthropic-version"]).toBeDefined();

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("anthropic-version defaults to 2023-06-01 if not provided", async () => {
    const dir = makeTempDir();
    let capturedHeaders: Record<string, string> = {};
    const up = startMockUpstreamWithBody((req) => {
      capturedHeaders = {};
      for (const [key, value] of req.headers.entries()) {
        capturedHeaders[key] = value;
      }
      return new Response(JSON.stringify({ ok: true }), {
        headers: { "content-type": "application/json" },
      });
    });
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "version-key");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    await res.json();

    expect(capturedHeaders["anthropic-version"]).toBe("2023-06-01");

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("client-provided anthropic-version is forwarded", async () => {
    const dir = makeTempDir();
    let capturedHeaders: Record<string, string> = {};
    const up = startMockUpstreamWithBody((req) => {
      capturedHeaders = {};
      for (const [key, value] of req.headers.entries()) {
        capturedHeaders[key] = value;
      }
      return new Response(JSON.stringify({ ok: true }), {
        headers: { "content-type": "application/json" },
      });
    });
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "version-key2");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "anthropic-version": "2024-01-01",
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    await res.json();

    expect(capturedHeaders["anthropic-version"]).toBe("2024-01-01");

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("response custom headers forwarded to client", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(
      () =>
        new Response(JSON.stringify({ ok: true }), {
          headers: {
            "content-type": "application/json",
            "x-custom-response": "present",
          },
        }),
    );
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "resp-header-key");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    await res.text();

    // Custom response headers should be forwarded
    expect(res.headers.get("x-custom-response")).toBe("present");

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });
});

// ── Edge Cases ───────────────────────────────────────────────────

describe("Edge Cases", () => {
  test("GET request proxied without body", async () => {
    const dir = makeTempDir();
    let capturedMethod = "";
    const up = startMockUpstream((req) => {
      capturedMethod = req.method;
      return new Response(JSON.stringify({ models: [] }), {
        headers: { "content-type": "application/json" },
      });
    });
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "get-key");

    const res = await fetch(`${p.url}/v1/models`, { method: "GET" });
    expect(res.status).toBe(200);
    expect(capturedMethod).toBe("GET");
    const body = await res.json();
    expect(body.models).toEqual([]);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("upstream returns empty body (204)", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(
      () => new Response(null, { status: 204 }),
    );
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "empty-body-key");

    const res = await fetch(`${p.url}/v1/something`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(204);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("non-JSON response body records zero tokens", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(
      () =>
        new Response("plain text response", {
          status: 200,
          headers: { "content-type": "text/plain" },
        }),
    );
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "plain-key");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    expect(res.status).toBe(200);
    await res.text();

    const stats = await fetch(`${p.url}/admin/stats`);
    const body = await stats.json();
    // Still counts as a request and success
    expect(body.totals.totalRequests).toBe(1);
    expect(body.totals.successfulRequests).toBe(1);
    // But no tokens extracted from non-JSON
    expect(body.totals.totalTokensIn).toBe(0);
    expect(body.totals.totalTokensOut).toBe(0);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("non-admin paths that start with /admin are still admin", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(jsonOkHandler());
    const p = startProxy({
      dataDir: dir,
      upstream: up.url,
      adminToken: "secret",
    });

    // /admin/unknown should be caught by admin router and return 404
    const res = await fetch(`${p.url}/admin/unknown`, {
      headers: { authorization: "Bearer secret" },
    });
    expect(res.status).toBe(404);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("concurrent requests are handled", async () => {
    const dir = makeTempDir();
    let requestCount = 0;
    const up = startMockUpstream(async () => {
      requestCount++;
      await Bun.sleep(10);
      return new Response(
        JSON.stringify({
          id: `msg_concurrent_${requestCount}`,
          usage: { input_tokens: 1, output_tokens: 1 },
        }),
        { headers: { "content-type": "application/json" } },
      );
    });
    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "concurrent-key");

    const promises = Array.from({ length: 5 }, (_, i) =>
      fetch(`${p.url}/v1/messages`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: "claude-3-haiku-20240307",
          max_tokens: 1,
          messages: [{ role: "user", content: `concurrent ${i}` }],
        }),
      }),
    );

    const responses = await Promise.all(promises);
    for (const res of responses) {
      expect(res.status).toBe(200);
      await res.json();
    }

    expect(requestCount).toBe(5);

    const stats = await fetch(`${p.url}/admin/stats`);
    const body = await stats.json();
    expect(body.totals.totalRequests).toBe(5);
    expect(body.totals.successfulRequests).toBe(5);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });
});

// ── Data Persistence ─────────────────────────────────────────────

describe("Data Persistence", () => {
  test("keys persist across KeyManager restarts (SQLite)", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(jsonOkHandler());

    // First instance: add keys and tokens
    const p1 = startProxy({ dataDir: dir, upstream: up.url });
    p1.km.addKey(FAKE_KEY_1, "persist-key-1");
    p1.km.addKey(FAKE_KEY_2, "persist-key-2");
    p1.km.addToken(PROXY_TOKEN_ALICE, "alice");
    p1.stop();

    // Second instance: data should be there
    const p2 = startProxy({ dataDir: dir, upstream: up.url });

    const keysRes = await fetch(`${p2.url}/admin/keys`);
    const keysBody = await keysRes.json();
    expect(keysBody.keys).toHaveLength(2);
    expect(
      keysBody.keys.map((k: { label: string }) => k.label).sort(),
    ).toEqual(["persist-key-1", "persist-key-2"]);

    const tokensRes = await fetch(`${p2.url}/admin/tokens`);
    const tokensBody = await tokensRes.json();
    expect(tokensBody.tokens).toHaveLength(1);
    expect(tokensBody.tokens[0].label).toBe("alice");

    p2.stop();
    up.stop();
    cleanupTempDir(dir);
  });
});

// ── Streaming Token Tracking with Per-User ───────────────────────

describe("Streaming Token Tracking with Per-User Stats", () => {
  test("streaming response tracks tokens per user", async () => {
    const dir = makeTempDir();
    const encoder = new TextEncoder();
    const up = startMockUpstreamWithBody(() => {
      const stream = new ReadableStream({
        start(controller) {
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({
                type: "message_start",
                message: {
                  usage: { input_tokens: 50, output_tokens: 0 },
                },
              })}\n\n`,
            ),
          );
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({
                type: "content_block_delta",
                delta: { type: "text_delta", text: "Hello" },
              })}\n\n`,
            ),
          );
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({
                type: "message_delta",
                usage: { output_tokens: 35 },
              })}\n\n`,
            ),
          );
          controller.close();
        },
      });
      return new Response(stream, {
        headers: { "content-type": "text/event-stream" },
      });
    });

    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "stream-user-key");
    p.km.addToken(PROXY_TOKEN_ALICE, "alice");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": PROXY_TOKEN_ALICE,
      },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 100,
        stream: true,
        messages: [{ role: "user", content: "test" }],
      }),
    });

    // Must consume the full stream for flush to fire
    const text = await res.text();
    expect(text).toContain("Hello");

    // Check key stats
    const stats = await fetch(`${p.url}/admin/stats`);
    const statsBody = await stats.json();
    expect(statsBody.keys[0].stats.totalTokensIn).toBe(50);
    expect(statsBody.keys[0].stats.totalTokensOut).toBe(35);

    // Check alice's token stats
    const tokens = await fetch(`${p.url}/admin/tokens`);
    const tokensBody = await tokens.json();
    const alice = tokensBody.tokens.find(
      (t: { label: string }) => t.label === "alice",
    );
    expect(alice.stats.totalTokensIn).toBe(50);
    expect(alice.stats.totalTokensOut).toBe(35);
    expect(alice.stats.successfulRequests).toBe(1);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });
});

// ── Rate Limit with Retry-After Parsing ──────────────────────────

describe("Rate Limit Retry-After Parsing", () => {
  test("retry-after header is parsed from upstream 429", async () => {
    const dir = makeTempDir();
    let attempts = 0;
    const up = startMockUpstream(() => {
      attempts++;
      if (attempts === 1) {
        return new Response(
          JSON.stringify({ error: { type: "rate_limit_error" } }),
          {
            status: 429,
            headers: { "retry-after": "120" },
          },
        );
      }
      return new Response(JSON.stringify({ ok: true }), {
        headers: { "content-type": "application/json" },
      });
    });

    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "retry-key-1");
    p.km.addKey(FAKE_KEY_2, "retry-key-2");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    await res.text();

    // The rate-limited key should have its availableAt set into the future
    const stats = await fetch(`${p.url}/admin/stats`);
    const body = await stats.json();
    const limitedKey = body.keys.find(
      (k: { label: string }) => k.label === "retry-key-1",
    );
    expect(limitedKey).toBeDefined();
    expect(limitedKey.isAvailable).toBe(false);
    // availableAt should be about 120s in the future
    expect(limitedKey.availableAt).toBeGreaterThan(Date.now());

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("missing retry-after defaults to 60 seconds", async () => {
    const dir = makeTempDir();
    let attempts = 0;
    const up = startMockUpstream(() => {
      attempts++;
      if (attempts === 1) {
        // No retry-after header
        return new Response(
          JSON.stringify({ error: { type: "rate_limit_error" } }),
          { status: 429 },
        );
      }
      return new Response(JSON.stringify({ ok: true }), {
        headers: { "content-type": "application/json" },
      });
    });

    const p = startProxy({ dataDir: dir, upstream: up.url });
    p.km.addKey(FAKE_KEY_1, "no-retry-key-1");
    p.km.addKey(FAKE_KEY_2, "no-retry-key-2");

    const res = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-3-haiku-20240307",
        max_tokens: 1,
        messages: [],
      }),
    });
    await res.text();

    const stats = await fetch(`${p.url}/admin/stats`);
    const body = await stats.json();
    const limitedKey = body.keys.find(
      (k: { label: string }) => k.label === "no-retry-key-1",
    );
    expect(limitedKey).toBeDefined();
    expect(limitedKey.isAvailable).toBe(false);
    // Should be about 60s in the future (default)
    const diff = limitedKey.availableAt - Date.now();
    expect(diff).toBeGreaterThan(50000); // at least ~50s
    expect(diff).toBeLessThan(70000); // at most ~70s

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });
});

// ── Key and Token Masking ────────────────────────────────────────

describe("Key and Token Masking", () => {
  test("API keys are masked in list responses", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(jsonOkHandler());
    const p = startProxy({ dataDir: dir, upstream: up.url });

    await fetch(`${p.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_1 }),
    });

    const list = await fetch(`${p.url}/admin/keys`);
    const body = await list.json();
    const key = body.keys[0];

    // Masked key should show first 10 chars + ... + last 4 chars
    expect(key.maskedKey).toBe(
      `${FAKE_KEY_1.slice(0, 10)}...${FAKE_KEY_1.slice(-4)}`,
    );
    // Full key should NOT be present
    expect(JSON.stringify(body)).not.toContain(FAKE_KEY_1);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("proxy tokens are masked in list responses", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(jsonOkHandler());
    const p = startProxy({ dataDir: dir, upstream: up.url });

    await fetch(`${p.url}/admin/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: PROXY_TOKEN_ALICE, label: "alice" }),
    });

    const list = await fetch(`${p.url}/admin/tokens`);
    const body = await list.json();
    const token = body.tokens[0];

    // Masked token: first 4 chars + ... + last 4 chars
    expect(token.maskedToken).toBe(
      `${PROXY_TOKEN_ALICE.slice(0, 4)}...${PROXY_TOKEN_ALICE.slice(-4)}`,
    );
    // Full token should NOT be present
    expect(JSON.stringify(body)).not.toContain(PROXY_TOKEN_ALICE);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });
});

// ── Add Key/Token Response Format ────────────────────────────────

describe("Add Key/Token Response Format", () => {
  test("POST /admin/keys returns masked key in response", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(jsonOkHandler());
    const p = startProxy({ dataDir: dir, upstream: up.url });

    const res = await fetch(`${p.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_1, label: "my-key" }),
    });
    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.added).toBeDefined();
    expect(body.added.label).toBe("my-key");
    expect(body.added.maskedKey).toBe(
      `${FAKE_KEY_1.slice(0, 10)}...${FAKE_KEY_1.slice(-4)}`,
    );

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("adding key with non-string label returns 400", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(jsonOkHandler());
    const p = startProxy({ dataDir: dir, upstream: up.url });

    const res = await fetch(`${p.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_1, label: 123 }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain("label");

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });
});

// ── Schema Tracking End-to-End ─────────────────────────────────

describe("Schema Tracking End-to-End", () => {
  test("proxy request populates GET /admin/schema with tracked headers and fields", async () => {
    const dir = makeTempDir();

    // Mock upstream returns a realistic Claude API response
    const up = startMockUpstream(() =>
      new Response(
        JSON.stringify({
          id: "msg_e2e",
          type: "message",
          role: "assistant",
          content: [{ type: "text", text: "Hello" }],
          model: "claude-sonnet-4-20250514",
          stop_reason: "end_turn",
          usage: { input_tokens: 50, output_tokens: 20 },
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
            "x-request-id": "e2e-req-id",
            "anthropic-organization-id": "org-123",
          },
        },
      ),
    );

    const p = startProxy({ dataDir: dir, upstream: up.url });

    // 1. Add a key
    await fetch(`${p.url}/admin/keys`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: FAKE_KEY_1, label: "e2e-key" }),
    });

    // 2. Make a proxied request
    const proxyRes = await fetch(`${p.url}/v1/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": "doesnt-matter",
      },
      body: JSON.stringify({ model: "claude-sonnet-4-20250514", messages: [{ role: "user", content: "Hi" }] }),
    });
    expect(proxyRes.status).toBe(200);
    // Consume the body to ensure tracking completes
    await proxyRes.json();

    // 3. GET /admin/schema and verify tracked data
    const schemaRes = await fetch(`${p.url}/admin/schema`);
    expect(schemaRes.status).toBe(200);
    const schema = (await schemaRes.json()) as {
      headers: { name: string; sampleValues: string[]; hitCount: number }[];
      fields: { endpoint: string; context: string; path: string; jsonTypes: string[] }[];
    };

    // Verify headers were tracked
    const headerNames = schema.headers.map((h) => h.name);
    expect(headerNames).toContain("content-type");
    expect(headerNames).toContain("x-request-id");
    expect(headerNames).toContain("anthropic-organization-id");

    // Verify response body fields were tracked
    expect(schema.fields.length).toBeGreaterThan(0);
    const fieldPaths = schema.fields.map((f) => f.path);
    expect(fieldPaths).toContain("id");
    expect(fieldPaths).toContain("type");
    expect(fieldPaths).toContain("model");
    expect(fieldPaths).toContain("stop_reason");
    expect(fieldPaths).toContain("usage.input_tokens");
    expect(fieldPaths).toContain("usage.output_tokens");
    expect(fieldPaths).toContain("content[].type");

    // Verify fields are associated with the correct endpoint
    const idField = schema.fields.find((f) => f.path === "id");
    expect(idField!.endpoint).toBe("/v1/messages");
    expect(idField!.context).toBe("response");
    expect(idField!.jsonTypes).toContain("string");

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("GET /admin/schema returns empty arrays with no prior requests", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(jsonOkHandler());
    const p = startProxy({ dataDir: dir, upstream: up.url });

    const res = await fetch(`${p.url}/admin/schema`);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { headers: unknown[]; fields: unknown[] };
    expect(body.headers).toEqual([]);
    expect(body.fields).toEqual([]);

    p.stop();
    up.stop();
    cleanupTempDir(dir);
  });

  test("proxy request delivers schema changes to dynamically-added webhook", async () => {
    const dir = makeTempDir();

    const webhookPayloads: { text: string; changes: unknown[] }[] = [];
    const webhookServer = Bun.serve({
      port: 0,
      async fetch(req) {
        webhookPayloads.push((await req.json()) as { text: string; changes: unknown[] });
        return new Response("ok");
      },
    });

    const up = startMockUpstream(() =>
      new Response(
        JSON.stringify({
          id: "msg_wh",
          type: "message",
          usage: { input_tokens: 10, output_tokens: 5 },
        }),
        { status: 200, headers: { "content-type": "application/json", "x-request-id": "wh-req-1" } },
      ),
    );

    const p = startProxy({ dataDir: dir, upstream: up.url });

    try {
      // Add a key
      await fetch(`${p.url}/admin/keys`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ key: FAKE_KEY_1, label: "wh-key" }),
      });

      // Add a webhook via admin API
      const addRes = await fetch(`${p.url}/admin/schema/webhooks`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ url: `http://localhost:${webhookServer.port}`, label: "e2e-hook" }),
      });
      expect(addRes.status).toBe(200);

      // Proxy a request — triggers schema changes and webhook delivery
      const proxyRes = await fetch(`${p.url}/v1/messages`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "doesnt-matter" },
        body: JSON.stringify({ model: "claude-sonnet-4-20250514", messages: [{ role: "user", content: "Hi" }] }),
      });
      expect(proxyRes.status).toBe(200);
      await proxyRes.json();

      // Wait for webhook batch window (default 5s) to expire and deliver
      await new Promise((r) => setTimeout(r, 6_000));

      expect(webhookPayloads.length).toBeGreaterThanOrEqual(1);
      const allChanges = webhookPayloads.flatMap((p) => p.changes) as { type: string }[];
      // Only header changes should be delivered to webhooks (not body field changes)
      expect(allChanges.some((c) => c.type === "new_header")).toBe(true);
      expect(allChanges.every((c) => c.type === "new_header" || c.type === "new_header_value")).toBe(true);
    } finally {
      p.stop();
      up.stop();
      webhookServer.stop(true);
      cleanupTempDir(dir);
    }
  }, 15_000);

  test("webhook CRUD lifecycle via admin API", async () => {
    const dir = makeTempDir();
    const up = startMockUpstream(jsonOkHandler());
    const p = startProxy({ dataDir: dir, upstream: up.url });

    try {
      // Initially empty
      const listRes1 = await fetch(`${p.url}/admin/schema/webhooks`);
      const list1 = (await listRes1.json()) as { webhooks: unknown[] };
      expect(list1.webhooks).toHaveLength(0);

      // Add two webhooks
      await fetch(`${p.url}/admin/schema/webhooks`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ url: "http://example.com/hook1", label: "first" }),
      });
      await fetch(`${p.url}/admin/schema/webhooks`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ url: "http://example.com/hook2", label: "second" }),
      });

      const listRes2 = await fetch(`${p.url}/admin/schema/webhooks`);
      const list2 = (await listRes2.json()) as { webhooks: { url: string; label: string }[] };
      expect(list2.webhooks).toHaveLength(2);

      // Remove one
      await fetch(`${p.url}/admin/schema/webhooks/remove`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ url: "http://example.com/hook1" }),
      });

      const listRes3 = await fetch(`${p.url}/admin/schema/webhooks`);
      const list3 = (await listRes3.json()) as { webhooks: { url: string }[] };
      expect(list3.webhooks).toHaveLength(1);
      expect(list3.webhooks[0]!.url).toBe("http://example.com/hook2");
    } finally {
      p.stop();
      up.stop();
      cleanupTempDir(dir);
    }
  });
});
