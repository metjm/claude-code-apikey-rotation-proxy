import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { handleAdminRoute } from "../src/admin.ts";
import { KeyManager } from "../src/key-manager.ts";
import { SchemaTracker } from "../src/schema-tracker.ts";
import type { ProxyConfig } from "../src/types.ts";
import { unixMs } from "../src/types.ts";

// ── Helpers ───────────────────────────────────────────────────────

const VALID_KEY = "sk-ant-api03-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
const VALID_KEY_2 = "sk-ant-api03-YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY";
const VALID_TOKEN = "my-proxy-token-12345678";
const VALID_TOKEN_2 = "another-proxy-token-abc";
const ADMIN_SECRET = "test-admin-secret-token";

function makeReq(
  method: string,
  path: string,
  body?: unknown,
  headers?: Record<string, string>,
): Request {
  const opts: RequestInit = { method, headers: { ...headers } };
  if (body !== undefined) {
    opts.body = JSON.stringify(body);
    (opts.headers as Record<string, string>)["content-type"] =
      "application/json";
  }
  return new Request(`http://localhost${path}`, opts);
}

function makeAuthedReq(
  method: string,
  path: string,
  body?: unknown,
): Request {
  return makeReq(method, path, body, {
    authorization: `Bearer ${ADMIN_SECRET}`,
  });
}

async function jsonBody(res: Response): Promise<unknown> {
  return res.json();
}

function makeConfig(overrides?: Partial<ProxyConfig>): ProxyConfig {
  return {
    port: 3000,
    upstream: "https://api.anthropic.com",
    adminToken: null,
    dataDir: "/tmp",
    maxRetriesPerRequest: 3,
    firstChunkTimeoutMs: 16_000,
    streamIdleTimeoutMs: 120_000,
    maxFirstChunkRetries: 2,
    webhookUrl: null,
    ...overrides,
  };
}

function makeTempDir(): string {
  return mkdtempSync(join(tmpdir(), "admin-test-"));
}

// ── Test suite ───────────────────────────────────────────────────

let tempDir: string;
let km: KeyManager;
let st: SchemaTracker;

beforeEach(() => {
  tempDir = makeTempDir();
  km = new KeyManager(tempDir);
  st = new SchemaTracker(km.dbPath);
});

afterEach(() => {
  try { st.close(); } catch {}
  try { km.close(); } catch {}
  try {
    rmSync(tempDir, { recursive: true, force: true });
  } catch {}
});

// ═══════════════════════════════════════════════════════════════════
// Route Dispatch
// ═══════════════════════════════════════════════════════════════════

describe("Route Dispatch", () => {
  const config = makeConfig();

  test("returns null for non-admin path /v1/messages", async () => {
    const req = makeReq("GET", "/v1/messages");
    const result = await handleAdminRoute(req, km, config, st);
    expect(result).toBeNull();
  });

  test("returns null for root path /", async () => {
    const req = makeReq("GET", "/");
    const result = await handleAdminRoute(req, km, config, st);
    expect(result).toBeNull();
  });

  test("returns null for non-admin path /health", async () => {
    const req = makeReq("GET", "/health");
    const result = await handleAdminRoute(req, km, config, st);
    expect(result).toBeNull();
  });

  test("returns null for path that starts with /admin but not /admin/", async () => {
    const req = makeReq("GET", "/administrator");
    const result = await handleAdminRoute(req, km, config, st);
    expect(result).toBeNull();
  });

  test("routes GET /admin/keys", async () => {
    const req = makeReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const body = await jsonBody(res!);
    expect(body).toHaveProperty("keys");
  });

  test("routes POST /admin/keys", async () => {
    const req = makeReq("POST", "/admin/keys", { key: VALID_KEY });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(201);
  });

  test("routes POST /admin/keys/remove", async () => {
    const req = makeReq("POST", "/admin/keys/remove", { key: "nonexistent" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    // Returns 404 because the key does not exist, but route was matched
    expect(res!.status).toBe(404);
  });

  test("routes POST /admin/keys/reset-cooldowns", async () => {
    km.addKey(VALID_KEY, "reset-route");
    const req = makeReq("POST", "/admin/keys/reset-cooldowns");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const body = await jsonBody(res!);
    expect(body).toHaveProperty("reset");
    expect(body).toHaveProperty("availableKeys");
  });

  test("routes GET /admin/tokens", async () => {
    const req = makeReq("GET", "/admin/tokens");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const body = await jsonBody(res!);
    expect(body).toHaveProperty("tokens");
  });

  test("routes POST /admin/tokens", async () => {
    const req = makeReq("POST", "/admin/tokens", { token: VALID_TOKEN });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(201);
  });

  test("routes POST /admin/tokens/remove", async () => {
    const req = makeReq("POST", "/admin/tokens/remove", {
      token: "nonexistent",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(404);
  });

  test("routes GET /admin/stats", async () => {
    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const body = await jsonBody(res!);
    expect(body).toHaveProperty("totals");
    expect(body).toHaveProperty("capacitySummary");
  });

  test("routes GET /admin/capacity/timeseries", async () => {
    const req = makeReq("GET", "/admin/capacity/timeseries");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const body = await jsonBody(res!);
    expect(body).toHaveProperty("buckets");
  });

  test("routes GET /admin/health", async () => {
    const req = makeReq("GET", "/admin/health");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const body = await jsonBody(res!);
    expect(body).toHaveProperty("status");
  });

  test("routes GET /admin/events", async () => {
    const controller = new AbortController();
    const req = new Request("http://localhost/admin/events", {
      method: "GET",
      signal: controller.signal,
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    expect(res!.headers.get("content-type")).toBe("text/event-stream");
    controller.abort();
  });

  test("returns 404 for unknown admin path /admin/unknown", async () => {
    const req = makeReq("GET", "/admin/unknown");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(404);
    const body = await jsonBody(res!);
    expect(body).toEqual({ error: "Not found" });
  });

  test("returns 404 for /admin/keys/foo", async () => {
    const req = makeReq("GET", "/admin/keys/foo");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(404);
  });

  test("returns 405 for DELETE /admin/keys", async () => {
    const req = makeReq("DELETE", "/admin/keys");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(405);
    const body = await jsonBody(res!);
    expect(body).toEqual({ error: "Method not allowed" });
  });

  test("returns 405 for PUT /admin/keys", async () => {
    const req = makeReq("PUT", "/admin/keys");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(405);
  });

  test("returns 405 for PATCH /admin/stats", async () => {
    const req = makeReq("PATCH", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(405);
  });

  test("returns 405 for POST /admin/health", async () => {
    const req = makeReq("POST", "/admin/health");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(405);
  });

  test("returns 405 for DELETE /admin/tokens", async () => {
    const req = makeReq("DELETE", "/admin/tokens");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(405);
  });

  test("returns 405 for GET /admin/keys/remove", async () => {
    const req = makeReq("GET", "/admin/keys/remove");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(405);
  });

  test("returns 405 for GET /admin/tokens/remove", async () => {
    const req = makeReq("GET", "/admin/tokens/remove");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(405);
  });
});

describe("Capacity admin payloads", () => {
  test("GET /admin/stats includes keys with capacity state and a pool summary", async () => {
    const entry = km.addKey(VALID_KEY, "cap-admin");
    const baseNow = Date.now();
    km.recordCapacityObservation(entry, {
      seenAt: unixMs(baseNow),
      httpStatus: 200,
      organizationId: "org-admin",
      windows: [{ windowName: "unified", status: "allowed_warning", utilization: 0.75, resetAt: unixMs(Date.now() + 60_000) }],
    });

    const res = await handleAdminRoute(makeReq("GET", "/admin/stats"), km, makeConfig(), st);
    const body = await jsonBody(res!);
    expect(body.keys[0].capacity.organizationId).toBe("org-admin");
    expect(body.keys[0].capacity.responseCount).toBe(1);
    expect(body.keys[0].capacity.normalizedHeaderCount).toBe(1);
    expect(body.keys[0].capacity.signalCoverage[0].signalName).toBe("organization");
    expect(body.keys[0].capacityHealth).toBe("warning");
    expect(body.capacitySummary.healthyKeys).toBe(0);
    expect(body.capacitySummary.warningKeys).toBe(1);
    expect(body.capacitySummary.rejectedKeys).toBe(0);
    expect(body.capacitySummary.windows[0].windowName).toBe("unified");
  });

  test("GET /admin/stats includes recent sessions per key", async () => {
    const originalNow = Date.now;
    let fakeNow = 1_000_000;
    Date.now = () => fakeNow;

    try {
      km.addKey(VALID_KEY, "cap-admin-a");
      km.addKey(VALID_KEY_2, "cap-admin-b");

      // Bucket-of-3: first three sessions land on the alphabetically-first key,
      // the fourth rolls over to the next account.
      expect(km.getKeyForConversation("user-1:session-a", "session-a").entry?.key).toBe(VALID_KEY);
      expect(km.getKeyForConversation("user-1:session-b", "session-b").entry?.key).toBe(VALID_KEY);
      expect(km.getKeyForConversation("user-1:session-c", "session-c").entry?.key).toBe(VALID_KEY);
      expect(km.getKeyForConversation("user-1:session-d", "session-d").entry?.key).toBe(VALID_KEY_2);

      const res = await handleAdminRoute(makeReq("GET", "/admin/stats"), km, makeConfig(), st);
      const body = await jsonBody(res!) as {
        keys: Array<{ label: string; recentSessions15m: Array<{ sessionId: string }> }>;
      };

      const keyed = new Map(body.keys.map((key) => [String(key.label), key.recentSessions15m.map((session) => session.sessionId).sort()]));
      expect(keyed.get("cap-admin-a")).toEqual(["session-a", "session-b", "session-c"]);
      expect(keyed.get("cap-admin-b")).toEqual(["session-d"]);
    } finally {
      Date.now = originalNow;
    }
  });

  test("GET /admin/stats keeps successful-response rejected telemetry out of hard-rejection counts", async () => {
    const entry = km.addKey(VALID_KEY, "cap-observed-rejected");
    const baseNow = Date.now();
    km.recordCapacityObservation(entry, {
      seenAt: unixMs(baseNow),
      httpStatus: 200,
      organizationId: "org-admin",
      overageStatus: "rejected",
      overageDisabledReason: "out_of_credits",
      windows: [
        {
          windowName: "unified-7d",
          status: "rejected",
          utilization: 1,
          resetAt: unixMs(Date.now() + 60_000),
        },
      ],
    });

    const res = await handleAdminRoute(makeReq("GET", "/admin/stats"), km, makeConfig(), st);
    const body = await jsonBody(res!) as {
      availableKeys: number;
      keys: Array<{
        isAvailable: boolean;
        capacityHealth: string;
        capacity: {
          overageStatus: string | null;
          overageDisabledReason: string | null;
          windows: Array<{ status: string }>;
        };
      }>;
      capacitySummary: {
        warningKeys: number;
        rejectedKeys: number;
        overageRejectedKeys: number;
        windows: Array<{ windowName: string; rejectedKeys: number }>;
      };
    };

    expect(body.availableKeys).toBe(1);
    expect(body.keys).toHaveLength(1);
    expect(body.keys[0]!.isAvailable).toBe(true);
    expect(body.keys[0]!.capacityHealth).toBe("warning");
    expect(body.keys[0]!.capacity.overageStatus).toBe("rejected");
    expect(body.keys[0]!.capacity.overageDisabledReason).toBe("out_of_credits");
    expect(body.keys[0]!.capacity.windows[0]!.status).toBe("allowed_warning");
    expect(body.capacitySummary.warningKeys).toBe(1);
    expect(body.capacitySummary.rejectedKeys).toBe(0);
    expect(body.capacitySummary.overageRejectedKeys).toBe(1);
    expect(body.capacitySummary.windows[0]!.windowName).toBe("unified-7d");
    expect(body.capacitySummary.windows[0]!.warningKeys).toBe(1);
    expect(body.capacitySummary.windows[0]!.rejectedKeys).toBe(0);
  });

  test("GET /admin/capacity/timeseries returns rollups by window", async () => {
    const entry = km.addKey(VALID_KEY, "cap-ts");
    km.recordCapacityObservation(entry, {
      seenAt: unixMs(1_000),
      httpStatus: 200,
      windows: [{ windowName: "unified", status: "allowed", utilization: 0.4, resetAt: unixMs(Date.now() + 60_000) }],
    });
    km.close();
    km = new KeyManager(tempDir);

    const res = await handleAdminRoute(makeReq("GET", "/admin/capacity/timeseries?hours=24&resolution=hour&key=cap-ts"), km, makeConfig(), st);
    const body = await jsonBody(res!);
    expect(body.buckets).toHaveLength(1);
    expect(body.buckets[0].windowName).toBe("unified");
    expect(body.buckets[0].allowed).toBe(1);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Admin Auth
// ═══════════════════════════════════════════════════════════════════

describe("Admin Auth", () => {
  const authedConfig = makeConfig({ adminToken: ADMIN_SECRET });

  test("returns 401 without token when ADMIN_TOKEN is set", async () => {
    const req = makeReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
    const body = await jsonBody(res!);
    expect(body).toEqual({ error: "Unauthorized" });
  });

  test("returns 401 with wrong Bearer token", async () => {
    const req = makeReq("GET", "/admin/keys", undefined, {
      authorization: "Bearer wrong-token",
    });
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
  });

  test("returns 401 with token but missing Bearer prefix", async () => {
    const req = makeReq("GET", "/admin/keys", undefined, {
      authorization: ADMIN_SECRET,
    });
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
  });

  test("returns 401 for POST /admin/keys without auth", async () => {
    const req = makeReq("POST", "/admin/keys", { key: VALID_KEY });
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
  });

  test("returns 401 for POST /admin/keys/remove without auth", async () => {
    const req = makeReq("POST", "/admin/keys/remove", { key: VALID_KEY });
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
  });

  test("returns 401 for GET /admin/stats without auth", async () => {
    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
  });

  test("returns 401 for GET /admin/tokens without auth", async () => {
    const req = makeReq("GET", "/admin/tokens");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
  });

  test("returns 401 for POST /admin/tokens without auth", async () => {
    const req = makeReq("POST", "/admin/tokens", { token: VALID_TOKEN });
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
  });

  test("returns 401 for POST /admin/tokens/remove without auth", async () => {
    const req = makeReq("POST", "/admin/tokens/remove", {
      token: VALID_TOKEN,
    });
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
  });

  test("accepts correct Bearer token", async () => {
    const req = makeAuthedReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
  });

  test("accepts correct Bearer token for POST", async () => {
    const req = makeAuthedReq("POST", "/admin/keys", { key: VALID_KEY });
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(201);
  });

  test("skips auth for /admin/health even when ADMIN_TOKEN set", async () => {
    const req = makeReq("GET", "/admin/health");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const body = await jsonBody(res!);
    expect(body).toHaveProperty("status");
  });

  test("skips auth for /admin/events even when ADMIN_TOKEN set", async () => {
    const controller = new AbortController();
    const req = new Request("http://localhost/admin/events", {
      method: "GET",
      signal: controller.signal,
    });
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    expect(res!.headers.get("content-type")).toBe("text/event-stream");
    controller.abort();
  });

  test("skips auth entirely when adminToken is null", async () => {
    const noAuthConfig = makeConfig({ adminToken: null });
    const req = makeReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, noAuthConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
  });

  test("auth check happens before 404/405 for protected paths", async () => {
    // Even a wrong method on a valid path should still return 401 first
    const req = makeReq("DELETE", "/admin/keys");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    // Auth is checked before method dispatch, so we get 401
    expect(res!.status).toBe(401);
  });
});

// ═══════════════════════════════════════════════════════════════════
// GET /admin/keys
// ═══════════════════════════════════════════════════════════════════

describe("GET /admin/keys", () => {
  const config = makeConfig();

  test("returns 200 with empty keys array", async () => {
    const req = makeReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    expect(res!.headers.get("content-type")).toBe("application/json");
    const body = (await jsonBody(res!)) as { keys: unknown[] };
    expect(body.keys).toEqual([]);
  });

  test("returns masked key data after adding keys", async () => {
    km.addKey(VALID_KEY, "my-key");

    const req = makeReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    const body = (await jsonBody(res!)) as { keys: Record<string, unknown>[] };
    expect(body.keys).toHaveLength(1);

    const k = body.keys[0]!;
    expect(k["maskedKey"]).toBe(`${VALID_KEY.slice(0, 10)}...${VALID_KEY.slice(-4)}`);
    expect(k["label"]).toBe("my-key");
    // Must NOT contain the raw key
    expect(k).not.toHaveProperty("key");
  });

  test("includes all stats fields", async () => {
    km.addKey(VALID_KEY);

    const req = makeReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as { keys: Record<string, unknown>[] };
    const k = body.keys[0]!;
    const stats = k["stats"] as Record<string, unknown>;

    expect(stats).toHaveProperty("totalRequests");
    expect(stats).toHaveProperty("successfulRequests");
    expect(stats).toHaveProperty("rateLimitHits");
    expect(stats).toHaveProperty("errors");
    expect(stats).toHaveProperty("lastUsedAt");
    expect(stats).toHaveProperty("addedAt");
    expect(stats).toHaveProperty("totalTokensIn");
    expect(stats).toHaveProperty("totalTokensOut");
    expect(stats["totalRequests"]).toBe(0);
    expect(stats["successfulRequests"]).toBe(0);
    expect(stats["rateLimitHits"]).toBe(0);
    expect(stats["errors"]).toBe(0);
    expect(stats["lastUsedAt"]).toBeNull();
    expect(typeof stats["addedAt"]).toBe("number");
    expect(stats["totalTokensIn"]).toBe(0);
    expect(stats["totalTokensOut"]).toBe(0);
  });

  test("includes availability status", async () => {
    km.addKey(VALID_KEY);

    const req = makeReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as { keys: Record<string, unknown>[] };
    const k = body.keys[0]!;

    expect(k).toHaveProperty("availableAt");
    expect(k).toHaveProperty("isAvailable");
    expect(k["isAvailable"]).toBe(true);
    expect(k["availableAt"]).toBe(0);
  });

  test("returns multiple keys", async () => {
    km.addKey(VALID_KEY, "first-key");
    km.addKey(VALID_KEY_2, "second-key");

    const req = makeReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as { keys: Record<string, unknown>[] };
    expect(body.keys).toHaveLength(2);
    const labels = body.keys.map((k) => k["label"]);
    expect(labels).toContain("first-key");
    expect(labels).toContain("second-key");
  });
});

// ═══════════════════════════════════════════════════════════════════
// POST /admin/keys
// ═══════════════════════════════════════════════════════════════════

describe("POST /admin/keys", () => {
  const config = makeConfig();

  test("returns 201 with added key info", async () => {
    const req = makeReq("POST", "/admin/keys", { key: VALID_KEY });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(201);
    expect(res!.headers.get("content-type")).toBe("application/json");
    const body = (await jsonBody(res!)) as {
      added: { label: string; maskedKey: string };
    };
    expect(body.added).toBeDefined();
    expect(body.added.maskedKey).toBe(
      `${VALID_KEY.slice(0, 10)}...${VALID_KEY.slice(-4)}`,
    );
    // Default label pattern: key-N
    expect(body.added.label).toBe("key-1");
  });

  test("accepts optional label", async () => {
    const req = makeReq("POST", "/admin/keys", {
      key: VALID_KEY,
      label: "production-key",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(201);
    const body = (await jsonBody(res!)) as {
      added: { label: string; maskedKey: string };
    };
    expect(body.added.label).toBe("production-key");
  });

  test("returns 400 for missing key field", async () => {
    const req = makeReq("POST", "/admin/keys", { label: "no-key" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Missing or empty 'key' field");
  });

  test("returns 400 for empty key string", async () => {
    const req = makeReq("POST", "/admin/keys", { key: "" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Missing or empty 'key' field");
  });

  test("returns 400 for non-string key (number)", async () => {
    const req = makeReq("POST", "/admin/keys", { key: 12345 });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Missing or empty 'key' field");
  });

  test("returns 400 for non-string key (null)", async () => {
    const req = makeReq("POST", "/admin/keys", { key: null });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
  });

  test("returns 400 for non-string key (boolean)", async () => {
    const req = makeReq("POST", "/admin/keys", { key: true });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
  });

  test("returns 400 for non-string label", async () => {
    const req = makeReq("POST", "/admin/keys", {
      key: VALID_KEY,
      label: 12345,
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("'label' must be a string");
  });

  test("returns 400 for non-string label (boolean)", async () => {
    const req = makeReq("POST", "/admin/keys", {
      key: VALID_KEY,
      label: false,
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("'label' must be a string");
  });

  test("returns 400 for invalid key format (not sk-ant-*)", async () => {
    const req = makeReq("POST", "/admin/keys", { key: "invalid-api-key" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("sk-ant-");
  });

  test("returns 400 for duplicate key", async () => {
    km.addKey(VALID_KEY, "first");

    const req = makeReq("POST", "/admin/keys", { key: VALID_KEY });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("already registered");
  });

  test("returns 400 for invalid JSON body", async () => {
    const req = new Request("http://localhost/admin/keys", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "not json at all {{{",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Invalid JSON body");
  });

  test("key is actually stored after adding", async () => {
    const req = makeReq("POST", "/admin/keys", { key: VALID_KEY });
    await handleAdminRoute(req, km, config, st);

    // Verify via list
    const listReq = makeReq("GET", "/admin/keys");
    const listRes = await handleAdminRoute(listReq, km, config, st);
    const body = (await jsonBody(listRes!)) as {
      keys: Record<string, unknown>[];
    };
    expect(body.keys).toHaveLength(1);
  });

  test("second key gets default label key-2", async () => {
    km.addKey(VALID_KEY);
    const req = makeReq("POST", "/admin/keys", { key: VALID_KEY_2 });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(201);
    const body = (await jsonBody(res!)) as {
      added: { label: string; maskedKey: string };
    };
    expect(body.added.label).toBe("key-2");
  });
});

// ═══════════════════════════════════════════════════════════════════
// POST /admin/keys/remove
// ═══════════════════════════════════════════════════════════════════

describe("POST /admin/keys/remove", () => {
  const config = makeConfig();

  test("returns 200 with { removed: true }", async () => {
    km.addKey(VALID_KEY, "to-remove");

    const req = makeReq("POST", "/admin/keys/remove", { key: VALID_KEY });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    const body = await jsonBody(res!);
    expect(body).toEqual({ removed: true });
  });

  test("key is actually gone after removal", async () => {
    km.addKey(VALID_KEY);
    const removeReq = makeReq("POST", "/admin/keys/remove", {
      key: VALID_KEY,
    });
    await handleAdminRoute(removeReq, km, config, st);

    const listReq = makeReq("GET", "/admin/keys");
    const listRes = await handleAdminRoute(listReq, km, config, st);
    const body = (await jsonBody(listRes!)) as {
      keys: unknown[];
    };
    expect(body.keys).toHaveLength(0);
  });

  test("returns 404 for unknown key", async () => {
    const req = makeReq("POST", "/admin/keys/remove", {
      key: "sk-ant-api03-nonexistent-key",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(404);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Key not found");
  });

  test("returns 400 for missing key field", async () => {
    const req = makeReq("POST", "/admin/keys/remove", { notKey: "value" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("'key'");
  });

  test("returns 400 for non-string key field", async () => {
    const req = makeReq("POST", "/admin/keys/remove", { key: 12345 });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
  });

  test("returns 400 for invalid JSON body", async () => {
    const req = new Request("http://localhost/admin/keys/remove", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{{broken json",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Invalid JSON body");
  });

  test("returns 400 for empty body", async () => {
    const req = new Request("http://localhost/admin/keys/remove", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
  });
});

// ═══════════════════════════════════════════════════════════════════
// GET /admin/tokens
// ═══════════════════════════════════════════════════════════════════

describe("GET /admin/tokens", () => {
  const config = makeConfig();

  test("returns 200 with empty tokens array", async () => {
    const req = makeReq("GET", "/admin/tokens");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    expect(res!.headers.get("content-type")).toBe("application/json");
    const body = (await jsonBody(res!)) as { tokens: unknown[] };
    expect(body.tokens).toEqual([]);
  });

  test("returns masked token data after adding tokens", async () => {
    km.addToken(VALID_TOKEN, "alice");

    const req = makeReq("GET", "/admin/tokens");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    const body = (await jsonBody(res!)) as {
      tokens: Record<string, unknown>[];
    };
    expect(body.tokens).toHaveLength(1);

    const t = body.tokens[0]!;
    expect(t["maskedToken"]).toBe(
      `${VALID_TOKEN.slice(0, 4)}...${VALID_TOKEN.slice(-4)}`,
    );
    expect(t["label"]).toBe("alice");
    // Must NOT contain raw token
    expect(t).not.toHaveProperty("token");
  });

  test("includes all stats fields", async () => {
    km.addToken(VALID_TOKEN);

    const req = makeReq("GET", "/admin/tokens");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      tokens: Record<string, unknown>[];
    };
    const t = body.tokens[0]!;
    const stats = t["stats"] as Record<string, unknown>;

    expect(stats).toHaveProperty("totalRequests");
    expect(stats).toHaveProperty("successfulRequests");
    expect(stats).toHaveProperty("errors");
    expect(stats).toHaveProperty("lastUsedAt");
    expect(stats).toHaveProperty("addedAt");
    expect(stats).toHaveProperty("totalTokensIn");
    expect(stats).toHaveProperty("totalTokensOut");
    expect(stats["totalRequests"]).toBe(0);
    expect(stats["successfulRequests"]).toBe(0);
    expect(stats["errors"]).toBe(0);
    expect(stats["lastUsedAt"]).toBeNull();
    expect(typeof stats["addedAt"]).toBe("number");
    expect(stats["totalTokensIn"]).toBe(0);
    expect(stats["totalTokensOut"]).toBe(0);
  });

  test("returns multiple tokens", async () => {
    km.addToken(VALID_TOKEN, "alice");
    km.addToken(VALID_TOKEN_2, "bob");

    const req = makeReq("GET", "/admin/tokens");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      tokens: Record<string, unknown>[];
    };
    expect(body.tokens).toHaveLength(2);
    const labels = body.tokens.map((t) => t["label"]);
    expect(labels).toContain("alice");
    expect(labels).toContain("bob");
  });
});

// ═══════════════════════════════════════════════════════════════════
// POST /admin/tokens
// ═══════════════════════════════════════════════════════════════════

describe("POST /admin/tokens", () => {
  const config = makeConfig();

  test("returns 201 with added token info", async () => {
    const req = makeReq("POST", "/admin/tokens", { token: VALID_TOKEN });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(201);
    expect(res!.headers.get("content-type")).toBe("application/json");
    const body = (await jsonBody(res!)) as { added: { label: string } };
    expect(body.added).toBeDefined();
    expect(body.added.label).toBe("user-1");
  });

  test("accepts optional label", async () => {
    const req = makeReq("POST", "/admin/tokens", {
      token: VALID_TOKEN,
      label: "alice-token",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(201);
    const body = (await jsonBody(res!)) as { added: { label: string } };
    expect(body.added.label).toBe("alice-token");
  });

  test("returns 400 for missing token field", async () => {
    const req = makeReq("POST", "/admin/tokens", { label: "no-token" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Missing or empty 'token' field");
  });

  test("returns 400 for empty token string", async () => {
    const req = makeReq("POST", "/admin/tokens", { token: "" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Missing or empty 'token' field");
  });

  test("returns 400 for non-string token (number)", async () => {
    const req = makeReq("POST", "/admin/tokens", { token: 99999 });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Missing or empty 'token' field");
  });

  test("returns 400 for non-string token (null)", async () => {
    const req = makeReq("POST", "/admin/tokens", { token: null });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
  });

  test("returns 400 for non-string label", async () => {
    const req = makeReq("POST", "/admin/tokens", {
      token: VALID_TOKEN,
      label: 12345,
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("'label' must be a string");
  });

  test("returns 400 for non-string label (object)", async () => {
    const req = makeReq("POST", "/admin/tokens", {
      token: VALID_TOKEN,
      label: { name: "bad" },
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("'label' must be a string");
  });

  test("returns 400 for token too short (< 8 chars)", async () => {
    const req = makeReq("POST", "/admin/tokens", { token: "short" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("at least 8 characters");
  });

  test("returns 400 for token exactly 7 chars", async () => {
    const req = makeReq("POST", "/admin/tokens", { token: "1234567" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
  });

  test("accepts token exactly 8 chars", async () => {
    const req = makeReq("POST", "/admin/tokens", { token: "12345678" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(201);
  });

  test("returns 400 for duplicate token", async () => {
    km.addToken(VALID_TOKEN, "first");

    const req = makeReq("POST", "/admin/tokens", { token: VALID_TOKEN });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("already registered");
  });

  test("returns 400 for invalid JSON body", async () => {
    const req = new Request("http://localhost/admin/tokens", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{{{not json",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Invalid JSON body");
  });

  test("token is actually stored after adding", async () => {
    const addReq = makeReq("POST", "/admin/tokens", { token: VALID_TOKEN });
    await handleAdminRoute(addReq, km, config, st);

    const listReq = makeReq("GET", "/admin/tokens");
    const listRes = await handleAdminRoute(listReq, km, config, st);
    const body = (await jsonBody(listRes!)) as {
      tokens: Record<string, unknown>[];
    };
    expect(body.tokens).toHaveLength(1);
  });

  test("second token gets default label user-2", async () => {
    km.addToken(VALID_TOKEN);
    const req = makeReq("POST", "/admin/tokens", { token: VALID_TOKEN_2 });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(201);
    const body = (await jsonBody(res!)) as { added: { label: string } };
    expect(body.added.label).toBe("user-2");
  });
});

// ═══════════════════════════════════════════════════════════════════
// POST /admin/tokens/remove
// ═══════════════════════════════════════════════════════════════════

describe("POST /admin/tokens/remove", () => {
  const config = makeConfig();

  test("returns 200 with { removed: true }", async () => {
    km.addToken(VALID_TOKEN, "to-remove");

    const req = makeReq("POST", "/admin/tokens/remove", {
      token: VALID_TOKEN,
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    const body = await jsonBody(res!);
    expect(body).toEqual({ removed: true });
  });

  test("token is actually gone after removal", async () => {
    km.addToken(VALID_TOKEN);
    const removeReq = makeReq("POST", "/admin/tokens/remove", {
      token: VALID_TOKEN,
    });
    await handleAdminRoute(removeReq, km, config, st);

    const listReq = makeReq("GET", "/admin/tokens");
    const listRes = await handleAdminRoute(listReq, km, config, st);
    const body = (await jsonBody(listRes!)) as { tokens: unknown[] };
    expect(body.tokens).toHaveLength(0);
  });

  test("returns 404 for unknown token", async () => {
    const req = makeReq("POST", "/admin/tokens/remove", {
      token: "nonexistent-token-value",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(404);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Token not found");
  });

  test("returns 400 for missing token field", async () => {
    const req = makeReq("POST", "/admin/tokens/remove", {
      notToken: "value",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("'token'");
  });

  test("returns 400 for non-string token field", async () => {
    const req = makeReq("POST", "/admin/tokens/remove", { token: 12345 });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
  });

  test("returns 400 for invalid JSON body", async () => {
    const req = new Request("http://localhost/admin/tokens/remove", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{{broken json",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
    const body = (await jsonBody(res!)) as { error: string };
    expect(body.error).toContain("Invalid JSON body");
  });

  test("returns 400 for empty body", async () => {
    const req = new Request("http://localhost/admin/tokens/remove", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "",
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
  });
});

// ═══════════════════════════════════════════════════════════════════
// POST /admin/keys/update
// ═══════════════════════════════════════════════════════════════════

describe("POST /admin/keys/update", () => {
  const config = makeConfig();

  test("returns 200 and updates label", async () => {
    km.addKey("sk-ant-api03-key-to-rename-0000", "old-name");
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { key: "sk-ant-api03-key-to-rename-0000", label: "new-name" }), km, config, st)!;
    expect(res!.status).toBe(200);
    const body = await res!.json();
    expect(body.updated).toBe(true);
    expect(body.label).toBe("new-name");
    // Verify it stuck
    const keys = km.listKeys();
    expect(keys.some(k => k.label === "new-name")).toBe(true);
  });

  test("returns 404 for unknown key", async () => {
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { key: "sk-ant-api03-nonexistent-999", label: "x" }), km, config, st)!;
    expect(res!.status).toBe(404);
  });

  test("returns 400 for missing key field", async () => {
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { label: "x" }), km, config, st)!;
    expect(res!.status).toBe(400);
  });

  test("returns 400 when neither label nor priority is provided", async () => {
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { key: "sk-ant-api03-x" }), km, config, st)!;
    expect(res!.status).toBe(400);
  });

  test("returns 400 for empty label", async () => {
    km.addKey("sk-ant-api03-empty-label-test-0", "something");
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { key: "sk-ant-api03-empty-label-test-0", label: "" }), km, config, st)!;
    expect(res!.status).toBe(400);
  });

  test("returns 400 for non-string label", async () => {
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { key: "sk-ant-api03-x", label: 123 }), km, config, st)!;
    expect(res!.status).toBe(400);
  });

  test("returns 400 for invalid JSON", async () => {
    const req = new Request("http://localhost/admin/keys/update", { method: "POST", body: "not json", headers: { "content-type": "application/json" } });
    const res = await handleAdminRoute(req, km, config, st)!;
    expect(res!.status).toBe(400);
  });

  test("updates priority via full key", async () => {
    km.addKey("sk-ant-api03-priority-test-0000", "pri-test");
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { key: "sk-ant-api03-priority-test-0000", priority: 1 }), km, config, st)!;
    expect(res!.status).toBe(200);
    const body = await res!.json();
    expect(body.updated).toBe(true);
    expect(body.priority).toBe(1);
    expect(km.listKeys().find(k => k.label === "pri-test")!.priority).toBe(1);
  });

  test("updates priority via masked key", async () => {
    km.addKey("sk-ant-api03-masked-priority-test", "masked-pri");
    const masked = km.listKeys().find(k => k.label === "masked-pri")!.maskedKey;
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { maskedKey: masked, priority: 3 }), km, config, st)!;
    expect(res!.status).toBe(200);
    expect(km.listKeys().find(k => k.label === "masked-pri")!.priority).toBe(3);
  });

  test("returns 400 for invalid priority value", async () => {
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { key: "sk-ant-api03-x", priority: 5 }), km, config, st)!;
    expect(res!.status).toBe(400);
  });

  test("accepts priority 4 (disabled) and excludes the key from rotation", async () => {
    km.addKey("sk-ant-api03-disable-me-via-admin0", "to-disable");
    const res = await handleAdminRoute(
      makeReq("POST", "/admin/keys/update", { key: "sk-ant-api03-disable-me-via-admin0", priority: 4 }),
      km, config, st,
    )!;
    expect(res!.status).toBe(200);
    expect(km.listKeys().find(k => k.label === "to-disable")!.priority).toBe(4);
    expect(km.getNextAvailableKey()).toBeNull();
  });

  test("updates both label and priority together", async () => {
    km.addKey("sk-ant-api03-both-update-test-00", "old");
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { key: "sk-ant-api03-both-update-test-00", label: "new", priority: 1 }), km, config, st)!;
    expect(res!.status).toBe(200);
    const entry = km.listKeys().find(k => k.label === "new");
    expect(entry).toBeDefined();
    expect(entry!.priority).toBe(1);
  });

  test("label update works via masked key", async () => {
    km.addKey("sk-ant-api03-label-needs-key-000", "lbl");
    const masked = km.listKeys().find(k => k.label === "lbl")!.maskedKey;
    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/update", { maskedKey: masked, label: "new-lbl" }), km, config, st)!;
    expect(res!.status).toBe(200);
    expect(km.listKeys().find(k => k.label === "new-lbl")).toBeDefined();
  });
});

describe("POST /admin/keys/reset-cooldowns", () => {
  const config = makeConfig();

  test("returns 200 and makes cooled keys available again", async () => {
    const cooled = km.addKey("sk-ant-api03-cooldown-reset-test-0000", "cooled");
    km.addKey("sk-ant-api03-cooldown-reset-test-1111", "ready");
    km.recordRateLimit(cooled, 300);

    const res = await handleAdminRoute(makeReq("POST", "/admin/keys/reset-cooldowns"), km, config, st)!;
    expect(res!.status).toBe(200);
    const body = await res!.json();
    expect(body.reset).toBe(1);
    expect(body.availableKeys).toBe(2);
    expect(body.totalKeys).toBe(2);
    expect(km.listKeys().every(k => k.isAvailable)).toBe(true);
  });
});

// POST /admin/tokens/update
// ═══════════════════════════════════════════════════════════════════

describe("POST /admin/tokens/update", () => {
  const config = makeConfig();

  test("returns 200 and updates label", async () => {
    km.addToken("old-token-rename-test", "old-name");
    const res = await handleAdminRoute(makeReq("POST", "/admin/tokens/update", { token: "old-token-rename-test", label: "alice@co.com" }), km, config, st)!;
    expect(res!.status).toBe(200);
    const body = await res!.json();
    expect(body.updated).toBe(true);
    expect(body.label).toBe("alice@co.com");
    const tokens = km.listTokens();
    expect(tokens.some(t => t.label === "alice@co.com")).toBe(true);
  });

  test("returns 404 for unknown token", async () => {
    const res = await handleAdminRoute(makeReq("POST", "/admin/tokens/update", { token: "nonexistent-token-value", label: "x" }), km, config, st)!;
    expect(res!.status).toBe(404);
  });

  test("returns 400 for missing token field", async () => {
    const res = await handleAdminRoute(makeReq("POST", "/admin/tokens/update", { label: "x" }), km, config, st)!;
    expect(res!.status).toBe(400);
  });

  test("returns 400 for missing label field", async () => {
    const res = await handleAdminRoute(makeReq("POST", "/admin/tokens/update", { token: "something" }), km, config, st)!;
    expect(res!.status).toBe(400);
  });

  test("returns 400 for empty label", async () => {
    km.addToken("empty-label-token-test", "something");
    const res = await handleAdminRoute(makeReq("POST", "/admin/tokens/update", { token: "empty-label-token-test", label: "" }), km, config, st)!;
    expect(res!.status).toBe(400);
  });

  test("returns 400 for non-string label", async () => {
    const res = await handleAdminRoute(makeReq("POST", "/admin/tokens/update", { token: "x", label: 42 }), km, config, st)!;
    expect(res!.status).toBe(400);
  });
});

// GET /admin/stats
// ═══════════════════════════════════════════════════════════════════

describe("GET /admin/stats", () => {
  const config = makeConfig();

  test("returns 200 with aggregated stats structure", async () => {
    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    expect(res!.headers.get("content-type")).toBe("application/json");
    const body = (await jsonBody(res!)) as Record<string, unknown>;
    expect(body).toHaveProperty("keyCount");
    expect(body).toHaveProperty("availableKeys");
    expect(body).toHaveProperty("totals");
    expect(body).toHaveProperty("keys");
  });

  test("returns zeros when no keys", async () => {
    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      keyCount: number;
      availableKeys: number;
      totals: Record<string, number>;
      keys: unknown[];
    };

    expect(body.keyCount).toBe(0);
    expect(body.availableKeys).toBe(0);
    expect(body.totals.totalRequests).toBe(0);
    expect(body.totals.successfulRequests).toBe(0);
    expect(body.totals.rateLimitHits).toBe(0);
    expect(body.totals.errors).toBe(0);
    expect(body.totals.totalTokensIn).toBe(0);
    expect(body.totals.totalTokensOut).toBe(0);
    expect(body.keys).toEqual([]);
  });

  test("keyCount is correct", async () => {
    km.addKey(VALID_KEY);
    km.addKey(VALID_KEY_2);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as { keyCount: number };
    expect(body.keyCount).toBe(2);
  });

  test("availableKeys is correct", async () => {
    km.addKey(VALID_KEY);
    km.addKey(VALID_KEY_2);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as { availableKeys: number };
    expect(body.availableKeys).toBe(2);
  });

  test("availableKeys excludes rate-limited keys", async () => {
    const entry1 = km.addKey(VALID_KEY);
    km.addKey(VALID_KEY_2);

    // Rate-limit the first key
    km.recordRateLimit(entry1, 60);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as { availableKeys: number; keyCount: number };
    expect(body.keyCount).toBe(2);
    expect(body.availableKeys).toBe(1);
  });

  test("totals.totalRequests sums across keys", async () => {
    const entry1 = km.addKey(VALID_KEY);
    const entry2 = km.addKey(VALID_KEY_2);

    km.recordRequest(entry1);
    km.recordRequest(entry1);
    km.recordRequest(entry2);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      totals: Record<string, number>;
    };
    expect(body.totals.totalRequests).toBe(3);
  });

  test("totals.successfulRequests sums across keys", async () => {
    const entry1 = km.addKey(VALID_KEY);
    const entry2 = km.addKey(VALID_KEY_2);

    km.recordSuccess(entry1, 100, 200);
    km.recordSuccess(entry2, 50, 75);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      totals: Record<string, number>;
    };
    expect(body.totals.successfulRequests).toBe(2);
  });

  test("totals.rateLimitHits sums across keys", async () => {
    const entry1 = km.addKey(VALID_KEY);
    const entry2 = km.addKey(VALID_KEY_2);

    km.recordRateLimit(entry1, 30);
    km.recordRateLimit(entry2, 60);
    km.recordRateLimit(entry2, 60);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      totals: Record<string, number>;
    };
    expect(body.totals.rateLimitHits).toBe(3);
  });

  test("totals.errors sums across keys", async () => {
    const entry1 = km.addKey(VALID_KEY);
    const entry2 = km.addKey(VALID_KEY_2);

    km.recordError(entry1);
    km.recordError(entry1);
    km.recordError(entry2);
    km.recordError(entry2);
    km.recordError(entry2);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      totals: Record<string, number>;
    };
    expect(body.totals.errors).toBe(5);
  });

  test("totals.totalTokensIn sums across keys", async () => {
    const entry1 = km.addKey(VALID_KEY);
    const entry2 = km.addKey(VALID_KEY_2);

    km.recordSuccess(entry1, 100, 0);
    km.recordSuccess(entry1, 200, 0);
    km.recordSuccess(entry2, 300, 0);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      totals: Record<string, number>;
    };
    expect(body.totals.totalTokensIn).toBe(600);
  });

  test("totals.totalTokensOut sums across keys", async () => {
    const entry1 = km.addKey(VALID_KEY);
    const entry2 = km.addKey(VALID_KEY_2);

    km.recordSuccess(entry1, 0, 500);
    km.recordSuccess(entry2, 0, 750);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      totals: Record<string, number>;
    };
    expect(body.totals.totalTokensOut).toBe(1250);
  });

  test("keys array in stats contains masked entries", async () => {
    km.addKey(VALID_KEY, "stats-key");

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      keys: Record<string, unknown>[];
    };
    expect(body.keys).toHaveLength(1);
    expect(body.keys[0]).toHaveProperty("maskedKey");
    expect(body.keys[0]).toHaveProperty("label");
    expect(body.keys[0]).toHaveProperty("stats");
    expect(body.keys[0]).toHaveProperty("isAvailable");
    expect(body.keys[0]).not.toHaveProperty("key");
  });

  test("combined stats scenario with mixed operations", async () => {
    const entry1 = km.addKey(VALID_KEY);
    const entry2 = km.addKey(VALID_KEY_2);

    km.recordRequest(entry1);
    km.recordRequest(entry2);
    km.recordSuccess(entry1, 100, 200);
    km.recordError(entry2);
    km.recordRateLimit(entry2, 30);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      keyCount: number;
      totals: Record<string, number>;
    };

    expect(body.keyCount).toBe(2);
    expect(body.totals.totalRequests).toBe(2);
    expect(body.totals.successfulRequests).toBe(1);
    expect(body.totals.errors).toBe(1);
    expect(body.totals.rateLimitHits).toBe(1);
    expect(body.totals.totalTokensIn).toBe(100);
    expect(body.totals.totalTokensOut).toBe(200);
  });
});

// ═══════════════════════════════════════════════════════════════════
// GET /admin/health
// ═══════════════════════════════════════════════════════════════════

describe("GET /admin/health", () => {
  const config = makeConfig();

  test("returns status no_keys when empty", async () => {
    const req = makeReq("GET", "/admin/health");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    expect(res!.headers.get("content-type")).toBe("application/json");
    const body = (await jsonBody(res!)) as {
      status: string;
      keys: { total: number; available: number };
    };
    expect(body.status).toBe("no_keys");
    expect(body.keys.total).toBe(0);
    expect(body.keys.available).toBe(0);
  });

  test("returns status ok when keys exist", async () => {
    km.addKey(VALID_KEY);

    const req = makeReq("GET", "/admin/health");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      status: string;
      keys: { total: number; available: number };
    };
    expect(body.status).toBe("ok");
  });

  test("returns correct total and available counts", async () => {
    km.addKey(VALID_KEY);
    km.addKey(VALID_KEY_2);

    const req = makeReq("GET", "/admin/health");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      status: string;
      keys: { total: number; available: number };
    };
    expect(body.keys.total).toBe(2);
    expect(body.keys.available).toBe(2);
  });

  test("available count decreases when key is rate-limited", async () => {
    const entry = km.addKey(VALID_KEY);
    km.addKey(VALID_KEY_2);
    km.recordRateLimit(entry, 60);

    const req = makeReq("GET", "/admin/health");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      status: string;
      keys: { total: number; available: number };
    };
    expect(body.keys.total).toBe(2);
    expect(body.keys.available).toBe(1);
  });

  test("still returns ok even when some keys are rate-limited", async () => {
    const entry = km.addKey(VALID_KEY);
    km.addKey(VALID_KEY_2);
    km.recordRateLimit(entry, 300);

    const req = makeReq("GET", "/admin/health");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as { status: string };
    expect(body.status).toBe("ok");
  });

  test("returns ok even when all keys are rate-limited (total > 0)", async () => {
    const entry = km.addKey(VALID_KEY);
    km.recordRateLimit(entry, 300);

    const req = makeReq("GET", "/admin/health");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      status: string;
      keys: { total: number; available: number };
    };
    // status is based on totalCount > 0, not availableCount
    expect(body.status).toBe("ok");
    expect(body.keys.total).toBe(1);
    expect(body.keys.available).toBe(0);
  });
});

// ═══════════════════════════════════════════════════════════════════
// GET /admin/events (SSE)
// ═══════════════════════════════════════════════════════════════════

describe("GET /admin/events", () => {
  const config = makeConfig();

  test("returns 200 with text/event-stream content-type", async () => {
    const controller = new AbortController();
    const req = new Request("http://localhost/admin/events", {
      method: "GET",
      signal: controller.signal,
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    expect(res!.headers.get("content-type")).toBe("text/event-stream");
    expect(res!.headers.get("cache-control")).toBe("no-cache");
    expect(res!.headers.get("connection")).toBe("keep-alive");
    controller.abort();
  });

  test("sends initial keys snapshot event", async () => {
    km.addKey(VALID_KEY, "event-key");

    const controller = new AbortController();
    const req = new Request("http://localhost/admin/events", {
      method: "GET",
      signal: controller.signal,
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();

    const reader = res!.body!.getReader();

    // Read the first chunk (the initial snapshot)
    // The stream enqueues strings directly, so value is already a string
    const { value, done } = await reader.read();
    expect(done).toBe(false);
    const text = value as unknown as string;

    // SSE format: data: JSON\n\n
    expect(text).toStartWith("data: ");
    expect(text).toEndWith("\n\n");

    // Parse the JSON payload
    const jsonStr = text.replace(/^data: /, "").replace(/\n\n$/, "");
    const event = JSON.parse(jsonStr) as Record<string, unknown>;
    expect(event.type).toBe("keys");
    expect(event.ts).toBeDefined();
    expect(typeof event.ts).toBe("string");

    // Should include keys array
    const keys = event.keys as Record<string, unknown>[];
    expect(keys).toBeInstanceOf(Array);
    expect(keys).toHaveLength(1);
    expect(keys[0]).toHaveProperty("maskedKey");
    expect(keys[0]).toHaveProperty("label");
    expect(keys[0]!["label"]).toBe("event-key");

    controller.abort();
    reader.releaseLock();
  });

  test("includes tokens in initial snapshot", async () => {
    km.addToken(VALID_TOKEN, "event-token");

    const controller = new AbortController();
    const req = new Request("http://localhost/admin/events", {
      method: "GET",
      signal: controller.signal,
    });
    const res = await handleAdminRoute(req, km, config, st);

    const reader = res!.body!.getReader();

    const { value } = await reader.read();
    const text = value as unknown as string;
    const jsonStr = text.replace(/^data: /, "").replace(/\n\n$/, "");
    const event = JSON.parse(jsonStr) as Record<string, unknown>;

    expect(event.type).toBe("keys");
    const tokens = event.tokens as Record<string, unknown>[];
    expect(tokens).toBeInstanceOf(Array);
    expect(tokens).toHaveLength(1);
    expect(tokens[0]).toHaveProperty("maskedToken");
    expect(tokens[0]!["label"]).toBe("event-token");

    controller.abort();
    reader.releaseLock();
  });

  test("sends initial snapshot with empty keys and tokens", async () => {
    const controller = new AbortController();
    const req = new Request("http://localhost/admin/events", {
      method: "GET",
      signal: controller.signal,
    });
    const res = await handleAdminRoute(req, km, config, st);

    const reader = res!.body!.getReader();

    const { value } = await reader.read();
    const text = value as unknown as string;
    const jsonStr = text.replace(/^data: /, "").replace(/\n\n$/, "");
    const event = JSON.parse(jsonStr) as Record<string, unknown>;

    expect(event.type).toBe("keys");
    expect(event.keys).toEqual([]);
    expect(event.tokens).toEqual([]);

    controller.abort();
    reader.releaseLock();
  });

  test("formats events as SSE with data: prefix and double newline", async () => {
    const controller = new AbortController();
    const req = new Request("http://localhost/admin/events", {
      method: "GET",
      signal: controller.signal,
    });
    const res = await handleAdminRoute(req, km, config, st);

    const reader = res!.body!.getReader();

    const { value } = await reader.read();
    const text = value as unknown as string;

    // Verify exact SSE format
    const lines = text.split("\n");
    // Should be "data: {...}", "", "" (the split of \n\n)
    expect(lines[0]).toStartWith("data: ");
    // The content after "data: " should be valid JSON
    const jsonPart = lines[0]!.slice(6);
    expect(() => JSON.parse(jsonPart)).not.toThrow();

    controller.abort();
    reader.releaseLock();
  });
});

// ═══════════════════════════════════════════════════════════════════
// Response format checks
// ═══════════════════════════════════════════════════════════════════

describe("Response format", () => {
  const config = makeConfig();

  test("all JSON responses have application/json content-type", async () => {
    const endpoints: [string, string][] = [
      ["GET", "/admin/keys"],
      ["GET", "/admin/tokens"],
      ["GET", "/admin/stats"],
      ["GET", "/admin/health"],
    ];

    for (const [method, path] of endpoints) {
      const req = makeReq(method, path);
      const res = await handleAdminRoute(req, km, config, st);
      expect(res!.headers.get("content-type")).toBe("application/json");
    }
  });

  test("error responses have application/json content-type", async () => {
    const req = makeReq("GET", "/admin/unknown");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.headers.get("content-type")).toBe("application/json");
  });

  test("401 responses have application/json content-type", async () => {
    const authedConfig = makeConfig({ adminToken: ADMIN_SECRET });
    const req = makeReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res!.headers.get("content-type")).toBe("application/json");
  });

  test("405 responses have application/json content-type", async () => {
    const req = makeReq("DELETE", "/admin/keys");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.headers.get("content-type")).toBe("application/json");
  });

  test("JSON responses are pretty-printed (indented)", async () => {
    const req = makeReq("GET", "/admin/health");
    const res = await handleAdminRoute(req, km, config, st);
    const text = await res!.text();
    // JSON.stringify with null, 2 produces indented output
    expect(text).toContain("\n");
    expect(text).toContain("  ");
  });
});

// ═══════════════════════════════════════════════════════════════════
// Cross-cutting: keys and tokens together
// ═══════════════════════════════════════════════════════════════════

describe("Cross-cutting scenarios", () => {
  const config = makeConfig();

  test("adding and removing keys does not affect tokens", async () => {
    km.addToken(VALID_TOKEN, "my-token");
    km.addKey(VALID_KEY, "my-key");
    km.removeKey(VALID_KEY);

    const req = makeReq("GET", "/admin/tokens");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      tokens: Record<string, unknown>[];
    };
    expect(body.tokens).toHaveLength(1);
    expect(body.tokens[0]!["label"]).toBe("my-token");
  });

  test("adding and removing tokens does not affect keys", async () => {
    km.addKey(VALID_KEY, "my-key");
    km.addToken(VALID_TOKEN, "my-token");
    km.removeToken(VALID_TOKEN);

    const req = makeReq("GET", "/admin/keys");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      keys: Record<string, unknown>[];
    };
    expect(body.keys).toHaveLength(1);
    expect(body.keys[0]!["label"]).toBe("my-key");
  });

  test("stats reflect only keys, not tokens", async () => {
    km.addKey(VALID_KEY);
    km.addToken(VALID_TOKEN);

    const req = makeReq("GET", "/admin/stats");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as { keyCount: number };
    // keyCount counts only API keys
    expect(body.keyCount).toBe(1);
  });

  test("health reflects only keys for total/available", async () => {
    km.addKey(VALID_KEY);
    km.addToken(VALID_TOKEN);

    const req = makeReq("GET", "/admin/health");
    const res = await handleAdminRoute(req, km, config, st);
    const body = (await jsonBody(res!)) as {
      keys: { total: number; available: number };
    };
    expect(body.keys.total).toBe(1);
    expect(body.keys.available).toBe(1);
  });

  test("full CRUD workflow for keys via admin routes", async () => {
    // 1. List (empty)
    let res = await handleAdminRoute(makeReq("GET", "/admin/keys"), km, config, st);
    let body = (await jsonBody(res!)) as { keys: unknown[] };
    expect(body.keys).toHaveLength(0);

    // 2. Add
    res = await handleAdminRoute(
      makeReq("POST", "/admin/keys", { key: VALID_KEY, label: "test" }),
      km,
      config,
      st,
    );
    expect(res!.status).toBe(201);

    // 3. List (one key)
    res = await handleAdminRoute(makeReq("GET", "/admin/keys"), km, config, st);
    body = (await jsonBody(res!)) as { keys: unknown[] };
    expect(body.keys).toHaveLength(1);

    // 4. Remove
    res = await handleAdminRoute(
      makeReq("POST", "/admin/keys/remove", { key: VALID_KEY }),
      km,
      config,
      st,
    );
    expect(res!.status).toBe(200);

    // 5. List (empty again)
    res = await handleAdminRoute(makeReq("GET", "/admin/keys"), km, config, st);
    body = (await jsonBody(res!)) as { keys: unknown[] };
    expect(body.keys).toHaveLength(0);
  });

  test("full CRUD workflow for tokens via admin routes", async () => {
    // 1. List (empty)
    let res = await handleAdminRoute(
      makeReq("GET", "/admin/tokens"),
      km,
      config,
      st,
    );
    let body = (await jsonBody(res!)) as { tokens: unknown[] };
    expect(body.tokens).toHaveLength(0);

    // 2. Add
    res = await handleAdminRoute(
      makeReq("POST", "/admin/tokens", {
        token: VALID_TOKEN,
        label: "alice",
      }),
      km,
      config,
      st,
    );
    expect(res!.status).toBe(201);

    // 3. List (one token)
    res = await handleAdminRoute(
      makeReq("GET", "/admin/tokens"),
      km,
      config,
      st,
    );
    body = (await jsonBody(res!)) as { tokens: unknown[] };
    expect(body.tokens).toHaveLength(1);

    // 4. Remove
    res = await handleAdminRoute(
      makeReq("POST", "/admin/tokens/remove", { token: VALID_TOKEN }),
      km,
      config,
      st,
    );
    expect(res!.status).toBe(200);

    // 5. List (empty again)
    res = await handleAdminRoute(
      makeReq("GET", "/admin/tokens"),
      km,
      config,
      st,
    );
    body = (await jsonBody(res!)) as { tokens: unknown[] };
    expect(body.tokens).toHaveLength(0);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Schema Admin Endpoints
// ═══════════════════════════════════════════════════════════════════

describe("POST /admin/schema/webhooks/test (no webhook)", () => {
  const config = makeConfig();

  test("returns 422 with sent:false when no webhook URL configured", async () => {
    const req = makeReq("POST", "/admin/schema/webhooks/test");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(422);
    const body = await jsonBody(res!);
    expect(body).toEqual({ sent: false, error: "No webhook URL configured" });
  });
});

describe("Schema endpoints require authentication", () => {
  const authedConfig = makeConfig({ adminToken: ADMIN_SECRET });

  test("GET /admin/schema returns 401 without bearer token", async () => {
    const req = makeReq("GET", "/admin/schema");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
    const body = await jsonBody(res!);
    expect(body).toEqual({ error: "Unauthorized" });
  });

  test("POST /admin/schema/webhooks/test returns 401 without bearer token", async () => {
    const req = makeReq("POST", "/admin/schema/webhooks/test");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
    const body = await jsonBody(res!);
    expect(body).toEqual({ error: "Unauthorized" });
  });

  test("GET /admin/schema returns 200 with valid bearer token", async () => {
    const req = makeAuthedReq("GET", "/admin/schema");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const body = (await jsonBody(res!)) as { headers: unknown[]; fields: unknown[] };
    expect(body).toHaveProperty("headers");
    expect(body).toHaveProperty("fields");
  });

  test("POST /admin/schema/webhooks/test returns 422 with valid bearer token (no webhook)", async () => {
    const req = makeAuthedReq("POST", "/admin/schema/webhooks/test");
    const res = await handleAdminRoute(req, km, authedConfig, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(422);
    const body = await jsonBody(res!);
    expect(body).toEqual({ sent: false, error: "No webhook URL configured" });
  });
});

describe("GET /admin/schema returns recorded data", () => {
  const config = makeConfig();

  test("returns headers and fields after recording schema data", async () => {
    // Record some schema data directly
    st.recordHeaders(new Headers({ "x-test-header": "test-value", "content-type": "application/json" }));
    st.recordResponseJson("/v1/messages", JSON.stringify({ id: "msg_1", type: "message" }));

    const req = makeReq("GET", "/admin/schema");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);

    const body = (await jsonBody(res!)) as {
      headers: { name: string; sampleValues: string[] }[];
      fields: { endpoint: string; path: string; jsonTypes: string[] }[];
    };

    // Verify headers
    expect(body.headers.length).toBeGreaterThanOrEqual(2);
    const headerNames = body.headers.map((h) => h.name);
    expect(headerNames).toContain("x-test-header");
    expect(headerNames).toContain("content-type");

    // Verify fields
    expect(body.fields.length).toBeGreaterThanOrEqual(2);
    const fieldPaths = body.fields.map((f) => f.path);
    expect(fieldPaths).toContain("id");
    expect(fieldPaths).toContain("type");

    // Verify field structure
    const idField = body.fields.find((f) => f.path === "id");
    expect(idField).toBeDefined();
    expect(idField!.endpoint).toBe("/v1/messages");
    expect(idField!.jsonTypes).toContain("string");
  });
});

// ═══════════════════════════════════════════════════════════════════
// Webhook CRUD endpoints
// ═══════════════════════════════════════════════════════════════════

describe("Webhook CRUD admin endpoints", () => {
  const config = makeConfig();

  test("GET /admin/schema/webhooks returns empty list initially", async () => {
    const req = makeReq("GET", "/admin/schema/webhooks");
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const body = (await jsonBody(res!)) as { webhooks: unknown[] };
    expect(body.webhooks).toHaveLength(0);
  });

  test("POST /admin/schema/webhooks adds a webhook", async () => {
    const req = makeReq("POST", "/admin/schema/webhooks", { url: "http://example.com/hook", label: "test" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const body = await jsonBody(res!);
    expect(body).toEqual({ ok: true });

    // Verify it appears in the list
    const listReq = makeReq("GET", "/admin/schema/webhooks");
    const listRes = await handleAdminRoute(listReq, km, config, st);
    const listBody = (await jsonBody(listRes!)) as { webhooks: { url: string; label: string }[] };
    expect(listBody.webhooks).toHaveLength(1);
    expect(listBody.webhooks[0]!.url).toBe("http://example.com/hook");
    expect(listBody.webhooks[0]!.label).toBe("test");
  });

  test("POST /admin/schema/webhooks rejects invalid URL", async () => {
    const req = makeReq("POST", "/admin/schema/webhooks", { url: "not-a-url" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(400);
  });

  test("POST /admin/schema/webhooks rejects missing URL", async () => {
    const req = makeReq("POST", "/admin/schema/webhooks", { label: "test" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(400);
  });

  test("POST /admin/schema/webhooks rejects duplicate URL", async () => {
    st.addWebhook("http://example.com/dup");
    const req = makeReq("POST", "/admin/schema/webhooks", { url: "http://example.com/dup" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(409);
  });

  test("POST /admin/schema/webhooks/remove removes a webhook", async () => {
    st.addWebhook("http://example.com/to-remove", "doomed");
    expect(st.listWebhooks().length).toBeGreaterThanOrEqual(1);

    const req = makeReq("POST", "/admin/schema/webhooks/remove", { url: "http://example.com/to-remove" });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);

    const remaining = st.listWebhooks().filter(w => w.url === "http://example.com/to-remove");
    expect(remaining).toHaveLength(0);
  });
});

describe("POST /admin/keys/update — allowedDays", () => {
  const config = makeConfig();

  test("updates allowedDays via full key", async () => {
    km.addKey(VALID_KEY, "ad-test");
    const req = makeReq("POST", "/admin/keys/update", { key: VALID_KEY, allowedDays: [1, 2, 3, 4, 5] });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    const body = (await jsonBody(res!)) as { updated: boolean; allowedDays: number[] };
    expect(body.updated).toBe(true);
    expect(body.allowedDays).toEqual([1, 2, 3, 4, 5]);
    expect(km.listKeys()[0]!.allowedDays).toEqual([1, 2, 3, 4, 5]);
  });

  test("updates allowedDays via masked key", async () => {
    km.addKey(VALID_KEY, "ad-test");
    const masked = km.listKeys()[0]!.maskedKey;
    const req = makeReq("POST", "/admin/keys/update", { maskedKey: masked, allowedDays: [0, 6] });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    const body = (await jsonBody(res!)) as { updated: boolean; allowedDays: number[] };
    expect(body.allowedDays).toEqual([0, 6]);
  });

  test("returns 400 for empty allowedDays array", async () => {
    km.addKey(VALID_KEY, "ad-test");
    const req = makeReq("POST", "/admin/keys/update", { key: VALID_KEY, allowedDays: [] });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
  });

  test("returns 400 for invalid day values", async () => {
    km.addKey(VALID_KEY, "ad-test");
    const req = makeReq("POST", "/admin/keys/update", { key: VALID_KEY, allowedDays: [7] });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(400);
  });

  test("combined update: label + priority + allowedDays", async () => {
    km.addKey(VALID_KEY, "ad-test");
    const req = makeReq("POST", "/admin/keys/update", {
      key: VALID_KEY,
      label: "new-label",
      priority: 1,
      allowedDays: [1, 3, 5],
    });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    const body = (await jsonBody(res!)) as { updated: boolean; label: string; priority: number; allowedDays: number[] };
    expect(body.label).toBe("new-label");
    expect(body.priority).toBe(1);
    expect(body.allowedDays).toEqual([1, 3, 5]);
  });

  test("allowedDays appears in response and deduplicates/sorts", async () => {
    km.addKey(VALID_KEY, "ad-test");
    const req = makeReq("POST", "/admin/keys/update", { key: VALID_KEY, allowedDays: [5, 3, 1, 3, 5] });
    const res = await handleAdminRoute(req, km, config, st);
    expect(res!.status).toBe(200);
    const body = (await jsonBody(res!)) as { allowedDays: number[] };
    expect(body.allowedDays).toEqual([1, 3, 5]);
  });
});

describe("GET /admin/capacity/forecast", () => {
  test("returns the full 168-slot seasonal factor table", async () => {
    const req = makeReq("GET", "/admin/capacity/forecast");
    const res = await handleAdminRoute(req, km, makeConfig(), st);
    expect(res!.status).toBe(200);
    const body = (await jsonBody(res!)) as {
      weeks: number;
      generatedAt: number;
      totalSamples: number;
      slots: Array<{ dow: number; hour: number; factor: number; samples: number }>;
    };
    expect(body.weeks).toBe(4);
    expect(body.slots).toHaveLength(168);
    expect(body.totalSamples).toBe(0);
    for (const slot of body.slots) {
      expect(slot.factor).toBe(1);
      expect(slot.samples).toBe(0);
    }
  });

  test("respects the weeks query parameter", async () => {
    const req = makeReq("GET", "/admin/capacity/forecast?weeks=2");
    const res = await handleAdminRoute(req, km, makeConfig(), st);
    expect(res!.status).toBe(200);
    const body = (await jsonBody(res!)) as { weeks: number };
    expect(body.weeks).toBe(2);
  });

  test("clamps the weeks parameter to the 1..4 range", async () => {
    const req = makeReq("GET", "/admin/capacity/forecast?weeks=999");
    const res = await handleAdminRoute(req, km, makeConfig(), st);
    const body = (await jsonBody(res!)) as { weeks: number };
    expect(body.weeks).toBe(4);

    const reqZero = makeReq("GET", "/admin/capacity/forecast?weeks=0");
    const resZero = await handleAdminRoute(reqZero, km, makeConfig(), st);
    const bodyZero = (await jsonBody(resZero!)) as { weeks: number };
    expect(bodyZero.weeks).toBe(1);
  });

  test("bogus weeks param falls back to default", async () => {
    const req = makeReq("GET", "/admin/capacity/forecast?weeks=not-a-number");
    const res = await handleAdminRoute(req, km, makeConfig(), st);
    const body = (await jsonBody(res!)) as { weeks: number };
    expect(body.weeks).toBe(4);
  });
});
