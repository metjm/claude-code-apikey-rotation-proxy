import type { KeyManager } from "./key-manager.ts";
import type { ProxyConfig, AddKeyRequest, AddTokenRequest } from "./types.ts";
import { log } from "./logger.ts";
import { subscribe, type ProxyEvent } from "./events.ts";
import type { SchemaTracker } from "./schema-tracker.ts";

type RouteHandler = (
  req: Request,
  keyManager: KeyManager,
) => Response | Promise<Response>;

const routes: ReadonlyMap<string, ReadonlyMap<string, RouteHandler>> = new Map([
  [
    "/admin/keys",
    new Map<string, RouteHandler>([
      ["GET", handleListKeys],
      ["POST", handleAddKey],
    ]),
  ],
  [
    "/admin/keys/remove",
    new Map<string, RouteHandler>([["POST", handleRemoveKey]]),
  ],
  [
    "/admin/keys/update",
    new Map<string, RouteHandler>([["POST", handleUpdateKey]]),
  ],
  [
    "/admin/keys/reset-cooldowns",
    new Map<string, RouteHandler>([["POST", handleResetKeyCooldowns]]),
  ],
  [
    "/admin/tokens",
    new Map<string, RouteHandler>([
      ["GET", handleListTokens],
      ["POST", handleAddToken],
    ]),
  ],
  [
    "/admin/tokens/remove",
    new Map<string, RouteHandler>([["POST", handleRemoveToken]]),
  ],
  [
    "/admin/tokens/update",
    new Map<string, RouteHandler>([["POST", handleUpdateToken]]),
  ],
  [
    "/admin/stats",
    new Map<string, RouteHandler>([["GET", handleStats]]),
  ],
  [
    "/admin/stats/timeseries",
    new Map<string, RouteHandler>([["GET", handleTimeseriesStats]]),
  ],
  [
    "/admin/capacity/timeseries",
    new Map<string, RouteHandler>([["GET", handleCapacityTimeseries]]),
  ],
  [
    "/admin/capacity/forecast",
    new Map<string, RouteHandler>([["GET", handleCapacityForecast]]),
  ],
  [
    "/admin/latency/timeseries",
    new Map<string, RouteHandler>([["GET", handleLatencyTimeseries]]),
  ],
  [
    "/admin/health",
    new Map<string, RouteHandler>([["GET", handleHealth]]),
  ],
  [
    "/admin/events",
    new Map<string, RouteHandler>([["GET", handleEvents]]),
  ],
]);

/**
 * Try to handle an admin route. Returns null if the path isn't an admin route.
 */
export async function handleAdminRoute(
  req: Request,
  keyManager: KeyManager,
  config: ProxyConfig,
  schemaTracker: SchemaTracker,
): Promise<Response | null> {
  const url = new URL(req.url);

  if (!url.pathname.startsWith("/admin/")) return null;

  // Auth check (skip for public bootstrap/health/event endpoints)
  if (
    url.pathname !== "/admin/bootstrap"
    && url.pathname !== "/admin/health"
    && url.pathname !== "/admin/events"
    && config.adminToken !== null
  ) {
    const bearer = req.headers.get("authorization");
    if (bearer !== `Bearer ${config.adminToken}`) {
      return json({ error: "Unauthorized" }, 401);
    }
  }

  // Schema-specific routes (after auth, before generic dispatch)
  if (url.pathname === "/admin/bootstrap" && req.method === "GET") {
    return json({ authRequired: config.adminToken !== null });
  }
  if (url.pathname === "/admin/schema" && req.method === "GET") {
    return json({ headers: schemaTracker.listHeaders(), fields: schemaTracker.listFields() });
  }
  if (url.pathname === "/admin/schema/webhooks" && req.method === "GET") {
    return json({ webhooks: schemaTracker.listWebhooks() });
  }
  if (url.pathname === "/admin/schema/webhooks" && req.method === "POST") {
    const body = await req.json() as { url?: string; label?: string };
    if (!body.url || typeof body.url !== "string") return json({ error: "url is required" }, 400);
    try { new URL(body.url); } catch { return json({ error: "Invalid URL" }, 400); }
    try {
      schemaTracker.addWebhook(body.url, body.label);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("UNIQUE constraint")) return json({ error: "Webhook URL already exists" }, 409);
      throw err;
    }
    return json({ ok: true });
  }
  if (url.pathname === "/admin/schema/webhooks/remove" && req.method === "POST") {
    const body = await req.json() as { url?: string };
    if (!body.url || typeof body.url !== "string") return json({ error: "url is required" }, 400);
    schemaTracker.removeWebhook(body.url);
    return json({ ok: true });
  }
  if (url.pathname === "/admin/schema/webhooks/test" && req.method === "POST") {
    const body = await req.json().catch(() => ({})) as { url?: string };
    const sent = schemaTracker.sendTestNotification(body.url);
    if (!sent) return json({ sent: false, error: "No webhook URL configured" }, 422);
    return json({ sent: true });
  }

  const methodHandlers = routes.get(url.pathname);
  if (methodHandlers === undefined) {
    return json({ error: "Not found" }, 404);
  }

  const handler = methodHandlers.get(req.method);
  if (handler === undefined) {
    return json({ error: "Method not allowed" }, 405);
  }

  return handler(req, keyManager);
}

// ── Route handlers ────────────────────────────────────────────────

function handleListKeys(
  _req: Request,
  keyManager: KeyManager,
): Response {
  return json({ keys: keyManager.listKeys() });
}

async function handleAddKey(
  req: Request,
  keyManager: KeyManager,
): Promise<Response> {
  const body = await parseJsonBody<AddKeyRequest>(req);
  if (body === null) {
    return json({ error: "Invalid JSON body" }, 400);
  }

  if (typeof body.key !== "string" || body.key.length === 0) {
    return json({ error: "Missing or empty 'key' field" }, 400);
  }

  if (body.label !== undefined && typeof body.label !== "string") {
    return json({ error: "'label' must be a string" }, 400);
  }

  try {
    const entry = keyManager.addKey(body.key, body.label);
    return json({
      added: {
        label: entry.label,
        maskedKey: `${entry.key.slice(0, 10)}...${entry.key.slice(-4)}`,
      },
    }, 201);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    log("warn", "Failed to add key", { error: message });
    return json({ error: message }, 400);
  }
}

async function handleRemoveKey(
  req: Request,
  keyManager: KeyManager,
): Promise<Response> {
  const body = await parseJsonBody<{ key?: string; maskedKey?: string }>(req);
  if (body === null) {
    return json({ error: "Invalid JSON body" }, 400);
  }

  let removed = false;
  if (typeof body.maskedKey === "string") {
    removed = keyManager.removeKeyByMasked(body.maskedKey);
  } else if (typeof body.key === "string") {
    removed = keyManager.removeKey(body.key);
  } else {
    return json({ error: "Need 'key' or 'maskedKey' field" }, 400);
  }

  if (!removed) {
    return json({ error: "Key not found" }, 404);
  }
  return json({ removed: true });
}

async function handleUpdateKey(
  req: Request,
  keyManager: KeyManager,
): Promise<Response> {
  const body = await parseJsonBody<{
    key?: string;
    maskedKey?: string;
    label?: string;
    priority?: number;
    allowedDays?: number[];
  }>(req);
  if (body === null) {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const hasKey = typeof body.key === "string";
  const hasMaskedKey = typeof body.maskedKey === "string";
  if (!hasKey && !hasMaskedKey) {
    return json({ error: "Need 'key' or 'maskedKey' field" }, 400);
  }

  const hasLabel = typeof body.label === "string";
  const hasPriority = typeof body.priority === "number";
  const hasAllowedDays = Array.isArray(body.allowedDays);
  if (!hasLabel && !hasPriority && !hasAllowedDays) {
    return json({ error: "Need 'label', 'priority', and/or 'allowedDays' field" }, 400);
  }

  if (hasLabel && body.label!.length === 0) {
    return json({ error: "'label' must not be empty" }, 400);
  }
  if (hasPriority && (body.priority! < 1 || body.priority! > 4)) {
    return json({ error: "'priority' must be 1, 2, 3, or 4" }, 400);
  }
  if (hasAllowedDays) {
    const days = body.allowedDays!;
    if (days.length === 0 || !days.every(d => Number.isInteger(d) && d >= 0 && d <= 6)) {
      return json({ error: "'allowedDays' must be a non-empty array of integers 0-6" }, 400);
    }
  }

  // Update label (accepts full key or masked key)
  if (hasLabel) {
    let updated = false;
    if (hasMaskedKey) {
      updated = keyManager.updateKeyLabelByMasked(body.maskedKey!, body.label!);
    } else if (hasKey) {
      updated = keyManager.updateKeyLabel(body.key!, body.label!);
    }
    if (!updated) return json({ error: "Key not found" }, 404);
  }

  // Update priority (accepts full key or masked key)
  if (hasPriority) {
    const updated = hasKey
      ? keyManager.updateKeyPriority(body.key!, body.priority!)
      : keyManager.updateKeyPriorityByMask(body.maskedKey!, body.priority!);
    if (!updated) return json({ error: "Key not found" }, 404);
  }

  // Update allowed days
  let sortedDays: number[] | undefined;
  if (hasAllowedDays) {
    sortedDays = [...new Set(body.allowedDays!)].sort((a, b) => a - b);
    const updated = hasKey
      ? keyManager.updateKeyAllowedDays(body.key!, sortedDays)
      : keyManager.updateKeyAllowedDaysByMask(body.maskedKey!, sortedDays);
    if (!updated) return json({ error: "Key not found" }, 404);
  }

  return json({
    updated: true,
    ...(hasLabel ? { label: body.label } : {}),
    ...(hasPriority ? { priority: body.priority } : {}),
    ...(hasAllowedDays ? { allowedDays: sortedDays } : {}),
  });
}

function handleResetKeyCooldowns(
  _req: Request,
  keyManager: KeyManager,
): Response {
  const reset = keyManager.resetKeyCooldowns();
  return json({
    reset,
    availableKeys: keyManager.availableCount(),
    totalKeys: keyManager.totalCount(),
  });
}

// ── Token handlers ────────────────────────────────────────────────

function handleListTokens(
  _req: Request,
  keyManager: KeyManager,
): Response {
  return json({ tokens: keyManager.listTokens() });
}

async function handleAddToken(
  req: Request,
  keyManager: KeyManager,
): Promise<Response> {
  const body = await parseJsonBody<AddTokenRequest>(req);
  if (body === null) {
    return json({ error: "Invalid JSON body" }, 400);
  }

  if (typeof body.token !== "string" || body.token.length === 0) {
    return json({ error: "Missing or empty 'token' field" }, 400);
  }

  if (body.label !== undefined && typeof body.label !== "string") {
    return json({ error: "'label' must be a string" }, 400);
  }

  try {
    const entry = keyManager.addToken(body.token, body.label);
    return json({
      added: {
        label: entry.label,
      },
    }, 201);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    log("warn", "Failed to add token", { error: message });
    return json({ error: message }, 400);
  }
}

async function handleRemoveToken(
  req: Request,
  keyManager: KeyManager,
): Promise<Response> {
  const body = await parseJsonBody<{ token?: string; maskedToken?: string }>(req);
  if (body === null) {
    return json({ error: "Invalid JSON body" }, 400);
  }

  let removed = false;
  if (typeof body.maskedToken === "string") {
    removed = keyManager.removeTokenByMasked(body.maskedToken);
  } else if (typeof body.token === "string") {
    removed = keyManager.removeToken(body.token);
  } else {
    return json({ error: "Need 'token' or 'maskedToken' field" }, 400);
  }

  if (!removed) {
    return json({ error: "Token not found" }, 404);
  }
  return json({ removed: true });
}

async function handleUpdateToken(
  req: Request,
  keyManager: KeyManager,
): Promise<Response> {
  const body = await parseJsonBody<{ token?: string; maskedToken?: string; label: string }>(req);
  if (body === null || typeof body.label !== "string") {
    return json({ error: "Invalid JSON body — need 'label' field" }, 400);
  }
  if (body.label.length === 0) {
    return json({ error: "'label' must not be empty" }, 400);
  }

  let updated = false;
  if (typeof body.maskedToken === "string") {
    updated = keyManager.updateTokenLabelByMasked(body.maskedToken, body.label);
  } else if (typeof body.token === "string") {
    updated = keyManager.updateTokenLabel(body.token, body.label);
  } else {
    return json({ error: "Need 'token' or 'maskedToken' field" }, 400);
  }

  if (!updated) {
    return json({ error: "Token not found" }, 404);
  }
  return json({ updated: true, label: body.label });
}

function handleStats(
  _req: Request,
  keyManager: KeyManager,
): Response {
  const keys = keyManager.listKeys();
  const capacitySummary = keyManager.getCapacitySummary();
  const totals = keys.reduce(
    (acc, k) => ({
      totalRequests: acc.totalRequests + k.stats.totalRequests,
      successfulRequests: acc.successfulRequests + k.stats.successfulRequests,
      rateLimitHits: acc.rateLimitHits + k.stats.rateLimitHits,
      errors: acc.errors + k.stats.errors,
      totalTokensIn: acc.totalTokensIn + k.stats.totalTokensIn,
      totalTokensOut: acc.totalTokensOut + k.stats.totalTokensOut,
    }),
    {
      totalRequests: 0,
      successfulRequests: 0,
      rateLimitHits: 0,
      errors: 0,
      totalTokensIn: 0,
      totalTokensOut: 0,
    },
  );

  return json({
    keyCount: keys.length,
    availableKeys: keys.filter((k) => k.isAvailable).length,
    totals,
    keys,
    capacitySummary,
    requestsByTier: keyManager.getRequestsByTier(),
  });
}

function handleTimeseriesStats(
  req: Request,
  keyManager: KeyManager,
): Response {
  const url = new URL(req.url);
  const hours = Math.min(Number(url.searchParams.get("hours") ?? 24), 720);
  const resolution = url.searchParams.get("resolution") === "day" ? "day" as const : "hour" as const;
  const keyLabel = url.searchParams.get("key") ?? undefined;
  const userLabel = url.searchParams.get("user") ?? undefined;
  const buckets = keyManager.queryTimeseries({
    hours,
    resolution,
    ...(keyLabel !== undefined ? { keyLabel } : {}),
    ...(userLabel !== undefined ? { userLabel } : {}),
  });
  return json({ resolution, buckets });
}

function handleCapacityTimeseries(
  req: Request,
  keyManager: KeyManager,
): Response {
  const url = new URL(req.url);
  const hours = Math.min(Number(url.searchParams.get("hours") ?? 24), 720);
  const resolution = url.searchParams.get("resolution") === "day" ? "day" as const : "hour" as const;
  const keyLabel = url.searchParams.get("key") ?? undefined;
  const buckets = keyManager.queryCapacityTimeseries({
    hours,
    resolution,
    ...(keyLabel !== undefined ? { keyLabel } : {}),
  });
  return json({ resolution, buckets });
}

function handleCapacityForecast(
  req: Request,
  keyManager: KeyManager,
): Response {
  const url = new URL(req.url);
  const weeksParam = url.searchParams.get("weeks");
  const parsed = weeksParam !== null ? Number(weeksParam) : Number.NaN;
  const table = Number.isFinite(parsed)
    ? keyManager.computeSeasonalRequestFactors(Math.max(1, Math.min(parsed, 4)))
    : keyManager.computeSeasonalRequestFactors();
  return json(table);
}

// Bucket sizes for the four ranges the dashboard chart offers. Each range
// gets ~30-60 points so the line stays readable at 600-1200px wide. Empty
// buckets get null avgs in the response so the chart breaks the line at gaps.
const LATENCY_RANGE_PRESETS: Record<string, { rangeMs: number; bucketMs: number }> = {
  "30m": { rangeMs: 30 * 60 * 1000,       bucketMs: 60 * 1000 },        // 30 × 1m
  "1h":  { rangeMs: 60 * 60 * 1000,       bucketMs: 2 * 60 * 1000 },    // 30 × 2m
  "5h":  { rangeMs: 5 * 60 * 60 * 1000,   bucketMs: 10 * 60 * 1000 },   // 30 × 10m
  "24h": { rangeMs: 24 * 60 * 60 * 1000,  bucketMs: 30 * 60 * 1000 },   // 48 × 30m
};

function handleLatencyTimeseries(
  req: Request,
  keyManager: KeyManager,
): Response {
  const url = new URL(req.url);
  const rangeKey = (url.searchParams.get("range") ?? "30m").toLowerCase();
  const preset = LATENCY_RANGE_PRESETS[rangeKey];
  if (preset === undefined) {
    return json({
      error: `Unknown range "${rangeKey}". Allowed: ${Object.keys(LATENCY_RANGE_PRESETS).join(", ")}`,
    }, 400);
  }
  const buckets = keyManager.getRequestLatencyTimeseries(preset.rangeMs, preset.bucketMs);
  return json({ range: rangeKey, bucketMs: preset.bucketMs, buckets });
}

function handleEvents(
  _req: Request,
  keyManager: KeyManager,
): Response {
  const stream = new ReadableStream({
    start(controller) {
      function sendKeys() {
        try {
          const ev: ProxyEvent = {
            type: "keys", ts: new Date().toISOString(),
            keys: keyManager.listKeys(), tokens: keyManager.listTokens(),
            currentBucket: keyManager.getCurrentBucket(),
            capacitySummary: keyManager.getCapacitySummary(),
            requestsByTier: keyManager.getRequestsByTier(),
          };
          controller.enqueue(`data: ${JSON.stringify(ev)}\n\n`);
        } catch {}
      }

      // Initial snapshot + heartbeat every 5s with fresh stats
      sendKeys();
      const heartbeat = setInterval(sendKeys, 5_000);

      const unsubscribe = subscribe((event) => {
        try {
          controller.enqueue(`data: ${JSON.stringify(event)}\n\n`);
        } catch {
          unsubscribe();
          clearInterval(heartbeat);
        }
      });

      _req.signal.addEventListener("abort", () => {
        unsubscribe();
        clearInterval(heartbeat);
        try { controller.close(); } catch {};
      });
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      "connection": "keep-alive",
    },
  });
}

function handleHealth(
  _req: Request,
  keyManager: KeyManager,
): Response {
  const total = keyManager.totalCount();
  const available = keyManager.availableCount();
  return json({
    status: total > 0 ? "ok" : "no_keys",
    keys: { total, available },
  });
}

// ── Helpers ───────────────────────────────────────────────────────

function json(data: unknown, status: number = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json" },
  });
}

async function parseJsonBody<T>(req: Request): Promise<T | null> {
  try {
    return (await req.json()) as T;
  } catch {
    return null;
  }
}
