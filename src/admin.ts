import type { KeyManager } from "./key-manager.ts";
import type { ProxyConfig, AddKeyRequest } from "./types.ts";
import { log } from "./logger.ts";
import { subscribe, type ProxyEvent } from "./events.ts";

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
    "/admin/stats",
    new Map<string, RouteHandler>([["GET", handleStats]]),
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
export function handleAdminRoute(
  req: Request,
  keyManager: KeyManager,
  config: ProxyConfig,
): Response | Promise<Response> | null {
  const url = new URL(req.url);

  if (!url.pathname.startsWith("/admin/")) return null;

  // Auth check (skip for /admin/health and /admin/events which are local-only)
  if (url.pathname !== "/admin/health" && url.pathname !== "/admin/events" && config.adminToken !== null) {
    const bearer = req.headers.get("authorization");
    if (bearer !== `Bearer ${config.adminToken}`) {
      return json({ error: "Unauthorized" }, 401);
    }
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
  const body = await parseJsonBody<{ key: string }>(req);
  if (body === null || typeof body.key !== "string") {
    return json({ error: "Invalid JSON body — need 'key' field" }, 400);
  }

  const removed = keyManager.removeKey(body.key);
  if (!removed) {
    return json({ error: "Key not found" }, 404);
  }
  return json({ removed: true });
}

function handleStats(
  _req: Request,
  keyManager: KeyManager,
): Response {
  const keys = keyManager.listKeys();
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
  });
}

function handleEvents(
  _req: Request,
  keyManager: KeyManager,
): Response {
  // Send initial state as first event, then stream live events
  const stream = new ReadableStream({
    start(controller) {
      const initial: ProxyEvent = {
        type: "keys",
        ts: new Date().toISOString(),
        keys: keyManager.listKeys(),
      };
      controller.enqueue(`data: ${JSON.stringify(initial)}\n\n`);

      const unsubscribe = subscribe((event) => {
        try {
          controller.enqueue(`data: ${JSON.stringify(event)}\n\n`);
        } catch {
          unsubscribe();
        }
      });

      // Clean up if client disconnects
      _req.signal.addEventListener("abort", () => {
        unsubscribe();
        try { controller.close(); } catch {}
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
