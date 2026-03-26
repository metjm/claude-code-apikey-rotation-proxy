import { loadConfig } from "./config.ts";
import { KeyManager } from "./key-manager.ts";
import { handleAdminRoute } from "./admin.ts";
import { proxyRequest } from "./proxy.ts";
import { log } from "./logger.ts";
import type { UnixMs } from "./types.ts";

const config = loadConfig();
const keyManager = new KeyManager(config.dataDir);

const server = Bun.serve({
  port: config.port,

  async fetch(req: Request): Promise<Response> {
    // Admin routes
    const adminResponse = handleAdminRoute(req, keyManager, config);
    if (adminResponse !== null) {
      return adminResponse;
    }

    // Proxy everything else to the Anthropic API
    const result = await proxyRequest(req, keyManager, config);

    switch (result.kind) {
      case "success":
        return result.response;

      case "no_keys":
        return errorResponse(
          503,
          "No API keys configured. Add keys via POST /admin/keys.",
        );

      case "all_exhausted": {
        const waitSecs = Math.ceil(
          Math.max(0, result.earliestAvailableAt - (Date.now() as UnixMs)) / 1000,
        );
        return errorResponse(
          429,
          `All API keys are rate-limited. Earliest available in ${waitSecs}s.`,
          { "retry-after": String(waitSecs) },
        );
      }

      case "error":
        return new Response(result.body, {
          status: result.status,
          headers: { "content-type": "application/json" },
        });

      case "rate_limited":
        // This shouldn't happen — proxyRequest retries internally.
        // But handle it for exhaustiveness.
        return errorResponse(429, "Rate limited", {
          "retry-after": String(result.retryAfterSecs),
        });
    }
  },

  error(err: Error): Response {
    log("error", "Unhandled server error", { error: err.message });
    return errorResponse(500, "Internal proxy error");
  },
});

function errorResponse(
  status: number,
  message: string,
  extraHeaders?: Record<string, string>,
): Response {
  return new Response(
    JSON.stringify({ error: { type: "proxy_error", message } }),
    {
      status,
      headers: { "content-type": "application/json", ...extraHeaders },
    },
  );
}

log("info", `Proxy listening on http://localhost:${server.port}`, {
  upstream: config.upstream,
  keys: keyManager.totalCount(),
  availableKeys: keyManager.availableCount(),
});
