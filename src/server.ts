#!/usr/bin/env bun
import { loadConfig } from "./config.ts";
import { KeyManager } from "./key-manager.ts";
import { handleAdminRoute } from "./admin.ts";
import { proxyRequest } from "./proxy.ts";
import { log } from "./logger.ts";
import { serviceInstall, serviceStatus, serviceUninstall } from "./service.ts";
import type { UnixMs } from "./types.ts";

const subcommand = process.argv[2];

if (subcommand === "service") {
  const action = process.argv[3];
  const config = loadConfig();

  switch (action) {
    case "install":
      serviceInstall(config.dataDir);
      break;
    case "uninstall":
      serviceUninstall();
      break;
    case "status":
      serviceStatus();
      break;
    default:
      console.log("Usage: claude-proxy service <install|uninstall|status>");
      process.exit(1);
  }
} else if (subcommand === "help" || subcommand === "--help" || subcommand === "-h") {
  console.log(`claude-proxy — API key rotation proxy for Claude Code

Usage:
  claude-proxy                        Start the proxy server
  claude-proxy service install        Install as a system service (auto-start)
  claude-proxy service uninstall      Remove the system service
  claude-proxy service status         Check service status

Environment:
  PORT              Listen port (default: 4080)
  UPSTREAM_URL      Anthropic API URL (default: https://api.anthropic.com)
  ADMIN_TOKEN       Bearer token for /admin/* endpoints (optional)
  DATA_DIR          Where to store key state (default: ./data)
  MAX_RETRIES       Max key rotation attempts per request (default: 10)
  LOG_LEVEL         debug | info | warn | error (default: info)`);
} else {
  startServer();
}

function startServer(): void {
  const config = loadConfig();
  const keyManager = new KeyManager(config.dataDir);

  const server = Bun.serve({
    port: config.port,

    async fetch(req: Request): Promise<Response> {
      const adminResponse = handleAdminRoute(req, keyManager, config);
      if (adminResponse !== null) {
        return adminResponse;
      }

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

  log("info", `Proxy listening on http://localhost:${server.port}`, {
    upstream: config.upstream,
    keys: keyManager.totalCount(),
    availableKeys: keyManager.availableCount(),
  });
}

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
