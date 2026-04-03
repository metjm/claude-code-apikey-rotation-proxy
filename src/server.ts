#!/usr/bin/env bun
import { loadConfig } from "./config.ts";
import { KeyManager } from "./key-manager.ts";
import { handleAdminRoute } from "./admin.ts";
import { proxyRequest } from "./proxy.ts";
import { log } from "./logger.ts";
import { SchemaTracker } from "./schema-tracker.ts";
import { join } from "node:path";
import { serviceInstall, serviceStatus, serviceUninstall } from "./service.ts";
import { claudeConfigInstall, claudeConfigUninstall, claudeConfigStatus } from "./claude-config.ts";
import type { ProxyTokenEntry, UnixMs } from "./types.ts";

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
} else if (subcommand === "claude") {
  const action = process.argv[3];
  const config = loadConfig();

  switch (action) {
    case "install":
      claudeConfigInstall(config.port);
      break;
    case "uninstall":
      claudeConfigUninstall();
      break;
    case "status":
      claudeConfigStatus(config.port);
      break;
    default:
      console.log("Usage: claude-proxy claude <install|uninstall|status>");
      process.exit(1);
  }
} else if (subcommand === "help" || subcommand === "--help" || subcommand === "-h") {
  console.log(`claude-proxy — API key rotation proxy for Claude Code

Usage:
  claude-proxy                        Start the proxy server
  claude-proxy service install        Install as a system service (auto-start)
  claude-proxy service uninstall      Remove the system service
  claude-proxy service status         Check service status
  claude-proxy claude install         Add proxy to Claude Code settings
  claude-proxy claude uninstall       Remove proxy from Claude Code settings
  claude-proxy claude status          Check if Claude Code is using the proxy

Environment:
  PORT              Listen port (default: 4080)
  UPSTREAM_URL      Anthropic API URL (default: https://api.anthropic.com)
  ADMIN_TOKEN       Bearer token for /admin/* endpoints (optional)
  DATA_DIR          Where to store the database (default: ./data)
  DB_PATH           Full path to SQLite database (overrides DATA_DIR; default: DATA_DIR/state.db)
  MAX_RETRIES       Max key rotation attempts per request (default: 10)
  WEBHOOK_URL       Slack-compatible webhook URL for API schema change notifications (optional)
  LOG_LEVEL         debug | info | warn | error (default: info)`);
} else {
  startServer();
}

function startServer(): void {
  const config = loadConfig();
  const keyManager = new KeyManager(config.dataDir);
  const schemaTracker = new SchemaTracker(keyManager.dbPath, config.webhookUrl);

  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    try {
      await Promise.race([
        schemaTracker.flushAllWebhooks(),
        new Promise((resolve) => setTimeout(resolve, 5_000)),
      ]);
    } catch {}
    schemaTracker.close();
    keyManager.close();
    process.exit(0);
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);

  const server = Bun.serve({
    port: config.port,

    async fetch(req: Request): Promise<Response> {
      // Serve dashboard
      const url = new URL(req.url);
      if (url.pathname === "/dashboard" || url.pathname === "/dashboard/") {
        return new Response(Bun.file(join(import.meta.dir, "../public/dashboard.html")));
      }
      if (url.pathname === "/dashboard/chart.umd.min.js") {
        return new Response(Bun.file(join(import.meta.dir, "../public/chart.umd.min.js")), {
          headers: { "content-type": "application/javascript" },
        });
      }

      const adminResponse = await handleAdminRoute(req, keyManager, config, schemaTracker);
      if (adminResponse !== null) {
        return adminResponse;
      }

      // Auth gate: if proxy tokens are configured, require a valid one
      let proxyUser: ProxyTokenEntry | null = null;
      if (keyManager.hasTokens()) {
        const incoming = extractProxyToken(req);
        if (incoming === null) {
          return errorResponse(401, "Proxy authentication required. Set your API key to a valid proxy token.");
        }
        proxyUser = keyManager.validateToken(incoming);
        if (proxyUser === null) {
          return errorResponse(401, "Invalid proxy token.");
        }
      }

      const incomingXApiKey = req.headers.get("x-api-key");
      const incomingAuth = req.headers.get("authorization");
      log("info", "Incoming request auth", {
        hasXApiKey: !!incomingXApiKey,
        xApiKeyPrefix: incomingXApiKey?.slice(0, 20),
        hasAuthorization: !!incomingAuth,
        authorizationPrefix: incomingAuth?.slice(0, 30),
      });

      const result = await proxyRequest(req, keyManager, config, schemaTracker, proxyUser);

      switch (result.kind) {
        case "success":
          return result.response;

        case "no_keys":
          return proxyUser
            ? errorResponse(503, "No API keys configured. Add keys via the admin API.")
            : errorResponse(503, "Service not available.");

        case "all_exhausted": {
          const waitSecs = Math.ceil(
            Math.max(0, result.earliestAvailableAt - (Date.now() as UnixMs)) / 1000,
          );
          return proxyUser
            ? errorResponse(429, `All API keys are rate-limited. Retry in ${waitSecs}s.`, { "retry-after": String(waitSecs) })
            : errorResponse(429, "Too many requests.", { "retry-after": String(waitSecs) });
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

function extractProxyToken(req: Request): string | null {
  const xApiKey = req.headers.get("x-api-key");
  if (xApiKey) return xApiKey;

  const auth = req.headers.get("authorization");
  if (auth && auth.startsWith("Bearer ")) return auth.slice(7);

  return null;
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
