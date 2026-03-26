#!/usr/bin/env bun
/**
 * CLI to manage the proxy without needing curl.
 *
 * Usage:
 *   claude-proxy-ctl add sk-ant-xxx... [label]
 *   claude-proxy-ctl list
 *   claude-proxy-ctl remove sk-ant-xxx...
 *   claude-proxy-ctl stats
 */

const BASE = process.env["PROXY_URL"] ?? "http://localhost:4080";
const TOKEN = process.env["ADMIN_TOKEN"] ?? "";

const [command, ...args] = process.argv.slice(2);

async function main(): Promise<void> {
  const headers: Record<string, string> = {
    "content-type": "application/json",
  };
  if (TOKEN) {
    headers["authorization"] = `Bearer ${TOKEN}`;
  }

  switch (command) {
    case "add": {
      const key = args[0];
      const label = args[1];
      if (!key) {
        console.error("Usage: bun run src/cli.ts add <api-key> [label]");
        process.exit(1);
      }
      const res = await fetch(`${BASE}/admin/keys`, {
        method: "POST",
        headers,
        body: JSON.stringify({ key, label }),
      });
      console.log(await res.json());
      break;
    }

    case "list": {
      const res = await fetch(`${BASE}/admin/keys`, { headers });
      console.log(JSON.stringify(await res.json(), null, 2));
      break;
    }

    case "remove": {
      const key = args[0];
      if (!key) {
        console.error("Usage: bun run src/cli.ts remove <api-key>");
        process.exit(1);
      }
      const res = await fetch(`${BASE}/admin/keys/remove`, {
        method: "POST",
        headers,
        body: JSON.stringify({ key }),
      });
      console.log(await res.json());
      break;
    }

    case "stats": {
      const res = await fetch(`${BASE}/admin/stats`, { headers });
      console.log(JSON.stringify(await res.json(), null, 2));
      break;
    }

    default:
      console.log(`Commands: add, list, remove, stats`);
      process.exit(1);
  }
}

main().catch((err: unknown) => {
  console.error("CLI error:", err);
  process.exit(1);
});
