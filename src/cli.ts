#!/usr/bin/env bun
/**
 * CLI to manage proxy keys. Works offline — reads/writes state.json directly.
 *
 * Usage:
 *   claude-proxy-ctl add sk-ant-xxx... [label]
 *   claude-proxy-ctl list
 *   claude-proxy-ctl remove sk-ant-xxx...
 *   claude-proxy-ctl stats
 */

import { setLogLevel } from "./logger.ts";
setLogLevel("error");

import { loadConfig } from "./config.ts";
import { KeyManager } from "./key-manager.ts";

const config = loadConfig();
const keyManager = new KeyManager(config.dataDir, {
  perConversationPinning: config.perConversationPinning,
});

const [command, ...args] = process.argv.slice(2);

switch (command) {
  case "add": {
    const key = args[0];
    const label = args[1];
    if (!key) {
      console.error("Usage: claude-proxy-ctl add <api-key> [label]");
      process.exit(1);
    }
    try {
      const entry = keyManager.addKey(key, label);
      console.log(`Added key "${entry.label}"`);
    } catch (err) {
      console.error(`Error: ${(err as Error).message}`);
      process.exit(1);
    }
    break;
  }

  case "list": {
    const keys = keyManager.listKeys();
    if (keys.length === 0) {
      console.log("No keys configured.");
    } else {
      console.log(JSON.stringify(keys, null, 2));
    }
    break;
  }

  case "remove": {
    const key = args[0];
    if (!key) {
      console.error("Usage: claude-proxy-ctl remove <api-key>");
      process.exit(1);
    }
    if (keyManager.removeKey(key)) {
      console.log("Key removed.");
    } else {
      console.error("Key not found.");
      process.exit(1);
    }
    break;
  }

  case "stats": {
    const keys = keyManager.listKeys();
    for (const k of keys) {
      console.log(`\n${k.label} (${k.maskedKey})${k.isAvailable ? "" : " [rate-limited]"}`);
      console.log(`  Requests: ${k.stats.successfulRequests}/${k.stats.totalRequests} successful`);
      console.log(`  Tokens: ${k.stats.totalTokensIn} in / ${k.stats.totalTokensOut} out`);
      console.log(`  Rate limits: ${k.stats.rateLimitHits}, Errors: ${k.stats.errors}`);
    }
    if (keys.length === 0) console.log("No keys configured.");
    break;
  }

  case "watch": {
    const { startWatch } = await import("./watch.ts");
    startWatch();
    break;
  }

  default:
    console.log("Commands: add, list, remove, stats, watch");
    process.exit(1);
}
