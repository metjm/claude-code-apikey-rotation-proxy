import { join } from "node:path";
import type { ProxyConfig } from "./types.ts";

function env(key: string): string | undefined {
  return process.env[key];
}

function envInt(key: string, fallback: number): number {
  const raw = env(key);
  if (raw === undefined) return fallback;
  const parsed = parseInt(raw, 10);
  if (Number.isNaN(parsed)) {
    throw new Error(`${key} must be an integer, got "${raw}"`);
  }
  return parsed;
}

function envBool(key: string, fallback: boolean): boolean {
  const raw = env(key);
  if (raw === undefined) return fallback;
  const v = raw.trim().toLowerCase();
  if (v === "true" || v === "1" || v === "yes") return true;
  if (v === "false" || v === "0" || v === "no" || v === "") return false;
  throw new Error(`${key} must be a boolean (true/false/1/0), got "${raw}"`);
}

export function loadConfig(): ProxyConfig {
  return {
    port: envInt("PORT", 4080),
    upstream: env("UPSTREAM_URL") ?? "https://api.anthropic.com",
    adminToken: env("ADMIN_TOKEN") ?? null,
    dataDir: env("DATA_DIR") ?? join(process.cwd(), "data"),
    maxRetriesPerRequest: envInt("MAX_RETRIES", 10),
    firstChunkTimeoutMs: envInt("FIRST_CHUNK_TIMEOUT_MS", 16_000),
    streamIdleTimeoutMs: envInt("STREAM_IDLE_TIMEOUT_MS", 120_000),
    maxFirstChunkRetries: envInt("MAX_FIRST_CHUNK_RETRIES", 2),
    webhookUrl: env("WEBHOOK_URL") ?? null,
    perConversationPinning: envBool("PER_CONVERSATION_PINNING", false),
  } as const;
}
