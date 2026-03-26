import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  type ApiKey,
  type ApiKeyEntry,
  type ApiKeyStats,
  type MaskedKeyEntry,
  type StoredState,
  asApiKey,
  asKeyLabel,
  now,
  unixMs,
} from "./types.ts";
import { log } from "./logger.ts";

export class KeyManager {
  private keys: ApiKeyEntry[] = [];
  private readonly statePath: string;
  private saveTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(dataDir: string, opts?: { registerShutdownHandler?: boolean }) {
    this.statePath = join(dataDir, "state.json");
    this.load();

    if (opts?.registerShutdownHandler) {
      const flush = () => this.flushAndExit();
      process.on("SIGTERM", flush);
      process.on("SIGINT", flush);
    }
  }

  private flushAndExit(): void {
    if (this.saveTimer) {
      clearTimeout(this.saveTimer);
      this.saveTimer = null;
      this.saveNow();
    }
    process.exit(0);
  }

  // ── Key selection ───────────────────────────────────────────────

  /**
   * Pick the best available key.
   * Strategy: stick with the most-recently-used key until it's rate-limited,
   * then fall back to the next one. This preserves API prompt caching which
   * is per-key.
   * Returns null only if no keys are registered at all.
   */
  getNextAvailableKey(): ApiKeyEntry | null {
    if (this.keys.length === 0) return null;

    const currentTime = now();
    const available = this.keys
      .filter((k) => k.availableAt <= currentTime)
      .sort((a, b) => (b.stats.lastUsedAt ?? 0) - (a.stats.lastUsedAt ?? 0));

    if (available.length > 0) return available[0]!;
    return null;
  }

  /**
   * Get the key that will become available soonest.
   * Used when all keys are rate-limited.
   */
  getEarliestAvailableKey(): ApiKeyEntry | null {
    if (this.keys.length === 0) return null;
    return [...this.keys].sort((a, b) => a.availableAt - b.availableAt)[0]!;
  }

  /** How many keys are currently usable (not rate-limited). */
  availableCount(): number {
    const currentTime = now();
    return this.keys.filter((k) => k.availableAt <= currentTime).length;
  }

  /** Total number of registered keys. */
  totalCount(): number {
    return this.keys.length;
  }

  // ── Stats recording ─────────────────────────────────────────────

  recordRequest(entry: ApiKeyEntry): void {
    entry.stats = {
      ...entry.stats,
      totalRequests: entry.stats.totalRequests + 1,
      lastUsedAt: now(),
    };
    this.scheduleSave();
  }

  recordSuccess(entry: ApiKeyEntry, tokensIn: number, tokensOut: number): void {
    entry.stats = {
      ...entry.stats,
      successfulRequests: entry.stats.successfulRequests + 1,
      totalTokensIn: entry.stats.totalTokensIn + tokensIn,
      totalTokensOut: entry.stats.totalTokensOut + tokensOut,
    };
    this.scheduleSave();
  }

  recordRateLimit(entry: ApiKeyEntry, retryAfterSecs: number): void {
    const waitMs = (retryAfterSecs > 0 ? retryAfterSecs : 60) * 1000;
    entry.stats = {
      ...entry.stats,
      rateLimitHits: entry.stats.rateLimitHits + 1,
    };
    entry.availableAt = unixMs(Date.now() + waitMs);
    log("warn", "Key rate-limited", {
      label: entry.label,
      retryAfterSecs,
      availableAt: new Date(entry.availableAt).toISOString(),
    });
    this.scheduleSave();
  }

  recordError(entry: ApiKeyEntry): void {
    entry.stats = {
      ...entry.stats,
      errors: entry.stats.errors + 1,
    };
    this.scheduleSave();
  }

  // ── CRUD ────────────────────────────────────────────────────────

  addKey(rawKey: string, label?: string): ApiKeyEntry {
    const key: ApiKey = asApiKey(rawKey);

    if (this.keys.some((k) => k.key === key)) {
      throw new Error("Key already registered");
    }

    const entry: ApiKeyEntry = {
      key,
      label: asKeyLabel(label ?? `key-${this.keys.length + 1}`),
      stats: freshStats(),
      availableAt: unixMs(0),
    };

    this.keys.push(entry);
    this.saveNow();
    log("info", "Key added", { label: entry.label });
    return entry;
  }

  removeKey(rawKey: string): boolean {
    const idx = this.keys.findIndex((k) => k.key === rawKey);
    if (idx === -1) return false;
    const removed = this.keys.splice(idx, 1)[0]!;
    this.saveNow();
    log("info", "Key removed", { label: removed.label });
    return true;
  }

  listKeys(): readonly MaskedKeyEntry[] {
    const currentTime = now();
    return this.keys.map(
      (k): MaskedKeyEntry => ({
        maskedKey: maskKey(k.key),
        label: k.label,
        stats: k.stats,
        availableAt: k.availableAt,
        isAvailable: k.availableAt <= currentTime,
      })
    );
  }

  // ── Persistence ─────────────────────────────────────────────────

  private load(): void {
    if (!existsSync(this.statePath)) {
      this.keys = [];
      return;
    }
    try {
      const raw = readFileSync(this.statePath, "utf-8");
      const state = JSON.parse(raw) as StoredState;
      if (state.version !== 1) {
        throw new Error(`Unknown state version: ${String(state.version)}`);
      }
      this.keys = [...state.keys];
      log("info", `Loaded ${this.keys.length} key(s) from disk`);
    } catch (err) {
      log("error", "Failed to load state, starting fresh", {
        error: String(err),
      });
      this.keys = [];
    }
  }

  private scheduleSave(): void {
    if (this.saveTimer) return;
    this.saveTimer = setTimeout(() => {
      this.saveTimer = null;
      this.saveNow();
    }, 1_000);
  }

  private saveNow(): void {
    const dir = this.statePath.replace(/\/[^/]+$/, "");
    mkdirSync(dir, { recursive: true });

    // Atomic write: write to temp file, then rename so a kill mid-write
    // never leaves a truncated state.json
    const tmp = this.statePath + ".tmp";
    const state: StoredState = { version: 1, keys: this.keys };
    writeFileSync(tmp, JSON.stringify(state, null, 2));
    renameSync(tmp, this.statePath);
  }
}

// ── Helpers ───────────────────────────────────────────────────────

function freshStats(): ApiKeyStats {
  return {
    totalRequests: 0,
    successfulRequests: 0,
    rateLimitHits: 0,
    errors: 0,
    lastUsedAt: null,
    addedAt: now(),
    totalTokensIn: 0,
    totalTokensOut: 0,
  };
}

function maskKey(key: string): string {
  if (key.length <= 12) return "****";
  return `${key.slice(0, 10)}...${key.slice(-4)}`;
}
