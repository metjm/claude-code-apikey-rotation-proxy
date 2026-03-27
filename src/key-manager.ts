import { Database } from "bun:sqlite";
import { existsSync, mkdirSync, readFileSync, unlinkSync } from "node:fs";
import { dirname, join } from "node:path";
import {
  type ApiKey,
  type ApiKeyEntry,
  type ApiKeyStats,
  type KeyLabel,
  type MaskedKeyEntry,
  type MaskedTokenEntry,
  type ProxyToken,
  type ProxyTokenEntry,
  type ProxyTokenStats,
  type StoredState,
  type UnixMs,
  asApiKey,
  asKeyLabel,
  asProxyToken,
  now,
  unixMs,
} from "./types.ts";
import { log } from "./logger.ts";

// ── SQLite row shapes ─────────────────────────────────────────────

interface KeyRow {
  key: string;
  label: string;
  total_requests: number;
  successful_requests: number;
  rate_limit_hits: number;
  errors: number;
  last_used_at: number | null;
  added_at: number;
  total_tokens_in: number;
  total_tokens_out: number;
  available_at: number;
}

interface TokenRow {
  token: string;
  label: string;
  total_requests: number;
  successful_requests: number;
  errors: number;
  last_used_at: number | null;
  added_at: number;
  total_tokens_in: number;
  total_tokens_out: number;
}

// ── KeyManager ────────────────────────────────────────────────────

export class KeyManager {
  private keys: ApiKeyEntry[] = [];
  private tokens: ProxyTokenEntry[] = [];
  private readonly db: Database;
  private readonly dbPath: string;
  private saveTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(dataDir: string, opts?: { registerShutdownHandler?: boolean }) {
    this.dbPath = process.env["DB_PATH"] ?? join(dataDir, "state.db");
    mkdirSync(dirname(this.dbPath), { recursive: true });

    this.db = new Database(this.dbPath, { create: true });
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA busy_timeout = 5000");

    this.initSchema();
    this.migrateFromJson(dataDir);
    this.loadFromDb();

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
    this.db.close();
    process.exit(0);
  }

  // ── Schema ──────────────────────────────────────────────────────

  private initSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS api_keys (
        key TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        total_requests INTEGER NOT NULL DEFAULT 0,
        successful_requests INTEGER NOT NULL DEFAULT 0,
        rate_limit_hits INTEGER NOT NULL DEFAULT 0,
        errors INTEGER NOT NULL DEFAULT 0,
        last_used_at INTEGER,
        added_at INTEGER NOT NULL,
        total_tokens_in INTEGER NOT NULL DEFAULT 0,
        total_tokens_out INTEGER NOT NULL DEFAULT 0,
        available_at INTEGER NOT NULL DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS proxy_tokens (
        token TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        total_requests INTEGER NOT NULL DEFAULT 0,
        successful_requests INTEGER NOT NULL DEFAULT 0,
        errors INTEGER NOT NULL DEFAULT 0,
        last_used_at INTEGER,
        added_at INTEGER NOT NULL,
        total_tokens_in INTEGER NOT NULL DEFAULT 0,
        total_tokens_out INTEGER NOT NULL DEFAULT 0
      );
    `);
  }

  // ── Migration from legacy state.json ────────────────────────────

  private migrateFromJson(dataDir: string): void {
    const jsonPath = join(dataDir, "state.json");
    if (!existsSync(jsonPath)) return;

    // Only migrate into an empty database
    const keyCount = this.db.query("SELECT COUNT(*) as c FROM api_keys").get() as { c: number };
    const tokenCount = this.db.query("SELECT COUNT(*) as c FROM proxy_tokens").get() as { c: number };
    if (keyCount.c > 0 || tokenCount.c > 0) return;

    try {
      const raw = readFileSync(jsonPath, "utf-8");
      const state = JSON.parse(raw) as StoredState;

      const insertKey = this.db.prepare(`
        INSERT OR IGNORE INTO api_keys (key, label, total_requests, successful_requests,
          rate_limit_hits, errors, last_used_at, added_at, total_tokens_in, total_tokens_out, available_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      const insertToken = this.db.prepare(`
        INSERT OR IGNORE INTO proxy_tokens (token, label, total_requests, successful_requests,
          errors, last_used_at, added_at, total_tokens_in, total_tokens_out)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);

      this.db.transaction(() => {
        for (const k of state.keys) {
          insertKey.run(
            k.key, k.label, k.stats.totalRequests, k.stats.successfulRequests,
            k.stats.rateLimitHits, k.stats.errors, k.stats.lastUsedAt, k.stats.addedAt,
            k.stats.totalTokensIn, k.stats.totalTokensOut, k.availableAt,
          );
        }
        for (const t of state.tokens ?? []) {
          insertToken.run(
            t.token, t.label, t.stats.totalRequests, t.stats.successfulRequests,
            t.stats.errors, t.stats.lastUsedAt, t.stats.addedAt,
            t.stats.totalTokensIn, t.stats.totalTokensOut,
          );
        }
      })();

      // Remove old file after successful migration
      try { unlinkSync(jsonPath); } catch {}
      // Also clean up .tmp file if it exists
      try { unlinkSync(jsonPath + ".tmp"); } catch {}

      log("info", `Migrated ${state.keys.length} key(s) and ${(state.tokens ?? []).length} token(s) from state.json to SQLite`);
    } catch (err) {
      log("error", "Failed to migrate state.json", { error: String(err) });
    }
  }

  // ── Load from DB ────────────────────────────────────────────────

  private loadFromDb(): void {
    const keyRows = this.db.query("SELECT * FROM api_keys").all() as KeyRow[];
    this.keys = keyRows.map((r) => rowToKeyEntry(r));

    const tokenRows = this.db.query("SELECT * FROM proxy_tokens").all() as TokenRow[];
    this.tokens = tokenRows.map((r) => rowToTokenEntry(r));

    log("info", `Loaded ${this.keys.length} key(s) and ${this.tokens.length} token(s) from SQLite`);
  }

  // ── Key selection ───────────────────────────────────────────────

  getNextAvailableKey(): ApiKeyEntry | null {
    if (this.keys.length === 0) return null;

    const currentTime = now();
    const available = this.keys
      .filter((k) => k.availableAt <= currentTime)
      .sort((a, b) => (b.stats.lastUsedAt ?? 0) - (a.stats.lastUsedAt ?? 0));

    if (available.length > 0) return available[0]!;
    return null;
  }

  getEarliestAvailableKey(): ApiKeyEntry | null {
    if (this.keys.length === 0) return null;
    return [...this.keys].sort((a, b) => a.availableAt - b.availableAt)[0]!;
  }

  availableCount(): number {
    const currentTime = now();
    return this.keys.filter((k) => k.availableAt <= currentTime).length;
  }

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
    this.db.run(
      `INSERT INTO api_keys (key, label, total_requests, successful_requests,
        rate_limit_hits, errors, last_used_at, added_at, total_tokens_in, total_tokens_out, available_at)
      VALUES (?, ?, 0, 0, 0, 0, NULL, ?, 0, 0, 0)`,
      [entry.key, entry.label, entry.stats.addedAt],
    );
    log("info", "Key added", { label: entry.label });
    return entry;
  }

  updateKeyLabel(rawKey: string, newLabel: string): boolean {
    const entry = this.keys.find((k) => k.key === rawKey);
    if (!entry) return false;
    (entry as { label: KeyLabel }).label = asKeyLabel(newLabel);
    this.db.run("UPDATE api_keys SET label = ? WHERE key = ?", [newLabel, rawKey]);
    log("info", "Key label updated", { label: newLabel });
    return true;
  }

  removeKey(rawKey: string): boolean {
    const idx = this.keys.findIndex((k) => k.key === rawKey);
    if (idx === -1) return false;
    const removed = this.keys.splice(idx, 1)[0]!;
    this.db.run("DELETE FROM api_keys WHERE key = ?", [removed.key]);
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

  // ── Proxy token CRUD ──────────────────────────────────────────

  addToken(rawToken: string, label?: string): ProxyTokenEntry {
    const token = asProxyToken(rawToken);

    if (this.tokens.some((t) => t.token === token)) {
      throw new Error("Token already registered");
    }

    const entry: ProxyTokenEntry = {
      token,
      label: label ?? `user-${this.tokens.length + 1}`,
      stats: freshTokenStats(),
    };

    this.tokens.push(entry);
    this.db.run(
      `INSERT INTO proxy_tokens (token, label, total_requests, successful_requests,
        errors, last_used_at, added_at, total_tokens_in, total_tokens_out)
      VALUES (?, ?, 0, 0, 0, NULL, ?, 0, 0)`,
      [entry.token, entry.label, entry.stats.addedAt],
    );
    log("info", "Proxy token added", { label: entry.label });
    return entry;
  }

  updateTokenLabel(rawToken: string, newLabel: string): boolean {
    const entry = this.tokens.find((t) => t.token === rawToken);
    if (!entry) return false;
    (entry as { label: string }).label = newLabel;
    this.db.run("UPDATE proxy_tokens SET label = ? WHERE token = ?", [newLabel, rawToken]);
    log("info", "Token label updated", { label: newLabel });
    return true;
  }

  removeToken(rawToken: string): boolean {
    const idx = this.tokens.findIndex((t) => t.token === rawToken);
    if (idx === -1) return false;
    const removed = this.tokens.splice(idx, 1)[0]!;
    this.db.run("DELETE FROM proxy_tokens WHERE token = ?", [removed.token]);
    log("info", "Proxy token removed", { label: removed.label });
    return true;
  }

  listTokens(): readonly MaskedTokenEntry[] {
    return this.tokens.map(
      (t): MaskedTokenEntry => ({
        maskedToken: maskToken(t.token),
        label: t.label,
        stats: t.stats,
      })
    );
  }

  hasTokens(): boolean {
    return this.tokens.length > 0;
  }

  validateToken(raw: string): ProxyTokenEntry | null {
    return this.tokens.find((t) => t.token === raw) ?? null;
  }

  // ── Proxy token stats ─────────────────────────────────────────

  recordTokenRequest(entry: ProxyTokenEntry): void {
    entry.stats = {
      ...entry.stats,
      totalRequests: entry.stats.totalRequests + 1,
      lastUsedAt: now(),
    };
    this.scheduleSave();
  }

  recordTokenSuccess(entry: ProxyTokenEntry, tokensIn: number, tokensOut: number): void {
    entry.stats = {
      ...entry.stats,
      successfulRequests: entry.stats.successfulRequests + 1,
      totalTokensIn: entry.stats.totalTokensIn + tokensIn,
      totalTokensOut: entry.stats.totalTokensOut + tokensOut,
    };
    this.scheduleSave();
  }

  recordTokenError(entry: ProxyTokenEntry): void {
    entry.stats = {
      ...entry.stats,
      errors: entry.stats.errors + 1,
    };
    this.scheduleSave();
  }

  // ── Persistence ─────────────────────────────────────────────────

  private scheduleSave(): void {
    if (this.saveTimer) return;
    this.saveTimer = setTimeout(() => {
      this.saveTimer = null;
      this.saveNow();
    }, 1_000);
  }

  private saveNow(): void {
    const updateKey = this.db.prepare(`
      UPDATE api_keys SET
        total_requests = ?, successful_requests = ?, rate_limit_hits = ?,
        errors = ?, last_used_at = ?, total_tokens_in = ?, total_tokens_out = ?,
        available_at = ?
      WHERE key = ?
    `);
    const updateToken = this.db.prepare(`
      UPDATE proxy_tokens SET
        total_requests = ?, successful_requests = ?, errors = ?,
        last_used_at = ?, total_tokens_in = ?, total_tokens_out = ?
      WHERE token = ?
    `);

    this.db.transaction(() => {
      for (const k of this.keys) {
        updateKey.run(
          k.stats.totalRequests, k.stats.successfulRequests, k.stats.rateLimitHits,
          k.stats.errors, k.stats.lastUsedAt, k.stats.totalTokensIn, k.stats.totalTokensOut,
          k.availableAt, k.key,
        );
      }
      for (const t of this.tokens) {
        updateToken.run(
          t.stats.totalRequests, t.stats.successfulRequests, t.stats.errors,
          t.stats.lastUsedAt, t.stats.totalTokensIn, t.stats.totalTokensOut,
          t.token,
        );
      }
    })();
  }
}

// ── Helpers ───────────────────────────────────────────────────────

function rowToKeyEntry(r: KeyRow): ApiKeyEntry {
  return {
    key: r.key as ApiKey,
    label: r.label as KeyLabel,
    stats: {
      totalRequests: r.total_requests,
      successfulRequests: r.successful_requests,
      rateLimitHits: r.rate_limit_hits,
      errors: r.errors,
      lastUsedAt: r.last_used_at as UnixMs | null,
      addedAt: r.added_at as UnixMs,
      totalTokensIn: r.total_tokens_in,
      totalTokensOut: r.total_tokens_out,
    },
    availableAt: r.available_at as UnixMs,
  };
}

function rowToTokenEntry(r: TokenRow): ProxyTokenEntry {
  return {
    token: r.token as ProxyToken,
    label: r.label,
    stats: {
      totalRequests: r.total_requests,
      successfulRequests: r.successful_requests,
      errors: r.errors,
      lastUsedAt: r.last_used_at as UnixMs | null,
      addedAt: r.added_at as UnixMs,
      totalTokensIn: r.total_tokens_in,
      totalTokensOut: r.total_tokens_out,
    },
  };
}

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

function freshTokenStats(): ProxyTokenStats {
  return {
    totalRequests: 0,
    successfulRequests: 0,
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

function maskToken(token: string): string {
  if (token.length <= 12) return "****";
  return `${token.slice(0, 4)}...${token.slice(-4)}`;
}

