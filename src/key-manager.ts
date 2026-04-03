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
  type TimeseriesBucket,
  type TimeseriesQuery,
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
  total_cache_read: number;
  total_cache_creation: number;
  available_at: number;
  priority: number;
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
  total_cache_read: number;
  total_cache_creation: number;
}

// ── KeyManager ────────────────────────────────────────────────────

interface BucketAccumulator {
  requests: number;
  successes: number;
  errors: number;
  rateLimits: number;
  tokensIn: number;
  tokensOut: number;
  cacheRead: number;
  cacheCreation: number;
}

function emptyBucket(): BucketAccumulator {
  return { requests: 0, successes: 0, errors: 0, rateLimits: 0, tokensIn: 0, tokensOut: 0, cacheRead: 0, cacheCreation: 0 };
}

function currentBucketKey(): string {
  return new Date().toISOString().slice(0, 13);
}

export class KeyManager {
  private keys: ApiKeyEntry[] = [];
  private tokens: ProxyTokenEntry[] = [];
  private readonly db: Database;
  readonly dbPath: string;
  private readonly cleanupInterval: ReturnType<typeof setInterval>;
  private saveTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly tsAccumulator = new Map<string, BucketAccumulator>();

  constructor(dataDir: string) {
    this.dbPath = process.env["DB_PATH"] ?? join(dataDir, "state.db");
    mkdirSync(dirname(this.dbPath), { recursive: true });

    this.db = new Database(this.dbPath, { create: true });
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA busy_timeout = 5000");

    this.initSchema();
    this.migrateSchema();
    this.migrateFromJson(dataDir);
    this.loadFromDb();

    this.cleanupInterval = setInterval(() => this.cleanupOldTimeseries(), 60 * 60 * 1000);
  }

  close(): void {
    clearInterval(this.cleanupInterval);
    if (this.saveTimer) {
      clearTimeout(this.saveTimer);
      this.saveTimer = null;
    }
    this.saveNow();
    this.db.close();
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
        total_cache_read INTEGER NOT NULL DEFAULT 0,
        total_cache_creation INTEGER NOT NULL DEFAULT 0,
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
        total_tokens_out INTEGER NOT NULL DEFAULT 0,
        total_cache_read INTEGER NOT NULL DEFAULT 0,
        total_cache_creation INTEGER NOT NULL DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS stats_timeseries (
        bucket       TEXT NOT NULL,
        key_label    TEXT NOT NULL,
        user_label   TEXT NOT NULL,
        requests     INTEGER NOT NULL DEFAULT 0,
        successes    INTEGER NOT NULL DEFAULT 0,
        errors       INTEGER NOT NULL DEFAULT 0,
        rate_limits  INTEGER NOT NULL DEFAULT 0,
        tokens_in    INTEGER NOT NULL DEFAULT 0,
        tokens_out   INTEGER NOT NULL DEFAULT 0,
        cache_read   INTEGER NOT NULL DEFAULT 0,
        cache_creation INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (bucket, key_label, user_label)
      );

      CREATE INDEX IF NOT EXISTS idx_stats_ts_bucket ON stats_timeseries(bucket);
    `);

    // Migrate existing tables to add cache columns
    const migrate = (table: string, col: string) => {
      try { this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${col} INTEGER NOT NULL DEFAULT 0`); } catch {}
    };
    migrate("api_keys", "total_cache_read");
    migrate("api_keys", "total_cache_creation");
    migrate("proxy_tokens", "total_cache_read");
    migrate("proxy_tokens", "total_cache_creation");
    migrate("stats_timeseries", "cache_read");
    migrate("stats_timeseries", "cache_creation");

    // One-time reset: old token counts were inaccurate (missing cache tokens)
    const marker = this.db.query("SELECT 1 FROM stats_timeseries WHERE key_label = '__reset_v2__'").get();
    if (!marker) {
      this.db.exec("UPDATE api_keys SET total_tokens_in = 0, total_tokens_out = 0, total_cache_read = 0, total_cache_creation = 0");
      this.db.exec("UPDATE proxy_tokens SET total_tokens_in = 0, total_tokens_out = 0, total_cache_read = 0, total_cache_creation = 0");
      this.db.exec("DELETE FROM stats_timeseries");
      this.db.run("INSERT INTO stats_timeseries (bucket, key_label, user_label) VALUES ('__reset_v2__', '__reset_v2__', '__reset_v2__')");
      log("info", "Reset token stats (v2: added cache tracking)");
    }
  }

  // ── Schema migrations ──────────────────────────────────────────

  private migrateSchema(): void {
    // Add priority column (added in v2)
    try {
      this.db.exec("ALTER TABLE api_keys ADD COLUMN priority INTEGER NOT NULL DEFAULT 2");
    } catch {
      // Column already exists — ignore
    }
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
      .sort((a, b) => {
        // Sort by priority first (lower = preferred), then LRU within same tier
        if (a.priority !== b.priority) return a.priority - b.priority;
        return (b.stats.lastUsedAt ?? 0) - (a.stats.lastUsedAt ?? 0);
      });

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
    this.tsIncrement(entry.label, "__all__", "requests");
    this.scheduleSave();
  }

  recordSuccess(entry: ApiKeyEntry, tokensIn: number, tokensOut: number, cacheRead = 0, cacheCreation = 0): void {
    entry.stats = {
      ...entry.stats,
      successfulRequests: entry.stats.successfulRequests + 1,
      totalTokensIn: entry.stats.totalTokensIn + tokensIn,
      totalTokensOut: entry.stats.totalTokensOut + tokensOut,
      totalCacheRead: entry.stats.totalCacheRead + cacheRead,
      totalCacheCreation: entry.stats.totalCacheCreation + cacheCreation,
    };
    this.tsIncrement(entry.label, "__all__", "successes");
    this.tsAddTokens(entry.label, "__all__", tokensIn, tokensOut, cacheRead, cacheCreation);
    this.scheduleSave();
  }

  recordRateLimit(entry: ApiKeyEntry, retryAfterSecs: number): void {
    const waitMs = (retryAfterSecs > 0 ? retryAfterSecs : 60) * 1000;
    entry.stats = {
      ...entry.stats,
      rateLimitHits: entry.stats.rateLimitHits + 1,
    };
    entry.availableAt = unixMs(Date.now() + waitMs);
    this.tsIncrement(entry.label, "__all__", "rateLimits");
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
    this.tsIncrement(entry.label, "__all__", "errors");
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
      priority: 2,
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

  updateKeyPriority(rawKey: string, priority: number): boolean {
    const entry = this.keys.find((k) => k.key === rawKey);
    if (!entry) return false;
    entry.priority = priority;
    this.db.run("UPDATE api_keys SET priority = ? WHERE key = ?", [priority, rawKey]);
    log("info", "Key priority updated", { label: entry.label, priority });
    return true;
  }

  updateKeyPriorityByMask(masked: string, priority: number): boolean {
    const entry = this.keys.find((k) => maskKey(k.key) === masked);
    if (!entry) return false;
    entry.priority = priority;
    this.db.run("UPDATE api_keys SET priority = ? WHERE key = ?", [priority, entry.key]);
    log("info", "Key priority updated", { label: entry.label, priority });
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

  removeKeyByMasked(masked: string): boolean {
    const idx = this.keys.findIndex((k) => maskKey(k.key) === masked);
    if (idx === -1) return false;
    const removed = this.keys.splice(idx, 1)[0]!;
    this.db.run("DELETE FROM api_keys WHERE key = ?", [removed.key]);
    log("info", "Key removed", { label: removed.label });
    return true;
  }

  updateKeyLabelByMasked(masked: string, newLabel: string): boolean {
    const entry = this.keys.find((k) => maskKey(k.key) === masked);
    if (!entry) return false;
    (entry as { label: KeyLabel }).label = asKeyLabel(newLabel);
    this.db.run("UPDATE api_keys SET label = ? WHERE key = ?", [newLabel, entry.key]);
    log("info", "Key label updated", { label: newLabel });
    return true;
  }

  listKeys(): readonly MaskedKeyEntry[] {
    const currentTime = now();
    const recentErrs = this.recentErrorsByDimension("key_label", "user_label", "__all__");
    return this.keys.map(
      (k): MaskedKeyEntry => ({
        maskedKey: maskKey(k.key),
        label: k.label,
        stats: k.stats,
        availableAt: k.availableAt,
        isAvailable: k.availableAt <= currentTime,
        priority: k.priority,
        recentErrors: recentErrs.get(k.label) ?? 0,
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

  removeTokenByMasked(masked: string): boolean {
    const idx = this.tokens.findIndex((t) => maskToken(t.token) === masked);
    if (idx === -1) return false;
    const removed = this.tokens.splice(idx, 1)[0]!;
    this.db.run("DELETE FROM proxy_tokens WHERE token = ?", [removed.token]);
    log("info", "Proxy token removed", { label: removed.label });
    return true;
  }

  updateTokenLabelByMasked(masked: string, newLabel: string): boolean {
    const entry = this.tokens.find((t) => maskToken(t.token) === masked);
    if (!entry) return false;
    (entry as { label: string }).label = newLabel;
    this.db.run("UPDATE proxy_tokens SET label = ? WHERE token = ?", [newLabel, entry.token]);
    log("info", "Token label updated", { label: newLabel });
    return true;
  }

  listTokens(): readonly MaskedTokenEntry[] {
    const recentErrs = this.recentErrorsByDimension("user_label", "key_label", "__all__");
    return this.tokens.map(
      (t): MaskedTokenEntry => ({
        maskedToken: maskToken(t.token),
        label: t.label,
        stats: t.stats,
        recentErrors: recentErrs.get(t.label) ?? 0,
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
    this.tsIncrement("__all__", entry.label, "requests");
    this.scheduleSave();
  }

  recordTokenSuccess(entry: ProxyTokenEntry, tokensIn: number, tokensOut: number, cacheRead = 0, cacheCreation = 0): void {
    entry.stats = {
      ...entry.stats,
      successfulRequests: entry.stats.successfulRequests + 1,
      totalTokensIn: entry.stats.totalTokensIn + tokensIn,
      totalTokensOut: entry.stats.totalTokensOut + tokensOut,
      totalCacheRead: entry.stats.totalCacheRead + cacheRead,
      totalCacheCreation: entry.stats.totalCacheCreation + cacheCreation,
    };
    this.tsIncrement("__all__", entry.label, "successes");
    this.tsAddTokens("__all__", entry.label, tokensIn, tokensOut, cacheRead, cacheCreation);
    this.scheduleSave();
  }

  recordTokenError(entry: ProxyTokenEntry): void {
    entry.stats = {
      ...entry.stats,
      errors: entry.stats.errors + 1,
    };
    this.tsIncrement("__all__", entry.label, "errors");
    this.scheduleSave();
  }

  // ── Recent error queries ────────────────────────────────────────

  /**
   * Returns errors in the last hour grouped by a dimension (key_label or user_label).
   * Combines persisted DB rows with the in-memory accumulator for the current bucket.
   */
  private recentErrorsByDimension(
    groupCol: "key_label" | "user_label",
    filterCol: "key_label" | "user_label",
    filterVal: string,
  ): Map<string, number> {
    const cutoff = new Date(Date.now() - 60 * 60 * 1000).toISOString().slice(0, 13);
    const rows = this.db.query(
      `SELECT ${groupCol} AS lbl, SUM(errors) AS errs FROM stats_timeseries
       WHERE bucket >= ? AND ${filterCol} = ? AND ${groupCol} != '__all__'
       GROUP BY ${groupCol}`
    ).all(cutoff, filterVal) as { lbl: string; errs: number }[];

    const result = new Map<string, number>();
    for (const r of rows) result.set(r.lbl, r.errs);

    // Add unflushed in-memory accumulator data for the current bucket
    const bucket = currentBucketKey();
    for (const [mapKey, acc] of this.tsAccumulator) {
      if (acc.errors === 0) continue;
      const [b, keyLabel, userLabel] = mapKey.split("|");
      if (b !== bucket) continue;
      const dim = groupCol === "key_label" ? keyLabel! : userLabel!;
      const filter = filterCol === "key_label" ? keyLabel! : userLabel!;
      if (filter !== filterVal || dim === "__all__") continue;
      result.set(dim, (result.get(dim) ?? 0) + acc.errors);
    }

    return result;
  }

  // ── Timeseries helpers ──────────────────────────────────────────

  private tsIncrement(keyLabel: string, userLabel: string, field: keyof Omit<BucketAccumulator, "tokensIn" | "tokensOut" | "cacheRead" | "cacheCreation">): void {
    const bucket = currentBucketKey();
    const mapKey = `${bucket}|${keyLabel}|${userLabel}`;
    const acc = this.tsAccumulator.get(mapKey) ?? emptyBucket();
    acc[field]++;
    this.tsAccumulator.set(mapKey, acc);

    const globalKey = `${bucket}|__all__|__all__`;
    if (mapKey !== globalKey) {
      const global = this.tsAccumulator.get(globalKey) ?? emptyBucket();
      global[field]++;
      this.tsAccumulator.set(globalKey, global);
    }
  }

  private tsAddTokens(keyLabel: string, userLabel: string, tokensIn: number, tokensOut: number, cacheRead = 0, cacheCreation = 0): void {
    if (tokensIn === 0 && tokensOut === 0 && cacheRead === 0 && cacheCreation === 0) return;
    const bucket = currentBucketKey();
    const mapKey = `${bucket}|${keyLabel}|${userLabel}`;
    const acc = this.tsAccumulator.get(mapKey) ?? emptyBucket();
    acc.tokensIn += tokensIn;
    acc.tokensOut += tokensOut;
    acc.cacheRead += cacheRead;
    acc.cacheCreation += cacheCreation;
    this.tsAccumulator.set(mapKey, acc);

    const globalKey = `${bucket}|__all__|__all__`;
    if (mapKey !== globalKey) {
      const global = this.tsAccumulator.get(globalKey) ?? emptyBucket();
      global.tokensIn += tokensIn;
      global.tokensOut += tokensOut;
      global.cacheRead += cacheRead;
      global.cacheCreation += cacheCreation;
      this.tsAccumulator.set(globalKey, global);
    }
  }

  getCurrentBucket(): TimeseriesBucket {
    const bucket = currentBucketKey();

    const row = this.db.query(
      "SELECT requests, successes, errors, rate_limits, tokens_in, tokens_out, cache_read, cache_creation FROM stats_timeseries WHERE bucket = ? AND key_label = '__all__' AND user_label = '__all__'"
    ).get(bucket) as { requests: number; successes: number; errors: number; rate_limits: number; tokens_in: number; tokens_out: number; cache_read: number; cache_creation: number } | null;

    const acc = this.tsAccumulator.get(`${bucket}|__all__|__all__`);

    return {
      bucket,
      requests:      (row?.requests ?? 0)       + (acc?.requests ?? 0),
      successes:     (row?.successes ?? 0)      + (acc?.successes ?? 0),
      errors:        (row?.errors ?? 0)         + (acc?.errors ?? 0),
      rateLimits:    (row?.rate_limits ?? 0)    + (acc?.rateLimits ?? 0),
      tokensIn:      (row?.tokens_in ?? 0)      + (acc?.tokensIn ?? 0),
      tokensOut:     (row?.tokens_out ?? 0)     + (acc?.tokensOut ?? 0),
      cacheRead:     (row?.cache_read ?? 0)     + (acc?.cacheRead ?? 0),
      cacheCreation: (row?.cache_creation ?? 0) + (acc?.cacheCreation ?? 0),
    };
  }

  queryTimeseries(opts: TimeseriesQuery): TimeseriesBucket[] {
    const hours = Math.min(opts.hours ?? 24, 720);
    const resolution = opts.resolution ?? "hour";
    const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString().slice(0, 13);

    const groupExpr = resolution === "day" ? "substr(bucket, 1, 10)" : "bucket";
    const conditions = ["bucket >= ?"];
    const params: (string | null)[] = [cutoff];

    if (opts.keyLabel) {
      conditions.push("key_label = ?");
      params.push(opts.keyLabel);
    }
    if (opts.userLabel) {
      conditions.push("user_label = ?");
      params.push(opts.userLabel);
    }
    if (!opts.keyLabel && !opts.userLabel) {
      conditions.push("key_label = '__all__' AND user_label = '__all__'");
    }

    const sql = `
      SELECT ${groupExpr} AS b,
        SUM(requests) AS requests, SUM(successes) AS successes,
        SUM(errors) AS errors, SUM(rate_limits) AS rate_limits,
        SUM(tokens_in) AS tokens_in, SUM(tokens_out) AS tokens_out,
        SUM(cache_read) AS cache_read, SUM(cache_creation) AS cache_creation
      FROM stats_timeseries
      WHERE ${conditions.join(" AND ")}
      GROUP BY b ORDER BY b
    `;

    const rows = this.db.query(sql).all(...params) as Array<{
      b: string; requests: number; successes: number; errors: number;
      rate_limits: number; tokens_in: number; tokens_out: number;
      cache_read: number; cache_creation: number;
    }>;

    return rows.map((r) => ({
      bucket: r.b,
      requests: r.requests,
      successes: r.successes,
      errors: r.errors,
      rateLimits: r.rate_limits,
      tokensIn: r.tokens_in,
      tokensOut: r.tokens_out,
      cacheRead: r.cache_read,
      cacheCreation: r.cache_creation,
    }));
  }

  private cleanupOldTimeseries(): void {
    const cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 13);
    this.db.run("DELETE FROM stats_timeseries WHERE bucket < ?", [cutoff]);
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
        total_cache_read = ?, total_cache_creation = ?, available_at = ?
      WHERE key = ?
    `);
    const updateToken = this.db.prepare(`
      UPDATE proxy_tokens SET
        total_requests = ?, successful_requests = ?, errors = ?,
        last_used_at = ?, total_tokens_in = ?, total_tokens_out = ?,
        total_cache_read = ?, total_cache_creation = ?
      WHERE token = ?
    `);
    const upsertTs = this.db.prepare(`
      INSERT INTO stats_timeseries (bucket, key_label, user_label, requests, successes, errors, rate_limits, tokens_in, tokens_out, cache_read, cache_creation)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(bucket, key_label, user_label) DO UPDATE SET
        requests       = requests       + excluded.requests,
        successes      = successes      + excluded.successes,
        errors         = errors         + excluded.errors,
        rate_limits    = rate_limits    + excluded.rate_limits,
        tokens_in      = tokens_in      + excluded.tokens_in,
        tokens_out     = tokens_out     + excluded.tokens_out,
        cache_read     = cache_read     + excluded.cache_read,
        cache_creation = cache_creation + excluded.cache_creation
    `);

    this.db.transaction(() => {
      for (const k of this.keys) {
        updateKey.run(
          k.stats.totalRequests, k.stats.successfulRequests, k.stats.rateLimitHits,
          k.stats.errors, k.stats.lastUsedAt, k.stats.totalTokensIn, k.stats.totalTokensOut,
          k.stats.totalCacheRead, k.stats.totalCacheCreation, k.availableAt, k.key,
        );
      }
      for (const t of this.tokens) {
        updateToken.run(
          t.stats.totalRequests, t.stats.successfulRequests, t.stats.errors,
          t.stats.lastUsedAt, t.stats.totalTokensIn, t.stats.totalTokensOut,
          t.stats.totalCacheRead, t.stats.totalCacheCreation, t.token,
        );
      }
      for (const [mapKey, acc] of this.tsAccumulator) {
        const [bucket, keyLabel, userLabel] = mapKey.split("|");
        upsertTs.run(bucket, keyLabel, userLabel, acc.requests, acc.successes, acc.errors, acc.rateLimits, acc.tokensIn, acc.tokensOut, acc.cacheRead, acc.cacheCreation);
      }
      this.tsAccumulator.clear();
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
      totalCacheRead: r.total_cache_read ?? 0,
      totalCacheCreation: r.total_cache_creation ?? 0,
    },
    availableAt: r.available_at as UnixMs,
    priority: r.priority,
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
      totalCacheRead: r.total_cache_read ?? 0,
      totalCacheCreation: r.total_cache_creation ?? 0,
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
    totalCacheRead: 0,
    totalCacheCreation: 0,
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
    totalCacheRead: 0,
    totalCacheCreation: 0,
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

