import { Database } from "bun:sqlite";
import { existsSync, mkdirSync, readFileSync, unlinkSync } from "node:fs";
import { dirname, join } from "node:path";
import {
  type ApiKey,
  type ApiKeyCapacityState,
  type ApiKeyEntry,
  type ApiKeyStats,
  type CapacityHealth,
  type CapacityObservation,
  type CapacitySummary,
  type CapacitySummaryWindow,
  type CapacityTimeseriesBucket,
  type CapacityTimeseriesQuery,
  type CapacityWindowSnapshot,
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

interface CapacityStateRow {
  key: string;
  response_count: number;
  normalized_header_count: number;
  last_response_at: number | null;
  last_header_at: number | null;
  last_upstream_status: number | null;
  last_request_id: string | null;
  organization_id: string | null;
  representative_claim: string | null;
  retry_after_secs: number | null;
  should_retry: number | null;
  fallback_available: number | null;
  fallback_percentage: number | null;
  overage_status: string | null;
  overage_disabled_reason: string | null;
  latency_ms: number | null;
}

interface CapacityCoverageRow {
  key: string;
  signal_name: string;
  seen_count: number;
  last_seen_at: number | null;
}

interface CapacityWindowRow {
  key: string;
  window_name: string;
  status: string | null;
  utilization: number | null;
  reset_at: number | null;
  surpassed_threshold: number | null;
  last_seen_at: number | null;
}

interface CapacityTsRow {
  b: string;
  window_name: string;
  samples: number;
  allowed_count: number;
  warning_count: number;
  rejected_count: number;
  utilization_sum: number;
  utilization_samples: number;
  utilization_max: number;
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

interface CapacityBucketAccumulator {
  samples: number;
  allowedCount: number;
  warningCount: number;
  rejectedCount: number;
  utilizationSum: number;
  utilizationSamples: number;
  utilizationMax: number;
}

function emptyBucket(): BucketAccumulator {
  return { requests: 0, successes: 0, errors: 0, rateLimits: 0, tokensIn: 0, tokensOut: 0, cacheRead: 0, cacheCreation: 0 };
}

function emptyCapacityBucket(): CapacityBucketAccumulator {
  return { samples: 0, allowedCount: 0, warningCount: 0, rejectedCount: 0, utilizationSum: 0, utilizationSamples: 0, utilizationMax: 0 };
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
  private readonly capacityTsAccumulator = new Map<string, CapacityBucketAccumulator>();

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

      CREATE TABLE IF NOT EXISTS api_key_capacity_state (
        key TEXT PRIMARY KEY,
        response_count INTEGER NOT NULL DEFAULT 0,
        normalized_header_count INTEGER NOT NULL DEFAULT 0,
        last_response_at INTEGER,
        last_header_at INTEGER,
        last_upstream_status INTEGER,
        last_request_id TEXT,
        organization_id TEXT,
        representative_claim TEXT,
        retry_after_secs INTEGER,
        should_retry INTEGER,
        fallback_available INTEGER,
        fallback_percentage REAL,
        overage_status TEXT,
        overage_disabled_reason TEXT,
        latency_ms INTEGER
      );

      CREATE TABLE IF NOT EXISTS api_key_capacity_signal_coverage (
        key TEXT NOT NULL,
        signal_name TEXT NOT NULL,
        seen_count INTEGER NOT NULL DEFAULT 0,
        last_seen_at INTEGER,
        PRIMARY KEY (key, signal_name)
      );

      CREATE TABLE IF NOT EXISTS api_key_capacity_windows (
        key TEXT NOT NULL,
        window_name TEXT NOT NULL,
        status TEXT,
        utilization REAL,
        reset_at INTEGER,
        surpassed_threshold REAL,
        last_seen_at INTEGER,
        PRIMARY KEY (key, window_name)
      );

      CREATE TABLE IF NOT EXISTS capacity_window_timeseries (
        bucket TEXT NOT NULL,
        key_label TEXT NOT NULL,
        window_name TEXT NOT NULL,
        samples INTEGER NOT NULL DEFAULT 0,
        allowed_count INTEGER NOT NULL DEFAULT 0,
        warning_count INTEGER NOT NULL DEFAULT 0,
        rejected_count INTEGER NOT NULL DEFAULT 0,
        utilization_sum REAL NOT NULL DEFAULT 0,
        utilization_samples INTEGER NOT NULL DEFAULT 0,
        utilization_max REAL NOT NULL DEFAULT 0,
        PRIMARY KEY (bucket, key_label, window_name)
      );

      CREATE INDEX IF NOT EXISTS idx_capacity_window_ts_bucket ON capacity_window_timeseries(bucket);
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
    migrate("api_key_capacity_state", "response_count");
    migrate("api_key_capacity_state", "normalized_header_count");
    migrate("capacity_window_timeseries", "utilization_samples");

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
    const stateRows = this.db.query("SELECT * FROM api_key_capacity_state").all() as CapacityStateRow[];
    const coverageRows = this.db.query("SELECT * FROM api_key_capacity_signal_coverage").all() as CapacityCoverageRow[];
    const windowRows = this.db.query("SELECT * FROM api_key_capacity_windows").all() as CapacityWindowRow[];
    const statesByKey = new Map(stateRows.map((row) => [row.key, row]));
    const coverageByKey = new Map<string, CapacityCoverageRow[]>();
    for (const row of coverageRows) {
      const existing = coverageByKey.get(row.key) ?? [];
      existing.push(row);
      coverageByKey.set(row.key, existing);
    }
    const windowsByKey = new Map<string, CapacityWindowRow[]>();
    for (const row of windowRows) {
      const existing = windowsByKey.get(row.key) ?? [];
      existing.push(row);
      windowsByKey.set(row.key, existing);
    }

    const keyRows = this.db.query("SELECT * FROM api_keys").all() as KeyRow[];
    this.keys = keyRows.map((r) => rowToKeyEntry(
      r,
      statesByKey.get(r.key) ?? null,
      coverageByKey.get(r.key) ?? [],
      windowsByKey.get(r.key) ?? [],
    ));

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
        // Hard routing still follows the original rules: priority first, then sticky reuse.
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

  getCapacityHealth(entry: ApiKeyEntry): CapacityHealth {
    return deriveCapacityHealth(entry);
  }

  getCapacitySummary(): CapacitySummary {
    const keys = this.keys;
    let healthyKeys = 0;
    let warningKeys = 0;
    let rejectedKeys = 0;
    let coolingDownKeys = 0;
    let unknownKeys = 0;
    let fallbackAvailableKeys = 0;
    let overageRejectedKeys = 0;
    let lastUpdatedAt: UnixMs | null = null;
    const orgs = new Set<string>();
    const windowMap = new Map<string, {
      utils: number[];
      nextResetAt: UnixMs | null;
      knownKeys: number;
      allowedKeys: number;
      warningKeys: number;
      rejectedKeys: number;
    }>();

    for (const key of keys) {
      const health = this.getCapacityHealth(key);
      if (health === "healthy") healthyKeys++;
      else if (health === "warning") warningKeys++;
      else if (health === "rejected") rejectedKeys++;
      else if (health === "cooling_down") coolingDownKeys++;
      else unknownKeys++;

      if (key.capacity.organizationId) orgs.add(key.capacity.organizationId);
      if (key.capacity.fallbackAvailable) fallbackAvailableKeys++;
      if (key.capacity.overageStatus === "rejected") overageRejectedKeys++;
      if (key.capacity.lastHeaderAt && (!lastUpdatedAt || key.capacity.lastHeaderAt > lastUpdatedAt)) {
        lastUpdatedAt = key.capacity.lastHeaderAt;
      }

      for (const window of activeCapacityWindows(key)) {
        const existing = windowMap.get(window.windowName) ?? {
          utils: [],
          nextResetAt: null,
          knownKeys: 0,
          allowedKeys: 0,
          warningKeys: 0,
          rejectedKeys: 0,
        };
        existing.knownKeys++;
        if (window.utilization !== null) existing.utils.push(window.utilization);
        if (window.status === "allowed") existing.allowedKeys++;
        else if (window.status === "allowed_warning") existing.warningKeys++;
        else if (window.status === "rejected") existing.rejectedKeys++;
        if (window.resetAt !== null && window.resetAt > now()) {
          if (existing.nextResetAt === null || window.resetAt < existing.nextResetAt) {
            existing.nextResetAt = window.resetAt;
          }
        }
        windowMap.set(window.windowName, existing);
      }
    }

    const windows: CapacitySummaryWindow[] = [...windowMap.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([windowName, summary]) => {
        const sortedUtils = [...summary.utils].sort((a, b) => a - b);
        const medianUtilization = sortedUtils.length === 0
          ? null
          : sortedUtils[Math.floor(sortedUtils.length / 2)]!;
        const maxUtilization = sortedUtils.length === 0
          ? null
          : sortedUtils[sortedUtils.length - 1]!;
        return {
          windowName,
          knownKeys: summary.knownKeys,
          allowedKeys: summary.allowedKeys,
          warningKeys: summary.warningKeys,
          rejectedKeys: summary.rejectedKeys,
          maxUtilization,
          medianUtilization,
          nextResetAt: summary.nextResetAt,
        };
      });

    return {
      healthyKeys,
      warningKeys,
      rejectedKeys,
      coolingDownKeys,
      unknownKeys,
      fallbackAvailableKeys,
      overageRejectedKeys,
      distinctOrganizations: orgs.size,
      lastUpdatedAt,
      windows,
    };
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

  resetKeyCooldowns(): number {
    const currentTime = now();
    let reset = 0;
    for (const entry of this.keys) {
      if (entry.availableAt > currentTime) reset++;
      entry.availableAt = unixMs(0);
    }
    this.db.run("UPDATE api_keys SET available_at = 0");
    log("info", "Reset key cooldowns", { resetKeys: reset, totalKeys: this.keys.length });
    return reset;
  }

  recordError(entry: ApiKeyEntry): void {
    entry.stats = {
      ...entry.stats,
      errors: entry.stats.errors + 1,
    };
    this.tsIncrement(entry.label, "__all__", "errors");
    this.scheduleSave();
  }

  recordCapacityObservation(entry: ApiKeyEntry, observation: CapacityObservation): void {
    const next: {
      -readonly [K in keyof ApiKeyCapacityState]: ApiKeyCapacityState[K];
    } = { ...entry.capacity, signalCoverage: [...entry.capacity.signalCoverage], windows: [...entry.capacity.windows] };
    const observedSignals = observation.observedSignals !== undefined
      ? new Set(observation.observedSignals)
      : inferObservedSignals(observation);
    const signalCoverageMap = new Map(next.signalCoverage.map((signal) => [signal.signalName, { ...signal }]));

    next.responseCount = entry.capacity.responseCount + 1;
    next.lastResponseAt = observation.seenAt;
    if (observation.httpStatus !== undefined) next.lastUpstreamStatus = observation.httpStatus;
    if (observation.requestId !== undefined) next.lastRequestId = observation.requestId;
    if (observation.organizationId !== undefined) next.organizationId = observation.organizationId;
    if (observation.representativeClaim !== undefined) next.representativeClaim = observation.representativeClaim;
    if (observation.retryAfterSecs !== undefined) next.retryAfterSecs = observation.retryAfterSecs;
    if (observation.shouldRetry !== undefined) next.shouldRetry = observation.shouldRetry;
    if (observation.fallbackAvailable !== undefined) next.fallbackAvailable = observation.fallbackAvailable;
    if (observation.fallbackPercentage !== undefined) next.fallbackPercentage = observation.fallbackPercentage;
    if (observation.overageStatus !== undefined) next.overageStatus = observation.overageStatus;
    if (observation.overageDisabledReason !== undefined) next.overageDisabledReason = observation.overageDisabledReason;
    if (observation.latencyMs !== undefined) next.latencyMs = observation.latencyMs;

    for (const signalName of observedSignals) {
      const existing = signalCoverageMap.get(signalName) ?? {
        signalName,
        seenCount: 0,
        lastSeenAt: null,
      };
      existing.seenCount += 1;
      existing.lastSeenAt = observation.seenAt;
      signalCoverageMap.set(signalName, existing);
    }

    const windowMap = new Map(next.windows.map((window) => [window.windowName, { ...window }]));
    for (const window of observation.windows ?? []) {
      const existing = windowMap.get(window.windowName) ?? {
        windowName: window.windowName,
        status: null,
        utilization: null,
        resetAt: null,
        surpassedThreshold: null,
        lastSeenAt: null,
      };
      if (window.status !== undefined) existing.status = window.status;
      if (window.utilization !== undefined) existing.utilization = window.utilization;
      if (window.resetAt !== undefined) existing.resetAt = window.resetAt;
      if (window.surpassedThreshold !== undefined) existing.surpassedThreshold = window.surpassedThreshold;
      existing.lastSeenAt = window.lastSeenAt ?? observation.seenAt;
      windowMap.set(window.windowName, existing);

      if (window.status !== undefined || window.utilization !== undefined) {
        this.recordCapacityTimeseries(entry.label, window.windowName, window.status ?? null, window.utilization ?? null);
      }
    }

    if (observedSignals.size > 0) {
      next.normalizedHeaderCount = entry.capacity.normalizedHeaderCount + 1;
      next.lastHeaderAt = observation.seenAt;
    }
    next.signalCoverage = [...signalCoverageMap.values()].sort((a, b) => a.signalName.localeCompare(b.signalName));
    next.windows = [...windowMap.values()].sort((a, b) => a.windowName.localeCompare(b.windowName));
    entry.capacity = next;
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
      capacity: freshCapacityState(),
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
    this.db.run("DELETE FROM api_key_capacity_state WHERE key = ?", [removed.key]);
    this.db.run("DELETE FROM api_key_capacity_signal_coverage WHERE key = ?", [removed.key]);
    this.db.run("DELETE FROM api_key_capacity_windows WHERE key = ?", [removed.key]);
    log("info", "Key removed", { label: removed.label });
    return true;
  }

  removeKeyByMasked(masked: string): boolean {
    const idx = this.keys.findIndex((k) => maskKey(k.key) === masked);
    if (idx === -1) return false;
    const removed = this.keys.splice(idx, 1)[0]!;
    this.db.run("DELETE FROM api_keys WHERE key = ?", [removed.key]);
    this.db.run("DELETE FROM api_key_capacity_state WHERE key = ?", [removed.key]);
    this.db.run("DELETE FROM api_key_capacity_signal_coverage WHERE key = ?", [removed.key]);
    this.db.run("DELETE FROM api_key_capacity_windows WHERE key = ?", [removed.key]);
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
        capacity: k.capacity,
        capacityHealth: this.getCapacityHealth(k),
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

  private recordCapacityTimeseries(
    keyLabel: string,
    windowName: string,
    status: string | null,
    utilization: number | null,
  ): void {
    if (status === null && utilization === null) return;
    const bucket = currentBucketKey();
    const mapKey = `${bucket}|${keyLabel}|${windowName}`;
    const acc = this.capacityTsAccumulator.get(mapKey) ?? emptyCapacityBucket();
    acc.samples++;
    if (status === "allowed") acc.allowedCount++;
    else if (status === "allowed_warning") acc.warningCount++;
    else if (status === "rejected") acc.rejectedCount++;
    if (utilization !== null) {
      acc.utilizationSum += utilization;
      acc.utilizationSamples++;
      acc.utilizationMax = Math.max(acc.utilizationMax, utilization);
    }
    this.capacityTsAccumulator.set(mapKey, acc);
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

  queryCapacityTimeseries(opts: CapacityTimeseriesQuery): CapacityTimeseriesBucket[] {
    const hours = Math.min(opts.hours ?? 24, 720);
    const resolution = opts.resolution ?? "hour";
    const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString().slice(0, 13);
    const groupExpr = resolution === "day" ? "substr(bucket, 1, 10)" : "bucket";
    const params: string[] = [cutoff];
    const conditions = ["bucket >= ?"];

    if (opts.keyLabel) {
      conditions.push("key_label = ?");
      params.push(opts.keyLabel);
    }

    const sql = `
      SELECT ${groupExpr} AS b,
        window_name,
        SUM(samples) AS samples,
        SUM(allowed_count) AS allowed_count,
        SUM(warning_count) AS warning_count,
        SUM(rejected_count) AS rejected_count,
        SUM(utilization_sum) AS utilization_sum,
        SUM(utilization_samples) AS utilization_samples,
        MAX(utilization_max) AS utilization_max
      FROM capacity_window_timeseries
      WHERE ${conditions.join(" AND ")}
      GROUP BY b, window_name
      ORDER BY b, window_name
    `;

    const rows = this.db.query(sql).all(...params) as CapacityTsRow[];
    return rows.map((row) => ({
      bucket: row.b,
      windowName: row.window_name,
      samples: row.samples,
      allowed: row.allowed_count,
      warning: row.warning_count,
      rejected: row.rejected_count,
      avgUtilization: row.utilization_samples > 0 ? row.utilization_sum / row.utilization_samples : null,
      maxUtilization: row.utilization_samples > 0 ? row.utilization_max : null,
    }));
  }

  private cleanupOldTimeseries(): void {
    const cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 13);
    this.db.run("DELETE FROM stats_timeseries WHERE bucket < ?", [cutoff]);
    this.db.run("DELETE FROM capacity_window_timeseries WHERE bucket < ?", [cutoff]);
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
    const upsertCapacityState = this.db.prepare(`
      INSERT INTO api_key_capacity_state (
        key, response_count, normalized_header_count, last_response_at, last_header_at, last_upstream_status, last_request_id,
        organization_id, representative_claim, retry_after_secs, should_retry,
        fallback_available, fallback_percentage, overage_status, overage_disabled_reason, latency_ms
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(key) DO UPDATE SET
        response_count = excluded.response_count,
        normalized_header_count = excluded.normalized_header_count,
        last_response_at = excluded.last_response_at,
        last_header_at = excluded.last_header_at,
        last_upstream_status = excluded.last_upstream_status,
        last_request_id = excluded.last_request_id,
        organization_id = excluded.organization_id,
        representative_claim = excluded.representative_claim,
        retry_after_secs = excluded.retry_after_secs,
        should_retry = excluded.should_retry,
        fallback_available = excluded.fallback_available,
        fallback_percentage = excluded.fallback_percentage,
        overage_status = excluded.overage_status,
        overage_disabled_reason = excluded.overage_disabled_reason,
        latency_ms = excluded.latency_ms
    `);
    const clearCapacitySignalCoverage = this.db.prepare("DELETE FROM api_key_capacity_signal_coverage WHERE key = ?");
    const upsertCapacitySignalCoverage = this.db.prepare(`
      INSERT INTO api_key_capacity_signal_coverage (key, signal_name, seen_count, last_seen_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(key, signal_name) DO UPDATE SET
        seen_count = excluded.seen_count,
        last_seen_at = excluded.last_seen_at
    `);
    const clearCapacityWindows = this.db.prepare("DELETE FROM api_key_capacity_windows WHERE key = ?");
    const upsertCapacityWindow = this.db.prepare(`
      INSERT INTO api_key_capacity_windows (key, window_name, status, utilization, reset_at, surpassed_threshold, last_seen_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(key, window_name) DO UPDATE SET
        status = excluded.status,
        utilization = excluded.utilization,
        reset_at = excluded.reset_at,
        surpassed_threshold = excluded.surpassed_threshold,
        last_seen_at = excluded.last_seen_at
    `);
    const upsertCapacityTs = this.db.prepare(`
      INSERT INTO capacity_window_timeseries (
        bucket, key_label, window_name, samples, allowed_count, warning_count, rejected_count, utilization_sum, utilization_samples, utilization_max
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(bucket, key_label, window_name) DO UPDATE SET
        samples = samples + excluded.samples,
        allowed_count = allowed_count + excluded.allowed_count,
        warning_count = warning_count + excluded.warning_count,
        rejected_count = rejected_count + excluded.rejected_count,
        utilization_sum = utilization_sum + excluded.utilization_sum,
        utilization_samples = utilization_samples + excluded.utilization_samples,
        utilization_max = MAX(utilization_max, excluded.utilization_max)
    `);

    this.db.transaction(() => {
      for (const k of this.keys) {
        updateKey.run(
          k.stats.totalRequests, k.stats.successfulRequests, k.stats.rateLimitHits,
          k.stats.errors, k.stats.lastUsedAt, k.stats.totalTokensIn, k.stats.totalTokensOut,
          k.stats.totalCacheRead, k.stats.totalCacheCreation, k.availableAt, k.key,
        );
        upsertCapacityState.run(
          k.key,
          k.capacity.responseCount,
          k.capacity.normalizedHeaderCount,
          k.capacity.lastResponseAt,
          k.capacity.lastHeaderAt,
          k.capacity.lastUpstreamStatus,
          k.capacity.lastRequestId,
          k.capacity.organizationId,
          k.capacity.representativeClaim,
          k.capacity.retryAfterSecs,
          boolToInt(k.capacity.shouldRetry),
          boolToInt(k.capacity.fallbackAvailable),
          k.capacity.fallbackPercentage,
          k.capacity.overageStatus,
          k.capacity.overageDisabledReason,
          k.capacity.latencyMs,
        );
        clearCapacitySignalCoverage.run(k.key);
        for (const signal of k.capacity.signalCoverage) {
          upsertCapacitySignalCoverage.run(
            k.key,
            signal.signalName,
            signal.seenCount,
            signal.lastSeenAt,
          );
        }
        clearCapacityWindows.run(k.key);
        for (const window of k.capacity.windows) {
          upsertCapacityWindow.run(
            k.key,
            window.windowName,
            window.status,
            window.utilization,
            window.resetAt,
            window.surpassedThreshold,
            window.lastSeenAt,
          );
        }
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
        upsertTs.run(bucket!, keyLabel!, userLabel!, acc.requests, acc.successes, acc.errors, acc.rateLimits, acc.tokensIn, acc.tokensOut, acc.cacheRead, acc.cacheCreation);
      }
      for (const [mapKey, acc] of this.capacityTsAccumulator) {
        const [bucket, keyLabel, windowName] = mapKey.split("|");
        upsertCapacityTs.run(
          bucket!,
          keyLabel!,
          windowName!,
          acc.samples,
          acc.allowedCount,
          acc.warningCount,
          acc.rejectedCount,
          acc.utilizationSum,
          acc.utilizationSamples,
          acc.utilizationMax,
        );
      }
      this.tsAccumulator.clear();
      this.capacityTsAccumulator.clear();
    })();
  }
}

// ── Helpers ───────────────────────────────────────────────────────

function rowToKeyEntry(
  r: KeyRow,
  state: CapacityStateRow | null,
  coverage: CapacityCoverageRow[],
  windows: CapacityWindowRow[],
): ApiKeyEntry {
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
    capacity: stateToCapacity(state, coverage, windows),
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

function freshCapacityState(): ApiKeyCapacityState {
  return {
    responseCount: 0,
    normalizedHeaderCount: 0,
    lastResponseAt: null,
    lastHeaderAt: null,
    lastUpstreamStatus: null,
    lastRequestId: null,
    organizationId: null,
    representativeClaim: null,
    retryAfterSecs: null,
    shouldRetry: null,
    fallbackAvailable: null,
    fallbackPercentage: null,
    overageStatus: null,
    overageDisabledReason: null,
    latencyMs: null,
    signalCoverage: [],
    windows: [],
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

function stateToCapacity(
  state: CapacityStateRow | null,
  coverage: CapacityCoverageRow[],
  windows: CapacityWindowRow[],
): ApiKeyCapacityState {
  if (state === null && coverage.length === 0 && windows.length === 0) return freshCapacityState();
  return {
    responseCount: state?.response_count ?? 0,
    normalizedHeaderCount: state?.normalized_header_count ?? 0,
    lastResponseAt: (state?.last_response_at as UnixMs | null) ?? null,
    lastHeaderAt: (state?.last_header_at as UnixMs | null) ?? null,
    lastUpstreamStatus: state?.last_upstream_status ?? null,
    lastRequestId: state?.last_request_id ?? null,
    organizationId: state?.organization_id ?? null,
    representativeClaim: state?.representative_claim ?? null,
    retryAfterSecs: state?.retry_after_secs ?? null,
    shouldRetry: intToBool(state?.should_retry ?? null),
    fallbackAvailable: intToBool(state?.fallback_available ?? null),
    fallbackPercentage: state?.fallback_percentage ?? null,
    overageStatus: state?.overage_status ?? null,
    overageDisabledReason: state?.overage_disabled_reason ?? null,
    latencyMs: state?.latency_ms ?? null,
    signalCoverage: coverage
      .map((signal) => ({
        signalName: signal.signal_name,
        seenCount: signal.seen_count,
        lastSeenAt: (signal.last_seen_at as UnixMs | null) ?? null,
      }))
      .sort((a, b) => a.signalName.localeCompare(b.signalName)),
    windows: windows
      .map((window): CapacityWindowSnapshot => ({
        windowName: window.window_name,
        status: window.status ?? null,
        utilization: window.utilization ?? null,
        resetAt: (window.reset_at as UnixMs | null) ?? null,
        surpassedThreshold: window.surpassed_threshold ?? null,
        lastSeenAt: (window.last_seen_at as UnixMs | null) ?? null,
      }))
      .sort((a, b) => a.windowName.localeCompare(b.windowName)),
  };
}

function deriveCapacityHealth(entry: ApiKeyEntry): CapacityHealth {
  if (entry.availableAt > now()) return "cooling_down";
  const windows = activeCapacityWindows(entry);
  if (windows.some((window) => window.status === "rejected")) return "warning";
  if (windows.some((window) => window.status === "allowed_warning")) return "warning";
  if (windows.some((window) => window.status === "allowed")) return "healthy";
  return "unknown";
}

function activeCapacityWindows(entry: ApiKeyEntry): CapacityWindowSnapshot[] {
  const nowMs = now();
  return entry.capacity.windows.filter((window) => window.resetAt === null || window.resetAt > nowMs);
}

function boolToInt(value: boolean | null): number | null {
  if (value === null) return null;
  return value ? 1 : 0;
}

function intToBool(value: number | null): boolean | null {
  if (value === null) return null;
  return value === 1;
}

function inferObservedSignals(observation: CapacityObservation): Set<string> {
  const signals = new Set<string>();
  if (observation.requestId !== undefined) signals.add("request_id");
  if (observation.organizationId !== undefined) signals.add("organization");
  if (observation.representativeClaim !== undefined) signals.add("representative_claim");
  if (observation.retryAfterSecs !== undefined) signals.add("retry_after");
  if (observation.shouldRetry !== undefined) signals.add("should_retry");
  if (observation.fallbackAvailable !== undefined || observation.fallbackPercentage !== undefined) signals.add("fallback");
  if (observation.overageStatus !== undefined || observation.overageDisabledReason !== undefined) signals.add("overage");
  if ((observation.windows?.length ?? 0) > 0) signals.add("windows");
  return signals;
}
