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
  type SeasonalFactorSlot,
  type SeasonalFactorTable,
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
  allowed_days: string;
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

interface ConversationAffinityRow {
  conversation_key: string;
  key: string;
  session_id: string | null;
  assigned_at: number;
  last_seen_at: number;
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

interface ConversationAffinityEntry {
  conversationKey: string;
  key: ApiKey;
  sessionId: string | null;
  assignedAt: UnixMs;
  lastSeenAt: UnixMs;
}

interface ConversationKeySelection {
  entry: ApiKeyEntry | null;
  routingDecision: "global_sticky_fallback" | "conversation_affinity_hit" | "conversation_new_assignment" | "conversation_affinity_remapped";
  affinityHit: boolean;
  remapped: boolean;
  priorityTier: number | null;
  candidateCount: number;
  conversationCountForSelectedKey: number | null;
  /** Pool that the selected entry belongs to right now. Reflects the per-account
   *  gating: Preferred is always Primary; Normal drops to Secondary once either
   *  window exceeds NORMAL_PRIMARY_UTIL_LIMIT; Fallback drops to Tertiary once
   *  either exceeds FALLBACK_PRIMARY_UTIL_LIMIT. */
  pool: Pool | null;
  /** Worst-window headroom (0..1) of the selected key at decision time. */
  worstHeadroom: number | null;
}

export interface RoutingCandidateSnapshot {
  label: string;
  priority: number;
  pool: Pool;
  available: boolean;
  availableAt: number;
  util5h: number | null;
  util7d: number | null;
  reset5h: number | null;
  reset7d: number | null;
  recentSessions: number;
  sessionBucket: number;
  worstHeadroom: number;
}

export interface RoutingDecisionRecord {
  decidedAt: number;
  conversationKey: string | null;
  sessionId: string | null;
  chosenKeyLabel: string | null;
  routingDecision: string;
  priorityTier: number | null;
  pool: string | null;
  candidateCount: number | null;
  affinityHit: boolean;
  remapped: boolean;
  conversationCountForSelected: number | null;
  worstHeadroom: number | null;
  candidates: RoutingCandidateSnapshot[];
}

interface RoutingDecisionRow {
  decided_at: number;
  conversation_key: string | null;
  session_id: string | null;
  chosen_key_label: string | null;
  routing_decision: string;
  priority_tier: number | null;
  pool: string | null;
  candidate_count: number | null;
  affinity_hit: number;
  remapped: number;
  conversation_count_for_selected: number | null;
  worst_headroom: number | null;
  candidates_json: string;
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

// Returns the resetAt of the named window for sorting purposes. Missing
// windows or unknown reset times sort first so a fresh account gets probed
// rather than parked indefinitely.
function resetAtForSort(entry: ApiKeyEntry, windowName: string): number {
  for (const window of entry.capacity.windows) {
    if (window.windowName !== windowName) continue;
    if (window.resetAt === null || window.resetAt === undefined) return 0;
    return window.resetAt;
  }
  return 0;
}

// Parse a stats_timeseries bucket key ("YYYY-MM-DDTHH") into a (dow, hour)
// slot. Buckets are generated in UTC (see currentBucketKey), so the
// day-of-week and hour returned are also UTC. Returns null on malformed
// input — the caller treats that slot as "no data".
function parseBucketToSlot(bucket: string): { dow: number; hour: number } | null {
  if (typeof bucket !== "string" || bucket.length < 13) return null;
  const date = new Date(bucket + ":00:00.000Z");
  const time = date.getTime();
  if (Number.isNaN(time)) return null;
  return { dow: date.getUTCDay(), hour: date.getUTCHours() };
}

const CONVERSATION_AFFINITY_TTL_MS = 60 * 60 * 1000;
const RECENT_SESSION_WINDOW_MS = 15 * 60 * 1000;
const PRIMARY_CAPACITY_WINDOW_NAMES = new Set(["unified", "unified-5h", "unified-7d"]);

// Sentinel priority for keys the user has paused. Disabled keys are retained
// in storage (so settings, labels, and history survive) but are excluded from
// routing, availability counts, and cooldown timing. Numerically higher than
// any real tier so existing "sort by priority ascending" ordering puts them
// at the bottom of the UI without extra branching.
const DISABLED_PRIORITY = 4;

// Window durations in ms. Used to compute elapsed fraction.
const WINDOW_DURATION_MS: Record<string, number> = {
  "unified-5h": 5 * 60 * 60 * 1000,
  "unified-7d": 7 * 24 * 60 * 60 * 1000,
};

// Windows this close to their reset are ignored for headroom math — a key
// with 36% utilization and 2 minutes until reset isn't really "used 36%",
// it's about to refresh.
const NEAR_RESET_ELAPSED_THRESHOLD = 0.95;

// Per-account pool gating thresholds. A Normal account stays in the Primary
// pool while both its weekly and 5h utilization are under 75%; a Fallback
// account stays in Primary while both are under 50%. Otherwise it drops to
// Secondary (Normal) or Tertiary (Fallback) and only serves traffic when the
// higher pools have no available account.
const NORMAL_PRIMARY_UTIL_LIMIT = 0.75;
const FALLBACK_PRIMARY_UTIL_LIMIT = 0.50;

// How many sessions one account claims before the rotation moves on. Sessions
// are counted from the last RECENT_SESSION_WINDOW_MS of conversation activity.
const SESSION_BUCKET_SIZE = 3;

// How long routing decision records are kept in the DB. Hourly cleanup drops
// anything older — the data is only useful for recent forensics.
const ROUTING_DECISION_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

// Seasonal factor table: how many weeks back, how few observations collapse a
// slot to a "no signal" factor of 1, and how far we let a busy/quiet slot
// stretch the projection. Clamp keeps a single anomalous week from dominating.
const SEASONAL_DEFAULT_WEEKS = 4;
const SEASONAL_MIN_SAMPLES_PER_SLOT = 3;
const SEASONAL_FACTOR_CLAMP_MIN = 0.1;
const SEASONAL_FACTOR_CLAMP_MAX = 5.0;
const SEASONAL_SLOT_COUNT = 7 * 24;

type Pool = "primary" | "secondary" | "tertiary";

export class KeyManager {
  private keys: ApiKeyEntry[] = [];
  private tokens: ProxyTokenEntry[] = [];
  private readonly conversationAffinities = new Map<string, ConversationAffinityEntry>();
  private readonly db: Database;
  readonly dbPath: string;
  private readonly cleanupInterval: ReturnType<typeof setInterval>;
  private saveTimer: ReturnType<typeof setTimeout> | null = null;
  private isClosed = false;
  private readonly tsAccumulator = new Map<string, BucketAccumulator>();
  private readonly capacityTsAccumulator = new Map<string, CapacityBucketAccumulator>();
  // In-memory cumulative counter of requests routed to each priority tier.
  // Not persisted — resets on restart. Serves as live observability for the
  // routing policy ("is traffic actually flowing through Normal/Fallback?").
  private readonly requestsByTier: Map<number, number> = new Map();

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

    this.cleanupInterval = setInterval(() => {
      if (this.isClosed) return;
      try {
        this.cleanupOldTimeseries();
        this.cleanupOldRoutingDecisions();
        this.cleanupExpiredConversationAffinities(true);
        this.prunePastResetCapacityWindows();
      } catch (error) {
        log("warn", "Failed to clean up key manager timeseries", {
          dbPath: this.dbPath,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }, 60 * 60 * 1000);
  }

  // Drop in-memory window records whose resetAt has already passed — the
  // stored utilization is stale (the rate-limit cycle has flipped) and the
  // next API response will repopulate with fresh values. Keeping expired
  // entries around makes routing, dashboards, and debugging harder. Also
  // persists the pruned state so we don't reload zombies on restart.
  private prunePastResetCapacityWindows(): void {
    const currentTime = now();
    let prunedAny = false;
    for (const entry of this.keys) {
      const kept = entry.capacity.windows.filter((w) => {
        if (w.resetAt === null || w.resetAt === undefined) return true;
        return w.resetAt > currentTime;
      });
      if (kept.length !== entry.capacity.windows.length) {
        entry.capacity = { ...entry.capacity, windows: kept };
        prunedAny = true;
      }
    }
    if (prunedAny) this.scheduleSave();
  }

  close(): void {
    if (this.isClosed) return;
    clearInterval(this.cleanupInterval);
    if (this.saveTimer) {
      clearTimeout(this.saveTimer);
      this.saveTimer = null;
    }
    try {
      this.saveNow();
    } catch (error) {
      log("warn", "Failed to flush key manager state during close", {
        dbPath: this.dbPath,
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      this.isClosed = true;
      this.db.close();
    }
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

      CREATE TABLE IF NOT EXISTS conversation_affinities (
        conversation_key TEXT PRIMARY KEY,
        key TEXT NOT NULL,
        session_id TEXT,
        assigned_at INTEGER NOT NULL,
        last_seen_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_conversation_affinities_key ON conversation_affinities(key);
      CREATE INDEX IF NOT EXISTS idx_conversation_affinities_last_seen ON conversation_affinities(last_seen_at);

      CREATE TABLE IF NOT EXISTS routing_decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        decided_at INTEGER NOT NULL,
        conversation_key TEXT,
        session_id TEXT,
        chosen_key_label TEXT,
        routing_decision TEXT NOT NULL,
        priority_tier INTEGER,
        pool TEXT,
        candidate_count INTEGER,
        affinity_hit INTEGER NOT NULL DEFAULT 0,
        remapped INTEGER NOT NULL DEFAULT 0,
        conversation_count_for_selected INTEGER,
        worst_headroom REAL,
        candidates_json TEXT NOT NULL DEFAULT '[]'
      );

      CREATE INDEX IF NOT EXISTS idx_routing_decisions_decided_at ON routing_decisions(decided_at);
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
    try { this.db.exec("DELETE FROM api_key_capacity_windows WHERE window_name = 'unified-overage'"); } catch {}
    try { this.db.exec("ALTER TABLE conversation_affinities ADD COLUMN session_id TEXT"); } catch {}
    try {
      this.db.exec(`
        UPDATE conversation_affinities
        SET session_id = CASE
          WHEN session_id IS NOT NULL THEN session_id
          WHEN instr(conversation_key, ':') > 0 THEN substr(conversation_key, instr(conversation_key, ':') + 1)
          ELSE conversation_key
        END
        WHERE session_id IS NULL
      `);
    } catch {}

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

    // Add allowed_days column (JSON array, default = all days)
    try {
      this.db.exec("ALTER TABLE api_keys ADD COLUMN allowed_days TEXT NOT NULL DEFAULT '[0,1,2,3,4,5,6]'");
    } catch {}
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

    const affinityRows = this.db.query("SELECT * FROM conversation_affinities").all() as ConversationAffinityRow[];
    this.conversationAffinities.clear();
    const validKeys = new Set(this.keys.map((entry) => entry.key));
    const cutoff = unixMs(Date.now() - CONVERSATION_AFFINITY_TTL_MS);
    for (const row of affinityRows) {
      if ((row.last_seen_at as UnixMs) < cutoff) continue;
      if (!validKeys.has(row.key as ApiKey)) continue;
      this.conversationAffinities.set(row.conversation_key, {
        conversationKey: row.conversation_key,
        key: row.key as ApiKey,
        sessionId: row.session_id,
        assignedAt: row.assigned_at as UnixMs,
        lastSeenAt: row.last_seen_at as UnixMs,
      });
    }
    this.cleanupExpiredConversationAffinities(true);
    this.prunePastResetCapacityWindows();

    log("info", `Loaded ${this.keys.length} key(s) and ${this.tokens.length} token(s) from SQLite`);
  }

  // ── Key selection ───────────────────────────────────────────────

  getNextAvailableKey(excludedKeys?: ReadonlySet<string>): ApiKeyEntry | null {
    if (this.keys.length === 0) return null;

    const currentTime = now();
    const currentDay = new Date().getDay();
    const available = this.keys.filter(
      (k) =>
        k.priority !== DISABLED_PRIORITY
        && k.availableAt <= currentTime
        && k.allowedDays.includes(currentDay)
        && !excludedKeys?.has(k.key),
    );
    if (available.length === 0) return null;

    const sessionCounts = this.countRecentSessionsByKey(
      unixMs(currentTime - RECENT_SESSION_WINDOW_MS),
    );
    const pool = this.pickActivePool(available, currentTime);
    if (pool.length === 0) return null;
    return [...pool].sort((a, b) => this.compareForSort(a, b, sessionCounts, currentTime))[0]!;
  }

  getKeyForConversation(conversationKey: string | null, sessionId?: string | null): ConversationKeySelection {
    const selection = this.computeKeyForConversation(conversationKey, sessionId);
    this.logRoutingDecision(conversationKey, sessionId ?? null, selection, now());
    return selection;
  }

  private computeKeyForConversation(conversationKey: string | null, sessionId?: string | null): ConversationKeySelection {
    if (conversationKey === null) {
      const entry = this.getNextAvailableKey();
      const currentTime = now();
      return {
        entry,
        routingDecision: "global_sticky_fallback",
        affinityHit: false,
        remapped: false,
        priorityTier: entry?.priority ?? null,
        candidateCount: entry === null ? 0 : this.countAvailableKeysAtPriority(entry.priority),
        conversationCountForSelectedKey: entry === null ? null : this.countConversationAffinitiesByKey().get(entry.key) ?? 0,
        pool: entry === null ? null : this.assignPool(entry, currentTime),
        worstHeadroom: entry === null ? null : this.worstHeadroom(entry, currentTime),
      };
    }

    this.cleanupExpiredConversationAffinities(true);
    const currentTime = now();
    const existing = this.conversationAffinities.get(conversationKey);
    if (existing !== undefined) {
      existing.sessionId = sessionId ?? existing.sessionId;
      existing.lastSeenAt = currentTime;
      const mappedEntry = this.keys.find((entry) => entry.key === existing.key) ?? null;
      if (mappedEntry !== null && this.isKeyAvailable(mappedEntry)) {
        this.scheduleSave();
        return {
          entry: mappedEntry,
          routingDecision: "conversation_affinity_hit",
          affinityHit: true,
          remapped: false,
          priorityTier: mappedEntry.priority,
          candidateCount: this.countAvailableKeysAtPriority(mappedEntry.priority),
          conversationCountForSelectedKey: this.countConversationAffinitiesByKey().get(mappedEntry.key) ?? 0,
          pool: this.assignPool(mappedEntry, currentTime),
          worstHeadroom: this.worstHeadroom(mappedEntry, currentTime),
        };
      }
    }

    const fallback = this.selectLeastLoadedAvailableKey();
    if (fallback === null) {
      return {
        entry: null,
        routingDecision: existing === undefined ? "conversation_new_assignment" : "conversation_affinity_remapped",
        affinityHit: false,
        remapped: false,
        priorityTier: null,
        candidateCount: 0,
        conversationCountForSelectedKey: null,
        pool: null,
        worstHeadroom: null,
      };
    }

    const assignedAt = existing?.assignedAt ?? currentTime;
    this.conversationAffinities.set(conversationKey, {
      conversationKey,
      key: fallback.entry.key,
      sessionId: sessionId ?? existing?.sessionId ?? null,
      assignedAt,
      lastSeenAt: currentTime,
    });
    this.scheduleSave();

    return {
      entry: fallback.entry,
      routingDecision: existing === undefined ? "conversation_new_assignment" : "conversation_affinity_remapped",
      affinityHit: false,
      remapped: existing !== undefined && existing.key !== fallback.entry.key,
      priorityTier: fallback.priorityTier,
      candidateCount: fallback.candidateCount,
      conversationCountForSelectedKey: (fallback.conversationCounts.get(fallback.entry.key) ?? 0) + (existing?.key === fallback.entry.key ? 0 : 1),
      pool: fallback.pool,
      worstHeadroom: fallback.worstHeadroom,
    };
  }

  getEarliestAvailableAt(): UnixMs {
    const currentDay = new Date().getDay();
    const midnight = midnightLocalMs();

    let earliest = Infinity;
    for (const k of this.keys) {
      if (k.priority === DISABLED_PRIORITY) continue;
      if (k.allowedDays.includes(currentDay)) {
        earliest = Math.min(earliest, k.availableAt);
      } else {
        earliest = Math.min(earliest, midnight);
      }
    }
    return earliest === Infinity ? unixMs(0) : unixMs(earliest);
  }

  availableCount(): number {
    const currentTime = now();
    const currentDay = new Date().getDay();
    return this.keys.filter(
      (k) =>
        k.priority !== DISABLED_PRIORITY
        && k.availableAt <= currentTime
        && k.allowedDays.includes(currentDay),
    ).length;
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
      // Reserved for future operational states. Successful-response quota headers
      // are analytics-only in this file and intentionally collapse to "warning".
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
    this.requestsByTier.set(entry.priority, (this.requestsByTier.get(entry.priority) ?? 0) + 1);
    this.scheduleSave();
  }

  /** Snapshot of per-tier cumulative request counts since process start.
   *  Returned as an object keyed by tier number. Empty tiers are omitted.
   *  Resets on restart — this is live observability, not durable history. */
  getRequestsByTier(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const [tier, count] of this.requestsByTier) out[String(tier)] = count;
    return out;
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

    // Capacity observations are merged as passive telemetry. They do not change
    // routing; 429 handling still owns cooldowns through availableAt.
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
    next.windows = sanitizeCapacityWindows([...windowMap.values()], now());
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
      allowedDays: ALL_DAYS,
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

  updateKeyAllowedDays(rawKey: string, allowedDays: readonly number[]): boolean {
    const entry = this.keys.find((k) => k.key === rawKey);
    if (!entry) return false;
    entry.allowedDays = allowedDays;
    this.db.run("UPDATE api_keys SET allowed_days = ? WHERE key = ?", [JSON.stringify(allowedDays), rawKey]);
    log("info", "Key allowed days updated", { label: entry.label, allowedDays });
    return true;
  }

  updateKeyAllowedDaysByMask(masked: string, allowedDays: readonly number[]): boolean {
    const entry = this.keys.find((k) => maskKey(k.key) === masked);
    if (!entry) return false;
    entry.allowedDays = allowedDays;
    this.db.run("UPDATE api_keys SET allowed_days = ? WHERE key = ?", [JSON.stringify(allowedDays), entry.key]);
    log("info", "Key allowed days updated", { label: entry.label, allowedDays });
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
    this.db.run("DELETE FROM conversation_affinities WHERE key = ?", [removed.key]);
    this.removeConversationAffinitiesForKey(removed.key);
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
    this.db.run("DELETE FROM conversation_affinities WHERE key = ?", [removed.key]);
    this.removeConversationAffinitiesForKey(removed.key);
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
    const currentDay = new Date().getDay();
    const recentErrs = this.recentErrorsByDimension("key_label", "user_label", "__all__");
    const recentSessionsByKey = this.listRecentConversationSessionsByKey(
      unixMs(Date.now() - RECENT_SESSION_WINDOW_MS),
    );
    return this.keys.map(
      (k): MaskedKeyEntry => ({
        maskedKey: maskKey(k.key),
        label: k.label,
        stats: k.stats,
        // Surface sanitized windows to the dashboard so past-reset windows
        // (where the stored util is stale) don't show as "80% used" when the
        // reset has actually happened. Routing still uses raw windows but
        // applies its own near-reset bypass.
        capacity: { ...k.capacity, windows: sanitizeCapacityWindows(k.capacity.windows, currentTime) },
        capacityHealth: this.getCapacityHealth(k),
        availableAt: k.availableAt,
        isAvailable: k.priority !== DISABLED_PRIORITY
          && k.availableAt <= currentTime
          && k.allowedDays.includes(currentDay),
        priority: k.priority,
        allowedDays: k.allowedDays,
        recentErrors: recentErrs.get(k.label) ?? 0,
        recentSessions15m: recentSessionsByKey.get(k.key) ?? [],
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

  /** Seasonal request-volume factor table — one entry per (dow, hour) slot
   *  describing how busy that slot has historically been relative to the
   *  fleet average. A factor of 1.0 means "typical", 2.0 means "twice as
   *  busy", 0.2 means "quiet". Slots with too few observations collapse to
   *  1.0. Used by the dashboard forecast widget to project future pressure.
   *  Buckets are in UTC — the caller is responsible for any local-time
   *  presentation. */
  computeSeasonalRequestFactors(weeks: number = SEASONAL_DEFAULT_WEEKS): SeasonalFactorTable {
    const cutoffMs = Date.now() - weeks * 7 * 24 * 60 * 60 * 1000;
    const cutoff = new Date(cutoffMs).toISOString().slice(0, 13);
    const rows = this.db.query(
      "SELECT bucket, SUM(requests) AS requests FROM stats_timeseries " +
      "WHERE bucket >= ? AND key_label = '__all__' AND user_label = '__all__' " +
      "GROUP BY bucket"
    ).all(cutoff) as Array<{ bucket: string; requests: number }>;

    const totals: number[] = new Array(SEASONAL_SLOT_COUNT).fill(0);
    const counts: number[] = new Array(SEASONAL_SLOT_COUNT).fill(0);
    let grandTotal = 0;
    let grandCount = 0;

    for (const row of rows) {
      const slot = parseBucketToSlot(row.bucket);
      if (slot === null) continue;
      const idx = slot.dow * 24 + slot.hour;
      const requests = Number(row.requests) || 0;
      totals[idx] = (totals[idx] ?? 0) + requests;
      counts[idx] = (counts[idx] ?? 0) + 1;
      grandTotal += requests;
      grandCount += 1;
    }

    const meanPerHour = grandCount > 0 ? grandTotal / grandCount : 0;
    const slots: SeasonalFactorSlot[] = new Array(SEASONAL_SLOT_COUNT);
    for (let dow = 0; dow < 7; dow++) {
      for (let hour = 0; hour < 24; hour++) {
        const idx = dow * 24 + hour;
        const slotCount = counts[idx] ?? 0;
        const slotTotal = totals[idx] ?? 0;
        let factor = 1;
        if (slotCount >= SEASONAL_MIN_SAMPLES_PER_SLOT && meanPerHour > 0) {
          const avg = slotTotal / slotCount;
          factor = Math.min(
            SEASONAL_FACTOR_CLAMP_MAX,
            Math.max(SEASONAL_FACTOR_CLAMP_MIN, avg / meanPerHour),
          );
        }
        slots[idx] = { dow, hour, factor, samples: slotCount };
      }
    }

    return {
      weeks,
      generatedAt: unixMs(Date.now()),
      totalSamples: grandCount,
      slots,
    };
  }

  private cleanupOldTimeseries(): void {
    const cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 13);
    this.db.run("DELETE FROM stats_timeseries WHERE bucket < ?", [cutoff]);
    this.db.run("DELETE FROM capacity_window_timeseries WHERE bucket < ?", [cutoff]);
  }

  private cleanupOldRoutingDecisions(): void {
    const cutoff = Date.now() - ROUTING_DECISION_RETENTION_MS;
    this.db.run("DELETE FROM routing_decisions WHERE decided_at < ?", [cutoff]);
  }

  // Compact snapshot of every non-disabled key at decision time — what each
  // one's pool, utilization, reset countdown, recent session count, and
  // availability were. Stored per-row so a decision can be re-evaluated
  // forensically without replaying live state.
  private snapshotRoutingCandidates(currentTime: UnixMs): RoutingCandidateSnapshot[] {
    const recent = this.countRecentSessionsByKey(
      unixMs(currentTime - RECENT_SESSION_WINDOW_MS),
    );
    const snapshots: RoutingCandidateSnapshot[] = [];
    for (const entry of this.keys) {
      if (entry.priority === DISABLED_PRIORITY) continue;
      let util5h: number | null = null;
      let util7d: number | null = null;
      let reset5h: number | null = null;
      let reset7d: number | null = null;
      for (const window of entry.capacity.windows) {
        if (window.windowName === "unified-5h") {
          util5h = window.utilization ?? null;
          reset5h = window.resetAt ?? null;
        } else if (window.windowName === "unified-7d") {
          util7d = window.utilization ?? null;
          reset7d = window.resetAt ?? null;
        }
      }
      const recentSessions = recent.get(entry.key) ?? 0;
      snapshots.push({
        label: entry.label,
        priority: entry.priority,
        pool: this.assignPool(entry, currentTime),
        available: this.isKeyAvailable(entry),
        availableAt: entry.availableAt,
        util5h, util7d, reset5h, reset7d,
        recentSessions,
        sessionBucket: Math.floor(recentSessions / SESSION_BUCKET_SIZE),
        worstHeadroom: this.worstHeadroom(entry, currentTime),
      });
    }
    return snapshots;
  }

  private logRoutingDecision(
    conversationKey: string | null,
    sessionId: string | null,
    selection: ConversationKeySelection,
    currentTime: UnixMs,
  ): void {
    try {
      const candidates = this.snapshotRoutingCandidates(currentTime);
      this.db.run(
        `INSERT INTO routing_decisions
         (decided_at, conversation_key, session_id, chosen_key_label, routing_decision,
          priority_tier, pool, candidate_count, affinity_hit, remapped,
          conversation_count_for_selected, worst_headroom, candidates_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          currentTime,
          conversationKey,
          sessionId,
          selection.entry?.label ?? null,
          selection.routingDecision,
          selection.priorityTier,
          selection.pool,
          selection.candidateCount,
          selection.affinityHit ? 1 : 0,
          selection.remapped ? 1 : 0,
          selection.conversationCountForSelectedKey,
          selection.worstHeadroom,
          JSON.stringify(candidates),
        ],
      );
    } catch (error) {
      log("warn", "Failed to persist routing decision", {
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /** Most recent routing decisions (newest first), up to `limit` rows.
   *  Reads straight from the DB — intended for dashboards / CLI forensics. */
  getRecentRoutingDecisions(limit = 200): RoutingDecisionRecord[] {
    const rows = this.db.query(
      `SELECT decided_at, conversation_key, session_id, chosen_key_label, routing_decision,
              priority_tier, pool, candidate_count, affinity_hit, remapped,
              conversation_count_for_selected, worst_headroom, candidates_json
       FROM routing_decisions
       ORDER BY decided_at DESC
       LIMIT ?`,
    ).all(limit) as RoutingDecisionRow[];
    return rows.map((r) => ({
      decidedAt: r.decided_at,
      conversationKey: r.conversation_key,
      sessionId: r.session_id,
      chosenKeyLabel: r.chosen_key_label,
      routingDecision: r.routing_decision,
      priorityTier: r.priority_tier,
      pool: r.pool,
      candidateCount: r.candidate_count,
      affinityHit: r.affinity_hit === 1,
      remapped: r.remapped === 1,
      conversationCountForSelected: r.conversation_count_for_selected,
      worstHeadroom: r.worst_headroom,
      candidates: JSON.parse(r.candidates_json) as RoutingCandidateSnapshot[],
    }));
  }

  private cleanupExpiredConversationAffinities(persist: boolean): void {
    const cutoff = unixMs(Date.now() - CONVERSATION_AFFINITY_TTL_MS);
    const validKeys = new Set(this.keys.map((entry) => entry.key));
    let removed = false;
    for (const [conversationKey, affinity] of this.conversationAffinities) {
      if (affinity.lastSeenAt >= cutoff && validKeys.has(affinity.key)) continue;
      this.conversationAffinities.delete(conversationKey);
      removed = true;
    }
    if (!removed) return;
    if (persist) this.scheduleSave();
  }

  private removeConversationAffinitiesForKey(key: ApiKey): void {
    for (const [conversationKey, affinity] of this.conversationAffinities) {
      if (affinity.key !== key) continue;
      this.conversationAffinities.delete(conversationKey);
    }
  }

  private isKeyAvailable(entry: ApiKeyEntry): boolean {
    const currentTime = now();
    const currentDay = new Date().getDay();
    return entry.priority !== DISABLED_PRIORITY
      && entry.availableAt <= currentTime
      && entry.allowedDays.includes(currentDay);
  }

  private countAvailableKeysAtPriority(priority: number): number {
    return this.keys.filter((entry) => this.isKeyAvailable(entry) && entry.priority === priority).length;
  }

  private countConversationAffinitiesByKey(cutoff?: UnixMs): Map<ApiKey, number> {
    const counts = new Map<ApiKey, number>();
    for (const affinity of this.conversationAffinities.values()) {
      if (cutoff !== undefined && affinity.lastSeenAt < cutoff) continue;
      counts.set(affinity.key, (counts.get(affinity.key) ?? 0) + 1);
    }
    return counts;
  }

  private listRecentConversationSessionsByKey(
    cutoff: UnixMs,
  ): Map<ApiKey, Array<{ sessionId: string; lastSeenAt: string }>> {
    const sessionsByKey = new Map<ApiKey, Array<{ sessionId: string; lastSeenAt: string }>>();
    for (const affinity of this.conversationAffinities.values()) {
      if (affinity.lastSeenAt < cutoff) continue;
      const existing = sessionsByKey.get(affinity.key) ?? [];
      existing.push({
        sessionId: affinity.sessionId ?? affinity.conversationKey,
        lastSeenAt: new Date(affinity.lastSeenAt).toISOString(),
      });
      sessionsByKey.set(affinity.key, existing);
    }

    for (const sessions of sessionsByKey.values()) {
      sessions.sort((a, b) =>
        b.lastSeenAt.localeCompare(a.lastSeenAt) || a.sessionId.localeCompare(b.sessionId)
      );
    }
    return sessionsByKey;
  }

  // Worst-window headroom for a key. Returns 1 - max(utilization) across
  // the 5h and 7d windows, ignoring any window close enough to reset that
  // its utilization is about to be wiped. If a key reports no primary
  // windows yet, we treat it as full headroom — no telemetry means we can't
  // claim it's reserved, and the proxy's existing cooldown-based availability
  // check is the real backstop.
  private worstHeadroom(entry: ApiKeyEntry, currentTime: UnixMs): number {
    let highestUtilization = 0;
    let sawUsableWindow = false;
    for (const window of entry.capacity.windows) {
      if (window.windowName !== "unified-5h" && window.windowName !== "unified-7d") continue;
      const duration = WINDOW_DURATION_MS[window.windowName];
      if (duration === undefined) continue;
      if (window.resetAt !== null && window.resetAt !== undefined) {
        const elapsedFraction = (duration - (window.resetAt - currentTime)) / duration;
        if (elapsedFraction >= NEAR_RESET_ELAPSED_THRESHOLD) continue;
      }
      const util = window.utilization ?? 0;
      if (util > highestUtilization) highestUtilization = util;
      sawUsableWindow = true;
    }
    if (!sawUsableWindow) return 1;
    return Math.max(0, 1 - highestUtilization);
  }

  // Per-account pool placement. Preferred accounts are always Primary; Normal
  // and Fallback drop out of Primary once either of their two utilization
  // windows crosses the gating threshold. Unknown utilization counts as 0%
  // so freshly added keys aren't penalized.
  private assignPool(entry: ApiKeyEntry, currentTime: UnixMs): Pool {
    if (entry.priority === 1) return "primary";

    let weeklyUtil = 0;
    let fiveHourUtil = 0;
    for (const window of entry.capacity.windows) {
      if (window.windowName !== "unified-5h" && window.windowName !== "unified-7d") continue;
      const duration = WINDOW_DURATION_MS[window.windowName];
      if (duration === undefined) continue;
      if (window.resetAt !== null && window.resetAt !== undefined) {
        const elapsedFraction = (duration - (window.resetAt - currentTime)) / duration;
        if (elapsedFraction >= NEAR_RESET_ELAPSED_THRESHOLD) continue;
      }
      const util = window.utilization ?? 0;
      if (window.windowName === "unified-7d" && util > weeklyUtil) weeklyUtil = util;
      if (window.windowName === "unified-5h" && util > fiveHourUtil) fiveHourUtil = util;
    }

    if (entry.priority === 2) {
      return weeklyUtil < NORMAL_PRIMARY_UTIL_LIMIT && fiveHourUtil < NORMAL_PRIMARY_UTIL_LIMIT
        ? "primary"
        : "secondary";
    }
    // Fallback (priority 3) and any future lower tier follow the same rule.
    return weeklyUtil < FALLBACK_PRIMARY_UTIL_LIMIT && fiveHourUtil < FALLBACK_PRIMARY_UTIL_LIMIT
      ? "primary"
      : "tertiary";
  }

  // Cascade: walk Primary → Secondary → Tertiary, returning the first
  // non-empty pool. Caller is expected to have already filtered on
  // `isKeyAvailable` (cooldown / disabled / allowedDays).
  private pickActivePool(available: ApiKeyEntry[], currentTime: UnixMs): ApiKeyEntry[] {
    const buckets: Record<Pool, ApiKeyEntry[]> = { primary: [], secondary: [], tertiary: [] };
    for (const entry of available) {
      buckets[this.assignPool(entry, currentTime)].push(entry);
    }
    if (buckets.primary.length > 0) return buckets.primary;
    if (buckets.secondary.length > 0) return buckets.secondary;
    return buckets.tertiary;
  }

  // Compound sort comparator for accounts inside the active pool. Primary
  // signal is the rotation-of-3 bucket: the account that has fewest recent
  // sessions modulo 3 wins, so each account claims SESSION_BUCKET_SIZE
  // sessions before the rotation moves on. Secondary signals are reset
  // timing (drain accounts whose budget is about to refresh) and current
  // utilization (drain hot accounts toward their limit). Final tiebreak is
  // sticky most-recently-used so a quiet workload doesn't hop unnecessarily.
  private compareForSort(
    a: ApiKeyEntry,
    b: ApiKeyEntry,
    sessionCounts: Map<ApiKey, number>,
    currentTime: UnixMs,
  ): number {
    const bucketA = Math.floor((sessionCounts.get(a.key) ?? 0) / SESSION_BUCKET_SIZE);
    const bucketB = Math.floor((sessionCounts.get(b.key) ?? 0) / SESSION_BUCKET_SIZE);
    if (bucketA !== bucketB) return bucketA - bucketB;

    const weeklyDiff = resetAtForSort(a, "unified-7d") - resetAtForSort(b, "unified-7d");
    if (weeklyDiff !== 0) return weeklyDiff;

    const fiveHourDiff = resetAtForSort(a, "unified-5h") - resetAtForSort(b, "unified-5h");
    if (fiveHourDiff !== 0) return fiveHourDiff;

    const headroomDiff = this.worstHeadroom(a, currentTime) - this.worstHeadroom(b, currentTime);
    if (headroomDiff !== 0) return headroomDiff;

    const lastUsedDiff = (b.stats.lastUsedAt ?? 0) - (a.stats.lastUsedAt ?? 0);
    if (lastUsedDiff !== 0) return lastUsedDiff;

    return a.label.localeCompare(b.label);
  }

  private countRecentSessionsByKey(cutoff: UnixMs): Map<ApiKey, number> {
    const counts = new Map<ApiKey, number>();
    for (const affinity of this.conversationAffinities.values()) {
      if (affinity.lastSeenAt < cutoff) continue;
      counts.set(affinity.key, (counts.get(affinity.key) ?? 0) + 1);
    }
    return counts;
  }

  private selectLeastLoadedAvailableKey(): {
    entry: ApiKeyEntry;
    priorityTier: number;
    candidateCount: number;
    conversationCounts: Map<ApiKey, number>;
    pool: Pool;
    /** Worst-window headroom of the selected key, for log observability. */
    worstHeadroom: number;
  } | null {
    const available = this.keys.filter((entry) => this.isKeyAvailable(entry));
    if (available.length === 0) return null;

    const currentTime = now();
    const pool = this.pickActivePool(available, currentTime);
    if (pool.length === 0) return null;

    const sessionCounts = this.countRecentSessionsByKey(
      unixMs(currentTime - RECENT_SESSION_WINDOW_MS),
    );
    const sorted = [...pool].sort((a, b) => this.compareForSort(a, b, sessionCounts, currentTime));
    const winner = sorted[0]!;
    return {
      entry: winner,
      priorityTier: winner.priority,
      candidateCount: this.countAvailableKeysAtPriority(winner.priority),
      conversationCounts: this.countConversationAffinitiesByKey(),
      pool: this.assignPool(winner, currentTime),
      worstHeadroom: this.worstHeadroom(winner, currentTime),
    };
  }

  // ── Persistence ─────────────────────────────────────────────────

  private scheduleSave(): void {
    if (this.isClosed || this.saveTimer) return;
    this.saveTimer = setTimeout(() => {
      this.saveTimer = null;
      if (this.isClosed) return;
      try {
        this.saveNow();
      } catch (error) {
        log("warn", "Failed to persist key manager state", {
          dbPath: this.dbPath,
          error: error instanceof Error ? error.message : String(error),
        });
      }
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
    const clearConversationAffinities = this.db.prepare("DELETE FROM conversation_affinities");
    const insertConversationAffinity = this.db.prepare(`
      INSERT INTO conversation_affinities (conversation_key, key, session_id, assigned_at, last_seen_at)
      VALUES (?, ?, ?, ?, ?)
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
      clearConversationAffinities.run();
      for (const affinity of this.conversationAffinities.values()) {
        insertConversationAffinity.run(
          affinity.conversationKey,
          affinity.key,
          affinity.sessionId,
          affinity.assignedAt,
          affinity.lastSeenAt,
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
    allowedDays: parseAllowedDays(r.allowed_days),
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
  const currentTime = now();
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
    windows: sanitizeCapacityWindows(windows
      .map((window): CapacityWindowSnapshot => ({
        windowName: window.window_name,
        status: window.status ?? null,
        utilization: window.utilization ?? null,
        resetAt: (window.reset_at as UnixMs | null) ?? null,
        surpassedThreshold: window.surpassed_threshold ?? null,
        lastSeenAt: (window.last_seen_at as UnixMs | null) ?? null,
      }))
    , currentTime),
  };
}

function deriveCapacityHealth(entry: ApiKeyEntry): CapacityHealth {
  if (entry.availableAt > now()) return "cooling_down";
  const windows = activeCapacityWindows(entry);
  if (hasCapacityWarningTelemetry(windows)) return "warning";
  if (hasCapacityHealthyTelemetry(windows)) return "healthy";
  return "unknown";
}

function activeCapacityWindows(entry: ApiKeyEntry): CapacityWindowSnapshot[] {
  return sanitizeCapacityWindows(entry.capacity.windows, now());
}

function hasCapacityWarningTelemetry(windows: CapacityWindowSnapshot[]): boolean {
  return windows.some((window) => window.status === "rejected" || window.status === "allowed_warning");
}

function hasCapacityHealthyTelemetry(windows: CapacityWindowSnapshot[]): boolean {
  return windows.some((window) => window.status === "allowed");
}

const PRIMARY_WINDOW_ADVISORY_WARNING_UTILIZATION: Readonly<Record<string, number>> = {
  "unified-5h": 0.9,
  "unified-7d": 0.75,
};

function hasSurpassedStoredThreshold(window: CapacityWindowSnapshot): boolean {
  return window.utilization !== null
    && window.utilization !== undefined
    && window.surpassedThreshold !== null
    && window.surpassedThreshold !== undefined
    && window.utilization >= window.surpassedThreshold;
}

function normalizePrimaryCapacityWindow(window: CapacityWindowSnapshot): CapacityWindowSnapshot {
  const advisoryWarningUtilization = PRIMARY_WINDOW_ADVISORY_WARNING_UTILIZATION[window.windowName];
  if (advisoryWarningUtilization === undefined) return { ...window };

  const hasUtilizationWarning = window.utilization !== null
    && window.utilization !== undefined
    && window.utilization >= advisoryWarningUtilization;

  const hasDerivedWarning = hasSurpassedStoredThreshold(window) || hasUtilizationWarning;

  if (window.status === "rejected") {
    return {
      ...window,
      status: hasDerivedWarning ? "allowed_warning" : "allowed",
    };
  }

  if (window.status === "allowed_warning") {
    return {
      ...window,
      status: hasDerivedWarning || window.surpassedThreshold === null
        ? "allowed_warning"
        : "allowed",
    };
  }

  return {
    ...window,
    status: hasDerivedWarning ? "allowed_warning" : "allowed",
  };
}

function normalizeUnifiedCapacityWindow(
  unified: CapacityWindowSnapshot,
  detailedWindows: readonly CapacityWindowSnapshot[],
): CapacityWindowSnapshot {
  if (unified.status === "rejected") return { ...unified };
  if (detailedWindows.length === 0) return { ...unified };

  const hasWarningDetail = detailedWindows.some((window) =>
    window.status === "allowed_warning" || window.status === "rejected"
  );

  return {
    ...unified,
    status: hasWarningDetail ? "allowed_warning" : "allowed",
  };
}

function sanitizeCapacityWindows(
  windows: readonly CapacityWindowSnapshot[],
  currentTime: UnixMs,
): CapacityWindowSnapshot[] {
  const relevant = windows
    .filter((window) => isRelevantCapacityWindow(window, currentTime))
    .map((window) => normalizePrimaryCapacityWindow(window));

  const normalized = new Map(relevant.map((window) => [window.windowName, window]));
  const unified = normalized.get("unified");
  if (unified !== undefined) {
    const detailedWindows = relevant.filter((window) =>
      window.windowName === "unified-5h" || window.windowName === "unified-7d"
    );
    normalized.set("unified", normalizeUnifiedCapacityWindow(unified, detailedWindows));
  }

  return [...normalized.values()]
    .sort((a, b) => a.windowName.localeCompare(b.windowName));
}

function isRelevantCapacityWindow(
  window: CapacityWindowSnapshot,
  currentTime: UnixMs,
): boolean {
  if (!PRIMARY_CAPACITY_WINDOW_NAMES.has(window.windowName)) return false;
  if (window.lastSeenAt === null) return false;
  // Data stays valid until the window's resetAt passes. Once resetAt is
  // behind us, the observed utilization is meaningless (the rate-limit
  // cycle has flipped). No lastSeenAt-based stale-out — a 7d observation
  // is useful for the whole cycle even if the key sat idle since.
  if (window.resetAt !== null && window.resetAt <= currentTime) return false;
  return true;
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

const ALL_DAYS: readonly number[] = [0, 1, 2, 3, 4, 5, 6];

function parseAllowedDays(raw: string): readonly number[] {
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return ALL_DAYS;
    const days = parsed.filter((d: unknown): d is number =>
      typeof d === "number" && Number.isInteger(d) && d >= 0 && d <= 6
    );
    return days.length === 0 ? ALL_DAYS : [...new Set(days)].sort((a, b) => a - b);
  } catch {
    return ALL_DAYS;
  }
}

function midnightLocalMs(): number {
  const d = new Date();
  d.setHours(24, 0, 0, 0);
  return d.getTime();
}
