// ── Branded types ─────────────────────────────────────────────────
// Prevents accidentally passing raw strings where a typed key is expected.

declare const __brand: unique symbol;
type Brand<T, B extends string> = T & { readonly [__brand]: B };

export type ApiKey = Brand<string, "ApiKey">;
export type KeyLabel = Brand<string, "KeyLabel">;
export type ProxyToken = Brand<string, "ProxyToken">;
export type UnixMs = Brand<number, "UnixMs">;

export function asApiKey(raw: string): ApiKey {
  if (!raw.startsWith("sk-ant-")) {
    throw new TypeError(`Invalid API key format: must start with "sk-ant-"`);
  }
  return raw as ApiKey;
}

export function asKeyLabel(raw: string): KeyLabel {
  return raw as KeyLabel;
}

export function asProxyToken(raw: string): ProxyToken {
  if (raw.length < 8) {
    throw new TypeError("Proxy token must be at least 8 characters");
  }
  return raw as ProxyToken;
}

export function now(): UnixMs {
  return Date.now() as UnixMs;
}

export function unixMs(n: number): UnixMs {
  return n as UnixMs;
}

// ── Key entry ─────────────────────────────────────────────────────

export interface ApiKeyStats {
  readonly totalRequests: number;
  readonly successfulRequests: number;
  readonly rateLimitHits: number;
  readonly errors: number;
  readonly lastUsedAt: UnixMs | null;
  readonly addedAt: UnixMs;
  readonly totalTokensIn: number;
  readonly totalTokensOut: number;
  readonly totalCacheRead: number;
  readonly totalCacheCreation: number;
}

export type CapacityHealth =
  | "healthy"
  | "warning"
  | "rejected"
  | "cooling_down"
  | "unknown";

export interface CapacityWindowSnapshot {
  readonly windowName: string;
  readonly status: string | null;
  readonly utilization: number | null;
  readonly resetAt: UnixMs | null;
  readonly surpassedThreshold: number | null;
  readonly lastSeenAt: UnixMs | null;
}

export interface CapacitySignalCoverage {
  readonly signalName: string;
  readonly seenCount: number;
  readonly lastSeenAt: UnixMs | null;
}

export interface ApiKeyCapacityState {
  readonly responseCount: number;
  readonly normalizedHeaderCount: number;
  readonly lastResponseAt: UnixMs | null;
  readonly lastHeaderAt: UnixMs | null;
  readonly lastUpstreamStatus: number | null;
  readonly lastRequestId: string | null;
  readonly organizationId: string | null;
  readonly representativeClaim: string | null;
  readonly retryAfterSecs: number | null;
  readonly shouldRetry: boolean | null;
  readonly fallbackAvailable: boolean | null;
  readonly fallbackPercentage: number | null;
  readonly overageStatus: string | null;
  readonly overageDisabledReason: string | null;
  readonly latencyMs: number | null;
  readonly signalCoverage: readonly CapacitySignalCoverage[];
  readonly windows: readonly CapacityWindowSnapshot[];
}

export interface ApiKeyEntry {
  readonly key: ApiKey;
  readonly label: KeyLabel;
  stats: ApiKeyStats;
  capacity: ApiKeyCapacityState;
  /** Key is rate-limited until this time. 0 = available now. */
  availableAt: UnixMs;
  /** Selection priority: 1 = Preferred, 2 = Normal, 3 = Fallback, 4 = Disabled (excluded from routing). */
  priority: number;
  /** Days of the week this key may be used. 0=Sun … 6=Sat. Default: all days. */
  allowedDays: readonly number[];
  /** Adaptive minimum gap between consecutive requests on this key (ms). AIMD:
   *  each fresh short-term 429 grows the gap multiplicatively (decorrelated
   *  jitter, capped); each fresh post-cooldown success shrinks it additively.
   *  Long-term 429s reset to 0. Anthropic's `retry-after` is honored exactly
   *  for the cooldown duration — this gap is what we layer on top to prevent
   *  the thundering-herd of waiting requests all firing the instant the
   *  cooldown lifts. In-memory only. */
  interRequestGapMs: number;
  /** Earliest time we'll fire the next request on this key. Reservation
   *  timestamp updated atomically by reserveFireSlot — concurrent claims in
   *  the same JS tick get sequential fire-times stacked by interRequestGapMs.
   *  In-memory only. */
  nextRequestAt: UnixMs;
}

export interface MaskedKeyEntry {
  readonly maskedKey: string;
  readonly label: KeyLabel;
  readonly stats: ApiKeyStats;
  readonly capacity: ApiKeyCapacityState;
  readonly capacityHealth: CapacityHealth;
  readonly availableAt: UnixMs;
  readonly isAvailable: boolean;
  readonly priority: number;
  readonly allowedDays: readonly number[];
  readonly recentErrors: number;
  /** Current AIMD inter-request gap (ms). 0 means no enforced spacing between
   *  requests on this key; positive means past 429s have made us pace
   *  consecutive requests. Each fresh post-cooldown success shrinks it; each
   *  fresh short-term 429 grows it (with decorrelated jitter). */
  readonly interRequestGapMs: number;
  readonly recentSessions: readonly {
    readonly sessionId: string;
    readonly actor: string;
    readonly firstSeenAt: string;
    readonly lastSeenAt: string;
    readonly totalRequests: number;
    readonly conversations: readonly {
      readonly hash: string | null;
      readonly firstSeenAt: string;
      readonly lastSeenAt: string;
      readonly requestCount: number;
    }[];
  }[];
}

// ── Proxy token entry ─────────────────────────────────────────────

export interface ProxyTokenStats {
  readonly totalRequests: number;
  readonly successfulRequests: number;
  readonly errors: number;
  readonly lastUsedAt: UnixMs | null;
  readonly addedAt: UnixMs;
  readonly totalTokensIn: number;
  readonly totalTokensOut: number;
  readonly totalCacheRead: number;
  readonly totalCacheCreation: number;
}

export interface ProxyTokenEntry {
  readonly token: ProxyToken;
  readonly label: string;
  stats: ProxyTokenStats;
}

export interface MaskedTokenEntry {
  readonly maskedToken: string;
  readonly label: string;
  readonly stats: ProxyTokenStats;
  readonly recentErrors: number;
}

// ── Persisted state ───────────────────────────────────────────────

export interface StoredState {
  readonly version: 1;
  readonly keys: readonly (Omit<ApiKeyEntry, "capacity" | "interRequestGapMs" | "nextRequestAt"> & { readonly capacity?: ApiKeyCapacityState })[];
  readonly tokens?: readonly ProxyTokenEntry[];
}

// ── Proxy result (discriminated union) ────────────────────────────

interface ProxySuccess {
  readonly kind: "success";
  readonly response: Response;
  readonly usedKey: ApiKeyEntry;
}

interface ProxyRateLimited {
  readonly kind: "rate_limited";
  readonly retryAfterSecs: number;
  readonly usedKey: ApiKeyEntry;
}

interface ProxyError {
  readonly kind: "error";
  readonly status: number;
  readonly body: string;
  readonly usedKey: ApiKeyEntry;
}

interface ProxyNoKeys {
  readonly kind: "no_keys";
}

interface ProxyAllExhausted {
  readonly kind: "all_exhausted";
  readonly earliestAvailableAt: UnixMs;
}

export type ProxyResult =
  | ProxySuccess
  | ProxyRateLimited
  | ProxyError
  | ProxyNoKeys
  | ProxyAllExhausted;

// ── Admin API request types ───────────────────────────────────────

export interface AddKeyRequest {
  readonly key: string;
  readonly label?: string | undefined;
}

export interface AddTokenRequest {
  readonly token: string;
  readonly label?: string | undefined;
}

// ── Config ────────────────────────────────────────────────────────

export interface ProxyConfig {
  readonly port: number;
  readonly upstream: string;
  readonly adminToken: string | null;
  readonly dataDir: string;
  readonly maxRetriesPerRequest: number;
  readonly firstChunkTimeoutMs: number;
  readonly firstChunkTimeoutMsContext1m: number;
  readonly streamIdleTimeoutMs: number;
  readonly maxFirstChunkRetries: number;
  readonly webhookUrl: string | null;
  /** When true, /v1/messages calls within a session pin per-conversation (by
   *  first-message hash) so sub-agents can land on different keys. When false
   *  (the default), every call in a session shares one key — simpler,
   *  matches pre-conversation-pinning behavior. */
  readonly perConversationPinning: boolean;
}

// ── Timeseries statistics ────────────────────────────────────────

export interface TimeseriesBucket {
  readonly bucket: string;
  readonly requests: number;
  readonly successes: number;
  readonly errors: number;
  readonly rateLimits: number;
  readonly tokensIn: number;
  readonly tokensOut: number;
  readonly cacheRead: number;
  readonly cacheCreation: number;
}

export interface TimeseriesQuery {
  readonly hours?: number;
  readonly keyLabel?: string;
  readonly userLabel?: string;
  readonly resolution?: "hour" | "day";
}

export interface CapacityWindowObservation {
  readonly windowName: string;
  readonly status?: string | null;
  readonly utilization?: number | null;
  readonly resetAt?: UnixMs | null;
  readonly surpassedThreshold?: number | null;
  readonly lastSeenAt?: UnixMs | null;
}

export interface CapacityObservation {
  readonly seenAt: UnixMs;
  readonly httpStatus?: number;
  readonly requestId?: string | null;
  readonly organizationId?: string | null;
  readonly representativeClaim?: string | null;
  readonly retryAfterSecs?: number | null;
  readonly shouldRetry?: boolean | null;
  readonly fallbackAvailable?: boolean | null;
  readonly fallbackPercentage?: number | null;
  readonly overageStatus?: string | null;
  readonly overageDisabledReason?: string | null;
  readonly latencyMs?: number | null;
  readonly observedSignals?: readonly string[];
  readonly windows?: readonly CapacityWindowObservation[];
}

export interface CapacitySummaryWindow {
  readonly windowName: string;
  readonly knownKeys: number;
  readonly allowedKeys: number;
  readonly warningKeys: number;
  readonly rejectedKeys: number;
  readonly maxUtilization: number | null;
  readonly medianUtilization: number | null;
  readonly nextResetAt: UnixMs | null;
}

export interface CapacitySummary {
  readonly healthyKeys: number;
  readonly warningKeys: number;
  readonly rejectedKeys: number;
  readonly coolingDownKeys: number;
  readonly unknownKeys: number;
  readonly fallbackAvailableKeys: number;
  readonly overageRejectedKeys: number;
  readonly distinctOrganizations: number;
  readonly lastUpdatedAt: UnixMs | null;
  readonly windows: readonly CapacitySummaryWindow[];
}

export interface CapacityTimeseriesQuery {
  readonly hours?: number;
  readonly keyLabel?: string;
  readonly resolution?: "hour" | "day";
}

export interface CapacityTimeseriesBucket {
  readonly bucket: string;
  readonly windowName: string;
  readonly samples: number;
  readonly allowed: number;
  readonly warning: number;
  readonly rejected: number;
  readonly avgUtilization: number | null;
  readonly maxUtilization: number | null;
  /** Mean of per-key average utilizations in this bucket. Differs from
   *  avgUtilization (sample-weighted) when some keys produced far more
   *  telemetry than others. Each key contributes equally — operator
   *  intuition is "fleet of N keys, each one slot of capacity". */
  readonly avgUtilizationPerKey: number | null;
  /** Number of distinct keys that produced any utilization telemetry in
   *  this bucket. Used together with the response-level fleetSize to
   *  derive fleet-wide headroom (keys with no data count as 100% remaining). */
  readonly keysObserved: number;
  /** Fleet-wide effective utilization for this (bucket, window), expressed
   *  as a 0..1 fraction of total fleet capacity, computed server-side and
   *  ready for `headroom = 1 - effectiveFleetUtilization`. Two corrections
   *  over the raw sample average:
   *   - Forward-fill: a key with persisted utilization (lastSeenAt < bucket
   *     end, resetAt > bucket start) but no sample in this bucket is treated
   *     as still sitting at that utilization, instead of disappearing and
   *     boosting apparent headroom.
   *   - Cross-window fold: in the 5h line, a key whose 7d utilization is at
   *     the cap counts as 100% util regardless of 5h state, because a
   *     weekly-blocked key can't serve any traffic. The 7d line is unaffected
   *     by 5h state.
   *  Null when fleet size is zero. */
  readonly effectiveFleetUtilization: number | null;
  /** Number of distinct keys whose effective utilization is known for this
   *  (bucket, window) — either sampled directly OR forward-filled from
   *  persisted state. The remainder of the fleet (fleetSize - keysAccounted)
   *  is treated as 0% utilization (unknown = full headroom). */
  readonly keysAccounted: number;
}

// ── Seasonal request factors (traffic pattern by day-of-week × hour) ─────

/** One entry in a 168-slot table (dow × hour). `factor` is the request
 *  volume in this slot relative to the mean across all observed slots — 1.0
 *  means "average hour", 2.0 means "twice as busy as average", 0.2 means
 *  "quiet". Slots with fewer than MIN_BASELINE_SAMPLES_PER_SLOT observations
 *  are returned with factor = 1 (treated as "no signal"). */
export interface SeasonalFactorSlot {
  readonly dow: number;
  readonly hour: number;
  readonly factor: number;
  readonly samples: number;
}

export interface SeasonalFactorTable {
  readonly weeks: number;
  readonly generatedAt: UnixMs;
  readonly totalSamples: number;
  readonly slots: readonly SeasonalFactorSlot[];
}

// ── Logging ───────────────────────────────────────────────────────

export type LogLevel = "info" | "warn" | "error" | "debug";

export interface LogEntry {
  readonly level: LogLevel;
  readonly msg: string;
  readonly [extra: string]: unknown;
}
