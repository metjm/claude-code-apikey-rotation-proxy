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
  /** Selection priority: 1 = Preferred, 2 = Normal, 3 = Fallback. */
  priority: number;
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
  readonly recentErrors: number;
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
  readonly keys: readonly (Omit<ApiKeyEntry, "capacity"> & { readonly capacity?: ApiKeyCapacityState })[];
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
  readonly webhookUrl: string | null;
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
}

// ── Logging ───────────────────────────────────────────────────────

export type LogLevel = "info" | "warn" | "error" | "debug";

export interface LogEntry {
  readonly level: LogLevel;
  readonly msg: string;
  readonly [extra: string]: unknown;
}
