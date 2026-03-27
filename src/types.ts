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
}

export interface ApiKeyEntry {
  readonly key: ApiKey;
  readonly label: KeyLabel;
  stats: ApiKeyStats;
  /** Key is rate-limited until this time. 0 = available now. */
  availableAt: UnixMs;
}

export interface MaskedKeyEntry {
  readonly maskedKey: string;
  readonly label: KeyLabel;
  readonly stats: ApiKeyStats;
  readonly availableAt: UnixMs;
  readonly isAvailable: boolean;
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
}

// ── Persisted state ───────────────────────────────────────────────

export interface StoredState {
  readonly version: 1;
  readonly keys: readonly ApiKeyEntry[];
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
}

export interface TimeseriesQuery {
  readonly hours?: number;
  readonly keyLabel?: string;
  readonly userLabel?: string;
  readonly resolution?: "hour" | "day";
}

// ── Logging ───────────────────────────────────────────────────────

export type LogLevel = "info" | "warn" | "error" | "debug";

export interface LogEntry {
  readonly level: LogLevel;
  readonly msg: string;
  readonly [extra: string]: unknown;
}
