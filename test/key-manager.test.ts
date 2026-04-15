import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  KeyManager,
  spreadProbabilityFromPeak,
  hashConversationKeyToUnit,
} from "../src/key-manager.ts";
import type {
  ApiKeyEntry,
  ProxyTokenEntry,
  StoredState,
} from "../src/types.ts";
import { unixMs } from "../src/types.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const VALID_KEY_1 = "sk-ant-api03-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const VALID_KEY_2 = "sk-ant-api03-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const VALID_KEY_3 = "sk-ant-api03-cccccccccccccccccccccccccccccccccc";
const VALID_TOKEN_1 = "my-proxy-token-aaaa";
const VALID_TOKEN_2 = "my-proxy-token-bbbb";

let tempDir: string;
let savedDbPath: string | undefined;
let managers: KeyManager[];

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), "km-test-"));
  managers = [];
  // Stash and clear DB_PATH so it does not leak between tests
  savedDbPath = process.env["DB_PATH"];
  delete process.env["DB_PATH"];
});

afterEach(() => {
  for (const manager of managers) {
    try {
      manager.close();
    } catch {
      // best-effort cleanup
    }
  }
  managers = [];
  // Restore original env
  if (savedDbPath !== undefined) {
    process.env["DB_PATH"] = savedDbPath;
  } else {
    delete process.env["DB_PATH"];
  }
  try {
    rmSync(tempDir, { recursive: true, force: true });
  } catch {
    // best-effort cleanup
  }
});

/** Create a KeyManager for testing. */
function create(dataDir?: string): KeyManager {
  const manager = new KeyManager(dataDir ?? tempDir);
  managers.push(manager);
  return manager;
}

function trackManager(manager: KeyManager): KeyManager {
  managers.push(manager);
  return manager;
}

/** Build a legacy state.json payload for migration tests. */
function writeLegacyState(
  dir: string,
  state: StoredState,
): void {
  writeFileSync(join(dir, "state.json"), JSON.stringify(state));
}

/**
 * Wait for the debounced save timer to fire.
 * The timer is 1 000 ms, so 1 200 ms gives comfortable headroom.
 */
function waitForSave(ms = 1_300): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ===========================================================================
// Tests
// ===========================================================================

// ── Schema & Initialization ─────────────────────────────────────────────────

describe("Schema & Initialization", () => {
  test("creates state.db in dataDir", () => {
    create();
    expect(existsSync(join(tempDir, "state.db"))).toBe(true);
  });

  test("creates parent directories recursively", () => {
    const nested = join(tempDir, "a", "b", "c");
    create(nested);
    expect(existsSync(join(nested, "state.db"))).toBe(true);
  });

  test("DB_PATH env var overrides default location", () => {
    const customPath = join(tempDir, "custom", "my.db");
    process.env["DB_PATH"] = customPath;
    create();
    expect(existsSync(customPath)).toBe(true);
    // The default location should NOT have been created
    expect(existsSync(join(tempDir, "state.db"))).toBe(false);
  });

  test("tables api_keys and proxy_tokens are created", () => {
    create();
    const db = new Database(join(tempDir, "state.db"), { readonly: true });
    const tables = db
      .query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
      )
      .all() as { name: string }[];
    const names = tables.map((t) => t.name);
    expect(names).toContain("api_keys");
    expect(names).toContain("proxy_tokens");
    db.close();
  });

  test("WAL mode is enabled", () => {
    create();
    const db = new Database(join(tempDir, "state.db"), { readonly: true });
    const row = db.query("PRAGMA journal_mode").get() as {
      journal_mode: string;
    };
    expect(row.journal_mode).toBe("wal");
    db.close();
  });
});

// ── Migration from state.json ───────────────────────────────────────────────

describe("Migration from state.json", () => {
  const legacyKey = (key: string, label: string): StoredState["keys"][number] => ({
    key: key as any,
    label: label as any,
    stats: {
      totalRequests: 5,
      successfulRequests: 4,
      rateLimitHits: 1,
      errors: 0,
      lastUsedAt: 1000 as any,
      addedAt: 500 as any,
      totalTokensIn: 100,
      totalTokensOut: 200,
    },
    availableAt: 0 as any,
  });

  const legacyToken = (
    token: string,
    label: string,
  ): NonNullable<StoredState["tokens"]>[number] => ({
    token: token as any,
    label,
    stats: {
      totalRequests: 3,
      successfulRequests: 2,
      errors: 1,
      lastUsedAt: 900 as any,
      addedAt: 400 as any,
      totalTokensIn: 50,
      totalTokensOut: 80,
    },
  });

  test("migrates keys from legacy state.json", () => {
    writeLegacyState(tempDir, {
      version: 1,
      keys: [legacyKey(VALID_KEY_1, "key-one")],
    });
    const km = create();
    const keys = km.listKeys();
    expect(keys.length).toBe(1);
    expect(keys[0]!.label).toBe("key-one");
  });

  test("migrates tokens from legacy state.json", () => {
    writeLegacyState(tempDir, {
      version: 1,
      keys: [],
      tokens: [legacyToken(VALID_TOKEN_1, "tok-one")],
    });
    const km = create();
    const tokens = km.listTokens();
    expect(tokens.length).toBe(1);
    expect(tokens[0]!.label).toBe("tok-one");
  });

  test("preserves all stats during migration", () => {
    writeLegacyState(tempDir, {
      version: 1,
      keys: [legacyKey(VALID_KEY_1, "key-one")],
      tokens: [legacyToken(VALID_TOKEN_1, "tok-one")],
    });
    const km = create();

    const ks = km.listKeys()[0]!.stats;
    expect(ks.totalRequests).toBe(5);
    expect(ks.successfulRequests).toBe(4);
    expect(ks.rateLimitHits).toBe(1);
    expect(ks.errors).toBe(0);
    expect(ks.lastUsedAt).toBe(1000);
    expect(ks.addedAt).toBe(500);
    expect(ks.totalTokensIn).toBe(100);
    expect(ks.totalTokensOut).toBe(200);

    const ts = km.listTokens()[0]!.stats;
    expect(ts.totalRequests).toBe(3);
    expect(ts.successfulRequests).toBe(2);
    expect(ts.errors).toBe(1);
    expect(ts.lastUsedAt).toBe(900);
    expect(ts.addedAt).toBe(400);
    expect(ts.totalTokensIn).toBe(50);
    expect(ts.totalTokensOut).toBe(80);
  });

  test("deletes state.json after successful migration", () => {
    writeLegacyState(tempDir, {
      version: 1,
      keys: [legacyKey(VALID_KEY_1, "k")],
    });
    create();
    expect(existsSync(join(tempDir, "state.json"))).toBe(false);
  });

  test("skips migration if DB already has data", () => {
    // First KeyManager creates DB with a key
    const km1 = create();
    km1.addKey(VALID_KEY_1, "existing");

    // Now drop a state.json -- it should be ignored
    writeLegacyState(tempDir, {
      version: 1,
      keys: [legacyKey(VALID_KEY_2, "from-json")],
    });

    const km2 = create();
    const labels = km2.listKeys().map((k) => k.label);
    expect(labels).toContain("existing");
    expect(labels).not.toContain("from-json");
    // state.json should still be on disk (not deleted)
    expect(existsSync(join(tempDir, "state.json"))).toBe(true);
  });

  test("skips migration if no state.json exists", () => {
    // Just constructing should not throw
    const km = create();
    expect(km.totalCount()).toBe(0);
  });

  test("handles corrupted state.json gracefully", () => {
    writeFileSync(join(tempDir, "state.json"), "NOT VALID JSON {{{");
    // Should not throw -- the constructor logs an error and continues
    const km = create();
    expect(km.totalCount()).toBe(0);
  });
});

// ── Key CRUD ────────────────────────────────────────────────────────────────

describe("Key CRUD", () => {
  test("addKey() with valid key and label", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "my-label");
    expect(entry.key).toBe(VALID_KEY_1);
    expect(entry.label).toBe("my-label");
    expect(entry.stats.totalRequests).toBe(0);
    expect(entry.availableAt).toBe(0);
  });

  test("addKey() with auto-generated label", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1);
    expect(entry.label).toBe("key-1");

    const entry2 = km.addKey(VALID_KEY_2);
    expect(entry2.label).toBe("key-2");
  });

  test("addKey() rejects duplicate keys", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "first");
    expect(() => km.addKey(VALID_KEY_1, "second")).toThrow(
      "Key already registered",
    );
  });

  test("addKey() rejects invalid key format (not sk-ant-*)", () => {
    const km = create();
    expect(() => km.addKey("invalid-key-format")).toThrow(
      'Invalid API key format: must start with "sk-ant-"',
    );
  });

  test("removeKey() succeeds and returns true", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "removable");
    expect(km.removeKey(VALID_KEY_1)).toBe(true);
    expect(km.totalCount()).toBe(0);
  });

  test("removeKey() returns false for unknown key", () => {
    const km = create();
    expect(km.removeKey("sk-ant-api03-nonexistent")).toBe(false);
  });

  test("listKeys() returns masked keys with stats", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "label-a");
    const list = km.listKeys();

    expect(list.length).toBe(1);
    const entry = list[0]!;
    // Key is masked -- should NOT equal the raw key
    expect(entry.maskedKey).not.toBe(VALID_KEY_1);
    // Masked format: first 10 chars + "..." + last 4 chars
    expect(entry.maskedKey).toBe(
      `${VALID_KEY_1.slice(0, 10)}...${VALID_KEY_1.slice(-4)}`,
    );
    expect(entry.label).toBe("label-a");
    expect(entry.stats.totalRequests).toBe(0);
    expect(entry.recentSessions15m).toEqual([]);
  });

  test("listKeys() tracks recent sessions active in the last 15 minutes", () => {
    const km = create();
    const originalNow = Date.now;
    let fakeNow = 1_000_000;
    Date.now = () => fakeNow;

    try {
      km.addKey(VALID_KEY_1, "a");
      km.addKey(VALID_KEY_2, "b");

      expect(km.getKeyForConversation("user-1:session-a", "session-a").entry?.key).toBe(VALID_KEY_1);
      expect(km.getKeyForConversation("user-1:session-b", "session-b").entry?.key).toBe(VALID_KEY_2);

      let keys = km.listKeys().sort((a, b) => String(a.label).localeCompare(String(b.label)));
      expect(keys.map((key) => key.recentSessions15m.map((session) => session.sessionId))).toEqual([
        ["session-a"],
        ["session-b"],
      ]);

      fakeNow += 16 * 60 * 1000;
      keys = km.listKeys().sort((a, b) => String(a.label).localeCompare(String(b.label)));
      expect(keys.map((key) => key.recentSessions15m)).toEqual([[], []]);

      expect(km.getKeyForConversation("user-1:session-a", "session-a").entry?.key).toBe(VALID_KEY_1);
      keys = km.listKeys().sort((a, b) => String(a.label).localeCompare(String(b.label)));
      expect(keys.map((key) => key.recentSessions15m.map((session) => session.sessionId))).toEqual([
        ["session-a"],
        [],
      ]);
    } finally {
      Date.now = originalNow;
    }
  });

  test("listKeys() returns empty array when no keys", () => {
    const km = create();
    expect(km.listKeys()).toEqual([]);
  });

  test("listKeys() includes isAvailable based on current time", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "avail-test");

    // Key starts with availableAt = 0, so it should be available
    let list = km.listKeys();
    expect(list[0]!.isAvailable).toBe(true);

    // Simulate rate limit far in the future
    km.recordRateLimit(entry, 99999);
    list = km.listKeys();
    expect(list[0]!.isAvailable).toBe(false);
  });
});

// ── Key Selection ───────────────────────────────────────────────────────────

describe("Key Selection", () => {
  test("getNextAvailableKey() returns null when empty", () => {
    const km = create();
    expect(km.getNextAvailableKey()).toBeNull();
  });

  test("getNextAvailableKey() returns available key", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "one");
    const key = km.getNextAvailableKey();
    expect(key).not.toBeNull();
    expect(key!.key).toBe(VALID_KEY_1);
  });

  test("getNextAvailableKey() skips rate-limited keys", () => {
    const km = create();
    const k1 = km.addKey(VALID_KEY_1, "limited");
    km.addKey(VALID_KEY_2, "free");

    // Rate-limit key 1 far into the future
    km.recordRateLimit(k1, 99999);

    const selected = km.getNextAvailableKey();
    expect(selected).not.toBeNull();
    expect(selected!.key).toBe(VALID_KEY_2);
  });

  test("getNextAvailableKey() returns null when all rate-limited", () => {
    const km = create();
    const k1 = km.addKey(VALID_KEY_1, "a");
    const k2 = km.addKey(VALID_KEY_2, "b");

    km.recordRateLimit(k1, 99999);
    km.recordRateLimit(k2, 99999);

    expect(km.getNextAvailableKey()).toBeNull();
  });

  test("getNextAvailableKey() prefers most recently used (sticky for cache)", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "old");
    const k2 = km.addKey(VALID_KEY_2, "recent");

    // Use key 2 so it has a more recent lastUsedAt
    km.recordRequest(k2);

    const selected = km.getNextAvailableKey();
    expect(selected).not.toBeNull();
    expect(selected!.key).toBe(VALID_KEY_2);
  });

  test("getEarliestAvailableAt() returns soonest cooldown for rate-limited keys", () => {
    const km = create();
    const k1 = km.addKey(VALID_KEY_1, "far");
    const k2 = km.addKey(VALID_KEY_2, "soon");
    km.recordRateLimit(k1, 99999);
    km.recordRateLimit(k2, 10);
    const earliest = km.getEarliestAvailableAt();
    // Should be approximately k2's availableAt (soonest cooldown)
    expect(earliest).toBeGreaterThan(0);
    expect(Math.abs(earliest - k2.availableAt)).toBeLessThan(1000);
  });

  test("availableCount() and totalCount() are correct", () => {
    const km = create();
    expect(km.totalCount()).toBe(0);
    expect(km.availableCount()).toBe(0);

    const k1 = km.addKey(VALID_KEY_1, "a");
    km.addKey(VALID_KEY_2, "b");

    expect(km.totalCount()).toBe(2);
    expect(km.availableCount()).toBe(2);

    km.recordRateLimit(k1, 99999);

    expect(km.totalCount()).toBe(2);
    expect(km.availableCount()).toBe(1);
  });

  test("getKeyForConversation() keeps the same conversation sticky to its assigned key", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");
    const other = km.addKey(VALID_KEY_2, "b");

    const first = km.getKeyForConversation("user-1:session-a");
    expect(first.entry?.key).toBe(VALID_KEY_1);
    expect(first.affinityHit).toBe(false);

    km.recordRequest(other);

    const second = km.getKeyForConversation("user-1:session-a");
    expect(second.entry?.key).toBe(VALID_KEY_1);
    expect(second.affinityHit).toBe(true);
    expect(second.routingDecision).toBe("conversation_affinity_hit");
  });

  test("getKeyForConversation() balances different conversations across equal-priority keys", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");
    km.addKey(VALID_KEY_2, "b");

    const first = km.getKeyForConversation("user-1:session-a");
    expect(first.entry?.key).toBe(VALID_KEY_1);
    km.recordRequest(first.entry!);

    const second = km.getKeyForConversation("user-1:session-b");
    expect(second.entry?.key).toBe(VALID_KEY_2);
    km.recordRequest(second.entry!);

    const third = km.getKeyForConversation("user-1:session-c");
    expect(third.entry?.key).toBe(VALID_KEY_1);
    expect(third.priorityTier).toBe(2);
  });

  test("getKeyForConversation() only balances within the best available priority tier", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "preferred-a");
    km.addKey(VALID_KEY_2, "preferred-b");
    km.addKey(VALID_KEY_3, "normal-c");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 1);
    km.updateKeyPriority(VALID_KEY_3, 2);

    const first = km.getKeyForConversation("user-1:session-a");
    expect(first.entry?.key).toBe(VALID_KEY_1);
    km.recordRequest(first.entry!);

    const second = km.getKeyForConversation("user-1:session-b");
    expect(second.entry?.key).toBe(VALID_KEY_2);
    km.recordRequest(second.entry!);

    const third = km.getKeyForConversation("user-1:session-c");
    expect([VALID_KEY_1, VALID_KEY_2]).toContain(third.entry?.key);
    expect(third.entry?.key).not.toBe(VALID_KEY_3);
    expect(third.priorityTier).toBe(1);
  });

  test("getKeyForConversation() remaps a conversation when its assigned key is rate-limited", () => {
    const km = create();
    const firstKey = km.addKey(VALID_KEY_1, "a");
    km.addKey(VALID_KEY_2, "b");

    const first = km.getKeyForConversation("user-1:session-a");
    expect(first.entry?.key).toBe(VALID_KEY_1);

    km.recordRateLimit(firstKey, 99999);

    const second = km.getKeyForConversation("user-1:session-a");
    expect(second.entry?.key).toBe(VALID_KEY_2);
    expect(second.remapped).toBe(true);
    expect(second.routingDecision).toBe("conversation_affinity_remapped");
  });

  test("getKeyForConversation() persists conversation affinity across reload", () => {
    const km1 = create();
    km1.addKey(VALID_KEY_1, "a");
    km1.addKey(VALID_KEY_2, "b");
    const first = km1.getKeyForConversation("user-1:session-a");
    expect(first.entry?.key).toBe(VALID_KEY_1);
    km1.close();

    const km2 = create();
    const second = km2.getKeyForConversation("user-1:session-a");
    expect(second.entry?.key).toBe(VALID_KEY_1);
    expect(second.affinityHit).toBe(true);
  });
});

// ── Key Stats Recording ─────────────────────────────────────────────────────

describe("Key Stats Recording", () => {
  test("recordRequest() increments totalRequests and sets lastUsedAt", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "stat-test");

    expect(entry.stats.lastUsedAt).toBeNull();
    const before = Date.now();
    km.recordRequest(entry);
    const after = Date.now();

    expect(entry.stats.totalRequests).toBe(1);
    expect(entry.stats.lastUsedAt).not.toBeNull();
    expect(entry.stats.lastUsedAt!).toBeGreaterThanOrEqual(before);
    expect(entry.stats.lastUsedAt!).toBeLessThanOrEqual(after);
  });

  test("recordSuccess() increments successfulRequests and adds tokens", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "success-test");

    km.recordSuccess(entry, 100, 200);
    expect(entry.stats.successfulRequests).toBe(1);
    expect(entry.stats.totalTokensIn).toBe(100);
    expect(entry.stats.totalTokensOut).toBe(200);

    km.recordSuccess(entry, 50, 75);
    expect(entry.stats.successfulRequests).toBe(2);
    expect(entry.stats.totalTokensIn).toBe(150);
    expect(entry.stats.totalTokensOut).toBe(275);
  });

  test("recordRateLimit() increments rateLimitHits and sets availableAt", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "rl-test");

    const before = Date.now();
    km.recordRateLimit(entry, 30);
    const after = Date.now();

    expect(entry.stats.rateLimitHits).toBe(1);
    // availableAt should be ~30s in the future
    expect(entry.availableAt).toBeGreaterThanOrEqual(before + 30_000);
    expect(entry.availableAt).toBeLessThanOrEqual(after + 30_000);
  });

  test("recordRateLimit() defaults to 60s when retryAfterSecs <= 0", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "rl-default");

    const before = Date.now();
    km.recordRateLimit(entry, 0);
    const after = Date.now();

    // Should default to 60s
    expect(entry.availableAt).toBeGreaterThanOrEqual(before + 60_000);
    expect(entry.availableAt).toBeLessThanOrEqual(after + 60_000);

    // Also test negative value
    const entry2 = km.addKey(VALID_KEY_2, "rl-negative");
    const before2 = Date.now();
    km.recordRateLimit(entry2, -5);
    const after2 = Date.now();
    expect(entry2.availableAt).toBeGreaterThanOrEqual(before2 + 60_000);
    expect(entry2.availableAt).toBeLessThanOrEqual(after2 + 60_000);
  });

  test("resetKeyCooldowns() clears cooldowns immediately and persists", () => {
    const km = create();
    const cooled = km.addKey(VALID_KEY_1, "cooled");
    km.addKey(VALID_KEY_2, "ready");

    km.recordRateLimit(cooled, 300);
    expect(km.availableCount()).toBe(1);

    expect(km.resetKeyCooldowns()).toBe(1);
    expect(km.availableCount()).toBe(2);
    expect(km.listKeys().every((key) => key.isAvailable)).toBe(true);

    const reloaded = create();
    expect(reloaded.availableCount()).toBe(2);
    expect(reloaded.listKeys().every((key) => key.availableAt === 0)).toBe(true);
  });

  test("recordError() increments errors", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "err-test");

    km.recordError(entry);
    expect(entry.stats.errors).toBe(1);
    km.recordError(entry);
    expect(entry.stats.errors).toBe(2);
  });

  test("stats accumulate correctly across multiple calls", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "accumulate");

    km.recordRequest(entry);
    km.recordRequest(entry);
    km.recordRequest(entry);
    km.recordSuccess(entry, 10, 20);
    km.recordSuccess(entry, 30, 40);
    km.recordRateLimit(entry, 5);
    km.recordError(entry);
    km.recordError(entry);

    expect(entry.stats.totalRequests).toBe(3);
    expect(entry.stats.successfulRequests).toBe(2);
    expect(entry.stats.rateLimitHits).toBe(1);
    expect(entry.stats.errors).toBe(2);
    expect(entry.stats.totalTokensIn).toBe(40);
    expect(entry.stats.totalTokensOut).toBe(60);
  });
});

// ── Token CRUD ──────────────────────────────────────────────────────────────

describe("Token CRUD", () => {
  test("addToken() with valid token and label", () => {
    const km = create();
    const entry = km.addToken(VALID_TOKEN_1, "my-user");
    expect(entry.token).toBe(VALID_TOKEN_1);
    expect(entry.label).toBe("my-user");
    expect(entry.stats.totalRequests).toBe(0);
  });

  test("addToken() with auto-generated label", () => {
    const km = create();
    const t1 = km.addToken(VALID_TOKEN_1);
    expect(t1.label).toBe("user-1");

    const t2 = km.addToken(VALID_TOKEN_2);
    expect(t2.label).toBe("user-2");
  });

  test("addToken() rejects duplicate tokens", () => {
    const km = create();
    km.addToken(VALID_TOKEN_1, "first");
    expect(() => km.addToken(VALID_TOKEN_1, "second")).toThrow(
      "Token already registered",
    );
  });

  test("addToken() rejects short tokens (< 8 chars)", () => {
    const km = create();
    expect(() => km.addToken("short")).toThrow(
      "Proxy token must be at least 8 characters",
    );
    expect(() => km.addToken("1234567")).toThrow(
      "Proxy token must be at least 8 characters",
    );
  });

  test("removeToken() succeeds and returns true", () => {
    const km = create();
    km.addToken(VALID_TOKEN_1, "removable");
    expect(km.removeToken(VALID_TOKEN_1)).toBe(true);
    expect(km.hasTokens()).toBe(false);
  });

  test("removeToken() returns false for unknown token", () => {
    const km = create();
    expect(km.removeToken("unknown-token-value")).toBe(false);
  });

  test("listTokens() returns masked tokens", () => {
    const km = create();
    km.addToken(VALID_TOKEN_1, "user-a");
    const list = km.listTokens();

    expect(list.length).toBe(1);
    const entry = list[0]!;
    expect(entry.maskedToken).not.toBe(VALID_TOKEN_1);
    // Masked format for tokens: first 4 chars + "..." + last 4 chars
    expect(entry.maskedToken).toBe(
      `${VALID_TOKEN_1.slice(0, 4)}...${VALID_TOKEN_1.slice(-4)}`,
    );
    expect(entry.label).toBe("user-a");
    expect(entry.stats.totalRequests).toBe(0);
  });

  test("hasTokens() returns true/false correctly", () => {
    const km = create();
    expect(km.hasTokens()).toBe(false);

    km.addToken(VALID_TOKEN_1, "tok");
    expect(km.hasTokens()).toBe(true);

    km.removeToken(VALID_TOKEN_1);
    expect(km.hasTokens()).toBe(false);
  });

  test("validateToken() returns entry for valid token", () => {
    const km = create();
    km.addToken(VALID_TOKEN_1, "validate-me");
    const entry = km.validateToken(VALID_TOKEN_1);
    expect(entry).not.toBeNull();
    expect(entry!.token).toBe(VALID_TOKEN_1);
    expect(entry!.label).toBe("validate-me");
  });

  test("validateToken() returns null for invalid token", () => {
    const km = create();
    km.addToken(VALID_TOKEN_1, "only-one");
    expect(km.validateToken("wrong-token-entirely")).toBeNull();
  });
});

// ── Token Stats Recording ───────────────────────────────────────────────────

describe("Token Stats Recording", () => {
  test("recordTokenRequest() increments totalRequests", () => {
    const km = create();
    const entry = km.addToken(VALID_TOKEN_1, "tr-test");

    expect(entry.stats.totalRequests).toBe(0);
    km.recordTokenRequest(entry);
    expect(entry.stats.totalRequests).toBe(1);
    km.recordTokenRequest(entry);
    expect(entry.stats.totalRequests).toBe(2);
    expect(entry.stats.lastUsedAt).not.toBeNull();
  });

  test("recordTokenSuccess() increments successfulRequests and adds tokens", () => {
    const km = create();
    const entry = km.addToken(VALID_TOKEN_1, "ts-test");

    km.recordTokenSuccess(entry, 100, 200);
    expect(entry.stats.successfulRequests).toBe(1);
    expect(entry.stats.totalTokensIn).toBe(100);
    expect(entry.stats.totalTokensOut).toBe(200);

    km.recordTokenSuccess(entry, 50, 75);
    expect(entry.stats.successfulRequests).toBe(2);
    expect(entry.stats.totalTokensIn).toBe(150);
    expect(entry.stats.totalTokensOut).toBe(275);
  });

  test("recordTokenError() increments errors", () => {
    const km = create();
    const entry = km.addToken(VALID_TOKEN_1, "te-test");

    km.recordTokenError(entry);
    expect(entry.stats.errors).toBe(1);
    km.recordTokenError(entry);
    expect(entry.stats.errors).toBe(2);
  });
});

// ── Label Updates ───────────────────────────────────────────────────────────

describe("Key label updates", () => {
  test("updateKeyLabel changes the label in memory and DB", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "old-label");
    const result = km.updateKeyLabel(VALID_KEY_1, "new-label");
    expect(result).toBe(true);
    const listed = km.listKeys();
    expect(listed[0]!.label).toBe("new-label");
    // Verify persisted in DB
    const km2 = trackManager(new KeyManager(tempDir));
    expect(km2.listKeys()[0]!.label).toBe("new-label");
  });

  test("updateKeyLabel returns false for unknown key", () => {
    const km = create();
    expect(km.updateKeyLabel("sk-ant-api03-nonexistent", "label")).toBe(false);
  });

  test("updateKeyLabel preserves stats", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "orig");
    km.recordRequest(entry);
    km.recordSuccess(entry, 100, 50);
    km.updateKeyLabel(VALID_KEY_1, "renamed");
    const listed = km.listKeys();
    expect(listed[0]!.label).toBe("renamed");
    expect(listed[0]!.stats.totalRequests).toBe(1);
    expect(listed[0]!.stats.totalTokensIn).toBe(100);
  });
});

describe("Key priority", () => {
  test("new keys default to priority 2 (Normal)", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");
    const listed = km.listKeys();
    expect(listed[0]!.priority).toBe(2);
  });

  test("updateKeyPriority changes priority in memory and DB", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");
    const result = km.updateKeyPriority(VALID_KEY_1, 1);
    expect(result).toBe(true);
    expect(km.listKeys()[0]!.priority).toBe(1);
    // Verify persisted
    const km2 = trackManager(new KeyManager(tempDir));
    expect(km2.listKeys()[0]!.priority).toBe(1);
  });

  test("updateKeyPriority returns false for unknown key", () => {
    const km = create();
    expect(km.updateKeyPriority("sk-ant-api03-nonexistent", 1)).toBe(false);
  });

  test("updateKeyPriorityByMask changes priority via masked key", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");
    const masked = km.listKeys()[0]!.maskedKey;
    const result = km.updateKeyPriorityByMask(masked, 3);
    expect(result).toBe(true);
    expect(km.listKeys()[0]!.priority).toBe(3);
  });

  test("getNextAvailableKey() prefers lower priority number", () => {
    const km = create();
    const k1 = km.addKey(VALID_KEY_1, "fallback");
    km.addKey(VALID_KEY_2, "preferred");
    // Make k1 more recently used so it would normally be selected first
    km.recordRequest(k1);
    // Set k1 to Fallback, k2 to Preferred
    km.updateKeyPriority(VALID_KEY_1, 3);
    km.updateKeyPriority(VALID_KEY_2, 1);
    const selected = km.getNextAvailableKey();
    expect(selected).not.toBeNull();
    expect(selected!.key).toBe(VALID_KEY_2);
  });

  test("getNextAvailableKey() falls back to lower-priority keys when higher are rate-limited", () => {
    const km = create();
    const k1 = km.addKey(VALID_KEY_1, "preferred");
    km.addKey(VALID_KEY_2, "fallback");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 3);
    // Rate-limit the preferred key
    km.recordRateLimit(k1, 99999);
    const selected = km.getNextAvailableKey();
    expect(selected).not.toBeNull();
    expect(selected!.key).toBe(VALID_KEY_2);
  });

  test("within same priority, LRU ordering is preserved", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");
    const k2 = km.addKey(VALID_KEY_2, "b");
    // Both at same priority (default 2)
    // Use k2 so it becomes most recently used
    km.recordRequest(k2);
    const selected = km.getNextAvailableKey();
    expect(selected).not.toBeNull();
    expect(selected!.key).toBe(VALID_KEY_2);
  });

  test("priority persists across reload", () => {
    const km1 = create();
    km1.addKey(VALID_KEY_1, "a");
    km1.updateKeyPriority(VALID_KEY_1, 3);
    const km2 = trackManager(new KeyManager(tempDir));
    expect(km2.listKeys()[0]!.priority).toBe(3);
  });
});

describe("Key day scheduling", () => {
  test("new keys default to all days [0,1,2,3,4,5,6]", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "test");
    const listed = km.listKeys();
    expect(listed[0]!.allowedDays).toEqual([0, 1, 2, 3, 4, 5, 6]);
  });

  test("updateKeyAllowedDays changes days in memory and persists to DB", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "test");
    const updated = km.updateKeyAllowedDays(VALID_KEY_1, [1, 2, 3, 4, 5]);
    expect(updated).toBe(true);
    expect(km.listKeys()[0]!.allowedDays).toEqual([1, 2, 3, 4, 5]);

    // Reload from DB
    km.close();
    const km2 = create();
    expect(km2.listKeys()[0]!.allowedDays).toEqual([1, 2, 3, 4, 5]);
    km2.close();
  });

  test("updateKeyAllowedDays returns false for unknown key", () => {
    const km = create();
    expect(km.updateKeyAllowedDays("sk-ant-api03-unknown", [1, 2])).toBe(false);
  });

  test("updateKeyAllowedDaysByMask works via masked key", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "test");
    const masked = km.listKeys()[0]!.maskedKey;
    const updated = km.updateKeyAllowedDaysByMask(masked, [0, 6]);
    expect(updated).toBe(true);
    expect(km.listKeys()[0]!.allowedDays).toEqual([0, 6]);
  });

  test("getNextAvailableKey() skips keys not allowed on current day", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "restricted");
    km.addKey(VALID_KEY_2, "all-days");

    // Set key 1 to a day that is NOT today
    const today = new Date().getDay();
    const notToday = (today + 1) % 7;
    km.updateKeyAllowedDays(VALID_KEY_1, [notToday]);

    const next = km.getNextAvailableKey();
    expect(next).not.toBeNull();
    expect(next!.key).toBe(VALID_KEY_2);
  });

  test("getNextAvailableKey() returns null when all keys are day-restricted", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "restricted");

    const today = new Date().getDay();
    const notToday = (today + 1) % 7;
    km.updateKeyAllowedDays(VALID_KEY_1, [notToday]);

    expect(km.getNextAvailableKey()).toBeNull();
  });

  test("availableCount() excludes day-restricted keys", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "restricted");
    km.addKey(VALID_KEY_2, "all-days");

    expect(km.availableCount()).toBe(2);

    const today = new Date().getDay();
    const notToday = (today + 1) % 7;
    km.updateKeyAllowedDays(VALID_KEY_1, [notToday]);

    expect(km.availableCount()).toBe(1);
  });

  test("listKeys() returns allowedDays and correct isAvailable for day-restricted keys", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "restricted");

    const today = new Date().getDay();
    const notToday = (today + 1) % 7;
    km.updateKeyAllowedDays(VALID_KEY_1, [notToday]);

    const listed = km.listKeys();
    expect(listed[0]!.allowedDays).toEqual([notToday]);
    expect(listed[0]!.isAvailable).toBe(false);
  });

  test("allowedDays persists across reload", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "test");
    km.updateKeyAllowedDays(VALID_KEY_1, [1, 3, 5]);
    km.close();

    const km2 = create();
    expect(km2.listKeys()[0]!.allowedDays).toEqual([1, 3, 5]);
    km2.close();
  });

  test("getEarliestAvailableAt() returns midnight when all keys are day-restricted", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "restricted");

    const today = new Date().getDay();
    const notToday = (today + 1) % 7;
    km.updateKeyAllowedDays(VALID_KEY_1, [notToday]);

    const earliest = km.getEarliestAvailableAt();
    const nowMs = Date.now();
    // Should be in the future (midnight)
    expect(earliest).toBeGreaterThan(nowMs);
    // Should be at most ~24 hours from now
    expect(earliest - nowMs).toBeLessThanOrEqual(24 * 60 * 60 * 1000);
  });
});

describe("Token label updates", () => {
  test("updateTokenLabel changes the label in memory and DB", () => {
    const km = create();
    km.addToken(VALID_TOKEN_1, "old-label");
    const result = km.updateTokenLabel(VALID_TOKEN_1, "new-label");
    expect(result).toBe(true);
    const listed = km.listTokens();
    expect(listed[0]!.label).toBe("new-label");
    // Verify persisted
    const km2 = trackManager(new KeyManager(tempDir));
    expect(km2.listTokens()[0]!.label).toBe("new-label");
  });

  test("updateTokenLabel returns false for unknown token", () => {
    const km = create();
    expect(km.updateTokenLabel("nonexistent-token-value", "label")).toBe(false);
  });

  test("updateTokenLabel preserves stats", () => {
    const km = create();
    const entry = km.addToken(VALID_TOKEN_1, "orig");
    km.recordTokenRequest(entry);
    km.recordTokenSuccess(entry, 200, 100);
    km.updateTokenLabel(VALID_TOKEN_1, "renamed");
    const listed = km.listTokens();
    expect(listed[0]!.label).toBe("renamed");
    expect(listed[0]!.stats.totalRequests).toBe(1);
    expect(listed[0]!.stats.totalTokensIn).toBe(200);
  });

  test("updateTokenLabel is reflected in validateToken", () => {
    const km = create();
    km.addToken(VALID_TOKEN_1, "before");
    km.updateTokenLabel(VALID_TOKEN_1, "after");
    const entry = km.validateToken(VALID_TOKEN_1);
    expect(entry).not.toBeNull();
    expect(entry!.label).toBe("after");
  });
});

// ── Persistence ─────────────────────────────────────────────────────────────

describe("Persistence", () => {
  test("stats survive save/reload cycle", async () => {
    const km1 = create();
    const keyEntry = km1.addKey(VALID_KEY_1, "persist-key");
    const tokenEntry = km1.addToken(VALID_TOKEN_1, "persist-token");

    // Record various stats
    km1.recordRequest(keyEntry);
    km1.recordRequest(keyEntry);
    km1.recordSuccess(keyEntry, 100, 200);
    km1.recordRateLimit(keyEntry, 10);
    km1.recordError(keyEntry);

    km1.recordTokenRequest(tokenEntry);
    km1.recordTokenSuccess(tokenEntry, 50, 75);
    km1.recordTokenError(tokenEntry);

    // Wait for debounced save to fire
    await waitForSave();

    // Create a new KeyManager pointing to the same directory
    const km2 = create();

    // Verify key stats persisted
    const keys = km2.listKeys();
    expect(keys.length).toBe(1);
    const ks = keys[0]!.stats;
    expect(ks.totalRequests).toBe(2);
    expect(ks.successfulRequests).toBe(1);
    expect(ks.rateLimitHits).toBe(1);
    expect(ks.errors).toBe(1);
    expect(ks.totalTokensIn).toBe(100);
    expect(ks.totalTokensOut).toBe(200);
    expect(ks.lastUsedAt).not.toBeNull();

    // Verify token stats persisted
    const tokens = km2.listTokens();
    expect(tokens.length).toBe(1);
    const ts = tokens[0]!.stats;
    expect(ts.totalRequests).toBe(1);
    expect(ts.successfulRequests).toBe(1);
    expect(ts.errors).toBe(1);
    expect(ts.totalTokensIn).toBe(50);
    expect(ts.totalTokensOut).toBe(75);
  });

  test("CRUD operations persist immediately (add key, new KeyManager, key still there)", () => {
    const km1 = create();
    km1.addKey(VALID_KEY_1, "immediate");

    // No waiting -- addKey writes to DB synchronously via db.run()
    const km2 = create();
    const keys = km2.listKeys();
    expect(keys.length).toBe(1);
    expect(keys[0]!.label).toBe("immediate");
  });

  test("CRUD operations persist immediately (add token, new KeyManager, token still there)", () => {
    const km1 = create();
    km1.addToken(VALID_TOKEN_1, "imm-tok");

    const km2 = create();
    const tokens = km2.listTokens();
    expect(tokens.length).toBe(1);
    expect(tokens[0]!.label).toBe("imm-tok");
  });

  test("CRUD operations persist immediately (remove key, new KeyManager, key gone)", () => {
    const km1 = create();
    km1.addKey(VALID_KEY_1, "doomed");
    km1.removeKey(VALID_KEY_1);

    const km2 = create();
    expect(km2.totalCount()).toBe(0);
  });

  test("debounced save fires after ~1s", async () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "debounce-test");

    km.recordRequest(entry);
    km.recordSuccess(entry, 10, 20);

    // Immediately check DB -- stats should still be zero because save is debounced
    const dbImmediate = new Database(join(tempDir, "state.db"), {
      readonly: true,
    });
    const rowBefore = dbImmediate
      .query("SELECT total_requests FROM api_keys WHERE key = ?")
      .get(VALID_KEY_1) as { total_requests: number };
    expect(rowBefore.total_requests).toBe(0);
    dbImmediate.close();

    // Wait for the debounced save
    await waitForSave();

    // Now the DB should have the updated stats
    const dbAfter = new Database(join(tempDir, "state.db"), {
      readonly: true,
    });
    const rowAfter = dbAfter
      .query(
        "SELECT total_requests, successful_requests, total_tokens_in, total_tokens_out FROM api_keys WHERE key = ?",
      )
      .get(VALID_KEY_1) as {
      total_requests: number;
      successful_requests: number;
      total_tokens_in: number;
      total_tokens_out: number;
    };
    expect(rowAfter.total_requests).toBe(1);
    expect(rowAfter.successful_requests).toBe(1);
    expect(rowAfter.total_tokens_in).toBe(10);
    expect(rowAfter.total_tokens_out).toBe(20);
    dbAfter.close();
  });

  test("multiple rapid stat updates are batched into one save", async () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "batch-test");

    // Fire many stat updates in quick succession
    for (let i = 0; i < 10; i++) {
      km.recordRequest(entry);
    }
    km.recordSuccess(entry, 500, 1000);
    km.recordError(entry);
    km.recordRateLimit(entry, 5);

    // Wait for the single debounced save
    await waitForSave();

    // All updates should be persisted in one batch
    const db = new Database(join(tempDir, "state.db"), { readonly: true });
    const row = db
      .query(
        "SELECT total_requests, successful_requests, errors, rate_limit_hits, total_tokens_in, total_tokens_out FROM api_keys WHERE key = ?",
      )
      .get(VALID_KEY_1) as {
      total_requests: number;
      successful_requests: number;
      errors: number;
      rate_limit_hits: number;
      total_tokens_in: number;
      total_tokens_out: number;
    };
    expect(row.total_requests).toBe(10);
    expect(row.successful_requests).toBe(1);
    expect(row.errors).toBe(1);
    expect(row.rate_limit_hits).toBe(1);
    expect(row.total_tokens_in).toBe(500);
    expect(row.total_tokens_out).toBe(1000);
    db.close();
  });

  test("token stats also persist via debounced save", async () => {
    const km = create();
    const entry = km.addToken(VALID_TOKEN_1, "persist-tok-stats");

    km.recordTokenRequest(entry);
    km.recordTokenRequest(entry);
    km.recordTokenSuccess(entry, 25, 50);
    km.recordTokenError(entry);

    await waitForSave();

    const db = new Database(join(tempDir, "state.db"), { readonly: true });
    const row = db
      .query(
        "SELECT total_requests, successful_requests, errors, total_tokens_in, total_tokens_out FROM proxy_tokens WHERE token = ?",
      )
      .get(VALID_TOKEN_1) as {
      total_requests: number;
      successful_requests: number;
      errors: number;
      total_tokens_in: number;
      total_tokens_out: number;
    };
    expect(row.total_requests).toBe(2);
    expect(row.successful_requests).toBe(1);
    expect(row.errors).toBe(1);
    expect(row.total_tokens_in).toBe(25);
    expect(row.total_tokens_out).toBe(50);
    db.close();
  });

  test("availableAt persists across reload", async () => {
    const km1 = create();
    const entry = km1.addKey(VALID_KEY_1, "avail-persist");
    km1.recordRateLimit(entry, 99999);
    const savedAvailableAt = entry.availableAt;

    await waitForSave();

    const km2 = create();
    const keys = km2.listKeys();
    expect(keys.length).toBe(1);
    expect(keys[0]!.availableAt).toBe(savedAvailableAt);
    expect(keys[0]!.isAvailable).toBe(false);
  });
});

// ── Capacity telemetry ───────────────────────────────────────────────────────

describe("Capacity telemetry", () => {
  test("recordCapacityObservation merges sparse updates without wiping prior fields", () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "cap-merge");
    const baseNow = Date.now();

    km.recordCapacityObservation(entry, {
      seenAt: unixMs(baseNow),
      httpStatus: 200,
      organizationId: "org-1",
      representativeClaim: "seven_day",
      windows: [
        {
          windowName: "unified-7d",
          status: "allowed_warning",
          utilization: 0.92,
          resetAt: unixMs(Date.now() + 60_000),
        },
      ],
    });

    km.recordCapacityObservation(entry, {
      seenAt: unixMs(baseNow + 1_000),
      httpStatus: 200,
      requestId: "req-123",
      windows: [
        {
          windowName: "unified-5h",
          status: "allowed",
          utilization: 0.41,
          resetAt: unixMs(Date.now() + 120_000),
        },
      ],
    });

    const key = km.listKeys()[0]!;
    expect(key.capacity.organizationId).toBe("org-1");
    expect(key.capacity.representativeClaim).toBe("seven_day");
    expect(key.capacity.lastRequestId).toBe("req-123");
    expect(key.capacity.responseCount).toBe(2);
    expect(key.capacity.normalizedHeaderCount).toBe(2);
    expect(key.capacity.signalCoverage.find((signal) => signal.signalName === "windows")!.seenCount).toBe(2);
    expect(key.capacity.signalCoverage.find((signal) => signal.signalName === "request_id")!.seenCount).toBe(1);
    expect(key.capacity.windows.map((w) => w.windowName).sort()).toEqual(["unified-5h", "unified-7d"]);
    expect(key.capacity.windows.find((w) => w.windowName === "unified-7d")!.utilization).toBe(0.92);
    expect(key.capacityHealth).toBe("warning");
  });

  test("capacity state persists across reload", async () => {
    const km1 = create();
    const entry = km1.addKey(VALID_KEY_1, "cap-persist");
    const baseNow = Date.now();

    km1.recordCapacityObservation(entry, {
      seenAt: unixMs(baseNow),
      httpStatus: 429,
      organizationId: "org-persist",
      retryAfterSecs: 45,
      overageStatus: "rejected",
      overageDisabledReason: "out_of_credits",
      windows: [
        {
          windowName: "unified",
          status: "rejected",
          utilization: 1,
          resetAt: unixMs(Date.now() + 180_000),
        },
      ],
    });

    await waitForSave();

    const km2 = create();
    const key = km2.listKeys()[0]!;
    expect(key.capacity.responseCount).toBe(1);
    expect(key.capacity.normalizedHeaderCount).toBe(1);
    expect(key.capacity.organizationId).toBe("org-persist");
    expect(key.capacity.retryAfterSecs).toBe(45);
    expect(key.capacity.overageStatus).toBe("rejected");
    expect(key.capacity.windows[0]!.windowName).toBe("unified");
    expect(key.capacity.windows[0]!.status).toBe("rejected");
  });

  test("legacy overage window telemetry is ignored for primary capacity health", () => {
    const km = create();
    const healthy = km.addKey(VALID_KEY_1, "healthy");
    const warning = km.addKey(VALID_KEY_2, "warning");
    const observedRejected = km.addKey(VALID_KEY_3, "observed-rejected");
    const baseNow = Date.now();

    km.recordCapacityObservation(healthy, {
      seenAt: unixMs(baseNow),
      httpStatus: 200,
      organizationId: "org-a",
      windows: [{ windowName: "unified-5h", status: "allowed", utilization: 0.25, resetAt: unixMs(Date.now() + 60_000) }],
    });
    km.recordCapacityObservation(warning, {
      seenAt: unixMs(baseNow + 100),
      httpStatus: 200,
      organizationId: "org-a",
      fallbackAvailable: true,
      windows: [{ windowName: "unified-5h", status: "allowed_warning", utilization: 0.86, resetAt: unixMs(Date.now() + 60_000) }],
    });
    km.recordCapacityObservation(observedRejected, {
      seenAt: unixMs(baseNow + 200),
      httpStatus: 200,
      organizationId: "org-b",
      overageStatus: "rejected",
      windows: [
        { windowName: "unified-5h", status: "allowed", utilization: 0.18, resetAt: unixMs(Date.now() + 60_000) },
        { windowName: "unified-overage", status: "rejected", utilization: null, resetAt: null, lastSeenAt: unixMs(baseNow + 200) },
      ],
    });

    const summary = km.getCapacitySummary();
    const observed = km.listKeys().find((key) => key.label === "observed-rejected");
    expect(observed).toBeDefined();
    expect(observed!.isAvailable).toBe(true);
    expect(observed!.capacity.windows.find((window) => window.windowName === "unified-overage")).toBeUndefined();
    expect(observed!.capacityHealth).toBe("healthy");
    expect(summary.healthyKeys).toBe(2);
    expect(summary.warningKeys).toBe(1);
    expect(summary.rejectedKeys).toBe(0);
    expect(summary.coolingDownKeys).toBe(0);
    expect(summary.fallbackAvailableKeys).toBe(1);
    expect(summary.overageRejectedKeys).toBe(1);
    expect(summary.distinctOrganizations).toBe(2);
    expect(summary.windows[0]!.windowName).toBe("unified-5h");
    expect(summary.windows[0]!.warningKeys).toBe(1);
    expect(summary.windows[0]!.rejectedKeys).toBe(0);
  });

  test("stored threshold-only warning statuses self-heal to healthy on reload", async () => {
    const km1 = create();
    const entry = km1.addKey(VALID_KEY_1, "legacy-threshold-warning");
    const baseNow = Date.now();

    km1.recordCapacityObservation(entry, {
      seenAt: unixMs(baseNow),
      httpStatus: 200,
      organizationId: "org-threshold-heal",
      windows: [
        {
          windowName: "unified",
          status: "allowed_warning",
          utilization: null,
          resetAt: unixMs(baseNow + 26 * 60_000),
          surpassedThreshold: null,
        },
        {
          windowName: "unified-5h",
          status: "allowed",
          utilization: 0.14,
          resetAt: unixMs(baseNow + 26 * 60_000),
          surpassedThreshold: 0.9,
        },
        {
          windowName: "unified-7d",
          status: "allowed_warning",
          utilization: 0.25,
          resetAt: unixMs(baseNow + 6 * 24 * 60 * 60_000 + 7 * 60 * 60_000),
          surpassedThreshold: 0.75,
        },
      ],
    });

    await waitForSave();

    const km2 = create();
    const key = km2.listKeys().find((candidate) => candidate.label === "legacy-threshold-warning");
    expect(key).toBeDefined();
    expect(key!.capacityHealth).toBe("healthy");
    expect(key!.capacity.windows.find((window) => window.windowName === "unified")!.status).toBe("allowed");
    expect(key!.capacity.windows.find((window) => window.windowName === "unified-5h")!.status).toBe("allowed");
    expect(key!.capacity.windows.find((window) => window.windowName === "unified-7d")!.status).toBe("allowed");
  });

  test("observations remain valid until their resetAt passes, regardless of how old the observation is", () => {
    // A 7-day window observed a week ago is still informative for the first
    // 7 days — the resetAt is the canonical expiry, not lastSeenAt. Earlier
    // behavior discarded anything not seen in the last 6 hours, which threw
    // away 7d telemetry prematurely.
    const km = create();
    const originalNow = Date.now;
    const fakeNow = 1_000_000_000;
    Date.now = () => fakeNow;
    try {
      const entry = km.addKey(VALID_KEY_1, "observed-a-while-ago");
      km.recordCapacityObservation(entry, {
        seenAt: unixMs(fakeNow - (7 * 60 * 60 * 1000)), // 7h ago
        httpStatus: 200,
        windows: [
          {
            windowName: "unified-7d",
            status: "allowed_warning",
            utilization: 1,
            resetAt: unixMs(fakeNow + (7 * 24 * 60 * 60 * 1000)), // 7d ahead
          },
        ],
      });

      const key = km.listKeys()[0]!;
      const summary = km.getCapacitySummary();
      const sevenD = key.capacity.windows.find((w) => w.windowName === "unified-7d");
      expect(sevenD).toBeDefined();
      expect(sevenD!.utilization).toBe(1);
      expect(key.capacityHealth).toBe("warning");
      expect(summary.warningKeys).toBe(1);
      expect(summary.windows.some((w) => w.windowName === "unified-7d")).toBe(true);
    } finally {
      Date.now = originalNow;
    }
  });

  test("windows whose resetAt has passed are hidden from listKeys and pruned on the periodic sweep", () => {
    // Observe a 7d window with resetAt in the future. Then advance the clock
    // past the resetAt without any further observation — which is what
    // happens when a key sits idle through its reset. The zombie state in
    // memory should not leak into listKeys (sanitize hides it) and the
    // periodic prune should drop it entirely.
    const km = create();
    const originalNow = Date.now;
    let fakeNow = 1_000_000_000;
    Date.now = () => fakeNow;
    try {
      const entry = km.addKey(VALID_KEY_1, "past-reset");
      km.recordCapacityObservation(entry, {
        seenAt: unixMs(fakeNow - (60 * 1000)),
        httpStatus: 200,
        windows: [
          {
            windowName: "unified-7d",
            status: "allowed",
            utilization: 0.5,
            resetAt: unixMs(fakeNow + (60 * 60 * 1000)), // 1h in future at observation time
          },
        ],
      });

      // Before reset — present in listKeys.
      expect(km.listKeys()[0]!.capacity.windows.some((w) => w.windowName === "unified-7d")).toBe(true);

      // Advance the clock past the resetAt.
      fakeNow += 2 * 60 * 60 * 1000;

      // listKeys now hides the past-reset window via sanitize.
      const masked = km.listKeys()[0]!;
      expect(masked.capacity.windows.some((w) => w.windowName === "unified-7d")).toBe(false);

      // Raw in-memory state still holds the zombie until prune.
      const raw = (km as unknown as { keys: Array<{ capacity: { windows: Array<{ windowName: string }> } }> }).keys;
      expect(raw[0]!.capacity.windows.some((w) => w.windowName === "unified-7d")).toBe(true);

      // Prune removes it.
      (km as unknown as { prunePastResetCapacityWindows: () => void }).prunePastResetCapacityWindows();
      expect(raw[0]!.capacity.windows.some((w) => w.windowName === "unified-7d")).toBe(false);
    } finally {
      Date.now = originalNow;
    }
  });

  test("queryCapacityTimeseries returns per-window rollups", async () => {
    const km = create();
    const entry = km.addKey(VALID_KEY_1, "cap-ts");

    km.recordCapacityObservation(entry, {
      seenAt: unixMs(3_000),
      httpStatus: 200,
      windows: [{ windowName: "unified", status: "allowed_warning", utilization: 0.77, resetAt: unixMs(Date.now() + 60_000) }],
    });
    km.recordCapacityObservation(entry, {
      seenAt: unixMs(4_000),
      httpStatus: 200,
      windows: [{ windowName: "unified", status: "allowed", utilization: 0.55, resetAt: unixMs(Date.now() + 60_000) }],
    });

    await waitForSave();

    const buckets = km.queryCapacityTimeseries({ hours: 24, resolution: "hour", keyLabel: "cap-ts" });
    expect(buckets.length).toBe(1);
    expect(buckets[0]!.windowName).toBe("unified");
    expect(buckets[0]!.samples).toBe(2);
    expect(buckets[0]!.warning).toBe(1);
    expect(buckets[0]!.allowed).toBe(1);
    expect(buckets[0]!.maxUtilization).toBe(0.77);
  });

  test("getNextAvailableKey ignores capacity analytics and keeps the original sticky selection logic", () => {
    const km = create();
    const warning = km.addKey(VALID_KEY_1, "warning-first");
    const healthy = km.addKey(VALID_KEY_2, "healthy-second");

    km.recordCapacityObservation(warning, {
      seenAt: unixMs(10_000),
      httpStatus: 200,
      windows: [{ windowName: "unified", status: "allowed_warning", utilization: 0.88, resetAt: unixMs(Date.now() + 60_000) }],
    });
    km.recordCapacityObservation(healthy, {
      seenAt: unixMs(10_500),
      httpStatus: 200,
      windows: [{ windowName: "unified", status: "allowed", utilization: 0.2, resetAt: unixMs(Date.now() + 60_000) }],
    });

    km.recordRequest(warning);

    expect(km.getNextAvailableKey()!.label).toBe(warning.label);
  });
});

// ── close() ────────────────────────────────────────────────────────────────────

describe("close()", () => {
  test("flushes data and persists across close/reopen", () => {
    const km1 = create();
    km1.addKey(VALID_KEY_1, "close-test");
    km1.close();

    const km2 = create();
    const keys = km2.listKeys();
    expect(keys).toHaveLength(1);
    expect(keys[0]!.label).toBe("close-test");
  });
});

// ── Tier-reserve routing ────────────────────────────────────────────────────

describe("tier-reserve routing (headroom floors)", () => {
  // Helper: set a key's capacity to a specific 5h and 7d utilization, with
  // resets both FIVE_H/2 and SEVEN_D/2 in the future — well clear of the
  // near-reset threshold so utilization counts.
  const FIVE_H = 5 * 60 * 60 * 1000;
  const SEVEN_D = 7 * 24 * 60 * 60 * 1000;

  function setUtilization(km: KeyManager, rawKey: string, util5h: number, util7d: number) {
    const entry = km.listKeys().find((k) => k.label !== undefined);
    const key = km.listKeys().find((k) => k.maskedKey === mask(rawKey))!;
    void entry; void key;
    // Resolve the actual ApiKeyEntry via the internal keys array by label lookup
    // (tests outside the class don't have direct access). Use a capacity
    // observation through the public API instead.
    const label = km.listKeys().find((k) => k.maskedKey.length > 0)?.label;
    void label;
    const allKeys = (km as unknown as { keys: Array<{ key: string; label: string } & Record<string, unknown>> }).keys;
    const internalEntry = allKeys.find((e) => e.key === rawKey)!;
    km.recordCapacityObservation(internalEntry as never, {
      seenAt: unixMs(Date.now()),
      httpStatus: 200,
      windows: [
        { windowName: "unified-5h", status: "allowed", utilization: util5h, resetAt: unixMs(Date.now() + FIVE_H / 2) },
        { windowName: "unified-7d", status: "allowed", utilization: util7d, resetAt: unixMs(Date.now() + SEVEN_D / 2) },
      ],
    });
  }

  function mask(key: string): string {
    // Mirrors maskKey logic in key-manager; tests only use this for lookup.
    return `${key.slice(0, 14)}...${key.slice(-4)}`;
  }

  test("tier 1 with 0% utilization wins over tier 2 and 3 (baseline)", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "preferred");
    km.addKey(VALID_KEY_2, "normal");
    km.addKey(VALID_KEY_3, "fallback");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);
    km.updateKeyPriority(VALID_KEY_3, 3);

    const pick = km.getKeyForConversation(null);
    expect(pick.priorityTier).toBe(1);
    expect(pick.entry!.label).toBe("preferred");
    expect(pick.spilledFromTier).toBeNull();
  });

  test("tier 1 over 30% util still wins — no reserve on tier 1", () => {
    // Tier 1 has no floor, so even at 95% utilization it's preferred over
    // tier 2. The reserve applies only to 2 and 3.
    const km = create();
    km.addKey(VALID_KEY_1, "preferred-hot");
    km.addKey(VALID_KEY_2, "normal-cold");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);
    setUtilization(km, VALID_KEY_1, 0.95, 0.8);
    setUtilization(km, VALID_KEY_2, 0.05, 0.05);

    const pick = km.getKeyForConversation(null);
    expect(pick.priorityTier).toBe(1);
    expect(pick.entry!.label).toBe("preferred-hot");
  });

  test("tier 2 gated out by 30% floor → spill to tier 3 if tier 3 has ≥50% headroom", () => {
    // Tier 1 missing. Tier 2 at 75% util (headroom 25%, below 30% floor).
    // Tier 3 at 30% util (headroom 70%, above 50% floor). Tier 3 wins.
    // Use a conversationKey so the richer selection path runs and populates
    // spilledFromTier (non-conversation requests also spill correctly, but
    // only the conversation path reports the telemetry).
    const km = create();
    km.addKey(VALID_KEY_2, "normal-used");
    km.addKey(VALID_KEY_3, "fallback-fresh");
    km.updateKeyPriority(VALID_KEY_2, 2);
    km.updateKeyPriority(VALID_KEY_3, 3);
    setUtilization(km, VALID_KEY_2, 0.75, 0.20);
    setUtilization(km, VALID_KEY_3, 0.30, 0.20);

    const pick = km.getKeyForConversation("user-spill:session-a");
    expect(pick.entry!.label).toBe("fallback-fresh");
    expect(pick.priorityTier).toBe(3);
    expect(pick.spilledFromTier).toBe(2);
  });

  test("fallback with 60% utilization is GATED OUT (50% headroom floor)", () => {
    // Only a tier-3 key exists, at 60% util (headroom 40%, below 50% floor).
    // Tier 3 is the only tier present, so it goes through fall-through logic
    // and still gets selected — reserves are a planning signal, not a gate
    // when there's no alternative.
    const km = create();
    km.addKey(VALID_KEY_3, "fallback-below-reserve");
    km.updateKeyPriority(VALID_KEY_3, 3);
    setUtilization(km, VALID_KEY_3, 0.60, 0.40);

    const pick = km.getKeyForConversation(null);
    expect(pick.entry!.label).toBe("fallback-below-reserve");
    expect(pick.priorityTier).toBe(3);
    // spilledFromTier is null because we didn't shift away from a higher tier —
    // we fell through the reserve check because every tier was below its floor.
    expect(pick.spilledFromTier).toBeNull();
  });

  test("existing invariant: preferred tier keys never yield to lower tiers when eligible", () => {
    // This is the regression-lock against the old "within tier" invariant.
    // Tier 1 at moderate util (eligible — no floor on tier 1), tier 3 with
    // pristine headroom. Tier 1 still wins.
    const km = create();
    km.addKey(VALID_KEY_1, "preferred-moderate");
    km.addKey(VALID_KEY_3, "fallback-pristine");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_3, 3);
    setUtilization(km, VALID_KEY_1, 0.50, 0.40);
    setUtilization(km, VALID_KEY_3, 0.00, 0.00);

    const pick = km.getKeyForConversation(null);
    expect(pick.priorityTier).toBe(1);
    expect(pick.entry!.label).toBe("preferred-moderate");
    expect(pick.spilledFromTier).toBeNull();
  });

  test("windows near reset (>=95% elapsed) don't count against headroom", () => {
    // A key at 90% utilization with 1 minute until reset isn't "nearly out" —
    // it's about to refresh. Near-reset windows should be ignored for the
    // headroom check, so such a key stays eligible.
    const km = create();
    km.addKey(VALID_KEY_2, "normal-about-to-reset");
    km.updateKeyPriority(VALID_KEY_2, 2);
    km.recordCapacityObservation(
      (km as unknown as { keys: Array<{ key: string } & Record<string, unknown>> })
        .keys.find((e) => e.key === VALID_KEY_2)! as never,
      {
        seenAt: unixMs(Date.now()),
        httpStatus: 200,
        windows: [
          { windowName: "unified-5h", status: "allowed", utilization: 0.90, resetAt: unixMs(Date.now() + 60_000) },
          { windowName: "unified-7d", status: "allowed", utilization: 0.05, resetAt: unixMs(Date.now() + SEVEN_D / 2) },
        ],
      },
    );

    const pick = km.getKeyForConversation(null);
    // Without the near-reset bypass, the key at 90% util would fail the 30%
    // floor. With it, only the 7d window (5% util) counts — eligible.
    expect(pick.priorityTier).toBe(2);
  });

  test("requestsByTier counter increments on each routed request", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "pref");
    km.addKey(VALID_KEY_2, "norm");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);

    expect(km.getRequestsByTier()).toEqual({});

    const first = km.getKeyForConversation(null);
    km.recordRequest(first.entry!);
    expect(km.getRequestsByTier()).toEqual({ "1": 1 });

    const second = km.getKeyForConversation(null);
    km.recordRequest(second.entry!);
    expect(km.getRequestsByTier()).toEqual({ "1": 2 });
  });
});

// ── Layer 2 pace-aware spreading ───────────────────────────────────────────

describe("spreadProbabilityFromPeak (pure function)", () => {
  test("below 85% peak → no spread", () => {
    expect(spreadProbabilityFromPeak(0.0)).toBe(0);
    expect(spreadProbabilityFromPeak(0.5)).toBe(0);
    expect(spreadProbabilityFromPeak(0.849)).toBe(0);
  });

  test("85-95% peak → 10% spread", () => {
    expect(spreadProbabilityFromPeak(0.85)).toBe(0.10);
    expect(spreadProbabilityFromPeak(0.90)).toBe(0.10);
    expect(spreadProbabilityFromPeak(0.949)).toBe(0.10);
  });

  test("95-100% peak → 30% spread", () => {
    expect(spreadProbabilityFromPeak(0.95)).toBe(0.30);
    expect(spreadProbabilityFromPeak(0.99)).toBe(0.30);
  });

  test("100-120% peak → 50% spread", () => {
    expect(spreadProbabilityFromPeak(1.0)).toBe(0.50);
    expect(spreadProbabilityFromPeak(1.1)).toBe(0.50);
    expect(spreadProbabilityFromPeak(1.19)).toBe(0.50);
  });

  test(">=120% peak → 60% spread (capped)", () => {
    expect(spreadProbabilityFromPeak(1.2)).toBe(0.60);
    expect(spreadProbabilityFromPeak(1.5)).toBe(0.60);
    expect(spreadProbabilityFromPeak(10)).toBe(0.60);
  });

  test("never exceeds 0.60 — preferred tier always retains the majority", () => {
    for (let p = 0; p <= 5; p += 0.1) {
      expect(spreadProbabilityFromPeak(p)).toBeLessThanOrEqual(0.60);
    }
  });
});

describe("hashConversationKeyToUnit (pure function)", () => {
  test("returns [0, 1)", () => {
    for (const key of ["a", "alice:session", "user-42", "", "long-session-id-with-dashes-and-uuid-1234"]) {
      const h = hashConversationKeyToUnit(key);
      expect(h).toBeGreaterThanOrEqual(0);
      expect(h).toBeLessThan(1);
    }
  });

  test("deterministic: same input → same output", () => {
    const key = "user-alpha:session-beta";
    expect(hashConversationKeyToUnit(key)).toBe(hashConversationKeyToUnit(key));
  });

  test("different inputs produce different outputs (reasonably uniform)", () => {
    const samples = new Set<number>();
    for (let i = 0; i < 100; i++) {
      samples.add(hashConversationKeyToUnit(`user-${i}:session-${i}`));
    }
    // Very loose uniformity check — 100 distinct inputs shouldn't collide much.
    expect(samples.size).toBeGreaterThan(95);
  });

  test("distribution is roughly uniform across 10 buckets for 10000 keys", () => {
    const buckets = new Array(10).fill(0);
    for (let i = 0; i < 10000; i++) {
      const h = hashConversationKeyToUnit(`conv-${i}:sess-${i * 7 + 11}`);
      buckets[Math.floor(h * 10)]++;
    }
    // Each bucket should hold roughly 1000; allow ±250 before we call it uneven.
    for (const count of buckets) {
      expect(count).toBeGreaterThan(750);
      expect(count).toBeLessThan(1250);
    }
  });
});

describe("tierProjectedPeak (Layer 2 projection)", () => {
  const FIVE_H_MS = 5 * 60 * 60 * 1000;
  const SEVEN_D_MS = 7 * 24 * 60 * 60 * 1000;

  function addKeyWithCapacity(
    km: KeyManager,
    rawKey: string,
    label: string,
    priority: number,
    windows: Array<{ windowName: string; util: number; resetIn: number }>,
  ): void {
    km.addKey(rawKey, label);
    km.updateKeyPriority(rawKey, priority);
    const entry = (km as unknown as { keys: Array<{ key: string } & Record<string, unknown>> })
      .keys.find((e) => e.key === rawKey)!;
    const nowMs = Date.now();
    km.recordCapacityObservation(entry as never, {
      seenAt: unixMs(nowMs),
      httpStatus: 200,
      windows: windows.map((w) => ({
        windowName: w.windowName,
        status: "allowed",
        utilization: w.util,
        resetAt: unixMs(nowMs + w.resetIn),
      })),
    });
  }

  function getPeak(km: KeyManager, tier: number): number | null {
    return (km as unknown as {
      _testTierProjectedPeak: (t: number, n: number) => number | null;
    })._testTierProjectedPeak(tier, Date.now());
  }

  test("no keys in tier → null", () => {
    const km = create();
    expect(getPeak(km, 1)).toBeNull();
  });

  test("key with no observed windows → null (no data to project)", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "fresh");
    km.updateKeyPriority(VALID_KEY_1, 1);
    expect(getPeak(km, 1)).toBeNull();
  });

  test("on-pace key (util ~= elapsed) → peak near 1.0", () => {
    const km = create();
    // util 0.60 at 60% elapsed (resetIn = 40% of window remaining)
    addKeyWithCapacity(km, VALID_KEY_1, "on-pace", 1, [
      { windowName: "unified-5h", util: 0.60, resetIn: FIVE_H_MS * 0.40 },
    ]);
    const peak = getPeak(km, 1);
    expect(peak).not.toBeNull();
    expect(peak!).toBeCloseTo(1.0, 1);
  });

  test("ahead-of-pace key → peak > 1.0 (projected to bust budget, below cap)", () => {
    const km = create();
    // util 0.45 at 40% elapsed (remaining 60%) → pace 1.125 → projected 1.125
    // (kept below the 1.5 cap so we're testing raw projection, not clamping)
    addKeyWithCapacity(km, VALID_KEY_1, "hot", 1, [
      { windowName: "unified-5h", util: 0.45, resetIn: FIVE_H_MS * 0.60 },
    ]);
    const peak = getPeak(km, 1);
    expect(peak).not.toBeNull();
    expect(peak!).toBeGreaterThan(1.0);
    expect(peak!).toBeLessThan(1.5);
  });

  test("projection capped at LAYER2_PROJECTION_CAP (1.5)", () => {
    const km = create();
    // Extreme pace: util 0.5 at 10% elapsed → pace 5 → projects to 5.0, capped to 1.5
    addKeyWithCapacity(km, VALID_KEY_1, "extreme", 1, [
      { windowName: "unified-5h", util: 0.5, resetIn: FIVE_H_MS * 0.90 },
    ]);
    expect(getPeak(km, 1)!).toBe(1.5);
  });

  test("dead-zone skip: util 0.05 at 1% elapsed → ignored (no peak)", () => {
    const km = create();
    addKeyWithCapacity(km, VALID_KEY_1, "early", 1, [
      { windowName: "unified-5h", util: 0.05, resetIn: FIVE_H_MS * 0.99 },
    ]);
    expect(getPeak(km, 1)).toBeNull();
  });

  test("picks the MAX across keys in tier", () => {
    const km = create();
    addKeyWithCapacity(km, VALID_KEY_1, "chill", 1, [
      { windowName: "unified-5h", util: 0.20, resetIn: FIVE_H_MS * 0.40 }, // pace 0.33
    ]);
    addKeyWithCapacity(km, VALID_KEY_2, "hot", 1, [
      { windowName: "unified-5h", util: 0.80, resetIn: FIVE_H_MS * 0.60 }, // pace 2.0
    ]);
    const peak = getPeak(km, 1);
    // Capped at 1.5 (from the hot key, pace 2.0)
    expect(peak).toBe(1.5);
  });

  test("considers both 5h and 7d windows — picks whichever projects higher", () => {
    const km = create();
    addKeyWithCapacity(km, VALID_KEY_1, "mixed", 1, [
      { windowName: "unified-5h", util: 0.10, resetIn: FIVE_H_MS * 0.90 }, // pace 1.0
      { windowName: "unified-7d", util: 0.80, resetIn: SEVEN_D_MS * 0.20 }, // pace 1.0
    ]);
    const peak = getPeak(km, 1)!;
    expect(peak).toBeCloseTo(1.0, 1);
  });

  test("ignores keys at other tiers", () => {
    const km = create();
    addKeyWithCapacity(km, VALID_KEY_1, "tier-1-chill", 1, [
      { windowName: "unified-5h", util: 0.10, resetIn: FIVE_H_MS * 0.40 }, // pace 0.17
    ]);
    addKeyWithCapacity(km, VALID_KEY_2, "tier-2-hot", 2, [
      { windowName: "unified-5h", util: 0.90, resetIn: FIVE_H_MS * 0.60 }, // pace 2.25
    ]);
    // Tier 1 has only the chill key — peak low.
    const tier1Peak = getPeak(km, 1)!;
    expect(tier1Peak).toBeLessThan(0.3);
    // Tier 2 has only the hot key — peak high.
    const tier2Peak = getPeak(km, 2)!;
    expect(tier2Peak).toBeGreaterThan(1.0);
  });

  test("ignores keys in cooldown", () => {
    const km = create();
    const hot = addKeyWithCapacity(km, VALID_KEY_1, "cooling", 1, [
      { windowName: "unified-5h", util: 0.90, resetIn: FIVE_H_MS * 0.60 },
    ]);
    void hot;
    const rawEntry = (km as unknown as { keys: Array<{ key: string; availableAt: number }> })
      .keys.find((e) => e.key === VALID_KEY_1)!;
    rawEntry.availableAt = Date.now() + 60000; // 1min cooldown
    expect(getPeak(km, 1)).toBeNull();
  });
});

describe("shouldSpread decision (deterministic per conversation)", () => {
  function check(km: KeyManager, key: string | null, prob: number): boolean {
    return (km as unknown as {
      _testShouldSpread: (k: string | null, p: number) => boolean;
    })._testShouldSpread(key, prob);
  }

  test("probability 0 → always false", () => {
    const km = create();
    expect(check(km, "any", 0)).toBe(false);
    expect(check(km, null, 0)).toBe(false);
  });

  test("probability 1 → always true", () => {
    const km = create();
    expect(check(km, "any", 1)).toBe(true);
    expect(check(km, null, 1)).toBe(true);
  });

  test("same conversation → consistent answer across many calls", () => {
    const km = create();
    const key = "user-42:session-abc";
    const first = check(km, key, 0.30);
    for (let i = 0; i < 100; i++) {
      expect(check(km, key, 0.30)).toBe(first);
    }
  });

  test("hashed distribution tracks probability (conversation path)", () => {
    const km = create();
    let trueCount = 0;
    const n = 5000;
    for (let i = 0; i < n; i++) {
      if (check(km, `conv-${i}:sess-${i * 3 + 7}`, 0.30)) trueCount++;
    }
    const ratio = trueCount / n;
    // 30% probability over a 5000-sample hash should land within ±3%.
    expect(ratio).toBeGreaterThan(0.27);
    expect(ratio).toBeLessThan(0.33);
  });

  test("rng override drives the no-conversationKey path", () => {
    // Always-0.9 rng → never below 0.3 → never true
    const kmAlwaysHigh = new KeyManager(tempDir, { rng: () => 0.9 });
    managers.push(kmAlwaysHigh);
    expect(check(kmAlwaysHigh, null, 0.30)).toBe(false);
    // Always-0.1 rng → always below 0.3 → always true
    const tempDir2 = mkdtempSync(join(tmpdir(), "km-rng-2-"));
    const kmAlwaysLow = new KeyManager(tempDir2, { rng: () => 0.1 });
    managers.push(kmAlwaysLow);
    expect(check(kmAlwaysLow, null, 0.30)).toBe(true);
    rmSync(tempDir2, { recursive: true, force: true });
  });
});

describe("Layer 2 integration — pace-aware tier spreading", () => {
  const FIVE_H_MS = 5 * 60 * 60 * 1000;

  function seedCapacity(
    km: KeyManager,
    rawKey: string,
    windows: Array<{ windowName: string; util: number; resetIn: number }>,
  ): void {
    const entry = (km as unknown as { keys: Array<{ key: string } & Record<string, unknown>> })
      .keys.find((e) => e.key === rawKey)!;
    km.recordCapacityObservation(entry as never, {
      seenAt: unixMs(Date.now()),
      httpStatus: 200,
      windows: windows.map((w) => ({
        windowName: w.windowName,
        status: "allowed",
        utilization: w.util,
        resetAt: unixMs(Date.now() + w.resetIn),
      })),
    });
  }

  test("low-peak tier 1 → no spread, tier 1 wins (regression on Layer 1 behavior)", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "preferred");
    km.addKey(VALID_KEY_2, "normal");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);
    // Tier 1 at 20% util, 40% elapsed → pace 0.5, peak 0.5 → below 0.85 threshold
    seedCapacity(km, VALID_KEY_1, [
      { windowName: "unified-5h", util: 0.20, resetIn: FIVE_H_MS * 0.60 },
    ]);
    seedCapacity(km, VALID_KEY_2, [
      { windowName: "unified-5h", util: 0.10, resetIn: FIVE_H_MS * 0.40 },
    ]);

    for (let i = 0; i < 100; i++) {
      const pick = km.getKeyForConversation(`conv-${i}:sess-${i}`);
      expect(pick.priorityTier).toBe(1);
      expect(pick.spilledFromTier).toBeNull();
    }
  });

  test("high-peak tier 1 → some conversations spread to tier 2 (probability ~30%)", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "preferred-hot");
    km.addKey(VALID_KEY_2, "normal-ready");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);
    // Tier 1 on-pace (pace ~1.0 → projected 0.97, spread prob 0.30)
    seedCapacity(km, VALID_KEY_1, [
      { windowName: "unified-5h", util: 0.58, resetIn: FIVE_H_MS * 0.40 },
    ]);
    // Tier 2 fresh — passes 30% headroom floor (unknown → optimistic)
    seedCapacity(km, VALID_KEY_2, [
      { windowName: "unified-5h", util: 0.05, resetIn: FIVE_H_MS * 0.40 },
    ]);

    let tier1Count = 0;
    let tier2Count = 0;
    for (let i = 0; i < 1000; i++) {
      const pick = km.getKeyForConversation(`conv-${i}:sess-${i * 13 + 5}`);
      if (pick.priorityTier === 1) tier1Count++;
      if (pick.priorityTier === 2) tier2Count++;
      // Clear affinity so each call re-selects (otherwise the first call per
      // conversationKey sticks for the next hour).
      (km as unknown as { conversationAffinities: Map<string, unknown> })
        .conversationAffinities.clear();
    }
    const tier2Ratio = tier2Count / 1000;
    // Expect ~30% ±3% (hash distribution tested elsewhere at tighter bounds)
    expect(tier2Ratio).toBeGreaterThan(0.26);
    expect(tier2Ratio).toBeLessThan(0.34);
    expect(tier1Count + tier2Count).toBe(1000);
  });

  test("extreme peak (pace 3x) → higher spread probability (~50%)", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "preferred-crisis");
    km.addKey(VALID_KEY_2, "normal-ready");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);
    // Tier 1 severely ahead of pace: projects to 3x (capped at 1.5) → prob 0.60
    seedCapacity(km, VALID_KEY_1, [
      { windowName: "unified-5h", util: 0.60, resetIn: FIVE_H_MS * 0.80 },
    ]);
    seedCapacity(km, VALID_KEY_2, [
      { windowName: "unified-5h", util: 0.05, resetIn: FIVE_H_MS * 0.40 },
    ]);

    let tier2Count = 0;
    for (let i = 0; i < 1000; i++) {
      const pick = km.getKeyForConversation(`conv-${i}:sess-${i * 7 + 3}`);
      if (pick.priorityTier === 2) tier2Count++;
      (km as unknown as { conversationAffinities: Map<string, unknown> })
        .conversationAffinities.clear();
    }
    // At pace 3x → projection 3.0 → capped to 1.5 → >=1.2 → prob 0.60
    const ratio = tier2Count / 1000;
    expect(ratio).toBeGreaterThan(0.55);
    expect(ratio).toBeLessThan(0.65);
  });

  test("high peak but tier 2 below its reserve floor → stays on tier 1", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "preferred-hot");
    km.addKey(VALID_KEY_2, "normal-also-drained");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);
    seedCapacity(km, VALID_KEY_1, [
      { windowName: "unified-5h", util: 0.80, resetIn: FIVE_H_MS * 0.40 },
    ]);
    // Tier 2 at 80% util — headroom 20%, below 30% floor — NOT eligible.
    seedCapacity(km, VALID_KEY_2, [
      { windowName: "unified-5h", util: 0.80, resetIn: FIVE_H_MS * 0.40 },
    ]);

    // All 200 picks should stay on tier 1 (tier 2 gated out).
    for (let i = 0; i < 200; i++) {
      const pick = km.getKeyForConversation(`conv-${i}:sess-${i}`);
      expect(pick.priorityTier).toBe(1);
    }
  });

  test("cascade: tier 1 hot, tier 2 ineligible, tier 3 eligible → some spread to tier 3", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "pref-hot");
    km.addKey(VALID_KEY_2, "normal-drained");
    km.addKey(VALID_KEY_3, "fallback-fresh");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);
    km.updateKeyPriority(VALID_KEY_3, 3);
    seedCapacity(km, VALID_KEY_1, [
      { windowName: "unified-5h", util: 0.80, resetIn: FIVE_H_MS * 0.40 },
    ]);
    // Tier 2 below 30% floor
    seedCapacity(km, VALID_KEY_2, [
      { windowName: "unified-5h", util: 0.80, resetIn: FIVE_H_MS * 0.40 },
    ]);
    // Tier 3 fresh, passes 50% floor
    seedCapacity(km, VALID_KEY_3, [
      { windowName: "unified-5h", util: 0.05, resetIn: FIVE_H_MS * 0.40 },
    ]);

    let tier3Count = 0;
    for (let i = 0; i < 500; i++) {
      const pick = km.getKeyForConversation(`conv-${i}:sess-${i * 11}`);
      if (pick.priorityTier === 3) tier3Count++;
      (km as unknown as { conversationAffinities: Map<string, unknown> })
        .conversationAffinities.clear();
    }
    // Some (but not all) conversations spread all the way to tier 3.
    expect(tier3Count).toBeGreaterThan(0);
    expect(tier3Count).toBeLessThan(500);
  });

  test("spilledFromTier reports the preemptive shift", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "pref");
    km.addKey(VALID_KEY_2, "normal");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);
    // Extreme tier 1 peak so most picks spread.
    seedCapacity(km, VALID_KEY_1, [
      { windowName: "unified-5h", util: 0.60, resetIn: FIVE_H_MS * 0.80 },
    ]);
    seedCapacity(km, VALID_KEY_2, [
      { windowName: "unified-5h", util: 0.05, resetIn: FIVE_H_MS * 0.40 },
    ]);

    let foundSpill = false;
    for (let i = 0; i < 100; i++) {
      const pick = km.getKeyForConversation(`conv-${i}:sess-${i}`);
      if (pick.priorityTier === 2) {
        expect(pick.spilledFromTier).toBe(1);
        foundSpill = true;
      } else {
        expect(pick.spilledFromTier).toBeNull();
      }
      (km as unknown as { conversationAffinities: Map<string, unknown> })
        .conversationAffinities.clear();
    }
    expect(foundSpill).toBe(true);
  });

  test("conversation affinity wins over Layer 2 — once assigned, stays put", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "pref");
    km.addKey(VALID_KEY_2, "normal");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);
    // Low pace initially — no spread — assigns to tier 1.
    seedCapacity(km, VALID_KEY_1, [
      { windowName: "unified-5h", util: 0.10, resetIn: FIVE_H_MS * 0.80 },
    ]);
    seedCapacity(km, VALID_KEY_2, [
      { windowName: "unified-5h", util: 0.05, resetIn: FIVE_H_MS * 0.40 },
    ]);

    const conversationKey = "sticky-conv:sticky-sess";
    const firstPick = km.getKeyForConversation(conversationKey);
    const assignedKey = firstPick.entry!.key;
    expect(firstPick.priorityTier).toBe(1);

    // Now fake heavy pressure on tier 1.
    seedCapacity(km, VALID_KEY_1, [
      { windowName: "unified-5h", util: 0.60, resetIn: FIVE_H_MS * 0.80 },
    ]);

    // Subsequent calls on the SAME conversation key stick with their assigned
    // tier-1 key regardless of Layer 2 pressure.
    for (let i = 0; i < 20; i++) {
      const pick = km.getKeyForConversation(conversationKey);
      expect(pick.entry!.key).toBe(assignedKey);
      expect(pick.priorityTier).toBe(1);
      expect(pick.routingDecision).toBe("conversation_affinity_hit");
    }
  });

  test("no projection data (unknown keys) → Layer 2 silent, Layer 1 behavior preserved", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "pref-unknown");
    km.addKey(VALID_KEY_2, "normal-unknown");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 2);
    // No seedCapacity calls — both keys have empty capacity.windows.

    // 100% of traffic to tier 1 because (a) Layer 2 can't project anything
    // without data, (b) tier 1 floor=0 accepts the unknown key.
    let tier1 = 0;
    for (let i = 0; i < 100; i++) {
      const pick = km.getKeyForConversation(`conv-${i}`);
      if (pick.priorityTier === 1) tier1++;
    }
    expect(tier1).toBe(100);
  });

  test("deterministic replay: running Layer 2 twice with same inputs = identical outcomes", () => {
    // Use a fixture whose projected peak lands well inside a probability
    // bucket (0.45 util at 50% elapsed → projected 0.9, in the 0.85-0.95
    // range → prob 0.10). Values exactly at a bucket boundary can flip
    // between KeyManager instances due to sub-millisecond time drift
    // during setup — a real ambiguity in the math, not a bug, so we test
    // stability in a region where the two sides of the decision agree.
    const km1 = create();
    km1.addKey(VALID_KEY_1, "pref");
    km1.addKey(VALID_KEY_2, "normal");
    km1.updateKeyPriority(VALID_KEY_1, 1);
    km1.updateKeyPriority(VALID_KEY_2, 2);
    seedCapacity(km1, VALID_KEY_1, [
      { windowName: "unified-5h", util: 0.45, resetIn: FIVE_H_MS * 0.50 },
    ]);
    seedCapacity(km1, VALID_KEY_2, [
      { windowName: "unified-5h", util: 0.05, resetIn: FIVE_H_MS * 0.50 },
    ]);

    const tempDir2 = mkdtempSync(join(tmpdir(), "km-replay-"));
    const km2 = new KeyManager(tempDir2);
    managers.push(km2);
    km2.addKey(VALID_KEY_1, "pref");
    km2.addKey(VALID_KEY_2, "normal");
    km2.updateKeyPriority(VALID_KEY_1, 1);
    km2.updateKeyPriority(VALID_KEY_2, 2);
    seedCapacity(km2, VALID_KEY_1, [
      { windowName: "unified-5h", util: 0.45, resetIn: FIVE_H_MS * 0.50 },
    ]);
    seedCapacity(km2, VALID_KEY_2, [
      { windowName: "unified-5h", util: 0.05, resetIn: FIVE_H_MS * 0.50 },
    ]);

    const outcomes1: number[] = [];
    const outcomes2: number[] = [];
    for (let i = 0; i < 50; i++) {
      const key = `replay-${i}:sess`;
      outcomes1.push(km1.getKeyForConversation(key).priorityTier!);
      (km1 as unknown as { conversationAffinities: Map<string, unknown> })
        .conversationAffinities.clear();
      outcomes2.push(km2.getKeyForConversation(key).priorityTier!);
      (km2 as unknown as { conversationAffinities: Map<string, unknown> })
        .conversationAffinities.clear();
    }
    expect(outcomes1).toEqual(outcomes2);
    rmSync(tempDir2, { recursive: true, force: true });
  });
});
