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
import { KeyManager } from "../src/key-manager.ts";
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
    expect(entry.recentLinkedSessions15m).toBe(0);
  });

  test("listKeys() tracks linked sessions active in the last 15 minutes", () => {
    const km = create();
    const originalNow = Date.now;
    let fakeNow = 1_000_000;
    Date.now = () => fakeNow;

    try {
      km.addKey(VALID_KEY_1, "a");
      km.addKey(VALID_KEY_2, "b");

      expect(km.getKeyForConversation("user-1:session-a").entry?.key).toBe(VALID_KEY_1);
      expect(km.getKeyForConversation("user-1:session-b").entry?.key).toBe(VALID_KEY_2);

      let keys = km.listKeys().sort((a, b) => String(a.label).localeCompare(String(b.label)));
      expect(keys.map((key) => key.recentLinkedSessions15m)).toEqual([1, 1]);

      fakeNow += 16 * 60 * 1000;
      keys = km.listKeys().sort((a, b) => String(a.label).localeCompare(String(b.label)));
      expect(keys.map((key) => key.recentLinkedSessions15m)).toEqual([0, 0]);

      expect(km.getKeyForConversation("user-1:session-a").entry?.key).toBe(VALID_KEY_1);
      keys = km.listKeys().sort((a, b) => String(a.label).localeCompare(String(b.label)));
      expect(keys.map((key) => key.recentLinkedSessions15m)).toEqual([1, 0]);
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

    km.recordCapacityObservation(entry, {
      seenAt: unixMs(1_000),
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
      seenAt: unixMs(2_000),
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

    km1.recordCapacityObservation(entry, {
      seenAt: unixMs(5_000),
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

  test("getCapacitySummary keeps successful-response rejection telemetry in warning-only health", () => {
    const km = create();
    const healthy = km.addKey(VALID_KEY_1, "healthy");
    const warning = km.addKey(VALID_KEY_2, "warning");
    const observedRejected = km.addKey(VALID_KEY_3, "observed-rejected");

    km.recordCapacityObservation(healthy, {
      seenAt: unixMs(1_000),
      httpStatus: 200,
      organizationId: "org-a",
      windows: [{ windowName: "unified-5h", status: "allowed", utilization: 0.25, resetAt: unixMs(Date.now() + 60_000) }],
    });
    km.recordCapacityObservation(warning, {
      seenAt: unixMs(1_100),
      httpStatus: 200,
      organizationId: "org-a",
      fallbackAvailable: true,
      windows: [{ windowName: "unified-5h", status: "allowed_warning", utilization: 0.86, resetAt: unixMs(Date.now() + 60_000) }],
    });
    km.recordCapacityObservation(observedRejected, {
      seenAt: unixMs(1_200),
      httpStatus: 200,
      organizationId: "org-b",
      overageStatus: "rejected",
      windows: [{ windowName: "unified-5h", status: "rejected", utilization: 1, resetAt: unixMs(Date.now() + 60_000) }],
    });

    const summary = km.getCapacitySummary();
    const observed = km.listKeys().find((key) => key.label === "observed-rejected");
    expect(observed).toBeDefined();
    expect(observed!.isAvailable).toBe(true);
    expect(observed!.capacityHealth).toBe("warning");
    expect(summary.healthyKeys).toBe(1);
    expect(summary.warningKeys).toBe(2);
    expect(summary.rejectedKeys).toBe(0);
    expect(summary.coolingDownKeys).toBe(0);
    expect(summary.fallbackAvailableKeys).toBe(1);
    expect(summary.overageRejectedKeys).toBe(1);
    expect(summary.distinctOrganizations).toBe(2);
    expect(summary.windows[0]!.windowName).toBe("unified-5h");
    expect(summary.windows[0]!.warningKeys).toBe(1);
    expect(summary.windows[0]!.rejectedKeys).toBe(1);
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
