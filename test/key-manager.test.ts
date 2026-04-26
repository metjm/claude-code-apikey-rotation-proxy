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

/** Create a KeyManager for testing. Default to per-conversation pinning so
 *  hash-suffix-bearing tests exercise the dashboard filter — tests for the
 *  default session-only product behavior override via the opts arg. */
function create(dataDir?: string, opts?: { perConversationPinning?: boolean }): KeyManager {
  const manager = new KeyManager(dataDir ?? tempDir, {
    perConversationPinning: opts?.perConversationPinning ?? true,
  });
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
    expect(entry.recentSessions).toEqual([]);
  });

  test("listKeys() groups multiple conversations under one session-id and exposes their hashes", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");

    // Three sub-agents share a session-id but have distinct first-message hashes.
    km.getKeyForConversation("user-1:session-x:1111111111111111", "session-x");
    km.getKeyForConversation("user-1:session-x:2222222222222222", "session-x");
    km.getKeyForConversation("user-1:session-x:3333333333333333", "session-x");
    // Plus a second session with no augmenting hash — represents non-/v1/messages
    // traffic (count_tokens probes, etc.) that still pins for routing but should
    // be hidden from the dashboard sessions table.
    km.getKeyForConversation("user-1:session-y", "session-y");

    const entry = km.listKeys().find((k) => k.label === "a")!;
    const sessions = entry.recentSessions;
    const xs = sessions.find((s) => s.sessionId === "session-x");
    const ys = sessions.find((s) => s.sessionId === "session-y");

    expect(xs).toBeDefined();
    expect(xs!.conversations.length).toBe(3);
    expect(xs!.conversations.map((c) => c.hash).sort()).toEqual([
      "1111111111111111",
      "2222222222222222",
      "3333333333333333",
    ]);
    expect(xs!.actor).toBe("user-1");
    expect(xs!.firstSeenAt).toBeTruthy();
    expect(xs!.totalRequests).toBeGreaterThan(0);
    for (const conv of xs!.conversations) {
      expect(conv.firstSeenAt).toBeTruthy();
      expect(conv.requestCount).toBeGreaterThan(0);
    }

    // session-y was pinned (affinity exists for routing) but its 2-part key
    // means it's not a real conversation turn — filtered from the dashboard.
    expect(ys).toBeUndefined();
  });

  test("listKeys() tracks recent sessions active in the last 2 minutes", () => {
    const km = create();
    const originalNow = Date.now;
    let fakeNow = 1_000_000;
    Date.now = () => fakeNow;

    try {
      km.addKey(VALID_KEY_1, "a");
      km.addKey(VALID_KEY_2, "b");

      // Bucket-of-3 routing fills A first, so use 4 sessions to land on both.
      // 3-part conversationKeys (with hash suffix) so they show up in recentSessions
      // — the dashboard hides 2-part keys (count_tokens, etc.).
      expect(km.getKeyForConversation("user-1:session-a:aaaaaaaaaaaaaaaa", "session-a").entry?.key).toBe(VALID_KEY_1);
      expect(km.getKeyForConversation("user-1:session-b:bbbbbbbbbbbbbbbb", "session-b").entry?.key).toBe(VALID_KEY_1);
      expect(km.getKeyForConversation("user-1:session-c:cccccccccccccccc", "session-c").entry?.key).toBe(VALID_KEY_1);
      expect(km.getKeyForConversation("user-1:session-d:dddddddddddddddd", "session-d").entry?.key).toBe(VALID_KEY_2);

      let keys = km.listKeys().sort((a, b) => String(a.label).localeCompare(String(b.label)));
      expect(keys.map((key) => key.recentSessions.map((session) => session.sessionId).sort())).toEqual([
        ["session-a", "session-b", "session-c"],
        ["session-d"],
      ]);

      fakeNow += 3 * 60 * 1000;
      keys = km.listKeys().sort((a, b) => String(a.label).localeCompare(String(b.label)));
      expect(keys.map((key) => key.recentSessions)).toEqual([[], []]);

      expect(km.getKeyForConversation("user-1:session-a:aaaaaaaaaaaaaaaa", "session-a").entry?.key).toBe(VALID_KEY_1);
      keys = km.listKeys().sort((a, b) => String(a.label).localeCompare(String(b.label)));
      expect(keys.map((key) => key.recentSessions.map((session) => session.sessionId))).toEqual([
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

  test("getKeyForConversation() fills 3 sessions on one account before rotating to the next", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");
    km.addKey(VALID_KEY_2, "b");

    const picks: string[] = [];
    for (let i = 0; i < 6; i++) {
      const pick = km.getKeyForConversation(`user-1:session-${i}`);
      picks.push(pick.entry!.key);
      km.recordRequest(pick.entry!);
    }
    expect(picks.slice(0, 3).every((k) => k === VALID_KEY_1)).toBe(true);
    expect(picks.slice(3, 6).every((k) => k === VALID_KEY_2)).toBe(true);
  });

  test("getKeyForConversation() skips Normal-tier when it has been demoted to Secondary", () => {
    const km = create();
    const p1 = km.addKey(VALID_KEY_1, "preferred-a");
    const p2 = km.addKey(VALID_KEY_2, "preferred-b");
    const n3 = km.addKey(VALID_KEY_3, "normal-c");
    km.updateKeyPriority(VALID_KEY_1, 1);
    km.updateKeyPriority(VALID_KEY_2, 1);
    km.updateKeyPriority(VALID_KEY_3, 2);
    void p1; void p2;
    // Push the Normal account above 75% so it drops to Secondary
    km.recordCapacityObservation(n3, {
      seenAt: unixMs(Date.now()),
      httpStatus: 200,
      windows: [{
        windowName: "unified-7d",
        status: "allowed",
        utilization: 0.80,
        resetAt: unixMs(Date.now() + 24 * 60 * 60 * 1000),
      }],
    });

    const tiers = new Set<number>();
    const labels = new Set<string>();
    for (let i = 0; i < 9; i++) {
      const pick = km.getKeyForConversation(`user-1:session-${i}`);
      tiers.add(pick.priorityTier!);
      labels.add(pick.entry!.key);
      km.recordRequest(pick.entry!);
    }
    expect(tiers).toEqual(new Set([1]));
    expect(labels).toEqual(new Set([VALID_KEY_1, VALID_KEY_2]));
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

  test("getNextAvailableKey() picks Preferred when a hot Fallback would otherwise sit alongside it", () => {
    const km = create();
    const k1 = km.addKey(VALID_KEY_1, "fallback");
    km.addKey(VALID_KEY_2, "preferred");
    km.recordRequest(k1);
    km.updateKeyPriority(VALID_KEY_1, 3);
    km.updateKeyPriority(VALID_KEY_2, 1);
    // Push fallback above its 50% gate so it drops to Tertiary
    km.recordCapacityObservation(k1, {
      seenAt: unixMs(Date.now()),
      httpStatus: 200,
      windows: [{
        windowName: "unified-7d",
        status: "allowed",
        utilization: 0.6,
        resetAt: unixMs(Date.now() + 24 * 60 * 60 * 1000),
      }],
    });
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

describe("Disabled priority (4)", () => {
  test("getNextAvailableKey() skips disabled keys entirely", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "disabled-one");
    km.addKey(VALID_KEY_2, "active");
    km.updateKeyPriority(VALID_KEY_1, 4);

    const selected = km.getNextAvailableKey();
    expect(selected?.key).toBe(VALID_KEY_2);
  });

  test("getNextAvailableKey() returns null when all keys are disabled", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");
    km.addKey(VALID_KEY_2, "b");
    km.updateKeyPriority(VALID_KEY_1, 4);
    km.updateKeyPriority(VALID_KEY_2, 4);

    expect(km.getNextAvailableKey()).toBeNull();
  });

  test("availableCount() excludes disabled keys", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");
    km.addKey(VALID_KEY_2, "b");
    expect(km.availableCount()).toBe(2);

    km.updateKeyPriority(VALID_KEY_1, 4);
    expect(km.availableCount()).toBe(1);
  });

  test("listKeys() reports isAvailable: false for disabled keys", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "off");
    km.updateKeyPriority(VALID_KEY_1, 4);

    const listed = km.listKeys();
    expect(listed[0]!.priority).toBe(4);
    expect(listed[0]!.isAvailable).toBe(false);
  });

  test("disabled priority persists across reload", () => {
    const km1 = create();
    km1.addKey(VALID_KEY_1, "a");
    km1.updateKeyPriority(VALID_KEY_1, 4);

    const km2 = trackManager(new KeyManager(tempDir));
    expect(km2.listKeys()[0]!.priority).toBe(4);
    expect(km2.getNextAvailableKey()).toBeNull();
  });

  test("re-enabling a disabled key puts it back into rotation", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "a");
    km.updateKeyPriority(VALID_KEY_1, 4);
    expect(km.getNextAvailableKey()).toBeNull();

    km.updateKeyPriority(VALID_KEY_1, 2);
    expect(km.getNextAvailableKey()?.key).toBe(VALID_KEY_1);
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

// ── Pool-based routing (per-account pool assignment + cascade) ─────────────

describe("Pool-based routing", () => {
  const FIVE_H = 5 * 60 * 60 * 1000;
  const SEVEN_D = 7 * 24 * 60 * 60 * 1000;
  const VALID_KEY_4 = "sk-ant-api03-dddddddddddddddddddddddddddddddddd";
  const VALID_KEY_5 = "sk-ant-api03-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

  type WindowSpec = { name: "unified-5h" | "unified-7d"; util: number; resetAt?: number };
  function setWindows(km: KeyManager, entry: ApiKeyEntry, windows: WindowSpec[]): void {
    const now = Date.now();
    km.recordCapacityObservation(entry, {
      seenAt: unixMs(now),
      httpStatus: 200,
      windows: windows.map((w) => ({
        windowName: w.name,
        status: "allowed",
        utilization: w.util,
        resetAt: unixMs(w.resetAt ?? now + (w.name === "unified-5h" ? FIVE_H / 2 : SEVEN_D / 2)),
      })),
    });
  }

  function callAssignPool(km: KeyManager, entry: ApiKeyEntry): "primary" | "secondary" | "tertiary" {
    return (km as unknown as {
      assignPool: (e: ApiKeyEntry, t: number) => "primary" | "secondary" | "tertiary";
    }).assignPool(entry, unixMs(Date.now()));
  }

  // ── assignPool matrix ───────────────────────────────────────────────────

  describe("assignPool — per-account pool placement", () => {
    test("Preferred (priority 1) is always Primary even with no telemetry", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "p");
      km.updateKeyPriority(VALID_KEY_1, 1);
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Preferred at 99% util on both windows is still Primary", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "p-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      setWindows(km, entry, [
        { name: "unified-5h", util: 0.99 },
        { name: "unified-7d", util: 0.99 },
      ]);
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Normal at 0/0 → Primary", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n");
      setWindows(km, entry, [
        { name: "unified-5h", util: 0 },
        { name: "unified-7d", util: 0 },
      ]);
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Normal at 74% weekly + 74% 5h → Primary (just under 75%)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-edge");
      setWindows(km, entry, [
        { name: "unified-5h", util: 0.74 },
        { name: "unified-7d", util: 0.74 },
      ]);
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Normal at exactly 75% weekly → Secondary", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-w75");
      setWindows(km, entry, [
        { name: "unified-7d", util: 0.75 },
      ]);
      expect(callAssignPool(km, entry)).toBe("secondary");
    });

    test("Normal at exactly 75% 5h → Secondary", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-5h75");
      setWindows(km, entry, [
        { name: "unified-5h", util: 0.75 },
      ]);
      expect(callAssignPool(km, entry)).toBe("secondary");
    });

    test("Normal at 75% on both → Secondary", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-both");
      setWindows(km, entry, [
        { name: "unified-5h", util: 0.75 },
        { name: "unified-7d", util: 0.75 },
      ]);
      expect(callAssignPool(km, entry)).toBe("secondary");
    });

    test("Normal with no capacity windows → Primary (unknown counts as 0)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-fresh");
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Normal with windows but null utilization → Primary (null = 0)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-nullutil");
      km.recordCapacityObservation(entry, {
        seenAt: unixMs(Date.now()),
        httpStatus: 200,
        windows: [{ windowName: "unified-7d", status: "allowed", resetAt: unixMs(Date.now() + SEVEN_D / 2) }],
      });
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Fallback at low util (49/49) → Tertiary (util doesn't promote Fallback)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "f-lowutil");
      km.updateKeyPriority(VALID_KEY_1, 3);
      setWindows(km, entry, [
        { name: "unified-5h", util: 0.49 },
        { name: "unified-7d", util: 0.49 },
      ]);
      expect(callAssignPool(km, entry)).toBe("tertiary");
    });

    test("Fallback at high util → Tertiary", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "f-hot");
      km.updateKeyPriority(VALID_KEY_1, 3);
      setWindows(km, entry, [
        { name: "unified-5h", util: 0.90 },
        { name: "unified-7d", util: 0.90 },
      ]);
      expect(callAssignPool(km, entry)).toBe("tertiary");
    });

    test("Fallback with no capacity windows → Tertiary (default last-resort)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "f-fresh");
      km.updateKeyPriority(VALID_KEY_1, 3);
      expect(callAssignPool(km, entry)).toBe("tertiary");
    });

    test("Fallback with 7d near reset (>=95% elapsed) → Primary (drain before rollover)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "f-near-reset");
      km.updateKeyPriority(VALID_KEY_1, 3);
      const elapsed = SEVEN_D * 0.96;
      const resetIn = SEVEN_D - elapsed;
      setWindows(km, entry, [
        { name: "unified-7d", util: 0.80, resetAt: Date.now() + resetIn },
      ]);
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Fallback with 7d far from reset stays Tertiary (no promotion)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "f-far");
      km.updateKeyPriority(VALID_KEY_1, 3);
      setWindows(km, entry, [
        { name: "unified-7d", util: 0.90, resetAt: Date.now() + SEVEN_D / 2 },
      ]);
      expect(callAssignPool(km, entry)).toBe("tertiary");
    });

    test("Near-reset window (>=95% elapsed) is ignored — Normal at 90% near reset stays Primary", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-nearreset");
      const elapsed = SEVEN_D * 0.96;
      const resetIn = SEVEN_D - elapsed;
      setWindows(km, entry, [
        { name: "unified-7d", util: 0.90, resetAt: Date.now() + resetIn },
      ]);
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Other windows (e.g. 'unified') don't affect pool placement", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-other");
      km.recordCapacityObservation(entry, {
        seenAt: unixMs(Date.now()),
        httpStatus: 200,
        windows: [{ windowName: "unified", status: "allowed", utilization: 0.99, resetAt: unixMs(Date.now() + 60_000) }],
      });
      expect(callAssignPool(km, entry)).toBe("primary");
    });
  });

  // ── Cascade Primary → Secondary → Tertiary ──────────────────────────────

  describe("Cascade: Primary → Secondary → Tertiary", () => {
    test("Primary present → Primary picked over a demoted Secondary", () => {
      const km = create();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      // n must be demoted to Secondary so the cascade rule applies
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      const pick = km.getNextAvailableKey();
      expect(pick?.key).toBe(p.key);
    });

    test("All Primary on cooldown → Secondary picked", () => {
      const km = create();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      km.recordRateLimit(p, 9999);
      const pick = km.getNextAvailableKey();
      expect(pick?.key).toBe(n.key);
    });

    test("All Primary disabled → Secondary picked", () => {
      const km = create();
      km.addKey(VALID_KEY_1, "p-disabled");
      const n = km.addKey(VALID_KEY_2, "n-hot");
      km.updateKeyPriority(VALID_KEY_1, 4);
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      const pick = km.getNextAvailableKey();
      expect(pick?.key).toBe(n.key);
    });

    test("All Primary outside allowedDays → Secondary picked", () => {
      const km = create();
      const p = km.addKey(VALID_KEY_1, "p-banned");
      const n = km.addKey(VALID_KEY_2, "n-hot");
      const today = new Date().getDay();
      const otherDays = [0, 1, 2, 3, 4, 5, 6].filter((d) => d !== today);
      km.updateKeyAllowedDays(p.key, otherDays);
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      const pick = km.getNextAvailableKey();
      expect(pick?.key).toBe(n.key);
    });

    test("Primary + Secondary all on cooldown → Tertiary picked", () => {
      const km = create();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n-hot");
      const f = km.addKey(VALID_KEY_3, "f-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      km.updateKeyPriority(VALID_KEY_3, 3);
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      setWindows(km, f, [{ name: "unified-7d", util: 0.60 }]);
      km.recordRateLimit(p, 9999);
      km.recordRateLimit(n, 9999);
      const pick = km.getNextAvailableKey();
      expect(pick?.key).toBe(f.key);
    });

    test("All accounts on cooldown → null", () => {
      const km = create();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n");
      km.recordRateLimit(p, 9999);
      km.recordRateLimit(n, 9999);
      expect(km.getNextAvailableKey()).toBeNull();
    });

    test("Normal at 80% (Secondary) NOT picked while Preferred is in Primary pool", () => {
      const km = create();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      for (let i = 0; i < 5; i++) {
        const pick = km.getNextAvailableKey()!;
        expect(pick.key).toBe(p.key);
        km.recordRequest(pick);
      }
    });

    test("Fallback at 60% (Tertiary) NOT picked while Normal at 70% sits in Primary", () => {
      const km = create();
      const n = km.addKey(VALID_KEY_1, "n-warm");
      const f = km.addKey(VALID_KEY_2, "f-hot");
      km.updateKeyPriority(VALID_KEY_2, 3);
      setWindows(km, n, [{ name: "unified-7d", util: 0.70 }]);
      setWindows(km, f, [{ name: "unified-7d", util: 0.60 }]);
      for (let i = 0; i < 5; i++) {
        const pick = km.getNextAvailableKey()!;
        expect(pick.key).toBe(n.key);
        km.recordRequest(pick);
      }
    });
  });

  // ── Bucket-of-3 rotation ────────────────────────────────────────────────

  describe("Bucket-of-3 rotation", () => {
    test("Three fresh accounts: first 3 picks all on soonest-weekly-reset", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      const c = km.addKey(VALID_KEY_3, "c");
      // a resets soonest, then b, then c
      setWindows(km, a, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, b, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);
      setWindows(km, c, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D * 0.75 }]);

      const picks: string[] = [];
      for (let i = 0; i < 9; i++) {
        const pick = km.getKeyForConversation(`conv-${i}`);
        picks.push(pick.entry!.key);
        km.recordRequest(pick.entry!);
      }
      expect(picks.slice(0, 3)).toEqual([a.key, a.key, a.key]);
      expect(picks.slice(3, 6)).toEqual([b.key, b.key, b.key]);
      expect(picks.slice(6, 9)).toEqual([c.key, c.key, c.key]);
    });

    test("After all reach 3 sessions, rotation wraps back to soonest-reset", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      setWindows(km, a, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, b, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);

      for (let i = 0; i < 6; i++) {
        const pick = km.getKeyForConversation(`conv-${i}`);
        km.recordRequest(pick.entry!);
      }
      const seventh = km.getKeyForConversation("conv-7");
      expect(seventh.entry?.key).toBe(a.key);
    });

    test("Mid-rotation cooldown: account drops out, rotation continues with remainder", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      setWindows(km, a, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, b, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);

      // Two picks land on A, then A goes on cooldown
      km.recordRequest(km.getKeyForConversation("conv-1").entry!);
      km.recordRequest(km.getKeyForConversation("conv-2").entry!);
      km.recordRateLimit(a, 9999);

      // Next pick must go to B (A on cooldown)
      const pick = km.getKeyForConversation("conv-3");
      expect(pick.entry?.key).toBe(b.key);
    });

    test("Recent session count comes from a 15-minute window — older affinities don't count", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      setWindows(km, a, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, b, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);

      // Inject 3 stale (>15min old) affinities for A directly
      const staleTime = unixMs(baseNow - 20 * 60 * 1000);
      const affinities = (km as unknown as { conversationAffinities: Map<string, {
        conversationKey: string; key: string; sessionId: string | null; assignedAt: number; lastSeenAt: number;
      }> }).conversationAffinities;
      for (let i = 0; i < 3; i++) {
        affinities.set(`stale-${i}`, {
          conversationKey: `stale-${i}`,
          key: a.key,
          sessionId: null,
          assignedAt: staleTime,
          lastSeenAt: staleTime,
        });
      }

      // Stale affinities don't count as "recent sessions" → A is still picked
      const pick = km.getKeyForConversation("new-conv");
      expect(pick.entry?.key).toBe(a.key);
    });

    test("Affinity hits keep a session 'recent' (lastSeenAt updates)", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      setWindows(km, a, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, b, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);

      // Three conversations land on A
      km.getKeyForConversation("conv-1");
      km.getKeyForConversation("conv-2");
      km.getKeyForConversation("conv-3");

      // 4th NEW conversation should now go to B (A's bucket is full)
      const fourth = km.getKeyForConversation("conv-4");
      expect(fourth.entry?.key).toBe(b.key);

      // Hitting affinity for conv-1 does not change the rotation
      const hit = km.getKeyForConversation("conv-1");
      expect(hit.entry?.key).toBe(a.key);
      expect(hit.affinityHit).toBe(true);
    });
  });

  // ── Sort order: secondary keys ──────────────────────────────────────────

  describe("Sort order", () => {
    test("Soonest weekly reset wins when bucket counts tied", () => {
      const km = create();
      const baseNow = Date.now();
      const late = km.addKey(VALID_KEY_1, "late");
      const soon = km.addKey(VALID_KEY_2, "soon");
      setWindows(km, late, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D * 0.75 }]);
      setWindows(km, soon, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D * 0.25 }]);
      expect(km.getNextAvailableKey()?.key).toBe(soon.key);
    });

    test("Soonest 5h reset wins when buckets + weekly reset tied", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      // identical 7d resetAt
      const sevenDReset = baseNow + SEVEN_D / 2;
      setWindows(km, a, [
        { name: "unified-7d", util: 0.10, resetAt: sevenDReset },
        { name: "unified-5h", util: 0.10, resetAt: baseNow + FIVE_H * 0.75 },
      ]);
      setWindows(km, b, [
        { name: "unified-7d", util: 0.10, resetAt: sevenDReset },
        { name: "unified-5h", util: 0.10, resetAt: baseNow + FIVE_H * 0.25 },
      ]);
      expect(km.getNextAvailableKey()?.key).toBe(b.key);
    });

    test("Higher util wins when buckets and resets tied (drain hot accounts first)", () => {
      const km = create();
      const baseNow = Date.now();
      const cool = km.addKey(VALID_KEY_1, "cool");
      const hot = km.addKey(VALID_KEY_2, "hot");
      const sevenDReset = baseNow + SEVEN_D / 2;
      const fiveHReset = baseNow + FIVE_H / 2;
      setWindows(km, cool, [
        { name: "unified-7d", util: 0.10, resetAt: sevenDReset },
        { name: "unified-5h", util: 0.10, resetAt: fiveHReset },
      ]);
      setWindows(km, hot, [
        { name: "unified-7d", util: 0.40, resetAt: sevenDReset },
        { name: "unified-5h", util: 0.40, resetAt: fiveHReset },
      ]);
      expect(km.getNextAvailableKey()?.key).toBe(hot.key);
    });

    test("MRU wins as a final tiebreak after util", () => {
      const km = create();
      const old = km.addKey(VALID_KEY_1, "old");
      const recent = km.addKey(VALID_KEY_2, "recent");
      km.recordRequest(recent);
      expect(km.getNextAvailableKey()?.key).toBe(recent.key);
    });

    test("Alphabetical label is the absolute last tiebreak", () => {
      const km = create();
      // Two fresh keys, no telemetry, no usage — must be deterministic
      km.addKey(VALID_KEY_1, "zzz");
      km.addKey(VALID_KEY_2, "aaa");
      // VALID_KEY_2 has label "aaa" < "zzz", so it wins the alphabetical tiebreak
      expect(km.getNextAvailableKey()?.label).toBe("aaa");
    });
  });

  // ── Unknown reset / unknown utilization ─────────────────────────────────

  describe("Unknown reset / unknown utilization", () => {
    test("Account with no unified-7d window sorts first (unknown weekly = 0 = soonest)", () => {
      const km = create();
      const baseNow = Date.now();
      const known = km.addKey(VALID_KEY_1, "known");
      const fresh = km.addKey(VALID_KEY_2, "fresh");
      setWindows(km, known, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);
      // fresh has no windows at all
      expect(km.getNextAvailableKey()?.key).toBe(fresh.key);
    });

    test("Account with null resetAt sorts first (eager probe)", () => {
      const km = create();
      const baseNow = Date.now();
      const known = km.addKey(VALID_KEY_1, "known");
      const unknown = km.addKey(VALID_KEY_2, "unknown");
      setWindows(km, known, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);
      km.recordCapacityObservation(unknown, {
        seenAt: unixMs(baseNow),
        httpStatus: 200,
        windows: [{ windowName: "unified-7d", status: "allowed", utilization: 0.10 }],
      });
      expect(km.getNextAvailableKey()?.key).toBe(unknown.key);
    });

    test("Account with no util data treated as 0% — Normal stays Primary", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-noutil");
      km.recordCapacityObservation(entry, {
        seenAt: unixMs(Date.now()),
        httpStatus: 200,
        windows: [{ windowName: "unified-7d", status: "allowed", resetAt: unixMs(Date.now() + SEVEN_D / 2) }],
      });
      expect(callAssignPool(km, entry)).toBe("primary");
    });
  });

  // ── Affinity vs pool demotion ───────────────────────────────────────────

  describe("Affinity vs pool demotion", () => {
    test("Affinity holds when account drifts into Secondary (still available)", () => {
      const km = create();
      const a = km.addKey(VALID_KEY_1, "a");
      km.addKey(VALID_KEY_2, "b");

      const first = km.getKeyForConversation("conv-1");
      expect(first.entry?.key).toBe(a.key);

      // Drift A above the 75% gate
      setWindows(km, a, [{ name: "unified-7d", util: 0.80 }]);

      const repeat = km.getKeyForConversation("conv-1");
      expect(repeat.entry?.key).toBe(a.key);
      expect(repeat.affinityHit).toBe(true);
      expect(repeat.pool).toBe("secondary");
    });

    test("Affinity holds on a Fallback-pinned key even though its pool is Tertiary", () => {
      // With the Fallback-is-tertiary rule, the only way a Fallback key gets
      // pinned via a new assignment is when Primary/Secondary are exhausted.
      // Once pinned, subsequent requests should still be affinity hits even
      // though the pool stays Tertiary.
      const km = create();
      const a = km.addKey(VALID_KEY_1, "a");
      km.updateKeyPriority(VALID_KEY_1, 3);

      const first = km.getKeyForConversation("conv-1");
      expect(first.entry?.key).toBe(a.key);

      const repeat = km.getKeyForConversation("conv-1");
      expect(repeat.entry?.key).toBe(a.key);
      expect(repeat.affinityHit).toBe(true);
      expect(repeat.pool).toBe("tertiary");
    });

    test("Affinity broken when account on cooldown → reassigned via cascade", () => {
      const km = create();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");

      km.getKeyForConversation("conv-1");
      km.recordRateLimit(a, 9999);

      const repeat = km.getKeyForConversation("conv-1");
      expect(repeat.entry?.key).toBe(b.key);
      expect(repeat.remapped).toBe(true);
    });

    test("Affinity broken when account disabled → reassigned", () => {
      const km = create();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");

      km.getKeyForConversation("conv-1");
      km.updateKeyPriority(a.key, 4);

      const repeat = km.getKeyForConversation("conv-1");
      expect(repeat.entry?.key).toBe(b.key);
      expect(repeat.remapped).toBe(true);
    });

    test("Affinity broken when account outside allowedDays → reassigned", () => {
      const km = create();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");

      km.getKeyForConversation("conv-1");
      const today = new Date().getDay();
      km.updateKeyAllowedDays(a.key, [0, 1, 2, 3, 4, 5, 6].filter((d) => d !== today));

      const repeat = km.getKeyForConversation("conv-1");
      expect(repeat.entry?.key).toBe(b.key);
      expect(repeat.remapped).toBe(true);
    });
  });

  // ── Filters preserved ────────────────────────────────────────────────────

  describe("Filters (preserved behavior)", () => {
    test("Disabled (priority 4) accounts never selected", () => {
      const km = create();
      km.addKey(VALID_KEY_1, "disabled");
      const live = km.addKey(VALID_KEY_2, "live");
      km.updateKeyPriority(VALID_KEY_1, 4);
      for (let i = 0; i < 5; i++) {
        const pick = km.getNextAvailableKey()!;
        expect(pick.key).toBe(live.key);
        km.recordRequest(pick);
      }
    });

    test("excludedKeys parameter respected", () => {
      const km = create();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      const pick = km.getNextAvailableKey(new Set([a.key]));
      expect(pick?.key).toBe(b.key);
    });

    test("excludedKeys returning empty fallback when all excluded", () => {
      const km = create();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      const pick = km.getNextAvailableKey(new Set([a.key, b.key]));
      expect(pick).toBeNull();
    });
  });

  // ── Selection result observability ──────────────────────────────────────

  describe("Selection result fields", () => {
    test("pool field reflects the chosen account's pool", () => {
      const km = create();
      const a = km.addKey(VALID_KEY_1, "p");
      km.updateKeyPriority(VALID_KEY_1, 1);
      const sel = km.getKeyForConversation("conv-1");
      expect(sel.entry?.key).toBe(a.key);
      expect(sel.pool).toBe("primary");
    });

    test("pool field is 'secondary' when Normal is demoted and chosen", () => {
      const km = create();
      const n = km.addKey(VALID_KEY_1, "n-hot");
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      const sel = km.getKeyForConversation("conv-1");
      expect(sel.pool).toBe("secondary");
    });

    test("pool field is 'tertiary' when a Fallback key is chosen", () => {
      const km = create();
      const f = km.addKey(VALID_KEY_1, "f-hot");
      km.updateKeyPriority(VALID_KEY_1, 3);
      setWindows(km, f, [{ name: "unified-7d", util: 0.55 }]);
      const sel = km.getKeyForConversation("conv-1");
      expect(sel.pool).toBe("tertiary");
    });

    test("pool is null when no entry can be chosen", () => {
      const km = create();
      const k = km.addKey(VALID_KEY_1, "k");
      km.recordRateLimit(k, 9999);
      const sel = km.getKeyForConversation("conv-1");
      expect(sel.entry).toBeNull();
      expect(sel.pool).toBeNull();
    });

    test("worstHeadroom is the chosen key's headroom (1 - max util across 5h/7d)", () => {
      const km = create();
      const a = km.addKey(VALID_KEY_1, "a");
      setWindows(km, a, [
        { name: "unified-5h", util: 0.40 },
        { name: "unified-7d", util: 0.30 },
      ]);
      const sel = km.getKeyForConversation(null);
      expect(sel.entry?.key).toBe(a.key);
      expect(sel.worstHeadroom).toBeCloseTo(0.60, 5);
    });

    test("priorityTier mirrors the chosen account's configured priority", () => {
      const km = create();
      km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      // Push n to Secondary so the Preferred wins by cascade
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      const sel = km.getKeyForConversation("conv-1");
      expect(sel.priorityTier).toBe(1);
    });

    test("requestsByTier increments per recorded request, keyed by priority", () => {
      const km = create();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n");
      km.updateKeyPriority(VALID_KEY_1, 1);
      expect(km.getRequestsByTier()).toEqual({});
      km.recordRequest(p);
      km.recordRequest(p);
      km.recordRequest(n);
      expect(km.getRequestsByTier()).toEqual({ "1": 2, "2": 1 });
    });
  });

  // ── End-to-end / compound scenarios ─────────────────────────────────────

  describe("Compound scenarios", () => {
    test("Mixed pool: Preferred and Normal in Primary; Fallback stays Tertiary → Fallback never picked while Primary has options", () => {
      const km = create();
      const baseNow = Date.now();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n");
      const f = km.addKey(VALID_KEY_3, "f");
      km.updateKeyPriority(VALID_KEY_1, 1);
      km.updateKeyPriority(VALID_KEY_3, 3);
      // f has the soonest weekly reset, but Fallback-as-tertiary means that
      // doesn't promote it into Primary. Primary (p, n) should be drained first.
      setWindows(km, p, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D * 0.75 }]);
      setWindows(km, n, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);
      setWindows(km, f, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 4 }]);

      const picks = new Set<string>();
      for (let i = 0; i < 6; i++) {
        const pick = km.getKeyForConversation(`conv-${i}`);
        picks.add(pick.entry!.key);
        km.recordRequest(pick.entry!);
      }
      expect(picks.has(f.key)).toBe(false); // Fallback never touched
      expect(picks.has(p.key)).toBe(true);
      expect(picks.has(n.key)).toBe(true);
    });

    test("Mixed pool: Fallback with 7d near reset IS promoted into Primary and wins reset-soonest sort", () => {
      const km = create();
      const baseNow = Date.now();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n");
      const f = km.addKey(VALID_KEY_3, "f");
      km.updateKeyPriority(VALID_KEY_1, 1);
      km.updateKeyPriority(VALID_KEY_3, 3);
      // f is near-reset → promoted to Primary and has the soonest reset → wins the sort.
      const elapsed = SEVEN_D * 0.96;
      setWindows(km, p, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D * 0.75 }]);
      setWindows(km, n, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);
      setWindows(km, f, [{ name: "unified-7d", util: 0.80, resetAt: baseNow + (SEVEN_D - elapsed) }]);

      for (let i = 0; i < 3; i++) {
        const pick = km.getKeyForConversation(`conv-${i}`);
        expect(pick.entry?.key).toBe(f.key);
      }
    });

    test("Mixed pool: cooldown on Primary, Secondary takes over with bucket-of-3", () => {
      const km = create();
      const baseNow = Date.now();
      const p = km.addKey(VALID_KEY_1, "p");
      const n1 = km.addKey(VALID_KEY_2, "n1-hot");
      const n2 = km.addKey(VALID_KEY_3, "n2-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      setWindows(km, n1, [{ name: "unified-7d", util: 0.80, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, n2, [{ name: "unified-7d", util: 0.80, resetAt: baseNow + SEVEN_D / 2 }]);
      km.recordRateLimit(p, 9999);

      const picks: string[] = [];
      for (let i = 0; i < 6; i++) {
        const pick = km.getKeyForConversation(`conv-${i}`);
        picks.push(pick.entry!.key);
        km.recordRequest(pick.entry!);
      }
      expect(picks.slice(0, 3)).toEqual([n1.key, n1.key, n1.key]);
      expect(picks.slice(3, 6)).toEqual([n2.key, n2.key, n2.key]);
    });

    test("Cascade transition: Primary becomes available again mid-stream → routing returns to Primary", () => {
      const km = create();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      // n must be Secondary so the cascade is meaningful
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      km.recordRateLimit(p, 1);
      // First pick: Primary on cooldown → cascades to Secondary (n)
      expect(km.getKeyForConversation("c1").entry?.key).toBe(n.key);
      // Clear the cooldown
      km.resetKeyCooldowns();
      // Now a fresh pick should go back to Primary
      expect(km.getKeyForConversation("c2").entry?.key).toBe(p.key);
    });

    test("Pool transition: Normal at 70% (Primary) → drift to 80% → next pick avoids it", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_4, "n-warm");
      const b = km.addKey(VALID_KEY_5, "n-cool");
      // Equal priority (default 2). a has soonest reset → would pick first.
      setWindows(km, a, [{ name: "unified-7d", util: 0.70, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, b, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);
      // a is in Primary now (70% < 75%) — wins on soonest-reset rule
      expect(km.getNextAvailableKey()?.key).toBe(a.key);

      // Drift a to 80% — now in Secondary, b becomes the only Primary
      setWindows(km, a, [{ name: "unified-7d", util: 0.80, resetAt: baseNow + SEVEN_D / 4 }]);
      expect(km.getNextAvailableKey()?.key).toBe(b.key);
    });

    test("All Primary excluded by allowedDays + cooldown combo → cascade still works", () => {
      const km = create();
      const today = new Date().getDay();
      const banned = km.addKey(VALID_KEY_1, "p-banned");
      const cooled = km.addKey(VALID_KEY_2, "p-cooled");
      const n = km.addKey(VALID_KEY_3, "n-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      km.updateKeyPriority(VALID_KEY_2, 1);
      km.updateKeyAllowedDays(banned.key, [0, 1, 2, 3, 4, 5, 6].filter((d) => d !== today));
      km.recordRateLimit(cooled, 9999);
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      expect(km.getNextAvailableKey()?.key).toBe(n.key);
    });
  });

  // ── Additional edge cases (extra coverage) ─────────────────────────────

  describe("Edge cases & coverage gaps", () => {
    test("Normal: weekly < 75% but only that window present → Primary (5h treated as 0)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-w-only");
      setWindows(km, entry, [{ name: "unified-7d", util: 0.74 }]);
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Normal: 5h < 75% but only that window present → Primary", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-5h-only");
      setWindows(km, entry, [{ name: "unified-5h", util: 0.74 }]);
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Normal: 80% weekly + 30% 5h → Secondary (weekly above gate decides)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-mixed");
      setWindows(km, entry, [
        { name: "unified-7d", util: 0.80 },
        { name: "unified-5h", util: 0.30 },
      ]);
      expect(callAssignPool(km, entry)).toBe("secondary");
    });

    test("Normal: 30% weekly + 80% 5h → Secondary (5h above gate decides)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-mixed-rev");
      setWindows(km, entry, [
        { name: "unified-7d", util: 0.30 },
        { name: "unified-5h", util: 0.80 },
      ]);
      expect(callAssignPool(km, entry)).toBe("secondary");
    });

    test("Fallback: 30% weekly + 60% 5h → Tertiary (5h above 50% decides)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "f-mixed");
      km.updateKeyPriority(VALID_KEY_1, 3);
      setWindows(km, entry, [
        { name: "unified-7d", util: 0.30 },
        { name: "unified-5h", util: 0.60 },
      ]);
      expect(callAssignPool(km, entry)).toBe("tertiary");
    });

    test("Near-reset on 5h window is ignored — Normal at 99% 5h near reset stays Primary", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-5h-near");
      const elapsed = FIVE_H * 0.97;
      const resetIn = FIVE_H - elapsed;
      setWindows(km, entry, [
        { name: "unified-5h", util: 0.99, resetAt: Date.now() + resetIn },
      ]);
      expect(callAssignPool(km, entry)).toBe("primary");
    });

    test("Cascade: every account on cooldown across all pools → null", () => {
      const km = create();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n-hot");
      const f = km.addKey(VALID_KEY_3, "f-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      km.updateKeyPriority(VALID_KEY_3, 3);
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      setWindows(km, f, [{ name: "unified-7d", util: 0.60 }]);
      km.recordRateLimit(p, 9999);
      km.recordRateLimit(n, 9999);
      km.recordRateLimit(f, 9999);
      expect(km.getNextAvailableKey()).toBeNull();
    });

    test("Cascade with multiple Tertiary candidates: bucket-of-3 still applies", () => {
      const km = create();
      const baseNow = Date.now();
      const f1 = km.addKey(VALID_KEY_1, "f1");
      const f2 = km.addKey(VALID_KEY_2, "f2");
      km.updateKeyPriority(VALID_KEY_1, 3);
      km.updateKeyPriority(VALID_KEY_2, 3);
      setWindows(km, f1, [{ name: "unified-7d", util: 0.55, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, f2, [{ name: "unified-7d", util: 0.55, resetAt: baseNow + SEVEN_D / 2 }]);
      const picks: string[] = [];
      for (let i = 0; i < 6; i++) {
        const pick = km.getKeyForConversation(`c-${i}`);
        picks.push(pick.entry!.key);
        km.recordRequest(pick.entry!);
      }
      expect(picks.slice(0, 3)).toEqual([f1.key, f1.key, f1.key]);
      expect(picks.slice(3, 6)).toEqual([f2.key, f2.key, f2.key]);
    });

    test("Recent session count survives reload (affinities are persisted)", () => {
      const km1 = create();
      km1.addKey(VALID_KEY_1, "a");
      km1.addKey(VALID_KEY_2, "b");
      // Two affinities pre-loaded onto a
      km1.getKeyForConversation("c-1");
      km1.getKeyForConversation("c-2");
      km1.close();

      const km2 = create();
      // a now has 2 recent sessions, b has 0; both bucket 0; alphabetical → a
      const third = km2.getKeyForConversation("c-3");
      expect(third.entry?.key).toBe(VALID_KEY_1);
      // Fourth lands on b (a fills to 3)
      const fourth = km2.getKeyForConversation("c-4");
      expect(fourth.entry?.key).toBe(VALID_KEY_2);
    });

    test("excludedKeys interacting with pool: all Primary excluded → drops to Secondary", () => {
      const km = create();
      const p = km.addKey(VALID_KEY_1, "p");
      const n = km.addKey(VALID_KEY_2, "n-hot");
      km.updateKeyPriority(VALID_KEY_1, 1);
      setWindows(km, n, [{ name: "unified-7d", util: 0.80 }]);
      const pick = km.getNextAvailableKey(new Set([p.key]));
      expect(pick?.key).toBe(n.key);
    });

    test("Sort precedence: bucket beats weekly reset — A (3 sessions, soonest reset) loses to B (0 sessions)", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      // a has the soonest reset
      setWindows(km, a, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, b, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);

      // Three picks fill A's bucket
      km.recordRequest(km.getKeyForConversation("c1").entry!);
      km.recordRequest(km.getKeyForConversation("c2").entry!);
      km.recordRequest(km.getKeyForConversation("c3").entry!);

      // The 4th pick must skip A (bucket full) → goes to B even though A still resets sooner
      const fourth = km.getKeyForConversation("c4");
      expect(fourth.entry?.key).toBe(b.key);
    });

    test("Sort precedence: weekly beats 5h — A (later weekly, sooner 5h) loses to B", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      setWindows(km, a, [
        { name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D * 0.75 },
        { name: "unified-5h", util: 0.10, resetAt: baseNow + FIVE_H * 0.10 },
      ]);
      setWindows(km, b, [
        { name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D * 0.25 },
        { name: "unified-5h", util: 0.10, resetAt: baseNow + FIVE_H * 0.90 },
      ]);
      expect(km.getNextAvailableKey()?.key).toBe(b.key);
    });

    test("Sort precedence: 5h beats util — A (later 5h, hotter) loses to B", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      const sevenD = baseNow + SEVEN_D / 2;
      setWindows(km, a, [
        { name: "unified-7d", util: 0.10, resetAt: sevenD },
        { name: "unified-5h", util: 0.40, resetAt: baseNow + FIVE_H * 0.75 },
      ]);
      setWindows(km, b, [
        { name: "unified-7d", util: 0.10, resetAt: sevenD },
        { name: "unified-5h", util: 0.10, resetAt: baseNow + FIVE_H * 0.25 },
      ]);
      expect(km.getNextAvailableKey()?.key).toBe(b.key);
    });

    test("Sort precedence: util beats MRU — older usage on hot account beats fresher use on cool account", () => {
      const km = create();
      const baseNow = Date.now();
      const sevenD = baseNow + SEVEN_D / 2;
      const fiveH = baseNow + FIVE_H / 2;
      const cool = km.addKey(VALID_KEY_1, "cool");
      const hot = km.addKey(VALID_KEY_2, "hot");
      setWindows(km, cool, [
        { name: "unified-7d", util: 0.10, resetAt: sevenD },
        { name: "unified-5h", util: 0.10, resetAt: fiveH },
      ]);
      setWindows(km, hot, [
        { name: "unified-7d", util: 0.40, resetAt: sevenD },
        { name: "unified-5h", util: 0.40, resetAt: fiveH },
      ]);
      // cool was used most recently
      km.recordRequest(cool);
      // hot still wins (worstHeadroom 0.6 < 0.9)
      expect(km.getNextAvailableKey()?.key).toBe(hot.key);
    });

    test("Bucket boundary: 3rd session still on same account, 4th rolls over", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      setWindows(km, a, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, b, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);
      const picks: string[] = [];
      for (let i = 0; i < 4; i++) {
        const pick = km.getKeyForConversation(`c-${i}`);
        picks.push(pick.entry!.key);
        km.recordRequest(pick.entry!);
      }
      expect(picks).toEqual([a.key, a.key, a.key, b.key]);
    });

    test("Bucket math: 5 sessions = bucket 1 (floor 5/3); 6 sessions = bucket 2", () => {
      const km = create();
      const baseNow = Date.now();
      const a = km.addKey(VALID_KEY_1, "a");
      const b = km.addKey(VALID_KEY_2, "b");
      setWindows(km, a, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 4 }]);
      setWindows(km, b, [{ name: "unified-7d", util: 0.10, resetAt: baseNow + SEVEN_D / 2 }]);

      // Inject 6 affinities for A and 5 for B directly
      const affinities = (km as unknown as { conversationAffinities: Map<string, {
        conversationKey: string; key: string; sessionId: string | null; assignedAt: number; lastSeenAt: number;
      }> }).conversationAffinities;
      for (let i = 0; i < 6; i++) {
        affinities.set(`a-conv-${i}`, {
          conversationKey: `a-conv-${i}`, key: a.key, sessionId: null,
          assignedAt: unixMs(Date.now()), lastSeenAt: unixMs(Date.now()),
        });
      }
      for (let i = 0; i < 5; i++) {
        affinities.set(`b-conv-${i}`, {
          conversationKey: `b-conv-${i}`, key: b.key, sessionId: null,
          assignedAt: unixMs(Date.now()), lastSeenAt: unixMs(Date.now()),
        });
      }
      // A bucket = 6/3 = 2; B bucket = 5/3 = 1; B wins on bucket
      expect(km.getNextAvailableKey()?.key).toBe(b.key);
    });

    test("Past-reset window utilization is ignored by assignPool (window pruned by maintenance)", () => {
      const km = create();
      const entry = km.addKey(VALID_KEY_1, "n-past");
      // util 90% but resetAt is in the past → window will be pruned
      km.recordCapacityObservation(entry, {
        seenAt: unixMs(Date.now() - 10_000),
        httpStatus: 200,
        windows: [{ windowName: "unified-7d", status: "allowed", utilization: 0.90, resetAt: unixMs(Date.now() - 1) }],
      });
      // After prunePastResetCapacityWindows the entry has no usable window → Primary
      // (We exercise the user-visible behavior via getNextAvailableKey)
      const pick = km.getNextAvailableKey();
      expect(pick?.key).toBe(entry.key);
    });

    test("Affinity hit pool field reflects current pool placement", () => {
      const km = create();
      const a = km.addKey(VALID_KEY_1, "a");
      km.addKey(VALID_KEY_2, "b");
      km.getKeyForConversation("c1");
      // Drift a into Secondary
      setWindows(km, a, [{ name: "unified-7d", util: 0.80 }]);
      const hit = km.getKeyForConversation("c1");
      expect(hit.affinityHit).toBe(true);
      expect(hit.pool).toBe("secondary");
    });

    test("global_sticky_fallback selection (null conversationKey) populates pool field", () => {
      const km = create();
      km.addKey(VALID_KEY_1, "a");
      const sel = km.getKeyForConversation(null);
      expect(sel.entry?.key).toBe(VALID_KEY_1);
      expect(sel.pool).toBe("primary");
      expect(sel.routingDecision).toBe("global_sticky_fallback");
    });

    test("Selection result entry/pool both null when fleet is empty", () => {
      const km = create();
      const sel = km.getKeyForConversation("c1");
      expect(sel.entry).toBeNull();
      expect(sel.pool).toBeNull();
      expect(sel.priorityTier).toBeNull();
      expect(sel.worstHeadroom).toBeNull();
    });
  });
});

// ── Seasonal request factors ───────────────────────────────────────────────

describe("Seasonal request factors", () => {
  function insertBucket(
    km: KeyManager,
    bucketIsoHour: string,
    requests: number,
  ): void {
    const db = (km as unknown as { db: Database }).db;
    db.run(
      "INSERT INTO stats_timeseries (bucket, key_label, user_label, requests) VALUES (?, '__all__', '__all__', ?)",
      [bucketIsoHour, requests],
    );
  }

  /** Build a bucket string in UTC at weeksAgo full weeks offset, at the
   *  given day-of-week (0=Sun..6=Sat UTC) and hour (0..23 UTC). */
  function bucketAt(weeksAgo: number, dow: number, hour: number): string {
    const reference = new Date();
    // Anchor to start of current UTC week (Sunday 00:00)
    const sundayUtc = new Date(Date.UTC(
      reference.getUTCFullYear(),
      reference.getUTCMonth(),
      reference.getUTCDate() - reference.getUTCDay(),
      0, 0, 0, 0,
    ));
    const target = new Date(sundayUtc.getTime() - weeksAgo * 7 * 24 * 60 * 60 * 1000);
    target.setUTCDate(target.getUTCDate() + dow);
    target.setUTCHours(hour, 0, 0, 0);
    return target.toISOString().slice(0, 13);
  }

  test("empty db → every slot returns factor 1 with 0 samples", () => {
    const km = create();
    const table = km.computeSeasonalRequestFactors();
    expect(table.slots).toHaveLength(7 * 24);
    expect(table.totalSamples).toBe(0);
    for (const slot of table.slots) {
      expect(slot.factor).toBe(1);
      expect(slot.samples).toBe(0);
    }
  });

  test("totalSamples counts observed buckets, not requests", () => {
    const km = create();
    insertBucket(km, bucketAt(1, 2, 14), 100);
    insertBucket(km, bucketAt(2, 2, 14), 100);
    insertBucket(km, bucketAt(3, 2, 14), 100);
    const table = km.computeSeasonalRequestFactors();
    expect(table.totalSamples).toBe(3);
  });

  test("thin slot (< MIN_BASELINE_SAMPLES_PER_SLOT) → factor stays 1", () => {
    const km = create();
    // Just two observations in this slot — below the 3-sample threshold
    insertBucket(km, bucketAt(1, 3, 10), 999);
    insertBucket(km, bucketAt(2, 3, 10), 999);
    const table = km.computeSeasonalRequestFactors();
    const slot = table.slots.find((s) => s.dow === 3 && s.hour === 10)!;
    expect(slot.samples).toBe(2);
    expect(slot.factor).toBe(1);
  });

  test("uniform hourly traffic over 4 weeks → every populated slot factor ≈ 1", () => {
    const km = create();
    // Fill every (dow, hour) slot with 3 weeks of 10 requests — each slot
    // has identical history, so every factor should be 1.0.
    for (let dow = 0; dow < 7; dow++) {
      for (let hour = 0; hour < 24; hour++) {
        for (let week = 1; week <= 3; week++) {
          insertBucket(km, bucketAt(week, dow, hour), 10);
        }
      }
    }
    const table = km.computeSeasonalRequestFactors();
    for (const slot of table.slots) {
      expect(slot.samples).toBe(3);
      expect(slot.factor).toBeCloseTo(1, 5);
    }
  });

  test("single hot slot drives its factor above 1 while quiet slots compress below", () => {
    const km = create();
    // 3 weeks of 1-req baseline in every slot, but Tuesday 2pm UTC gets
    // 100 requests each week — should show a large factor for that slot.
    for (let dow = 0; dow < 7; dow++) {
      for (let hour = 0; hour < 24; hour++) {
        for (let week = 1; week <= 3; week++) {
          const requests = (dow === 2 && hour === 14) ? 100 : 1;
          insertBucket(km, bucketAt(week, dow, hour), requests);
        }
      }
    }
    const table = km.computeSeasonalRequestFactors();
    const hot = table.slots.find((s) => s.dow === 2 && s.hour === 14)!;
    const cool = table.slots.find((s) => s.dow === 0 && s.hour === 3)!;
    // With the 5.0 clamp, the hot slot saturates to 5.0; quiet slots stay well below 1
    expect(hot.factor).toBeGreaterThan(4.5);
    expect(cool.factor).toBeLessThan(1);
  });

  test("factors are clamped to [0.1, 5.0]", () => {
    const km = create();
    // 1 request in every slot baseline + one slot with 10000 → would yield
    // a factor well over 100× without the clamp.
    for (let dow = 0; dow < 7; dow++) {
      for (let hour = 0; hour < 24; hour++) {
        for (let week = 1; week <= 3; week++) {
          const requests = (dow === 2 && hour === 14) ? 10_000 : 1;
          insertBucket(km, bucketAt(week, dow, hour), requests);
        }
      }
    }
    const table = km.computeSeasonalRequestFactors();
    const hot = table.slots.find((s) => s.dow === 2 && s.hour === 14)!;
    expect(hot.factor).toBeLessThanOrEqual(5.0);
    for (const slot of table.slots) {
      if (slot.samples > 0) {
        expect(slot.factor).toBeGreaterThanOrEqual(0.1);
        expect(slot.factor).toBeLessThanOrEqual(5.0);
      }
    }
  });

  test("weeks argument limits the retrospective window", () => {
    const km = create();
    // Heavy traffic ONLY in week 5 (beyond default 4-week window) across
    // three distinct slots so we don't collide on the unique bucket PK
    insertBucket(km, bucketAt(5, 1, 9), 1000);
    insertBucket(km, bucketAt(5, 1, 10), 1000);
    insertBucket(km, bucketAt(5, 1, 11), 1000);
    // With the default of 4 weeks, this data is ignored
    const tableDefault = km.computeSeasonalRequestFactors();
    expect(tableDefault.totalSamples).toBe(0);
    // With weeks=6 it comes into scope
    const tableWide = km.computeSeasonalRequestFactors(6);
    expect(tableWide.totalSamples).toBe(3);
  });

  test("respects weeks=1 — only last week of buckets factored in", () => {
    const km = create();
    // Week 1 (in scope) is uniform-busy; week 3 (out of scope) is spiked
    for (let dow = 0; dow < 7; dow++) {
      for (let hour = 0; hour < 24; hour++) {
        insertBucket(km, bucketAt(1, dow, hour), 10);
      }
    }
    insertBucket(km, bucketAt(3, 2, 14), 100_000);
    const table = km.computeSeasonalRequestFactors(1);
    const hot = table.slots.find((s) => s.dow === 2 && s.hour === 14)!;
    expect(hot.factor).toBeCloseTo(1, 5);
  });

  test("zero requests in most slots with a non-zero outlier → outlier hot, rest clamped to min", () => {
    const km = create();
    // Week 1..3 of 0-request hours everywhere EXCEPT Tuesday 2pm
    for (let dow = 0; dow < 7; dow++) {
      for (let hour = 0; hour < 24; hour++) {
        for (let week = 1; week <= 3; week++) {
          const requests = (dow === 2 && hour === 14) ? 10 : 0;
          insertBucket(km, bucketAt(week, dow, hour), requests);
        }
      }
    }
    const table = km.computeSeasonalRequestFactors();
    const hot = table.slots.find((s) => s.dow === 2 && s.hour === 14)!;
    expect(hot.factor).toBeGreaterThan(1);
    // Quiet slots (0 requests vs small mean) clamp down to 0.1
    const quiet = table.slots.find((s) => s.dow === 0 && s.hour === 3)!;
    expect(quiet.factor).toBe(0.1);
  });

  test("parseBucketToSlot handles the range correctly via method behavior", () => {
    const km = create();
    // Insert one bucket per (dow, hour) at least 3 times and verify every
    // slot index is reachable
    for (let dow = 0; dow < 7; dow++) {
      for (let hour = 0; hour < 24; hour++) {
        for (let week = 1; week <= 3; week++) {
          insertBucket(km, bucketAt(week, dow, hour), dow * 24 + hour);
        }
      }
    }
    const table = km.computeSeasonalRequestFactors();
    // Every slot should have 3 samples
    for (const slot of table.slots) {
      expect(slot.samples).toBe(3);
    }
    // Slot factors should follow the dow*24+hour pattern monotonically
    const first = table.slots[0]!;
    const last = table.slots[table.slots.length - 1]!;
    expect(last.factor).toBeGreaterThan(first.factor);
  });

  test("malformed bucket string is skipped silently", () => {
    const km = create();
    const db = (km as unknown as { db: Database }).db;
    db.run(
      "INSERT INTO stats_timeseries (bucket, key_label, user_label, requests) VALUES ('garbage', '__all__', '__all__', 100)",
    );
    insertBucket(km, bucketAt(1, 0, 0), 10);
    insertBucket(km, bucketAt(2, 0, 0), 10);
    insertBucket(km, bucketAt(3, 0, 0), 10);
    const table = km.computeSeasonalRequestFactors();
    // Malformed row ignored; the well-formed slot still counts
    const slot = table.slots.find((s) => s.dow === 0 && s.hour === 0)!;
    expect(slot.samples).toBe(3);
  });
});

// ── Routing Decision Logging ────────────────────────────────────────────────

describe("Routing Decision Logging", () => {
  test("new assignment persists a row with selection + candidates snapshot", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    const sel = km.getKeyForConversation("conv-1", "session-abc");
    expect(sel.entry).not.toBeNull();

    const rows = km.getRecentRoutingDecisions();
    expect(rows.length).toBe(1);
    const row = rows[0]!;
    expect(row.conversationKey).toBe("conv-1");
    expect(row.sessionId).toBe("session-abc");
    expect(row.chosenKeyLabel).toBe(sel.entry!.label);
    expect(row.routingDecision).toBe("conversation_new_assignment");
    expect(row.affinityHit).toBe(false);
    expect(row.remapped).toBe(false);
    expect(row.priorityTier).toBe(sel.priorityTier);
    expect(row.pool).toBe(sel.pool);
    expect(row.candidateCount).toBe(sel.candidateCount);
    expect(row.worstHeadroom).toBe(sel.worstHeadroom);
    expect(row.conversationCountForSelected).toBe(sel.conversationCountForSelectedKey);
    expect(row.candidates.length).toBe(2);
    const labels = row.candidates.map((c) => c.label).sort();
    expect(labels).toEqual(["alpha", "beta"]);
    for (const c of row.candidates) {
      expect(c.available).toBe(true);
      expect(c.pool).toBe("primary");
      expect(c.priority).toBe(2); // default Normal
      expect(typeof c.sessionBucket).toBe("number");
      expect(typeof c.recentSessions).toBe("number");
    }
  });

  test("affinity hit is recorded after a new assignment; newest first", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.getKeyForConversation("conv-2", "session-x");
    km.getKeyForConversation("conv-2", "session-x");

    const rows = km.getRecentRoutingDecisions();
    expect(rows.length).toBe(2);
    expect(rows[0]!.routingDecision).toBe("conversation_affinity_hit");
    expect(rows[0]!.affinityHit).toBe(true);
    expect(rows[1]!.routingDecision).toBe("conversation_new_assignment");
    expect(rows[1]!.affinityHit).toBe(false);
    // Newest-first ordering on decidedAt
    expect(rows[0]!.decidedAt).toBeGreaterThanOrEqual(rows[1]!.decidedAt);
  });

  test("affinity remap fires when mapped key goes on cooldown", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    // First call pins conv to whichever key the selector picks first.
    const first = km.getKeyForConversation("conv-remap", "sess");
    expect(first.entry).not.toBeNull();
    const firstLabel = first.entry!.label;

    // Force the pinned key onto a long cooldown so the affinity can't be honored.
    const rawEntries = (km as unknown as { keys: ApiKeyEntry[] }).keys;
    const pinned = rawEntries.find((k) => k.label === firstLabel)!;
    km.recordRateLimit(pinned, 9_999);

    km.getKeyForConversation("conv-remap", "sess");

    const rows = km.getRecentRoutingDecisions();
    expect(rows[0]!.routingDecision).toBe("conversation_affinity_remapped");
    expect(rows[0]!.remapped).toBe(true);
    expect(rows[0]!.chosenKeyLabel).not.toBe(firstLabel);
    // The cooldown'd key should be visible in the candidates snapshot as unavailable
    const cooled = rows[0]!.candidates.find((c) => c.label === firstLabel)!;
    expect(cooled.available).toBe(false);
    expect(cooled.availableAt).toBeGreaterThan(Date.now());
  });

  test("global_sticky_fallback (null conversationKey) is recorded", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.getKeyForConversation(null, null);
    const rows = km.getRecentRoutingDecisions();
    expect(rows.length).toBe(1);
    expect(rows[0]!.routingDecision).toBe("global_sticky_fallback");
    expect(rows[0]!.conversationKey).toBeNull();
    expect(rows[0]!.sessionId).toBeNull();
  });

  test("no keys available still persists a row with null chosen key + empty candidates", () => {
    const km = create();
    km.getKeyForConversation("conv-empty", "session-y");
    const rows = km.getRecentRoutingDecisions();
    expect(rows.length).toBe(1);
    expect(rows[0]!.chosenKeyLabel).toBeNull();
    expect(rows[0]!.priorityTier).toBeNull();
    expect(rows[0]!.pool).toBeNull();
    expect(rows[0]!.candidates.length).toBe(0);
  });

  test("candidate snapshot reflects pool, priority, and utilization", () => {
    const km = create();
    const alpha = km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");
    // Put alpha on Fallback tier with 7d util = 72% → should land in tertiary
    km.updateKeyPriority(VALID_KEY_1, 3);
    km.recordCapacityObservation(alpha, {
      seenAt: unixMs(Date.now()),
      httpStatus: 200,
      windows: [{
        windowName: "unified-7d",
        status: "allowed",
        utilization: 0.72,
        resetAt: unixMs(Date.now() + 6 * 24 * 60 * 60 * 1000),
      }],
    });

    km.getKeyForConversation("conv-pool", "s");
    const row = km.getRecentRoutingDecisions()[0]!;
    const alphaSnap = row.candidates.find((c) => c.label === "alpha")!;
    const betaSnap = row.candidates.find((c) => c.label === "beta")!;
    expect(alphaSnap.priority).toBe(3);
    expect(alphaSnap.pool).toBe("tertiary");
    expect(alphaSnap.util7d).toBeCloseTo(0.72);
    expect(alphaSnap.reset7d).not.toBeNull();
    expect(betaSnap.pool).toBe("primary");
    // Beta (Normal, no observations) should be the winner because alpha is in tertiary
    expect(row.chosenKeyLabel).toBe("beta");
  });

  test("candidate snapshot excludes disabled keys", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");
    km.updateKeyPriority(VALID_KEY_2, 4); // Disabled sentinel

    km.getKeyForConversation("conv-disabled", "s");
    const row = km.getRecentRoutingDecisions()[0]!;
    const labels = row.candidates.map((c) => c.label);
    expect(labels).toContain("alpha");
    expect(labels).not.toContain("beta");
  });

  test("candidate snapshot tracks sessionBucket from recent conversations", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    // 4 distinct conversations — 4 recent sessions on the only key.
    for (let i = 0; i < 4; i++) km.getKeyForConversation(`conv-sess-${i}`, `s-${i}`);
    const row = km.getRecentRoutingDecisions()[0]!;
    const alphaSnap = row.candidates.find((c) => c.label === "alpha")!;
    expect(alphaSnap.recentSessions).toBeGreaterThanOrEqual(4);
    expect(alphaSnap.sessionBucket).toBe(Math.floor(alphaSnap.recentSessions / 3));
  });

  test("getRecentRoutingDecisions honors limit and returns newest first", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    for (let i = 0; i < 5; i++) km.getKeyForConversation(`c-${i}`, `s-${i}`);
    const limited = km.getRecentRoutingDecisions(3);
    expect(limited.length).toBe(3);
    // decidedAt monotonically non-increasing
    for (let i = 1; i < limited.length; i++) {
      expect(limited[i - 1]!.decidedAt).toBeGreaterThanOrEqual(limited[i]!.decidedAt);
    }
  });

  test("cleanupOldRoutingDecisions drops only rows past retention, keeps fresh ones", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.getKeyForConversation("c-old", "s-old");
    km.getKeyForConversation("c-fresh", "s-fresh");
    const db = (km as unknown as { db: Database }).db;
    const eightDaysAgo = Date.now() - 8 * 24 * 60 * 60 * 1000;
    const sixDaysAgo = Date.now() - 6 * 24 * 60 * 60 * 1000;
    db.run(
      "UPDATE routing_decisions SET decided_at = ? WHERE conversation_key = 'c-old'",
      [eightDaysAgo],
    );
    db.run(
      "UPDATE routing_decisions SET decided_at = ? WHERE conversation_key = 'c-fresh'",
      [sixDaysAgo],
    );
    (km as unknown as { cleanupOldRoutingDecisions: () => void }).cleanupOldRoutingDecisions();
    const remaining = km.getRecentRoutingDecisions();
    expect(remaining.length).toBe(1);
    expect(remaining[0]!.conversationKey).toBe("c-fresh");
  });

  test("routing_decisions schema and index are created on init", () => {
    create();
    const db = new Database(join(tempDir, "state.db"), { readonly: true });
    const tables = db.query("SELECT name FROM sqlite_master WHERE type='table'").all() as { name: string }[];
    expect(tables.map((t) => t.name)).toContain("routing_decisions");
    const indexes = db.query("SELECT name FROM sqlite_master WHERE type='index'").all() as { name: string }[];
    expect(indexes.map((i) => i.name)).toContain("idx_routing_decisions_decided_at");
    db.close();
  });

  test("conversationCountForSelected tracks affinities on the chosen key", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.getKeyForConversation("conv-A", "sA");
    km.getKeyForConversation("conv-B", "sB");
    const rows = km.getRecentRoutingDecisions();
    // Newest first: conv-B is index 0, conv-A is index 1.
    expect(rows[0]!.conversationKey).toBe("conv-B");
    expect(rows[0]!.conversationCountForSelected).toBe(2);
    expect(rows[1]!.conversationKey).toBe("conv-A");
    expect(rows[1]!.conversationCountForSelected).toBe(1);
  });
});

// ── Short-Cooldown Affinity Passthrough ────────────────────────────────────

describe("Short-Cooldown Affinity Passthrough", () => {
  function rawEntries(km: KeyManager): ApiKeyEntry[] {
    return (km as unknown as { keys: ApiKeyEntry[] }).keys;
  }
  function findEntry(km: KeyManager, label: string): ApiKeyEntry {
    return rawEntries(km).find((k) => k.label === label)!;
  }

  test("short cooldown on pinned key → null entry + passthrough + cooldown ms", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    const first = km.getKeyForConversation("conv-pt", "sess");
    const pinnedLabel = first.entry!.label;

    // 90s cooldown on the pinned key — below the 5-min threshold.
    km.recordRateLimit(findEntry(km, pinnedLabel), 90);

    const second = km.getKeyForConversation("conv-pt", "sess");
    expect(second.entry).toBeNull();
    expect(second.routingDecision).toBe("conversation_affinity_cooldown_passthrough");
    expect(second.affinityHit).toBe(false);
    expect(second.remapped).toBe(false);
    expect(second.cooldownRemainingMs).not.toBeNull();
    expect(second.cooldownRemainingMs!).toBeGreaterThan(85 * 1000);
    expect(second.cooldownRemainingMs!).toBeLessThanOrEqual(90 * 1000);
  });

  test("passthrough does NOT overwrite the affinity — pinned key stays pinned", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    const first = km.getKeyForConversation("conv-stay", "sess");
    const pinned = findEntry(km, first.entry!.label);
    km.recordRateLimit(pinned, 60);
    km.getKeyForConversation("conv-stay", "sess"); // passthrough

    const affinities = (km as unknown as {
      conversationAffinities: Map<string, { key: string }>;
    }).conversationAffinities;
    expect(affinities.get("conv-stay")!.key).toBe(pinned.key);
  });

  test("after the cooldown elapses, the next call is an affinity hit on the same key", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    const first = km.getKeyForConversation("conv-return", "sess");
    const pinned = findEntry(km, first.entry!.label);
    km.recordRateLimit(pinned, 60);
    expect(km.getKeyForConversation("conv-return", "sess").routingDecision)
      .toBe("conversation_affinity_cooldown_passthrough");

    // Simulate time passing — clear the cooldown.
    (pinned as unknown as { availableAt: number }).availableAt = 0;

    const back = km.getKeyForConversation("conv-return", "sess");
    expect(back.routingDecision).toBe("conversation_affinity_hit");
    expect(back.entry!.label).toBe(pinned.label);
  });

  test("long cooldown (30 min) on pinned key → remap, overwrite affinity", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    const first = km.getKeyForConversation("conv-remap-long", "sess");
    const pinnedLabel = first.entry!.label;
    km.recordRateLimit(findEntry(km, pinnedLabel), 30 * 60);

    const second = km.getKeyForConversation("conv-remap-long", "sess");
    expect(second.routingDecision).toBe("conversation_affinity_remapped");
    expect(second.remapped).toBe(true);
    expect(second.entry!.label).not.toBe(pinnedLabel);

    const affinities = (km as unknown as {
      conversationAffinities: Map<string, { key: string }>;
    }).conversationAffinities;
    expect(affinities.get("conv-remap-long")!.key).toBe(second.entry!.key);
  });

  test("solo fleet + short cooldown → passthrough (not remap-to-null)", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.getKeyForConversation("conv-solo", "sess");
    km.recordRateLimit(findEntry(km, "alpha"), 90);

    const second = km.getKeyForConversation("conv-solo", "sess");
    expect(second.entry).toBeNull();
    expect(second.routingDecision).toBe("conversation_affinity_cooldown_passthrough");
    expect(second.cooldownRemainingMs).not.toBeNull();
  });

  test("passthrough decision persists to routing_decisions", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    const first = km.getKeyForConversation("conv-log-pt", "sess");
    km.recordRateLimit(findEntry(km, first.entry!.label), 60);
    km.getKeyForConversation("conv-log-pt", "sess");

    const rows = km.getRecentRoutingDecisions();
    expect(rows[0]!.routingDecision).toBe("conversation_affinity_cooldown_passthrough");
    expect(rows[0]!.chosenKeyLabel).toBeNull();
    expect(rows[0]!.affinityHit).toBe(false);
    expect(rows[0]!.remapped).toBe(false);
  });

  test("cooldown at exactly the 5-min threshold still triggers passthrough", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    const first = km.getKeyForConversation("conv-edge", "sess");
    km.recordRateLimit(findEntry(km, first.entry!.label), 300);
    const second = km.getKeyForConversation("conv-edge", "sess");
    expect(second.routingDecision).toBe("conversation_affinity_cooldown_passthrough");
  });

  test("cooldown one second past threshold triggers a remap", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    const first = km.getKeyForConversation("conv-past-edge", "sess");
    km.recordRateLimit(findEntry(km, first.entry!.label), 301);
    const second = km.getKeyForConversation("conv-past-edge", "sess");
    expect(second.routingDecision).toBe("conversation_affinity_remapped");
  });

  test("disabled pinned key → remap (not passthrough) even with availableAt in past", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    const first = km.getKeyForConversation("conv-disabled-pin", "sess");
    const pinnedLabel = first.entry!.label;
    km.updateKeyPriority(
      pinnedLabel === "alpha" ? VALID_KEY_1 : VALID_KEY_2,
      4, // DISABLED_PRIORITY
    );

    const second = km.getKeyForConversation("conv-disabled-pin", "sess");
    expect(second.routingDecision).toBe("conversation_affinity_remapped");
    expect(second.entry!.label).not.toBe(pinnedLabel);
  });

  test("cooldownRemainingMs is null on all non-passthrough decisions", () => {
    const km = create();
    km.addKey(VALID_KEY_1, "alpha");
    km.addKey(VALID_KEY_2, "beta");

    const newAssign = km.getKeyForConversation("conv-c", "sess");
    expect(newAssign.cooldownRemainingMs).toBeNull();

    const hit = km.getKeyForConversation("conv-c", "sess");
    expect(hit.routingDecision).toBe("conversation_affinity_hit");
    expect(hit.cooldownRemainingMs).toBeNull();

    const sticky = km.getKeyForConversation(null, null);
    expect(sticky.routingDecision).toBe("global_sticky_fallback");
    expect(sticky.cooldownRemainingMs).toBeNull();
  });
});
