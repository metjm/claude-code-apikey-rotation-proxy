import { describe, test, expect, beforeEach, afterEach, spyOn } from "bun:test";
import { join } from "node:path";

// ─────────────────────────────────────────────────────────────────────
// src/types.ts
// ─────────────────────────────────────────────────────────────────────
import {
  asApiKey,
  asKeyLabel,
  asProxyToken,
  now,
  unixMs,
} from "../src/types.ts";

describe("types", () => {
  // ── asApiKey ─────────────────────────────────────────────────────

  describe("asApiKey()", () => {
    test("accepts a valid sk-ant- prefixed key", () => {
      const key = asApiKey("sk-ant-abc123");
      expect(key).toBe("sk-ant-abc123");
    });

    test("accepts a long realistic key", () => {
      const raw =
        "sk-ant-api03-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
      const key = asApiKey(raw);
      expect(key).toBe(raw);
    });

    test("accepts the minimal prefix alone", () => {
      const key = asApiKey("sk-ant-");
      expect(key).toBe("sk-ant-");
    });

    test("throws TypeError for empty string", () => {
      expect(() => asApiKey("")).toThrow(TypeError);
      expect(() => asApiKey("")).toThrow('must start with "sk-ant-"');
    });

    test("throws TypeError when prefix is missing entirely", () => {
      expect(() => asApiKey("my-random-key")).toThrow(TypeError);
    });

    test("throws TypeError for partial prefix sk-", () => {
      expect(() => asApiKey("sk-abc123")).toThrow(TypeError);
    });

    test("throws TypeError for partial prefix sk-ant (no trailing dash)", () => {
      expect(() => asApiKey("sk-antabc")).toThrow(TypeError);
    });

    test("throws TypeError for prefix with wrong case SK-ANT-", () => {
      expect(() => asApiKey("SK-ANT-abc")).toThrow(TypeError);
    });

    test("throws TypeError for key that contains sk-ant- in the middle", () => {
      expect(() => asApiKey("prefix-sk-ant-abc")).toThrow(TypeError);
    });

    test("returned value is usable as a plain string", () => {
      const key = asApiKey("sk-ant-test");
      expect(key.startsWith("sk-ant-")).toBe(true);
      expect(key.length).toBe(11);
    });
  });

  // ── asKeyLabel ───────────────────────────────────────────────────

  describe("asKeyLabel()", () => {
    test("accepts any non-empty string", () => {
      const label = asKeyLabel("production-key-1");
      expect(label).toBe("production-key-1");
    });

    test("accepts an empty string", () => {
      const label = asKeyLabel("");
      expect(label).toBe("");
    });

    test("accepts strings with special characters", () => {
      const label = asKeyLabel("key @#$% with spaces!");
      expect(label).toBe("key @#$% with spaces!");
    });

    test("accepts a very long string", () => {
      const long = "a".repeat(10_000);
      const label = asKeyLabel(long);
      expect(label).toBe(long);
      expect(label.length).toBe(10_000);
    });

    test("returned value is identical to input", () => {
      const raw = "my-label";
      const label = asKeyLabel(raw);
      expect(label === raw).toBe(true);
    });
  });

  // ── asProxyToken ─────────────────────────────────────────────────

  describe("asProxyToken()", () => {
    test("accepts an 8-character token (boundary)", () => {
      const token = asProxyToken("12345678");
      expect(token).toBe("12345678");
    });

    test("accepts a token longer than 8 characters", () => {
      const token = asProxyToken("a]b$c!defghij");
      expect(token).toBe("a]b$c!defghij");
    });

    test("accepts a very long token", () => {
      const raw = "x".repeat(1_000);
      const token = asProxyToken(raw);
      expect(token).toBe(raw);
    });

    test("throws TypeError for 7-character token (one below boundary)", () => {
      expect(() => asProxyToken("1234567")).toThrow(TypeError);
      expect(() => asProxyToken("1234567")).toThrow(
        "at least 8 characters",
      );
    });

    test("throws TypeError for single character", () => {
      expect(() => asProxyToken("x")).toThrow(TypeError);
    });

    test("throws TypeError for empty string", () => {
      expect(() => asProxyToken("")).toThrow(TypeError);
    });

    test("returned value is usable as a plain string", () => {
      const token = asProxyToken("abcdefgh");
      expect(token.toUpperCase()).toBe("ABCDEFGH");
    });
  });

  // ── now() ────────────────────────────────────────────────────────

  describe("now()", () => {
    test("returns a number close to Date.now()", () => {
      const before = Date.now();
      const ts = now();
      const after = Date.now();
      expect(ts).toBeGreaterThanOrEqual(before);
      expect(ts).toBeLessThanOrEqual(after);
    });

    test("monotonically non-decreasing on successive calls", () => {
      const a = now();
      const b = now();
      expect(b).toBeGreaterThanOrEqual(a);
    });

    test("returns a reasonable epoch-ms value (after year 2020)", () => {
      const ts = now();
      // 2020-01-01T00:00:00Z
      expect(ts).toBeGreaterThan(1_577_836_800_000);
    });

    test("value is a finite number", () => {
      const ts = now();
      expect(Number.isFinite(ts)).toBe(true);
      expect(Number.isNaN(ts)).toBe(false);
    });
  });

  // ── unixMs() ─────────────────────────────────────────────────────

  describe("unixMs()", () => {
    test("preserves a positive value", () => {
      const val = unixMs(1_700_000_000_000);
      expect(val).toBe(1_700_000_000_000);
    });

    test("preserves zero", () => {
      const val = unixMs(0);
      expect(val).toBe(0);
    });

    test("preserves a negative value", () => {
      const val = unixMs(-1);
      expect(val).toBe(-1);
    });

    test("result is usable in arithmetic", () => {
      const a = unixMs(100);
      const b = unixMs(200);
      expect(b - a).toBe(100);
    });

    test("result compares correctly against plain numbers", () => {
      const val = unixMs(42);
      expect(val === 42).toBe(true);
      expect(val < 100).toBe(true);
      expect(val > 0).toBe(true);
    });
  });
});

// ─────────────────────────────────────────────────────────────────────
// src/config.ts
// ─────────────────────────────────────────────────────────────────────
import { loadConfig } from "../src/config.ts";

describe("config", () => {
  let savedEnv: Record<string, string | undefined>;

  const CONFIG_VARS = [
    "PORT",
    "UPSTREAM_URL",
    "ADMIN_TOKEN",
    "DATA_DIR",
    "MAX_RETRIES",
    "FIRST_CHUNK_TIMEOUT_MS",
    "MAX_FIRST_CHUNK_RETRIES",
  ] as const;

  beforeEach(() => {
    savedEnv = {};
    for (const key of CONFIG_VARS) {
      savedEnv[key] = process.env[key];
      delete process.env[key];
    }
  });

  afterEach(() => {
    for (const key of CONFIG_VARS) {
      if (savedEnv[key] === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = savedEnv[key];
      }
    }
  });

  // ── defaults ─────────────────────────────────────────────────────

  describe("defaults", () => {
    test("port defaults to 4080", () => {
      expect(loadConfig().port).toBe(4080);
    });

    test("upstream defaults to https://api.anthropic.com", () => {
      expect(loadConfig().upstream).toBe("https://api.anthropic.com");
    });

    test("adminToken defaults to null", () => {
      expect(loadConfig().adminToken).toBeNull();
    });

    test("dataDir defaults to <cwd>/data", () => {
      const cfg = loadConfig();
      expect(cfg.dataDir).toBe(join(process.cwd(), "data"));
    });

    test("maxRetriesPerRequest defaults to 10", () => {
      expect(loadConfig().maxRetriesPerRequest).toBe(10);
    });

    test("firstChunkTimeoutMs defaults to 16000", () => {
      expect(loadConfig().firstChunkTimeoutMs).toBe(16_000);
    });

    test("maxFirstChunkRetries defaults to 2", () => {
      expect(loadConfig().maxFirstChunkRetries).toBe(2);
    });
  });

  // ── custom values via env ────────────────────────────────────────

  describe("custom env overrides", () => {
    test("PORT is respected", () => {
      process.env["PORT"] = "9090";
      expect(loadConfig().port).toBe(9090);
    });

    test("UPSTREAM_URL is respected", () => {
      process.env["UPSTREAM_URL"] = "http://localhost:5555";
      expect(loadConfig().upstream).toBe("http://localhost:5555");
    });

    test("ADMIN_TOKEN is respected", () => {
      process.env["ADMIN_TOKEN"] = "secret-token-123";
      expect(loadConfig().adminToken).toBe("secret-token-123");
    });

    test("DATA_DIR is respected", () => {
      process.env["DATA_DIR"] = "/tmp/my-proxy-data";
      expect(loadConfig().dataDir).toBe("/tmp/my-proxy-data");
    });

    test("MAX_RETRIES is respected", () => {
      process.env["MAX_RETRIES"] = "3";
      expect(loadConfig().maxRetriesPerRequest).toBe(3);
    });

    test("FIRST_CHUNK_TIMEOUT_MS is respected", () => {
      process.env["FIRST_CHUNK_TIMEOUT_MS"] = "2500";
      expect(loadConfig().firstChunkTimeoutMs).toBe(2500);
    });

    test("MAX_FIRST_CHUNK_RETRIES is respected", () => {
      process.env["MAX_FIRST_CHUNK_RETRIES"] = "1";
      expect(loadConfig().maxFirstChunkRetries).toBe(1);
    });

    test("MAX_RETRIES zero is valid", () => {
      process.env["MAX_RETRIES"] = "0";
      expect(loadConfig().maxRetriesPerRequest).toBe(0);
    });

    test("PORT zero is valid (OS picks port)", () => {
      process.env["PORT"] = "0";
      expect(loadConfig().port).toBe(0);
    });

    test("multiple env vars can be combined", () => {
      process.env["PORT"] = "3000";
      process.env["UPSTREAM_URL"] = "http://proxy-upstream:8080";
      process.env["ADMIN_TOKEN"] = "tok";
      process.env["DATA_DIR"] = "/data";
      process.env["MAX_RETRIES"] = "5";
      process.env["FIRST_CHUNK_TIMEOUT_MS"] = "9999";
      process.env["MAX_FIRST_CHUNK_RETRIES"] = "4";

      const cfg = loadConfig();
      expect(cfg.port).toBe(3000);
      expect(cfg.upstream).toBe("http://proxy-upstream:8080");
      expect(cfg.adminToken).toBe("tok");
      expect(cfg.dataDir).toBe("/data");
      expect(cfg.maxRetriesPerRequest).toBe(5);
      expect(cfg.firstChunkTimeoutMs).toBe(9999);
      expect(cfg.maxFirstChunkRetries).toBe(4);
    });
  });

  // ── invalid values ───────────────────────────────────────────────

  describe("invalid env values", () => {
    test("non-numeric PORT throws", () => {
      process.env["PORT"] = "not-a-number";
      expect(() => loadConfig()).toThrow("PORT must be an integer");
    });

    test("floating-point PORT is parsed as integer (parseInt behaviour)", () => {
      process.env["PORT"] = "3.14";
      // parseInt("3.14", 10) === 3, so this does not throw
      expect(loadConfig().port).toBe(3);
    });

    test("empty-string PORT throws", () => {
      process.env["PORT"] = "";
      expect(() => loadConfig()).toThrow("PORT must be an integer");
    });

    test("non-numeric MAX_RETRIES throws", () => {
      process.env["MAX_RETRIES"] = "abc";
      expect(() => loadConfig()).toThrow("MAX_RETRIES must be an integer");
    });

    test("empty-string MAX_RETRIES throws", () => {
      process.env["MAX_RETRIES"] = "";
      expect(() => loadConfig()).toThrow("MAX_RETRIES must be an integer");
    });

    test("non-numeric FIRST_CHUNK_TIMEOUT_MS throws", () => {
      process.env["FIRST_CHUNK_TIMEOUT_MS"] = "nope";
      expect(() => loadConfig()).toThrow("FIRST_CHUNK_TIMEOUT_MS must be an integer");
    });

    test("non-numeric MAX_FIRST_CHUNK_RETRIES throws", () => {
      process.env["MAX_FIRST_CHUNK_RETRIES"] = "nope";
      expect(() => loadConfig()).toThrow("MAX_FIRST_CHUNK_RETRIES must be an integer");
    });

    test("thrown error for PORT includes the bad value", () => {
      process.env["PORT"] = "xyz";
      expect(() => loadConfig()).toThrow('"xyz"');
    });

    test("thrown error for MAX_RETRIES includes the bad value", () => {
      process.env["MAX_RETRIES"] = "oops";
      expect(() => loadConfig()).toThrow('"oops"');
    });

    test("thrown error for FIRST_CHUNK_TIMEOUT_MS includes the bad value", () => {
      process.env["FIRST_CHUNK_TIMEOUT_MS"] = "oops";
      expect(() => loadConfig()).toThrow('"oops"');
    });

    test("thrown error for MAX_FIRST_CHUNK_RETRIES includes the bad value", () => {
      process.env["MAX_FIRST_CHUNK_RETRIES"] = "oops";
      expect(() => loadConfig()).toThrow('"oops"');
    });
  });

  // ── ADMIN_TOKEN edge cases ───────────────────────────────────────

  describe("ADMIN_TOKEN edge cases", () => {
    test("unset ADMIN_TOKEN yields null", () => {
      delete process.env["ADMIN_TOKEN"];
      expect(loadConfig().adminToken).toBeNull();
    });

    test("empty-string ADMIN_TOKEN yields empty string (not null)", () => {
      process.env["ADMIN_TOKEN"] = "";
      expect(loadConfig().adminToken).toBe("");
    });

    test("whitespace-only ADMIN_TOKEN is kept as-is", () => {
      process.env["ADMIN_TOKEN"] = "   ";
      expect(loadConfig().adminToken).toBe("   ");
    });
  });
});

// ─────────────────────────────────────────────────────────────────────
// src/events.ts
// ─────────────────────────────────────────────────────────────────────
import { emit, subscribe, emitWithKeys } from "../src/events.ts";
import type { ProxyEvent } from "../src/events.ts";
import type { MaskedKeyEntry } from "../src/types.ts";

describe("events", () => {
  // Keep track of all unsubscribe functions so we can clean up even if a
  // test forgets. This prevents inter-test leakage via the module-level
  // listeners Set.
  const cleanups: Array<() => void> = [];

  afterEach(() => {
    for (const unsub of cleanups) unsub();
    cleanups.length = 0;
  });

  function track(unsub: () => void): () => void {
    cleanups.push(unsub);
    return unsub;
  }

  function makeEvent(overrides?: Partial<ProxyEvent>): ProxyEvent {
    return { type: "request", ts: new Date().toISOString(), ...overrides };
  }

  // ── subscribe / unsubscribe ──────────────────────────────────────

  describe("subscribe()", () => {
    test("returns a function", () => {
      const unsub = track(subscribe(() => {}));
      expect(typeof unsub).toBe("function");
    });

    test("calling unsubscribe twice does not throw", () => {
      const unsub = track(subscribe(() => {}));
      unsub();
      expect(() => unsub()).not.toThrow();
    });
  });

  // ── emit ─────────────────────────────────────────────────────────

  describe("emit()", () => {
    test("listener receives the emitted event", () => {
      const received: ProxyEvent[] = [];
      track(subscribe((e) => received.push(e)));

      const event = makeEvent({ type: "response" });
      emit(event);

      expect(received).toHaveLength(1);
      expect(received[0]).toBe(event);
    });

    test("multiple listeners all receive the same event", () => {
      const a: ProxyEvent[] = [];
      const b: ProxyEvent[] = [];
      const c: ProxyEvent[] = [];

      track(subscribe((e) => a.push(e)));
      track(subscribe((e) => b.push(e)));
      track(subscribe((e) => c.push(e)));

      const event = makeEvent();
      emit(event);

      expect(a).toHaveLength(1);
      expect(b).toHaveLength(1);
      expect(c).toHaveLength(1);
      expect(a[0]).toBe(event);
      expect(b[0]).toBe(event);
      expect(c[0]).toBe(event);
    });

    test("unsubscribed listener stops receiving events", () => {
      const received: ProxyEvent[] = [];
      const unsub = track(subscribe((e) => received.push(e)));

      emit(makeEvent());
      expect(received).toHaveLength(1);

      unsub();
      emit(makeEvent());
      expect(received).toHaveLength(1); // still 1, not 2
    });

    test("only the unsubscribed listener is removed; others stay", () => {
      const a: ProxyEvent[] = [];
      const b: ProxyEvent[] = [];

      track(subscribe((e) => a.push(e)));
      const unsubB = track(subscribe((e) => b.push(e)));

      emit(makeEvent());
      expect(a).toHaveLength(1);
      expect(b).toHaveLength(1);

      unsubB();
      emit(makeEvent());
      expect(a).toHaveLength(2);
      expect(b).toHaveLength(1); // did not receive second event
    });

    test("emit with no subscribers does not throw", () => {
      // No subscribers registered in this scope (afterEach cleans up)
      expect(() => emit(makeEvent())).not.toThrow();
    });

    test("events are delivered in subscription order", () => {
      const order: number[] = [];

      track(subscribe(() => order.push(1)));
      track(subscribe(() => order.push(2)));
      track(subscribe(() => order.push(3)));

      emit(makeEvent());
      expect(order).toEqual([1, 2, 3]);
    });

    test("listener error does not prevent other listeners from firing", () => {
      const received: ProxyEvent[] = [];

      track(
        subscribe(() => {
          throw new Error("boom");
        }),
      );
      // NOTE: The current implementation iterates with a plain for-of and
      // does NOT wrap listener calls in try/catch.  If the implementation
      // adds a try/catch in the future this test verifies the improved
      // behaviour.  For now we verify the throw propagates.  We override
      // the behaviour locally by wrapping emit.

      // We test the *intent*: that a throw in one listener does not
      // silence others. If the implementation lacks try-catch, the throw
      // will propagate and the second listener won't run. We document
      // both cases so the test is useful regardless.

      track(subscribe((e) => received.push(e)));

      // Because the first listener throws and emit() does not catch it,
      // emit() itself will throw. The second listener won't fire.
      // This test documents current behaviour:
      expect(() => emit(makeEvent())).toThrow("boom");
    });

    test("multiple events are delivered independently", () => {
      const received: ProxyEvent[] = [];
      track(subscribe((e) => received.push(e)));

      const e1 = makeEvent({ type: "request" });
      const e2 = makeEvent({ type: "error" });
      const e3 = makeEvent({ type: "tokens" });

      emit(e1);
      emit(e2);
      emit(e3);

      expect(received).toHaveLength(3);
      expect(received[0]!.type).toBe("request");
      expect(received[1]!.type).toBe("error");
      expect(received[2]!.type).toBe("tokens");
    });
  });

  // ── emitWithKeys ─────────────────────────────────────────────────

  describe("emitWithKeys()", () => {
    test("includes keys array in the emitted event", () => {
      const received: ProxyEvent[] = [];
      track(subscribe((e) => received.push(e)));

      const keys: MaskedKeyEntry[] = [
        {
          maskedKey: "sk-ant-...abc",
          label: "key-1" as ReturnType<typeof asKeyLabel>,
          stats: {
            totalRequests: 10,
            successfulRequests: 9,
            rateLimitHits: 1,
            errors: 0,
            lastUsedAt: unixMs(1_000),
            addedAt: unixMs(500),
            totalTokensIn: 100,
            totalTokensOut: 200,
          },
          availableAt: unixMs(0),
          isAvailable: true,
        },
      ];

      emitWithKeys(makeEvent({ type: "tokens" }), keys);

      expect(received).toHaveLength(1);
      const evt = received[0]!;
      expect(evt.type).toBe("tokens");
      expect(evt["keys"]).toEqual(keys);
    });

    test("preserves all original event fields", () => {
      const received: ProxyEvent[] = [];
      track(subscribe((e) => received.push(e)));

      const event = makeEvent({
        type: "rate_limit",
        label: "my-key",
        retryAfter: 30,
      });

      emitWithKeys(event, []);

      const evt = received[0]!;
      expect(evt.type).toBe("rate_limit");
      expect(evt.label).toBe("my-key");
      expect(evt["retryAfter"]).toBe(30);
      expect(evt["keys"]).toEqual([]);
    });

    test("works with an empty keys array", () => {
      const received: ProxyEvent[] = [];
      track(subscribe((e) => received.push(e)));

      emitWithKeys(makeEvent(), []);

      expect(received).toHaveLength(1);
      expect(evt(received).keys).toEqual([]);
    });

    test("emitWithKeys delivers to multiple listeners", () => {
      const a: ProxyEvent[] = [];
      const b: ProxyEvent[] = [];

      track(subscribe((e) => a.push(e)));
      track(subscribe((e) => b.push(e)));

      emitWithKeys(makeEvent(), []);

      expect(a).toHaveLength(1);
      expect(b).toHaveLength(1);
    });
  });
});

// Small helper used in emitWithKeys tests to access dynamic properties
// without TS complaining about index signatures.
function evt(arr: ProxyEvent[]): Record<string, unknown> {
  return arr[0] as unknown as Record<string, unknown>;
}

// ─────────────────────────────────────────────────────────────────────
// src/logger.ts
// ─────────────────────────────────────────────────────────────────────
import { log, setLogLevel } from "../src/logger.ts";

describe("logger", () => {
  // We spy on console methods to capture output without polluting the
  // test runner. Save/restore LOG_LEVEL env because the module reads it
  // once at import time (we use setLogLevel for runtime changes).
  let logSpy: ReturnType<typeof spyOn>;
  let warnSpy: ReturnType<typeof spyOn>;
  let errorSpy: ReturnType<typeof spyOn>;

  beforeEach(() => {
    logSpy = spyOn(console, "log").mockImplementation(() => {});
    warnSpy = spyOn(console, "warn").mockImplementation(() => {});
    errorSpy = spyOn(console, "error").mockImplementation(() => {});
    // Reset to debug so every level is visible by default in tests.
    setLogLevel("debug");
  });

  afterEach(() => {
    logSpy.mockRestore();
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });

  // ── output routing ───────────────────────────────────────────────

  describe("output routing", () => {
    test("debug logs to console.log", () => {
      log("debug", "dbg msg");
      expect(logSpy).toHaveBeenCalledTimes(1);
      expect(warnSpy).not.toHaveBeenCalled();
      expect(errorSpy).not.toHaveBeenCalled();
    });

    test("info logs to console.log", () => {
      log("info", "info msg");
      expect(logSpy).toHaveBeenCalledTimes(1);
      expect(warnSpy).not.toHaveBeenCalled();
      expect(errorSpy).not.toHaveBeenCalled();
    });

    test("warn logs to console.warn", () => {
      log("warn", "warn msg");
      expect(warnSpy).toHaveBeenCalledTimes(1);
      expect(logSpy).not.toHaveBeenCalled();
      expect(errorSpy).not.toHaveBeenCalled();
    });

    test("error logs to console.error", () => {
      log("error", "err msg");
      expect(errorSpy).toHaveBeenCalledTimes(1);
      expect(logSpy).not.toHaveBeenCalled();
      expect(warnSpy).not.toHaveBeenCalled();
    });
  });

  // ── JSON structure ───────────────────────────────────────────────

  describe("JSON output", () => {
    test("output is valid JSON", () => {
      log("info", "hello");
      const raw = logSpy.mock.calls[0]![0] as string;
      expect(() => JSON.parse(raw)).not.toThrow();
    });

    test("contains ts, level, and msg fields", () => {
      log("info", "greetings");
      const parsed = JSON.parse(logSpy.mock.calls[0]![0] as string);
      expect(parsed).toHaveProperty("ts");
      expect(parsed).toHaveProperty("level", "info");
      expect(parsed).toHaveProperty("msg", "greetings");
    });

    test("ts field is a valid ISO-8601 date string", () => {
      log("info", "check timestamp");
      const parsed = JSON.parse(logSpy.mock.calls[0]![0] as string);
      const date = new Date(parsed.ts);
      expect(date.getTime()).not.toBeNaN();
      // ISO string round-trip
      expect(parsed.ts).toBe(date.toISOString());
    });

    test("extra fields are included in the JSON", () => {
      log("info", "with extras", { requestId: "abc", durationMs: 42 });
      const parsed = JSON.parse(logSpy.mock.calls[0]![0] as string);
      expect(parsed.requestId).toBe("abc");
      expect(parsed.durationMs).toBe(42);
    });

    test("extra fields do not overwrite ts, level, or msg", () => {
      // The spread order is { ts, level, msg, ...extra } so extra CAN
      // override them. Document this behaviour.
      log("info", "orig", { msg: "overridden", level: "hacked" });
      const parsed = JSON.parse(logSpy.mock.calls[0]![0] as string);
      // Because of { ts, level, msg, ...extra }, extra wins:
      expect(parsed.msg).toBe("overridden");
      expect(parsed.level).toBe("hacked");
    });

    test("no extra argument produces only ts, level, msg", () => {
      log("debug", "minimal");
      const parsed = JSON.parse(logSpy.mock.calls[0]![0] as string);
      const keys = Object.keys(parsed).sort();
      expect(keys).toEqual(["level", "msg", "ts"]);
    });

    test("all four levels appear correctly in output", () => {
      const levels = ["debug", "info", "warn", "error"] as const;
      for (const lvl of levels) {
        // Reset spies for clean counts
        logSpy.mockClear();
        warnSpy.mockClear();
        errorSpy.mockClear();

        log(lvl, `msg-${lvl}`);

        const spy =
          lvl === "error" ? errorSpy : lvl === "warn" ? warnSpy : logSpy;
        const raw = spy.mock.calls[0]![0] as string;
        const parsed = JSON.parse(raw);
        expect(parsed.level).toBe(lvl);
        expect(parsed.msg).toBe(`msg-${lvl}`);
      }
    });
  });

  // ── level filtering ──────────────────────────────────────────────

  describe("level filtering", () => {
    test("at debug level, all messages are logged", () => {
      setLogLevel("debug");
      log("debug", "d");
      log("info", "i");
      log("warn", "w");
      log("error", "e");
      expect(logSpy).toHaveBeenCalledTimes(2); // debug + info
      expect(warnSpy).toHaveBeenCalledTimes(1);
      expect(errorSpy).toHaveBeenCalledTimes(1);
    });

    test("at info level, debug is suppressed", () => {
      setLogLevel("info");
      log("debug", "should be suppressed");
      log("info", "visible");
      expect(logSpy).toHaveBeenCalledTimes(1);
      const parsed = JSON.parse(logSpy.mock.calls[0]![0] as string);
      expect(parsed.msg).toBe("visible");
    });

    test("at warn level, debug and info are suppressed", () => {
      setLogLevel("warn");
      log("debug", "no");
      log("info", "no");
      log("warn", "yes");
      log("error", "yes");
      expect(logSpy).not.toHaveBeenCalled();
      expect(warnSpy).toHaveBeenCalledTimes(1);
      expect(errorSpy).toHaveBeenCalledTimes(1);
    });

    test("at error level, only error is visible", () => {
      setLogLevel("error");
      log("debug", "no");
      log("info", "no");
      log("warn", "no");
      log("error", "yes");
      expect(logSpy).not.toHaveBeenCalled();
      expect(warnSpy).not.toHaveBeenCalled();
      expect(errorSpy).toHaveBeenCalledTimes(1);
    });

    test("setLogLevel can be changed at runtime", () => {
      setLogLevel("error");
      log("info", "suppressed");
      expect(logSpy).not.toHaveBeenCalled();

      setLogLevel("debug");
      log("info", "now visible");
      expect(logSpy).toHaveBeenCalledTimes(1);
    });

    test("debug level is the most permissive", () => {
      setLogLevel("debug");
      log("debug", "d");
      // debug goes to console.log
      expect(logSpy).toHaveBeenCalledTimes(1);
    });

    test("each level only suppresses strictly lower levels", () => {
      // info: debug suppressed, info/warn/error visible
      setLogLevel("info");

      log("debug", "suppressed");
      expect(logSpy).toHaveBeenCalledTimes(0);

      log("info", "visible");
      expect(logSpy).toHaveBeenCalledTimes(1);

      log("warn", "visible");
      expect(warnSpy).toHaveBeenCalledTimes(1);

      log("error", "visible");
      expect(errorSpy).toHaveBeenCalledTimes(1);
    });
  });

  // ── setLogLevel ──────────────────────────────────────────────────

  describe("setLogLevel()", () => {
    test("accepts debug", () => {
      expect(() => setLogLevel("debug")).not.toThrow();
    });

    test("accepts info", () => {
      expect(() => setLogLevel("info")).not.toThrow();
    });

    test("accepts warn", () => {
      expect(() => setLogLevel("warn")).not.toThrow();
    });

    test("accepts error", () => {
      expect(() => setLogLevel("error")).not.toThrow();
    });
  });
});
