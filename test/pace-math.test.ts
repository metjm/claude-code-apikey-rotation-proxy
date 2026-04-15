import { describe, test, expect } from "bun:test";
import { PaceMath } from "../public/pace.js";

// Real-data fixture based on a snapshot taken 2026-04-14 ~12:45 UTC from the
// production database. Used to assert the math matches operator intuition on
// real keys, not just synthetic cases.
const NOW = 1776170704339;
const FIVE_H = 5 * 60 * 60 * 1000;
const SEVEN_D = 7 * 24 * 60 * 60 * 1000;

function mkWindow(
  windowName: string,
  utilization: number | null,
  resetAt: number | null,
) {
  return {
    windowName,
    status: "allowed" as const,
    utilization,
    resetAt,
    surpassedThreshold: windowName === "unified-5h" ? 0.9 : 0.75,
    lastSeenAt: NOW,
  };
}

function mkKey(
  label: string,
  priority: number,
  windows: ReturnType<typeof mkWindow>[],
  isAvailable = true,
) {
  return {
    maskedKey: "sk-ant-***" + label,
    label,
    priority,
    isAvailable,
    availableAt: 0,
    allowedDays: [0, 1, 2, 3, 4, 5, 6],
    stats: {
      totalRequests: 0,
      successfulRequests: 0,
      rateLimitHits: 0,
      errors: 0,
      lastUsedAt: null,
      addedAt: NOW,
      totalTokensIn: 0,
      totalTokensOut: 0,
      totalCacheRead: 0,
      totalCacheCreation: 0,
    },
    capacity: {
      responseCount: 0,
      normalizedHeaderCount: 0,
      lastResponseAt: NOW,
      lastHeaderAt: NOW,
      lastUpstreamStatus: 200,
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
      windows,
    },
    capacityHealth: "healthy" as const,
    recentErrors: 0,
    recentSessions15m: [],
  };
}

// Real-data snapshot — all four keys coasting.
const realMailTill = mkKey("mail@till.dev", 2, [
  mkWindow("unified-5h", 0.03, 1776186000000), // reset in 4.25h
  mkWindow("unified-7d", 0.36, 1776171600000), // reset in 15 min — near reset
]);
const realOnegc = mkKey("till@onegc.com", 1, [
  mkWindow("unified-5h", 0.19, 1776171600000), // reset in 15 min
  mkWindow("unified-7d", 0.45, 1776196800000), // reset in 7h15m
]);
const realTrainly = mkKey("till@trainly.ai", 1, [
  mkWindow("unified-5h", 0.00, 1776178800000), // reset in 2h15m
  mkWindow("unified-7d", 0.20, 1776330000000), // reset in 44h
]);
const realGabe = mkKey("gabe@personal", 3, [
  mkWindow("unified-5h", 0.37, 1776175200000), // reset in 1h15m
  mkWindow("unified-7d", 0.08, 1776693600000), // reset in 6 days
]);

// ── computeWindowPace ──────────────────────────────────────────────────────

describe("computeWindowPace", () => {
  test("real data: mail@till.dev 5h — 3% util, 15% elapsed → dim (behind)", () => {
    const r = PaceMath.computeWindowPace({
      utilization: 0.03,
      resetAt: 1776186000000,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.utilization).toBeCloseTo(0.03, 6);
    expect(r.elapsedFrac).toBeCloseTo(0.15, 2);
    expect(r.paceRatio).toBeCloseTo(0.2, 1);
    expect(r.tone).toBe("dim"); // pace 0.2 < 0.85
    expect(r.deadZone).toBe(false);
  });

  test("real data: till@onegc 7d — 45% util, 95.7% elapsed → dim (behind, safe)", () => {
    const r = PaceMath.computeWindowPace({
      utilization: 0.45,
      resetAt: 1776196800000,
      windowName: "unified-7d",
      now: NOW,
    });
    expect(r.elapsedFrac).toBeCloseTo(0.957, 2);
    expect(r.paceRatio).toBeCloseTo(0.47, 2);
    expect(r.tone).toBe("dim");
  });

  test("on pace: util=0.5, elapsed=0.5 → green (ratio=1.0)", () => {
    const r = PaceMath.computeWindowPace({
      utilization: 0.5,
      resetAt: NOW + FIVE_H / 2,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.paceRatio).toBeCloseTo(1.0, 2);
    expect(r.tone).toBe("green");
  });

  test("yellow tone: pace ratio 1.3", () => {
    const r = PaceMath.computeWindowPace({
      utilization: 0.65,
      resetAt: NOW + FIVE_H / 2,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.paceRatio).toBeCloseTo(1.3, 2);
    expect(r.tone).toBe("yellow");
  });

  test("orange tone: pace ratio 1.6", () => {
    const r = PaceMath.computeWindowPace({
      utilization: 0.8,
      resetAt: NOW + FIVE_H / 2,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.paceRatio).toBeCloseTo(1.6, 2);
    expect(r.tone).toBe("orange");
  });

  test("red tone: pace ratio 3.0", () => {
    const r = PaceMath.computeWindowPace({
      utilization: 0.9,
      resetAt: NOW + FIVE_H * 0.7,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.paceRatio).toBeCloseTo(3.0, 1);
    expect(r.tone).toBe("red");
  });

  test("dead zone: elapsed < 2% → dim even with high util", () => {
    // elapsed = 0.01 (1%), util = 0.9 → pace = 90 but still dim
    const r = PaceMath.computeWindowPace({
      utilization: 0.9,
      resetAt: NOW + FIVE_H * 0.99,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.elapsedFrac).toBeCloseTo(0.01, 2);
    expect(r.tone).toBe("dim");
    expect(r.deadZone).toBe(true);
  });

  test("dead zone: util < 0.01 → dim", () => {
    const r = PaceMath.computeWindowPace({
      utilization: 0.005,
      resetAt: NOW + FIVE_H / 2,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.tone).toBe("dim");
    expect(r.deadZone).toBe(true);
  });

  test("dead zone: absolute elapsed < 60s → dim", () => {
    // 7d window, elapsed = 30 seconds → fraction 4.96e-8 but also < 60s wall
    const r = PaceMath.computeWindowPace({
      utilization: 0.5,
      resetAt: NOW + (SEVEN_D - 30 * 1000),
      windowName: "unified-7d",
      now: NOW,
    });
    expect(r.tone).toBe("dim");
    expect(r.deadZone).toBe(true);
  });

  test("null utilization → dim", () => {
    const r = PaceMath.computeWindowPace({
      utilization: null,
      resetAt: NOW + FIVE_H / 2,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.utilization).toBeNull();
    expect(r.tone).toBe("dim");
    expect(r.deadZone).toBe(true);
  });

  test("null resetAt → elapsedFrac null, tone dim", () => {
    const r = PaceMath.computeWindowPace({
      utilization: 0.5,
      resetAt: null,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.elapsedFrac).toBeNull();
    expect(r.paceRatio).toBeNull();
    expect(r.tone).toBe("dim");
  });

  test("resetAt in past → elapsedFrac clamped to 1.0", () => {
    const r = PaceMath.computeWindowPace({
      utilization: 0.5,
      resetAt: NOW - FIVE_H, // reset already passed
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.elapsedFrac).toBe(1.0);
    expect(r.paceRatio).toBeCloseTo(0.5, 2);
    expect(r.tone).toBe("dim"); // 0.5 < 0.85
  });

  test("unknown window name → elapsedFrac null, tone dim", () => {
    const r = PaceMath.computeWindowPace({
      utilization: 0.5,
      resetAt: NOW + FIVE_H,
      windowName: "bogus-window",
      now: NOW,
    });
    expect(r.elapsedFrac).toBeNull();
    expect(r.tone).toBe("dim");
  });

  test("utilization clamped to 0..1", () => {
    const over = PaceMath.computeWindowPace({
      utilization: 1.5,
      resetAt: NOW + FIVE_H / 2,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(over.utilization).toBe(1.0);
    const under = PaceMath.computeWindowPace({
      utilization: -0.1,
      resetAt: NOW + FIVE_H / 2,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(under.utilization).toBe(0);
  });

  test("projected util at reset: linear projection", () => {
    // At 20% elapsed with 40% util → projected 40/20 * 100 = 2.0, clamped to 2.0
    const r = PaceMath.computeWindowPace({
      utilization: 0.4,
      resetAt: NOW + FIVE_H * 0.8,
      windowName: "unified-5h",
      now: NOW,
    });
    expect(r.projectedUtilAtReset).toBeCloseTo(2.0, 1);
  });

  test("tone rank matches tone name", () => {
    const toneCases = [
      { util: 0.005, elapsed: 0.5, expected: "dim" },   // dead zone
      { util: 0.5, elapsed: 0.5, expected: "green" },
      { util: 0.65, elapsed: 0.5, expected: "yellow" },
      { util: 0.8, elapsed: 0.5, expected: "orange" },
      { util: 0.9, elapsed: 0.3, expected: "red" },
    ];
    const expectedRanks = { dim: 0, green: 1, yellow: 2, orange: 3, red: 4 };
    for (const c of toneCases) {
      const r = PaceMath.computeWindowPace({
        utilization: c.util,
        resetAt: NOW + FIVE_H * (1 - c.elapsed),
        windowName: "unified-5h",
        now: NOW,
      });
      expect(r.tone).toBe(c.expected);
      expect(r.toneRank).toBe(expectedRanks[c.expected as keyof typeof expectedRanks]);
    }
  });
});

// ── computePoolAggregate ───────────────────────────────────────────────────

describe("computePoolAggregate", () => {
  const realKeys = [realMailTill, realOnegc, realTrainly, realGabe];

  test("real data: 5h aggregate — all coasting → dim", () => {
    const summary = {
      windowName: "unified-5h",
      maxUtilization: 0.37,
      medianUtilization: 0.11,
      nextResetAt: 1776171600000,
      knownKeys: 4,
      allowedKeys: 4,
      warningKeys: 0,
      rejectedKeys: 0,
    };
    const r = PaceMath.computePoolAggregate(summary, realKeys, "unified-5h", NOW);
    expect(r.windowName).toBe("unified-5h");
    expect(r.keysReporting).toBe(4);
    expect(r.meanUtil).toBeCloseTo((0.03 + 0.19 + 0.0 + 0.37) / 4, 4);
    expect(r.maxUtil).toBeCloseTo(0.37, 4);
    expect(r.medianUtil).toBeCloseTo(0.11, 4);
    expect(r.tone).toBe("dim"); // aggregate pace ~0.15/0.95 = 0.16 → dim
  });

  test("real data: 7d aggregate → dim (fleet coasting)", () => {
    const summary = {
      windowName: "unified-7d",
      maxUtilization: 0.45,
      medianUtilization: 0.28,
      nextResetAt: 1776171600000,
      knownKeys: 4,
      allowedKeys: 4,
      warningKeys: 0,
      rejectedKeys: 0,
    };
    const r = PaceMath.computePoolAggregate(summary, realKeys, "unified-7d", NOW);
    expect(r.meanUtil).toBeCloseTo((0.36 + 0.45 + 0.20 + 0.08) / 4, 4);
    expect(r.tone).toBe("dim");
  });

  test("synthetic: all keys on pace → aggregate green", () => {
    const onPaceKeys = [
      mkKey("a", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]),
      mkKey("b", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]),
      mkKey("c", 2, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]),
    ];
    const summary = {
      windowName: "unified-5h",
      maxUtilization: 0.5,
      medianUtilization: 0.5,
      nextResetAt: NOW + FIVE_H / 2,
      knownKeys: 3,
      allowedKeys: 3,
      warningKeys: 0,
      rejectedKeys: 0,
    };
    const r = PaceMath.computePoolAggregate(summary, onPaceKeys, "unified-5h", NOW);
    expect(r.meanUtil).toBeCloseTo(0.5, 4);
    expect(r.tone).toBe("green");
  });

  test("synthetic: ahead-of-pace fleet → aggregate red", () => {
    const hotKeys = [
      mkKey("a", 1, [mkWindow("unified-5h", 0.9, NOW + FIVE_H * 0.7)]),
      mkKey("b", 1, [mkWindow("unified-5h", 0.95, NOW + FIVE_H * 0.7)]),
    ];
    const r = PaceMath.computePoolAggregate(null, hotKeys, "unified-5h", NOW);
    expect(r.tone).toBe("red");
  });

  test("empty keys → null values, tone dim", () => {
    const r = PaceMath.computePoolAggregate(null, [], "unified-5h", NOW);
    expect(r.meanUtil).toBeNull();
    expect(r.maxUtil).toBeNull();
    expect(r.aggregatePace).toBeNull();
    expect(r.tone).toBe("dim");
    expect(r.keysReporting).toBe(0);
  });

  test("missing summaryWindow → derives max and median from keys", () => {
    const keys = [
      mkKey("a", 1, [mkWindow("unified-5h", 0.1, NOW + FIVE_H / 2)]),
      mkKey("b", 1, [mkWindow("unified-5h", 0.8, NOW + FIVE_H / 2)]),
    ];
    const r = PaceMath.computePoolAggregate(null, keys, "unified-5h", NOW);
    expect(r.maxUtil).toBeCloseTo(0.8, 4);
    expect(r.medianUtil).toBeCloseTo(0.45, 4); // average of 0.1 and 0.8
  });

  test("summaryWindow maxUtilization wins over derived max", () => {
    const keys = [
      mkKey("a", 1, [mkWindow("unified-5h", 0.1, NOW + FIVE_H / 2)]),
    ];
    const summary = {
      windowName: "unified-5h",
      maxUtilization: 0.95,
      medianUtilization: 0.1,
      nextResetAt: NOW,
      knownKeys: 1, allowedKeys: 1, warningKeys: 0, rejectedKeys: 0,
    };
    const r = PaceMath.computePoolAggregate(summary, keys, "unified-5h", NOW);
    expect(r.maxUtil).toBeCloseTo(0.95, 4);
  });

  test("aggregate dead-zone when fleet is fresh", () => {
    // Just after a mass reset: elapsed ~0, util ~0
    const freshKeys = [
      mkKey("a", 1, [mkWindow("unified-5h", 0.001, NOW + FIVE_H * 0.999)]),
      mkKey("b", 1, [mkWindow("unified-5h", 0.001, NOW + FIVE_H * 0.999)]),
    ];
    const r = PaceMath.computePoolAggregate(null, freshKeys, "unified-5h", NOW);
    expect(r.tone).toBe("dim");
    expect(r.deadZone).toBe(true);
  });
});

// ── sortKeysForDisplay ─────────────────────────────────────────────────────

describe("sortKeysForDisplay", () => {
  test("priority preservation: priority 1 DIM sorts BEFORE priority 2 RED", () => {
    // The load-bearing assertion: preferred keys always precede normal keys,
    // regardless of pace tone. Mirrors key-manager.ts selectLeastLoadedAvailableKey
    // which exhausts priority tier 1 fully before priority 2.
    const dimPreferred = mkKey("p1-dim", 1, [
      mkWindow("unified-5h", 0.05, NOW + FIVE_H * 0.5), // dim, behind pace
    ]);
    const redNormal = mkKey("p2-red", 2, [
      mkWindow("unified-5h", 0.95, NOW + FIVE_H * 0.7), // red, burning hot
    ]);
    const sorted = PaceMath.sortKeysForDisplay([redNormal, dimPreferred], NOW);
    expect(sorted[0].label).toBe("p1-dim");
    expect(sorted[1].label).toBe("p2-red");
  });

  test("priority preservation: all tiers correctly bucketed", () => {
    const p1 = mkKey("a-p1", 1, [mkWindow("unified-5h", 0.05, NOW + FIVE_H / 2)]);
    const p2 = mkKey("b-p2", 2, [mkWindow("unified-5h", 0.9, NOW + FIVE_H * 0.7)]); // red
    const p3 = mkKey("c-p3", 3, [mkWindow("unified-5h", 0.9, NOW + FIVE_H * 0.7)]); // red
    const sorted = PaceMath.sortKeysForDisplay([p3, p2, p1], NOW);
    expect(sorted.map((k) => k.label)).toEqual(["a-p1", "b-p2", "c-p3"]);
  });

  test("within tier: worst tone floats to top", () => {
    const green = mkKey("green", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]);
    const red   = mkKey("red",   1, [mkWindow("unified-5h", 0.9, NOW + FIVE_H * 0.7)]);
    const sorted = PaceMath.sortKeysForDisplay([green, red], NOW);
    expect(sorted[0].label).toBe("red");
    expect(sorted[1].label).toBe("green");
  });

  test("within tier + same tone: reset-soon wins", () => {
    // Both keys in dim tone (behind pace), differ only in resetAt.
    const soon = mkKey("soon", 1, [mkWindow("unified-5h", 0.05, NOW + FIVE_H * 0.05)]);
    const late = mkKey("late", 1, [mkWindow("unified-5h", 0.05, NOW + FIVE_H * 0.5)]);
    const sorted = PaceMath.sortKeysForDisplay([late, soon], NOW);
    expect(sorted[0].label).toBe("soon");
  });

  test("deterministic final tiebreaker on label", () => {
    const b = mkKey("b", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]);
    const a = mkKey("a", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]);
    const sorted = PaceMath.sortKeysForDisplay([b, a], NOW);
    expect(sorted.map((k) => k.label)).toEqual(["a", "b"]);
  });

  test("default priority = 2 if missing", () => {
    const p1 = mkKey("p1", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]);
    const noPri = { ...mkKey("unset", 2, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]), priority: undefined };
    const p3 = mkKey("p3", 3, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]);
    const sorted = PaceMath.sortKeysForDisplay([p3, noPri, p1], NOW);
    expect(sorted.map((k) => k.label)).toEqual(["p1", "unset", "p3"]);
  });

  test("empty array → empty", () => {
    expect(PaceMath.sortKeysForDisplay([], NOW)).toEqual([]);
  });

  test("jitter stability: sort order stable across 60 ticks (1 second apart)", () => {
    // This is the regression test against rows hopping around. If tones are
    // bucketed correctly (not raw pace ratios), two keys with the same tone
    // must never swap places as `now` advances second-by-second for a stable
    // input set.
    const keys = [
      mkKey("p1-red",   1, [mkWindow("unified-5h", 0.9, NOW + FIVE_H * 0.7)]),
      mkKey("p1-green", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]),
      mkKey("p2-red",   2, [mkWindow("unified-5h", 0.9, NOW + FIVE_H * 0.7)]),
      mkKey("p2-green", 2, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]),
      mkKey("p3-dim",   3, [mkWindow("unified-5h", 0.05, NOW + FIVE_H * 0.5)]),
    ];
    let expected: string[] | null = null;
    for (let i = 0; i < 60; i++) {
      const now = NOW + i * 1000;
      const sorted = PaceMath.sortKeysForDisplay(keys, now);
      const labels = sorted.map((k) => k.label);
      if (expected === null) expected = labels;
      expect(labels).toEqual(expected);
    }
  });

  test("real-data sort: preserves priority tiers (p1 → p2 → p3)", () => {
    const sorted = PaceMath.sortKeysForDisplay(
      [realGabe, realMailTill, realTrainly, realOnegc],
      NOW,
    );
    // Priority-1 keys (till@onegc.com, till@trainly.ai) come first.
    // Then priority-2 (mail@till.dev). Then priority-3 (gabe@personal).
    const priorities = sorted.map((k) => k.priority);
    expect(priorities).toEqual([1, 1, 2, 3]);
  });
});

// ── upcomingResets ─────────────────────────────────────────────────────────

describe("upcomingResets", () => {
  test("splits ticks into per-window tracks sorted by resetAt ASC", () => {
    const keys = [
      mkKey("a", 1, [
        mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2),
        mkWindow("unified-7d", 0.5, NOW + SEVEN_D / 2),
      ]),
      mkKey("b", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 4)]),
    ];
    const result = PaceMath.upcomingResets(keys, NOW);
    expect(result.tracks).toBeTruthy();
    const fiveH = result.tracks["unified-5h"];
    const sevenD = result.tracks["unified-7d"];
    expect(fiveH.ticks.length).toBe(2);
    expect(sevenD.ticks.length).toBe(1);
    // Ticks sorted by resetAt within each track.
    expect(fiveH.ticks[0].resetAt).toBeLessThan(fiveH.ticks[1].resetAt);
  });

  test("each track scales to its own data", () => {
    // 5h tick imminent, 7d tick 5 days out. Previously the calendar would
    // stretch to cover the 7d reset and crush the 5h tick into the left edge.
    // Now each track auto-zooms: the 5h span covers just past the 5h tick,
    // the 7d span covers just past the 7d tick.
    const keys = [
      mkKey("a", 1, [
        mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2),
        mkWindow("unified-7d", 0.5, NOW + 5 * 24 * 3600 * 1000),
      ]),
    ];
    const result = PaceMath.upcomingResets(keys, NOW);
    const fiveH = result.tracks["unified-5h"];
    const sevenD = result.tracks["unified-7d"];
    // 5h span covers ~5h/2 * 1.1 ≈ 2.75h — orders of magnitude smaller than
    // the 7d span, which is the whole point of independent scaling.
    expect(fiveH.span).toBeLessThan(FIVE_H);
    expect(sevenD.span).toBeGreaterThan(4 * 24 * 3600 * 1000);
    expect(sevenD.span / fiveH.span).toBeGreaterThan(40); // different magnitudes
    // Each tick visible in the middle-to-right of its track, not crammed in.
    expect(fiveH.ticks[0].position).toBeGreaterThan(0.5);
    expect(sevenD.ticks[0].position).toBeGreaterThan(0.5);
  });

  test("minimum span keeps axis readable when reset is imminent", () => {
    // 5h reset in 30 seconds — without minSpan we'd have a 30-second span
    // which is useless on an axis.
    const keys = [
      mkKey("a", 1, [mkWindow("unified-5h", 0.8, NOW + 30 * 1000)]),
    ];
    const result = PaceMath.upcomingResets(keys, NOW);
    // Minimum span for 5h track is 30 minutes (30 * 60 * 1000 ms).
    expect(result.tracks["unified-5h"].span).toBeGreaterThanOrEqual(30 * 60 * 1000);
  });

  test("filters out resets in the past", () => {
    const keys = [
      mkKey("a", 1, [mkWindow("unified-5h", 0.5, NOW - 1000)]), // past
      mkKey("b", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]),
    ];
    const result = PaceMath.upcomingResets(keys, NOW);
    expect(result.tracks["unified-5h"].ticks.length).toBe(1);
    expect(result.tracks["unified-5h"].ticks[0].keyLabel).toBe("b");
  });

  test("ignores windows that aren't 5h or 7d", () => {
    const keys = [
      mkKey("a", 1, [{ ...mkWindow("unified-5h", 0.5, NOW + FIVE_H), windowName: "other" }]),
      mkKey("b", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]),
    ];
    const result = PaceMath.upcomingResets(keys, NOW);
    expect(result.tracks["unified-5h"].ticks.length).toBe(1);
    expect(result.tracks["unified-5h"].ticks[0].keyLabel).toBe("b");
  });

  test("positions normalized 0..1", () => {
    const keys = [
      mkKey("a", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]),
      mkKey("b", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 4)]),
    ];
    const result = PaceMath.upcomingResets(keys, NOW);
    for (const t of result.tracks["unified-5h"].ticks) {
      expect(t.position).toBeGreaterThanOrEqual(0);
      expect(t.position).toBeLessThanOrEqual(1);
    }
  });

  test("empty keys → both tracks empty", () => {
    const result = PaceMath.upcomingResets([], NOW);
    expect(result.tracks["unified-5h"].ticks).toEqual([]);
    expect(result.tracks["unified-7d"].ticks).toEqual([]);
  });

  test("each tick has a tone derived from projected utilization", () => {
    const keys = [
      mkKey("hot", 1, [mkWindow("unified-5h", 0.9, NOW + FIVE_H * 0.7)]), // red
      mkKey("ok",  1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2)]),   // green
    ];
    const result = PaceMath.upcomingResets(keys, NOW);
    const ticks = result.tracks["unified-5h"].ticks;
    const hot = ticks.find((t: any) => t.keyLabel === "hot");
    const ok  = ticks.find((t: any) => t.keyLabel === "ok");
    expect(hot.tone).toBe("red");
    expect(ok.tone).toBe("green");
  });

  test("real-data: produces tracks with the expected upcoming resets", () => {
    const keys = [realMailTill, realOnegc, realTrainly, realGabe];
    const result = PaceMath.upcomingResets(keys, NOW);
    const fiveH = result.tracks["unified-5h"];
    const sevenD = result.tracks["unified-7d"];
    // 4 keys × 2 windows = 8 resets, minus mail@till.dev 7d which resets very
    // soon. All should be in the future.
    expect(fiveH.ticks.length + sevenD.ticks.length).toBeGreaterThanOrEqual(4);
    // 5h track earliest should be ~15 min out (till@onegc).
    expect(fiveH.ticks[0].resetAt - NOW).toBeLessThan(16 * 60 * 1000);
    // 7d track earliest should be ~15 min out (mail@till.dev 7d).
    expect(sevenD.ticks[0].resetAt - NOW).toBeLessThan(16 * 60 * 1000);
  });
});

// ── demoFixture ────────────────────────────────────────────────────────────

describe("demoFixture", () => {
  test("returns keys covering every tone bucket", () => {
    const keys = PaceMath.demoFixture(NOW);
    const tones = new Set();
    for (const k of keys) {
      const rank = PaceMath.worstToneRank(k, NOW);
      tones.add(rank);
    }
    // Should include all 5 tone ranks (0..4)
    for (let r = 0; r <= 4; r++) {
      expect(tones.has(r)).toBe(true);
    }
  });

  test("includes a reset-soon case", () => {
    const keys = PaceMath.demoFixture(NOW);
    const hasSoon = keys.some((k: any) => {
      const r = PaceMath.earliestResetAt(k);
      return r !== null && r - NOW < 30 * 60 * 1000;
    });
    expect(hasSoon).toBe(true);
  });

  test("fixture keys are well-formed (have all fields renderKeys expects)", () => {
    const keys = PaceMath.demoFixture(NOW);
    for (const k of keys) {
      expect(k.label).toBeTruthy();
      expect(typeof k.priority).toBe("number");
      expect(k.capacity).toBeTruthy();
      expect(Array.isArray(k.capacity.windows)).toBe(true);
      expect(k.stats).toBeTruthy();
      expect(Array.isArray(k.allowedDays)).toBe(true);
    }
  });
});

// ── worstToneRank / earliestResetAt helpers ───────────────────────────────

describe("worstToneRank", () => {
  test("returns max across 5h and 7d", () => {
    const key = mkKey("mixed", 1, [
      mkWindow("unified-5h", 0.5, NOW + FIVE_H / 2),    // green, rank 1
      mkWindow("unified-7d", 0.9, NOW + SEVEN_D * 0.7), // red, rank 4
    ]);
    expect(PaceMath.worstToneRank(key, NOW)).toBe(4);
  });

  test("no windows → rank 0 (dim)", () => {
    const key = mkKey("empty", 1, []);
    expect(PaceMath.worstToneRank(key, NOW)).toBe(0);
  });
});

describe("earliestResetAt", () => {
  test("returns min resetAt across windows", () => {
    const key = mkKey("k", 1, [
      mkWindow("unified-5h", 0.5, NOW + FIVE_H),
      mkWindow("unified-7d", 0.5, NOW + SEVEN_D),
    ]);
    expect(PaceMath.earliestResetAt(key)).toBe(NOW + FIVE_H);
  });

  test("null when no windows have resetAt", () => {
    const key = mkKey("k", 1, [mkWindow("unified-5h", 0.5, null)]);
    expect(PaceMath.earliestResetAt(key)).toBeNull();
  });
});

// ── toneFromPace ───────────────────────────────────────────────────────────

describe("toneFromPace thresholds", () => {
  test("boundaries map to expected tones", () => {
    expect(PaceMath.toneFromPace(0.0)).toBe("dim");
    expect(PaceMath.toneFromPace(0.5)).toBe("dim");
    expect(PaceMath.toneFromPace(0.84)).toBe("dim");
    expect(PaceMath.toneFromPace(0.85)).toBe("green");
    expect(PaceMath.toneFromPace(1.0)).toBe("green");
    expect(PaceMath.toneFromPace(1.14)).toBe("green");
    expect(PaceMath.toneFromPace(1.15)).toBe("yellow");
    expect(PaceMath.toneFromPace(1.49)).toBe("yellow");
    expect(PaceMath.toneFromPace(1.5)).toBe("orange");
    expect(PaceMath.toneFromPace(1.99)).toBe("orange");
    expect(PaceMath.toneFromPace(2.0)).toBe("red");
    expect(PaceMath.toneFromPace(10)).toBe("red");
  });
});

// ── computeFleetPressureGradient ───────────────────────────────────────────

describe("computeFleetPressureGradient", () => {
  test("empty keys → all samples dim, no pressure", () => {
    const result = PaceMath.computeFleetPressureGradient([], "unified-5h", NOW, 10);
    expect(result.samples.length).toBe(11);
    for (const s of result.samples) {
      expect(s.pressure).toBe(0);
      expect(s.tone).toBe("dim");
    }
    expect(result.maxPressureInWindow).toBe(0);
    expect(result.currentTone).toBe("dim");
  });

  test("all-coasting fleet → gradient stays dim throughout", () => {
    // Three keys, each at 10% util with 30% elapsed. Pace 0.33× — coasting.
    // Projected forward, they'd still be well under 50% at window end.
    const keys = [
      mkKey("a", 1, [mkWindow("unified-5h", 0.10, NOW + FIVE_H * 0.7)]),
      mkKey("b", 1, [mkWindow("unified-5h", 0.10, NOW + FIVE_H * 0.7)]),
      mkKey("c", 1, [mkWindow("unified-5h", 0.10, NOW + FIVE_H * 0.7)]),
    ];
    const result = PaceMath.computeFleetPressureGradient(keys, "unified-5h", NOW, 20);
    for (const s of result.samples) {
      expect(s.tone).toBe("dim");
    }
    expect(result.maxPressureInWindow).toBeLessThan(0.5);
  });

  test("one key burning hot → strip lights up before the reset", () => {
    // util=0.8 at 50% elapsed → pace 1.6. Projected linearly, it hits the
    // limit before the current window resets. The strip should show orange/red
    // at some point during the current cycle.
    const keys = [
      mkKey("hot", 1, [mkWindow("unified-5h", 0.8, NOW + FIVE_H * 0.5)]),
    ];
    const result = PaceMath.computeFleetPressureGradient(keys, "unified-5h", NOW, 60);
    const hotSamples = result.samples.filter(
      (s: any) => s.tone === "orange" || s.tone === "red",
    );
    expect(hotSamples.length).toBeGreaterThan(0);
    expect(result.maxPressureInWindow).toBeGreaterThan(0.85);
  });

  test("reset mid-horizon → pressure drops after the reset sample", () => {
    // Key at 0.8 util with reset in 1 hour. Burn rate is high. Pressure
    // should peak near the reset, then drop, then climb again through the
    // next cycle.
    const keys = [
      mkKey("peaky", 1, [mkWindow("unified-5h", 0.8, NOW + FIVE_H * 0.2)]),
    ];
    const result = PaceMath.computeFleetPressureGradient(keys, "unified-5h", NOW, 100);
    // Find a sample right before the reset and one right after.
    const resetPosition = 0.2; // reset happens at 20% of horizon (1h into 5h)
    const samplesBeforeReset = result.samples.filter(
      (s: any) => s.position > 0.15 && s.position < 0.2,
    );
    const samplesAfterReset = result.samples.filter(
      (s: any) => s.position > 0.2 && s.position < 0.25,
    );
    expect(samplesBeforeReset.length).toBeGreaterThan(0);
    expect(samplesAfterReset.length).toBeGreaterThan(0);
    const maxBefore = Math.max(...samplesBeforeReset.map((s: any) => s.pressure));
    const minAfter = Math.min(...samplesAfterReset.map((s: any) => s.pressure));
    expect(minAfter).toBeLessThan(maxBefore);
  });

  test("dead-zone keys (elapsed < 2%) contribute nothing to pressure", () => {
    // A key at 5% util with only 1% elapsed would look like pace 5× and
    // project to catastrophe. Dead-zone rule suppresses this early-window
    // noise.
    const keys = [
      mkKey("early", 1, [mkWindow("unified-5h", 0.05, NOW + FIVE_H * 0.99)]),
    ];
    const result = PaceMath.computeFleetPressureGradient(keys, "unified-5h", NOW, 20);
    expect(result.maxPressureInWindow).toBe(0);
    for (const s of result.samples) {
      expect(s.tone).toBe("dim");
    }
  });

  test("unknown keys (null utilization) contribute nothing to pressure", () => {
    // A window present in the list but with no observed utilization is
    // unknown — it shouldn't project as if it were at 0% either. Just skip.
    const keys = [
      mkKey("unknown", 1, [mkWindow("unified-5h", null, NOW + FIVE_H * 0.5)]),
    ];
    const result = PaceMath.computeFleetPressureGradient(keys, "unified-5h", NOW, 20);
    expect(result.maxPressureInWindow).toBe(0);
  });

  test("sample count honored, positions 0..1", () => {
    const keys = [
      mkKey("a", 1, [mkWindow("unified-5h", 0.5, NOW + FIVE_H * 0.5)]),
    ];
    const result = PaceMath.computeFleetPressureGradient(keys, "unified-5h", NOW, 30);
    expect(result.samples.length).toBe(31); // inclusive: sampleCount + 1
    expect(result.samples[0].position).toBe(0);
    expect(result.samples[30].position).toBe(1);
  });

  test("pressureStateWord maps tones to human labels", () => {
    expect(PaceMath.pressureStateWord("dim")).toBe("Coasting");
    expect(PaceMath.pressureStateWord("yellow")).toBe("Steady");
    expect(PaceMath.pressureStateWord("orange")).toBe("Watch");
    expect(PaceMath.pressureStateWord("red")).toBe("Tight");
  });

  test("pressureTone bucket boundaries", () => {
    expect(PaceMath.pressureTone(0.0)).toBe("dim");
    expect(PaceMath.pressureTone(0.49)).toBe("dim");
    expect(PaceMath.pressureTone(0.5)).toBe("yellow");
    expect(PaceMath.pressureTone(0.69)).toBe("yellow");
    expect(PaceMath.pressureTone(0.7)).toBe("orange");
    expect(PaceMath.pressureTone(0.84)).toBe("orange");
    expect(PaceMath.pressureTone(0.85)).toBe("red");
    expect(PaceMath.pressureTone(1.0)).toBe("red");
  });

  test("real-data: produces a coasting gradient for the current fleet state", () => {
    // Real data: all keys at low util → gradient should be entirely dim.
    const keys = [realMailTill, realOnegc, realTrainly, realGabe];
    const result5h = PaceMath.computeFleetPressureGradient(keys, "unified-5h", NOW, 30);
    expect(result5h.currentTone).toBe("dim");
    const result7d = PaceMath.computeFleetPressureGradient(keys, "unified-7d", NOW, 30);
    expect(result7d.currentTone).toBe("dim");
  });
});
