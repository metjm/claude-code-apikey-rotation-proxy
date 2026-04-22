import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { KeyManager } from "../src/key-manager.ts";
import { handleAdminRoute } from "../src/admin.ts";
import { SchemaTracker } from "../src/schema-tracker.ts";
import type { ProxyConfig } from "../src/types.ts";
import { PaceMath } from "../public/pace.js";

const ADMIN_TOKEN = "forecast-integration-admin-secret";
const FIVE_H_MS = 5 * 60 * 60 * 1000;
const SEVEN_D_MS = 7 * 24 * 60 * 60 * 1000;

interface Proxy {
  url: string;
  km: KeyManager;
  stop: () => void;
}

function makeConfig(dataDir: string, adminToken: string | null): ProxyConfig {
  return {
    port: 0,
    upstream: "http://localhost:1",
    adminToken,
    dataDir,
    maxRetriesPerRequest: 3,
    firstChunkTimeoutMs: 16_000,
    streamIdleTimeoutMs: 120_000,
    maxFirstChunkRetries: 2,
    webhookUrl: null,
  };
}

function startProxy(dataDir: string, adminToken: string | null): Proxy {
  const km = new KeyManager(dataDir);
  const st = new SchemaTracker(km.dbPath);
  const config = makeConfig(dataDir, adminToken);

  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const adminResponse = await handleAdminRoute(req, km, config, st);
      if (adminResponse !== null) return adminResponse;
      return new Response("not found", { status: 404 });
    },
  });

  return {
    url: `http://localhost:${server.port}`,
    km,
    stop: () => { server.stop(true); st.close(); km.close(); },
  };
}

function rawDb(km: KeyManager): Database {
  return (km as unknown as { db: Database }).db;
}

function insertBucket(
  km: KeyManager,
  bucketIsoHour: string,
  requests: number,
  keyLabel: string = "__all__",
  userLabel: string = "__all__",
): void {
  rawDb(km).run(
    "INSERT INTO stats_timeseries (bucket, key_label, user_label, requests) VALUES (?, ?, ?, ?)",
    [bucketIsoHour, keyLabel, userLabel, requests],
  );
}

// Build an ISO hour bucket string (YYYY-MM-DDTHH) in UTC at the given
// weeksAgo offset, day-of-week (0=Sun..6=Sat UTC), and hour (0..23 UTC).
function bucketAt(weeksAgo: number, dow: number, hour: number, reference: Date = new Date()): string {
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

function makeCapacityKey(
  label: string,
  priority: number,
  utilization: number,
  resetInMs: number,
  windowName: "unified-5h" | "unified-7d",
  now: number,
) {
  const surpassedThreshold = windowName === "unified-5h" ? 0.9 : 0.75;
  return {
    maskedKey: "sk-ant-***" + label,
    label,
    priority,
    isAvailable: true,
    availableAt: 0,
    allowedDays: [0, 1, 2, 3, 4, 5, 6],
    stats: {
      totalRequests: 0,
      successfulRequests: 0,
      rateLimitHits: 0,
      errors: 0,
      lastUsedAt: null,
      addedAt: now,
      totalTokensIn: 0,
      totalTokensOut: 0,
      totalCacheRead: 0,
      totalCacheCreation: 0,
    },
    capacity: {
      responseCount: 0,
      normalizedHeaderCount: 0,
      lastResponseAt: now,
      lastHeaderAt: now,
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
      windows: [{
        windowName,
        status: "allowed" as const,
        utilization,
        resetAt: now + resetInMs,
        surpassedThreshold,
        lastSeenAt: now,
      }],
    },
    capacityHealth: "healthy" as const,
    recentErrors: 0,
    recentSessions15m: [],
  };
}

// ─────────────────────────────────────────────────────────────────────

describe("Forecast end-to-end integration", () => {
  let tempDir: string;
  let proxy: Proxy;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "forecast-e2e-"));
    proxy = startProxy(tempDir, ADMIN_TOKEN);
  });

  afterEach(() => {
    proxy.stop();
    try { rmSync(tempDir, { recursive: true, force: true }); } catch {}
  });

  test("full loop: seeded 4-week data surfaces the Tuesday-2pm UTC spike as the largest factor", async () => {
    // bucketAt anchors on current week's Sunday, so weeks 1..3 are fully
    // inside the 4-week cutoff on any day-of-week. (Week 4 straddles the
    // cutoff on weekdays, so we avoid it here and keep the row count exact.)
    const insertedCount = { n: 0 };
    for (let week = 1; week <= 3; week++) {
      for (let dow = 0; dow < 7; dow++) {
        for (let hour = 0; hour < 24; hour++) {
          const baseline = 5 + ((dow + hour) % 7);
          const requests = (dow === 2 && hour === 14) ? 500 : baseline;
          insertBucket(proxy.km, bucketAt(week, dow, hour), requests);
          insertedCount.n++;
        }
      }
    }

    const res = await fetch(`${proxy.url}/admin/capacity/forecast`, {
      headers: { authorization: `Bearer ${ADMIN_TOKEN}` },
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      weeks: number;
      generatedAt: number;
      totalSamples: number;
      slots: Array<{ dow: number; hour: number; factor: number; samples: number }>;
    };
    expect(body.weeks).toBe(4);
    expect(body.slots).toHaveLength(168);
    expect(body.totalSamples).toBe(insertedCount.n);

    const tuesday2pm = body.slots.find((s) => s.dow === 2 && s.hour === 14)!;
    const sortedByFactor = [...body.slots].sort((a, b) => b.factor - a.factor);
    expect(sortedByFactor[0]).toEqual(tuesday2pm);
    expect(tuesday2pm.factor).toBeGreaterThan(1);
  });

  test("auth: /admin/capacity/forecast without Authorization returns 401 when admin token configured", async () => {
    const res = await fetch(`${proxy.url}/admin/capacity/forecast`);
    expect(res.status).toBe(401);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("Unauthorized");

    const resBadToken = await fetch(`${proxy.url}/admin/capacity/forecast`, {
      headers: { authorization: "Bearer wrong-token" },
    });
    expect(resBadToken.status).toBe(401);
  });

  test("round-trip through PaceMath.computeFleetPressureGradient produces a valid gradient", async () => {
    for (let week = 1; week <= 4; week++) {
      for (let dow = 0; dow < 7; dow++) {
        for (let hour = 0; hour < 24; hour++) {
          const requests = (dow === 3 && hour === 10) ? 200 : 10;
          insertBucket(proxy.km, bucketAt(week, dow, hour), requests);
        }
      }
    }

    const res = await fetch(`${proxy.url}/admin/capacity/forecast`, {
      headers: { authorization: `Bearer ${ADMIN_TOKEN}` },
    });
    const table = await res.json();

    const now = Date.now();
    const sampleCount = 60;
    const keys = [
      makeCapacityKey("a", 1, 0.4, FIVE_H_MS * 0.5, "unified-5h", now),
      makeCapacityKey("b", 1, 0.25, FIVE_H_MS * 0.7, "unified-5h", now),
      makeCapacityKey("c", 2, 0.6, FIVE_H_MS * 0.3, "unified-5h", now),
    ];

    const gradient = PaceMath.computeFleetPressureGradient(
      keys, "unified-5h", now, sampleCount, table,
    );

    expect(gradient.samples.length).toBe(sampleCount + 1);
    for (const s of gradient.samples) {
      expect(s.pressure).toBeGreaterThanOrEqual(0);
      expect(s.pressure).toBeLessThanOrEqual(1);
    }
    expect(gradient.peakTimeMs).not.toBeNull();
    expect(gradient.peakTimeMs!).toBeGreaterThanOrEqual(now);
    expect(gradient.peakTimeMs!).toBeLessThanOrEqual(now + FIVE_H_MS);
    expect(typeof gradient.currentTone).toBe("string");
    expect(["dim", "yellow", "orange", "red"]).toContain(gradient.currentTone);
  });

  test("__reset_v2__ sentinel row is excluded from factor computation", async () => {
    // Seed a uniform 3-week baseline — every slot has 3 samples of value 10,
    // so every factor should be exactly 1 before the sentinel is injected.
    let expectedTotal = 0;
    for (let week = 1; week <= 3; week++) {
      for (let dow = 0; dow < 7; dow++) {
        for (let hour = 0; hour < 24; hour++) {
          insertBucket(proxy.km, bucketAt(week, dow, hour), 10);
          expectedTotal++;
        }
      }
    }

    // Inject a __reset_v2__ sentinel row at a recent bucket with a wildly
    // inflated value. If the aggregation counted it, the slot factor would
    // jump to the 5.0 clamp — the filter on key_label='__all__' AND
    // user_label='__all__' must exclude it.
    const recentBucket = bucketAt(1, 2, 14);
    insertBucket(proxy.km, recentBucket, 1_000_000_000, "__reset_v2__", "__reset_v2__");

    const res = await fetch(`${proxy.url}/admin/capacity/forecast`, {
      headers: { authorization: `Bearer ${ADMIN_TOKEN}` },
    });
    const body = (await res.json()) as {
      totalSamples: number;
      slots: Array<{ dow: number; hour: number; factor: number; samples: number }>;
    };

    expect(body.totalSamples).toBe(expectedTotal);
    const spiked = body.slots.find((s) => s.dow === 2 && s.hour === 14)!;
    expect(spiked.factor).toBeCloseTo(1, 5);
  });

  test("performance: computeSeasonalRequestFactors × 10 over 720 rows completes under 100ms", () => {
    // Seed 30 days × 24 hours = 720 one-bucket-per-hour rows.
    const reference = new Date();
    for (let daysAgo = 0; daysAgo < 30; daysAgo++) {
      for (let hour = 0; hour < 24; hour++) {
        const ts = new Date(reference.getTime() - daysAgo * 24 * 60 * 60 * 1000);
        ts.setUTCHours(hour, 0, 0, 0);
        const bucket = ts.toISOString().slice(0, 13);
        try {
          insertBucket(proxy.km, bucket, 50 + (daysAgo + hour) % 13);
        } catch {
          // Ignore any PK collision — we just need ~720 rows for timing.
        }
      }
    }

    const t0 = performance.now();
    for (let i = 0; i < 10; i++) {
      proxy.km.computeSeasonalRequestFactors();
    }
    const elapsed = performance.now() - t0;

    // Observed on an M-series MacBook: ~4ms total for 10 runs over 720 rows
    // (~0.4ms per call). 100ms leaves generous headroom for slower CI hardware
    // without masking a real regression.
    expect(elapsed).toBeLessThan(100);
  });

  test("concurrent computeSeasonalRequestFactors calls return identical tables", async () => {
    for (let week = 1; week <= 4; week++) {
      for (let dow = 0; dow < 7; dow++) {
        for (let hour = 0; hour < 24; hour++) {
          const requests = (dow === 4 && hour === 9) ? 300 : 20;
          insertBucket(proxy.km, bucketAt(week, dow, hour), requests);
        }
      }
    }

    const results = await Promise.all([
      Promise.resolve(proxy.km.computeSeasonalRequestFactors()),
      Promise.resolve(proxy.km.computeSeasonalRequestFactors()),
      Promise.resolve(proxy.km.computeSeasonalRequestFactors()),
      Promise.resolve(proxy.km.computeSeasonalRequestFactors()),
      Promise.resolve(proxy.km.computeSeasonalRequestFactors()),
    ]);

    const first = results[0]!;
    for (let i = 1; i < results.length; i++) {
      const table = results[i]!;
      expect(table.weeks).toBe(first.weeks);
      expect(table.totalSamples).toBe(first.totalSamples);
      expect(table.slots.length).toBe(first.slots.length);
      for (let idx = 0; idx < table.slots.length; idx++) {
        expect(table.slots[idx]!.factor).toBe(first.slots[idx]!.factor);
        expect(table.slots[idx]!.samples).toBe(first.slots[idx]!.samples);
        expect(table.slots[idx]!.dow).toBe(first.slots[idx]!.dow);
        expect(table.slots[idx]!.hour).toBe(first.slots[idx]!.hour);
      }
    }
  });

  test("weeks param honored end-to-end with clamping to [1, 4]", async () => {
    // Age the data relative to Date.now() via raw hour offsets — this makes
    // inside/outside-cutoff membership deterministic regardless of the week
    // boundary alignment that bucketAt() uses.
    const nowMs = Date.now();
    function bucketAtHoursAgo(hoursAgo: number): string {
      const d = new Date(nowMs - hoursAgo * 60 * 60 * 1000);
      return d.toISOString().slice(0, 13);
    }

    // Inside weeks=1 (last 7 days): 5 distinct hourly buckets starting 6h ago.
    const inWeek1: string[] = [];
    for (let h = 6; h < 11; h++) {
      const bucket = bucketAtHoursAgo(h);
      insertBucket(proxy.km, bucket, 5);
      inWeek1.push(bucket);
    }
    // Outside weeks=1 but inside weeks=4 (8..25 days old): 10 distinct buckets.
    const inWeek4Only: string[] = [];
    for (let d = 0; d < 10; d++) {
      const hoursAgo = 10 * 24 + d; // ~10 days ago + distinct hour each
      const bucket = bucketAtHoursAgo(hoursAgo);
      insertBucket(proxy.km, bucket, 15);
      inWeek4Only.push(bucket);
    }
    // 10 weeks old — never visible even under weeks=999 (clamps to 4).
    const ancient: string[] = [];
    for (let d = 0; d < 3; d++) {
      const hoursAgo = 70 * 24 + d;
      const bucket = bucketAtHoursAgo(hoursAgo);
      insertBucket(proxy.km, bucket, 999_999);
      ancient.push(bucket);
    }

    const authHeader = { authorization: `Bearer ${ADMIN_TOKEN}` };

    const res1 = await fetch(`${proxy.url}/admin/capacity/forecast?weeks=1`, { headers: authHeader });
    const body1 = (await res1.json()) as { weeks: number; totalSamples: number };
    expect(body1.weeks).toBe(1);
    expect(body1.totalSamples).toBe(inWeek1.length);

    const res4 = await fetch(`${proxy.url}/admin/capacity/forecast?weeks=4`, { headers: authHeader });
    const body4 = (await res4.json()) as { weeks: number; totalSamples: number };
    expect(body4.weeks).toBe(4);
    expect(body4.totalSamples).toBe(inWeek1.length + inWeek4Only.length);

    // weeks=1 strictly smaller than weeks=4 (more data in the wider window).
    expect(body1.totalSamples).toBeLessThan(body4.totalSamples);

    // weeks=999 must clamp to 4 — not pick up the 10-week-old rows.
    const resClamped = await fetch(`${proxy.url}/admin/capacity/forecast?weeks=999`, { headers: authHeader });
    const bodyClamped = (await resClamped.json()) as { weeks: number; totalSamples: number };
    expect(bodyClamped.weeks).toBe(4);
    expect(bodyClamped.totalSamples).toBe(body4.totalSamples);
    void ancient;
  });

  test("clock edge: cutoff is inclusive — buckets exactly at the cutoff hour are kept, buckets one hour older are dropped", () => {
    // Pin Date.now to a week boundary so the 4-week cutoff lands cleanly at a
    // known ISO hour. 2026-02-01 00:00 UTC happens to be a Sunday.
    const pinnedNow = Date.UTC(2026, 1, 1, 0, 0, 0);
    const originalNow = Date.now;
    Date.now = () => pinnedNow;
    try {
      const cutoffMs = pinnedNow - 4 * 7 * 24 * 60 * 60 * 1000;
      const cutoffBucket = new Date(cutoffMs).toISOString().slice(0, 13);
      const cutoffMinusOneHour = new Date(cutoffMs - 60 * 60 * 1000).toISOString().slice(0, 13);

      // 3 hits exactly at the cutoff (same ISO hour, different key/user just
      // to satisfy the composite primary key).
      insertBucket(proxy.km, cutoffBucket, 50, "__all__", "__all__");
      // 1 extra row at a distinct bucket also inside the window to supply the
      // minimum sample threshold isn't the focus here — we're checking that
      // the cutoff row counts.

      // 1 hit one hour OLDER than the cutoff — SQL cutoff comparison is `>=`
      // so this must be excluded.
      insertBucket(proxy.km, cutoffMinusOneHour, 9999, "__all__", "__all__");

      const table = proxy.km.computeSeasonalRequestFactors();

      // totalSamples counts buckets observed within [cutoffBucket, now).
      // The at-cutoff row is present, the one-hour-older row is filtered out.
      expect(table.totalSamples).toBe(1);

      // Parse the cutoff bucket and verify the corresponding slot actually
      // received the sample (confirming the sentinel row survived the filter).
      const cutoffDate = new Date(cutoffBucket + ":00:00.000Z");
      const dow = cutoffDate.getUTCDay();
      const hour = cutoffDate.getUTCHours();
      const slot = table.slots.find((s) => s.dow === dow && s.hour === hour)!;
      expect(slot.samples).toBe(1);
    } finally {
      Date.now = originalNow;
    }
  });
});
