import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import puppeteer, { type Browser, type Page } from "puppeteer";
import { KeyManager } from "../src/key-manager.ts";
import { SchemaTracker } from "../src/schema-tracker.ts";
import { handleAdminRoute } from "../src/admin.ts";
import { proxyRequest } from "../src/proxy.ts";
import { emitWithKeys } from "../src/events.ts";
import type { ApiKeyEntry, ProxyConfig, ProxyTokenEntry } from "../src/types.ts";
import { unixMs } from "../src/types.ts";

// ── Test helpers ──────────────────────────────────────────────────

const VALID_KEY_1 = "sk-ant-api03-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const VALID_KEY_2 = "sk-ant-api03-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const VALID_KEY_3 = "sk-ant-api03-cccccccccccccccccccccccccccccccccc";
const ADMIN_TOKEN = "dashboard-e2e-admin-token";

interface MockUpstream {
  url: string;
  stop: () => void;
}

interface ProxyInstance {
  url: string;
  km: KeyManager;
  stop: () => void;
}

function startMockUpstream(): MockUpstream {
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      try {
        await Promise.race([
          req.arrayBuffer(),
          new Promise((resolve) => setTimeout(resolve, 50)),
        ]);
      } catch {}
      return new Response(JSON.stringify({ ok: true }), {
        headers: { "content-type": "application/json" },
      });
    },
  });
  return {
    url: `http://localhost:${server.port}`,
    stop: () => server.stop(true),
  };
}

function startProxy(opts: {
  dataDir: string;
  upstream: string;
  adminToken?: string | null;
}): ProxyInstance {
  const km = new KeyManager(opts.dataDir);
  const st = new SchemaTracker(km.dbPath);
  const config: ProxyConfig = {
    port: 0,
    upstream: opts.upstream,
    adminToken: opts.adminToken ?? null,
    dataDir: opts.dataDir,
    maxRetriesPerRequest: 10,
    firstChunkTimeoutMs: 16_000,
    firstChunkTimeoutMsContext1m: 120_000,
    maxFirstChunkRetries: 2,
    webhookUrl: null,
  };

  const server = Bun.serve({
    port: 0,
    async fetch(req: Request): Promise<Response> {
      const url = new URL(req.url);

      // Serve the REAL dashboard (same wiring as src/server.ts)
      if (url.pathname === "/dashboard.html" || url.pathname === "/dashboard" || url.pathname === "/dashboard/") {
        return new Response(
          Bun.file(new URL("../public/dashboard.html", import.meta.url).pathname),
          { headers: { "content-type": "text/html" } },
        );
      }
      if (url.pathname === "/dashboard/pace.js") {
        return new Response(
          Bun.file(new URL("../public/pace.js", import.meta.url).pathname),
          { headers: { "content-type": "application/javascript" } },
        );
      }
      if (url.pathname === "/dashboard/vue.global.prod.js") {
        return new Response(
          Bun.file(new URL("../public/vue.global.prod.js", import.meta.url).pathname),
          { headers: { "content-type": "application/javascript" } },
        );
      }
      if (url.pathname === "/dashboard/chart.umd.min.js") {
        return new Response(
          Bun.file(new URL("../public/chart.umd.min.js", import.meta.url).pathname),
          { headers: { "content-type": "application/javascript" } },
        );
      }
      if (url.pathname === "/dashboard/chart.umd.min.js.map") {
        return new Response(null, { status: 204 });
      }
      if (url.pathname === "/favicon.ico") return new Response(null, { status: 204 });

      const adminResponse = await handleAdminRoute(req, km, config, st);
      if (adminResponse !== null) return adminResponse;

      let proxyUser: ProxyTokenEntry | null = null;
      if (km.hasTokens()) {
        const incoming = req.headers.get("x-api-key") ?? null;
        if (!incoming) {
          return new Response(JSON.stringify({ error: { message: "auth required" } }), { status: 401 });
        }
        proxyUser = km.validateToken(incoming);
        if (!proxyUser) {
          return new Response(JSON.stringify({ error: { message: "invalid" } }), { status: 401 });
        }
      }

      const result = await proxyRequest(req, km, config, st, proxyUser);
      if (result.kind === "success") return result.response;
      return new Response(JSON.stringify({ error: result }), { status: 500 });
    },
  });

  return {
    url: `http://localhost:${server.port}`,
    km,
    stop: () => {
      server.stop(true);
      st.close();
      km.close();
    },
  };
}

function makeTempDir(): string {
  return mkdtempSync(join(tmpdir(), "dashboard-e2e-"));
}

function cleanupTempDir(dir: string): void {
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

/** UTC bucket string "YYYY-MM-DDTHH" N weeks ago at (dow, hour). */
function bucketAt(weeksAgo: number, dow: number, hour: number): string {
  const ref = new Date();
  const sundayUtc = new Date(Date.UTC(
    ref.getUTCFullYear(),
    ref.getUTCMonth(),
    ref.getUTCDate() - ref.getUTCDay(),
    0, 0, 0, 0,
  ));
  const target = new Date(sundayUtc.getTime() - weeksAgo * 7 * 24 * 60 * 60 * 1000);
  target.setUTCDate(target.getUTCDate() + dow);
  target.setUTCHours(hour, 0, 0, 0);
  return target.toISOString().slice(0, 13);
}

function insertBucket(km: KeyManager, bucketIsoHour: string, requests: number): void {
  const db = (km as unknown as { db: Database }).db;
  db.run(
    "INSERT INTO stats_timeseries (bucket, key_label, user_label, requests) VALUES (?, '__all__', '__all__', ?)",
    [bucketIsoHour, requests],
  );
}

/** Seed >336 buckets (2 weeks × 7 × 24) plus a Tuesday 2pm UTC spike. */
function seedSeasonalFactors(km: KeyManager): number {
  let n = 0;
  for (let week = 1; week <= 3; week++) {
    for (let dow = 0; dow < 7; dow++) {
      for (let hour = 0; hour < 24; hour++) {
        const requests = (dow === 2 && hour === 14) ? 500 : 10;
        insertBucket(km, bucketAt(week, dow, hour), requests);
        n++;
      }
    }
  }
  return n;
}

/** Record utilization on each key so fleet gradient has live signal.
 *  `entries` must be the live ApiKeyEntry objects returned from addKey(). */
function seedCapacityWindows(km: KeyManager, entries: readonly ApiKeyEntry[], utilizations: readonly number[]): void {
  const now = Date.now();
  const resetAt5h = now + 3 * 60 * 60 * 1000;
  const resetAt7d = now + 4 * 24 * 60 * 60 * 1000;
  entries.forEach((entry, i) => {
    const util = utilizations[i] ?? 0.5;
    km.recordCapacityObservation(entry, {
      seenAt: unixMs(now),
      httpStatus: 200,
      windows: [
        { windowName: "unified-5h", status: "allowed", utilization: util, resetAt: unixMs(resetAt5h) },
        { windowName: "unified-7d", status: "allowed", utilization: util * 0.4, resetAt: unixMs(resetAt7d) },
      ],
    });
  });
}

// ── Browser launcher ──────────────────────────────────────────────

async function launchBrowser(): Promise<Browser> {
  return puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
}

/** Visit the dashboard with the admin token pre-seeded into sessionStorage
 *  so the auth overlay never shows. */
async function openDashboard(browser: Browser, proxyUrl: string, adminToken: string): Promise<Page> {
  const page = await browser.newPage();
  page.on("pageerror", (err) => {
    // Surface JS errors so failures aren't mysterious.
    // eslint-disable-next-line no-console
    console.log("[dashboard pageerror]", err.message);
  });
  await page.evaluateOnNewDocument((tok: string) => {
    try { sessionStorage.setItem("adminToken", tok); } catch {}
  }, adminToken);
  await page.goto(`${proxyUrl}/dashboard.html`, { waitUntil: "domcontentloaded" });
  return page;
}

// ── Tests ─────────────────────────────────────────────────────────

describe("Dashboard fleet-pressure gradient (real browser)", () => {
  let browser: Browser;

  beforeAll(async () => {
    browser = await launchBrowser();
  });

  afterAll(async () => {
    await browser.close();
  });

  test("renders gradient strips, peak caption, tone and NO learning pill once seasonal samples exceed threshold", async () => {
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

    try {
      // Three keys with live utilization so the fleet gradient exits dead-zone.
      const e1 = proxy.km.addKey(VALID_KEY_1, "key-a");
      const e2 = proxy.km.addKey(VALID_KEY_2, "key-b");
      const e3 = proxy.km.addKey(VALID_KEY_3, "key-c");
      const seeded = seedSeasonalFactors(proxy.km);
      expect(seeded).toBeGreaterThan(336); // past LEARNING_SAMPLE_THRESHOLD
      seedCapacityWindows(proxy.km, [e1, e2, e3], [0.55, 0.6, 0.65]);

      // Sanity: forecast endpoint reports enough samples.
      const forecast = await fetch(`${proxy.url}/admin/capacity/forecast`, {
        headers: { Authorization: `Bearer ${ADMIN_TOKEN}` },
      }).then((r) => r.json() as Promise<{ totalSamples: number; slots: unknown[] }>);
      expect(forecast.totalSamples).toBeGreaterThan(336);
      expect(forecast.slots).toHaveLength(168);

      const page = await openDashboard(browser, proxy.url, ADMIN_TOKEN);
      await page.waitForSelector("#fleet-pressure .fleet-pressure-row", { timeout: 15_000 });

      const rows = await page.$$eval(".fleet-pressure-row", (els) =>
        els.map((el) => ({
          label: el.querySelector(".fleet-pressure-label")?.textContent?.trim() ?? "",
          state: el.querySelector(".fleet-pressure-state")?.textContent?.trim() ?? "",
          stateClass: el.querySelector(".fleet-pressure-state")?.className ?? "",
          fillBackground: (el.querySelector(".fleet-pressure-strip-fill") as HTMLElement | null)?.style.background ?? "",
          caption: el.querySelector(".fleet-pressure-caption")?.textContent?.trim() ?? "",
          hasLearningPill: el.querySelector(".fleet-pressure-learning") !== null,
        })),
      );

      expect(rows).toHaveLength(2); // 5h and 7d
      const labels = rows.map((r) => r.label);
      expect(labels).toContain("5h");
      expect(labels).toContain("7d");

      for (const r of rows) {
        expect(r.fillBackground).toContain("linear-gradient");
        // Caption has "peak" and a percentage digit.
        expect(r.caption.toLowerCase()).toContain("peak");
        expect(r.caption).toMatch(/\d+%/);
        // Seasonal samples exceed threshold — Learning pill must NOT appear.
        expect(r.hasLearningPill).toBe(false);
        // Tone class is one of the four sanctioned buckets.
        expect(r.stateClass).toMatch(/tone-(dim|yellow|orange|red)/);
      }

      // With ~60% util ~2h into a 5h window, fleet mean pressure should already
      // put 5h into yellow-or-hotter (NOT dim).
      const row5h = rows.find((r) => r.label === "5h");
      expect(row5h).toBeDefined();
      expect(row5h!.stateClass).not.toContain("tone-dim");

      await page.close();
    } finally {
      proxy.stop();
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  }, 60_000);

  test("shows Learning pill when seasonal sample count is below threshold (empty DB)", async () => {
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

    try {
      // Keys with fleet utilization so the row renders at all, but NO timeseries.
      const e1 = proxy.km.addKey(VALID_KEY_1, "key-a");
      const e2 = proxy.km.addKey(VALID_KEY_2, "key-b");
      seedCapacityWindows(proxy.km, [e1, e2], [0.4, 0.5]);

      const forecast = await fetch(`${proxy.url}/admin/capacity/forecast`, {
        headers: { Authorization: `Bearer ${ADMIN_TOKEN}` },
      }).then((r) => r.json() as Promise<{ totalSamples: number }>);
      expect(forecast.totalSamples).toBe(0);

      const page = await openDashboard(browser, proxy.url, ADMIN_TOKEN);
      await page.waitForSelector("#fleet-pressure .fleet-pressure-row", { timeout: 10_000 });
      // Wait for forecast fetch to complete and learning pill to appear.
      await page.waitForSelector(".fleet-pressure-learning", { timeout: 10_000 });

      const learningPillCount = await page.$$eval(".fleet-pressure-learning", (els) => els.length);
      expect(learningPillCount).toBeGreaterThanOrEqual(1); // 5h and/or 7d row carries the pill

      await page.close();
    } finally {
      proxy.stop();
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  }, 60_000);

  test("hovering a gradient strip reveals a tooltip with time + expected utilization", async () => {
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

    try {
      const e1 = proxy.km.addKey(VALID_KEY_1, "key-a");
      const e2 = proxy.km.addKey(VALID_KEY_2, "key-b");
      const e3 = proxy.km.addKey(VALID_KEY_3, "key-c");
      seedSeasonalFactors(proxy.km);
      seedCapacityWindows(proxy.km, [e1, e2, e3], [0.55, 0.6, 0.65]);

      const page = await openDashboard(browser, proxy.url, ADMIN_TOKEN);
      await page.waitForSelector("#fleet-pressure .fleet-pressure-strip", { timeout: 15_000 });

      // Hover the middle of the 5h strip.
      const strip = await page.$("#fleet-pressure .fleet-pressure-strip");
      expect(strip).not.toBeNull();
      const box = await strip!.boundingBox();
      expect(box).not.toBeNull();
      const centerX = box!.x + box!.width / 2;
      const centerY = box!.y + box!.height / 2;
      await page.mouse.move(centerX, centerY);

      // Allow the mousemove handler to run and the tooltip to become visible.
      await page.waitForFunction(
        () => {
          const tip = document.querySelector("#fleet-pressure .fleet-pressure-tooltip") as HTMLElement | null;
          const utilEl = document.querySelector("#fleet-pressure .fleet-pressure-tooltip-util") as HTMLElement | null;
          if (!tip || !utilEl) return false;
          return window.getComputedStyle(tip).opacity === "1" && /\d+%/.test(utilEl.textContent ?? "");
        },
        { timeout: 5_000 },
      );

      const tipData = await page.evaluate(() => {
        const time = document.querySelector("#fleet-pressure .fleet-pressure-tooltip-time")?.textContent?.trim() ?? "";
        const util = document.querySelector("#fleet-pressure .fleet-pressure-tooltip-util")?.textContent?.trim() ?? "";
        const utilClass = document.querySelector("#fleet-pressure .fleet-pressure-tooltip-util")?.className ?? "";
        const factor = document.querySelector("#fleet-pressure .fleet-pressure-tooltip-factor")?.textContent?.trim() ?? "";
        return { time, util, utilClass, factor };
      });

      // Tooltip fields populated.
      expect(tipData.time.length).toBeGreaterThan(0);
      expect(tipData.util).toMatch(/\d+%/);
      // Tooltip util carries a tone class.
      expect(tipData.utilClass).toMatch(/tone-(dim|yellow|orange|red)/);
      // Seasonal factor line present (at least "typical hour" fallback for flat slots).
      expect(tipData.factor.length).toBeGreaterThan(0);

      await page.close();
    } finally {
      proxy.stop();
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  }, 60_000);

  test("Capacity Headroom chart plots fleet headroom across both 5h and 7d windows", async () => {
    // Build a fleet of 4 keys. Two produce telemetry on BOTH 5h and 7d windows
    // — one running hot, one cool — leaving two with no observations. Under
    // the new headroom math, unobserved keys count as 100% remaining, so a
    // single hot account on a small fleet must NOT crater the chart to "the
    // highest account's utilization". We then verify the API surfaces
    // fleetSize and the new per-key/keysObserved fields, that the dashboard
    // panel is relabeled, AND that the chart's actual plotted y-values match
    // the expected headroom math (not just the dataset labels).
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

    try {
      const hot = proxy.km.addKey(VALID_KEY_1, "headroom-hot");
      const cool = proxy.km.addKey(VALID_KEY_2, "headroom-cool");
      proxy.km.addKey(VALID_KEY_3, "headroom-quiet-1");
      proxy.km.addKey("sk-ant-api03-dddddddddddddddddddddddddddddddddd", "headroom-quiet-2");

      const resetAt5h = Date.now() + 60 * 60 * 1000;
      const resetAt7d = Date.now() + 3 * 24 * 60 * 60 * 1000;
      // hot @ 0.9 / 0.5, cool @ 0.1 / 0.3 → 5h per-key avg = 0.5, 7d = 0.4.
      // Two of four keys observed → 5h fleet headroom = 1 - 0.5*2/4 = 0.75;
      // 7d fleet headroom = 1 - 0.4*2/4 = 0.80. The two windows must plot
      // distinct y-values so we know the per-window aggregation is real.
      proxy.km.recordCapacityObservation(hot, {
        seenAt: unixMs(Date.now()),
        httpStatus: 200,
        windows: [
          { windowName: "unified-5h", status: "allowed_warning", utilization: 0.9, resetAt: unixMs(resetAt5h) },
          { windowName: "unified-7d", status: "allowed", utilization: 0.5, resetAt: unixMs(resetAt7d) },
        ],
      });
      proxy.km.recordCapacityObservation(cool, {
        seenAt: unixMs(Date.now()),
        httpStatus: 200,
        windows: [
          { windowName: "unified-5h", status: "allowed", utilization: 0.1, resetAt: unixMs(resetAt5h) },
          { windowName: "unified-7d", status: "allowed", utilization: 0.3, resetAt: unixMs(resetAt7d) },
        ],
      });

      // Flush the in-memory accumulator into capacity_window_timeseries so the
      // query endpoint actually sees the seeded rows.
      proxy.stop();
      const reloaded = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

      try {
        const tsRes = await fetch(`${reloaded.url}/admin/capacity/timeseries?hours=24&resolution=hour`, {
          headers: { Authorization: `Bearer ${ADMIN_TOKEN}` },
        });
        expect(tsRes.status).toBe(200);
        const tsBody = await tsRes.json() as {
          fleetSize: number;
          buckets: Array<{
            windowName: string;
            maxUtilization: number | null;
            avgUtilization: number | null;
            avgUtilizationPerKey: number | null;
            keysObserved: number;
          }>;
        };
        expect(tsBody.fleetSize).toBe(4);
        const fiveH = tsBody.buckets.find((b) => b.windowName === "unified-5h");
        const sevenD = tsBody.buckets.find((b) => b.windowName === "unified-7d");
        expect(fiveH).toBeDefined();
        expect(sevenD).toBeDefined();
        expect(fiveH!.keysObserved).toBe(2);
        expect(sevenD!.keysObserved).toBe(2);
        expect(fiveH!.avgUtilizationPerKey!).toBeCloseTo(0.5, 6);
        expect(sevenD!.avgUtilizationPerKey!).toBeCloseTo(0.4, 6);
        expect(fiveH!.maxUtilization).toBe(0.9);

        // Fleet headroom the dashboard plots: 1 - (perKeyAvg * observed /
        // fleetSize). For 5h: 1 - 0.5*2/4 = 0.75. For 7d: 1 - 0.4*2/4 = 0.80.
        // The OLD chart would have plotted maxUtilization 0.9 → 0.5 i.e. as
        // if the worst account spoke for the whole fleet.
        const expected5h = 1 - (fiveH!.avgUtilizationPerKey! * fiveH!.keysObserved / tsBody.fleetSize);
        const expected7d = 1 - (sevenD!.avgUtilizationPerKey! * sevenD!.keysObserved / tsBody.fleetSize);
        expect(expected5h).toBeCloseTo(0.75, 6);
        expect(expected7d).toBeCloseTo(0.80, 6);

        const page = await openDashboard(browser, reloaded.url, ADMIN_TOKEN);
        await page.waitForSelector("#chart-capacity", { timeout: 15_000 });

        // Panel title was renamed.
        const panelTitle = await page.$eval(
          "#chart-capacity",
          (canvas) => canvas.closest(".panel")?.querySelector(".panel-title")?.textContent?.trim() ?? "",
        );
        expect(panelTitle).toBe("Capacity Headroom");

        // Read the chart's dataset labels AND their actual plotted y-values
        // (not just the labels) so a bug in the bucketHeadroom JS that left
        // labels correct but plotted nonsense would be caught.
        const chartShape = await page.waitForFunction(
          () => {
            const canvas = document.getElementById("chart-capacity");
            if (!canvas) return false;
            const chart = (window as unknown as { Chart: { getChart: (c: Element) => unknown } }).Chart.getChart(canvas);
            if (!chart) return false;
            const datasets = (chart as { data: { datasets: { label: string; data: number[] }[] } }).data.datasets;
            if (!datasets || datasets.length === 0) return false;
            return datasets.map((d) => ({
              label: d.label,
              data: Array.from(d.data),
            }));
          },
          { timeout: 15_000 },
        ).then((handle) => handle.jsonValue() as Promise<Array<{ label: string; data: number[] }>>);

        // Both windows must have their own line.
        expect(chartShape.length).toBeGreaterThanOrEqual(2);
        const ds5h = chartShape.find((d) => d.label.toLowerCase().includes("5h"));
        const ds7d = chartShape.find((d) => d.label.toLowerCase().includes("7d"));
        expect(ds5h).toBeDefined();
        expect(ds7d).toBeDefined();
        for (const ds of chartShape) {
          expect(ds.label.toLowerCase()).toContain("headroom");
          expect(ds.label.toLowerCase()).not.toContain("max util");
        }

        // The chart fills in all bucket slots over the requested range — only
        // the most-recent bucket(s) carry real telemetry, the rest are gaps
        // and must plot at headroom = 1 ("unknown = 100% remaining"). We
        // identify the data-bearing bucket as the one whose 5h value matches
        // the expected 0.75; every OTHER bucket must equal exactly 1.
        const observed5h = ds5h!.data.find((v) => Math.abs(v - expected5h) < 1e-6);
        expect(observed5h).toBeDefined();
        const gapCount5h = ds5h!.data.filter((v) => v === 1).length;
        expect(gapCount5h).toBeGreaterThan(0);

        // 7d: identify the data-bearing bucket and verify its value.
        const observed7d = ds7d!.data.find((v) => Math.abs(v - expected7d) < 1e-6);
        expect(observed7d).toBeDefined();
        // 7d must plot a DIFFERENT y-value than 5h in that same bucket, proving
        // the chart computes per-window headroom (not one shared aggregate).
        expect(expected7d).not.toBeCloseTo(expected5h, 2);

        // Pure-function check on the dashboard's bucketHeadroom: empty/null
        // row → 1; a row with keysObserved exceeding fleetSize is clamped
        // (rather than going negative and being floored to 0).
        const fnChecks = await page.evaluate(() => {
          const fn = (window as unknown as { __capacityHeadroom: (row: unknown) => number }).__capacityHeadroom;
          return {
            nullRow: fn(null),
            emptyRow: fn({}),
            unknownUtil: fn({ avgUtilizationPerKey: null, keysObserved: 0 }),
            clamped: fn({ avgUtilizationPerKey: 1.0, keysObserved: 99 }), // observed >> fleetSize=4
          };
        });
        expect(fnChecks.nullRow).toBe(1);
        expect(fnChecks.emptyRow).toBe(1);
        expect(fnChecks.unknownUtil).toBe(1);
        // observed=99 clamped to fleetSize=4 → fleetUtil = 1.0 * 4 / 4 = 1 →
        // headroom = 0 (a fully-saturated fleet, not a misleading -negative).
        expect(fnChecks.clamped).toBe(0);

        await page.close();
      } finally {
        reloaded.stop();
      }
    } finally {
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  }, 60_000);

  test("Capacity Headroom chart drops a 7d-exhausted key onto the 5h line via the API's effectiveFleetUtilization", async () => {
    // Regression guard for the original report: a key that has burned through
    // its weekly quota stops sending samples (cooldown), and naive math then
    // boosts apparent 5h headroom because the exhausted key just disappears.
    // Server-computed effectiveFleetUtilization counts a 7d-exhausted key as
    // 100% on the 5h line too, so the chart reflects the real ceiling.
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

    try {
      const fried = proxy.km.addKey(VALID_KEY_1, "headroom-fried");
      const healthy = proxy.km.addKey(VALID_KEY_2, "headroom-healthy");

      const resetAt5h = Date.now() + 60 * 60 * 1000;
      const resetAt7d = Date.now() + 3 * 24 * 60 * 60 * 1000;

      // fried: 7d cap reached (util=1.0). 5h sample looks healthy (0.2) but
      // shouldn't matter — the key is weekly-blocked.
      proxy.km.recordCapacityObservation(fried, {
        seenAt: unixMs(Date.now()),
        httpStatus: 200,
        windows: [
          { windowName: "unified-5h", status: "allowed", utilization: 0.2, resetAt: unixMs(resetAt5h) },
          { windowName: "unified-7d", status: "allowed_warning", utilization: 1.0, resetAt: unixMs(resetAt7d) },
        ],
      });
      proxy.km.recordCapacityObservation(healthy, {
        seenAt: unixMs(Date.now()),
        httpStatus: 200,
        windows: [
          { windowName: "unified-5h", status: "allowed", utilization: 0.4, resetAt: unixMs(resetAt5h) },
          { windowName: "unified-7d", status: "allowed", utilization: 0.4, resetAt: unixMs(resetAt7d) },
        ],
      });

      proxy.stop();
      const reloaded = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });
      try {
        const tsRes = await fetch(`${reloaded.url}/admin/capacity/timeseries?hours=24&resolution=hour`, {
          headers: { Authorization: `Bearer ${ADMIN_TOKEN}` },
        });
        expect(tsRes.status).toBe(200);
        const tsBody = await tsRes.json() as {
          fleetSize: number;
          buckets: Array<{
            windowName: string;
            effectiveFleetUtilization: number | null;
            keysAccounted: number;
          }>;
        };
        expect(tsBody.fleetSize).toBe(2);

        const fiveH = tsBody.buckets.find((b) => b.windowName === "unified-5h");
        const sevenD = tsBody.buckets.find((b) => b.windowName === "unified-7d");
        expect(fiveH).toBeDefined();
        expect(sevenD).toBeDefined();

        // 5h with cross-fold: fried counts as 1.0 (weekly-blocked), healthy 0.4.
        // Effective fleet util = (1.0 + 0.4) / 2 = 0.70 → headroom = 0.30.
        expect(fiveH!.effectiveFleetUtilization!).toBeCloseTo(0.70, 6);
        // 7d: fried 1.0, healthy 0.4 → 0.70 → headroom = 0.30.
        expect(sevenD!.effectiveFleetUtilization!).toBeCloseTo(0.70, 6);
        expect(fiveH!.keysAccounted).toBe(2);

        const page = await openDashboard(browser, reloaded.url, ADMIN_TOKEN);
        await page.waitForSelector("#chart-capacity", { timeout: 15_000 });

        const chartShape = await page.waitForFunction(
          () => {
            const canvas = document.getElementById("chart-capacity");
            if (!canvas) return false;
            const chart = (window as unknown as { Chart: { getChart: (c: Element) => unknown } }).Chart.getChart(canvas);
            if (!chart) return false;
            const datasets = (chart as { data: { datasets: { label: string; data: number[] }[] } }).data.datasets;
            if (!datasets || datasets.length === 0) return false;
            return datasets.map((d) => ({ label: d.label, data: Array.from(d.data) }));
          },
          { timeout: 15_000 },
        ).then((handle) => handle.jsonValue() as Promise<Array<{ label: string; data: number[] }>>);

        const ds5h = chartShape.find((d) => d.label.toLowerCase().includes("5h"));
        expect(ds5h).toBeDefined();

        // The 5h line in the data-bearing bucket must show 0.30 headroom
        // (cross-fold from 7d), NOT 0.80 (which is what a naïve 1 - 0.4*1/2
        // calculation that ignored the exhausted key would produce).
        const expected5h = 0.30;
        const plottedExpected = ds5h!.data.find((v) => Math.abs(v - expected5h) < 1e-6);
        expect(plottedExpected).toBeDefined();
        const wrongValue = ds5h!.data.find((v) => Math.abs(v - 0.80) < 1e-6);
        expect(wrongValue).toBeUndefined();

        await page.close();
      } finally {
        reloaded.stop();
      }
    } finally {
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  }, 60_000);

  test("serves /admin/capacity/forecast with 168 slots and reflects Tuesday 2pm UTC spike", async () => {
    // Pure API assertion guarding the data contract the dashboard relies on.
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });
    try {
      seedSeasonalFactors(proxy.km);
      const res = await fetch(`${proxy.url}/admin/capacity/forecast`, {
        headers: { Authorization: `Bearer ${ADMIN_TOKEN}` },
      });
      expect(res.status).toBe(200);
      const body = await res.json() as {
        weeks: number;
        generatedAt: string;
        totalSamples: number;
        slots: Array<{ dow: number; hour: number; factor: number; samples: number }>;
      };
      expect(body.slots).toHaveLength(168);
      expect(body.totalSamples).toBeGreaterThan(336);
      const spike = body.slots.find((s) => s.dow === 2 && s.hour === 14);
      expect(spike).toBeDefined();
      expect(spike!.factor).toBeGreaterThan(1);
      const quiet = body.slots.find((s) => s.dow === 0 && s.hour === 3);
      expect(quiet!.factor).toBeLessThan(spike!.factor);
    } finally {
      proxy.stop();
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  });
});

describe("Dashboard sessions cell (Vue render)", () => {
  let browser: Browser;

  beforeAll(async () => {
    browser = await launchBrowser();
  });

  afterAll(async () => {
    await browser.close();
  });

  test("renders sessions with actor, age, request count and aligns columns across rows", async () => {
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

    try {
      // One key so the bucket-of-3 routing doesn't split conversations across
      // accounts — we want both sessions and all of multi's three conversations
      // pinned to the same key for the per-cell alignment assertions.
      proxy.km.addKey(VALID_KEY_1, "key-a");

      // Seed the targeted key with a session that has 1 conversation, plus a
      // session that has 3 sub-agent conversations sharing one session-id —
      // the multi-conv case the redesign exists for.
      const sessionLone = "10000000-0000-0000-0000-000000000001";
      const sessionMulti = "20000000-0000-0000-0000-000000000002";
      const aLoneHash = "aaaaaaaaaaaaaaaa";
      const bMulti1 = "bbbb111111111111";
      const bMulti2 = "bbbb222222222222";
      const bMulti3 = "bbbb333333333333";

      // Use an explicit actor in the conversation key so the cell shows it.
      const actor = "till@trainly";
      proxy.km.getKeyForConversation(`${actor}:${sessionLone}:${aLoneHash}`, sessionLone);
      proxy.km.getKeyForConversation(`${actor}:${sessionMulti}:${bMulti1}`, sessionMulti);
      proxy.km.getKeyForConversation(`${actor}:${sessionMulti}:${bMulti2}`, sessionMulti);
      proxy.km.getKeyForConversation(`${actor}:${sessionMulti}:${bMulti3}`, sessionMulti);

      const page = await openDashboard(browser, proxy.url, ADMIN_TOKEN);
      await page.waitForSelector("#key-cards .ops-table tbody tr", { timeout: 15_000 });
      // Vue render is synchronous after data lands, but give it a tick.
      await page.waitForSelector(".session-group", { timeout: 5_000 });

      // 1. Sessions cell shows both sessions for key-a, sorted alphabetically.
      const sessionIds = await page.$$eval(
        "#key-cards .ops-table tbody tr:first-child .session-group .cell-id",
        (els) => els.map((e) => e.getAttribute("title")),
      );
      expect(sessionIds).toEqual([sessionLone, sessionMulti]);

      // 2. The multi-conv session shows ×3 chip and 3 conversation rows.
      const multiGroup = await page.$$eval(
        "#key-cards .ops-table tbody tr:first-child .session-group",
        (groups) =>
          groups.map((g) => ({
            sessionId: g.querySelector(".cell-id")?.getAttribute("title"),
            countText: g.querySelector(".session-count")?.textContent ?? "",
            convHashes: [...g.querySelectorAll(".cell-hash")].map((c) => c.getAttribute("title")),
            actor: g.querySelector(".cell-actor")?.textContent?.trim() ?? "",
            metaText: g.querySelector(".cell-meta-row")?.textContent?.replace(/\s+/g, " ").trim() ?? "",
          })),
      );
      const multi = multiGroup.find((m) => m.sessionId === sessionMulti);
      const lone = multiGroup.find((m) => m.sessionId === sessionLone);
      expect(multi).toBeDefined();
      expect(multi!.countText).toBe("×3");
      expect(multi!.convHashes.sort()).toEqual([bMulti1, bMulti2, bMulti3].sort());
      expect(multi!.actor).toBe(actor);
      // Meta row should mention "old" and "req" — actor first, then age, then req count.
      expect(multi!.metaText).toContain(actor);
      expect(multi!.metaText).toContain("old");
      expect(multi!.metaText).toContain("req");
      // Single-conv session has no count chip and no conversation rows.
      expect(lone).toBeDefined();
      expect(lone!.countText).toBe("");
      expect(lone!.convHashes).toEqual([]);

      // 3. Column alignment: identifiers share the same x-left edge and
      //    every right cell shares the same x-right edge across the session
      //    row and its sub-conversation rows — within a small tolerance.
      //    Session identity is now carried by a tinted background, not a
      //    leading colored dot, so we no longer assert dot positions.
      const alignment = await page.$eval(
        "#key-cards .ops-table tbody tr:first-child td:nth-child(5)",
        (cell) => {
          const ids = [...cell.querySelectorAll(".cell-id, .cell-hash")];
          const rights = [...cell.querySelectorAll(".cell-right")];
          const idLefts = ids.map((e) => (e as HTMLElement).getBoundingClientRect().left);
          const rightRights = rights.map((e) => (e as HTMLElement).getBoundingClientRect().right);
          const spread = (vals: number[]) => Math.max(...vals) - Math.min(...vals);
          // Each session-group must paint a non-transparent tinted background
          // — that's the new identity affordance replacing the dot.
          const groups = [...cell.querySelectorAll(".session-group")] as HTMLElement[];
          const tints = groups.map((g) => window.getComputedStyle(g).backgroundColor);
          return {
            ids: ids.length,
            rights: rights.length,
            idSpread: spread(idLefts),
            rightSpread: spread(rightRights),
            tints,
          };
        },
      );
      expect(alignment.ids).toBeGreaterThan(2); // session + 3 conversations
      expect(alignment.rights).toBeGreaterThan(2);
      expect(alignment.idSpread).toBeLessThan(2); // sub-pixel tolerance
      expect(alignment.rightSpread).toBeLessThan(2);
      // Both session groups in this cell (lone + multi) must have a visible
      // background tint — and the tints must differ so the two sessions read
      // as visually distinct.
      expect(alignment.tints.length).toBe(2);
      for (const tint of alignment.tints) {
        expect(tint).not.toBe("rgba(0, 0, 0, 0)");
        expect(tint).not.toBe("transparent");
      }
      expect(alignment.tints[0]).not.toBe(alignment.tints[1]);

      // 4. No JS errors during render.
      const errors: string[] = [];
      page.on("pageerror", (e) => errors.push(e.message));
      // Force a re-render by simulating a 1s tick equivalent (the dashboard
      // also auto-ticks; this just exercises the path explicitly).
      await page.evaluate(() => (window as unknown as { renderKeys: () => void }).renderKeys());
      expect(errors).toEqual([]);
    } finally {
      proxy.stop();
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  });

  test("single-conv session: chart sits inline with the session row, keyed by the conversation hash", async () => {
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

    try {
      proxy.km.addKey(VALID_KEY_1, "key-a");
      const sessionId = "30000000-0000-0000-0000-000000000003";
      const hash = "feedabe1deadbeef";
      proxy.km.getKeyForConversation(`till@trainly:${sessionId}:${hash}`, sessionId);

      const page = await openDashboard(browser, proxy.url, ADMIN_TOKEN);
      await page.waitForSelector(".session-group .thr-canvas", { timeout: 15_000 });

      // Token event must include conversationHash matching the conv. Only
      // events with the right (sessionId, conversationHash) pair will land
      // in this chart's bucket.
      emitWithKeys(
        { type: "tokens", ts: new Date().toISOString(), label: "key-a",
          sessionId, conversationHash: hash,
          input: 800, output: 0, cacheRead: 200, cacheCreation: 0, partial: true },
        proxy.km.listKeys(),
      );
      emitWithKeys(
        { type: "tokens", ts: new Date().toISOString(), label: "key-a",
          sessionId, conversationHash: hash,
          input: 0, output: 350, cacheRead: 0, cacheCreation: 0, partial: true },
        proxy.km.listKeys(),
      );

      await page.waitForFunction(
        (sid: string) => {
          const group = [...document.querySelectorAll(".session-group")].find(
            (el) => el.querySelector(".cell-id")?.getAttribute("title") === sid,
          );
          if (!group) return false;
          const inEl = group.querySelector(".thr-in") as HTMLElement | null;
          const outEl = group.querySelector(".thr-out") as HTMLElement | null;
          if (!inEl || !outEl) return false;
          return inEl.textContent?.includes("1.0k") === true
            && outEl.textContent?.includes("350") === true;
        },
        { timeout: 5_000 },
        sessionId,
      );

      // A single-conv session should have exactly one chart.
      const chartCount = await page.$$eval(".session-group .thr-canvas", (els) => els.length);
      expect(chartCount).toBe(1);

      const pixelStats = await page.$eval(
        ".session-group .thr-canvas",
        (canvas) => {
          const ctx = (canvas as HTMLCanvasElement).getContext("2d")!;
          const { width, height } = canvas as HTMLCanvasElement;
          const data = ctx.getImageData(0, 0, width, height).data;
          let blue = 0, green = 0, opaque = 0;
          for (let i = 0; i < data.length; i += 4) {
            const r = data[i], g = data[i + 1], b = data[i + 2], a = data[i + 3];
            if (a === 0) continue;
            opaque++;
            if (b > 200 && r < 130) blue++;
            if (g > 150 && r < 100 && b < 120) green++;
          }
          return { blue, green, opaque };
        },
      );
      expect(pixelStats.opaque).toBeGreaterThan(0);
      expect(pixelStats.blue).toBeGreaterThan(0);
      expect(pixelStats.green).toBeGreaterThan(0);

      await page.close();
    } finally {
      proxy.stop();
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  }, 60_000);

  test("multi-conv session: one chart per sub-conversation, each fed independently by conversationHash", async () => {
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

    try {
      proxy.km.addKey(VALID_KEY_1, "key-a");
      const sessionId = "50000000-0000-0000-0000-000000000005";
      const hashA = "aaaaaaaaaaaaaaaa";
      const hashB = "bbbbbbbbbbbbbbbb";
      proxy.km.getKeyForConversation(`till@trainly:${sessionId}:${hashA}`, sessionId);
      proxy.km.getKeyForConversation(`till@trainly:${sessionId}:${hashB}`, sessionId);

      const page = await openDashboard(browser, proxy.url, ADMIN_TOKEN);
      await page.waitForFunction(
        () => document.querySelectorAll(".session-group .thr-canvas").length >= 2,
        { timeout: 15_000 },
      );

      const keys = proxy.km.listKeys();
      // Send to conv A only. Conv B's chart must stay quiet.
      emitWithKeys(
        { type: "tokens", ts: new Date().toISOString(), label: "key-a",
          sessionId, conversationHash: hashA,
          input: 1500, output: 600, cacheRead: 0, cacheCreation: 0, partial: true },
        keys,
      );

      // Per-conv legends. Conv A shows the totals; conv B stays at 0/0.
      await page.waitForFunction(
        (sid: string, ha: string) => {
          const group = [...document.querySelectorAll(".session-group")].find(
            (el) => el.querySelector(".cell-id")?.getAttribute("title") === sid,
          );
          if (!group) return false;
          // Each conversation row has its own .thr-in / .thr-out.
          const ins  = [...group.querySelectorAll(".thr-in")];
          const outs = [...group.querySelectorAll(".thr-out")];
          if (ins.length < 2 || outs.length < 2) return false;
          // Find the conv row whose .cell-hash title matches hashA — its
          // chart should be the one carrying the totals.
          const convRows = [...group.querySelectorAll(".cell-hash")];
          const hashes = convRows.map((c) => c.getAttribute("title"));
          if (!hashes.includes(ha)) return false;
          const aIdx = hashes.indexOf(ha);
          const aIn  = ins[aIdx]?.textContent  ?? "";
          const aOut = outs[aIdx]?.textContent ?? "";
          return aIn.includes("1.5k") && aOut.includes("600");
        },
        { timeout: 5_000 },
        sessionId,
        hashA,
      );

      const summary = await page.evaluate((sid: string, ha: string, hb: string) => {
        const group = [...document.querySelectorAll(".session-group")].find(
          (el) => el.querySelector(".cell-id")?.getAttribute("title") === sid,
        ) as HTMLElement | undefined;
        if (!group) return null;
        const convRows = [...group.querySelectorAll(".cell-hash")] as HTMLElement[];
        const ins  = [...group.querySelectorAll(".thr-in")]  as HTMLElement[];
        const outs = [...group.querySelectorAll(".thr-out")] as HTMLElement[];
        const hashes = convRows.map((c) => c.getAttribute("title"));
        const aIdx = hashes.indexOf(ha);
        const bIdx = hashes.indexOf(hb);
        return {
          chartCount: group.querySelectorAll(".thr-canvas").length,
          convCount:  convRows.length,
          aIn:  ins[aIdx]?.textContent  ?? "",
          aOut: outs[aIdx]?.textContent ?? "",
          bIn:  ins[bIdx]?.textContent  ?? "",
          bOut: outs[bIdx]?.textContent ?? "",
        };
      }, sessionId, hashA, hashB);

      expect(summary).not.toBeNull();
      // Two conversations → two charts under the multi-conv session row.
      expect(summary!.chartCount).toBe(2);
      expect(summary!.convCount).toBe(2);
      expect(summary!.aIn).toContain("1.5k");
      expect(summary!.aOut).toContain("600");
      // Conv B never received an event, so its legend is still at 0.
      expect(summary!.bIn).toContain("0");
      expect(summary!.bOut).toContain("0");

      await page.close();
    } finally {
      proxy.stop();
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  }, 60_000);

  test("session-move button reassigns a session's affinity to another available key", async () => {
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

    try {
      proxy.km.addKey(VALID_KEY_1, "key-a");
      proxy.km.addKey(VALID_KEY_2, "key-b");

      // Pin a session to key-a (bucket-of-3 routing fills the first available
      // key first, so a single brand-new session lands on whichever sorts
      // first).
      const sessionId = "60000000-0000-0000-0000-000000000006";
      const hash = "cafebabedeadbeef";
      const initial = proxy.km.getKeyForConversation(
        `till@trainly:${sessionId}:${hash}`, sessionId,
      );
      const originalKeyLabel = proxy.km.listKeys()
        .find((k) => k.maskedKey === initial.entry?.key.slice(0, 10) + "..." + initial.entry?.key.slice(-4))
        ?.label ?? "";
      expect(originalKeyLabel.length).toBeGreaterThan(0);

      const page = await openDashboard(browser, proxy.url, ADMIN_TOKEN);
      await page.waitForSelector(".session-group .session-move", { timeout: 15_000 });

      await page.evaluate((sid: string) => {
        const group = [...document.querySelectorAll(".session-group")].find(
          (el) => el.querySelector(".cell-id")?.getAttribute("title") === sid,
        );
        const btn = group?.querySelector(".session-move") as HTMLButtonElement | null;
        if (!btn) throw new Error("move button not found");
        btn.click();
      }, sessionId);

      await page.waitForSelector(".picker-overlay .picker-row:not(.disabled)", { timeout: 5_000 });
      await page.evaluate((sid: string, fromLabel: string) => {
        const group = [...document.querySelectorAll(".session-group")].find(
          (el) => el.querySelector(".cell-id")?.getAttribute("title") === sid,
        );
        const rows = [...(group?.querySelectorAll(".picker-overlay .picker-row:not(.disabled)") ?? [])];
        const target = rows.find((r) => (r.querySelector(".picker-label")?.textContent ?? "").trim() !== fromLabel)
          ?? rows[0];
        if (!target) throw new Error("no picker row available");
        (target as HTMLElement).click();
      }, sessionId, originalKeyLabel);

      await page.waitForFunction(() => !document.querySelector(".picker-overlay"), { timeout: 2_000 });

      const after = proxy.km.getKeyForConversation(
        `till@trainly:${sessionId}:${hash}`, sessionId,
      );
      const afterLabel = proxy.km.listKeys().find(
        (k) => k.maskedKey === after.entry?.key.slice(0, 10) + "..." + after.entry?.key.slice(-4),
      )?.label ?? "";
      expect(after.affinityHit).toBe(true);
      expect(afterLabel).not.toBe(originalKeyLabel);

      await page.close();
    } finally {
      proxy.stop();
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  }, 60_000);

  test("log scale keeps quiet bars visible alongside loud ones, but loud still reads taller", async () => {
    const dataDir = makeTempDir();
    const upstream = startMockUpstream();
    const proxy = startProxy({ dataDir, upstream: upstream.url, adminToken: ADMIN_TOKEN });

    try {
      proxy.km.addKey(VALID_KEY_1, "key-a");
      const loudSession  = "40000000-0000-0000-0000-00000000004a";
      const quietSession = "40000000-0000-0000-0000-00000000004b";
      const loudHash  = "1111111111111111";
      const quietHash = "2222222222222222";
      proxy.km.getKeyForConversation(`till@trainly:${loudSession}:${loudHash}`,  loudSession);
      proxy.km.getKeyForConversation(`till@trainly:${quietSession}:${quietHash}`, quietSession);

      const page = await openDashboard(browser, proxy.url, ADMIN_TOKEN);
      await page.waitForFunction(
        () => document.querySelectorAll(".session-group .thr-canvas").length >= 2,
        { timeout: 15_000 },
      );

      const keys = proxy.km.listKeys();
      // 100× linear input ratio. On a *linear* scale the quiet bar would
      // be 1% of the loud bar (one pixel) and the whole point of the
      // chart would be lost. On a log scale the quiet bar should still
      // be a meaningful fraction of the loud bar's height.
      emitWithKeys(
        { type: "tokens", ts: new Date().toISOString(), label: "key-a",
          sessionId: loudSession, conversationHash: loudHash,
          input: 100000, output: 0, cacheRead: 0, cacheCreation: 0, partial: true },
        keys,
      );
      emitWithKeys(
        { type: "tokens", ts: new Date().toISOString(), label: "key-a",
          sessionId: quietSession, conversationHash: quietHash,
          input: 1000, output: 500, cacheRead: 0, cacheCreation: 0, partial: true },
        keys,
      );

      await page.waitForFunction(
        (loud: string, quiet: string) => {
          const groups = [...document.querySelectorAll(".session-group")] as HTMLElement[];
          const ofId = (sid: string) => groups.find((g) => g.querySelector(".cell-id")?.getAttribute("title") === sid);
          const a = ofId(loud), b = ofId(quiet);
          if (!a || !b) return false;
          const txt = (el: Element | null) => el?.textContent ?? "";
          return txt(a.querySelector(".thr-in")).includes("100k")
            && txt(b.querySelector(".thr-in")).includes("1.0k");
        },
        { timeout: 5_000 },
        loudSession,
        quietSession,
      );
      // Let one ticker pass refine peakIn/peakOut and trigger a repaint.
      await new Promise((resolve) => setTimeout(resolve, 400));

      const tallest = await page.evaluate((loud: string, quiet: string) => {
        const groups = [...document.querySelectorAll(".session-group")] as HTMLElement[];
        const ofId = (sid: string) => groups.find((g) => g.querySelector(".cell-id")?.getAttribute("title") === sid);
        function tallestBlue(group: HTMLElement | undefined): number {
          if (!group) return -1;
          const canvas = group.querySelector(".thr-canvas") as HTMLCanvasElement | null;
          if (!canvas) return -1;
          const ctx = canvas.getContext("2d")!;
          const { width, height } = canvas;
          const data = ctx.getImageData(0, 0, width, height).data;
          let best = 0;
          for (let x = 0; x < width; x++) {
            let count = 0;
            for (let y = 0; y < height; y++) {
              const i = (y * width + x) * 4;
              const r = data[i], g = data[i + 1], b = data[i + 2], a = data[i + 3];
              if (a !== 0 && b > 200 && r < 130 && g < 200) count++;
            }
            if (count > best) best = count;
          }
          return best;
        }
        return {
          loud:  tallestBlue(ofId(loud)),
          quiet: tallestBlue(ofId(quiet)),
        };
      }, loudSession, quietSession);

      expect(tallest.loud).toBeGreaterThan(0);
      expect(tallest.quiet).toBeGreaterThan(0);
      // Loud is taller — direction is preserved.
      expect(tallest.loud).toBeGreaterThan(tallest.quiet);
      // But the quiet bar is still a meaningful fraction of the loud one
      // (anything > ~5% proves the log scale isn't behaving linearly —
      // 1% input ratio on a linear scale would round to 1px).
      expect(tallest.quiet).toBeGreaterThan(tallest.loud * 0.25);

      await page.close();
    } finally {
      proxy.stop();
      upstream.stop();
      cleanupTempDir(dataDir);
    }
  }, 60_000);
});
