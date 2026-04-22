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
