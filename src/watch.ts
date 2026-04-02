import { loadConfig } from "./config.ts";
import type { MaskedKeyEntry } from "./types.ts";

// ── ANSI ─────────────────────────────────────────────────────────

const ALT_ON = "\x1b[?1049h";
const ALT_OFF = "\x1b[?1049l";
const HIDE_CUR = "\x1b[?25l";
const SHOW_CUR = "\x1b[?25h";
const CLEAR = "\x1b[2J\x1b[H";
const BOLD = "\x1b[1m";
const DIM = "\x1b[2m";
const RST = "\x1b[0m";
const fg = (n: number) => `\x1b[38;5;${n}m`;

const KEY_COLORS = [14, 13, 11, 10, 12, 9, 208, 177] as const;
const GREEN = fg(10);
const YELLOW = fg(11);
const RED = fg(9);
const GRAY = fg(240);
const WHITE = fg(15);
const CYAN = fg(14);

// ── Helpers ──────────────────────────────────────────────────────

function fmtTime(iso: string): string {
  const d = new Date(iso);
  return [d.getHours(), d.getMinutes(), d.getSeconds()].map((n) => String(n).padStart(2, "0")).join(":");
}

function fmtNum(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 10_000) return `${(n / 1_000).toFixed(0)}k`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

function fmtCountdown(ms: number): string {
  const totalSec = Math.ceil(ms / 1000);
  const d = Math.floor(totalSec / 86400);
  const h = Math.floor((totalSec % 86400) / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h${String(m).padStart(2, "0")}m`;
  if (m > 0) return `${m}m${String(s).padStart(2, "0")}s`;
  return `${s}s`;
}

function fmtUptime(ms: number): string {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m${String(s % 60).padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  return `${h}h${String(m % 60).padStart(2, "0")}m`;
}

function bar(ratio: number, width: number, color: string): string {
  const filled = Math.round(ratio * width);
  return color + "\u2588".repeat(filled) + GRAY + "\u2591".repeat(width - filled) + RST;
}

function pad(s: string, n: number): string {
  const vis = s.replace(/\x1b\[[0-9;]*m/g, "");
  return vis.length >= n ? s : s + " ".repeat(n - vis.length);
}

function trunc(s: string, n: number): string {
  return s.length > n ? s.slice(0, n - 1) + "\u2026" : s;
}

// ── Color assignment ─────────────────────────────────────────────

const colorMap = new Map<string, string>();
let colorIdx = 0;
function keyColor(label: string): string {
  let c = colorMap.get(label);
  if (!c) {
    c = fg(KEY_COLORS[colorIdx++ % KEY_COLORS.length]!);
    colorMap.set(label, c);
  }
  return c;
}

// ── Event types ──────────────────────────────────────────────────

interface Event {
  type: string;
  ts: string;
  label?: string;
  keys?: MaskedKeyEntry[];
  [k: string]: unknown;
}

// ── Render ────────────────────────────────────────────────────────

function render(
  keys: readonly MaskedKeyEntry[],
  activity: Event[],
  startedAt: number,
  connected: boolean,
  paused: boolean,
  cols: number,
  rows: number,
): void {
  const totalReqs = keys.reduce((s, k) => s + k.stats.totalRequests, 0);
  const availCount = keys.filter((k) => k.isAvailable).length;
  const sep = DIM + "\u2500".repeat(cols) + RST;
  const connStatus = connected ? `${GREEN}\u25cf${RST}` : `${RED}\u25cf reconnecting${RST}`;

  // Header
  const hdr = `${BOLD}${CYAN} CLAUDE PROXY ${RST} ${connStatus} ${DIM}\u2502${RST} ${WHITE}${keys.length}${RST} keys  ${DIM}\u2502${RST} ${GREEN}${availCount}${RST} avail  ${DIM}\u2502${RST} ${WHITE}${totalReqs}${RST} reqs  ${DIM}\u2502${RST} ${DIM}${fmtUptime(Date.now() - startedAt)}${RST}`;

  // Keys
  const keyLines: string[] = [];
  for (const k of keys.slice(0, 8)) {
    const c = keyColor(k.label);
    const ratio = totalReqs > 0 ? k.stats.totalRequests / totalReqs : 0;
    let badge: string;
    if (k.isAvailable) {
      badge = `${GREEN}[OK]${RST}`;
    } else {
      const remaining = k.availableAt - Date.now();
      badge = remaining > 0
        ? `${YELLOW}[RATE-LTD ${fmtCountdown(remaining)}]${RST}`
        : `${YELLOW}[RATE-LTD]${RST}`;
    }
    keyLines.push(
      `${pad(`  ${c}${BOLD}${k.label}${RST}`, 16)}  ${bar(ratio, 14, c)}  ${pad(`${WHITE}${String(k.stats.totalRequests).padStart(4)} req${RST}`, 12)}  ${DIM}${fmtNum(k.stats.totalTokensIn).padStart(6)}/${fmtNum(k.stats.totalTokensOut).padStart(6)} tok${RST}  ${badge}`
    );
  }
  if (keys.length === 0) keyLines.push(`  ${DIM}Waiting for server...${RST}`);

  // Activity log
  const fixedRows = 1 + 1 + Math.max(keyLines.length, 1) + 1 + 1 + 1 + 1;
  const logRows = Math.max(3, rows - fixedRows);
  const visible = activity.slice(-(logRows - 1));
  const logLines = [`  ${BOLD}RECENT ACTIVITY${RST}`];
  for (const e of visible) {
    const time = `${GRAY}${fmtTime(e.ts)}${RST}`;
    const lbl = e.label ? pad(`${keyColor(e.label)}${e.label}${RST}`, 10) : pad(`${DIM}system${RST}`, 10);
    const usr = e["user"] ? pad(`${WHITE}${e["user"]}${RST}`, 10) : pad("", 10);
    logLines.push(`  ${time}  ${lbl}  ${usr}  ${fmtEvent(e, cols)}`);
  }
  while (logLines.length < logRows) logLines.push("");

  // Footer
  const ctrl = `${DIM}  q${RST} quit  ${DIM}\u2502${RST}  ${DIM}SPACE${RST} pause  ${DIM}\u2502${RST}  ${DIM}c${RST} clear`;
  const pind = paused ? `${YELLOW}${BOLD} PAUSED ${RST}` : "";
  const cLen = ctrl.replace(/\x1b\[[0-9;]*m/g, "").length;
  const pLen = pind.replace(/\x1b\[[0-9;]*m/g, "").length;
  const footer = ctrl + " ".repeat(Math.max(0, cols - cLen - pLen)) + pind;

  process.stdout.write(
    CLEAR + "\x1b[1;1H" +
    [pad(hdr, cols), sep, keyLines.join("\n"), sep, logLines.join("\n"), sep, footer].join("\n")
  );
}

function fmtEvent(e: Event, _cols: number): string {
  switch (e.type) {
    case "request": {
      const msg = pad(`${WHITE}Proxying request${RST}`, 30);
      return `${msg}  ${DIM}${e["method"]} ${trunc(String(e["path"]), 20)}  #${e["attempt"]}${RST}`;
    }
    case "response": {
      const s = e["status"] as number;
      const c = s >= 200 && s < 400 ? GREEN : s === 429 ? YELLOW : RED;
      const msg = pad(`${c}Upstream responded${RST}`, 30);
      return `${msg}  ${c}${s}${RST}`;
    }
    case "tokens": {
      const msg = pad(`${GREEN}Token usage${RST}`, 30);
      return `${msg}  ${GREEN}${fmtNum(e["input"] as number)} in / ${fmtNum(e["output"] as number)} out${RST}`;
    }
    case "rate_limit": {
      const msg = pad(`${YELLOW}${BOLD}Rate limited${RST}`, 30);
      return `${msg}  ${YELLOW}retry ${e["retryAfter"]}s, ${e["availableKeys"]} avail${RST}`;
    }
    case "error": {
      const msg = pad(`${RED}Error${RST}`, 30);
      const detail = e["status"] ? String(e["status"]) : trunc(String(e["error"] ?? ""), 40);
      return `${msg}  ${RED}${detail}${RST}`;
    }
    default:
      return DIM + e.type + RST;
  }
}

// ── Main ─────────────────────────────────────────────────────────

export async function startWatch(): Promise<void> {
  const config = loadConfig();
  const url = `http://localhost:${config.port}/admin/events`;

  let keys: readonly MaskedKeyEntry[] = [];
  const activity: Event[] = [];
  const MAX_ACTIVITY = 200;
  const startedAt = Date.now();
  let paused = false;
  let cols = process.stdout.columns ?? 80;
  let rows = process.stdout.rows ?? 24;

  // Terminal setup
  let cleaned = false;
  function cleanup() {
    if (cleaned) return;
    cleaned = true;
    process.stdout.write(SHOW_CUR + ALT_OFF);
    try { process.stdin.setRawMode?.(false); } catch {}
  }
  process.on("SIGINT", () => { cleanup(); process.exit(0); });
  process.on("SIGTERM", () => { cleanup(); process.exit(0); });
  process.on("exit", cleanup);
  process.stdout.write(ALT_ON + HIDE_CUR);

  if (typeof process.stdin.setRawMode === "function") {
    process.stdin.setRawMode(true);
    process.stdin.resume();
    process.stdin.setEncoding("utf-8");
  }

  function doRender() {
    render(keys, activity, startedAt, connected, paused, cols, rows);
  }

  process.stdin.on("data", (key: string) => {
    if (key === "q" || key === "\x03") { cleanup(); process.exit(0); }
    if (key === "c") { activity.length = 0; doRender(); }
    if (key === " ") { paused = !paused; doRender(); }
  });

  process.stdout.on("resize", () => {
    cols = process.stdout.columns ?? 80;
    rows = process.stdout.rows ?? 24;
    doRender();
  });

  let connected = false;
  doRender();

  // Re-render every second for uptime counter + connection status
  setInterval(() => { if (!paused) render(keys, activity, startedAt, connected, paused, cols, rows); }, 1_000);

  // Connect to SSE with auto-reconnect
  async function connect(): Promise<void> {
    while (true) {
      try {
        const res = await fetch(url);
        if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
        connected = true;

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buf = "";

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buf += decoder.decode(value, { stream: true });
          const parts = buf.split("\n\n");
          buf = parts.pop() ?? "";

          for (const part of parts) {
            const dataLine = part.split("\n").find((l) => l.startsWith("data: "));
            if (!dataLine) continue;
            try {
              const event = JSON.parse(dataLine.slice(6)) as Event;
              if (event.keys) keys = event.keys as MaskedKeyEntry[];
              if (event.type !== "keys") {
                activity.push(event);
                if (activity.length > MAX_ACTIVITY) activity.shift();
              }
            } catch {}
          }
        }
      } catch {
        connected = false;
      }

      // Wait before reconnecting
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  connect();
}
