import { openSync, readSync, readFileSync, fstatSync, existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { logDir } from "./service.ts";
import { loadConfig } from "./config.ts";
import type { StoredState, ApiKeyEntry, UnixMs } from "./types.ts";

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

interface LogEntry { ts: string; level: string; msg: string; label?: string; [k: string]: unknown }

function fmtTime(iso: string): string {
  const d = new Date(iso);
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}:${String(d.getSeconds()).padStart(2, "0")}`;
}

function fmtNum(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 10_000) return `${(n / 1_000).toFixed(0)}k`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
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

function sep(cols: number): string {
  return DIM + "\u2500".repeat(cols) + RST;
}

// ── Read state.json ──────────────────────────────────────────────

function readState(statePath: string): StoredState | null {
  try {
    return JSON.parse(readFileSync(statePath, "utf-8")) as StoredState;
  } catch {
    return null;
  }
}

// ── Log file discovery ───────────────────────────────────────────

function findLogFiles(): string[] {
  const dir = logDir();
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((f) => f.startsWith("output-") && f.endsWith(".log"))
    .sort()
    .map((f) => join(dir, f));
}

// ── Color assignment ─────────────────────────────────────────────

const keyColorMap = new Map<string, string>();
let colorIdx = 0;

function keyColor(label: string): string {
  let c = keyColorMap.get(label);
  if (!c) {
    c = fg(KEY_COLORS[colorIdx % KEY_COLORS.length]!);
    keyColorMap.set(label, c);
    colorIdx++;
  }
  return c;
}

// ── Log entry formatting ─────────────────────────────────────────

function msgColor(e: LogEntry): string {
  switch (e.msg) {
    case "Token usage": case "Token usage (stream)": return GREEN;
    case "Rate limited, trying next key": case "Key rate-limited": return YELLOW;
    case "Upstream error": case "Upstream fetch failed": return RED;
    case "Upstream responded": {
      const s = e["status"] as number;
      return s >= 200 && s < 400 ? GREEN : s === 429 ? YELLOW : RED;
    }
    default: return "";
  }
}

function logDetail(e: LogEntry): string {
  switch (e.msg) {
    case "Proxying request":
      return `${DIM}${e["method"]} ${trunc(String(e["path"]), 20)}  #${e["attempt"]}${RST}`;
    case "Upstream responded": {
      const s = e["status"] as number;
      const c = s >= 200 && s < 400 ? GREEN : s === 429 ? YELLOW : RED;
      return `${c}${s}${RST}`;
    }
    case "Token usage": case "Token usage (stream)":
      return `${GREEN}${fmtNum(e["input"] as number)} in / ${fmtNum(e["output"] as number)} out${RST}`;
    case "Rate limited, trying next key":
      return `${YELLOW}retry ${e["retryAfter"]}s, ${e["availableKeys"]} avail${RST}`;
    case "Key rate-limited":
      return `${YELLOW}${BOLD}retry ${e["retryAfterSecs"]}s${RST}`;
    case "Upstream error":
      return `${RED}${e["status"]}${RST}`;
    case "Upstream fetch failed":
      return `${RED}${trunc(String(e["error"] ?? ""), 40)}${RST}`;
    default:
      return DIM + trunc(e.msg, 40) + RST;
  }
}

// ── Render ────────────────────────────────────────────────────────

function render(
  keys: readonly ApiKeyEntry[],
  entries: LogEntry[],
  startedAt: number,
  paused: boolean,
  cols: number,
  rows: number,
): void {
  const now = Date.now() as UnixMs;
  const totalReqs = keys.reduce((s, k) => s + k.stats.totalRequests, 0);
  const availCount = keys.filter((k) => k.availableAt <= now).length;

  // Header
  const hdr = `${BOLD}${CYAN} CLAUDE PROXY ${RST} ${DIM}│${RST} ${WHITE}${keys.length}${RST} keys  ${DIM}│${RST} ${GREEN}${availCount}${RST} avail  ${DIM}│${RST} ${WHITE}${totalReqs}${RST} reqs  ${DIM}│${RST} ${DIM}${fmtUptime(Date.now() - startedAt)}${RST}`;

  // Keys
  const keyLines: string[] = [];
  for (const k of keys.slice(0, 8)) {
    const c = keyColor(k.label);
    const ratio = totalReqs > 0 ? k.stats.totalRequests / totalReqs : 0;
    const avail = k.availableAt <= now;
    const badge = !avail ? `${YELLOW}[RATE-LTD]${RST}` : `${GREEN}[OK]${RST}`;
    keyLines.push(
      `${pad(`  ${c}${BOLD}${k.label}${RST}`, 16)}  ${bar(ratio, 14, c)}  ${pad(`${WHITE}${String(k.stats.totalRequests).padStart(4)} req${RST}`, 12)}  ${DIM}${fmtNum(k.stats.totalTokensIn).padStart(6)}/${fmtNum(k.stats.totalTokensOut).padStart(6)} tok${RST}  ${badge}`
    );
  }
  if (keys.length === 0) keyLines.push(`  ${DIM}No keys configured${RST}`);

  // Log
  const fixedRows = 1 + 1 + Math.max(keyLines.length, 1) + 1 + 1 + 1 + 1;
  const logRows = Math.max(3, rows - fixedRows);
  const visible = entries.slice(-(logRows - 1));
  const logLines = [`  ${BOLD}RECENT ACTIVITY${RST}`];
  for (const e of visible) {
    const time = `${GRAY}${fmtTime(e.ts)}${RST}`;
    const lbl = e.label ? pad(`${keyColor(e.label)}${e.label}${RST}`, 10) : pad(`${DIM}system${RST}`, 10);
    const mc = msgColor(e);
    const msg = pad(`${mc}${trunc(e.msg, 28)}${RST}`, 30);
    logLines.push(`  ${time}  ${lbl}  ${msg}  ${logDetail(e)}`);
  }
  while (logLines.length < logRows) logLines.push("");

  // Footer
  const ctrl = `${DIM}  q${RST} quit  ${DIM}│${RST}  ${DIM}SPACE${RST} pause  ${DIM}│${RST}  ${DIM}c${RST} clear`;
  const pind = paused ? `${YELLOW}${BOLD} PAUSED ${RST}` : "";
  const ctrlLen = ctrl.replace(/\x1b\[[0-9;]*m/g, "").length;
  const pindLen = pind.replace(/\x1b\[[0-9;]*m/g, "").length;
  const footer = ctrl + " ".repeat(Math.max(0, cols - ctrlLen - pindLen)) + pind;

  process.stdout.write(
    CLEAR + "\x1b[1;1H" +
    [pad(hdr, cols), sep(cols), keyLines.join("\n"), sep(cols), logLines.join("\n"), sep(cols), footer].join("\n")
  );
}

// ── Main ─────────────────────────────────────────────────────────

export function startWatch(): void {
  const config = loadConfig();
  const statePath = join(config.dataDir, "state.json");

  const entries: LogEntry[] = [];
  const MAX_ENTRIES = 200;
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

  process.stdin.on("data", (key: string) => {
    if (key === "q" || key === "\x03") { cleanup(); process.exit(0); }
    if (key === "c") { entries.length = 0; doRender(); }
    if (key === " ") { paused = !paused; doRender(); }
  });

  process.stdout.on("resize", () => {
    cols = process.stdout.columns ?? 80;
    rows = process.stdout.rows ?? 24;
    doRender();
  });

  function doRender() {
    const state = readState(statePath);
    const keys = state?.keys ?? [];
    render(keys, entries, startedAt, paused, cols, rows);
  }

  // Load recent entries from latest log file
  const logFiles = findLogFiles();
  if (logFiles.length > 0) {
    const latest = logFiles[logFiles.length - 1]!;
    loadTail(latest, entries, MAX_ENTRIES);
  }
  doRender();

  // Tail log file + refresh stats
  let tailingFile: string | null = null;
  let tailFd: number | null = null;
  let tailOffset = 0;
  let lineBuf = "";
  const buf = Buffer.alloc(64 * 1024);

  setInterval(() => {
    // Discover new log files on restart
    const files = findLogFiles();
    const latest = files.length > 0 ? files[files.length - 1]! : null;
    if (latest && latest !== tailingFile) {
      tailingFile = latest;
      tailFd = openSync(latest, "r");
      tailOffset = fstatSync(tailFd).size;
      lineBuf = "";
    }

    // Read new log entries
    if (tailFd !== null) {
      const stat = fstatSync(tailFd);
      if (stat.size < tailOffset) tailOffset = 0;
      if (stat.size > tailOffset) {
        const n = readSync(tailFd, buf, 0, Math.min(stat.size - tailOffset, buf.length), tailOffset);
        tailOffset += n;
        lineBuf += buf.toString("utf-8", 0, n);
        const lines = lineBuf.split("\n");
        lineBuf = lines.pop() ?? "";
        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const e = JSON.parse(line) as LogEntry;
            if (typeof e.ts === "string" && typeof e.msg === "string") {
              entries.push(e);
              if (entries.length > MAX_ENTRIES) entries.shift();
            }
          } catch {}
        }
      }
    }

    if (!paused) doRender();
  }, 500);
}

function loadTail(logPath: string, entries: LogEntry[], max: number): void {
  const fd = openSync(logPath, "r");
  const size = fstatSync(fd).size;
  const readSize = Math.min(size, 64 * 1024);
  if (readSize === 0) return;
  const buf = Buffer.alloc(readSize);
  readSync(fd, buf, 0, readSize, size - readSize);
  for (const line of buf.toString("utf-8").split("\n")) {
    if (!line.trim()) continue;
    try {
      const e = JSON.parse(line) as LogEntry;
      if (typeof e.ts === "string" && typeof e.msg === "string") {
        entries.push(e);
        if (entries.length > max) entries.shift();
      }
    } catch {}
  }
}
