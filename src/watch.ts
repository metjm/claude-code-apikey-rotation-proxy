import { openSync, readSync, fstatSync, existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { loadConfig } from "./config.ts";
import { KeyManager } from "./key-manager.ts";
import { setLogLevel } from "./logger.ts";
import { logDir } from "./service.ts";

setLogLevel("error");

// ── ANSI helpers ─────────────────────────────────────────────────

const ALT_ON = "\x1b[?1049h";
const ALT_OFF = "\x1b[?1049l";
const HIDE_CUR = "\x1b[?25l";
const SHOW_CUR = "\x1b[?25h";
const CLEAR = "\x1b[2J\x1b[H";
const BOLD = "\x1b[1m";
const DIM = "\x1b[2m";
const RST = "\x1b[0m";

function fg(n: number): string {
  return `\x1b[38;5;${n}m`;
}

function moveTo(r: number, c: number): string {
  return `\x1b[${r};${c}H`;
}

// ── Palette ──────────────────────────────────────────────────────

const KEY_COLORS = [14, 13, 11, 10, 12, 9, 208, 177] as const;
const GREEN = fg(10);
const YELLOW = fg(11);
const RED = fg(9);
const GRAY = fg(240);
const WHITE = fg(15);
const CYAN = fg(14);

// ── Types ────────────────────────────────────────────────────────

interface LogEntry {
  ts: string;
  level: string;
  msg: string;
  label?: string;
  [key: string]: unknown;
}

interface KeyDisplay {
  label: string;
  color: string;
  lastStatus: "ok" | "rate-limited" | "error";
}

interface State {
  entries: LogEntry[];
  keyColors: Map<string, string>;
  keyStatuses: Map<string, "ok" | "rate-limited" | "error">;
  colorIdx: number;
  startedAt: number;
  paused: boolean;
  cols: number;
  rows: number;
}

// ── Formatting ───────────────────────────────────────────────────

function fmtTime(iso: string): string {
  const d = new Date(iso);
  const h = String(d.getHours()).padStart(2, "0");
  const m = String(d.getMinutes()).padStart(2, "0");
  const s = String(d.getSeconds()).padStart(2, "0");
  return `${h}:${m}:${s}`;
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
  const empty = width - filled;
  return color + "\u2588".repeat(filled) + GRAY + "\u2591".repeat(empty) + RST;
}

function pad(s: string, n: number): string {
  const visible = s.replace(/\x1b\[[0-9;]*m/g, "");
  return visible.length >= n ? s : s + " ".repeat(n - visible.length);
}

function truncate(s: string, n: number): string {
  return s.length > n ? s.slice(0, n - 1) + "\u2026" : s;
}

// ── State management ─────────────────────────────────────────────

const MAX_ENTRIES = 200;

function getKeyColor(state: State, label: string): string {
  let color = state.keyColors.get(label);
  if (!color) {
    const cidx = state.colorIdx % KEY_COLORS.length;
    color = fg(KEY_COLORS[cidx]!);
    state.keyColors.set(label, color);
    state.colorIdx++;
  }
  return color;
}

function processEntry(state: State, entry: LogEntry): void {
  state.entries.push(entry);
  if (state.entries.length > MAX_ENTRIES) {
    state.entries.shift();
  }

  if (!entry.label) return;

  // Track live status from log events
  switch (entry.msg) {
    case "Rate limited, trying next key":
    case "Key rate-limited":
      state.keyStatuses.set(entry.label, "rate-limited");
      break;
    case "Upstream error":
    case "Upstream fetch failed":
      state.keyStatuses.set(entry.label, "error");
      break;
    case "Upstream responded": {
      const s = entry["status"] as number;
      if (s >= 200 && s < 400) {
        state.keyStatuses.set(entry.label, "ok");
      }
      break;
    }
  }
}

// ── Rendering ────────────────────────────────────────────────────

function renderHeader(keyManager: KeyManager, state: State, cols: number): string {
  const total = keyManager.totalCount();
  const avail = keyManager.availableCount();
  const keys = keyManager.listKeys();
  const totalReqs = keys.reduce((s, k) => s + k.stats.totalRequests, 0);
  const uptime = fmtUptime(Date.now() - state.startedAt);

  const title = `${BOLD}${CYAN} CLAUDE PROXY ${RST}`;
  const stats = `${DIM}│${RST} ${WHITE}${total}${RST} keys  ${DIM}│${RST} ${GREEN}${avail}${RST} avail  ${DIM}│${RST} ${WHITE}${totalReqs}${RST} reqs  ${DIM}│${RST} ${DIM}${uptime}${RST}`;
  return pad(title + " " + stats, cols);
}

function renderSeparator(cols: number): string {
  return DIM + "\u2500".repeat(cols) + RST;
}

function renderKeys(keyManager: KeyManager, state: State, _cols: number): string {
  const keys = keyManager.listKeys();
  if (keys.length === 0) {
    return `  ${DIM}No keys configured${RST}`;
  }

  const totalReqs = keys.reduce((s, k) => s + k.stats.totalRequests, 0);
  const barWidth = 14;
  const lines: string[] = [];

  for (const k of keys.slice(0, 8)) {
    const color = getKeyColor(state, k.label);
    const ratio = totalReqs > 0 ? k.stats.totalRequests / totalReqs : 0;
    const label = pad(`  ${color}${BOLD}${k.label}${RST}`, 16);
    const pbar = bar(ratio, barWidth, color);
    const reqs = pad(`${WHITE}${String(k.stats.totalRequests).padStart(4)} req${RST}`, 12);
    const tok = `${DIM}${fmtNum(k.stats.totalTokensIn).padStart(6)}/${fmtNum(k.stats.totalTokensOut).padStart(6)} tok${RST}`;

    const liveStatus = state.keyStatuses.get(k.label);
    const status = !k.isAvailable ? "rate-limited" : liveStatus ?? "ok";
    let badge: string;
    if (status === "rate-limited") {
      badge = `${YELLOW}[RATE-LTD]${RST}`;
    } else if (status === "error") {
      badge = `${RED}[ERROR]${RST}`;
    } else {
      badge = `${GREEN}[OK]${RST}`;
    }

    lines.push(`${label}  ${pbar}  ${reqs}  ${tok}  ${badge}`);
  }

  if (keys.length > 8) {
    lines.push(`  ${DIM}+${keys.length - 8} more...${RST}`);
  }

  return lines.join("\n");
}

function formatLogDetail(entry: LogEntry, maxWidth: number): string {
  switch (entry.msg) {
    case "Proxying request":
      return `${DIM}${entry["method"]} ${truncate(entry["path"] as string, 20)}  #${entry["attempt"]}${RST}`;
    case "Upstream responded": {
      const s = entry["status"] as number;
      const color = s >= 200 && s < 400 ? GREEN : s === 429 ? YELLOW : RED;
      return `${color}${s}${RST}`;
    }
    case "Token usage":
    case "Token usage (stream)":
      return `${GREEN}${fmtNum(entry["input"] as number)} in / ${fmtNum(entry["output"] as number)} out${RST}`;
    case "Rate limited, trying next key":
      return `${YELLOW}retry ${entry["retryAfter"]}s, ${entry["availableKeys"]} keys left${RST}`;
    case "Key rate-limited":
      return `${YELLOW}${BOLD}retry ${entry["retryAfterSecs"]}s${RST}`;
    case "Upstream error":
      return `${RED}${entry["status"]}${RST}`;
    case "Upstream fetch failed":
      return `${RED}${truncate(String(entry["error"] ?? ""), maxWidth)}${RST}`;
    default:
      return DIM + truncate(entry.msg, maxWidth) + RST;
  }
}

function msgColor(entry: LogEntry): string {
  switch (entry.msg) {
    case "Token usage":
    case "Token usage (stream)":
      return GREEN;
    case "Rate limited, trying next key":
    case "Key rate-limited":
      return YELLOW;
    case "Upstream error":
    case "Upstream fetch failed":
      return RED;
    case "Upstream responded": {
      const s = entry["status"] as number;
      return s >= 200 && s < 400 ? GREEN : s === 429 ? YELLOW : RED;
    }
    default:
      return "";
  }
}

function renderLog(state: State, cols: number, maxRows: number): string {
  const header = `  ${BOLD}RECENT ACTIVITY${RST}`;
  const lines: string[] = [header];

  const visible = state.entries.slice(-(maxRows - 1));

  for (const e of visible) {
    const time = `${GRAY}${fmtTime(e.ts)}${RST}`;
    const color = e.label ? getKeyColor(state, e.label) : GRAY;
    const labelStr = e.label
      ? pad(`${color}${e.label}${RST}`, 10)
      : pad(`${DIM}system${RST}`, 10);

    const mc = msgColor(e);
    const msgStr = pad(`${mc}${truncate(e.msg, 28)}${RST}`, 30);

    const detailWidth = Math.max(10, cols - 58);
    const detail = formatLogDetail(e, detailWidth);

    lines.push(`  ${time}  ${labelStr}  ${msgStr}  ${detail}`);
  }

  while (lines.length < maxRows) {
    lines.push("");
  }

  return lines.join("\n");
}

function renderFooter(state: State, cols: number): string {
  const controls = `${DIM}  q${RST} quit  ${DIM}│${RST}  ${DIM}SPACE${RST} pause  ${DIM}│${RST}  ${DIM}c${RST} clear`;
  const pauseIndicator = state.paused ? `${YELLOW}${BOLD} PAUSED ${RST}` : "";
  const left = controls;
  const leftLen = left.replace(/\x1b\[[0-9;]*m/g, "").length;
  const pauseLen = pauseIndicator.replace(/\x1b\[[0-9;]*m/g, "").length;
  const gap = Math.max(0, cols - leftLen - pauseLen);
  return left + " ".repeat(gap) + pauseIndicator;
}

function render(keyManager: KeyManager, state: State): void {
  const { cols, rows } = state;

  const keyCount = Math.min(keyManager.totalCount(), 8) + (keyManager.totalCount() > 8 ? 1 : 0);
  const keySectionRows = Math.max(keyCount, 1);

  const fixedRows = 1 + 1 + keySectionRows + 1 + 1 + 1;
  const logRows = Math.max(3, rows - fixedRows);

  const parts: string[] = [
    moveTo(1, 1),
    renderHeader(keyManager, state, cols),
    "\n",
    renderSeparator(cols),
    "\n",
    renderKeys(keyManager, state, cols),
    "\n",
    renderSeparator(cols),
    "\n",
    renderLog(state, cols, logRows),
    "\n",
    renderSeparator(cols),
    "\n",
    renderFooter(state, cols),
  ];

  process.stdout.write(CLEAR + parts.join(""));
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

// ── Main ─────────────────────────────────────────────────────────

export function startWatch(): void {
  const config = loadConfig();
  const keyManager = new KeyManager(config.dataDir);

  const state: State = {
    entries: [],
    keyColors: new Map(),
    keyStatuses: new Map(),
    colorIdx: 0,
    startedAt: Date.now(),
    paused: false,
    cols: process.stdout.columns ?? 80,
    rows: process.stdout.rows ?? 24,
  };

  // ── Terminal setup ───────────────────────────────────────────
  let cleaned = false;
  function cleanup(): void {
    if (cleaned) return;
    cleaned = true;
    process.stdout.write(SHOW_CUR + ALT_OFF);
    try {
      if (typeof process.stdin.setRawMode === "function") {
        process.stdin.setRawMode(false);
      }
    } catch {}
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

  // ── Keyboard ─────────────────────────────────────────────────
  process.stdin.on("data", (key: string) => {
    if (key === "q" || key === "\x03") {
      cleanup();
      process.exit(0);
    }
    if (key === "c") {
      state.entries.length = 0;
      render(keyManager, state);
    }
    if (key === " ") {
      state.paused = !state.paused;
      render(keyManager, state);
    }
  });

  // ── Resize ───────────────────────────────────────────────────
  process.stdout.on("resize", () => {
    state.cols = process.stdout.columns ?? 80;
    state.rows = process.stdout.rows ?? 24;
    render(keyManager, state);
  });

  // ── Initial render with state.json stats ─────────────────────
  render(keyManager, state);

  // ── Tail log files ───────────────────────────────────────────
  const logFiles = findLogFiles();

  // Load recent entries from the latest log file for the activity feed
  if (logFiles.length > 0) {
    const latest = logFiles[logFiles.length - 1]!;
    loadRecentEntries(latest, state);
    render(keyManager, state);
  }

  // Start tailing the latest log file (or wait for one)
  let tailingFile: string | null = null;
  let tailFd: number | null = null;
  let tailOffset = 0;
  let lineBuf = "";
  const buf = Buffer.alloc(64 * 1024);

  setInterval(() => {
    // Check for new log files
    const currentFiles = findLogFiles();
    const currentLatest = currentFiles.length > 0 ? currentFiles[currentFiles.length - 1]! : null;

    if (currentLatest && currentLatest !== tailingFile) {
      // New log file appeared (server restarted)
      tailingFile = currentLatest;
      tailFd = openSync(currentLatest, "r");
      tailOffset = fstatSync(tailFd).size;
      lineBuf = "";
    }

    if (tailFd === null) return;

    const stat = fstatSync(tailFd);
    if (stat.size < tailOffset) tailOffset = 0;
    if (stat.size <= tailOffset) {
      // No new log data, but still re-render to update stats from state.json
      if (!state.paused) render(keyManager, state);
      return;
    }

    const bytesToRead = Math.min(stat.size - tailOffset, buf.length);
    const n = readSync(tailFd, buf, 0, bytesToRead, tailOffset);
    tailOffset += n;

    lineBuf += buf.toString("utf-8", 0, n);
    const lines = lineBuf.split("\n");
    lineBuf = lines.pop() ?? "";

    let changed = false;
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const entry = JSON.parse(line) as LogEntry;
        if (typeof entry.ts === "string" && typeof entry.msg === "string") {
          processEntry(state, entry);
          changed = true;
        }
      } catch {}
    }

    if (!state.paused) {
      // Always re-render: stats come from keyManager (state.json), activity from log
      render(keyManager, state);
    }
  }, 500);
}

function loadRecentEntries(logPath: string, state: State): void {
  const fd = openSync(logPath, "r");
  const size = fstatSync(fd).size;
  const readSize = Math.min(size, 64 * 1024);
  if (readSize === 0) return;

  const buf = Buffer.alloc(readSize);
  readSync(fd, buf, 0, readSize, size - readSize);
  const chunk = buf.toString("utf-8");
  const lines = chunk.split("\n");

  for (const line of lines) {
    if (!line.trim()) continue;
    try {
      const entry = JSON.parse(line) as LogEntry;
      if (typeof entry.ts === "string" && typeof entry.msg === "string") {
        processEntry(state, entry);
      }
    } catch {}
  }
}
