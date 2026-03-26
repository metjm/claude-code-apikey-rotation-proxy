import type { LogLevel } from "./types.ts";

const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 } as const satisfies Record<LogLevel, number>;

let minLevel: LogLevel = (process.env["LOG_LEVEL"] as LogLevel | undefined) ?? "info";

export function setLogLevel(level: LogLevel): void {
  minLevel = level;
}

export function log(level: LogLevel, msg: string, extra?: Record<string, unknown>): void {
  if (LEVELS[level] < LEVELS[minLevel]) return;

  const entry = {
    ts: new Date().toISOString(),
    level,
    msg,
    ...extra,
  };
  const line = JSON.stringify(entry);

  if (level === "error") {
    console.error(line);
  } else if (level === "warn") {
    console.warn(line);
  } else {
    console.log(line);
  }
}
