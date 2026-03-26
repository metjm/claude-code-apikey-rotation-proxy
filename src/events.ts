import type { MaskedKeyEntry } from "./types.ts";

export interface ProxyEvent {
  type: "request" | "response" | "tokens" | "rate_limit" | "error" | "keys";
  ts: string;
  label?: string;
  [key: string]: unknown;
}

type Listener = (event: ProxyEvent) => void;

const listeners = new Set<Listener>();

export function emit(event: ProxyEvent): void {
  for (const fn of listeners) {
    fn(event);
  }
}

export function subscribe(fn: Listener): () => void {
  listeners.add(fn);
  return () => { listeners.delete(fn); };
}

/** Convenience: emit + include a full keys snapshot for the watch TUI. */
export function emitWithKeys(event: ProxyEvent, keys: readonly MaskedKeyEntry[]): void {
  emit({ ...event, keys });
}
