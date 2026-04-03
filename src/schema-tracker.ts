import { Database } from "bun:sqlite";
import { log } from "./logger.ts";
import { WebhookNotifier } from "./webhook-notifier.ts";

const MAX_SAMPLE_VALUES = 50;
const MAX_STRING_LENGTH_FOR_SAMPLE = 200;
const MAX_FIELD_PATHS = 10_000;
/** After this many unique string samples, check if the field looks like free-text. */
const FREE_TEXT_CHECK_THRESHOLD = 10;
/** If every sample so far is unique and average length exceeds this, treat as free-text. */
const FREE_TEXT_AVG_LENGTH = 30;

/** Headers whose new values should NOT trigger webhook notifications (high-cardinality / noisy). */
const VALUE_NOTIFY_SUPPRESS_EXACT = new Set([
  "anthropic-organization-id",
  "cf-ray",
  "content-length",
  "date",
  "request-id",
  "retry-after",
  "server-timing",
  "set-cookie",
  "x-envoy-upstream-service-time",
]);
const VALUE_NOTIFY_SUPPRESS_SUFFIXES = [
  "-reset",
  "-utilization",
  "-surpassed-threshold",
  "-percentage",
];

function shouldSuppressValueNotify(headerName: string): boolean {
  if (VALUE_NOTIFY_SUPPRESS_EXACT.has(headerName)) return true;
  for (const suffix of VALUE_NOTIFY_SUPPRESS_SUFFIXES) {
    if (headerName.endsWith(suffix)) return true;
  }
  return false;
}

interface ObservedHeader {
  name: string;
  firstSeenAt: string;
  lastSeenAt: string;
  sampleValues: Set<string>;
  valueOverflow: boolean;
  hitCount: number;
}

interface ObservedField {
  endpoint: string;
  context: string;
  path: string;
  jsonTypes: Set<string>;
  firstSeenAt: string;
  lastSeenAt: string;
  sampleValues: Set<string>;
  valueOverflow: boolean;
  hitCount: number;
}

interface HeaderRow {
  name: string;
  first_seen_at: string;
  last_seen_at: string;
  sample_values: string;
  value_overflow: number;
  hit_count: number;
}

interface FieldRow {
  endpoint: string;
  context: string;
  path: string;
  json_types: string;
  first_seen_at: string;
  last_seen_at: string;
  sample_values: string;
  value_overflow: number;
  hit_count: number;
}

interface WebhookRow {
  url: string;
  label: string;
  created_at: string;
}

export type SchemaChange =
  | { readonly type: "new_header"; readonly name: string; readonly value: string }
  | { readonly type: "new_header_value"; readonly name: string; readonly value: string; readonly previousValues: readonly string[] }
  | { readonly type: "new_field"; readonly endpoint: string; readonly context: string; readonly path: string; readonly jsonType: string }
  | { readonly type: "new_field_type"; readonly endpoint: string; readonly context: string; readonly path: string; readonly newType: string; readonly previousTypes: readonly string[] }
  | { readonly type: "new_field_value"; readonly endpoint: string; readonly context: string; readonly path: string; readonly value: string };

export class SchemaTracker {
  private readonly db: Database;
  private readonly notifiers = new Map<string, WebhookNotifier>();
  private readonly headers = new Map<string, ObservedHeader>();
  private readonly fields = new Map<string, ObservedField>();
  private readonly dirtyHeaders = new Set<string>();
  private readonly dirtyFields = new Set<string>();
  private saveTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(dbPath: string, seedWebhookUrl?: string | null) {
    this.db = new Database(dbPath, { create: true });
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA busy_timeout = 5000");
    this.initSchema();
    this.loadFromDb();
    this.loadWebhooks(seedWebhookUrl ?? null);
  }

  private initSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS observed_headers (
        name            TEXT PRIMARY KEY,
        first_seen_at   TEXT NOT NULL,
        last_seen_at    TEXT NOT NULL,
        sample_values   TEXT NOT NULL DEFAULT '[]',
        value_overflow  INTEGER NOT NULL DEFAULT 0,
        hit_count       INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS observed_schema (
        endpoint        TEXT NOT NULL,
        context         TEXT NOT NULL,
        path            TEXT NOT NULL,
        json_types      TEXT NOT NULL DEFAULT '[]',
        first_seen_at   TEXT NOT NULL,
        last_seen_at    TEXT NOT NULL,
        sample_values   TEXT NOT NULL DEFAULT '[]',
        value_overflow  INTEGER NOT NULL DEFAULT 0,
        hit_count       INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (endpoint, context, path)
      );
      CREATE TABLE IF NOT EXISTS webhooks (
        url         TEXT PRIMARY KEY,
        label       TEXT NOT NULL DEFAULT '',
        created_at  TEXT NOT NULL
      );
    `);
  }

  private loadFromDb(): void {
    const headerRows = this.db.query("SELECT * FROM observed_headers").all() as HeaderRow[];
    for (const r of headerRows) {
      this.headers.set(r.name, {
        name: r.name,
        firstSeenAt: r.first_seen_at,
        lastSeenAt: r.last_seen_at,
        sampleValues: new Set(JSON.parse(r.sample_values) as string[]),
        valueOverflow: r.value_overflow === 1,
        hitCount: r.hit_count,
      });
    }

    const fieldRows = this.db.query("SELECT * FROM observed_schema").all() as FieldRow[];
    for (const r of fieldRows) {
      const mapKey = `${r.endpoint}|${r.context}|${r.path}`;
      this.fields.set(mapKey, {
        endpoint: r.endpoint,
        context: r.context,
        path: r.path,
        jsonTypes: new Set(JSON.parse(r.json_types) as string[]),
        firstSeenAt: r.first_seen_at,
        lastSeenAt: r.last_seen_at,
        sampleValues: new Set(JSON.parse(r.sample_values) as string[]),
        valueOverflow: r.value_overflow === 1,
        hitCount: r.hit_count,
      });
    }

    log("info", `Schema tracker loaded ${headerRows.length} header(s) and ${fieldRows.length} field(s)`);
  }

  private loadWebhooks(seedUrl: string | null): void {
    // Seed env-var webhook into DB if not already present
    if (seedUrl) {
      this.db.prepare(
        "INSERT OR IGNORE INTO webhooks (url, label, created_at) VALUES (?, '', ?)",
      ).run(seedUrl, new Date().toISOString());
    }
    const rows = this.db.query("SELECT * FROM webhooks").all() as WebhookRow[];
    for (const r of rows) {
      this.notifiers.set(r.url, new WebhookNotifier(r.url));
    }
    log("info", `Schema tracker loaded ${rows.length} webhook(s)`);
  }

  private enqueueToAll(changes: SchemaChange[]): void {
    // Only notify webhooks for header-related changes, excluding noisy value changes
    const headerChanges = changes.filter((c) => {
      if (c.type === "new_header") return true;
      if (c.type === "new_header_value") return !shouldSuppressValueNotify(c.name);
      return false;
    });
    if (headerChanges.length === 0) return;
    for (const notifier of this.notifiers.values()) {
      notifier.enqueue(headerChanges);
    }
  }

  async flushAllWebhooks(): Promise<void> {
    await Promise.all([...this.notifiers.values()].map((n) => n.flush()));
  }

  recordHeaders(headers: Headers): SchemaChange[] {
    const changes: SchemaChange[] = [];
    const ts = new Date().toISOString();

    for (const [name, value] of headers.entries()) {
      const existing = this.headers.get(name);
      if (!existing) {
        this.headers.set(name, {
          name, firstSeenAt: ts, lastSeenAt: ts,
          sampleValues: new Set([value]),
          valueOverflow: false, hitCount: 1,
        });
        changes.push({ type: "new_header", name, value });
      } else {
        existing.lastSeenAt = ts;
        existing.hitCount++;
        if (!existing.valueOverflow && !existing.sampleValues.has(value)) {
          if (existing.sampleValues.size < MAX_SAMPLE_VALUES) {
            const previousValues = [...existing.sampleValues];
            existing.sampleValues.add(value);
            changes.push({ type: "new_header_value", name, value, previousValues });
          } else {
            existing.valueOverflow = true;
          }
        }
      }
      this.dirtyHeaders.add(name);
    }

    this.scheduleSave();
    if (changes.length > 0) this.enqueueToAll(changes);
    return changes;
  }

  recordResponseJson(endpoint: string, text: string): SchemaChange[] {
    try {
      const parsed: unknown = JSON.parse(text);
      return this.recordBody(endpoint, "response", parsed);
    } catch { return []; }
  }

  recordStreamEvent(endpoint: string, eventType: string, event: unknown): SchemaChange[] {
    return this.recordBody(endpoint, eventType, event);
  }

  private recordBody(endpoint: string, context: string, body: unknown): SchemaChange[] {
    const changes: SchemaChange[] = [];
    const ts = new Date().toISOString();

    walkJson(body, "", (path, value, jsonType) => {
      if (!path) return; // skip root

      const mapKey = `${endpoint}|${context}|${path}`;
      const existing = this.fields.get(mapKey);

      if (!existing) {
        // Enforce field-path cap to prevent unbounded growth from dynamic keys
        if (this.fields.size >= MAX_FIELD_PATHS) {
          log("warn", "Schema tracker field-path cap reached", { cap: MAX_FIELD_PATHS });
          return;
        }

        const valueOverflow = !shouldSampleValue(value, jsonType);
        const sampleValues = new Set<string>();
        if (!valueOverflow && value !== undefined) sampleValues.add(String(value));

        this.fields.set(mapKey, {
          endpoint, context, path,
          jsonTypes: new Set([jsonType]),
          firstSeenAt: ts, lastSeenAt: ts,
          sampleValues, valueOverflow, hitCount: 1,
        });
        this.dirtyFields.add(mapKey);
        changes.push({ type: "new_field", endpoint, context, path, jsonType });
      } else {
        existing.lastSeenAt = ts;
        existing.hitCount++;
        this.dirtyFields.add(mapKey);

        if (!existing.jsonTypes.has(jsonType)) {
          const previousTypes = [...existing.jsonTypes];
          existing.jsonTypes.add(jsonType);
          changes.push({ type: "new_field_type", endpoint, context, path, newType: jsonType, previousTypes });
        }

        if (value !== undefined && !existing.valueOverflow && shouldSampleValue(value, jsonType)) {
          const strVal = String(value);
          if (!existing.sampleValues.has(strVal)) {
            if (existing.sampleValues.size < MAX_SAMPLE_VALUES) {
              existing.sampleValues.add(strVal);
              changes.push({ type: "new_field_value", endpoint, context, path, value: strVal });
              // Stop sampling early if this looks like free-text data
              if (looksLikeFreeText(existing)) {
                existing.valueOverflow = true;
              }
            } else {
              existing.valueOverflow = true;
            }
          }
        }
      }
    });

    if (this.dirtyFields.size > 0) this.scheduleSave();
    if (changes.length > 0) this.enqueueToAll(changes);
    return changes;
  }

  listHeaders(): { name: string; firstSeenAt: string; lastSeenAt: string; sampleValues: string[]; valueOverflow: boolean; hitCount: number }[] {
    return [...this.headers.values()]
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((h) => ({
        name: h.name, firstSeenAt: h.firstSeenAt, lastSeenAt: h.lastSeenAt,
        sampleValues: [...h.sampleValues], valueOverflow: h.valueOverflow, hitCount: h.hitCount,
      }));
  }

  listFields(): { endpoint: string; context: string; path: string; jsonTypes: string[]; firstSeenAt: string; lastSeenAt: string; sampleValues: string[]; valueOverflow: boolean; hitCount: number }[] {
    return [...this.fields.values()]
      .sort((a, b) => a.endpoint.localeCompare(b.endpoint) || a.context.localeCompare(b.context) || a.path.localeCompare(b.path))
      .map((f) => ({
        endpoint: f.endpoint, context: f.context, path: f.path, jsonTypes: [...f.jsonTypes],
        firstSeenAt: f.firstSeenAt, lastSeenAt: f.lastSeenAt, sampleValues: [...f.sampleValues],
        valueOverflow: f.valueOverflow, hitCount: f.hitCount,
      }));
  }

  listWebhooks(): { url: string; label: string; createdAt: string }[] {
    return (this.db.query("SELECT * FROM webhooks ORDER BY created_at").all() as WebhookRow[])
      .map((r) => ({ url: r.url, label: r.label, createdAt: r.created_at }));
  }

  addWebhook(url: string, label?: string): void {
    const ts = new Date().toISOString();
    this.db.prepare("INSERT INTO webhooks (url, label, created_at) VALUES (?, ?, ?)").run(url, label ?? "", ts);
    this.notifiers.set(url, new WebhookNotifier(url));
    log("info", "Webhook added", { url, label });
  }

  removeWebhook(url: string): void {
    const notifier = this.notifiers.get(url);
    if (notifier) {
      notifier.flush().catch(() => {});
      this.notifiers.delete(url);
    }
    this.db.prepare("DELETE FROM webhooks WHERE url = ?").run(url);
    log("info", "Webhook removed", { url });
  }

  sendTestNotification(url?: string): boolean {
    const testPayload: SchemaChange[] = [{
      type: "new_header",
      name: "x-test-notification",
      value: "This is a test notification from the API schema tracker",
    }];
    if (url) {
      const notifier = this.notifiers.get(url);
      if (!notifier) return false;
      notifier.enqueue(testPayload);
      return true;
    }
    if (this.notifiers.size === 0) return false;
    this.enqueueToAll(testPayload);
    return true;
  }

  private scheduleSave(): void {
    if (this.saveTimer) return;
    this.saveTimer = setTimeout(() => {
      this.saveTimer = null;
      this.saveNow();
    }, 1_000);
  }

  private saveNow(): void {
    if (this.dirtyHeaders.size === 0 && this.dirtyFields.size === 0) return;

    const upsertHeader = this.db.prepare(`
      INSERT INTO observed_headers (name, first_seen_at, last_seen_at, sample_values, value_overflow, hit_count)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(name) DO UPDATE SET
        last_seen_at = excluded.last_seen_at,
        sample_values = excluded.sample_values,
        value_overflow = excluded.value_overflow,
        hit_count = excluded.hit_count
    `);
    const upsertField = this.db.prepare(`
      INSERT INTO observed_schema (endpoint, context, path, json_types, first_seen_at, last_seen_at, sample_values, value_overflow, hit_count)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(endpoint, context, path) DO UPDATE SET
        json_types = excluded.json_types,
        last_seen_at = excluded.last_seen_at,
        sample_values = excluded.sample_values,
        value_overflow = excluded.value_overflow,
        hit_count = excluded.hit_count
    `);

    this.db.transaction(() => {
      for (const name of this.dirtyHeaders) {
        const h = this.headers.get(name);
        if (h) upsertHeader.run(h.name, h.firstSeenAt, h.lastSeenAt, JSON.stringify([...h.sampleValues]), h.valueOverflow ? 1 : 0, h.hitCount);
      }
      for (const key of this.dirtyFields) {
        const f = this.fields.get(key);
        if (f) upsertField.run(f.endpoint, f.context, f.path, JSON.stringify([...f.jsonTypes]), f.firstSeenAt, f.lastSeenAt, JSON.stringify([...f.sampleValues]), f.valueOverflow ? 1 : 0, f.hitCount);
      }
      this.dirtyHeaders.clear();
      this.dirtyFields.clear();
    })();
  }

  close(): void {
    if (this.saveTimer) {
      clearTimeout(this.saveTimer);
      this.saveTimer = null;
    }
    this.saveNow();
    this.notifiers.clear();
    this.db.close();
  }
}

function walkJson(
  obj: unknown,
  path: string,
  callback: (path: string, value: unknown, jsonType: string) => void,
): void {
  if (obj === null) { callback(path, null, "null"); return; }
  if (obj === undefined) return;

  if (Array.isArray(obj)) {
    callback(path, undefined, "array");
    for (const item of obj) walkJson(item, `${path}[]`, callback);
    return;
  }

  const t = typeof obj;
  if (t === "object") {
    callback(path, undefined, "object");
    for (const [key, val] of Object.entries(obj as Record<string, unknown>)) {
      walkJson(val, path ? `${path}.${key}` : key, callback);
    }
    return;
  }

  callback(path, obj, t);
}

function shouldSampleValue(value: unknown, jsonType: string): boolean {
  if (jsonType === "object" || jsonType === "array") return false;
  if (jsonType === "string" && typeof value === "string" && value.length > MAX_STRING_LENGTH_FOR_SAMPLE) return false;
  return true;
}

/**
 * Returns true if a field's collected samples look like free-text/high-cardinality
 * strings rather than a small enum. Checked after adding a new sample.
 *
 * Heuristic: if we've seen >= FREE_TEXT_CHECK_THRESHOLD unique values,
 * every value seen so far is unique (no repeats — uniqueRatio = samples/hits is high),
 * and the average sample length exceeds FREE_TEXT_AVG_LENGTH, it's likely free-text.
 */
function looksLikeFreeText(field: ObservedField): boolean {
  if (field.sampleValues.size < FREE_TEXT_CHECK_THRESHOLD) return false;
  // If we've seen many hits but still no duplicates, cardinality is high
  const uniqueRatio = field.sampleValues.size / field.hitCount;
  if (uniqueRatio < 0.8) return false; // Some repeats → probably enum-like
  let totalLen = 0;
  for (const v of field.sampleValues) totalLen += v.length;
  return (totalLen / field.sampleValues.size) > FREE_TEXT_AVG_LENGTH;
}
