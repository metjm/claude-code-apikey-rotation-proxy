import { log } from "./logger.ts";
import type { SchemaChange } from "./schema-tracker.ts";

const DEFAULT_BATCH_WINDOW_MS = 5_000;
const MAX_BACKOFF_MS = 5 * 60 * 1_000; // 5 minutes

export class WebhookNotifier {
  private pending: SchemaChange[] = [];
  private timer: ReturnType<typeof setTimeout> | null = null;
  private consecutiveFailures = 0;
  private currentBatchWindowMs: number;

  constructor(
    private readonly webhookUrl: string,
    private readonly baseBatchWindowMs: number = DEFAULT_BATCH_WINDOW_MS,
  ) {
    this.currentBatchWindowMs = baseBatchWindowMs;
  }

  enqueue(changes: SchemaChange[]): void {
    if (changes.length === 0) return;
    this.pending.push(...changes);
    if (!this.timer) {
      this.timer = setTimeout(() => {
        this.timer = null;
        this.flush();
      }, this.currentBatchWindowMs);
    }
  }

  async flush(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    const changes = this.pending;
    this.pending = [];
    if (changes.length === 0) return;

    const text = formatChangesForSlack(changes);
    try {
      const resp = await fetch(this.webhookUrl, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ text, changes }),
      });
      if (!resp.ok) {
        this.handleFailure(`HTTP ${resp.status}`);
      } else {
        this.handleSuccess();
      }
    } catch (err) {
      this.handleFailure(String(err));
    }
  }

  private handleSuccess(): void {
    if (this.consecutiveFailures > 0) {
      log("info", "Webhook delivery recovered", { previousFailures: this.consecutiveFailures });
    }
    this.consecutiveFailures = 0;
    this.currentBatchWindowMs = this.baseBatchWindowMs;
  }

  private handleFailure(reason: string): void {
    this.consecutiveFailures++;
    this.currentBatchWindowMs = Math.min(
      this.currentBatchWindowMs * 2,
      MAX_BACKOFF_MS,
    );
    log("warn", "Webhook delivery failed", {
      reason,
      consecutiveFailures: this.consecutiveFailures,
      nextBatchWindowMs: this.currentBatchWindowMs,
    });
  }
}

function formatChangesForSlack(changes: SchemaChange[]): string {
  const lines: string[] = ["*Claude API Schema Changes Detected*\n"];

  const newHeaders = changes.filter((c): c is SchemaChange & { type: "new_header" } => c.type === "new_header");
  const newHeaderValues = changes.filter((c): c is SchemaChange & { type: "new_header_value" } => c.type === "new_header_value");
  const newFields = changes.filter((c): c is SchemaChange & { type: "new_field" } => c.type === "new_field");
  const newTypes = changes.filter((c): c is SchemaChange & { type: "new_field_type" } => c.type === "new_field_type");
  const newValues = changes.filter((c): c is SchemaChange & { type: "new_field_value" } => c.type === "new_field_value");

  if (newHeaders.length > 0) {
    lines.push("*New Headers:*");
    for (const c of newHeaders) lines.push(`  • \`${c.name}\` — value: \`${c.value}\``);
  }
  if (newHeaderValues.length > 0) {
    lines.push("*New Header Values:*");
    for (const c of newHeaderValues) lines.push(`  • \`${c.name}\` — new: \`${c.value}\` (known: ${c.previousValues.map((v) => `\`${v}\``).join(", ")})`);
  }
  if (newFields.length > 0) {
    lines.push("*New Response Fields:*");
    for (const c of newFields) lines.push(`  • \`${c.endpoint}\` › \`${c.path}\` — type: \`${c.jsonType}\` (ctx: ${c.context})`);
  }
  if (newTypes.length > 0) {
    lines.push("*New Field Types:*");
    for (const c of newTypes) lines.push(`  • \`${c.endpoint}\` › \`${c.path}\` — new type: \`${c.newType}\` (was: ${c.previousTypes.map((t) => `\`${t}\``).join(", ")})`);
  }
  if (newValues.length > 0 && newValues.length <= 20) {
    lines.push("*New Field Values:*");
    for (const c of newValues) lines.push(`  • \`${c.endpoint}\` › \`${c.path}\` — new value: \`${c.value}\``);
  } else if (newValues.length > 20) {
    lines.push(`*New Field Values:* ${newValues.length} new values across various fields`);
  }

  return lines.join("\n");
}
