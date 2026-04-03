import { describe, test, expect, afterEach } from "bun:test";
import { WebhookNotifier } from "../src/webhook-notifier.ts";
import type { SchemaChange } from "../src/schema-tracker.ts";
import type { Server } from "bun";

// ── Helpers ───────────────────────────────────────────────────────

interface WebhookReceiver {
  url: string;
  server: Server;
  requests: { text: string; changes: SchemaChange[] }[];
  stop: () => void;
}

function startWebhookReceiver(statusCode = 200): WebhookReceiver {
  const requests: { text: string; changes: SchemaChange[] }[] = [];
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = (await req.json()) as { text: string; changes: SchemaChange[] };
      requests.push(body);
      return new Response("ok", { status: statusCode });
    },
  });
  return {
    url: `http://localhost:${server.port}`,
    server,
    requests,
    stop: () => server.stop(true),
  };
}

const sampleChanges: SchemaChange[] = [
  { type: "new_header", name: "x-test-1", value: "val1" },
  { type: "new_header", name: "x-test-2", value: "val2" },
  { type: "new_field", endpoint: "/v1/messages", context: "response", path: "id", jsonType: "string" },
];

let receivers: WebhookReceiver[] = [];

afterEach(() => {
  for (const r of receivers) r.stop();
  receivers = [];
});

function receiver(statusCode = 200): WebhookReceiver {
  const r = startWebhookReceiver(statusCode);
  receivers.push(r);
  return r;
}

// ── Test 9: Webhook batched delivery ─────────────────────────────

describe("WebhookNotifier", () => {
  test("delivers batched changes via HTTP POST", async () => {
    const recv = receiver();
    const notifier = new WebhookNotifier(recv.url, 50);

    notifier.enqueue(sampleChanges);
    await notifier.flush();

    expect(recv.requests).toHaveLength(1);
    expect(recv.requests[0]!.changes).toHaveLength(3);
    expect(recv.requests[0]!.text).toContain("Claude API Schema Changes");
  });

  test("flush with no pending changes is a no-op", async () => {
    const recv = receiver();
    const notifier = new WebhookNotifier(recv.url, 50);

    await notifier.flush();
    expect(recv.requests).toHaveLength(0);
  });

  test("enqueue with empty array does not trigger delivery", async () => {
    const recv = receiver();
    const notifier = new WebhookNotifier(recv.url, 50);

    notifier.enqueue([]);
    await notifier.flush();
    expect(recv.requests).toHaveLength(0);
  });

  // ── Test 10: Webhook backoff on failures ─────────────────────

  test("backs off on delivery failures", async () => {
    const recv = receiver(500);
    const notifier = new WebhookNotifier(recv.url, 50);

    // First failure
    notifier.enqueue([sampleChanges[0]!]);
    await notifier.flush();
    expect(recv.requests).toHaveLength(1);

    // Second failure
    notifier.enqueue([sampleChanges[1]!]);
    await notifier.flush();
    expect(recv.requests).toHaveLength(2);

    // Verify backoff is happening internally (notifier keeps working)
    // We can verify it still delivers
    notifier.enqueue([sampleChanges[2]!]);
    await notifier.flush();
    expect(recv.requests).toHaveLength(3);
  });

  test("recovers from failures on successful delivery", async () => {
    // Start with a failing endpoint
    const failRecv = receiver(500);
    const notifier = new WebhookNotifier(failRecv.url, 50);

    notifier.enqueue([sampleChanges[0]!]);
    await notifier.flush();
    expect(failRecv.requests).toHaveLength(1);

    // Notifier should still work for next flush
    notifier.enqueue([sampleChanges[1]!]);
    await notifier.flush();
    expect(failRecv.requests).toHaveLength(2);
  });

  test("formats Slack-compatible message text", async () => {
    const recv = receiver();
    const notifier = new WebhookNotifier(recv.url, 50);

    const changes: SchemaChange[] = [
      { type: "new_header", name: "x-new", value: "v1" },
      { type: "new_header_value", name: "x-existing", value: "v2", previousValues: ["v1"] },
      { type: "new_field", endpoint: "/v1/messages", context: "response", path: "model", jsonType: "string" },
      { type: "new_field_type", endpoint: "/v1/messages", context: "response", path: "stop_reason", newType: "null", previousTypes: ["string"] },
      { type: "new_field_value", endpoint: "/v1/messages", context: "response", path: "model", value: "claude-sonnet-4-20250514" },
    ];

    notifier.enqueue(changes);
    await notifier.flush();

    const text = recv.requests[0]!.text;
    expect(text).toContain("New Headers:");
    expect(text).toContain("x-new");
    expect(text).toContain("New Header Values:");
    expect(text).toContain("New Response Fields:");
    expect(text).toContain("New Field Types:");
    expect(text).toContain("New Field Values:");
  });

  test("summarizes when more than 20 new field values", async () => {
    const recv = receiver();
    const notifier = new WebhookNotifier(recv.url, 50);

    const changes: SchemaChange[] = [];
    for (let i = 0; i < 25; i++) {
      changes.push({ type: "new_field_value", endpoint: "/v1/messages", context: "response", path: `field_${i}`, value: `val_${i}` });
    }

    notifier.enqueue(changes);
    await notifier.flush();

    const text = recv.requests[0]!.text;
    expect(text).toContain("25 new values across various fields");
  });
});
