import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SchemaTracker } from "../src/schema-tracker.ts";

// ── Helpers ───────────────────────────────────────────────────────

let tempDir: string;

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), "schema-test-"));
});

afterEach(() => {
  try {
    rmSync(tempDir, { recursive: true, force: true });
  } catch {}
});

function createTracker(seedWebhookUrl?: string | null): SchemaTracker {
  const dbPath = join(tempDir, "test-state.db");
  return new SchemaTracker(dbPath, seedWebhookUrl ?? undefined);
}

// ── Test 1: New header detection ─────────────────────────────────

describe("Header Tracking", () => {
  test("detects new headers and returns changes on first call only", () => {
    const st = createTracker();

    const headers = new Headers({
      "content-type": "application/json",
      "x-request-id": "abc",
    });

    const changes1 = st.recordHeaders(headers);
    expect(changes1).toHaveLength(2);
    expect(changes1.every((c) => c.type === "new_header")).toBe(true);
    expect(changes1.map((c) => c.type === "new_header" && c.name).sort()).toEqual([
      "content-type",
      "x-request-id",
    ]);

    const changes2 = st.recordHeaders(headers);
    expect(changes2).toHaveLength(0);

    const listed = st.listHeaders();
    expect(listed).toHaveLength(2);
    expect(listed.every((h) => h.hitCount === 2)).toBe(true);

    st.close();
  });

  // ── Test 2: Header value tracking with overflow ──────────────

  test("tracks header values and overflows at cap", () => {
    const st = createTracker();

    // Record 50 unique values
    for (let i = 0; i < 50; i++) {
      st.recordHeaders(new Headers({ "x-variant": `value-${i}` }));
    }

    let listed = st.listHeaders();
    expect(listed[0]!.sampleValues).toHaveLength(50);
    expect(listed[0]!.valueOverflow).toBe(false);

    // 51st value should trigger overflow
    const changes = st.recordHeaders(new Headers({ "x-variant": "value-50" }));
    expect(changes.filter((c) => c.type === "new_header_value")).toHaveLength(0);

    listed = st.listHeaders();
    expect(listed[0]!.valueOverflow).toBe(true);

    st.close();
  });

  // ── Test 13: Empty headers ──────────────────────────────────

  test("handles empty headers without error", () => {
    const st = createTracker();
    const changes = st.recordHeaders(new Headers());
    expect(changes).toHaveLength(0);
    st.close();
  });

  test("tracks new header values and includes previous values", () => {
    const st = createTracker();

    st.recordHeaders(new Headers({ "x-region": "us-east-1" }));
    const changes = st.recordHeaders(new Headers({ "x-region": "eu-west-1" }));

    expect(changes).toHaveLength(1);
    const c = changes[0]!;
    expect(c.type).toBe("new_header_value");
    if (c.type === "new_header_value") {
      expect(c.value).toBe("eu-west-1");
      expect(c.previousValues).toEqual(["us-east-1"]);
    }

    st.close();
  });
});

// ── Test 3: JSON body field discovery ────────────────────────────

describe("Body Schema Tracking", () => {
  test("discovers all fields in a JSON response", () => {
    const st = createTracker();

    const json = JSON.stringify({
      id: "msg_1",
      type: "message",
      usage: { input_tokens: 100 },
    });

    const changes1 = st.recordResponseJson("/v1/messages", json);
    const newFields = changes1.filter((c) => c.type === "new_field");
    const paths = newFields.map((c) => c.type === "new_field" && c.path);
    expect(paths).toContain("id");
    expect(paths).toContain("type");
    expect(paths).toContain("usage");
    expect(paths).toContain("usage.input_tokens");

    // Second call returns no changes
    const changes2 = st.recordResponseJson("/v1/messages", json);
    expect(changes2).toHaveLength(0);

    // Hit counts should be 2
    const fields = st.listFields();
    expect(fields.every((f) => f.hitCount === 2)).toBe(true);

    st.close();
  });

  // ── Test 4: New field type detection ─────────────────────────

  test("detects new type for existing field", () => {
    const st = createTracker();

    st.recordResponseJson("/v1/messages", JSON.stringify({ stop_reason: "end_turn" }));
    const changes = st.recordResponseJson("/v1/messages", JSON.stringify({ stop_reason: null }));

    const typeChange = changes.find((c) => c.type === "new_field_type");
    expect(typeChange).toBeDefined();
    if (typeChange?.type === "new_field_type") {
      expect(typeChange.newType).toBe("null");
      expect(typeChange.previousTypes).toEqual(["string"]);
    }

    st.close();
  });

  // ── Test 5: Value tracking for enum-like fields ──────────────

  test("tracks values for enum-like fields", () => {
    const st = createTracker();

    st.recordResponseJson("/v1/messages", JSON.stringify({ stop_reason: "end_turn" }));
    st.recordResponseJson("/v1/messages", JSON.stringify({ stop_reason: "max_tokens" }));

    const fields = st.listFields();
    const stopReason = fields.find((f) => f.path === "stop_reason");
    expect(stopReason).toBeDefined();
    expect(stopReason!.sampleValues).toContain("end_turn");
    expect(stopReason!.sampleValues).toContain("max_tokens");
    expect(stopReason!.valueOverflow).toBe(false);

    st.close();
  });

  // ── Test 6: Streaming SSE event tracking ─────────────────────

  test("records streaming event schema", () => {
    const st = createTracker();

    const changes = st.recordStreamEvent(
      "/v1/messages",
      "message_start",
      { type: "message_start", message: { usage: { input_tokens: 50 } } },
    );

    const newFields = changes.filter((c) => c.type === "new_field");
    const paths = newFields.map((c) => c.type === "new_field" && c.path);
    expect(paths).toContain("type");
    expect(paths).toContain("message.usage.input_tokens");

    // Check context is "message_start"
    const fields = st.listFields();
    const typeField = fields.find((f) => f.path === "type" && f.context === "message_start");
    expect(typeField).toBeDefined();

    st.close();
  });

  // ── Test 8: Long strings not sampled ─────────────────────────

  test("does not sample strings longer than 200 characters", () => {
    const st = createTracker();

    const longString = "x".repeat(201);
    st.recordResponseJson("/v1/messages", JSON.stringify({ content: longString }));

    const fields = st.listFields();
    const contentField = fields.find((f) => f.path === "content");
    expect(contentField).toBeDefined();
    expect(contentField!.sampleValues).toHaveLength(0);
    expect(contentField!.valueOverflow).toBe(true);

    st.close();
  });

  test("handles invalid JSON gracefully", () => {
    const st = createTracker();
    const changes = st.recordResponseJson("/v1/messages", "not valid json{{{");
    expect(changes).toHaveLength(0);
    st.close();
  });

  test("tracks array element fields with [] syntax", () => {
    const st = createTracker();

    const json = JSON.stringify({
      content: [{ type: "text", text: "hello" }],
    });
    st.recordResponseJson("/v1/messages", json);

    const fields = st.listFields();
    const paths = fields.map((f) => f.path);
    expect(paths).toContain("content");
    expect(paths).toContain("content[].type");
    expect(paths).toContain("content[].text");

    st.close();
  });
});

// ── Test 7: Persistence round-trip ───────────────────────────────

describe("Persistence", () => {
  test("persists headers and fields across restarts", () => {
    const dbPath = join(tempDir, "test-state.db");

    // First instance: record data and close
    const st1 = new SchemaTracker(dbPath, null);
    st1.recordHeaders(new Headers({ "x-custom": "val1" }));
    st1.recordResponseJson("/v1/messages", JSON.stringify({ id: "msg_1", type: "message" }));
    st1.close();

    // Second instance: verify data loaded
    const st2 = new SchemaTracker(dbPath, null);
    const headers = st2.listHeaders();
    expect(headers).toHaveLength(1);
    expect(headers[0]!.name).toBe("x-custom");
    expect(headers[0]!.sampleValues).toContain("val1");

    const fields = st2.listFields();
    expect(fields.length).toBeGreaterThanOrEqual(2);
    expect(fields.find((f) => f.path === "id")).toBeDefined();
    expect(fields.find((f) => f.path === "type")).toBeDefined();

    // Recording the same data should produce no changes
    const changes = st2.recordHeaders(new Headers({ "x-custom": "val1" }));
    expect(changes).toHaveLength(0);

    st2.close();
  });
});

// ── Test 11: No-webhook-configured path ──────────────────────────

describe("Webhook Integration", () => {
  test("works without any webhooks configured", () => {
    const st = createTracker();

    const changes = st.recordHeaders(new Headers({ "x-foo": "bar" }));
    expect(changes).toHaveLength(1);
    expect(st.sendTestNotification()).toBe(false);

    st.close();
  });

  test("sendTestNotification returns true when webhook configured", () => {
    const st = createTracker("http://localhost:1/webhook");

    expect(st.sendTestNotification()).toBe(true);

    st.close();
  });

  test("addWebhook / listWebhooks / removeWebhook CRUD", () => {
    const st = createTracker();

    expect(st.listWebhooks()).toHaveLength(0);

    st.addWebhook("http://example.com/hook1", "slack");
    st.addWebhook("http://example.com/hook2");
    expect(st.listWebhooks()).toHaveLength(2);
    expect(st.listWebhooks()[0]!.label).toBe("slack");
    expect(st.listWebhooks()[1]!.url).toBe("http://example.com/hook2");

    st.removeWebhook("http://example.com/hook1");
    expect(st.listWebhooks()).toHaveLength(1);
    expect(st.listWebhooks()[0]!.url).toBe("http://example.com/hook2");

    st.close();
  });

  test("seed URL is persisted to webhooks table", () => {
    const dbPath = join(tempDir, "seed-test.db");
    const st1 = new SchemaTracker(dbPath, "http://example.com/seed");
    expect(st1.listWebhooks()).toHaveLength(1);
    expect(st1.listWebhooks()[0]!.url).toBe("http://example.com/seed");
    st1.close();

    // Re-opening should NOT duplicate the seed
    const st2 = new SchemaTracker(dbPath, "http://example.com/seed");
    expect(st2.listWebhooks()).toHaveLength(1);
    st2.close();
  });
});

// ── SchemaTracker → WebhookNotifier end-to-end wiring ────────────

describe("SchemaTracker → WebhookNotifier wiring", () => {
  test("recording new headers fires a webhook with the changes", async () => {
    const received: { text: string; changes: unknown[] }[] = [];
    const webhookServer = Bun.serve({
      port: 0,
      async fetch(req) {
        const body = (await req.json()) as { text: string; changes: unknown[] };
        received.push(body);
        return new Response("ok");
      },
    });

    try {
      const st = createTracker(`http://localhost:${webhookServer.port}`);

      st.recordHeaders(new Headers({ "x-new-header": "hello" }));
      await st.flushAllWebhooks();

      expect(received).toHaveLength(1);
      expect(received[0]!.changes).toHaveLength(1);
      const change = received[0]!.changes[0] as { type: string; name: string };
      expect(change.type).toBe("new_header");
      expect(change.name).toBe("x-new-header");
      expect(received[0]!.text).toContain("x-new-header");

      st.close();
    } finally {
      webhookServer.stop(true);
    }
  });

  test("recording new body fields does NOT fire a webhook", async () => {
    const received: unknown[] = [];
    const webhookServer = Bun.serve({
      port: 0,
      async fetch(req) { received.push(await req.json()); return new Response("ok"); },
    });

    try {
      const st = createTracker(`http://localhost:${webhookServer.port}`);

      st.recordResponseJson("/v1/messages", JSON.stringify({ id: "msg_1", type: "message" }));
      await st.flushAllWebhooks();

      // Body field changes should not trigger webhooks
      expect(received).toHaveLength(0);

      st.close();
    } finally {
      webhookServer.stop(true);
    }
  });

  test("no webhook fires when recording already-known data", async () => {
    const received: unknown[] = [];
    const webhookServer = Bun.serve({
      port: 0,
      async fetch(req) {
        received.push(await req.json());
        return new Response("ok");
      },
    });

    try {
      const st = createTracker(`http://localhost:${webhookServer.port}`);

      // First call: new data → webhook
      st.recordHeaders(new Headers({ "x-known": "val" }));
      await st.flushAllWebhooks();
      expect(received).toHaveLength(1);

      // Second call: same data → no webhook
      st.recordHeaders(new Headers({ "x-known": "val" }));
      await st.flushAllWebhooks();
      // Should still be 1 — no new changes, so no new webhook
      expect(received).toHaveLength(1);

      st.close();
    } finally {
      webhookServer.stop(true);
    }
  });

  test("dynamically added webhook receives notifications", async () => {
    const received: unknown[] = [];
    const webhookServer = Bun.serve({
      port: 0,
      async fetch(req) {
        received.push(await req.json());
        return new Response("ok");
      },
    });

    try {
      const st = createTracker(); // no seed URL
      st.addWebhook(`http://localhost:${webhookServer.port}`, "test-hook");

      st.recordHeaders(new Headers({ "x-dynamic": "yes" }));
      await st.flushAllWebhooks();

      expect(received).toHaveLength(1);

      st.close();
    } finally {
      webhookServer.stop(true);
    }
  });
});

// ── Multi-webhook and removal tests ─────────────────────────────

describe("Multiple webhooks fan-out", () => {
  test("both webhooks receive the same schema change notification", async () => {
    const received1: unknown[] = [];
    const received2: unknown[] = [];
    const server1 = Bun.serve({
      port: 0,
      async fetch(req) { received1.push(await req.json()); return new Response("ok"); },
    });
    const server2 = Bun.serve({
      port: 0,
      async fetch(req) { received2.push(await req.json()); return new Response("ok"); },
    });

    try {
      const st = createTracker();
      st.addWebhook(`http://localhost:${server1.port}`, "hook-1");
      st.addWebhook(`http://localhost:${server2.port}`, "hook-2");

      st.recordHeaders(new Headers({ "x-fanout": "test" }));
      await st.flushAllWebhooks();

      expect(received1).toHaveLength(1);
      expect(received2).toHaveLength(1);
      // Both should contain the same change
      const c1 = (received1[0] as { changes: { type: string; name: string }[] }).changes;
      const c2 = (received2[0] as { changes: { type: string; name: string }[] }).changes;
      expect(c1[0]!.type).toBe("new_header");
      expect(c1[0]!.name).toBe("x-fanout");
      expect(c2[0]!.type).toBe("new_header");
      expect(c2[0]!.name).toBe("x-fanout");

      st.close();
    } finally {
      server1.stop(true);
      server2.stop(true);
    }
  });
});

describe("Webhook removal stops delivery", () => {
  test("removed webhook does not receive subsequent notifications", async () => {
    const received: unknown[] = [];
    const server = Bun.serve({
      port: 0,
      async fetch(req) { received.push(await req.json()); return new Response("ok"); },
    });

    try {
      const st = createTracker();
      const url = `http://localhost:${server.port}`;
      st.addWebhook(url, "temp");

      // First change — webhook should fire
      st.recordHeaders(new Headers({ "x-before-remove": "yes" }));
      await st.flushAllWebhooks();
      expect(received).toHaveLength(1);

      // Remove webhook
      st.removeWebhook(url);

      // Second change — webhook should NOT fire
      st.recordHeaders(new Headers({ "x-after-remove": "yes" }));
      await st.flushAllWebhooks();
      expect(received).toHaveLength(1); // still 1, not 2

      st.close();
    } finally {
      server.stop(true);
    }
  });
});

describe("sendTestNotification targeting specific URL", () => {
  test("sends test only to the specified webhook URL", async () => {
    const received1: unknown[] = [];
    const received2: unknown[] = [];
    const server1 = Bun.serve({
      port: 0,
      async fetch(req) { received1.push(await req.json()); return new Response("ok"); },
    });
    const server2 = Bun.serve({
      port: 0,
      async fetch(req) { received2.push(await req.json()); return new Response("ok"); },
    });

    try {
      const st = createTracker();
      const url1 = `http://localhost:${server1.port}`;
      const url2 = `http://localhost:${server2.port}`;
      st.addWebhook(url1, "target");
      st.addWebhook(url2, "bystander");

      // Send test only to url1
      const sent = st.sendTestNotification(url1);
      expect(sent).toBe(true);
      await st.flushAllWebhooks();

      expect(received1).toHaveLength(1);
      expect(received2).toHaveLength(0);

      // Verify the test payload
      const payload = received1[0] as { changes: { type: string; name: string }[] };
      expect(payload.changes[0]!.name).toBe("x-test-notification");

      st.close();
    } finally {
      server1.stop(true);
      server2.stop(true);
    }
  });

  test("returns false for non-existent webhook URL", () => {
    const st = createTracker();
    st.addWebhook("http://example.com/real", "real");

    expect(st.sendTestNotification("http://example.com/fake")).toBe(false);

    st.close();
  });
});

// ── Test 14: Field-path cap ──────────────────────────────────────

describe("Field-path Cap", () => {
  test("stops adding fields beyond MAX_FIELD_PATHS", () => {
    const st = createTracker();

    // Generate a wide object with many keys to approach the cap
    // MAX_FIELD_PATHS is 10_000, but we can test the mechanism with a realistic scenario
    const wideObj: Record<string, string> = {};
    for (let i = 0; i < 100; i++) {
      wideObj[`field_${i}`] = `value_${i}`;
    }

    // Record many times with different endpoints to accumulate fields
    for (let batch = 0; batch < 105; batch++) {
      st.recordResponseJson(`/v1/endpoint_${batch}`, JSON.stringify(wideObj));
    }

    // Should have capped at 10000
    const fields = st.listFields();
    expect(fields.length).toBeLessThanOrEqual(10_000);

    st.close();
  });
});
