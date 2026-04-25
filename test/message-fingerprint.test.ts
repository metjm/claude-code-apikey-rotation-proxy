import { describe, test, expect } from "bun:test";
import { extractMessagesFingerprint } from "../src/message-fingerprint.ts";

function bodyOf(obj: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(obj));
}

describe("extractMessagesFingerprint", () => {
  test("returns null when body is null", () => {
    expect(extractMessagesFingerprint(null, "/v1/messages")).toBeNull();
  });

  test("returns null when path is not /v1/messages", () => {
    const body = bodyOf({ messages: [{ role: "user", content: "hi" }] });
    expect(extractMessagesFingerprint(body, "/v1/complete")).toBeNull();
  });

  test("matches /v1/messages even with prefix", () => {
    const body = bodyOf({ messages: [{ role: "user", content: "hi" }] });
    expect(extractMessagesFingerprint(body, "/proxy/v1/messages")).not.toBeNull();
  });

  test("returns null for unparseable body", () => {
    const body = new TextEncoder().encode("not json");
    expect(extractMessagesFingerprint(body, "/v1/messages")).toBeNull();
  });

  test("returns null when messages field is missing", () => {
    const body = bodyOf({ model: "claude-opus-4-7" });
    expect(extractMessagesFingerprint(body, "/v1/messages")).toBeNull();
  });

  test("returns null when messages is empty", () => {
    const body = bodyOf({ messages: [] });
    expect(extractMessagesFingerprint(body, "/v1/messages")).toBeNull();
  });

  test("extracts fingerprint from string content", () => {
    const body = bodyOf({ messages: [{ role: "user", content: "hello world" }] });
    const fp = extractMessagesFingerprint(body, "/v1/messages");
    expect(fp).not.toBeNull();
    expect(fp!.messageCount).toBe(1);
    expect(fp!.firstMessageHash).toMatch(/^[0-9a-f]{16}$/);
    expect(fp!.secondMessageHash).toBeNull();
    expect(fp!.firstMessagePreview).toBe("hello world");
  });

  test("extracts preview from content-block array", () => {
    const body = bodyOf({
      messages: [{
        role: "user",
        content: [
          { type: "tool_result", tool_use_id: "t1", content: "ignored" },
          { type: "text", text: "this is the first text block" },
        ],
      }],
    });
    const fp = extractMessagesFingerprint(body, "/v1/messages");
    expect(fp!.firstMessagePreview).toBe("this is the first text block");
  });

  test("clips long previews to max length and collapses whitespace", () => {
    const longText = "x".repeat(200);
    const noisyText = "  hello\n\n\nworld   tabs\there";
    const fp1 = extractMessagesFingerprint(
      bodyOf({ messages: [{ role: "user", content: longText }] }),
      "/v1/messages",
    );
    expect(fp1!.firstMessagePreview.length).toBeLessThanOrEqual(100);

    const fp2 = extractMessagesFingerprint(
      bodyOf({ messages: [{ role: "user", content: noisyText }] }),
      "/v1/messages",
    );
    expect(fp2!.firstMessagePreview).toBe("hello world tabs here");
  });

  test("hash is stable across calls with identical content", () => {
    const body = bodyOf({ messages: [{ role: "user", content: "stable" }] });
    const a = extractMessagesFingerprint(body, "/v1/messages")!;
    const b = extractMessagesFingerprint(body, "/v1/messages")!;
    expect(a.firstMessageHash).toBe(b.firstMessageHash);
  });

  test("hash differs when first message content differs", () => {
    const a = extractMessagesFingerprint(
      bodyOf({ messages: [{ role: "user", content: "alpha" }] }),
      "/v1/messages",
    )!;
    const b = extractMessagesFingerprint(
      bodyOf({ messages: [{ role: "user", content: "beta" }] }),
      "/v1/messages",
    )!;
    expect(a.firstMessageHash).not.toBe(b.firstMessageHash);
  });

  test("messageCount and secondMessageHash reflect a multi-turn conversation", () => {
    const body = bodyOf({
      messages: [
        { role: "user", content: "turn 1 user" },
        { role: "assistant", content: "turn 1 assistant" },
        { role: "user", content: "turn 2 user" },
      ],
    });
    const fp = extractMessagesFingerprint(body, "/v1/messages")!;
    expect(fp.messageCount).toBe(3);
    expect(fp.secondMessageHash).toMatch(/^[0-9a-f]{16}$/);
    expect(fp.secondMessageHash).not.toBe(fp.firstMessageHash);
  });

  test("first message hash is unchanged when later messages are appended", () => {
    const turn1 = extractMessagesFingerprint(
      bodyOf({ messages: [{ role: "user", content: "shared first" }] }),
      "/v1/messages",
    )!;
    const turn2 = extractMessagesFingerprint(
      bodyOf({
        messages: [
          { role: "user", content: "shared first" },
          { role: "assistant", content: "reply" },
          { role: "user", content: "follow up" },
        ],
      }),
      "/v1/messages",
    )!;
    expect(turn2.firstMessageHash).toBe(turn1.firstMessageHash);
    expect(turn2.messageCount).toBe(3);
  });
});
