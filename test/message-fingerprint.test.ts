import { describe, test, expect } from "bun:test";
import {
  computeFirstMessageHash,
  extractActorFromConversationKey,
  extractFirstMessageHashFromConversationKey,
} from "../src/message-fingerprint.ts";

function bodyOf(obj: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(obj));
}

describe("computeFirstMessageHash", () => {
  test("returns null when body is null", () => {
    expect(computeFirstMessageHash(null, "/v1/messages")).toBeNull();
  });

  test("returns null when path is not under /v1/messages", () => {
    const body = bodyOf({ messages: [{ role: "user", content: "hi" }] });
    expect(computeFirstMessageHash(body, "/v1/complete")).toBeNull();
  });

  test("matches /v1/messages and its sub-paths (e.g. count_tokens)", () => {
    const body = bodyOf({ messages: [{ role: "user", content: "hi" }] });
    expect(computeFirstMessageHash(body, "/v1/messages")).not.toBeNull();
    expect(computeFirstMessageHash(body, "/v1/messages/count_tokens")).not.toBeNull();
  });

  test("returns null for unparseable body", () => {
    const body = new TextEncoder().encode("not json");
    expect(computeFirstMessageHash(body, "/v1/messages")).toBeNull();
  });

  test("returns null when messages field is missing or empty", () => {
    expect(computeFirstMessageHash(bodyOf({ model: "x" }), "/v1/messages")).toBeNull();
    expect(computeFirstMessageHash(bodyOf({ messages: [] }), "/v1/messages")).toBeNull();
  });

  test("produces a 16-char hex hash for valid bodies", () => {
    const body = bodyOf({ messages: [{ role: "user", content: "hello" }] });
    const hash = computeFirstMessageHash(body, "/v1/messages");
    expect(hash).toMatch(/^[0-9a-f]{16}$/);
  });

  test("hash is stable across calls with identical content", () => {
    const body = bodyOf({ messages: [{ role: "user", content: "stable" }] });
    expect(computeFirstMessageHash(body, "/v1/messages"))
      .toBe(computeFirstMessageHash(body, "/v1/messages"));
  });

  test("hash differs when first message content differs", () => {
    const a = computeFirstMessageHash(
      bodyOf({ messages: [{ role: "user", content: "alpha" }] }),
      "/v1/messages",
    );
    const b = computeFirstMessageHash(
      bodyOf({ messages: [{ role: "user", content: "beta" }] }),
      "/v1/messages",
    );
    expect(a).not.toBe(b);
  });

  test("hash is unchanged when later messages are appended (append-only invariant)", () => {
    const turn1 = computeFirstMessageHash(
      bodyOf({ messages: [{ role: "user", content: "shared first" }] }),
      "/v1/messages",
    );
    const turn2 = computeFirstMessageHash(
      bodyOf({
        messages: [
          { role: "user", content: "shared first" },
          { role: "assistant", content: "reply" },
          { role: "user", content: "follow up" },
        ],
      }),
      "/v1/messages",
    );
    expect(turn2).toBe(turn1);
  });

  test("hash differs across content shapes (string vs blocks) even if text is identical", () => {
    const asString = computeFirstMessageHash(
      bodyOf({ messages: [{ role: "user", content: "hi" }] }),
      "/v1/messages",
    );
    const asBlocks = computeFirstMessageHash(
      bodyOf({ messages: [{ role: "user", content: [{ type: "text", text: "hi" }] }] }),
      "/v1/messages",
    );
    expect(asString).not.toBe(asBlocks);
  });
});

describe("extractFirstMessageHashFromConversationKey", () => {
  test("returns the trailing 16-hex hash when present", () => {
    expect(extractFirstMessageHashFromConversationKey("till:abc-123:0123456789abcdef"))
      .toBe("0123456789abcdef");
  });

  test("returns null for legacy 2-part keys without a hash", () => {
    expect(extractFirstMessageHashFromConversationKey("till:abc-123")).toBeNull();
  });

  test("ignores trailing segments that are not 16-hex", () => {
    expect(extractFirstMessageHashFromConversationKey("till:abc-123:nothex"))
      .toBeNull();
    expect(extractFirstMessageHashFromConversationKey("till:abc-123:0123456789ABCDEF"))
      .toBeNull();
  });

  test("round-trips with extractActorFromConversationKey", () => {
    expect(extractActorFromConversationKey("till@trainly.ai:abc-123:0123456789abcdef"))
      .toBe("till@trainly.ai");
    expect(extractActorFromConversationKey("till:abc-123")).toBe("till");
    expect(extractActorFromConversationKey("standalone")).toBe("standalone");
  });

  test("round-trips with computeFirstMessageHash", () => {
    const body = new TextEncoder().encode(JSON.stringify({
      messages: [{ role: "user", content: "round trip" }],
    }));
    const hash = computeFirstMessageHash(body, "/v1/messages")!;
    const conversationKey = `actor:session:${hash}`;
    expect(extractFirstMessageHashFromConversationKey(conversationKey)).toBe(hash);
  });
});
