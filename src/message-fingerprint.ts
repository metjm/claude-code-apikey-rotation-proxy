import { createHash } from "node:crypto";

const HASH_HEX_LEN = 16;
const HASH_SUFFIX_PATTERN = /:([0-9a-f]{16})$/;

export function computeFirstMessageHash(
  body: Uint8Array | null,
  path: string,
): string | null {
  if (body === null) return null;
  // Only the exact /v1/messages endpoint represents a real conversation
  // turn. Sibling paths like /v1/messages/count_tokens are non-streaming
  // probes that share `messages[0]` with the real call but produce no
  // throughput — pinning them creates phantom 0/0 rows on the dashboard.
  if (path !== "/v1/messages") return null;

  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(body));
  } catch {
    return null;
  }

  if (typeof parsed !== "object" || parsed === null) return null;
  const messages = (parsed as { messages?: unknown }).messages;
  if (!Array.isArray(messages) || messages.length === 0) return null;

  const serialized = JSON.stringify(messages[0]);
  if (serialized === undefined) return null;
  return createHash("sha256").update(serialized).digest("hex").slice(0, HASH_HEX_LEN);
}

export function extractFirstMessageHashFromConversationKey(
  conversationKey: string,
): string | null {
  return conversationKey.match(HASH_SUFFIX_PATTERN)?.[1] ?? null;
}

export function extractActorFromConversationKey(conversationKey: string): string {
  const idx = conversationKey.indexOf(":");
  return idx === -1 ? conversationKey : conversationKey.slice(0, idx);
}
