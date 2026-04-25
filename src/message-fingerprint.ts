import { createHash } from "node:crypto";

export type MessagesFingerprint = {
  readonly firstMessageHash: string;
  readonly secondMessageHash: string | null;
  readonly messageCount: number;
  readonly firstMessagePreview: string;
};

const PREVIEW_MAX_LEN = 100;
const HASH_HEX_LEN = 16;

export function extractMessagesFingerprint(
  body: Uint8Array | null,
  path: string,
): MessagesFingerprint | null {
  if (body === null) return null;
  if (!path.endsWith("/v1/messages")) return null;

  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(body));
  } catch {
    return null;
  }

  if (typeof parsed !== "object" || parsed === null) return null;
  const messages = (parsed as { messages?: unknown }).messages;
  if (!Array.isArray(messages) || messages.length === 0) return null;

  const firstHash = hashMessage(messages[0]);
  if (firstHash === null) return null;

  return {
    firstMessageHash: firstHash,
    secondMessageHash: messages.length >= 2 ? hashMessage(messages[1]) : null,
    messageCount: messages.length,
    firstMessagePreview: extractTextPreview(messages[0], PREVIEW_MAX_LEN),
  };
}

function hashMessage(message: unknown): string | null {
  const serialized = JSON.stringify(message);
  if (serialized === undefined) return null;
  return createHash("sha256").update(serialized).digest("hex").slice(0, HASH_HEX_LEN);
}

function extractTextPreview(message: unknown, maxLen: number): string {
  if (typeof message !== "object" || message === null) return "";
  const content = (message as { content?: unknown }).content;
  if (typeof content === "string") return clip(content, maxLen);
  if (Array.isArray(content)) {
    for (const block of content) {
      if (
        typeof block === "object" && block !== null
        && (block as { type?: unknown }).type === "text"
        && typeof (block as { text?: unknown }).text === "string"
      ) {
        return clip((block as { text: string }).text, maxLen);
      }
    }
  }
  return "";
}

function clip(text: string, maxLen: number): string {
  return text.slice(0, maxLen).replace(/\s+/g, " ").trim();
}
