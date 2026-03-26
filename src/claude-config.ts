import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";

const SETTINGS_PATH = join(homedir(), ".claude", "settings.json");
const ENV_KEY = "ANTHROPIC_BASE_URL";

interface ClaudeSettings {
  env?: Record<string, string>;
  [key: string]: unknown;
}

function readSettings(): ClaudeSettings {
  if (!existsSync(SETTINGS_PATH)) return {};
  const raw = readFileSync(SETTINGS_PATH, "utf-8");
  return JSON.parse(raw) as ClaudeSettings;
}

function writeSettings(settings: ClaudeSettings): void {
  mkdirSync(join(homedir(), ".claude"), { recursive: true });
  writeFileSync(SETTINGS_PATH, JSON.stringify(settings, null, 2) + "\n");
}

export function claudeConfigInstall(port: number): void {
  const settings = readSettings();
  const url = `http://localhost:${port}`;

  const existing = settings.env?.[ENV_KEY];
  if (existing === url) {
    console.log(`Already configured: ${ENV_KEY}=${url}`);
    return;
  }

  if (existing !== undefined) {
    console.log(`Overwriting existing ${ENV_KEY}=${existing}`);
  }

  settings.env = { ...settings.env, [ENV_KEY]: url };
  writeSettings(settings);
  console.log(`Set ${ENV_KEY}=${url} in ${SETTINGS_PATH}`);
  console.log("Claude Code will now use the proxy for all new sessions.");
}

export function claudeConfigUninstall(): void {
  const settings = readSettings();

  if (settings.env?.[ENV_KEY] === undefined) {
    console.log(`${ENV_KEY} is not set in ${SETTINGS_PATH}`);
    return;
  }

  delete settings.env[ENV_KEY];
  if (settings.env !== undefined && Object.keys(settings.env).length === 0) {
    delete settings.env;
  }
  writeSettings(settings);
  console.log(`Removed ${ENV_KEY} from ${SETTINGS_PATH}`);
  console.log("Claude Code will use the default Anthropic API for new sessions.");
}

export function claudeConfigStatus(port: number): void {
  const settings = readSettings();
  const current = settings.env?.[ENV_KEY];
  const expected = `http://localhost:${port}`;

  if (current === expected) {
    console.log(`Active: ${ENV_KEY}=${current}`);
  } else if (current !== undefined) {
    console.log(`Set to different URL: ${ENV_KEY}=${current} (expected ${expected})`);
  } else {
    console.log("Not configured. Run 'claude-proxy claude install' to enable.");
  }
}
