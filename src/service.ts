import { existsSync, mkdirSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { execSync } from "node:child_process";
import { homedir } from "node:os";

type Platform = "linux" | "darwin";

function getPlatform(): Platform {
  const p = process.platform;
  if (p === "linux") return "linux";
  if (p === "darwin") return "darwin";
  throw new Error(`Unsupported platform: ${p}. Only linux (systemd) and macOS (launchd) are supported.`);
}

function getBunPath(): string {
  const result = execSync("which bun", { encoding: "utf-8" }).trim();
  if (!result) throw new Error("Could not find bun on PATH");
  return result;
}

function getServerScript(): string {
  return resolve(dirname(new URL(import.meta.url).pathname), "server.ts");
}

const SERVICE_NAME = "claude-proxy";

// ── Systemd (Linux) ───────────────────────────────────────────────

function systemdUnitPath(): string {
  return join(homedir(), ".config", "systemd", "user", `${SERVICE_NAME}.service`);
}

function generateSystemdUnit(bunPath: string, serverScript: string, dataDir: string): string {
  return `[Unit]
Description=Claude Code API Key Rotation Proxy
After=network.target

[Service]
Type=simple
ExecStart=${bunPath} run ${serverScript}
Restart=on-failure
RestartSec=5
Environment=DATA_DIR=${dataDir}
WorkingDirectory=${dirname(serverScript)}

[Install]
WantedBy=default.target
`;
}

function installSystemd(dataDir: string): void {
  const bunPath = getBunPath();
  const serverScript = getServerScript();
  const unitPath = systemdUnitPath();
  const unit = generateSystemdUnit(bunPath, serverScript, dataDir);

  mkdirSync(dirname(unitPath), { recursive: true });
  writeFileSync(unitPath, unit);
  console.log(`Wrote ${unitPath}`);

  execSync("systemctl --user daemon-reload", { stdio: "inherit" });
  execSync(`systemctl --user enable ${SERVICE_NAME}`, { stdio: "inherit" });
  execSync(`systemctl --user start ${SERVICE_NAME}`, { stdio: "inherit" });
  console.log(`\nService installed and started.`);
  console.log(`  systemctl --user status ${SERVICE_NAME}`);
  console.log(`  journalctl --user -u ${SERVICE_NAME} -f`);
}

function uninstallSystemd(): void {
  try { execSync(`systemctl --user stop ${SERVICE_NAME}`, { stdio: "inherit" }); } catch {}
  try { execSync(`systemctl --user disable ${SERVICE_NAME}`, { stdio: "inherit" }); } catch {}

  const unitPath = systemdUnitPath();
  if (existsSync(unitPath)) {
    unlinkSync(unitPath);
    console.log(`Removed ${unitPath}`);
  }
  try { execSync("systemctl --user daemon-reload", { stdio: "inherit" }); } catch {}
  console.log("Service uninstalled.");
}

function statusSystemd(): void {
  try {
    execSync(`systemctl --user status ${SERVICE_NAME}`, { stdio: "inherit" });
  } catch {
    // systemctl returns non-zero for inactive services, that's fine
  }
}

// ── Launchd (macOS) ──────────────────────────────────────────────

function launchdPlistPath(): string {
  return join(homedir(), "Library", "LaunchAgents", `com.${SERVICE_NAME}.plist`);
}

function launchdLabel(): string {
  return `com.${SERVICE_NAME}`;
}

function generateLaunchdPlist(bunPath: string, serverScript: string, dataDir: string): string {
  const logDir = join(homedir(), "Library", "Logs", SERVICE_NAME);
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${launchdLabel()}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${bunPath}</string>
    <string>run</string>
    <string>${serverScript}</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>DATA_DIR</key>
    <string>${dataDir}</string>
  </dict>
  <key>WorkingDirectory</key>
  <string>${dirname(serverScript)}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${logDir}/stdout.log</string>
  <key>StandardErrorPath</key>
  <string>${logDir}/stderr.log</string>
</dict>
</plist>
`;
}

function installLaunchd(dataDir: string): void {
  const bunPath = getBunPath();
  const serverScript = getServerScript();
  const plistPath = launchdPlistPath();
  const logDir = join(homedir(), "Library", "Logs", SERVICE_NAME);

  mkdirSync(dirname(plistPath), { recursive: true });
  mkdirSync(logDir, { recursive: true });

  const plist = generateLaunchdPlist(bunPath, serverScript, dataDir);
  writeFileSync(plistPath, plist);
  console.log(`Wrote ${plistPath}`);

  execSync(`launchctl load -w ${plistPath}`, { stdio: "inherit" });
  console.log(`\nService installed and started.`);
  console.log(`  launchctl list | grep ${SERVICE_NAME}`);
  console.log(`  tail -f ${logDir}/stdout.log`);
}

function uninstallLaunchd(): void {
  const plistPath = launchdPlistPath();
  if (existsSync(plistPath)) {
    try { execSync(`launchctl unload ${plistPath}`, { stdio: "inherit" }); } catch {}
    unlinkSync(plistPath);
    console.log(`Removed ${plistPath}`);
  }
  console.log("Service uninstalled.");
}

function statusLaunchd(): void {
  try {
    execSync(`launchctl list ${launchdLabel()}`, { stdio: "inherit" });
  } catch {
    console.log("Service is not loaded.");
  }
}

// ── Public API ────────────────────────────────────────────────────

export function serviceInstall(dataDir: string): void {
  const platform = getPlatform();
  const resolvedDataDir = resolve(dataDir);
  mkdirSync(resolvedDataDir, { recursive: true });
  console.log(`Data directory: ${resolvedDataDir}`);

  if (platform === "linux") installSystemd(resolvedDataDir);
  else installLaunchd(resolvedDataDir);
}

export function serviceUninstall(): void {
  const platform = getPlatform();
  if (platform === "linux") uninstallSystemd();
  else uninstallLaunchd();
}

export function serviceStatus(): void {
  const platform = getPlatform();
  if (platform === "linux") statusSystemd();
  else statusLaunchd();
}
