import { existsSync, mkdirSync, openSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { execSync } from "node:child_process";
import { homedir } from "node:os";

type Backend = "launchd" | "systemd" | "pidfile";

function detectBackend(): Backend {
  if (process.platform === "darwin") return "launchd";
  if (process.platform === "linux") {
    // Check if PID 1 is actually systemd — wrapper scripts can fake `systemctl`
    try {
      const init = readFileSync("/proc/1/comm", "utf-8").trim();
      if (init === "systemd") return "systemd";
    } catch {}
    return "pidfile";
  }
  return "pidfile";
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

// ── PID file (universal fallback) ─────────────────────────────────

function pidDir(): string {
  return join(homedir(), `.${SERVICE_NAME}`);
}

function pidFilePath(): string {
  return join(pidDir(), "pid");
}

export function logDir(): string {
  return join(pidDir(), "logs");
}

function newLogFilePath(): string {
  const ts = new Date().toISOString().replace(/[:.]/g, "-").replace("T", "_").replace("Z", "");
  return join(logDir(), `output-${ts}.log`);
}

function latestLogFile(): string | null {
  const dir = logDir();
  if (!existsSync(dir)) return null;
  const { readdirSync } = require("node:fs") as typeof import("node:fs");
  const files = readdirSync(dir)
    .filter((f: string) => f.startsWith("output-") && f.endsWith(".log"))
    .sort()
    .reverse();
  return files.length > 0 ? join(dir, files[0]!) : null;
}

function readPid(): number | null {
  const p = pidFilePath();
  if (!existsSync(p)) return null;
  const raw = readFileSync(p, "utf-8").trim();
  const pid = parseInt(raw, 10);
  if (Number.isNaN(pid)) return null;

  // Check if process is alive
  try {
    process.kill(pid, 0);
    return pid;
  } catch {
    // Stale PID file
    unlinkSync(p);
    return null;
  }
}

function installPidfile(dataDir: string): void {
  const existing = readPid();
  if (existing !== null) {
    console.log(`Already running (pid ${existing}). Run 'claude-proxy service uninstall' first.`);
    return;
  }

  const bunPath = getBunPath();
  const serverScript = getServerScript();
  const dir = pidDir();
  mkdirSync(dir, { recursive: true });

  const logsDir = logDir();
  mkdirSync(logsDir, { recursive: true });
  const logPath = newLogFilePath();
  const logFd = openSync(logPath, "a");

  const child = Bun.spawn([bunPath, "run", serverScript], {
    stdin: "ignore",
    stdout: logFd,
    stderr: logFd,
    env: { ...process.env, DATA_DIR: dataDir },
    cwd: dirname(serverScript),
  });

  child.unref();

  writeFileSync(pidFilePath(), String(child.pid));
  console.log(`\nStarted claude-proxy (pid ${child.pid})`);
  console.log(`  Logs: ${logPath}`);
  console.log(`  Stop: claude-proxy service uninstall`);
  process.exit(0);
}

function uninstallPidfile(): void {
  const pid = readPid();
  if (pid !== null) {
    try {
      process.kill(pid, "SIGTERM");
      console.log(`Stopped process ${pid}.`);
    } catch {
      console.log(`Process ${pid} already dead.`);
    }
  } else {
    console.log("Not running.");
  }
  const p = pidFilePath();
  if (existsSync(p)) unlinkSync(p);
  console.log("Service uninstalled.");
}

function statusPidfile(): void {
  const pid = readPid();
  if (pid !== null) {
    console.log(`Running (pid ${pid})`);
    const latest = latestLogFile();
    if (latest) console.log(`  Logs: ${latest}`);
  } else {
    console.log("Not running.");
  }
}

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
    // systemctl returns non-zero for inactive services
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

const BACKEND_LABELS: Record<Backend, string> = {
  launchd: "launchd (macOS)",
  systemd: "systemd (Linux)",
  pidfile: "background process",
};

export function serviceInstall(dataDir: string): void {
  const backend = detectBackend();
  const resolvedDataDir = resolve(dataDir);
  mkdirSync(resolvedDataDir, { recursive: true });
  console.log(`Data directory: ${resolvedDataDir}`);
  console.log(`Backend: ${BACKEND_LABELS[backend]}`);

  switch (backend) {
    case "systemd": installSystemd(resolvedDataDir); break;
    case "launchd": installLaunchd(resolvedDataDir); break;
    case "pidfile": installPidfile(resolvedDataDir); break;
  }
}

export function serviceUninstall(): void {
  const backend = detectBackend();
  switch (backend) {
    case "systemd": uninstallSystemd(); break;
    case "launchd": uninstallLaunchd(); break;
    case "pidfile": uninstallPidfile(); break;
  }
}

export function serviceStatus(): void {
  const backend = detectBackend();
  switch (backend) {
    case "systemd": statusSystemd(); break;
    case "launchd": statusLaunchd(); break;
    case "pidfile": statusPidfile(); break;
  }
}
