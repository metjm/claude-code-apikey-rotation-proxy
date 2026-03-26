# claude-proxy

A pass-through proxy for the Anthropic API that rotates multiple API keys. When a key gets rate-limited, it switches to the next one. Keys that hit a rate limit are shelved until their cooldown expires so they aren't retried needlessly. Per-key stats (requests, errors, rate limit hits) are persisted to disk.

Supports both regular API keys (`sk-ant-api-*`) and OAuth tokens (`sk-ant-oat-*`).

Requires [Bun](https://bun.sh).

## Install

```
git clone git@github.com:metjm/claude-code-apikey-rotation-proxy.git
cd claude-code-apikey-rotation-proxy
bun install
bun link
```

This puts `claude-proxy` and `claude-proxy-ctl` on your PATH.

## Setup

```
claude-proxy-ctl add sk-ant-oat-your-key-here my-label
claude-proxy-ctl add sk-ant-oat-another-key second-label
claude-proxy service install
claude-proxy claude install
```

That's it. All new Claude Code sessions will route through the proxy.

## Commands

`claude-proxy` starts the server. `claude-proxy service install/uninstall/status` manages it as a background service. `claude-proxy claude install/uninstall/status` toggles `ANTHROPIC_BASE_URL` in `~/.claude/settings.json`. `claude-proxy-ctl add/remove/list/stats` manages keys against a running proxy.

## Configuration

All via environment variables: `PORT` (default 4080), `UPSTREAM_URL` (default https://api.anthropic.com), `ADMIN_TOKEN` (optional auth for /admin endpoints), `DATA_DIR` (default ./data), `MAX_RETRIES` (default 10), `LOG_LEVEL` (default info).

## Undo everything

```
claude-proxy claude uninstall
claude-proxy service uninstall
```
