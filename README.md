# claude-proxy

A pass-through proxy for the Anthropic API that rotates multiple API keys. When a key gets rate-limited, it switches to the next one. Keys that hit a rate limit are shelved until their cooldown expires so they aren't retried needlessly. Per-key stats (requests, errors, rate limit hits, token usage) are persisted to disk.

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

```
claude-proxy                          Start the server directly
claude-proxy service install          Run as a background service
claude-proxy service uninstall        Stop and remove the service
claude-proxy service status           Check if the service is running
claude-proxy claude install           Add proxy to Claude Code settings
claude-proxy claude uninstall         Remove proxy from Claude Code settings
claude-proxy claude status            Check Claude Code configuration
claude-proxy-ctl add <key> [label]    Register an API key
claude-proxy-ctl remove <key>         Remove an API key
claude-proxy-ctl list                 Show all keys with stats
claude-proxy-ctl stats                Aggregate stats across all keys
```

## Configuration

```
PORT              Listen port                          default: 4080
UPSTREAM_URL      Anthropic API URL                    default: https://api.anthropic.com
ADMIN_TOKEN       Bearer token for /admin/* endpoints  optional
DATA_DIR          Where to store key state             default: ./data
MAX_RETRIES       Key rotation attempts per request    default: 10
LOG_LEVEL         debug | info | warn | error          default: info
```

## Undo everything

```
claude-proxy claude uninstall
claude-proxy service uninstall
```
