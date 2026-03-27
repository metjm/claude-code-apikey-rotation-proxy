# claude-proxy

A pass-through proxy for the Anthropic API that rotates multiple API keys. When a key gets rate-limited, it switches to the next one. Keys that hit a rate limit are shelved until their cooldown expires so they aren't retried needlessly. Per-key and per-user stats (requests, errors, rate limit hits, token usage) are persisted to SQLite.

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

## Multi-user access

Proxy tokens let you share a single proxy with multiple users. Each user gets their own token and their usage is tracked independently. When no proxy tokens are configured, the proxy is open (no auth required).

Add a proxy token for each user:

```bash
curl -X POST http://localhost:4080/admin/tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"token": "alice-secret-token", "label": "alice"}'
```

Each user sets their Claude Code API key to their proxy token:

```bash
ANTHROPIC_API_KEY=alice-secret-token claude
```

The proxy authenticates the user via the `x-api-key` or `Authorization: Bearer` header (the same headers Claude Code already sends), then replaces it with a real API key before forwarding to Anthropic. The user's proxy token never reaches the upstream API.

Manage tokens via the admin API:

```
GET    /admin/tokens          List all tokens (masked) with per-user stats
POST   /admin/tokens          Add token: {"token": "...", "label": "..."}
POST   /admin/tokens/remove   Remove token: {"token": "..."}
```

## Dashboard

A live web dashboard is available at `http://localhost:4080/dashboard`. It shows:

- Real-time key status with availability badges and rate-limit countdowns
- Per-key usage bars, request counts, and token throughput
- Per-user stats for all proxy tokens
- Live activity log of all proxy events (requests, responses, errors, rate limits)
- Forms to add/remove API keys and proxy tokens

The dashboard requires the admin token to access (prompted on load). If no `ADMIN_TOKEN` is configured on the server, the dashboard is accessible without authentication.

## Watch TUI

A terminal-based live monitor is also available:

```
claude-proxy-ctl watch
```

Shows the same real-time information as the dashboard in a terminal UI with color-coded key status, usage bars, and a scrolling activity log.

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
claude-proxy-ctl watch                Live terminal dashboard
```

## Admin API

All admin endpoints (except `/admin/health` and `/admin/events`) require `Authorization: Bearer <ADMIN_TOKEN>` when `ADMIN_TOKEN` is set.

```
GET    /admin/keys             List all API keys (masked) with stats
POST   /admin/keys             Add key: {"key": "sk-ant-...", "label": "..."}
POST   /admin/keys/remove      Remove key: {"key": "sk-ant-..."}
GET    /admin/tokens           List all proxy tokens (masked) with stats
POST   /admin/tokens           Add token: {"token": "...", "label": "..."}
POST   /admin/tokens/remove    Remove token: {"token": "..."}
GET    /admin/stats            Aggregated stats across all keys
GET    /admin/health           Health check (no auth required)
GET    /admin/events           SSE event stream (no auth required)
```

## Configuration

All configuration is via environment variables:

```
PORT              Listen port                          default: 4080
UPSTREAM_URL      Anthropic API URL                    default: https://api.anthropic.com
ADMIN_TOKEN       Bearer token for /admin/* endpoints  optional
DATA_DIR          Directory for the SQLite database    default: ./data
DB_PATH           Full path to SQLite database file    overrides DATA_DIR/state.db
MAX_RETRIES       Key rotation attempts per request    default: 10
LOG_LEVEL         debug | info | warn | error          default: info
```

## Storage

State is persisted in a SQLite database (`state.db`) using Bun's built-in `bun:sqlite` with WAL mode. The database contains two tables: `api_keys` and `proxy_tokens`, storing credentials and cumulative usage stats.

If a legacy `state.json` file exists from a previous version, it is automatically migrated to SQLite on first startup and then deleted.

Stats are updated in memory on every request and flushed to the database every second (debounced). CRUD operations (add/remove key/token) write to the database immediately. On shutdown (SIGTERM/SIGINT), any pending stats are flushed before exit.

## Testing

```
bun test
```

415 integration tests covering types, configuration, events, key management, proxy request handling, admin API endpoints, and full end-to-end server flows.

## Undo everything

```
claude-proxy claude uninstall
claude-proxy service uninstall
```
