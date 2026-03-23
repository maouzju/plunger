# Plunger

English | [中文](README.md)

Zero-config local resilient proxy that provides automatic disconnection recovery for streaming API calls from Claude Code / Codex CLI / Cursor. Especially useful when accessing APIs through third-party relay services prone to frequent disconnections.

## What It Does

Plunger 🪠 sits between your AI client and the Anthropic API. When a streaming response drops mid-way — due to network hiccups, server errors, or stalls — it automatically retries the request with accumulated context, so you don't lose progress.

**Supported Clients:**

| Client | Hijack Method | Status |
|---|---|---|
| Claude Code | Rewrites `ANTHROPIC_BASE_URL` in `~/.claude/settings.json` | Verified |
| Codex CLI | Rewrites base URL in `~/.codex/config.toml` | Verified |
| Cursor | Proxies via `/v1/chat/completions` endpoint | Not fully tested |

**How It Works:**

1. On startup, reads `~/.claude/settings.json` and `~/.codex/config.toml` to capture the current upstream URL
2. Rewrites configs to route traffic through the local proxy
3. Monitors streams for disconnections, stalls, and errors
4. On failure, injects the partial response as an assistant prefill and retries
5. On shutdown, restores original settings (fail-open design)

## Features

- **Automatic stream recovery** — detects mid-stream disconnections and resumes with accumulated context
- **Stall detection** — configurable first-byte timeout, inter-chunk stall timeout, and visible output timeout
- **SSE heartbeat** — periodic heartbeat frames to keep connections alive
- **Exponential backoff** — retries with jitter (1s base, 1.5x factor, 30s cap)
- **Fail-open watchdog** — separate process monitors proxy health and restores settings if the proxy dies
- **Multi-client support** — hijacks both Claude Code and Codex CLI configs, compatible with Cursor and other OpenAI-compatible clients
- **Provider switching** — detects CC Switch provider changes without restart
- **Desktop UI** — tkinter-based control panel for real-time monitoring and configuration
- **Minimal dependencies** — only requires `aiohttp`

## Supported Endpoints

| Endpoint | Description |
|---|---|
| `POST /v1/messages` | Anthropic Messages API (streaming & non-streaming) |
| `POST /v1/responses` | Responses API |
| `POST /v1/chat/completions` | OpenAI-compatible chat completions |
| `GET /health` | Health check |

## Installation

### Windows Users

Download the Windows release archive, extract it, and double-click `Plunger.exe` — no Python installation required:

👉 [**Download the Windows release**](https://github.com/maouzju/plunger/releases/latest)

If Windows SmartScreen blocks the extracted files on first launch, that is the usual trust warning for unsigned internet downloads. You can either click `More info` -> `Run anyway`, or remove the download marker in PowerShell:

```powershell
Get-ChildItem "$env:USERPROFILE\Downloads\Plunger\*" -Recurse | Unblock-File -ErrorAction SilentlyContinue
```

### From Source

Requires **Python 3.10+**.

```bash
git clone https://github.com/maouzju/plunger.git
cd plunger
pip install .
```

## Usage

### Desktop UI (Recommended)

Double-click **Plunger.exe** or run:

```bash
python run.py
```

The desktop control panel provides:

- Real-time proxy status and connection count
- Recovery event history
- Adjustable timeout and retry parameters
- One-click start/stop

![Plunger UI](plunger_preview.png)

The proxy listens on `http://127.0.0.1:8462` by default, automatically hijacks Claude and Codex settings, and restores them on exit.

### Headless Mode

```bash
python run.py --headless
```

### CLI Options

```
-p, --port              Listen port (default: 8462)
-t, --timeout           Stall timeout in seconds (default: 60)
-r, --retries           Max retries, -1 for unlimited (default: -1)
-u, --upstream          Manual upstream URL override
--max-body-mb           Max request body size in MiB (default: 32)
--safe-resume-body-mb   Max body size for resume/prefill in MiB (default: 19)
--watch-interval        Settings poll interval in seconds (default: 1)
```

## How Recovery Works

```
Client ──► Plunger ──► Anthropic API / OpenAI-compatible API
              │
              ├─ Stream starts normally
              ├─ Disconnection detected (timeout/error/stall)
              ├─ Partial text saved
              ├─ Partial text injected as assistant prefill in retry request
              └─ Stream continues from where it left off
```

The proxy tracks partial responses and, on retry, injects the accumulated text as an assistant message prefill — telling the API to continue from where it stopped rather than starting over.

## Known Limitations

- **Not compatible with CC Switch failover** — Plunger works by hijacking `ANTHROPIC_BASE_URL`. CC Switch's automatic failover attempts to switch the upstream URL, which conflicts with Plunger. Manual provider switching via CC Switch is supported, but automatic failover is not yet compatible.

## Configuration Files

| Path | Purpose |
|---|---|
| `~/.claude/settings.json` | Hijacked to redirect to local proxy |
| `~/.codex/config.toml` | Codex CLI config, hijacked |
| `~/.claude/plunger/` | Recovery data directory |
| `~/.claude/plunger/events.json` | Recovery event history |
| `~/.claude/plunger/service.log` | Service logs |

## License

[MIT](LICENSE)
