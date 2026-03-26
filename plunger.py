#!/usr/bin/env python3
"""
Zero-config resilient proxy for Claude-compatible streaming APIs.

Behavior:
- Reads ~/.claude/settings.json on startup.
- Captures the current ANTHROPIC_BASE_URL as the real upstream.
- Rewrites settings.json so Claude talks to this local proxy instead.
- Polls settings.json for external changes (for example CC Switch provider changes)
  and re-applies the local proxy automatically.
- Restores the latest real upstream on shutdown.
"""

from __future__ import annotations

import argparse
import asyncio
import atexit
import json
import logging
import os
import random
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.request import ProxyHandler, Request, build_opener

from plunger_shared import CLAUDE_DIR, DEFAULT_PORT, DEFAULT_STALL_TIMEOUT, LOCAL_HOST, RECOVERY_DIR

try:
    import aiohttp
    from aiohttp import web
except ImportError:
    print("Installing missing dependency: aiohttp")
    os.system(f"{sys.executable} -m pip install aiohttp")
    import aiohttp
    from aiohttp import web


def _is_frozen_app() -> bool:
    return bool(getattr(sys, "frozen", False))


def _self_launch_command(*extra_args: str) -> list[str]:
    if _is_frozen_app():
        return [sys.executable, "--headless", *extra_args]
    return [sys.executable, str(Path(__file__).resolve()), *extra_args]


def _format_retry_policy(max_retries: int) -> str:
    return "unlimited" if max_retries < 0 else str(max_retries)


def _retry_delay(attempt: int) -> float:
    """Exponential backoff with jitter: 1s, 1.5s, 2.25s, ... capped at 30s, ±20% jitter."""
    delay = RECOVERY_RETRY_DELAY * (RECOVERY_RETRY_BACKOFF_FACTOR ** min(attempt, 20))
    delay = min(delay, RECOVERY_RETRY_DELAY_MAX)
    jitter = delay * random.uniform(-0.2, 0.2)
    return delay + jitter


def _is_prefill_rejection(exc: Exception) -> bool:
    """Check if an upstream error is about assistant message prefill not being supported."""
    msg = str(exc).lower()
    if "prefill" in msg or "must end with a user message" in msg:
        return True
    if "latest assistant message cannot be modified" in msg and (
        "thinking" in msg or "redacted_thinking" in msg
    ):
        return True
    if "must remain as they were in the original response" in msg and (
        "thinking" in msg or "redacted_thinking" in msg
    ):
        return True
    return False

DEFAULT_TOOL_WAIT_TIMEOUT = 30.0
DEFAULT_MAX_REQUEST_BODY_MB = 32.0
DEFAULT_SAFE_RESUME_BODY_MB = 19.0
CC_SWITCH_PROXY_PORT = 15721
CC_SWITCH_PROXY_URL = f"http://{LOCAL_HOST}:{CC_SWITCH_PROXY_PORT}"
RECOVERY_RETRY_DELAY = 1.0
RECOVERY_RETRY_DELAY_MAX = 30.0
RECOVERY_RETRY_BACKOFF_FACTOR = 1.5
MIN_FIRST_BYTE_TIMEOUT = 90.0
DEFAULT_VISIBLE_OUTPUT_TIMEOUT = 60.0
MIN_RESPONSES_TAIL_FETCH_TIMEOUT = 12.0
MAX_RESPONSES_TAIL_FETCH_TIMEOUT = 30.0
MIN_HEARTBEAT_INTERVAL = 1.5
MAX_HEARTBEAT_INTERVAL = 3.0
SSE_HEARTBEAT_FRAME = b": keep-alive\n\n"
ANTHROPIC_PING_FRAME = b'event: ping\ndata: {"type":"ping"}\n\n'
WATCHDOG_POLL_INTERVAL = 1.0
WATCHDOG_STARTUP_GRACE = 20.0
WATCHDOG_FAILURE_GRACE = 12.0

CLAUDE_SETTINGS = CLAUDE_DIR / "settings.json"
STATE_FILE = CLAUDE_DIR / ".plunger_state.json"
LAST_INTERRUPTED_FILE = RECOVERY_DIR / "last_interrupted.json"
ACTIVE_SESSIONS_FILE = RECOVERY_DIR / "active_sessions.json"
EVENT_HISTORY_FILE = RECOVERY_DIR / "events.json"
WATCHDOG_LOG_FILE = RECOVERY_DIR / "watchdog.log"
SERVICE_LOG_FILE = RECOVERY_DIR / "service.log"
CC_SWITCH_SETTINGS = Path.home() / ".cc-switch" / "settings.json"
CODEX_DIR = Path.home() / ".codex"
CODEX_CONFIG = CODEX_DIR / "config.toml"
CODEX_STATE_FILE = CODEX_DIR / ".plunger_state.json"


def _build_log_handlers() -> tuple[list[logging.Handler], bool]:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    try:
        handlers.insert(0, logging.FileHandler(SERVICE_LOG_FILE, encoding="utf-8"))
        return handlers, True
    except OSError:
        return handlers, False


RECOVERY_DIR.mkdir(parents=True, exist_ok=True)
LOG_HANDLERS, FILE_LOGGING_ENABLED = _build_log_handlers()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=LOG_HANDLERS,
)
log = logging.getLogger("resilient-proxy")
if not FILE_LOGGING_ENABLED:
    log.warning("Service log file unavailable; continuing with console logging only")
LOCAL_OPENER = build_opener(ProxyHandler({}))

CONTINUE_PATTERNS = (
    "continue",
    "go on",
    "keep going",
    "carry on",
    "resume",
    "pick up where you left off",
    "继续",
    "接着",
    "接着做",
    "继续做",
    "继续刚才",
    "继续上次",
    "往下",
    "往后",
)


REQUEST_TITLE_HEADER_NAMES = (
    "x-claude-conversation-title",
    "x-conversation-title",
    "x-session-title",
    "x-chat-title",
    "x-request-title",
    "conversation-title",
    "session-title",
    "chat-title",
)

REQUEST_TITLE_BODY_FIELDS = (
    "title",
    "conversation_title",
    "conversationTitle",
    "session_title",
    "sessionTitle",
    "chat_title",
    "chatTitle",
    "request_title",
    "requestTitle",
)

REQUEST_TITLE_BODY_CONTAINERS = (
    "metadata",
    "meta",
    "client_metadata",
    "conversation",
    "session",
    "chat",
)

REQUEST_TITLE_NOISE_PREFIXES = (
    "<system-reminder",
    "</system-reminder",
    "system-reminder>",
    "<environment_context",
    "</environment_context",
    "environment_context>",
    "contents of ",
    "gitstatus:",
    "git status:",
    "recent commits:",
    "recent changes:",
    "filesystem sandboxing",
    "approval policy",
    "when using search",
    "when searching",
    "you are codex",
    "you are claude",
    "the user is in",
)

REQUEST_TITLE_NOISE_TAGS = (
    "system-reminder",
    "environment_context",
    "collaboration_mode",
    "skills_instructions",
    "cwd",
    "shell",
    "current_date",
    "timezone",
)


def _strip_request_title_noise_blocks(text: Any) -> str:
    cleaned = str(text or "")
    for tag in REQUEST_TITLE_NOISE_TAGS:
        cleaned = re.sub(
            fr"<{re.escape(tag)}\b[^>]*>.*?</{re.escape(tag)}>",
            "\n",
            cleaned,
            flags=re.IGNORECASE | re.DOTALL,
        )
    cleaned = re.sub(r"```.*?```", "\n", cleaned, flags=re.DOTALL)
    return cleaned


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        prefix=f"{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
        os.replace(temp_path, path)
    except Exception:
        with suppress(OSError):
            os.unlink(temp_path)
        raise


def _extract_base_url(settings: dict[str, Any]) -> str:
    env = settings.get("env", {})
    value = str(env.get("ANTHROPIC_BASE_URL", "")).strip()
    return value.rstrip("/")


def _extract_auth_token(settings: dict[str, Any]) -> str:
    env = settings.get("env", {})
    return str(env.get("ANTHROPIC_AUTH_TOKEN", "")).strip()


def _is_cc_switch_proxy_url(base_url: str) -> bool:
    return str(base_url or "").strip().rstrip("/") == CC_SWITCH_PROXY_URL


def _shutdown_skips_restore(mode: str) -> bool:
    return str(mode or "").strip().lower() in {"takeover", "restart"}


def _with_base_url(settings: dict[str, Any], base_url: str) -> dict[str, Any]:
    updated = deepcopy(settings)
    env = updated.setdefault("env", {})
    env["ANTHROPIC_BASE_URL"] = base_url
    return updated


def _with_restore_env(
    settings: dict[str, Any],
    *,
    base_url: str,
    auth_token: str | None = None,
) -> dict[str, Any]:
    updated = _with_base_url(settings, base_url)
    if auth_token is not None:
        env = updated.setdefault("env", {})
        env["ANTHROPIC_AUTH_TOKEN"] = auth_token
    return updated


_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


def _build_forward_headers(req: web.Request) -> dict[str, str]:
    headers: dict[str, str] = {}
    for header_name, value in req.headers.items():
        if header_name.lower() in _HOP_BY_HOP_HEADERS:
            continue
        headers[header_name] = value
    headers.setdefault("Content-Type", "application/json")
    return headers


def _mb_to_bytes(value_mb: float) -> int:
    return max(1, int(value_mb * 1024 * 1024))


def _format_bytes(num_bytes: int) -> str:
    units = ("B", "KiB", "MiB", "GiB")
    value = float(max(0, num_bytes))
    unit_index = 0
    while value >= 1024.0 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"
    return f"{value:.1f} {units[unit_index]}"


def _json_body_size_bytes(payload: Any) -> int:
    return len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))


@web.middleware
async def proxy_error_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    try:
        return await handler(request)
    except web.HTTPRequestEntityTooLarge:
        max_size = int(getattr(request.app, "_client_max_size", 0) or 0)
        message = "Request body is too large for the local proxy."
        if max_size > 0:
            message += f" Current proxy limit: {_format_bytes(max_size)}."
        message += " Reduce the request size or raise --max-body-mb."
        log.warning("   REQUEST TOO LARGE | %s | limit=%s", request.path, _format_bytes(max_size))
        return web.json_response(
            {"error": {"type": "proxy_error", "message": message}},
            status=413,
        )


def _local_port_is_open(port: int) -> bool:
    try:
        with socket.create_connection((LOCAL_HOST, port), timeout=0.2):
            return True
    except OSError:
        return False


def _timestamp_now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _shorten_text(value: Any, limit: int = 88) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3].rstrip()}..."


def _has_non_whitespace_text(value: Any) -> bool:
    return any(not char.isspace() for char in str(value or ""))


def _extract_generic_request_text(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""

    if "messages" in payload:
        text = _extract_last_messages_user_text(payload.get("messages"))
        if text:
            return text

    if "input" in payload:
        text = _extract_last_responses_user_text(payload.get("input"))
        if text:
            return text

    for field_name in (
        "prompt",
        "query",
        "question",
        "instruction",
        "instructions",
        "text",
        "message",
        "content",
    ):
        if field_name not in payload:
            continue
        text = _extract_text_from_content(payload.get(field_name))
        if text:
            return text

    return ""


def _copy_response_headers(headers: Any, *, streaming: bool = False) -> dict[str, str]:
    copied: dict[str, str] = {}
    for header_name, value in getattr(headers, "items", lambda: [])():
        lowered = str(header_name).lower()
        if lowered in _HOP_BY_HOP_HEADERS:
            continue
        if lowered == "content-length" and streaming:
            continue
        copied[str(header_name)] = str(value)
    return copied


def _try_parse_json_payload(body: bytes, content_type: str = "") -> dict[str, Any] | None:
    if not body:
        return None

    normalized_type = str(content_type or "").lower()
    sample = body.lstrip()[:1]
    if "json" not in normalized_type and sample not in {b"{", b"["}:
        return None

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return None

    return payload if isinstance(payload, dict) else None


def _request_wants_stream(headers: Any, body: Any) -> bool:
    accept = ""
    if hasattr(headers, "get"):
        accept = str(headers.get("Accept", "") or headers.get("accept", "")).lower()

    if "text/event-stream" in accept:
        return True

    if isinstance(body, dict):
        return bool(body.get("stream"))

    return False


def _response_should_stream(response: aiohttp.ClientResponse) -> bool:
    content_type = str(response.headers.get("Content-Type", "")).lower()
    if "text/event-stream" in content_type:
        return True

    transfer_encoding = str(response.headers.get("Transfer-Encoding", "")).lower()
    if "chunked" in transfer_encoding:
        return True

    return bool(response.chunked)


def _is_textual_content_type(content_type: str) -> bool:
    normalized = str(content_type or "").lower()
    return (
        normalized.startswith("text/")
        or "json" in normalized
        or "javascript" in normalized
        or "xml" in normalized
        or "event-stream" in normalized
    )


def _append_preview_text(state: "StreamState", chunk: bytes, content_type: str, *, limit: int = 240) -> None:
    state.bytes_forwarded += len(chunk)
    if len(state.accumulated_text) >= limit or not _is_textual_content_type(content_type):
        return

    text = chunk.decode("utf-8", errors="replace")
    if not text:
        return

    remaining = limit - len(state.accumulated_text)
    state.accumulated_text += text[:remaining]


def _summarize_upstream_error(status: int, text: str) -> str:
    try:
        payload = json.loads(text)
    except Exception:
        payload = None

    if isinstance(payload, dict):
        error = payload.get("error", {})
        if isinstance(error, dict):
            error_type = str(error.get("type", "")).strip()
            message = str(error.get("message", "")).strip()
            request_id = str(payload.get("request_id", "") or error.get("request_id", "")).strip()
            parts = [part for part in (error_type, message) if part]
            if parts:
                summary = f"HTTP {status}: {' | '.join(parts)}"
                if request_id:
                    summary += f" (request_id: {request_id})"
                return summary

    return f"HTTP {status}: {text[:300]}"


def _append_watchdog_log(message: str) -> None:
    try:
        WATCHDOG_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with WATCHDOG_LOG_FILE.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(f"[{_timestamp_now()}] {message}\n")
    except Exception:
        pass


def _terminate_pid_best_effort(pid: int | None) -> None:
    """End a child process without raising (used for safety watchdog teardown)."""
    if pid is None or pid <= 0:
        return

    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            PROCESS_TERMINATE = 0x0001
            handle = kernel32.OpenProcess(PROCESS_TERMINATE, False, pid)
            if handle:
                kernel32.TerminateProcess(handle, 1)
                kernel32.CloseHandle(handle)
        except Exception:
            pass
        return

    with suppress(Exception):
        os.kill(pid, signal.SIGTERM)


def _process_exists(pid: int) -> bool:
    if pid <= 0:
        return False

    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000, False, pid)
            if not handle:
                return False
            kernel32.CloseHandle(handle)
            return True
        except Exception:
            pass

    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _proxy_health_is_ok(listen_port: int) -> bool:
    try:
        # Bypass system proxies so watchdog health checks hit the local service directly.
        with LOCAL_OPENER.open(f"http://{LOCAL_HOST}:{listen_port}/health", timeout=1.5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return False
    return payload.get("status") == "ok"


def _request_local_json(
    listen_port: int,
    path: str,
    *,
    method: str = "GET",
    timeout: float = 1.2,
) -> dict[str, Any] | None:
    request = Request(f"http://{LOCAL_HOST}:{listen_port}{path}", method=method)
    with LOCAL_OPENER.open(request, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    return data if isinstance(data, dict) else None


def _read_proxy_state_for_port(listen_port: int) -> dict[str, Any] | None:
    if not STATE_FILE.exists():
        return None
    try:
        state = _read_json(STATE_FILE)
    except Exception:
        return None
    if not isinstance(state, dict):
        return None

    expected_proxy_url = f"http://{LOCAL_HOST}:{listen_port}"
    state_proxy_url = str(state.get("proxy_url", "")).strip().rstrip("/")
    if state_proxy_url != expected_proxy_url:
        return None
    return state


def _looks_like_existing_proxy_instance(
    listen_port: int,
    health: dict[str, Any] | None,
    state: dict[str, Any] | None,
) -> bool:
    if isinstance(health, dict):
        if str(health.get("status", "")).strip().lower() == "ok":
            listen = str(health.get("listen", "")).strip().rstrip("/")
            expected = f"http://{LOCAL_HOST}:{listen_port}"
            has_proxy_shape = any(
                key in health
                for key in ("listen", "upstream", "started_at", "active_sessions", "events")
            )
            if has_proxy_shape and (not listen or listen == expected):
                return True

    if not isinstance(state, dict):
        return False

    try:
        pid = int(state.get("pid") or 0)
    except Exception:
        pid = 0
    return pid > 0 and _process_exists(pid)


def _wait_for_local_port_release(listen_port: int, timeout: float) -> bool:
    deadline = time.monotonic() + max(0.0, timeout)
    while time.monotonic() < deadline:
        if not _local_port_is_open(listen_port):
            return True
        time.sleep(0.15)
    return not _local_port_is_open(listen_port)


def _take_over_existing_proxy_instance(listen_port: int) -> None:
    health = None
    with suppress(Exception):
        health = _request_local_json(listen_port, "/health", timeout=0.8)

    state = _read_proxy_state_for_port(listen_port)
    if not _looks_like_existing_proxy_instance(listen_port, health, state):
        return

    proxy_url = f"http://{LOCAL_HOST}:{listen_port}"
    started_at = str((health or {}).get("started_at", "")).strip()

    try:
        previous_pid = int((state or {}).get("pid") or 0)
    except Exception:
        previous_pid = 0

    try:
        previous_watchdog_pid = int((health or {}).get("safety_watchdog_pid") or 0)
    except Exception:
        previous_watchdog_pid = 0

    log.info(
        "Detected existing proxy on %s (pid=%s, started_at=%s); taking over",
        proxy_url,
        previous_pid or "<unknown>",
        started_at or "<unknown>",
    )

    with suppress(Exception):
        _request_local_json(
            listen_port,
            "/control/shutdown?mode=takeover",
            method="POST",
            timeout=1.2,
        )

    if _wait_for_local_port_release(listen_port, timeout=8.0):
        log.info("Previous proxy on %s stopped cleanly", proxy_url)
        return

    if previous_pid > 0 and _process_exists(previous_pid):
        log.warning(
            "Previous proxy on %s did not stop in time; terminating pid %s",
            proxy_url,
            previous_pid,
        )
        _terminate_pid_best_effort(previous_pid)

    if previous_watchdog_pid > 0 and _process_exists(previous_watchdog_pid):
        _terminate_pid_best_effort(previous_watchdog_pid)

    if _wait_for_local_port_release(listen_port, timeout=3.0):
        log.info("Previous proxy on %s was force-terminated for takeover", proxy_url)
        return

    log.warning(
        "Port %s is still busy after takeover attempt; startup may fail if another service owns it",
        proxy_url,
    )



def _extract_codex_base_url_from_config(config_text: str) -> str:
    match = re.search(r'(?m)^\s*base_url\s*=\s*"([^"]+)"\s*$', str(config_text or ""))
    if not match:
        return ""
    return match.group(1).strip().rstrip("/")




def _restore_fail_open_settings(listen_port: int) -> tuple[bool, bool]:
    restored_claude = False
    restored_codex = False

    try:
        settings_manager = SettingsManager(listen_port)
        state = settings_manager._read_state()
        if state:
            settings_manager.current_upstream = str(state.get("upstream_base_url", "")).strip().rstrip("/")
            settings_manager.restore_upstream = str(
                state.get("restore_base_url", settings_manager.current_upstream)
            ).strip().rstrip("/")
            settings_manager.restore_auth_token = str(state.get("restore_auth_token", "")).strip()
            settings_manager._hijacked = True
            before = ""
            if CLAUDE_SETTINGS.exists():
                before = _extract_base_url(_read_json(CLAUDE_SETTINGS)).rstrip("/")
            settings_manager.restore()
            if before == settings_manager.proxy_url:
                restored_claude = True
    except Exception as exc:
        _append_watchdog_log(f"failed to restore Claude settings: {exc}")

    try:
        codex_manager = CodexConfigManager(listen_port)
        state = codex_manager._read_state()
        if state:
            codex_manager.provider_name = str(state.get("provider_name", "")).strip()
            codex_manager.original_base_url = str(state.get("original_base_url", "")).strip().rstrip("/")
            codex_manager._hijacked = True
            before = ""
            if CODEX_CONFIG.exists():
                with suppress(Exception):
                    text = CODEX_CONFIG.read_text(encoding="utf-8")
                    before, _ = codex_manager._extract_provider_base_url(text, codex_manager.provider_name)
                    before = before.rstrip("/")
            codex_manager.restore()
            if before == codex_manager.proxy_base_url.rstrip("/"):
                restored_codex = True
    except Exception as exc:
        _append_watchdog_log(f"failed to restore Codex config: {exc}")

    return restored_claude, restored_codex


def _is_client_disconnect_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return isinstance(exc, (BrokenPipeError, ConnectionResetError)) or any(
        fragment in text
        for fragment in (
            "closing transport",
            "cannot write to closing transport",
            "connection reset by peer",
            "broken pipe",
            "client disconnected",
        )
    )


def _looks_like_downstream_write_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return isinstance(exc, BrokenPipeError) or any(
        fragment in text
        for fragment in (
            "closing transport",
            "cannot write to closing transport",
            "broken pipe",
            "client disconnected",
        )
    )


def _request_transport_is_closing(req: web.Request) -> bool:
    transport = req.transport
    return transport is None or transport.is_closing()


def _is_downstream_disconnect(
    req: web.Request,
    exc: BaseException,
    stream: "SSEStreamWriter | None" = None,
) -> bool:
    if stream is not None and stream.is_disconnected():
        return True
    if _looks_like_downstream_write_error(exc):
        return True
    return isinstance(exc, ConnectionResetError) and _request_transport_is_closing(req)


def _endpoint_label(endpoint: str) -> str:
    return endpoint.replace("/v1/", "").strip("/") or "unknown"


def _extract_text_from_content(content: Any) -> str:
    if isinstance(content, str):
        return content

    if isinstance(content, dict):
        if isinstance(content.get("text"), str):
            return content["text"]
        if "content" in content:
            return _extract_text_from_content(content.get("content"))
        return ""

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            text = _extract_text_from_content(item)
            if text:
                parts.append(text)
        return "".join(parts)

    return ""


def _append_text_to_content(content: Any, suffix: str) -> Any:
    if not suffix:
        return deepcopy(content)

    if isinstance(content, str):
        return f"{content}{suffix}"

    if isinstance(content, dict):
        patched = deepcopy(content)
        if isinstance(patched.get("text"), str):
            patched["text"] = f"{patched['text']}{suffix}"
            return patched
        if "content" in patched:
            patched["content"] = _append_text_to_content(patched.get("content"), suffix)
            return patched
        patched["content"] = suffix
        return patched

    if isinstance(content, list):
        patched = deepcopy(content)
        if patched and isinstance(patched[-1], dict):
            last_block = dict(patched[-1])
            if isinstance(last_block.get("text"), str):
                last_block["text"] = f"{last_block['text']}{suffix}"
                patched[-1] = last_block
                return patched
        patched.append({"type": "text", "text": suffix})
        return patched

    return suffix


_DROP_EMPTY_BLOCK = object()


def _strip_empty_text_blocks(value: Any) -> tuple[Any, int]:
    removed = 0

    def _walk(node: Any) -> Any:
        nonlocal removed

        if isinstance(node, list):
            cleaned_items: list[Any] = []
            for item in node:
                cleaned = _walk(item)
                if cleaned is _DROP_EMPTY_BLOCK:
                    continue
                cleaned_items.append(cleaned)
            return cleaned_items

        if isinstance(node, dict):
            block_type = str(node.get("type", "")).strip().lower()
            if block_type in {"text", "input_text"}:
                text = node.get("text", "")
                if isinstance(text, str) and not text.strip():
                    removed += 1
                    return _DROP_EMPTY_BLOCK

            cleaned_dict: dict[str, Any] = {}
            for key, item in node.items():
                if isinstance(item, (list, dict)):
                    cleaned = _walk(item)
                    if cleaned is _DROP_EMPTY_BLOCK:
                        continue
                    cleaned_dict[key] = cleaned
                else:
                    cleaned_dict[key] = item
            return cleaned_dict

        return node

    cleaned_value = _walk(value)
    if cleaned_value is _DROP_EMPTY_BLOCK:
        return None, removed
    return cleaned_value, removed


def _extract_last_messages_user_text(messages: Any) -> str:
    if not isinstance(messages, list):
        return ""

    for message in reversed(messages):
        if not isinstance(message, dict):
            continue
        if message.get("role") != "user":
            continue
        return _extract_text_from_content(message.get("content"))

    return ""


def _extract_first_messages_user_text(messages: Any) -> str:
    if not isinstance(messages, list):
        return ""

    for message in messages:
        if not isinstance(message, dict):
            continue
        if message.get("role") != "user":
            continue
        return _extract_text_from_content(message.get("content"))

    return ""


def _extract_last_responses_user_text(input_value: Any) -> str:
    if isinstance(input_value, str):
        return input_value

    if isinstance(input_value, dict):
        role = input_value.get("role")
        if role == "user":
            return _extract_text_from_content(input_value.get("content"))
        if input_value.get("type") in {"input_text", "text"}:
            return str(input_value.get("text", ""))
        return _extract_text_from_content(input_value.get("content"))

    if isinstance(input_value, list):
        for item in reversed(input_value):
            text = _extract_last_responses_user_text(item)
            if text:
                return text

    return ""


def _extract_first_responses_user_text(input_value: Any) -> str:
    if isinstance(input_value, str):
        return input_value

    if isinstance(input_value, dict):
        role = input_value.get("role")
        if role == "user":
            return _extract_text_from_content(input_value.get("content"))
        if input_value.get("type") in {"input_text", "text"}:
            return str(input_value.get("text", ""))
        return _extract_text_from_content(input_value.get("content"))

    if isinstance(input_value, list):
        for item in input_value:
            text = _extract_first_responses_user_text(item)
            if text:
                return text

    return ""


def _clean_request_title(text: Any, limit: int = 80) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    normalized = normalized.strip("`'\"[]{}<>|")
    normalized = normalized.strip()
    if not normalized:
        return ""
    return normalized[:limit].rstrip(" -:|,.;/")


def _looks_like_noise_request_title(text: Any) -> bool:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if not normalized:
        return True

    lowered = normalized.lower()
    if any(lowered.startswith(prefix) for prefix in REQUEST_TITLE_NOISE_PREFIXES):
        return True
    if re.fullmatch(r"</?[^>]+>", normalized):
        return True
    if "```" in normalized:
        return True

    visible_chars = len(re.findall(r"[0-9A-Za-z\u4e00-\u9fff]", normalized))
    if visible_chars < 2:
        return True

    noisy_punctuation = len(re.findall(r"[<>{}\[\]|`]", normalized))
    if noisy_punctuation >= max(6, visible_chars):
        return True

    return False


def _extract_explicit_request_title(headers: Any, body: Any) -> str:
    if hasattr(headers, "items"):
        lowered_headers = {str(key).lower(): value for key, value in headers.items()}
    else:
        lowered_headers = {}

    for header_name in REQUEST_TITLE_HEADER_NAMES:
        candidate = _clean_request_title(lowered_headers.get(header_name, ""))
        if candidate and not _looks_like_noise_request_title(candidate):
            return candidate

    if not isinstance(body, dict):
        return ""

    for field_name in REQUEST_TITLE_BODY_FIELDS:
        candidate = _clean_request_title(body.get(field_name, ""))
        if candidate and not _looks_like_noise_request_title(candidate):
            return candidate

    for container_name in REQUEST_TITLE_BODY_CONTAINERS:
        container = body.get(container_name)
        if not isinstance(container, dict):
            continue
        for field_name in REQUEST_TITLE_BODY_FIELDS:
            candidate = _clean_request_title(container.get(field_name, ""))
            if candidate and not _looks_like_noise_request_title(candidate):
                return candidate

    return ""


def _score_request_title_candidate(text: str) -> int:
    score = 0
    length = len(text)
    if 6 <= length <= 60:
        score += 4
    elif 3 <= length <= 96:
        score += 3
    elif length <= 140:
        score += 1
    else:
        score -= 2

    if text.endswith((":", "：", "-", "|")):
        score -= 1
    if "?" in text or "？" in text:
        score += 1
    if re.search(r"[A-Za-z\u4e00-\u9fff]", text):
        score += 1

    return score


def _iter_request_title_candidates(text: Any) -> list[str]:
    raw_text = str(text or "")
    if not raw_text.strip():
        return []

    cleaned = _strip_request_title_noise_blocks(raw_text)

    candidates: list[str] = []
    seen: set[str] = set()
    for raw_line in re.split(r"[\r\n]+", cleaned):
        normalized_line = re.sub(r"\s+", " ", raw_line).strip(" \t-*>|")
        if not normalized_line:
            continue

        fragments = [normalized_line]
        if len(normalized_line) > 48:
            fragments = re.split(r" \| | (?<=[。！？.!?])\s+", normalized_line)

        for fragment in fragments:
            candidate = _clean_request_title(fragment, limit=120)
            if not candidate or candidate in seen or _looks_like_noise_request_title(candidate):
                continue
            seen.add(candidate)
            candidates.append(candidate)

    if candidates:
        return candidates

    fallback = _clean_request_title(cleaned, limit=120)
    if fallback and not _looks_like_noise_request_title(fallback):
        return [fallback]
    return []


def _build_request_title(request_title: Any, first_user_text: Any, last_user_text: Any, limit: int = 80) -> str:
    explicit_title = _clean_request_title(request_title, limit=limit)
    if explicit_title and not _looks_like_noise_request_title(explicit_title):
        return explicit_title

    best_candidate = ""
    best_score = -10_000
    for priority, text in enumerate((first_user_text, last_user_text)):
        for candidate in _iter_request_title_candidates(text):
            normalized = _clean_request_title(candidate, limit=limit)
            if not normalized or _looks_like_noise_request_title(normalized):
                continue
            score = _score_request_title_candidate(normalized) - priority
            if score > best_score:
                best_candidate = normalized
                best_score = score

    if best_candidate:
        return best_candidate

    for text in (first_user_text, last_user_text):
        fallback = _summarize_request_text(text, limit=limit)
        if fallback:
            return fallback
    return ""


def _summarize_request_text(text: Any, limit: int = 50) -> str:
    normalized = _clean_request_title(_strip_request_title_noise_blocks(text), limit=limit)
    if _looks_like_noise_request_title(normalized):
        return ""
    return normalized


def _normalize_prompt_text(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip().lower()
    return normalized.strip("`'\"。，、,.!?！？:：;；()[]{}")


def _looks_like_continue_prompt(text: str) -> bool:
    normalized = _normalize_prompt_text(text)
    if not normalized or len(normalized) > 80:
        return False
    return any(pattern in normalized for pattern in CONTINUE_PATTERNS)


def _request_client_label(req: web.Request) -> str:
    peer = req.transport.get_extra_info("peername") if req.transport else None
    if isinstance(peer, tuple) and len(peer) >= 2:
        return f"{peer[0]}:{peer[1]}"
    return ""


def _append_unique(items: list[str], value: Any) -> None:
    normalized = str(value or "").strip()
    if normalized and normalized not in items:
        items.append(normalized)


def _iter_dict_nodes(value: Any) -> Any:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_dict_nodes(child)
        return

    if isinstance(value, list):
        for child in value:
            yield from _iter_dict_nodes(child)


def _messages_body_contains_tool_result(body: Any) -> bool:
    if not isinstance(body, dict):
        return False

    for item in _iter_dict_nodes(body.get("messages")):
        block_type = str(item.get("type", "")).strip()
        if block_type == "tool_result":
            return True
        if str(item.get("tool_use_id", "")).strip():
            return True
    return False


def _messages_tool_use_blocks(message: Any) -> list[dict[str, Any]]:
    if not isinstance(message, dict) or str(message.get("role", "")).strip().lower() != "assistant":
        return []

    content = message.get("content")
    if not isinstance(content, list):
        return []

    blocks: list[dict[str, Any]] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if str(block.get("type", "")).strip() != "tool_use":
            continue
        blocks.append(block)
    return blocks


def _messages_tool_result_ids(message: Any) -> list[str]:
    if not isinstance(message, dict) or str(message.get("role", "")).strip().lower() != "user":
        return []

    tool_use_ids: list[str] = []
    for item in _iter_dict_nodes(message.get("content")):
        block_type = str(item.get("type", "")).strip()
        if block_type != "tool_result":
            continue
        _append_unique(tool_use_ids, item.get("tool_use_id"))
    return tool_use_ids


def _find_messages_dangling_tool_use(messages: Any) -> dict[str, Any] | None:
    if not isinstance(messages, list):
        return None

    for index, message in enumerate(messages):
        tool_use_blocks = _messages_tool_use_blocks(message)
        if not tool_use_blocks:
            continue

        tool_use_ids: list[str] = []
        tool_names: list[str] = []
        for block in tool_use_blocks:
            _append_unique(tool_use_ids, block.get("id"))
            _append_unique(tool_names, block.get("name") or block.get("type"))

        next_message = messages[index + 1] if index + 1 < len(messages) else None
        tool_result_ids = _messages_tool_result_ids(next_message)
        missing_ids = [tool_use_id for tool_use_id in tool_use_ids if tool_use_id not in tool_result_ids]
        if missing_ids:
            return {
                "assistant_index": index,
                "tool_use_ids": tool_use_ids,
                "missing_tool_use_ids": missing_ids,
                "tool_names": tool_names,
            }

    return None


def _remove_messages_tool_use_blocks(message: Any) -> dict[str, Any] | None:
    if not isinstance(message, dict):
        return None

    content = message.get("content")
    if not isinstance(content, list):
        return deepcopy(message)

    filtered_content = [
        deepcopy(block)
        for block in content
        if not (isinstance(block, dict) and str(block.get("type", "")).strip() == "tool_use")
    ]
    if not filtered_content:
        return None

    patched = deepcopy(message)
    patched["content"] = filtered_content
    return patched


def _is_responses_tool_call_type(item_type: str) -> bool:
    normalized = item_type.strip().lower()
    if not normalized:
        return False
    if normalized in {
        "function_call",
        "computer_call",
        "mcp_call",
        "web_search_call",
        "custom_tool_call",
    }:
        return True
    return normalized.endswith("_call")


def _is_responses_tool_result_type(item_type: str) -> bool:
    normalized = item_type.strip().lower()
    if not normalized:
        return False
    if normalized in {
        "function_call_output",
        "computer_call_output",
        "mcp_call_output",
        "web_search_call_output",
        "tool_result",
    }:
        return True
    return normalized.endswith("_call_output") or normalized.endswith("_tool_output")


def _responses_body_contains_tool_result(body: Any) -> bool:
    if not isinstance(body, dict):
        return False

    for item in _iter_dict_nodes(body.get("input")):
        item_type = str(item.get("type", "")).strip()
        if _is_responses_tool_result_type(item_type):
            return True
    return False


def _responses_tool_result_response_id(body: Any) -> str:
    if not isinstance(body, dict):
        return ""

    for field_name in ("previous_response_id", "response_id"):
        value = str(body.get(field_name, "")).strip()
        if value:
            return value
    return ""


def _chat_completions_body_contains_tool_result(body: Any) -> bool:
    if not isinstance(body, dict):
        return False

    for item in _iter_dict_nodes(body.get("messages")):
        role = str(item.get("role", "")).strip().lower()
        if role == "tool":
            return True
        if str(item.get("tool_call_id", "")).strip():
            return True
    return False


class RecoveryStore:
    def __init__(self) -> None:
        self.root = RECOVERY_DIR
        self.last_interrupted_file = LAST_INTERRUPTED_FILE
        self.active_sessions_file = ACTIVE_SESSIONS_FILE
        self.active_sessions: dict[str, dict[str, Any]] = {}
        self.running_session_ids: set[str] = set()
        self._flush_active_sessions()

    def begin_session(
        self,
        endpoint: str,
        body: dict[str, Any] | None,
        upstream: str,
        continued_from: dict[str, Any] | None = None,
        client_label: str = "",
        request_title: str = "",
        method: str = "POST",
    ) -> dict[str, Any]:
        payload = body if isinstance(body, dict) else {}
        session_id = f"{int(time.time() * 1000)}-{os.getpid()}"
        if endpoint == "/v1/messages" or "messages" in payload:
            last_user_text = _extract_last_messages_user_text(payload.get("messages"))
            first_user_text = _extract_first_messages_user_text(payload.get("messages"))
        elif endpoint == "/v1/responses" or "input" in payload:
            last_user_text = _extract_last_responses_user_text(payload.get("input"))
            first_user_text = _extract_first_responses_user_text(payload.get("input"))
        else:
            last_user_text = _extract_generic_request_text(payload)
            first_user_text = last_user_text

        request_summary = _build_request_title(request_title, first_user_text, last_user_text)
        if not request_summary:
            request_summary = _clean_request_title(f"{method.upper()} {endpoint}", limit=80)

        payload = {
            "session_id": session_id,
            "endpoint": endpoint,
            "model": payload.get("model", ""),
            "upstream": upstream,
            "started_at": _timestamp_now(),
            "last_user_text": last_user_text,
            "request_summary": request_summary,
            "request_body": deepcopy(payload) if payload else None,
            "request_method": method.upper(),
            "status": "running",
            "continued_from_session_id": (continued_from or {}).get("session_id", ""),
            "client_label": client_label,
            "_last_saved_len": 0,
            "_last_saved_at": 0.0,
        }
        self.running_session_ids.add(session_id)
        return payload

    def maybe_save_progress(
        self,
        session: dict[str, Any],
        state: StreamState,
        *,
        force: bool = False,
    ) -> None:
        now = time.time()
        text_len = len(state.accumulated_text)
        if not force:
            if text_len == 0 and not state.response_id and not state.message_id:
                return
            if text_len - session.get("_last_saved_len", 0) < 200 and now - session.get("_last_saved_at", 0.0) < 2.0:
                return

        payload = {
            "session_id": session["session_id"],
            "endpoint": session["endpoint"],
            "model": session.get("model", ""),
            "upstream": session.get("upstream", ""),
            "started_at": session.get("started_at", ""),
            "updated_at": _timestamp_now(),
            "status": session.get("status", "running"),
            "reason": session.get("reason", ""),
            "last_user_text": session.get("last_user_text", ""),
            "request_summary": session.get("request_summary", ""),
            "continued_from_session_id": session.get("continued_from_session_id", ""),
            "request_method": session.get("request_method", "POST"),
            "request_body": session.get("request_body"),
            "partial_text": state.accumulated_text,
            "bytes_forwarded": state.bytes_forwarded,
            "message_id": state.message_id,
            "response_id": state.response_id,
            "response_item_id": state.response_item_id,
        }
        try:
            _write_json(self.last_interrupted_file, payload)
        except Exception as exc:
            log.warning("Failed to persist recovery snapshot: %s", exc)
        self._upsert_active_session(session, state)
        session["_last_saved_len"] = text_len
        session["_last_saved_at"] = now

    def mark_interrupted(self, session: dict[str, Any], state: StreamState, reason: str) -> None:
        session["status"] = "interrupted"
        session["reason"] = reason
        self.running_session_ids.discard(str(session.get("session_id", "")))
        self.maybe_save_progress(session, state, force=True)
        self._remove_active_session(str(session.get("session_id", "")))

    def mark_completed(self, session: dict[str, Any], state: StreamState) -> None:
        session["status"] = "completed"
        self.running_session_ids.discard(str(session.get("session_id", "")))
        existing = self.load_snapshot()
        if existing and existing.get("session_id") == session.get("session_id"):
            with suppress(FileNotFoundError):
                self.last_interrupted_file.unlink()
        self._remove_active_session(str(session.get("session_id", "")))

    def load_snapshot(
        self,
        endpoint: str | None = None,
        *,
        status: str | None = None,
    ) -> dict[str, Any] | None:
        if not self.last_interrupted_file.exists():
            return None

        try:
            payload = _read_json(self.last_interrupted_file)
        except Exception:
            return None

        if status and payload.get("status") != status:
            return None
        if endpoint and payload.get("endpoint") != endpoint:
            return None
        return payload

    def load_last_interrupted(self, endpoint: str | None = None) -> dict[str, Any] | None:
        return self.load_snapshot(endpoint, status="interrupted")

    def track_active_session(self, session: dict[str, Any], state: StreamState) -> None:
        if str(session.get("status", "running")).strip() != "running":
            return
        self._upsert_active_session(session, state)

    def active_sessions_snapshot(self) -> list[dict[str, Any]]:
        items = [
            dict(item)
            for item in self.active_sessions.values()
            if str(item.get("status", "")).strip() == "running"
        ]
        items.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
        return items

    def _upsert_active_session(self, session: dict[str, Any], state: StreamState) -> None:
        session_id = str(session.get("session_id", "")).strip()
        if not session_id:
            return
        if session_id not in self.running_session_ids:
            return
        if str(session.get("status", "running")).strip() != "running":
            return

        payload = {
            "session_id": session_id,
            "endpoint": session.get("endpoint", ""),
            "endpoint_label": _endpoint_label(str(session.get("endpoint", ""))),
            "model": session.get("model", ""),
            "upstream": session.get("upstream", ""),
            "started_at": session.get("started_at", ""),
            "updated_at": _timestamp_now(),
            "status": session.get("status", "running"),
            "reason": session.get("reason", ""),
            "request_summary": session.get("request_summary", ""),
            "continued_from_session_id": session.get("continued_from_session_id", ""),
            "client_label": session.get("client_label", ""),
            "request_method": session.get("request_method", "POST"),
            "last_user_text_preview": _shorten_text(str(session.get("last_user_text", "")), 120),
            "partial_text_preview": _shorten_text(state.accumulated_text, 120),
            "partial_chars": len(state.accumulated_text),
            "bytes_forwarded": state.bytes_forwarded,
            "message_id": state.message_id,
            "response_id": state.response_id,
            "response_item_id": state.response_item_id,
        }
        self.active_sessions[session_id] = payload
        self._flush_active_sessions()

    def _remove_active_session(self, session_id: str) -> None:
        if not session_id:
            return
        if self.active_sessions.pop(session_id, None) is None:
            return
        self._flush_active_sessions()

    def _flush_active_sessions(self) -> None:
        payload = {"sessions": self.active_sessions_snapshot()}
        try:
            _write_json(self.active_sessions_file, payload)
        except Exception as exc:
            log.warning("Failed to persist active sessions snapshot: %s", exc)


class EventHistory:
    def __init__(self, path: Path = EVENT_HISTORY_FILE, limit: int = 200) -> None:
        self.path = path
        self.limit = limit
        self.events = self._load()

    def add(
        self,
        kind: str,
        title: str,
        detail: str,
        *,
        endpoint: str = "",
        model: str = "",
        tone: str = "neutral",
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event = {
            "id": f"{int(time.time() * 1000)}-{os.getpid()}",
            "created_at_ms": int(time.time() * 1000),
            "timestamp": _timestamp_now(),
            "kind": kind,
            "tone": tone,
            "title": title,
            "detail": _shorten_text(detail, 160),
            "endpoint": endpoint,
            "endpoint_label": _endpoint_label(endpoint),
            "model": _shorten_text(model, 48),
            "meta": dict(meta or {}),
        }
        self.events = [event, *self.events[: self.limit - 1]]
        self._persist()
        return event

    def snapshot(self, limit: int | None = None) -> list[dict[str, Any]]:
        items = self.events if limit is None else self.events[:limit]
        return [dict(item) for item in items]

    def _load(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            payload = _read_json(self.path)
        except Exception:
            return []

        items = payload.get("events")
        if not isinstance(items, list):
            return []

        cleaned: list[dict[str, Any]] = []
        for item in items[: self.limit]:
            if isinstance(item, dict):
                cleaned.append(dict(item))
        return cleaned

    def _persist(self) -> None:
        try:
            _write_json(
                self.path,
                {
                    "updated_at": _timestamp_now(),
                    "events": self.events[: self.limit],
                },
            )
        except Exception as exc:
            log.warning("Failed to persist event history: %s", exc)


class CodexConfigManager:
    def __init__(self, listen_port: int) -> None:
        self.listen_port = listen_port
        self.proxy_base_url = f"http://{LOCAL_HOST}:{listen_port}/v1"
        self.original_base_url = ""
        self.provider_name = ""
        self._hijacked = False
        self._restored = False

    def hijack(self) -> None:
        if not CODEX_CONFIG.exists():
            return

        text = CODEX_CONFIG.read_text(encoding="utf-8")
        provider_name = self._extract_provider_name(text)
        if not provider_name:
            return

        base_url, line_index = self._extract_provider_base_url(text, provider_name)
        if line_index is None or not base_url:
            return

        self.provider_name = provider_name
        if base_url.rstrip("/") == self.proxy_base_url.rstrip("/"):
            recovered = self._read_state()
            if recovered:
                self.original_base_url = recovered.get("original_base_url", "")
            self._hijacked = True
            return

        self.original_base_url = base_url.rstrip("/")
        updated = self._replace_provider_base_url(text, provider_name, self.proxy_base_url)
        if updated == text:
            return

        CODEX_CONFIG.write_text(updated, encoding="utf-8", newline="\n")
        self._write_state()
        self._hijacked = True
        log.info("codex config hijacked: %s -> %s", self.original_base_url, self.proxy_base_url)

    def restore(self) -> None:
        if self._restored:
            return
        self._restored = True

        try:
            if not self._hijacked or not self.original_base_url or not CODEX_CONFIG.exists():
                return

            text = CODEX_CONFIG.read_text(encoding="utf-8")
            if self.proxy_base_url.rstrip("/") not in text:
                return

            updated = self._replace_provider_base_url(text, self.provider_name, self.original_base_url)
            if updated != text:
                CODEX_CONFIG.write_text(updated, encoding="utf-8", newline="\n")
                log.info("codex config restored: %s -> %s", self.proxy_base_url, self.original_base_url)
        finally:
            with suppress(FileNotFoundError):
                CODEX_STATE_FILE.unlink()

    def discard_state(self) -> None:
        self._restored = True
        with suppress(FileNotFoundError):
            CODEX_STATE_FILE.unlink()

    def _extract_provider_name(self, text: str) -> str:
        match = re.search(r'(?m)^\s*model_provider\s*=\s*"([^"]+)"\s*$', text)
        return match.group(1).strip() if match else ""

    def _extract_provider_base_url(self, text: str, provider_name: str) -> tuple[str, int | None]:
        lines = text.splitlines()
        section_header = f"[model_providers.{provider_name}]"
        in_section = False
        for index, line in enumerate(lines):
            stripped = line.strip()
            if stripped == section_header:
                in_section = True
                continue
            if in_section and stripped.startswith("[") and stripped.endswith("]"):
                break
            if in_section:
                match = re.match(r'^\s*base_url\s*=\s*"([^"]+)"\s*$', line)
                if match:
                    return match.group(1).strip(), index
        return "", None

    def _replace_provider_base_url(self, text: str, provider_name: str, new_base_url: str) -> str:
        lines = text.splitlines()
        section_header = f"[model_providers.{provider_name}]"
        in_section = False
        replaced = False

        for index, line in enumerate(lines):
            stripped = line.strip()
            if stripped == section_header:
                in_section = True
                continue
            if in_section and stripped.startswith("[") and stripped.endswith("]"):
                break
            if in_section and re.match(r'^\s*base_url\s*=\s*"([^"]+)"\s*$', line):
                indent = re.match(r"^\s*", line).group(0)
                lines[index] = f'{indent}base_url = "{new_base_url}"'
                replaced = True
                break

        if not replaced:
            return text

        trailing_newline = "\n" if text.endswith("\n") else ""
        return "\n".join(lines) + trailing_newline

    def _write_state(self) -> None:
        _write_json(
            CODEX_STATE_FILE,
            {
                "provider_name": self.provider_name,
                "original_base_url": self.original_base_url,
                "proxy_base_url": self.proxy_base_url,
                "updated_at": _timestamp_now(),
            },
        )

    def _read_state(self) -> dict[str, Any] | None:
        if not CODEX_STATE_FILE.exists():
            return None
        try:
            return _read_json(CODEX_STATE_FILE)
        except Exception:
            return None

    @classmethod
    def discover_upstream(cls, listen_port: int) -> tuple[str, str]:
        proxy_base_url = f"http://{LOCAL_HOST}:{listen_port}/v1".rstrip("/")
        manager = cls(listen_port)

        recovered = manager._read_state()
        if recovered:
            original_base_url = str(recovered.get("original_base_url", "")).strip().rstrip("/")
            recovered_proxy_url = str(recovered.get("proxy_base_url", "")).strip().rstrip("/")
            if recovered_proxy_url == proxy_base_url and original_base_url and original_base_url != proxy_base_url:
                return original_base_url, "codex_state"

        if not CODEX_CONFIG.exists():
            return "", ""

        try:
            text = CODEX_CONFIG.read_text(encoding="utf-8")
        except Exception:
            return "", ""

        provider_name = manager._extract_provider_name(text)
        if not provider_name:
            return "", ""

        base_url, _line_index = manager._extract_provider_base_url(text, provider_name)
        base_url = base_url.strip().rstrip("/")
        if base_url and base_url != proxy_base_url:
            return base_url, "codex_config"

        return "", ""


class SettingsManager:
    def __init__(
        self,
        listen_port: int,
        upstream_override: str | None = None,
        poll_interval: float = 1.0,
    ) -> None:
        self.listen_port = listen_port
        self.proxy_url = f"http://{LOCAL_HOST}:{listen_port}"
        self.upstream_override = upstream_override.rstrip("/") if upstream_override else None
        self.poll_interval = poll_interval
        self.current_upstream = ""
        self.current_upstream_source = "unknown"
        self.restore_upstream = ""
        self.restore_auth_token = ""
        self._hijacked = False
        self._restored = False
        self._last_settings_mtime_ns: int | None = None
        self._watch_task: asyncio.Task[None] | None = None

    def hijack(self) -> str:
        if not CLAUDE_SETTINGS.exists():
            raise RuntimeError(f"settings.json not found: {CLAUDE_SETTINGS}")

        self._repair_stale_hijack_if_needed()

        settings = _read_json(CLAUDE_SETTINGS)
        on_disk_base_url = _extract_base_url(settings)

        if self.upstream_override:
            upstream = self.upstream_override
            source = "manual_override"
            if on_disk_base_url and on_disk_base_url != self.proxy_url:
                log.info("Using manual upstream override instead of settings.json")
        elif on_disk_base_url and on_disk_base_url != self.proxy_url:
            upstream, source = self._normalize_upstream_from_settings(settings, on_disk_base_url)
        elif on_disk_base_url == self.proxy_url:
            upstream, source = self._recover_stale_upstream(settings)
            if not upstream:
                raise RuntimeError(
                    "settings.json already points to this proxy, but no real upstream "
                    "could be recovered"
                )
            log.warning(
                "settings.json already points to %s; recovered upstream %s via %s",
                self.proxy_url,
                upstream,
                source,
            )
        else:
            raise RuntimeError("settings.json does not contain ANTHROPIC_BASE_URL")

        self.current_upstream = upstream.rstrip("/")
        self.restore_upstream = self.current_upstream
        self.restore_auth_token = _extract_auth_token(settings)
        self.current_upstream_source = source
        self._persist_state()

        if on_disk_base_url != self.proxy_url:
            _write_json(CLAUDE_SETTINGS, _with_base_url(settings, self.proxy_url))
            log.info("settings.json hijacked: %s -> %s", on_disk_base_url, self.proxy_url)

        self._remember_settings_version()
        self._hijacked = True
        return self.current_upstream

    async def start_watch(self) -> None:
        if self._watch_task is None:
            self._watch_task = asyncio.create_task(
                self._watch_loop(),
                name="resilient-proxy-settings-watch",
            )

    async def stop_watch(self) -> None:
        if self._watch_task is None:
            return
        self._watch_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._watch_task
        self._watch_task = None

    async def _watch_loop(self) -> None:
        while True:
            await asyncio.sleep(self.poll_interval)
            try:
                self.sync_from_disk()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("settings watcher error: %s", exc)

    def sync_from_disk(self) -> None:
        if not CLAUDE_SETTINGS.exists():
            return

        current_mtime_ns = CLAUDE_SETTINGS.stat().st_mtime_ns
        if self._last_settings_mtime_ns == current_mtime_ns:
            return

        settings = _read_json(CLAUDE_SETTINGS)
        on_disk_base_url = _extract_base_url(settings)
        self._last_settings_mtime_ns = current_mtime_ns

        if not on_disk_base_url:
            log.warning("settings.json changed, but ANTHROPIC_BASE_URL is missing")
            return

        if on_disk_base_url == self.proxy_url:
            return

        # If CC Switch has just taken over, avoid a write tug-of-war by not re-hijacking.
        if _is_cc_switch_proxy_url(on_disk_base_url) and self._cc_switch_proxy_enabled():
            previous = self.current_upstream or "<none>"
            self.current_upstream = on_disk_base_url.rstrip("/")
            self.restore_upstream = self.current_upstream
            self.restore_auth_token = _extract_auth_token(settings)
            self.current_upstream_source = "cc_switch"
            if self.current_upstream != previous:
                self._persist_state()
            log.info(
                "Detected CC Switch takeover (%s); skip re-hijack to avoid settings conflict",
                on_disk_base_url,
            )
            return

        if self.upstream_override:
            log.info(
                "Detected settings.json change to %s; keeping manual upstream %s",
                on_disk_base_url,
                self.upstream_override,
            )
        else:
            previous = self.current_upstream or "<none>"
            self.current_upstream, self.current_upstream_source = self._normalize_upstream_from_settings(
                settings,
                on_disk_base_url,
            )
            self.restore_upstream = self.current_upstream
            self.restore_auth_token = _extract_auth_token(settings)
            if self.current_upstream != previous:
                log.info(
                    "Detected settings.json change: upstream %s -> %s",
                    previous,
                    self.current_upstream,
                )
                self._persist_state()

        _write_json(CLAUDE_SETTINGS, _with_base_url(settings, self.proxy_url))
        self._remember_settings_version()
        log.info("settings.json re-hijacked: %s -> %s", on_disk_base_url, self.proxy_url)

    def restore(self) -> None:
        if self._restored:
            return
        self._restored = True

        should_clear_state = False
        try:
            if not self._hijacked:
                should_clear_state = True
                return

            if not self.current_upstream:
                log.warning("Skipping restore because the real upstream is unknown")
                return

            if not CLAUDE_SETTINGS.exists():
                log.warning("Skipping restore because settings.json no longer exists")
                return

            settings = _read_json(CLAUDE_SETTINGS)
            on_disk_base_url = _extract_base_url(settings)
            if on_disk_base_url == self.proxy_url:
                restore_target = (self.restore_upstream or self.current_upstream).rstrip("/")
                _write_json(
                    CLAUDE_SETTINGS,
                    _with_restore_env(
                        settings,
                        base_url=restore_target,
                        auth_token=self.restore_auth_token,
                    ),
                )
                log.info("settings.json restored: %s -> %s", self.proxy_url, restore_target)
                should_clear_state = True
            else:
                log.info("settings.json already changed externally; leaving %s", on_disk_base_url)
                should_clear_state = True
        except Exception as exc:
            log.error("Failed to restore settings.json: %s", exc)
            if self.current_upstream:
                log.error("Please restore ANTHROPIC_BASE_URL to: %s", self.current_upstream)
        finally:
            if should_clear_state:
                self._clear_state()

    def discard_state(self) -> None:
        self._restored = True
        self._clear_state()

    def _recover_stale_upstream(self, settings: dict[str, Any]) -> tuple[str, str]:
        if self.upstream_override:
            return self.upstream_override, "manual_override"

        state = self._read_state()
        if state:
            proxy_url = str(state.get("proxy_url", "")).strip().rstrip("/")
            upstream = str(state.get("upstream_base_url", "")).strip().rstrip("/")
            if proxy_url == self.proxy_url and upstream and upstream != self.proxy_url:
                return upstream, "proxy_state"

        codex_upstream, codex_source = CodexConfigManager.discover_upstream(self.listen_port)
        if codex_upstream:
            return codex_upstream, codex_source

        auth_token = str(settings.get("env", {}).get("ANTHROPIC_AUTH_TOKEN", "")).strip()
        if auth_token == "PROXY_MANAGED" and self._cc_switch_proxy_enabled():
            return CC_SWITCH_PROXY_URL, "cc_switch"

        if self._cc_switch_proxy_enabled() or _local_port_is_open(CC_SWITCH_PROXY_PORT):
            return CC_SWITCH_PROXY_URL, "cc_switch"

        return "", ""

    def _normalize_upstream_from_settings(
        self,
        settings: dict[str, Any],
        base_url: str,
    ) -> tuple[str, str]:
        normalized = str(base_url or "").strip().rstrip("/")
        if not normalized:
            return "", "claude_settings"

        auth_token = _extract_auth_token(settings)
        if (
            auth_token == "PROXY_MANAGED"
            and not _is_cc_switch_proxy_url(normalized)
            and normalized != self.proxy_url
            and (self._cc_switch_proxy_enabled() or _local_port_is_open(CC_SWITCH_PROXY_PORT))
        ):
            log.warning(
                "settings.json has direct upstream %s with PROXY_MANAGED token; "
                "using CC Switch local proxy %s instead",
                normalized,
                CC_SWITCH_PROXY_URL,
            )
            return CC_SWITCH_PROXY_URL, "cc_switch_recovered"

        return normalized, "claude_settings"

    def _cc_switch_proxy_enabled(self) -> bool:
        if not CC_SWITCH_SETTINGS.exists():
            return False
        try:
            settings = _read_json(CC_SWITCH_SETTINGS)
        except Exception:
            return False
        return bool(settings.get("enableLocalProxy"))

    def _repair_stale_hijack_if_needed(self) -> None:
        state = self._read_state()
        if not state or not CLAUDE_SETTINGS.exists():
            return

        state_proxy = str(state.get("proxy_url", "")).strip().rstrip("/")
        if state_proxy != self.proxy_url:
            return

        try:
            state_pid = int(state.get("pid") or 0)
        except Exception:
            state_pid = 0
        if state_pid > 0 and _process_exists(state_pid):
            return

        settings = _read_json(CLAUDE_SETTINGS)
        on_disk_base_url = _extract_base_url(settings).rstrip("/")
        if on_disk_base_url != self.proxy_url:
            return

        restore_target = str(
            state.get("restore_base_url") or state.get("upstream_base_url") or ""
        ).strip().rstrip("/")
        if not restore_target:
            return

        restore_auth_token = state.get("restore_auth_token")
        if restore_auth_token is not None:
            restore_auth_token = str(restore_auth_token).strip()

        _write_json(
            CLAUDE_SETTINGS,
            _with_restore_env(
                settings,
                base_url=restore_target,
                auth_token=restore_auth_token,
            ),
        )
        self._clear_state()
        log.warning(
            "Recovered stale hijack from dead process pid=%s: %s -> %s",
            state_pid or "<unknown>",
            self.proxy_url,
            restore_target,
        )

    def _remember_settings_version(self) -> None:
        if CLAUDE_SETTINGS.exists():
            self._last_settings_mtime_ns = CLAUDE_SETTINGS.stat().st_mtime_ns

    def _persist_state(self) -> None:
        payload = {
            "listen_port": self.listen_port,
            "proxy_url": self.proxy_url,
            "upstream_base_url": self.current_upstream,
            "restore_base_url": self.restore_upstream,
            "restore_auth_token": self.restore_auth_token,
            "upstream_source": self.current_upstream_source,
            "pid": os.getpid(),
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        _write_json(STATE_FILE, payload)

    def _read_state(self) -> dict[str, Any] | None:
        if not STATE_FILE.exists():
            return None
        try:
            return _read_json(STATE_FILE)
        except Exception:
            return None

    def _clear_state(self) -> None:
        with suppress(FileNotFoundError):
            STATE_FILE.unlink()


@dataclass
class StreamState:
    accumulated_text: str = ""
    bytes_forwarded: int = 0
    is_first_attempt: bool = True
    message_id: str = ""
    response_id: str = ""
    response_item_id: str = ""
    model: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    skip_prefix: str = ""
    completed: bool = False
    recovery_triggered: bool = False
    request_summary: str = ""
    awaiting_tool_result: bool = False
    tool_names: list[str] = field(default_factory=list)
    done_sent: bool = False
    start_time: float = field(default_factory=time.time)
    last_visible_output_at: float = field(default_factory=time.monotonic)


class SSEStreamWriter:
    def __init__(
        self,
        response: web.StreamResponse,
        heartbeat_interval: float,
        *,
        heartbeat_frame: bytes = SSE_HEARTBEAT_FRAME,
    ):
        self.response = response
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_frame = heartbeat_frame or SSE_HEARTBEAT_FRAME
        self._lock = asyncio.Lock()
        self._last_write_at = time.monotonic()
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._disconnect_error: BaseException | None = None
        self._closed = False

    def start(self) -> None:
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def raise_if_disconnected(self) -> None:
        if self._disconnect_error is not None:
            raise self._disconnect_error

    def is_disconnected(self) -> bool:
        return self._disconnect_error is not None

    async def write(self, payload: bytes) -> None:
        self.raise_if_disconnected()
        async with self._lock:
            self.raise_if_disconnected()
            try:
                await self.response.write(payload)
            except Exception as exc:
                if _is_client_disconnect_error(exc):
                    self._disconnect_error = exc
                raise
            self._last_write_at = time.monotonic()

    async def write_block(self, block: str) -> None:
        await self.write(f"{block}\n\n".encode())

    async def write_done(self) -> None:
        await self.write(b"data: [DONE]\n\n")

    async def write_done_once(self, state: StreamState) -> None:
        if state.done_sent:
            return
        await self.write_done()
        state.done_sent = True

    async def write_event(self, event_type: str, data_string: str) -> None:
        payload = ""
        if event_type:
            payload += f"event: {event_type}\n"
        payload += f"data: {data_string}\n\n"
        await self.write(payload.encode())

    async def close(self) -> None:
        self._closed = True
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self.heartbeat_interval)
            if self._closed:
                return
            if time.monotonic() - self._last_write_at < self.heartbeat_interval:
                continue
            try:
                await self.write(self.heartbeat_frame)
            except Exception as exc:
                if _is_client_disconnect_error(exc):
                    self._disconnect_error = exc
                else:
                    log.warning("SSE heartbeat failed: %s", exc)
                return


class _StreamTimeout(asyncio.TimeoutError):
    def __init__(self, timeout: float, phase: str):
        self.timeout = timeout
        self.phase = phase
        super().__init__(f"{phase} timeout after {timeout:.1f}s")


class _RetryableRequestError(Exception):
    def __init__(self, message: str, *, status_code: int = 504):
        self.status_code = status_code
        super().__init__(message)


class ResilientProxy:
    def __init__(
        self,
        settings_manager: SettingsManager,
        stall_timeout: float,
        max_retries: int,
        *,
        max_request_body_bytes: int | None = None,
        safe_resume_body_bytes: int | None = None,
    ):
        self.settings_manager = settings_manager
        self.stall_timeout = stall_timeout
        self.max_retries = max_retries
        self.max_request_body_bytes = max(
            1,
            int(
                _mb_to_bytes(DEFAULT_MAX_REQUEST_BODY_MB)
                if max_request_body_bytes is None
                else max_request_body_bytes
            ),
        )
        self.safe_resume_body_bytes = max(
            0,
            int(
                _mb_to_bytes(DEFAULT_SAFE_RESUME_BODY_MB)
                if safe_resume_body_bytes is None
                else safe_resume_body_bytes
            ),
        )
        self.safety_watchdog_pid: int | None = None
        self.first_byte_timeout = max(MIN_FIRST_BYTE_TIMEOUT, stall_timeout * 1.5)
        self.visible_output_timeout = max(DEFAULT_VISIBLE_OUTPUT_TIMEOUT, stall_timeout)
        self.direct_body_timeout = max(stall_timeout, 30.0)
        self.buffered_request_timeout = max(self.first_byte_timeout, 150.0)
        self.heartbeat_interval = max(
            MIN_HEARTBEAT_INTERVAL,
            min(MAX_HEARTBEAT_INTERVAL, stall_timeout / 3),
        )
        self.tool_wait_timeout = max(10.0, min(DEFAULT_TOOL_WAIT_TIMEOUT, stall_timeout))
        self.recovery_store = RecoveryStore()
        self.event_history = EventHistory()
        self.pending_tool_waits: dict[str, dict[str, Any]] = {}
        self.stats = {
            "total": 0,
            "success": 0,
            "recoveries_triggered": 0,
            "recoveries_succeeded": 0,
            "retry_attempts": 0,
        }
        self.started_at_ms = int(time.time() * 1000)
        self.started_at = _timestamp_now()

    def _should_retry(self, attempt: int) -> bool:
        return self.max_retries < 0 or attempt < self.max_retries

    def _messages_stream_timeouts(self) -> tuple[float, float, float]:
        return self.first_byte_timeout, self.stall_timeout, self.visible_output_timeout

    def _responses_stream_timeouts(self) -> tuple[float, float, float]:
        # Codex clients tend to enforce their own request deadline, so responses
        # streams need to recover no later than messages streams.
        return self.first_byte_timeout, self.stall_timeout, self.visible_output_timeout

    def _responses_tail_fetch_timeout(self) -> float:
        # Some relays close the SSE before the response object becomes fetchable.
        # Give tail fetch enough time to catch up, but keep it bounded so recovery
        # still falls back to stream retry in a reasonable time.
        return min(
            max(self.stall_timeout, MIN_RESPONSES_TAIL_FETCH_TIMEOUT),
            MAX_RESPONSES_TAIL_FETCH_TIMEOUT,
        )

    def _make_client_stream(self, response: web.StreamResponse, endpoint: str) -> SSEStreamWriter:
        heartbeat_frame = ANTHROPIC_PING_FRAME if endpoint == "/v1/messages" else SSE_HEARTBEAT_FRAME
        return SSEStreamWriter(
            response,
            self.heartbeat_interval,
            heartbeat_frame=heartbeat_frame,
        )

    def _allow_proxy_body_growth(
        self,
        endpoint: str,
        original_body: dict[str, Any],
        candidate_body: dict[str, Any],
        *,
        reason: str,
    ) -> bool:
        if self.safe_resume_body_bytes <= 0:
            return True

        original_size = _json_body_size_bytes(original_body)
        candidate_size = _json_body_size_bytes(candidate_body)
        if candidate_size <= original_size:
            return True

        if original_size >= self.safe_resume_body_bytes or candidate_size > self.safe_resume_body_bytes:
            log.warning(
                "   SKIP %s for %s: proxy-added body growth would increase request from %s to %s "
                "(safe cap %s)",
                reason,
                endpoint,
                _format_bytes(original_size),
                _format_bytes(candidate_size),
                _format_bytes(self.safe_resume_body_bytes),
            )
            return False
        return True

    def _with_messages_assistant_prefill(
        self,
        body: dict[str, Any],
        partial_text: str,
        *,
        insert_before_final_user: bool,
        reason: str,
    ) -> dict[str, Any] | None:
        messages = deepcopy(body.get("messages", []))
        insert_at = len(messages)
        if insert_before_final_user and messages and isinstance(messages[-1], dict) and messages[-1].get("role") == "user":
            insert_at -= 1
        messages.insert(
            insert_at,
            {
                "role": "assistant",
                "content": partial_text,
            },
        )
        patched = deepcopy(body)
        patched["messages"] = messages
        if not self._allow_proxy_body_growth("/v1/messages", body, patched, reason=reason):
            return None
        return patched

    def _with_chat_completions_assistant_prefill(
        self,
        body: dict[str, Any],
        partial_text: str,
        *,
        reason: str,
    ) -> dict[str, Any] | None:
        messages = deepcopy(body.get("messages", []))
        messages.append(
            {
                "role": "assistant",
                "content": partial_text,
            },
        )
        patched = deepcopy(body)
        patched["messages"] = messages
        if not self._allow_proxy_body_growth("/v1/chat/completions", body, patched, reason=reason):
            return None
        return patched

    def _resolve_tool_wait_from_request(self, endpoint: str, body: Any) -> None:
        if endpoint == "/v1/messages":
            has_tool_result = _messages_body_contains_tool_result(body)
        elif endpoint == "/v1/responses":
            has_tool_result = _responses_body_contains_tool_result(body)
        elif endpoint == "/v1/chat/completions":
            has_tool_result = _chat_completions_body_contains_tool_result(body)
        else:
            has_tool_result = False

        if not has_tool_result:
            return

        matches = [
            item
            for item in self.pending_tool_waits.values()
            if str(item.get("endpoint", "")).strip() == endpoint
        ]
        if not matches:
            return

        if endpoint == "/v1/responses":
            response_id = _responses_tool_result_response_id(body)
            if response_id:
                exact_matches = [
                    item
                    for item in matches
                    if str(item.get("response_id", "")).strip() == response_id
                ]
                if exact_matches:
                    matches = exact_matches

        matches.sort(key=lambda item: float(item.get("_wait_started_at_ts", 0.0) or 0.0))
        pending = matches[0]
        self.pending_tool_waits.pop(str(pending.get("session_id", "")), None)
        wait_seconds = max(0, int(time.time() - float(pending.get("_wait_started_at_ts", 0.0) or 0.0)))
        tools_label = ", ".join(pending.get("tool_names", [])[:3]).strip() or "tool call"
        self.event_history.add(
            "tool_resumed",
            "Tool continuation resumed",
            f"{_endpoint_label(endpoint)} | tool results arrived after {wait_seconds}s | {tools_label}",
            endpoint=endpoint,
            model=str(pending.get("model", "")),
            tone="neutral",
            meta={
                "wait_seconds": wait_seconds,
                "tool_names": list(pending.get("tool_names", [])),
                "tool_count": int(pending.get("tool_count", 0) or 0),
                "request_summary": pending.get("request_summary", ""),
            },
        )

    def _register_pending_tool_wait(
        self,
        endpoint: str,
        session: dict[str, Any],
        state: StreamState,
    ) -> None:
        if not state.awaiting_tool_result:
            return

        now = time.time()
        session_id = str(session.get("session_id", "")).strip()
        if not session_id:
            return

        tool_names = [name for name in state.tool_names if str(name).strip()]
        pending = {
            "session_id": session_id,
            "endpoint": endpoint,
            "endpoint_label": _endpoint_label(endpoint),
            "model": state.model or session.get("model", ""),
            "response_id": state.response_id,
            "message_id": state.message_id,
            "started_at": session.get("started_at", ""),
            "updated_at": _timestamp_now(),
            "status": "waiting_tool_result",
            "request_summary": session.get("request_summary", ""),
            "client_label": session.get("client_label", ""),
            "last_user_text_preview": _shorten_text(str(session.get("last_user_text", "")), 120),
            "partial_text_preview": _shorten_text(state.accumulated_text, 120),
            "partial_chars": len(state.accumulated_text),
            "tool_names": tool_names,
            "tool_count": len(tool_names),
            "_wait_started_at_ts": now,
            "_timed_out": False,
        }
        self.pending_tool_waits[session_id] = pending
        tools_label = ", ".join(tool_names[:3]).strip() or "tool call"
        self.event_history.add(
            "tool_wait",
            "Waiting for tool results",
            f"{_endpoint_label(endpoint)} | waiting for local tool results | {tools_label}",
            endpoint=endpoint,
            model=state.model,
            tone="neutral",
            meta={
                "tool_names": tool_names,
                "tool_count": len(tool_names),
                "request_summary": state.request_summary,
            },
        )

    def _check_pending_tool_wait_timeouts(self) -> None:
        now = time.time()
        for pending in self.pending_tool_waits.values():
            if pending.get("_timed_out"):
                continue

            waited = now - float(pending.get("_wait_started_at_ts", 0.0) or 0.0)
            if waited < self.tool_wait_timeout:
                continue

            pending["_timed_out"] = True
            pending["status"] = "tool_result_timeout"
            pending["updated_at"] = _timestamp_now()
            wait_seconds = max(0, int(waited))
            tools_label = ", ".join(pending.get("tool_names", [])[:3]).strip() or "tool call"
            self.event_history.add(
                "tool_wait_timeout",
                "Tool continuation stalled",
                f"{_endpoint_label(str(pending.get('endpoint', '')))} | still waiting after {wait_seconds}s | {tools_label}",
                endpoint=str(pending.get("endpoint", "")),
                model=str(pending.get("model", "")),
                tone="warning",
                meta={
                    "wait_seconds": wait_seconds,
                    "tool_names": list(pending.get("tool_names", [])),
                    "tool_count": int(pending.get("tool_count", 0) or 0),
                    "request_summary": pending.get("request_summary", ""),
                },
            )

    def _pending_tool_waits_snapshot(self) -> list[dict[str, Any]]:
        self._check_pending_tool_wait_timeouts()
        items: list[dict[str, Any]] = []
        now = time.time()
        for pending in self.pending_tool_waits.values():
            snapshot = {
                key: value
                for key, value in pending.items()
                if not str(key).startswith("_")
            }
            snapshot["wait_seconds"] = max(
                0,
                int(now - float(pending.get("_wait_started_at_ts", 0.0) or 0.0)),
            )
            items.append(snapshot)
        items.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
        return items

    def _resolve_attempt_target(
        self,
        endpoint: str,
        headers: dict[str, str],
        attempt: int,
        *,
        app_type: str | None = None,
    ) -> tuple[str, dict[str, str], str]:
        upstream = self.settings_manager.current_upstream
        attempt_headers = dict(headers)
        attempt_route = "primary"
        return upstream, attempt_headers, attempt_route

    async def handle_messages(self, req: web.Request) -> web.StreamResponse:
        self.stats["total"] += 1

        upstream = self.settings_manager.current_upstream
        if not upstream:
            return web.json_response(
                {"error": {"type": "proxy_error", "message": "No upstream is available"}},
                status=503,
            )

        body = await req.json()
        self._resolve_tool_wait_from_request("/v1/messages", body)
        body, resumed = self._maybe_resume_messages(body)
        sanitized_body, removed_empty_blocks = _strip_empty_text_blocks(body)
        if isinstance(sanitized_body, dict):
            body = sanitized_body
        if removed_empty_blocks:
            log.info("   Sanitized %s empty text blocks from /v1/messages request", removed_empty_blocks)
        headers = _build_forward_headers(req)

        model = body.get("model", "?")
        stream = body.get("stream", False)
        log.info(">> POST /v1/messages | model=%s stream=%s | -> %s", model, stream, upstream)
        if resumed:
            session_id = str(resumed.get("session_id", "")).strip()
            if session_id:
                log.info(
                    "   Auto-resume enabled for /v1/messages from session %s via %s",
                    session_id,
                    resumed.get("resume_mode", "unknown"),
                )
            else:
                log.info(
                    "   Local continue repair enabled for /v1/messages via %s",
                    resumed.get("resume_mode", "unknown"),
                )

        if not stream:
            body_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
            return await self._proxy_buffered_request(
                req,
                endpoint="/v1/messages",
                body_bytes=body_bytes,
                body=body,
                headers=headers,
                app_type="claude",
            )
        return await self._resilient_messages(req, body, headers, resumed)

    async def handle_responses(self, req: web.Request) -> web.StreamResponse:
        self.stats["total"] += 1

        upstream = self.settings_manager.current_upstream
        if not upstream:
            return web.json_response(
                {"error": {"type": "proxy_error", "message": "No upstream is available"}},
                status=503,
            )

        body = await req.json()
        self._resolve_tool_wait_from_request("/v1/responses", body)
        body, resumed = self._maybe_resume_responses(body)
        headers = _build_forward_headers(req)

        model = body.get("model", "?")
        stream = body.get("stream", False)
        log.info(">> POST /v1/responses | model=%s stream=%s | -> %s", model, stream, upstream)
        if resumed:
            log.info(
                "   Auto-resume enabled for /v1/responses from session %s via %s",
                resumed.get("session_id", ""),
                resumed.get("resume_mode", "unknown"),
            )

        if not stream:
            body_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
            return await self._proxy_buffered_request(
                req,
                endpoint="/v1/responses",
                body_bytes=body_bytes,
                body=body,
                headers=headers,
                app_type="codex",
            )
        return await self._resilient_responses(req, body, headers, resumed)

    async def handle_chat_completions(self, req: web.Request) -> web.StreamResponse:
        self.stats["total"] += 1

        upstream = self.settings_manager.current_upstream
        if not upstream:
            return web.json_response(
                {"error": {"type": "proxy_error", "message": "No upstream is available"}},
                status=503,
            )

        body = await req.json()
        self._resolve_tool_wait_from_request("/v1/chat/completions", body)
        headers = _build_forward_headers(req)

        model = body.get("model", "?")
        stream = body.get("stream", False)
        log.info(">> POST /v1/chat/completions | model=%s stream=%s | -> %s", model, stream, upstream)

        if not stream:
            body_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
            return await self._proxy_buffered_request(
                req,
                endpoint="/v1/chat/completions",
                body_bytes=body_bytes,
                body=body,
                headers=headers,
                app_type="codex",
            )
        return await self._resilient_chat_completions(req, body, headers)

    async def handle_health(self, _request: web.Request) -> web.Response:
        return web.json_response(self._build_health_payload())

    async def handle_shutdown(self, request: web.Request) -> web.Response:
        shutdown_mode = str(request.query.get("mode", "manual")).strip().lower() or "manual"
        skip_restore = _shutdown_skips_restore(shutdown_mode)
        cleanup_state = request.app.get("cleanup_state")
        if skip_restore and isinstance(cleanup_state, dict):
            cleanup_state["skip_restore"] = True
            cleanup_state["reason"] = shutdown_mode

        self.event_history.add(
            "service_takeover" if skip_restore else "service_stop",
            "Proxy handoff" if skip_restore else "Proxy stopping",
            (
                "Another Plunger instance is taking over this port; restore is skipped."
                if skip_restore
                else "Manual shutdown requested from the desktop control panel."
            ),
            tone="neutral",
            meta={"reason": shutdown_mode, "skip_restore": skip_restore},
        )
        stop_event = request.app["stop_event"]
        asyncio.get_running_loop().call_later(0.2, stop_event.set)
        return web.json_response(
            {"status": "stopping", "mode": shutdown_mode, "skip_restore": skip_restore}
        )


    def _summarize_saved_session(self, snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
        if not snapshot:
            return None

        partial_text = str(snapshot.get("partial_text", ""))
        return {
            "session_id": snapshot.get("session_id", ""),
            "endpoint": snapshot.get("endpoint", ""),
            "endpoint_label": _endpoint_label(str(snapshot.get("endpoint", ""))),
            "model": snapshot.get("model", ""),
            "status": snapshot.get("status", ""),
            "reason": snapshot.get("reason", ""),
            "started_at": snapshot.get("started_at", ""),
            "updated_at": snapshot.get("updated_at", ""),
            "response_id": snapshot.get("response_id", ""),
            "message_id": snapshot.get("message_id", ""),
            "partial_chars": len(partial_text),
            "bytes_forwarded": int(snapshot.get("bytes_forwarded", 0) or 0),
            "partial_text_preview": _shorten_text(partial_text, 120),
            "request_body_saved": snapshot.get("request_body") is not None,
            "last_user_text": snapshot.get("last_user_text", ""),
            "last_user_text_preview": _shorten_text(str(snapshot.get("last_user_text", "")), 120),
            "request_summary": snapshot.get("request_summary", ""),
        }

    def _build_health_payload(self) -> dict[str, Any]:
        active_sessions = self.recovery_store.active_sessions_snapshot()
        pending_tool_waits = self._pending_tool_waits_snapshot()
        last_interrupted = self._summarize_saved_session(
            self.recovery_store.load_last_interrupted()
        )
        return {
            "status": "ok",
            "listen": self.settings_manager.proxy_url,
            "upstream": self.settings_manager.current_upstream,
            "upstream_source": self.settings_manager.current_upstream_source,
            "started_at": self.started_at,
            "started_at_ms": self.started_at_ms,
            "stall_timeout": self.stall_timeout,
            "first_byte_timeout": self.first_byte_timeout,
            "visible_output_timeout": self.visible_output_timeout,
            "buffered_request_timeout": self.buffered_request_timeout,
            "max_request_body_bytes": self.max_request_body_bytes,
            "safe_resume_body_bytes": self.safe_resume_body_bytes,
            "responses_stall_timeout": self.stall_timeout * 2,
            "responses_first_byte_timeout": self.first_byte_timeout * 2,
            "responses_visible_output_timeout": self.visible_output_timeout * 2,
            "heartbeat_interval": self.heartbeat_interval,
            "safety_watchdog_pid": self.safety_watchdog_pid,
            "active_session": active_sessions[0] if active_sessions else None,
            "active_sessions": active_sessions,
            "active_sessions_count": len(active_sessions),
            "pending_tool_wait": pending_tool_waits[0] if pending_tool_waits else None,
            "pending_tool_waits": pending_tool_waits,
            "pending_tool_waits_count": len(pending_tool_waits),
            "last_interrupted": last_interrupted,
            "events": self.event_history.snapshot(),
            **self.stats,
        }

    def _record_disconnect(
        self,
        endpoint: str,
        state: StreamState,
        attempt: int,
        reason: str,
    ) -> None:
        detail = f"{_endpoint_label(endpoint)} | attempt {attempt + 1} | {_shorten_text(reason, 96)}"
        if state.accumulated_text:
            detail += f" | buffered {len(state.accumulated_text)} chars"
        self.event_history.add(
            "disconnect",
            "Stream interrupted",
            detail,
            endpoint=endpoint,
            model=state.model,
            tone="warning",
            meta={
                "attempt": attempt + 1,
                "buffered_chars": len(state.accumulated_text),
                "reason": reason,
                "request_summary": state.request_summary,
            },
        )

    def _trigger_recovery(
        self,
        endpoint: str,
        state: StreamState,
        attempt: int,
        reason: str,
    ) -> None:
        if state.recovery_triggered:
            return
        state.recovery_triggered = True
        self.stats["recoveries_triggered"] += 1
        self._record_disconnect(endpoint, state, attempt, reason)

    def _record_recovery(
        self,
        endpoint: str,
        state: StreamState,
        attempt: int,
        mode: str,
    ) -> None:
        if state.recovery_triggered:
            self.stats["recoveries_succeeded"] += 1
        detail = (
            f"{_endpoint_label(endpoint)} | recovered via {mode} after {attempt} retr"
            f"{'y' if attempt == 1 else 'ies'}"
        )
        if state.accumulated_text:
            detail += f" | delivered {len(state.accumulated_text)} chars"
        self.event_history.add(
            "recovered",
            "Connection recovered",
            detail,
            endpoint=endpoint,
            model=state.model,
            tone="success",
            meta={
                "attempt": attempt,
                "mode": mode,
                "delivered_chars": len(state.accumulated_text),
                "recovery_duration_ms": int((time.time() - state.start_time) * 1000),
                "request_summary": state.request_summary,
            },
        )

    def _record_failure(self, endpoint: str, state: StreamState, reason: str) -> None:
        detail = f"{_endpoint_label(endpoint)} | {_shorten_text(reason, 112)}"
        if state.accumulated_text:
            detail += f" | buffered {len(state.accumulated_text)} chars"
        self.event_history.add(
            "failed",
            "Recovery failed",
            detail,
            endpoint=endpoint,
            model=state.model,
            tone="danger",
            meta={
                "buffered_chars": len(state.accumulated_text),
                "reason": reason,
                "request_summary": state.request_summary,
            },
        )

    def _build_proxy_error_response(self, message: str, *, status: int = 504) -> web.Response:
        return web.json_response(
            {
                "error": {
                    "type": "proxy_error",
                    "message": message,
                }
            },
            status=status,
        )

    def _build_upstream_url(self, upstream: str, endpoint: str, query_string: str = "") -> str:
        url = f"{upstream}{endpoint}"
        if query_string:
            url += f"?{query_string}"
        return url

    def _infer_app_type(self, endpoint: str, body: dict[str, Any] | None = None) -> str:
        payload = body if isinstance(body, dict) else {}
        if endpoint.startswith("/v1/responses") or endpoint.startswith("/v1/chat/completions") or "input" in payload:
            return "codex"
        return "claude"

    def _begin_proxy_session(
        self,
        req: web.Request,
        endpoint: str,
        body: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], StreamState]:
        request_title = _extract_explicit_request_title(req.headers, body)
        session = self.recovery_store.begin_session(
            endpoint,
            body,
            self.settings_manager.current_upstream,
            client_label=_request_client_label(req),
            request_title=request_title,
            method=req.method,
        )
        state = StreamState(
            model=str(session.get("model", "")).strip(),
            request_summary=str(session.get("request_summary", "")).strip(),
        )
        self.recovery_store.track_active_session(session, state)
        return session, state

    async def _proxy_buffered_request(
        self,
        req: web.Request,
        *,
        endpoint: str,
        body_bytes: bytes,
        body: dict[str, Any] | None,
        headers: dict[str, str],
        app_type: str,
    ) -> web.Response:
        session, state = self._begin_proxy_session(req, endpoint, body)
        last_error = "request failed"
        last_status = 504

        attempt = 0
        while True:
            self.recovery_store.track_active_session(session, state)
            upstream, attempt_headers, attempt_route = self._resolve_attempt_target(
                endpoint,
                headers,
                attempt,
                app_type=app_type,
            )
            if not upstream:
                last_error = "no upstream is available"
                last_status = 503
                if self._should_retry(attempt):
                    self._trigger_recovery(endpoint, state, attempt, last_error)
                    attempt += 1
                    await asyncio.sleep(_retry_delay(attempt))
                    continue
                self.recovery_store.mark_interrupted(session, state, last_error)
                self._record_failure(endpoint, state, last_error)
                return self._build_proxy_error_response(last_error, status=last_status)

            if attempt > 0:
                log.warning("   RETRY #%s | %s (%s)", attempt, endpoint, attempt_route)

            url = self._build_upstream_url(upstream, endpoint, req.query_string)
            client_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None))
            request_context = client_session.request(req.method, url, data=body_bytes, headers=attempt_headers)
            upstream_response: aiohttp.ClientResponse | None = None
            try:
                upstream_response = await asyncio.wait_for(
                    request_context.__aenter__(),
                    timeout=self.buffered_request_timeout,
                )
                payload = await asyncio.wait_for(
                    upstream_response.read(),
                    timeout=self.direct_body_timeout,
                )
                content_type = str(upstream_response.headers.get("Content-Type", ""))
                _append_preview_text(state, payload, content_type)
                self.recovery_store.track_active_session(session, state)

                if upstream_response.status >= 500 and self._should_retry(attempt):
                    preview = payload.decode("utf-8", errors="replace")
                    raise _RetryableRequestError(
                        f"HTTP {upstream_response.status}: {_shorten_text(preview, 160)}",
                        status_code=502,
                    )

                response_headers = _copy_response_headers(upstream_response.headers)
                self.recovery_store.mark_completed(session, state)
                if attempt > 0:
                    self._record_recovery(endpoint, state, attempt, "request retry")
                if upstream_response.status < 500:
                    self.stats["success"] += 1
                return web.Response(
                    body=payload,
                    status=upstream_response.status,
                    headers=response_headers,
                )
            except asyncio.TimeoutError:
                if upstream_response is None:
                    error = _RetryableRequestError(
                        f"first byte timeout after {self.buffered_request_timeout:.1f}s",
                        status_code=504,
                    )
                else:
                    error = _RetryableRequestError(
                        f"response body timeout after {self.direct_body_timeout:.1f}s",
                        status_code=504,
                    )
                last_error = str(error)
                last_status = error.status_code
            except _RetryableRequestError as exc:
                last_error = str(exc)
                last_status = exc.status_code
            except aiohttp.ClientError as exc:
                if _is_downstream_disconnect(req, exc):
                    self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                    return web.Response(status=499, text="client disconnected")
                last_error = str(exc)
                last_status = 502
            except OSError as exc:
                if _is_downstream_disconnect(req, exc):
                    self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                    return web.Response(status=499, text="client disconnected")
                last_error = str(exc)
                last_status = 502
            except Exception as exc:
                if _is_downstream_disconnect(req, exc):
                    self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                    return web.Response(status=499, text="client disconnected")
                last_error = f"{type(exc).__name__}: {exc}"
                last_status = 502
            finally:
                with suppress(Exception):
                    await request_context.__aexit__(None, None, None)
                with suppress(Exception):
                    await client_session.close()

            if self._should_retry(attempt):
                self._trigger_recovery(endpoint, state, attempt, last_error)
                attempt += 1
                await asyncio.sleep(_retry_delay(attempt))
                continue

            self.recovery_store.mark_interrupted(session, state, last_error)
            self._record_failure(endpoint, state, last_error)
            return self._build_proxy_error_response(last_error, status=last_status)

    async def _proxy_streaming_request(
        self,
        req: web.Request,
        *,
        endpoint: str,
        body_bytes: bytes,
        body: dict[str, Any] | None,
        headers: dict[str, str],
        app_type: str,
    ) -> web.StreamResponse:
        session, state = self._begin_proxy_session(req, endpoint, body)
        last_error = "request failed"
        last_status = 504

        attempt = 0
        while True:
            self.recovery_store.track_active_session(session, state)
            upstream, attempt_headers, attempt_route = self._resolve_attempt_target(
                endpoint,
                headers,
                attempt,
                app_type=app_type,
            )
            if not upstream:
                last_error = "no upstream is available"
                last_status = 503
                if self._should_retry(attempt):
                    self._trigger_recovery(endpoint, state, attempt, last_error)
                    attempt += 1
                    await asyncio.sleep(_retry_delay(attempt))
                    continue
                self.recovery_store.mark_interrupted(session, state, last_error)
                self._record_failure(endpoint, state, last_error)
                return self._build_proxy_error_response(last_error, status=last_status)

            if attempt > 0:
                log.warning("   RETRY #%s | %s (%s)", attempt, endpoint, attempt_route)

            url = self._build_upstream_url(upstream, endpoint, req.query_string)
            client_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None))
            request_context = client_session.request(req.method, url, data=body_bytes, headers=attempt_headers)
            upstream_response: aiohttp.ClientResponse | None = None
            downstream: web.StreamResponse | None = None
            try:
                upstream_response = await asyncio.wait_for(
                    request_context.__aenter__(),
                    timeout=self.first_byte_timeout,
                )

                if upstream_response.status >= 400:
                    payload = await asyncio.wait_for(
                        upstream_response.read(),
                        timeout=self.direct_body_timeout,
                    )
                    content_type = str(upstream_response.headers.get("Content-Type", ""))
                    _append_preview_text(state, payload, content_type)
                    self.recovery_store.track_active_session(session, state)
                    if upstream_response.status >= 500 and self._should_retry(attempt):
                        preview = payload.decode("utf-8", errors="replace")
                        raise _RetryableRequestError(
                            f"HTTP {upstream_response.status}: {_shorten_text(preview, 160)}",
                            status_code=502,
                        )
                    response_headers = _copy_response_headers(upstream_response.headers)
                    self.recovery_store.mark_completed(session, state)
                    if attempt > 0:
                        self._record_recovery(endpoint, state, attempt, "request retry")
                    if upstream_response.status < 500:
                        self.stats["success"] += 1
                    return web.Response(
                        body=payload,
                        status=upstream_response.status,
                        headers=response_headers,
                    )

                content_type = str(upstream_response.headers.get("Content-Type", ""))
                try:
                    first_chunk = await asyncio.wait_for(
                        upstream_response.content.readany(),
                        timeout=self.first_byte_timeout,
                    )
                except asyncio.TimeoutError as exc:
                    raise _RetryableRequestError(
                        f"first byte timeout after {self.first_byte_timeout:.1f}s",
                        status_code=504,
                    ) from exc

                if not first_chunk:
                    response_headers = _copy_response_headers(upstream_response.headers)
                    self.recovery_store.mark_completed(session, state)
                    if attempt > 0:
                        self._record_recovery(endpoint, state, attempt, "request retry")
                    self.stats["success"] += 1
                    return web.Response(
                        body=b"",
                        status=upstream_response.status,
                        headers=response_headers,
                    )

                _append_preview_text(state, first_chunk, content_type)
                self.recovery_store.track_active_session(session, state)

                downstream = web.StreamResponse(
                    status=upstream_response.status,
                    headers=_copy_response_headers(upstream_response.headers, streaming=True),
                )
                await downstream.prepare(req)
                await downstream.write(first_chunk)

                while True:
                    try:
                        chunk = await asyncio.wait_for(
                            upstream_response.content.readany(),
                            timeout=self.stall_timeout,
                        )
                    except asyncio.TimeoutError as exc:
                        raise _StreamTimeout(self.stall_timeout, "stall") from exc

                    if not chunk:
                        break

                    _append_preview_text(state, chunk, content_type)
                    self.recovery_store.track_active_session(session, state)
                    await downstream.write(chunk)

                with suppress(Exception):
                    await downstream.write_eof()

                self.recovery_store.mark_completed(session, state)
                if attempt > 0:
                    self._record_recovery(endpoint, state, attempt, "request retry")
                self.stats["success"] += 1
                return downstream
            except _RetryableRequestError as exc:
                last_error = str(exc)
                last_status = exc.status_code
            except (_StreamTimeout, asyncio.TimeoutError) as exc:
                last_error = str(exc)
                last_status = 504
                if downstream is not None:
                    self.recovery_store.mark_interrupted(session, state, last_error)
                    self._record_failure(endpoint, state, last_error)
                    with suppress(Exception):
                        await downstream.write_eof()
                    return downstream
            except aiohttp.ClientError as exc:
                if _is_downstream_disconnect(req, exc):
                    self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                    if downstream is not None:
                        with suppress(Exception):
                            await downstream.write_eof()
                        return downstream
                    return web.Response(status=499, text="client disconnected")
                last_error = str(exc)
                last_status = 502
                if downstream is not None:
                    self.recovery_store.mark_interrupted(session, state, last_error)
                    self._record_failure(endpoint, state, last_error)
                    with suppress(Exception):
                        await downstream.write_eof()
                    return downstream
            except OSError as exc:
                if _is_downstream_disconnect(req, exc):
                    self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                    if downstream is not None:
                        with suppress(Exception):
                            await downstream.write_eof()
                        return downstream
                    return web.Response(status=499, text="client disconnected")
                last_error = str(exc)
                last_status = 502
                if downstream is not None:
                    self.recovery_store.mark_interrupted(session, state, last_error)
                    self._record_failure(endpoint, state, last_error)
                    with suppress(Exception):
                        await downstream.write_eof()
                    return downstream
            except Exception as exc:
                if _is_downstream_disconnect(req, exc):
                    self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                    if downstream is not None:
                        with suppress(Exception):
                            await downstream.write_eof()
                        return downstream
                    return web.Response(status=499, text="client disconnected")
                last_error = f"{type(exc).__name__}: {exc}"
                last_status = 502
                if downstream is not None:
                    self.recovery_store.mark_interrupted(session, state, last_error)
                    self._record_failure(endpoint, state, last_error)
                    with suppress(Exception):
                        await downstream.write_eof()
                    return downstream
            finally:
                with suppress(Exception):
                    await request_context.__aexit__(None, None, None)
                with suppress(Exception):
                    await client_session.close()

            if self._should_retry(attempt):
                self._trigger_recovery(endpoint, state, attempt, last_error)
                attempt += 1
                await asyncio.sleep(_retry_delay(attempt))
                continue

            self.recovery_store.mark_interrupted(session, state, last_error)
            self._record_failure(endpoint, state, last_error)
            return self._build_proxy_error_response(last_error, status=last_status)

    def _maybe_resume_messages(self, body: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
        last_interrupted = self.recovery_store.load_last_interrupted("/v1/messages")
        if not last_interrupted:
            return self._maybe_repair_messages_dangling_tool_use(body)

        if body.get("model") and last_interrupted.get("model") and body["model"] != last_interrupted["model"]:
            return self._maybe_repair_messages_dangling_tool_use(body)

        last_user_text = _extract_last_messages_user_text(body.get("messages"))
        if not _looks_like_continue_prompt(last_user_text):
            return body, None

        partial_text = str(last_interrupted.get("partial_text", "")).strip()
        if partial_text:
            messages = deepcopy(body.get("messages", []))
            if self._messages_already_include_assistant(messages, partial_text):
                return body, None

            merged = self._maybe_extend_resume_assistant_message(body, partial_text)
            if merged is not None:
                resumed = dict(last_interrupted)
                resumed["resume_mode"] = "assistant_partial"
                return merged, resumed

            patched = self._with_messages_assistant_prefill(
                body,
                partial_text,
                insert_before_final_user=True,
                reason="auto-resume prefill",
            )
            if not patched:
                return body, None
            resumed = dict(last_interrupted)
            resumed["resume_mode"] = "assistant_partial"
            return patched, resumed

        patched = self._rebuild_messages_resume_request(last_interrupted, body)
        if not patched:
            return self._maybe_repair_messages_dangling_tool_use(body)
        if not self._allow_proxy_body_growth(
            "/v1/messages",
            body,
            patched,
            reason="auto-resume request replay",
        ):
            return self._maybe_repair_messages_dangling_tool_use(body)

        resumed = dict(last_interrupted)
        resumed["resume_mode"] = "request_body"
        return patched, resumed

    def _maybe_resume_responses(self, body: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
        last_interrupted = self.recovery_store.load_last_interrupted("/v1/responses")
        if not last_interrupted:
            return body, None

        if body.get("previous_response_id"):
            return body, None

        if body.get("model") and last_interrupted.get("model") and body["model"] != last_interrupted["model"]:
            return body, None

        last_user_text = _extract_last_responses_user_text(body.get("input"))
        if not _looks_like_continue_prompt(last_user_text):
            return body, None

        response_id = str(last_interrupted.get("response_id", "")).strip()
        if response_id:
            patched = deepcopy(body)
            patched["previous_response_id"] = response_id
            if not self._allow_proxy_body_growth(
                "/v1/responses",
                body,
                patched,
                reason="responses previous_response_id replay",
            ):
                return body, None
            resumed = dict(last_interrupted)
            resumed["resume_mode"] = "previous_response_id"
            return patched, resumed

        patched = self._rebuild_responses_resume_request(last_interrupted, body)
        if not patched:
            return body, None
        if not self._allow_proxy_body_growth(
            "/v1/responses",
            body,
            patched,
            reason="responses request replay",
        ):
            return body, None

        resumed = dict(last_interrupted)
        resumed["resume_mode"] = "request_body"
        return patched, resumed

    def _build_resume_instruction(
        self,
        last_interrupted: dict[str, Any],
        *,
        no_visible_output: bool,
    ) -> str:
        fragments = [
            "Your previous attempt to answer my immediately preceding request was interrupted before the reply finished.",
        ]
        if no_visible_output:
            fragments.append("No visible assistant text reached the client from that interrupted attempt.")
        fragments.extend(
            [
                "Continue the same task from the current workspace state instead of restarting from scratch.",
                "Do not repeat steps that are already complete.",
                "Provide only the next useful continuation.",
            ]
        )
        return " ".join(fragments)

    def _build_tool_handoff_resume_instruction(self, tool_names: list[str] | None = None) -> str:
        fragments = [
            "Your previous response ended while a local tool handoff was still pending.",
        ]
        normalized_tool_names = [str(name).strip() for name in (tool_names or []) if str(name).strip()]
        if normalized_tool_names:
            fragments.append(
                f"The interrupted handoff involved: {', '.join(normalized_tool_names[:3])}."
            )
        fragments.extend(
            [
                "Continue the same task from the current workspace state instead of restarting from scratch.",
                "Do not repeat steps that are already complete.",
                "Provide only the next useful continuation.",
            ]
        )
        return " ".join(fragments)

    def _append_messages_user_prompt(self, messages: list[Any], prompt: str) -> list[Any]:
        patched = deepcopy(messages)
        if patched and isinstance(patched[-1], dict) and str(patched[-1].get("role", "")).strip().lower() == "user":
            last_message = deepcopy(patched[-1])
            content = last_message.get("content")
            if isinstance(content, str):
                existing = content.rstrip()
                last_message["content"] = f"{existing}\n\n{prompt}" if existing else prompt
                patched[-1] = last_message
                return patched
            if isinstance(content, list):
                blocks = deepcopy(content)
                blocks.append({"type": "text", "text": prompt})
                last_message["content"] = blocks
                patched[-1] = last_message
                return patched

        patched.append({"role": "user", "content": prompt})
        return patched

    def _maybe_repair_messages_dangling_tool_use(
        self,
        body: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        last_user_text = _extract_last_messages_user_text(body.get("messages"))
        if not _looks_like_continue_prompt(last_user_text):
            return body, None

        dangling = _find_messages_dangling_tool_use(body.get("messages"))
        if not dangling:
            return body, None

        messages = body.get("messages")
        if not isinstance(messages, list):
            return body, None

        assistant_index = int(dangling.get("assistant_index", -1))
        if assistant_index < 0 or assistant_index >= len(messages):
            return body, None

        patched_messages = deepcopy(messages[:assistant_index])
        preserved_assistant = _remove_messages_tool_use_blocks(messages[assistant_index])
        if preserved_assistant is not None:
            patched_messages.append(preserved_assistant)

        recovery_prompt = self._build_tool_handoff_resume_instruction(
            dangling.get("tool_names", []),
        )
        patched_messages = self._append_messages_user_prompt(patched_messages, recovery_prompt)

        patched = deepcopy(body)
        patched["messages"] = patched_messages
        if not self._allow_proxy_body_growth(
            "/v1/messages",
            body,
            patched,
            reason="dangling tool_use continue repair",
        ):
            return body, None

        return patched, {
            "resume_mode": "dangling_tool_use_continue",
            "tool_use_ids": list(dangling.get("tool_use_ids", [])),
            "tool_names": list(dangling.get("tool_names", [])),
        }

    def _rebuild_messages_resume_request(
        self,
        last_interrupted: dict[str, Any],
        current_body: dict[str, Any],
    ) -> dict[str, Any] | None:
        request_body = last_interrupted.get("request_body")
        if not isinstance(request_body, dict):
            return None

        messages = request_body.get("messages")
        if not isinstance(messages, list):
            return None

        patched = deepcopy(request_body)
        patched["messages"] = deepcopy(messages)
        patched["messages"].append(
            {
                "role": "user",
                "content": self._build_resume_instruction(last_interrupted, no_visible_output=True),
            }
        )
        if "stream" in current_body:
            patched["stream"] = current_body["stream"]
        return patched

    def _rebuild_responses_resume_request(
        self,
        last_interrupted: dict[str, Any],
        current_body: dict[str, Any],
    ) -> dict[str, Any] | None:
        request_body = last_interrupted.get("request_body")
        if not isinstance(request_body, dict) or "input" not in request_body:
            return None

        patched = deepcopy(request_body)
        patched["input"] = self._append_responses_recovery_input(
            request_body.get("input"),
            self._build_resume_instruction(last_interrupted, no_visible_output=True),
        )
        if "stream" in current_body:
            patched["stream"] = current_body["stream"]
        return patched

    def _append_responses_recovery_input(self, input_value: Any, prompt: str) -> Any:
        recovery_message = {
            "role": "user",
            "content": [{"type": "input_text", "text": prompt}],
        }
        if isinstance(input_value, str):
            existing = input_value.rstrip()
            return f"{existing}\n\n{prompt}" if existing else prompt

        if isinstance(input_value, list):
            patched = deepcopy(input_value)
            patched.append(recovery_message)
            return patched

        if isinstance(input_value, dict):
            return [deepcopy(input_value), recovery_message]

        return [recovery_message]

    def _maybe_extend_resume_assistant_message(
        self,
        body: dict[str, Any],
        partial_text: str,
    ) -> dict[str, Any] | None:
        messages = body.get("messages")
        if not isinstance(messages, list) or len(messages) < 2:
            return None

        candidate_index = len(messages) - 1
        final_message = messages[-1]
        if isinstance(final_message, dict) and final_message.get("role") == "user":
            candidate_index -= 1
        if candidate_index < 0:
            return None

        candidate = messages[candidate_index]
        if not isinstance(candidate, dict) or candidate.get("role") != "assistant":
            return None

        existing = _extract_text_from_content(candidate.get("content"))
        if not existing.strip():
            return None
        if not partial_text.startswith(existing):
            return None

        missing_suffix = partial_text[len(existing):]
        if not missing_suffix:
            return deepcopy(body)

        patched = deepcopy(body)
        patched_messages = deepcopy(messages)
        patched_message = dict(patched_messages[candidate_index])
        patched_message["content"] = _append_text_to_content(
            patched_message.get("content"),
            missing_suffix,
        )
        patched_messages[candidate_index] = patched_message
        patched["messages"] = patched_messages
        if not self._allow_proxy_body_growth(
            "/v1/messages",
            body,
            patched,
            reason="auto-resume assistant fill",
        ):
            return None
        return patched

    def _messages_already_include_assistant(self, messages: list[Any], partial_text: str) -> bool:
        if not partial_text:
            return True

        for message in messages:
            if not isinstance(message, dict) or message.get("role") != "assistant":
                continue
            existing = _extract_text_from_content(message.get("content"))
            if not existing.strip():
                continue
            if existing == partial_text or existing.startswith(partial_text):
                return True

        return False

    async def handle_catchall(self, req: web.Request) -> web.StreamResponse:
        self.stats["total"] += 1
        upstream = self.settings_manager.current_upstream
        if not upstream:
            return web.json_response(
                {"error": {"type": "proxy_error", "message": "No upstream is available"}},
                status=503,
            )

        body = await req.read()
        headers = dict(req.headers)
        headers.pop("host", None)
        parsed_body = _try_parse_json_payload(body, str(req.headers.get("Content-Type", "")))
        app_type = self._infer_app_type(req.path, parsed_body)

        return await self._proxy_streaming_request(
            req,
            endpoint=req.path,
            body_bytes=body,
            body=parsed_body,
            headers=headers,
            app_type=app_type,
        )

    async def _direct(
        self,
        upstream: str,
        endpoint: str,
        body: dict[str, Any],
        headers: dict[str, str],
    ) -> web.Response:
        url = f"{upstream}{endpoint}"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body, headers=headers) as response:
                payload = await response.read()
                self.stats["success"] += 1
                return web.Response(
                    body=payload,
                    status=response.status,
                    content_type=response.content_type,
                )

    async def _resilient_messages(
        self,
        req: web.Request,
        body: dict[str, Any],
        headers: dict[str, str],
        resumed: dict[str, Any] | None,
    ) -> web.StreamResponse:
        state = StreamState()
        original_messages = deepcopy(body.get("messages", []))
        request_title = _extract_explicit_request_title(req.headers, body)
        session = self.recovery_store.begin_session(
            "/v1/messages",
            body,
            self.settings_manager.current_upstream,
            resumed,
            client_label=_request_client_label(req),
            request_title=request_title,
        )
        state.request_summary = str(session.get("request_summary", "")).strip()
        self.recovery_store.track_active_session(session, state)

        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            }
        )
        await response.prepare(req)
        client_stream = self._make_client_stream(response, "/v1/messages")
        client_stream.start()
        try:
            attempt = 0
            prefill_disabled = False
            while True:
                try:
                    client_stream.raise_if_disconnected()
                    upstream, attempt_headers, attempt_route = self._resolve_attempt_target(
                        "/v1/messages",
                        headers,
                        attempt,
                        app_type="claude",
                    )
                    if not upstream:
                        self._trigger_recovery("/v1/messages", state, attempt, "no upstream is available")
                        attempt += 1
                        await asyncio.sleep(_retry_delay(attempt))
                        continue

                    attempt_body = body
                    if attempt > 0:
                        state.is_first_attempt = False
                        state.skip_prefix = state.accumulated_text
                        self.stats["retry_attempts"] += 1
                        attempt_body = deepcopy(body)
                        attempt_body["messages"] = deepcopy(original_messages)
                        if not prefill_disabled and _has_non_whitespace_text(state.accumulated_text):
                            maybe_prefilled_body = self._with_messages_assistant_prefill(
                                attempt_body,
                                state.accumulated_text,
                                insert_before_final_user=False,
                                reason=f"retry #{attempt} prefill",
                            )
                            if maybe_prefilled_body is not None:
                                attempt_body = maybe_prefilled_body
                            else:
                                prefill_disabled = True
                                log.warning("   RETRY #%s prefill skipped to avoid oversized upstream request", attempt)
                        elif state.accumulated_text and not prefill_disabled:
                            log.warning("   RETRY #%s has whitespace-only partial text; retrying without assistant prefix", attempt)
                        log.warning(
                            "   RETRY #%s | %s chars accumulated | -> %s (%s)%s",
                            attempt,
                            len(state.accumulated_text),
                            upstream,
                            attempt_route,
                            " (prefill disabled)" if prefill_disabled else "",
                        )
                        await asyncio.sleep(_retry_delay(attempt))

                    state.last_visible_output_at = time.monotonic()
                    if await self._stream_messages(upstream, attempt_body, attempt_headers, state, client_stream, session):
                        self.stats["success"] += 1
                        self.recovery_store.mark_completed(session, state)
                        self._register_pending_tool_wait("/v1/messages", session, state)
                        if attempt > 0:
                            self._record_recovery("/v1/messages", state, attempt, "stream retry")
                        elapsed = time.time() - state.start_time
                        log.info(
                            "<< OK | %s chars | %s retries | %.1fs",
                            len(state.accumulated_text),
                            attempt,
                            elapsed,
                        )
                        return response
                except _StreamTimeout as exc:
                    log.warning("   STALL: %s", exc)
                    self._trigger_recovery("/v1/messages", state, attempt, str(exc))
                except asyncio.TimeoutError:
                    log.warning("   STALL: %.1fs with no data", self.stall_timeout)
                    self._trigger_recovery("/v1/messages", state, attempt, f"stall after {self.stall_timeout:.1f}s")
                except aiohttp.ClientError as exc:
                    if _is_downstream_disconnect(req, exc, client_stream):
                        self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                        log.info("   CLIENT CLOSED /v1/messages: %s", exc)
                        return response
                    log.warning("   CONN ERROR: %s", exc)
                    self._trigger_recovery("/v1/messages", state, attempt, str(exc))
                except OSError as exc:
                    if _is_downstream_disconnect(req, exc, client_stream):
                        self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                        log.info("   CLIENT CLOSED /v1/messages: %s", exc)
                        return response
                    log.warning("   CONN ERROR: %s", exc)
                    self._trigger_recovery("/v1/messages", state, attempt, str(exc))
                except _UpstreamClientError as exc:
                    if _is_prefill_rejection(exc) and not prefill_disabled:
                        prefill_disabled = True
                        log.warning("   PREFILL REJECTED: %s — disabling prefill and retrying", exc)
                    else:
                        log.error("   CLIENT ERROR: %s", exc)
                        self.recovery_store.mark_interrupted(session, state, f"client_error: {exc}")
                        self._record_failure("/v1/messages", state, f"client error: {exc}")
                        await client_stream.write_event(
                            "error",
                            json.dumps(
                                {
                                    "type": "error",
                                    "error": {"type": "api_error", "message": str(exc)},
                                }
                            ),
                        )
                        return response
                except Exception as exc:
                    if _is_downstream_disconnect(req, exc, client_stream):
                        self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                        log.info("   CLIENT CLOSED /v1/messages: %s", exc)
                        return response
                    log.error("   ERROR: %s: %s", type(exc).__name__, exc)
                    self.recovery_store.mark_interrupted(session, state, f"proxy_error: {type(exc).__name__}: {exc}")
                    self._record_failure("/v1/messages", state, f"{type(exc).__name__}: {exc}")
                    payload = json.dumps(
                        {
                            "type": "error",
                            "error": {
                                "type": "proxy_error",
                                "message": f"{type(exc).__name__}: {exc}",
                            },
                        }
                    )
                    await client_stream.write_event("error", payload)
                    return response

                attempt += 1
        finally:
            await client_stream.close()

    async def _resilient_responses(
        self,
        req: web.Request,
        body: dict[str, Any],
        headers: dict[str, str],
        resumed: dict[str, Any] | None,
    ) -> web.StreamResponse:
        state = StreamState()
        original_body = deepcopy(body)
        request_title = _extract_explicit_request_title(req.headers, body)
        session = self.recovery_store.begin_session(
            "/v1/responses",
            body,
            self.settings_manager.current_upstream,
            resumed,
            client_label=_request_client_label(req),
            request_title=request_title,
        )
        state.request_summary = str(session.get("request_summary", "")).strip()
        self.recovery_store.track_active_session(session, state)

        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            }
        )
        await response.prepare(req)
        client_stream = self._make_client_stream(response, "/v1/responses")
        client_stream.start()
        try:
            attempt = 0
            while True:
                try:
                    client_stream.raise_if_disconnected()
                    upstream, attempt_headers, attempt_route = self._resolve_attempt_target(
                        "/v1/responses",
                        headers,
                        attempt,
                        app_type="codex",
                    )
                    if not upstream:
                        self._trigger_recovery("/v1/responses", state, attempt, "no upstream is available")
                        attempt += 1
                        await asyncio.sleep(_retry_delay(attempt))
                        continue

                    attempt_body = deepcopy(original_body)
                    if attempt > 0:
                        state.is_first_attempt = False
                        state.skip_prefix = state.accumulated_text
                        self.stats["retry_attempts"] += 1
                        log.warning(
                            "   RETRY #%s | %s chars accumulated | response_id=%s | -> %s (%s)",
                            attempt,
                            len(state.accumulated_text),
                            state.response_id or "<none>",
                            upstream,
                            attempt_route,
                        )
                        await asyncio.sleep(_retry_delay(attempt))

                    state.last_visible_output_at = time.monotonic()
                    if await self._stream_responses(upstream, attempt_body, attempt_headers, state, client_stream, session):
                        self.stats["success"] += 1
                        self.recovery_store.mark_completed(session, state)
                        self._register_pending_tool_wait("/v1/responses", session, state)
                        if attempt > 0:
                            self._record_recovery("/v1/responses", state, attempt, "stream retry")
                        elapsed = time.time() - state.start_time
                        log.info(
                            "<< OK | %s chars | %s retries | %.1fs",
                            len(state.accumulated_text),
                            attempt,
                            elapsed,
                        )
                        return response
                except _StreamTimeout as exc:
                    log.warning("   STALL: %s", exc)
                    self._trigger_recovery("/v1/responses", state, attempt, str(exc))
                except asyncio.TimeoutError:
                    log.warning("   STALL: %.1fs with no data", self.stall_timeout)
                    self._trigger_recovery("/v1/responses", state, attempt, f"stall after {self.stall_timeout:.1f}s")
                except aiohttp.ClientError as exc:
                    if _is_downstream_disconnect(req, exc, client_stream):
                        self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                        log.info("   CLIENT CLOSED /v1/responses: %s", exc)
                        return response
                    log.warning("   CONN ERROR: %s", exc)
                    self._trigger_recovery("/v1/responses", state, attempt, str(exc))
                except OSError as exc:
                    if _is_downstream_disconnect(req, exc, client_stream):
                        self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                        log.info("   CLIENT CLOSED /v1/responses: %s", exc)
                        return response
                    log.warning("   CONN ERROR: %s", exc)
                    self._trigger_recovery("/v1/responses", state, attempt, str(exc))
                except _UpstreamClientError as exc:
                    log.error("   CLIENT ERROR: %s", exc)
                    self.recovery_store.mark_interrupted(session, state, f"client_error: {exc}")
                    self._record_failure("/v1/responses", state, f"client error: {exc}")
                    await client_stream.write_event(
                        "error",
                        json.dumps(
                            {
                                "type": "error",
                                "error": {"type": "api_error", "message": str(exc)},
                            }
                        ),
                    )
                    return response
                except Exception as exc:
                    if _is_downstream_disconnect(req, exc, client_stream):
                        self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                        log.info("   CLIENT CLOSED /v1/responses: %s", exc)
                        return response
                    log.error("   ERROR: %s: %s", type(exc).__name__, exc)
                    self.recovery_store.mark_interrupted(session, state, f"proxy_error: {type(exc).__name__}: {exc}")
                    self._record_failure("/v1/responses", state, f"{type(exc).__name__}: {exc}")
                    payload = json.dumps(
                        {
                            "type": "error",
                            "error": {
                                "type": "proxy_error",
                                "message": f"{type(exc).__name__}: {exc}",
                            },
                        }
                    )
                    await client_stream.write_event("error", payload)
                    return response

                if await self._recover_response_tail(upstream, attempt_headers, state, client_stream):
                    self.stats["success"] += 1
                    self.recovery_store.mark_completed(session, state)
                    self._record_recovery("/v1/responses", state, attempt, "tail fetch")
                    elapsed = time.time() - state.start_time
                    log.info(
                        "<< RECOVERED | %s chars | %s retries | %.1fs",
                        len(state.accumulated_text),
                        attempt,
                        elapsed,
                    )
                    return response

                attempt += 1
        finally:
            await client_stream.close()

    async def _resilient_chat_completions(
        self,
        req: web.Request,
        body: dict[str, Any],
        headers: dict[str, str],
    ) -> web.StreamResponse:
        state = StreamState()
        original_messages = deepcopy(body.get("messages", []))
        request_title = _extract_explicit_request_title(req.headers, body)
        session = self.recovery_store.begin_session(
            "/v1/chat/completions",
            body,
            self.settings_manager.current_upstream,
            client_label=_request_client_label(req),
            request_title=request_title,
        )
        state.request_summary = str(session.get("request_summary", "")).strip()
        self.recovery_store.track_active_session(session, state)

        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            }
        )
        await response.prepare(req)
        client_stream = self._make_client_stream(response, "/v1/chat/completions")
        client_stream.start()
        try:
            attempt = 0
            prefill_disabled = False
            while True:
                try:
                    client_stream.raise_if_disconnected()
                    upstream, attempt_headers, attempt_route = self._resolve_attempt_target(
                        "/v1/chat/completions",
                        headers,
                        attempt,
                        app_type="codex",
                    )
                    if not upstream:
                        self._trigger_recovery("/v1/chat/completions", state, attempt, "no upstream is available")
                        attempt += 1
                        await asyncio.sleep(_retry_delay(attempt))
                        continue

                    attempt_body = body
                    if attempt > 0:
                        state.is_first_attempt = False
                        state.skip_prefix = state.accumulated_text
                        self.stats["retry_attempts"] += 1
                        attempt_body = deepcopy(body)
                        attempt_body["messages"] = deepcopy(original_messages)
                        if not prefill_disabled and _has_non_whitespace_text(state.accumulated_text):
                            maybe_prefilled_body = self._with_chat_completions_assistant_prefill(
                                attempt_body,
                                state.accumulated_text,
                                reason=f"retry #{attempt} prefill",
                            )
                            if maybe_prefilled_body is not None:
                                attempt_body = maybe_prefilled_body
                            else:
                                prefill_disabled = True
                                log.warning("   RETRY #%s prefill skipped to avoid oversized upstream request", attempt)
                        elif state.accumulated_text and not prefill_disabled:
                            log.warning("   RETRY #%s has whitespace-only partial text; retrying without assistant prefix", attempt)
                        log.warning(
                            "   RETRY #%s | %s chars accumulated | -> %s (%s)%s",
                            attempt,
                            len(state.accumulated_text),
                            upstream,
                            attempt_route,
                            " (prefill disabled)" if prefill_disabled else "",
                        )
                        await asyncio.sleep(_retry_delay(attempt))

                    state.last_visible_output_at = time.monotonic()
                    if await self._stream_chat_completions(upstream, attempt_body, attempt_headers, state, client_stream, session):
                        self.stats["success"] += 1
                        self.recovery_store.mark_completed(session, state)
                        self._register_pending_tool_wait("/v1/chat/completions", session, state)
                        if attempt > 0:
                            self._record_recovery("/v1/chat/completions", state, attempt, "stream retry")
                        elapsed = time.time() - state.start_time
                        log.info(
                            "<< OK | %s chars | %s retries | %.1fs",
                            len(state.accumulated_text),
                            attempt,
                            elapsed,
                        )
                        return response
                except _StreamTimeout as exc:
                    log.warning("   STALL: %s", exc)
                    self._trigger_recovery("/v1/chat/completions", state, attempt, str(exc))
                except asyncio.TimeoutError:
                    log.warning("   STALL: %.1fs with no data", self.stall_timeout)
                    self._trigger_recovery("/v1/chat/completions", state, attempt, f"stall after {self.stall_timeout:.1f}s")
                except aiohttp.ClientError as exc:
                    if _is_downstream_disconnect(req, exc, client_stream):
                        self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                        log.info("   CLIENT CLOSED /v1/chat/completions: %s", exc)
                        return response
                    log.warning("   CONN ERROR: %s", exc)
                    self._trigger_recovery("/v1/chat/completions", state, attempt, str(exc))
                except OSError as exc:
                    if _is_downstream_disconnect(req, exc, client_stream):
                        self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                        log.info("   CLIENT CLOSED /v1/chat/completions: %s", exc)
                        return response
                    log.warning("   CONN ERROR: %s", exc)
                    self._trigger_recovery("/v1/chat/completions", state, attempt, str(exc))
                except _UpstreamClientError as exc:
                    if _is_prefill_rejection(exc) and not prefill_disabled:
                        prefill_disabled = True
                        log.warning("   PREFILL REJECTED: %s - disabling prefill and retrying", exc)
                    else:
                        log.error("   CLIENT ERROR: %s", exc)
                        self.recovery_store.mark_interrupted(session, state, f"client_error: {exc}")
                        self._record_failure("/v1/chat/completions", state, f"client error: {exc}")
                        await client_stream.write_event(
                            "",
                            json.dumps(
                                {
                                    "error": {
                                        "message": str(exc),
                                        "type": "api_error",
                                    }
                                }
                            ),
                        )
                        return response
                except Exception as exc:
                    if _is_downstream_disconnect(req, exc, client_stream):
                        self.recovery_store.mark_interrupted(session, state, "client_disconnected")
                        log.info("   CLIENT CLOSED /v1/chat/completions: %s", exc)
                        return response
                    log.error("   ERROR: %s: %s", type(exc).__name__, exc)
                    self.recovery_store.mark_interrupted(session, state, f"proxy_error: {type(exc).__name__}: {exc}")
                    self._record_failure("/v1/chat/completions", state, f"{type(exc).__name__}: {exc}")
                    await client_stream.write_event(
                        "",
                        json.dumps(
                            {
                                "error": {
                                    "message": f"{type(exc).__name__}: {exc}",
                                    "type": "proxy_error",
                                }
                            }
                        ),
                    )
                    return response

                attempt += 1
        finally:
            await client_stream.close()

    async def _stream_messages(
        self,
        upstream: str,
        body: dict[str, Any],
        headers: dict[str, str],
        state: StreamState,
        client_stream: SSEStreamWriter,
        recovery_session: dict[str, Any],
    ) -> bool:
        url = f"{upstream}/v1/messages"
        timeout = aiohttp.ClientTimeout(total=None)

        async with aiohttp.ClientSession(timeout=timeout) as client_session:
            async with client_session.post(url, json=body, headers=headers) as response:
                if 400 <= response.status < 500:
                    text = await response.text()
                    raise _UpstreamClientError(_summarize_upstream_error(response.status, text))

                if response.status >= 500:
                    text = await response.text()
                    raise aiohttp.ClientError(f"HTTP {response.status}: {text[:200]}")

                await self._relay_sse_stream(
                    response,
                    state,
                    client_stream,
                    recovery_session,
                    self._on_event,
                    first_byte_timeout=self._messages_stream_timeouts()[0],
                    stall_timeout=self._messages_stream_timeouts()[1],
                    visible_output_timeout=self._messages_stream_timeouts()[2],
                )

        if not state.completed:
            raise aiohttp.ClientError("stream closed before message_stop")

        return True

    async def _stream_responses(
        self,
        upstream: str,
        body: dict[str, Any],
        headers: dict[str, str],
        state: StreamState,
        client_stream: SSEStreamWriter,
        recovery_session: dict[str, Any],
    ) -> bool:
        url = f"{upstream}/v1/responses"
        timeout = aiohttp.ClientTimeout(total=None)

        async with aiohttp.ClientSession(timeout=timeout) as client_session:
            async with client_session.post(url, json=body, headers=headers) as response:
                if 400 <= response.status < 500:
                    text = await response.text()
                    raise _UpstreamClientError(_summarize_upstream_error(response.status, text))

                if response.status >= 500:
                    text = await response.text()
                    raise aiohttp.ClientError(f"HTTP {response.status}: {text[:200]}")

                await self._relay_sse_stream(
                    response,
                    state,
                    client_stream,
                    recovery_session,
                    self._on_responses_event,
                    first_byte_timeout=self._responses_stream_timeouts()[0],
                    stall_timeout=self._responses_stream_timeouts()[1],
                    visible_output_timeout=self._responses_stream_timeouts()[2],
                )

        if not state.completed:
            raise aiohttp.ClientError("stream closed before response.completed")

        return True

    async def _stream_chat_completions(
        self,
        upstream: str,
        body: dict[str, Any],
        headers: dict[str, str],
        state: StreamState,
        client_stream: SSEStreamWriter,
        recovery_session: dict[str, Any],
    ) -> bool:
        url = f"{upstream}/v1/chat/completions"
        timeout = aiohttp.ClientTimeout(total=None)

        async with aiohttp.ClientSession(timeout=timeout) as client_session:
            async with client_session.post(url, json=body, headers=headers) as response:
                if 400 <= response.status < 500:
                    text = await response.text()
                    raise _UpstreamClientError(_summarize_upstream_error(response.status, text))

                if response.status >= 500:
                    text = await response.text()
                    raise aiohttp.ClientError(f"HTTP {response.status}: {text[:200]}")

                await self._relay_sse_stream(
                    response,
                    state,
                    client_stream,
                    recovery_session,
                    self._on_chat_completions_event,
                    first_byte_timeout=self._responses_stream_timeouts()[0],
                    stall_timeout=self._responses_stream_timeouts()[1],
                    visible_output_timeout=self._responses_stream_timeouts()[2],
                )

        if not state.completed:
            raise aiohttp.ClientError("stream closed before chat completion finished")

        return True

    async def _relay_sse_stream(
        self,
        response: aiohttp.ClientResponse,
        state: StreamState,
        client_stream: SSEStreamWriter,
        recovery_session: dict[str, Any],
        on_block: Callable[[str, StreamState, SSEStreamWriter], Awaitable[None]],
        *,
        first_byte_timeout: float,
        stall_timeout: float,
        visible_output_timeout: float,
    ) -> None:
        buffer = ""
        saw_any_bytes = False
        while True:
            timeout = first_byte_timeout if not saw_any_bytes else stall_timeout
            try:
                raw_chunk = await asyncio.wait_for(response.content.readany(), timeout=timeout)
            except asyncio.TimeoutError as exc:
                phase = "first byte" if not saw_any_bytes else "stall"
                raise _StreamTimeout(timeout, phase) from exc

            if not raw_chunk:
                break

            saw_any_bytes = True
            buffer += raw_chunk.decode("utf-8", errors="replace")
            while "\n\n" in buffer:
                block, buffer = buffer.split("\n\n", 1)
                await on_block(block, state, client_stream)
                self.recovery_store.maybe_save_progress(recovery_session, state)
                if not state.completed:
                    visible_silence = time.monotonic() - state.last_visible_output_at
                    if visible_silence >= visible_output_timeout:
                        raise _StreamTimeout(
                            visible_output_timeout,
                            "no visible output",
                        )

        if buffer.strip():
            await on_block(buffer.strip(), state, client_stream)
            self.recovery_store.maybe_save_progress(recovery_session, state, force=True)
            if not state.completed:
                visible_silence = time.monotonic() - state.last_visible_output_at
                if visible_silence >= visible_output_timeout:
                    raise _StreamTimeout(
                        visible_output_timeout,
                        "no visible output",
                    )

    async def _on_event(
        self,
        block: str,
        state: StreamState,
        client_stream: SSEStreamWriter,
    ) -> None:
        event_type = ""
        data_string = ""
        for line in block.split("\n"):
            if line.startswith("event: "):
                event_type = line[7:].strip()
            elif line.startswith("data: "):
                data_string = line[6:]

        if not data_string:
            return

        try:
            data = json.loads(data_string)
        except json.JSONDecodeError:
            await client_stream.write_block(block)
            return

        event_type = event_type or data.get("type", "")

        if event_type == "message_start":
            message = data.get("message", {})
            state.message_id = message.get("id", "")
            state.model = message.get("model", "")
            state.input_tokens = message.get("usage", {}).get("input_tokens", 0)
            if state.is_first_attempt:
                await self._forward(event_type, data_string, client_stream)
            return

        if event_type == "content_block_start":
            content_block = data.get("content_block", {})
            content_type = str(content_block.get("type", "")).strip()
            if content_type == "tool_use":
                state.awaiting_tool_result = True
                _append_unique(state.tool_names, content_block.get("name") or content_type)
                state.last_visible_output_at = time.monotonic()
            if state.is_first_attempt or data.get("index", 0) > 0:
                await self._forward(event_type, data_string, client_stream)
            return

        if event_type == "content_block_delta":
            delta = data.get("delta", {})
            if delta.get("type") == "text_delta":
                text = delta.get("text", "")
                previous_skip_prefix = state.skip_prefix
                text = self._filter_retry_delta(state, text)
                if len(state.skip_prefix) < len(previous_skip_prefix):
                    # Retried streams may spend a while replaying already-delivered text
                    # before they reach new visible output. Treat consumed replay prefix
                    # as upstream progress so we do not self-timeout mid-recovery.
                    state.last_visible_output_at = time.monotonic()
                if not text:
                    return
                state.accumulated_text += text
                if text:
                    state.last_visible_output_at = time.monotonic()
                if data.get("delta", {}).get("text") != text:
                    patched = dict(data)
                    patched["delta"] = dict(delta)
                    patched["delta"]["text"] = text
                    data_string = json.dumps(patched, ensure_ascii=False)
            elif delta:
                state.last_visible_output_at = time.monotonic()
            await self._forward(event_type, data_string, client_stream)
            return

        if event_type == "message_delta":
            delta_payload = data.get("delta", {})
            stop_reason = str(delta_payload.get("stop_reason", "") or data.get("stop_reason", "")).strip()
            if stop_reason == "tool_use":
                state.awaiting_tool_result = True
            usage = data.get("usage", {})
            state.output_tokens += usage.get("output_tokens", 0)
            await self._forward(event_type, data_string, client_stream)
            return

        if event_type == "message_stop":
            state.completed = True
            await self._forward(event_type, data_string, client_stream)
            return

        await self._forward(event_type, data_string, client_stream)

    async def _on_responses_event(
        self,
        block: str,
        state: StreamState,
        client_stream: SSEStreamWriter,
    ) -> None:
        event_type = ""
        data_string = ""
        for line in block.split("\n"):
            if line.startswith("event: "):
                event_type = line[7:].strip()
            elif line.startswith("data: "):
                data_string = line[6:]

        if not data_string:
            return

        if data_string == "[DONE]":
            await client_stream.write_done_once(state)
            return

        try:
            data = json.loads(data_string)
        except json.JSONDecodeError:
            await client_stream.write_block(block)
            return

        event_type = event_type or data.get("type", "")

        if event_type == "response.created":
            response = data.get("response", {})
            state.response_id = response.get("id", state.response_id)
            state.model = response.get("model", state.model)
            if state.is_first_attempt:
                await self._forward(event_type, data_string, client_stream)
            return

        if event_type == "response.output_item.added":
            item = data.get("item", {})
            item_type = str(item.get("type", "")).strip()
            if item.get("type") == "message":
                state.response_item_id = item.get("id", state.response_item_id)
            if _is_responses_tool_call_type(item_type):
                state.awaiting_tool_result = True
                _append_unique(
                    state.tool_names,
                    item.get("name") or item.get("tool_name") or item.get("call_name") or item_type,
                )
                state.last_visible_output_at = time.monotonic()
            if state.is_first_attempt:
                await self._forward(event_type, data_string, client_stream)
            return

        if event_type == "response.output_text.delta":
            state.response_id = data.get("response_id", state.response_id)
            state.response_item_id = data.get("item_id", state.response_item_id)
            delta = data.get("delta", "")
            previous_skip_prefix = state.skip_prefix
            delta = self._filter_retry_delta(state, delta)
            if len(state.skip_prefix) < len(previous_skip_prefix):
                # Responses retries can also replay previously delivered text before
                # emitting anything new. Count that replay consumption as progress.
                state.last_visible_output_at = time.monotonic()
            if not delta:
                return

            state.accumulated_text += delta
            state.last_visible_output_at = time.monotonic()
            if data.get("delta") != delta:
                patched = dict(data)
                patched["delta"] = delta
                data_string = json.dumps(patched, ensure_ascii=False)
            await self._forward(event_type, data_string, client_stream)
            return

        if event_type == "response.output_text.done":
            if state.is_first_attempt:
                await self._forward(event_type, data_string, client_stream)
            return

        if _is_responses_tool_call_type(str(data.get("type", "")).strip()):
            state.awaiting_tool_result = True
            _append_unique(
                state.tool_names,
                data.get("name") or data.get("tool_name") or data.get("call_name") or data.get("type"),
            )
            state.last_visible_output_at = time.monotonic()

        if event_type.endswith(".delta") or event_type.endswith(".done"):
            state.last_visible_output_at = time.monotonic()

        if event_type == "response.completed":
            response = data.get("response", {})
            state.response_id = response.get("id", state.response_id)
            state.completed = True
            await self._forward(event_type, data_string, client_stream)
            return

        if event_type in {"response.failed", "response.incomplete"}:
            state.response_id = data.get("response", {}).get("id", state.response_id)
            await self._forward(event_type, data_string, client_stream)
            return

        if state.is_first_attempt:
            await self._forward(event_type, data_string, client_stream)

    def _track_chat_completion_tool_delta(self, delta: dict[str, Any], state: StreamState) -> None:
        function_call = delta.get("function_call")
        if isinstance(function_call, dict):
            state.awaiting_tool_result = True
            _append_unique(state.tool_names, function_call.get("name"))
            state.last_visible_output_at = time.monotonic()

        tool_calls = delta.get("tool_calls")
        if not isinstance(tool_calls, list):
            return

        state.awaiting_tool_result = True
        state.last_visible_output_at = time.monotonic()
        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                continue
            function = tool_call.get("function")
            if isinstance(function, dict):
                _append_unique(state.tool_names, function.get("name"))

    async def _on_chat_completions_event(
        self,
        block: str,
        state: StreamState,
        client_stream: SSEStreamWriter,
    ) -> None:
        event_type = ""
        data_string = ""
        for line in block.split("\n"):
            if line.startswith("event: "):
                event_type = line[7:].strip()
            elif line.startswith("data: "):
                data_string = line[6:]

        if not data_string:
            return

        if data_string == "[DONE]":
            state.completed = True
            await client_stream.write_done_once(state)
            return

        try:
            data = json.loads(data_string)
        except json.JSONDecodeError:
            await client_stream.write_block(block)
            return

        event_type = event_type or str(data.get("object", "") or data.get("type", "")).strip()
        state.model = str(data.get("model", state.model) or state.model)
        state.response_id = str(data.get("id", state.response_id) or state.response_id)

        choices = data.get("choices")
        if not isinstance(choices, list) or not choices:
            if state.is_first_attempt:
                await self._forward(event_type, data_string, client_stream)
            return

        if len(choices) != 1 or not isinstance(choices[0], dict):
            for choice in choices:
                if not isinstance(choice, dict):
                    continue
                finish_reason = choice.get("finish_reason")
                if finish_reason is not None:
                    state.completed = True
                delta = choice.get("delta")
                if isinstance(delta, dict):
                    self._track_chat_completion_tool_delta(delta, state)
                    if isinstance(delta.get("content"), str):
                        state.last_visible_output_at = time.monotonic()
            if state.is_first_attempt or state.completed or state.awaiting_tool_result:
                await self._forward(event_type, data_string, client_stream)
            return

        choice = choices[0]
        finish_reason = choice.get("finish_reason")
        if finish_reason is not None:
            state.completed = True
            await self._forward(event_type, data_string, client_stream)
            return

        delta = choice.get("delta")
        if not isinstance(delta, dict):
            if state.is_first_attempt:
                await self._forward(event_type, data_string, client_stream)
            return

        self._track_chat_completion_tool_delta(delta, state)

        content = delta.get("content")
        if isinstance(content, str):
            previous_skip_prefix = state.skip_prefix
            content = self._filter_retry_delta(state, content)
            if len(state.skip_prefix) < len(previous_skip_prefix):
                state.last_visible_output_at = time.monotonic()
            if not content:
                return
            state.accumulated_text += content
            state.last_visible_output_at = time.monotonic()
            if delta.get("content") != content:
                patched = dict(data)
                patched_choice = dict(choice)
                patched_delta = dict(delta)
                patched_delta["content"] = content
                patched_choice["delta"] = patched_delta
                patched["choices"] = [patched_choice]
                data_string = json.dumps(patched, ensure_ascii=False)
            await self._forward(event_type, data_string, client_stream)
            return

        if delta.get("role"):
            if state.is_first_attempt:
                await self._forward(event_type, data_string, client_stream)
            return

        if state.is_first_attempt or state.awaiting_tool_result:
            await self._forward(event_type, data_string, client_stream)

    async def _recover_response_tail(
        self,
        upstream: str,
        headers: dict[str, str],
        state: StreamState,
        client_stream: SSEStreamWriter,
    ) -> bool:
        if not state.response_id:
            return False

        request_headers = {
            key: value
            for key, value in headers.items()
            if key.lower() not in {"content-type", "accept"}
        }

        tail_timeout = self._responses_tail_fetch_timeout()
        deadline = time.monotonic() + tail_timeout
        url = f"{upstream}/v1/responses/{state.response_id}"
        last_status = "pending"

        async with aiohttp.ClientSession() as session:
            while time.monotonic() < deadline:
                try:
                    async with session.request("GET", url, headers=request_headers) as response:
                        if response.status == 404:
                            log.info("tail fetch unavailable for %s: upstream returned 404", state.response_id)
                            return False
                        if response.status >= 500:
                            last_status = f"http_{response.status}"
                            await asyncio.sleep(1)
                            continue
                        if response.status >= 400:
                            log.info("tail fetch unavailable for %s: upstream returned %s", state.response_id, response.status)
                            return False

                        payload = await response.json()
                except aiohttp.ClientError:
                    last_status = "client_error"
                    await asyncio.sleep(1)
                    continue
                except Exception as exc:
                    last_status = f"error:{type(exc).__name__}"
                    log.warning("tail recovery error for %s: %s", state.response_id, exc)
                    await asyncio.sleep(1)
                    continue

                status = str(payload.get("status", "")).lower()
                if status:
                    last_status = status
                if status == "completed":
                    await self._forward_recovered_response(payload, state, client_stream)
                    return True
                if status in {"failed", "incomplete", "cancelled", "canceled"}:
                    log.info("tail fetch stopped for %s: upstream status=%s", state.response_id, status)
                    return False

                await asyncio.sleep(1)

        log.info(
            "tail fetch timed out for %s after %.1fs (last_status=%s)",
            state.response_id,
            tail_timeout,
            last_status,
        )
        return False

    async def _forward_recovered_response(
        self,
        response_obj: dict[str, Any],
        state: StreamState,
        client_stream: SSEStreamWriter,
    ) -> None:
        full_text, item_id = self._extract_response_text(response_obj)
        state.response_item_id = item_id or state.response_item_id
        state.response_id = response_obj.get("id", state.response_id)

        missing = self._compute_missing_suffix(state.accumulated_text, full_text)
        if missing:
            state.accumulated_text += missing
            state.last_visible_output_at = time.monotonic()
            await self._forward(
                "response.output_text.delta",
                json.dumps(
                    {
                        "type": "response.output_text.delta",
                        "response_id": state.response_id,
                        "item_id": state.response_item_id,
                        "output_index": 0,
                        "content_index": 0,
                        "delta": missing,
                    },
                    ensure_ascii=False,
                ),
                client_stream,
            )

        await self._forward(
            "response.completed",
            json.dumps(
                {
                    "type": "response.completed",
                    "response": response_obj,
                },
                ensure_ascii=False,
            ),
            client_stream,
        )
        state.completed = True
        await client_stream.write_done_once(state)

    def _extract_response_text(self, response_obj: dict[str, Any]) -> tuple[str, str]:
        text_parts: list[str] = []
        item_id = ""

        output = response_obj.get("output", [])
        if isinstance(output, list):
            for item in output:
                if not isinstance(item, dict):
                    continue
                if not item_id and item.get("type") == "message":
                    item_id = item.get("id", "")
                content = item.get("content", [])
                if not isinstance(content, list):
                    continue
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") == "output_text":
                        text_parts.append(str(part.get("text", "")))

        return "".join(text_parts), item_id

    def _filter_retry_delta(self, state: StreamState, delta: str) -> str:
        if not delta or not state.skip_prefix:
            return delta

        remaining = state.skip_prefix
        matched = 0
        max_match = min(len(delta), len(remaining))
        while matched < max_match and delta[matched] == remaining[matched]:
            matched += 1

        if matched == 0:
            state.skip_prefix = ""
            return delta

        if matched == len(delta):
            state.skip_prefix = remaining[matched:]
            return ""

        if matched == len(remaining):
            state.skip_prefix = ""
            return delta[matched:]

        state.skip_prefix = ""
        return delta

    def _compute_missing_suffix(self, current_text: str, full_text: str) -> str:
        if not full_text:
            return ""
        if full_text.startswith(current_text):
            return full_text[len(current_text):]

        overlap = min(len(current_text), len(full_text))
        while overlap > 0:
            if current_text[-overlap:] == full_text[:overlap]:
                return full_text[overlap:]
            overlap -= 1

        return full_text

    async def _forward(
        self,
        event_type: str,
        data_string: str,
        client_stream: SSEStreamWriter,
    ) -> None:
        await client_stream.write_event(event_type, data_string)


class _UpstreamClientError(Exception):
    pass


def _spawn_safety_watchdog(parent_pid: int, listen_port: int) -> int | None:
    try:
        creationflags = 0
        for flag_name in ("DETACHED_PROCESS", "CREATE_NEW_PROCESS_GROUP"):
            creationflags |= getattr(subprocess, flag_name, 0)

        process = subprocess.Popen(
            _self_launch_command(
                "--watchdog-parent-pid",
                str(parent_pid),
                "--watchdog-listen-port",
                str(listen_port),
            ),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            creationflags=creationflags,
        )
        _append_watchdog_log(f"watchdog started for proxy pid {parent_pid} on port {listen_port}")
        return process.pid
    except Exception as exc:
        log.warning("Failed to start safety watchdog: %s", exc)
        return None


def run_watchdog(parent_pid: int, listen_port: int) -> int:
    proxy_url = f"http://{LOCAL_HOST}:{listen_port}"
    startup_deadline = time.monotonic() + WATCHDOG_STARTUP_GRACE
    unhealthy_since: float | None = None
    _append_watchdog_log(f"watchdog monitoring {proxy_url} for parent pid {parent_pid}")

    while True:
        parent_alive = _process_exists(parent_pid)
        health_ok = _proxy_health_is_ok(listen_port)
        now = time.monotonic()

        if health_ok:
            unhealthy_since = None
        elif unhealthy_since is None:
            unhealthy_since = now

        if not parent_alive:
            restored_claude, restored_codex = _restore_fail_open_settings(listen_port)
            _append_watchdog_log(
                f"parent pid {parent_pid} exited; restored fail-open settings "
                f"(claude={restored_claude}, codex={restored_codex})"
            )
            return 0

        if now >= startup_deadline and unhealthy_since is not None:
            unhealthy_for = now - unhealthy_since
            if unhealthy_for >= WATCHDOG_FAILURE_GRACE:
                restored_claude, restored_codex = _restore_fail_open_settings(listen_port)
                _append_watchdog_log(
                    f"proxy health unavailable for {unhealthy_for:.1f}s; restored fail-open settings "
                    f"(claude={restored_claude}, codex={restored_codex})"
                )
                return 0

        time.sleep(WATCHDOG_POLL_INTERVAL)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plunger local LLM proxy with zero extra setup")
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help="Listen port")
    parser.add_argument("-t", "--timeout", type=float, default=DEFAULT_STALL_TIMEOUT, help="Stall timeout in seconds")
    parser.add_argument("-r", "--retries", type=int, default=-1, help="Retry count (-1 = unlimited)")
    parser.add_argument(
        "--max-body-mb",
        type=float,
        default=DEFAULT_MAX_REQUEST_BODY_MB,
        help="Maximum request body size accepted by the local proxy in MiB",
    )
    parser.add_argument(
        "--safe-resume-body-mb",
        type=float,
        default=DEFAULT_SAFE_RESUME_BODY_MB,
        help="Skip proxy-added resume/prefill growth when a request would exceed this size in MiB",
    )
    parser.add_argument(
        "-u",
        "--upstream",
        help="Optional manual upstream override for recovery/debugging",
    )
    parser.add_argument(
        "--watch-interval",
        type=float,
        default=1.0,
        help="settings.json poll interval in seconds",
    )
    parser.add_argument("--watchdog-parent-pid", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--watchdog-listen-port", type=int, default=0, help=argparse.SUPPRESS)
    return parser


def _log_banner(settings_manager: SettingsManager, upstream: str, args: argparse.Namespace) -> None:
    first_byte_timeout = max(MIN_FIRST_BYTE_TIMEOUT, args.timeout * 1.5)
    visible_output_timeout = max(DEFAULT_VISIBLE_OUTPUT_TIMEOUT, args.timeout)
    log.info("=" * 58)
    log.info("  Plunger")
    log.info("=" * 58)
    log.info("  Upstream:  %s", upstream)
    log.info("  Listen:    %s", settings_manager.proxy_url)
    log.info("  Timeout:   %ss", args.timeout)
    log.info("  FirstByte: %ss", first_byte_timeout)
    log.info("  Visible:   %ss", visible_output_timeout)
    log.info("  BodyLimit: %s", _format_bytes(_mb_to_bytes(args.max_body_mb)))
    log.info("  ResumeCap: %s", _format_bytes(_mb_to_bytes(args.safe_resume_body_mb)))
    log.info("  Responses: %ss stall / %ss first-byte / %ss visible", args.timeout * 2, first_byte_timeout * 2, visible_output_timeout * 2)
    log.info("  Retries:   %s", _format_retry_policy(args.retries))
    log.info("  Restore:   automatic on shutdown")
    log.info("=" * 58)


async def run_proxy(args: argparse.Namespace) -> int:
    stop_event = asyncio.Event()
    runner: web.AppRunner | None = None
    watchdog_pid: int | None = None

    _take_over_existing_proxy_instance(args.port)

    settings_manager = SettingsManager(
        listen_port=args.port,
        upstream_override=args.upstream,
        poll_interval=args.watch_interval,
    )
    codex_config_manager = CodexConfigManager(args.port)

    try:
        upstream = settings_manager.hijack()
        codex_config_manager.hijack()
    except Exception as exc:
        with suppress(Exception):
            codex_config_manager.restore()
        with suppress(Exception):
            settings_manager.restore()
        log.error("%s", exc)
        sys.exit(1)

    restored = [False]
    cleanup_state = {"skip_restore": False, "reason": "manual"}
    watchdog_pid_holder: list[int | None] = [None]

    def cleanup() -> None:
        if restored[0]:
            return
        restored[0] = True
        if cleanup_state["skip_restore"]:
            log.info(
                "Skipping config restore during shutdown (%s); another proxy instance is taking over",
                cleanup_state["reason"],
            )
            codex_config_manager.discard_state()
            settings_manager.discard_state()
        else:
            codex_config_manager.restore()
            settings_manager.restore()
        wd = watchdog_pid_holder[0]
        if wd:
            _terminate_pid_best_effort(wd)
            watchdog_pid_holder[0] = None

    atexit.register(cleanup)
    loop = asyncio.get_running_loop()

    def handle_signal(_signum: int, _frame: Any) -> None:
        loop.call_soon_threadsafe(stop_event.set)

    previous_handlers: list[tuple[int, Any]] = []
    if threading.current_thread() is threading.main_thread():
        for signal_name in ("SIGINT", "SIGTERM"):
            if hasattr(signal, signal_name):
                sig = getattr(signal, signal_name)
                previous_handlers.append((sig, signal.getsignal(sig)))
                signal.signal(sig, handle_signal)

    proxy = ResilientProxy(
        settings_manager,
        args.timeout,
        args.retries,
        max_request_body_bytes=_mb_to_bytes(args.max_body_mb),
        safe_resume_body_bytes=_mb_to_bytes(args.safe_resume_body_mb),
    )
    watchdog_pid = _spawn_safety_watchdog(os.getpid(), args.port)
    proxy.safety_watchdog_pid = watchdog_pid

    app = web.Application(
        client_max_size=proxy.max_request_body_bytes,
        middlewares=[proxy_error_middleware],
    )
    app["stop_event"] = stop_event
    app["cleanup_state"] = cleanup_state
    app.router.add_post("/v1/messages", proxy.handle_messages)
    app.router.add_post("/v1/responses", proxy.handle_responses)
    app.router.add_post("/v1/chat/completions", proxy.handle_chat_completions)
    app.router.add_get("/health", proxy.handle_health)
    app.router.add_post("/control/shutdown", proxy.handle_shutdown)
    app.router.add_route("*", "/{path:.*}", proxy.handle_catchall)

    async def on_startup(_app: web.Application) -> None:
        await settings_manager.start_watch()

    async def on_cleanup(_app: web.Application) -> None:
        await settings_manager.stop_watch()
        cleanup()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    try:
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, host=LOCAL_HOST, port=args.port)
        await site.start()
        proxy.event_history.add(
            "service_start",
            "Proxy active",
            (
                f"Listening on {settings_manager.proxy_url} and forwarding to {upstream} "
                f"via {settings_manager.current_upstream_source}."
            ),
            tone="success",
            meta={
                "listen": settings_manager.proxy_url,
                "upstream": upstream,
                "upstream_source": settings_manager.current_upstream_source,
            },
        )
        _log_banner(settings_manager, upstream, args)
        await stop_event.wait()
    finally:
        for sig, previous in previous_handlers:
            with suppress(Exception):
                signal.signal(sig, previous)
        if runner is not None:
            with suppress(Exception):
                await runner.cleanup()
        cleanup()

    return 0


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    if args.watchdog_parent_pid and args.watchdog_listen_port:
        raise SystemExit(run_watchdog(args.watchdog_parent_pid, args.watchdog_listen_port))
    raise SystemExit(asyncio.run(run_proxy(args)))


if __name__ == "__main__":
    main()
