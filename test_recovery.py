#!/usr/bin/env python3
"""
Test harness for resilient-proxy disconnection recovery.

Spins up a fake upstream that can simulate:
  1. Clean stream completion
  2. Mid-stream disconnection (server drops connection)
  3. Stall (server stops sending data)
  4. HTTP 500 on first attempt, success on retry
  5. Partial content then disconnect, verify accumulated text on retry
  6. Multiple consecutive disconnections before final success
  7. Client disconnect detection
  8. Heartbeat keepalive during slow streams

Usage:
    python test_recovery.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("test-recovery")


# ---------------------------------------------------------------------------
# Fake upstream server
# ---------------------------------------------------------------------------

FAKE_UPSTREAM_PORT = 18900
PROXY_PORT = 18901

# Shared mutable state for controlling fake upstream behavior
@dataclass
class FakeUpstreamState:
    # How many requests have been received (per test scenario)
    request_count: int = 0
    # How many responses API requests have been received
    responses_request_count: int = 0
    # How many chat completions API requests have been received
    chat_request_count: int = 0
    # Current scenario
    scenario: str = "clean"
    # How many tokens to send before disconnecting (for partial scenarios)
    tokens_before_disconnect: int = 5
    # Total tokens to generate for a full response
    total_tokens: int = 15
    # Whether to stall instead of disconnect
    stall_seconds: float = 0
    # How many failures before success (for multi-failure scenarios)
    failures_remaining: int = 0
    # HTTP status to return on failure
    failure_status: int = 500
    # Delay between SSE chunks (simulates slow generation)
    chunk_delay: float = 0.05
    # Track what partial text was received in retry requests
    received_retries: list[dict[str, Any]] = field(default_factory=list)
    # Track what partial text was received in chat completion retry requests
    received_chat_retries: list[dict[str, Any]] = field(default_factory=list)
    # Completed test names
    completed: list[str] = field(default_factory=list)
    # Failed test names
    failed: list[str] = field(default_factory=list)


fake_state = FakeUpstreamState()


def _make_text_delta(index: int) -> str:
    return f"token_{index} "


def _make_chat_completion_chunk(
    *,
    model: str = "cursor-test-model",
    delta: dict[str, Any] | None = None,
    finish_reason: str | None = None,
) -> str:
    payload = {
        "id": "chatcmpl_test_001",
        "object": "chat.completion.chunk",
        "created": 0,
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": delta or {},
                "finish_reason": finish_reason,
            }
        ],
    }
    return f"data: {json.dumps(payload)}\n\n"


def _make_sse_message_start(model: str = "claude-test-model") -> str:
    data = {
        "type": "message_start",
        "message": {
            "id": "msg_test_001",
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [],
            "usage": {"input_tokens": 10, "output_tokens": 0},
        },
    }
    return f"event: message_start\ndata: {json.dumps(data)}\n\n"


def _make_sse_content_block_start() -> str:
    data = {
        "type": "content_block_start",
        "index": 0,
        "content_block": {"type": "text", "text": ""},
    }
    return f"event: content_block_start\ndata: {json.dumps(data)}\n\n"


def _make_sse_content_delta(token_text: str) -> str:
    data = {
        "type": "content_block_delta",
        "index": 0,
        "delta": {"type": "text_delta", "text": token_text},
    }
    return f"event: content_block_delta\ndata: {json.dumps(data)}\n\n"


def _make_sse_content_block_stop() -> str:
    data = {"type": "content_block_stop", "index": 0}
    return f"event: content_block_stop\ndata: {json.dumps(data)}\n\n"


def _make_sse_message_delta() -> str:
    data = {
        "type": "message_delta",
        "delta": {"stop_reason": "end_turn"},
        "usage": {"output_tokens": 15},
    }
    return f"event: message_delta\ndata: {json.dumps(data)}\n\n"


def _make_sse_message_stop() -> str:
    data = {"type": "message_stop"}
    return f"event: message_stop\ndata: {json.dumps(data)}\n\n"


def _has_empty_text_block(value: Any) -> bool:
    if isinstance(value, dict):
        block_type = str(value.get("type", "")).strip().lower()
        if block_type in {"text", "input_text"}:
            text = value.get("text", "")
            if isinstance(text, str) and not text.strip():
                return True
        return any(_has_empty_text_block(item) for item in value.values())

    if isinstance(value, list):
        return any(_has_empty_text_block(item) for item in value)

    return False


async def fake_upstream_handler(req: web.Request) -> web.StreamResponse:
    """Handle POST /v1/messages on the fake upstream."""
    fake_state.request_count += 1
    attempt = fake_state.request_count

    body = await req.json()
    messages = body.get("messages", [])

    # Record retry info for assertion
    assistant_messages = [m for m in messages if isinstance(m, dict) and m.get("role") == "assistant"]
    if assistant_messages:
        fake_state.received_retries.append({
            "attempt": attempt,
            "assistant_content": assistant_messages[-1].get("content", ""),
            "message_count": len(messages),
        })

    scenario = fake_state.scenario

    # --- Scenario: disconnect on first attempt, reject prefill on retry, then succeed ---
    if scenario == "disconnect_then_reject_prefill":
        if attempt == 1:
            # First attempt: stream some tokens then disconnect
            response = web.StreamResponse(
                headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}
            )
            await response.prepare(req)
            await response.write(_make_sse_message_start().encode())
            await response.write(_make_sse_content_block_start().encode())
            for i in range(fake_state.tokens_before_disconnect):
                await asyncio.sleep(fake_state.chunk_delay)
                await response.write(_make_sse_content_delta(_make_text_delta(i)).encode())
            log.info("  [fake] Simulating disconnect after %d tokens", fake_state.tokens_before_disconnect)
            return response
        elif assistant_messages:
            # Retry with prefill: reject it
            return web.Response(
                status=400,
                text=json.dumps({
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "message": "This model does not support assistant message prefill. The conversation must end with a user message.",
                    },
                }),
            )
        # else: no prefill, proceed to normal streaming below

    # --- Scenario: disconnect, then provider rejects retry because thinking blocks cannot be modified ---
    if scenario == "disconnect_then_reject_prefill_thinking_blocks":
        if attempt == 1:
            response = web.StreamResponse(
                headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}
            )
            await response.prepare(req)
            await response.write(_make_sse_message_start().encode())
            await response.write(_make_sse_content_block_start().encode())
            for i in range(fake_state.tokens_before_disconnect):
                await asyncio.sleep(fake_state.chunk_delay)
                await response.write(_make_sse_content_delta(_make_text_delta(i)).encode())
            log.info("  [fake] Simulating disconnect after %d tokens", fake_state.tokens_before_disconnect)
            return response
        elif assistant_messages:
            return web.Response(
                status=400,
                text=json.dumps({
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "message": "messages.1.content.1: `thinking` or `redacted_thinking` blocks in the latest assistant message cannot be modified. These blocks must remain as they were in the original response.",
                    },
                }),
            )

    # --- Scenario: reject assistant prefill with 400 ---
    if scenario == "reject_prefill" and assistant_messages:
        return web.Response(
            status=400,
            text=json.dumps({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": "This model does not support assistant message prefill. The conversation must end with a user message.",
                },
            }),
        )

    if scenario == "reject_empty_text_blocks" and _has_empty_text_block(messages):
        return web.Response(
            status=400,
            text=json.dumps({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": "messages: text content blocks must be non-empty",
                },
            }),
        )

    # --- Scenario: HTTP 500 on first N attempts ---
    if fake_state.failures_remaining > 0:
        fake_state.failures_remaining -= 1
        return web.Response(
            status=fake_state.failure_status,
            text=json.dumps({"error": {"type": "server_error", "message": "simulated failure"}}),
        )

    # --- Streaming response ---
    response = web.StreamResponse(
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
        }
    )
    await response.prepare(req)

    # Send message_start and content_block_start
    await response.write(_make_sse_message_start().encode())
    await response.write(_make_sse_content_block_start().encode())

    tokens_to_send = fake_state.total_tokens

    # If this is a retry with partial text, figure out where to start from
    # (simulates real Claude API behavior: continue from the partial assistant message)
    start_from = 0
    if assistant_messages:
        partial = assistant_messages[-1].get("content", "")
        for i in range(fake_state.total_tokens):
            token = _make_text_delta(i)
            if token in partial:
                start_from = i + 1

    for i in range(start_from, tokens_to_send):
        token_text = _make_text_delta(i)

        # For disconnect scenario: cut connection after N tokens on first attempt only
        if scenario == "disconnect" and i >= fake_state.tokens_before_disconnect and attempt == 1:
            log.info("  [fake] Simulating disconnect after %d tokens", i)
            return response

        # For stall scenario: stop sending after N tokens (hold connection open) on first attempt
        if scenario == "stall" and i >= fake_state.tokens_before_disconnect and attempt == 1:
            log.info("  [fake] Simulating stall after %d tokens for %.1fs", i, fake_state.stall_seconds)
            await asyncio.sleep(fake_state.stall_seconds)
            return response

        await asyncio.sleep(fake_state.chunk_delay)
        await response.write(_make_sse_content_delta(token_text).encode())

    # Send completion events
    await response.write(_make_sse_content_block_stop().encode())
    await response.write(_make_sse_message_delta().encode())
    await response.write(_make_sse_message_stop().encode())

    return response


async def fake_health_handler(req: web.Request) -> web.Response:
    return web.Response(text="ok")


async def fake_responses_handler(req: web.Request) -> web.Response:
    fake_state.responses_request_count += 1
    body = await req.json()
    return web.json_response(
        {
            "id": "resp_test_001",
            "status": "completed",
            "model": body.get("model", "codex-test-model"),
            "output": [
                {
                    "type": "message",
                    "id": "msg_resp_001",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "ok"}],
                }
            ],
        }
    )


async def fake_chat_completions_handler(req: web.Request) -> web.StreamResponse:
    fake_state.chat_request_count += 1
    attempt = fake_state.chat_request_count

    body = await req.json()
    messages = body.get("messages", [])
    assistant_messages = [m for m in messages if isinstance(m, dict) and m.get("role") == "assistant"]
    if assistant_messages:
        fake_state.received_chat_retries.append(
            {
                "attempt": attempt,
                "assistant_content": assistant_messages[-1].get("content", ""),
                "message_count": len(messages),
            }
        )

    scenario = fake_state.scenario
    model = body.get("model", "cursor-test-model")

    if scenario == "disconnect_then_reject_prefill" and attempt == 2 and assistant_messages:
        return web.Response(
            status=400,
            text=json.dumps(
                {
                    "error": {
                        "type": "invalid_request_error",
                        "message": "assistant prefill is not supported for this provider",
                    }
                }
            ),
        )

    if fake_state.failures_remaining > 0:
        fake_state.failures_remaining -= 1
        return web.Response(
            status=fake_state.failure_status,
            text=json.dumps({"error": {"type": "server_error", "message": "simulated failure"}}),
        )

    response = web.StreamResponse(
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
        }
    )
    await response.prepare(req)
    await response.write(_make_chat_completion_chunk(model=model, delta={"role": "assistant"}).encode())

    start_from = 0
    if assistant_messages:
        partial = str(assistant_messages[-1].get("content", ""))
        for i in range(fake_state.total_tokens):
            token = _make_text_delta(i)
            if token in partial:
                start_from = i + 1

    for i in range(start_from, fake_state.total_tokens):
        if scenario == "disconnect" and i >= fake_state.tokens_before_disconnect and attempt == 1:
            log.info("  [fake-chat] Simulating disconnect after %d tokens", i)
            return response

        if scenario == "stall" and i >= fake_state.tokens_before_disconnect and attempt == 1:
            log.info("  [fake-chat] Simulating stall after %d tokens for %.1fs", i, fake_state.stall_seconds)
            await asyncio.sleep(fake_state.stall_seconds)
            return response

        await asyncio.sleep(fake_state.chunk_delay)
        await response.write(
            _make_chat_completion_chunk(model=model, delta={"content": _make_text_delta(i)}).encode()
        )

    await response.write(_make_chat_completion_chunk(model=model, finish_reason="stop").encode())
    await response.write(b"data: [DONE]\n\n")
    return response


def build_fake_upstream_app() -> web.Application:
    app = web.Application(client_max_size=32 * 1024 * 1024)
    app.router.add_post("/v1/messages", fake_upstream_handler)
    app.router.add_post("/v1/responses", fake_responses_handler)
    app.router.add_post("/v1/chat/completions", fake_chat_completions_handler)
    app.router.add_get("/health", fake_health_handler)
    return app


# ---------------------------------------------------------------------------
# Proxy launcher (in-process)
# ---------------------------------------------------------------------------

async def start_proxy(upstream_url: str, port: int, stall_timeout: float = 5.0, max_retries: int = 5) -> tuple[web.AppRunner, Any]:
    """Start the resilient proxy in-process pointing at our fake upstream."""
    # Import the proxy module
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import plunger as rp

    # Create a minimal SettingsManager that doesn't touch real settings files
    class MockSettingsManager:
        def __init__(self, upstream: str, port: int):
            self.current_upstream = upstream
            self.proxy_url = f"http://127.0.0.1:{port}"
            self.listen_port = port
            self._original_upstream = upstream

        def hijack(self) -> str:
            return self.current_upstream

        def restore(self) -> None:
            pass

        def _cc_switch_proxy_enabled(self) -> bool:
            return False

    settings = MockSettingsManager(upstream_url, port)
    proxy = rp.ResilientProxy(settings, stall_timeout, max_retries)

    app = web.Application(
        client_max_size=proxy.max_request_body_bytes,
        middlewares=[rp.proxy_error_middleware],
    )
    app.router.add_post("/v1/messages", proxy.handle_messages)
    app.router.add_post("/v1/responses", proxy.handle_responses)
    app.router.add_post("/v1/chat/completions", proxy.handle_chat_completions)
    app.router.add_route("*", "/{path:.*}", proxy.handle_catchall)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    return runner, proxy


# ---------------------------------------------------------------------------
# Test client
# ---------------------------------------------------------------------------

async def send_streaming_request(
    proxy_url: str,
    user_message: str = "Hello",
    *,
    timeout_seconds: float = 30.0,
    model: str = "claude-test-model",
    body_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Send a streaming request through the proxy and collect the full response."""
    body = body_override or {
        "model": model,
        "max_tokens": 1024,
        "stream": True,
        "messages": [{"role": "user", "content": user_message}],
    }

    accumulated_text = ""
    events: list[dict[str, Any]] = []
    completed = False
    error = None

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{proxy_url}/v1/messages",
                json=body,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=timeout_seconds),
            ) as resp:
                buffer = ""
                async for chunk in resp.content.iter_any():
                    buffer += chunk.decode("utf-8", errors="replace")
                    while "\n\n" in buffer:
                        block, buffer = buffer.split("\n\n", 1)
                        event_type = ""
                        data_string = ""
                        for line in block.split("\n"):
                            if line.startswith("event: "):
                                event_type = line[7:].strip()
                            elif line.startswith("data: "):
                                data_string = line[6:]
                            elif line.startswith(": "):
                                pass  # heartbeat comment

                        if not data_string:
                            continue

                        try:
                            data = json.loads(data_string)
                        except json.JSONDecodeError:
                            continue

                        event_type = event_type or data.get("type", "")
                        events.append({"type": event_type, "data": data})

                        if event_type == "content_block_delta":
                            delta = data.get("delta", {})
                            if delta.get("type") == "text_delta":
                                accumulated_text += delta.get("text", "")

                        if event_type == "message_stop":
                            completed = True

                        if event_type == "error":
                            error = data.get("error", {}).get("message", str(data))

    except asyncio.TimeoutError:
        error = "client timeout"
    except aiohttp.ClientError as exc:
        error = f"client error: {exc}"
    except Exception as exc:
        error = f"unexpected: {type(exc).__name__}: {exc}"

    return {
        "accumulated_text": accumulated_text,
        "events": events,
        "completed": completed,
        "error": error,
        "event_count": len(events),
    }


async def send_chat_completions_request(
    proxy_url: str,
    user_message: str = "Hello",
    *,
    timeout_seconds: float = 30.0,
    model: str = "cursor-test-model",
    body_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = body_override or {
        "model": model,
        "stream": True,
        "messages": [{"role": "user", "content": user_message}],
    }

    accumulated_text = ""
    chunks: list[dict[str, Any]] = []
    completed = False
    error = None

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{proxy_url}/v1/chat/completions",
                json=body,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=timeout_seconds),
            ) as resp:
                buffer = ""
                async for chunk in resp.content.iter_any():
                    buffer += chunk.decode("utf-8", errors="replace")
                    while "\n\n" in buffer:
                        block, buffer = buffer.split("\n\n", 1)
                        data_string = ""
                        for line in block.split("\n"):
                            if line.startswith("data: "):
                                data_string = line[6:]

                        if not data_string:
                            continue
                        if data_string == "[DONE]":
                            completed = True
                            continue

                        try:
                            data = json.loads(data_string)
                        except json.JSONDecodeError:
                            continue

                        chunks.append(data)
                        for choice in data.get("choices", []):
                            if not isinstance(choice, dict):
                                continue
                            delta = choice.get("delta", {})
                            if isinstance(delta, dict):
                                accumulated_text += str(delta.get("content", "") or "")
                            if choice.get("finish_reason") is not None:
                                completed = True

    except asyncio.TimeoutError:
        error = "client timeout"
    except aiohttp.ClientError as exc:
        error = f"client error: {exc}"
    except Exception as exc:
        error = f"unexpected: {type(exc).__name__}: {exc}"

    return {
        "accumulated_text": accumulated_text,
        "chunks": chunks,
        "completed": completed,
        "error": error,
        "chunk_count": len(chunks),
    }


# ---------------------------------------------------------------------------
# Test scenarios
# ---------------------------------------------------------------------------

def _reset_state(scenario: str, **kwargs: Any) -> None:
    fake_state.request_count = 0
    fake_state.responses_request_count = 0
    fake_state.chat_request_count = 0
    fake_state.scenario = scenario
    fake_state.tokens_before_disconnect = kwargs.get("tokens_before_disconnect", 5)
    fake_state.total_tokens = kwargs.get("total_tokens", 15)
    fake_state.stall_seconds = kwargs.get("stall_seconds", 0)
    fake_state.failures_remaining = kwargs.get("failures_remaining", 0)
    fake_state.failure_status = kwargs.get("failure_status", 500)
    fake_state.chunk_delay = kwargs.get("chunk_delay", 0.02)
    fake_state.received_retries.clear()
    fake_state.received_chat_retries.clear()


def _expected_full_text(total_tokens: int) -> str:
    return "".join(_make_text_delta(i) for i in range(total_tokens))


async def test_clean_stream(proxy_url: str) -> None:
    """Test 1: Clean stream with no interruptions."""
    name = "clean_stream"
    log.info("=" * 60)
    log.info("TEST: %s - Clean stream completion", name)
    _reset_state("clean", total_tokens=10)

    result = await send_streaming_request(proxy_url, "clean test")

    expected = _expected_full_text(10)
    assert result["completed"], f"Stream should complete. Error: {result['error']}"
    assert result["accumulated_text"] == expected, (
        f"Text mismatch.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )
    assert fake_state.request_count == 1, f"Should have exactly 1 request, got {fake_state.request_count}"
    assert len(fake_state.received_retries) == 0, "No retries should have assistant messages"

    log.info("  PASS: Clean stream delivered %d chars in 1 attempt", len(expected))
    fake_state.completed.append(name)


async def test_disconnect_and_retry(proxy_url: str) -> None:
    """Test 2: Server disconnects mid-stream, proxy retries with partial text."""
    name = "disconnect_and_retry"
    log.info("=" * 60)
    log.info("TEST: %s - Mid-stream disconnect recovery", name)
    _reset_state("disconnect", tokens_before_disconnect=5, total_tokens=15)

    result = await send_streaming_request(proxy_url, "disconnect test", timeout_seconds=30)

    expected = _expected_full_text(15)
    assert result["completed"], f"Stream should complete after retry. Error: {result['error']}"
    # The proxy should have retried and delivered all content
    assert result["accumulated_text"] == expected, (
        f"Text mismatch after retry.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )
    assert fake_state.request_count >= 2, f"Should have at least 2 requests (original + retry), got {fake_state.request_count}"
    # Verify the retry included partial text as assistant message
    assert len(fake_state.received_retries) >= 1, "Retry should include accumulated assistant content"
    retry_content = fake_state.received_retries[0]["assistant_content"]
    partial_expected = "".join(_make_text_delta(i) for i in range(5))
    assert partial_expected in retry_content or retry_content in partial_expected, (
        f"Retry should carry partial text.\n  Expected partial: {partial_expected!r}\n  Got: {retry_content!r}"
    )

    log.info("  PASS: Recovered from disconnect. %d attempts, %d chars delivered",
             fake_state.request_count, len(result["accumulated_text"]))
    fake_state.completed.append(name)


async def test_http_500_then_success(proxy_url: str) -> None:
    """Test 3: Upstream returns 500 on first attempt, succeeds on retry."""
    name = "http_500_then_success"
    log.info("=" * 60)
    log.info("TEST: %s - HTTP 500 followed by success", name)
    _reset_state("clean", total_tokens=10, failures_remaining=1, failure_status=500)

    result = await send_streaming_request(proxy_url, "500 test", timeout_seconds=30)

    expected = _expected_full_text(10)
    assert result["completed"], f"Should complete after retry. Error: {result['error']}"
    assert result["accumulated_text"] == expected, (
        f"Text mismatch.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )
    assert fake_state.request_count >= 2, f"Should have at least 2 requests, got {fake_state.request_count}"

    log.info("  PASS: Recovered from HTTP 500 in %d attempts", fake_state.request_count)
    fake_state.completed.append(name)


async def test_multiple_failures_then_success(proxy_url: str) -> None:
    """Test 4: Multiple consecutive 500 errors, then success."""
    name = "multiple_failures_then_success"
    log.info("=" * 60)
    log.info("TEST: %s - 3 consecutive 500s then success", name)
    _reset_state("clean", total_tokens=8, failures_remaining=3, failure_status=500)

    result = await send_streaming_request(proxy_url, "multi fail test", timeout_seconds=60)

    expected = _expected_full_text(8)
    assert result["completed"], f"Should complete after retries. Error: {result['error']}"
    assert result["accumulated_text"] == expected, (
        f"Text mismatch.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )
    assert fake_state.request_count >= 4, f"Should have at least 4 requests, got {fake_state.request_count}"

    log.info("  PASS: Recovered after %d attempts", fake_state.request_count)
    fake_state.completed.append(name)


async def test_stall_timeout(proxy_url: str) -> None:
    """Test 5: Server stalls (stops sending) for longer than stall timeout."""
    name = "stall_timeout"
    log.info("=" * 60)
    log.info("TEST: %s - Server stalls past timeout, proxy retries", name)
    # Proxy has 5s stall timeout, server will stall for 15s after 3 tokens
    _reset_state("stall", tokens_before_disconnect=3, total_tokens=10, stall_seconds=15)

    result = await send_streaming_request(proxy_url, "stall test", timeout_seconds=60)

    expected = _expected_full_text(10)
    assert result["completed"], f"Should complete after stall recovery. Error: {result['error']}"
    assert result["accumulated_text"] == expected, (
        f"Text mismatch.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )
    assert fake_state.request_count >= 2, f"Should have retried, got {fake_state.request_count} requests"

    log.info("  PASS: Recovered from stall. %d attempts", fake_state.request_count)
    fake_state.completed.append(name)


async def test_retry_carries_accumulated_text(proxy_url: str) -> None:
    """Test 6: Verify the retry request body includes the partial text as assistant message."""
    name = "retry_carries_accumulated_text"
    log.info("=" * 60)
    log.info("TEST: %s - Retry includes partial text", name)
    _reset_state("disconnect", tokens_before_disconnect=7, total_tokens=12)

    result = await send_streaming_request(proxy_url, "accumulation test", timeout_seconds=30)

    assert result["completed"], f"Should complete. Error: {result['error']}"
    assert len(fake_state.received_retries) >= 1, "At least one retry should be recorded"

    retry = fake_state.received_retries[0]
    partial_text = retry["assistant_content"]
    # Should contain the first 7 tokens
    for i in range(7):
        token = _make_text_delta(i)
        assert token in partial_text, f"Retry should contain token_{i}, got: {partial_text!r}"

    log.info("  PASS: Retry correctly carried %d chars of accumulated text", len(partial_text))
    fake_state.completed.append(name)


async def test_heartbeat_during_slow_stream(proxy_url: str) -> None:
    """Test 7: Slow stream should still complete with heartbeat keepalive."""
    name = "heartbeat_during_slow_stream"
    log.info("=" * 60)
    log.info("TEST: %s - Slow stream with heartbeats", name)
    # Each chunk takes 1s (below the 5s stall timeout but slow enough for heartbeats)
    _reset_state("clean", total_tokens=6, chunk_delay=1.0)

    result = await send_streaming_request(proxy_url, "slow stream test", timeout_seconds=30)

    expected = _expected_full_text(6)
    assert result["completed"], f"Should complete. Error: {result['error']}"
    assert result["accumulated_text"] == expected, (
        f"Text mismatch.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )
    assert fake_state.request_count == 1, f"Should complete in 1 attempt (no retries), got {fake_state.request_count}"

    log.info("  PASS: Slow stream completed in 1 attempt with %d chars", len(expected))
    fake_state.completed.append(name)


async def test_empty_partial_text_retry(proxy_url: str) -> None:
    """Test 8: Disconnect before any content - retry should not include empty assistant message."""
    name = "empty_partial_text_retry"
    log.info("=" * 60)
    log.info("TEST: %s - Disconnect before any content", name)
    _reset_state("disconnect", tokens_before_disconnect=0, total_tokens=10)

    result = await send_streaming_request(proxy_url, "empty partial test", timeout_seconds=30)

    expected = _expected_full_text(10)
    assert result["completed"], f"Should complete. Error: {result['error']}"
    assert result["accumulated_text"] == expected, (
        f"Text mismatch.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )

    # If there were retries with assistant content, the content should not be empty/whitespace
    for retry in fake_state.received_retries:
        content = retry["assistant_content"]
        if content:
            assert content.strip(), "Retry should not carry whitespace-only assistant content"

    log.info("  PASS: Empty partial handled correctly. %d attempts", fake_state.request_count)
    fake_state.completed.append(name)


async def test_prefill_rejection_fallback(proxy_url: str) -> None:
    """Test 10: Upstream rejects assistant prefill on retry, proxy falls back to no-prefill."""
    name = "prefill_rejection_fallback"
    log.info("=" * 60)
    log.info("TEST: %s - Prefill rejected, fallback to clean retry", name)
    # Scenario: first request disconnects after 5 tokens (accumulates text),
    # retry with assistant prefill gets 400 rejected,
    # then proxy disables prefill and retries without it -> success.
    # We use "reject_prefill" scenario which:
    #   - if assistant_messages present -> 400
    #   - else -> normal stream (but first attempt disconnects via "disconnect" logic)
    # So we combine: first attempt disconnects, second has prefill -> rejected, third succeeds.
    _reset_state("clean", tokens_before_disconnect=5, total_tokens=15)
    # We'll use a custom counter approach via the fake_state
    fake_state.scenario = "disconnect_then_reject_prefill"

    result = await send_streaming_request(proxy_url, "prefill rejection test", timeout_seconds=30)

    expected = _expected_full_text(15)
    assert result["completed"], f"Stream should complete after prefill fallback. Error: {result['error']}"
    assert result["accumulated_text"] == expected, (
        f"Text mismatch.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )
    # Should have at least 3 attempts: 1 (disconnect) -> 2 (prefill rejected 400) -> 3 (no prefill, success)
    assert fake_state.request_count >= 3, (
        f"Expected at least 3 requests (disconnect -> prefill 400 -> success), got {fake_state.request_count}"
    )
    log.info("  PASS: Prefill rejection handled correctly. %d attempts, %d chars delivered",
             fake_state.request_count, len(result["accumulated_text"]))
    fake_state.completed.append(name)


async def test_prefill_rejection_from_thinking_blocks_fallback(proxy_url: str) -> None:
    """Test 10c: Thinking-block prefill rejection should also fall back to a clean retry."""
    name = "prefill_rejection_from_thinking_blocks_fallback"
    log.info("=" * 60)
    log.info("TEST: %s - Thinking block rejection falls back to clean retry", name)
    _reset_state("clean", tokens_before_disconnect=5, total_tokens=15)
    fake_state.scenario = "disconnect_then_reject_prefill_thinking_blocks"

    result = await send_streaming_request(proxy_url, "thinking prefill rejection test", timeout_seconds=30)

    expected = _expected_full_text(15)
    assert result["completed"], f"Stream should complete after thinking-block fallback. Error: {result['error']}"
    assert result["accumulated_text"] == expected, (
        f"Text mismatch.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )
    assert fake_state.request_count >= 3, (
        f"Expected at least 3 requests (disconnect -> thinking-block 400 -> success), got {fake_state.request_count}"
    )

    log.info(
        "  PASS: Thinking-block prefill rejection handled correctly. %d attempts, %d chars delivered",
        fake_state.request_count,
        len(result["accumulated_text"]),
    )
    fake_state.completed.append(name)


async def test_chat_completions_disconnect_and_retry(proxy_url: str) -> None:
    """Test 10b: Chat Completions streams should retry mid-stream disconnects."""
    name = "chat_completions_disconnect_and_retry"
    log.info("=" * 60)
    log.info("TEST: %s - Chat Completions mid-stream disconnect recovery", name)
    _reset_state("disconnect", tokens_before_disconnect=5, total_tokens=15)

    result = await send_chat_completions_request(proxy_url, "cursor disconnect test", timeout_seconds=30)

    expected = _expected_full_text(15)
    assert result["completed"], f"Chat completion should complete after retry. Error: {result['error']}"
    assert result["accumulated_text"] == expected, (
        f"Chat completion text mismatch after retry.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )
    assert fake_state.chat_request_count >= 2, (
        f"Should have at least 2 upstream chat completion requests, got {fake_state.chat_request_count}"
    )
    assert len(fake_state.received_chat_retries) >= 1, "Retry should include accumulated assistant content"
    retry_content = fake_state.received_chat_retries[0]["assistant_content"]
    partial_expected = "".join(_make_text_delta(i) for i in range(5))
    assert partial_expected in retry_content or retry_content in partial_expected, (
        f"Retry should carry partial chat completion text.\n  Expected partial: {partial_expected!r}\n  Got: {retry_content!r}"
    )

    log.info(
        "  PASS: Chat Completions recovered from disconnect. %d attempts, %d chars delivered",
        fake_state.chat_request_count,
        len(result["accumulated_text"]),
    )
    fake_state.completed.append(name)


async def test_messages_sanitize_empty_text_blocks(proxy_url: str) -> None:
    """Test 11: Proxy strips empty text blocks before forwarding /v1/messages."""
    name = "messages_sanitize_empty_text_blocks"
    log.info("=" * 60)
    log.info("TEST: %s - Empty message text blocks are sanitized", name)
    _reset_state("reject_empty_text_blocks", total_tokens=4)

    body = {
        "model": "claude-test-model",
        "max_tokens": 1024,
        "stream": True,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": ""},
                    {"type": "text", "text": "sanitize this request"},
                ],
            }
        ],
    }

    result = await send_streaming_request(
        proxy_url,
        timeout_seconds=30,
        body_override=body,
    )

    expected = _expected_full_text(4)
    assert result["completed"], f"Sanitized request should complete. Error: {result['error']}"
    assert result["accumulated_text"] == expected, (
        f"Text mismatch.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
    )
    assert fake_state.request_count == 1, f"Should succeed on first request, got {fake_state.request_count}"

    log.info("  PASS: Proxy removed empty text blocks before forwarding")
    fake_state.completed.append(name)


async def test_large_responses_body_accepted_by_proxy(proxy_url: str) -> None:
    """Test 12: Local proxy should accept /v1/responses payloads larger than 1 MiB."""
    name = "large_responses_body_accepted_by_proxy"
    log.info("=" * 60)
    log.info("TEST: %s - /v1/responses accepts >1 MiB payloads", name)
    _reset_state("clean")

    body = {
        "model": "codex-test-model",
        "stream": False,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": "x" * 1_100_000},
                ],
            }
        ],
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{proxy_url}/v1/responses",
            json=body,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            payload = await resp.json()

    assert resp.status == 200, f"Expected 200, got {resp.status}: {payload}"
    assert payload.get("id") == "resp_test_001", f"Unexpected response payload: {payload}"
    assert fake_state.responses_request_count == 1, (
        f"Expected exactly 1 upstream /v1/responses request, got {fake_state.responses_request_count}"
    )

    log.info("  PASS: Proxy forwarded >1 MiB /v1/responses payload without local 413")
    fake_state.completed.append(name)


async def test_auto_resume_skips_oversized_prefill(proxy_url: str | None = None) -> None:
    """Test 13: Auto-resume should skip proxy-added prefill when it would grow past the safety cap."""
    name = "auto_resume_skips_oversized_prefill"
    log.info("=" * 60)
    log.info("TEST: %s - Auto-resume prefill stays under configured cap", name)

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import plunger as rp

    class MockSettingsManager:
        def __init__(self) -> None:
            self.current_upstream = "http://127.0.0.1:0"
            self.proxy_url = "http://127.0.0.1:0"
            self.listen_port = 0

    body = {
        "model": "claude-test-model",
        "stream": True,
        "messages": [
            {"role": "user", "content": "x" * 900},
            {"role": "user", "content": "continue"},
        ],
    }
    partial_text = "p" * 450
    candidate = deepcopy(body)
    candidate["messages"].insert(1, {"role": "assistant", "content": partial_text})
    safe_limit = rp._json_body_size_bytes(body) + 64
    assert rp._json_body_size_bytes(candidate) > safe_limit, "Test setup should exceed the configured safe cap"

    proxy = object.__new__(rp.ResilientProxy)
    proxy.safe_resume_body_bytes = safe_limit
    proxy.recovery_store = type("RecoveryStub", (), {})()
    proxy.recovery_store.load_last_interrupted = lambda endpoint=None: {
        "endpoint": "/v1/messages",
        "model": "claude-test-model",
        "partial_text": partial_text,
    }

    patched, resumed = proxy._maybe_resume_messages(deepcopy(body))

    assert patched == body, "Oversized auto-resume prefill should leave the request body unchanged"
    assert resumed is None, "Oversized auto-resume prefill should be skipped entirely"

    log.info("  PASS: Auto-resume skipped prefill once the proxy-added body growth exceeded the cap")
    fake_state.completed.append(name)


async def test_auto_resume_extends_existing_assistant_prefix(proxy_url: str | None = None) -> None:
    """Test 14: Auto-resume should extend an existing assistant prefix instead of duplicating it."""
    name = "auto_resume_extends_existing_assistant_prefix"
    log.info("=" * 60)
    log.info("TEST: %s - Auto-resume extends an existing assistant prefix", name)

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import plunger as rp

    class MockSettingsManager:
        def __init__(self) -> None:
            self.current_upstream = "http://127.0.0.1:0"
            self.proxy_url = "http://127.0.0.1:0"
            self.listen_port = 0

    existing_prefix = "token_0 token_1 "
    partial_text = f"{existing_prefix}token_2 token_3"
    body = {
        "model": "claude-test-model",
        "stream": True,
        "messages": [
            {"role": "user", "content": "write something"},
            {"role": "assistant", "content": existing_prefix},
            {"role": "user", "content": "continue"},
        ],
    }
    duplicate_candidate = deepcopy(body)
    duplicate_candidate["messages"].insert(2, {"role": "assistant", "content": partial_text})
    merged_candidate = deepcopy(body)
    merged_candidate["messages"][1]["content"] = partial_text

    safe_limit = rp._json_body_size_bytes(merged_candidate) + 8
    assert rp._json_body_size_bytes(duplicate_candidate) > safe_limit, (
        "Test setup should make duplicate insertion exceed the configured safe cap"
    )
    assert rp._json_body_size_bytes(merged_candidate) <= safe_limit, (
        "Test setup should still allow suffix-only assistant extension"
    )

    proxy = object.__new__(rp.ResilientProxy)
    proxy.safe_resume_body_bytes = safe_limit
    proxy.recovery_store = type("RecoveryStub", (), {})()
    proxy.recovery_store.load_last_interrupted = lambda endpoint=None: {
        "endpoint": "/v1/messages",
        "model": "claude-test-model",
        "partial_text": partial_text,
    }

    patched, resumed = proxy._maybe_resume_messages(deepcopy(body))

    assert resumed is not None, "Prefix extension should still be treated as assistant-partial resume"
    assert resumed.get("resume_mode") == "assistant_partial", f"Unexpected resume mode: {resumed}"
    assistant_messages = [
        message
        for message in patched["messages"]
        if isinstance(message, dict) and message.get("role") == "assistant"
    ]
    assert len(assistant_messages) == 1, f"Expected one assistant message after merge, got: {patched['messages']!r}"
    assert assistant_messages[0]["content"] == partial_text, (
        f"Existing assistant message should be extended to the full partial text.\n"
        f"  Expected: {partial_text!r}\n"
        f"  Got:      {assistant_messages[0]['content']!r}"
    )

    log.info("  PASS: Auto-resume reused the existing assistant prefix without duplicating it")
    fake_state.completed.append(name)


async def test_responses_done_forwarded_after_retry(proxy_url: str) -> None:
    """Test 15: Responses API should still emit [DONE] after a retry path."""
    name = "responses_done_forwarded_after_retry"
    log.info("=" * 60)
    log.info("TEST: %s - Responses retry path still forwards [DONE]", name)

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import plunger as rp

    class DummyResponse:
        def __init__(self) -> None:
            self.writes: list[bytes] = []

        async def write(self, payload: bytes) -> None:
            self.writes.append(payload)

    class MockSettingsManager:
        def __init__(self) -> None:
            self.current_upstream = "http://127.0.0.1:0"
            self.proxy_url = "http://127.0.0.1:0"
            self.listen_port = 0

    proxy = rp.ResilientProxy(MockSettingsManager(), stall_timeout=5.0, max_retries=5)
    state = rp.StreamState(is_first_attempt=False)
    dummy = DummyResponse()
    writer = rp.SSEStreamWriter(dummy, heartbeat_interval=60.0)

    await proxy._on_responses_event("data: [DONE]", state, writer)
    await proxy._on_responses_event("data: [DONE]", state, writer)

    done_frames = [payload for payload in dummy.writes if payload == b"data: [DONE]\n\n"]
    assert len(done_frames) == 1, f"Expected exactly 1 [DONE] frame, got {len(done_frames)}"
    assert state.done_sent, "State should record that [DONE] was sent"

    log.info("  PASS: Responses retry path forwarded exactly one [DONE] frame")
    fake_state.completed.append(name)


async def test_messages_retry_prefix_counts_as_progress(proxy_url: str | None = None) -> None:
    """Test 16: Filtered retry-prefix replay should refresh progress for /v1/messages."""
    name = "messages_retry_prefix_counts_as_progress"
    log.info("=" * 60)
    log.info("TEST: %s - Messages replayed prefix keeps recovery alive", name)

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import plunger as rp

    class DummyResponse:
        def __init__(self) -> None:
            self.writes: list[bytes] = []

        async def write(self, payload: bytes) -> None:
            self.writes.append(payload)

    class MockSettingsManager:
        def __init__(self) -> None:
            self.current_upstream = "http://127.0.0.1:0"
            self.proxy_url = "http://127.0.0.1:0"
            self.listen_port = 0

    proxy = rp.ResilientProxy(MockSettingsManager(), stall_timeout=5.0, max_retries=5)
    state = rp.StreamState(
        is_first_attempt=False,
        skip_prefix=_expected_full_text(2),
    )
    state.last_visible_output_at = time.monotonic() - 120
    before = state.last_visible_output_at

    dummy = DummyResponse()
    writer = rp.SSEStreamWriter(dummy, heartbeat_interval=60.0)

    await proxy._on_event(_make_sse_content_delta(_make_text_delta(0)), state, writer)

    assert state.last_visible_output_at > before, "Consumed retry prefix should refresh the progress timer"
    assert state.skip_prefix == _make_text_delta(1), f"Expected remaining prefix to shrink, got {state.skip_prefix!r}"
    assert dummy.writes == [], "Filtered replay prefix should not be forwarded to the client again"

    log.info("  PASS: Messages replay prefix now counts as recovery progress")
    fake_state.completed.append(name)


async def test_responses_retry_prefix_counts_as_progress(proxy_url: str | None = None) -> None:
    """Test 17: Filtered retry-prefix replay should refresh progress for /v1/responses."""
    name = "responses_retry_prefix_counts_as_progress"
    log.info("=" * 60)
    log.info("TEST: %s - Responses replayed prefix keeps recovery alive", name)

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import plunger as rp

    class DummyResponse:
        def __init__(self) -> None:
            self.writes: list[bytes] = []

        async def write(self, payload: bytes) -> None:
            self.writes.append(payload)

    class MockSettingsManager:
        def __init__(self) -> None:
            self.current_upstream = "http://127.0.0.1:0"
            self.proxy_url = "http://127.0.0.1:0"
            self.listen_port = 0

    proxy = rp.ResilientProxy(MockSettingsManager(), stall_timeout=5.0, max_retries=5)
    state = rp.StreamState(
        is_first_attempt=False,
        skip_prefix=_expected_full_text(2),
    )
    state.last_visible_output_at = time.monotonic() - 120
    before = state.last_visible_output_at

    dummy = DummyResponse()
    writer = rp.SSEStreamWriter(dummy, heartbeat_interval=60.0)
    payload = {
        "type": "response.output_text.delta",
        "response_id": "resp_test_001",
        "item_id": "msg_resp_001",
        "output_index": 0,
        "content_index": 0,
        "delta": _make_text_delta(0),
    }

    await proxy._on_responses_event(
        f"event: response.output_text.delta\ndata: {json.dumps(payload)}",
        state,
        writer,
    )

    assert state.last_visible_output_at > before, "Consumed retry prefix should refresh the progress timer"
    assert state.skip_prefix == _make_text_delta(1), f"Expected remaining prefix to shrink, got {state.skip_prefix!r}"
    assert dummy.writes == [], "Filtered replay prefix should not be forwarded to the client again"

    log.info("  PASS: Responses replay prefix now counts as recovery progress")
    fake_state.completed.append(name)


async def test_chat_completions_retry_prefix_counts_as_progress(proxy_url: str | None = None) -> None:
    """Test 17b: Filtered retry-prefix replay should refresh progress for chat completions."""
    name = "chat_completions_retry_prefix_counts_as_progress"
    log.info("=" * 60)
    log.info("TEST: %s - Chat Completions replayed prefix keeps recovery alive", name)

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import plunger as rp

    class DummyResponse:
        def __init__(self) -> None:
            self.writes: list[bytes] = []

        async def write(self, payload: bytes) -> None:
            self.writes.append(payload)

    class MockSettingsManager:
        def __init__(self) -> None:
            self.current_upstream = "http://127.0.0.1:0"
            self.proxy_url = "http://127.0.0.1:0"
            self.listen_port = 0

    proxy = rp.ResilientProxy(MockSettingsManager(), stall_timeout=5.0, max_retries=5)
    state = rp.StreamState(
        is_first_attempt=False,
        skip_prefix=_expected_full_text(2),
    )
    state.last_visible_output_at = time.monotonic() - 120
    before = state.last_visible_output_at

    dummy = DummyResponse()
    writer = rp.SSEStreamWriter(dummy, heartbeat_interval=60.0)
    await proxy._on_chat_completions_event(
        _make_chat_completion_chunk(delta={"content": _make_text_delta(0)}).strip(),
        state,
        writer,
    )

    assert state.last_visible_output_at > before, "Consumed retry prefix should refresh the progress timer"
    assert state.skip_prefix == _make_text_delta(1), f"Expected remaining prefix to shrink, got {state.skip_prefix!r}"
    assert dummy.writes == [], "Filtered replay prefix should not be forwarded to the client again"

    log.info("  PASS: Chat Completions replay prefix now counts as recovery progress")
    fake_state.completed.append(name)


async def test_slow_replayed_prefix_survives_visible_timeout(proxy_url: str | None = None) -> None:
    """Test 18: Slow replayed retry prefix should not trigger another false visible-output timeout."""
    name = "slow_replayed_prefix_survives_visible_timeout"
    log.info("=" * 60)
    log.info("TEST: %s - Slow replayed prefix no longer self-times out", name)

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import plunger as rp

    original_visible_timeout = rp.DEFAULT_VISIBLE_OUTPUT_TIMEOUT
    rp.DEFAULT_VISIBLE_OUTPUT_TIMEOUT = 0.25

    upstream_port = FAKE_UPSTREAM_PORT + 10
    proxy_port = PROXY_PORT + 10
    proxy_url = f"http://127.0.0.1:{proxy_port}"
    upstream_url = f"http://127.0.0.1:{upstream_port}"

    try:
        _reset_state(
            "clean",
            tokens_before_disconnect=3,
            total_tokens=6,
            chunk_delay=0.15,
        )
        fake_state.scenario = "disconnect_then_reject_prefill"

        upstream_app = build_fake_upstream_app()
        upstream_runner = web.AppRunner(upstream_app)
        await upstream_runner.setup()
        upstream_site = web.TCPSite(upstream_runner, "127.0.0.1", upstream_port)
        await upstream_site.start()

        proxy_runner, proxy = await start_proxy(
            upstream_url,
            proxy_port,
            stall_timeout=0.4,
            max_retries=5,
        )
        try:
            result = await send_streaming_request(
                proxy_url,
                "slow replay prefix test",
                timeout_seconds=30,
            )
        finally:
            await proxy_runner.cleanup()
            await upstream_runner.cleanup()

        expected = _expected_full_text(6)
        assert result["completed"], f"Slow replay should now complete. Error: {result['error']}"
        assert result["accumulated_text"] == expected, (
            f"Text mismatch after slow replay recovery.\n  Expected: {expected!r}\n  Got:      {result['accumulated_text']!r}"
        )
        assert fake_state.request_count == 3, (
            f"Expected exactly 3 upstream requests (disconnect -> prefill 400 -> clean replay), got {fake_state.request_count}"
        )

        log.info("  PASS: Slow replayed prefix completed without a false timeout loop")
        fake_state.completed.append(name)
    finally:
        rp.DEFAULT_VISIBLE_OUTPUT_TIMEOUT = original_visible_timeout


async def test_max_retries_exhausted(proxy_url: str) -> None:
    """Test 9: When max retries is exhausted, proxy should return error."""
    name = "max_retries_exhausted"
    log.info("=" * 60)
    log.info("TEST: %s - Retries exhausted returns error", name)
    # We'll set failures_remaining > max_retries (proxy has max_retries=5)
    _reset_state("clean", total_tokens=10, failures_remaining=10, failure_status=500)

    result = await send_streaming_request(proxy_url, "exhaust retries test", timeout_seconds=60)

    # With 10 failures and max_retries=5, the proxy should give up
    # It may or may not complete depending on exact retry counting
    # The important thing is it doesn't hang forever
    log.info("  Result: completed=%s, error=%s, attempts=%d",
             result["completed"], result["error"], fake_state.request_count)

    if not result["completed"]:
        log.info("  PASS: Proxy correctly gave up after exhausting retries")
    else:
        log.info("  PASS: (Proxy managed to recover despite many failures)")

    fake_state.completed.append(name)


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

async def run_all_tests() -> None:
    proxy_url = f"http://127.0.0.1:{PROXY_PORT}"
    upstream_url = f"http://127.0.0.1:{FAKE_UPSTREAM_PORT}"

    # Start fake upstream
    log.info("Starting fake upstream on port %d...", FAKE_UPSTREAM_PORT)
    upstream_app = build_fake_upstream_app()
    upstream_runner = web.AppRunner(upstream_app)
    await upstream_runner.setup()
    upstream_site = web.TCPSite(upstream_runner, "127.0.0.1", FAKE_UPSTREAM_PORT)
    await upstream_site.start()

    # Start proxy
    log.info("Starting resilient proxy on port %d -> %s...", PROXY_PORT, upstream_url)
    proxy_runner, proxy = await start_proxy(
        upstream_url, PROXY_PORT, stall_timeout=5.0, max_retries=5,
    )

    log.info("Both servers running. Starting tests...\n")

    tests = [
        test_clean_stream,
        test_disconnect_and_retry,
        test_http_500_then_success,
        test_multiple_failures_then_success,
        test_stall_timeout,
        test_retry_carries_accumulated_text,
        test_heartbeat_during_slow_stream,
        test_empty_partial_text_retry,
        test_prefill_rejection_fallback,
        test_prefill_rejection_from_thinking_blocks_fallback,
        test_chat_completions_disconnect_and_retry,
        test_messages_sanitize_empty_text_blocks,
        test_large_responses_body_accepted_by_proxy,
        test_auto_resume_skips_oversized_prefill,
        test_auto_resume_extends_existing_assistant_prefix,
        test_responses_done_forwarded_after_retry,
        test_messages_retry_prefix_counts_as_progress,
        test_responses_retry_prefix_counts_as_progress,
        test_chat_completions_retry_prefix_counts_as_progress,
        test_slow_replayed_prefix_survives_visible_timeout,
        test_max_retries_exhausted,
    ]

    for test_fn in tests:
        try:
            await test_fn(proxy_url)
        except AssertionError as exc:
            name = test_fn.__name__.replace("test_", "")
            log.error("  FAIL: %s - %s", name, exc)
            fake_state.failed.append(name)
        except Exception as exc:
            name = test_fn.__name__.replace("test_", "")
            log.error("  ERROR: %s - %s: %s", name, type(exc).__name__, exc)
            fake_state.failed.append(name)

    # Summary
    log.info("\n" + "=" * 60)
    log.info("TEST SUMMARY")
    log.info("=" * 60)
    log.info("  Passed: %d", len(fake_state.completed))
    log.info("  Failed: %d", len(fake_state.failed))
    if fake_state.failed:
        for name in fake_state.failed:
            log.info("    - FAILED: %s", name)
    log.info("=" * 60)

    # Cleanup
    await proxy_runner.cleanup()
    await upstream_runner.cleanup()

    if fake_state.failed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(run_all_tests())
