import asyncio

import plunger


class DummyResponse:
    def __init__(self) -> None:
        self.writes: list[bytes] = []

    async def write(self, payload: bytes) -> None:
        self.writes.append(payload)


class MockSettingsManager:
    def __init__(self) -> None:
        self.current_upstream = "http://127.0.0.1:0"
        self.current_upstream_source = "test"
        self.proxy_url = "http://127.0.0.1:0"
        self.listen_port = 0


def test_sse_writer_uses_custom_heartbeat_frame() -> None:
    async def _run() -> None:
        dummy = DummyResponse()
        writer = plunger.SSEStreamWriter(
            dummy,
            heartbeat_interval=0.01,
            heartbeat_frame=plunger.ANTHROPIC_PING_FRAME,
        )
        writer.start()
        await asyncio.sleep(0.025)
        await writer.close()

        assert dummy.writes, "Expected at least one heartbeat frame"
        assert all(
            payload == plunger.ANTHROPIC_PING_FRAME for payload in dummy.writes
        ), f"Expected Anthropic ping heartbeats, got {dummy.writes!r}"

    asyncio.run(_run())


def test_make_client_stream_uses_anthropic_ping_for_messages_only() -> None:
    proxy = plunger.ResilientProxy(MockSettingsManager(), stall_timeout=5.0, max_retries=0)

    messages_writer = proxy._make_client_stream(DummyResponse(), "/v1/messages")
    responses_writer = proxy._make_client_stream(DummyResponse(), "/v1/responses")

    assert messages_writer.heartbeat_frame == plunger.ANTHROPIC_PING_FRAME
    assert responses_writer.heartbeat_frame == plunger.SSE_HEARTBEAT_FRAME
