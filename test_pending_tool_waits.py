import plunger as rp


class MockSettingsManager:
    def __init__(self) -> None:
        self.current_upstream = "http://127.0.0.1:0"
        self.current_upstream_source = "test"
        self.proxy_url = "http://127.0.0.1:0"
        self.listen_port = 0


def _make_proxy() -> rp.ResilientProxy:
    return rp.ResilientProxy(MockSettingsManager(), stall_timeout=60.0, max_retries=0)


def test_register_pending_tool_wait_keeps_response_id_for_responses() -> None:
    proxy = _make_proxy()
    state = rp.StreamState(
        awaiting_tool_result=True,
        response_id="resp_match",
        model="gpt-5.4",
        tool_names=["shell_command"],
    )
    session = {
        "session_id": "session-1",
        "started_at": "2026-03-23 14:38:08",
        "request_summary": "tool wait test",
        "client_label": "127.0.0.1:58567",
    }

    proxy._register_pending_tool_wait("/v1/responses", session, state)

    pending = proxy.pending_tool_waits["session-1"]
    assert pending["response_id"] == "resp_match"


def test_responses_tool_result_prefers_matching_previous_response_id() -> None:
    proxy = _make_proxy()
    proxy.pending_tool_waits = {
        "oldest": {
            "session_id": "oldest",
            "endpoint": "/v1/responses",
            "response_id": "resp_oldest",
            "tool_names": ["shell_command"],
            "tool_count": 1,
            "request_summary": "older session",
            "_wait_started_at_ts": 100.0,
        },
        "target": {
            "session_id": "target",
            "endpoint": "/v1/responses",
            "response_id": "resp_target",
            "tool_names": ["shell_command"],
            "tool_count": 1,
            "request_summary": "target session",
            "_wait_started_at_ts": 200.0,
        },
    }

    body = {
        "previous_response_id": "resp_target",
        "input": [
            {
                "type": "function_call_output",
                "call_id": "call_123",
                "output": "ok",
            }
        ],
    }

    proxy._resolve_tool_wait_from_request("/v1/responses", body)

    assert "target" not in proxy.pending_tool_waits
    assert "oldest" in proxy.pending_tool_waits


def test_responses_tool_result_falls_back_to_oldest_without_response_id_match() -> None:
    proxy = _make_proxy()
    proxy.pending_tool_waits = {
        "oldest": {
            "session_id": "oldest",
            "endpoint": "/v1/responses",
            "response_id": "resp_oldest",
            "tool_names": ["shell_command"],
            "tool_count": 1,
            "request_summary": "older session",
            "_wait_started_at_ts": 100.0,
        },
        "newer": {
            "session_id": "newer",
            "endpoint": "/v1/responses",
            "response_id": "resp_newer",
            "tool_names": ["shell_command"],
            "tool_count": 1,
            "request_summary": "newer session",
            "_wait_started_at_ts": 200.0,
        },
    }

    body = {
        "previous_response_id": "resp_missing",
        "input": [
            {
                "type": "function_call_output",
                "call_id": "call_123",
                "output": "ok",
            }
        ],
    }

    proxy._resolve_tool_wait_from_request("/v1/responses", body)

    assert "oldest" not in proxy.pending_tool_waits
    assert "newer" in proxy.pending_tool_waits


def test_messages_continue_repairs_dangling_tool_use() -> None:
    proxy = _make_proxy()
    proxy.recovery_store = type(
        "RecoveryStub",
        (),
        {"load_last_interrupted": lambda self, endpoint=None: None},
    )()

    body = {
        "model": "claude-opus-4-6",
        "stream": True,
        "messages": [
            {"role": "user", "content": "Read the file and fix the bug."},
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "I will inspect the file first."},
                    {"type": "tool_use", "id": "toolu_123", "name": "Read", "input": {"file_path": "a.py"}},
                ],
            },
            {"role": "user", "content": "continue"},
        ],
    }

    patched, resumed = proxy._maybe_resume_messages(body)

    assert resumed is not None, "Dangling tool_use should be repaired before forwarding upstream"
    assert resumed["resume_mode"] == "dangling_tool_use_continue"
    assert patched != body
    assert patched["messages"][1]["role"] == "assistant"
    assert patched["messages"][1]["content"] == [
        {"type": "text", "text": "I will inspect the file first."}
    ]
    assert patched["messages"][-1]["role"] == "user"
    assert "Continue the same task from the current workspace state" in patched["messages"][-1]["content"]
    assert all(
        block.get("type") != "tool_use"
        for message in patched["messages"]
        if isinstance(message, dict) and isinstance(message.get("content"), list)
        for block in message["content"]
        if isinstance(block, dict)
    )


def test_messages_continue_does_not_repair_when_tool_result_is_present() -> None:
    proxy = _make_proxy()
    proxy.recovery_store = type(
        "RecoveryStub",
        (),
        {"load_last_interrupted": lambda self, endpoint=None: None},
    )()

    body = {
        "model": "claude-opus-4-6",
        "stream": True,
        "messages": [
            {"role": "user", "content": "Read the file and fix the bug."},
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "toolu_123", "name": "Read", "input": {"file_path": "a.py"}},
                ],
            },
            {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_123", "content": "done"},
                ],
            },
            {"role": "user", "content": "continue"},
        ],
    }

    patched, resumed = proxy._maybe_resume_messages(body)

    assert patched == body
    assert resumed is None
