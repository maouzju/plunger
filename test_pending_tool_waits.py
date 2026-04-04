import asyncio

import plunger as rp


class MockSettingsManager:
    def __init__(self) -> None:
        self.current_upstream = "http://127.0.0.1:0"
        self.current_upstream_source = "test"
        self.proxy_url = "http://127.0.0.1:0"
        self.listen_port = 0


def _make_proxy() -> rp.ResilientProxy:
    return rp.ResilientProxy(MockSettingsManager(), stall_timeout=60.0, max_retries=0)


def test_responses_tail_fetch_timeout_uses_bounded_window() -> None:
    proxy = rp.ResilientProxy(MockSettingsManager(), stall_timeout=60.0, max_retries=0)
    assert proxy._responses_tail_fetch_timeout() == 30.0

    fast_proxy = rp.ResilientProxy(MockSettingsManager(), stall_timeout=5.0, max_retries=0)
    assert fast_proxy._responses_tail_fetch_timeout() == 12.0


def test_persistent_stats_survive_proxy_restart(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(rp, "STATS_FILE", tmp_path / "stats.json")

    proxy = _make_proxy()
    proxy._increment_stat("total")
    proxy._increment_stat("total")
    proxy._increment_stat("recoveries_triggered")
    proxy._increment_stat("success")
    proxy.persistent_stats.flush(force=True)

    payload = proxy._build_health_payload()
    assert payload["total"] == 2
    assert payload["current_run_stats"]["total"] == 2
    assert payload["current_run_recovery_summary"] == {"triggers": 0, "success": 0}
    assert payload["lifetime_total"] == 2
    assert payload["lifetime_recoveries_triggered"] == 1
    assert payload["lifetime_success"] == 1

    restarted_proxy = _make_proxy()
    restarted_payload = restarted_proxy._build_health_payload()
    assert restarted_payload["total"] == 0
    assert restarted_payload["current_run_stats"]["total"] == 0
    assert restarted_payload["current_run_recovery_summary"] == {"triggers": 0, "success": 0}
    assert restarted_payload["lifetime_total"] == 2
    assert restarted_payload["lifetime_recoveries_triggered"] == 1
    assert restarted_payload["lifetime_success"] == 1


def test_persistent_stats_batch_disk_writes(monkeypatch, tmp_path) -> None:
    writes: list[dict[str, object]] = []

    monkeypatch.setattr(
        rp,
        "_write_json",
        lambda path, payload: writes.append(dict(payload)),
    )

    stats = rp.PersistentStats(
        tmp_path / "stats.json",
        persist_interval_seconds=60.0,
        max_pending_updates=10,
    )
    stats.add("total")
    stats.add("success")

    assert writes == []

    stats.flush(force=True)

    assert len(writes) == 1
    assert writes[0]["total"] == 1
    assert writes[0]["success"] == 1


def test_recovery_outcome_tracker_uses_root_session_ids() -> None:
    tracker = rp.RecoveryOutcomeTracker()

    tracker.record("root-a", "disconnect")
    tracker.record("root-a", "recovered")
    tracker.record("root-b", "disconnect")
    tracker.record("root-b", "failed")

    assert tracker.summary() == {"triggers": 2, "success": 1}


def test_handle_shutdown_takeover_marks_cleanup_to_skip_restore() -> None:
    proxy = _make_proxy()
    recorded_events: list[tuple[tuple[object, ...], dict[str, object]]] = []
    proxy.event_history = type(
        "EventHistoryStub",
        (),
        {"add": lambda self, *args, **kwargs: recorded_events.append((args, kwargs))},
    )()

    class DummyRequest:
        query = {"mode": "takeover"}
        app = {
            "stop_event": asyncio.Event(),
            "cleanup_state": {"skip_restore": False, "reason": "manual"},
        }

    async def run_case() -> None:
        response = await proxy.handle_shutdown(DummyRequest())
        assert response.status == 200
        assert DummyRequest.app["cleanup_state"] == {
            "skip_restore": True,
            "reason": "takeover",
        }
        await asyncio.sleep(0.25)
        assert DummyRequest.app["stop_event"].is_set()

    asyncio.run(run_case())

    assert recorded_events
    args, kwargs = recorded_events[0]
    assert args[:3] == (
        "service_takeover",
        "Proxy handoff",
        "Another Plunger instance is taking over this port; restore is skipped.",
    )
    assert kwargs["meta"] == {"reason": "takeover", "skip_restore": True}


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


def test_build_health_payload_prefers_pending_tool_wait_status(monkeypatch) -> None:
    proxy = _make_proxy()
    proxy.recovery_store = type(
        "RecoveryStub",
        (),
        {
            "active_sessions_snapshot": lambda self: [
                {
                    "session_id": "session-1",
                    "endpoint": "/v1/responses",
                    "endpoint_label": "responses",
                    "model": "gpt-5.4",
                    "upstream": "http://127.0.0.1:15721",
                    "started_at": "2026-03-30 11:27:23",
                    "updated_at": "2026-03-30 11:27:24",
                    "status": "running",
                    "request_summary": "permissions instructions",
                    "client_label": "127.0.0.1:52031",
                }
            ],
            "load_last_interrupted": lambda self, endpoint=None: None,
        },
    )()
    proxy.pending_tool_waits = {
        "session-1": {
            "session_id": "session-1",
            "endpoint": "/v1/responses",
            "endpoint_label": "responses",
            "model": "gpt-5.4",
            "started_at": "2026-03-30 11:27:23",
            "updated_at": "2026-03-30 11:27:52",
            "status": "tool_result_timeout",
            "request_summary": "permissions instructions",
            "client_label": "127.0.0.1:52031",
            "tool_names": ["shell_command"],
            "tool_count": 1,
            "_wait_started_at_ts": 120.0,
            "_timed_out": True,
        }
    }

    monkeypatch.setattr(rp.time, "time", lambda: 200.0)

    payload = proxy._build_health_payload()

    assert payload["active_sessions_count"] == 1
    assert payload["active_session"]["status"] == "running"
    assert payload["active_session"]["upstream"] == "http://127.0.0.1:15721"
    assert payload["display_active_sessions_count"] == 1
    assert payload["display_active_session"]["status"] == "tool_result_timeout"
    assert payload["display_active_session"]["tool_names"] == ["shell_command"]
    assert payload["display_active_session"]["wait_seconds"] == 80
    assert payload["display_active_session"]["upstream"] == "http://127.0.0.1:15721"
    assert payload["pending_tool_waits_count"] == 1


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
