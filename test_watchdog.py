import json
from argparse import Namespace

import plunger


class _DummyResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __enter__(self) -> "_DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


class _DummyOpener:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.calls: list[tuple[str, float | None]] = []

    def open(self, url: str, timeout: float | None = None) -> _DummyResponse:
        self.calls.append((url, timeout))
        return _DummyResponse(self.payload)


def test_proxy_health_is_ok_uses_lightweight_healthz(monkeypatch) -> None:
    opener = _DummyOpener({"status": "ok"})
    monkeypatch.setattr(plunger, "LOCAL_OPENER", opener)

    assert plunger._proxy_health_is_ok(8462) is True
    assert opener.calls == [("http://127.0.0.1:8462/healthz", 1.0)]


def test_watchdog_failure_grace_extends_while_port_is_open(monkeypatch) -> None:
    monkeypatch.setattr(plunger, "_local_port_is_open", lambda port: True)
    assert (
        plunger._watchdog_failure_grace_for_port(8462)
        == plunger.WATCHDOG_PORT_OPEN_FAILURE_GRACE
    )

    monkeypatch.setattr(plunger, "_local_port_is_open", lambda port: False)
    assert (
        plunger._watchdog_failure_grace_for_port(8462)
        == plunger.WATCHDOG_FAILURE_GRACE
    )


def test_health_failure_burst_filters_recent_failed_events(monkeypatch) -> None:
    monkeypatch.setattr(plunger.time, "time", lambda: 200.0)

    events = [
        {"kind": "failed", "created_at_ms": 190_000},
        {"kind": "failed", "created_at_ms": 10_000},
        {"kind": "recovered", "created_at_ms": 199_000},
        {"kind": "failed", "created_at_ms": 198_000},
    ]

    burst = plunger._health_failure_burst(events, window_seconds=30.0)

    assert burst == [
        {"kind": "failed", "created_at_ms": 190_000},
        {"kind": "failed", "created_at_ms": 198_000},
    ]


def test_health_disconnect_burst_filters_recent_restartworthy_disconnects(monkeypatch) -> None:
    monkeypatch.setattr(plunger.time, "time", lambda: 200.0)

    events = [
        {
            "kind": "disconnect",
            "created_at_ms": 195_000,
            "meta": {"reason": "HTTP 502: bad response status code 502"},
        },
        {
            "kind": "disconnect",
            "created_at_ms": 196_000,
            "meta": {"reason": "stall after 60.0s"},
        },
        {
            "kind": "disconnect",
            "created_at_ms": 197_000,
            "meta": {"reason": "proxy_error: error sending request"},
        },
        {"kind": "recovered", "created_at_ms": 198_000},
    ]

    burst = plunger._health_disconnect_burst(events, window_seconds=30.0)

    assert burst == [
        {
            "kind": "disconnect",
            "created_at_ms": 195_000,
            "meta": {"reason": "HTTP 502: bad response status code 502"},
        },
        {
            "kind": "disconnect",
            "created_at_ms": 197_000,
            "meta": {"reason": "proxy_error: error sending request"},
        },
    ]


def test_maybe_evolve_aggressive_policy_writes_generated_policy(tmp_path, monkeypatch) -> None:
    policy_file = tmp_path / "aggressive_policy.py"
    state_file = tmp_path / "aggressive_state.json"
    log_file = tmp_path / "aggressive.log"

    monkeypatch.setattr(plunger, "AGGRESSIVE_POLICY_FILE", policy_file)
    monkeypatch.setattr(plunger, "AGGRESSIVE_STATE_FILE", state_file)
    monkeypatch.setattr(plunger, "AGGRESSIVE_LOG_FILE", log_file)
    monkeypatch.setattr(plunger.time, "time", lambda: 200.0)

    policy = plunger._maybe_evolve_aggressive_policy(
        enabled=True,
        disconnect_burst=[
            {
                "id": "d1",
                "kind": "disconnect",
                "created_at_ms": 195_000,
                "meta": {"reason": "HTTP 502: bad response status code 502"},
            },
            {
                "id": "d2",
                "kind": "disconnect",
                "created_at_ms": 196_000,
                "meta": {"reason": "proxy_error: error sending request"},
            },
            {
                "id": "d3",
                "kind": "disconnect",
                "created_at_ms": 197_000,
                "meta": {"reason": "HTTP 503: upstream unavailable"},
            },
        ],
        failure_burst=[],
        effective_timeout=60.0,
    )

    assert policy["disconnect_burst_threshold"] == 2
    assert policy["timeout_floor"] == 75.0
    assert "POLICY =" in policy_file.read_text(encoding="utf-8")
    assert state_file.exists()


def test_run_supervisor_restarts_and_tunes_timeout_after_failure_burst(monkeypatch) -> None:
    incidents: list[tuple[str, str, dict[str, object] | None]] = []
    spawns: list[tuple[int, float | None]] = []

    class _StubIncidents:
        def add(self, kind: str, reason: str, *, meta=None):
            incidents.append((kind, reason, meta))
            return {"kind": kind, "reason": reason}

    class _StopLoop(RuntimeError):
        pass

    args = Namespace(
        port=8462,
        timeout=60.0,
        retries=-1,
        max_body_mb=32.0,
        safe_resume_body_mb=19.0,
        watch_interval=1.0,
        upstream=None,
        supervisor_proxy_pid=321,
    )

    monkeypatch.setattr(plunger.os, "getpid", lambda: 9001)
    monkeypatch.setattr(plunger, "IncidentHistory", _StubIncidents)
    monkeypatch.setattr(plunger, "_append_supervisor_log", lambda message: None)
    monkeypatch.setattr(plunger, "_write_supervisor_state", lambda payload: None)
    monkeypatch.setattr(plunger, "_clear_supervisor_state", lambda expected_pid=None: None)
    monkeypatch.setattr(plunger, "_proxy_health_is_ok", lambda port: True)
    monkeypatch.setattr(
        plunger,
        "_request_local_json",
        lambda port, path, timeout=1.0, method="GET": {
            "pid": 321,
            "events": [
                {
                    "id": "f1",
                    "kind": "failed",
                    "created_at_ms": 195_000,
                    "meta": {"reason": "stall after 60.0s"},
                },
                {
                    "id": "f2",
                    "kind": "failed",
                    "created_at_ms": 196_000,
                    "meta": {"reason": "stall after 60.0s"},
                },
                {
                    "id": "f3",
                    "kind": "failed",
                    "created_at_ms": 197_000,
                    "meta": {"reason": "stall after 60.0s"},
                },
            ],
        },
    )
    monkeypatch.setattr(plunger.time, "time", lambda: 200.0)
    monkeypatch.setattr(plunger.time, "monotonic", lambda: 500.0)
    monkeypatch.setattr(
        plunger,
        "_spawn_proxy_under_supervision",
        lambda ns, *, supervisor_pid, timeout_override=None: spawns.append(
            (supervisor_pid, timeout_override)
        )
        or 654,
    )
    monkeypatch.setattr(
        plunger.time,
        "sleep",
        lambda seconds: (_ for _ in ()).throw(_StopLoop()),
    )

    try:
        plunger.run_supervisor(args)
    except _StopLoop:
        pass

    assert len(incidents) == 1
    assert incidents[0][0] == "restart"
    assert "failed recoveries" in incidents[0][1]
    assert spawns == [(9001, 75.0)]


def test_run_supervisor_restarts_after_disconnect_burst(monkeypatch) -> None:
    incidents: list[tuple[str, str, dict[str, object] | None]] = []
    spawns: list[tuple[int, float | None]] = []

    class _StubIncidents:
        def add(self, kind: str, reason: str, *, meta=None):
            incidents.append((kind, reason, meta))
            return {"kind": kind, "reason": reason}

    class _StopLoop(RuntimeError):
        pass

    args = Namespace(
        port=8462,
        timeout=60.0,
        retries=-1,
        max_body_mb=32.0,
        safe_resume_body_mb=19.0,
        watch_interval=1.0,
        upstream=None,
        supervisor_proxy_pid=321,
    )

    monkeypatch.setattr(plunger.os, "getpid", lambda: 9002)
    monkeypatch.setattr(plunger, "IncidentHistory", _StubIncidents)
    monkeypatch.setattr(plunger, "_append_supervisor_log", lambda message: None)
    monkeypatch.setattr(plunger, "_write_supervisor_state", lambda payload: None)
    monkeypatch.setattr(plunger, "_clear_supervisor_state", lambda expected_pid=None: None)
    monkeypatch.setattr(plunger, "_proxy_health_is_ok", lambda port: True)
    monkeypatch.setattr(
        plunger,
        "_request_local_json",
        lambda port, path, timeout=1.0, method="GET": {
            "pid": 321,
            "events": [
                {
                    "id": "d1",
                    "kind": "disconnect",
                    "created_at_ms": 195_000,
                    "meta": {"reason": "HTTP 502: bad response status code 502"},
                },
                {
                    "id": "d2",
                    "kind": "disconnect",
                    "created_at_ms": 196_000,
                    "meta": {"reason": "proxy_error: error sending request"},
                },
                {
                    "id": "d3",
                    "kind": "disconnect",
                    "created_at_ms": 197_000,
                    "meta": {"reason": "HTTP 503: upstream unavailable"},
                },
            ],
        },
    )
    monkeypatch.setattr(plunger.time, "time", lambda: 200.0)
    monkeypatch.setattr(plunger.time, "monotonic", lambda: 500.0)
    monkeypatch.setattr(
        plunger,
        "_spawn_proxy_under_supervision",
        lambda ns, *, supervisor_pid, timeout_override=None: spawns.append(
            (supervisor_pid, timeout_override)
        )
        or 655,
    )
    monkeypatch.setattr(
        plunger.time,
        "sleep",
        lambda seconds: (_ for _ in ()).throw(_StopLoop()),
    )

    try:
        plunger.run_supervisor(args)
    except _StopLoop:
        pass

    assert len(incidents) == 1
    assert incidents[0][0] == "restart"
    assert "disconnects within" in incidents[0][1]
    assert spawns == [(9002, 75.0)]


def test_trigger_recovery_updates_active_session_snapshot_immediately(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(plunger, "RECOVERY_DIR", tmp_path)
    monkeypatch.setattr(plunger, "LAST_INTERRUPTED_FILE", tmp_path / "last_interrupted.json")
    monkeypatch.setattr(plunger, "ACTIVE_SESSIONS_FILE", tmp_path / "active_sessions.json")

    store = plunger.RecoveryStore()
    session = store.begin_session(
        "/v1/messages",
        {
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "503 也要一直重试"}],
        },
        "http://127.0.0.1:15721",
    )
    state = plunger.StreamState(request_summary=session["request_summary"])
    store.track_active_session(session, state)

    server = object.__new__(plunger.ResilientProxy)
    server.recovery_store = store
    server.stats = {"recoveries_triggered": 0}
    disconnects: list[tuple[str, int, str]] = []
    server._record_disconnect = (
        lambda endpoint, state, attempt, reason: disconnects.append((endpoint, attempt, reason))
    )

    plunger.ResilientProxy._trigger_recovery(
        server,
        "/v1/messages",
        state,
        0,
        'HTTP 503: {"error":{"message":"No available channel"}}',
        session=session,
    )

    snapshot = store.active_sessions_snapshot()
    assert len(snapshot) == 1
    assert snapshot[0]["status"] == "recovering"
    assert snapshot[0]["recovery_attempt"] == 1
    assert snapshot[0]["reason"].startswith("HTTP 503:")
    assert disconnects == [
        (
            "/v1/messages",
            0,
            'HTTP 503: {"error":{"message":"No available channel"}}',
        )
    ]
    assert server.stats["recoveries_triggered"] == 1

    plunger.ResilientProxy._trigger_recovery(
        server,
        "/v1/messages",
        state,
        2,
        "HTTP 503: upstream overloaded",
        session=session,
    )

    snapshot = store.active_sessions_snapshot()
    assert snapshot[0]["status"] == "recovering"
    assert snapshot[0]["recovery_attempt"] == 3
    assert snapshot[0]["reason"] == "HTTP 503: upstream overloaded"
    assert len(disconnects) == 1
    assert server.stats["recoveries_triggered"] == 1
