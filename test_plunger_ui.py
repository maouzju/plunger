import subprocess
from argparse import Namespace
from datetime import datetime

import plunger_ui


class RaisingStateFile:
    def write_text(self, *args, **kwargs) -> None:
        raise PermissionError("denied")


class StubRecoveryDir:
    def mkdir(self, *args, **kwargs) -> None:
        return None


class FakeRoot:
    def __init__(self) -> None:
        self.after_calls: list[tuple[int, object]] = []
        self.cancelled: str | None = None
        self.destroyed = False

    def after(self, delay_ms: int, callback) -> str:
        self.after_calls.append((delay_ms, callback))
        return f"after-{len(self.after_calls)}"

    def after_cancel(self, after_id: str) -> None:
        self.cancelled = after_id

    def destroy(self) -> None:
        self.destroyed = True


class FakeProcess:
    def __init__(self, *, running: bool = True, exit_on_wait: bool = False) -> None:
        self.running = running
        self.exit_on_wait = exit_on_wait
        self.wait_calls: list[float | None] = []
        self.terminate_calls = 0

    def poll(self) -> int | None:
        return None if self.running else 0

    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls.append(timeout)
        if self.exit_on_wait:
            self.running = False
            return 0
        if self.running:
            raise subprocess.TimeoutExpired("proxy", timeout or 0.0)
        return 0

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.running = False


class FakeVar:
    def __init__(self, value=None) -> None:
        self.value = value

    def set(self, value) -> None:
        self.value = value

    def get(self):
        return self.value


class FakeActionButton:
    def set_scheme(self, *args, **kwargs) -> None:
        return None

    def stop_pulse(self) -> None:
        return None

    def start_arc(self) -> None:
        return None

    def start_pulse(self) -> None:
        return None

    def stop_arc(self) -> None:
        return None


def _make_dashboard(process: FakeProcess) -> plunger_ui.ProxyDashboard:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.root = FakeRoot()
    dashboard.after_id = "after-1"
    dashboard.closed = False
    dashboard.process = process
    dashboard._extract_started_at_ms = plunger_ui.ProxyDashboard._extract_started_at_ms.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    return dashboard


def test_on_close_only_closes_window_without_stopping_proxy() -> None:
    dashboard = _make_dashboard(FakeProcess())
    dashboard._on_close()

    assert dashboard.closed is True
    assert dashboard.after_id is None
    assert dashboard.root.cancelled == "after-1"
    assert dashboard.root.destroyed is True


def test_wait_for_proxy_exit_does_not_finish_while_tracked_process_is_still_running(
    monkeypatch,
) -> None:
    class FakeClock:
        def __init__(self) -> None:
            self.now = 0.0

        def time(self) -> float:
            return self.now

        def sleep(self, seconds: float) -> None:
            self.now += seconds

    clock = FakeClock()
    monkeypatch.setattr(plunger_ui.time, "time", clock.time)
    monkeypatch.setattr(plunger_ui.time, "sleep", clock.sleep)

    process = FakeProcess(exit_on_wait=True)
    dashboard = _make_dashboard(process)
    dashboard._fetch_health = lambda: None

    assert dashboard._wait_for_proxy_exit(
        expected_started_at_ms=None,
        expected_pid=None,
        timeout=1.0,
    ) is True
    assert process.wait_calls == [0.15]
    assert dashboard.process is None


def test_wait_for_proxy_exit_waits_for_expected_pid_to_terminate(monkeypatch) -> None:
    class FakeClock:
        def __init__(self) -> None:
            self.now = 0.0

        def time(self) -> float:
            return self.now

        def sleep(self, seconds: float) -> None:
            self.now += seconds

    clock = FakeClock()
    monkeypatch.setattr(plunger_ui.time, "time", clock.time)
    monkeypatch.setattr(plunger_ui.time, "sleep", clock.sleep)

    dashboard = _make_dashboard(FakeProcess(running=False))
    dashboard.process = None
    dashboard._fetch_health = lambda: None

    states = iter(["unknown", "unknown", "dead"])
    observed: list[int] = []
    monkeypatch.setattr(
        plunger_ui,
        "_managed_proxy_process_state",
        lambda pid: observed.append(pid) or next(states),
    )

    assert dashboard._wait_for_proxy_exit(
        expected_started_at_ms=123,
        expected_pid=456,
        timeout=1.0,
    ) is True
    assert observed == [456, 456, 456]
    assert clock.now >= 0.3


def test_write_gui_state_is_best_effort(monkeypatch) -> None:
    monkeypatch.setattr(plunger_ui, "GUI_STATE_FILE", RaisingStateFile())
    monkeypatch.setattr(plunger_ui, "RECOVERY_DIR", StubRecoveryDir())

    plunger_ui._write_gui_state()


def test_managed_proxy_process_state_accepts_older_build_path(monkeypatch) -> None:
    monkeypatch.setattr(plunger_ui, "_process_exists", lambda pid: True)
    monkeypatch.setattr(
        plunger_ui,
        "_process_command_line",
        lambda pid: 'D:\\old-build\\Plunger\\Plunger.exe --headless --port 8462',
    )

    assert plunger_ui._managed_proxy_process_state(1234) == "managed"


def test_managed_proxy_process_state_rechecks_command_line_each_time(monkeypatch) -> None:
    monkeypatch.setattr(plunger_ui, "_process_exists", lambda pid: True)
    commands = iter(
        [
            'D:\\old-build\\Plunger\\Plunger.exe --headless --port 8462',
            'C:\\Windows\\System32\\notepad.exe',
        ]
    )
    monkeypatch.setattr(
        plunger_ui,
        "_process_command_line",
        lambda pid: next(commands),
    )

    assert plunger_ui._managed_proxy_process_state(1234) == "managed"
    assert plunger_ui._managed_proxy_process_state(1234) == "other"


def test_start_proxy_uses_hidden_headless_command_for_frozen_build(monkeypatch) -> None:
    popen_calls: list[tuple[list[str], dict[str, object]]] = []
    previous_instance: dict[str, int | None] = {}

    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.args = Namespace(
        port=8462,
        timeout=60.0,
        retries=-1,
        watch_interval=1.0,
        upstream=None,
    )
    dashboard.base_url = "http://127.0.0.1:8462"
    dashboard.process = None
    dashboard.pending_action = None
    dashboard.pending_deadline = 0.0
    dashboard.pending_start_previous_started_at_ms = None
    dashboard.pending_stop_expected_pid = None
    health_calls = iter(
        [
            {"started_at_ms": 1234, "pid": 4321},
            None,
        ]
    )
    dashboard._fetch_health = lambda: next(health_calls)
    dashboard._stop_existing_proxy_before_start = (
        lambda previous_started_at_ms, previous_pid: previous_instance.update(
            {
                "started_at_ms": previous_started_at_ms,
                "pid": previous_pid,
            }
        )
        or True
    )
    dashboard._extract_started_at_ms = plunger_ui.ProxyDashboard._extract_started_at_ms.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._set_busy = lambda message: None
    dashboard._poll_pending_action = lambda: None
    dashboard._t = lambda key, **kwargs: key

    monkeypatch.setattr(plunger_ui, "_is_frozen_app", lambda: True)
    monkeypatch.setattr(
        plunger_ui.subprocess,
        "Popen",
        lambda command, **kwargs: popen_calls.append((command, kwargs)) or FakeProcess(),
    )

    dashboard._start_proxy()

    assert previous_instance == {"started_at_ms": 1234, "pid": 4321}
    assert len(popen_calls) == 1
    command, kwargs = popen_calls[0]
    assert command == [
        plunger_ui.sys.executable,
        "--headless",
        "--port",
        "8462",
        "--timeout",
        "60.0",
        "--retries",
        "-1",
        "--watch-interval",
        "1.0",
        "--aggressive-autoevolve",
        "--enable-supervisor",
    ]
    assert kwargs["cwd"] == str(plunger_ui._app_base_dir())


def test_stop_existing_proxy_before_start_force_terminates_previous_pid_after_timeout(
    monkeypatch,
) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.process = None
    dashboard._request_json = lambda *args, **kwargs: {"status": "stopping"}
    wait_calls: list[tuple[int | None, int | None, float]] = []
    wait_results = iter([False, True])
    dashboard._wait_for_proxy_exit = (
        lambda previous_started_at_ms, previous_pid, timeout: wait_calls.append(
            (previous_started_at_ms, previous_pid, timeout)
        )
        or next(wait_results)
    )
    dashboard._terminate_tracked_process = lambda wait_timeout: None

    terminated: list[int] = []
    monkeypatch.setattr(plunger_ui, "_managed_proxy_process_state", lambda pid: "managed")
    monkeypatch.setattr(plunger_ui, "_terminate_pid_tree", lambda pid: terminated.append(pid))

    assert dashboard._stop_existing_proxy_before_start(1234, 4321) is True

    assert wait_calls == [(1234, 4321, 3.0), (1234, 4321, 1.0)]
    assert terminated == [4321]


def test_stop_existing_proxy_before_start_aborts_when_pid_cannot_be_verified(monkeypatch) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.process = None
    dashboard._request_json = lambda *args, **kwargs: {"status": "stopping"}
    dashboard._wait_for_proxy_exit = lambda previous_started_at_ms, previous_pid, timeout: False
    dashboard._terminate_tracked_process = lambda wait_timeout: None

    monkeypatch.setattr(plunger_ui, "_managed_proxy_process_state", lambda pid: "unknown")

    assert dashboard._stop_existing_proxy_before_start(1234, 4321) is False


def test_start_proxy_uses_state_pid_when_health_is_unavailable(monkeypatch) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.args = Namespace(
        port=8462,
        timeout=60.0,
        retries=-1,
        watch_interval=1.0,
        upstream=None,
    )
    dashboard.base_url = "http://127.0.0.1:8462"
    dashboard.process = None
    dashboard.pending_action = None
    dashboard.pending_deadline = 0.0
    dashboard.pending_start_previous_started_at_ms = None
    dashboard.pending_stop_expected_pid = None
    dashboard._fetch_health = lambda: None
    previous_instance: dict[str, int | None] = {}
    dashboard._stop_existing_proxy_before_start = (
        lambda previous_started_at_ms, previous_pid: previous_instance.update(
            {
                "started_at_ms": previous_started_at_ms,
                "pid": previous_pid,
            }
        )
        or True
    )
    dashboard._extract_started_at_ms = plunger_ui.ProxyDashboard._extract_started_at_ms.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._set_busy = lambda message: None
    dashboard._poll_pending_action = lambda: None
    dashboard._t = lambda key, **kwargs: key

    monkeypatch.setattr(plunger_ui.time, "time", lambda: 100.0)
    monkeypatch.setattr(plunger_ui, "_read_proxy_pid", lambda base_url: 7654)
    monkeypatch.setattr(plunger_ui, "_is_frozen_app", lambda: True)
    monkeypatch.setattr(
        plunger_ui.subprocess,
        "Popen",
        lambda command, **kwargs: FakeProcess(),
    )

    dashboard._start_proxy()

    assert previous_instance == {"started_at_ms": None, "pid": 7654}
    assert dashboard.pending_start_previous_pid == 7654
    assert dashboard.pending_deadline == 120.0


def test_stop_proxy_terminates_supervisor_before_shutdown(monkeypatch) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.closed = False
    dashboard.pending_start_previous_started_at_ms = None
    dashboard.pending_stop_expected_pid = None
    dashboard.pending_action = None
    dashboard.pending_deadline = 0.0
    dashboard._fetch_health = lambda: {"pid": 8765, "supervisor_pid": 4321}
    requests: list[tuple[tuple[object, ...], dict[str, object]]] = []
    terminated: list[int] = []
    dashboard._request_json = (
        lambda *args, **kwargs: requests.append((args, kwargs)) or {"status": "stopping"}
    )
    dashboard._set_busy = lambda message: None
    dashboard._poll_pending_action = lambda: None
    dashboard._t = lambda key, **kwargs: key

    monkeypatch.setattr(plunger_ui, "_terminate_pid_tree", lambda pid: terminated.append(pid))
    monkeypatch.setattr(plunger_ui, "_read_supervisor_pid", lambda: 0)

    dashboard._stop_proxy()

    assert terminated == [4321]
    assert dashboard.pending_stop_expected_pid == 8765
    assert requests == [(("/control/shutdown",), {"method": "POST", "timeout": 1.2})]


def test_stop_proxy_uses_state_pid_when_health_pid_is_missing(monkeypatch) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.base_url = "http://127.0.0.1:8462"
    dashboard.closed = False
    dashboard.pending_start_previous_started_at_ms = None
    dashboard.pending_stop_expected_pid = None
    dashboard.pending_action = None
    dashboard.pending_deadline = 0.0
    dashboard._fetch_health = lambda: {"supervisor_pid": 4321}
    dashboard._request_json = lambda *args, **kwargs: {"status": "stopping"}
    dashboard._set_busy = lambda message: None
    dashboard._poll_pending_action = lambda: None
    dashboard._t = lambda key, **kwargs: key

    monkeypatch.setattr(plunger_ui, "_terminate_pid_tree", lambda pid: None)
    monkeypatch.setattr(plunger_ui, "_read_supervisor_pid", lambda: 0)
    monkeypatch.setattr(plunger_ui, "_read_proxy_pid", lambda base_url: 7654)

    dashboard._stop_proxy()

    assert dashboard.pending_stop_expected_pid == 7654


def test_poll_pending_stop_force_terminates_expected_pid_after_deadline(monkeypatch) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.root = FakeRoot()
    dashboard.closed = False
    dashboard.process = None
    dashboard.pending_action = "stop"
    dashboard.pending_deadline = 1.0
    dashboard.pending_start_previous_started_at_ms = None
    dashboard.pending_stop_expected_pid = 8765
    dashboard._fetch_health = lambda: None
    dashboard._t = lambda key, **kwargs: key
    dashboard._clear_busy = lambda message: None
    dashboard._refresh_state = lambda: None

    terminated: list[int] = []
    monkeypatch.setattr(plunger_ui.time, "time", lambda: 5.0)
    monkeypatch.setattr(plunger_ui, "_managed_proxy_process_state", lambda pid: "managed")
    monkeypatch.setattr(plunger_ui, "_terminate_pid_tree", lambda pid: terminated.append(pid))

    dashboard._poll_pending_action()

    assert terminated == [8765]
    assert dashboard.pending_action == "stop"
    assert dashboard.pending_deadline == 7.0
    assert len(dashboard.root.after_calls) == 1


def test_poll_pending_stop_waits_for_expected_proxy_pid_before_marking_stopped(monkeypatch) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.root = FakeRoot()
    dashboard.closed = False
    dashboard.process = None
    dashboard.pending_action = "stop"
    dashboard.pending_deadline = 10.0
    dashboard.pending_start_previous_started_at_ms = None
    dashboard.pending_stop_expected_pid = 8765
    dashboard._fetch_health = lambda: None
    dashboard._t = lambda key, **kwargs: key
    clear_messages: list[str] = []
    refresh_calls: list[str] = []
    dashboard._clear_busy = lambda message: clear_messages.append(message)
    dashboard._refresh_state = lambda: refresh_calls.append("refresh")

    monkeypatch.setattr(plunger_ui.time, "time", lambda: 1.0)
    pid_state = {"value": "unknown"}
    monkeypatch.setattr(
        plunger_ui,
        "_managed_proxy_process_state",
        lambda pid: pid_state["value"],
    )

    dashboard._poll_pending_action()

    assert dashboard.pending_action == "stop"
    assert clear_messages == []
    assert refresh_calls == []
    assert len(dashboard.root.after_calls) == 1

    pid_state["value"] = "dead"
    dashboard.root.after_calls.clear()
    dashboard._poll_pending_action()

    assert dashboard.pending_action is None
    assert dashboard.pending_stop_expected_pid is None
    assert clear_messages == ["footer_stopped"]
    assert refresh_calls == ["refresh"]


def test_poll_pending_stop_times_out_when_expected_pid_is_unknown(monkeypatch) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.root = FakeRoot()
    dashboard.closed = False
    dashboard.process = None
    dashboard.pending_action = "stop"
    dashboard.pending_deadline = 1.0
    dashboard.pending_start_previous_started_at_ms = None
    dashboard.pending_stop_expected_pid = 8765
    dashboard._fetch_health = lambda: None
    dashboard._t = lambda key, **kwargs: key
    clear_messages: list[str] = []
    refresh_calls: list[str] = []
    dashboard._clear_busy = lambda message: clear_messages.append(message)
    dashboard._refresh_state = lambda: refresh_calls.append("refresh")

    monkeypatch.setattr(plunger_ui.time, "time", lambda: 5.0)
    monkeypatch.setattr(plunger_ui, "_managed_proxy_process_state", lambda pid: "unknown")

    dashboard._poll_pending_action()

    assert dashboard.pending_action is None
    assert dashboard.pending_stop_expected_pid is None
    assert clear_messages == ["footer_stop_timeout"]
    assert refresh_calls == ["refresh"]


def test_is_previous_instance_health_falls_back_to_pid_for_older_health_payloads() -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.pending_start_previous_started_at_ms = None
    dashboard.pending_start_previous_pid = 4321
    dashboard._extract_started_at_ms = plunger_ui.ProxyDashboard._extract_started_at_ms.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )

    assert dashboard._is_previous_instance_health({"pid": 4321}) is True
    assert dashboard._is_previous_instance_health({"pid": 9876}) is False


def test_is_previous_instance_health_prefers_started_at_ms_over_pid_reuse() -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.pending_start_previous_started_at_ms = 1234
    dashboard.pending_start_previous_pid = 4321
    dashboard._extract_started_at_ms = plunger_ui.ProxyDashboard._extract_started_at_ms.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )

    assert dashboard._is_previous_instance_health({"pid": 4321, "started_at_ms": 1234}) is True
    assert dashboard._is_previous_instance_health({"pid": 4321, "started_at_ms": 9999}) is False


def test_poll_pending_start_waits_when_previous_instance_matches_pid_only(monkeypatch) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.root = FakeRoot()
    dashboard.closed = False
    dashboard.process = FakeProcess()
    dashboard.pending_action = "start"
    dashboard.pending_deadline = 10.0
    dashboard.pending_start_previous_started_at_ms = None
    dashboard.pending_start_previous_pid = 4321
    dashboard.pending_stop_expected_pid = None
    health_calls = iter([{"pid": 4321}, {"pid": 9876}])
    dashboard._fetch_health = lambda: next(health_calls)
    dashboard._extract_started_at_ms = plunger_ui.ProxyDashboard._extract_started_at_ms.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._is_previous_instance_health = (
        plunger_ui.ProxyDashboard._is_previous_instance_health.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )
    clear_messages: list[str] = []
    applied: list[dict[str, int]] = []
    dashboard._clear_busy = lambda message: clear_messages.append(message)
    dashboard._apply_health = lambda health: applied.append(dict(health))
    dashboard._refresh_state = lambda: None
    dashboard._t = lambda key, **kwargs: key

    monkeypatch.setattr(plunger_ui.time, "time", lambda: 1.0)

    dashboard._poll_pending_action()

    assert dashboard.pending_action == "start"
    assert clear_messages == []
    assert applied == []
    assert len(dashboard.root.after_calls) == 1

    dashboard.root.after_calls.clear()
    dashboard._poll_pending_action()

    assert dashboard.pending_action is None
    assert dashboard.pending_start_previous_pid is None
    assert clear_messages == ["footer_online"]
    assert applied == [{"pid": 9876}]


def test_summarize_recovery_outcomes_ignore_recovered_chain_that_later_failed(monkeypatch) -> None:
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None) -> "FixedDateTime":
            return cls(2026, 3, 24, 12, 0, 0)

    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard._parse_timestamp = plunger_ui.ProxyDashboard._parse_timestamp.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._event_datetime = plunger_ui.ProxyDashboard._event_datetime.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._event_recovery_chain_key = (
        plunger_ui.ProxyDashboard._event_recovery_chain_key.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )

    monkeypatch.setattr(plunger_ui, "datetime", FixedDateTime)

    events = [
        {
            "kind": "disconnect",
            "endpoint": "/v1/messages",
            "created_at_ms": int(datetime(2026, 3, 24, 9, 0, 0).timestamp() * 1000),
            "meta": {"request_summary": "same task"},
        },
        {
            "kind": "recovered",
            "endpoint": "/v1/messages",
            "created_at_ms": int(datetime(2026, 3, 24, 9, 0, 0).timestamp() * 1000),
            "meta": {"request_summary": "same task"},
        },
        {
            "kind": "failed",
            "endpoint": "/v1/messages",
            "created_at_ms": int(datetime(2026, 3, 24, 9, 1, 0).timestamp() * 1000),
            "meta": {"request_summary": "same task"},
        },
        {
            "kind": "disconnect",
            "endpoint": "/v1/messages",
            "created_at_ms": int(datetime(2026, 3, 24, 10, 0, 0).timestamp() * 1000),
            "meta": {"request_summary": "rescued task"},
        },
        {
            "kind": "recovered",
            "endpoint": "/v1/messages",
            "created_at_ms": int(datetime(2026, 3, 24, 10, 1, 0).timestamp() * 1000),
            "meta": {"request_summary": "rescued task"},
        },
    ]

    summary = dashboard._summarize_recovery_outcomes(events)

    assert summary == {
        "triggers": 2,
        "success": 1,
        "today_success": 1,
        "history_success": 1,
    }


def test_summarize_recovery_outcomes_keeps_distinct_session_ids_separate(monkeypatch) -> None:
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None) -> "FixedDateTime":
            return cls(2026, 3, 24, 12, 0, 0)

    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard._parse_timestamp = plunger_ui.ProxyDashboard._parse_timestamp.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._event_datetime = plunger_ui.ProxyDashboard._event_datetime.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._event_recovery_chain_key = (
        plunger_ui.ProxyDashboard._event_recovery_chain_key.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )

    monkeypatch.setattr(plunger_ui, "datetime", FixedDateTime)

    events = [
        {
            "kind": "disconnect",
            "endpoint": "/v1/messages",
            "created_at_ms": int(datetime(2026, 3, 24, 9, 0, 0).timestamp() * 1000),
            "meta": {"root_session_id": "root-1", "request_summary": "same summary"},
        },
        {
            "kind": "recovered",
            "endpoint": "/v1/messages",
            "created_at_ms": int(datetime(2026, 3, 24, 9, 1, 0).timestamp() * 1000),
            "meta": {"root_session_id": "root-1", "request_summary": "same summary"},
        },
        {
            "kind": "disconnect",
            "endpoint": "/v1/messages",
            "created_at_ms": int(datetime(2026, 3, 24, 9, 2, 0).timestamp() * 1000),
            "meta": {"root_session_id": "root-2", "request_summary": "same summary"},
        },
        {
            "kind": "failed",
            "endpoint": "/v1/messages",
            "created_at_ms": int(datetime(2026, 3, 24, 9, 3, 0).timestamp() * 1000),
            "meta": {"root_session_id": "root-2", "request_summary": "same summary"},
        },
    ]

    summary = dashboard._summarize_recovery_outcomes(events)

    assert summary == {
        "triggers": 2,
        "success": 1,
        "today_success": 1,
        "history_success": 1,
    }


def test_apply_health_prefers_lifetime_totals_and_finalized_current_run_counts(monkeypatch) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.language = "zh_CN"
    dashboard.base_url = "http://127.0.0.1:8462"
    dashboard.pending_action = None
    dashboard.action_var = FakeVar()
    dashboard.subtitle_var = FakeVar()
    dashboard.listen_var = FakeVar()
    dashboard.upstream_var = FakeVar()
    dashboard.message_var = FakeVar()
    dashboard.current_run_only_var = FakeVar(False)
    dashboard.reconnect_only_var = FakeVar(False)
    dashboard.action_button = FakeActionButton()
    dashboard.stat_value_vars = {
        key: FakeVar("0")
        for key in ("requests", "success", "triggers", "today_success", "history_success")
    }
    dashboard._t = plunger_ui.ProxyDashboard._t.__get__(dashboard, plunger_ui.ProxyDashboard)
    dashboard._parse_timestamp = plunger_ui.ProxyDashboard._parse_timestamp.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._event_datetime = plunger_ui.ProxyDashboard._event_datetime.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._event_recovery_chain_key = (
        plunger_ui.ProxyDashboard._event_recovery_chain_key.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )
    dashboard._summarize_recovery_outcomes = (
        plunger_ui.ProxyDashboard._summarize_recovery_outcomes.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )
    dashboard._events_since_start = plunger_ui.ProxyDashboard._events_since_start.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._source_label = plunger_ui.ProxyDashboard._source_label.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._load_recorded_events = lambda: []
    dashboard._load_recorded_stats = lambda: {"total": 88}
    dashboard._merge_active_sessions = lambda *args: []
    dashboard._fill_active_sessions = lambda *args, **kwargs: None
    dashboard._fill_events = lambda *args, **kwargs: None

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None) -> "FixedDateTime":
            return cls(2026, 3, 24, 12, 0, 0)

    monkeypatch.setattr(plunger_ui, "datetime", FixedDateTime)

    health = {
        "started_at": "2026-03-24 10:00:00",
        "started_at_ms": int(datetime(2026, 3, 24, 10, 0, 0).timestamp() * 1000),
        "listen": "http://127.0.0.1:8462",
        "upstream": "http://127.0.0.1:15721",
        "upstream_source": "cc_switch",
        "lifetime_total": 88,
        "current_run_recovery_summary": {"triggers": 2, "success": 1},
        "events": [
            {
                "kind": "disconnect",
                "endpoint": "/v1/messages",
                "created_at_ms": int(datetime(2026, 3, 24, 10, 5, 0).timestamp() * 1000),
                "meta": {"request_summary": "same task"},
            },
            {
                "kind": "recovered",
                "endpoint": "/v1/messages",
                "created_at_ms": int(datetime(2026, 3, 24, 10, 6, 0).timestamp() * 1000),
                "meta": {"request_summary": "same task"},
            },
            {
                "kind": "failed",
                "endpoint": "/v1/messages",
                "created_at_ms": int(datetime(2026, 3, 24, 10, 7, 0).timestamp() * 1000),
                "meta": {"request_summary": "same task"},
            },
            {
                "kind": "disconnect",
                "endpoint": "/v1/messages",
                "created_at_ms": int(datetime(2026, 3, 24, 10, 8, 0).timestamp() * 1000),
                "meta": {"request_summary": "rescued task"},
            },
            {
                "kind": "recovered",
                "endpoint": "/v1/messages",
                "created_at_ms": int(datetime(2026, 3, 24, 10, 9, 0).timestamp() * 1000),
                "meta": {"request_summary": "rescued task"},
            },
        ],
    }

    dashboard._apply_health(health)

    assert dashboard.stat_value_vars["requests"].get() == "88"
    assert dashboard.stat_value_vars["triggers"].get() == "2"
    assert dashboard.stat_value_vars["success"].get() == "1"
    assert dashboard.stat_value_vars["today_success"].get() == "1"
    assert dashboard.stat_value_vars["history_success"].get() == "1"


def test_is_newer_version_handles_v_prefix_and_prerelease() -> None:
    assert plunger_ui._is_newer_version("v0.2.0", "0.1.9") is True
    assert plunger_ui._is_newer_version("0.1.0", "v0.1") is False
    assert plunger_ui._is_newer_version("0.1.0-rc1", "0.1.0") is False


def test_fetch_latest_release_info_prefers_windows_zip_asset(monkeypatch) -> None:
    monkeypatch.setattr(
        plunger_ui,
        "_github_request_json",
        lambda url, timeout=8.0: {
            "tag_name": "v0.2.0",
            "html_url": "https://github.com/maouzju/plunger/releases/tag/v0.2.0",
            "assets": [
                {
                    "name": "notes.txt",
                    "browser_download_url": "https://example.com/notes.txt",
                },
                {
                    "name": "Plunger.exe",
                    "browser_download_url": "https://example.com/Plunger.exe",
                },
                {
                    "name": "Plunger-windows.zip",
                    "browser_download_url": "https://example.com/Plunger-windows.zip",
                },
            ],
        },
    )
    monkeypatch.setattr(plunger_ui.os, "name", "nt")

    info = plunger_ui._fetch_latest_release_info("0.1.0")

    assert info["latest_version"] == "0.2.0"
    assert info["is_newer"] is True
    assert info["asset_name"] == "Plunger-windows.zip"
    assert info["asset_url"] == "https://example.com/Plunger-windows.zip"


def test_format_active_session_shows_recovering_retry_state() -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.language = "zh_CN"
    dashboard._t = plunger_ui.ProxyDashboard._t.__get__(dashboard, plunger_ui.ProxyDashboard)
    dashboard._endpoint_label = plunger_ui.ProxyDashboard._endpoint_label.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._request_summary_is_usable = (
        plunger_ui.ProxyDashboard._request_summary_is_usable.__get__(
            dashboard, plunger_ui.ProxyDashboard
        )
    )
    dashboard._format_active_session = plunger_ui.ProxyDashboard._format_active_session.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )

    formatted = dashboard._format_active_session(
        {
            "status": "recovering",
            "endpoint_label": "messages",
            "updated_at": "2026-03-29 15:20:00",
            "request_summary": "为我在这个文件夹下设计一个",
            "model": "claude-sonnet-4-6",
            "client_label": "127.0.0.1:10101",
            "recovery_attempt": 4,
            "reason": 'HTTP 503: {"error":{"message":"No available channel"}}',
            "last_user_text_preview": "为我在这个文件夹下设计一个",
            "partial_text_preview": "",
            "partial_chars": 0,
        }
    )

    assert "第 4 次重试" in formatted
    assert "提示：" in formatted
    assert "中转站" in formatted
    assert "可用通道" in formatted
