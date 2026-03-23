import subprocess
from argparse import Namespace

import plunger_ui


class RaisingStateFile:
    def write_text(self, *args, **kwargs) -> None:
        raise PermissionError("denied")


class StubRecoveryDir:
    def mkdir(self, *args, **kwargs) -> None:
        return None


class FakeRoot:
    def __init__(self) -> None:
        self.cancelled: str | None = None
        self.destroyed = False

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


def test_on_close_waits_for_graceful_shutdown_before_terminating() -> None:
    dashboard = _make_dashboard(FakeProcess())
    requests: list[tuple[tuple[object, ...], dict[str, object]]] = []
    terminate_calls: list[float] = []

    dashboard._fetch_health = lambda: {"started_at_ms": 123}
    dashboard._request_json = (
        lambda *args, **kwargs: requests.append((args, kwargs)) or {"status": "stopping"}
    )
    dashboard._wait_for_proxy_exit = lambda started_at_ms, timeout: (
        started_at_ms == 123 and timeout == 8.0
    )
    dashboard._terminate_tracked_process = lambda wait_timeout: terminate_calls.append(wait_timeout)

    dashboard._on_close()

    assert dashboard.closed is True
    assert dashboard.after_id is None
    assert dashboard.root.cancelled == "after-1"
    assert dashboard.root.destroyed is True
    assert len(requests) == 1
    assert requests[0][0] == ("/control/shutdown",)
    assert requests[0][1] == {"method": "POST", "timeout": 1.2}
    assert terminate_calls == []


def test_on_close_terminates_when_graceful_shutdown_stalls() -> None:
    dashboard = _make_dashboard(FakeProcess())
    terminate_calls: list[float] = []

    dashboard._fetch_health = lambda: {"started_at_ms": 321}
    dashboard._request_json = lambda *args, **kwargs: {"status": "stopping"}
    dashboard._wait_for_proxy_exit = lambda started_at_ms, timeout: False
    dashboard._terminate_tracked_process = lambda wait_timeout: terminate_calls.append(wait_timeout)

    dashboard._on_close()

    assert dashboard.root.destroyed is True
    assert terminate_calls == [1.5]


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

    assert dashboard._wait_for_proxy_exit(expected_started_at_ms=None, timeout=1.0) is True
    assert process.wait_calls == [0.15]
    assert dashboard.process is None


def test_write_gui_state_is_best_effort(monkeypatch) -> None:
    monkeypatch.setattr(plunger_ui, "GUI_STATE_FILE", RaisingStateFile())
    monkeypatch.setattr(plunger_ui, "RECOVERY_DIR", StubRecoveryDir())

    plunger_ui._write_gui_state()


def test_start_proxy_uses_embedded_runner_for_frozen_build(monkeypatch) -> None:
    created: list[FakeEmbeddedProcess] = []

    class FakeEmbeddedProcess:
        def __init__(self, cli_args: list[str], *, base_url: str) -> None:
            self.cli_args = cli_args
            self.base_url = base_url
            self.started = False
            created.append(self)

        def start(self) -> None:
            self.started = True

        def poll(self) -> int | None:
            return None

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
    dashboard._fetch_health = lambda: None
    dashboard._stop_existing_proxy_before_start = lambda previous_started_at_ms: None
    dashboard._extract_started_at_ms = plunger_ui.ProxyDashboard._extract_started_at_ms.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )
    dashboard._set_busy = lambda message: None
    dashboard._poll_pending_action = lambda: None
    dashboard._t = lambda key, **kwargs: key

    monkeypatch.setattr(plunger_ui, "_is_frozen_app", lambda: True)
    monkeypatch.setattr(plunger_ui, "EmbeddedProxyProcess", FakeEmbeddedProcess)
    monkeypatch.setattr(
        plunger_ui.subprocess,
        "Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Popen should not be used")),
    )

    dashboard._start_proxy()

    assert len(created) == 1
    assert dashboard.process is created[0]
    assert created[0].started is True
    assert created[0].base_url == "http://127.0.0.1:8462"
    assert created[0].cli_args == [
        "--port",
        "8462",
        "--timeout",
        "60.0",
        "--retries",
        "-1",
        "--watch-interval",
        "1.0",
    ]
