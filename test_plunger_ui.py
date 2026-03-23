import subprocess

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
