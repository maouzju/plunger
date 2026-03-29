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

    assert dashboard._wait_for_proxy_exit(expected_started_at_ms=None, timeout=1.0) is True
    assert process.wait_calls == [0.15]
    assert dashboard.process is None


def test_write_gui_state_is_best_effort(monkeypatch) -> None:
    monkeypatch.setattr(plunger_ui, "GUI_STATE_FILE", RaisingStateFile())
    monkeypatch.setattr(plunger_ui, "RECOVERY_DIR", StubRecoveryDir())

    plunger_ui._write_gui_state()


def test_start_proxy_uses_hidden_headless_command_for_frozen_build(monkeypatch) -> None:
    popen_calls: list[tuple[list[str], dict[str, object]]] = []

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
    monkeypatch.setattr(
        plunger_ui.subprocess,
        "Popen",
        lambda command, **kwargs: popen_calls.append((command, kwargs)) or FakeProcess(),
    )

    dashboard._start_proxy()

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


def test_stop_proxy_terminates_supervisor_before_shutdown(monkeypatch) -> None:
    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard.closed = False
    dashboard.pending_start_previous_started_at_ms = None
    dashboard.pending_action = None
    dashboard.pending_deadline = 0.0
    dashboard._fetch_health = lambda: {"supervisor_pid": 4321}
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
    assert requests == [(("/control/shutdown",), {"method": "POST", "timeout": 1.2})]


def test_summarize_recovery_counts_tracks_today_and_history(monkeypatch) -> None:
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None) -> "FixedDateTime":
            return cls(2026, 3, 24, 12, 0, 0)

    dashboard = object.__new__(plunger_ui.ProxyDashboard)
    dashboard._parse_timestamp = plunger_ui.ProxyDashboard._parse_timestamp.__get__(
        dashboard, plunger_ui.ProxyDashboard
    )

    monkeypatch.setattr(plunger_ui, "datetime", FixedDateTime)

    events = [
        {
            "kind": "recovered",
            "created_at_ms": int(datetime(2026, 3, 24, 9, 0, 0).timestamp() * 1000),
        },
        {
            "kind": "recovered",
            "created_at_ms": int(datetime(2026, 3, 23, 22, 0, 0).timestamp() * 1000),
        },
        {
            "kind": "recovered",
            "timestamp": "2026-03-24 08:30:00",
        },
        {
            "kind": "disconnect",
            "created_at_ms": int(datetime(2026, 3, 24, 10, 0, 0).timestamp() * 1000),
        },
    ]

    today_success, history_success = dashboard._summarize_recovery_counts(events)

    assert today_success == 2
    assert history_success == 3


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
