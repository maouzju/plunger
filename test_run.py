import run


def test_discover_managed_ports_uses_state_ports_before_default(monkeypatch) -> None:
    states = {
        run.PROXY_STATE_FILE: {"listen_port": 9123},
        run.CODEX_STATE_FILE: {"proxy_base_url": "http://127.0.0.1:8567/v1"},
    }
    monkeypatch.setattr(run, "_read_json_file", lambda path: states.get(path))

    assert run._discover_managed_ports(8462) == [9123, 8567, 8462]


def test_scan_managed_process_pids_matches_only_other_related_processes(monkeypatch) -> None:
    monkeypatch.setattr(
        run,
        "_managed_process_markers",
        lambda: (
            "d:\\git\\resilient-proxy\\run.pyw",
            "d:\\git\\resilient-proxy\\plunger.py",
        ),
    )
    monkeypatch.setattr(
        run,
        "_snapshot_windows_processes",
        lambda: [
            {"ProcessId": 100, "CommandLine": 'pythonw "D:\\Git\\resilient-proxy\\run.pyw"'},
            {"ProcessId": 101, "CommandLine": 'python "D:\\Git\\resilient-proxy\\plunger.py"'},
            {"ProcessId": 102, "CommandLine": 'python "D:\\Git\\other\\run.pyw"'},
            {"ProcessId": 103, "CommandLine": ""},
        ],
    )

    assert run._scan_managed_process_pids(current_pid=100) == {101}


def test_filter_managed_process_pids_keeps_only_verified_managed_processes(monkeypatch) -> None:
    monkeypatch.setattr(
        run,
        "_managed_process_markers",
        lambda: ("d:\\git\\resilient-proxy\\plunger.py",),
    )
    monkeypatch.setattr(
        run,
        "_snapshot_windows_processes",
        lambda: [
            {"ProcessId": 222, "CommandLine": 'python "D:\\Git\\resilient-proxy\\plunger.py" --run-supervisor'},
            {"ProcessId": 333, "CommandLine": 'python "D:\\Git\\other\\worker.py"'},
        ],
    )

    assert run._filter_managed_process_pids({111, 222, 333}, current_pid=900) == {222}


def test_request_shutdown_for_port_uses_takeover_mode(monkeypatch) -> None:
    calls: list[tuple[int, str, str, float]] = []

    def fake_request_local_json(
        port: int,
        path: str,
        *,
        method: str = "GET",
        timeout: float = 1.2,
    ) -> dict[str, object]:
        calls.append((port, path, method, timeout))
        if path == "/health":
            return {"safety_watchdog_pid": 4321, "supervisor_pid": 9876}
        return {"status": "stopping"}

    monkeypatch.setattr(run, "_request_local_json", fake_request_local_json)

    assert run._request_shutdown_for_port(8462) == {4321, 9876}
    assert calls == [
        (8462, "/health", "GET", 0.8),
        (8462, "/control/shutdown?mode=takeover", "POST", 1.2),
    ]


def test_collect_state_pids_includes_supervisor_state_pid(monkeypatch) -> None:
    states = {
        run.GUI_STATE_FILE: {"pid": 111},
        run.PROXY_STATE_FILE: {"pid": 222},
        run.SUPERVISOR_STATE_FILE: {"pid": 333},
    }
    monkeypatch.setattr(run, "_read_json_file", lambda path: states.get(path))

    assert run._collect_state_pids() == {111, 222, 333}


def test_prelaunch_cleanup_shuts_down_then_kills_without_restoring_settings(monkeypatch) -> None:
    events: list[tuple[str, object]] = []

    monkeypatch.setattr(run.os, "getpid", lambda: 900)
    monkeypatch.setattr(run, "_discover_managed_ports", lambda default_port: [9123, default_port])
    monkeypatch.setattr(run, "_collect_state_pids", lambda: {111, 222})
    monkeypatch.setattr(
        run,
        "_request_shutdown_for_port",
        lambda port: events.append(("shutdown", port)) or ({333} if port == 9123 else {555}),
    )
    monkeypatch.setattr(
        run,
        "_wait_for_port_release",
        lambda port, timeout: events.append(("wait", (port, timeout))) or False,
    )
    monkeypatch.setattr(
        run,
        "_scan_managed_process_pids",
        lambda current_pid: events.append(("scan", current_pid)) or {222, 444},
    )
    monkeypatch.setattr(
        run,
        "_filter_managed_process_pids",
        lambda candidate_pids, current_pid: events.append(("filter", (set(candidate_pids), current_pid))) or set(candidate_pids),
    )
    monkeypatch.setattr(
        run,
        "_terminate_pid_tree",
        lambda pid, *, current_pid: events.append(("kill", (pid, current_pid))),
    )
    monkeypatch.setattr(
        run,
        "_clear_stale_gui_state",
        lambda current_pid: events.append(("clear-gui", current_pid)),
    )

    run._prelaunch_cleanup(8462)

    assert events[:5] == [
        ("shutdown", 9123),
        ("wait", (9123, 1.5)),
        ("shutdown", 8462),
        ("wait", (8462, 1.5)),
        ("scan", 900),
    ]
    assert ("filter", ({111, 222, 333, 444, 555}, 900)) in events
    assert [
        event for event in events if event[0] == "kill"
    ] == [
        ("kill", (111, 900)),
        ("kill", (222, 900)),
        ("kill", (333, 900)),
        ("kill", (444, 900)),
        ("kill", (555, 900)),
    ]
    assert events[-3:] == [
        ("wait", (9123, 2.5)),
        ("wait", (8462, 2.5)),
        ("clear-gui", 900),
    ]


def test_main_headless_runs_prelaunch_cleanup_before_proxy_main(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(run, "_prelaunch_cleanup", lambda port: calls.append(("cleanup", port)))
    monkeypatch.setattr(
        run.sys,
        "argv",
        ["run.py", "--headless", "--port", "9123", "--timeout", "60"],
    )

    def fake_proxy_main() -> None:
        calls.append(("proxy_main", list(run.sys.argv)))

    import types

    monkeypatch.setitem(run.sys.modules, "plunger", types.SimpleNamespace(main=fake_proxy_main))

    run.main()

    assert calls == [
        ("cleanup", 9123),
        ("proxy_main", ["run.py", "--port", "9123", "--timeout", "60"]),
    ]


def test_should_prelaunch_cleanup_skips_internal_headless_roles() -> None:
    assert run._should_prelaunch_cleanup(["--headless", "--port", "8462"]) is True
    assert run._should_prelaunch_cleanup(["--headless", "--run-supervisor", "--port", "8462"]) is False
    assert run._should_prelaunch_cleanup(["--headless", "--watchdog-parent-pid", "123", "--watchdog-listen-port", "8462"]) is False
    assert run._should_prelaunch_cleanup(["--headless", "--watchdog-parent-pid=123", "--watchdog-listen-port=8462"]) is False
    assert run._should_prelaunch_cleanup(["--headless", "--managed-by-supervisor", "--supervisor-pid", "456"]) is False


def test_main_headless_supervisor_skips_prelaunch_cleanup(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(run, "_prelaunch_cleanup", lambda port: calls.append(("cleanup", port)))
    monkeypatch.setattr(
        run.sys,
        "argv",
        ["run.py", "--headless", "--run-supervisor", "--port", "9123", "--timeout", "60"],
    )

    def fake_proxy_main() -> None:
        calls.append(("proxy_main", list(run.sys.argv)))

    import types

    monkeypatch.setitem(run.sys.modules, "plunger", types.SimpleNamespace(main=fake_proxy_main))

    run.main()

    assert calls == [
        ("proxy_main", ["run.py", "--run-supervisor", "--port", "9123", "--timeout", "60"]),
    ]


def test_main_headless_watchdog_skips_prelaunch_cleanup(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(run, "_prelaunch_cleanup", lambda port: calls.append(("cleanup", port)))
    monkeypatch.setattr(
        run.sys,
        "argv",
        [
            "run.py",
            "--headless",
            "--watchdog-parent-pid",
            "123",
            "--watchdog-listen-port",
            "9123",
        ],
    )

    def fake_proxy_main() -> None:
        calls.append(("proxy_main", list(run.sys.argv)))

    import types

    monkeypatch.setitem(run.sys.modules, "plunger", types.SimpleNamespace(main=fake_proxy_main))

    run.main()

    assert calls == [
        (
            "proxy_main",
            [
                "run.py",
                "--watchdog-parent-pid",
                "123",
                "--watchdog-listen-port",
                "9123",
            ],
        ),
    ]
