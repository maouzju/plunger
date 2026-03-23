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


def test_prelaunch_cleanup_shuts_down_then_kills_without_restoring_settings(monkeypatch) -> None:
    events: list[tuple[str, object]] = []

    monkeypatch.setattr(run.os, "getpid", lambda: 900)
    monkeypatch.setattr(run, "_discover_managed_ports", lambda default_port: [9123, default_port])
    monkeypatch.setattr(run, "_collect_state_pids", lambda: {111, 222})
    monkeypatch.setattr(
        run,
        "_request_shutdown_for_port",
        lambda port: events.append(("shutdown", port)) or (333 if port == 9123 else 0),
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
    assert [
        event for event in events if event[0] == "kill"
    ] == [
        ("kill", (111, 900)),
        ("kill", (222, 900)),
        ("kill", (333, 900)),
        ("kill", (444, 900)),
    ]
    assert events[-3:] == [
        ("wait", (9123, 2.5)),
        ("wait", (8462, 2.5)),
        ("clear-gui", 900),
    ]
