#!/usr/bin/env python3
"""Entrypoint for Plunger."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import ProxyHandler, Request, build_opener

from plunger_shared import CLAUDE_DIR, LOCAL_HOST, RECOVERY_DIR

GUI_STATE_FILE = RECOVERY_DIR / "gui_state.json"
PROXY_STATE_FILE = CLAUDE_DIR / ".plunger_state.json"
CODEX_STATE_FILE = Path.home() / ".codex" / ".plunger_state.json"
LOCAL_OPENER = build_opener(ProxyHandler({}))


def _read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _extract_positive_int(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def _discover_managed_ports(default_port: int) -> list[int]:
    ports: list[int] = []

    proxy_state = _read_json_file(PROXY_STATE_FILE)
    proxy_port = _extract_positive_int((proxy_state or {}).get("listen_port"))
    if proxy_port:
        ports.append(proxy_port)

    codex_state = _read_json_file(CODEX_STATE_FILE)
    codex_proxy_url = str((codex_state or {}).get("proxy_base_url", "")).strip()
    with suppress(ValueError):
        parsed = urlparse(codex_proxy_url)
        if parsed.port and parsed.port not in ports:
            ports.append(parsed.port)

    if default_port not in ports:
        ports.append(default_port)
    return ports


def _request_local_json(
    port: int,
    path: str,
    *,
    method: str = "GET",
    timeout: float = 1.2,
) -> dict[str, Any] | None:
    request = Request(f"http://{LOCAL_HOST}:{port}{path}", method=method)
    with LOCAL_OPENER.open(request, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    return data if isinstance(data, dict) else None


def _request_shutdown_for_port(port: int) -> int:
    watchdog_pid = 0
    with suppress(Exception):
        health = _request_local_json(port, "/health", timeout=0.8) or {}
        watchdog_pid = _extract_positive_int(health.get("safety_watchdog_pid"))
    with suppress(Exception):
        _request_local_json(port, "/control/shutdown?mode=takeover", method="POST", timeout=1.2)
    return watchdog_pid


def _local_port_is_open(port: int) -> bool:
    try:
        with socket.create_connection((LOCAL_HOST, port), timeout=0.2):
            return True
    except OSError:
        return False


def _wait_for_port_release(port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _local_port_is_open(port):
            return True
        time.sleep(0.15)
    return not _local_port_is_open(port)


def _normalize_process_text(value: str) -> str:
    return value.replace("/", "\\").lower()


def _managed_process_markers() -> tuple[str, ...]:
    if getattr(sys, "frozen", False):
        return (_normalize_process_text(str(Path(sys.executable).resolve())),)

    base_dir = Path(__file__).resolve().parent
    return tuple(
        _normalize_process_text(str((base_dir / name).resolve()))
        for name in ("run.pyw", "run.py", "plunger_ui.py", "plunger.py")
    )


def _snapshot_windows_processes() -> list[dict[str, Any]]:
    if os.name != "nt":
        return []

    command = [
        "powershell",
        "-NoProfile",
        "-Command",
        (
            "$items = Get-CimInstance Win32_Process | "
            "Select-Object ProcessId, CommandLine; "
            "$items | ConvertTo-Json -Compress"
        ),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    payload = result.stdout.strip()
    if result.returncode != 0 or not payload:
        return []

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return []

    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def _is_managed_process(
    process: dict[str, Any],
    *,
    current_pid: int,
    markers: tuple[str, ...],
) -> bool:
    pid = _extract_positive_int(process.get("ProcessId"))
    if not pid or pid == current_pid:
        return False

    command_line = _normalize_process_text(str(process.get("CommandLine") or ""))
    if not command_line:
        return False
    return any(marker and marker in command_line for marker in markers)


def _read_state_pid(path: Path) -> int:
    state = _read_json_file(path)
    return _extract_positive_int((state or {}).get("pid"))


def _collect_state_pids() -> set[int]:
    pids = {
        pid
        for pid in (
            _read_state_pid(GUI_STATE_FILE),
            _read_state_pid(PROXY_STATE_FILE),
        )
        if pid
    }
    return pids


def _scan_managed_process_pids(current_pid: int) -> set[int]:
    markers = _managed_process_markers()
    pids = {
        _extract_positive_int(process.get("ProcessId"))
        for process in _snapshot_windows_processes()
        if _is_managed_process(process, current_pid=current_pid, markers=markers)
    }
    pids.discard(0)
    return pids


def _terminate_pid_tree(pid: int, *, current_pid: int) -> None:
    if not pid or pid == current_pid:
        return

    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            check=False,
        )
        return

    with suppress(ProcessLookupError, PermissionError):
        os.kill(pid, 15)


def _clear_stale_gui_state(current_pid: int) -> None:
    state_pid = _read_state_pid(GUI_STATE_FILE)
    if not state_pid or state_pid == current_pid:
        return
    with suppress(FileNotFoundError):
        GUI_STATE_FILE.unlink()


def _prelaunch_cleanup(listen_port: int) -> None:
    current_pid = os.getpid()
    ports = _discover_managed_ports(listen_port)
    candidate_pids = _collect_state_pids()

    for port in ports:
        watchdog_pid = _request_shutdown_for_port(port)
        if watchdog_pid:
            candidate_pids.add(watchdog_pid)
        _wait_for_port_release(port, timeout=1.5)

    candidate_pids.update(_scan_managed_process_pids(current_pid))

    for pid in sorted(candidate_pids):
        _terminate_pid_tree(pid, current_pid=current_pid)

    for port in ports:
        _wait_for_port_release(port, timeout=2.5)

    _clear_stale_gui_state(current_pid)


def main() -> None:
    if "--headless" in sys.argv[1:]:
        from plunger import main as proxy_main

        sys.argv = [sys.argv[0], *[arg for arg in sys.argv[1:] if arg != "--headless"]]
        proxy_main()
        return

    from plunger_ui import build_arg_parser, main as ui_main

    args = build_arg_parser().parse_args()
    _prelaunch_cleanup(args.port)
    ui_main(args)


if __name__ == "__main__":
    main()
