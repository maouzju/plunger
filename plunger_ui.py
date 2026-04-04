#!/usr/bin/env python3
"""Desktop control panel for Plunger."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import tkinter as tk
import webbrowser
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from tkinter import ttk
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from plunger_shared import (
    APP_VERSION,
    CLAUDE_DIR,
    DEFAULT_PORT,
    DEFAULT_STALL_TIMEOUT,
    GITHUB_RELEASES_API_URL,
    GITHUB_RELEASES_PAGE_URL,
    PACKAGE_NAME,
    PREFERRED_WINDOWS_RELEASE_ASSETS,
    RECOVERY_DIR,
)

POLL_INTERVAL_MS = 1600
WINDOW_BG = "#faf6f1"
CARD_BG = "#ffffff"
CARD_BORDER = "#e8ddd0"
TEXT_PRIMARY = "#3d3225"
TEXT_SECONDARY = "#9a8e82"
ACCENT = "#ff7b54"
ACCENT_HOVER = "#ff9a76"
DANGER = "#e85d4a"
DANGER_HOVER = "#f07060"
WARNING = "#f0b429"
BADGE_ONLINE_BG = "#4ecdc4"
BADGE_ONLINE_FG = "#ffffff"
BADGE_OFFLINE_BG = "#e8e0d8"
BADGE_OFFLINE_FG = "#b0a090"
SETTINGS_BG = "#f5efe8"
HEADING_BG = "#ff7b54"
HEADING_FG = "#ffffff"
HEADING_ACTIVE = "#ff9a76"
STRIPE_COLOR = "#f0ebe3"

# CJK-friendly font family
_FONT_UI = "Microsoft YaHei UI"
_FONT_MONO = "Consolas"
_FONT_TITLE = "Microsoft YaHei UI"
UI_SETTINGS_FILE = RECOVERY_DIR / "ui_settings.json"
GUI_STATE_FILE = RECOVERY_DIR / "gui_state.json"
UI_LOG_FILE = RECOVERY_DIR / "ui.log"
EVENT_HISTORY_FILE = RECOVERY_DIR / "events.json"
STATS_FILE = RECOVERY_DIR / "stats.json"
SUPERVISOR_STATE_FILE = RECOVERY_DIR / "supervisor_state.json"
PROXY_STATE_FILE = CLAUDE_DIR / ".plunger_state.json"
STATS_KEYS = (
    "total",
    "success",
    "recoveries_triggered",
    "recoveries_succeeded",
    "retry_attempts",
)
PROCESS_COMMAND_LOOKUP_TIMEOUT_SECONDS = 0.6
STARTUP_DIR = Path(os.getenv("APPDATA", str(Path.home() / "AppData" / "Roaming"))) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
STARTUP_LAUNCHER_FILE = STARTUP_DIR / "Plunger Launcher.vbs"

RECOVERY_DIR.mkdir(parents=True, exist_ok=True)


def _build_ui_log_handlers() -> list[logging.Handler]:
    try:
        return [logging.FileHandler(UI_LOG_FILE, encoding="utf-8")]
    except OSError:
        return [logging.StreamHandler()]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=_build_ui_log_handlers(),
    force=True,
)
log = logging.getLogger("resilient-proxy-ui")
LOCAL_OPENER = build_opener(ProxyHandler({}))
_PYPROJECT_VERSION_RE = re.compile(r'(?m)^version\s*=\s*"([^"]+)"\s*$')


def _is_frozen_app() -> bool:
    return bool(getattr(sys, "frozen", False))


def _app_executable_path() -> Path:
    return Path(sys.executable).resolve()


def _read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _extract_positive_int(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def _read_supervisor_pid() -> int:
    payload = _read_json_file(SUPERVISOR_STATE_FILE)
    return _extract_positive_int((payload or {}).get("pid"))


def _read_proxy_pid(expected_base_url: str) -> int:
    payload = _read_json_file(PROXY_STATE_FILE)
    if not isinstance(payload, dict):
        return 0
    state_proxy_url = str(payload.get("proxy_url", "")).strip().rstrip("/")
    if state_proxy_url and state_proxy_url != expected_base_url.rstrip("/"):
        return 0
    pid = _extract_positive_int(payload.get("pid"))
    if not pid:
        return 0
    return pid if _managed_proxy_process_state(pid) in {"managed", "unknown"} else 0


def _terminate_pid_tree(pid: int) -> None:
    if pid <= 0:
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


def _process_exists(pid: int) -> bool:
    if pid <= 0:
        return False

    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000, False, pid)
            if not handle:
                return False
            kernel32.CloseHandle(handle)
            return True
        except Exception:
            pass

    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _normalize_process_text(value: str) -> str:
    return value.replace("/", "\\").lower()


def _managed_process_markers() -> tuple[str, ...]:
    markers: list[str] = []
    if _is_frozen_app():
        with suppress(Exception):
            markers.append(_normalize_process_text(str(Path(sys.executable).resolve())))
        markers.append(_normalize_process_text(sys.executable))
        return tuple(dict.fromkeys(marker for marker in markers if marker))

    base_dir = _app_base_dir()
    for name in ("run.pyw", "run.py", "plunger_ui.py", "plunger.py"):
        with suppress(Exception):
            markers.append(_normalize_process_text(str((base_dir / name).resolve())))
    return tuple(dict.fromkeys(marker for marker in markers if marker))


def _command_line_mentions_name(command_line: str, name: str) -> bool:
    normalized_name = _normalize_process_text(name).strip()
    if not normalized_name:
        return False
    return any(
        token in command_line
        for token in (
            f"\\{normalized_name}",
            f" {normalized_name}",
            f'"{normalized_name}"',
        )
    ) or command_line.endswith(normalized_name)


def _looks_like_managed_proxy_command(command_line: str) -> bool:
    if any(marker and marker in command_line for marker in _managed_process_markers()):
        return True

    if _command_line_mentions_name(command_line, "plunger.exe"):
        return True
    if _command_line_mentions_name(command_line, "plunger.py"):
        return True
    if _command_line_mentions_name(command_line, "plunger_ui.py"):
        return True

    if any(_command_line_mentions_name(command_line, name) for name in ("run.py", "run.pyw")):
        repo_hint = _normalize_process_text(_app_base_dir().name)
        package_hint = _normalize_process_text(PACKAGE_NAME)
        return bool(
            (repo_hint and repo_hint in command_line)
            or (package_hint and package_hint in command_line)
        )

    return False


def _process_command_line(pid: int) -> str:
    if pid <= 0:
        return ""

    if os.name == "nt":
        command = [
            "powershell",
            "-NoProfile",
            "-Command",
            (
                f'$process = Get-CimInstance Win32_Process -Filter "ProcessId = {pid}" '
                "| Select-Object -First 1 -ExpandProperty CommandLine; "
                'if ($null -ne $process) { $process }'
            ),
        ]
    else:
        command = ["ps", "-p", str(pid), "-o", "command="]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        timeout=PROCESS_COMMAND_LOOKUP_TIMEOUT_SECONDS,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _managed_proxy_process_state(pid: int) -> str:
    if not _process_exists(pid):
        return "dead"

    try:
        command_line = _normalize_process_text(_process_command_line(pid))
    except subprocess.TimeoutExpired:
        command_line = ""

    if not command_line:
        state = "unknown"
    elif _looks_like_managed_proxy_command(command_line):
        state = "managed"
    else:
        state = "other"
    return state


def _is_managed_proxy_process(pid: int) -> bool:
    return _managed_proxy_process_state(pid) == "managed"


def _write_gui_state(pid: int | None = None) -> None:
    try:
        RECOVERY_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "pid": pid or os.getpid(),
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        GUI_STATE_FILE.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        log.warning("Failed to write GUI state file %s: %s", GUI_STATE_FILE, exc)


def _clear_gui_state(expected_pid: int | None = None) -> None:
    try:
        exists = GUI_STATE_FILE.exists()
    except OSError as exc:
        log.warning("Failed to inspect GUI state file %s: %s", GUI_STATE_FILE, exc)
        return

    if not exists:
        return

    if expected_pid is not None:
        try:
            payload = json.loads(GUI_STATE_FILE.read_text(encoding="utf-8"))
        except OSError as exc:
            log.warning("Failed to read GUI state file %s: %s", GUI_STATE_FILE, exc)
            return
        except Exception:
            payload = {}
        try:
            state_pid = int(payload.get("pid") or 0)
        except Exception:
            state_pid = 0
        if state_pid not in (0, expected_pid):
            return

    with suppress(FileNotFoundError, OSError):
        GUI_STATE_FILE.unlink()


def _show_root_window(root: tk.Tk) -> None:
    with suppress(Exception):
        root.update_idletasks()
    with suppress(Exception):
        root.deiconify()
    with suppress(Exception):
        root.lift()
    with suppress(Exception):
        root.attributes("-topmost", True)
        root.after(200, lambda: root.attributes("-topmost", False))
    with suppress(Exception):
        root.focus_force()


def _app_base_dir() -> Path:
    if _is_frozen_app():
        return _app_executable_path().parent
    return Path(__file__).resolve().parent


def _detect_app_version() -> str:
    try:
        version = importlib.metadata.version(PACKAGE_NAME).strip()
    except importlib.metadata.PackageNotFoundError:
        version = ""
    if version:
        return version

    pyproject_path = Path(__file__).resolve().with_name("pyproject.toml")
    try:
        match = _PYPROJECT_VERSION_RE.search(pyproject_path.read_text(encoding="utf-8"))
    except OSError:
        match = None
    if match:
        parsed = match.group(1).strip()
        if parsed:
            return parsed
    return APP_VERSION


def _normalize_release_version(value: str) -> str:
    return value.strip().lstrip("vV")


def _version_sort_key(value: str) -> tuple[tuple[int, ...], int]:
    cleaned = _normalize_release_version(value).lower()
    if not cleaned:
        return (0,), 1

    prerelease = 0 if re.search(r"(?:-|(?:a|alpha|b|beta|rc)\d*)", cleaned) else 1
    core = re.split(r"[-+]", cleaned, maxsplit=1)[0]
    numbers = tuple(int(part) for part in re.findall(r"\d+", core))
    if not numbers:
        return (0,), prerelease
    return numbers, prerelease


def _is_newer_version(candidate: str, current: str) -> bool:
    candidate_numbers, candidate_stability = _version_sort_key(candidate)
    current_numbers, current_stability = _version_sort_key(current)
    width = max(len(candidate_numbers), len(current_numbers))
    candidate_numbers = candidate_numbers + (0,) * (width - len(candidate_numbers))
    current_numbers = current_numbers + (0,) * (width - len(current_numbers))
    if candidate_numbers != current_numbers:
        return candidate_numbers > current_numbers
    return candidate_stability > current_stability


def _github_request_json(url: str, *, timeout: float = 8.0) -> dict[str, Any]:
    request = Request(url)
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("User-Agent", f"Plunger/{_detect_app_version()}")
    with build_opener().open(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        payload = response.read().decode(charset)
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("Unexpected release payload")
    return data


def _pick_release_asset(release: dict[str, Any]) -> dict[str, str] | None:
    assets = release.get("assets")
    if not isinstance(assets, list):
        return None

    normalized_assets: list[dict[str, str]] = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        name = str(asset.get("name", "")).strip()
        url = str(asset.get("browser_download_url", "")).strip()
        if not name or not url:
            continue
        normalized_assets.append({"name": name, "url": url})

    if not normalized_assets:
        return None

    if os.name == "nt":
        for preferred_name in PREFERRED_WINDOWS_RELEASE_ASSETS:
            for asset in normalized_assets:
                if asset["name"].lower() == preferred_name.lower():
                    return asset
        for asset in normalized_assets:
            name = asset["name"].lower()
            if "plunger" in name and (name.endswith(".zip") or name.endswith(".exe")):
                return asset

    return normalized_assets[0]


def _fetch_latest_release_info(current_version: str) -> dict[str, Any]:
    release = _github_request_json(GITHUB_RELEASES_API_URL)
    tag_name = str(release.get("tag_name") or release.get("name") or "").strip()
    latest_version = _normalize_release_version(tag_name)
    if not latest_version:
        raise ValueError("Latest release tag is missing")

    asset = _pick_release_asset(release) or {}
    return {
        "current_version": current_version,
        "latest_version": latest_version,
        "is_newer": _is_newer_version(latest_version, current_version),
        "asset_name": str(asset.get("name", "")).strip(),
        "asset_url": str(asset.get("url", "")).strip(),
        "html_url": str(release.get("html_url") or GITHUB_RELEASES_PAGE_URL).strip() or GITHUB_RELEASES_PAGE_URL,
    }


def _default_update_download_dir() -> Path:
    downloads = Path.home() / "Downloads" / "Plunger"
    if downloads.parent.exists():
        return downloads
    return Path.home() / "Plunger"


def _download_release_asset(download_url: str, asset_name: str, *, timeout: float = 30.0) -> Path:
    target_dir = _default_update_download_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    safe_name = Path(asset_name).name or "Plunger-update.zip"
    target_path = target_dir / safe_name

    request = Request(download_url)
    request.add_header("Accept", "application/octet-stream")
    request.add_header("User-Agent", f"Plunger/{_detect_app_version()}")
    with build_opener().open(request, timeout=timeout) as response:
        with target_path.open("wb") as handle:
            shutil.copyfileobj(response, handle)
    return target_path


def _open_path_in_shell(path: Path) -> None:
    resolved = path.resolve()
    if os.name == "nt":
        if resolved.exists() and resolved.is_file():
            subprocess.Popen(["explorer", "/select,", str(resolved)])
            return
        os.startfile(str(resolved))  # type: ignore[attr-defined]
        return

    target = resolved if resolved.exists() else resolved.parent
    webbrowser.open(target.as_uri())


def _format_request_error(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        return f"HTTP {exc.code}"
    if isinstance(exc, URLError):
        reason = str(getattr(exc, "reason", "")).strip()
        return reason or exc.__class__.__name__
    message = str(exc).strip()
    return message or exc.__class__.__name__


LANGUAGES = {
    "zh_CN": "\u7b80\u4f53\u4e2d\u6587",
    "en_US": "English",
}

TRANSLATIONS: dict[str, dict[str, str]] = {
    "zh_CN": {
        "window_title": "马桶塞",
        "hero_title": "马桶塞",
        "hero_subtitle": "Plunger \u81ea\u52a8\u758f\u901a agent \u4fbf\u79d8\u60c5\u51b5\n\uff08\u8bf7\u52ff\u548c CC Switch \u7684\u6545\u969c\u8f6c\u79fb\u540c\u65f6\u4f7f\u7528\uff09",
        "settings_title": "\u754c\u9762\u8bbe\u7f6e",
        "language_label": "\u8bed\u8a00",
        "launch_on_boot_label": "\u5f00\u673a\u542f\u52a8",
        "auto_start_proxy_label": "\u542f\u52a8\u540e\u81ea\u52a8\u5f00\u542f\u4ee3\u7406",
        "update_title": "\u7248\u672c\u66f4\u65b0",
        "current_version_label": "\u5f53\u524d\u7248\u672c\uff1a{version}",
        "update_status_idle": "\u70b9\u51fb\u6309\u94ae\u540e\u68c0\u67e5 GitHub \u4e0a\u662f\u5426\u6709\u65b0\u7248\u672c\u3002",
        "update_status_checking": "\u6b63\u5728\u68c0\u67e5 GitHub Release...",
        "update_status_latest": "\u5f53\u524d\u5df2\u662f\u6700\u65b0\u7248\u672c {version}\u3002",
        "update_status_available_download": "\u53d1\u73b0\u65b0\u7248\u672c {version}\uff0c\u53ef\u4ee5\u76f4\u63a5\u4e0b\u8f7d\u66f4\u65b0\u5305\u3002",
        "update_status_available_release": "\u53d1\u73b0\u65b0\u7248\u672c {version}\uff0c\u53ef\u6253\u5f00 GitHub \u53d1\u5e03\u9875\u66f4\u65b0\u3002",
        "update_status_downloading": "\u6b63\u5728\u4e0b\u8f7d {name}...",
        "update_status_downloaded": "\u5df2\u4e0b\u8f7d\u5230 {path}\u3002",
        "update_status_failed": "\u68c0\u67e5\u66f4\u65b0\u5931\u8d25\uff1a{reason}",
        "update_status_download_failed": "\u4e0b\u8f7d\u66f4\u65b0\u5931\u8d25\uff1a{reason}",
        "update_button_check": "\u68c0\u67e5\u66f4\u65b0",
        "update_button_checking": "\u68c0\u67e5\u4e2d...",
        "update_button_download": "\u4e0b\u8f7d\u66f4\u65b0",
        "update_button_downloading": "\u4e0b\u8f7d\u4e2d...",
        "update_button_open_download": "\u6253\u5f00\u4e0b\u8f7d\u76ee\u5f55",
        "update_button_open_release": "\u6253\u5f00\u53d1\u5e03\u9875",
        "action_on": "\u5f00\u542f\u4ee3\u7406",
        "action_off": "\u5173\u95ed\u4ee3\u7406",
        "status_online": "\u8fd0\u884c\u4e2d",
        "status_offline": "\u5df2\u505c\u6b62",
        "subtitle_online_with_since": "\u4ee3\u7406\u5df2\u542f\u52a8\u3002\u5f00\u59cb\u65f6\u95f4\uff1a{started_at}\u3002",
        "subtitle_online_no_since": "\u4ee3\u7406\u5df2\u542f\u52a8\u3002",
        "subtitle_starting": "\u6b63\u5728\u7b49\u5f85\u4ee3\u7406\u542f\u52a8",
        "subtitle_offline": "\u4ee3\u7406\u672a\u542f\u52a8",
        "listen_prefix": "\u76d1\u542c\u5730\u5740\uff1a{value}",
        "upstream_prefix": "\u4e0a\u6e38\u5730\u5740\uff1a{value}",
        "upstream_waiting": "\u7b49\u5f85\u542f\u52a8",
        "upstream_stopped": "\u672a\u542f\u52a8",
        "source_unknown": "\u672a\u77e5\u6765\u6e90",
        "source_manual_override": "\u624b\u52a8\u6307\u5b9a",
        "source_claude_settings": "Claude \u8bbe\u7f6e",
        "source_proxy_state": "\u4ee3\u7406\u72b6\u6001\u6062\u590d",
        "source_codex_state": "Codex \u72b6\u6001\u6062\u590d",
        "source_codex_config": "Codex \u914d\u7f6e",
        "source_cc_switch": "CC Switch",
        "stats_requests": "\u7d2f\u8ba1\u8bf7\u6c42",
        "stats_success": "\u672c\u6b21\u758f\u901a\u6210\u529f",
        "stats_triggers": "\u672c\u6b21\u65ad\u8fde\u89e6\u53d1",
        "stats_today_success": "\u4eca\u65e5\u758f\u901a\u6b21\u6570",
        "stats_history_success": "\u5386\u53f2\u758f\u901a\u6b21\u6570",
        "active_title": "\u5f53\u524d\u6d3b\u8dc3\u4f1a\u8bdd",
        "active_none": "\u5f53\u524d\u6ca1\u6709\u6b63\u5728\u6d41\u5f0f\u4f20\u8f93\u6216\u7b49\u5f85\u5de5\u5177\u7ed3\u679c\u7684\u4f1a\u8bdd\u3002",
        "active_running": "{endpoint} \u6b63\u5728\u5904\u7406\u4e2d\uff0c\u6700\u8fd1\u66f4\u65b0\uff1a{updated_at}\u3002",
        "active_recovering": "{endpoint} \u6b63\u5728\u65ad\u8fde\u6062\u590d\u4e2d\uff0c\u7b2c {attempt} \u6b21\u91cd\u8bd5\uff0c\u6700\u8fd1\u66f4\u65b0\uff1a{updated_at}\u3002",
        "active_waiting_tool": "{endpoint} \u5df2\u53d1\u51fa\u5de5\u5177\u8c03\u7528\uff0c\u6b63\u5728\u7b49\u5f85\u672c\u5730\u5de5\u5177\u7ed3\u679c\u3002\u6700\u8fd1\u66f4\u65b0\uff1a{updated_at}\u3002",
        "active_tool_timeout": "{endpoint} \u53d1\u51fa\u5de5\u5177\u8c03\u7528\u540e\u5df2\u7b49\u5f85 {seconds} \u79d2\uff0c\u4e0b\u4e00\u8f6e\u8bf7\u6c42\u8fd8\u6ca1\u56de\u6765\u3002\u6700\u8fd1\u66f4\u65b0\uff1a{updated_at}\u3002",
        "active_count_label": "\u5171 {count} \u6761\u6d3b\u8dc3\u4f1a\u8bdd",
        "active_summary_label": "\u6807\u9898\uff1a{summary}",
        "active_model_label": "\u6a21\u578b\uff1a{model}",
        "active_client_label": "\u8fde\u63a5\uff1a{client}",
        "active_hint_label": "\u63d0\u793a\uff1a{hint}",
        "active_hint_station_issue": "\u5df2\u91cd\u8bd5\u591a\u6b21\u4ecd\u672a\u8fde\u4e0a\uff0c\u8f83\u53ef\u80fd\u662f\u4e2d\u8f6c\u7ad9\u6216\u4e0a\u6e38\u901a\u9053\u6682\u65f6\u4e0d\u53ef\u7528\uff0c\u4e0d\u662f\u4f60\u5f53\u524d\u64cd\u4f5c\u5361\u4f4f\u4e86\u3002",
        "active_hint_tool_timeout": "\u672c\u5730\u5de5\u5177\u7ed3\u679c\u7b49\u5f85\u8d85\u65f6\uff0c\u8f83\u53ef\u80fd\u662f\u5de5\u5177\u8c03\u7528\u6216 agent \u63a5\u529b\u94fe\u8def\u6ca1\u6709\u7eed\u4e0a\uff0c\u4e0d\u662f\u4e2d\u8f6c\u7ad9 503 \u65ad\u8fde\u3002",
        "active_reason_station_no_channel": "\u4e2d\u8f6c\u7ad9\u5f53\u524d\u6ca1\u6709\u53ef\u7528\u901a\u9053\uff0c\u6b63\u5728\u81ea\u52a8\u91cd\u8bd5\u3002",
        "active_reason_station_503": "\u4e2d\u8f6c\u7ad9\u6682\u65f6\u4e0d\u53ef\u7528\uff0c\u6b63\u5728\u81ea\u52a8\u91cd\u8bd5\u3002",
        "active_reason_station_502": "\u4e2d\u8f6c\u7ad9\u4e0e\u4e0a\u6e38\u8fde\u63a5\u5f02\u5e38\uff0c\u6b63\u5728\u81ea\u52a8\u91cd\u8bd5\u3002",
        "active_reason_station_timeout": "\u4e2d\u8f6c\u7ad9\u8fde\u63a5\u8d85\u65f6\uff0c\u6b63\u5728\u81ea\u52a8\u91cd\u8bd5\u3002",
        "active_reason_waiting_tool": "\u5df2\u53d1\u51fa\u672c\u5730\u5de5\u5177\u8c03\u7528\uff0c\u6b63\u5728\u7b49\u5f85 tool_result \u56de\u6765\u3002",
        "active_reason_tool_timeout": "\u672c\u5730\u5de5\u5177\u7ed3\u679c\u7b49\u5f85\u8d85\u65f6\uff0c\u4ee3\u7406\u8fd8\u6ca1\u6709\u7b49\u5230\u65b0\u7684 tool_result \u8bf7\u6c42\u3002",
        "active_reason_label": "\u6700\u8fd1\u5f02\u5e38\uff1a{reason}",
        "active_last_user_label": "\u6700\u8fd1\u8f93\u5165\uff1a{preview}",
        "active_partial_label": "\u6700\u8fd1\u8f93\u51fa\uff1a{preview}",
        "active_partial_chars_label": "\u5df2\u89c1\u6587\u672c\uff1a{chars} \u5b57\u7b26",
        "active_tools_label": "\u5de5\u5177\uff1a{tools}",
        "active_wait_age_label": "\u5df2\u7b49\u5f85\uff1a{seconds} \u79d2",
        "active_more_label": "\u8fd8\u6709 {count} \u6761\u6d3b\u8dc3\u4f1a\u8bdd\u672a\u5c55\u793a\u3002",
        "active_col_status": "\u72b6\u6001",
        "active_col_endpoint": "\u6807\u9898",
        "active_col_model": "\u6a21\u578b",
        "active_col_client": "\u8fde\u63a5",
        "active_col_input": "\u6982\u89c8",
        "active_status_running": "\u5904\u7406\u4e2d",
        "active_status_recovering": "\u91cd\u8bd5\u4e2d",
        "active_status_waiting_tool": "\u7b49\u5f85\u5de5\u5177",
        "active_status_tool_timeout": "\u5de5\u5177\u8d85\u65f6",
        "events_title": "\u6700\u8fd1\u758f\u901a",
        "events_subtitle": "\u8fd9\u91cc\u4f1a\u5c55\u793a\u8fd1\u671f\u7684\u65ad\u8fde\u3001\u6062\u590d\u3001\u5de5\u5177\u63a5\u529b\u7a7a\u7a97\u548c\u5173\u505c\u52a8\u4f5c\u3002",
        "events_scope_current_run": "\u672c\u8f6e\u4ee3\u7406",
        "events_scope_reconnect_only": "\u4ec5\u67e5\u770b\u91cd\u8fde",
        "col_time": "\u65f6\u95f4",
        "col_state": "\u72b6\u6001",
        "col_summary": "\u6458\u8981",
        "footer_idle": "\u9762\u677f\u5f53\u524d\u7a7a\u95f2\u3002",
        "footer_refresh": "\u6700\u8fd1\u7684\u65ad\u8fde\u548c\u6062\u590d\u4f1a\u81ea\u52a8\u5237\u65b0\u3002",
        "footer_starting": "\u6b63\u5728\u542f\u52a8\u4ee3\u7406...",
        "footer_stopping": "\u6b63\u5728\u505c\u6b62\u4ee3\u7406...",
        "footer_online": "\u4ee3\u7406\u5df2\u4e0a\u7ebf\u3002",
        "footer_stopped": "\u4ee3\u7406\u5df2\u505c\u6b62\u3002",
        "footer_start_failed": "\u4ee3\u7406\u542f\u52a8\u5931\u8d25\u3002\u8bf7\u68c0\u67e5\u672c\u673a\u7684 Claude \u8bbe\u7f6e\u540e\u518d\u91cd\u8bd5\u3002",
        "footer_start_timeout": "\u542f\u52a8\u8d85\u65f6\u3002\u4ee3\u7406\u6ca1\u6709\u5728\u9884\u671f\u65f6\u95f4\u5185\u901a\u8fc7\u5065\u5eb7\u68c0\u67e5\u3002",
        "footer_stop_timeout": "\u505c\u6b62\u8d85\u65f6\u3002\u65e7\u7684\u4ee3\u7406\u8fdb\u7a0b\u4ecd\u7136\u5b58\u6d3b\uff0c\u8bf7\u7a0d\u540e\u518d\u8bd5\u6216\u624b\u52a8\u68c0\u67e5\u3002",
        "footer_launch_on_boot_enabled": "\u5df2\u5f00\u542f\u5f00\u673a\u542f\u52a8\u3002",
        "footer_launch_on_boot_disabled": "\u5df2\u5173\u95ed\u5f00\u673a\u542f\u52a8\u3002",
        "footer_auto_start_proxy_enabled": "\u5df2\u5f00\u542f\u201c\u542f\u52a8\u540e\u81ea\u52a8\u5f00\u542f\u4ee3\u7406\u201d\u3002",
        "footer_auto_start_proxy_disabled": "\u5df2\u5173\u95ed\u201c\u542f\u52a8\u540e\u81ea\u52a8\u5f00\u542f\u4ee3\u7406\u201d\u3002",
        "footer_startup_setting_failed": "\u542f\u52a8\u9879\u8bbe\u7f6e\u5931\u8d25\uff0c\u8bf7\u68c0\u67e5\u6587\u4ef6\u6743\u9650\u3002",
        "footer_update_checking": "\u6b63\u5728\u68c0\u67e5\u66f4\u65b0...",
        "footer_update_latest": "\u5df2\u7ecf\u662f\u6700\u65b0\u7248\u672c\u3002",
        "footer_update_available": "\u53d1\u73b0\u65b0\u7248\u672c {version}\u3002",
        "footer_update_downloading": "\u6b63\u5728\u4e0b\u8f7d\u66f4\u65b0...",
        "footer_update_downloaded": "\u66f4\u65b0\u5305\u5df2\u4e0b\u8f7d\u5230 {path}\u3002",
        "footer_update_release_opened": "\u5df2\u6253\u5f00 GitHub \u53d1\u5e03\u9875\u3002",
        "footer_update_failed": "\u68c0\u67e5\u66f4\u65b0\u5931\u8d25\uff1a{reason}",
        "footer_update_download_failed": "\u4e0b\u8f7d\u66f4\u65b0\u5931\u8d25\uff1a{reason}",
        "trigger_hint": "\"\u7d2f\u8ba1\u8bf7\u6c42\"\u4f1a\u8de8\u8fc7\u4ee3\u7406\u91cd\u542f\u7d2f\u79ef\u3002\"\u672c\u6b21\u65ad\u8fde\u89e6\u53d1\"\u548c\"\u672c\u6b21\u758f\u901a\u6210\u529f\"\u4f1a\u6309\u672c\u6b21\u8fd0\u884c\u7684\u4e8b\u4ef6\u91cd\u65b0\u7ed3\u7b97\uff0c\u540c\u4e00\u6761\u4efb\u52a1\u5982\u679c\u540e\u9762\u53c8 failed \u4e86\uff0c\u4e0d\u4f1a\u7ee7\u7eed\u7b97\u6210\u529f\u3002",
        "event_none_state": "\u6682\u65e0\u6d3b\u52a8",
        "event_none_summary": "\u6700\u8fd1\u8fd8\u6ca1\u6709\u65ad\u8fde\u6216\u6062\u590d\u4e8b\u4ef6\u3002",
        "event_none_summary_current_run": "\u672c\u8f6e\u4ee3\u7406\u542f\u52a8\u540e\uff0c\u6682\u65f6\u8fd8\u6ca1\u6709\u65ad\u8fde\u6216\u6062\u590d\u4e8b\u4ef6\u3002",
        "event_state_disconnect": "\u65ad\u8fde",
        "event_state_recovered": "\u5df2\u6062\u590d",
        "event_state_failed": "\u5f02\u5e38",
        "event_state_service_start": "\u5df2\u542f\u52a8",
        "event_state_service_stop": "\u505c\u6b62\u4e2d",
        "event_state_tool_wait": "\u7b49\u5de5\u5177",
        "event_state_tool_resumed": "\u5df2\u7eed\u4e0a",
        "event_state_tool_wait_timeout": "\u5de5\u5177\u5361\u4f4f",
        "event_state_default": "\u4e8b\u4ef6",
        "event_service_start": "\u5f00\u59cb\u76d1\u542c {listen}\uff0c\u4e0a\u6e38\u4e3a {upstream}\uff0c\u6765\u6e90\uff1a{source}\u3002",
        "event_service_stop": "\u4ece\u63a7\u5236\u9762\u677f\u53d1\u8d77\u4e86\u624b\u52a8\u505c\u6b62\u3002",
        "event_disconnect": "{endpoint} \u5728\u7b2c {attempt} \u6b21\u6062\u590d\u5c1d\u8bd5\u4e2d\u65ad\u8fde\uff1a{reason}",
        "event_recovered_with_retry": "{endpoint} \u7ecf\u8fc7 {attempt} \u6b21\u81ea\u52a8\u91cd\u8bd5\u540e\u6062\u590d\uff0c\u65b9\u5f0f\uff1a{mode}\u3002",
        "event_recovered_no_retry": "{endpoint} \u5df2\u6062\u590d\uff0c\u65b9\u5f0f\uff1a{mode}\u3002",
        "event_failed": "{endpoint} \u51fa\u73b0\u5f02\u5e38\uff1a{reason}",
        "event_tool_wait": "{endpoint} \u5df2\u7ed3\u675f\u672c\u8f6e\u8f93\u51fa\uff0c\u6b63\u5728\u7b49\u5f85\u672c\u5730\u5de5\u5177\u7ed3\u679c\uff1a{tools}",
        "event_tool_resumed": "{endpoint} \u5df2\u6536\u5230\u5de5\u5177\u7ed3\u679c\uff0c\u7b49\u5f85 {seconds} \u79d2\u540e\u7eed\u4e0a\u3002",
        "event_tool_wait_timeout": "{endpoint} \u53d1\u51fa\u5de5\u5177\u8c03\u7528\u540e\u5df2\u7b49\u5f85 {seconds} \u79d2\uff0c\u65b0\u7684 tool_result \u8fd8\u6ca1\u53d1\u51fa\u6765\u3002",
        "event_request_summary": "\u6807\u9898\uff1a{summary}",
        "event_buffered": "\u5df2\u7f13\u51b2 {chars} \u4e2a\u5b57\u7b26",
        "event_delivered": "\u5df2\u8f6c\u53d1 {chars} \u4e2a\u5b57\u7b26",
        "event_tools_unknown": "\u5de5\u5177\u8c03\u7528",
        "event_mode_stream_retry": "\u6d41\u5f0f\u91cd\u8bd5",
        "event_mode_tail_fetch": "\u5c3e\u90e8\u8865\u62c9",
        "event_mode_unknown": "\u672a\u77e5\u65b9\u5f0f",
        "endpoint_messages": "messages",
        "endpoint_responses": "responses",
        "tab_home": "\u4e3b\u754c\u9762",
        "tab_active_sessions": "\u5f53\u524d\u6d3b\u8dc3\u4f1a\u8bdd",
        "tab_events": "\u758f\u901a\u65e5\u5fd7",
        "tab_settings": "\u8bbe\u7f6e",
        "home_activity_title": "\u5b9e\u65f6\u6982\u89c8",
        "settings_subtitle": "\u5728\u8fd9\u91cc\u8c03\u6574\u754c\u9762\u8bed\u8a00\u3001\u542f\u52a8\u884c\u4e3a\u548c\u7248\u672c\u66f4\u65b0\u3002",
    },
    "en_US": {
        "window_title": "Plunger",
        "hero_title": "Plunger",
        "hero_subtitle": "Clogged connection? One plunge and you're good to go.",
        "settings_title": "Interface Settings",
        "language_label": "Language",
        "launch_on_boot_label": "Launch On Startup",
        "auto_start_proxy_label": "Auto Start Proxy After Launch",
        "update_title": "Version Update",
        "current_version_label": "Current version: {version}",
        "update_status_idle": "Use the button below to check whether GitHub has a newer release.",
        "update_status_checking": "Checking GitHub releases...",
        "update_status_latest": "You're already on the latest version: {version}.",
        "update_status_available_download": "Version {version} is available and can be downloaded now.",
        "update_status_available_release": "Version {version} is available. Open the GitHub release page to update.",
        "update_status_downloading": "Downloading {name}...",
        "update_status_downloaded": "Downloaded to {path}.",
        "update_status_failed": "Update check failed: {reason}",
        "update_status_download_failed": "Update download failed: {reason}",
        "update_button_check": "Check For Updates",
        "update_button_checking": "Checking...",
        "update_button_download": "Download Update",
        "update_button_downloading": "Downloading...",
        "update_button_open_download": "Open Download Folder",
        "update_button_open_release": "Open Release Page",
        "action_on": "Turn On",
        "action_off": "Turn Off",
        "status_online": "Online",
        "status_offline": "Offline",
        "subtitle_online_with_since": "The proxy is active. Running since {started_at}.",
        "subtitle_online_no_since": "The proxy is active.",
        "subtitle_starting": "Waiting for proxy to start.",
        "subtitle_offline": "Proxy is not running.",
        "listen_prefix": "Listen: {value}",
        "upstream_prefix": "Upstream: {value}",
        "upstream_waiting": "waiting to start",
        "upstream_stopped": "not started",
        "source_unknown": "unknown source",
        "source_manual_override": "manual override",
        "source_claude_settings": "Claude settings",
        "source_proxy_state": "proxy state",
        "source_codex_state": "Codex state",
        "source_codex_config": "Codex config",
        "source_cc_switch": "CC Switch",
        "stats_requests": "Total Requests",
        "stats_success": "Recovered This Run",
        "stats_triggers": "Triggers This Run",
        "stats_today_success": "Plunges Today",
        "stats_history_success": "Recorded Plunges",
        "active_title": "Active Session",
        "active_none": "There is no streaming session or pending tool-result handoff right now.",
        "active_running": "{endpoint} is still in progress. Last update: {updated_at}.",
        "active_recovering": "{endpoint} is retrying after a disconnect (attempt {attempt}). Last update: {updated_at}.",
        "active_waiting_tool": "{endpoint} finished with tool calls and is waiting for local tool results. Last update: {updated_at}.",
        "active_tool_timeout": "{endpoint} has been waiting {seconds}s for the next request after tool calls. Last update: {updated_at}.",
        "active_count_label": "{count} active sessions",
        "active_summary_label": "Title: {summary}",
        "active_model_label": "Model: {model}",
        "active_client_label": "Connection: {client}",
        "active_hint_label": "Hint: {hint}",
        "active_hint_station_issue": "It has retried several times and still cannot connect. This is more likely a relay or upstream-channel issue, not your current action being stuck.",
        "active_hint_tool_timeout": "Waiting for local tool results has timed out. This is more likely a tool-call or agent handoff issue, not a relay-side 503 disconnect.",
        "active_reason_station_no_channel": "The relay currently has no available channel and is retrying automatically.",
        "active_reason_station_503": "The relay is temporarily unavailable and is retrying automatically.",
        "active_reason_station_502": "The relay lost its upstream connection and is retrying automatically.",
        "active_reason_station_timeout": "The relay timed out while connecting and is retrying automatically.",
        "active_reason_waiting_tool": "A local tool call was sent and the proxy is waiting for the next tool_result request.",
        "active_reason_tool_timeout": "Waiting for local tool results has timed out and no new tool_result request has arrived yet.",
        "active_reason_label": "Latest error: {reason}",
        "active_last_user_label": "Latest input: {preview}",
        "active_partial_label": "Latest output: {preview}",
        "active_partial_chars_label": "Visible text: {chars} chars",
        "active_tools_label": "Tools: {tools}",
        "active_wait_age_label": "Waiting: {seconds}s",
        "active_more_label": "{count} more active sessions are not shown.",
        "active_col_status": "Status",
        "active_col_endpoint": "Title",
        "active_col_model": "Model",
        "active_col_client": "Connection",
        "active_col_input": "Overview",
        "active_status_running": "Running",
        "active_status_recovering": "Retrying",
        "active_status_waiting_tool": "Waiting Tool",
        "active_status_tool_timeout": "Tool Timeout",
        "events_title": "Recent Resilience Events",
        "events_subtitle": "Disconnects, recoveries, tool-handoff gaps, and shutdown signals appear here.",
        "events_scope_current_run": "Current Run",
        "events_scope_reconnect_only": "Reconnects Only",
        "col_time": "Time",
        "col_state": "State",
        "col_summary": "Summary",
        "footer_idle": "The panel is idle.",
        "footer_refresh": "Recent disconnects and recoveries refresh automatically.",
        "footer_starting": "Starting proxy...",
        "footer_stopping": "Stopping proxy...",
        "footer_online": "Proxy is online.",
        "footer_stopped": "Proxy has stopped.",
        "footer_start_failed": "Proxy failed to start. Check your local Claude settings and retry.",
        "footer_start_timeout": "Startup timed out. The proxy did not answer the health check.",
        "footer_stop_timeout": "Shutdown timed out. The previous proxy process still appears to be alive.",
        "footer_launch_on_boot_enabled": "Launch on startup is enabled.",
        "footer_launch_on_boot_disabled": "Launch on startup is disabled.",
        "footer_auto_start_proxy_enabled": "\"Auto start proxy after launch\" is enabled.",
        "footer_auto_start_proxy_disabled": "\"Auto start proxy after launch\" is disabled.",
        "footer_startup_setting_failed": "Failed to update the startup setting. Check file permissions and retry.",
        "footer_update_checking": "Checking for updates...",
        "footer_update_latest": "You're already on the latest version.",
        "footer_update_available": "Version {version} is available.",
        "footer_update_downloading": "Downloading the update...",
        "footer_update_downloaded": "The update package was downloaded to {path}.",
        "footer_update_release_opened": "The GitHub release page has been opened.",
        "footer_update_failed": "Update check failed: {reason}",
        "footer_update_download_failed": "Update download failed: {reason}",
        "trigger_hint": "\"Total Requests\" is cumulative across proxy restarts. \"Triggers This Run\" and \"Recovered This Run\" are recalculated from the current run's events so a request that later fails will not keep counting as recovered.",
        "event_none_state": "No activity",
        "event_none_summary": "No recent disconnect or recovery events yet.",
        "event_none_summary_current_run": "No disconnect or recovery events have occurred in the current run yet.",
        "event_state_disconnect": "Disconnect",
        "event_state_recovered": "Recovered",
        "event_state_failed": "Anomaly",
        "event_state_service_start": "Online",
        "event_state_service_stop": "Stopping",
        "event_state_tool_wait": "Tool Wait",
        "event_state_tool_resumed": "Resumed",
        "event_state_tool_wait_timeout": "Tool Gap",
        "event_state_default": "Event",
        "event_service_start": "Listening on {listen}, forwarding to {upstream}, source: {source}.",
        "event_service_stop": "A manual stop was requested from the control panel.",
        "event_disconnect": "{endpoint} disconnected during recovery attempt {attempt}: {reason}",
        "event_recovered_with_retry": "{endpoint} recovered after {attempt} automatic retries via {mode}.",
        "event_recovered_no_retry": "{endpoint} recovered via {mode}.",
        "event_failed": "{endpoint} hit an anomaly: {reason}",
        "event_tool_wait": "{endpoint} finished this turn and is now waiting for local tool results: {tools}",
        "event_tool_resumed": "{endpoint} received tool results and resumed after {seconds}s.",
        "event_tool_wait_timeout": "{endpoint} has been waiting {seconds}s after tool calls and no new tool_result request has arrived.",
        "event_request_summary": "Title: {summary}",
        "event_buffered": "{chars} chars buffered",
        "event_delivered": "{chars} chars delivered",
        "event_tools_unknown": "tool call",
        "event_mode_stream_retry": "stream retry",
        "event_mode_tail_fetch": "tail fetch",
        "event_mode_unknown": "unknown mode",
        "endpoint_messages": "messages",
        "endpoint_responses": "responses",
        "tab_home": "Home",
        "tab_active_sessions": "Active Sessions",
        "tab_events": "Events",
        "tab_settings": "Settings",
        "home_activity_title": "Live Snapshot",
        "settings_subtitle": "Adjust the interface language, launch behavior, and update flow here.",
    },
}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Desktop UI for Plunger")
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help="Proxy port")
    parser.add_argument("-t", "--timeout", type=float, default=DEFAULT_STALL_TIMEOUT, help="Stream stall timeout")
    parser.add_argument("-r", "--retries", type=int, default=-1, help="Retry count (-1 = unlimited)")
    parser.add_argument(
        "--watch-interval",
        type=float,
        default=1.0,
        help="settings.json poll interval in seconds",
    )
    parser.add_argument("-u", "--upstream", help="Optional manual upstream override")
    return parser


class FancyButton(tk.Canvas):
    """Rounded, animated button with hover glow and idle breathing pulse."""

    _PULSE_STEP_MS = 50
    _PULSE_STEPS = 40  # half-cycle frames (slower, Apple-like breathing)

    def __init__(
        self,
        parent: tk.Misc,
        *,
        textvariable: tk.StringVar,
        command: Any,
        btn_width: int = 200,
        btn_height: int = 64,
        radius: int = 32,
        font: tuple[str, ...] = (_FONT_UI, 16, "bold"),
        bg_color: str = ACCENT,
        hover_color: str = ACCENT_HOVER,
        press_color: str = "#e06840",
        fg_color: str = "#ffffff",
        disabled_bg: str = "#ddc8b8",
        disabled_fg: str = "#ffffff",
    ) -> None:
        # Determine parent background for seamless blending
        try:
            parent_bg = str(parent.cget("bg"))
        except Exception:
            parent_bg = CARD_BG
        super().__init__(
            parent, width=btn_width + 40, height=btn_height + 40,
            bg=parent_bg, highlightthickness=0, bd=0,
        )
        self._btn_w = btn_width + 40
        self._btn_h = btn_height + 40
        self._ox = 20  # offset for centering button in larger canvas
        self._oy = 20
        self._real_w = btn_width
        self._real_h = btn_height
        self._r = radius
        self._font = font
        self._bg = bg_color
        self._hover = hover_color
        self._press = press_color
        self._fg = fg_color
        self._disabled_bg = disabled_bg
        self._disabled_fg = disabled_fg
        self._command = command
        self._textvar = textvariable
        self._disabled = False
        self._hovered = False
        self._pressed = False

        # Pulse / glow state
        self._pulse_after: str | None = None
        self._pulse_phase = 0  # 0 .. _PULSE_STEPS*2
        self._pulse_running = False
        # Shadow for glow (drawn behind the body)
        self._shadow_id: int | None = None
        # Soft drop shadow
        self._shadow_ids: list[int] = []
        # Rotating arc light (shown when proxy is running)
        self._arc_id: int | None = None
        self._arc_angle = 0.0
        self._arc_running = False
        self._arc_after: str | None = None

        # Use oval for circular buttons (when width == height and radius >= half size)
        self._is_circle = (btn_width == btn_height and radius >= btn_width // 2 - 10)

        ox, oy = self._ox, self._oy
        x1, y1 = ox + 3, oy + 3
        x2, y2 = ox + btn_width - 3, oy + btn_height - 3

        # Apple-style: draw layered soft shadows first
        self._draw_shadows()

        if self._is_circle:
            self._body_id = self.create_oval(x1, y1, x2, y2, fill=bg_color, outline="")
        else:
            self._body_id = self._rounded_rect(x1, y1, x2, y2, radius, fill=bg_color, outline="")

        cx = ox + btn_width // 2
        cy = oy + btn_height // 2
        self._text_id = self.create_text(
            cx, cy + 2,
            text=textvariable.get(),
            fill=fg_color,
            font=font,
            width=btn_width - 40,
        )

        textvariable.trace_add("write", self._on_text_changed)
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        self.bind("<ButtonPress-1>", self._on_press)
        self.bind("<ButtonRelease-1>", self._on_release)

    # -- drawing helpers --------------------------------------------------

    def _draw_shadows(self) -> None:
        """Draw a single soft drop shadow beneath the button."""
        for sid in self._shadow_ids:
            self.delete(sid)
        self._shadow_ids.clear()
        ox, oy = self._ox, self._oy
        # Single soft shadow: slightly offset down, blended toward parent bg
        spread = 10
        offset_y = 6
        color = self._lerp_hex(CARD_BG, "#b0a090", 0.35)
        if self._is_circle:
            sid = self.create_oval(
                ox + 3 - spread, oy + 3 - spread + offset_y,
                ox + self._real_w - 3 + spread, oy + self._real_h - 3 + spread + offset_y,
                fill=color, outline="",
            )
        else:
            sid = self._rounded_rect(
                ox + 3 - spread, oy + 3 - spread + offset_y,
                ox + self._real_w - 3 + spread, oy + self._real_h - 3 + spread + offset_y,
                self._r + spread,
                fill=color, outline="",
            )
        self.tag_lower(sid)
        self._shadow_ids.append(sid)

    def _draw_highlight(self) -> None:
        """No-op: keep the button clean and flat (Apple-style simplicity)."""
        self._highlight_id = None

    def _rounded_rect(
        self, x1: int, y1: int, x2: int, y2: int, r: int, **kw: Any,
    ) -> int:
        points = [
            x1 + r, y1,
            x2 - r, y1,
            x2, y1,
            x2, y1 + r,
            x2, y2 - r,
            x2, y2,
            x2 - r, y2,
            x1 + r, y2,
            x1, y2,
            x1, y2 - r,
            x1, y1 + r,
            x1, y1,
        ]
        return self.create_polygon(points, smooth=True, **kw)

    def _current_bg(self) -> str:
        if self._disabled:
            return self._disabled_bg
        if self._pressed:
            return self._press
        if self._hovered:
            return self._hover
        return self._bg

    def _redraw(self) -> None:
        self.itemconfig(self._body_id, fill=self._current_bg())
        self.itemconfig(self._text_id, fill=self._disabled_fg if self._disabled else self._fg)

    # -- public API -------------------------------------------------------

    def set_scheme(self, bg: str, hover: str, press: str) -> None:
        self._bg = bg
        self._hover = hover
        self._press = press
        self._redraw()

    def set_disabled(self, disabled: bool) -> None:
        self._disabled = disabled
        if disabled:
            self.stop_pulse()
        self._redraw()

    def start_pulse(self) -> None:
        if self._pulse_running:
            return
        self._pulse_running = True
        self._pulse_phase = 0
        self._pulse_tick()

    def stop_pulse(self) -> None:
        self._pulse_running = False
        if self._pulse_after is not None:
            self.after_cancel(self._pulse_after)
            self._pulse_after = None
        if self._shadow_id is not None:
            self.delete(self._shadow_id)
            self._shadow_id = None

    def start_arc(self) -> None:
        """Start the rotating arc light around the button edge."""
        if self._arc_running:
            return
        self._arc_running = True
        self._arc_angle = 0.0
        self._arc_tick()

    def stop_arc(self) -> None:
        """Stop the rotating arc light."""
        self._arc_running = False
        if self._arc_after is not None:
            self.after_cancel(self._arc_after)
            self._arc_after = None
        if self._arc_id is not None:
            self.delete(self._arc_id)
            self._arc_id = None

    # -- pulse animation --------------------------------------------------

    def _pulse_tick(self) -> None:
        if not self._pulse_running:
            return
        n = self._PULSE_STEPS
        phase = self._pulse_phase % (n * 2)
        t = phase / n if phase < n else (n * 2 - phase) / n  # 0→1→0 triangle wave
        # Apple-style: ease-in-out curve for smoother breathing
        t = t * t * (3 - 2 * t)  # smoothstep
        alpha = int(25 + 45 * t)  # softer shadow opacity 25..70
        spread = int(6 + 14 * t)  # wider spread 6..20

        # Draw soft glow behind body
        if self._shadow_id is not None:
            self.delete(self._shadow_id)
        glow_color = self._lerp_hex(self._bg, "#ffffff", 0.35)
        if self._is_circle:
            self._shadow_id = self.create_oval(
                self._ox + 3 - spread, self._oy + 3 - spread,
                self._ox + self._real_w - 3 + spread, self._oy + self._real_h - 3 + spread,
                fill="", outline=self._apply_alpha(glow_color, alpha / 255),
                width=max(3, spread // 2),
            )
        else:
            self._shadow_id = self._rounded_rect(
                self._ox + 3 - spread, self._oy + 3 - spread,
                self._ox + self._real_w - 3 + spread, self._oy + self._real_h - 3 + spread,
                self._r + spread,
                fill="", outline=self._apply_alpha(glow_color, alpha / 255),
                width=max(3, spread // 2),
            )
        self.tag_lower(self._shadow_id)
        # Keep layered shadows behind everything
        for sid in self._shadow_ids:
            self.tag_lower(sid)

        self._pulse_phase += 1
        self._pulse_after = self.after(self._PULSE_STEP_MS, self._pulse_tick)

    # -- rotating arc light -----------------------------------------------

    _ARC_STEP_MS = 30
    _ARC_SPEED = 4.0  # degrees per tick
    _ARC_EXTENT = 90   # arc sweep length in degrees

    def _arc_tick(self) -> None:
        if not self._arc_running:
            return
        if self._arc_id is not None:
            self.delete(self._arc_id)

        ox, oy = self._ox, self._oy
        # Draw arc just outside the button circle
        pad = 4
        x1 = ox + 3 - pad
        y1 = oy + 3 - pad
        x2 = ox + self._real_w - 3 + pad
        y2 = oy + self._real_h - 3 + pad

        # Bright white-ish glow arc
        glow_color = self._lerp_hex(self._bg, "#ffffff", 0.6)
        self._arc_id = self.create_arc(
            x1, y1, x2, y2,
            start=self._arc_angle,
            extent=self._ARC_EXTENT,
            style="arc",
            outline=glow_color,
            width=3,
        )
        # Place arc between shadow and body
        self.tag_raise(self._arc_id)
        self.tag_raise(self._body_id)
        self.tag_raise(self._text_id)

        self._arc_angle = (self._arc_angle + self._ARC_SPEED) % 360
        self._arc_after = self.after(self._ARC_STEP_MS, self._arc_tick)

    @staticmethod
    def _lerp_hex(c1: str, c2: str, t: float) -> str:
        r1, g1, b1 = int(c1[1:3], 16), int(c1[3:5], 16), int(c1[5:7], 16)
        r2, g2, b2 = int(c2[1:3], 16), int(c2[3:5], 16), int(c2[5:7], 16)
        r = int(r1 + (r2 - r1) * t)
        g = int(g1 + (g2 - g1) * t)
        b = int(b1 + (b2 - b1) * t)
        return f"#{r:02x}{g:02x}{b:02x}"

    @staticmethod
    def _apply_alpha(color: str, alpha: float) -> str:
        """Blend *color* toward the canvas parent bg (faking alpha)."""
        bg = CARD_BG
        r1, g1, b1 = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        r2, g2, b2 = int(bg[1:3], 16), int(bg[3:5], 16), int(bg[5:7], 16)
        r = int(r1 * alpha + r2 * (1 - alpha))
        g = int(g1 * alpha + g2 * (1 - alpha))
        b = int(b1 * alpha + b2 * (1 - alpha))
        return f"#{r:02x}{g:02x}{b:02x}"

    # -- event handlers ---------------------------------------------------

    def _on_text_changed(self, *_: Any) -> None:
        self.itemconfig(self._text_id, text=self._textvar.get())

    def _on_enter(self, _e: tk.Event[Any]) -> None:
        self._hovered = True
        self.configure(cursor="hand2")
        self._redraw()

    def _on_leave(self, _e: tk.Event[Any]) -> None:
        self._hovered = False
        self._pressed = False
        self._redraw()

    def _on_press(self, _e: tk.Event[Any]) -> None:
        if self._disabled:
            return
        self._pressed = True
        self._redraw()

    def _on_release(self, _e: tk.Event[Any]) -> None:
        if self._disabled:
            return
        self._pressed = False
        self._redraw()
        if self._hovered and self._command:
            self._command()


class EmbeddedProxyProcess:
    """Run the proxy inside a background thread for frozen single-file builds."""

    def __init__(self, cli_args: list[str], *, base_url: str) -> None:
        self._cli_args = list(cli_args)
        self._base_url = base_url.rstrip("/")
        self._exit_code: int | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="plunger-embedded-proxy",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def poll(self) -> int | None:
        if self._thread.is_alive():
            return None
        return 0 if self._exit_code is None else self._exit_code

    def wait(self, timeout: float | None = None) -> int:
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise subprocess.TimeoutExpired("embedded-proxy", timeout or 0.0)
        return 0 if self._exit_code is None else self._exit_code

    def terminate(self) -> None:
        request = Request(f"{self._base_url}/control/shutdown", method="POST")
        with suppress(Exception):
            with LOCAL_OPENER.open(request, timeout=1.2) as response:
                response.read()

    def _run(self) -> None:
        try:
            from plunger import build_arg_parser as build_proxy_arg_parser, run_proxy

            proxy_args = build_proxy_arg_parser().parse_args(self._cli_args)
            self._exit_code = int(asyncio.run(run_proxy(proxy_args)))
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 0
            self._exit_code = int(code)
        except Exception:
            log.exception("Embedded proxy thread crashed")
            self._exit_code = 1


class ProxyDashboard:
    def __init__(self, root: tk.Tk, args: argparse.Namespace) -> None:
        self.root = root
        # Keep the controller alive for the lifetime of the Tk root.
        # Tk keeps widgets alive, but not this Python state container or the
        # StringVar objects it owns.
        self.root._proxy_dashboard = self  # type: ignore[attr-defined]
        self.args = args
        self.base_url = f"http://127.0.0.1:{args.port}"
        self.process: subprocess.Popen[bytes] | EmbeddedProxyProcess | None = None
        self.after_id: str | None = None
        self.pending_action: str | None = None
        self.pending_deadline = 0.0
        self.pending_start_previous_started_at_ms: int | None = None
        self.pending_start_previous_pid: int | None = None
        self.pending_stop_expected_pid: int | None = None
        self.closed = False
        self.latest_health: dict[str, Any] | None = None
        self.auto_start_attempted = False

        settings = self._load_ui_settings()
        language = str(settings.get("language", "zh_CN"))
        self.language = language if language in LANGUAGES else "zh_CN"

        self.status_var = tk.StringVar()
        self.subtitle_var = tk.StringVar()
        self.listen_var = tk.StringVar()
        self.upstream_var = tk.StringVar()
        self.action_var = tk.StringVar()
        self.message_var = tk.StringVar()
        self.retry_hint_var = tk.StringVar()
        self.active_session_var = tk.StringVar()
        self.current_run_only_var = tk.BooleanVar(value=True)
        self.reconnect_only_var = tk.BooleanVar(value=False)
        self.launch_on_boot_var = tk.BooleanVar(value=self._is_launch_on_boot_enabled())
        self.auto_start_proxy_var = tk.BooleanVar(value=bool(settings.get("auto_start_proxy", False)))
        self.current_version = _detect_app_version()
        self.current_version_var = tk.StringVar()
        self.update_status_var = tk.StringVar()
        self.update_button_var = tk.StringVar()
        self.update_action_mode = "check"
        self.update_button_enabled = True
        self.update_status_key = "update_status_idle"
        self.update_status_kwargs: dict[str, Any] = {}
        self.latest_release_info: dict[str, Any] | None = None
        self.downloaded_update_path: Path | None = None
        self.update_task_active = False
        self.stat_label_vars = {
            key: tk.StringVar()
            for key in ("requests", "success", "triggers", "today_success", "history_success")
        }
        self.stat_value_vars = {
            key: tk.StringVar(value="0")
            for key in ("requests", "success", "triggers", "today_success", "history_success")
        }

        self._configure_window()
        self._configure_styles()
        self._build_layout()
        self._apply_language()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._refresh_state()
        self._schedule_refresh()
        self.root.after(220, self._maybe_auto_start_proxy)

    def _build_proxy_cli_args(self) -> list[str]:
        cli_args = [
            "--port",
            str(self.args.port),
            "--timeout",
            str(self.args.timeout),
            "--retries",
            str(self.args.retries),
            "--watch-interval",
            str(self.args.watch_interval),
            "--aggressive-autoevolve",
            "--enable-supervisor",
        ]
        if self.args.upstream:
            cli_args.extend(["--upstream", self.args.upstream])
        return cli_args

    def _build_proxy_command(self) -> list[str]:
        if _is_frozen_app():
            return [sys.executable, "--headless", *self._build_proxy_cli_args()]
        return [
            sys.executable,
            str(Path(__file__).with_name("plunger.py")),
            *self._build_proxy_cli_args(),
        ]

    def _load_ui_settings(self) -> dict[str, Any]:
        if not UI_SETTINGS_FILE.exists():
            return {}
        try:
            return json.loads(UI_SETTINGS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_ui_settings(self) -> None:
        UI_SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        UI_SETTINGS_FILE.write_text(
            json.dumps(
                {
                    "language": self.language,
                    "launch_on_boot": bool(self.launch_on_boot_var.get()),
                    "auto_start_proxy": bool(self.auto_start_proxy_var.get()),
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    def _t(self, key: str, **kwargs: Any) -> str:
        template = TRANSLATIONS.get(self.language, TRANSLATIONS["zh_CN"]).get(key, key)
        return template.format(**kwargs)

    def _set_update_status(self, key: str, **kwargs: Any) -> None:
        self.update_status_key = key
        self.update_status_kwargs = dict(kwargs)
        self.update_status_var.set(self._t(key, **kwargs))

    def _set_update_button_state(self, key: str, *, mode: str, enabled: bool) -> None:
        self.update_action_mode = mode
        self.update_button_enabled = enabled
        self.update_button_var.set(self._t(key))
        if hasattr(self, "update_button"):
            self.update_button.configure(state="normal" if enabled else "disabled")

    def _apply_update_texts(self) -> None:
        self.current_version_var.set(self._t("current_version_label", version=self.current_version))
        self.update_status_var.set(self._t(self.update_status_key, **self.update_status_kwargs))
        button_key = {
            "check": "update_button_check",
            "checking": "update_button_checking",
            "download": "update_button_download",
            "downloading": "update_button_downloading",
            "open_download": "update_button_open_download",
            "open_release": "update_button_open_release",
        }.get(self.update_action_mode, "update_button_check")
        self.update_button_var.set(self._t(button_key))
        if hasattr(self, "update_button"):
            self.update_button.configure(state="normal" if self.update_button_enabled else "disabled")

    def _configure_window(self) -> None:
        self.root.geometry("1220x1104")
        self.root.minsize(1020, 780)
        self.root.configure(bg=WINDOW_BG)
        self._set_window_icon()

    def _set_window_icon(self) -> None:
        """Create a plunger icon as the window icon."""
        size = 64
        img = tk.PhotoImage(width=size, height=size)
        # Draw on a temporary canvas, then read pixels
        tmp = tk.Canvas(self.root, width=size, height=size, bg="#ffffff", highlightthickness=0)
        # Wooden handle
        tmp.create_rectangle(28, 4, 36, 32, fill="#c8956c", outline="#a67548", width=1)
        # T-bar grip
        tmp.create_rectangle(20, 2, 44, 8, fill="#b07840", outline="#8c5e30", width=1)
        # Rubber dome
        tmp.create_arc(8, 22, 56, 58, start=0, extent=180, fill=ACCENT, outline="#d06040", width=2, style="chord")
        # Bottom rim
        tmp.create_rectangle(8, 40, 56, 46, fill=ACCENT, outline="#d06040", width=1)
        # Highlight
        tmp.create_arc(18, 28, 40, 44, start=30, extent=120, fill="#ff9a76", outline="", style="pieslice")
        tmp.update()
        # Render canvas to PhotoImage via postscript is unreliable on Windows;
        # instead, build a simple XBM-style icon by putting pixels row by row.
        # Use a simpler approach: draw directly into PhotoImage
        tmp.destroy()

        # Build icon using PhotoImage horizontal lines
        bg = "#ffffff"
        handle_color = "#c8956c"
        grip_color = "#b07840"
        dome_color = ACCENT  # "#ff7b54"

        # Fill background
        row_bg = "{" + " ".join([bg] * size) + "}"
        img.put(" ".join([row_bg] * size), to=(0, 0))

        # T-bar grip (y=2..7, x=20..43)
        for y in range(4, 12):
            img.put("{" + " ".join([grip_color] * 24) + "}", to=(20, y))

        # Handle (y=8..31, x=28..35)
        for y in range(12, 36):
            img.put("{" + " ".join([handle_color] * 8) + "}", to=(28, y))

        # Dome (approximate semicircle y=30..50, centered at x=32)
        import math
        cx, cy, rx, ry = 32, 38, 22, 16
        for y in range(30, 52):
            dy = y - cy
            if dy < 0 and abs(dy) <= ry:
                half_w = int(rx * math.sqrt(1 - (dy / ry) ** 2))
                x1 = max(0, cx - half_w)
                x2 = min(size, cx + half_w)
                w = x2 - x1
                if w > 0:
                    img.put("{" + " ".join([dome_color] * w) + "}", to=(x1, y))
            elif dy >= 0:
                # Bottom rectangle part
                img.put("{" + " ".join([dome_color] * 44) + "}", to=(10, y))

        self.root.iconphoto(True, img)
        self._icon_img = img  # prevent GC

    def _configure_styles(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")
        style.configure(
            "Primary.TButton",
            font=(_FONT_UI, 12, "bold"),
            padding=(22, 12),
            foreground="white",
            background=ACCENT,
            borderwidth=0,
        )
        style.map("Primary.TButton", background=[("active", ACCENT_HOVER), ("disabled", "#ddc8b8")])
        style.configure(
            "Danger.TButton",
            font=(_FONT_UI, 12, "bold"),
            padding=(22, 12),
            foreground="white",
            background=DANGER,
            borderwidth=0,
        )
        style.map("Danger.TButton", background=[("active", DANGER_HOVER), ("disabled", "#ddd")])
        style.configure(
            "Panel.TCombobox",
            padding=6,
            arrowsize=14,
            fieldbackground="#f5efe8",
            background="#e8e0d8",
            foreground=TEXT_PRIMARY,
            selectbackground=ACCENT,
        )
        style.map("Panel.TCombobox", fieldbackground=[("readonly", "#f5efe8")])
        style.configure(
            "Events.Treeview",
            font=(_FONT_UI, 10),
            rowheight=36,
            background=CARD_BG,
            fieldbackground=CARD_BG,
            foreground=TEXT_PRIMARY,
            borderwidth=0,
        )
        style.configure(
            "Events.Treeview.Heading",
            font=(_FONT_UI, 10, "bold"),
            background=HEADING_BG,
            foreground=HEADING_FG,
            relief="flat",
        )
        style.map("Events.Treeview.Heading", background=[("active", HEADING_ACTIVE)])
        style.configure(
            "Active.Treeview",
            font=(_FONT_UI, 10),
            rowheight=36,
            background=CARD_BG,
            fieldbackground=CARD_BG,
            foreground=TEXT_PRIMARY,
            borderwidth=0,
        )
        style.configure(
            "Active.Treeview.Heading",
            font=(_FONT_UI, 10, "bold"),
            background=HEADING_BG,
            foreground=HEADING_FG,
            relief="flat",
        )
        style.map("Active.Treeview.Heading", background=[("active", HEADING_ACTIVE)])

    def _build_layout(self) -> None:
        shell = tk.Frame(self.root, bg=WINDOW_BG)
        shell.pack(fill="both", expand=True, padx=22, pady=20)

        nav_card = self._card(shell, pady=12, padx=14)
        nav_card.pack(fill="x", pady=(0, 16))

        tab_bar = tk.Frame(nav_card, bg=CARD_BG)
        tab_bar.pack(fill="x")
        for column in range(4):
            tab_bar.grid_columnconfigure(column, weight=1, uniform="top-tabs")

        self._tab_buttons: list[tk.Label] = []
        self._tab_frames: list[tk.Frame] = []
        self._active_tab = 0

        self.tab_home_btn = tk.Label(
            tab_bar,
            bg=ACCENT,
            fg="#ffffff",
            font=(_FONT_UI, 11, "bold"),
            padx=22,
            pady=10,
            cursor="hand2",
            bd=0,
            relief="flat",
        )
        self.tab_home_btn.grid(row=0, column=0, sticky="ew", padx=3)
        self.tab_home_btn.bind("<Button-1>", lambda _: self._switch_tab(0))
        self._tab_buttons.append(self.tab_home_btn)

        self.tab_active_btn = tk.Label(
            tab_bar,
            bg="#f3ede6",
            fg=TEXT_SECONDARY,
            font=(_FONT_UI, 11),
            padx=22,
            pady=10,
            cursor="hand2",
            bd=0,
            relief="flat",
        )
        self.tab_active_btn.grid(row=0, column=1, sticky="ew", padx=3)
        self.tab_active_btn.bind("<Button-1>", lambda _: self._switch_tab(1))
        self._tab_buttons.append(self.tab_active_btn)

        self.tab_events_btn = tk.Label(
            tab_bar,
            bg="#f3ede6",
            fg=TEXT_SECONDARY,
            font=(_FONT_UI, 11),
            padx=22,
            pady=10,
            cursor="hand2",
            bd=0,
            relief="flat",
        )
        self.tab_events_btn.grid(row=0, column=2, sticky="ew", padx=3)
        self.tab_events_btn.bind("<Button-1>", lambda _: self._switch_tab(2))
        self._tab_buttons.append(self.tab_events_btn)

        self.tab_settings_btn = tk.Label(
            tab_bar,
            bg="#f3ede6",
            fg=TEXT_SECONDARY,
            font=(_FONT_UI, 11),
            padx=22,
            pady=10,
            cursor="hand2",
            bd=0,
            relief="flat",
        )
        self.tab_settings_btn.grid(row=0, column=3, sticky="ew", padx=3)
        self.tab_settings_btn.bind("<Button-1>", lambda _: self._switch_tab(3))
        self._tab_buttons.append(self.tab_settings_btn)

        page_stack = tk.Frame(shell, bg=WINDOW_BG)
        page_stack.pack(fill="both", expand=True)

        home_page = tk.Frame(page_stack, bg=WINDOW_BG)
        self._tab_frames.append(home_page)

        hero = self._card(home_page, pady=24, padx=24)
        hero.pack(fill="x")

        hero_body = tk.Frame(hero, bg=CARD_BG)
        hero_body.pack(fill="both", expand=True)

        hero_left = tk.Frame(hero_body, bg=CARD_BG)
        hero_left.pack(side="left", fill="both", expand=True)

        hero_right = tk.Frame(hero_body, bg=CARD_BG)
        hero_right.pack(side="right", padx=(20, 10), anchor="center")

        mask_frame = tk.Frame(hero_left, bg=CARD_BG)
        mask_frame.pack(anchor="w", pady=(0, 4))
        self.mask_canvas = tk.Canvas(
            mask_frame,
            width=52,
            height=52,
            bg=CARD_BG,
            highlightthickness=0,
        )
        self.mask_canvas.pack(side="left", padx=(0, 14))
        self._draw_p5_mask(self.mask_canvas)

        self.hero_title_label = tk.Label(
            mask_frame,
            bg=CARD_BG,
            fg=ACCENT,
            font=(_FONT_TITLE, 28, "bold"),
        )
        self.hero_title_label.pack(side="left", anchor="s", pady=(0, 2))

        self.hero_subtitle_label = tk.Label(
            hero_left,
            bg=CARD_BG,
            fg=TEXT_SECONDARY,
            font=(_FONT_UI, 10, "italic"),
        )
        self.hero_subtitle_label.pack(anchor="w", pady=(2, 14))

        tk.Label(
            hero_left,
            textvariable=self.subtitle_var,
            bg=CARD_BG,
            fg=TEXT_PRIMARY,
            font=(_FONT_UI, 12),
            wraplength=620,
            justify="left",
        ).pack(anchor="w", pady=(8, 10))

        tk.Label(
            hero_left,
            textvariable=self.listen_var,
            bg=CARD_BG,
            fg=TEXT_SECONDARY,
            font=(_FONT_MONO, 10),
        ).pack(anchor="w", pady=(4, 2))
        tk.Label(
            hero_left,
            textvariable=self.upstream_var,
            bg=CARD_BG,
            fg=TEXT_SECONDARY,
            font=(_FONT_MONO, 10),
            wraplength=700,
            justify="left",
        ).pack(anchor="w")

        self.action_button = FancyButton(
            hero_right,
            textvariable=self.action_var,
            command=self._toggle_proxy,
            btn_width=286,
            btn_height=286,
            radius=140,
            font=(_FONT_UI, 34, "bold"),
            bg_color=ACCENT,
            hover_color=ACCENT_HOVER,
            press_color="#e06840",
            fg_color="#ffffff",
            disabled_bg="#ddc8b8",
            disabled_fg="#f7efe8",
        )
        self.action_button.pack(anchor="center")

        stat_row = tk.Frame(home_page, bg=WINDOW_BG)
        stat_row.pack(fill="x", pady=(20, 0))
        for key in ("requests", "triggers", "success"):
            card = self._card(stat_row, pady=18, padx=18)
            card.pack(side="left", fill="both", expand=True)
            if key != "success":
                card.pack_configure(padx=(0, 12))
            tk.Label(
                card,
                textvariable=self.stat_label_vars[key],
                bg=CARD_BG,
                fg=TEXT_SECONDARY,
                font=(_FONT_UI, 10, "bold"),
            ).pack(anchor="w")
            tk.Label(
                card,
                textvariable=self.stat_value_vars[key],
                bg=CARD_BG,
                fg=ACCENT,
                font=(_FONT_UI, 28, "bold"),
            ).pack(anchor="w", pady=(8, 0))

        extra_stat_row = tk.Frame(home_page, bg=WINDOW_BG)
        extra_stat_row.pack(fill="x", pady=(12, 0))
        for index, key in enumerate(("today_success", "history_success")):
            card = self._card(extra_stat_row, pady=18, padx=18)
            card.pack(side="left", fill="both", expand=True)
            if index == 0:
                card.pack_configure(padx=(0, 12))
            tk.Label(
                card,
                textvariable=self.stat_label_vars[key],
                bg=CARD_BG,
                fg=TEXT_SECONDARY,
                font=(_FONT_UI, 10, "bold"),
            ).pack(anchor="w")
            tk.Label(
                card,
                textvariable=self.stat_value_vars[key],
                bg=CARD_BG,
                fg=ACCENT,
                font=(_FONT_UI, 28, "bold"),
            ).pack(anchor="w", pady=(8, 0))

        active_page = self._card(page_stack, pady=18, padx=20)
        self._tab_frames.append(active_page)

        self.active_title_label = tk.Label(
            active_page,
            bg=CARD_BG,
            fg=ACCENT,
            font=(_FONT_UI, 18, "bold"),
        )
        self.active_title_label.pack(anchor="w")

        self.active_count_label = tk.Label(
            active_page,
            bg=CARD_BG,
            fg=TEXT_SECONDARY,
            font=(_FONT_UI, 10),
        )
        self.active_count_label.pack(anchor="w", pady=(6, 12))

        active_table_wrap = tk.Frame(active_page, bg=CARD_BG)
        active_table_wrap.pack(fill="both", expand=True)

        self.active_tree = ttk.Treeview(
            active_table_wrap,
            columns=("status", "endpoint", "model", "client", "input"),
            show="headings",
            style="Active.Treeview",
        )
        self.active_tree.column("status", width=90, minwidth=70, stretch=False, anchor="w")
        self.active_tree.column("endpoint", width=280, minwidth=160, stretch=True, anchor="w")
        self.active_tree.column("model", width=120, minwidth=80, stretch=False, anchor="w")
        self.active_tree.column("client", width=160, minwidth=100, stretch=False, anchor="w")
        self.active_tree.column("input", width=350, minwidth=200, stretch=True, anchor="w")
        self.active_tree.grid(row=0, column=0, sticky="nsew")

        self.active_tree.tag_configure("running", foreground="#2eaa8a")
        self.active_tree.tag_configure("recovering", foreground="#d09520")
        self.active_tree.tag_configure("waiting", foreground="#d09520")
        self.active_tree.tag_configure("timeout", foreground="#e85d4a")

        active_sb_y = ttk.Scrollbar(active_table_wrap, orient="vertical", command=self.active_tree.yview)
        active_sb_y.grid(row=0, column=1, sticky="ns")
        active_sb_x = ttk.Scrollbar(active_table_wrap, orient="horizontal", command=self.active_tree.xview)
        active_sb_x.grid(row=1, column=0, sticky="ew")
        self.active_tree.configure(yscrollcommand=active_sb_y.set, xscrollcommand=active_sb_x.set)
        active_table_wrap.grid_rowconfigure(0, weight=1)
        active_table_wrap.grid_columnconfigure(0, weight=1)

        events_page = self._card(page_stack, pady=18, padx=18)
        self._tab_frames.append(events_page)

        header = tk.Frame(events_page, bg=CARD_BG)
        header.pack(fill="x")

        header_top = tk.Frame(header, bg=CARD_BG)
        header_top.pack(fill="x")

        self.events_title_label = tk.Label(
            header_top,
            bg=CARD_BG,
            fg=ACCENT,
            font=(_FONT_UI, 18, "bold"),
        )
        self.events_title_label.pack(side="left", anchor="w")

        self.events_scope_check = tk.Checkbutton(
            header_top,
            bg=CARD_BG,
            activebackground=CARD_BG,
            fg=TEXT_SECONDARY,
            activeforeground=TEXT_PRIMARY,
            selectcolor=CARD_BG,
            highlightthickness=0,
            bd=0,
            relief="flat",
            font=(_FONT_UI, 10),
            variable=self.current_run_only_var,
            command=self._on_events_scope_toggled,
        )
        self.events_scope_check.pack(side="right", anchor="e")

        self.events_reconnect_only_check = tk.Checkbutton(
            header_top,
            bg=CARD_BG,
            activebackground=CARD_BG,
            fg=TEXT_SECONDARY,
            activeforeground=TEXT_PRIMARY,
            selectcolor=CARD_BG,
            highlightthickness=0,
            bd=0,
            relief="flat",
            font=(_FONT_UI, 10),
            variable=self.reconnect_only_var,
            command=self._on_events_scope_toggled,
        )
        self.events_reconnect_only_check.pack(side="right", anchor="e", padx=(0, 8))

        self.events_subtitle_label = tk.Label(
            header,
            bg=CARD_BG,
            fg=TEXT_SECONDARY,
            font=(_FONT_UI, 10),
        )
        self.events_subtitle_label.pack(anchor="w", pady=(6, 12))

        table_wrap = tk.Frame(events_page, bg=CARD_BG)
        table_wrap.pack(fill="both", expand=True)

        self.events_tree = ttk.Treeview(
            table_wrap,
            columns=("time", "state", "summary"),
            show="headings",
            style="Events.Treeview",
        )
        self.events_tree.column("time", width=160, minwidth=120, stretch=False, anchor="w")
        self.events_tree.column("state", width=80, minwidth=60, stretch=False, anchor="w")
        self.events_tree.column("summary", width=760, minwidth=400, stretch=True, anchor="w")
        self.events_tree.grid(row=0, column=0, sticky="nsew")

        self.events_tree.tag_configure("success", foreground="#2eaa8a")
        self.events_tree.tag_configure("warning", foreground="#d09520")
        self.events_tree.tag_configure("danger", foreground="#e85d4a")
        self.events_tree.tag_configure("neutral", foreground=TEXT_PRIMARY)

        scrollbar_y = ttk.Scrollbar(table_wrap, orient="vertical", command=self.events_tree.yview)
        scrollbar_y.grid(row=0, column=1, sticky="ns")
        scrollbar_x = ttk.Scrollbar(table_wrap, orient="horizontal", command=self.events_tree.xview)
        scrollbar_x.grid(row=1, column=0, sticky="ew")
        self.events_tree.configure(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)
        table_wrap.grid_rowconfigure(0, weight=1)
        table_wrap.grid_columnconfigure(0, weight=1)

        settings_page = tk.Frame(page_stack, bg=WINDOW_BG)
        self._tab_frames.append(settings_page)

        settings_shell = tk.Frame(settings_page, bg=WINDOW_BG)
        settings_shell.pack(fill="both", expand=True)

        settings_card = tk.Frame(
            settings_shell,
            bg=SETTINGS_BG,
            highlightthickness=1,
            highlightbackground=ACCENT,
            padx=28,
            pady=26,
        )
        settings_card.pack(pady=(28, 0))

        self.settings_title_label = tk.Label(
            settings_card,
            bg=SETTINGS_BG,
            fg=ACCENT,
            font=(_FONT_UI, 20, "bold"),
        )
        self.settings_title_label.pack(anchor="w")

        self.settings_subtitle_label = tk.Label(
            settings_card,
            bg=SETTINGS_BG,
            fg=TEXT_SECONDARY,
            font=(_FONT_UI, 10),
            justify="left",
            wraplength=560,
        )
        self.settings_subtitle_label.pack(anchor="w", pady=(8, 0))

        language_card = tk.Frame(
            settings_card,
            bg="#fffaf5",
            highlightthickness=1,
            highlightbackground=CARD_BORDER,
            padx=18,
            pady=16,
        )
        language_card.pack(fill="x", pady=(18, 0))

        self.language_label = tk.Label(
            language_card,
            bg="#fffaf5",
            fg=TEXT_SECONDARY,
            font=(_FONT_UI, 10, "bold"),
        )
        self.language_label.pack(anchor="w")

        language_row = tk.Frame(language_card, bg="#fffaf5")
        language_row.pack(anchor="w", pady=(10, 0))

        self.language_combo = ttk.Combobox(
            language_row,
            state="readonly",
            width=14,
            style="Panel.TCombobox",
            values=[LANGUAGES["zh_CN"], LANGUAGES["en_US"]],
        )
        self.language_combo.pack(side="left")
        self.language_combo.bind("<<ComboboxSelected>>", self._on_language_selected)

        startup_card = tk.Frame(
            settings_card,
            bg="#fffaf5",
            highlightthickness=1,
            highlightbackground=CARD_BORDER,
            padx=18,
            pady=16,
        )
        startup_card.pack(fill="x", pady=(14, 0))

        self.launch_on_boot_check = tk.Checkbutton(
            startup_card,
            bg="#fffaf5",
            activebackground="#fffaf5",
            fg=TEXT_SECONDARY,
            activeforeground=TEXT_PRIMARY,
            selectcolor="#fffaf5",
            highlightthickness=0,
            bd=0,
            relief="flat",
            font=(_FONT_UI, 10),
            variable=self.launch_on_boot_var,
            command=self._on_launch_on_boot_toggled,
        )
        self.launch_on_boot_check.pack(anchor="w")

        self.auto_start_proxy_check = tk.Checkbutton(
            startup_card,
            bg="#fffaf5",
            activebackground="#fffaf5",
            fg=TEXT_SECONDARY,
            activeforeground=TEXT_PRIMARY,
            selectcolor="#fffaf5",
            highlightthickness=0,
            bd=0,
            relief="flat",
            font=(_FONT_UI, 10),
            variable=self.auto_start_proxy_var,
            command=self._on_auto_start_proxy_toggled,
        )
        self.auto_start_proxy_check.pack(anchor="w", pady=(8, 0))

        update_card = tk.Frame(
            settings_card,
            bg="#fffaf5",
            highlightthickness=1,
            highlightbackground=CARD_BORDER,
            padx=18,
            pady=16,
        )
        update_card.pack(fill="x", pady=(14, 0))

        self.update_title_label = tk.Label(
            update_card,
            bg="#fffaf5",
            fg=TEXT_SECONDARY,
            font=(_FONT_UI, 10, "bold"),
        )
        self.update_title_label.pack(anchor="w")

        self.current_version_label = tk.Label(
            update_card,
            textvariable=self.current_version_var,
            bg="#fffaf5",
            fg=TEXT_PRIMARY,
            font=(_FONT_UI, 11, "bold"),
        )
        self.current_version_label.pack(anchor="w", pady=(8, 0))

        update_row = tk.Frame(update_card, bg="#fffaf5")
        update_row.pack(fill="x", pady=(10, 0))

        self.update_status_label = tk.Label(
            update_row,
            textvariable=self.update_status_var,
            bg="#fffaf5",
            fg=TEXT_SECONDARY,
            font=(_FONT_UI, 10),
            justify="left",
            wraplength=360,
        )
        self.update_status_label.pack(side="left", fill="x", expand=True)

        self.update_button = tk.Button(
            update_row,
            textvariable=self.update_button_var,
            command=self._on_update_button_clicked,
            bg=ACCENT,
            fg="#ffffff",
            activebackground=ACCENT_HOVER,
            activeforeground="#ffffff",
            relief="flat",
            bd=0,
            padx=18,
            pady=10,
            cursor="hand2",
            font=(_FONT_UI, 10, "bold"),
        )
        self.update_button.pack(side="right", padx=(14, 0))

        self._switch_tab(0)

    def _card(self, parent: tk.Misc, *, padx: int, pady: int) -> tk.Frame:
        return tk.Frame(
            parent,
            bg=CARD_BG,
            highlightthickness=1,
            highlightbackground=CARD_BORDER,
            padx=padx,
            pady=pady,
        )

    def _switch_tab(self, index: int) -> None:
        self._active_tab = index
        for i, (btn, frame) in enumerate(zip(self._tab_buttons, self._tab_frames)):
            if i == index:
                btn.configure(bg=ACCENT, fg="#ffffff", font=(_FONT_UI, 11, "bold"))
                frame.pack(fill="both", expand=True, pady=(0, 0))
            else:
                btn.configure(bg="#f3ede6", fg=TEXT_SECONDARY, font=(_FONT_UI, 11))
                frame.pack_forget()

    def _draw_p5_mask(self, canvas: tk.Canvas) -> None:
        """Draw a stylized toilet plunger icon on the canvas."""
        # Long wooden handle
        canvas.create_rectangle(23, 0, 29, 30, fill="#c8956c", outline="#a67548", width=1)
        # Handle top knob
        canvas.create_oval(20, 0, 32, 6, fill="#b07840", outline="#8c5e30", width=1)
        # Rubber cup - thick dome using overlapping arcs
        canvas.create_arc(
            4, 18, 48, 52,
            start=0, extent=180,
            fill=ACCENT, outline="#d06040", width=2, style="pieslice",
        )
        # Inner shadow on cup for depth
        canvas.create_arc(
            8, 22, 44, 50,
            start=10, extent=60,
            fill="#ff9a76", outline="", style="pieslice",
        )
        # Bottom rim of cup (flat edge)
        canvas.create_line(4, 35, 48, 35, fill="#d06040", width=2)

    def _apply_language(self) -> None:
        self.root.title(self._t("window_title"))
        self.hero_title_label.configure(text=self._t("hero_title"))
        self.hero_subtitle_label.configure(text=self._t("hero_subtitle"))
        self.settings_title_label.configure(text=self._t("settings_title"))
        self.settings_subtitle_label.configure(text=self._t("settings_subtitle"))
        self.language_label.configure(text=self._t("language_label"))
        self.launch_on_boot_check.configure(text=self._t("launch_on_boot_label"))
        self.auto_start_proxy_check.configure(text=self._t("auto_start_proxy_label"))
        self.update_title_label.configure(text=self._t("update_title"))
        self.events_title_label.configure(text=self._t("events_title"))
        self.events_subtitle_label.configure(text=self._t("events_subtitle"))
        self.events_scope_check.configure(text=self._t("events_scope_current_run"))
        self.events_reconnect_only_check.configure(text=self._t("events_scope_reconnect_only"))
        self.active_title_label.configure(text=self._t("active_title"))
        self.tab_home_btn.configure(text=self._t("tab_home"))
        self.tab_active_btn.configure(text=self._t("tab_active_sessions"))
        self.tab_events_btn.configure(text=self._t("tab_events"))
        self.tab_settings_btn.configure(text=self._t("tab_settings"))
        self._switch_tab(self._active_tab)
        self.events_tree.heading("time", text=self._t("col_time"))
        self.events_tree.heading("state", text=self._t("col_state"))
        self.events_tree.heading("summary", text=self._t("col_summary"))
        self.active_tree.heading("status", text=self._t("active_col_status"))
        self.active_tree.heading("endpoint", text=self._t("active_col_endpoint"))
        self.active_tree.heading("model", text=self._t("active_col_model"))
        self.active_tree.heading("client", text=self._t("active_col_client"))
        self.active_tree.heading("input", text=self._t("active_col_input"))
        self.stat_label_vars["requests"].set(self._t("stats_requests"))
        self.stat_label_vars["success"].set(self._t("stats_success"))
        self.stat_label_vars["triggers"].set(self._t("stats_triggers"))
        self.stat_label_vars["today_success"].set(self._t("stats_today_success"))
        self.stat_label_vars["history_success"].set(self._t("stats_history_success"))
        self.retry_hint_var.set(self._t("trigger_hint"))
        self.language_combo.set(LANGUAGES[self.language])
        self.current_version_var.set(self._t("current_version_label", version=self.current_version))
        self._apply_update_texts()
        self._apply_health(self.latest_health)

    def _on_language_selected(self, _event: tk.Event[Any]) -> None:
        mapping = {label: code for code, label in LANGUAGES.items()}
        new_language = mapping.get(self.language_combo.get(), "zh_CN")
        if new_language == self.language:
            return
        self.language = new_language
        self._save_ui_settings()
        self._apply_language()

    def _on_events_scope_toggled(self) -> None:
        self._apply_health(self.latest_health)

    def _on_launch_on_boot_toggled(self) -> None:
        enabled = bool(self.launch_on_boot_var.get())
        try:
            self._set_launch_on_boot(enabled)
        except Exception:
            self.launch_on_boot_var.set(not enabled)
            self.message_var.set(self._t("footer_startup_setting_failed"))
            return
        self._save_ui_settings()
        self.message_var.set(
            self._t("footer_launch_on_boot_enabled" if enabled else "footer_launch_on_boot_disabled")
        )

    def _on_auto_start_proxy_toggled(self) -> None:
        enabled = bool(self.auto_start_proxy_var.get())
        self._save_ui_settings()
        self.message_var.set(
            self._t(
                "footer_auto_start_proxy_enabled"
                if enabled
                else "footer_auto_start_proxy_disabled"
            )
        )

    def _on_update_button_clicked(self) -> None:
        if self.update_task_active:
            return

        if self.update_action_mode == "download":
            self._start_update_download()
            return

        if self.update_action_mode == "open_download":
            if self.downloaded_update_path is not None:
                try:
                    _open_path_in_shell(self.downloaded_update_path)
                except Exception as exc:
                    reason = _format_request_error(exc)
                    self._set_update_status("update_status_failed", reason=reason)
                    self.message_var.set(self._t("footer_update_failed", reason=reason))
                else:
                    self.message_var.set(
                        self._t("footer_update_downloaded", path=str(self.downloaded_update_path))
                    )
                return
            self._set_update_button_state("update_button_check", mode="check", enabled=True)

        if self.update_action_mode == "open_release":
            self._open_release_page()
            return

        self._start_update_check()

    def _start_update_check(self) -> None:
        if self.update_task_active:
            return
        self.update_task_active = True
        self.downloaded_update_path = None
        self._set_update_status("update_status_checking")
        self._set_update_button_state("update_button_checking", mode="checking", enabled=False)
        self.message_var.set(self._t("footer_update_checking"))
        threading.Thread(
            target=self._run_update_check,
            name="plunger-update-check",
            daemon=True,
        ).start()

    def _run_update_check(self) -> None:
        try:
            release_info = _fetch_latest_release_info(self.current_version)
        except Exception as exc:
            reason = _format_request_error(exc)
            with suppress(Exception):
                self.root.after(0, lambda: self._finish_update_check_error(reason))
            return

        with suppress(Exception):
            self.root.after(0, lambda: self._finish_update_check(release_info))

    def _finish_update_check(self, release_info: dict[str, Any]) -> None:
        if self.closed:
            return
        self.update_task_active = False
        self.latest_release_info = dict(release_info)
        latest_version = str(release_info.get("latest_version", "")).strip() or self.current_version
        asset_url = str(release_info.get("asset_url", "")).strip()

        if bool(release_info.get("is_newer")):
            if asset_url:
                self._set_update_status("update_status_available_download", version=latest_version)
                self._set_update_button_state("update_button_download", mode="download", enabled=True)
            else:
                self._set_update_status("update_status_available_release", version=latest_version)
                self._set_update_button_state("update_button_open_release", mode="open_release", enabled=True)
            self.message_var.set(self._t("footer_update_available", version=latest_version))
            return

        self._set_update_status("update_status_latest", version=latest_version)
        self._set_update_button_state("update_button_check", mode="check", enabled=True)
        self.message_var.set(self._t("footer_update_latest"))

    def _finish_update_check_error(self, reason: str) -> None:
        if self.closed:
            return
        self.update_task_active = False
        self.latest_release_info = None
        self.downloaded_update_path = None
        self._set_update_status("update_status_failed", reason=reason)
        self._set_update_button_state("update_button_check", mode="check", enabled=True)
        self.message_var.set(self._t("footer_update_failed", reason=reason))

    def _start_update_download(self) -> None:
        if self.update_task_active:
            return

        release_info = self.latest_release_info or {}
        asset_url = str(release_info.get("asset_url", "")).strip()
        asset_name = str(release_info.get("asset_name", "")).strip()
        if not asset_url or not asset_name:
            self._open_release_page()
            return

        self.update_task_active = True
        self._set_update_status("update_status_downloading", name=asset_name)
        self._set_update_button_state("update_button_downloading", mode="downloading", enabled=False)
        self.message_var.set(self._t("footer_update_downloading"))
        threading.Thread(
            target=self._run_update_download,
            args=(asset_url, asset_name),
            name="plunger-update-download",
            daemon=True,
        ).start()

    def _run_update_download(self, asset_url: str, asset_name: str) -> None:
        try:
            target_path = _download_release_asset(asset_url, asset_name)
        except Exception as exc:
            reason = _format_request_error(exc)
            with suppress(Exception):
                self.root.after(0, lambda: self._finish_update_download_error(reason))
            return

        with suppress(Exception):
            self.root.after(0, lambda: self._finish_update_download(target_path))

    def _finish_update_download(self, target_path: Path) -> None:
        if self.closed:
            return
        self.update_task_active = False
        self.downloaded_update_path = target_path
        self._set_update_status("update_status_downloaded", path=str(target_path))
        self._set_update_button_state("update_button_open_download", mode="open_download", enabled=True)
        self.message_var.set(self._t("footer_update_downloaded", path=str(target_path)))
        with suppress(Exception):
            _open_path_in_shell(target_path)

    def _finish_update_download_error(self, reason: str) -> None:
        if self.closed:
            return
        self.update_task_active = False
        self._set_update_status("update_status_download_failed", reason=reason)
        self._set_update_button_state("update_button_download", mode="download", enabled=True)
        self.message_var.set(self._t("footer_update_download_failed", reason=reason))

    def _open_release_page(self) -> None:
        release_url = str((self.latest_release_info or {}).get("html_url", "")).strip() or GITHUB_RELEASES_PAGE_URL
        try:
            webbrowser.open(release_url)
        except Exception as exc:
            reason = _format_request_error(exc)
            self._set_update_status("update_status_failed", reason=reason)
            self.message_var.set(self._t("footer_update_failed", reason=reason))
            return

        self._set_update_button_state("update_button_open_release", mode="open_release", enabled=True)
        if self.latest_release_info and self.latest_release_info.get("latest_version"):
            latest_version = str(self.latest_release_info["latest_version"])
            self._set_update_status("update_status_available_release", version=latest_version)
        self.message_var.set(self._t("footer_update_release_opened"))

    def _toggle_proxy(self) -> None:
        if self.pending_action:
            return
        if self._fetch_health():
            self._stop_proxy()
        else:
            self._start_proxy()

    def _stop_existing_proxy_before_start(
        self,
        previous_started_at_ms: int | None,
        previous_pid: int | None,
    ) -> bool:
        if previous_started_at_ms is None and previous_pid is None:
            return True

        log.info("Requesting shutdown for existing proxy before starting a new instance")
        with suppress(Exception):
            self._request_json("/control/shutdown", method="POST", timeout=1.2)

        if self._wait_for_proxy_exit(previous_started_at_ms, previous_pid, timeout=3.0):
            return True

        if previous_pid and _is_managed_proxy_process(previous_pid):
            log.warning(
                "Existing proxy pid %s did not exit after shutdown request; force terminating",
                previous_pid,
            )
            _terminate_pid_tree(previous_pid)
            if self._wait_for_proxy_exit(previous_started_at_ms, previous_pid, timeout=1.0):
                return True

        if self.process and self.process.poll() is None:
            log.warning("Existing proxy did not exit after shutdown request; terminating tracked process")
            self._terminate_tracked_process(wait_timeout=0.5)
            if self._wait_for_proxy_exit(previous_started_at_ms, previous_pid, timeout=1.0):
                return True

        pid_state = _managed_proxy_process_state(previous_pid) if previous_pid else "dead"
        if previous_pid and pid_state in {"managed", "unknown"}:
            log.warning(
                "Existing proxy pid %s still appears alive after shutdown request (state=%s); refusing to start a replacement",
                previous_pid,
                pid_state,
            )
            return False
        return True

    def _start_proxy(self) -> None:
        if self.process and self.process.poll() is None:
            return
        self.pending_start_previous_pid = None
        self.pending_stop_expected_pid = None
        previous_health = self._fetch_health()
        previous_started_at_ms = self._extract_started_at_ms(previous_health)
        previous_pid = _extract_positive_int((previous_health or {}).get("pid"))
        if not previous_pid:
            previous_pid = _read_proxy_pid(self.base_url)
        if not self._stop_existing_proxy_before_start(previous_started_at_ms, previous_pid):
            self.pending_start_previous_started_at_ms = None
            self.pending_start_previous_pid = None
            self.pending_stop_expected_pid = None
            self.message_var.set(self._t("footer_start_failed"))
            self._refresh_state()
            return
        previous_health = self._fetch_health()
        self.pending_start_previous_started_at_ms = self._extract_started_at_ms(previous_health)
        remaining_previous_pid = _extract_positive_int((previous_health or {}).get("pid"))
        if not remaining_previous_pid and self.pending_start_previous_started_at_ms is None:
            remaining_previous_pid = previous_pid
        self.pending_start_previous_pid = remaining_previous_pid or None
        creationflags = 0
        for flag_name in ("CREATE_NO_WINDOW", "CREATE_NEW_PROCESS_GROUP"):
            creationflags |= getattr(subprocess, flag_name, 0)

        self.process = subprocess.Popen(
            self._build_proxy_command(),
            cwd=str(_app_base_dir()),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        self.pending_action = "start"
        self.pending_deadline = time.time() + (
            20.0
            if (
                self.pending_start_previous_started_at_ms is not None
                or self.pending_start_previous_pid is not None
            )
            else 12.0
        )
        self._set_busy(self._t("footer_starting"))
        self._poll_pending_action()

    def _stop_proxy(self) -> None:
        self.pending_start_previous_started_at_ms = None
        self.pending_start_previous_pid = None
        current_health = self._fetch_health()
        current_pid = _extract_positive_int((current_health or {}).get("pid"))
        if not current_pid:
            current_pid = _read_proxy_pid(self.base_url)
        self.pending_stop_expected_pid = current_pid or None
        supervisor_pid = _extract_positive_int((current_health or {}).get("supervisor_pid"))
        if not supervisor_pid:
            supervisor_pid = _read_supervisor_pid()
        if supervisor_pid:
            _terminate_pid_tree(supervisor_pid)
        with suppress(Exception):
            self._request_json("/control/shutdown", method="POST", timeout=1.2)
        self.pending_action = "stop"
        self.pending_deadline = time.time() + 8.0
        self._set_busy(self._t("footer_stopping"))
        self._poll_pending_action()

    def _poll_pending_action(self) -> None:
        if self.closed or not self.pending_action:
            return

        try:
            health = self._fetch_health()
            now = time.time()
            if self.pending_action == "start":
                if health:
                    if self._is_previous_instance_health(health):
                        if now > self.pending_deadline:
                            self.pending_action = None
                            self.pending_start_previous_started_at_ms = None
                            self.pending_start_previous_pid = None
                            self.pending_stop_expected_pid = None
                            self._clear_busy(self._t("footer_start_timeout"))
                            self._refresh_state()
                            return
                        self.root.after(320, self._poll_pending_action)
                        return
                    self.pending_action = None
                    self.pending_start_previous_started_at_ms = None
                    self.pending_start_previous_pid = None
                    self.pending_stop_expected_pid = None
                    self._clear_busy(self._t("footer_online"))
                    self._apply_health(health)
                    return
                if self.process and self.process.poll() is not None:
                    self.process = None
                    self.pending_action = None
                    self.pending_start_previous_started_at_ms = None
                    self.pending_start_previous_pid = None
                    self.pending_stop_expected_pid = None
                    self._clear_busy(self._t("footer_start_failed"))
                    self._refresh_state()
                    return
                if now > self.pending_deadline:
                    self.pending_action = None
                    self.pending_start_previous_started_at_ms = None
                    self.pending_start_previous_pid = None
                    self.pending_stop_expected_pid = None
                    self._clear_busy(self._t("footer_start_timeout"))
                    self._refresh_state()
                    return
            elif self.pending_action == "stop":
                if self.process:
                    with suppress(Exception):
                        self.process.wait(timeout=0.3)
                    if self.process.poll() is not None:
                        self.process = None
                stop_pid_state = (
                    _managed_proxy_process_state(self.pending_stop_expected_pid)
                    if self.pending_stop_expected_pid
                    else "dead"
                )
                stop_pid_alive = stop_pid_state in {"managed", "unknown"}
                if not health and self.process is None and not stop_pid_alive:
                    self.pending_action = None
                    self.pending_start_previous_started_at_ms = None
                    self.pending_start_previous_pid = None
                    self.pending_stop_expected_pid = None
                    self._clear_busy(self._t("footer_stopped"))
                    self._refresh_state()
                    return
                if now > self.pending_deadline:
                    if stop_pid_state == "managed" and self.pending_stop_expected_pid:
                        _terminate_pid_tree(self.pending_stop_expected_pid)
                    elif stop_pid_state == "unknown":
                        self.pending_action = None
                        self.pending_start_previous_started_at_ms = None
                        self.pending_start_previous_pid = None
                        self.pending_stop_expected_pid = None
                        self._clear_busy(self._t("footer_stop_timeout"))
                        self._refresh_state()
                        return
                    if self.process and self.process.poll() is None:
                        self._terminate_tracked_process(wait_timeout=0.5)
                    self.pending_deadline = now + 2.0
        except Exception:
            log.exception("Pending action polling failed")
            self.pending_action = None
            self.pending_start_previous_started_at_ms = None
            self.pending_start_previous_pid = None
            self.pending_stop_expected_pid = None
            self.process = None
            self._refresh_state()
            return

        self.root.after(320, self._poll_pending_action)

    def _set_busy(self, message: str) -> None:
        self.action_button.set_disabled(True)
        self.message_var.set(message)

    def _clear_busy(self, message: str) -> None:
        self.action_button.set_disabled(False)
        self.message_var.set(message)

    def _schedule_refresh(self) -> None:
        if self.closed:
            return
        self.after_id = self.root.after(POLL_INTERVAL_MS, self._refresh_loop)

    def _maybe_auto_start_proxy(self) -> None:
        if self.closed or self.auto_start_attempted:
            return
        self.auto_start_attempted = True
        if not self.auto_start_proxy_var.get():
            return
        if self.pending_action:
            return
        self._start_proxy()

    def _refresh_loop(self) -> None:
        self.after_id = None
        try:
            self._refresh_state()
        except Exception:
            log.exception("Refresh loop failed")
        self._schedule_refresh()

    def _refresh_state(self) -> None:
        if self.process and self.process.poll() is not None:
            self.process = None
        self.latest_health = self._fetch_health()
        self._apply_health(self.latest_health)

    def _load_recorded_events(self) -> list[dict[str, Any]]:
        try:
            payload = json.loads(EVENT_HISTORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            return []

        items = payload.get("events")
        if not isinstance(items, list):
            return []
        return [dict(item) for item in items if isinstance(item, dict)]

    def _load_recorded_stats(self) -> dict[str, int]:
        payload = _read_json_file(STATS_FILE) or {}
        return {
            key: _extract_positive_int(payload.get(key))
            for key in STATS_KEYS
        }

    def _event_datetime(self, event: dict[str, Any]) -> datetime | None:
        if not isinstance(event, dict):
            return None
        try:
            created_at_ms = int(event.get("created_at_ms"))
        except (TypeError, ValueError):
            created_at_ms = 0
        if created_at_ms > 0:
            return datetime.fromtimestamp(created_at_ms / 1000)
        return self._parse_timestamp(str(event.get("timestamp", "")).strip())

    def _event_recovery_chain_key(self, event: dict[str, Any]) -> str:
        meta = event.get("meta")
        if isinstance(meta, dict):
            for key in ("root_session_id", "session_id", "continued_from_session_id"):
                value = str(meta.get(key, "")).strip()
                if value:
                    return value
        endpoint = str(event.get("endpoint", "")).strip()
        summary = ""
        if isinstance(meta, dict):
            summary = str(meta.get("request_summary", "")).strip()
        if not summary:
            summary = str(event.get("detail", "")).strip()
        return f"{endpoint}|{summary}"

    def _summarize_recovery_outcomes(self, events: list[dict[str, Any]]) -> dict[str, int]:
        today = datetime.now().date()
        ordered = sorted(
            [event for event in events if isinstance(event, dict)],
            key=lambda event: (
                _extract_positive_int(event.get("created_at_ms")),
                str(event.get("timestamp", "")).strip(),
            ),
        )
        chains_by_key: dict[str, list[dict[str, Any]]] = {}

        for event in ordered:
            kind = str(event.get("kind", "")).strip()
            if kind not in {"disconnect", "recovered", "failed"}:
                continue
            chain_key = self._event_recovery_chain_key(event)
            event_dt = self._event_datetime(event)
            history = chains_by_key.setdefault(chain_key, [])

            # Collapse reconnect events into request chains so a later failed
            # overrides an earlier recovered event for the same task.
            if kind == "disconnect" or not history:
                history.append(
                    {
                        "has_disconnect": kind == "disconnect",
                        "latest_kind": kind,
                        "latest_at": event_dt,
                    }
                )
                continue

            chain = history[-1]
            chain["latest_kind"] = kind
            chain["latest_at"] = event_dt

        trigger_count = 0
        success_count = 0
        today_success = 0
        history_success = 0

        for history in chains_by_key.values():
            for chain in history:
                if chain.get("has_disconnect"):
                    trigger_count += 1
                if chain.get("latest_kind") != "recovered":
                    continue
                success_count += 1
                history_success += 1
                latest_at = chain.get("latest_at")
                if latest_at is not None and latest_at.date() == today:
                    today_success += 1

        return {
            "triggers": trigger_count,
            "success": success_count,
            "today_success": today_success,
            "history_success": history_success,
        }

    def _apply_health(self, health: dict[str, Any] | None) -> None:
        running = health is not None
        self.action_var.set(self._t("action_off") if running else self._t("action_on"))
        if running:
            self.action_button.set_scheme(DANGER, DANGER_HOVER, "#c04030")
            self.action_button.stop_pulse()
            self.action_button.start_arc()
        else:
            self.action_button.set_scheme(ACCENT, ACCENT_HOVER, "#e06840")
            self.action_button.start_pulse()
            self.action_button.stop_arc()

        if running:
            started_at = str(health.get("started_at", "")).strip()
            if started_at:
                self.subtitle_var.set(self._t("subtitle_online_with_since", started_at=started_at))
            else:
                self.subtitle_var.set(self._t("subtitle_online_no_since"))
            self.listen_var.set(self._t("listen_prefix", value=health.get("listen", self.base_url)))
            upstream = health.get("upstream") or self._t("upstream_waiting")
            source = self._source_label(str(health.get("upstream_source", "")).strip())
            self.upstream_var.set(self._t("upstream_prefix", value=f"{upstream} ({source})"))
            events = health.get("events", [])
            if not isinstance(events, list):
                events = self._load_recorded_events()
            lifetime_stats = health.get("lifetime_stats")
            if not isinstance(lifetime_stats, dict):
                lifetime_stats = self._load_recorded_stats()
            all_outcomes = self._summarize_recovery_outcomes(events)
            current_run_summary = health.get("current_run_recovery_summary")
            if isinstance(current_run_summary, dict):
                current_run_outcomes = {
                    "triggers": _extract_positive_int(current_run_summary.get("triggers")),
                    "success": _extract_positive_int(current_run_summary.get("success")),
                }
            else:
                current_run_events = self._events_since_start(
                    events,
                    started_at=str(health.get("started_at", "")).strip(),
                    started_at_ms=health.get("started_at_ms"),
                )
                summarized = self._summarize_recovery_outcomes(current_run_events)
                current_run_outcomes = {
                    "triggers": summarized["triggers"],
                    "success": summarized["success"],
                }
            self.stat_value_vars["requests"].set(
                str(
                    _extract_positive_int(
                        health.get("lifetime_total", lifetime_stats.get("total", health.get("total", 0)))
                    )
                )
            )
            self.stat_value_vars["success"].set(str(current_run_outcomes["success"]))
            self.stat_value_vars["triggers"].set(str(current_run_outcomes["triggers"]))
            self.stat_value_vars["today_success"].set(str(all_outcomes["today_success"]))
            self.stat_value_vars["history_success"].set(str(all_outcomes["history_success"]))
            display_active_sessions = health.get("display_active_sessions")
            if not isinstance(display_active_sessions, list):
                display_active_sessions = [
                    health.get("display_active_session")
                ] if health.get("display_active_session") else []
            if not display_active_sessions:
                active_sessions = health.get("active_sessions")
                if not isinstance(active_sessions, list):
                    active_sessions = [health.get("active_session")] if health.get("active_session") else []
                pending_tool_waits = health.get("pending_tool_waits")
                if not isinstance(pending_tool_waits, list):
                    pending_tool_waits = [health.get("pending_tool_wait")] if health.get("pending_tool_wait") else []
                display_active_sessions = self._merge_active_sessions(active_sessions, pending_tool_waits)
            merged_active_sessions = self._merge_active_sessions(display_active_sessions)
            self._fill_active_sessions(merged_active_sessions)
            filtered_events = self._filter_events(
                events,
                started_at=str(health.get("started_at", "")).strip(),
                started_at_ms=health.get("started_at_ms"),
            )
            self._fill_events(filtered_events, current_run_only=self.current_run_only_var.get())
            if not self.pending_action:
                self.message_var.set(self._t("footer_refresh"))
        else:
            subtitle_key = "subtitle_starting" if self.pending_action == "start" else "subtitle_offline"
            upstream_label_key = "upstream_waiting" if self.pending_action == "start" else "upstream_stopped"
            self.subtitle_var.set(self._t(subtitle_key))
            self.listen_var.set(self._t("listen_prefix", value=self.base_url))
            self.upstream_var.set(self._t("upstream_prefix", value=self._t(upstream_label_key)))
            self._fill_active_sessions([])
            recorded_stats = self._load_recorded_stats()
            recorded_events = self._load_recorded_events()
            recorded_outcomes = self._summarize_recovery_outcomes(recorded_events)
            self.stat_value_vars["requests"].set(str(recorded_stats.get("total", 0)))
            self.stat_value_vars["success"].set("0")
            self.stat_value_vars["triggers"].set("0")
            self.stat_value_vars["today_success"].set(str(recorded_outcomes["today_success"]))
            self.stat_value_vars["history_success"].set(str(recorded_outcomes["history_success"]))
            self._fill_events([], current_run_only=self.current_run_only_var.get())
            if not self.pending_action:
                self.message_var.set(self._t("footer_idle"))

    def _events_since_start(
        self,
        events: list[dict[str, Any]],
        *,
        started_at: str,
        started_at_ms: Any,
    ) -> list[dict[str, Any]]:
        try:
            cutoff_ms = int(started_at_ms)
        except (TypeError, ValueError):
            cutoff_ms = 0

        cutoff_dt = self._parse_timestamp(started_at)
        time_filtered: list[dict[str, Any]] = []
        for event in events:
            if not isinstance(event, dict):
                continue
            if cutoff_ms > 0:
                try:
                    event_ms = int(event.get("created_at_ms"))
                except (TypeError, ValueError):
                    event_ms = 0
                if event_ms >= cutoff_ms:
                    time_filtered.append(event)
                    continue
            if cutoff_dt is None:
                time_filtered.append(event)
                continue
            event_dt = self._event_datetime(event)
            if event_dt is not None and event_dt >= cutoff_dt:
                time_filtered.append(event)
        return time_filtered

    def _filter_events(
        self,
        events: list[dict[str, Any]],
        *,
        started_at: str,
        started_at_ms: Any,
    ) -> list[dict[str, Any]]:
        if self.current_run_only_var.get():
            time_filtered = self._events_since_start(
                events,
                started_at=started_at,
                started_at_ms=started_at_ms,
            )
        else:
            time_filtered = [e for e in events if isinstance(e, dict)]

        if self.reconnect_only_var.get():
            reconnect_kinds = {"disconnect", "recovered", "failed"}
            return [e for e in time_filtered if e.get("kind") in reconnect_kinds]

        return time_filtered

    def _parse_timestamp(self, value: str) -> datetime | None:
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    def _is_launch_on_boot_enabled(self) -> bool:
        return STARTUP_LAUNCHER_FILE.exists()

    def _set_launch_on_boot(self, enabled: bool) -> None:
        if enabled:
            STARTUP_DIR.mkdir(parents=True, exist_ok=True)
            if _is_frozen_app():
                launcher_target = _app_executable_path()
                quoted_target = str(launcher_target).replace('"', '""')
                command = f'""{quoted_target}""'
            else:
                launcher_target = Path(__file__).with_name("launch_gui.vbs").resolve()
                quoted_target = str(launcher_target).replace('"', '""')
                command = f'wscript.exe ""{quoted_target}""'
            STARTUP_LAUNCHER_FILE.write_text(
                'CreateObject("WScript.Shell").Run "' + command + '", 0, False\n',
                encoding="utf-8",
            )
            return
        with suppress(FileNotFoundError):
            STARTUP_LAUNCHER_FILE.unlink()

    def _fill_events(self, events: list[dict[str, Any]], *, current_run_only: bool) -> None:
        for item_id in self.events_tree.get_children():
            self.events_tree.delete(item_id)

        if not events:
            empty_summary_key = (
                "event_none_summary_current_run" if current_run_only else "event_none_summary"
            )
            self.events_tree.insert(
                "",
                "end",
                values=("-", self._t("event_none_state"), self._t(empty_summary_key)),
                tags=("neutral",),
            )
            return

        for event in events:
            self.events_tree.insert(
                "",
                "end",
                values=(
                    event.get("timestamp", "-"),
                    self._event_state_label(str(event.get("kind", ""))),
                    self._event_summary(event),
                ),
                tags=(str(event.get("tone", "neutral")),),
            )

    def _fill_active_sessions(self, sessions: Any) -> None:
        self.active_tree.delete(*self.active_tree.get_children())

        normalized = self._merge_active_sessions(sessions)
        if not normalized:
            self.active_count_label.configure(text=self._t("active_none"))
            return

        self.active_count_label.configure(
            text=self._t("active_count_label", count=len(normalized))
        )

        for session in normalized:
            status_raw = str(session.get("status", "")).strip()
            reason = str(session.get("reason", "")).strip()
            reason_display = self._humanize_active_status_reason(session)
            player_hint = self._active_session_player_hint(session, reason=reason)
            if status_raw == "recovering":
                status_text = self._t("active_status_recovering")
                tag = "recovering"
            elif status_raw == "running":
                status_text = self._t("active_status_running")
                tag = "running"
            elif status_raw == "tool_result_timeout":
                status_text = self._t("active_status_tool_timeout")
                tag = "timeout"
            else:
                status_text = self._t("active_status_waiting_tool")
                tag = "waiting"

            endpoint = self._endpoint_label(str(session.get("endpoint_label", "")).strip())
            if endpoint == self._t("event_state_default"):
                endpoint = self._endpoint_label(str(session.get("endpoint", "")).strip())

            request_summary = str(session.get("request_summary", "")).strip()
            summary = request_summary if self._request_summary_is_usable(request_summary) else ""
            if summary:
                endpoint_display = summary
            else:
                endpoint_display = endpoint

            model = str(session.get("model", "")).strip() or "-"
            client_label = str(session.get("client_label", "")).strip() or "-"
            input_display = self._active_session_overview_text(session)

            self.active_tree.insert(
                "", "end",
                values=(status_text, endpoint_display, model, client_label, input_display),
                tags=(tag,),
            )

    def _format_active_sessions(self, sessions: Any) -> str:
        normalized = self._merge_active_sessions(sessions)
        if not normalized:
            return self._t("active_none")

        blocks = [self._t("active_count_label", count=len(normalized))]
        limit = 4
        for index, session in enumerate(normalized[:limit], start=1):
            blocks.append(f"[{index}] {self._format_active_session(session)}")
        remaining = len(normalized) - limit
        if remaining > 0:
            blocks.append(self._t("active_more_label", count=remaining))
        return "\n\n".join(blocks)

    def _request_summary_is_usable(self, value: Any) -> bool:
        normalized = re.sub(r"\s+", " ", str(value or "")).strip()
        if not normalized or normalized == "-":
            return False

        lowered = normalized.lower()
        if lowered.startswith("<system-reminder"):
            return False
        if lowered.startswith("contents of "):
            return False
        if lowered.startswith("gitstatus:") or lowered.startswith("git status:"):
            return False
        if lowered.startswith("recent commits:") or lowered.startswith("recent changes:"):
            return False
        if lowered.startswith("<environment_context"):
            return False
        if re.fullmatch(r"</?[^>]+>", normalized):
            return False
        return True

    def _humanize_recovery_reason(self, reason: str) -> str:
        normalized = str(reason or "").strip()
        if not normalized:
            return ""

        lowered = normalized.lower()
        if "no available channel" in lowered or "model_not_found" in lowered:
            return self._t("active_reason_station_no_channel")
        if "http 503" in lowered or "upstream unavailable" in lowered:
            return self._t("active_reason_station_503")
        if "http 502" in lowered or "bad gateway" in lowered:
            return self._t("active_reason_station_502")
        if "stall after" in lowered or "timeout" in lowered:
            return self._t("active_reason_station_timeout")
        return normalized

    def _humanize_active_status_reason(self, session: Any) -> str:
        if not isinstance(session, dict):
            return ""

        status = str(session.get("status", "")).strip()
        if status == "recovering":
            return self._humanize_recovery_reason(str(session.get("reason", "")).strip())
        if status == "tool_result_timeout":
            return self._t("active_reason_tool_timeout")
        if status == "waiting_tool_result":
            return self._t("active_reason_waiting_tool")
        return str(session.get("reason", "")).strip()

    def _merge_active_sessions(self, sessions: Any, pending_tool_waits: Any = None) -> list[dict[str, Any]]:
        def _coerce_items(value: Any, fallback_item: Any = None) -> list[dict[str, Any]]:
            items = value
            if not isinstance(items, list):
                items = [fallback_item] if isinstance(fallback_item, dict) else []
            return [dict(item) for item in items if isinstance(item, dict)]

        active_items = _coerce_items(
            sessions,
            sessions if isinstance(sessions, dict) else None,
        )
        pending_items = _coerce_items(
            pending_tool_waits,
            pending_tool_waits if isinstance(pending_tool_waits, dict) else None,
        )

        def _session_key(item: dict[str, Any]) -> str:
            session_id = str(item.get("session_id", "")).strip()
            if session_id:
                return session_id
            return "|".join(
                [
                    str(item.get("endpoint", "")).strip(),
                    str(item.get("response_id", "")).strip(),
                    str(item.get("message_id", "")).strip(),
                    str(item.get("started_at", "")).strip(),
                    str(item.get("request_summary", "")).strip(),
                ]
            )

        merged: dict[str, dict[str, Any]] = {}
        for session in active_items:
            merged[_session_key(session)] = session
        for pending in pending_items:
            key = _session_key(pending)
            existing = merged.get(key)
            if existing is None:
                merged[key] = pending
            else:
                merged[key] = {**existing, **pending}

        normalized = [
            session
            for session in merged.values()
            if str(session.get("status", "")).strip() in {
                "running",
                "recovering",
                "waiting_tool_result",
                "tool_result_timeout",
            }
        ]
        normalized.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
        return normalized

    def _active_session_player_hint(self, session: Any, *, reason: str = "") -> str:
        if not isinstance(session, dict):
            return ""

        status = str(session.get("status", "")).strip()
        if status == "tool_result_timeout":
            return self._t("active_hint_tool_timeout")
        if status != "recovering":
            return ""

        try:
            recovery_attempt = int(session.get("recovery_attempt", 0) or 0)
        except (TypeError, ValueError):
            recovery_attempt = 0
        if recovery_attempt < 3:
            return ""

        normalized_reason = str(reason or session.get("reason", "")).strip().lower()
        if not normalized_reason:
            return ""

        issue_markers = (
            "no available channel",
            "model_not_found",
            "http 503",
            "http 502",
            "bad gateway",
            "upstream unavailable",
            "proxy_error",
            "stall after",
            "timeout",
            "connection",
        )
        if any(marker in normalized_reason for marker in issue_markers):
            return self._t("active_hint_station_issue")
        return ""

    def _active_session_overview_text(self, session: Any) -> str:
        if not isinstance(session, dict):
            return "-"

        status = str(session.get("status", "")).strip()
        reason_display = self._humanize_active_status_reason(session)
        player_hint = self._active_session_player_hint(
            session,
            reason=str(session.get("reason", "")).strip(),
        )
        if player_hint:
            return player_hint
        if status in {"recovering", "waiting_tool_result", "tool_result_timeout"} and reason_display:
            return reason_display
        last_user_preview = str(session.get("last_user_text_preview", "")).strip()
        return last_user_preview or "-"

    def _format_active_session(self, session: Any) -> str:
        if not isinstance(session, dict):
            return self._t("active_none")

        status = str(session.get("status", "")).strip()
        if status not in {"running", "recovering", "waiting_tool_result", "tool_result_timeout"}:
            return self._t("active_none")

        endpoint = self._endpoint_label(str(session.get("endpoint_label", "")).strip())
        if endpoint == self._t("event_state_default"):
            endpoint = self._endpoint_label(str(session.get("endpoint", "")).strip())
        updated_at = str(session.get("updated_at", "")).strip() or "-"
        last_user_preview = str(session.get("last_user_text_preview", "")).strip()
        request_summary = str(session.get("request_summary", "")).strip()
        summary = request_summary if self._request_summary_is_usable(request_summary) else ""
        if not summary and self._request_summary_is_usable(last_user_preview):
            summary = last_user_preview
        if not summary:
            summary = "-"
        model = str(session.get("model", "")).strip() or "-"
        client_label = str(session.get("client_label", "")).strip()
        tool_names = session.get("tool_names")
        if isinstance(tool_names, list):
            tool_label = ", ".join(str(item).strip() for item in tool_names if str(item).strip())
        else:
            tool_label = ""
        try:
            partial_chars = int(session.get("partial_chars", 0) or 0)
        except (TypeError, ValueError):
            partial_chars = 0
        try:
            wait_seconds = int(session.get("wait_seconds", 0) or 0)
        except (TypeError, ValueError):
            wait_seconds = 0
        try:
            recovery_attempt = int(session.get("recovery_attempt", 0) or 0)
        except (TypeError, ValueError):
            recovery_attempt = 0
        reason = str(session.get("reason", "")).strip()
        reason_display = self._humanize_active_status_reason(session)
        player_hint = self._active_session_player_hint(session, reason=reason)

        if status == "recovering":
            headline = self._t(
                "active_recovering",
                endpoint=endpoint,
                updated_at=updated_at,
                attempt=max(1, recovery_attempt),
            )
        elif status == "running":
            headline = self._t("active_running", endpoint=endpoint, updated_at=updated_at)
        elif status == "tool_result_timeout":
            headline = self._t(
                "active_tool_timeout",
                endpoint=endpoint,
                updated_at=updated_at,
                seconds=wait_seconds,
            )
        else:
            headline = self._t("active_waiting_tool", endpoint=endpoint, updated_at=updated_at)

        lines = [
            headline,
            self._t("active_summary_label", summary=summary),
            self._t("active_model_label", model=model),
        ]
        if partial_chars > 0:
            lines.append(self._t("active_partial_chars_label", chars=partial_chars))
        if client_label:
            lines.append(self._t("active_client_label", client=client_label))
        if player_hint:
            lines.append(self._t("active_hint_label", hint=player_hint))
        if reason_display:
            lines.append(self._t("active_reason_label", reason=reason_display))
        if tool_label:
            lines.append(self._t("active_tools_label", tools=tool_label))
        if status != "running" and wait_seconds > 0:
            lines.append(self._t("active_wait_age_label", seconds=wait_seconds))

        if last_user_preview and last_user_preview != summary:
            lines.append(self._t("active_last_user_label", preview=last_user_preview))

        partial_preview = str(session.get("partial_text_preview", "")).strip()
        if partial_preview:
            lines.append(self._t("active_partial_label", preview=partial_preview))

        return "\n".join(lines)

    def _event_state_label(self, kind: str) -> str:
        return {
            "disconnect": self._t("event_state_disconnect"),
            "recovered": self._t("event_state_recovered"),
            "failed": self._t("event_state_failed"),
            "service_start": self._t("event_state_service_start"),
            "service_stop": self._t("event_state_service_stop"),
            "tool_wait": self._t("event_state_tool_wait"),
            "tool_resumed": self._t("event_state_tool_resumed"),
            "tool_wait_timeout": self._t("event_state_tool_wait_timeout"),
        }.get(kind, self._t("event_state_default"))

    def _event_summary(self, event: dict[str, Any]) -> str:
        kind = str(event.get("kind", ""))
        meta = event.get("meta", {})
        if not isinstance(meta, dict):
            meta = {}

        if kind == "service_start":
            listen = str(meta.get("listen", "")).strip() or self.base_url
            upstream = str(meta.get("upstream", "")).strip() or self._t("upstream_waiting")
            source = self._source_label(str(meta.get("upstream_source", "")).strip())
            return self._t("event_service_start", listen=listen, upstream=upstream, source=source)

        if kind == "service_stop":
            return self._t("event_service_stop")

        endpoint = self._endpoint_label(str(event.get("endpoint_label", "")))
        reason = str(meta.get("reason", "")).strip() or str(event.get("detail", "")).strip()

        if kind == "disconnect":
            summary = self._t(
                "event_disconnect",
                endpoint=endpoint,
                attempt=meta.get("attempt", "?"),
                reason=reason,
            )
            buffered = int(meta.get("buffered_chars", 0) or 0)
            if buffered > 0:
                summary += f" | {self._t('event_buffered', chars=buffered)}"
            return self._append_request_summary(summary, meta)

        if kind == "recovered":
            attempt = int(meta.get("attempt", 0) or 0)
            mode = self._mode_label(str(meta.get("mode", "")).strip())
            if attempt > 0:
                summary = self._t(
                    "event_recovered_with_retry",
                    endpoint=endpoint,
                    attempt=attempt,
                    mode=mode,
                )
            else:
                summary = self._t("event_recovered_no_retry", endpoint=endpoint, mode=mode)
            delivered = int(meta.get("delivered_chars", 0) or 0)
            if delivered > 0:
                summary += f" | {self._t('event_delivered', chars=delivered)}"
            return self._append_request_summary(summary, meta)

        if kind == "failed":
            summary = self._t("event_failed", endpoint=endpoint, reason=reason)
            buffered = int(meta.get("buffered_chars", 0) or 0)
            if buffered > 0:
                summary += f" | {self._t('event_buffered', chars=buffered)}"
            return self._append_request_summary(summary, meta)

        if kind == "tool_wait":
            tools = meta.get("tool_names")
            if isinstance(tools, list):
                tool_label = ", ".join(str(item).strip() for item in tools if str(item).strip())
            else:
                tool_label = ""
            tool_label = tool_label or self._t("event_tools_unknown")
            summary = self._t("event_tool_wait", endpoint=endpoint, tools=tool_label)
            return self._append_request_summary(summary, meta)

        if kind == "tool_resumed":
            wait_seconds = int(meta.get("wait_seconds", 0) or 0)
            summary = self._t("event_tool_resumed", endpoint=endpoint, seconds=wait_seconds)
            return self._append_request_summary(summary, meta)

        if kind == "tool_wait_timeout":
            wait_seconds = int(meta.get("wait_seconds", 0) or 0)
            summary = self._t("event_tool_wait_timeout", endpoint=endpoint, seconds=wait_seconds)
            return self._append_request_summary(summary, meta)

        return str(event.get("detail", "")).strip() or str(event.get("title", "")).strip()

    def _append_request_summary(self, summary: str, meta: dict[str, Any]) -> str:
        request_summary = str(meta.get("request_summary", "")).strip()
        if not self._request_summary_is_usable(request_summary):
            return summary
        return f"{summary} | {self._t('event_request_summary', summary=request_summary)}"

    def _source_label(self, source: str) -> str:
        key = f"source_{source}" if source else "source_unknown"
        fallback = self._t("source_unknown")
        return TRANSLATIONS.get(self.language, TRANSLATIONS["zh_CN"]).get(key, fallback)

    def _mode_label(self, mode: str) -> str:
        key = {
            "stream retry": "event_mode_stream_retry",
            "tail fetch": "event_mode_tail_fetch",
        }.get(mode, "event_mode_unknown")
        return self._t(key)

    def _endpoint_label(self, endpoint_label: str) -> str:
        key = {
            "messages": "endpoint_messages",
            "responses": "endpoint_responses",
        }.get(endpoint_label, "")
        return self._t(key) if key else endpoint_label or "api"

    def _fetch_health(self) -> dict[str, Any] | None:
        with suppress(HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError):
            return self._request_json("/health", timeout=1.0)
        return None

    def _extract_started_at_ms(self, health: dict[str, Any] | None) -> int | None:
        if not isinstance(health, dict):
            return None
        try:
            started_at_ms = int(health.get("started_at_ms"))
        except Exception:
            return None
        return started_at_ms if started_at_ms > 0 else None

    def _wait_for_proxy_exit(
        self,
        expected_started_at_ms: int | None,
        expected_pid: int | None,
        timeout: float,
    ) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.process and self.process.poll() is not None:
                self.process = None

            health = self._fetch_health()
            current_started_at_ms = self._extract_started_at_ms(health)
            expected_pid_state = _managed_proxy_process_state(expected_pid) if expected_pid else "dead"
            expected_pid_alive = expected_pid_state in {"managed", "unknown"}
            if expected_started_at_ms is not None and health is not None:
                if current_started_at_ms != expected_started_at_ms and not expected_pid_alive:
                    return True

            if health is None and self.process is None and not expected_pid_alive:
                return True

            if self.process and self.process.poll() is None:
                with suppress(Exception):
                    self.process.wait(timeout=0.15)
                if self.process.poll() is not None:
                    self.process = None
                    if self._fetch_health() is None and not expected_pid_alive:
                        return True

            time.sleep(0.15)

        return self._fetch_health() is None and not bool(
            expected_pid and _managed_proxy_process_state(expected_pid) in {"managed", "unknown"}
        ) and (
            self.process is None or self.process.poll() is not None
        )

    def _terminate_tracked_process(self, wait_timeout: float) -> None:
        if not self.process:
            return
        if self.process.poll() is not None:
            self.process = None
            return
        with suppress(Exception):
            self.process.terminate()
        with suppress(Exception):
            self.process.wait(timeout=wait_timeout)
        if self.process.poll() is not None:
            self.process = None

    def _is_previous_instance_health(self, health: dict[str, Any] | None) -> bool:
        previous_started_at_ms = getattr(self, "pending_start_previous_started_at_ms", None)
        previous_pid = getattr(self, "pending_start_previous_pid", None)
        current_started_at_ms = self._extract_started_at_ms(health)
        if previous_started_at_ms is not None and current_started_at_ms is not None:
            return current_started_at_ms == previous_started_at_ms
        current_pid = _extract_positive_int((health or {}).get("pid"))
        return bool(previous_pid and current_pid and current_pid == previous_pid)

    def _request_json(self, path: str, *, method: str = "GET", timeout: float = 1.2, body: dict[str, Any] | None = None) -> dict[str, Any]:
        request = Request(f"{self.base_url}{path}", method=method)
        if body is not None:
            request.add_header("Content-Type", "application/json")
            request.data = json.dumps(body).encode("utf-8")
        # Local health/control requests must never follow system proxy settings.
        with LOCAL_OPENER.open(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("Unexpected payload shape")
        return data

    def _on_close(self) -> None:
        self.closed = True
        if self.after_id is not None:
            self.root.after_cancel(self.after_id)
            self.after_id = None
        self.root.destroy()


def main(args: argparse.Namespace | None = None) -> None:
    # Enable DPI awareness on Windows to fix blurry text
    with suppress(Exception):
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)  # type: ignore[union-attr]

    if args is None:
        args = build_arg_parser().parse_args()

    _write_gui_state()
    try:
        root = tk.Tk()
        root._proxy_dashboard = ProxyDashboard(root, args)  # type: ignore[attr-defined]
        _show_root_window(root)
        root.mainloop()
    finally:
        _clear_gui_state(os.getpid())


if __name__ == "__main__":
    main()
