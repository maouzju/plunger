from pathlib import Path

APP_NAME = "Plunger"
PACKAGE_NAME = "plunger-proxy"
APP_VERSION = "0.2.0"
GITHUB_REPOSITORY = "maouzju/plunger"
GITHUB_RELEASES_API_URL = f"https://api.github.com/repos/{GITHUB_REPOSITORY}/releases/latest"
GITHUB_RELEASES_PAGE_URL = f"https://github.com/{GITHUB_REPOSITORY}/releases/latest"
PREFERRED_WINDOWS_RELEASE_ASSETS = ("Plunger-windows.zip", "Plunger.exe")

LOCAL_HOST = "127.0.0.1"
DEFAULT_PORT = 8462
DEFAULT_STALL_TIMEOUT = 60.0

CLAUDE_DIR = Path.home() / ".claude"
RECOVERY_DIR = CLAUDE_DIR / "plunger"
