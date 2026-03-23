from pathlib import Path

LOCAL_HOST = "127.0.0.1"
DEFAULT_PORT = 8462
DEFAULT_STALL_TIMEOUT = 60.0

CLAUDE_DIR = Path.home() / ".claude"
RECOVERY_DIR = CLAUDE_DIR / "plunger"
