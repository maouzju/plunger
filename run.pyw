#!/usr/bin/env python3
"""Windows GUI entrypoint without an attached console window."""

from __future__ import annotations

import traceback
from pathlib import Path

from run import main

ERROR_LOG_FILE = Path(__file__).with_name("runpyw-error.log")


def _record_startup_exception() -> None:
    message = traceback.format_exc()
    try:
        ERROR_LOG_FILE.write_text(message, encoding="utf-8")
    except Exception:
        pass

    try:
        import tkinter as tk
        from tkinter import messagebox

        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "Plunger",
            "Failed to open the desktop UI.\n\n"
            f"Details were written to:\n{ERROR_LOG_FILE}",
        )
        root.destroy()
    except Exception:
        pass


if __name__ == "__main__":
    try:
        main()
    except Exception:
        _record_startup_exception()
        raise
