"""Drive-level access boundary.

The bot may work in any folder on any local drive, C: included.
"""

import ctypes
import os
import string
from pathlib import Path
from typing import List

_FIRST_ALLOWED_LETTER = "C"


def allowed_drives() -> List[Path]:
    """Existing drive roots from C: upward (C:\\, D:\\, ...)."""
    if os.name != "nt":
        return []
    try:
        mask = ctypes.windll.kernel32.GetLogicalDrives()  # type: ignore[attr-defined]
    except Exception:
        return []
    drives = []
    for i, letter in enumerate(string.ascii_uppercase):
        if letter >= _FIRST_ALLOWED_LETTER and mask & (1 << i):
            drives.append(Path(f"{letter}:\\"))
    return drives


def is_allowed_path(path: Path) -> bool:
    """True if path lies on a drive from C: upward (always False off Windows)."""
    if os.name != "nt":
        return False
    drive = path.drive[:1].upper()
    return bool(drive) and drive >= _FIRST_ALLOWED_LETTER and drive.isalpha()
