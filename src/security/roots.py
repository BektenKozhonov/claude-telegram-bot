"""Drive-level access boundary.

The bot may work in any folder on drives D: and beyond (the system drive C: and
its user profile, which holds Claude credentials, stay off limits).
"""

import ctypes
import os
import string
from pathlib import Path
from typing import List

_FIRST_ALLOWED_LETTER = "D"


def allowed_drives() -> List[Path]:
    """Existing drive roots from D: upward (D:\\, E:\\, ...)."""
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
    """True if path lies on a drive from D: upward (always False off Windows)."""
    if os.name != "nt":
        return False
    drive = path.drive[:1].upper()
    return bool(drive) and drive >= _FIRST_ALLOWED_LETTER and drive.isalpha()
