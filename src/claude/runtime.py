"""Runtime state shared by the Telegram layer and the Claude SDK layer.

- ``settings()``: persistent user settings in ``data/settings.json``
  (permission mode, model, output toggles, tools allowed "always").
- ``permission_requesters``: per-user coroutine asking the human in Telegram.
- ``active_clients``: per-user running SDK client, used by /abort.
"""

import json
import os
import threading
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

import structlog

logger = structlog.get_logger()

# Permission modes offered in Telegram: value -> (emoji, label)
MODES: Dict[str, tuple] = {  # type: ignore[type-arg]
    "default": ("\U0001f6e0", "Обычный"),
    "plan": ("\U0001f4cb", "План"),
    "acceptEdits": ("✏️", "Авто-правки"),
    "bypassPermissions": ("🔓", "Всё разрешено"),
}

# Tools that never need confirmation (read-only)
SAFE_TOOLS = {
    "Read",
    "Glob",
    "Grep",
    "LS",
    "NotebookRead",
    "TodoRead",
    "TodoWrite",
    "WebSearch",
    "Skill",
    "Task",
    "Agent",
    "TaskOutput",
    "ToolSearch",
    "ListMcpResourcesTool",
    "ReadMcpResourceTool",
}

DEFAULTS: Dict[str, Any] = {
    "mode": "default",
    "model": "",
    "compact_output": True,
    "live_feed": True,
    "send_files": True,
    "footer": False,
    "queue": True,
    "always_allowed_tools": [],
}


class UserSettings:
    """Small JSON settings store with atomic writes and a .bak copy."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._data: Dict[str, Any] = dict(DEFAULTS)
        self._load()

    def _load(self) -> None:
        for candidate in (self.path, self.path.with_suffix(".json.bak")):
            try:
                loaded = json.loads(candidate.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    self._data.update(loaded)
                    return
            except (OSError, ValueError):
                continue
        # First run: migrate the model chosen with the old /model command
        old_model = self.path.parent / "model.txt"
        try:
            self._data["model"] = old_model.read_text(encoding="utf-8").strip()
        except OSError:
            pass

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        if self.path.exists():
            try:
                self.path.replace(self.path.with_suffix(".json.bak"))
            except OSError:
                pass
        os.replace(tmp, self.path)

    def get(self, key: str) -> Any:
        with self._lock:
            return self._data.get(key, DEFAULTS.get(key))

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value
            self._save()

    def toggle(self, key: str) -> bool:
        value = not bool(self.get(key))
        self.set(key, value)
        return value

    def always_allowed(self) -> List[str]:
        return list(self.get("always_allowed_tools") or [])

    def add_always_allowed(self, tool: str) -> None:
        tools = self.always_allowed()
        if tool not in tools:
            tools.append(tool)
            self.set("always_allowed_tools", tools)


_settings: Optional[UserSettings] = None


def settings() -> UserSettings:
    global _settings
    if _settings is None:
        _settings = UserSettings(Path.cwd() / "data" / "settings.json")
    return _settings


# user_id -> async (tool_name, tool_input) -> "once" | "always" | "reject"
PermissionRequester = Callable[[str, Dict[str, Any]], Awaitable[str]]
permission_requesters: Dict[int, PermissionRequester] = {}

# user_id -> async (questions) -> {question text: answer} | None (AskUserQuestion)
QuestionAsker = Callable[[List[Dict[str, Any]]], Awaitable[Optional[Dict[str, str]]]]
question_askers: Dict[int, QuestionAsker] = {}

# user_id -> running ClaudeSDKClient (for /abort)
active_clients: Dict[int, Any] = {}
