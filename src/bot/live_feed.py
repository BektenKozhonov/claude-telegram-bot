"""Terminal-like live feed of Claude's work in Telegram.

``LiveFormatter`` turns SDK stream updates (reasoning, tool calls, tool results)
into HTML chunks; ``LiveFeed`` delivers them as separate messages, batching
chunks so Telegram rate limits are respected.
"""

import asyncio
import html
import re
import time
from typing import Any, Callable, Dict, List, Optional

import structlog
from telegram.error import BadRequest, RetryAfter

logger = structlog.get_logger()

_MESSAGE_LIMIT = 3800
_ICONS = {
    "Read": "\U0001f4d6",
    "Write": "✍️",
    "Edit": "✏️",
    "MultiEdit": "✏️",
    "NotebookEdit": "✏️",
    "Bash": "\U0001f4bb",
    "Grep": "\U0001f50d",
    "Glob": "\U0001f4c2",
    "LS": "\U0001f4c2",
    "WebFetch": "\U0001f310",
    "WebSearch": "\U0001f310",
    "Task": "\U0001f916",
    "TodoWrite": "\U0001f4dd",
}


def _esc(text: str) -> str:
    return html.escape(text, quote=False)


def _cut(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _diff_block(old: str, new: str, limit: int = 1400) -> str:
    """Compact -/+ diff for an Edit call, as a highlighted code block."""
    lines = [f"- {_cut(line, 160)}" for line in old.splitlines()]
    lines += [f"+ {_cut(line, 160)}" for line in new.splitlines()]
    body = "\n".join(lines)
    if len(body) > limit:
        body = body[:limit].rsplit("\n", 1)[0] + "\n…"
    return f'<pre><code class="language-diff">{_esc(body)}</code></pre>'


class LiveFormatter:
    """Stateful converter of stream updates into display chunks."""

    def __init__(self, redact: Callable[[str], str] = lambda s: s) -> None:
        self._redact = redact
        self._names: Dict[str, str] = {}
        # Last assistant text: commentary if tools follow, else the final answer
        # (which the bot sends itself, so it is not repeated in the feed).
        self._pending_text = ""

    def events(self, upd: Any) -> List[str]:
        out: List[str] = []
        md = upd.metadata or {}
        if upd.type == "assistant":
            thinking = (md.get("thinking") or "").strip()
            text = upd.content.strip() if isinstance(upd.content, str) else ""
            if self._pending_text and (thinking or upd.tool_calls):
                out.append(self._commentary(self._pending_text))
                self._pending_text = ""
            if thinking:
                out.append(self._thinking(thinking))
            if upd.tool_calls:
                if text:
                    out.append(self._commentary(text))
                for tc in upd.tool_calls:
                    if tc.get("id"):
                        self._names[tc["id"]] = tc.get("name", "")
                    chunk = self._tool(tc)
                    if chunk:
                        out.append(chunk)
            elif text:
                if self._pending_text:
                    out.append(self._commentary(self._pending_text))
                self._pending_text = text
        elif upd.type == "user":
            for res in md.get("tool_results", []):
                chunk = self._result(res)
                if chunk:
                    out.append(chunk)
        return out

    @staticmethod
    def _commentary(text: str) -> str:
        return f"\U0001f4ac {_esc(_cut(text, 1500))}"

    @staticmethod
    def _thinking(text: str) -> str:
        return (
            "\U0001f4ad <i>Рассуждение</i>\n"
            f"<blockquote expandable>{_esc(_cut(text, 1800))}</blockquote>"
        )

    def _tool(self, tc: Dict[str, Any]) -> str:
        name = tc.get("name", "unknown")
        inp = tc.get("input") or {}
        icon = _ICONS.get(name, "\U0001f527")
        path = inp.get("file_path") or inp.get("path") or inp.get("notebook_path")
        head = f"{icon} <b>{_esc(name)}</b>"

        if name == "Read" and path:
            extra = ""
            if inp.get("offset") or inp.get("limit"):
                extra = f" · строки {inp.get('offset', 1)}+{inp.get('limit', '…')}"
            return f"{head} <code>{_esc(str(path))}</code>{extra}"
        if name == "Write" and path:
            content = str(inp.get("content", ""))
            lines = content.count("\n") + 1 if content else 0
            preview = "\n".join(content.splitlines()[:12])
            block = (
                f"<pre>{_esc(_cut(self._redact(preview), 900))}</pre>" if preview else ""
            )
            return f"{head} <code>{_esc(str(path))}</code> · {lines} строк\n{block}"
        if name == "Edit" and path:
            diff = _diff_block(
                self._redact(str(inp.get("old_string", ""))),
                self._redact(str(inp.get("new_string", ""))),
            )
            return f"{head} <code>{_esc(str(path))}</code>\n{diff}"
        if name == "MultiEdit" and path:
            edits = inp.get("edits") or []
            diffs = "\n".join(
                _diff_block(
                    self._redact(str(e.get("old_string", ""))),
                    self._redact(str(e.get("new_string", ""))),
                    600,
                )
                for e in edits[:3]
                if isinstance(e, dict)
            )
            more = f"\n… ещё правок: {len(edits) - 3}" if len(edits) > 3 else ""
            return (
                f"{head} <code>{_esc(str(path))}</code> · правок: {len(edits)}"
                f"\n{diffs}{more}"
            )
        if name == "Bash":
            cmd = self._redact(str(inp.get("command", "")))
            desc = inp.get("description")
            note = f"\n<i>{_esc(_cut(str(desc), 160))}</i>" if desc else ""
            return f"{head}\n<pre>{_esc(_cut(cmd, 700))}</pre>{note}"
        if name in ("Grep", "Glob"):
            pat = _esc(_cut(str(inp.get("pattern", "")), 120))
            where = f" в <code>{_esc(str(path))}</code>" if path else ""
            return f"{head} <code>{pat}</code>{where}"
        if name in ("WebFetch", "WebSearch"):
            target = inp.get("url") or inp.get("query") or ""
            return f"{head} {_esc(_cut(str(target), 160))}"
        if name == "Task":
            return f"{head} {_esc(_cut(str(inp.get('description', '')), 160))}"
        if name == "TodoWrite":
            marks = {"completed": "✅", "in_progress": "▶️", "pending": "⬜"}
            todos = inp.get("todos") or []
            rows = [
                f"{marks.get(t.get('status'), '⬜')} {_esc(_cut(str(t.get('content', '')), 100))}"
                for t in todos
                if isinstance(t, dict)
            ]
            return head + ("\n" + "\n".join(rows) if rows else "")
        detail = path or next((v for v in inp.values() if isinstance(v, str) and v), "")
        return f"{head} {_esc(_cut(str(detail), 160))}".rstrip()

    def _result(self, res: Dict[str, Any]) -> str:
        name = self._names.get(res.get("id", ""), "")
        text = (res.get("text") or "").strip()
        if res.get("is_error"):
            return f"❌ <b>{_esc(name or 'Ошибка')}</b>: <code>{_esc(_cut(self._redact(text), 400))}</code>"
        if name == "Bash" and text:
            return f"<pre>{_esc(_cut(self._redact(text), 800))}</pre>"
        return ""


class LiveFeed:
    """Sends chunks as messages, at most one message per ``min_interval``."""

    def __init__(self, message: Any, min_interval: float = 1.5) -> None:
        self._message = message
        self._min_interval = min_interval
        self._buffer: List[str] = []
        self._task: Optional["asyncio.Task[None]"] = None
        self._lock = asyncio.Lock()
        self._last = 0.0

    def add(self, chunk: str) -> None:
        self._buffer.append(chunk)
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while self._buffer:
            wait = self._last + self._min_interval - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            await self._flush()

    async def _flush(self) -> None:
        async with self._lock:
            parts: List[str] = []
            size = 0
            while self._buffer and size + len(self._buffer[0]) + 2 <= _MESSAGE_LIMIT:
                chunk = self._buffer.pop(0)
                parts.append(chunk)
                size += len(chunk) + 2
            if not parts and self._buffer:
                parts.append(self._buffer.pop(0)[:_MESSAGE_LIMIT])
            if not parts:
                return
            await self._send("\n\n".join(parts))
            self._last = time.monotonic()

    async def _send(self, text: str) -> None:
        for _ in range(3):
            try:
                await self._message.reply_text(
                    text, parse_mode="HTML", disable_notification=True
                )
                return
            except RetryAfter as e:
                await asyncio.sleep(float(e.retry_after) + 1)
            except BadRequest:
                plain = html.unescape(re.sub(r"<[^>]+>", "", text))
                try:
                    await self._message.reply_text(
                        plain[:_MESSAGE_LIMIT], disable_notification=True
                    )
                except Exception:
                    pass
                return
            except Exception as e:
                logger.warning("Live feed send failed", error=str(e))
                return

    async def close(self) -> None:
        """Deliver everything still buffered."""
        if self._task is not None and not self._task.done():
            try:
                await self._task
            except Exception:
                pass
        while self._buffer:
            await self._flush()
