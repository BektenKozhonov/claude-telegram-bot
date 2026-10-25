"""Message orchestrator — single entry point for all Telegram updates.

Routes messages based on agentic vs classic mode. In agentic mode, provides
a minimal conversational interface (3 commands, no inline keyboards). In
classic mode, delegates to existing full-featured handlers.
"""

import asyncio
import re
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import structlog
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from ..claude import runtime
from ..claude.exceptions import ClaudeToolValidationError
from ..claude.sdk_integration import StreamUpdate
from ..config.settings import Settings
from ..projects import PrivateTopicsUnavailableError
from .live_feed import LiveFeed, LiveFormatter
from .utils.html_format import escape_html

logger = structlog.get_logger()

# Patterns that look like secrets/credentials in CLI arguments
_SECRET_PATTERNS: List[re.Pattern[str]] = [
    # API keys / tokens (sk-ant-..., sk-..., ghp_..., gho_..., github_pat_..., xoxb-...)
    re.compile(
        r"(sk-ant-api\d*-[A-Za-z0-9_-]{10})[A-Za-z0-9_-]*"
        r"|(sk-[A-Za-z0-9_-]{20})[A-Za-z0-9_-]*"
        r"|(ghp_[A-Za-z0-9]{5})[A-Za-z0-9]*"
        r"|(gho_[A-Za-z0-9]{5})[A-Za-z0-9]*"
        r"|(github_pat_[A-Za-z0-9_]{5})[A-Za-z0-9_]*"
        r"|(xoxb-[A-Za-z0-9]{5})[A-Za-z0-9-]*"
    ),
    # AWS access keys
    re.compile(r"(AKIA[0-9A-Z]{4})[0-9A-Z]{12}"),
    # Generic long hex/base64 tokens after common flags/env patterns
    re.compile(
        r"((?:--token|--secret|--password|--api-key|--apikey|--auth)"
        r"[= ]+)['\"]?[A-Za-z0-9+/_.:-]{8,}['\"]?"
    ),
    # Inline env assignments like KEY=value
    re.compile(
        r"((?:TOKEN|SECRET|PASSWORD|API_KEY|APIKEY|AUTH_TOKEN|PRIVATE_KEY"
        r"|ACCESS_KEY|CLIENT_SECRET|WEBHOOK_SECRET)"
        r"=)['\"]?[^\s'\"]{8,}['\"]?"
    ),
    # Bearer / Basic auth headers
    re.compile(r"(Bearer )[A-Za-z0-9+/_.:-]{8,}" r"|(Basic )[A-Za-z0-9+/=]{8,}"),
    # Connection strings with credentials  user:pass@host
    re.compile(r"://([^:]+:)[^@]{4,}(@)"),
]


def _redact_secrets(text: str) -> str:
    """Replace likely secrets/credentials with redacted placeholders."""
    result = text
    for pattern in _SECRET_PATTERNS:
        result = pattern.sub(
            lambda m: next((g + "***" for g in m.groups() if g is not None), "***"),
            result,
        )
    return result


# Tool name -> friendly emoji mapping for verbose output
_TOOL_ICONS: Dict[str, str] = {
    "Read": "\U0001f4d6",
    "Write": "\u270f\ufe0f",
    "Edit": "\u270f\ufe0f",
    "MultiEdit": "\u270f\ufe0f",
    "Bash": "\U0001f4bb",
    "Glob": "\U0001f50d",
    "Grep": "\U0001f50d",
    "LS": "\U0001f4c2",
    "Task": "\U0001f9e0",
    "TaskOutput": "\U0001f9e0",
    "WebFetch": "\U0001f310",
    "WebSearch": "\U0001f310",
    "NotebookRead": "\U0001f4d3",
    "NotebookEdit": "\U0001f4d3",
    "TodoRead": "\u2611\ufe0f",
    "TodoWrite": "\u2611\ufe0f",
}


def _tool_icon(name: str) -> str:
    """Return emoji for a tool, with a default wrench."""
    return _TOOL_ICONS.get(name, "\U0001f527")


class MessageOrchestrator:
    """Routes messages based on mode. Single entry point for all Telegram updates."""

    # Bottom-keyboard buttons: "<emoji> <mode label>" and "🧠 <model>"
    # Telegram clients may append U+FE0F to emoji, so tolerate it and spaces
    _MODE_BUTTON_RE = (
        r"^\s*(?:\U0001f6e0|\U0001f4cb|✏)️?\s*(?:Обычный|План|Авто-правки)\s*$"
    )
    _MODEL_BUTTON_RE = r"^\s*\U0001f9e0️?\s*.+$"
    _DIR_BUTTON_RE = r"^\s*\U0001f4c1️?\s*Папка\s*$"
    _QUEUE_LIMIT = 5
    _PERMISSION_TIMEOUT = 600

    def __init__(self, settings: Settings, deps: Dict[str, Any]):
        self.settings = settings
        self.deps = deps
        self._user_locks: Dict[int, asyncio.Lock] = {}
        self._queues: Dict[int, List[Tuple[Update, Any]]] = {}
        # request id -> (user_id, future resolved with once/always/reject)
        self._pending_perms: Dict[str, Tuple[int, "asyncio.Future[str]"]] = {}

    def _inject_deps(self, handler: Callable) -> Callable:  # type: ignore[type-arg]
        """Wrap handler to inject dependencies into context.bot_data."""

        async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            for key, value in self.deps.items():
                context.bot_data[key] = value
            context.bot_data["settings"] = self.settings
            context.user_data.pop("_thread_context", None)

            is_sync_bypass = handler.__name__ == "sync_threads"
            is_start_bypass = handler.__name__ in {"start_command", "agentic_start"}
            message_thread_id = self._extract_message_thread_id(update)
            should_enforce = self.settings.enable_project_threads

            if should_enforce:
                if self.settings.project_threads_mode == "private":
                    should_enforce = not is_sync_bypass and not (
                        is_start_bypass and message_thread_id is None
                    )
                else:
                    should_enforce = not is_sync_bypass

            if should_enforce:
                allowed = await self._apply_thread_routing_context(update, context)
                if not allowed:
                    return

            try:
                await handler(update, context)
            finally:
                if should_enforce:
                    self._persist_thread_state(context)

        return wrapped

    async def _apply_thread_routing_context(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> bool:
        """Enforce strict project-thread routing and load thread-local state."""
        manager = context.bot_data.get("project_threads_manager")
        if manager is None:
            await self._reject_for_thread_mode(
                update,
                "❌ <b>Project Thread Mode Misconfigured</b>\n\n"
                "Thread manager is not initialized.",
            )
            return False

        chat = update.effective_chat
        message = update.effective_message
        if not chat or not message:
            return False

        if self.settings.project_threads_mode == "group":
            if chat.id != self.settings.project_threads_chat_id:
                await self._reject_for_thread_mode(
                    update,
                    manager.guidance_message(mode=self.settings.project_threads_mode),
                )
                return False
        else:
            if getattr(chat, "type", "") != "private":
                await self._reject_for_thread_mode(
                    update,
                    manager.guidance_message(mode=self.settings.project_threads_mode),
                )
                return False

        message_thread_id = self._extract_message_thread_id(update)
        if not message_thread_id:
            await self._reject_for_thread_mode(
                update,
                manager.guidance_message(mode=self.settings.project_threads_mode),
            )
            return False

        project = await manager.resolve_project(chat.id, message_thread_id)
        if not project:
            await self._reject_for_thread_mode(
                update,
                manager.guidance_message(mode=self.settings.project_threads_mode),
            )
            return False

        state_key = f"{chat.id}:{message_thread_id}"
        thread_states = context.user_data.setdefault("thread_state", {})
        state = thread_states.get(state_key, {})

        project_root = project.absolute_path
        current_dir_raw = state.get("current_directory")
        current_dir = (
            Path(current_dir_raw).resolve() if current_dir_raw else project_root
        )
        if not self._is_within(current_dir, project_root) or not current_dir.is_dir():
            current_dir = project_root

        context.user_data["current_directory"] = current_dir
        context.user_data["claude_session_id"] = state.get("claude_session_id")
        context.user_data["_thread_context"] = {
            "chat_id": chat.id,
            "message_thread_id": message_thread_id,
            "state_key": state_key,
            "project_slug": project.slug,
            "project_root": str(project_root),
            "project_name": project.name,
        }
        return True

    def _persist_thread_state(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Persist compatibility keys back into per-thread state."""
        thread_context = context.user_data.get("_thread_context")
        if not thread_context:
            return

        project_root = Path(thread_context["project_root"])
        current_dir = context.user_data.get("current_directory", project_root)
        if not isinstance(current_dir, Path):
            current_dir = Path(str(current_dir))
        current_dir = current_dir.resolve()
        if not self._is_within(current_dir, project_root) or not current_dir.is_dir():
            current_dir = project_root

        thread_states = context.user_data.setdefault("thread_state", {})
        thread_states[thread_context["state_key"]] = {
            "current_directory": str(current_dir),
            "claude_session_id": context.user_data.get("claude_session_id"),
            "project_slug": thread_context["project_slug"],
        }

    @staticmethod
    def _is_within(path: Path, root: Path) -> bool:
        """Return True if path is within root."""
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False

    @staticmethod
    def _extract_message_thread_id(update: Update) -> Optional[int]:
        """Extract topic/thread id from update message for forum/direct topics."""
        message = update.effective_message
        if not message:
            return None
        message_thread_id = getattr(message, "message_thread_id", None)
        if isinstance(message_thread_id, int) and message_thread_id > 0:
            return message_thread_id
        dm_topic = getattr(message, "direct_messages_topic", None)
        topic_id = getattr(dm_topic, "topic_id", None) if dm_topic else None
        if isinstance(topic_id, int) and topic_id > 0:
            return topic_id
        return None

    async def _reject_for_thread_mode(self, update: Update, message: str) -> None:
        """Send a guidance response when strict thread routing rejects an update."""
        query = update.callback_query
        if query:
            try:
                await query.answer()
            except Exception:
                pass
            if query.message:
                await query.message.reply_text(message, parse_mode="HTML")
            return

        if update.effective_message:
            await update.effective_message.reply_text(message, parse_mode="HTML")

    def register_handlers(self, app: Application) -> None:
        """Register handlers based on mode."""
        if self.settings.agentic_mode:
            self._register_agentic_handlers(app)
        else:
            self._register_classic_handlers(app)

    def _register_agentic_handlers(self, app: Application) -> None:
        """Register agentic handlers: commands + text/file/photo."""
        from .handlers import command

        # Commands
        handlers = [
            ("start", self.agentic_start),
            ("new", self.agentic_new),
            ("status", self.agentic_status),
            ("verbose", self.agentic_verbose),
            ("repo", self.agentic_repo),
            ("sessions", self.agentic_sessions),
            ("model", self.agentic_model),
            ("cd", self.agentic_repo),
            ("abort", self.agentic_abort),
            ("settings", self.agentic_settings),
        ]
        if self.settings.enable_project_threads:
            handlers.append(("sync_threads", command.sync_threads))

        for cmd, handler in handlers:
            app.add_handler(CommandHandler(cmd, self._inject_deps(handler)))

        # Bottom-keyboard buttons (mode / model) open menus, never reach Claude
        button_re = (
            f"(?:{self._MODE_BUTTON_RE})|(?:{self._MODEL_BUTTON_RE})"
            f"|(?:{self._DIR_BUTTON_RE})"
        )
        app.add_handler(
            MessageHandler(
                filters.Regex(self._DIR_BUTTON_RE),
                self._inject_deps(self.agentic_dir_button),
            ),
            group=5,
        )
        app.add_handler(
            MessageHandler(
                filters.Regex(self._MODE_BUTTON_RE),
                self._inject_deps(self.agentic_mode_button),
            ),
            group=5,
        )
        app.add_handler(
            MessageHandler(
                filters.Regex(self._MODEL_BUTTON_RE),
                self._inject_deps(self.agentic_model_button),
            ),
            group=5,
        )

        # Text messages -> Claude
        app.add_handler(
            MessageHandler(
                filters.TEXT & ~filters.COMMAND & ~filters.Regex(button_re),
                self._inject_deps(self.agentic_text),
            ),
            group=10,
        )

        # File uploads -> Claude
        app.add_handler(
            MessageHandler(
                filters.Document.ALL, self._inject_deps(self.agentic_document)
            ),
            group=10,
        )

        # Photo uploads -> Claude
        app.add_handler(
            MessageHandler(filters.PHOTO, self._inject_deps(self.agentic_photo)),
            group=10,
        )

        # Only cd: callbacks (for project selection), scoped by pattern
        app.add_handler(
            CallbackQueryHandler(
                self._inject_deps(self._agentic_callback),
                pattern=r"^cd:",
            )
        )
        app.add_handler(
            CallbackQueryHandler(
                self._inject_deps(self._agentic_pick_callback),
                pattern=r"^(sess|model|mode|perm|settings|br):",
            )
        )

        logger.info("Agentic handlers registered")

    def _register_classic_handlers(self, app: Application) -> None:
        """Register full classic handler set (moved from core.py)."""
        from .handlers import callback, command, message

        handlers = [
            ("start", command.start_command),
            ("help", command.help_command),
            ("new", command.new_session),
            ("continue", command.continue_session),
            ("end", command.end_session),
            ("ls", command.list_files),
            ("cd", command.change_directory),
            ("pwd", command.print_working_directory),
            ("projects", command.show_projects),
            ("status", command.session_status),
            ("export", command.export_session),
            ("actions", command.quick_actions),
            ("git", command.git_command),
        ]
        if self.settings.enable_project_threads:
            handlers.append(("sync_threads", command.sync_threads))

        for cmd, handler in handlers:
            app.add_handler(CommandHandler(cmd, self._inject_deps(handler)))

        app.add_handler(
            MessageHandler(
                filters.TEXT & ~filters.COMMAND,
                self._inject_deps(message.handle_text_message),
            ),
            group=10,
        )
        app.add_handler(
            MessageHandler(
                filters.Document.ALL, self._inject_deps(message.handle_document)
            ),
            group=10,
        )
        app.add_handler(
            MessageHandler(filters.PHOTO, self._inject_deps(message.handle_photo)),
            group=10,
        )
        app.add_handler(
            CallbackQueryHandler(self._inject_deps(callback.handle_callback_query))
        )

        logger.info("Classic handlers registered (13 commands + full handler set)")

    async def get_bot_commands(self) -> list:  # type: ignore[type-arg]
        """Return bot commands appropriate for current mode."""
        if self.settings.agentic_mode:
            commands = [
                BotCommand("start", "Start the bot"),
                BotCommand("new", "Start a fresh session"),
                BotCommand("status", "Show session status"),
                BotCommand("verbose", "Set output verbosity (0/1/2)"),
                BotCommand("repo", "Выбрать папку (диски D:, E: …)"),
                BotCommand("sessions", "Continue a previous session"),
                BotCommand("model", "Choose Claude model"),
                BotCommand("abort", "Stop the current request"),
                BotCommand("settings", "Output and queue settings"),
            ]
            if self.settings.enable_project_threads:
                commands.append(BotCommand("sync_threads", "Sync project topics"))
            return commands
        else:
            commands = [
                BotCommand("start", "Start bot and show help"),
                BotCommand("help", "Show available commands"),
                BotCommand("new", "Clear context and start fresh session"),
                BotCommand("continue", "Explicitly continue last session"),
                BotCommand("end", "End current session and clear context"),
                BotCommand("ls", "List files in current directory"),
                BotCommand("cd", "Change directory (resumes project session)"),
                BotCommand("pwd", "Show current directory"),
                BotCommand("projects", "Show all projects"),
                BotCommand("status", "Show session status"),
                BotCommand("export", "Export current session"),
                BotCommand("actions", "Show quick actions"),
                BotCommand("git", "Git repository commands"),
            ]
            if self.settings.enable_project_threads:
                commands.append(BotCommand("sync_threads", "Sync project topics"))
            return commands

    # --- Agentic handlers ---

    async def agentic_start(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Brief welcome, no buttons."""
        user = update.effective_user
        sync_line = ""
        if (
            self.settings.enable_project_threads
            and self.settings.project_threads_mode == "private"
        ):
            if (
                not update.effective_chat
                or getattr(update.effective_chat, "type", "") != "private"
            ):
                await update.message.reply_text(
                    "🚫 <b>Private Topics Mode</b>\n\n"
                    "Use this bot in a private chat and run <code>/start</code> there.",
                    parse_mode="HTML",
                )
                return
            manager = context.bot_data.get("project_threads_manager")
            if manager:
                try:
                    result = await manager.sync_topics(
                        context.bot,
                        chat_id=update.effective_chat.id,
                    )
                    sync_line = (
                        "\n\n🧵 Topics synced"
                        f" (created {result.created}, reused {result.reused})."
                    )
                except PrivateTopicsUnavailableError:
                    await update.message.reply_text(
                        manager.private_topics_unavailable_message(),
                        parse_mode="HTML",
                    )
                    return
                except Exception:
                    sync_line = "\n\n🧵 Topic sync failed. Run /sync_threads to retry."
        current_dir = context.user_data.get(
            "current_directory", self.settings.approved_directory
        )
        dir_display = f"<code>{current_dir}/</code>"

        safe_name = escape_html(user.first_name)
        await update.message.reply_text(
            f"Hi {safe_name}! I'm your AI coding assistant.\n"
            f"Just tell me what you need — I can read, write, and run code.\n\n"
            f"Working in: {dir_display}\n"
            f"Commands: /new (reset) · /status"
            f"{sync_line}",
            parse_mode="HTML",
            reply_markup=self._main_keyboard(),
        )

    async def agentic_new(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Reset session, one-line confirmation."""
        context.user_data["claude_session_id"] = None
        context.user_data["session_started"] = True
        context.user_data["force_new_session"] = True

        await update.message.reply_text("Session reset. What's next?")

    async def agentic_status(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Compact one-line status, no buttons."""
        current_dir = context.user_data.get(
            "current_directory", self.settings.approved_directory
        )
        dir_display = str(current_dir)

        session_id = context.user_data.get("claude_session_id")
        session_status = "active" if session_id else "none"

        # Cost info
        cost_str = ""
        rate_limiter = context.bot_data.get("rate_limiter")
        if rate_limiter:
            try:
                user_status = rate_limiter.get_user_status(update.effective_user.id)
                cost_usage = user_status.get("cost_usage", {})
                current_cost = cost_usage.get("current", 0.0)
                cost_str = f" · Cost: ${current_cost:.2f}"
            except Exception:
                pass

        await update.message.reply_text(
            f"📂 {dir_display} · Session: {session_status}{cost_str}"
        )

    def _get_verbose_level(self, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Return effective verbose level: per-user override or global default."""
        user_override = context.user_data.get("verbose_level")
        if user_override is not None:
            return int(user_override)
        return self.settings.verbose_level

    async def agentic_verbose(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Set output verbosity: /verbose [0|1|2]."""
        args = update.message.text.split()[1:] if update.message.text else []
        if not args:
            current = self._get_verbose_level(context)
            labels = {0: "quiet", 1: "normal", 2: "detailed"}
            await update.message.reply_text(
                f"Verbosity: <b>{current}</b> ({labels.get(current, '?')})\n\n"
                "Usage: <code>/verbose 0|1|2</code>\n"
                "  0 = quiet (final response only)\n"
                "  1 = normal (tools + reasoning)\n"
                "  2 = detailed (tools with inputs + reasoning)",
                parse_mode="HTML",
            )
            return

        try:
            level = int(args[0])
            if level not in (0, 1, 2):
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "Please use: /verbose 0, /verbose 1, or /verbose 2"
            )
            return

        context.user_data["verbose_level"] = level
        labels = {0: "quiet", 1: "normal", 2: "detailed"}
        await update.message.reply_text(
            f"Verbosity set to <b>{level}</b> ({labels[level]})",
            parse_mode="HTML",
        )

    def _format_verbose_progress(
        self,
        activity_log: List[Dict[str, Any]],
        verbose_level: int,
        start_time: float,
    ) -> str:
        """Build the progress message text based on activity so far."""
        if not activity_log:
            return "Working..."

        elapsed = time.time() - start_time
        lines: List[str] = [f"Working... ({elapsed:.0f}s)\n"]

        for entry in activity_log[-15:]:  # Show last 15 entries max
            kind = entry.get("kind", "tool")
            if kind == "text":
                # Claude's intermediate reasoning/commentary
                snippet = entry.get("detail", "")
                if verbose_level >= 2:
                    lines.append(f"\U0001f4ac {snippet}")
                else:
                    # Level 1: one short line
                    lines.append(f"\U0001f4ac {snippet[:80]}")
            else:
                # Tool call
                icon = _tool_icon(entry["name"])
                if verbose_level >= 2 and entry.get("detail"):
                    lines.append(f"{icon} {entry['name']}: {entry['detail']}")
                else:
                    lines.append(f"{icon} {entry['name']}")

        if len(activity_log) > 15:
            lines.insert(1, f"... ({len(activity_log) - 15} earlier entries)\n")

        return "\n".join(lines)

    @staticmethod
    def _summarize_tool_input(tool_name: str, tool_input: Dict[str, Any]) -> str:
        """Return a short summary of tool input for verbose level 2."""
        if not tool_input:
            return ""
        if tool_name in ("Read", "Write", "Edit", "MultiEdit"):
            path = tool_input.get("file_path") or tool_input.get("path", "")
            if path:
                # Show just the filename, not the full path
                return path.rsplit("/", 1)[-1]
        if tool_name in ("Glob", "Grep"):
            pattern = tool_input.get("pattern", "")
            if pattern:
                return pattern[:60]
        if tool_name == "Bash":
            cmd = tool_input.get("command", "")
            if cmd:
                return _redact_secrets(cmd[:100])[:80]
        if tool_name in ("WebFetch", "WebSearch"):
            return (tool_input.get("url", "") or tool_input.get("query", ""))[:60]
        if tool_name == "Task":
            desc = tool_input.get("description", "")
            if desc:
                return desc[:60]
        # Generic: show first key's value
        for v in tool_input.values():
            if isinstance(v, str) and v:
                return v[:60]
        return ""

    @staticmethod
    def _start_typing_heartbeat(
        chat: Any,
        interval: float = 2.0,
    ) -> "asyncio.Task[None]":
        """Start a background typing indicator task.

        Sends typing every *interval* seconds, independently of
        stream events. Cancel the returned task in a ``finally``
        block.
        """

        async def _heartbeat() -> None:
            try:
                while True:
                    await asyncio.sleep(interval)
                    try:
                        await chat.send_action("typing")
                    except Exception:
                        pass
            except asyncio.CancelledError:
                pass

        return asyncio.create_task(_heartbeat())

    def _make_stream_callback(
        self,
        verbose_level: int,
        progress_msg: Any,
        tool_log: List[Dict[str, Any]],
        start_time: float,
    ) -> Optional[Callable[[StreamUpdate], Any]]:
        """Create a stream callback for verbose progress updates.

        Returns None when verbose_level is 0 (nothing to display).
        Typing indicators are handled by a separate heartbeat task.
        """
        if verbose_level == 0:
            return None

        last_edit_time = [0.0]  # mutable container for closure

        async def _on_stream(update_obj: StreamUpdate) -> None:
            # Capture tool calls
            if update_obj.tool_calls:
                for tc in update_obj.tool_calls:
                    name = tc.get("name", "unknown")
                    detail = self._summarize_tool_input(name, tc.get("input", {}))
                    tool_log.append({"kind": "tool", "name": name, "detail": detail})

            # Capture assistant text (reasoning / commentary)
            if update_obj.type == "assistant" and update_obj.content:
                text = update_obj.content.strip()
                if text and verbose_level >= 1:
                    # Collapse to first meaningful line, cap length
                    first_line = text.split("\n", 1)[0].strip()
                    if first_line:
                        tool_log.append({"kind": "text", "detail": first_line[:120]})

            # Throttle progress message edits to avoid Telegram rate limits
            now = time.time()
            if (now - last_edit_time[0]) >= 2.0 and tool_log:
                last_edit_time[0] = now
                new_text = self._format_verbose_progress(
                    tool_log, verbose_level, start_time
                )
                try:
                    await progress_msg.edit_text(new_text)
                except Exception:
                    pass

        return _on_stream

    # --- Permission requests, queue, abort, settings ---

    @staticmethod
    def _describe_tool_call(tool_name: str, tool_input: Dict[str, Any]) -> str:
        """HTML description of what Claude wants to do."""
        icon = _tool_icon(tool_name)
        if tool_name == "ExitPlanMode":
            plan = str(tool_input.get("plan", "")).strip()
            return "\U0001f4cb <b>План готов</b>\n\n" + escape_html(plan[:3000])
        if tool_name == "Bash":
            cmd = _redact_secrets(str(tool_input.get("command", "")))[:600]
            return f"{icon} <b>Команда</b>\n<pre>{escape_html(cmd)}</pre>"
        if tool_name in ("Write", "Edit", "MultiEdit", "NotebookEdit"):
            path = tool_input.get("file_path") or tool_input.get("notebook_path", "")
            return f"{icon} <b>{tool_name}</b>\n<code>{escape_html(str(path))}</code>"
        detail = MessageOrchestrator._summarize_tool_input(tool_name, tool_input)
        return f"{icon} <b>{escape_html(tool_name)}</b>\n{escape_html(detail)}"

    async def _request_permission(
        self,
        bot: Any,
        chat_id: int,
        user_id: int,
        tool_name: str,
        tool_input: Dict[str, Any],
    ) -> str:
        """Ask the user in Telegram; returns "once", "always" or "reject"."""
        request_id = uuid.uuid4().hex[:8]
        future: "asyncio.Future[str]" = asyncio.get_running_loop().create_future()
        self._pending_perms[request_id] = (user_id, future)

        if tool_name == "ExitPlanMode":
            buttons = [
                InlineKeyboardButton(
                    "✅ Выполнить", callback_data=f"perm:once:{request_id}"
                ),
                InlineKeyboardButton(
                    "❌ Отклонить", callback_data=f"perm:reject:{request_id}"
                ),
            ]
        else:
            buttons = [
                InlineKeyboardButton(
                    "✅ Один раз", callback_data=f"perm:once:{request_id}"
                ),
                InlineKeyboardButton(
                    "\U0001f513 Всегда", callback_data=f"perm:always:{request_id}"
                ),
                InlineKeyboardButton(
                    "❌ Отклонить", callback_data=f"perm:reject:{request_id}"
                ),
            ]
        text = "\U0001f510 <b>Разрешение</b>\n\n" + self._describe_tool_call(
            tool_name, tool_input
        )
        message = await bot.send_message(
            chat_id,
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([buttons]),
        )
        try:
            return await asyncio.wait_for(future, timeout=self._PERMISSION_TIMEOUT)
        except asyncio.TimeoutError:
            try:
                await message.edit_text(
                    text + "\n\n⌛ Время вышло, отклонено.", parse_mode="HTML"
                )
            except Exception:
                pass
            return "reject"
        finally:
            self._pending_perms.pop(request_id, None)

    async def _handle_permission_callback(self, query: Any, value: str) -> None:
        decision, _, request_id = value.partition(":")
        pending = self._pending_perms.get(request_id)
        if (
            decision not in ("once", "always", "reject")
            or pending is None
            or pending[0] != query.from_user.id
            or pending[1].done()
        ):
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            return
        pending[1].set_result(decision)
        verdict = {
            "once": "✅ Разрешено один раз",
            "always": "\U0001f513 Разрешено всегда",
            "reject": "❌ Отклонено",
        }[decision]
        try:
            await query.edit_message_text(
                (query.message.text_html or "") + f"\n\n<b>{verdict}</b>",
                parse_mode="HTML",
            )
        except Exception:
            pass

    async def agentic_abort(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Stop the running request, drop the queue and pending permissions."""
        user_id = update.effective_user.id
        dropped = len(self._queues.pop(user_id, []))
        for uid, future in list(self._pending_perms.values()):
            if uid == user_id and not future.done():
                future.set_result("reject")

        client = runtime.active_clients.get(user_id)
        if client is None and not dropped:
            await update.message.reply_text("Нечего останавливать.")
            return
        if client is not None:
            try:
                await client.interrupt()
            except Exception as e:
                logger.warning("Interrupt failed", error=str(e), user_id=user_id)
        suffix = f" Очередь очищена: {dropped}." if dropped else ""
        await update.message.reply_text(f"⏹ Остановлено.{suffix}")

    def _settings_menu(self) -> Tuple[str, InlineKeyboardMarkup]:
        s = runtime.settings()

        def mark(key: str) -> str:
            return "✅" if s.get(key) else "▫️"

        always = s.always_allowed()
        rows = [
            [
                InlineKeyboardButton(
                    f"{mark('live_feed')} Ход работы как в терминале",
                    callback_data="settings:live_feed",
                )
            ],
            [
                InlineKeyboardButton(
                    f"{mark('send_files')} Присылать созданные файлы",
                    callback_data="settings:send_files",
                )
            ],
            [
                InlineKeyboardButton(
                    f"{mark('compact_output')} Компактный вывод",
                    callback_data="settings:compact_output",
                )
            ],
            [
                InlineKeyboardButton(
                    f"{mark('footer')} Подпись под ответом",
                    callback_data="settings:footer",
                )
            ],
            [
                InlineKeyboardButton(
                    f"{mark('queue')} Очередь сообщений",
                    callback_data="settings:queue",
                )
            ],
            [
                InlineKeyboardButton(
                    f"\U0001f5d1 Сбросить «Всегда» ({len(always)})",
                    callback_data="settings:reset_always",
                )
            ],
        ]
        text = "<b>Настройки</b>"
        if always:
            text += "\nВсегда разрешено: " + ", ".join(
                f"<code>{escape_html(t)}</code>" for t in always
            )
        return text, InlineKeyboardMarkup(rows)

    async def agentic_settings(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        text, markup = self._settings_menu()
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=markup)

    async def _handle_settings_callback(self, query: Any, value: str) -> None:
        s = runtime.settings()
        if value == "reset_always":
            s.set("always_allowed_tools", [])
        elif value in ("live_feed", "send_files", "compact_output", "footer", "queue"):
            s.toggle(value)
        text, markup = self._settings_menu()
        try:
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
        except Exception:
            pass  # message not modified

    async def agentic_text(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Entry point for text: run now, or wait in the per-user queue."""
        user_id = update.effective_user.id
        lock = self._user_locks.setdefault(user_id, asyncio.Lock())

        if lock.locked():
            if not runtime.settings().get("queue"):
                await update.message.reply_text(
                    "Claude ещё работает. Дождитесь ответа или /abort."
                )
                return
            queue = self._queues.setdefault(user_id, [])
            if len(queue) >= self._QUEUE_LIMIT:
                await update.message.reply_text(
                    f"Очередь заполнена ({self._QUEUE_LIMIT}). Дождитесь или /abort."
                )
                return
            queue.append((update, context))
            await update.message.reply_text(f"⏳ В очереди: {len(queue)}")
            return

        async with lock:
            item: Optional[Tuple[Update, Any]] = (update, context)
            while item is not None:
                await self._agentic_text_with_approvals(*item)
                queue = self._queues.get(user_id)
                item = queue.pop(0) if queue else None

    async def _agentic_text_with_approvals(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Run one request with a Telegram approver registered for the user."""
        user_id = update.effective_user.id
        bot = context.bot
        chat_id = update.effective_chat.id

        async def requester(tool_name: str, tool_input: Dict[str, Any]) -> str:
            return await self._request_permission(
                bot, chat_id, user_id, tool_name, tool_input
            )

        runtime.permission_requesters[user_id] = requester
        try:
            await self._agentic_text_run(update, context)
        finally:
            runtime.permission_requesters.pop(user_id, None)

    def _make_compact_callback(
        self,
        progress_msg: Any,
        tool_log: List[Dict[str, Any]],
        start_time: float,
    ) -> Callable[[StreamUpdate], Any]:
        """One progress message: Thinking -> current tool -> Writing answer."""
        last_edit_time = [0.0]

        async def _on_stream(update_obj: StreamUpdate) -> None:
            status = None
            if update_obj.tool_calls:
                for tc in update_obj.tool_calls:
                    name = tc.get("name", "unknown")
                    detail = self._summarize_tool_input(name, tc.get("input", {}))
                    tool_log.append({"kind": "tool", "name": name, "detail": detail})
                    status = f"{_tool_icon(name)} {name}"
                    if detail:
                        status += f": {detail[:128]}"
            elif update_obj.type == "assistant" and update_obj.content:
                status = "✍️ Пишу ответ…"

            now = time.time()
            if status and (now - last_edit_time[0]) >= 2.0:
                last_edit_time[0] = now
                tools = sum(1 for e in tool_log if e.get("kind") == "tool")
                try:
                    await progress_msg.edit_text(
                        f"⏳ Работаю · вызовов: {tools} · "
                        f"{now - start_time:.0f} с\n{status}"
                    )
                except Exception:
                    pass

        return _on_stream

    async def _agentic_text_run(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Direct Claude passthrough. Simple progress. No suggestions."""
        user_id = update.effective_user.id
        message_text = update.message.text

        logger.info(
            "Agentic text message",
            user_id=user_id,
            message_length=len(message_text),
        )

        # Rate limit check
        rate_limiter = context.bot_data.get("rate_limiter")
        if rate_limiter:
            allowed, limit_message = await rate_limiter.check_rate_limit(user_id, 0.001)
            if not allowed:
                await update.message.reply_text(f"⏱️ {limit_message}")
                return

        chat = update.message.chat
        await chat.send_action("typing")

        verbose_level = self._get_verbose_level(context)
        live = bool(runtime.settings().get("live_feed")) and verbose_level > 0
        compact = (
            bool(runtime.settings().get("compact_output")) or live
        ) and verbose_level > 0
        progress_msg = await update.message.reply_text(
            "\U0001f4ad Думаю…" if compact else "Working..."
        )

        claude_integration = context.bot_data.get("claude_integration")
        if not claude_integration:
            await progress_msg.edit_text(
                "Claude integration not available. Check configuration."
            )
            return

        current_dir = context.user_data.get(
            "current_directory", self.settings.approved_directory
        )
        session_id = context.user_data.get("claude_session_id")

        # Check if /new was used — skip auto-resume for this first message.
        # Flag is only cleared after a successful run so retries keep the intent.
        force_new = bool(context.user_data.get("force_new_session"))

        # --- Verbose progress tracking via stream callback ---
        tool_log: List[Dict[str, Any]] = []
        start_time = time.time()
        if compact:
            on_stream = self._make_compact_callback(progress_msg, tool_log, start_time)
        else:
            on_stream = self._make_stream_callback(
                verbose_level, progress_msg, tool_log, start_time
            )

        # Terminal-like feed: reasoning, tool calls, diffs as separate messages
        feed: Optional[LiveFeed] = None
        if live and on_stream is not None:
            feed = LiveFeed(update.message)
            formatter_live = LiveFormatter(redact=_redact_secrets)
            status_stream = on_stream

            async def on_stream(update_obj: StreamUpdate) -> None:  # type: ignore[misc]
                try:
                    for chunk in formatter_live.events(update_obj):
                        feed.add(chunk)
                except Exception as feed_err:
                    logger.warning("Live feed failed", error=str(feed_err))
                await status_stream(update_obj)

        # Independent typing heartbeat — stays alive even with no stream events
        heartbeat = self._start_typing_heartbeat(chat)

        success = True
        try:
            claude_response = await claude_integration.run_command(
                prompt=message_text,
                working_directory=current_dir,
                user_id=user_id,
                session_id=session_id,
                on_stream=on_stream,
                force_new=force_new,
            )

            # New session created successfully — clear the one-shot flag
            if force_new:
                context.user_data["force_new_session"] = False

            context.user_data["claude_session_id"] = claude_response.session_id

            # Track directory changes
            from .handlers.message import _update_working_directory_from_claude_response

            _update_working_directory_from_claude_response(
                claude_response, context, self.settings, user_id
            )

            # Store interaction
            storage = context.bot_data.get("storage")
            if storage:
                try:
                    await storage.save_claude_interaction(
                        user_id=user_id,
                        session_id=claude_response.session_id,
                        prompt=message_text,
                        response=claude_response,
                        ip_address=None,
                    )
                except Exception as e:
                    logger.warning("Failed to log interaction", error=str(e))

            # Format response (no reply_markup — strip keyboards)
            from .utils.formatting import ResponseFormatter

            formatter = ResponseFormatter(self.settings)
            formatted_messages = formatter.format_claude_response(
                claude_response.content
            )

        except ClaudeToolValidationError as e:
            success = False
            logger.error("Tool validation error", error=str(e), user_id=user_id)
            from .utils.formatting import FormattedMessage

            formatted_messages = [FormattedMessage(str(e), parse_mode="HTML")]

        except Exception as e:
            success = False
            logger.error("Claude integration failed", error=str(e), user_id=user_id)
            from .handlers.message import _format_error_message
            from .utils.formatting import FormattedMessage

            formatted_messages = [
                FormattedMessage(_format_error_message(e), parse_mode="HTML")
            ]
        finally:
            heartbeat.cancel()
            if feed is not None:
                send_files = bool(runtime.settings().get("send_files"))
                await feed.close(list(formatter_live.written) if send_files else [])

        if compact:
            tool_count = sum(1 for e in tool_log if e.get("kind") == "tool")
            elapsed = time.time() - start_time
            icon = "✅" if success else "⚠️"
            try:
                await progress_msg.edit_text(
                    f"{icon} Готово · вызовов: {tool_count} · {elapsed:.0f} с"
                )
            except Exception:
                pass
        else:
            await progress_msg.delete()

        for i, message in enumerate(formatted_messages):
            if not message.text or not message.text.strip():
                continue
            try:
                await update.message.reply_text(
                    message.text,
                    parse_mode=message.parse_mode,
                    reply_markup=None,  # No keyboards in agentic mode
                    reply_to_message_id=(update.message.message_id if i == 0 else None),
                )
                if i < len(formatted_messages) - 1:
                    await asyncio.sleep(0.5)
            except Exception as send_err:
                logger.warning(
                    "Failed to send HTML response, retrying as plain text",
                    error=str(send_err),
                    message_index=i,
                )
                try:
                    await update.message.reply_text(
                        message.text,
                        reply_markup=None,
                        reply_to_message_id=(
                            update.message.message_id if i == 0 else None
                        ),
                    )
                except Exception as plain_err:
                    await update.message.reply_text(
                        f"Failed to deliver response "
                        f"(Telegram error: {str(plain_err)[:150]}). "
                        f"Please try again.",
                        reply_to_message_id=(
                            update.message.message_id if i == 0 else None
                        ),
                    )

        if runtime.settings().get("footer"):
            emoji, label = runtime.MODES.get(
                runtime.settings().get("mode"), runtime.MODES["default"]
            )
            try:
                await update.message.reply_text(
                    f"<i>{emoji} {label} · \U0001f9e0 {escape_html(self._model_label())}"
                    f" · ⏱ {time.time() - start_time:.0f} с</i>",
                    parse_mode="HTML",
                )
            except Exception:
                pass

        # Audit log
        audit_logger = context.bot_data.get("audit_logger")
        if audit_logger:
            await audit_logger.log_command(
                user_id=user_id,
                command="text_message",
                args=[message_text[:100]],
                success=success,
            )

    async def agentic_document(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Process file upload -> Claude, minimal chrome."""
        user_id = update.effective_user.id
        document = update.message.document

        logger.info(
            "Agentic document upload",
            user_id=user_id,
            filename=document.file_name,
        )

        # Security validation
        security_validator = context.bot_data.get("security_validator")
        if security_validator:
            valid, error = security_validator.validate_filename(document.file_name)
            if not valid:
                await update.message.reply_text(f"File rejected: {error}")
                return

        # Size check
        max_size = 10 * 1024 * 1024
        if document.file_size > max_size:
            await update.message.reply_text(
                f"File too large ({document.file_size / 1024 / 1024:.1f}MB). Max: 10MB."
            )
            return

        chat = update.message.chat
        await chat.send_action("typing")
        progress_msg = await update.message.reply_text("Working...")

        # Try enhanced file handler, fall back to basic
        features = context.bot_data.get("features")
        file_handler = features.get_file_handler() if features else None
        prompt: Optional[str] = None

        if file_handler:
            try:
                processed_file = await file_handler.handle_document_upload(
                    document,
                    user_id,
                    update.message.caption or "Please review this file:",
                )
                prompt = processed_file.prompt
            except Exception:
                file_handler = None

        if not file_handler:
            file = await document.get_file()
            file_bytes = await file.download_as_bytearray()
            try:
                content = file_bytes.decode("utf-8")
                if len(content) > 50000:
                    content = content[:50000] + "\n... (truncated)"
                caption = update.message.caption or "Please review this file:"
                prompt = (
                    f"{caption}\n\n**File:** `{document.file_name}`\n\n"
                    f"```\n{content}\n```"
                )
            except UnicodeDecodeError:
                await progress_msg.edit_text(
                    "Unsupported file format. Must be text-based (UTF-8)."
                )
                return

        # Process with Claude
        claude_integration = context.bot_data.get("claude_integration")
        if not claude_integration:
            await progress_msg.edit_text(
                "Claude integration not available. Check configuration."
            )
            return

        current_dir = context.user_data.get(
            "current_directory", self.settings.approved_directory
        )
        session_id = context.user_data.get("claude_session_id")

        # Check if /new was used — skip auto-resume for this first message.
        # Flag is only cleared after a successful run so retries keep the intent.
        force_new = bool(context.user_data.get("force_new_session"))

        verbose_level = self._get_verbose_level(context)
        tool_log: List[Dict[str, Any]] = []
        on_stream = self._make_stream_callback(
            verbose_level, progress_msg, tool_log, time.time()
        )

        heartbeat = self._start_typing_heartbeat(chat)
        try:
            claude_response = await claude_integration.run_command(
                prompt=prompt,
                working_directory=current_dir,
                user_id=user_id,
                session_id=session_id,
                on_stream=on_stream,
                force_new=force_new,
            )

            if force_new:
                context.user_data["force_new_session"] = False

            context.user_data["claude_session_id"] = claude_response.session_id

            from .handlers.message import _update_working_directory_from_claude_response

            _update_working_directory_from_claude_response(
                claude_response, context, self.settings, user_id
            )

            from .utils.formatting import ResponseFormatter

            formatter = ResponseFormatter(self.settings)
            formatted_messages = formatter.format_claude_response(
                claude_response.content
            )

            await progress_msg.delete()

            for i, message in enumerate(formatted_messages):
                await update.message.reply_text(
                    message.text,
                    parse_mode=message.parse_mode,
                    reply_markup=None,
                    reply_to_message_id=(update.message.message_id if i == 0 else None),
                )
                if i < len(formatted_messages) - 1:
                    await asyncio.sleep(0.5)

        except Exception as e:
            from .handlers.message import _format_error_message

            await progress_msg.edit_text(_format_error_message(e), parse_mode="HTML")
            logger.error("Claude file processing failed", error=str(e), user_id=user_id)
        finally:
            heartbeat.cancel()

    async def agentic_photo(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Process photo -> Claude, minimal chrome."""
        user_id = update.effective_user.id

        features = context.bot_data.get("features")
        image_handler = features.get_image_handler() if features else None

        if not image_handler:
            await update.message.reply_text("Photo processing is not available.")
            return

        chat = update.message.chat
        await chat.send_action("typing")
        progress_msg = await update.message.reply_text("Working...")

        try:
            photo = update.message.photo[-1]
            processed_image = await image_handler.process_image(
                photo, update.message.caption
            )

            claude_integration = context.bot_data.get("claude_integration")
            if not claude_integration:
                await progress_msg.edit_text(
                    "Claude integration not available. Check configuration."
                )
                return

            current_dir = context.user_data.get(
                "current_directory", self.settings.approved_directory
            )
            session_id = context.user_data.get("claude_session_id")

            # Check if /new was used — skip auto-resume for this first message.
            # Flag is only cleared after a successful run so retries keep the intent.
            force_new = bool(context.user_data.get("force_new_session"))

            verbose_level = self._get_verbose_level(context)
            tool_log: List[Dict[str, Any]] = []
            on_stream = self._make_stream_callback(
                verbose_level, progress_msg, tool_log, time.time()
            )

            heartbeat = self._start_typing_heartbeat(chat)
            try:
                claude_response = await claude_integration.run_command(
                    prompt=processed_image.prompt,
                    working_directory=current_dir,
                    user_id=user_id,
                    session_id=session_id,
                    on_stream=on_stream,
                    force_new=force_new,
                )
            finally:
                heartbeat.cancel()

            if force_new:
                context.user_data["force_new_session"] = False

            context.user_data["claude_session_id"] = claude_response.session_id

            from .utils.formatting import ResponseFormatter

            formatter = ResponseFormatter(self.settings)
            formatted_messages = formatter.format_claude_response(
                claude_response.content
            )

            await progress_msg.delete()

            for i, message in enumerate(formatted_messages):
                await update.message.reply_text(
                    message.text,
                    parse_mode=message.parse_mode,
                    reply_markup=None,
                    reply_to_message_id=(update.message.message_id if i == 0 else None),
                )
                if i < len(formatted_messages) - 1:
                    await asyncio.sleep(0.5)

        except Exception as e:
            from .handlers.message import _format_error_message

            await progress_msg.edit_text(_format_error_message(e), parse_mode="HTML")
            logger.error(
                "Claude photo processing failed", error=str(e), user_id=user_id
            )

    # --- Folder browser: any folder on drives D: and beyond ---

    _BROWSE_PAGE = 16
    _BROWSE_HIDDEN = {"$recycle.bin", "system volume information", "recovery"}

    def _path_allowed(self, path: Path) -> bool:
        from ..security.roots import is_allowed_path

        try:
            resolved = path.resolve()
        except OSError:
            return False
        return is_allowed_path(resolved) or self._is_within(
            resolved, self.settings.approved_directory
        )

    async def _switch_directory(
        self, context: ContextTypes.DEFAULT_TYPE, user_id: int, target: Path
    ) -> str:
        """Make target the working directory; returns a short status line."""
        context.user_data["current_directory"] = target

        claude_integration = context.bot_data.get("claude_integration")
        session_id = None
        if claude_integration:
            existing = await claude_integration._find_resumable_session(
                user_id, target
            )
            if existing:
                session_id = existing.session_id
        context.user_data["claude_session_id"] = session_id

        git_badge = " (git)" if (target / ".git").is_dir() else ""
        session_badge = " · session resumed" if session_id else ""
        return f"<code>{escape_html(str(target))}</code>{git_badge}{session_badge}"

    def _browse_view(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        path: Optional[Path],
        page: int = 0,
    ) -> Tuple[str, InlineKeyboardMarkup]:
        """Build the folder (or drive) list for the browser message."""
        from ..security.roots import allowed_drives

        rows: List[list] = []  # type: ignore[type-arg]
        if path is None:
            drives = allowed_drives()
            context.user_data["browse"] = {
                "path": None,
                "drives": [str(d) for d in drives],
                "children": [],
                "page": 0,
            }
            buttons = [
                InlineKeyboardButton(f"\U0001f4bd {d}", callback_data=f"br:dr:{i}")
                for i, d in enumerate(drives)
            ]
            for i in range(0, len(buttons), 3):
                rows.append(buttons[i : i + 3])
            return "<b>Диски</b>\nВыберите диск:", InlineKeyboardMarkup(rows)

        try:
            children = sorted(
                (
                    d.name
                    for d in path.iterdir()
                    if d.is_dir()
                    and not d.name.startswith(".")
                    and d.name.lower() not in self._BROWSE_HIDDEN
                ),
                key=str.lower,
            )
            error = ""
        except OSError as e:
            children, error = [], f"\n⚠️ {escape_html(str(e)[:120])}"

        pages = max(1, -(-len(children) // self._BROWSE_PAGE))
        page = min(max(page, 0), pages - 1)
        context.user_data["browse"] = {
            "path": str(path),
            "drives": [],
            "children": children,
            "page": page,
        }

        start = page * self._BROWSE_PAGE
        buttons = []
        for i in range(start, min(start + self._BROWSE_PAGE, len(children))):
            name = children[i]
            icon = "\U0001f4e6" if (path / name / ".git").is_dir() else "\U0001f4c1"
            label = name if len(name) <= 26 else name[:25] + "…"
            buttons.append(
                InlineKeyboardButton(f"{icon} {label}", callback_data=f"br:d:{i}")
            )
        for i in range(0, len(buttons), 2):
            rows.append(buttons[i : i + 2])

        if pages > 1:
            nav = []
            if page > 0:
                nav.append(
                    InlineKeyboardButton("◀", callback_data=f"br:pg:{page - 1}")
                )
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="br:noop"))
            if page < pages - 1:
                nav.append(
                    InlineKeyboardButton("▶", callback_data=f"br:pg:{page + 1}")
                )
            rows.append(nav)
        rows.append(
            [
                InlineKeyboardButton("⬆️ Вверх", callback_data="br:up"),
                InlineKeyboardButton("\U0001f4bd Диски", callback_data="br:drives"),
                InlineKeyboardButton("✅ Выбрать", callback_data="br:sel"),
            ]
        )
        text = (
            f"<b>Папка</b>\n<code>{escape_html(str(path))}</code>\n"
            f"Подпапок: {len(children)}{error}"
        )
        return text, InlineKeyboardMarkup(rows)

    async def _open_browser(
        self, message: Any, context: ContextTypes.DEFAULT_TYPE, path: Optional[Path]
    ) -> None:
        text, markup = self._browse_view(context, path)
        await message.reply_text(text, parse_mode="HTML", reply_markup=markup)

    async def _handle_browse_callback(
        self, query: Any, value: str, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        state = context.user_data.get("browse")
        if not state:
            await query.edit_message_text("Меню устарело. Отправьте /repo ещё раз.")
            return

        action, _, arg = value.partition(":")
        current = Path(state["path"]) if state.get("path") else None
        target: Optional[Path] = current
        page = 0

        if action == "noop":
            return
        if action == "drives":
            target = None
        elif action == "dr":
            try:
                target = Path(state["drives"][int(arg)])
            except (ValueError, IndexError):
                return
        elif action == "d" and current is not None:
            try:
                target = current / state["children"][int(arg)]
            except (ValueError, IndexError):
                return
        elif action == "up" and current is not None:
            target = None if current.parent == current else current.parent
        elif action == "pg" and current is not None:
            try:
                page = int(arg)
            except ValueError:
                return
        elif action == "sel" and current is not None:
            if not self._path_allowed(current) or not current.is_dir():
                await query.edit_message_text("Эта папка недоступна.")
                return
            status = await self._switch_directory(
                context, query.from_user.id, current
            )
            await query.edit_message_text(
                f"\U0001f4c2 Рабочая папка: {status}", parse_mode="HTML"
            )
            audit_logger = context.bot_data.get("audit_logger")
            if audit_logger:
                await audit_logger.log_command(
                    user_id=query.from_user.id,
                    command="cd",
                    args=[str(current)],
                    success=True,
                )
            return
        else:
            return

        if target is not None and (
            not self._path_allowed(target) or not target.is_dir()
        ):
            await query.message.reply_text("Папка недоступна.")
            return

        text, markup = self._browse_view(context, target, page)
        try:
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
        except Exception:
            pass  # message not modified

    async def agentic_repo(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Choose the working folder on any drive from D: upward.

        /repo            — folder browser starting at the current folder
        /repo <path|name> — switch directly (absolute path, or name under the base)
        """
        raw = (update.message.text or "").split(maxsplit=1)
        arg = raw[1].strip().strip('"') if len(raw) > 1 else ""
        base = self.settings.approved_directory
        current_dir = Path(context.user_data.get("current_directory", base))

        if not arg:
            await self._open_browser(update.message, context, current_dir)
            return

        candidate = Path(arg)
        target = candidate if candidate.is_absolute() else base / candidate
        if not target.is_dir() or not self._path_allowed(target):
            await update.message.reply_text(
                f"Папка недоступна или не найдена: <code>{escape_html(arg)}</code>",
                parse_mode="HTML",
            )
            return

        status = await self._switch_directory(
            context, update.effective_user.id, target.resolve()
        )
        await update.message.reply_text(
            f"\U0001f4c2 Рабочая папка: {status}", parse_mode="HTML"
        )

    async def agentic_dir_button(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Bottom-keyboard folder button: open the browser."""
        current = Path(
            context.user_data.get("current_directory", self.settings.approved_directory)
        )
        await self._open_browser(update.message, context, current)

    # Models offered by /model: (button label, value passed to Claude Code)
    _MODEL_CHOICES = [
        ("Default", ""),
        ("Fable 5.1", "claude-fable-5-1"),
        ("Opus 5.5", "claude-opus-5-5"),
        ("Sonnet 5.5", "claude-sonnet-5-5"),
        ("Haiku 5.5", "claude-haiku-5-5"),
    ]

    def _read_model(self) -> str:
        return str(runtime.settings().get("model") or "")

    def _model_label(self) -> str:
        current = self._read_model()
        for label, value in self._MODEL_CHOICES:
            if value == current:
                return label
        return current or "Default"

    def _main_keyboard(self) -> ReplyKeyboardMarkup:
        """Persistent bottom keyboard: current mode and model."""
        emoji, label = runtime.MODES.get(
            runtime.settings().get("mode"), runtime.MODES["default"]
        )
        return ReplyKeyboardMarkup(
            [
                [
                    KeyboardButton(f"{emoji} {label}"),
                    KeyboardButton(f"\U0001f9e0 {self._model_label()}"[:40]),
                    KeyboardButton("\U0001f4c1 Папка"),
                ]
            ],
            resize_keyboard=True,
            is_persistent=True,
        )

    async def _send_model_menu(self, update: Update) -> None:
        current = self._read_model()
        rows = []
        for label, value in self._MODEL_CHOICES:
            mark = "✅ " if value == current else ""
            rows.append(
                [InlineKeyboardButton(f"{mark}{label}", callback_data=f"model:{value}")]
            )
        await update.message.reply_text(
            f"Model: <code>{escape_html(current or 'default')}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def agentic_model(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Show or change the Claude model: /model [name]."""
        args = update.message.text.split()[1:] if update.message.text else []
        if args:
            value = args[0] if args[0].lower() != "default" else ""
            runtime.settings().set("model", value)
            await update.message.reply_text(
                f"Model: <code>{escape_html(value or 'default')}</code>",
                parse_mode="HTML",
                reply_markup=self._main_keyboard(),
            )
            return
        await self._send_model_menu(update)

    async def agentic_model_button(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Bottom-keyboard model button: open the model menu."""
        await self._send_model_menu(update)

    async def agentic_mode_button(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Bottom-keyboard mode button: open the mode menu."""
        current = runtime.settings().get("mode")
        descriptions = {
            "default": "спрашивает перед правками и командами",
            "plan": "только план, ничего не меняет",
            "acceptEdits": "правки файлов без вопросов, команды спрашивает",
        }
        rows = []
        for value, (emoji, label) in runtime.MODES.items():
            mark = "✅ " if value == current else ""
            rows.append(
                [
                    InlineKeyboardButton(
                        f"{mark}{emoji} {label}", callback_data=f"mode:{value}"
                    )
                ]
            )
        text = "<b>Режим</b>\n" + "\n".join(
            f"{e} {l} — {descriptions[v]}" for v, (e, l) in runtime.MODES.items()
        )
        await update.message.reply_text(
            text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows)
        )

    @staticmethod
    def _local_sessions(directory: Path, limit: int = 8) -> List[Dict[str, Any]]:
        """Read Claude Code session files for a directory from ~/.claude/projects."""
        import json
        from datetime import datetime

        folder = Path.home() / ".claude" / "projects"
        folder = folder / re.sub(r"[^A-Za-z0-9]", "-", str(directory))
        if not folder.is_dir():
            return []

        files = sorted(
            folder.glob("*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True
        )
        result: List[Dict[str, Any]] = []
        for f in files[:limit]:
            title = ""
            try:
                with f.open(encoding="utf-8") as fh:
                    for _, line in zip(range(80), fh):
                        try:
                            entry = json.loads(line)
                        except ValueError:
                            continue
                        if entry.get("type") != "user" or entry.get("isMeta"):
                            continue
                        content = entry.get("message", {}).get("content")
                        if isinstance(content, list):
                            content = " ".join(
                                p.get("text", "")
                                for p in content
                                if isinstance(p, dict)
                            )
                        if isinstance(content, str):
                            content = content.strip()
                            if content and not content.startswith("<"):
                                title = " ".join(content.split())
                                break
            except OSError:
                continue
            result.append(
                {
                    "id": f.stem,
                    "title": title or "(no title)",
                    "time": datetime.fromtimestamp(f.stat().st_mtime),
                }
            )
        return result

    async def agentic_sessions(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """List Claude Code sessions of the current directory (incl. local CLI)."""
        current_dir = Path(
            context.user_data.get("current_directory", self.settings.approved_directory)
        )
        current_id = context.user_data.get("claude_session_id")
        sessions = self._local_sessions(current_dir)
        if not sessions:
            await update.message.reply_text(
                f"No sessions in <code>{escape_html(str(current_dir))}</code>.",
                parse_mode="HTML",
            )
            return

        rows = []
        for s in sessions:
            mark = "● " if s["id"] == current_id else ""
            label = f"{mark}{s['time']:%d.%m %H:%M} · {s['title']}"[:60]
            rows.append([InlineKeyboardButton(label, callback_data=f"sess:{s['id']}")])
        await update.message.reply_text(
            f"Sessions in <code>{escape_html(str(current_dir))}</code>\n"
            "Pick one to continue it:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _agentic_pick_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle sess: and model: button presses."""
        query = update.callback_query
        await query.answer()
        kind, _, value = query.data.partition(":")

        if kind == "perm":
            await self._handle_permission_callback(query, value)
            return

        if kind == "settings":
            await self._handle_settings_callback(query, value)
            return

        if kind == "br":
            await self._handle_browse_callback(query, value, context)
            return

        if kind in ("model", "mode"):
            if kind == "model":
                if value not in {v for _, v in self._MODEL_CHOICES}:
                    return
                runtime.settings().set("model", value)
                confirmation = f"\U0001f9e0 {self._model_label()}"
            else:
                if value not in runtime.MODES:
                    return
                runtime.settings().set("mode", value)
                emoji, label = runtime.MODES[value]
                confirmation = f"{emoji} {label}"
            # The bottom keyboard can only be replaced by sending a new message
            try:
                await query.message.delete()
            except Exception:
                pass
            await query.message.chat.send_message(
                confirmation, reply_markup=self._main_keyboard()
            )
            return

        # kind == "sess": only accept ids that exist for the current directory
        current_dir = Path(
            context.user_data.get("current_directory", self.settings.approved_directory)
        )
        known = {s["id"]: s for s in self._local_sessions(current_dir, limit=50)}
        if value not in known:
            await query.edit_message_text("Session not found.")
            return

        claude_integration = context.bot_data.get("claude_integration")
        if claude_integration:
            await claude_integration.session_manager.adopt_session(
                query.from_user.id, current_dir, value
            )
        context.user_data["claude_session_id"] = value
        context.user_data["force_new_session"] = False
        await query.edit_message_text(
            f"Continuing: {escape_html(known[value]['title'][:80])}",
            parse_mode="HTML",
        )

    async def _agentic_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle cd: callbacks — switch directory and resume session if available."""
        query = update.callback_query
        await query.answer()

        data = query.data
        _, project_name = data.split(":", 1)

        base = self.settings.approved_directory
        new_path = base / project_name

        if not new_path.is_dir():
            await query.edit_message_text(
                f"Directory not found: <code>{escape_html(project_name)}</code>",
                parse_mode="HTML",
            )
            return

        context.user_data["current_directory"] = new_path

        # Look for a resumable session instead of always clearing
        claude_integration = context.bot_data.get("claude_integration")
        session_id = None
        if claude_integration:
            existing = await claude_integration._find_resumable_session(
                query.from_user.id, new_path
            )
            if existing:
                session_id = existing.session_id
        context.user_data["claude_session_id"] = session_id

        is_git = (new_path / ".git").is_dir()
        git_badge = " (git)" if is_git else ""
        session_badge = " · session resumed" if session_id else ""

        await query.edit_message_text(
            f"Switched to <code>{escape_html(project_name)}/</code>"
            f"{git_badge}{session_badge}",
            parse_mode="HTML",
        )

        # Audit log
        audit_logger = context.bot_data.get("audit_logger")
        if audit_logger:
            await audit_logger.log_command(
                user_id=query.from_user.id,
                command="cd",
                args=[project_name],
                success=True,
            )
