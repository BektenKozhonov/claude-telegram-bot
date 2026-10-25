# Claude Code Telegram — control Claude Code from your phone

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

A Telegram bot that gives you remote access to [Claude Code](https://claude.ai/code) running on your computer.
Message the bot from your phone: it runs Claude Code in the folder you pick, streams the work like a terminal
and asks for permissions with buttons. Conversations are stored in the same sessions as the local `claude`,
so `claude --resume` on the computer continues a chat started in Telegram.

A fork of [claude-code-telegram](https://github.com/RichardAtCT/claude-code-telegram) (MIT) with Windows fixes
and the convenience of [opencode-telegram-bot](https://www.npmjs.com/package/@grinev/opencode-telegram-bot).
The original README is kept in [docs/README.upstream.md](docs/README.upstream.md).

## Features

**Terminal-like live feed** — separate messages as Claude works: reasoning, tool calls, edit diffs
(`- old` / `+ new`), Bash commands with their output, and errors. Files Claude creates arrive as documents
when the task ends (read in place from the project folder, no copies are made; secret files such as `.env`
and keys are never sent).

**Permission buttons** — before writing a file or running a command you get a request with
"✅ Once / 🔓 Always / ❌ Reject". An unanswered request is rejected after 10 minutes.

**Modes** (button on the bottom keyboard): 🛠 Default, 📋 Plan (plans only, changes nothing), ✏️ Auto-edits.
A "bypass permissions" mode is intentionally not offered.

**Folder picker for any drive** ("📁 Папка" button, `/repo`, `/cd`) — browse drives D: and beyond with buttons
or switch directly: `/repo E:\work`. Drive C: is blocked.

**Run control** — `/abort` stops the task and clears the queue; messages sent while Claude is busy are queued
(up to 5).

**Sessions and model** — `/sessions` (resume any session of the folder, including ones started locally),
`/model` and the 🧠 keyboard button (Fable, Opus, Sonnet, Haiku), `/new`, `/status`.

**Settings** (`/settings`): terminal-like feed, send created files, compact output, footer under replies,
message queue, reset of the "Always allowed" list.

## Setup (Windows)

You need: Python 3.11+, [Claude Code CLI](https://claude.ai/code) already logged in, a bot token from
[@BotFather](https://t.me/botfather) and your numeric Telegram ID (ask [@userinfobot](https://t.me/userinfobot)).

```powershell
git clone <this-repository-url> claude-telegram-bot
cd claude-telegram-bot
uv venv --python 3.12 ..\venv            # or: python -m venv ..\venv
uv pip install -e . --python ..\venv\Scripts\python.exe
copy .env.example .env                   # then fill in .env
```

Minimal `.env`:

```env
TELEGRAM_BOT_TOKEN=token_from_BotFather
TELEGRAM_BOT_USERNAME=bot_name_without_@
APPROVED_DIRECTORY=D:\projects           # starting folder
ALLOWED_USERS=123456789                  # your ID only
USE_SDK=true
CLAUDE_CLI_PATH=C:\Users\<you>\.local\bin\claude.exe
DISABLE_TOOL_VALIDATION=true             # tool-name checks are replaced by Telegram permission prompts
```

Run from the project folder: `..\venv\Scripts\claude-telegram-bot.exe`.
For autostart, create a Windows Task Scheduler task triggered at logon that runs this command with the
project folder as its working directory.

## Security

- Only users listed in `ALLOWED_USERS` can talk to the bot. Run **one** instance per token
  (otherwise Telegram returns `409 Conflict`).
- Claude runs on your computer with your privileges. Files on drives D: and beyond are reachable; drive C:
  (user profile, tokens, Claude settings) is blocked by the bot's checks. These are bot-level checks, not OS
  isolation: a Bash command can step outside them, so read the permission prompts.
- "🔓 Always" allows a whole tool (for example `Bash`) permanently; reset it in `/settings`.
- Never publish `.env` or the bot token; if it leaks, revoke it in @BotFather (`/revoke`).
- Keep the repository private.
- Do not write to one session from Telegram and from the local `claude` at the same time.

## What was changed

| File | Purpose |
|------|---------|
| `src/bot/orchestrator.py` | commands, keyboard, modes, permissions, `/abort`, queue, `/settings`, folder browser |
| `src/bot/live_feed.py` | live feed and delivery of created files |
| `src/claude/runtime.py` | settings in `data/settings.json`, permission and active-session state |
| `src/claude/sdk_integration.py` | permission mode, `can_use_tool`, reasoning and tool results |
| `src/security/roots.py` | drive-level access boundary (D: and beyond) |

## License

MIT. The original project is © its authors; the modifications are distributed under the same terms.
