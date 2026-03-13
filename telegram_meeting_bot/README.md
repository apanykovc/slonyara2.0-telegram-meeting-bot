# Telegram Meeting Bot

## Runtime Source of Truth

Production runtime entrypoint is aiogram:
- `python -m telegram_meeting_bot`
- main app code: `telegram_meeting_bot/aiogram_app/app.py`

Legacy PTB snippets are archived under:
- `telegram_meeting_bot/legacy/`

When fixing bot behavior, patch `aiogram_app/app.py` first.

## Admin CLI

Local administration commands:

- `python -m telegram_meeting_bot.admin_cli status`
- `python -m telegram_meeting_bot.admin_cli backup --include-logs`
- `python -m telegram_meeting_bot.admin_cli verify-db`
- `python -m telegram_meeting_bot.admin_cli compact-chats`
- `python -m telegram_meeting_bot.admin_cli history --limit 10`
- `python -m telegram_meeting_bot.admin_cli flags`
- `python -m telegram_meeting_bot.admin_cli set-flag alerts.error_burst.enabled false`
- `python -m telegram_meeting_bot.admin_cli restore /path/to/backup.zip --force`
