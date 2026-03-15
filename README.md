# Telegram Meeting Bot

Telegram-бот для планирования напоминаний о встречах по короткой строке вида `ДД.ММ ТИП ЧЧ:ММ ПЕРЕГ [НОМЕР]`. Основной runtime работает на `aiogram v3`, старый код на `python-telegram-bot` сохранён в `telegram_meeting_bot/legacy/` только как архив.

## Что умеет

- Принимать строку встречи и превращать её в отложенное напоминание.
- Работать через личный диалог с ботом, а отправлять напоминания в личку, группу или тему.
- Хранить активные напоминания в SQLite.
- Поддерживать таймзоны и оффсет до события.
- Показывать список активных встреч и админ-панель.
- Делать backup/restore данных и управлять feature flags через CLI.

Пример сообщения, которое бот понимает:

```text
08.08 МТС 20:40 2в 88634
```

Где:

- `08.08` — дата
- `МТС` — тип встречи
- `20:40` — время
- `2в` — переговорка / место
- `88634` — опциональный номер

## Стек и структура

- Python 3
- `aiogram 3`
- `APScheduler`
- `aiohttp`
- `sqlite3`

Основные директории:

- `telegram_meeting_bot/aiogram_app/app.py` — основной runtime
- `telegram_meeting_bot/admin_cli.py` — локальная административная CLI
- `telegram_meeting_bot/core/` — storage, parsing, logging, flags, release history
- `telegram_meeting_bot/ui/` — тексты и клавиатуры
- `telegram_meeting_bot/legacy/` — архив старой PTB-реализации
- `tests/` — unit-тесты
- `data/` — runtime-данные, база, конфиги и логи

## Быстрый старт

1. Создайте и активируйте виртуальное окружение.
2. Установите зависимости:

```bash
pip install -r requirements.txt
```

3. Задайте токен бота через окружение:

```bash
export BOT_TOKEN=your_bot_token_here
```

4. Запустите приложение:

```bash
python -m telegram_meeting_bot
```

По умолчанию запускается `aiogram`-версия. Если нужен старый runtime, можно выставить:

```bash
export LEGACY_PTB=1
```

## Переменные окружения

Обязательная:

- `BOT_TOKEN` или `TELEGRAM_BOT_TOKEN` — токен Telegram-бота

Полезные опции:

- `ORG_TZ_DEFAULT` — таймзона по умолчанию, например `Europe/Moscow`
- `TELEGRAM_ADMIN_IDS` — список Telegram user id администраторов через запятую или пробел
- `APP_LOG_RETENTION_DAYS` — срок хранения app-логов
- `AUDIT_LOG_RETENTION_DAYS` — срок хранения audit-логов
- `ERROR_LOG_MAX_BYTES` — размер error-лога до ротации
- `ERROR_LOG_BACKUP_COUNT` — число архивов error-логов

Пример минимального окружения:

```env
BOT_TOKEN=your_bot_token_here
ORG_TZ_DEFAULT=Europe/Moscow
TELEGRAM_ADMIN_IDS=123456789
```

## Данные и хранение

Проект хранит состояние в директории `data/`:

- `data/reminders.db` — SQLite с активными и архивными напоминаниями
- `data/config.json` — настройки чатов
- `data/chats.json` — зарегистрированные чаты
- `data/admins.json` — логины администраторов
- `data/owners.json` — логины владельцев
- `data/feature_flags.json` — feature flags
- `data/release_history.json` — история запусков
- `data/logs/` — app, audit и error логи

При старте бот умеет автоматически мигрировать старый `data/reminders.json` в SQLite, если таблица ещё пустая.

## Команды бота

Telegram-команды, которые регистрируются на старте:

- `/start`
- `/version`
- `/admin`

Также в runtime есть обработчики для `/help`, `/menu`, `/register`, `/purge` и сценариев через inline/reply-клавиатуры.

## Admin CLI

Локальные административные команды:

```bash
python -m telegram_meeting_bot.admin_cli status
python -m telegram_meeting_bot.admin_cli backup --include-logs
python -m telegram_meeting_bot.admin_cli verify-db
python -m telegram_meeting_bot.admin_cli compact-chats
python -m telegram_meeting_bot.admin_cli history --limit 10
python -m telegram_meeting_bot.admin_cli flags
python -m telegram_meeting_bot.admin_cli set-flag alerts.error_burst.enabled false
python -m telegram_meeting_bot.admin_cli restore /path/to/backup.zip --force
```

## Тесты

Запуск тестов:

```bash
pytest
```

Сейчас тесты покрывают parsing, storage, flags, release history, logging, audit и клавиатуры.

## Примечания по разработке

- Если меняете поведение бота, сначала правьте `telegram_meeting_bot/aiogram_app/app.py`.
- `telegram_meeting_bot/legacy/` не является runtime source of truth.
- Runtime по умолчанию запускается через `python -m telegram_meeting_bot`.
