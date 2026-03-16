"""
Бот напоминаний для встреч в Telegram — улучшенная работа со ссылками (v2.5.0)

Новое:
- Поддержка ссылок на чаты и топики:
  • t.me/c/<id>/<topic> → chat_id=-100<id>, topic_id=<topic>
  • t.me/c/<id> → chat_id=-100<id>
  • web.telegram.org/k/#<id> → chat_id=<id>
  • @username / t.me/<username> → chat_id='@username'
  • число (в т.ч. -100...) → chat_id=<int>
  • 0 → «этот чат»
- Сообщения‑подсказки при неверных или неподдерживаемых ссылках

Остальной функционал прежний:
- формат: ДД.ММ ТИП ЧЧ:ММ ПЕРЕГ НОМЕР (пример: 08.08 МТС 20:40 2в 88634)
- оффсет/TZ; панель; активные; повторы; персист; catch‑up; тихие логи
"""

import asyncio
import contextvars
import hashlib
import json
import logging
import os
import random
import sys
import traceback
import uuid
from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional, Tuple, Union

import pytz
from httpx import Timeout
from telegram import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardMarkup,
    Update,
    User,
)
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest
from tzlocal import get_localzone_name

from ..core.constants import (
    ADMIN_IDS,
    ADMIN_USERNAMES,
    APP_LOG_RETENTION_DAYS,
    AUDIT_LOG_RETENTION_DAYS,
    AWAIT_ADMIN,
    AWAIT_TZ,
    BOT_TOKEN,
    CATCHUP_WINDOW_SECONDS,
    CB_ACTIONS,
    CB_ACTIVE,
    CB_ACTIVE_PAGE,
    CB_ADMIN_ADD,
    CB_ADMIN_DEL,
    CB_ADMINS,
    CB_ARCHIVE,
    CB_ARCHIVE_CLEAR,
    CB_ARCHIVE_CLEAR_CONFIRM,
    CB_ARCHIVE_PAGE,
    CB_CANCEL,
    CB_CHAT_DEL,
    CB_CHATS,
    CB_DISABLED,
    CB_HELP,
    CB_MENU,
    CB_OFF_DEC,
    CB_OFF_INC,
    CB_OFF_PRESET_10,
    CB_OFF_PRESET_15,
    CB_OFF_PRESET_20,
    CB_OFF_PRESET_30,
    CB_PICK_CHAT,
    CB_RRULE,
    CB_SENDNOW,
    CB_SET_OFFSET,
    CB_SET_TZ,
    CB_SET_TZ_CHICAGO,
    CB_SET_TZ_ENTER,
    CB_SET_TZ_LOCAL,
    CB_SET_TZ_MOSCOW,
    CB_SETTINGS,
    CB_SHIFT,
    ERROR_LOG_BACKUP_COUNT,
    ERROR_LOG_MAX_BYTES,
    LOGS_APP_DIR,
    LOGS_AUDIT_DIR,
    LOGS_ERROR_DIR,
    MEETING_REGEX,
    OWNER_USERNAMES,
    PAGE_SIZE,
    REMINDER_TEMPLATE,
    RR_DAILY,
    RR_ONCE,
    RR_WEEKLY,
    VERSION,
    recent_signatures,
)
from ..core.storage import (
    add_admin_username,
    add_job_record,
    archive_job,
    clear_archive,
    find_job_by_text,
    get_archive_page,
    get_chat_cfg_entry,
    get_job_record,
    get_jobs_for_chat,
    get_jobs_store,
    get_known_chats,
    get_offset_for_chat,
    register_chat,
    remove_admin_username,
    remove_job_record,
    resolve_tz_for_chat,
    set_jobs_store,
    unregister_chat,
    update_chat_cfg,
    upsert_job_record,
)
from ..ui.keyboards import (
    actions_kb,
    active_kb,
    admins_menu_kb,
    archive_clear_confirm_kb,
    archive_kb,
    chats_menu_kb,
    choose_chat_kb,
    job_kb,
    main_menu_kb,
    offset_menu_kb,
    panel_kb,
    reply_menu_kb,
    settings_menu_kb,
    tz_menu_kb,
)
from ..ui.texts import (
    create_reminder_hint,
    escape_md,
    menu_text_for,
    render_active_text,
    render_admins_text,
    render_archive_text,
    render_panel_text,
    show_help_text,
)

# ==========================
# ----- КОНФИГ И ЛОГИ -----
# ==========================

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
RUN_ID = uuid.uuid4().hex
CALLBACK_LOCK_TTL = 20.0  # WHY: предотвращаем повторные клики в течение короткого окна
IDEMPOTENCY_TTL = 60.0  # WHY: повторные запросы в течение минуты не запускают действие повторно
EDIT_DEBOUNCE_WINDOW = 0.35  # WHY: Telegram может ругаться при слишком частых edit_message_text

_edit_timestamps: dict[tuple[int, int], float] = {}

_log_user: contextvars.ContextVar[str] = contextvars.ContextVar("log_user", default="-")


def _current_user_tag() -> Optional[str]:
    value = _log_user.get("-")
    return None if value in {None, "-", ""} else value


def _iso_ts() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


def _serialize_user(user: Optional[User]) -> Optional[Dict[str, Any]]:
    if user is None:
        return None
    return {
        "user_id": user.id,
        "username": user.username,
        "full_name": user.full_name,
        "first_name": user.first_name,
        "last_name": user.last_name,
    }


class AuditJSONFormatter(logging.Formatter):
    KEYS: Tuple[str, ...] = (
        "ts",
        "event",
        "user_id",
        "chat_id",
        "topic_id",
        "reminder_id",
        "title",
        "when",
        "tz",
        "reason",
        "repeat_next_at",
        "run_id",
    )

    def format(self, record: logging.LogRecord) -> str:
        payload = dict(getattr(record, "json_payload", {}) or {})
        payload.setdefault("ts", _iso_ts())
        payload.setdefault("run_id", RUN_ID)
        payload.setdefault("event", getattr(record, "event", None) or payload.get("event"))
        for key in self.KEYS:
            payload.setdefault(key, None)
        user_tag = _current_user_tag()
        if user_tag and not payload.get("user"):
            payload["user"] = user_tag
        return json.dumps(payload, ensure_ascii=False)


def _infer_error_type(message: str) -> str:
    upper = (message or "").upper()
    if "FLOOD CONTROL" in upper:
        return "FLOOD_CONTROL"
    if "EVENT LOOP IS CLOSED" in upper or "TASK WAS DESTROYED" in upper:
        return "EVENT_LOOP_CLOSED"
    if "BAD GATEWAY" in upper or "502" in upper:
        return "BAD_GATEWAY"
    if "READERROR" in upper or "NETWORKERROR" in upper:
        return "NETWORK_READ_ERROR"
    return "ERROR"


class ErrorJSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = dict(getattr(record, "json_payload", {}) or {})
        message = record.getMessage()
        payload.setdefault("ts", _iso_ts())
        payload.setdefault("where", record.name)
        payload.setdefault("message", message.splitlines()[0] if message else "")
        payload.setdefault("type", getattr(record, "error_type", None) or _infer_error_type(payload["message"]))
        if "stack_id" not in payload and record.exc_info:
            stack_text = "".join(traceback.format_exception(*record.exc_info))
            payload.setdefault("stack", stack_text)
            payload["stack_id"] = hashlib.blake2b(stack_text.encode("utf-8"), digest_size=6).hexdigest()
        payload.setdefault("run_id", RUN_ID)
        user_tag = _current_user_tag()
        if user_tag and not payload.get("user"):
            payload["user"] = user_tag
        return json.dumps(payload, ensure_ascii=False)


class DailyFileHandler(logging.Handler):
    def __init__(
        self,
        directory: Path,
        prefix: str,
        *,
        retention_days: int,
        encoding: str = "utf-8",
    ) -> None:
        super().__init__()
        self.directory = Path(directory)
        self.prefix = prefix
        self.retention_days = retention_days
        self.encoding = encoding
        self._current_date: Optional[date] = None
        self._stream: Optional[Any] = None
        self._open_stream()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._ensure_stream()
            msg = self.format(record)
            self._stream.write(msg + "\n")  # type: ignore[operator]
            self._stream.flush()  # type: ignore[attr-defined]
        except Exception:
            self.handleError(record)

    def _ensure_stream(self) -> None:
        current = datetime.now().date()
        if self._current_date != current or self._stream is None:
            self._open_stream()

    def _open_stream(self) -> None:
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
        self.directory.mkdir(parents=True, exist_ok=True)
        self._current_date = datetime.now().date()
        path = self.directory / f"{self.prefix}_{self._current_date.isoformat()}.log"
        self._stream = path.open("a", encoding=self.encoding)
        self._cleanup()

    def _cleanup(self) -> None:
        if self.retention_days <= 0:
            return
        cutoff = datetime.now().date() - timedelta(days=self.retention_days)
        pattern = f"{self.prefix}_*.log"
        for file_path in self.directory.glob(pattern):
            stem = file_path.stem
            try:
                date_str = stem.split("_")[-1]
                file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if file_date < cutoff:
                try:
                    file_path.unlink()
                except OSError:
                    pass

    def close(self) -> None:
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
            self._stream = None
        super().close()


class SizedJSONFileHandler(logging.Handler):
    def __init__(
        self,
        directory: Path,
        prefix: str,
        *,
        max_bytes: int,
        backup_count: int,
        encoding: str = "utf-8",
    ) -> None:
        super().__init__()
        self.directory = Path(directory)
        self.prefix = prefix
        self.max_bytes = max(1, max_bytes)
        self.backup_count = max(1, backup_count)
        self.encoding = encoding
        self._stream: Optional[Any] = None
        self._path: Optional[Path] = None
        self._ensure_stream()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._ensure_stream()
            if self._stream.tell() >= self.max_bytes:  # type: ignore[attr-defined]
                self._rotate()
            msg = self.format(record)
            self._stream.write(msg + "\n")  # type: ignore[operator]
            self._stream.flush()  # type: ignore[attr-defined]
        except Exception:
            self.handleError(record)

    def _ensure_stream(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        desired_path = self.directory / f"{self.prefix}_{datetime.now().strftime('%Y-%m-%d')}.log"
        if self._path != desired_path:
            if self._stream:
                try:
                    self._stream.close()
                except Exception:
                    pass
            self._path = desired_path
            self._stream = self._path.open("a", encoding=self.encoding)
            self._cleanup()

    def _rotate(self) -> None:
        if not self._stream or not self._path:
            return
        try:
            self._stream.close()
        except Exception:
            pass
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        rotated = self.directory / f"{self.prefix}_{timestamp}.log"
        try:
            self._path.rename(rotated)
        except OSError:
            rotated = self.directory / f"{self.prefix}_{timestamp}_{uuid.uuid4().hex[:6]}.log"
            try:
                self._path.rename(rotated)
            except OSError:
                pass
        self._stream = self._path.open("a", encoding=self.encoding)
        self._cleanup()

    def _cleanup(self) -> None:
        files = sorted(
            self.directory.glob(f"{self.prefix}_*.log"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for path in files[self.backup_count :]:
            try:
                path.unlink()
            except OSError:
                pass

    def close(self) -> None:
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
            self._stream = None
        super().close()


# --- базовый логгер ---
root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
root_logger.handlers.clear()

error_handler = SizedJSONFileHandler(
    LOGS_ERROR_DIR,
    "error",
    max_bytes=ERROR_LOG_MAX_BYTES,
    backup_count=ERROR_LOG_BACKUP_COUNT,
)
error_handler.setLevel(logging.WARNING)
error_handler.setFormatter(ErrorJSONFormatter())
root_logger.addHandler(error_handler)

# --- app / audit / error логгеры ---
app_logger = logging.getLogger("reminder.app")
app_logger.setLevel(logging.INFO)
app_logger.propagate = False
app_handler = DailyFileHandler(LOGS_APP_DIR, "app", retention_days=APP_LOG_RETENTION_DAYS)
app_handler.setLevel(logging.INFO)
app_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", DATE_FORMAT))
app_logger.addHandler(app_handler)

if os.environ.get("BOT_CONSOLE_LOGS", "1") != "0":
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", DATE_FORMAT)
    )
    app_logger.addHandler(console_handler)

audit_logger = logging.getLogger("reminder.audit")
audit_logger.setLevel(logging.INFO)
audit_logger.propagate = False
audit_handler = DailyFileHandler(LOGS_AUDIT_DIR, "audit", retention_days=AUDIT_LOG_RETENTION_DAYS)
audit_handler.setLevel(logging.INFO)
audit_handler.setFormatter(AuditJSONFormatter())
audit_logger.addHandler(audit_handler)

error_logger = logging.getLogger("reminder.error")
error_logger.setLevel(logging.WARNING)
error_logger.propagate = False
error_logger.addHandler(error_handler)

# WHY: точка входа ожидает атрибут ``logger`` для сообщений в консоль
logger = app_logger


def app_log(message: str, **fields: Any) -> None:
    parts: list[str] = ["app", f"run_id={RUN_ID}"]
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, str):
            if any(ch.isspace() for ch in value) or '"' in value:
                parts.append(f'{key}="{value}"')
            else:
                parts.append(f"{key}={value}")
        else:
            parts.append(f"{key}={value}")
    parts.append(f'msg="{message}"')
    app_logger.info(" ".join(parts))


def _short_title(title: Optional[str]) -> Optional[str]:
    if not title:
        return None
    compact = " ".join(str(title).split())
    return compact[:140]


def _iso_field(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def audit_log(event: str, **fields: Any) -> None:
    payload = {
        "ts": _iso_ts(),
        "event": event,
        "run_id": RUN_ID,
        "user_id": fields.pop("user_id", None),
        "chat_id": fields.pop("chat_id", None),
        "topic_id": fields.pop("topic_id", None),
        "reminder_id": fields.pop("reminder_id", None),
        "title": _short_title(fields.pop("title", None)),
        "when": _iso_field(fields.pop("when", None)),
        "tz": fields.pop("tz", None),
        "reason": fields.pop("reason", None),
        "repeat_next_at": _iso_field(fields.pop("repeat_next_at", None)),
    }
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
        else:
            payload[key] = value
    audit_logger.info("", extra={"json_payload": payload})


def error_log(
    error_type: str,
    *,
    where: str,
    message: Optional[str] = None,
    level: int = logging.ERROR,
    exc_info: Any = None,
    **fields: Any,
) -> None:
    base_message = message or error_type
    payload: Dict[str, Any] = {
        "ts": _iso_ts(),
        "type": error_type,
        "where": where,
        "message": base_message.splitlines()[0] if base_message else "",
        "run_id": RUN_ID,
    }
    stack_text = None
    if exc_info:
        if exc_info is True:
            exc_info = sys.exc_info()
        if exc_info and all(exc_info):
            stack_text = "".join(traceback.format_exception(*exc_info))
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
        else:
            payload[key] = value
    if stack_text:
        payload["stack"] = stack_text
        payload["stack_id"] = hashlib.blake2b(stack_text.encode("utf-8"), digest_size=6).hexdigest()
    error_logger.log(level, "", extra={"json_payload": payload})


def cleanup_logs() -> None:
    """Удалить устаревшие логи согласно политике хранения."""

    def _cleanup_daily(directory: Path, prefix: str, days: int) -> None:
        if days <= 0:
            return
        cutoff = datetime.now().date() - timedelta(days=days)
        try:
            directory.mkdir(parents=True, exist_ok=True)
            for path in directory.glob(f"{prefix}_*.log"):
                stem = path.stem
                try:
                    date_part = stem.split("_")[-1]
                    file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if file_date < cutoff:
                    try:
                        path.unlink()
                    except OSError as exc:
                        error_log(
                            "LOG_CLEANUP_FAILED",
                            where="log.cleanup",
                            message=str(exc),
                            level=logging.WARNING,
                            path=str(path),
                        )
        except OSError as exc:
            error_log(
                "LOG_CLEANUP_SCAN_FAILED",
                where="log.cleanup",
                message=str(exc),
                level=logging.WARNING,
                base=str(directory),
            )


async def on_application_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Централизованный обработчик ошибок PTB."""

    # WHY: без этого PTB пишет предупреждение «No error handlers are registered»
    exc = context.error
    if not exc:
        return
    _set_log_user(update)
    message = f"{exc.__class__.__name__}: {exc}"
    error_type = _infer_error_type(message)
    error_log(
        error_type,
        where="telegram.ext.Application",
        message=message,
        exc_info=(exc.__class__, exc, exc.__traceback__),
    )

    def _cleanup_error(directory: Path, prefix: str, keep: int) -> None:
        if keep <= 0:
            return
        try:
            directory.mkdir(parents=True, exist_ok=True)
            files = sorted(
                directory.glob(f"{prefix}_*.log"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for path in files[keep:]:
                try:
                    path.unlink()
                except OSError as exc:
                    error_log(
                        "LOG_CLEANUP_FAILED",
                        where="log.cleanup",
                        message=str(exc),
                        level=logging.WARNING,
                        path=str(path),
                    )
        except OSError as exc:
            error_log(
                "LOG_CLEANUP_SCAN_FAILED",
                where="log.cleanup",
                message=str(exc),
                level=logging.WARNING,
                base=str(directory),
            )

    _cleanup_error(LOGS_APP_DIR, "app", APP_LOG_RETENTION_DAYS)
    _cleanup_error(LOGS_AUDIT_DIR, "audit", AUDIT_LOG_RETENTION_DAYS)
    _cleanup_error(LOGS_ERROR_DIR, "error", ERROR_LOG_BACKUP_COUNT)


cleanup_logs()

# Тише внешние логгеры
for noisy in ("httpx", "httpcore", "apscheduler", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


class _DropHttpxNoise(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return ("HTTP Request:" not in msg) and ("HTTP Response" not in msg)


logging.getLogger("httpx").addFilter(_DropHttpxNoise())
logging.getLogger("httpcore").addFilter(_DropHttpxNoise())

# Токен — лучше держать в ENV TELEGRAM_BOT_TOKEN


def _set_log_user(update: Update) -> None:
    """Сохранить имя пользователя в контекст логов."""
    user = update.effective_user
    if user:
        if user.username:
            _log_user.set(f"@{user.username}")
        else:
            _log_user.set(str(user.id))
    else:
        _log_user.set("-")


def is_owner(user: Optional[User]) -> bool:
    """Является ли пользователь владельцем бота (ID или логин)."""
    return bool(
        user
        and (
            user.id in ADMIN_IDS
            or (user.username and user.username.lower() in OWNER_USERNAMES)
        )
    )


def is_admin(user: Optional[User]) -> bool:
    """Администратор – владелец или пользователь из списка логинов."""
    if is_owner(user):
        return True
    username = getattr(user, "username", None)
    return bool(username and username.lower() in ADMIN_USERNAMES)


def can_manage_settings(user: Optional[User], chat: Optional[Any]) -> bool:
    """Определить, может ли пользователь менять настройки в конкретном чате."""

    chat_type = getattr(chat, "type", None)
    if chat_type == "private":
        return True
    return is_admin(user)


async def _auto_delete(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Удалить сообщение по расписанию."""
    data = ctx.job.data or {}
    chat_id = data.get("cid")
    msg_id = data.get("mid")
    try:
        await ctx.bot.delete_message(chat_id, msg_id)
    except Exception:
        pass


def auto_delete(message: Message, context: ContextTypes.DEFAULT_TYPE, delay: int = 15) -> None:
    """Запланировать удаление сообщения через указанное число секунд."""
    if not message:
        return
    context.job_queue.run_once(_auto_delete, delay, data={"cid": message.chat.id, "mid": message.message_id})


def _clear_wait_flags(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    exclude: Optional[set[str]] = None,
) -> bool:
    """Сбросить флаги ожидания пользовательского ввода."""

    cleared = False
    skip = exclude or set()
    if context and getattr(context, "user_data", None) is not None:
        if AWAIT_TZ not in skip and context.user_data.pop(AWAIT_TZ, None):
            cleared = True
        if AWAIT_ADMIN not in skip and context.user_data.pop(AWAIT_ADMIN, None):
            cleared = True
    return cleared


async def _cancel_previous_action(
    message: Optional[Message],
    context: ContextTypes.DEFAULT_TYPE,
    *,
    exclude: Optional[set[str]] = None,
) -> bool:
    """Отменить незавершённый ввод и уведомить пользователя."""

    if not context:
        return False
    had = _clear_wait_flags(context, exclude=exclude)
    if had and message is not None:
        note = await reply_text_safe(message, "⏹️ Предыдущее действие отменено.")
        auto_delete(note, context)
    return had


async def _collect_active_jobs(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    uid: int,
    admin: bool,
) -> list[dict]:
    """Вернуть список задач для отображения в меню активных напоминаний."""

    store = get_jobs_store()
    if admin:
        if chat_id > 0:
            jobs_all = list(store)
        else:
            jobs_all = [j for j in store if j.get("target_chat_id") == chat_id]
    else:
        if chat_id > 0:
            jobs_all = [j for j in store if j.get("author_id") == uid]
            allowed: set[int] = set()
            for job in jobs_all:
                cid = job.get("target_chat_id")
                if cid in allowed:
                    continue
                try:
                    member = await context.bot.get_chat_member(cid, uid)
                    if member.status not in ("left", "kicked"):
                        allowed.add(cid)
                except Exception:
                    pass
            jobs_all = [j for j in jobs_all if j.get("target_chat_id") in allowed]
        else:
            jobs_all = [
                j
                for j in store
                if j.get("target_chat_id") == chat_id and j.get("author_id") == uid
            ]
    return sorted(jobs_all, key=lambda x: x.get("run_at_utc", ""))


def _slice_jobs(jobs_all: list[dict], page: int) -> tuple[list[dict], int, int]:
    pages_total = max(1, (len(jobs_all) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(max(page, 1), pages_total)
    start = (page - 1) * PAGE_SIZE
    chunk = jobs_all[start:start + PAGE_SIZE]
    return chunk, page, pages_total


async def _show_archive_view(
    q: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    page: int,
    can_manage: bool,
    notice: str | None = None,
) -> None:
    items, total, actual_page, pages_total = get_archive_page(page, PAGE_SIZE)
    text_out = render_archive_text(
        items,
        total,
        actual_page,
        pages_total,
        page_size=PAGE_SIZE,
    )
    if notice:
        text_out = f"{text_out}\n\n<i>{escape(notice)}</i>"
    markup = archive_kb(
        actual_page,
        pages_total,
        has_entries=bool(items),
        can_clear=can_manage and total > 0,
    )
    try:
        await edit_text_safe(
            q.edit_message_text,
            text_out,
            reply_markup=markup,
            parse_mode="HTML",
        )
    except Exception:
        await reply_text_safe(
            q.message,
            text_out,
            reply_markup=markup,
            parse_mode="HTML",
        )


async def _build_active_payload(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    uid: int,
    admin: bool,
    page: int,
    *,
    page_prefix: str = CB_ACTIVE_PAGE,
    view: str = "all",
) -> Optional[tuple[str, InlineKeyboardMarkup]]:
    jobs_all = await _collect_active_jobs(context, chat_id, uid, admin)
    if not jobs_all:
        return None
    chunk, page, pages_total = _slice_jobs(jobs_all, page)
    text_out = render_active_text(chunk, len(jobs_all), page, pages_total, admin)
    markup = active_kb(
        chunk,
        page,
        pages_total,
        uid,
        admin,
        page_prefix=page_prefix,
        view=view,
    )
    return text_out, markup


async def _send_active_overview_message(
    message: Message,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    user: User,
) -> None:
    admin = is_admin(user)
    if not admin:
        note = await reply_text_safe(
            message,
            "⛔ Только администратор может просматривать активные напоминания.",
        )
        auto_delete(note, context)
        return
    uid = user.id
    payload = await _build_active_payload(context, chat_id, uid, admin, page=1)
    if not payload:
        note = await reply_text_safe(
            message,
            "Пока нет активных напоминаний.",
            reply_markup=_main_menu_keyboard(user, message.chat),
        )
        auto_delete(note, context)
        return
    text_out, markup = payload
    await reply_text_safe(message, text_out, reply_markup=markup, parse_mode="HTML")


def _make_reply_menu_keyboard(
    is_admin: bool = False,
    *,
    allow_settings: bool = False,
) -> ReplyKeyboardMarkup:
    """Преобразовать aiogram-клавиатуру в формат python-telegram-bot."""

    source = reply_menu_kb(is_admin, allow_settings=allow_settings)
    rows: list[list[str]] = []
    resize = True
    one_time = False

    keyboard_rows = getattr(source, "keyboard", None)
    if keyboard_rows:
        for row in keyboard_rows:
            rows.append([getattr(btn, "text", str(btn)) for btn in row])
        resize = bool(getattr(source, "resize_keyboard", True))
        one_time = bool(getattr(source, "one_time_keyboard", False))
    if not rows:
        if is_admin:
            rows = [["📝 Активные", "❓ Справка"]]
        elif allow_settings:
            rows = [["⚙️ Настройки"], ["❓ Справка"]]
        else:
            rows = [["📂 Мои встречи", "❓ Справка"]]

    return ReplyKeyboardMarkup(rows, resize_keyboard=resize, one_time_keyboard=one_time)


def _main_menu_keyboard(user: Optional[User], chat: Optional[Any]) -> InlineKeyboardMarkup:
    return main_menu_kb(
        is_admin(user),
        allow_settings=can_manage_settings(user, chat),
    )


def _reply_menu_keyboard(user: Optional[User], chat: Optional[Any]) -> ReplyKeyboardMarkup:
    return _make_reply_menu_keyboard(
        is_admin(user),
        allow_settings=can_manage_settings(user, chat),
    )

# ==========================
# ----- ПАРСЕР И ОШИБКИ -----
# ==========================
def parse_meeting_message(text: str, tz: pytz.BaseTzInfo) -> Optional[Dict[str, Any]]:
    m = MEETING_REGEX.match(text or "")
    if not m:
        return None
    day_str, month_str, mtype, time_str_raw, room, ticket = m.groups()
    try:
        d = int(day_str)
        mth = int(month_str)
        hh, mm = time_str_raw.replace(".", ":", 1).split(":")
        hh = int(hh)
        mm = int(mm)
    except Exception:
        return None

    now = datetime.now(tz)
    year = now.year
    try:
        candidate = tz.localize(datetime(year, mth, d, hh, mm))
    except ValueError:
        return None

    # Если дата/время уже прошли в этом году — переносим на следующий
    if (mth < now.month) or (mth == now.month and candidate <= now):
        try:
            candidate = tz.localize(datetime(year + 1, mth, d, hh, mm))
        except ValueError:
            return None

    date_str = f"{d:02d}.{mth:02d}"
    time_str_norm = f"{hh:02d}:{mm:02d}"
    mtype = mtype.strip()
    room = room.strip()
    ticket = (ticket or "").strip()
    canonical_parts = [date_str, mtype, time_str_norm, room]
    if ticket:
        canonical_parts.append(ticket)
    canonical_full = " ".join(canonical_parts)
    ticket_placeholder = f" {ticket}" if ticket else ""

    return {
        "dt_local": candidate,
        "date_str": date_str,
        "time_str": time_str_norm,
        "type": mtype,
        "room": room,
        "ticket": ticket,
        "canonical_full": canonical_full,
        "reminder_text": REMINDER_TEMPLATE.format(
            date=date_str,
            type=mtype,
            time=time_str_norm,
            room=room,
            ticket=ticket_placeholder,
        ),
    }

def explain_format_error(text: str) -> str:
    return (
        "🙈 *Не понял формат встречи.*\n"
        "Жду: `ДД.ММ ТИП ЧЧ:ММ ПЕРЕГ [НОМЕР]`\n"
        "Например: `08.08 МТС 20:40 2в 88634` или `08.08 МТС 20:40 2в`\n"
        "Вы прислали: `" + text.replace('`', 'ʼ') + "`"
    )


# ==========================
# ----- БЕЗОПАСНАЯ ОТПРАВКА -----
# ==========================
MAX_MESSAGE_LENGTH = 4096


def _split_text(text: str, limit: int = MAX_MESSAGE_LENGTH) -> list[str]:
    if len(text) <= limit:
        return [text]
    parts: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            parts.append(remaining)
            break
        split_idx = remaining.rfind("\n", 0, limit)
        if split_idx == -1 or split_idx < limit // 2:
            split_idx = limit
        chunk = remaining[:split_idx].rstrip()
        if not chunk:
            chunk = remaining[:limit]
            split_idx = limit
        parts.append(chunk)
        remaining = remaining[split_idx:].lstrip("\n")
    return parts or [text]


async def _call_with_retry(func, where: str, *, profile: str = "default", **kwargs):
    """Выполнить вызов Telegram API с повторными попытками.

    WHY: учитываем временные обрывы сети и лимиты Telegram,
    повторяя вызов с экспоненциальной задержкой.
    """
    if profile == "fast":
        attempts = 2
        delay = 1.0
        cap = 5.0
    else:
        attempts = 6
        delay = 1.0
        cap = 15.0

    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return await func(**kwargs)
        except RetryAfter as exc:
            last_err = exc
            sleep_for = int(getattr(exc, "retry_after", 2)) + 1
            error_log(
                "FLOOD_CONTROL",
                where=where,
                message=str(exc),
                level=logging.WARNING,
                attempt=attempt,
                sleep=sleep_for,
            )
            await asyncio.sleep(sleep_for)
        except (NetworkError, TimedOut) as exc:
            last_err = exc
            sleep_for = min(cap, delay) * random.uniform(0.7, 1.3)
            error_log(
                "NETWORK_RETRY",
                where=where,
                message=str(exc),
                level=logging.WARNING,
                attempt=attempt,
                sleep=round(sleep_for, 2),
            )
            await asyncio.sleep(sleep_for)
            delay = min(cap, delay * 2)
        except BadRequest as exc:
            msg_lower = str(exc).lower()
            if "message is not modified" in msg_lower:
                # WHY: Telegram возвращает это, если данные совпадают — считаем успехом без ретраев
                error_log(
                    "SKIP_NOT_MODIFIED",
                    where=where,
                    message=str(exc),
                    level=logging.INFO,
                )
                return None
            if kwargs.get("parse_mode"):
                # WHY: повторяем без разметки при ошибке парсинга
                error_log(
                    "PARSE_MODE_FALLBACK",
                    where=where,
                    message=str(exc),
                    level=logging.INFO,
                )
                kwargs = {k: v for k, v in kwargs.items() if k != "parse_mode"}
                continue
            last_err = exc
            break
        except Exception as exc:  # pragma: no cover - непредвиденная ошибка
            last_err = exc
            break

    if last_err is not None:
        raise last_err
    raise RuntimeError("Не удалось отправить сообщение")


async def _send_text_callable(func, text: str, where: str, *, split_long: bool = True, **kwargs):
    chunks = _split_text(text) if split_long else [text]
    result = None
    first_result = None
    for idx, chunk in enumerate(chunks):
        call_kwargs = dict(kwargs)
        call_kwargs["text"] = chunk
        if idx:
            call_kwargs.pop("reply_markup", None)
            call_kwargs.pop("parse_mode", None)
        profile = call_kwargs.pop("_retry_profile", "default")
        result = await _call_with_retry(func, where, profile=profile, **call_kwargs)
        if first_result is None:
            first_result = result
    return first_result or result


async def safe_send_message(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: Union[int, str],
    text: str,
    topic_id: Optional[int] = None,
    fast_retry: bool = False,
    **kwargs,
):
    send_kwargs = {"chat_id": chat_id, **kwargs}
    if topic_id is not None:
        send_kwargs["message_thread_id"] = topic_id
    if fast_retry:
        send_kwargs["_retry_profile"] = "fast"
    return await _send_text_callable(
        context.bot.send_message,
        text,
        where="bot.safe_send_message",
        **send_kwargs,
    )


async def reply_text_safe(message: Message, text: str, *, fast_retry: bool = True, **kwargs):
    where = f"bot.reply_text:{message.chat_id}"
    if fast_retry:
        kwargs["_retry_profile"] = "fast"
    return await _send_text_callable(
        message.reply_text,
        text,
        where=where,
        **kwargs,
    )


async def _apply_edit_debounce(kwargs: dict) -> None:
    chat_id = kwargs.get("chat_id")
    message_id = kwargs.get("message_id")
    if chat_id is None or message_id is None:
        return
    key = (chat_id, message_id)
    now = _loop_time()
    last = _edit_timestamps.get(key)
    if last is not None:
        wait_for = EDIT_DEBOUNCE_WINDOW - (now - last)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
    _edit_timestamps[key] = _loop_time()


async def edit_text_safe(func, text: str, *, fast_retry: bool = False, **kwargs):
    where = kwargs.pop("where", "bot.edit_message_text")
    if fast_retry:
        kwargs["_retry_profile"] = "fast"
    await _apply_edit_debounce(kwargs)
    return await _send_text_callable(
        func,
        text,
        where=where,
        split_long=False,
        **kwargs,
    )


async def edit_markup_safe(func, *, fast_retry: bool = False, **kwargs):
    where = kwargs.pop("where", "bot.edit_message_reply_markup")
    try:
        profile = "fast" if fast_retry else "default"
        return await _call_with_retry(func, where, profile=profile, **kwargs)
    except BadRequest as exc:
        if "Message is not modified" in str(exc):
            error_log(
                "MESSAGE_NOT_MODIFIED",
                where=where,
                message=str(exc),
                level=logging.INFO,
            )
            return None
        raise


# --- Очередь отправки с ограничением скорости ---
SEND_INTERVAL = 0.12  # WHY: сокращаем задержку до ~120 мс, не выходя за безопасный лимит Telegram
SEND_BURST = 3  # WHY: обрабатываем несколько сообщений за тик без избыточной задержки


async def process_send_queue(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить один элемент очереди, чтобы выдерживать лимит Telegram."""

    queue: asyncio.Queue = context.application.bot_data.setdefault(
        "send_queue", asyncio.Queue()
    )
    deliveries: list[tuple[int, str, Optional[int]]] = []
    for _ in range(SEND_BURST):
        if queue.empty():
            break
        try:
            deliveries.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            break

    if not deliveries:
        return

    async def _deliver(chat_id: int, text: str, topic_id: Optional[int]) -> None:
        try:
            await safe_send_message(
                context,
                chat_id=chat_id,
                text=text,
                topic_id=topic_id,
            )
        except Exception as e:
            error_log(
                "SEND_QUEUE_DELIVERY_FAILED",
                where="bot.process_send_queue",
                message=str(e),
                level=logging.WARNING,
                chat_id=chat_id,
            )
        finally:
            queue.task_done()

    for chat_id, text, topic_id in deliveries:
        context.application.create_task(_deliver(chat_id, text, topic_id))


async def cleanup_logs_job(_context: ContextTypes.DEFAULT_TYPE) -> None:
    """Периодическая очистка устаревших логов."""

    cleanup_logs()


# ==========================
# ----- ДЕДУП -----
# ==========================
def make_signature(chat_id: int, canonical_full: str, dt_local: datetime) -> str:
    return f"{chat_id}|{canonical_full}|{int(dt_local.timestamp())}"

def dedup_should_skip(signature: str) -> bool:
    if signature in recent_signatures:
        return True
    recent_signatures.append(signature)
    return False


def release_signature(signature: Optional[str]) -> None:
    """Убрать подпись из окна дедупликации, если она там ещё есть."""

    if not signature:
        return
    try:
        recent_signatures.remove(signature)
    except ValueError:
        pass


def _loop_time() -> float:
    try:
        return asyncio.get_running_loop().time()
    except RuntimeError:  # pragma: no cover - нет активного цикла
        return asyncio.get_event_loop().time()


def _callback_lock_key(q: Optional[Any]) -> Optional[tuple[int, int]]:
    if not q or not getattr(q, "message", None):
        return None
    msg = q.message
    if not msg:
        return None
    return (msg.chat.id, msg.message_id)


def _cleanup_callback_locks(context: ContextTypes.DEFAULT_TYPE) -> dict:
    locks = context.application.bot_data.setdefault("callback_locks", {})
    now = _loop_time()
    expired = [key for key, expiry in locks.items() if expiry <= now]
    for key in expired:
        locks.pop(key, None)
    return locks


def _acquire_callback_lock(context: ContextTypes.DEFAULT_TYPE, key: Optional[tuple[int, int]]) -> bool:
    if key is None:
        return True
    locks = _cleanup_callback_locks(context)
    now = _loop_time()
    expiry = locks.get(key)
    if expiry and expiry > now:
        return False
    locks[key] = now + CALLBACK_LOCK_TTL
    return True


def _release_callback_lock(context: ContextTypes.DEFAULT_TYPE, key: Optional[tuple[int, int]]) -> None:
    if key is None:
        return
    locks = context.application.bot_data.get("callback_locks")
    if not locks:
        return
    locks.pop(key, None)


def _cleanup_idempotency(context: ContextTypes.DEFAULT_TYPE) -> dict:
    store = context.application.bot_data.setdefault("idempotency", {})
    now = _loop_time()
    expired = [key for key, entry in store.items() if entry.get("expires", 0) <= now]
    for key in expired:
        store.pop(key, None)
    return store


def _start_idempotent(context: ContextTypes.DEFAULT_TYPE, key: str) -> tuple[bool, Optional[dict]]:
    store = _cleanup_idempotency(context)
    entry = store.get(key)
    now = _loop_time()
    if entry and entry.get("status") == "running" and entry.get("expires", 0) > now:
        return False, entry
    if entry and entry.get("status") == "done" and entry.get("expires", 0) > now:
        return False, entry
    store[key] = {"status": "running", "expires": now + IDEMPOTENCY_TTL}
    return True, None


def _mark_idempotent_done(context: ContextTypes.DEFAULT_TYPE, key: str, *, result: Optional[str] = None) -> None:
    store = _cleanup_idempotency(context)
    store[key] = {
        "status": "done",
        "expires": _loop_time() + IDEMPOTENCY_TTL,
        "result": result,
    }


def _reset_idempotent(context: ContextTypes.DEFAULT_TYPE, key: str) -> None:
    store = _cleanup_idempotency(context)
    store.pop(key, None)


def _freeze_markup(markup: Optional[InlineKeyboardMarkup]) -> Optional[InlineKeyboardMarkup]:
    if not markup or not getattr(markup, "inline_keyboard", None):
        return None
    frozen_rows: list[list[InlineKeyboardButton]] = []
    for row in markup.inline_keyboard:
        frozen_row: list[InlineKeyboardButton] = []
        for btn in row:
            text = btn.text or ""
            if not text.startswith("⏳"):
                text = f"⏳ {text}"
            op_id = uuid.uuid4().hex[:8]
            frozen_row.append(
                InlineKeyboardButton(text, callback_data=f"{CB_DISABLED}:{op_id}")
            )
        frozen_rows.append(frozen_row)
    return InlineKeyboardMarkup(frozen_rows)


async def freeze_query_markup(q) -> None:
    message = getattr(q, "message", None)
    if not message:
        return
    disabled = _freeze_markup(getattr(message, "reply_markup", None))
    if not disabled:
        return
    try:
        await edit_markup_safe(
            q.edit_message_reply_markup,
            reply_markup=disabled,
            fast_retry=True,
        )
    except Exception:
        # WHY: заморозка клавиатуры — вспомогательная операция, игнорируем сбои
        pass


# ==========================
# ----- ПОВТОРЫ -----
# ==========================
def _rrule_next_iso(current_iso: str, rrule: str) -> Optional[str]:
    """Вернуть ISO-время следующего запуска для правила повтора.

    current_iso: время предыдущего запуска в формате ISO.
    rrule: один из RR_ONCE/RR_DAILY/RR_WEEKLY.
    """
    if not current_iso or rrule == RR_ONCE:
        return None
    try:
        dt = datetime.fromisoformat(current_iso)
    except ValueError:
        return None
    if rrule == RR_DAILY:
        dt += timedelta(days=1)
    elif rrule == RR_WEEKLY:
        dt += timedelta(weeks=1)
    else:
        return None
    return dt.isoformat()


# ==========================
# ==========================
# ----- ПАНЕЛЬ -----
# ==========================
async def ensure_panel(update: Update, context: ContextTypes.DEFAULT_TYPE, create: bool = False):
    """Обновить панель; при create=True создаёт новую, если её нет."""
    chat_id = update.effective_chat.id
    entry = get_chat_cfg_entry(chat_id)
    msg_id = entry.get("panel_msg_id")
    text = render_panel_text(chat_id)
    emsg = update.effective_message
    user = getattr(update, "effective_user", None)
    admin = is_admin(user)

    if msg_id:
        try:
            await edit_text_safe(
                context.bot.edit_message_text,
                text,
                chat_id=chat_id,
                message_id=msg_id,
                reply_markup=panel_kb(admin),
                parse_mode="Markdown",
                where="bot.ensure_panel.edit",
            )
            return
        except BadRequest as e:
            if "Message is not modified" in str(e):
                return
        except Exception:
            pass

    if not create:
        return

    try:
        if emsg is not None:
            sent = await reply_text_safe(
                emsg,
                text,
                reply_markup=panel_kb(admin),
                parse_mode="Markdown",
                fast_retry=False,
            )
        else:
            sent = await safe_send_message(
                context,
                chat_id=chat_id,
                text=text,
                reply_markup=panel_kb(admin),
                parse_mode="Markdown",
                fast_retry=False,
            )
        update_chat_cfg(chat_id, panel_msg_id=sent.message_id)
    except Exception:
        if emsg is not None:
            sent = await reply_text_safe(
                emsg,
                text,
                reply_markup=panel_kb(admin),
                fast_retry=False,
            )
        else:
            sent = await safe_send_message(
                context,
                chat_id=chat_id,
                text=text,
                reply_markup=panel_kb(admin),
                fast_retry=False,
            )
        update_chat_cfg(chat_id, panel_msg_id=sent.message_id)


# ==========================
# ----- КОЛБЭКИ -----
# ==========================
async def _handle_callback_body(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _set_log_user(update)
    q = update.callback_query
    if not q or not q.data:
        return
    chat = q.message.chat
    chat_id = chat.id
    user = q.from_user
    uid = user.id
    admin = is_admin(user)
    can_manage = can_manage_settings(user, chat)
    data = q.data

    await _cancel_previous_action(q.message, context)

    if data.startswith(f"{CB_PICK_CHAT}:"):
        parts = data.split(":", 3)
        if len(parts) < 4:
            return
        sel = parts[1]
        topic = parts[2]
        token = parts[3]
        pend = context.user_data.get("pending_reminders", {}).pop(token, None)
        if not pend:
            msg = await reply_text_safe(q.message, "❌ Не найдено ожидающее напоминание.")
            auto_delete(msg, context)
            return
        try:
            cfg_chat_id = int(sel)
        except ValueError:
            cfg_chat_id = sel
        topic_id = None if topic == "0" else int(topic)
        await schedule_reminder_core(pend["text"], cfg_chat_id, update, context, user, topic_override=topic_id)
        context.user_data["last_target"] = {"chat_id": cfg_chat_id, "topic_id": topic_id}
        context.user_data["force_pick"] = False
        try:
            await q.message.delete()
        except Exception:
            pass
        return

    # Главные меню
    if data == CB_DISABLED:
        return

    if data == CB_MENU:
        text = menu_text_for(chat_id)
        try:
            await edit_text_safe(
                q.edit_message_text,
                text,
                reply_markup=_main_menu_keyboard(user, chat),
                parse_mode="Markdown",
            )
        except Exception:
            await reply_text_safe(
                q.message,
                text,
                reply_markup=_main_menu_keyboard(user, chat),
                parse_mode="Markdown",
            )
        return

    if data == CB_SETTINGS:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Только администратор может менять настройки.")
            auto_delete(msg, context)
            return
        text = "⚙️ *Настройки чата*\n\n" + menu_text_for(chat_id)
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=settings_menu_kb(is_owner(user)), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=settings_menu_kb(is_owner(user)), parse_mode="Markdown")
        return

    if data == CB_ADMINS:
        if not is_owner(user):
            msg = await reply_text_safe(q.message, "⛔ Только владелец может управлять администраторами.")
            auto_delete(msg, context)
            return
        text = render_admins_text(ADMIN_USERNAMES)
        try:
            await edit_text_safe(q.edit_message_text, 
                text,
                reply_markup=admins_menu_kb(ADMIN_USERNAMES),
                parse_mode="Markdown",
            )
        except Exception:
            await reply_text_safe(q.message, 
                text,
                reply_markup=admins_menu_kb(ADMIN_USERNAMES),
                parse_mode="Markdown",
            )
        return

    if data == CB_ADMIN_ADD:
        if not is_owner(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        context.user_data[AWAIT_ADMIN] = True
        msg = await reply_text_safe(
            q.message,
            "Отправьте @username для добавления в админы. Любая другая кнопка отменит запрос.",
        )
        auto_delete(msg, context, 60)
        return

    if data.startswith(f"{CB_ADMIN_DEL}:"):
        if not is_owner(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        uname = data.split(":", 1)[1]
        removed = remove_admin_username(uname)
        note = "✅ Удалён" if removed else "❌ Не найден"
        text = render_admins_text(ADMIN_USERNAMES)
        try:
            await edit_text_safe(q.edit_message_text, 
                text,
                reply_markup=admins_menu_kb(ADMIN_USERNAMES),
                parse_mode="Markdown",
            )
        except Exception:
            await reply_text_safe(q.message, 
                text,
                reply_markup=admins_menu_kb(ADMIN_USERNAMES),
                parse_mode="Markdown",
            )
        info = await reply_text_safe(q.message, f"{note}: @{uname}")
        auto_delete(info, context)
        return

    if data == CB_ACTIVE or data.startswith(f"{CB_ACTIVE_PAGE}:"):
        if not admin:
            msg = await reply_text_safe(q.message, "⛔ Только администратор может просматривать активные напоминания.")
            auto_delete(msg, context)
            return
        page = 1
        if data.startswith(f"{CB_ACTIVE_PAGE}:"):
            try:
                page = max(1, int(data.split(":")[1]))
            except Exception:
                page = 1
        payload = await _build_active_payload(
            context,
            chat_id,
            uid,
            admin,
            page,
            page_prefix=CB_ACTIVE_PAGE,
        )
        if not payload:
            msg = await reply_text_safe(
                q.message,
                "Пока нет активных напоминаний.",
                reply_markup=_main_menu_keyboard(user, chat),
            )
            auto_delete(msg, context)
            return
        text_out, markup = payload
        try:
            await edit_text_safe(
                q.edit_message_text,
                text_out,
                reply_markup=markup,
                parse_mode="HTML",
            )
        except Exception:
            await reply_text_safe(
                q.message,
                text_out,
                reply_markup=markup,
                parse_mode="HTML",
            )
        return

    if data == CB_HELP:
        text = show_help_text(update)
        try:
            await edit_text_safe(
                q.edit_message_text,
                text,
                reply_markup=_main_menu_keyboard(user, chat),
                parse_mode="Markdown",
            )
        except Exception:
            try:
                await reply_text_safe(
                    q.message,
                    text,
                    reply_markup=_main_menu_keyboard(user, chat),
                    parse_mode="Markdown",
                )
            except Exception:
                await reply_text_safe(
                    q.message,
                    text,
                    reply_markup=_main_menu_keyboard(user, chat),
                )
        return

    # ---- TZ (таймзона) ----
    if data == CB_SET_TZ:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        tz = resolve_tz_for_chat(chat_id)
        text = f"🌍 Текущая TZ: *{tz.zone}*\nВыберите пресет или введите вручную."
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=tz_menu_kb(), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=tz_menu_kb(), parse_mode="Markdown")
        return

    if data == CB_SET_TZ_LOCAL:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        tz_name = os.environ.get("ORG_TZ") or get_localzone_name()
        update_chat_cfg(chat_id, tz=tz_name)
        await reply_text_safe(q.message, f"✅ TZ установлена: *{tz_name}*", parse_mode="Markdown")
        await ensure_panel(update, context)
        return

    if data == CB_SET_TZ_MOSCOW:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        tz_name = "Europe/Moscow"
        update_chat_cfg(chat_id, tz=tz_name)
        await reply_text_safe(q.message, f"✅ TZ установлена: *{tz_name}*", parse_mode="Markdown")
        await ensure_panel(update, context)
        return

    if data == CB_SET_TZ_CHICAGO:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        tz_name = "America/Chicago"
        update_chat_cfg(chat_id, tz=tz_name)
        await reply_text_safe(q.message, f"✅ TZ установлена: *{tz_name}*", parse_mode="Markdown")
        await ensure_panel(update, context)
        return

    if data == CB_SET_TZ_ENTER:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        context.user_data[AWAIT_TZ] = True
        note = await reply_text_safe(
            q.message,
            "✏️ Отправьте название таймзоны, например `Europe/Moscow`. Нажмите любую другую кнопку, чтобы отменить.",
            parse_mode="Markdown",
        )
        auto_delete(note, context, 60)
        return

    if data == CB_SET_OFFSET:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        offset = get_offset_for_chat(chat_id)
        text = f"⏳ Текущий оффсет: *{offset} мин*"
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=offset_menu_kb(), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=offset_menu_kb(), parse_mode="Markdown")
        return

    if data == CB_OFF_DEC:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        off = max(1, get_offset_for_chat(chat_id) - 5)
        update_chat_cfg(chat_id, offset=off)
        await reply_text_safe(q.message, f"✅ Оффсет: *{off} мин*", parse_mode="Markdown")
        await ensure_panel(update, context)
        return

    if data == CB_OFF_INC:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        off = min(1440, get_offset_for_chat(chat_id) + 5)
        update_chat_cfg(chat_id, offset=off)
        await reply_text_safe(q.message, f"✅ Оффсет: *{off} мин*", parse_mode="Markdown")
        await ensure_panel(update, context)
        return

    if data in (CB_OFF_PRESET_10, CB_OFF_PRESET_15, CB_OFF_PRESET_20, CB_OFF_PRESET_30):
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        preset_map = { CB_OFF_PRESET_10: 10, CB_OFF_PRESET_15: 15, CB_OFF_PRESET_20: 20, CB_OFF_PRESET_30: 30 }
        preset = preset_map[data]
        update_chat_cfg(chat_id, offset=preset)
        await reply_text_safe(q.message, f"✅ Оффсет: *{preset} мин*", parse_mode="Markdown")
        await ensure_panel(update, context)
        return

    # ---- ЧАТЫ ----
    if data == CB_CHATS:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        known = get_known_chats()
        text = "📋 Зарегистрированные чаты"
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=chats_menu_kb(known), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=chats_menu_kb(known), parse_mode="Markdown")
        return

    if data == CB_ARCHIVE:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        await _show_archive_view(q, context, page=1, can_manage=can_manage)
        return

    if data.startswith(f"{CB_ARCHIVE_PAGE}:"):
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        try:
            page = int(data.split(":", 1)[1])
        except Exception:
            page = 1
        await _show_archive_view(q, context, page=page, can_manage=can_manage)
        return

    if data == CB_ARCHIVE_CLEAR:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        text = "<b>Очистить архив?</b>\nЭто действие необратимо."
        try:
            await edit_text_safe(
                q.edit_message_text,
                text,
                reply_markup=archive_clear_confirm_kb(),
                parse_mode="HTML",
            )
        except Exception:
            await reply_text_safe(
                q.message,
                text,
                reply_markup=archive_clear_confirm_kb(),
                parse_mode="HTML",
            )
        return

    if data == CB_ARCHIVE_CLEAR_CONFIRM:
        if not can_manage:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        removed = clear_archive()
        notice = "Архив очищен." if removed else "Архив уже пуст."
        await _show_archive_view(q, context, page=1, can_manage=can_manage, notice=notice)
        return

    if data.startswith(f"{CB_CHAT_DEL}:"):
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        parts = data.split(":", 2)
        if len(parts) < 3:
            return
        sel = parts[1]
        topic = parts[2]
        topic_val = None if topic == "0" else int(topic)
        unregister_chat(sel, topic_val)
        removed_by = _serialize_user(user)
        affected = get_jobs_for_chat(sel, topic_val)
        for rec in affected:
            job_id = rec.get("job_id")
            if not job_id:
                continue
            jobs = context.job_queue.get_jobs_by_name(job_id)
            for job in jobs:
                job.schedule_removal()
            release_signature(rec.get("signature"))
            archive_job(
                job_id,
                rec=rec,
                reason="chat_unregistered",
                removed_by=removed_by,
            )
        known = get_known_chats()
        text = "🗑️ Чат удалён"
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=chats_menu_kb(known), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=chats_menu_kb(known), parse_mode="Markdown")
        return

    # ---- Меню действий по задаче ----
    if data.startswith(f"{CB_ACTIONS}:"):
        parts = data.split(":")
        job_id = parts[1] if len(parts) > 1 else None
        if not job_id:
            return
        rec = get_job_record(job_id)
        if not (rec and (is_admin(user) or rec.get("author_id") == uid)):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        if len(parts) == 3 and parts[2] == "close":
            try:
                await q.message.delete()
            except Exception:
                pass
            return
        text = f"*Действия*\n{escape_md(rec.get('text', ''))}"
        if q.message.reply_markup and q.message.text and q.message.text.startswith("*Действия*"):
            await edit_text_safe(q.edit_message_text, text, reply_markup=actions_kb(job_id, is_admin(user)), parse_mode="Markdown")
        else:
            msg = await reply_text_safe(q.message, text, reply_markup=actions_kb(job_id, is_admin(user)), parse_mode="Markdown")
            auto_delete(msg, context, 60)
        return

    # ---- МГНОВЕННАЯ ОТПРАВКА / ОТМЕНА / СДВИГ ----
    if data.startswith(f"{CB_SENDNOW}:"):
        parts = data.split(":")
        job_id = parts[1] if len(parts) > 1 else None
        rec = get_job_record(job_id) if job_id else None
        if not (rec and (is_admin(user) or rec.get("author_id") == uid)):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        if len(parts) == 2:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да", callback_data=f"{CB_SENDNOW}:{job_id}:y")],
                [InlineKeyboardButton("↩️ Назад", callback_data=f"{CB_ACTIONS}:{job_id}")],
            ])
            await edit_text_safe(q.edit_message_text, "Отправить напоминание сейчас?", reply_markup=kb)
            return
        jobs = context.job_queue.get_jobs_by_name(job_id)
        if jobs:
            jobs[0].schedule_removal()
        dummy_ctx = SimpleNamespace(
            job=SimpleNamespace(name=job_id, data=rec),
            job_queue=context.job_queue,
            application=context.application,
            bot=context.bot,
        )
        await send_reminder(dummy_ctx)
        msg = await edit_text_safe(q.edit_message_text, f"📤 Отправлено\n{rec.get('text','')}")
        auto_delete(msg, context)
        dummy = SimpleNamespace(
            effective_chat=SimpleNamespace(id=rec.get("source_chat_id", chat_id)),
            effective_message=None,
        )
        await ensure_panel(dummy, context)
        return

    if data.startswith(f"{CB_CANCEL}:"):
        parts = data.split(":")
        job_id = parts[1] if len(parts) > 1 else None
        rec = get_job_record(job_id) if job_id else None
        if not (rec and (is_admin(user) or rec.get("author_id") == uid)):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        if len(parts) == 2:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да", callback_data=f"{CB_CANCEL}:{job_id}:y")],
                [InlineKeyboardButton("↩️ Назад", callback_data=f"{CB_ACTIONS}:{job_id}")],
            ])
            await edit_text_safe(q.edit_message_text, "Отменить напоминание?", reply_markup=kb)
            return
        jobs = context.job_queue.get_jobs_by_name(job_id)
        if jobs:
            jobs[0].schedule_removal()
        if rec:
            release_signature(rec.get("signature"))
        removed = False
        if rec:
            removed = archive_job(
                job_id,
                rec=rec,
                reason="manual_cancel",
                removed_by=_serialize_user(user),
            )
        if not removed:
            remove_job_record(job_id)
        if rec and rec.get("confirm_chat_id") and rec.get("confirm_message_id"):
            try:
                await edit_text_safe(
                    context.bot.edit_message_text,
                    f"❌ *Отменено*\n{rec.get('text','')}",
                    chat_id=rec["confirm_chat_id"],
                    message_id=rec["confirm_message_id"],
                    parse_mode="Markdown",
                    where="bot.cancel.confirm",
                )
            except Exception:
                pass
        msg = await edit_text_safe(q.edit_message_text, "🗑️ Напоминание отменено")
        auto_delete(msg, context)
        audit_log(
            "REM_CANCELED",
            reminder_id=job_id,
            chat_id=rec.get("target_chat_id") if rec else None,
            topic_id=rec.get("topic_id") if rec else None,
            user_id=uid,
            title=rec.get("text") if rec else None,
            reason="manual",
        )
        dummy = SimpleNamespace(
            effective_chat=SimpleNamespace(id=rec.get("source_chat_id", chat_id)),
            effective_message=None,
        )
        await ensure_panel(dummy, context)
        return

    if data.startswith(f"{CB_SHIFT}:"):
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        try:
            _, job_id, minutes_str = data.split(":")
            minutes = int(minutes_str)
        except Exception:
            return await reply_text_safe(q.message, "Некорректный формат сдвига.")
        jobs = context.job_queue.get_jobs_by_name(job_id)
        rec = get_job_record(job_id)
        if not rec:
            return await reply_text_safe(q.message, "Задача не найдена.")
        payload = (jobs[0].data or {}) if jobs else {
            "target_chat_id": rec.get("target_chat_id"),
            "topic_id": rec.get("topic_id"),
            "text": rec.get("text"),
            "source_chat_id": rec.get("source_chat_id"),
            "target_title": rec.get("target_title"),
            "author_id": rec.get("author_id"),
            "author_username": rec.get("author_username"),
            "created_at_utc": rec.get("created_at_utc"),
        }
        rrule = rec.get("rrule", RR_ONCE)
        if jobs:
            jobs[0].schedule_removal()

        new_job_id = f"rem-{uuid.uuid4().hex}"
        new_run_at = (datetime.now(pytz.utc) + timedelta(minutes=minutes)).isoformat()
        context.job_queue.run_once(
            send_reminder,
            when=minutes * 60,
            name=new_job_id,
            data={**payload, "job_id": new_job_id},
            chat_id=payload.get("target_chat_id"),
        )
        remove_job_record(job_id)
        add_job_record({
            **payload,
            "job_id": new_job_id,
            "run_at_utc": new_run_at,
            "confirm_chat_id": rec.get("confirm_chat_id"),
            "confirm_message_id": rec.get("confirm_message_id"),
            "rrule": rrule,
        })

        if rec.get("confirm_chat_id") and rec.get("confirm_message_id"):
            try:
                await edit_text_safe(
                    context.bot.edit_message_text,
                    f"⏩ *Смещено* на +{minutes} мин\n{payload.get('text','')}",
                    chat_id=rec["confirm_chat_id"],
                    message_id=rec["confirm_message_id"],
                    reply_markup=job_kb(new_job_id, rrule),
                    parse_mode="Markdown",
                    where="bot.shift.confirm",
                )
            except Exception:
                pass

        msg2 = await reply_text_safe(q.message, 
            f"⏩ Смещено на +{minutes} мин. Новый id: `{new_job_id}`", parse_mode="Markdown"
        )
        auto_delete(msg2, context)
        audit_log(
            "REM_RESCHEDULED",
            reminder_id=new_job_id,
            previous_id=job_id,
            chat_id=payload.get("target_chat_id"),
            topic_id=payload.get("topic_id"),
            title=payload.get("text"),
            user_id=uid,
            when=new_run_at,
            reason="manual_shift",
        )
        dummy = SimpleNamespace(effective_chat=SimpleNamespace(id=payload.get("source_chat_id", chat_id)), effective_message=None)
        await ensure_panel(dummy, context)
        return

    # ---- Переключение RRULE ----
    if data.startswith(f"{CB_RRULE}:"):
        if not is_admin(user):
            return await reply_text_safe(q.message, "⛔ Нет доступа.")
        try:
            _, job_id, current = data.split(":")
        except Exception:
            return
        rec = get_job_record(job_id)
        if not rec:
            return await reply_text_safe(q.message, "Задача не найдена.")
        cycle = {RR_ONCE: RR_DAILY, RR_DAILY: RR_WEEKLY, RR_WEEKLY: RR_ONCE}
        new_rule = cycle.get(current, RR_ONCE)
        upsert_job_record(job_id, {"rrule": new_rule})
        try:
            await edit_text_safe(
                context.bot.edit_message_text,
                (
                    f"📌 *Запланировано*\n{rec.get('text','')}\n"
                    f"🔁 Повтор: *{'разово' if new_rule==RR_ONCE else ('ежедневно' if new_rule==RR_DAILY else 'еженедельно')}*"
                ),
                chat_id=rec["confirm_chat_id"],
                message_id=rec["confirm_message_id"],
                reply_markup=job_kb(job_id, new_rule) if is_admin(user) else None,
                parse_mode="Markdown",
                where="bot.rrule.confirm",
            )
        except Exception:
            pass
        await reply_text_safe(q.message, f"🔁 Режим повтора: *{new_rule}*", parse_mode="Markdown")
        return


# ==========================
# ----- КОЛБЭК ЗАДАЧИ -----
# ==========================


async def on_noop(update: Update, _context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    try:
        await q.answer("⏳ Уже выполняю…", cache_time=1)
    except Exception:
        pass


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.data:
        return

    data = q.data

    if data == CB_DISABLED or data.startswith(f"{CB_DISABLED}:"):
        try:
            await q.answer("⏳ Уже обрабатываю…", cache_time=1)
        except Exception:
            pass
        return

    key = _callback_lock_key(q)
    if not _acquire_callback_lock(context, key):
        try:
            await q.answer("⏳ Уже обрабатываю…", cache_time=1)
        except Exception:
            pass
        return

    idem_raw = f"{data}|{getattr(q, 'from_user', None) and q.from_user.id}".encode("utf-8", "ignore")
    idem_key = hashlib.blake2b(idem_raw, digest_size=12).hexdigest()
    acquired, entry = _start_idempotent(context, idem_key)
    if not acquired:
        try:
            if entry and entry.get("status") == "done":
                await q.answer("✅ Уже выполнено", cache_time=1)
            else:
                await q.answer("⏳ Уже обрабатываю…", cache_time=1)
        except Exception:
            pass
        _release_callback_lock(context, key)
        return

    try:
        await q.answer("Принято, готовлю…", cache_time=1)
    except Exception:
        pass

    await freeze_query_markup(q)

    async def _runner() -> None:
        try:
            await _handle_callback_body(update, context)
            _mark_idempotent_done(context, idem_key)
        except Exception:
            _reset_idempotent(context, idem_key)
            raise
        finally:
            _release_callback_lock(context, key)

    context.application.create_task(_runner())


async def send_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data or {}
    chat_id = data.get("target_chat_id")
    topic_id = data.get("topic_id")
    text = data.get("text")
    job_id = context.job.name if context.job else data.get("job_id")

    author_id = data.get("author_id")
    audit_log(
        "REM_FIRED",
        reminder_id=job_id,
        chat_id=chat_id,
        topic_id=topic_id,
        title=text,
        user_id=author_id,
    )

    if chat_id and text:
        queue = context.application.bot_data.get("send_queue")
        if queue:
            await queue.put((chat_id, text, topic_id))
        else:
            await safe_send_message(context, chat_id=chat_id, text=text, topic_id=topic_id)

    if job_id:
        rec = get_job_record(job_id)
        if rec:
            rrule = rec.get("rrule", RR_ONCE)
            next_iso = _rrule_next_iso(rec.get("run_at_utc", ""), rrule)
            if next_iso:
                try:
                    dt_next = datetime.fromisoformat(next_iso)
                    delay = (dt_next - datetime.now(pytz.utc)).total_seconds()
                    if delay < 0:
                        delay = 1
                    context.job_queue.run_once(
                        send_reminder,
                        when=delay,
                        name=job_id,
                        data={
                            "job_id": job_id,
                            "target_chat_id": rec.get("target_chat_id"),
                            "topic_id": rec.get("topic_id"),
                            "text": rec.get("text"),
                            "source_chat_id": rec.get("source_chat_id"),
                            "target_title": rec.get("target_title"),
                            "author_id": rec.get("author_id"),
                            "author_username": rec.get("author_username"),
                            "created_at_utc": rec.get("created_at_utc"),
                        },
                        chat_id=rec.get("target_chat_id"),
                    )
                    upsert_job_record(job_id, {"run_at_utc": dt_next.isoformat()})
                    audit_log(
                        "REM_RESCHEDULED",
                        reminder_id=job_id,
                        chat_id=rec.get("target_chat_id"),
                        topic_id=rec.get("topic_id"),
                        title=rec.get("text"),
                        repeat_next_at=dt_next.isoformat(),
                        reason="repeat",
                        user_id=rec.get("author_id"),
                    )
                    return
                except Exception as e:
                    error_log(
                        "REM_RESCHEDULE_FAILED",
                        where="bot.send_reminder",
                        message=str(e),
                        level=logging.WARNING,
                        reminder_id=job_id,
                    )
        if rec:
            release_signature(rec.get("signature"))
        removed = False
        if rec:
            removed = archive_job(job_id, rec=rec, reason="completed")
        if not removed:
            remove_job_record(job_id)
        # Обновить подтверждение и панель
        if rec and rec.get("confirm_chat_id") and rec.get("confirm_message_id"):
            try:
                await edit_text_safe(
                    context.bot.edit_message_text,
                    f"✅ Выполнено\n{rec.get('text','')}",
                    chat_id=rec["confirm_chat_id"],
                    message_id=rec["confirm_message_id"],
                    parse_mode="Markdown",
                    where="bot.reminder.done",
                )
            except Exception:
                pass
        src_chat = (rec and rec.get("source_chat_id")) or data.get("source_chat_id")
        if src_chat:
            dummy = SimpleNamespace(effective_chat=SimpleNamespace(id=src_chat), effective_message=None)
            await ensure_panel(dummy, context)


async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _set_log_user(update)
    change = update.my_chat_member
    if not change:
        return
    new_status = getattr(change.new_chat_member, "status", None)
    if new_status not in {"left", "kicked"}:
        return
    chat = change.chat
    if not chat:
        return
    chat_id = chat.id
    records = get_jobs_for_chat(chat_id)
    if not records:
        return
    removed_by = _serialize_user(change.from_user)
    reason = "bot_removed" if new_status in {"left", "kicked"} else "chat_removed"
    for rec in records:
        job_id = rec.get("job_id")
        if not job_id:
            continue
        jobs = context.job_queue.get_jobs_by_name(job_id)
        for job in jobs:
            job.schedule_removal()
        release_signature(rec.get("signature"))
        archive_job(job_id, rec=rec, reason=reason, removed_by=removed_by)


# ==========================
# ----- ПЛАНИРОВЩИК -----
# ==========================
async def schedule_reminder_core(
    text_in: str,
    cfg_chat_id: Union[int, str],
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    user: Optional[User],
    topic_override: int | None = None,
) -> None:
    tz = resolve_tz_for_chat(cfg_chat_id)
    parsed = parse_meeting_message(text_in, tz)
    if not parsed:
        msg = explain_format_error(text_in)
        await reply_text_safe(update.effective_message, msg, parse_mode="Markdown")
        return

    offset = get_offset_for_chat(cfg_chat_id)
    tgt_chat = cfg_chat_id
    topic_id = topic_override
    chat_context = update.effective_chat if update else None
    menu_markup = _main_menu_keyboard(user, chat_context)
    admin = is_admin(user)

    target_title = next(
        (
            c.get("title")
            for c in get_known_chats()
            if str(c.get("chat_id")) == str(tgt_chat)
            and int(c.get("topic_id", 0)) == int(topic_id or 0)
        ),
        str(tgt_chat),
    )
    if target_title == str(tgt_chat):
        try:
            chat_obj = await context.bot.get_chat(tgt_chat)
            target_title = chat_obj.title or (chat_obj.username and f"@{chat_obj.username}") or str(tgt_chat)
        except Exception:
            pass

    dt_local = parsed["dt_local"]
    canonical_full = parsed["canonical_full"]
    reminder_text = parsed["reminder_text"]
    reminder_dt_local = dt_local - timedelta(minutes=offset)
    is_past = reminder_dt_local <= datetime.now(tz)

    # Проверка на дубликат по тексту
    if find_job_by_text(reminder_text):
        await reply_text_safe(update.effective_message,
            "⚠️ Такая напоминалка уже есть.",
            reply_markup=menu_markup,
            parse_mode="Markdown",
        )
        return

    sig = make_signature(cfg_chat_id, canonical_full, dt_local)
    if not is_past and dedup_should_skip(sig):
        audit_log(
            "REM_DEDUP_SKIPPED",
            title=canonical_full,
            chat_id=cfg_chat_id,
            topic_id=topic_id,
            user_id=getattr(user, "id", None),
            reason="duplicate",
        )
        return

    now_utc = datetime.now(pytz.utc)
    reminder_utc = reminder_dt_local.astimezone(pytz.utc)
    delay_seconds = (reminder_utc - now_utc).total_seconds()
    job_id = f"rem-{uuid.uuid4().hex}"

    if delay_seconds <= 0:
        audit_log(
            "REM_SEND_NOW",
            chat_id=tgt_chat,
            topic_id=topic_id,
            title=reminder_text,
            user_id=getattr(user, "id", None),
        )
        try:
            await safe_send_message(
                context,
                chat_id=tgt_chat,
                text=reminder_text,
                topic_id=topic_id,
            )
            await reply_text_safe(update.effective_message,
                f"✅ Напоминание уже должно было быть — отправил сразу в "
                f"{'этот чат' if tgt_chat == cfg_chat_id else 'выбранный чат'}.\n"
                f"{canonical_full}",
                reply_markup=menu_markup,
                parse_mode="Markdown",
                fast_retry=False,
            )
            dummy = SimpleNamespace(effective_chat=SimpleNamespace(id=cfg_chat_id), effective_message=None)
            await ensure_panel(dummy, context)
        except Exception as e:
            error_log(
                "REM_SEND_NOW_FAILED",
                where="bot.schedule_reminder_core",
                message="Immediate send failed",
                level=logging.ERROR,
                exc_info=True,
                chat_id=tgt_chat,
                topic_id=topic_id,
                title=reminder_text,
                error=str(e),
            )
            await reply_text_safe(update.effective_message,
                "⚠️ Не удалось отправить напоминание сразу.\n"
                f"Ошибка: `{e}`\nПроверьте права бота и настройки выбранного чата/темы.",
                parse_mode="Markdown",
                fast_retry=False,
            )
        finally:
            release_signature(sig)
        return

    if delay_seconds <= 2:
        error_log(
            "REM_SCHEDULE_ADJUSTED",
            where="bot.schedule_reminder_core",
            message="Задача скорректирована из-за слишком малого интервала",
            level=logging.WARNING,
            delay=round(delay_seconds, 2),
        )
        delay_seconds = 5
        reminder_utc = datetime.now(pytz.utc) + timedelta(seconds=delay_seconds)

    job_data = {
        "job_id": job_id,
        "target_chat_id": tgt_chat,
        "topic_id": topic_id,
        "text": reminder_text,
        "source_chat_id": cfg_chat_id,
        "target_title": target_title,
        "author_id": user.id,
        "author_username": getattr(user, "username", None),
        "created_at_utc": datetime.now(pytz.utc).isoformat(),
        "signature": sig,
    }
    context.job_queue.run_once(
        send_reminder,
        when=delay_seconds,
        name=job_id,
        data=job_data,
        chat_id=tgt_chat,
    )
    add_job_record({
        **job_data,
        "run_at_utc": reminder_utc.isoformat(),
        "rrule": RR_ONCE,
    })
    audit_log(
        "REM_SCHEDULED",
        reminder_id=job_id,
        chat_id=tgt_chat,
        topic_id=topic_id,
        user_id=getattr(user, "id", None),
        title=reminder_text,
        when=reminder_utc.isoformat(),
        tz=tz.zone,
        delay_sec=round(delay_seconds, 1),
    )

    kb = job_kb(job_id, RR_ONCE) if admin else None
    confirm = await reply_text_safe(update.effective_message,
        f"📌 *Запланировано для* *{target_title}* на *{reminder_dt_local.strftime('%d.%m %H:%M')}* (TZ: {tz.zone})\n"
        f"{canonical_full}\n"
        "🔁 Повтор: *разово* (нажмите, чтобы изменить)",
        reply_markup=kb,
        parse_mode="Markdown",
        fast_retry=False,
    )
    upsert_job_record(
        job_id,
        {"confirm_chat_id": confirm.chat.id, "confirm_message_id": confirm.message_id},
    )
    dummy = SimpleNamespace(effective_chat=SimpleNamespace(id=cfg_chat_id), effective_message=None)
    await ensure_panel(dummy, context)

# ==========================
# ----- КОМАНДЫ И ТЕКСТ -----
# ==========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _set_log_user(update)
    user = update.effective_user
    chat = update.effective_chat
    text = menu_text_for(chat.id)
    menu_markup = _main_menu_keyboard(user, chat)
    await reply_text_safe(
        update.message,
        text,
        reply_markup=menu_markup,
        parse_mode="Markdown",
    )
    if chat.type == "private":
        await safe_send_message(
            context,
            chat_id=chat.id,
            text="⌨️ Быстрые кнопки доступны под строкой ввода.",
            reply_markup=_reply_menu_keyboard(user, chat),
            fast_retry=False,
        )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _set_log_user(update)
    user = update.effective_user
    text = show_help_text(update)
    chat = update.effective_chat
    menu_markup = _main_menu_keyboard(user, chat)
    try:
        await reply_text_safe(
            update.message,
            text,
            reply_markup=menu_markup,
            parse_mode="Markdown",
        )
    except Exception:
        await reply_text_safe(
            update.message,
            text,
            reply_markup=menu_markup,
        )

async def cmd_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _set_log_user(update)
    if not is_admin(update.effective_user):
        msg = await reply_text_safe(update.message, "⛔ Только администратор может регистрировать чаты.")
        auto_delete(msg, context)
        return
    chat = update.effective_chat
    if chat.type == "private":
        msg = await reply_text_safe(update.message, "ℹ️ Личные диалоги регистрировать не нужно.")
        auto_delete(msg, context)
        return
    msg = update.message
    topic_id = getattr(msg, "message_thread_id", None)
    topic_title = None
    if topic_id is not None:
        try:
            topic = await context.bot.get_forum_topic(chat.id, topic_id)
            topic_title = topic.name
        except Exception:
            topic_title = str(topic_id)
    title = chat.title or (chat.username and f"@{chat.username}") or str(chat.id)
    if topic_id is not None:
        display_title = f"{title} / {topic_title}" if topic_title else f"{title} / {topic_id}"
    else:
        display_title = title
    added = register_chat(chat.id, display_title, topic_id, topic_title)
    note = "✅ Чат добавлен в список." if added else "ℹ️ Этот чат уже зарегистрирован."
    await reply_text_safe(update.message, note)


async def _get_valid_last_target(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> Optional[Dict[str, Any]]:
    entry = context.user_data.get("last_target") if context and getattr(context, "user_data", None) else None
    if not isinstance(entry, dict):
        return None
    chat_id = entry.get("chat_id")
    topic_id = entry.get("topic_id")
    if chat_id is None:
        return None
    message = update.effective_message
    if message and chat_id == message.chat.id and int(topic_id or 0) == int(getattr(message, "message_thread_id", 0) or 0):
        return {"chat_id": chat_id, "topic_id": topic_id}

    match = next(
        (
            candidate
            for candidate in get_known_chats()
            if str(candidate.get("chat_id")) == str(chat_id)
            and int(candidate.get("topic_id") or 0) == int(topic_id or 0)
        ),
        None,
    )
    if not match:
        context.user_data.pop("last_target", None)
        return None

    user = update.effective_user
    if isinstance(chat_id, int) and user:
        try:
            member = await context.bot.get_chat_member(chat_id, user.id)
        except Exception:
            context.user_data.pop("last_target", None)
            return None
        if member and member.status not in ("left", "kicked"):
            return {"chat_id": chat_id, "topic_id": topic_id}
        context.user_data.pop("last_target", None)
        return None

    return {"chat_id": chat_id, "topic_id": topic_id}


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _set_log_user(update)
    if not update.message or not update.message.text:
        return
    text_in = update.message.text.strip()
    chat = update.effective_chat
    chat_id = chat.id
    user = update.effective_user
    uid = user.id
    menu_markup = _main_menu_keyboard(user, chat)

    normalized = text_in.lower()
    quick_actions = {
        "активные": "active",
        "📝 активные": "active",
        "справка": "help",
        "❓ справка": "help",
        "➕ создать встречу": "create",
        "+ создать встречу": "create",
        "🆕 создать встречу": "create",
        "создать встречу": "create",
    }
    action = quick_actions.get(normalized)
    if action == "active":
        await _cancel_previous_action(update.message, context)
        if not is_admin(user):
            note = await reply_text_safe(update.message, "⛔ Только администратор может просматривать активные напоминания.")
            auto_delete(note, context)
            return
        await _send_active_overview_message(update.message, context, chat_id, user)
        return
    if action == "help":
        await _cancel_previous_action(update.message, context)
        await cmd_help(update, context)
        return
    if action == "create":
        await _cancel_previous_action(update.message, context)
        context.user_data["force_pick"] = True
        hint = create_reminder_hint(chat_id)
        await reply_text_safe(
            update.message,
            hint,
            reply_markup=menu_markup,
            parse_mode="Markdown",
        )
        return

    # ожидания ввода
    if context.user_data.get(AWAIT_TZ):
        try:
            pytz.timezone(text_in)
        except Exception:
            return await reply_text_safe(update.message, "Некорректная TZ. Пример: `Europe/Moscow`", parse_mode="Markdown")
        update_chat_cfg(chat_id, tz=text_in)
        context.user_data.pop(AWAIT_TZ, None)
        await reply_text_safe(update.message, f"✅ TZ установлена: *{text_in}*", parse_mode="Markdown")
        return

    if context.user_data.get(AWAIT_ADMIN):
        context.user_data.pop(AWAIT_ADMIN, None)
        if not is_owner(user):
            return
        uname = text_in.lstrip("@").strip().lower()
        if not uname:
            await reply_text_safe(update.message, "Нужен логин вида @username")
            return
        added = add_admin_username(uname)
        if added:
            await reply_text_safe(update.message, f"✅ @{uname} теперь админ")
        else:
            await reply_text_safe(update.message, "⚠️ Не удалось добавить (возможно, уже есть)")
        text = render_admins_text(ADMIN_USERNAMES)
        await reply_text_safe(update.message, 
            text,
            reply_markup=admins_menu_kb(ADMIN_USERNAMES),
            parse_mode="Markdown",
        )
        return

    # Ключевые фразы
    if text_in.lower() in {"меню", "menu"}:
        return await cmd_start(update, context)

    # Парсим встречу с учётом типа чата
    if chat.type == "private":
        tz_preview = resolve_tz_for_chat(chat_id)
        looks_like_reminder = parse_meeting_message(text_in, tz_preview) is not None
        last_target = await _get_valid_last_target(update, context)
        force_pick = bool(context.user_data.get("force_pick"))

        if looks_like_reminder and last_target and not force_pick:
            await schedule_reminder_core(
                text_in,
                last_target.get("chat_id"),
                update,
                context,
                user,
                topic_override=last_target.get("topic_id"),
            )
            context.user_data["force_pick"] = False
            context.user_data["last_target"] = last_target
            return

        candidates = []
        if looks_like_reminder:
            for c in get_known_chats():
                cid = c.get("chat_id")
                try:
                    member = await context.bot.get_chat_member(cid, uid)
                    if member.status not in ("left", "kicked"):
                        candidates.append(c)
                except Exception:
                    continue
            if candidates and (force_pick or not last_target):
                token = uuid.uuid4().hex
                context.user_data.setdefault("pending_reminders", {})[token] = {"text": text_in}
                candidates.append({"chat_id": chat_id, "title": "Личный чат"})
                return await reply_text_safe(update.message,
                    "📨 Куда отправить напоминание?",
                    reply_markup=choose_chat_kb(candidates, token, is_admin=is_admin(user)),
                )

        await schedule_reminder_core(text_in, chat_id, update, context, user)
        if looks_like_reminder:
            context.user_data["last_target"] = {"chat_id": chat_id, "topic_id": None}
            context.user_data["force_pick"] = False
        return

    if chat.type in ("group", "supergroup"):
        title = chat.title or (chat.username and f"@{chat.username}") or str(chat.id)
        register_chat(chat.id, title, topic_id=update.message.message_thread_id)
    await schedule_reminder_core(
        text_in,
        chat_id,
        update,
        context,
        user,
        topic_override=update.message.message_thread_id,
    )
# ==========================
# ----- ВОССТАНОВЛЕНИЕ ЗАДАЧ ПРИ СТАРТЕ -----
# ==========================
def restore_jobs(app: Application):
    items = get_jobs_store()
    now_utc = datetime.now(pytz.utc)
    restored = 0
    kept = []
    caught_up = 0
    for r in items:
        try:
            run_at = datetime.fromisoformat(r["run_at_utc"])
        except Exception:
            continue
        delay = (run_at - now_utc).total_seconds()
        if delay <= 0:
            if delay >= -CATCHUP_WINDOW_SECONDS:
                app.job_queue.run_once(
                    send_reminder,
                    when=1,
                    name=r["job_id"],
                    data={
                        "job_id": r["job_id"],
                        "target_chat_id": r["target_chat_id"],
                        "topic_id": r.get("topic_id"),
                        "text": r["text"],
                        "source_chat_id": r.get("source_chat_id"),
                    },
                    chat_id=r["target_chat_id"],
                )
                caught_up += 1
            continue
        app.job_queue.run_once(
            send_reminder,
            when=delay,
            name=r["job_id"],
            data={
                "job_id": r["job_id"],
                "target_chat_id": r["target_chat_id"],
                "topic_id": r.get("topic_id"),
                "text": r["text"],
                "source_chat_id": r.get("source_chat_id"),
            },
            chat_id=r["target_chat_id"],
        )
        kept.append(r)
        restored += 1
    set_jobs_store(kept)
    app_log("восстановление завершено", restored=restored, caught_up=caught_up)


# ==========================
# ----- ИНИЦИАЛИЗАЦИЯ: setMyCommands -----
# ==========================
async def post_init(app: Application):
    cmds = [
        BotCommand("start", "Приветствие и главное меню"),
        BotCommand("menu", "Главное меню"),
        BotCommand("help", "Справка по формату и функциям"),
        BotCommand("register", "Добавить этот чат в список"),
    ]
    try:
        await app.bot.set_my_commands(cmds)
        app_log("команды зарегистрированы")
    except Exception as e:
        error_log(
            "SET_COMMANDS_FAILED",
            where="bot.post_init",
            message=str(e),
            level=logging.WARNING,
        )
    app.bot_data.setdefault("send_queue", asyncio.Queue())
    if "send_worker_job" not in app.bot_data:
        app.bot_data["send_worker_job"] = app.job_queue.run_repeating(
            process_send_queue,
            interval=SEND_INTERVAL,
            first=0.0,
            name="send-worker",
            job_kwargs={"max_instances": 2, "coalesce": True, "misfire_grace_time": 10},
        )
    needs_cleanup = any(
        value > 0
        for value in (
            APP_LOG_RETENTION_DAYS,
            AUDIT_LOG_RETENTION_DAYS,
            ERROR_LOG_BACKUP_COUNT,
        )
    )
    if needs_cleanup and "log_cleanup_job" not in app.bot_data:
        app.bot_data["log_cleanup_job"] = app.job_queue.run_repeating(
            cleanup_logs_job,
            interval=24 * 60 * 60,
            first=60.0,
            name="log-cleanup",
        )


# ==========================
# ----- ГЛАВНЫЙ ЦИКЛ -----
# ==========================
def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Bot token not provided. Set TELEGRAM_BOT_TOKEN.")

    timeout = Timeout(connect=10.0, read=30.0, write=30.0, pool=5.0)
    request = HTTPXRequest(  # WHY: явное задание таймаутов совместимо с PTB 22.3
        http_version="1.1",
        connect_timeout=timeout.connect,
        read_timeout=timeout.read,
        write_timeout=timeout.write,
        pool_timeout=timeout.pool,
    )
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)  # WHY: увеличенные таймауты уменьшают ReadTimeout при сетевых лагах
        .post_init(post_init)
        .build()
    )

    # команды
    app.add_handler(CommandHandler(["start", "menu"], cmd_start, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler(["help"], cmd_help, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler(["register"], cmd_register, filters=filters.ChatType.GROUPS))
    # колбэки
    app.add_handler(CallbackQueryHandler(on_noop, pattern=rf"^{CB_DISABLED}(?::.*)?$"))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(ChatMemberHandler(on_my_chat_member, chat_member_types=ChatMemberHandler.MY_CHAT_MEMBER))
    # текст
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
            handle_message,
        )
    )
    app.add_error_handler(on_application_error)

    # восстановление задач
    restore_jobs(app)

    app_log("бот запущен", v=VERSION)
    app.run_polling()
    app_log("корректная остановка")
