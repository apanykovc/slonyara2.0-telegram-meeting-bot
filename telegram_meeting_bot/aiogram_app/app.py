from __future__ import annotations

import asyncio
import json
import logging
import tempfile
import time
import uuid
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional
from zipfile import ZIP_DEFLATED, ZipFile

import pytz
from html import escape
from aiohttp import ClientError
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    ChatMemberUpdated,
    FSInputFile,
    InlineKeyboardMarkup,
    Message,
    User,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from tzlocal import get_localzone_name

from telegram_meeting_bot.core import constants, logs as log_utils, storage
from telegram_meeting_bot.core.audit import audit_log
from telegram_meeting_bot.core import feature_flags, release_history
from telegram_meeting_bot.core.logging_setup import setup_logging
from telegram_meeting_bot.core.parsing import parse_meeting_message
from telegram_meeting_bot.ui import keyboards as ui_kb, texts as ui_txt


CB_NOOP = getattr(constants, "CB_NOOP", None) or getattr(constants, "CB_DISABLED", "noop")


logger = setup_logging()

router = Router()
scheduler = AsyncIOScheduler(timezone=timezone.utc)

STATE_AWAIT_TZ = "await_tz"
STATE_AWAIT_ADMIN_ADD = "await_admin"
STATE_AWAIT_ADMIN_DEL = "await_admin_del"
STATE_PENDING = "pending_reminders"
STATE_REPLY_MENU_SHOWN = "reply_menu_shown"
STATE_FORCE_PICK = "force_pick"
STATE_LAST_TARGET = "last_target"
CHAT_PICKER_PAGE_SIZE = 12
PENDING_PICK_TTL_SECONDS = 30 * 60
PENDING_PICK_MAX_ITEMS = 100
DEBOUNCE_TTL_SECONDS = 10 * 60
DEBOUNCE_MAX_USERS = 5000
SEND_RETRY_DELAY_MINUTES = 2


REPLY_MENU_ACTIONS = {
    "menu": {"меню"},
    "create": {"➕ создать встречу", "+ создать встречу", "🆕 создать встречу", "создать встречу"},
    "my": {"📂 мои встречи", "мои встречи"},
    "active": {"📝 активные", "активные"},
    "admin_panel": {"🛡️ админ-панель", "админ-панель"},
    "settings": {"⚙️ настройки", "настройки"},
    "help": {"❓ справка", "справка"},
}

REPLY_MENU_ALIASES = {
    alias.casefold(): action
    for action, aliases in REPLY_MENU_ACTIONS.items()
    for alias in aliases
}


class ErrorsMiddleware:
    async def __call__(self, handler, event, data):  # type: ignore[override]
        try:
            return await handler(event, data)
        except Exception as exc:  # pragma: no cover - defensive layer
            logger.exception("Unhandled error", exc_info=exc)
            message = getattr(event, "message", None)
            if isinstance(message, Message):
                with suppress(Exception):
                    await _answer_safe(message, "⚠️ Что-то пошло не так. Уже разбираюсь.")
            return None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_admin(user: Optional[User]) -> bool:
    if user is None:
        return False
    if user.id in constants.ADMIN_IDS:
        return True
    username = (user.username or "").lower().lstrip("@")
    if not username:
        return False
    if username in constants.ADMIN_USERNAMES:
        return True
    owners = getattr(constants, "OWNER_USERNAMES", {"panykovc"})
    return username in owners or username == "panykovc"


def _is_owner(user: Optional[User]) -> bool:
    if user is None:
        return False
    username = (user.username or "").lower().lstrip("@")
    owners = getattr(constants, "OWNER_USERNAMES", {"panykovc"})
    return username in owners


def _can_manage_settings(user: Optional[User], chat: Optional[Any]) -> bool:
    chat_type = _chat_kind(chat)
    if chat_type == "private":
        return True
    return _is_admin(user)


def _chat_kind(chat: Optional[Any]) -> str:
    if chat is None:
        return ""
    raw = getattr(chat, "type", None)
    value = getattr(raw, "value", raw)
    return str(value).lower() if value is not None else ""


def _is_private_chat(chat: Optional[Any]) -> bool:
    return _chat_kind(chat) == "private"


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


def _remember_owner_if_needed(user: Optional[User]) -> None:
    if user is None:
        return
    storage.remember_user_id(user.username, user.id)


def _build_admin_status() -> str:
    revision = release_history.current_revision()
    flags_view = feature_flags.list_flags_ordered()
    owner_ids = sorted(storage.get_owner_user_ids({"panykovc"}))
    jobs_total = len(storage.get_jobs_store())
    chats_total = len(storage.get_known_chats())
    history = release_history.get_history(limit=1)
    last_start = history[-1]["ts"] if history else "n/a"
    lines = [
        "🛡️ <b>Состояние админ-панели</b>",
        f"Версия: <code>{escape(str(revision.get('version') or constants.VERSION))}</code>",
        f"Коммит: <code>{escape(str(revision.get('commit') or 'n/a'))}</code>",
        f"Ветка: <code>{escape(str(revision.get('branch') or 'n/a'))}</code>",
        f"Есть локальные изменения: <code>{escape(str(bool(revision.get('dirty'))))}</code>",
        f"Активных задач: <b>{jobs_total}</b>",
        f"Зарегистрированных чатов: <b>{chats_total}</b>",
        f"ID владельца (panykovc): <code>{', '.join(map(str, owner_ids)) if owner_ids else 'не найден'}</code>",
        f"Последний запуск: <code>{escape(str(last_start))}</code>",
        "",
        "<b>Флаги</b>",
    ]
    for name, value, label, _ in flags_view:
        lines.append(
            f"• {escape(label)}: <b>{'ВКЛ' if value else 'ВЫКЛ'}</b> "
            f"<code>({escape(name)})</code>"
        )
    return "\n".join(lines)


def _verify_db_issues() -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for rec in storage.get_jobs_store():
        job_id = rec.get("job_id")
        run_at = rec.get("run_at_utc")
        if not isinstance(run_at, str) or not run_at:
            issues.append({"job_id": job_id, "issue": "missing_run_at_utc"})
        else:
            try:
                datetime.fromisoformat(run_at)
            except ValueError:
                issues.append({"job_id": job_id, "issue": "invalid_run_at_utc", "value": run_at})
        signature = rec.get("signature")
        if not isinstance(signature, str) or not signature:
            issues.append({"job_id": job_id, "issue": "missing_signature"})
    return issues


def _build_admin_verify_text() -> str:
    jobs = storage.get_jobs_store()
    issues = _verify_db_issues()
    lines = [
        "🧪 <b>Проверка БД</b>",
        f"Всего задач: <b>{len(jobs)}</b>",
        f"Проблем: <b>{len(issues)}</b>",
    ]
    if issues:
        lines.append("")
        lines.append("<b>Первые проблемы:</b>")
        for issue in issues[:10]:
            job_id = escape(str(issue.get("job_id")))
            reason = escape(str(issue.get("issue")))
            value = issue.get("value")
            if value is None:
                lines.append(f"• <code>{job_id}</code>: {reason}")
            else:
                lines.append(f"• <code>{job_id}</code>: {reason} (<code>{escape(str(value))}</code>)")
    return "\n".join(lines)


def _build_admin_history_text(limit: int = 8) -> str:
    items = release_history.get_history(limit=limit)
    lines = ["🧬 <b>История запусков</b>"]
    if not items:
        lines.append("Пока нет записей.")
        return "\n".join(lines)
    for item in reversed(items):
        lines.append(
            f"• <code>{escape(str(item.get('ts')))}</code> | "
            f"v=<code>{escape(str(item.get('version')))}</code> "
            f"commit=<code>{escape(str(item.get('commit') or 'n/a'))}</code> "
            f"dirty=<code>{escape(str(item.get('dirty')))}</code>"
        )
    return "\n".join(lines)


def _build_data_backup() -> Path:
    tmp_dir = Path(tempfile.mkdtemp(prefix="tmb-backup-"))
    archive = tmp_dir / f"backup-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.zip"
    data_dir = constants.DATA_DIR
    files = [p for p in data_dir.rglob("*") if p.is_file()]
    with ZipFile(archive, "w", compression=ZIP_DEFLATED) as zf:
        manifest = {
            "ts": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "version": constants.VERSION,
            "revision": release_history.current_revision(),
            "files": [str(p.relative_to(data_dir)) for p in files],
        }
        zf.writestr("MANIFEST.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        for path in files:
            zf.write(path, arcname=str(Path("data") / path.relative_to(data_dir)))
    return archive


def _paginate_jobs(
    page: int,
    page_size: int,
    *,
    predicate: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> tuple[list[Dict[str, Any]], int, int]:
    jobs_all = list(storage.get_jobs_store())
    if predicate is not None:
        jobs_filtered = [job for job in jobs_all if predicate(job)]
    else:
        jobs_filtered = jobs_all

    def sort_key(job: Dict[str, Any]) -> tuple[float, str, str, str]:
        run_iso = job.get("run_at_utc")
        timestamp = float("inf")
        if isinstance(run_iso, str) and run_iso:
            try:
                run_at = datetime.fromisoformat(run_iso)
            except ValueError:
                run_at = None
            if run_at is not None:
                if run_at.tzinfo is None:
                    run_at = run_at.replace(tzinfo=timezone.utc)
                timestamp = run_at.astimezone(timezone.utc).timestamp()
        title = job.get("target_title") or str(job.get("target_chat_id") or "")
        text = job.get("text") or ""
        job_id = job.get("job_id") or ""
        return timestamp, title, text, job_id

    jobs_filtered.sort(key=sort_key)
    total = len(jobs_filtered)
    pages_total = max(1, (total + page_size - 1) // page_size) if total else 1
    page = max(1, min(page, pages_total))
    start = (page - 1) * page_size
    chunk = jobs_filtered[start : start + page_size]
    return chunk, total, pages_total


def _normalize_active_filter_mode(raw: Any) -> str:
    mode = str(raw or constants.ACTIVE_FILTER_ALL).strip().lower()
    allowed = {
        constants.ACTIVE_FILTER_ALL,
        constants.ACTIVE_FILTER_TODAY,
        constants.ACTIVE_FILTER_TOMORROW,
        constants.ACTIVE_FILTER_WEEK,
    }
    if mode not in allowed:
        return constants.ACTIVE_FILTER_ALL
    return mode


def _parse_page_and_filter(data: str, page_prefix: str) -> tuple[int, str]:
    if not data.startswith(f"{page_prefix}:"):
        return 1, constants.ACTIVE_FILTER_ALL
    parts = data.split(":")
    page = 1
    if len(parts) >= 2:
        try:
            page = int(parts[1])
        except ValueError:
            page = 1
    mode = constants.ACTIVE_FILTER_ALL
    if len(parts) >= 3:
        mode = _normalize_active_filter_mode(parts[2])
    return page, mode


def _job_matches_filter_mode(job: Dict[str, Any], filter_mode: str) -> bool:
    mode = _normalize_active_filter_mode(filter_mode)
    if mode == constants.ACTIVE_FILTER_ALL:
        return True

    run_iso = job.get("run_at_utc")
    if not isinstance(run_iso, str) or not run_iso:
        return False
    try:
        run_at = datetime.fromisoformat(run_iso)
    except ValueError:
        return False
    if run_at.tzinfo is None:
        run_at = run_at.replace(tzinfo=timezone.utc)

    chat_id = _extract_chat_id(job.get("target_chat_id"))
    tz = storage.resolve_tz_for_chat(chat_id) if chat_id is not None else pytz.utc
    run_local = run_at.astimezone(tz)
    now_local = _utc_now().astimezone(tz)
    today = now_local.date()
    job_day = run_local.date()

    if mode == constants.ACTIVE_FILTER_TODAY:
        return job_day == today
    if mode == constants.ACTIVE_FILTER_TOMORROW:
        return job_day == today + timedelta(days=1)
    if mode == constants.ACTIVE_FILTER_WEEK:
        return today <= job_day <= today + timedelta(days=6)
    return True


def _active_filter_title_suffix(filter_mode: str) -> str:
    mode = _normalize_active_filter_mode(filter_mode)
    mapping = {
        constants.ACTIVE_FILTER_ALL: "",
        constants.ACTIVE_FILTER_TODAY: " · Сегодня",
        constants.ACTIVE_FILTER_TOMORROW: " · Завтра",
        constants.ACTIVE_FILTER_WEEK: " · 7 дней",
    }
    return mapping.get(mode, "")


def _schedule_job(job_id: str, run_at: datetime) -> None:
    scheduler.add_job(
        send_reminder_job,
        trigger=DateTrigger(run_date=run_at.astimezone(timezone.utc)),
        id=job_id,
        kwargs={"job_id": job_id},
        replace_existing=True,
    )


_RETRYABLE_TELEGRAM_ERRORS = (
    TelegramNetworkError,
    ClientError,
    asyncio.TimeoutError,
    OSError,
)


async def _telegram_call(
    action: Callable[[], Awaitable[Any]],
    *,
    description: str,
    swallow_bad_request: bool = False,
    retries: int = 3,
    base_delay: float = 0.75,
    bad_request_handler: Optional[Callable[[TelegramBadRequest], None]] = None,
    raise_on_failure: bool = False,
    on_give_up: Optional[Callable[[Exception], Awaitable[None]]] = None,
) -> Any:
    """Execute Telegram API call with retries and detailed logging."""

    for attempt in range(1, max(1, retries) + 1):
        try:
            return await action()
        except TelegramRetryAfter as exc:
            wait = float(getattr(exc, "retry_after", base_delay) or base_delay)
            logger.warning(
                "%s rate limited, sleeping for %.2fs (attempt %s/%s)",
                description,
                wait,
                attempt,
                retries,
            )
            await asyncio.sleep(wait)
        except TelegramBadRequest as exc:
            if bad_request_handler:
                try:
                    bad_request_handler(exc)
                except Exception:  # pragma: no cover - defensive logging
                    logger.exception("%s bad request handler failed", description)
                return None
            if swallow_bad_request:
                logger.warning("%s bad request: %s", description, exc)
                return None
            raise
        except _RETRYABLE_TELEGRAM_ERRORS as exc:
            if attempt >= retries:
                logger.error(
                    "%s failed after %s attempts: %s",
                    description,
                    attempt,
                    exc,
                )
                if on_give_up is not None:
                    with suppress(Exception):
                        await on_give_up(exc)
                if raise_on_failure:
                    raise
                return None
            logger.warning(
                "%s failed (attempt %s/%s): %s",
                description,
                attempt,
                retries,
                exc,
            )
            await asyncio.sleep(base_delay * attempt)
        except Exception:
            logger.exception("%s unexpected error", description)
            if raise_on_failure:
                raise
            return None


async def _send_safe(bot: Bot, chat_id: int | str, text: str, *, message_thread_id: Optional[int] = None) -> bool:
    async def _call() -> Any:
        return await bot.send_message(
            chat_id=chat_id, text=text, message_thread_id=message_thread_id
        )

    def _handle_bad_request(exc: TelegramBadRequest) -> None:
        details = str(exc).lower()
        if "kicked" in details:
            logger.warning("Bot removed from chat %s", chat_id)
        else:
            logger.warning("Failed to send message to %s: %s", chat_id, exc)

    async def _on_failure(exc: Exception) -> None:
        logger.warning(
            "bot.send_message to %s failed permanently: %s",
            chat_id,
            exc,
        )

    result = await _telegram_call(
        _call,
        description="bot.send_message",
        swallow_bad_request=True,
        bad_request_handler=_handle_bad_request,
        on_give_up=_on_failure,
    )
    return result is not None


async def _answer_safe(message: Message, *args, **kwargs) -> Any:
    async def _on_failure(exc: Exception) -> None:
        logger.warning(
            "message.answer failed for chat %s message %s: %s",
            getattr(message.chat, "id", None),
            message.message_id,
            exc,
        )

    return await _telegram_call(
        lambda: message.answer(*args, **kwargs),
        description="message.answer",
        raise_on_failure=False,
        on_give_up=_on_failure,
    )


async def _edit_text_safe(message: Message, *args, **kwargs) -> Any:
    parse_mode = kwargs.get("parse_mode")
    reply_markup = kwargs.get("reply_markup")

    if args:
        text = args[0]
    else:
        text = kwargs.get("text")

    def _current_text() -> Optional[str]:
        if parse_mode == ParseMode.HTML:
            return message.html_text
        if parse_mode == ParseMode.MARKDOWN:
            return message.text
        return message.text

    def _dump_markup(kb: Optional[InlineKeyboardMarkup]) -> Optional[tuple]:
        if kb is None:
            return None
        try:
            return tuple(tuple(repr(btn) for btn in row) for row in kb.inline_keyboard)
        except (AttributeError, TypeError):
            return None

    if text is not None:
        current = _current_text()
        if current == text:
            current_markup = _dump_markup(message.reply_markup)
            new_markup = _dump_markup(reply_markup)
            if current_markup == new_markup:
                logger.debug(
                    "Skip editing message %s in chat %s: content unchanged",
                    message.message_id,
                    getattr(message.chat, "id", None),
                )
                return message

    async def _on_failure(exc: Exception) -> None:
        logger.warning(
            "message.edit_text failed for chat %s message %s: %s",
            getattr(message.chat, "id", None),
            message.message_id,
            exc,
        )

    try:
        return await _telegram_call(
            lambda: message.edit_text(*args, **kwargs),
            description="message.edit_text",
            swallow_bad_request=False,
            on_give_up=_on_failure,
        )
    except TelegramBadRequest as exc:
        details = str(exc).lower()
        if "message is not modified" in details:
            logger.debug(
                "Skip editing message %s: Telegram says not modified",
                message.message_id,
            )
            return message
        raise


async def _callback_answer_safe(query: CallbackQuery, *args, **kwargs) -> Any:
    async def _on_failure(exc: Exception) -> None:
        logger.warning(
            "callback.answer failed for chat %s query %s: %s",
            getattr(getattr(query.message, "chat", None), "id", None),
            query.id,
            exc,
        )

    return await _telegram_call(
        lambda: query.answer(*args, **kwargs),
        description="callback.answer",
        swallow_bad_request=True,
        on_give_up=_on_failure,
    )


def _ack_callback_background(query: CallbackQuery, *args, **kwargs) -> asyncio.Task[Any]:
    async def _run() -> None:
        try:
            await _callback_answer_safe(query, *args, **kwargs)
        except Exception:
            logger.debug("Background callback answer failed", exc_info=True)

    task = asyncio.create_task(_run())
    return task


def _apply_offset(dt: datetime, minutes: int) -> datetime:
    return dt - timedelta(minutes=minutes)


def _extract_chat_id(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _normalize_chat_key(value: Any) -> str:
    chat_id = _extract_chat_id(value)
    if chat_id is not None:
        return str(chat_id)
    return str(value).strip()


def _normalize_username(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lstrip("@").lower()


def _normalize_topic_id(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _can_manage_job(user: Optional[User], job: Optional[Dict[str, Any]]) -> bool:
    if user is None or job is None:
        return False
    if _is_admin(user):
        return True
    if job.get("author_id") == user.id:
        return True
    job_author = _normalize_username(job.get("author_username"))
    if not job_author:
        return False
    return job_author == _normalize_username(user.username)


def _cleanup_pending(pending: Dict[str, Any], *, now_ts: Optional[float] = None) -> Dict[str, Any]:
    now_ts = now_ts or time.time()
    result: Dict[str, Any] = {}
    for token, entry in pending.items():
        if not isinstance(entry, dict):
            continue
        created_at = entry.get("created_at")
        try:
            created_at_ts = float(created_at)
        except (TypeError, ValueError):
            created_at_ts = now_ts
        if now_ts - created_at_ts > PENDING_PICK_TTL_SECONDS:
            continue
        result[token] = entry

    if len(result) <= PENDING_PICK_MAX_ITEMS:
        return result

    ordered = sorted(
        result.items(),
        key=lambda item: float(item[1].get("created_at") or 0.0),
        reverse=True,
    )
    return dict(ordered[:PENDING_PICK_MAX_ITEMS])


def _sync_job_schedule(job: Dict[str, Any]) -> Optional[datetime]:
    run_iso = job.get("run_at_utc")
    if not isinstance(run_iso, str):
        return None
    try:
        run_at = datetime.fromisoformat(run_iso)
    except (TypeError, ValueError):
        return None
    if run_at.tzinfo is None:
        run_at = run_at.replace(tzinfo=timezone.utc)

    cfg_chat_id = _extract_chat_id(job.get("target_chat_id"))
    if cfg_chat_id is None:
        cfg_chat_id = _extract_chat_id(job.get("source_chat_id"))

    if cfg_chat_id is None:
        tz = timezone.utc
        default_offset = 30
    else:
        tz = storage.resolve_tz_for_chat(cfg_chat_id)
        default_offset = storage.get_offset_for_chat(cfg_chat_id)

    stored_offset = storage.normalize_offset(job.get("offset_minutes"), fallback=None)
    if stored_offset == 0 and job.get("offset_minutes") is None:
        offset_minutes = default_offset
    else:
        offset_minutes = storage.normalize_offset(stored_offset or job.get("offset_minutes"), fallback=default_offset)

    updates: Dict[str, Any] = {}
    signature = job.get("signature")
    if not isinstance(signature, str) or not signature:
        text_raw = str(job.get("text") or "").strip()
        if text_raw:
            signature = f"{_normalize_chat_key(job.get('target_chat_id'))}:{_normalize_topic_id(job.get('topic_id'))}:{text_raw}"
            job["signature"] = signature
            updates["signature"] = signature
    if job.get("offset_minutes") != offset_minutes:
        job["offset_minutes"] = offset_minutes
        updates["offset_minutes"] = offset_minutes

    reminder_local = run_at.astimezone(tz)
    desired_local = reminder_local
    text = job.get("text")
    if isinstance(text, str) and text:
        parsed = parse_meeting_message(text, tz)
        if parsed:
            desired_local = _apply_offset(parsed["dt_local"], offset_minutes)

    if abs((desired_local - reminder_local).total_seconds()) >= 59:
        run_at = desired_local.astimezone(timezone.utc)
        job["run_at_utc"] = run_at.isoformat()
        updates["run_at_utc"] = job["run_at_utc"]

    job_id = job.get("job_id")
    if updates and isinstance(job_id, str):
        storage.upsert_job_record(job_id, updates)

    return run_at


def _resolve_target_title(target_chat_id: int | str) -> str:
    for known in storage.get_known_chats():
        if str(known.get("chat_id")) == str(target_chat_id):
            title = known.get("title")
            if title:
                return title
    return str(target_chat_id)


def _debounce(user_id: int, cooldown: float = 0.75) -> bool:
    now = time.monotonic()
    if len(_debounce.cache) > DEBOUNCE_MAX_USERS:
        stale = [uid for uid, ts in _debounce.cache.items() if now - ts > DEBOUNCE_TTL_SECONDS]
        for uid in stale:
            _debounce.cache.pop(uid, None)
        if len(_debounce.cache) > DEBOUNCE_MAX_USERS:
            _debounce.cache.clear()
    last = _debounce.cache.get(user_id, 0.0)
    if now - last < cooldown:
        return False
    _debounce.cache[user_id] = now
    return True


_debounce.cache: dict[int, float] = {}

async def _ensure_known_chat(message: Message) -> None:
    chat = message.chat
    if _chat_kind(chat) in {"group", "supergroup"}:
        title = chat.title or (chat.username and f"@{chat.username}") or str(chat.id)
        # В режиме "только личка управляет ботом" храним цели на уровне чата,
        # чтобы не раздувать реестр темами форума.
        storage.register_chat(chat.id, title)


async def _ensure_reply_menu(message: Message, state: FSMContext, *, force: bool = False) -> None:
    if not _is_private_chat(message.chat):
        return
    data = await state.get_data()
    if data.get(STATE_REPLY_MENU_SHOWN) and not force:
        return
    allow_settings = _is_private_chat(message.chat) or _is_admin(message.from_user)
    await _answer_safe(
        message,
        "👇 Быстрые действия",
        reply_markup=ui_kb.reply_menu_kb(
            _is_admin(message.from_user),
            allow_settings=allow_settings,
        ),
    )
    await state.update_data({STATE_REPLY_MENU_SHOWN: True})


async def _reset_interaction_state(
    state: FSMContext, *, preserve_pending: bool = False
) -> None:
    """Сбрасывает все флаги ожиданий и отложенные операции."""

    data = await state.get_data()
    updates: Dict[str, Any] = {}

    if data.get(STATE_AWAIT_TZ):
        updates[STATE_AWAIT_TZ] = False
    if data.get(STATE_AWAIT_ADMIN_ADD):
        updates[STATE_AWAIT_ADMIN_ADD] = False
    if data.get(STATE_AWAIT_ADMIN_DEL):
        updates[STATE_AWAIT_ADMIN_DEL] = False
    if not preserve_pending and data.get(STATE_PENDING):
        updates[STATE_PENDING] = {}

    if updates:
        await state.update_data(updates)


async def _pick_target_for_private(message: Message, state: FSMContext, text: str) -> bool:
    user = message.from_user
    if user is None:
        return False
    by_chat_id: dict[str, Dict[str, Any]] = {}
    for candidate in storage.get_known_chats():
        chat_id = candidate.get("chat_id")
        key = str(chat_id)
        if key in by_chat_id:
            continue
        title = candidate.get("title") or str(chat_id)
        by_chat_id[key] = {"chat_id": chat_id, "title": title, "topic_id": 0}

    candidates: list[Dict[str, Any]] = []
    for candidate in by_chat_id.values():
        chat_id = candidate.get("chat_id")
        member = await _telegram_call(
            lambda: message.bot.get_chat_member(chat_id, user.id),
            description="bot.get_chat_member",
            swallow_bad_request=True,
        )
        if member is None or member.status in {"left", "kicked"}:
            continue
        candidates.append(candidate)
    if not candidates:
        return False
    token = uuid.uuid4().hex
    data = await state.get_data()
    pending = _cleanup_pending(dict(data.get(STATE_PENDING, {})))
    pending[token] = {"text": text, "targets": candidates, "created_at": time.time()}
    await state.update_data({STATE_PENDING: pending})
    candidates_with_private = list(candidates)
    candidates_with_private.append({"chat_id": message.chat.id, "title": "Личный чат", "topic_id": 0})
    try:
        await _answer_safe(
            message,
            "📨 Куда отправить напоминание?",
            reply_markup=ui_kb.choose_chat_kb(
                candidates_with_private,
                token,
                is_admin=_is_admin(message.from_user),
                page=1,
                page_size=CHAT_PICKER_PAGE_SIZE,
            ),
        )
        return True
    except TelegramBadRequest as exc:
        logger.warning("Failed to show chat picker in private: %s", exc)
        pending.pop(token, None)
        await state.update_data({STATE_PENDING: pending})
        return False


async def _get_valid_last_target(
    message: Message,
    user: Optional[User],
    state: FSMContext,
    data: Dict[str, Any],
) -> Optional[tuple[int | str, Optional[int]]]:
    entry = data.get(STATE_LAST_TARGET)
    if not isinstance(entry, dict):
        return None
    chat_id = entry.get("chat_id")
    topic_id = entry.get("topic_id")
    if chat_id is None:
        return None
    current_topic = message.message_thread_id or 0
    if chat_id == message.chat.id and int(topic_id or 0) == int(current_topic):
        return chat_id, topic_id

    for candidate in storage.get_known_chats():
        candidate_chat = candidate.get("chat_id")
        candidate_topic = candidate.get("topic_id") or 0
        if str(candidate_chat) != str(chat_id) or int(candidate_topic) != int(topic_id or 0):
            continue
        if not user or not isinstance(chat_id, int):
            return chat_id, topic_id
        member = await _telegram_call(
            lambda: message.bot.get_chat_member(chat_id, user.id),
            description="bot.get_chat_member",
            swallow_bad_request=True,
        )
        if member and member.status not in {"left", "kicked"}:
            return chat_id, topic_id
        break

    await state.update_data({STATE_LAST_TARGET: None})
    return None


async def schedule_reminder(
    *,
    message: Message,
    source_chat_id: int,
    target_chat_id: int | str,
    user: Optional[User],
    text: str,
    topic_id: Optional[int] = None,
    notify: bool = True,
) -> None:
    tz = storage.resolve_tz_for_chat(int(target_chat_id) if isinstance(target_chat_id, int) else source_chat_id)
    parsed = parse_meeting_message(text, tz)
    if not parsed:
        if notify:
            await _answer_safe(message,
                (
                    "🙈 Не понял формат.\n"
                    "Жду строку вида: `ДД.ММ ТИП ЧЧ:ММ ПЕРЕГ НОМЕР`\n"
                    "Пример: `08.08 МТС 20:40 2в 88634`"
                ),
                parse_mode=ParseMode.MARKDOWN,
            )
        return

    target_key = _normalize_chat_key(target_chat_id)
    topic_key = _normalize_topic_id(topic_id)
    dedupe_signature = f"{target_key}:{topic_key}:{parsed['canonical_full']}"
    if (
        storage.find_job_by_signature(dedupe_signature)
        or storage.find_job_for_target_topic_text(target_key, topic_key, parsed["reminder_text"])
    ):
        if notify:
            await _answer_safe(message, "⚠️ Такая напоминалка уже есть.")
        return

    cfg_chat_id = _extract_chat_id(target_chat_id)
    if cfg_chat_id is None:
        cfg_chat_id = _extract_chat_id(source_chat_id)

    if cfg_chat_id is not None:
        offset_minutes = storage.get_offset_for_chat(cfg_chat_id)
    else:
        offset_minutes = storage.normalize_offset(None, fallback=30)
    reminder_local = _apply_offset(parsed["dt_local"], offset_minutes)
    reminder_utc = reminder_local.astimezone(timezone.utc)
    now_utc = _utc_now()

    job_id = f"rem-{uuid.uuid4().hex}"
    job_data = {
        "job_id": job_id,
        "target_chat_id": target_chat_id,
        "topic_id": topic_id,
        "text": parsed["reminder_text"],
        "source_chat_id": source_chat_id,
        "target_title": _resolve_target_title(target_chat_id),
        "author_id": getattr(user, "id", None),
        "author_username": getattr(user, "username", None),
        "created_at_utc": now_utc.isoformat(),
        "signature": dedupe_signature,
        "rrule": constants.RR_ONCE,
        "run_at_utc": reminder_utc.isoformat(),
        "offset_minutes": offset_minutes,
    }

    if reminder_utc <= now_utc:
        target_chat_norm = _extract_chat_id(target_chat_id)
        source_chat_norm = _extract_chat_id(source_chat_id)
        same_topic = int(topic_id or 0) == int(getattr(message, "message_thread_id", 0) or 0)
        suppress_immediate = (
            not notify
            and target_chat_norm is not None
            and source_chat_norm is not None
            and target_chat_norm == source_chat_norm
            and same_topic
        )

        if suppress_immediate:
            audit_log(
                "REM_SEND_NOW_SUPPRESSED",
                chat_id=target_chat_id,
                topic_id=topic_id,
                title=parsed["reminder_text"],
                user_id=getattr(user, "id", None),
                reason="notify_disabled_same_chat",
            )
        else:
            audit_log(
                "REM_SEND_NOW",
                chat_id=target_chat_id,
                topic_id=topic_id,
                title=parsed["reminder_text"],
                user_id=getattr(user, "id", None),
            )
            await _send_safe(
                message.bot,
                target_chat_id,
                job_data["text"],
                message_thread_id=topic_id,
            )

        if notify:
            await _answer_safe(
                message,
                "✅ Напоминание уже должно было прийти — отправил сразу.",
            )
        return

    _schedule_job(job_id, reminder_utc)
    storage.add_job_record(job_data)
    audit_log(
        "REM_SCHEDULED",
        reminder_id=job_id,
        chat_id=target_chat_id,
        topic_id=topic_id,
        user_id=getattr(user, "id", None),
        title=job_data["text"],
        when=reminder_utc,
        tz=getattr(tz, "zone", str(tz)),
        delay_sec=round((reminder_utc - now_utc).total_seconds(), 1),
    )
    if notify:
        await _answer_safe(message,
            f"📌 Запланировано на {reminder_local:%d.%m %H:%M}\n{parsed['canonical_full']}",
            reply_markup=ui_kb.job_kb(job_id) if _is_admin(user) else None,
            parse_mode=ParseMode.MARKDOWN,
        )

def _render_active(
    chunk: Iterable[Dict[str, Any]],
    total: int,
    page: int,
    pages_total: int,
    user: Optional[User],
    *,
    title: str,
    page_prefix: str,
    empty_message: str,
    view: str,
    filter_mode: str = constants.ACTIVE_FILTER_ALL,
) -> tuple[str, InlineKeyboardMarkup]:
    admin = _is_admin(user)
    text = ui_txt.render_active_text(
        list(chunk),
        total,
        page,
        pages_total,
        admin,
        title=title,
        empty_message=empty_message,
    )
    kb = ui_kb.active_kb(
        list(chunk),
        page,
        pages_total,
        uid=user.id if user else 0,
        is_admin=admin,
        page_prefix=page_prefix,
        view=view,
        filter_mode=filter_mode,
    )
    return text, kb


async def _show_active(
    message: Message,
    user: Optional[User],
    *,
    page: int = 1,
    mine: bool = False,
    filter_mode: str = constants.ACTIVE_FILTER_ALL,
) -> None:
    if not mine and not _is_admin(user):
        await _answer_safe(message, "⛔ Только администратор может просматривать активные напоминания.")
        return
    filter_mode = _normalize_active_filter_mode(filter_mode)
    predicate: Optional[Callable[[Dict[str, Any]], bool]] = None
    title = f"📝 Активные{_active_filter_title_suffix(filter_mode)}"
    page_prefix = constants.CB_ACTIVE_PAGE
    empty_message = "Пока нет активных напоминаний."
    view = "all"
    if mine:
        if not user:
            await _answer_safe(message, "⚠️ Доступно только пользователям.")
            return
        uid = user.id
        username = (user.username or "").lower()

        def predicate(job: Dict[str, Any]) -> bool:
            if job.get("author_id") == uid:
                return True
            if username and isinstance(job.get("author_username"), str):
                return job["author_username"].lower() == username
            return False

        title = f"📂 Мои встречи{_active_filter_title_suffix(filter_mode)}"
        page_prefix = constants.CB_MY_PAGE
        empty_message = "У вас пока нет запланированных встреч."
        view = "my"
    if filter_mode != constants.ACTIVE_FILTER_ALL:
        filter_labels = {
            constants.ACTIVE_FILTER_TODAY: "за сегодня",
            constants.ACTIVE_FILTER_TOMORROW: "на завтра",
            constants.ACTIVE_FILTER_WEEK: "на ближайшие 7 дней",
        }
        suffix = filter_labels.get(filter_mode, "")
        if suffix:
            empty_message = f"Пока нет встреч {suffix}."

    base_predicate = predicate

    def combined_predicate(job: Dict[str, Any]) -> bool:
        if base_predicate is not None and not base_predicate(job):
            return False
        return _job_matches_filter_mode(job, filter_mode)

    chunk, total, pages_total = _paginate_jobs(
        page,
        constants.PAGE_SIZE or 10,
        predicate=combined_predicate,
    )
    text, kb = _render_active(
        chunk,
        total,
        page,
        pages_total,
        user,
        title=title,
        page_prefix=page_prefix,
        empty_message=empty_message,
        view=view,
        filter_mode=filter_mode,
    )
    if message:
        try:
            await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except TelegramBadRequest:
            await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)


async def _show_create_hint(message: Message, user: Optional[User]) -> None:
    text = ui_txt.create_reminder_hint(message.chat.id)
    kb = ui_kb.main_menu_kb(
        _is_admin(user),
        allow_settings=_can_manage_settings(user, message.chat),
    )
    try:
        await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)


async def _show_admin_panel(message: Message, user: Optional[User]) -> None:
    if not _is_admin(user):
        await _answer_safe(message, "⛔ Только администратор может открыть админ-панель.")
        return
    text = "🛡️ Админ-панель"
    kb = ui_kb.admin_panel_kb(is_owner=_is_owner(user))
    try:
        await _edit_text_safe(message, text, reply_markup=kb)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb)


async def _show_settings(
    message: Message,
    user: Optional[User],
    state: FSMContext,
    *,
    back_callback: str = constants.CB_MENU,
) -> None:
    if not _can_manage_settings(user, message.chat):
        await _answer_safe(message, "⛔ Только администратор может менять настройки.")
        return
    await state.update_data({STATE_AWAIT_TZ: False, STATE_AWAIT_ADMIN_ADD: False, STATE_AWAIT_ADMIN_DEL: False})
    kb = ui_kb.settings_menu_kb(_is_owner(user), back_callback=back_callback)
    text = "⚙️ Настройки"
    try:
        await _edit_text_safe(message, text, reply_markup=kb)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb)


async def _show_chats(message: Message, *, back_callback: str = constants.CB_SETTINGS) -> None:
    known = storage.get_known_chats()
    kb = ui_kb.chats_menu_kb(known, back_callback=back_callback)
    text = "📋 Зарегистрированные чаты"
    try:
        await _edit_text_safe(message, text, reply_markup=kb)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb)


async def _show_logs_menu(
    message: Message,
    notice: str | None = None,
    *,
    back_callback: str = constants.CB_SETTINGS,
) -> None:
    text = "📜 Логи"
    if notice:
        text = f"{text}\n\n<i>{escape(notice)}</i>"
    kb = ui_kb.logs_menu_kb(back_callback=back_callback)
    try:
        await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)


async def _show_log_files(message: Message, log_type: str) -> None:
    files = await asyncio.to_thread(log_utils.list_log_files, log_type)
    text = ui_txt.render_log_file_list(log_type, files)
    kb = ui_kb.log_files_kb(log_type, files)
    try:
        await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)


async def _show_log_file(message: Message, log_type: str, file_name: str) -> None:
    try:
        info = await asyncio.to_thread(log_utils.get_log_file_info, log_type, file_name)
    except FileNotFoundError:
        await _answer_safe(message, "⚠️ Файл недоступен или был удалён.")
        await _show_log_files(message, log_type)
        return
    view = await asyncio.to_thread(log_utils.read_log_entries, log_type, info.path)
    text = ui_txt.render_log_file(log_type, info, view)
    kb = ui_kb.log_file_view_kb(log_type)
    try:
        await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)


async def _send_logs_archive(message: Message) -> None:
    path = await asyncio.to_thread(log_utils.build_logs_archive)
    try:
        file = FSInputFile(path, filename=path.name)
        await _telegram_call(
            lambda: message.answer_document(
                document=file,
                caption="📥 Архив логов",
            ),
            description="send logs archive",
        )
    finally:
        with suppress(Exception):
            path.unlink(missing_ok=True)


async def _send_data_backup(message: Message) -> None:
    path = await asyncio.to_thread(_build_data_backup)
    try:
        file = FSInputFile(path, filename=path.name)
        await _telegram_call(
            lambda: message.answer_document(
                document=file,
                caption="📥 Backup data archive",
            ),
            description="send data backup",
        )
    finally:
        with suppress(Exception):
            path.unlink(missing_ok=True)
        with suppress(Exception):
            path.parent.rmdir()


async def _show_admins(message: Message) -> None:
    admins = constants.ADMIN_USERNAMES
    text = ui_txt.render_admins_text(admins)
    kb = ui_kb.admins_menu_kb(admins)
    try:
        await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)


async def _show_admin_flags(message: Message) -> None:
    flags = feature_flags.list_flags()
    lines = ["🚩 <b>Флаги поведения</b>"]
    for name, value, label, description in feature_flags.list_flags_ordered():
        lines.append(
            f"• <b>{escape(label)}</b>: <b>{'ВКЛ' if value else 'ВЫКЛ'}</b>\n"
            f"  <code>{escape(name)}</code>"
        )
        if description:
            lines.append(f"  {escape(description)}")
    kb = ui_kb.admin_flags_kb(flags)
    text = "\n".join(lines)
    try:
        await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)


def _install_error_burst_notifier(bot: Bot) -> None:
    loop = asyncio.get_running_loop()

    def _callback(record: logging.LogRecord, count: int) -> None:
        payload = dict(getattr(record, "json_payload", {}) or {})
        where = payload.get("where") or record.name
        message = payload.get("message") or record.getMessage()
        error_type = payload.get("type") or getattr(record, "error_type", "ERROR")
        snapshot = {
            "where": where,
            "message": message,
            "type": error_type,
            "count": count,
        }

        def _schedule() -> None:
            asyncio.create_task(_notify_error_burst(bot, snapshot))

        try:
            loop.call_soon_threadsafe(_schedule)
        except RuntimeError:
            logger.exception("Failed to schedule error burst notification")

    log_utils.set_error_burst_callback(_callback)


async def _notify_error_burst(bot: Bot, snapshot: dict[str, Any]) -> None:
    if not feature_flags.is_enabled("alerts.error_burst.enabled", True):
        return
    owner_only = feature_flags.is_enabled("alerts.owner_only", True)
    if owner_only:
        recipients = storage.get_owner_user_ids({"panykovc"})
    else:
        recipients = set(constants.ADMIN_IDS)
        recipients.update(storage.get_user_ids_by_usernames(set(constants.ADMIN_USERNAMES)))
    if not recipients:
        return
    count = snapshot.get("count", 0)
    where = snapshot.get("where", "reminder.error")
    message = snapshot.get("message", "")
    error_type = snapshot.get("type", "ERROR")
    text = (
        "⚠️ <b>Внимание: серия ошибок</b>\n"
        f"За последние минуты зафиксировано <b>{int(count)}</b> ошибок."
        f"\nТип: <code>{escape(str(error_type))}</code>"
        f"\nИсточник: <code>{escape(str(where))}</code>"
    )
    if message:
        text += f"\nСообщение: <code>{escape(str(message))}</code>"
    text += "\n\nПроверьте error-логи."  # noqa: W503
    for admin_id in recipients:
        try:
            await bot.send_message(admin_id, text, parse_mode=ParseMode.HTML)
        except (TelegramBadRequest, TelegramNetworkError, ClientError, asyncio.TimeoutError, OSError):
            logger.debug("Failed to notify admin %s about error burst", admin_id, exc_info=True)


async def _show_archive(
    message: Message,
    user: Optional[User],
    *,
    page: int = 1,
    notice: Optional[str] = None,
    back_callback: str = constants.CB_SETTINGS,
) -> None:
    can_manage = _can_manage_settings(user, message.chat)
    items, total, actual_page, pages_total = storage.get_archive_page(page, constants.PAGE_SIZE)
    text = ui_txt.render_archive_text(
        items,
        total,
        actual_page,
        pages_total,
        page_size=constants.PAGE_SIZE,
    )
    if notice:
        text = f"{text}\n\n<i>{escape(notice)}</i>"
    kb = ui_kb.archive_kb(
        actual_page,
        pages_total,
        has_entries=bool(items),
        can_clear=can_manage and total > 0,
        back_callback=back_callback,
    )
    try:
        await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)

def _get_job(job_id: str) -> Optional[Dict[str, Any]]:
    try:
        return storage.get_job_record(job_id)
    except (ValueError, OSError):
        logger.exception("Failed to load job %s", job_id)
        return None


def _remove_job(
    job_id: str,
    *,
    archive_reason: Optional[str] = None,
    record: Optional[Dict[str, Any]] = None,
    removed_by: Optional[Dict[str, Any]] = None,
) -> None:
    removed = False
    if archive_reason:
        removed = storage.archive_job(
            job_id,
            rec=record,
            reason=archive_reason,
            removed_by=removed_by,
        )
    if not removed:
        storage.remove_job_record(job_id)
    with suppress(Exception):
        scheduler.remove_job(job_id)


def _parse_job_callback(data: str, prefix: str) -> tuple[Optional[str], tuple[str, ...]]:
    if not data.startswith(f"{prefix}:"):
        return None, ()
    parts = data.split(":")
    if len(parts) < 2:
        return None, ()
    job_id = parts[1]
    extras = tuple(parts[2:]) if len(parts) > 2 else ()
    return job_id or None, extras


def _resolve_view_hint(extras: tuple[str, ...]) -> Optional[str]:
    for extra in extras:
        if not extra or extra == "y":
            continue
        if extra in {"my", "all"}:
            return extra
    return None


async def _open_actions(
    message: Message,
    user: Optional[User],
    job_id: str,
    *,
    context: Optional[str] = None,
) -> None:
    job = _get_job(job_id)
    if not job:
        await _answer_safe(message, "Не найдено")
        return
    if not _can_manage_job(user, job):
        await _answer_safe(message, "⛔ Недостаточно прав для управления этой встречей.")
        return
    label = job.get("text", "напоминание")
    kb = ui_kb.actions_kb(job_id, is_admin=_is_admin(user), return_to=context)
    text = f"⚙️ Действия для «{label}»"
    try:
        await _edit_text_safe(message, text, reply_markup=kb)
    except TelegramBadRequest:
        await _answer_safe(message, text, reply_markup=kb)


def _update_job_time(job: Dict[str, Any], new_run: datetime) -> None:
    job["run_at_utc"] = new_run.astimezone(timezone.utc).isoformat()
    storage.upsert_job_record(job["job_id"], {"run_at_utc": job["run_at_utc"]})
    _schedule_job(job["job_id"], new_run)

async def send_reminder_job(job_id: str | None = None, **_: Any) -> bool:
    if not job_id:
        return False
    bot: Bot = send_reminder_job.bot  # type: ignore[attr-defined]
    job = _get_job(job_id)
    if not job:
        return False
    audit_log(
        "REM_FIRED",
        reminder_id=job_id,
        chat_id=job.get("target_chat_id"),
        topic_id=job.get("topic_id"),
        title=job.get("text"),
        user_id=job.get("author_id"),
    )
    delivered = await _send_safe(
        bot,
        job.get("target_chat_id"),
        job.get("text", ""),
        message_thread_id=job.get("topic_id"),
    )
    if not delivered:
        retry_at = _utc_now() + timedelta(minutes=SEND_RETRY_DELAY_MINUTES)
        _update_job_time(job, retry_at)
        audit_log(
            "REM_RESCHEDULED",
            reminder_id=job_id,
            chat_id=job.get("target_chat_id"),
            topic_id=job.get("topic_id"),
            title=job.get("text"),
            user_id=job.get("author_id"),
            repeat_next_at=retry_at,
            reason="send_retry",
        )
        return False
    rrule = job.get("rrule", constants.RR_ONCE)
    run_iso = job.get("run_at_utc")
    try:
        run_at = datetime.fromisoformat(run_iso) if run_iso else _utc_now()
        if run_at.tzinfo is None:
            run_at = run_at.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        run_at = _utc_now()
    if rrule == constants.RR_DAILY:
        next_run = run_at + timedelta(days=1)
        _update_job_time(job, next_run)
        audit_log(
            "REM_RESCHEDULED",
            reminder_id=job_id,
            chat_id=job.get("target_chat_id"),
            topic_id=job.get("topic_id"),
            title=job.get("text"),
            user_id=job.get("author_id"),
            repeat_next_at=next_run,
            reason="repeat",
        )
    elif rrule == constants.RR_WEEKLY:
        next_run = run_at + timedelta(weeks=1)
        _update_job_time(job, next_run)
        audit_log(
            "REM_RESCHEDULED",
            reminder_id=job_id,
            chat_id=job.get("target_chat_id"),
            topic_id=job.get("topic_id"),
            title=job.get("text"),
            user_id=job.get("author_id"),
            repeat_next_at=next_run,
            reason="repeat",
        )
    else:
        _remove_job(job_id, archive_reason="completed", record=job)
    return True


def restore_jobs() -> None:
    now = _utc_now()
    for job in storage.get_jobs_store():
        job_id = job.get("job_id")
        if not job_id:
            continue
        run_at = _sync_job_schedule(job)
        if run_at is None:
            continue
        delay = (run_at - now).total_seconds()
        if delay <= 0 and delay >= -constants.CATCHUP_WINDOW_SECONDS:
            asyncio.create_task(send_reminder_job(job_id))
        elif delay > 0:
            _schedule_job(job_id, run_at)


@router.my_chat_member()
async def on_my_chat_member(event: ChatMemberUpdated) -> None:
    new_status = getattr(event.new_chat_member, "status", None)
    if new_status not in {"left", "kicked"}:
        return
    chat = event.chat
    if chat is None:
        return
    records = storage.get_jobs_for_chat(chat.id)
    if not records:
        return
    removed_by = _serialize_user(event.from_user)
    reason = "bot_removed" if new_status in {"left", "kicked"} else "chat_removed"
    for rec in records:
        job_id = rec.get("job_id")
        if not job_id:
            continue
        _remove_job(job_id, archive_reason=reason, record=rec, removed_by=removed_by)


# === Commands ===


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    if not _is_private_chat(message.chat):
        return
    _remember_owner_if_needed(message.from_user)
    user = message.from_user
    text = ui_txt.menu_text_for(message.chat.id)
    await state.update_data({STATE_REPLY_MENU_SHOWN: False})
    await _answer_safe(
        message,
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=ui_kb.main_menu_kb(
            _is_admin(user),
            allow_settings=_can_manage_settings(user, message.chat),
        ),
    )
    await _ensure_reply_menu(message, state, force=True)


@router.message(Command("help"))
async def cmd_help(message: Message, state: FSMContext) -> None:
    if not _is_private_chat(message.chat):
        return
    _remember_owner_if_needed(message.from_user)
    user = message.from_user
    text = ui_txt.show_help_text()
    await state.update_data({STATE_REPLY_MENU_SHOWN: False})
    await _answer_safe(
        message,
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=ui_kb.main_menu_kb(
            _is_admin(user),
            allow_settings=_can_manage_settings(user, message.chat),
        ),
    )
    await _ensure_reply_menu(message, state, force=True)


@router.message(Command("version"))
async def cmd_version(message: Message) -> None:
    if not _is_private_chat(message.chat):
        return
    user = message.from_user
    _remember_owner_if_needed(user)
    if not _is_owner(user):
        await _answer_safe(message, "⛔ Только владелец может смотреть историю версий.")
        return
    current = release_history.current_revision()
    history = release_history.get_history(limit=5)
    lines = [
        "🧬 Версия бота",
        f"Текущая: {current.get('version')} | commit={current.get('commit') or 'n/a'} | branch={current.get('branch') or 'n/a'} | dirty={current.get('dirty')}",
        "",
        "Последние запуски:",
    ]
    if not history:
        lines.append("• (пусто)")
    else:
        for item in reversed(history):
            lines.append(
                f"• {item.get('ts')}: {item.get('version')} commit={item.get('commit') or 'n/a'} dirty={item.get('dirty')}"
            )
    await _answer_safe(message, "\n".join(lines))


@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if not _is_private_chat(message.chat):
        return
    _remember_owner_if_needed(message.from_user)
    await _show_admin_panel(message, message.from_user)


@router.message(Command("menu"))
async def cmd_menu(message: Message, state: FSMContext) -> None:
    if not _is_private_chat(message.chat):
        return
    _remember_owner_if_needed(message.from_user)
    await cmd_start(message, state)


@router.message(Command("register"))
async def cmd_register(message: Message) -> None:
    _remember_owner_if_needed(message.from_user)
    await _ensure_known_chat(message)
    if _is_private_chat(message.chat):
        await _answer_safe(message, "В личке регистрировать чат не нужно.")


@router.message(Command("purge"))
async def cmd_purge(message: Message) -> None:
    if not _is_private_chat(message.chat):
        return
    _remember_owner_if_needed(message.from_user)
    if not _is_admin(message.from_user):
        await _answer_safe(message, "Только для админов.")
        return
    storage.set_jobs_store([])
    scheduler.remove_all_jobs()
    await _answer_safe(message, "База напоминаний очищена ✅")

# === Text handlers ===


@router.message(F.chat.type == "private", F.text)
async def handle_private_text(message: Message, state: FSMContext) -> None:
    _remember_owner_if_needed(message.from_user)
    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return

    data = await state.get_data()
    if data.get(STATE_AWAIT_TZ):
        try:
            pytz.timezone(text)
        except pytz.UnknownTimeZoneError:
            await _answer_safe(message, "Некорректная TZ. Пример: `Europe/Moscow`", parse_mode=ParseMode.MARKDOWN)
            return
        storage.update_chat_cfg(message.chat.id, tz=text)
        await state.update_data({STATE_AWAIT_TZ: False})
        await _answer_safe(message, f"TZ обновлена: `{text}`", parse_mode=ParseMode.MARKDOWN)
        return

    if data.get(STATE_AWAIT_ADMIN_ADD):
        await state.update_data({STATE_AWAIT_ADMIN_ADD: False})
        if not _is_owner(message.from_user):
            await _answer_safe(message, "Только владелец может управлять админами.")
            return
        username = text.lstrip("@").lower()
        if not username:
            await _answer_safe(message, "Нужен логин вида @username")
            return
        added = storage.add_admin_username(username)
        await _answer_safe(message, "✅ Добавлен" if added else "⚠️ Уже в списке")
        return

    if data.get(STATE_AWAIT_ADMIN_DEL):
        await state.update_data({STATE_AWAIT_ADMIN_DEL: False})
        removed = storage.remove_admin_username(text.lstrip("@"))
        await _answer_safe(message, "✅ Удалён" if removed else "⚠️ Не найден")
        return

    force_pick = bool(data.get(STATE_FORCE_PICK))
    last_target = await _get_valid_last_target(message, message.from_user, state, data)

    action = REPLY_MENU_ALIASES.get(text.casefold())
    if action:
        await _reset_interaction_state(state)
        user = message.from_user
        if action == "menu":
            menu_text = ui_txt.menu_text_for(message.chat.id)
            await _answer_safe(message,
                menu_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=ui_kb.main_menu_kb(
                    _is_admin(user),
                    allow_settings=_can_manage_settings(user, message.chat),
                ),
            )
        elif action == "create":
            await state.update_data({STATE_FORCE_PICK: True})
            await _show_create_hint(message, user)
        elif action == "my":
            await _show_active(message, user, page=1, mine=True)
        elif action == "active":
            await _show_active(message, user, page=1)
        elif action == "admin_panel":
            await _show_admin_panel(message, user)
        elif action == "settings":
            if _is_admin(user):
                await _show_admin_panel(message, user)
            else:
                await _show_settings(message, user, state, back_callback=constants.CB_MENU)
        elif action == "help":
            help_text = ui_txt.show_help_text()
            await _answer_safe(message,
                help_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=ui_kb.main_menu_kb(
                    _is_admin(user),
                    allow_settings=_can_manage_settings(user, message.chat),
                ),
            )
        await _ensure_reply_menu(message, state, force=action in {"menu", "help"})
        return

    tz_preview = storage.resolve_tz_for_chat(message.chat.id)
    looks_like_reminder = parse_meeting_message(text, tz_preview) is not None

    if looks_like_reminder and last_target and not force_pick:
        target_chat_id, topic_id = last_target
        await schedule_reminder(
            message=message,
            source_chat_id=message.chat.id,
            target_chat_id=target_chat_id,
            user=message.from_user,
            text=text,
            topic_id=topic_id,
        )
        await state.update_data({STATE_FORCE_PICK: False, STATE_LAST_TARGET: {"chat_id": target_chat_id, "topic_id": topic_id}})
        return

    if looks_like_reminder and await _pick_target_for_private(message, state, text):
        return

    await schedule_reminder(
        message=message,
        source_chat_id=message.chat.id,
        target_chat_id=message.chat.id,
        user=message.from_user,
        text=text,
        topic_id=message.message_thread_id,
    )

    if looks_like_reminder:
        await state.update_data({
            STATE_LAST_TARGET: {"chat_id": message.chat.id, "topic_id": message.message_thread_id},
            STATE_FORCE_PICK: False,
        })


@router.message(F.chat.type.in_({"group", "supergroup"}), F.text)
async def handle_group_text(message: Message) -> None:
    _remember_owner_if_needed(message.from_user)
    if not message.text:
        return
    text = message.text.strip()
    if not text or text.startswith("/"):
        return
    await _ensure_known_chat(message)
    # В группах бот не должен отвечать и планировать напрямую.
    # Напоминания создаются только из личного диалога.
    return

# === Callback handling ===


@router.callback_query()
async def on_callback(query: CallbackQuery, state: FSMContext) -> None:
    data = query.data or ""
    user = query.from_user
    message = query.message
    _remember_owner_if_needed(user)

    if user and not data.startswith(CB_NOOP) and not _debounce(user.id):
        with suppress(Exception):
            await _callback_answer_safe(query, "⏳ Уже выполняю…", cache_time=1)
        return

    if data == CB_NOOP or data.startswith(f"{CB_NOOP}:"):
        with suppress(Exception):
            await _callback_answer_safe(query, "⏳ Уже выполняю…", cache_time=1)
        return

    if message is None:
        with suppress(Exception):
            await _callback_answer_safe(query, "Сообщение недоступно", show_alert=True)
        return
    if not _is_private_chat(message.chat):
        with suppress(Exception):
            await _callback_answer_safe(query, "Откройте бота в личке.")
        return

    _ack_callback_background(query, "В работе", cache_time=1)

    await _reset_interaction_state(
        state,
        preserve_pending=data.startswith(f"{constants.CB_PICK_CHAT}:")
    )

    if data == constants.CB_MENU:
        text = ui_txt.menu_text_for(message.chat.id)
        kb = ui_kb.main_menu_kb(
            _is_admin(user),
            allow_settings=_can_manage_settings(user, message.chat),
        )
        try:
            await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        except TelegramBadRequest:
            await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        await _ensure_reply_menu(message, state)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_HELP:
        text = ui_txt.show_help_text()
        kb = ui_kb.main_menu_kb(
            _is_admin(user),
            allow_settings=_can_manage_settings(user, message.chat),
        )
        try:
            await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        except TelegramBadRequest:
            await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        await _ensure_reply_menu(message, state)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_ADMIN_PANEL:
        await _show_admin_panel(message, user)
        await _ensure_reply_menu(message, state)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_ADMIN_STATUS:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор.")
            await _callback_answer_safe(query)
            return
        text = _build_admin_status()
        kb = ui_kb.admin_panel_kb(is_owner=_is_owner(user))
        try:
            await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except TelegramBadRequest:
            await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_ADMIN_FLAGS:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор.")
            await _callback_answer_safe(query)
            return
        await _show_admin_flags(message)
        await _callback_answer_safe(query)
        return

    if data.startswith(f"{constants.CB_ADMIN_FLAG_TOGGLE}:"):
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор.")
            await _callback_answer_safe(query)
            return
        name = data.split(":", 1)[1]
        flags = feature_flags.list_flags()
        current = bool(flags.get(name, False))
        feature_flags.set_flag(name, not current)
        await _show_admin_flags(message)
        await _callback_answer_safe(query, "Обновлено")
        return

    if data == constants.CB_ADMIN_BACKUP:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор.")
            await _callback_answer_safe(query)
            return
        await _send_data_backup(message)
        await _callback_answer_safe(query, "Backup отправлен")
        return

    if data == constants.CB_ADMIN_VERIFY_DB:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор.")
            await _callback_answer_safe(query)
            return
        text = _build_admin_verify_text()
        kb = ui_kb.admin_panel_kb(is_owner=_is_owner(user))
        try:
            await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except TelegramBadRequest:
            await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_ADMIN_HISTORY:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор.")
            await _callback_answer_safe(query)
            return
        text = _build_admin_history_text()
        kb = ui_kb.admin_panel_kb(is_owner=_is_owner(user))
        try:
            await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except TelegramBadRequest:
            await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_SETTINGS:
        back_callback = constants.CB_ADMIN_PANEL if _is_admin(user) else constants.CB_MENU
        await _show_settings(message, user, state, back_callback=back_callback)
        await _ensure_reply_menu(message, state)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_LOGS:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может управлять логами.")
            await _callback_answer_safe(query)
            return
        await _show_logs_menu(message, back_callback=constants.CB_ADMIN_PANEL)
        await _callback_answer_safe(query)
        return

    if data in {
        constants.CB_LOGS_APP,
        constants.CB_LOGS_AUDIT,
        constants.CB_LOGS_ERROR,
    }:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может управлять логами.")
            await _callback_answer_safe(query)
            return
        log_type = {
            constants.CB_LOGS_APP: log_utils.LOG_TYPE_APP,
            constants.CB_LOGS_AUDIT: log_utils.LOG_TYPE_AUDIT,
            constants.CB_LOGS_ERROR: log_utils.LOG_TYPE_ERROR,
        }[data]
        await _show_log_files(message, log_type)
        await _callback_answer_safe(query)
        return

    if data.startswith(f"{constants.CB_LOGS_FILE}:"):
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может управлять логами.")
            await _callback_answer_safe(query)
            return
        parts = data.split(":", 2)
        if len(parts) != 3:
            await _callback_answer_safe(query)
            return
        _, kind_raw, file_name = parts
        kind = kind_raw.lower()
        if kind not in {
            log_utils.LOG_TYPE_APP,
            log_utils.LOG_TYPE_AUDIT,
            log_utils.LOG_TYPE_ERROR,
        }:
            await _answer_safe(message, "⚠️ Неизвестный тип журнала.")
            await _show_logs_menu(message, back_callback=constants.CB_ADMIN_PANEL)
            await _callback_answer_safe(query)
            return
        await _show_log_file(message, kind, file_name)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_LOGS_DOWNLOAD:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может управлять логами.")
            await _callback_answer_safe(query)
            return
        await _send_logs_archive(message)
        await _callback_answer_safe(query, "Архив отправлен")
        return

    if data == constants.CB_LOGS_CLEAR:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может управлять логами.")
            await _callback_answer_safe(query)
            return
        text = (
            "❓ <b>Очистить журналы?</b>\n"
            "Вы уверены? Текущие файлы будут обнулены, архивы удалены."
        )
        kb = ui_kb.logs_clear_confirm_kb(back_callback=constants.CB_LOGS)
        try:
            await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except TelegramBadRequest:
            await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_LOGS_CLEAR_CONFIRM:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может управлять логами.")
            await _callback_answer_safe(query)
            return
        affected = await asyncio.to_thread(log_utils.clear_all_logs)
        note = "Логи очищены." if affected else "Логи уже пусты."
        await _show_logs_menu(message, notice=note, back_callback=constants.CB_ADMIN_PANEL)
        await _callback_answer_safe(query, "Очищено")
        return

    if data == constants.CB_CREATE:
        await state.update_data({STATE_FORCE_PICK: True})
        await _show_create_hint(message, user)
        await _ensure_reply_menu(message, state)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_MY or data.startswith(f"{constants.CB_MY_PAGE}:"):
        page, filter_mode = _parse_page_and_filter(data, constants.CB_MY_PAGE)
        await _show_active(message, user, page=page, mine=True, filter_mode=filter_mode)
        await _ensure_reply_menu(message, state)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_ACTIVE or data.startswith(f"{constants.CB_ACTIVE_PAGE}:"):
        page, filter_mode = _parse_page_and_filter(data, constants.CB_ACTIVE_PAGE)
        await _show_active(message, user, page=page, filter_mode=filter_mode)
        await _ensure_reply_menu(message, state)
        await _callback_answer_safe(query)
        return

    if data.startswith(f"{constants.CB_MY_FILTER}:"):
        mode = data.split(":", 1)[1] if ":" in data else constants.ACTIVE_FILTER_ALL
        await _show_active(message, user, page=1, mine=True, filter_mode=mode)
        await _ensure_reply_menu(message, state)
        await _callback_answer_safe(query)
        return

    if data.startswith(f"{constants.CB_ACTIVE_FILTER}:"):
        mode = data.split(":", 1)[1] if ":" in data else constants.ACTIVE_FILTER_ALL
        await _show_active(message, user, page=1, filter_mode=mode)
        await _ensure_reply_menu(message, state)
        await _callback_answer_safe(query)
        return

    if data.startswith(f"{constants.CB_ACTIVE_CLEAR}:"):
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        parts = data.split(":")
        if len(parts) < 3:
            await _callback_answer_safe(query, "Некорректные данные", show_alert=True)
            return
        view = parts[1] or "all"
        try:
            page = int(parts[2])
        except ValueError:
            page = 1
        filter_mode = constants.ACTIVE_FILTER_ALL
        confirmed = False
        if len(parts) >= 4:
            fourth = parts[3]
            if fourth == "y":
                confirmed = True
            else:
                filter_mode = _normalize_active_filter_mode(fourth)
        if len(parts) >= 5 and parts[4] == "y":
            confirmed = True

        if not confirmed:
            page_prefix = constants.CB_MY_PAGE if view == "my" else constants.CB_ACTIVE_PAGE
            kb = ui_kb.active_clear_confirm_kb(
                page,
                view=view,
                page_prefix=page_prefix,
                filter_mode=filter_mode,
            )
            text = (
                "❓ <b>Очистить активные напоминания?</b>\n"
                "Вы уверены? Все текущие задачи будут перенесены в архив."
            )
            try:
                await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
            except TelegramBadRequest:
                await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
            await _callback_answer_safe(query)
            return

        jobs = storage.get_jobs_store()
        removed_by = _serialize_user(user)
        removed = 0
        for rec in jobs:
            job_id = rec.get("job_id")
            if not job_id:
                continue
            _remove_job(
                job_id,
                archive_reason="bulk_clear",
                record=rec,
                removed_by=removed_by,
            )
            audit_log(
                "REM_CANCELED",
                reminder_id=job_id,
                chat_id=rec.get("target_chat_id"),
                topic_id=rec.get("topic_id"),
                user_id=getattr(user, "id", None),
                title=rec.get("text"),
                reason="bulk_clear",
            )
            removed += 1
        if view == "my":
            await _show_active(message, user, page=1, mine=True, filter_mode=filter_mode)
        else:
            await _show_active(message, user, page=1, filter_mode=filter_mode)
        await _ensure_reply_menu(message, state)
        await _callback_answer_safe(query, "Очищено")
        return

    if data == constants.CB_SET_TZ:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        kb = ui_kb.tz_menu_kb()
        try:
            await _edit_text_safe(message, "Выберите таймзону", reply_markup=kb)
        except TelegramBadRequest:
            await _answer_safe(message, "Выберите таймзону", reply_markup=kb)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_SET_TZ_LOCAL:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        tz_name = get_localzone_name()
        storage.update_chat_cfg(message.chat.id, tz=tz_name)
        await _answer_safe(message, f"TZ обновлена: {tz_name}")
        await _callback_answer_safe(query)
        return

    if data == constants.CB_SET_TZ_MOSCOW:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        storage.update_chat_cfg(message.chat.id, tz="Europe/Moscow")
        await _answer_safe(message, "TZ обновлена: Europe/Moscow")
        await _callback_answer_safe(query)
        return

    if data == constants.CB_SET_TZ_CHICAGO:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        storage.update_chat_cfg(message.chat.id, tz="America/Chicago")
        await _answer_safe(message, "TZ обновлена: America/Chicago")
        await _callback_answer_safe(query)
        return

    if data == constants.CB_SET_TZ_ENTER:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        await state.update_data({STATE_AWAIT_TZ: True})
        await _answer_safe(message, "Введи название таймзоны, например `Europe/Moscow`", parse_mode=ParseMode.MARKDOWN)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_SET_OFFSET:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        kb = ui_kb.offset_menu_kb()
        try:
            await _edit_text_safe(message, "⏳ Выберите оффсет", reply_markup=kb)
        except TelegramBadRequest:
            await _answer_safe(message, "⏳ Выберите оффсет", reply_markup=kb)
        await _callback_answer_safe(query)
        return

    if data in {constants.CB_OFF_DEC, constants.CB_OFF_INC} or data.startswith("off_p"):
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        entry = storage.get_chat_cfg_entry(message.chat.id)
        current = int(entry.get("offset", 30))
        if data == constants.CB_OFF_DEC:
            current = max(0, current - 5)
        elif data == constants.CB_OFF_INC:
            current += 5
        else:
            try:
                current = int(data.split("_p")[-1])
            except ValueError:
                current = 30
        storage.update_chat_cfg(message.chat.id, offset=current)
        await _answer_safe(message, f"⏳ Оффсет: {current} мин")
        await _callback_answer_safe(query)
        return

    if data == constants.CB_CHATS:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        await _show_chats(message, back_callback=constants.CB_ADMIN_PANEL)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_ARCHIVE:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        await _show_archive(message, user, page=1, back_callback=constants.CB_ADMIN_PANEL)
        await _callback_answer_safe(query)
        return

    if data.startswith(f"{constants.CB_ARCHIVE_PAGE}:"):
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        try:
            page = int(data.split(":", 1)[1])
        except ValueError:
            page = 1
        await _show_archive(message, user, page=page, back_callback=constants.CB_ADMIN_PANEL)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_ARCHIVE_CLEAR:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        text = (
            "❓ <b>Очистить архив?</b>\n"
            "Вы уверены? Это действие необратимо."
        )
        kb = ui_kb.archive_clear_confirm_kb()
        try:
            await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except TelegramBadRequest:
            await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_ARCHIVE_CLEAR_CONFIRM:
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        removed = storage.clear_archive()
        notice = "Архив очищен." if removed else "Архив уже пуст."
        await _show_archive(message, user, page=1, notice=notice, back_callback=constants.CB_ADMIN_PANEL)
        await _callback_answer_safe(query)
        return

    if data.startswith(f"{constants.CB_CHAT_DEL}:"):
        if not _is_admin(user):
            await _answer_safe(message, "⛔ Только администратор может менять настройки.")
            await _callback_answer_safe(query)
            return
        parts = data.split(":")
        chat_id = parts[1] if len(parts) > 1 else None
        try:
            topic_id = int(parts[2]) if len(parts) > 2 else 0
        except ValueError:
            topic_id = 0
        confirmed = len(parts) > 3 and parts[3] == "y"
        if chat_id is not None:
            if not confirmed:
                text = (
                    "❓ <b>Удалить чат из списка?</b>\n"
                    "Будут удалены все темы этого чата и связанные активные напоминания."
                )
                yes_data = f"{constants.CB_CHAT_DEL}:{chat_id}:{topic_id}:y"
                kb = ui_kb.confirm_kb(yes_data, constants.CB_CHATS)
                try:
                    await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
                except TelegramBadRequest:
                    await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
                await _callback_answer_safe(query)
                return
            storage.unregister_chat(chat_id, topic_id if topic_id else None)
            removed_by = _serialize_user(user)
            affected = storage.get_jobs_for_chat(chat_id, topic_id if topic_id else None)
            for rec in affected:
                job_id = rec.get("job_id")
                if not job_id:
                    continue
                _remove_job(
                    job_id,
                    archive_reason="chat_unregistered",
                    record=rec,
                    removed_by=removed_by,
                )
            await _show_chats(message, back_callback=constants.CB_ADMIN_PANEL)
        await _callback_answer_safe(query, "Удалено")
        return

    if data == constants.CB_ADMINS:
        if not _is_owner(user):
            await _answer_safe(message, "⛔ Только владелец может управлять администраторами.")
            await _callback_answer_safe(query)
            return
        await _show_admins(message)
        await _callback_answer_safe(query)
        return

    if data == constants.CB_ADMIN_ADD:
        if not _is_owner(user):
            await _answer_safe(message, "⛔ Только владелец может управлять администраторами.")
            await _callback_answer_safe(query)
            return
        await state.update_data({STATE_AWAIT_ADMIN_ADD: True})
        await _answer_safe(message, "Введи @username для добавления")
        await _callback_answer_safe(query)
        return

    if data.startswith(f"{constants.CB_ADMIN_DEL}:"):
        if not _is_owner(user):
            await _answer_safe(message, "⛔ Только владелец может управлять администраторами.")
            await _callback_answer_safe(query)
            return
        username = data.split(":", 1)[1]
        removed = storage.remove_admin_username(username)
        await _answer_safe(message, "✅ Удалён" if removed else "⚠️ Не найден")
        await _callback_answer_safe(query)
        return

    if data.startswith(f"{constants.CB_PICK_CHAT}:"):
        parts = data.split(":")
        if len(parts) < 4:
            await _callback_answer_safe(query, "Некорректные данные", show_alert=True)
            return
        chat_id_raw, topic_raw, token = parts[1], parts[2], parts[3]
        try:
            chat_id = int(chat_id_raw)
        except ValueError:
            chat_id = chat_id_raw
        topic_id = int(topic_raw) if topic_raw and topic_raw != "0" else None
        data_state = await state.get_data()
        pending = _cleanup_pending(dict(data_state.get(STATE_PENDING, {})))
        entry = pending.pop(token, None)
        await state.update_data({STATE_PENDING: pending})
        if not entry:
            await _callback_answer_safe(query, "Истекло", show_alert=True)
            return
        await schedule_reminder(
            message=message,
            source_chat_id=message.chat.id,
            target_chat_id=chat_id,
            user=user,
            text=entry.get("text", ""),
            topic_id=topic_id,
        )
        await state.update_data({
            STATE_LAST_TARGET: {"chat_id": chat_id, "topic_id": topic_id},
            STATE_FORCE_PICK: False,
        })
        await _callback_answer_safe(query, "Готово")
        return

    if data.startswith(f"{constants.CB_PICK_CHAT_PAGE}:"):
        parts = data.split(":")
        if len(parts) != 3:
            await _callback_answer_safe(query, "Некорректные данные", show_alert=True)
            return
        token = parts[1]
        try:
            page = int(parts[2])
        except ValueError:
            page = 1
        data_state = await state.get_data()
        pending = _cleanup_pending(dict(data_state.get(STATE_PENDING, {})))
        await state.update_data({STATE_PENDING: pending})
        entry = pending.get(token)
        if not entry:
            await _callback_answer_safe(query, "Истекло", show_alert=True)
            return
        targets = list(entry.get("targets", []))
        targets.append({"chat_id": message.chat.id, "title": "Личный чат", "topic_id": 0})
        kb = ui_kb.choose_chat_kb(
            targets,
            token,
            is_admin=_is_admin(user),
            page=page,
            page_size=CHAT_PICKER_PAGE_SIZE,
        )
        try:
            await _edit_text_safe(message, "📨 Куда отправить напоминание?", reply_markup=kb)
        except TelegramBadRequest:
            await _answer_safe(message, "📨 Куда отправить напоминание?", reply_markup=kb)
        await _callback_answer_safe(query)
        return

    if data.startswith(f"{constants.CB_ACTIONS}:"):
        parts = data.split(":")
        job_id = parts[1] if len(parts) > 1 else None
        if len(parts) > 2 and parts[2] == "close":
            target = parts[3] if len(parts) > 3 else None
            if target == "my":
                await _show_active(message, user, page=1, mine=True)
            else:
                await _show_active(message, user, page=1)
            await _callback_answer_safe(query)
            return
        if job_id:
            context = parts[2] if len(parts) > 2 else None
            await _open_actions(message, user, job_id, context=context)
            await _callback_answer_safe(query)
            return

    if data.startswith(f"{constants.CB_SENDNOW}:"):
        job_id, extras = _parse_job_callback(data, constants.CB_SENDNOW)
        if not job_id:
            await _callback_answer_safe(query, "Некорректные данные", show_alert=True)
            return
        job = _get_job(job_id)
        if not job:
            await _callback_answer_safe(query, "Не найдено", show_alert=True)
            return
        if not _can_manage_job(user, job):
            await _callback_answer_safe(query, "Недостаточно прав", show_alert=True)
            return
        sent = await send_reminder_job(job_id=job_id)
        view_hint = _resolve_view_hint(extras)
        if view_hint == "my":
            await _show_active(message, user, page=1, mine=True)
        elif view_hint == "all":
            await _show_active(message, user, page=1)
        await _callback_answer_safe(query, "Отправлено" if sent else "Не отправлено, повтор позже")
        return

    if data.startswith(f"{constants.CB_CANCEL}:"):
        job_id, extras = _parse_job_callback(data, constants.CB_CANCEL)
        if not job_id:
            await _callback_answer_safe(query, "Некорректные данные", show_alert=True)
            return
        job = _get_job(job_id)
        if job and not _can_manage_job(user, job):
            await _callback_answer_safe(query, "Недостаточно прав", show_alert=True)
            return
        if not extras or extras[-1] != "y":
            if not job:
                await _callback_answer_safe(query, "Не найдено", show_alert=True)
                return
            view_hint = _resolve_view_hint(extras)
            yes_parts = [constants.CB_CANCEL, job_id]
            if view_hint:
                yes_parts.append(view_hint)
            yes_parts.append("y")
            yes_data = ":".join(yes_parts)
            no_data = f"{constants.CB_ACTIONS}:{job_id}"
            if view_hint:
                no_data = f"{no_data}:{view_hint}"
            kb = ui_kb.confirm_kb(yes_data, no_data)
            preview = escape(job.get("text", "") or "")
            text = "❓ <b>Отменить напоминание?</b>\nВы уверены?"
            if preview:
                text = f"{text}\n\n<code>{preview}</code>"
            try:
                await _edit_text_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
            except TelegramBadRequest:
                await _answer_safe(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
            await _callback_answer_safe(query)
            return
        if job:
            _remove_job(
                job_id,
                archive_reason="manual_cancel",
                record=job,
                removed_by=_serialize_user(user),
            )
        else:
            _remove_job(job_id)
        if job:
            audit_log(
                "REM_CANCELED",
                reminder_id=job_id,
                chat_id=job.get("target_chat_id"),
                topic_id=job.get("topic_id"),
                user_id=getattr(user, "id", None),
                title=job.get("text"),
                reason="manual",
            )
        view_hint = _resolve_view_hint(extras)
        if view_hint == "my":
            await _show_active(message, user, page=1, mine=True)
        else:
            await _show_active(message, user, page=1)
        await _callback_answer_safe(query, "Удалено")
        return

    if data.startswith(f"{constants.CB_SHIFT}:"):
        parts = data.split(":")
        if len(parts) < 3:
            await _callback_answer_safe(query, "Некорректные данные", show_alert=True)
            return
        job_id = parts[1]
        try:
            minutes = int(parts[2])
        except ValueError:
            minutes = 5
        job = _get_job(job_id)
        if not job:
            await _callback_answer_safe(query, "Не найдено", show_alert=True)
            return
        if not _can_manage_job(user, job):
            await _callback_answer_safe(query, "Недостаточно прав", show_alert=True)
            return
        run_iso = job.get("run_at_utc")
        try:
            run_at = datetime.fromisoformat(run_iso) if run_iso else _utc_now()
            if run_at.tzinfo is None:
                run_at = run_at.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            run_at = _utc_now()
        new_run = run_at + timedelta(minutes=minutes)
        _update_job_time(job, new_run)
        audit_log(
            "REM_RESCHEDULED",
            reminder_id=job_id,
            chat_id=job.get("target_chat_id"),
            topic_id=job.get("topic_id"),
            title=job.get("text"),
            user_id=getattr(user, "id", None),
            when=new_run,
            reason="manual_shift",
        )
        await _callback_answer_safe(query, f"Сдвинуто на +{minutes} мин")
        return

    if data.startswith(f"{constants.CB_RRULE}:"):
        await _callback_answer_safe(query, "Повторы пока недоступны", show_alert=True)
        return

    await _callback_answer_safe(query, "Неизвестная кнопка", show_alert=True)

# === Lifecycle ===


async def on_startup(bot: Bot) -> None:
    commands = [
        BotCommand(command="start", description="Приветствие"),
        BotCommand(command="version", description="Текущая версия и история запусков"),
        BotCommand(command="admin", description="Админ-панель"),
    ]
    with suppress(Exception):
        await bot.set_my_commands(commands)
    send_reminder_job.bot = bot  # type: ignore[attr-defined]
    if not scheduler.running:
        scheduler.start()
    restore_jobs()
    removed = storage.compact_known_chats_by_chat_id()
    if removed:
        logger.info("Compacted known chats list: removed %s duplicate topic entries", removed)
    rev = release_history.record_startup_revision()
    logger.info(
        "Runtime revision: version=%s commit=%s branch=%s dirty=%s",
        rev.get("version"),
        rev.get("commit"),
        rev.get("branch"),
        rev.get("dirty"),
    )
    logger.info("Startup complete")


async def on_shutdown() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
    logger.info("Shutdown complete")
    log_utils.set_error_burst_callback(None)


async def main() -> None:
    cfg = storage.get_cfg()
    token = (cfg.get("token") if isinstance(cfg, dict) else None) or constants.BOT_TOKEN
    if not token:
        raise SystemExit("Token not configured")
    bot = Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    _install_error_burst_notifier(bot)
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.middleware(ErrorsMiddleware())
    dp.callback_query.middleware(ErrorsMiddleware())
    dp.include_router(router)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
