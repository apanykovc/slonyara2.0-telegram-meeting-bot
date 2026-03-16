from __future__ import annotations

from typing import Sequence

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from ..core import feature_flags
from ..core import logs as log_utils
from ..core.constants import (
    ACTIVE_FILTER_ALL,
    ACTIVE_FILTER_TODAY,
    ACTIVE_FILTER_TOMORROW,
    ACTIVE_FILTER_WEEK,
    CB_ACTIONS,
    CB_ACTIVE,
    CB_ACTIVE_CLEAR,
    CB_ACTIVE_FILTER,
    CB_ACTIVE_PAGE,
    CB_ADMIN_ADD,
    CB_ADMIN_BACKUP,
    CB_ADMIN_DEL,
    CB_ADMIN_FLAG_TOGGLE,
    CB_ADMIN_FLAGS,
    CB_ADMIN_HISTORY,
    CB_ADMIN_PANEL,
    CB_ADMIN_STATUS,
    CB_ADMIN_VERIFY_DB,
    CB_ADMINS,
    CB_ARCHIVE,
    CB_ARCHIVE_CLEAR,
    CB_ARCHIVE_CLEAR_CONFIRM,
    CB_ARCHIVE_PAGE,
    CB_CANCEL,
    CB_CHAT_DEL,
    CB_CHATS,
    CB_CREATE,
    CB_HELP,
    CB_LOGS,
    CB_LOGS_APP,
    CB_LOGS_AUDIT,
    CB_LOGS_CLEAR,
    CB_LOGS_CLEAR_CONFIRM,
    CB_LOGS_DOWNLOAD,
    CB_LOGS_ERROR,
    CB_LOGS_FILE,
    CB_MENU,
    CB_MY,
    CB_MY_FILTER,
    CB_OFF_DEC,
    CB_OFF_INC,
    CB_OFF_PRESET_10,
    CB_OFF_PRESET_15,
    CB_OFF_PRESET_20,
    CB_OFF_PRESET_30,
    CB_PICK_CHAT,
    CB_PICK_CHAT_PAGE,
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
    RR_DAILY,
    RR_ONCE,
    RR_WEEKLY,
)
from ..core.logs import LogFileInfo


def _format_size(value: int) -> str:
    units = ["Б", "КБ", "МБ", "ГБ"]
    size = float(max(value, 0))
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "Б":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} ГБ"


_LOG_TYPE_TO_CALLBACK = {
    log_utils.LOG_TYPE_APP: CB_LOGS_APP,
    log_utils.LOG_TYPE_AUDIT: CB_LOGS_AUDIT,
    log_utils.LOG_TYPE_ERROR: CB_LOGS_ERROR,
}


def main_menu_kb(
    is_admin: bool = False,
    *,
    allow_settings: bool = False,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="🆕 Создать встречу", callback_data=CB_CREATE)],
        [InlineKeyboardButton(text="📂 Мои встречи", callback_data=CB_MY)],
    ]
    if is_admin:
        rows[-1].append(InlineKeyboardButton(text="📝 Активные", callback_data=CB_ACTIVE))
        rows.append([InlineKeyboardButton(text="🛡️ Админ-панель", callback_data=CB_ADMIN_PANEL)])
    elif allow_settings:
        rows.append([InlineKeyboardButton(text="⚙️ Настройки", callback_data=CB_SETTINGS)])
    rows.append([InlineKeyboardButton(text="❓ Справка", callback_data=CB_HELP)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def reply_menu_kb(
    is_admin: bool = False,
    *,
    allow_settings: bool = False,
) -> ReplyKeyboardMarkup:
    """Отдельная клавиатура под строкой ввода с ключевыми действиями."""

    rows: list[list[KeyboardButton]] = [
        [
            KeyboardButton(text="➕ Создать встречу"),
            KeyboardButton(text="📂 Мои встречи"),
        ]
    ]
    if is_admin:
        rows.append(
            [
                KeyboardButton(text="📝 Активные"),
                KeyboardButton(text="🛡️ Админ-панель"),
            ]
        )
    elif allow_settings:
        rows.append([KeyboardButton(text="⚙️ Настройки")])
    rows.append([KeyboardButton(text="❓ Справка")])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выберите действие…",
    )


def settings_menu_kb(is_owner: bool = False, *, back_callback: str = CB_MENU) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="🕒 Таймзона", callback_data=CB_SET_TZ)],
        [InlineKeyboardButton(text="⏳ Оффсет (мин)", callback_data=CB_SET_OFFSET)],
        [InlineKeyboardButton(text="📋 Чаты", callback_data=CB_CHATS)],
        [InlineKeyboardButton(text="📦 Архив", callback_data=CB_ARCHIVE)],
        [InlineKeyboardButton(text="📜 Логи", callback_data=CB_LOGS)],
    ]
    if is_owner:
        rows.append([InlineKeyboardButton(text="👥 Админы", callback_data=CB_ADMINS)])
    rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback),
            InlineKeyboardButton(text="🏠 В меню", callback_data=CB_MENU),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def tz_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Локальная ОС", callback_data=CB_SET_TZ_LOCAL)],
            [InlineKeyboardButton(text="Europe/Moscow", callback_data=CB_SET_TZ_MOSCOW)],
            [InlineKeyboardButton(text="America/Chicago", callback_data=CB_SET_TZ_CHICAGO)],
            [InlineKeyboardButton(text="Ввести вручную", callback_data=CB_SET_TZ_ENTER)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_SETTINGS)],
        ]
    )


def offset_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="−5", callback_data=CB_OFF_DEC),
                InlineKeyboardButton(text="+5", callback_data=CB_OFF_INC),
            ],
            [
                InlineKeyboardButton(text="10", callback_data=CB_OFF_PRESET_10),
                InlineKeyboardButton(text="15", callback_data=CB_OFF_PRESET_15),
                InlineKeyboardButton(text="20", callback_data=CB_OFF_PRESET_20),
                InlineKeyboardButton(text="30", callback_data=CB_OFF_PRESET_30),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_SETTINGS)],
        ]
    )


def chats_menu_kb(known_chats: list | None = None, *, back_callback: str = CB_SETTINGS) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if known_chats:
        seen_chat_ids: set[str] = set()
        for chat in known_chats:
            chat_id = chat.get("chat_id")
            key = str(chat_id)
            if key in seen_chat_ids:
                continue
            seen_chat_ids.add(key)
            title = chat.get("title") or str(chat_id)
            rows.append(
                [
                    InlineKeyboardButton(text=title, callback_data=CB_CHATS),
                    InlineKeyboardButton(
                        text="❌",
                        callback_data=f"{CB_CHAT_DEL}:{chat_id}:0",
                    ),
                ]
            )
    else:
        rows.append([InlineKeyboardButton(text="(пусто)", callback_data=CB_CHATS)])
    rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback),
            InlineKeyboardButton(text="🏠 В меню", callback_data=CB_MENU),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def logs_menu_kb(*, back_callback: str = CB_SETTINGS) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📗 App", callback_data=CB_LOGS_APP)],
            [InlineKeyboardButton(text="🧾 Audit", callback_data=CB_LOGS_AUDIT)],
            [InlineKeyboardButton(text="❌ Error", callback_data=CB_LOGS_ERROR)],
            [InlineKeyboardButton(text="📥 Скачать все", callback_data=CB_LOGS_DOWNLOAD)],
            [InlineKeyboardButton(text="🧹 Очистить", callback_data=CB_LOGS_CLEAR)],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback),
                InlineKeyboardButton(text="🏠 В меню", callback_data=CB_MENU),
            ],
        ]
    )


def log_files_kb(log_type: str, files: Sequence[LogFileInfo]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    kind = log_type.lower()
    for info in files:
        label = info.label or info.name
        size_label = _format_size(info.size_bytes)
        text = f"{label} • {size_label}"
        callback = f"{CB_LOGS_FILE}:{kind}:{info.name}"
        rows.append([InlineKeyboardButton(text=text, callback_data=callback)])
    rows.append([InlineKeyboardButton(text="📥 Скачать все", callback_data=CB_LOGS_DOWNLOAD)])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_LOGS)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def log_file_view_kb(log_type: str) -> InlineKeyboardMarkup:
    kind = log_type.lower()
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ К файлам",
                    callback_data=_LOG_TYPE_TO_CALLBACK.get(kind, CB_LOGS),
                )
            ],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_LOGS),
                InlineKeyboardButton(text="🏠 В меню", callback_data=CB_MENU),
            ],
        ]
    )


def logs_clear_confirm_kb(*, back_callback: str = CB_LOGS) -> InlineKeyboardMarkup:
    return confirm_kb(CB_LOGS_CLEAR_CONFIRM, back_callback)


def job_kb(job_id: str, rrule: str = RR_ONCE) -> InlineKeyboardMarkup:
    label = {
        RR_ONCE: "🔁 Разово",
        RR_DAILY: "🔁 Ежедневно",
        RR_WEEKLY: "🔁 Еженедельно",
    }.get(rrule, "🔁 Разово")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"{CB_CANCEL}:{job_id}")],
            [
                InlineKeyboardButton(text="➕ +5 мин", callback_data=f"{CB_SHIFT}:{job_id}:5"),
                InlineKeyboardButton(text="➕ +10 мин", callback_data=f"{CB_SHIFT}:{job_id}:10"),
            ],
            [InlineKeyboardButton(text=label, callback_data=f"{CB_RRULE}:{job_id}:{rrule}")],
        ]
    )


def choose_chat_kb(
    chats: list,
    token: str,
    *,
    is_admin: bool = False,
    page: int = 1,
    page_size: int = 12,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    seen_chat_ids: set[str] = set()
    unique: list[dict] = []
    for chat in chats:
        chat_id = chat.get("chat_id")
        key = str(chat_id)
        if key in seen_chat_ids:
            continue
        seen_chat_ids.add(key)
        unique.append(chat)

    total = len(unique)
    pages_total = max(1, (total + page_size - 1) // page_size) if total else 1
    page = max(1, min(page, pages_total))
    start = (page - 1) * page_size
    chunk = unique[start : start + page_size]

    for chat in chunk:
        chat_id = chat.get("chat_id")
        try:
            topic_id = int(chat.get("topic_id") or 0)
        except (TypeError, ValueError):
            topic_id = 0
        title = str(chat.get("title") or chat_id or "")
        title = title.strip() or str(chat_id)
        if len(title) > 56:
            title = f"{title[:53]}..."
        rows.append(
            [
                InlineKeyboardButton(
                    text=title,
                    callback_data=f"{CB_PICK_CHAT}:{chat_id}:{topic_id}:{token}",
                )
            ]
        )
    nav: list[InlineKeyboardButton] = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{CB_PICK_CHAT_PAGE}:{token}:{page-1}"))
    if page < pages_total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{CB_PICK_CHAT_PAGE}:{token}:{page+1}"))
    if nav:
        rows.append(nav)
    if is_admin:
        rows.append([InlineKeyboardButton(text="📝 Активные", callback_data=CB_ACTIVE)])
    rows.append([InlineKeyboardButton(text="❓ Справка", callback_data=CB_HELP)])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data=CB_MENU)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def active_kb(
    chunk: list,
    page: int,
    pages_total: int,
    uid: int,
    is_admin: bool = False,
    *,
    page_prefix: str = CB_ACTIVE_PAGE,
    view: str = "all",
    filter_mode: str = ACTIVE_FILTER_ALL,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for job in chunk:
        job_id = job.get("job_id")
        if not job_id:
            continue
        if is_admin or job.get("author_id") == uid:
            label = job.get("text", "")
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"⚙️ {label}", callback_data=f"{CB_ACTIONS}:{job_id}:{view}"
                    )
                ]
            )
    nav: list[InlineKeyboardButton] = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{page_prefix}:{page-1}:{filter_mode}"))
    if page < pages_total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{page_prefix}:{page+1}:{filter_mode}"))
    if nav:
        rows.append(nav)
    else:
        rows.append([InlineKeyboardButton(text="⟲ Обновить", callback_data=f"{page_prefix}:{page}:{filter_mode}")])
    filter_cb = CB_MY_FILTER if view == "my" else CB_ACTIVE_FILTER
    filter_buttons = [
        ("Все", ACTIVE_FILTER_ALL),
        ("Сегодня", ACTIVE_FILTER_TODAY),
        ("Завтра", ACTIVE_FILTER_TOMORROW),
        ("7 дней", ACTIVE_FILTER_WEEK),
    ]
    rows.append(
        [
            InlineKeyboardButton(
                text=("• " + label) if mode == filter_mode else label,
                callback_data=f"{filter_cb}:{mode}",
            )
            for label, mode in filter_buttons
        ]
    )
    if is_admin and view == "all" and chunk:
        rows.append([
            InlineKeyboardButton(
                text="🧹 Очистить все",
                callback_data=f"{CB_ACTIVE_CLEAR}:{view}:{page}:{filter_mode}",
            )
        ])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data=CB_MENU)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def archive_kb(
    page: int,
    pages_total: int,
    *,
    has_entries: bool,
    can_clear: bool,
    back_callback: str = CB_SETTINGS,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    nav: list[InlineKeyboardButton] = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{CB_ARCHIVE_PAGE}:{page-1}"))
    if page < pages_total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{CB_ARCHIVE_PAGE}:{page+1}"))
    if nav:
        rows.append(nav)
    else:
        rows.append([InlineKeyboardButton(text="⟲ Обновить", callback_data=f"{CB_ARCHIVE_PAGE}:{page}")])
    if can_clear and has_entries:
        rows.append([InlineKeyboardButton(text="🧹 Очистить", callback_data=CB_ARCHIVE_CLEAR)])
    rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback),
            InlineKeyboardButton(text="🏠 В меню", callback_data=CB_MENU),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def archive_clear_confirm_kb() -> InlineKeyboardMarkup:
    return confirm_kb(CB_ARCHIVE_CLEAR_CONFIRM, CB_ARCHIVE)


def confirm_kb(yes_data: str, no_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да", callback_data=yes_data)],
            [InlineKeyboardButton(text="❌ Нет", callback_data=no_data)],
        ]
    )


def active_clear_confirm_kb(
    page: int,
    *,
    view: str = "all",
    page_prefix: str = CB_ACTIVE_PAGE,
    filter_mode: str = ACTIVE_FILTER_ALL,
) -> InlineKeyboardMarkup:
    yes_data = f"{CB_ACTIVE_CLEAR}:{view}:{page}:{filter_mode}:y"
    no_data = f"{page_prefix}:{page}:{filter_mode}"
    return confirm_kb(yes_data, no_data)


def actions_kb(
    job_id: str,
    is_admin: bool = False,
    *,
    return_to: str | None = None,
) -> InlineKeyboardMarkup:
    suffix = f":{return_to}" if return_to else ""
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="📤 Отправить сейчас", callback_data=f"{CB_SENDNOW}:{job_id}{suffix}")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"{CB_CANCEL}:{job_id}{suffix}")],
    ]
    if is_admin:
        rows.append(
            [
                InlineKeyboardButton(text="➕ +5", callback_data=f"{CB_SHIFT}:{job_id}:5"),
                InlineKeyboardButton(text="➕ +10", callback_data=f"{CB_SHIFT}:{job_id}:10"),
            ]
        )
    rows.append(
        [InlineKeyboardButton(text="↩️ Назад", callback_data=f"{CB_ACTIONS}:{job_id}:close{suffix}")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admins_menu_kb(admins: set[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for name in sorted(admins):
        rows.append(
            [InlineKeyboardButton(text=f"❌ @{name}", callback_data=f"{CB_ADMIN_DEL}:{name}")]
        )
    rows.append([InlineKeyboardButton(text="➕ Добавить", callback_data=CB_ADMIN_ADD)])
    rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_PANEL),
            InlineKeyboardButton(text="🏠 В меню", callback_data=CB_MENU),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_panel_kb(*, is_owner: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="🧭 Статус", callback_data=CB_ADMIN_STATUS)],
        [InlineKeyboardButton(text="🚩 Флаги", callback_data=CB_ADMIN_FLAGS)],
        [InlineKeyboardButton(text="🗄️ Бэкап", callback_data=CB_ADMIN_BACKUP)],
        [InlineKeyboardButton(text="🧪 Проверка БД", callback_data=CB_ADMIN_VERIFY_DB)],
        [InlineKeyboardButton(text="🧬 История версий", callback_data=CB_ADMIN_HISTORY)],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data=CB_SETTINGS)],
        [InlineKeyboardButton(text="📋 Чаты", callback_data=CB_CHATS)],
        [InlineKeyboardButton(text="📦 Архив", callback_data=CB_ARCHIVE)],
        [InlineKeyboardButton(text="📜 Логи", callback_data=CB_LOGS)],
    ]
    if is_owner:
        rows.append([InlineKeyboardButton(text="👥 Админы", callback_data=CB_ADMINS)])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data=CB_MENU)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_flags_kb(flags: dict[str, bool]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for name, value, label, _ in feature_flags.list_flags_ordered():
        state_icon = "✅" if value else "❌"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{state_icon} {label}",
                    callback_data=f"{CB_ADMIN_FLAG_TOGGLE}:{name}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_PANEL),
            InlineKeyboardButton(text="🏠 В меню", callback_data=CB_MENU),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def panel_kb(is_admin: bool = False) -> InlineKeyboardMarkup:
    return main_menu_kb(is_admin)
