from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from .constants import MEETING_REGEX, REMINDER_TEMPLATE

logger = logging.getLogger("reminder-bot.aiogram")


def _localize(tz, dt: datetime) -> datetime:
    """Attach timezone info to ``dt`` supporting both pytz and zoneinfo."""

    if hasattr(tz, "localize"):
        return tz.localize(dt)  # type: ignore[no-any-return]
    return dt.replace(tzinfo=tz)


def parse_meeting_message(text: str, tz) -> Optional[Dict[str, Any]]:
    """Разобрать строку вида ``ДД.ММ ТИП ЧЧ:ММ ПЕРЕГ [НОМЕР]``.

    Возвращает словарь с ключами:
    ``dt_local`` (aware ``datetime``), ``date_str``, ``time_str``, ``type``,
    ``room``, ``ticket``, ``canonical_full`` и ``reminder_text``.
    """

    match = MEETING_REGEX.match(text or "")
    if not match:
        return None

    day_str, month_str, meeting_type, time_part, room, ticket = match.groups()
    try:
        day = int(day_str)
        month = int(month_str)
        hour_str, minute_str = time_part.replace(".", ":", 1).split(":", 1)
        hour = int(hour_str)
        minute = int(minute_str)
    except (TypeError, ValueError):
        logger.debug("parse_meeting_message: failed to convert numbers", exc_info=True)
        return None

    now = datetime.now(tz)
    year = now.year

    try:
        candidate = _localize(tz, datetime(year, month, day, hour, minute))
    except Exception:
        logger.debug("parse_meeting_message: invalid date", exc_info=True)
        return None

    # если выбранное время уже прошло в текущем году — переносим на следующий
    if candidate <= now:
        try:
            candidate = _localize(tz, datetime(year + 1, month, day, hour, minute))
        except Exception:
            logger.debug("parse_meeting_message: invalid rollover date", exc_info=True)
            return None

    date_str = f"{day:02d}.{month:02d}"
    time_str = f"{hour:02d}:{minute:02d}"
    meeting_type = meeting_type.strip()
    room = room.strip()
    ticket = (ticket or "").strip()
    canonical_parts = [date_str, meeting_type, time_str, room]
    if ticket:
        canonical_parts.append(ticket)
    canonical = " ".join(canonical_parts)
    ticket_placeholder = f" {ticket}" if ticket else ""

    return {
        "dt_local": candidate,
        "date_str": date_str,
        "time_str": time_str,
        "type": meeting_type,
        "room": room,
        "ticket": ticket,
        "canonical_full": canonical,
        "reminder_text": REMINDER_TEMPLATE.format(
            date=date_str,
            type=meeting_type,
            time=time_str,
            room=room,
            ticket=ticket_placeholder,
        ),
    }
