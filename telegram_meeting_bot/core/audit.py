"""Audit logging helpers shared across bot implementations."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
import logging

from .logging_setup import RUN_ID


def _iso_ts() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


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


_AUDIT_LOGGER = logging.getLogger("reminder.audit")


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
    _AUDIT_LOGGER.info("", extra={"json_payload": payload})


__all__ = ["audit_log"]
