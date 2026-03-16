from __future__ import annotations

from typing import Dict

from .constants import FEATURE_FLAGS_PATH
from .storage import load_json, save_json

DEFAULT_FLAGS: Dict[str, bool] = {
    # Master switch for error burst notifications.
    "alerts.error_burst.enabled": True,
    # Send alerts only to known owners (e.g. panykovc).
    "alerts.owner_only": True,
}

FLAG_LABELS: Dict[str, str] = {
    "alerts.error_burst.enabled": "Алерты о всплесках ошибок",
    "alerts.owner_only": "Отправлять алерты только владельцу (panykovc)",
}

FLAG_DESCRIPTIONS: Dict[str, str] = {
    "alerts.error_burst.enabled": "Если включено, бот отправляет уведомления при серии ошибок.",
    "alerts.owner_only": "Если включено, алерты уходят только владельцу, а не всем администраторам.",
}

FLAG_ORDER: list[str] = [
    "alerts.error_burst.enabled",
    "alerts.owner_only",
]


def _load_flags() -> Dict[str, bool]:
    raw = load_json(FEATURE_FLAGS_PATH, {})
    if not isinstance(raw, dict):
        raw = {}
    flags: Dict[str, bool] = dict(DEFAULT_FLAGS)
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, bool):
            flags[key] = value
    return flags


def is_enabled(name: str, default: bool = False) -> bool:
    flags = _load_flags()
    return bool(flags.get(name, default))


def set_flag(name: str, value: bool) -> None:
    flags = _load_flags()
    flags[name] = bool(value)
    save_json(FEATURE_FLAGS_PATH, flags)


def list_flags() -> Dict[str, bool]:
    return _load_flags()


def list_flags_ordered() -> list[tuple[str, bool, str, str]]:
    flags = _load_flags()
    ordered_names = [name for name in FLAG_ORDER if name in flags] + [
        name for name in flags.keys() if name not in FLAG_ORDER
    ]
    result: list[tuple[str, bool, str, str]] = []
    for name in ordered_names:
        label = FLAG_LABELS.get(name, name)
        description = FLAG_DESCRIPTIONS.get(name, "")
        result.append((name, bool(flags.get(name, False)), label, description))
    return result
