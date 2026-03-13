"""Точка входа для запуска бота как модуля или скрипта."""

from __future__ import annotations

import logging
from pathlib import Path
import sys


def _resolve_main():
    """Вернуть модуль :mod:`telegram_meeting_bot.bot.main`.

    При запуске ``python -m telegram_meeting_bot`` структура пакета доступна,
    и работает относительный импорт. Если ``__main__.py`` запускают напрямую,
    мы добавляем родительскую директорию в ``sys.path``, чтобы абсолютный
    импорт разрешился корректно.
    """

    if __package__ in {None, ""}:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        return __import__("telegram_meeting_bot.bot.main", fromlist=["main"])
    return __import__(__package__ + ".bot.main", fromlist=["main"])


def main() -> None:
    """Импортировать и выполнить функцию ``main`` пакета."""
    try:
        module = _resolve_main()
    except ModuleNotFoundError as exc:  # pragma: no cover - зависимости при импорте
        missing = exc.name
        raise SystemExit(f"Отсутствует зависимость: {missing}") from exc

    logger = getattr(module, "logger", logging.getLogger("telegram_meeting_bot.__main__"))

    try:
        module.main()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановлено пользователем")


if __name__ == "__main__":  # pragma: no cover - запуск из CLI
    main()

