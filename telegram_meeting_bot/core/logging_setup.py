from __future__ import annotations

import hashlib
import json
import logging
import os
import traceback
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from .constants import (
    APP_LOG_RETENTION_DAYS,
    AUDIT_LOG_RETENTION_DAYS,
    ERROR_LOG_BACKUP_COUNT,
    ERROR_LOG_MAX_BYTES,
    LOGS_APP_DIR,
    LOGS_AUDIT_DIR,
    LOGS_ERROR_DIR,
)
from .logs import ERROR_BURST_MONITOR

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
RUN_ID = uuid.uuid4().hex


def _utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


class DailyFileHandler(logging.Handler):
    """Write logs to a file per day and prune old files."""

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
        self.retention_days = max(0, retention_days)
        self.encoding = encoding
        self._current_date: Optional[date] = None
        self._stream: Optional[Any] = None
        self._ensure_stream()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._ensure_stream()
            assert self._stream is not None
            msg = self.format(record)
            self._stream.write(msg + "\n")
            self._stream.flush()
        except Exception:
            self.handleError(record)

    def _ensure_stream(self) -> None:
        today = date.today()
        if self._stream is not None and self._current_date == today:
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
        self._current_date = today
        path = self.directory / f"{self.prefix}_{today.isoformat()}.log"
        self._stream = path.open("a", encoding=self.encoding)
        self._cleanup()

    def _cleanup(self) -> None:
        if self.retention_days <= 0:
            return

        cutoff = datetime.utcnow() - timedelta(days=self.retention_days)
        for log_path in self.directory.glob(f"{self.prefix}_*.log"):
            try:
                if datetime.utcfromtimestamp(log_path.stat().st_mtime) < cutoff:
                    log_path.unlink()
            except OSError:
                continue

    def close(self) -> None:
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
        self._stream = None
        super().close()


class SizedJSONFileHandler(logging.Handler):
    """Rotate JSON log files by size while keeping date-based naming."""

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
        self.backup_count = max(0, backup_count)
        self.encoding = encoding
        self._stream: Optional[Any] = None
        self._path: Optional[Path] = None
        self._ensure_stream()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._ensure_stream()
            assert self._stream is not None
            if self._stream.tell() >= self.max_bytes:
                self._rotate()
            msg = self.format(record)
            self._stream.write(msg + "\n")
            self._stream.flush()
        except Exception:
            self.handleError(record)

    def _ensure_stream(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        desired = self.directory / f"{self.prefix}_{date.today().isoformat()}.log"
        if self._path == desired and self._stream is not None:
            return
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
        self._path = desired
        self._stream = desired.open("a", encoding=self.encoding)
        self._cleanup()

    def _rotate(self) -> None:
        if not self._path or not self._stream:
            return
        try:
            self._stream.close()
        except Exception:
            pass
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        rotated = self.directory / f"{self.prefix}_{timestamp}.log"
        try:
            self._path.rename(rotated)
        except OSError:
            rotated = self.directory / f"{self.prefix}_{timestamp}_{uuid.uuid4().hex[:6]}.log"
            try:
                self._path.rename(rotated)
            except OSError:
                rotated = None
        self._stream = self._path.open("a", encoding=self.encoding)
        self._cleanup()

    def _cleanup(self) -> None:
        if self.backup_count <= 0:
            return

        files = sorted(
            self.directory.glob(f"{self.prefix}_*.log"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for path in files[self.backup_count :]:
            try:
                path.unlink()
            except OSError:
                continue

    def close(self) -> None:
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
        self._stream = None
        super().close()


class AuditJSONFormatter(logging.Formatter):
    KEYS = (
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
        payload.setdefault("ts", _utc_iso())
        payload.setdefault("run_id", RUN_ID)
        payload.setdefault("event", getattr(record, "event", None) or payload.get("event"))
        for key in self.KEYS:
            payload.setdefault(key, None)
        return json.dumps(payload, ensure_ascii=False)


class ErrorJSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = dict(getattr(record, "json_payload", {}) or {})
        message = record.getMessage()
        payload.setdefault("ts", _utc_iso())
        payload.setdefault("where", record.name)
        payload.setdefault("message", message.splitlines()[0] if message else "")
        payload.setdefault("type", getattr(record, "error_type", None) or "ERROR")
        payload.setdefault("run_id", RUN_ID)
        if record.exc_info:
            stack_text = "".join(traceback.format_exception(*record.exc_info))
            payload.setdefault("stack", stack_text)
            payload.setdefault(
                "stack_id",
                hashlib.blake2b(stack_text.encode("utf-8"), digest_size=6).hexdigest(),
            )
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str | int | None = None) -> logging.Logger:
    """Configure application, audit and error loggers."""

    for path in (LOGS_APP_DIR, LOGS_AUDIT_DIR, LOGS_ERROR_DIR):
        Path(path).mkdir(parents=True, exist_ok=True)

    if isinstance(level, str):
        resolved_level = getattr(logging, level.upper(), logging.INFO)
    elif isinstance(level, int):
        resolved_level = level
    else:
        resolved_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(resolved_level)
    root_logger.handlers.clear()
    logging.captureWarnings(True)

    error_handler = SizedJSONFileHandler(
        LOGS_ERROR_DIR,
        "error",
        max_bytes=ERROR_LOG_MAX_BYTES,
        backup_count=ERROR_LOG_BACKUP_COUNT,
    )
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(ErrorJSONFormatter())
    root_logger.addHandler(error_handler)

    ERROR_BURST_MONITOR.reset()
    root_logger.addHandler(ERROR_BURST_MONITOR)

    if os.environ.get("BOT_CONSOLE_LOGS", "1") != "0":
        console = logging.StreamHandler()
        console.setLevel(resolved_level)
        console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", DATE_FORMAT))
        root_logger.addHandler(console)

    app_logger = logging.getLogger("reminder-bot.aiogram")
    app_logger.handlers.clear()
    app_logger.setLevel(logging.INFO)
    app_logger.propagate = False

    app_handler = DailyFileHandler(
        LOGS_APP_DIR,
        "app",
        retention_days=APP_LOG_RETENTION_DAYS,
    )
    app_handler.setLevel(logging.INFO)
    app_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", DATE_FORMAT))
    app_logger.addHandler(app_handler)

    audit_logger = logging.getLogger("reminder.audit")
    audit_logger.handlers.clear()
    audit_logger.setLevel(logging.INFO)
    audit_logger.propagate = False

    audit_handler = DailyFileHandler(
        LOGS_AUDIT_DIR,
        "audit",
        retention_days=AUDIT_LOG_RETENTION_DAYS,
    )
    audit_handler.setLevel(logging.INFO)
    audit_handler.setFormatter(AuditJSONFormatter())
    audit_logger.addHandler(audit_handler)

    error_logger = logging.getLogger("reminder.error")
    error_logger.handlers.clear()
    error_logger.setLevel(logging.WARNING)
    error_logger.propagate = False
    error_logger.addHandler(error_handler)
    error_logger.addHandler(ERROR_BURST_MONITOR)

    return app_logger


__all__ = [
    "setup_logging",
    "DailyFileHandler",
    "SizedJSONFileHandler",
    "AuditJSONFormatter",
    "ErrorJSONFormatter",
]

