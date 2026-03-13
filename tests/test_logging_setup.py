import json
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from telegram_meeting_bot.core.logging_setup import (
    DailyFileHandler,
    ErrorJSONFormatter,
    SizedJSONFileHandler,
)


def _make_record(message: str = "test") -> logging.LogRecord:
    return logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=0,
        msg=message,
        args=(),
        exc_info=None,
    )


def test_daily_file_handler_retention_zero(tmp_path: Path) -> None:
    old_log = tmp_path / "app_2000-01-01.log"
    old_log.write_text("legacy")
    ten_days = 10 * 24 * 60 * 60
    past = time.time() - ten_days
    os.utime(old_log, (past, past))

    handler = DailyFileHandler(tmp_path, "app", retention_days=0)
    try:
        handler.emit(_make_record())
        assert old_log.exists(), "Retention=0 must preserve historical files"
    finally:
        handler.close()


def test_sized_json_handler_backup_zero(tmp_path: Path) -> None:
    old_files = []
    for idx in range(3):
        path = tmp_path / f"error_2024-01-0{idx + 1}.log"
        path.write_text("legacy")
        past = time.time() - (idx + 1) * 60
        os.utime(path, (past, past))
        old_files.append(path)

    handler = SizedJSONFileHandler(tmp_path, "error", max_bytes=128, backup_count=0)
    try:
        handler.emit(_make_record())
        for path in old_files:
            assert path.exists(), "Unlimited backup should not prune rotated files"
    finally:
        handler.close()


def test_error_json_formatter_includes_stack() -> None:
    formatter = ErrorJSONFormatter()
    record = _make_record("boom")
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        record.exc_info = sys.exc_info()

    payload = json.loads(formatter.format(record))
    assert payload["type"] == "ERROR"
    assert payload["stack"].startswith("Traceback")
    assert len(payload["stack_id"]) == 12
