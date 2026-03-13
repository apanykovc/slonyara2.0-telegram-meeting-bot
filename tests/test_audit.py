import logging
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from telegram_meeting_bot.core.audit import audit_log


@pytest.mark.usefixtures("caplog")
def test_audit_log_includes_expected_payload(caplog):
    caplog.set_level(logging.INFO, logger="reminder.audit")

    when = datetime(2024, 1, 2, 3, 4, 5)
    audit_log(
        "MEETING_ARCHIVED",
        user_id=123,
        chat_id=-100500,
        title="  Weekly   sync  ",
        when=when,
        extra_field="value",
    )

    records = [r for r in caplog.records if r.name == "reminder.audit"]
    assert records, "audit logger must emit a record"
    record = records[0]
    payload = getattr(record, "json_payload", {})

    assert payload["event"] == "MEETING_ARCHIVED"
    assert payload["user_id"] == 123
    assert payload["chat_id"] == -100500
    assert payload["title"] == "Weekly sync"
    assert payload["when"] == when.isoformat()
    assert payload["extra_field"] == "value"
