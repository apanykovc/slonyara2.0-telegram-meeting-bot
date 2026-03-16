from __future__ import annotations

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if "pytz" not in sys.modules:
    pytz_stub = types.ModuleType("pytz")
    pytz_stub.BaseTzInfo = object  # type: ignore[attr-defined]
    pytz_stub.timezone = lambda name: name  # type: ignore[assignment]
    pytz_stub.utc = "UTC"
    sys.modules["pytz"] = pytz_stub

if "tzlocal" not in sys.modules:
    tzlocal_stub = types.ModuleType("tzlocal")

    def _fake_get_localzone_name() -> str:
        return "UTC"

    tzlocal_stub.get_localzone_name = _fake_get_localzone_name  # type: ignore[attr-defined]
    sys.modules["tzlocal"] = tzlocal_stub

import pytest

from telegram_meeting_bot.core import storage


@pytest.fixture(autouse=True)
def isolate_storage_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    chats_path = tmp_path / "chats.json"
    cfg_path = tmp_path / "config.json"
    admins_path = tmp_path / "admins.json"
    jobs_db_path = tmp_path / "reminders.db"
    legacy_jobs_path = tmp_path / "reminders.json"
    owners_meta_path = tmp_path / "owners_meta.json"
    monkeypatch.setattr(storage, "TARGETS_PATH", chats_path)
    monkeypatch.setattr(storage, "CFG_PATH", cfg_path)
    monkeypatch.setattr(storage, "ADMINS_PATH", admins_path)
    monkeypatch.setattr(storage, "JOBS_DB_PATH", jobs_db_path)
    monkeypatch.setattr(storage, "LEGACY_JOBS_PATH", legacy_jobs_path)
    monkeypatch.setattr(storage, "OWNERS_META_PATH", owners_meta_path)
    yield


def _load_known_chats(path: Path) -> list:
    if not path.exists():
        return []
    return storage.load_json(path, [])


def test_register_chat_updates_title(tmp_path: Path):
    path = storage.TARGETS_PATH
    assert _load_known_chats(path) == []

    assert storage.register_chat(123, "Old title") is True
    chats = _load_known_chats(path)
    assert chats[0]["title"] == "Old title"

    assert storage.register_chat(123, "New title") is False
    chats = _load_known_chats(path)
    assert chats[0]["title"] == "New title"


def test_register_chat_updates_topic_title(tmp_path: Path):
    path = storage.TARGETS_PATH

    storage.register_chat(123, "Chat", topic_id=50, topic_title="Old topic")
    storage.register_chat(123, "Chat", topic_id=50, topic_title="New topic")

    chats = _load_known_chats(path)
    assert chats[0]["topic_title"] == "New topic"


def test_get_known_chats_deduplicates_same_chat_and_topic(tmp_path: Path) -> None:
    path = storage.TARGETS_PATH
    storage.save_json(
        path,
        [
            {"chat_id": 123, "title": "Chat", "topic_id": 10},
            {"chat_id": 123, "title": "Chat duplicate", "topic_id": "10"},
            {"chat_id": 124, "title": "Other"},
        ],
    )

    chats = storage.get_known_chats()
    assert len(chats) == 2
    assert chats[0]["chat_id"] == 123
    assert int(chats[0]["topic_id"]) == 10
    assert chats[1]["chat_id"] == 124


def test_unregister_chat_without_topic_removes_all_topics(tmp_path: Path) -> None:
    path = storage.TARGETS_PATH
    storage.save_json(
        path,
        [
            {"chat_id": 123, "title": "Chat", "topic_id": 10},
            {"chat_id": 123, "title": "Chat", "topic_id": 11},
            {"chat_id": 124, "title": "Other"},
        ],
    )

    storage.unregister_chat(123)

    chats = storage.load_json(path, [])
    assert chats == [{"chat_id": 124, "title": "Other"}]


def test_unregister_chat_with_topic_removes_only_selected_topic(tmp_path: Path) -> None:
    path = storage.TARGETS_PATH
    storage.save_json(
        path,
        [
            {"chat_id": 123, "title": "Chat", "topic_id": 10},
            {"chat_id": 123, "title": "Chat", "topic_id": 11},
            {"chat_id": 124, "title": "Other"},
        ],
    )

    storage.unregister_chat(123, topic_id=10)

    chats = storage.load_json(path, [])
    assert chats == [
        {"chat_id": 123, "title": "Chat", "topic_id": 11},
        {"chat_id": 124, "title": "Other"},
    ]


def test_compact_known_chats_by_chat_id(tmp_path: Path) -> None:
    path = storage.TARGETS_PATH
    storage.save_json(
        path,
        [
            {"chat_id": 123, "title": "Chat", "topic_id": 10},
            {"chat_id": 123, "title": "Chat", "topic_id": 11},
            {"chat_id": 124, "title": "Other"},
        ],
    )

    removed = storage.compact_known_chats_by_chat_id()
    chats = storage.load_json(path, [])

    assert removed == 1
    assert chats == [
        {"chat_id": 123, "title": "Chat"},
        {"chat_id": 124, "title": "Other"},
    ]


def test_find_job_by_signature(tmp_path: Path) -> None:
    rec = {
        "job_id": "rem-1",
        "text": "08.08 MTS 20:40 room",
        "signature": "-1001:0:08.08 MTS 20:40 room",
    }
    storage.add_job_record(rec)

    found = storage.find_job_by_signature("-1001:0:08.08 MTS 20:40 room")
    missing = storage.find_job_by_signature("-1002:0:08.08 MTS 20:40 room")

    assert found is not None
    assert found["job_id"] == "rem-1"
    assert missing is None


def test_find_job_for_target_topic_text(tmp_path: Path) -> None:
    storage.add_job_record(
        {
            "job_id": "rem-legacy",
            "target_chat_id": -1001,
            "topic_id": 0,
            "text": "08.08 MTS 20:40 room",
        }
    )
    same = storage.find_job_for_target_topic_text("-1001", 0, "08.08 MTS 20:40 room")
    other_topic = storage.find_job_for_target_topic_text("-1001", 10, "08.08 MTS 20:40 room")
    other_chat = storage.find_job_for_target_topic_text("-1002", 0, "08.08 MTS 20:40 room")

    assert same is not None
    assert same["job_id"] == "rem-legacy"
    assert other_topic is None
    assert other_chat is None


def test_owner_user_ids_roundtrip() -> None:
    storage.remember_owner_user_id("panykovc", 12345)
    storage.remember_owner_user_id("@PANYKOVC", 12345)
    ids = storage.get_owner_user_ids()
    assert 12345 in ids


def test_get_offset_defaults_to_30(monkeypatch: pytest.MonkeyPatch) -> None:
    chat_id = 42
    storage.update_chat_cfg(chat_id, offset="not-a-number")
    assert storage.get_offset_for_chat(chat_id) == 30


def test_normalize_offset_handles_invalid() -> None:
    assert storage.normalize_offset(15) == 15
    assert storage.normalize_offset(-5) == 0
    assert storage.normalize_offset("bad", fallback=30) == 30


def test_resolve_tz_uses_default_moscow(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ORG_TZ", raising=False)
    monkeypatch.setattr(storage, "DEFAULT_TZ_NAME", "Europe/Moscow", raising=False)
    monkeypatch.setattr(storage, "get_chat_cfg_entry", lambda _cid: {})
    assert storage.resolve_tz_for_chat(100) == "Europe/Moscow"


def test_resolve_tz_invalid_chat_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_timezone(name: str) -> str:
        if name == "Bad/Zone":
            raise ValueError("invalid tz")
        return name

    monkeypatch.setattr(storage.pytz, "timezone", fake_timezone)
    monkeypatch.delenv("ORG_TZ", raising=False)
    monkeypatch.setattr(storage, "DEFAULT_TZ_NAME", "Europe/Moscow", raising=False)
    monkeypatch.setattr(storage, "get_chat_cfg_entry", lambda _cid: {"tz": "Bad/Zone"})

    assert storage.resolve_tz_for_chat(200) == "Europe/Moscow"


def test_resolve_tz_invalid_default_uses_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_timezone(name: str) -> str:
        if name in {"Bad/Zone", "Wrong/Zone"}:
            raise ValueError("invalid tz")
        return name

    monkeypatch.setattr(storage.pytz, "timezone", fake_timezone)
    monkeypatch.setenv("ORG_TZ", "Wrong/Zone")
    monkeypatch.setattr(storage, "DEFAULT_TZ_NAME", "", raising=False)
    monkeypatch.setattr(storage, "get_chat_cfg_entry", lambda _cid: {"tz": "Bad/Zone"})

    assert storage.resolve_tz_for_chat(300) == storage.pytz.utc
