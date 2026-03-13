from pathlib import Path

from telegram_meeting_bot.core import release_history


def test_release_history_record_and_read(tmp_path: Path, monkeypatch):
    history_path = tmp_path / "release_history.json"
    monkeypatch.setattr(release_history, "RELEASE_HISTORY_PATH", history_path)
    monkeypatch.setattr(
        release_history,
        "current_revision",
        lambda: {"version": "2.5.0", "commit": "abc123", "branch": "main", "dirty": False},
    )

    first = release_history.record_startup_revision(max_entries=10)
    second = release_history.record_startup_revision(max_entries=10)
    history = release_history.get_history(limit=10)

    assert first["commit"] == "abc123"
    assert second["commit"] == "abc123"
    assert len(history) == 1
    assert history[0]["commit"] == "abc123"
