from pathlib import Path

from telegram_meeting_bot.core import feature_flags


def test_feature_flags_roundtrip(tmp_path: Path, monkeypatch):
    flags_path = tmp_path / "feature_flags.json"
    monkeypatch.setattr(feature_flags, "FEATURE_FLAGS_PATH", flags_path)

    feature_flags.set_flag("alerts.error_burst.enabled", False)
    feature_flags.set_flag("alerts.owner_only", True)

    assert feature_flags.is_enabled("alerts.error_burst.enabled", True) is False
    assert feature_flags.is_enabled("alerts.owner_only", False) is True
    flags = feature_flags.list_flags()
    assert flags["alerts.error_burst.enabled"] is False
    assert flags["alerts.owner_only"] is True
