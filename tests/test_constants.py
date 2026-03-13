import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from telegram_meeting_bot.core.constants import _load_username_set


def test_load_username_set_missing_file(tmp_path: Path) -> None:
    path = tmp_path / "admins.json"
    result = _load_username_set(path, fallback={"slonyara"})

    assert result == {"slonyara"}
    assert not path.exists(), "helper must not create files when they are absent"


def test_load_username_set_merges_valid_file(tmp_path: Path) -> None:
    path = tmp_path / "owners.json"
    path.write_text(json.dumps(["@Alice", "", "Bob"]))

    result = _load_username_set(path, fallback={"slonyara"})

    assert result == {"alice", "bob", "slonyara"}


def test_load_username_set_handles_corrupt_file(tmp_path: Path) -> None:
    path = tmp_path / "admins.json"
    path.write_text("not json")

    result = _load_username_set(path, fallback={"admin"})

    assert result == {"admin"}
