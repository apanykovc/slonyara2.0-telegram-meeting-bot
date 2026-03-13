import pytest
from datetime import datetime, timedelta, tzinfo

from telegram_meeting_bot.core import parsing as core_parsing

try:
    from telegram_meeting_bot.bot import main as legacy_main
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    legacy_main = None


PARSERS = [core_parsing.parse_meeting_message]
if legacy_main is not None:
    PARSERS.append(legacy_main.parse_meeting_message)


class FixedOffsetTZ(tzinfo):
    def __init__(self, offset_hours: int, name: str):
        self._offset = timedelta(hours=offset_hours)
        self._name = name

    def utcoffset(self, dt):
        return self._offset

    def tzname(self, dt):
        return self._name

    def dst(self, dt):
        return timedelta(0)

    def localize(self, dt: datetime) -> datetime:
        return dt.replace(tzinfo=self)


@pytest.fixture(scope="module")
def tz():
    return FixedOffsetTZ(3, "MSK")


@pytest.fixture(autouse=True)
def fixed_now(monkeypatch, tz):
    fixed_naive = datetime(2024, 5, 1, 12, 0)

    class DummyDateTime(datetime):
        @classmethod
        def now(cls, tz_arg=None):
            if tz_arg is None:
                return fixed_naive
            if hasattr(tz_arg, "localize"):
                return tz_arg.localize(fixed_naive)
            return fixed_naive.replace(tzinfo=tz_arg)

    monkeypatch.setattr(core_parsing, "datetime", DummyDateTime)
    if legacy_main is not None:
        monkeypatch.setattr(legacy_main, "datetime", DummyDateTime)
    yield


@pytest.mark.parametrize(
    "parser",
    PARSERS,
)
def test_parse_with_optional_ticket(parser, tz):
    result = parser("08.08 МТС 20:40 2в", tz)
    assert result is not None
    assert result["ticket"] == ""
    assert result["canonical_full"] == "08.08 МТС 20:40 2в"
    assert result["reminder_text"].endswith("2в")


@pytest.mark.parametrize(
    "parser",
    PARSERS,
)
def test_parse_accepts_ticket_with_extra_spaces(parser, tz):
    result = parser(" 08/08   МТС   20.40   2в    88634  ", tz)
    assert result is not None
    assert result["time_str"] == "20:40"
    assert result["ticket"] == "88634"
    assert result["canonical_full"] == "08.08 МТС 20:40 2в 88634"


@pytest.mark.parametrize(
    "parser",
    PARSERS,
)
def test_parse_requires_room(parser, tz):
    assert parser("08.08 МТС 20:40", tz) is None
