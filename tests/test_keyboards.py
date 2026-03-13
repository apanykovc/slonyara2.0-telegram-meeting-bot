import pytest

aiogram = pytest.importorskip("aiogram")

from telegram_meeting_bot.ui.keyboards import chats_menu_kb, choose_chat_kb


def _callback_values(markup):
    values = []
    for row in markup.inline_keyboard:
        for btn in row:
            values.append(btn.callback_data)
    return values


def test_choose_chat_kb_deduplicates_same_chat_id():
    chats = [
        {"chat_id": -1001, "title": "Team", "topic_id": 10},
        {"chat_id": -1001, "title": "Team", "topic_id": 11},
        {"chat_id": -1002, "title": "Ops", "topic_id": 1},
    ]

    kb = choose_chat_kb(chats, "tok")
    callbacks = _callback_values(kb)

    pick_callbacks = [x for x in callbacks if x and x.startswith("pick_chat:")]
    assert len(pick_callbacks) == 2
    assert "pick_chat:-1001:10:tok" in pick_callbacks or "pick_chat:-1001:11:tok" in pick_callbacks
    assert any(cb.startswith("pick_chat:-1002:") for cb in pick_callbacks)


def test_chats_menu_kb_shows_unique_chat_ids():
    known = [
        {"chat_id": -1001, "title": "Team", "topic_id": 10},
        {"chat_id": -1001, "title": "Team", "topic_id": 11},
        {"chat_id": -1002, "title": "Ops", "topic_id": 1},
    ]

    kb = chats_menu_kb(known)
    callbacks = _callback_values(kb)

    delete_callbacks = [x for x in callbacks if x and x.startswith("chat_del:")]
    assert delete_callbacks == ["chat_del:-1001:0", "chat_del:-1002:0"]


def test_choose_chat_kb_supports_pagination():
    chats = [{"chat_id": -(1000 + i), "title": f"Chat {i}", "topic_id": 0} for i in range(1, 16)]

    kb = choose_chat_kb(chats, "tok", page=2, page_size=5)
    callbacks = _callback_values(kb)

    pick_callbacks = [x for x in callbacks if x and x.startswith("pick_chat:")]
    assert len(pick_callbacks) == 5
    assert any(x == "pick_chat_page:tok:1" for x in callbacks)
    assert any(x == "pick_chat_page:tok:3" for x in callbacks)
