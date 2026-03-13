async def cmd_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _set_log_user(update)
    if not is_admin(update.effective_user):
        msg = await reply_text_safe(update.message, "⛔ Только администратор может регистрировать чаты.")
        auto_delete(msg, context)
        return
    chat = update.effective_chat
    if chat.type == "private":
        msg = await reply_text_safe(update.message, "ℹ️ Личные диалоги регистрировать не нужно.")
        auto_delete(msg, context)
        return
    msg = update.message
    topic_id = getattr(msg, "message_thread_id", None)
    topic_title = None
    if topic_id is not None:
        try:
            topic = await context.bot.get_forum_topic(chat.id, topic_id)
            topic_title = topic.name
        except Exception:
            topic_title = str(topic_id)
    title = chat.title or (chat.username and f"@{chat.username}") or str(chat.id)
    if topic_id is not None:
        display_title = f"{title} / {topic_title}" if topic_title else f"{title} / {topic_id}"
    else:
        display_title = title
    added = register_chat(chat.id, display_title, topic_id, topic_title)
    note = "✅ Чат добавлен в список." if added else "ℹ️ Этот чат уже зарегистрирован."
    await reply_text_safe(update.message, note)
