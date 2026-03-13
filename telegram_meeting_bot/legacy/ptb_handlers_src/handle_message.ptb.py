async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _set_log_user(update)
    if not update.message or not update.message.text:
        return
    text_in = update.message.text.strip()
    chat = update.effective_chat
    chat_id = chat.id
    user = update.effective_user
    uid = user.id

    # ожидания ввода
    if context.user_data.get(AWAIT_TZ):
        try:
            pytz.timezone(text_in)
        except Exception:
            return await reply_text_safe(update.message, "Некорректная TZ. Пример: `Europe/Moscow`", parse_mode="Markdown")
        update_chat_cfg(chat_id, tz=text_in)
        context.user_data.pop(AWAIT_TZ, None)
        await reply_text_safe(update.message, f"✅ TZ установлена: *{text_in}*", parse_mode="Markdown")
        return

    if context.user_data.get(AWAIT_ADMIN):
        context.user_data.pop(AWAIT_ADMIN, None)
        if not is_owner(user):
            return
        uname = text_in.lstrip("@").strip().lower()
        if not uname:
            await reply_text_safe(update.message, "Нужен логин вида @username")
            return
        added = add_admin_username(uname)
        if added:
            await reply_text_safe(update.message, f"✅ @{uname} теперь админ")
        else:
            await reply_text_safe(update.message, "⚠️ Не удалось добавить (возможно, уже есть)")
        text = render_admins_text(ADMIN_USERNAMES)
        await reply_text_safe(update.message, 
            text,
            reply_markup=admins_menu_kb(ADMIN_USERNAMES),
            parse_mode="Markdown",
        )
        return

    # Ключевые фразы
    if text_in.lower() in {"меню", "menu"}:
        return await cmd_start(update, context)

    # Парсим встречу с учётом типа чата
    if chat.type == "private":
        candidates = []
        for c in get_known_chats():
            cid = c.get("chat_id")
            try:
                member = await context.bot.get_chat_member(cid, uid)
                if member.status not in ("left", "kicked"):
                    candidates.append(c)
            except Exception:
                continue
        if candidates:
            token = uuid.uuid4().hex
            context.user_data.setdefault("pending_reminders", {})[token] = {"text": text_in}
            candidates.append({"chat_id": chat_id, "title": "Личный чат"})
            return await reply_text_safe(update.message, 
                "📨 Куда отправить напоминание?",
                reply_markup=choose_chat_kb(candidates, token),
            )
        await schedule_reminder_core(text_in, chat_id, update, context, user)
        return

    if chat.type in ("group", "supergroup"):
        title = chat.title or (chat.username and f"@{chat.username}") or str(chat.id)
        register_chat(chat.id, title, topic_id=update.message.message_thread_id)
    await schedule_reminder_core(
        text_in,
        chat_id,
        update,
        context,
        user,
        topic_override=update.message.message_thread_id,
    )
# ==========================
# ----- ВОССТАНОВЛЕНИЕ ЗАДАЧ ПРИ СТАРТЕ -----
# ==========================
def restore_jobs(app: Application):
    items = get_jobs_store()
    now_utc = datetime.now(pytz.utc)
    restored = 0
    kept = []
    caught_up = 0
    for r in items:
        try:
            run_at = datetime.fromisoformat(r["run_at_utc"])
        except Exception:
            continue
        delay = (run_at - now_utc).total_seconds()
        if delay <= 0:
            if delay >= -CATCHUP_WINDOW_SECONDS:
                app.job_queue.run_once(
                    send_reminder,
                    when=1,
                    name=r["job_id"],
                    data={
                        "job_id": r["job_id"],
                        "target_chat_id": r["target_chat_id"],
                        "topic_id": r.get("topic_id"),
                        "text": r["text"],
                        "source_chat_id": r.get("source_chat_id"),
                    },
                    chat_id=r["target_chat_id"],
                )
                caught_up += 1
            continue
        app.job_queue.run_once(
            send_reminder,
            when=delay,
            name=r["job_id"],
            data={
                "job_id": r["job_id"],
                "target_chat_id": r["target_chat_id"],
                "topic_id": r.get("topic_id"),
                "text": r["text"],
                "source_chat_id": r.get("source_chat_id"),
            },
            chat_id=r["target_chat_id"],
        )
        kept.append(r)
        restored += 1
    set_jobs_store(kept)
    app_log("восстановление завершено", restored=restored, caught_up=caught_up)


# ==========================
# ----- ИНИЦИАЛИЗАЦИЯ: setMyCommands -----
# ==========================