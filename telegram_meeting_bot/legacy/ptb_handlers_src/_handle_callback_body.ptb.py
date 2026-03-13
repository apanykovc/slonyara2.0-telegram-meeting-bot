async def _handle_callback_body(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _set_log_user(update)
    q = update.callback_query
    if not q or not q.data:
        return
    chat_id = q.message.chat.id
    user = q.from_user
    uid = user.id
    admin = is_admin(user)
    data = q.data

    def _user_payload(u: User | None) -> dict[str, Any] | None:
        if u is None:
            return None
        return {
            "user_id": u.id,
            "username": u.username,
            "full_name": u.full_name,
            "first_name": u.first_name,
            "last_name": u.last_name,
        }

    if data.startswith(f"{CB_PICK_CHAT}:"):
        parts = data.split(":", 3)
        if len(parts) < 4:
            return
        sel = parts[1]
        topic = parts[2]
        token = parts[3]
        pend = context.user_data.get("pending_reminders", {}).pop(token, None)
        if not pend:
            msg = await reply_text_safe(q.message, "❌ Не найдено ожидающее напоминание.")
            auto_delete(msg, context)
            return
        try:
            cfg_chat_id = int(sel)
        except ValueError:
            cfg_chat_id = sel
        topic_id = None if topic == "0" else int(topic)
        await schedule_reminder_core(pend["text"], cfg_chat_id, update, context, user, topic_override=topic_id)
        try:
            await q.message.delete()
        except Exception:
            pass
        return

    # Главные меню
    if data == CB_DISABLED:
        return

    if data == CB_MENU:
        text = menu_text_for(chat_id)
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=main_menu_kb(is_admin(user)), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=main_menu_kb(is_admin(user)), parse_mode="Markdown")
        return

    if data == CB_SETTINGS:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Только администратор может менять настройки.")
            auto_delete(msg, context)
            return
        text = "⚙️ *Настройки чата*\n\n" + menu_text_for(chat_id)
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=settings_menu_kb(is_owner(user)), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=settings_menu_kb(is_owner(user)), parse_mode="Markdown")
        return

    if data == CB_ADMINS:
        if not is_owner(user):
            msg = await reply_text_safe(q.message, "⛔ Только владелец может управлять администраторами.")
            auto_delete(msg, context)
            return
        text = render_admins_text(ADMIN_USERNAMES)
        try:
            await edit_text_safe(q.edit_message_text, 
                text,
                reply_markup=admins_menu_kb(ADMIN_USERNAMES),
                parse_mode="Markdown",
            )
        except Exception:
            await reply_text_safe(q.message, 
                text,
                reply_markup=admins_menu_kb(ADMIN_USERNAMES),
                parse_mode="Markdown",
            )
        return

    if data == CB_ADMIN_ADD:
        if not is_owner(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        context.user_data[AWAIT_ADMIN] = True
        msg = await reply_text_safe(q.message, "Отправьте @username для добавления в админы.")
        auto_delete(msg, context, 60)
        return

    if data.startswith(f"{CB_ADMIN_DEL}:"):
        if not is_owner(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        uname = data.split(":", 1)[1]
        removed = remove_admin_username(uname)
        note = "✅ Удалён" if removed else "❌ Не найден"
        text = render_admins_text(ADMIN_USERNAMES)
        try:
            await edit_text_safe(q.edit_message_text, 
                text,
                reply_markup=admins_menu_kb(ADMIN_USERNAMES),
                parse_mode="Markdown",
            )
        except Exception:
            await reply_text_safe(q.message, 
                text,
                reply_markup=admins_menu_kb(ADMIN_USERNAMES),
                parse_mode="Markdown",
            )
        info = await reply_text_safe(q.message, f"{note}: @{uname}")
        auto_delete(info, context)
        return

    if data == CB_ACTIVE or data.startswith(f"{CB_ACTIVE_PAGE}:"):
        page = 1
        if data.startswith(f"{CB_ACTIVE_PAGE}:"):
            try:
                page = max(1, int(data.split(":")[1]))
            except Exception:
                page = 1
        if admin:
            if chat_id > 0:
                jobs_all = get_jobs_store()
            else:
                jobs_all = [j for j in get_jobs_store() if j.get("target_chat_id") == chat_id]
        else:
            if chat_id > 0:
                jobs_all = [j for j in get_jobs_store() if j.get("author_id") == uid]
                allowed = set()
                for j in jobs_all:
                    cid = j.get("target_chat_id")
                    if cid in allowed:
                        continue
                    try:
                        member = await context.bot.get_chat_member(cid, uid)
                        if member.status not in ("left", "kicked"):
                            allowed.add(cid)
                    except Exception:
                        pass
                jobs_all = [j for j in jobs_all if j.get("target_chat_id") in allowed]
            else:
                jobs_all = [
                    j
                    for j in get_jobs_store()
                    if j.get("target_chat_id") == chat_id and j.get("author_id") == uid
                ]
        jobs = sorted(jobs_all, key=lambda x: x.get("run_at_utc", ""))
        if not jobs:
            msg = await reply_text_safe(q.message, 
                "Пока нет активных напоминаний.",
                reply_markup=main_menu_kb(admin),
            )
            auto_delete(msg, context)
            return
        pages_total = max(1, (len(jobs) + PAGE_SIZE - 1) // PAGE_SIZE)
        page = min(page, pages_total)
        start = (page - 1) * PAGE_SIZE
        chunk = jobs[start:start + PAGE_SIZE]
        text_out = render_active_text(chunk, len(jobs_all), page, pages_total, admin)
        try:
            await edit_text_safe(q.edit_message_text, 
                text_out,
                reply_markup=active_kb(chunk, page, pages_total, uid, admin),
                parse_mode="HTML",
            )
        except Exception:
            await reply_text_safe(q.message, 
                text_out,
                reply_markup=active_kb(chunk, page, pages_total, uid, admin),
                parse_mode="HTML",
            )
        return

    if data.startswith(f"{CB_ACTIVE_CLEAR}:"):
        if not admin:
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        parts = data.split(":")
        if len(parts) < 3:
            return
        view = parts[1] if len(parts) > 1 else "all"
        try:
            page = max(1, int(parts[2]))
        except Exception:
            page = 1
        if len(parts) == 3:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да", callback_data=f"{CB_ACTIVE_CLEAR}:{view}:{page}:y")],
                [InlineKeyboardButton("❌ Нет", callback_data=f"{CB_ACTIVE_PAGE}:{page}")],
            ])
            text = "Очистить активные напоминания?\nВы уверены?"
            await edit_text_safe(q.edit_message_text, text, reply_markup=kb)
            return
        records = list(get_jobs_store())
        removed = 0
        for rec in records:
            job_id = rec.get("job_id")
            if not job_id:
                continue
            jobs = context.job_queue.get_jobs_by_name(job_id)
            for job in jobs:
                job.schedule_removal()
            release_signature(rec.get("signature"))
            archived = archive_job(
                job_id,
                rec=rec,
                reason="bulk_clear",
                removed_by=_user_payload(user),
            )
            if not archived:
                remove_job_record(job_id)
            audit_log(
                "REM_CANCELED",
                reminder_id=job_id,
                chat_id=rec.get("target_chat_id"),
                topic_id=rec.get("topic_id"),
                user_id=uid,
                title=rec.get("text"),
                reason="bulk_clear",
            )
            removed += 1
        note = "🧹 Активные напоминания очищены" if removed else "Активных напоминаний уже нет"
        msg = await edit_text_safe(q.edit_message_text, note)
        auto_delete(msg, context)
        await ensure_panel(update, context)
        return

    if data == CB_HELP:
        text = show_help_text(update)
        try:
            await edit_text_safe(q.edit_message_text,
                text, reply_markup=main_menu_kb(is_admin(user)), parse_mode="Markdown"
            )
        except Exception:
            try:
                await reply_text_safe(q.message, 
                    text, reply_markup=main_menu_kb(is_admin(user)), parse_mode="Markdown"
                )
            except Exception:
                await reply_text_safe(q.message, 
                    text, reply_markup=main_menu_kb(is_admin(user))
                )
        return

    # ---- TZ (таймзона) ----
    if data == CB_SET_TZ:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        tz = resolve_tz_for_chat(chat_id)
        text = f"🌍 Текущая TZ: *{tz.zone}*\nВыберите пресет или введите вручную."
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=tz_menu_kb(), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=tz_menu_kb(), parse_mode="Markdown")
        return

    if data == CB_SET_TZ_LOCAL:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        tz_name = os.environ.get("ORG_TZ") or get_localzone_name()
        update_chat_cfg(chat_id, tz=tz_name)
        await reply_text_safe(q.message, f"✅ TZ установлена: *{tz_name}*", parse_mode="Markdown")
        await ensure_panel(update, context)
        return

    if data == CB_SET_OFFSET:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        offset = get_offset_for_chat(chat_id)
        text = f"⏳ Текущий оффсет: *{offset} мин*"
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=offset_menu_kb(), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=offset_menu_kb(), parse_mode="Markdown")
        return

    if data == CB_OFF_DEC:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        off = max(1, get_offset_for_chat(chat_id) - 5)
        update_chat_cfg(chat_id, offset=off)
        await reply_text_safe(q.message, f"✅ Оффсет: *{off} мин*", parse_mode="Markdown")
        await ensure_panel(update, context)
        return

    if data == CB_OFF_INC:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        off = min(1440, get_offset_for_chat(chat_id) + 5)
        update_chat_cfg(chat_id, offset=off)
        await reply_text_safe(q.message, f"✅ Оффсет: *{off} мин*", parse_mode="Markdown")
        await ensure_panel(update, context)
        return

    if data in (CB_OFF_PRESET_10, CB_OFF_PRESET_15, CB_OFF_PRESET_20, CB_OFF_PRESET_30):
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        preset_map = { CB_OFF_PRESET_10: 10, CB_OFF_PRESET_15: 15, CB_OFF_PRESET_20: 20, CB_OFF_PRESET_30: 30 }
        preset = preset_map[data]
        update_chat_cfg(chat_id, offset=preset)
        await reply_text_safe(q.message, f"✅ Оффсет: *{preset} мин*", parse_mode="Markdown")
        await ensure_panel(update, context)
        return

    # ---- ЧАТЫ ----
    if data == CB_CHATS:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        known = get_known_chats()
        text = "📋 Зарегистрированные чаты"
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=chats_menu_kb(known), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=chats_menu_kb(known), parse_mode="Markdown")
        return

    if data == CB_ARCHIVE:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        items, total, page, pages_total = get_archive_page(1, PAGE_SIZE)
        text = render_archive_text(items, total, page, pages_total, page_size=PAGE_SIZE)
        markup = archive_kb(page, pages_total, has_entries=bool(items), can_clear=True and total > 0)
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=markup, parse_mode="HTML")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=markup, parse_mode="HTML")
        return

    if data.startswith(f"{CB_ARCHIVE_PAGE}:"):
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        try:
            page_req = int(data.split(":", 1)[1])
        except Exception:
            page_req = 1
        items, total, page, pages_total = get_archive_page(page_req, PAGE_SIZE)
        text = render_archive_text(items, total, page, pages_total, page_size=PAGE_SIZE)
        markup = archive_kb(page, pages_total, has_entries=bool(items), can_clear=True and total > 0)
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=markup, parse_mode="HTML")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=markup, parse_mode="HTML")
        return

    if data == CB_ARCHIVE_CLEAR:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        text = "❓ <b>Очистить архив?</b>\nВы уверены? Это действие необратимо."
        try:
            await edit_text_safe(
                q.edit_message_text,
                text,
                reply_markup=archive_clear_confirm_kb(),
                parse_mode="HTML",
            )
        except Exception:
            await reply_text_safe(
                q.message,
                text,
                reply_markup=archive_clear_confirm_kb(),
                parse_mode="HTML",
            )
        return

    if data == CB_ARCHIVE_CLEAR_CONFIRM:
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        removed = clear_archive()
        notice = "Архив очищен." if removed else "Архив уже пуст."
        items, total, page, pages_total = get_archive_page(1, PAGE_SIZE)
        base_text = render_archive_text(items, total, page, pages_total, page_size=PAGE_SIZE)
        if notice:
            base_text = f"{base_text}\n\n<i>{notice}</i>"
        markup = archive_kb(page, pages_total, has_entries=bool(items), can_clear=True and total > 0)
        try:
            await edit_text_safe(q.edit_message_text, base_text, reply_markup=markup, parse_mode="HTML")
        except Exception:
            await reply_text_safe(q.message, base_text, reply_markup=markup, parse_mode="HTML")
        return

    if data.startswith(f"{CB_CHAT_DEL}:"):
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        parts = data.split(":", 2)
        if len(parts) < 3:
            return
        sel = parts[1]
        topic = parts[2]
        topic_val = None if topic == "0" else int(topic)
        unregister_chat(sel, topic_val)
        removed_by = _user_payload(user)
        affected = get_jobs_for_chat(sel, topic_val)
        for rec in affected:
            job_id = rec.get("job_id")
            if not job_id:
                continue
            jobs = context.job_queue.get_jobs_by_name(job_id)
            for job in jobs:
                job.schedule_removal()
            release_signature(rec.get("signature"))
            archive_job(
                job_id,
                rec=rec,
                reason="chat_unregistered",
                removed_by=removed_by,
            )
        known = get_known_chats()
        text = "🗑️ Чат удалён"
        try:
            await edit_text_safe(q.edit_message_text, text, reply_markup=chats_menu_kb(known), parse_mode="Markdown")
        except Exception:
            await reply_text_safe(q.message, text, reply_markup=chats_menu_kb(known), parse_mode="Markdown")
        return

    # ---- Меню действий по задаче ----
    if data.startswith(f"{CB_ACTIONS}:"):
        parts = data.split(":")
        job_id = parts[1] if len(parts) > 1 else None
        if not job_id:
            return
        rec = get_job_record(job_id)
        if not (rec and (is_admin(user) or rec.get("author_id") == uid)):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        if len(parts) == 3 and parts[2] == "close":
            try:
                await q.message.delete()
            except Exception:
                pass
            return
        text = f"*Действия*\n{escape_md(rec.get('text', ''))}"
        if q.message.reply_markup and q.message.text and q.message.text.startswith("*Действия*"):
            await edit_text_safe(q.edit_message_text, text, reply_markup=actions_kb(job_id, is_admin(user)), parse_mode="Markdown")
        else:
            msg = await reply_text_safe(q.message, text, reply_markup=actions_kb(job_id, is_admin(user)), parse_mode="Markdown")
            auto_delete(msg, context, 60)
        return

    # ---- МГНОВЕННАЯ ОТПРАВКА / ОТМЕНА / СДВИГ ----
    if data.startswith(f"{CB_SENDNOW}:"):
        parts = data.split(":")
        job_id = parts[1] if len(parts) > 1 else None
        rec = get_job_record(job_id) if job_id else None
        if not (rec and (is_admin(user) or rec.get("author_id") == uid)):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        if len(parts) == 2:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да", callback_data=f"{CB_SENDNOW}:{job_id}:y")],
                [InlineKeyboardButton("↩️ Назад", callback_data=f"{CB_ACTIONS}:{job_id}")],
            ])
            await edit_text_safe(q.edit_message_text, "Отправить напоминание сейчас?", reply_markup=kb)
            return
        jobs = context.job_queue.get_jobs_by_name(job_id)
        if jobs:
            jobs[0].schedule_removal()
        dummy_ctx = SimpleNamespace(
            job=SimpleNamespace(name=job_id, data=rec),
            job_queue=context.job_queue,
            application=context.application,
            bot=context.bot,
        )
        await send_reminder(dummy_ctx)
        msg = await edit_text_safe(q.edit_message_text, f"📤 Отправлено\n{rec.get('text','')}")
        auto_delete(msg, context)
        dummy = SimpleNamespace(
            effective_chat=SimpleNamespace(id=rec.get("source_chat_id", chat_id)),
            effective_message=None,
        )
        await ensure_panel(dummy, context)
        return

    if data.startswith(f"{CB_CANCEL}:"):
        parts = data.split(":")
        job_id = parts[1] if len(parts) > 1 else None
        rec = get_job_record(job_id) if job_id else None
        if not (rec and (is_admin(user) or rec.get("author_id") == uid)):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        if len(parts) == 2:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да", callback_data=f"{CB_CANCEL}:{job_id}:y")],
                [InlineKeyboardButton("❌ Нет", callback_data=f"{CB_ACTIONS}:{job_id}")],
            ])
            text = "Отменить напоминание?\nВы уверены?"
            if rec and rec.get("text"):
                text = f"{text}\n\n{rec.get('text')}"
            await edit_text_safe(q.edit_message_text, text, reply_markup=kb)
            return
        jobs = context.job_queue.get_jobs_by_name(job_id)
        if jobs:
            jobs[0].schedule_removal()
        if rec:
            release_signature(rec.get("signature"))
        removed = False
        if rec:
            removed = archive_job(
                job_id,
                rec=rec,
                reason="manual_cancel",
                removed_by=_user_payload(user),
            )
        if not removed:
            remove_job_record(job_id)
        if rec and rec.get("confirm_chat_id") and rec.get("confirm_message_id"):
            try:
                await edit_text_safe(
                    context.bot.edit_message_text,
                    f"❌ *Отменено*\n{rec.get('text','')}",
                    chat_id=rec["confirm_chat_id"],
                    message_id=rec["confirm_message_id"],
                    parse_mode="Markdown",
                    where="bot.cancel.confirm",
                )
            except Exception:
                pass
        msg = await edit_text_safe(q.edit_message_text, "🗑️ Напоминание отменено")
        auto_delete(msg, context)
        audit_log(
            "REM_CANCELED",
            reminder_id=job_id,
            chat_id=rec.get("target_chat_id") if rec else None,
            topic_id=rec.get("topic_id") if rec else None,
            user_id=uid,
            title=rec.get("text") if rec else None,
            reason="manual",
        )
        dummy = SimpleNamespace(
            effective_chat=SimpleNamespace(id=rec.get("source_chat_id", chat_id)),
            effective_message=None,
        )
        await ensure_panel(dummy, context)
        return

    if data.startswith(f"{CB_SHIFT}:"):
        if not is_admin(user):
            msg = await reply_text_safe(q.message, "⛔ Нет доступа.")
            auto_delete(msg, context)
            return
        try:
            _, job_id, minutes_str = data.split(":")
            minutes = int(minutes_str)
        except Exception:
            return await reply_text_safe(q.message, "Некорректный формат сдвига.")
        jobs = context.job_queue.get_jobs_by_name(job_id)
        rec = get_job_record(job_id)
        if not rec:
            return await reply_text_safe(q.message, "Задача не найдена.")
        payload = (jobs[0].data or {}) if jobs else {
            "target_chat_id": rec.get("target_chat_id"),
            "topic_id": rec.get("topic_id"),
            "text": rec.get("text"),
            "source_chat_id": rec.get("source_chat_id"),
            "target_title": rec.get("target_title"),
            "author_id": rec.get("author_id"),
            "author_username": rec.get("author_username"),
            "created_at_utc": rec.get("created_at_utc"),
        }
        rrule = rec.get("rrule", RR_ONCE)
        if jobs:
            jobs[0].schedule_removal()

        new_job_id = f"rem-{uuid.uuid4().hex}"
        new_run_at = (datetime.now(pytz.utc) + timedelta(minutes=minutes)).isoformat()
        context.job_queue.run_once(
            send_reminder,
            when=minutes * 60,
            name=new_job_id,
            data={**payload, "job_id": new_job_id},
            chat_id=payload.get("target_chat_id"),
        )
        remove_job_record(job_id)
        add_job_record({
            **payload,
            "job_id": new_job_id,
            "run_at_utc": new_run_at,
            "confirm_chat_id": rec.get("confirm_chat_id"),
            "confirm_message_id": rec.get("confirm_message_id"),
            "rrule": rrule,
        })

        if rec.get("confirm_chat_id") and rec.get("confirm_message_id"):
            try:
                await edit_text_safe(
                    context.bot.edit_message_text,
                    f"⏩ *Смещено* на +{minutes} мин\n{payload.get('text','')}",
                    chat_id=rec["confirm_chat_id"],
                    message_id=rec["confirm_message_id"],
                    reply_markup=job_kb(new_job_id, rrule),
                    parse_mode="Markdown",
                    where="bot.shift.confirm",
                )
            except Exception:
                pass

        msg2 = await reply_text_safe(q.message, 
            f"⏩ Смещено на +{minutes} мин. Новый id: `{new_job_id}`", parse_mode="Markdown"
        )
        auto_delete(msg2, context)
        audit_log(
            "REM_RESCHEDULED",
            reminder_id=new_job_id,
            previous_id=job_id,
            chat_id=payload.get("target_chat_id"),
            topic_id=payload.get("topic_id"),
            title=payload.get("text"),
            user_id=uid,
            when=new_run_at,
            reason="manual_shift",
        )
        dummy = SimpleNamespace(effective_chat=SimpleNamespace(id=payload.get("source_chat_id", chat_id)), effective_message=None)
        await ensure_panel(dummy, context)
        return

    # ---- Переключение RRULE ----
    if data.startswith(f"{CB_RRULE}:"):
        if not is_admin(user):
            return await reply_text_safe(q.message, "⛔ Нет доступа.")
        try:
            _, job_id, current = data.split(":")
        except Exception:
            return
        rec = get_job_record(job_id)
        if not rec:
            return await reply_text_safe(q.message, "Задача не найдена.")
        cycle = {RR_ONCE: RR_DAILY, RR_DAILY: RR_WEEKLY, RR_WEEKLY: RR_ONCE}
        new_rule = cycle.get(current, RR_ONCE)
        upsert_job_record(job_id, {"rrule": new_rule})
        try:
            await edit_text_safe(
                context.bot.edit_message_text,
                (
                    f"📌 *Запланировано*\n{rec.get('text','')}\n"
                    f"🔁 Повтор: *{'разово' if new_rule==RR_ONCE else ('ежедневно' if new_rule==RR_DAILY else 'еженедельно')}*"
                ),
                chat_id=rec["confirm_chat_id"],
                message_id=rec["confirm_message_id"],
                reply_markup=job_kb(job_id, new_rule) if is_admin(user) else None,
                parse_mode="Markdown",
                where="bot.rrule.confirm",
            )
        except Exception:
            pass
        await reply_text_safe(q.message, f"🔁 Режим повтора: *{new_rule}*", parse_mode="Markdown")
        return


# ==========================
# ----- КОЛБЭК ЗАДАЧИ -----
# ==========================

