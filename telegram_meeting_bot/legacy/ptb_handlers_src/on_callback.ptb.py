async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.data:
        return

    data = q.data

    if data == CB_DISABLED or data.startswith(f"{CB_DISABLED}:"):
        try:
            await q.answer("⏳ Уже обрабатываю…", cache_time=1)
        except Exception:
            pass
        return

    key = _callback_lock_key(q)
    if not _acquire_callback_lock(context, key):
        try:
            await q.answer("⏳ Уже обрабатываю…", cache_time=1)
        except Exception:
            pass
        return

    idem_raw = f"{data}|{getattr(q, 'from_user', None) and q.from_user.id}".encode("utf-8", "ignore")
    idem_key = hashlib.blake2b(idem_raw, digest_size=12).hexdigest()
    acquired, entry = _start_idempotent(context, idem_key)
    if not acquired:
        try:
            if entry and entry.get("status") == "done":
                await q.answer("✅ Уже выполнено", cache_time=1)
            else:
                await q.answer("⏳ Уже обрабатываю…", cache_time=1)
        except Exception:
            pass
        _release_callback_lock(context, key)
        return

    try:
        await q.answer("Принято, готовлю…", cache_time=1)
    except Exception:
        pass

    await freeze_query_markup(q)

    async def _runner() -> None:
        try:
            await _handle_callback_body(update, context)
            _mark_idempotent_done(context, idem_key)
        except Exception:
            _reset_idempotent(context, idem_key)
            raise
        finally:
            _release_callback_lock(context, key)

    context.application.create_task(_runner())

